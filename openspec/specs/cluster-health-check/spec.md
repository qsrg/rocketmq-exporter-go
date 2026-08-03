# cluster-health-check Specification

## Purpose
主动端到端流式健康探针能力：常驻 producer 按速率向 per-cluster 测试 topic 发时间戳消息、常驻 push consumer 消费回来，按 recency 推导 per-cluster 生产/消费链路健康，暴露 `rocketmq_health_check_*` 指标与 `/healthz` 端点。Go-only 新增，无 Java 对应。

## Requirements
### Requirement: 主动端到端流式健康探针
系统 SHALL 运行常驻 RocketMQ producer，按配置速率（`health-check-rate`，默认 2.0 条/秒/集群）持续向测试 topic 发送消息，消息 body SHALL 为发送时间戳（UnixMilli）。系统 SHALL 运行常驻 push consumer 订阅测试 topic，收到消息 SHALL 计算端到端延迟（收到时刻 − body 时间戳）并 ack 成功。producer SHALL 共享一个实例向各集群 topic 发送；每集群 SHALL 有独立的 consumer 实例与消费组。

#### Scenario: produce 成功计数
- **WHEN** producer `SendSync` 返回 `SEND_OK`
- **THEN** `rocketmq_health_check_produce_total{cluster="<c>",result="success"}` 递增，更新该集群 produce 延迟与 `last_produce_success`。

#### Scenario: produce 失败计数
- **WHEN** `SendSync` 失败（含 topic 不存在、超时、retry 耗尽）
- **THEN** `rocketmq_health_check_produce_total{cluster="<c>",result="failure"}` 递增并记录日志，`last_produce_success` 不更新。

#### Scenario: consume 计数与延迟
- **WHEN** consumer 收到一条消息
- **THEN** `rocketmq_health_check_consume_total{cluster="<c>"}` 递增，consume 延迟 = `now − body时间戳`，`last_consume` 更新，消息 ack 成功。

### Requirement: 多集群 per-cluster 探测
系统 SHALL 从 `ExamineBrokerClusterInfo().ClusterAddrTable` 发现集群，为每个集群用 `health-check-topic-prefix+cluster` 作为测试 topic、`health-check-group-prefix+cluster` 作为消费组独立探测。所有健康检查指标 SHALL 带 `cluster` 标签。系统 SHALL 每 `health-check-cluster-refresh`（默认 5m）重新发现集群，为新增集群启动探针、为消失集群撤销探针并清空其健康样本。多集群共享单一 topic 探测 SHALL NOT 被支持（路由无法区分目标集群）；单集群共享 topic 仍工作（`cluster` 标签为单一值）。

#### Scenario: per-cluster topic 路由隔离
- **WHEN** 集群 A 与 B 均被发现
- **THEN** 系统 produce 到 `HealthCheckTopic-A` 与 `HealthCheckTopic-B`，分别路由到各自集群 broker，指标以 `cluster` 标签区分。

#### Scenario: 集群动态增删
- **WHEN** 刷新后发现新增集群 C、消失集群 B
- **THEN** 为 C 启动探针并开始产指标，撤销 B 的探针并清空其健康样本。

#### Scenario: 发现失败不退出
- **WHEN** 启动时集群发现 RPC 失败
- **THEN** exporter 不退出，记录日志，下个刷新间隔重试，其余 6 个采集任务照常。

#### Scenario: topic 缺失时探针不退出并自愈
- **WHEN** 集群被发现但其 `HealthCheckTopic-<cluster>` 尚不存在（冷集群、topic 被删、或 broker 路由未传播）
- **THEN** produce loop 照常运行（send 失败计入 `produce_total{result="failure"}` 并记日志，`last_produce_success` 不更新），consumer 启动被 deferred 并按 `consumerRetryInterval` 周期重试；探针 SHALL NOT 因 `consumer.Start` 失败而整体退出或缺失。producer 的 send 经 broker `autoCreateTopicEnable` 自动创建 topic 后（或 operator 预建后），consumer 在下次重试启动，`status` 在 recency 内恢复为 1。

### Requirement: recency 状态推导与自愈
系统 SHALL 每 1 秒评估各集群状态：`status{produce}=1` 当且仅当 `now − last_produce_success < health-check-recency`；`status{consume}=1` 当且仅当 `now − last_consume < health-check-recency`；`status{overall}=1` 当且仅当两者均为 1。系统 SHALL NOT 维护在途表、单条消费超时或预检 supervisor。topic 或 consumer 缺失时 SHALL 依赖客户端自然失败（计数 + 日志）；集群恢复后 SHALL 自动恢复 status 为 1（与普通 RocketMQ 客户端行为一致）。

#### Scenario: 消费停滞触发不健康
- **WHEN** 某集群 consumer 超过 recency 未收到消息
- **THEN** `status{cluster="<c>",check="consume"}` 与 `status{cluster="<c>",check="overall"}` 翻为 0。

#### Scenario: 集群恢复自愈
- **WHEN** topic 后建使 produce 转成功，或 consumer 重连收到消息
- **THEN** 对应 `last_success` 新鲜，status 在下次评估翻回 1。

#### Scenario: 启动期未健康
- **WHEN** 首次成功探测前
- **THEN** `last_success` 为 0（epoch），status 为 0。

### Requirement: 健康检查 Prometheus 指标
系统 SHALL 暴露 5 个 `rocketmq_health_check_*` 指标（全部带 `cluster` 标签）：counter `produce_total{cluster,result=success|failure}`、counter `consume_total{cluster}`、gauge `status{cluster,check=produce|consume|overall}`、gauge `latency_seconds{cluster,check=produce|consume}`、gauge `last_success_timestamp_seconds{cluster,check=produce|consume|overall}`。这些指标为 Go 版新增、非 Java 对齐，已在本 change 记录。指标 SHALL 经由 metrics-endpoint 能力的 `/metrics` 暴露。

#### Scenario: 指标名与类型
- **WHEN** 健康检查运行
- **THEN** `/metrics` 含上述 5 个 family，counter 为 counter 类型，gauge 为 gauge 类型。

#### Scenario: cluster 标签区分多集群
- **WHEN** 多集群被探测
- **THEN** 每个指标样本带 `cluster` 标签，不同集群的样本可区分。

### Requirement: /healthz HTTP 端点
系统 SHALL 在 `health-check-path`（默认 `/healthz`）暴露 HTTP `GET` 端点，返回 per-cluster JSON（含每个集群的 produce/consume/overall 状态、延迟、`last_success`、`last_error`）。HTTP 状态码 SHALL 为 200 当所有集群 `overall=1`，503 当任一集群 `overall=0` 或尚未完成首次探测。`health-check-enabled=false` 时该端点 SHALL 返回 404。该端点与 `/metrics` SHALL 读同一份缓存结果，不触发额外探测。

#### Scenario: 全健康返回 200
- **WHEN** 所有集群 `overall=1`
- **THEN** `GET /healthz` 返回 200，body 为 JSON 含各集群详情。

#### Scenario: 任一不健康返回 503
- **WHEN** 任一集群 `overall=0` 或尚未探测
- **THEN** `GET /healthz` 返回 503。

#### Scenario: 健康检查禁用
- **WHEN** `health-check-enabled=false`
- **THEN** `GET /healthz` 返回 404。

### Requirement: 健康检查配置
系统 SHALL 通过 `flag` + `RMQ_` 前缀环境变量配置：`health-check-enabled`（默认 true）、`health-check-topic-prefix`（默认 `HealthCheckTopic-`）、`health-check-group-prefix`（默认 `HealthCheckGroup-`）、`health-check-rate`（默认 2.0 条/秒/集群）、`health-check-recency`（默认 5s）、`health-check-cluster-refresh`（默认 5m）、`health-check-path`（默认 `/healthz`）。ACL SHALL 复用 `enable-acl`/`access-key`/`secret-key`，producer/consumer 使用同一凭据；accessKey 需对每个集群测试 topic 有 PUB+SUB 权限。

#### Scenario: 默认值生效
- **WHEN** 未提供任何健康检查 flag/env
- **THEN** 探测速率 2.0/s，recency 5s，topic 前缀 `HealthCheckTopic-`，路径 `/healthz`。

#### Scenario: env 覆盖速率
- **WHEN** `RMQ_HEALTH_CHECK_RATE=5` 设置
- **THEN** 探测速率为 5.0 条/秒/集群。

### Requirement: 优雅退出与并发
系统 SHALL 以 `context.Context` 贯穿健康探针；收到 `SIGTERM`/`SIGINT` SHALL 停止 produce goroutine、关闭各集群 consumer、关闭共享 producer。per-cluster 状态 SHALL 由 mutex 保护；集群刷新增删探针 SHALL 与 produce/consume 并发安全。

#### Scenario: 优雅关闭
- **WHEN** 收到 SIGTERM
- **THEN** 所有 produce goroutine 停止，各 consumer 与共享 producer 调用 Shutdown，exporter 退出。

#### Scenario: 刷新与探测并发
- **WHEN** 集群刷新撤销某集群探针时该集群正有 produce 在途
- **THEN** 先停止该集群 produce goroutine 再关闭其 consumer，不产生 panic 或数据竞争。

