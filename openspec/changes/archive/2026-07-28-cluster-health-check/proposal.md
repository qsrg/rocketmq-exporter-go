## Why

一期已交付 Java Exporter 的 Go 对齐移植（`/metrics` + 6 个 cron 采集任务）。但现有指标都是**被动状态快照**--broker 活着、offset 在涨，不代表"此刻真的能发消息、能消费消息"。运维需要一个**主动端到端探针**：持续往集群发测试消息并消费回来，证明生产/消费链路实时可用，并在故障时秒级暴露。Java 版无此功能，这是 Go 版的**纯新增能力**，不在 Java 对齐范围内。

## What Changes

- **新增 `internal/health/` 包**：`Prober` 服务持有常驻 producer（共享）+ 每集群一个 push consumer，按可配速率（默认 2 条/秒/集群）持续发测试消息，body 携带发送时间戳；consumer 收到即算端到端延迟。
- **多集群**：从 `ExamineBrokerClusterInfo().ClusterAddrTable` 发现集群，每集群用 `topic-prefix+cluster` / `group-prefix+cluster` 独立探测（per-cluster topic 天然路由到该集群 broker），所有指标加 `cluster` 标签。集群发现周期性刷新（默认 5m），新增/消失集群自动加减探针。
- **健康状态推导**：无在途表、无单条超时、无预检 supervisor。topic/consumer 缺失时 send 报错或 consumer 收不到 -> 自然失败 -> 计数 + 日志；status 由 recency 推导（最近成功在 `recency` 内 = 健康）。集群恢复后自动恢复，行为与普通 RocketMQ 客户端一致。
- **5 个新增 Prometheus 指标**（`rocketmq_health_check_*` 前缀，全部带 `cluster` 标签）：`produce_total`(counter)、`consume_total`(counter)、`status`(gauge)、`latency_seconds`(gauge)、`last_success_timestamp_seconds`(gauge)。
- **HTTP `/healthz`**（路径可配）：读同一份缓存结果，返回 per-cluster JSON，所有集群健康 -> 200，任一不健康或尚未探测 -> 503。
- **collector 扩展（方案 2）**：在现有 `MetricsCollector` 加非 TTL 计数存储 + `counterFamily` 构建器 + `cluster` 标签；prober 喂计数与推导出的 gauge。
- **main.go 编码器小修复**：`/metrics` 空 family 回退当前硬编码 `# TYPE %s gauge`，改成按 `mf.GetType()` 输出类型，使 counter 空族不被误标 gauge（引入 counter 后必须，也顺带修一处潜在 bug）。

## Non-goals

- **不实现 Java 对齐指标**--本 change 只新增 `rocketmq_health_check_*`；不触碰现有保真指标（黄金法则）。
- **不做 OTLP/gRPC 健康导出**--仅 Prometheus 指标 + HTTP `/healthz`。
- **不自动创建测试 topic**--用户须为每个集群预建 `HealthCheckTopic-<cluster>` 并授 ACL 权限；exporter 只产/消。
- **不做单条消费超时/在途表/probeID 关联**--消费健康由 recency 体现，简化实现。
- **不做预检 supervisor / topicOK 标记**--依赖客户端自然失败 + 日志，与普通客户端行为一致。
- **不暴露 produce/consume 的 p99 等高阶分位**--仅平均/最近延迟；后续可加。
- **不处理共享 topic 探测多集群**--多集群必须 per-cluster topic（路由要求）；单集群共享 topic 仍工作（单 `cluster` 标签值）。

## Capabilities

### New Capabilities
- `cluster-health-check`：主动端到端流式健康探针，多集群 per-cluster 隔离探测，recency 推导状态，暴露 5 个 `rocketmq_health_check_*` 指标 + `/healthz` HTTP 端点。

### Modified Capabilities
- `metrics-endpoint`：`/metrics` 输出追加 5 个健康检查 family（带 `cluster` 标签）；`Gather()` 合并健康 family；空 family 编码器按真实 TYPE 输出。

## Impact

- **新增代码**：`internal/health/{probe.go, metrics_adapters.go, http.go, probe_test.go, probe_live_test.go}`；`internal/collector/health.go`（健康 store + family 构建器）；`internal/collector/collector.go`（加健康 store + `Gather` 追加 + `HealthDetail`）；`internal/config/config.go`（HealthCheck 配置 + flag/env）；`cmd/rmq-exporter/main.go`（装配 Prober + `/healthz` handler + 编码器 TYPE 修复）。
- **依赖**：复用现有 `rocketmq-client-go/v2`（producer/consumer，已在 `probe_live_test.go` 验证）；无新外部依赖。
- **保真事实来源**：无 Java 对应；本 change 为 Go-only 新增。producer/consumer 用法参考 `internal/service/probe_live_test.go`（已验证的 live 范式）。
- **ACL**：复用现有 `--enable-acl/--access-key/--secret-key`；accessKey 需对每个集群测试 topic 有 PUB+SUB 权限。
- **验收**：单测覆盖 recency 推导、计数/family 构建、`/healthz` JSON、多集群状态聚合；live 测试（`RMQ_LIVE_TESTS=1`）对真实 4.9.8 集群断言 `status{check="overall"}==1` 与 `/healthz` 200。
