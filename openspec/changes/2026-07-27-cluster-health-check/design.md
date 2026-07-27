## Context

一期 Go Exporter 已交付 Java 对齐的 `/metrics` + 6 个 cron 采集任务 + admin 客户端（含 ACL 签名，已 live 验证）。现有指标均为**被动状态快照**，无法证明"此刻能发能消费"。本 change 新增**主动端到端健康探针**：常驻 producer 持续发测试消息、常驻 consumer 消费回来，秒级暴露生产/消费链路健康。

**无 Java 对应**--这是 Go-only 新增能力，不参与 Java `/metrics` diff，不触碰保真指标（黄金法则）。producer/consumer 用法参考已验证的 `internal/service/probe_live_test.go`。目标 broker 协议：RocketMQ 4.9.8。

相关方：SRE（告警 + k8s 探针）、运维（多集群 namesrv 常见）。

## Goals / Non-Goals

**Goals**：主动流式探测 produce+consume；多集群 per-cluster 隔离；5 个 `rocketmq_health_check_*` 指标（带 `cluster` 标签）；`/healthz` HTTP 端点；recency 推导状态；自愈（集群恢复即恢复）；优雅退出。

**Non-Goals**（见 proposal）：Java 对齐指标；OTLP/gRPC；自动建 topic；单条超时/在途表/probeID；预检 supervisor；高阶延迟分位；共享 topic 多集群。

## Decisions

### D1 - 探测模型：主动端到端流式探测

持续发送（非周期 tick）。producer 按可配速率 R（默认 2 条/秒/集群）循环发消息，body = 发送时间戳（UnixMilli，同进程同时钟无偏移）。consumer 始终订阅、收到即处理。

- **produce**：`SendSync` 返回 `SEND_OK` -> `produce_total{result="success"}`++、更新 `last_produce_success`、记 produce 延迟（SendSync 往返耗时）；失败 -> `produce_total{result="failure"}`++ + `slog.Warn`。
- **consume**：handler 收到消息 -> `consume_total`++、`consume_latency = now - body时间戳`、更新 `last_consume`；始终返回 `ConsumeSuccess`（所有消息都算消费成功，ack 丢弃，防堆积）。
- **producer 共享一个实例**（一个连接，向各集群 topic 发送，按目标 topic 归属 cluster）；每集群一个 produce goroutine 控速；每集群一个 consumer 实例（各自 group）。
- **producer retry**：`WithRetry(2)`（与 `probe_live_test.go` 一致，普通客户端行为）；retry 耗尽后的失败才计 `result="failure"`。

### D2 - 多集群：per-cluster topic + per-cluster consumer + `cluster` 标签

一个 namesrv 可能有多个集群（现有 collector 已按 `ClusterAddrTable` 多集群枚举采集，如 `internal/task/collect.go:179`）。健康检查必须跟上：

- **集群发现**：启动时 `Admin.ExamineBrokerClusterInfo().ClusterAddrTable` 取集群名列表；每 `cluster-refresh`（默认 5m）重新发现，diff 出新增/消失集群，新增的起探针、消失的撤探针（关 consumer、停 goroutine）。发现失败不退出，下个刷新重试并 `slog.Warn`。
- **命名**：`topic = topic-prefix + cluster`（默认 `HealthCheckTopic-<cluster>`），`group = group-prefix + cluster`（默认 `HealthCheckGroup-<cluster>`）。用户为每个集群预建对应 topic。不同 topic 名经 namesrv 路由天然隔离到各自集群 broker，实现 per-cluster 探测。
- **指标归属**：所有指标加 `cluster` 标签（值为集群名）。produce 按目标 topic 归属；consume 按所在 consumer 实例归属。
- **多集群共享 topic 不支持**：路由无法区分目标集群。单集群部署（共享 topic/group）仍工作，`cluster` 标签为单一值。
- **足迹**：N 集群 = N 个 produce goroutine + N 个 consumer 实例 + 总速率 R×N。RocketMQ 一般 1~2 集群/namesrv，可接受。

### D3 - 状态推导：recency（无滚动窗口/无在途表/无预检）

简化核心：不做单条消费超时判定、不做 probeID 关联、不做在途表、不做预检 supervisor。状态由"最近一次成功是否在 recency 内"推导：

- 1s 评估 tick（per cluster）：`status{produce} = (now - last_produce_success < recency) ? 1 : 0`；`status{consume} = (now - last_consume < recency) ? 1 : 0`；`status{overall} = produce AND consume`。
- **失败体现**：topic 缺失 -> `SendSync` 报错 -> `produce_total{failure}`++ + 日志，`last_produce_success` 不更新 -> recency 后 `status{produce}=0`。consumer 挂/收不到 -> `last_consume` 不更新 -> recency 后 `status{consume}=0`。无事件时 1s tick 也翻转状态（consumer 挂 -> `last_consume` 陈旧 -> 0）。
- **自愈**：topic 后建 -> send 转成功 -> `last_produce_success` 新鲜 -> status 复 1。consumer 重连（client-go 自带重连重试）-> 收到消息 -> `last_consume` 新鲜 -> status 复 1。行为与普通 RocketMQ 客户端一致。
- **recency 默认 5s**：producer 2/s 时，5s 内有 ~10 次发送尝试，足够判定；transient 单次失败下次即恢复，不抖动。
- **启动期**：首次成功前 `last_success=0`（epoch）-> `now - 0` 巨大 -> status=0（正确，尚未探测成功）。

### D4 - 指标设计（Go-only 新增，全部带 `cluster` 标签）

全部前缀 `rocketmq_health_check_`，与对齐指标物理隔离；PR 与本 spec 显式记录（黄金法则：新增允许，须记录）。

| 指标名 | 类型 | 标签 | HELP |
|---|---|---|---|
| `rocketmq_health_check_produce_total` | counter | `cluster`, `result`=success\|failure | RocketMQ health check produce result count |
| `rocketmq_health_check_consume_total` | counter | `cluster` | RocketMQ health check consumed message count |
| `rocketmq_health_check_status` | gauge | `cluster`, `check`=produce\|consume\|overall | RocketMQ health check status (1=healthy, 0=unhealthy) |
| `rocketmq_health_check_latency_seconds` | gauge | `cluster`, `check`=produce\|consume | RocketMQ health check latency in seconds |
| `rocketmq_health_check_last_success_timestamp_seconds` | gauge | `cluster`, `check`=produce\|consume\|overall | Unix timestamp of last successful health check |

标签顺序按上表所列（counter/gauge 保真精神：标签名与顺序稳定，便于 PromQL/告警）。`consume_total` 无 `result` 标签（所有收到的消息都算 success；消费失败由 `status{consume}` recency 体现，不逐条计数）。

告警建议：`status{check="overall"}==0`（不健康）；`time()-last_success_timestamp_seconds{check="overall"}>N`（探测停滞）；`rate(produce_total{result="failure"}[1m])>0`（生产异常）；`latency_seconds{check="consume"}>N`（消费延迟 SLO）。

### D5 - 暴露面：`/metrics` 合并 + `/healthz`

- **`/metrics`**：现有 handler 不改逻辑，仅其 `Gather` 来源（collector.Gather）追加健康 family（D6）。健康 family 与对齐 family 一起输出。
- **`/healthz`**（新 handler，路径可配默认 `/healthz`）：读 collector 的 `HealthDetail()`，返回 JSON；HTTP 200 当所有集群 `overall=1`，任一 `overall=0` 或尚未探测 -> 503。
  ```
  {"overall":"ok|fail","last_probe_at":<unix>,
   "clusters":{
     "clusterA":{"overall":"ok|fail","produce":{"status":"ok|fail","latency":<s>,"last_success":<unix>,"last_error":<str>},"consume":{...}},
     "clusterB":{...}}}
  ```
- 两者读同一份缓存结果，无额外探测开销。`/healthz` 的 `last_error` 字符串（produce 失败原因 / consumer 错误）仅存于 prober 的 per-cluster 最近结果（非 TTL，供详情读；不进 Prometheus 指标）。

### D6 - collector 集成（方案 2）+ 编码器修复

用户选定方案 2（扩展现有 collector），非独立 registry。

- **collector 加健康 store**（`internal/collector/health.go` 新文件）：
  - 非 TTL 计数存储：`produceCount map[produceKey]int64`（key=`{cluster,result}`）、`consumeCount map[string]int64`（key=`cluster`）。计数器是累积量，不参与 TTL sweep。
  - 非 TTL gauge 存储：`status map[statusKey]int`（key=`{cluster,check}`）、`latency map[latencyKey]float64`、`lastSuccess map[statusKey]int64`。gauge 由 prober 1s tick 写入最新值。
  - `AddHealthProduce(cluster, result string)`、`AddHealthConsume(cluster string)`、`SetHealthStatus(cluster, check string, v int)`、`SetHealthLatency(cluster, check string, secs float64)`、`SetHealthLastSuccess(cluster, check string, ts int64)`、`HealthDetail() HealthSnapshot`。
- **`Gather()` 追加**：在现有 family 之后追加 2 个 counter family（`counterFamily` 新构建器，同构于 `gaugeFamily` 但 `Type=COUNTER`）+ 3 个 gauge family。标签名/顺序按 D4。
- **`HealthDetail()`**：返回 per-cluster 最近状态（含 last_error 字符串），供 `/healthz`。last_error 由 prober 通过 `SetHealthLastError(cluster, check, errStr)` 写入（非 TTL map）。
- **main.go 编码器修复**（`cmd/rmq-exporter/main.go` 空族回退分支）：
  ```go
  // 旧: fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", mf.GetName(), mf.GetHelp(), mf.GetName())
  // 新: 按 mf.GetType() 输出 "counter"/"gauge"/...
  typ := strings.ToLower(mf.GetType().String())
  fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", mf.GetName(), mf.GetHelp(), mf.GetName(), typ)
  ```
  引入 counter 后必须（counter 空族否则被误标 gauge）；也修一处潜在 bug，对未来非 gauge family 正确。对现有对齐指标（全 gauge）输出不变。

### D7 - 配置（新增 flag/env，沿用现有模式）

| flag | env | 默认 | 说明 |
|---|---|---|---|
| `--health-check-enabled` | `RMQ_HEALTH_CHECK_ENABLED` | `true` | 开关 |
| `--health-check-topic-prefix` | `RMQ_HEALTH_CHECK_TOPIC_PREFIX` | `HealthCheckTopic-` | topic = prefix + cluster |
| `--health-check-group-prefix` | `RMQ_HEALTH_CHECK_GROUP_PREFIX` | `HealthCheckGroup-` | group = prefix + cluster |
| `--health-check-rate` | `RMQ_HEALTH_CHECK_RATE` | `2.0` | 每集群发送速率（条/秒） |
| `--health-check-recency` | `RMQ_HEALTH_CHECK_RECENCY` | `5s` | status recency 阈值 |
| `--health-check-cluster-refresh` | `RMQ_HEALTH_CHECK_CLUSTER_REFRESH` | `5m` | 集群发现刷新间隔 |
| `--health-check-path` | `RMQ_HEALTH_CHECK_PATH` | `/healthz` | HTTP 路径 |

ACL 复用现有 `--enable-acl/--access-key/--secret-key`，producer/consumer 用同一凭据。文档注明：accessKey 需对每个集群的 `HealthCheckTopic-<cluster>` 有 PUB+SUB 权限。

`enabled=true` 但启动时集群发现失败：`slog.Warn` 并继续（不退出 exporter，其他 6 个采集任务照常）；下个 `cluster-refresh` 重试。发现成功但某集群 topic 不存在：不预检，produce 自然失败计数 + 日志。

### D8 - 并发与生命周期

- `context.Context` 贯穿；`SIGTERM`/`SIGINT` -> cancel -> 停 produce goroutine -> 关各 consumer -> 关 producer -> 退出。
- **per-cluster 状态**：每个集群一个 `clusterProbe` 结构（cluster 名、topic、group、consumer 实例、本地计数/last_success/latency/last_error），用 per-cluster mutex 保护；prober 持 `map[cluster]*clusterProbe`（全局 mutex 保护 map 增删）。
- **1s 评估 tick**：prober 一个 goroutine，遍历 `map[cluster]*clusterProbe`，算 recency status，写 collector。
- **集群刷新**：一个 goroutine 每 `cluster-refresh` 重新发现，diff 后起/撤 `clusterProbe`。撤 = 停 goroutine + `consumer.Shutdown` + 从 collector 清该 cluster 的健康样本（可选：保留 stale 或清空；默认清空，避免幽灵集群残留）。
- 共享 producer 由 prober 持有；各 clusterProbe 的 produce goroutine 调用同一 producer 的 `SendSync`（producer 线程安全）。

### D9 - 测试策略

- **producer/consumer 接口抽象**：Prober 依赖小接口 `type Producer interface{ SendSync(ctx, msg) (Result, error) }` / `type Consumer interface{ Subscribe(topic, handler) error; Start() error; Shutdown() error }`，便于单测注入 stub。真实实现用 `rocketmq-client-go/v2` 适配器（参考 `probe_live_test.go`）。
- **单测**（纯函数/可注入，table-driven）：
  - recency 推导：注入假 clock + 一组 last_success，断言 status 翻转。
  - `AddHealth*` -> `Gather()` 的 family 输出（name/type/labels/order/value），含 counter 空族的 `# TYPE counter` 行（验证 D6 编码器修复）。
  - `HealthDetail()` JSON 形状 + 多集群聚合 + overall 推导。
  - 集群刷新 diff：mock 集群列表变化，断言 `clusterProbe` 增删。
  - produce/consume 计数 + 延迟计算（body 时间戳 -> latency）。
- **Live 测试**（`RMQ_LIVE_TESTS=1` 守卫，复用现有 live 基建 + `probeCredentials()`）：
  - 对真实 4.9.8 集群（ACL 场景照搬凭据）跑 Prober，断言 `/metrics` 出现 `rocketmq_health_check_status{cluster=...,check="overall"} 1`、`/healthz` 返回 200、`consume_total` 递增、`latency_seconds{check="consume"}` 合理。
  - 多集群 live 测试可选（需测试环境有多集群 namesrv；否则单集群 live + 多集群单测覆盖）。
- **对齐验收**：健康指标是新增项，不参与 Java `/metrics` diff；但需确认它们出现在 Go `/metrics` 末尾且不破坏现有对齐指标输出（golden test 扩展）。

## Risks / Trade-offs

- [测试 topic 残留消息] consumer 长时间掉线后重连，涌入积压旧消息 -> `consume_latency` 瞬时偏大、`consume_total` 跳增，但 `last_consume` 立即新鲜 -> status 复 1。latency 偏大是真实积压信号，非误报。-> 接受；若需平滑可在 spec 后续加"latency 仅更新 recency 内消息"过滤（本 change 不做）。
- [多集群足迹] N 集群 = N consumer 实例。-> 接受（一般 1~2 集群）；超大 N 可后续改共享 consumer + 按 topic 归属（本 change 不做）。
- [recency 抖动] recency 边界附近状态可能翻转。-> 缓解：recency 默认 5s 给足余量；告警用 `for: 1m` 平滑。
- [集群刷新竞态] 刷新 diff 与 produce goroutine/consumer 并发。-> 缓解：per-cluster mutex + 撤探针先停 goroutine 再关 consumer；单测覆盖 diff 并发。
- [counter 空族 TYPE] 编码器修复必须覆盖 counter，否则空族被误标 gauge。-> 缓解：单测断言 counter 空族输出 `# TYPE ... counter`。
- [producer SendSync 阻塞] 单集群 send 慢会拖慢该集群 produce goroutine，不影响其他集群（各自 goroutine）。-> 可接受；`WithRetry(2)` + SendSync 内部超时兜底。
- [黄金法则] 新增 5 个 `rocketmq_health_check_*` 指标。-> 已在本 change 显式记录；不触碰对齐指标；前缀隔离。

## Migration Plan

纯新增功能，无原地迁移。上线步骤：
1. 用户为每个集群预建 `HealthCheckTopic-<cluster>`，并授予 accessKey 的 PUB+SUB 权限。
2. 配置 `--health-check-enabled=true`（默认开）+ topic/group prefix（默认即可）+ rate/recency。
3. 启动 exporter；`/metrics` 末尾出现 `rocketmq_health_check_*`；`/healthz` 返回 200（集群健康时）。
4. Grafana 加 health check 面板/告警（`status{check="overall"}==0`）。
5. 回滚 = `--health-check-enabled=false`，健康探针停，其他采集不受影响。

## Open Questions

- O1：集群刷新时消失集群的健康样本是否清空？当前默认清空（避免幽灵残留）。若 SRE 希望保留最后值观察衰减，可改为保留--待反馈。
- O2：`/healthz` 是否需要 `?cluster=xxx` 查询参数返回单集群？当前返回全量 JSON。若有 k8s per-cluster 探针需求再加。
- O3：produce retry `WithRetry(2)` 是否会掩盖瞬时故障导致 status 不够灵敏？recency 5s 内 ~10 次尝试应能穿透 retry 暴露持续故障；transient 故障本就不该告警。待 live 验证调参。
