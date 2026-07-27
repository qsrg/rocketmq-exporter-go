## Context

Java 版 RocketMQ Exporter（`v0.0.3-SNAPSHOT`，Spring Boot 2.7 / Jetty / `rocketmq-tools` 4.9.8）是行为事实来源。本 change 产出一个 Go 二进制，其 **Prometheus `/metrics` 输出面与 Java 版逐字一致**——Grafana dashboard 10477 与 `example.rules` 依赖精确的指标名、类型与标签集。CLAUDE.md 是治理规范；`/Users/wcf/java-project/rocketmq-exporter/src/main/java/org/apache/rocketmq/exporter/` 下的 Java 文件是保真参考。

相关方：运维（单个静态二进制，无 JVM）、SRE（现有 dashboard/告警必须继续可用）。目标 broker 协议：RocketMQ 4.9.8。

## Goals / Non-Goals

**Goals**：行为一致的 `/metrics`；6 个 cron 任务；admin 读 RPC 客户端；有界 worker pool；优雅退出；Apache 2.0 头注释。

**Non-Goals**（见 proposal）：OTLP gRPC（二期）；配置 key 保真；joor 反射；Java 类层级 / Spring DI；任何指标改名/新增/删除。

## Decisions

### D1 — Java → Go 子系统映射

| Java 子系统 | Go 目标 | 说明 |
|---|---|---|
| `RocketMQExporterApplication` + `RMQMetricsController` | `cmd/rmq-exporter/main.go` + `internal/collector/http.go` | `net/http` 单 handler，挂在 `webTelemetryPath`（默认 `/metrics`），默认地址 `:5557`。Spring `@SpringBootApplication` → `main` 装配。 |
| `collector/RMQMetricsCollector`（继承 `io.prometheus.client.Collector`） | `internal/collector/collector.go`，实现 `prometheus.Collector` | 每个 Java gauge 一个 `*prometheus.Desc`，名字/HELP/标签逐字一致。约 90 个 `Cache<…>` 字段 → `sync.RWMutex` 保护的 `map[key]value`，由一个 janitor goroutine 做 TTL 淘汰（替代 Guava `expireAfterWrite`）。`collect()` 与 `Collect(chan<- MetricFamilySamples)` 一一对应。 |
| `task/MetricsCollectTask`（7 个 `@Scheduled` 方法，6 个 cron key） | `internal/task/*.go` | 每个方法 → `CollectTask` 上的方法，注册到 `robfig/cron/v3` + `WithSeconds()`；加载时 `?`→`*`。 |
| `task/ClientMetricCollectorFixedThreadPoolExecutor` + `DiscardOldestPolicy` + `ClientMetricTaskRunnable` | `internal/task/workerpool.go` + `client_metric.go` | 有界 `chan`，容量 `queueSize`；非阻塞 `select` 投递，满时丢弃队列中最旧项（通过 ring buffer 或 drain 一个）以镜像 DiscardOldest。core/max=10。 |
| `service/client/MQAdminExtImpl` + `MQAdminInstance` | `internal/service/admin.go` | 仅移植任务用到的读 RPC（见 D2）；写/配置类 RPC 删除。`@Autowired` → 构造注入。 |
| `model/BrokerRuntimeStats` | `internal/model/broker_runtime_stats.go` | `KVTable`（map[string]string）→ `map[string]string` + 相同的 `loadTps`/`loadPutMessageDistributeTime`/`loadCommitLogDirCapacity` 字符串切分解析。 |
| `util/Utils` | `internal/util/utils.go` | `getFixedDouble`（`#.##` DecimalFormat → `strconv.FormatFloat(f,'f',2,-1)` 再 parse 回来——需匹配 Java 舍入）、`machineReadableByteCount`。 |
| `config/RMQConfigure` + `ScheduleConfig` + `CollectClientMetricExecutorConfig` | `internal/config/config.go` | `flag` + env，现代化默认值（见 D4）。 |
| `otlp/*` | `internal/otlp/` 预留（空） | 二期。 |

### D2 — Admin 客户端覆盖度（#1 风险）— SPIKE 结论

逐一列出 `MetricsCollectTask`（及 `ClientMetricTaskRunnable`）调用的 admin 方法及其对应的 RocketMQ 4.x `RequestCode`（**已对照 `/Users/wcf/java-project/rocketmq-4.9.8` 源码确认**，结论见 `internal/rmqremote/CODES.md`）：

| Java 方法 | RequestCode | int | 目标 |
|---|---|---|---|
| `examineBrokerClusterInfo` | `GET_BROKER_CLUSTER_INFO` | 106 | namesrv |
| `fetchAllTopicList` | `GET_ALL_TOPIC_LIST_FROM_NAMESERVER` | 206 | namesrv |
| `examineTopicStats` | `GET_TOPIC_STATS_INFO` | 202 | broker |
| `examineTopicRouteInfo` | `GET_ROUTEINFO_BY_TOPIC` | 105 | namesrv |
| `queryTopicConsumeByWho` | `QUERY_TOPIC_CONSUME_BY_WHO` | 300 | broker |
| `examineConsumerConnectionInfo` | `GET_CONSUMER_LIST_BY_GROUP` | 38 | broker |
| `examineConsumeStats` | `GET_CONSUME_STATS` | 208 | broker |
| `viewBrokerStatsData` | `VIEW_BROKER_STATS_DATA` | 315 | broker |
| `fetchBrokerRuntimeStats` | `GET_BROKER_RUNTIME_INFO`（返回 `KVTable`） | 28 | broker |
| `getAllProducerInfo` | `GET_ALL_PRODUCER_INFO` | 328 | broker |
| `getConsumerRunningInfo` | `GET_CONSUMER_RUNNING_INFO` | 307 | broker(client) |
| `queryMsgByOffset` | `PULL_MESSAGE`（经 `DefaultMQPullConsumer.pull`） | 11 | broker |

结论：任务依赖的读 RPC **一个都没有**被 `rocketmq-client-go/v2` 的 `admin` 包暴露，且其 remoting wire layer（`internal/remote`）不可外部导入。这印证了已批准的 **Path A**：把 wire layer 及所需内部件 vendor 进本模块，重写 import path，然后原生实现这些读 RPC 作为 `RemotingCommand` 往返。此 spike 是大规模编码的前置门。

### D3 — Vendoring 方法（Path A）

- 把 `rocketmq-client-go/v2/internal/remote`（及其依赖的 `protocol`/`primitive` 最小集）拷入 `internal/rmqremote/`，置于本模块自己的包路径下；重写所有 import path。
- 在 `internal/service/admin.go` 实现 `AdminClient`，持有 `*rmqremote.RemotingClient`、ACL 凭据、`invokeSync`/`invokeAsync` helper。每个读 RPC 构造正确的 `RemotingCommand`（RequestCode + ext fields），发往 namesrv 或 broker（由路由解析），用 `encoding/json` 解码 body（RocketMQ 4.x remoting body 为 JSON 序列化）。
- `ResponseCode` 整数（`TOPIC_NOT_EXIST`、`CONSUMER_NOT_ONLINE`、`SYSTEM_ERROR`、`SUCCESS`）作为 typed 常量保留；error 携带 code，供任务层做静默降级。
- ACL（**延后至 Phase 1.5**，经用户决策 2026-07-15）：一期 `enable-acl` 配置开关保留并被解析，但**不附加** `RPCHook` 签名。开关为 true 时仅记 warn 日志并继续（不签名），因此一期不支持开启 ACL 的 broker（已知限制，记于 proposal Non-goals）。HmacSHA1 签名实现待有真实 ACL broker 后补做。

### D4 — 配置

`flag` + env，默认值现代化（语义调参项保留，key 名可改）：

| flag | env | 默认 | Java 默认 |
|---|---|---|---|
| `namesrv` | `NAMESRV_ADDR` | `127.0.0.1:9876` | 同 |
| `listen` | `RMQ_LISTEN` | `:5557` | `19876`（改；已记录） |
| `telemetry-path` | `RMQ_TELEMETRY_PATH` | `/metrics` | 同 |
| `enable-collect` | `RMQ_ENABLE_COLLECT` | `true` | 同 |
| `enable-acl` | `RMQ_ENABLE_ACL` | `false` | 同 |
| `access-key`/`secret-key` | env | 空 | 同 |
| `cache-ttl` | `RMQ_CACHE_TTL` | `60s` | `outOfTimeSeconds=60` |
| `cron.*`（6 个） | env | `15 0/1 * * * ?` → `15 0/1 * * * *` | 同语义 |
| `pool-core`/`pool-max` | env | `10`/`10` | `10`/`10`（yml；Java 类默认 `20`——以 yml 为准） |
| `pool-queue` | env | `5000` | `5000` |

6 字段 cron 中的 `?` 在加载时转 `*`（依 CLAUDE.md）。

### D5 — 并发与生命周期

- `context.Context` 贯穿所有 RPC 与 cron 调度器；`SIGTERM`/`SIGINT` → cancel → 排空 worker pool → 关闭 remoting client → 退出。
- 共享指标存储：每个 cache 一把 `sync.RWMutex`（或单一 store mutex）；cron 写者 vs `/metrics` 读者。janitor 在 scrape 间隔淘汰过期项以约束内存。
- worker pool 不启无界 goroutine；`submit` 非阻塞并丢弃最旧（见 D1）。

## Risks / Trade-offs

- [Admin RPC 解码漂移] RocketMQ 4.x body 是 fastjson JSON；Go `encoding/json` 对数字/字符串字段解码可能不同（如 `null` vs 缺 key）。→ 缓解：每种 body 类型配 table-driven 解码测试，用从 4.9.8 真实 broker 抓取的 fixture；可选字段用 `*T` / `v, ok`。
- [ACL 延后的后果] 一期不签名，开启 ACL 的 broker 全部 RPC 403。→ 已知限制（记于 Non-goals），开关保留以便 Phase 1.5 无缝接入；非 ACL 集群不受影响。
- [vendor 的 remoting 代码腐化] fork `internal/remote` 意味着我们要自己维护。→ 接受此取舍；我们需要的面（TCP client、`RemotingCommand` 编解码、`invokeSync`）很小且在 4.x 上稳定。
- [丢弃最旧语义] Java `DiscardOldestPolicy` 是淘汰队首再跑新任务；Go `chan` 的 drop-oldest 必须一致（不是 drop-newest）。→ 缓解：workerpool 单测断言饱和时丢弃的是最旧项且调用方永不阻塞；对照 Java 运行的 client 指标覆盖度。
- [指标保真] 一处标签顺序错位即破坏 dashboard。→ 缓解：golden test 断言完整 name+label-order 清单，对照从 Java collector 生成的快照；CI 强制。
- [`getAllProducerInfo` 的 RequestCode] 必须从 `rocketmq-4.9.8` 源码确认确切 code，不可猜测。→ 缓解：有 task 核对 `/Users/wcf/java-project/rocketmq-4.9.8`。

## Migration Plan

全新 Go module——无原地迁移。上线：Go 与 Java 二进制并跑同一集群；diff 排序后的 `/metrics`（名/标签，值 ±ε）；连续 N 个 scrape 窗口 diff 为空后再切 dashboard。回滚 = Java exporter 继续运行。

## Open Questions

- O1：~~确认 `getAllProducerInfo` 的 RequestCode~~ **已解决**（task 5.1）：`GET_ALL_PRODUCER_INFO = 328`，见 `internal/rmqremote/CODES.md`。`ProducerTableInfo` body 形状仍待 task 6.3 用真实 fixture 验证。
- O2：Go vendor 的 remoting client 是否需要 VIP-channel（`-1099`）处理？Java 由 `isVIPChannel` 切换，默认 true。需在 4.9.8 上验证 broker 行为。
- O3：DiscardOldest vs drop-newest 的精确取舍——除非压测显示实际队列从不饱和，否则逐字移植 DiscardOldest。
