## Why

Apache RocketMQ Exporter 目前仅有 Java/Spring Boot 实现（`v0.0.3-SNAPSHOT`）。运维侧希望有一个静态 Go 二进制：内存更低、无 JVM、容器镜像更小。本次重写必须在 Prometheus 边界上与 Java 版**行为完全一致**——Grafana dashboard 10477 与 `example.rules` 依赖精确的指标名、类型和标签集——因此这是移植，不是重新设计。

## What Changes

- **新建 Go module** `github.com/wcf/rmq-exporter`，暴露单个 `GET /metrics` Prometheus 端点（默认 `:5557`，`flag`+env 配置，无 Spring/YAML）。
- **6 个 cron 采集任务**（`15 0/1 * * * ?` 语义，加载时 `?`→`*`，用 `robfig/cron/v3` + `WithSeconds()`），移植自 `task/MetricsCollectTask.java`，写入带 TTL 的缓存指标存储（`outOfTimeSeconds`）。
- **Prometheus collector** 镜像 `collector/RMQMetricsCollector.java`——每个 gauge 的名字、HELP 文本、标签名及顺序逐字一致（如 `rocketmq_group_diff{group,topic,countOfOnlineConsumers,msgModel}`、`rocketmq_brokeruntime_put_tps10{cluster,brokerIP,brokerHost,des,boottime,broker_version}`）。
- **Admin 客户端封装** 移植自 `service/client/MQAdminExtImpl.java` + `MQAdminInstance.java`，在 RocketMQ 4.x remoting 协议上实现 `MetricsCollectTask` 用到的读 RPC。**关键点**：`rocketmq-client-go/v2` 的 `admin` 包几乎缺少所有这些读 RPC，且其 wire layer 是内部包不可外部导入——因此我们把 remoting wire layer 及所需内部件 vendor 进本模块，原生实现这些读 RPC（Path A，已批准）。
- **有界 worker pool**（core/max=10，queue=5000，丢弃最旧）替代 `ClientMetricCollectorFixedThreadPoolExecutor` + `DiscardOldestPolicy`。
- 纯函数移植（`util/Utils.java`、`model/BrokerRuntimeStats.java`），配 table-driven 单测并对照 Java 输出。
- 所有源文件保留 Apache 2.0 头注释。

## Non-goals

- **OTLP gRPC**（Java 端口 5559，`otlp/*`）——二期；`internal/otlp/` 预留，不实现。
- **ACL 签名实现**——`enable-acl` 开关保留并解析，但一期**不附加** RPC 签名；开启 ACL 的 broker 一期不可用。签名实现移至 Phase 1.5，待有真实 ACL broker 再做。
- **配置保真**——不沿用 `application.yml` 的 key；现代化为 `flag`+env，默认值可调整。
- **joor 反射 hack**（仅 `viewMessage` 非指标路径用到）——删除。
- **Java 深层类层级 / Spring DI**——用结构体 + 手动构造注入替代；不引入 DI 框架。
- **指标改名/新增**——任何偏离 Java 版之处必须在本 change 中显式记录。
- **Java 8 / Spring Boot 2.7 运行时约束**——与 Go 二进制无关。

## Capabilities

### New Capabilities
- `metrics-endpoint`：暴露 `GET /metrics`，逐字保真输出完整 Java 指标集；TTL 缓存 gauge 存储；`promhttp`/自定义 collector 装配。
- `collection-tasks`：`MetricsCollectTask.java` 的 6 个 cron 方法，其按 topic/broker 的尽力而为错误处理（`TOPIC_NOT_EXIST`/`CONSUMER_NOT_ONLINE`/`SYSTEM_ERROR` 静默降级），以及 client 指标的"丢弃最旧"有界 worker pool。
- `rocketmq-admin-client`：`MQAdminExtImpl`/`MQAdminInstance` 的移植——vendor remoting wire layer、实现任务所需读 RPC、ACL、生命周期、基于 `ResponseCode` 的错误映射。
- `configuration`：`flag`+env 加载 namesrv、ACL、缓存 TTL、cron 表达式（`?`→`*`）、pool 规模、HTTP 监听地址/路径。

### Modified Capabilities
<!-- 无——这是全新的 Go module；openspec/specs/ 为空。 -->

## Impact

- **新增代码**：`cmd/rmq-exporter/`、`internal/{config,collector,task,service,model,otlp}/`（按 `CLAUDE.md` 布局），外加 vendor 进来的 remoting 包（置于 `internal/`）。
- **依赖**：`prometheus/client_golang` v1.23.2、`robfig/cron/v3` v3.0.1、vendor 的 `rocketmq-client-go/v2` 内部件。
- **保真事实来源**：`/Users/wcf/java-project/rocketmq-exporter` 下的 `collector/RMQMetricsCollector.java`、`task/MetricsCollectTask.java`、`service/client/MQAdminExtImpl.java`、`model/BrokerRuntimeStats.java`、`util/Utils.java`。
- **验收**：Java 与 Go 版并行运行，对排序后的 `/metrics` 做 diff（指标名/标签/值容差）。
