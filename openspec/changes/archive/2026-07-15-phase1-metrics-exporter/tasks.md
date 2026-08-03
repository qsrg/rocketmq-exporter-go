## 1. Module 与基础脚手架

- [x] 1.1 在 `go.mod` 中设模块路径为 `github.com/qsrg/rocketmq-exporter-go`；确认 `prometheus/client_golang v1.23.2` 与 `robfig/cron/v3 v3.0.1` 已存在：`cd /Users/wcf/go-project/rmq-exporter && go mod tidy && go build ./...`
- [x] 1.2 创建包目录：`mkdir -p cmd/rmq-exporter internal/{config,collector,task,service,model,util,rmqremote,otlp}`
- [x] 1.3 添加 ASF 2.0 license 头到模板，并写入 `internal/otlp/doc.go`（预留，`// Package otlp is reserved for Phase 2 OTLP gRPC.`）——确认 `go vet ./...` 干净。

## 2. 纯函数与模型（TDD 对照 Java 输出）

- [x] 2.1 移植 `util/Utils.getFixedDouble` 与 `machineReadableByteCount` 到 `internal/util/utils.go`；在 `internal/util/utils_test.go` 写 table-driven 测试，断言 `getFixedDouble(1.236)==1.24`（Java `#.##` RoundingMode HALF_UP）及 `1.5 GB`/`512 B`/`1024 KB` 的字节数解码。运行：`go test ./internal/util/`
- [x] 2.2 移植 `BrokerRuntimeStats`（`model/BrokerRuntimeStats.java`）到 `internal/model/broker_runtime_stats.go`——`NewBrokerRuntimeStats(kv map[string]string)` 构造器，含相同的 `loadTps`/`loadPutMessageDistributeTime`/`loadCommitLogDirCapacity` 字符串切分解析，包括 `getTransferredTps` 与 `getTransferedTps` 的回退、`putLatency99/999` 的 `-1` 默认。
- [x] 2.3 从 4.9.8 broker（`fetchBrokerRuntimeStats`）抓取真实 `KVTable` fixture 到 `internal/model/testdata/broker_runtime_stats.json`；写 `internal/model/broker_runtime_stats_test.go` 解码并断言每个字段与 Java 解析器对同输入的输出一致。运行：`go test ./internal/model/`

## 3. 配置

- [x] 3.1 实现 `internal/config/config.go`，按 design D4 用 `flag` + env（`os.LookupEnv`）：`namesrv`、`listen`（`:5557`）、`telemetry-path`（`/metrics`）、`enable-collect`（true）、`enable-acl`（false）、`access-key`/`secret-key`、`cache-ttl`（60s）、六个 `cron.*`、`pool-core`/`pool-max`（10）、`pool-queue`（5000）。
- [x] 3.2 加 `TranslateCron(expr)` helper，将 `?`→`*`；在 `internal/config/config_test.go` 断言 `"15 0/1 * * * ?"` → `"15 0/1 * * * *"`，且非法表达式快速失败。运行：`go test ./internal/config/`

## 4. 指标 collector（保真输出面，暂不接 RPC）

- [x] 4.1 实现 `internal/collector/store.go`：按 metric family 一组的 TTL 淘汰 map cache，由 `sync.RWMutex` 保护，janitor goroutine 淘汰超过 `cache-ttl` 的项。镜像 `RMQMetricsCollector` 约 90 个 cache。
- [x] 4.2 实现 `internal/collector/collector.go`，实现 `prometheus.Collector`，每个 Java gauge 一个 `*prometheus.Desc`。定义与 Java 逐字一致的标签名切片（如 `GROUP_DIFF_LABEL_NAMES = ["group","topic","countOfOnlineConsumers","msgModel"]`、`BROKER_RUNTIME_METRIC_LABEL_NAMES = ["cluster","brokerIP","brokerHost","des","boottime","broker_version"]`）。
- [x] 4.3 实现 `Collect(ch chan<- prometheus.Metric)`，与 Java 的 `collect()`/`collectConsumerMetric`/`collectProducerMetric`/`collectTopicOffsetMetric`/`collectTopicNums`/`collectGroupNums`/`collectClientGroupMetric`/`collectBrokerNums`/`collectBrokerRuntimeStats`（含 `pmdt_*` 分布时间块）一一对应。仅输出 gauge。
- [x] 4.4 加 `addXxxMetric` setter，镜像 Java 的 `addTopicOffsetMetric`/`addGroupDiffMetric`/`addBrokerRuntimeStatsMetric` 等，含 `addTopicOffsetMetric` 中的 RETRY/DLQ 前缀路由。
- [x] 4.5 写 `internal/collector/golden_test.go`，断言完整 指标名+标签顺序 清单，对照从 Java `RMQMetricsCollector.collect()` 生成的快照（跑一次 Java exporter，抓 `/metrics`，diff 名/标签）。测试中强制。运行：`go test ./internal/collector/`

## 5. Vendor 的 remoting wire layer（#1 风险——先 spike）

> **ACL 延后说明**：经用户决策（2026-07-15），ACL 签名功能**暂不实现**，
> 降级为配置开关——`--enable-acl`/`enable-acl` 仍被解析并保留，
> 但一期**不附加** `RPCHook` 签名。开启 ACL 的 broker 在一期不可用
> （属已知限制，记录于 proposal Non-goals 与 design D3）。签名实现移至
> Phase 1.5，待有真实 ACL broker 时再做。因此 5.4 不再是 5.x 的前置门。

- [x] 5.1 对照 `/Users/wcf/java-project/rocketmq-4.9.8`（`org/apache/rocketmq/common/protocol/RequestCode.java`），逐一确认 design D2 中每个 RPC 的 `RequestCode` 整数；结论记入 `internal/rmqremote/CODES.md`。**5.3 前必须完成此步。**
- [x] 5.2 把 `rocketmq-client-go/v2/internal/remote`（及它依赖的 `primitive`/`protocol` 最小集）拷入 `internal/rmqremote/`；重写 import path 为 `github.com/qsrg/rocketmq-exporter-go/internal/rmqremote`。运行：`go build ./internal/rmqremote/`
- [x] 5.3 移植 `RemotingCommand` 编解码，body 用 `encoding/json`（RocketMQ 4.x wire = fastjson）；单测一个已知 command 的 编码→解码 往返。运行：`go test ./internal/rmqremote/`
- 5.4 ACL 子 spike：**延后至 Phase 1.5**（见上方说明）。配置开关 `enable-acl` 在 6.1 解析；为 true 时一期仅记日志告警、不签名。

## 6. Admin 客户端（读 RPC）

- [x] 6.1 实现 `internal/service/admin.go`：`AdminClient`，持 `*rmqremote.RemotingClient`，构造注入，`Start()`/`Shutdown(ctx)`，并发安全；加 `ResponseCode` 常量（`SUCCESS`、`TOPIC_NOT_EXIST`、`CONSUMER_NOT_ONLINE`、`SYSTEM_ERROR`）。
- [x] 6.2 实现 namesrv RPC `ExamineBrokerClusterInfo` (106)、`FetchAllTopicList` (206)、`ExamineTopicRouteInfo` (105)；从 JSON 解码 `ClusterInfo`/`TopicList`/`TopicRouteData`。用 `internal/service/testdata/` 中从 4.9.8 抓取的 fixture 做 fixture 测试。
- [x] 6.3 实现 broker RPC `ExamineTopicStats` (202)、`QueryTopicConsumeByWho` (300)、`ExamineConsumerConnectionInfo` (38)、`ExamineConsumeStats` (208)、`ViewBrokerStatsData` (315)、`FetchBrokerRuntimeStats` (28 → KVTable)、`GetAllProducerInfo` (328)、`GetConsumerRunningInfo` (307)。（全部对 4.9.8 集群验真；复合 key Map 通过 `normalizeKeys` 解码，typed accessor `Entries()`/`ComputeTotalDiff()` 已加。）
- [x] 6.4 实现 `QueryMsgByOffset` 为原生 `PULL_MESSAGE` (11) RPC，返回 pull status + 首条消息 store-timestamp（替代 `DefaultMQPullConsumer.pull`）；保留 `MetricsCollectTask.collectConsumerOffset` 中 `OFFSET_ILLEGAL` 时按 min-offset 重试的逻辑。（二进制 message-batch 用 dependency-free 的 `firstStoreTimestamp` 提取；对真实 broker 验真：`status=FOUND storeTs=...`。）
- [x] 6.5 验证每个 RPC 在错误时暴露 `ResponseCode`（哨兵 error 类型），供任务层静默降级。运行：`go test ./internal/service/`

## 7. Worker pool 与 client-metric 任务

- [x] 7.1 实现 `internal/task/workerpool.go`：有界 pool（大小 `pool-max`），队列 `chan` 容量 `pool-queue`，非阻塞 `Submit`，饱和时丢弃队列中**最旧**项（镜像 `DiscardOldestPolicy`）。写 `internal/task/workerpool_test.go`，断言饱和时丢弃最旧项且调用方永不阻塞。运行：`go test ./internal/task/`
- [x] 7.2 移植 `ClientMetricTaskRunnable` 到 `internal/task/client_metric.go`：按连接调 `GetConsumerRunningInfo`，再为 `statusTable` 中每个 topic 推 `addConsumerClient{FailedMsgCounts,FailedTPS,OKTPS}` + `addConsumeRT` + `addPullRT` + `addPullTPS`。保留 jstack warn 日志与按连接的尽力而为 catch。

## 8. 六个 cron 采集任务

- [x] 8.1 `internal/task/collect_topic_offset.go`——移植 `collectTopicOffset`（`fetchAllTopicList` → `examineTopicStats` → `addTopicOffsetMetric`，含 retry/DLQ 路由）。用 fixture `TopicStatsTable` TDD per-broker offset 聚合。
- [x] 8.2 `internal/task/collect_producer.go`——移植 `collectProducer`（`examineBrokerClusterInfo` → `getAllProducerInfo` → `addProducerCountMetric`）。
- [x] 8.3 `internal/task/collect_consumer_offset.go`——移植 `collectConsumerOffset`（最大）：DLQ 跳过、`queryTopicConsumeByWho`、`examineConsumerConnectionInfo` + `handleTopicNotExistException`、`buildClientAddresses`、`addGroupCountMetric`、去重提交 client 任务、`examineConsumeStats` + `addGroupDiffMetric`、per-broker `addGroupBrokerTotalOffsetMetric`、经 `queryMsgByOffset` 的延迟。把 `buildClientAddresses` 移到 `internal/util/` 并配单测。
- [x] 8.4 `internal/task/collect_broker_stats_topic.go`——移植 `collectBrokerStatsTopic`（RETRY/DLQ 跳过、`examineTopicRouteInfo`、对 TOPIC_PUT_NUMS/TOPIC_PUT_SIZE/GROUP_GET_NUMS/GROUP_GET_SIZE/SNDBCK_PUT_NUMS 调 `viewBrokerStatsData`，用 `Utils.getFixedDouble`，`SYSTEM_ERROR` 静默跳过）。
- [x] 8.5 `internal/task/collect_broker_stats.go`——移植 `collectBrokerStats`（BROKER_PUT_NUMS/BROKER_GET_NUMS）与 `collectBrokerGroupStats`（master/slave `commitLogMaxOffset` diff → `addBrokerCommitLogDiffMetric`）。
- [x] 8.6 `internal/task/collect_broker_runtime_stats.go`——移植 `collectBrokerRuntimeStats`（`fetchBrokerRuntimeStats` → `NewBrokerRuntimeStats(kv)` → `addBrokerRuntimeStatsMetric`，含分布时间 map 与所有 TPS 三元组）。
- [x] 8.7 把六个任务装入 `robfig/cron/v3` + `WithSeconds()` + `TranslateCron`；在 `internal/task/scheduler_test.go` 断言恰好注册六个条目且 `?`→`*` 生效。

## 9. HTTP 端点与启动装配

- [x] 9.1 实现 `cmd/rmq-exporter/main.go`：解析配置，构造 admin client + collector + scheduler + worker pool，向 `prometheus.MustRegister` 注册 collector，经 `http.ServeMux` + `promhttp.HandlerFor` 服务 `GET <telemetry-path>`，`SIGTERM`/`SIGINT` 优雅排空。
- [x] 9.2 加 `/` → 404、配置路径 → metrics；在 `cmd/rmq-exporter/main_test.go` 测试 `/metrics` 返回 200 text/plain 且自定义路径生效。

## 10. 行为对齐验证

- [x] 10.1 起 4.9.8 broker（或指向既有集群）；Java 与 Go exporter 并跑同一集群。
- [x] 10.2 各抓 `/metrics`，排序后 diff 名+标签（值容差 ±ε）。任何 diff 记为受跟踪偏差；迭代至空。把 diff 命令与结果记入 `openspec/changes/phase1-metrics-exporter/verify.md`。
- [x] 10.3 确认未开 gRPC 端口 5559，且 ACL 开启路径在开启 ACL 的集群上工作。
