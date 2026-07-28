## 1. 配置层

- [x] 1.1 在 `internal/config/config.go` 加 `HealthCheckConfig` 结构（`Enabled`, `TopicPrefix`, `GroupPrefix`, `Rate`, `Recency`, `ClusterRefresh`, `Path`），并在 `Default()` 填默认值（true / `HealthCheckTopic-` / `HealthCheckGroup-` / 2.0 / 5s / 5m / `/healthz`）
- [x] 1.2 在 `RegisterFlags` 注册 7 个 `--health-check-*` flag，配 `RMQ_HEALTH_CHECK_*` env 回退（rate 用 float64、Recency/ClusterRefresh 用 Duration、Enabled 用 Bool）
- [x] 1.3 在 `internal/config/config_test.go` 加 table-driven 测试：默认值正确、`RMQ_HEALTH_CHECK_RATE=5` 覆盖、`--health-check-recency=10s` 覆盖、`--health-check-enabled=false` 覆盖
- [x] 1.4 `go test ./internal/config/ -run HealthCheck -v` 通过

## 2. collector 健康存储与指标 family

- [x] 2.1 新建 `internal/collector/health.go`：定义健康 store（`produceCount map[produceHealthKey]int64`、`consumeCount map[string]int64`、`status map[statusHealthKey]int`、`latency map[latencyHealthKey]float64`、`lastSuccess map[statusHealthKey]int64`、`lastError map[statusHealthKey]string`）+ `sync.Mutex`，挂在 `MetricsCollector` 上
- [x] 2.2 实现 setter：`AddHealthProduce(cluster, result string)`、`AddHealthConsume(cluster string)`、`SetHealthStatus(cluster, check string, v int)`、`SetHealthLatency(cluster, check string, secs float64)`、`SetHealthLastSuccess(cluster, check string, ts int64)`、`SetHealthLastError(cluster, check string, errStr string)`、`ClearHealthCluster(cluster string)`
- [x] 2.3 加 `counterFamily(name, help string, labelNames []string, samples []sample)` 构建器（同构 `gaugeFamily`，但 `Type=MetricType_COUNTER`）；定义健康标签切片：`healthProduceLabels=["cluster","result"]`、`healthConsumeLabels=["cluster"]`、`healthCheckLabels=["cluster","check"]`
- [x] 2.4 在 `internal/collector/collector.go` 的 `Gather()` 末尾追加 5 个健康 family：`produce_total`(counter)、`consume_total`(counter)、`status`(gauge)、`latency_seconds`(gauge)、`last_success_timestamp_seconds`(gauge)，位于现有保真 family 之后
- [x] 2.5 实现 `HealthDetail() HealthSnapshot`：返回 per-cluster 最近状态（含 `last_error` 字符串），供 `/healthz`
- [x] 2.6 新建 `internal/collector/health_test.go`：table-driven 断言 `AddHealthProduce("c1","success")` x3 后 `Gather()` 输出 `rocketmq_health_check_produce_total{cluster="c1",result="success"} 3`；断言 counter family 的 `# TYPE ... counter`；断言多 cluster 标签区分；断言 `ClearHealthCluster` 后该 cluster 样本消失；断言 `HealthDetail` JSON 字段
- [x] 2.7 `go test ./internal/collector/ -run Health -v` 通过

## 3. main.go 编码器 TYPE 修复

- [x] 3.1 在 `cmd/rmq-exporter/main.go` 的 `/metrics` handler 空 family 回退分支，把 `# TYPE %s gauge` 改为按 `strings.ToLower(mf.GetType().String())` 输出类型；确认 `strings` 已 import
- [x] 3.2 在 `internal/collector/golden_test.go` 加用例：构造一个空 counter family，断言输出 `# TYPE <name> counter`（非 gauge）；现有保真指标（全 gauge）输出不变
- [x] 3.3 `go test ./internal/collector/ ./cmd/rmq-exporter/ -v` 通过

## 4. health Prober 核心骨架

- [x] 4.1 新建 `internal/health/probe.go`：定义可注入接口 `type Producer interface { SendSync(ctx, *primitive.Message) (*primitive.SendResult, error); Shutdown(ctx) error }` 与 `type Consumer interface { Subscribe(topic string, handler) error; Start() error; Shutdown(ctx) error }`，以及 `type Clock interface { Now() time.Time }`
- [x] 4.2 定义 `clusterProbe` 结构（`cluster`, `topic`, `group`, `consumer Consumer`, 本地 `lastProduceSuccess`/`lastConsume`/`produceLatency`/`consumeLatency`/`lastProduceErr`/`lastConsumeErr` + `sync.Mutex`）
- [x] 4.3 定义 `Prober` 结构（共享 `Producer`、`map[string]*clusterProbe` + `sync.Mutex`、`*service.AdminClient`、`*collector.MetricsCollector`、`HealthCheckConfig`、`Clock`、`log`）
- [x] 4.4 实现 `clusterProbe.produceLoop(ctx)`：按 `rate` 用 `time.Ticker` 发送，`msg.Body=[]byte(strconv.FormatInt(now.UnixMilli(),10))`；`SendSync` 成功 -> `coll.AddHealthProduce(cluster,"success")` + 更新 `produceLatency`/`lastProduceSuccess` + `coll.SetHealthLatency/LastSuccess`；失败 -> `coll.AddHealthProduce(cluster,"failure")` + `SetHealthLastError` + `slog.Warn`
- [x] 4.5 实现 consumer handler：收到消息 -> `consumeLatency = now - parseBodyTs(msg.Body)` -> `coll.AddHealthConsume(cluster)` + 更新 `lastConsume` + `coll.SetHealthLatency(cluster,"consume",...)` + `coll.SetHealthLastSuccess(cluster,"consume",...)` -> 返回 `ConsumeSuccess`
- [x] 4.6 实现 `Prober.evalTick(ctx)`（1s）：遍历 `clusterProbe`，`status{produce}=now-lastProduceSuccess<recency`、`status{consume}=now-lastConsume<recency`、`overall=两者`，调 `coll.SetHealthStatus`；同时推导 `overall` 的 `last_success`
- [x] 4.7 新建 `internal/health/probe_test.go`：注入 fake Clock + stub Producer/Consumer，table-driven 测试：(a) produce 成功/失败分别计数+last_success 更新；(b) consume 延迟计算正确；(c) recency 内 status=1、超 recency 翻 0；(d) 启动期 last_success=0 -> status=0；(e) produce 失败后恢复 -> status 复 1
- [x] 4.8 `go test ./internal/health/ -run Probe -v` 通过

## 5. 多集群发现与刷新

- [x] 5.1 实现 `Prober.discoverClusters(ctx)`：调 `Admin.ExamineBrokerClusterInfo()`，返回 `ClusterAddrTable` 的集群名列表；失败返回 err
- [x] 5.2 实现 `Prober.reconcileClusters(ctx, discovered)`：diff 现有 map vs discovered；新增集群 -> 构造 `clusterProbe`（topic=`TopicPrefix+cluster`、group=`GroupPrefix+cluster`、subscribe+Start consumer、起 `produceLoop` goroutine）；消失集群 -> cancel 其 produceLoop ctx + `consumer.Shutdown` + `coll.ClearHealthCluster(cluster)` + 从 map 删
- [x] 5.3 实现 `Prober.Start(ctx)`：启动共享 Producer；首次 `discoverClusters`+`reconcileClusters`（失败 `slog.Warn` 不退出）；起 1s `evalTick` goroutine；起 `ClusterRefresh` ticker goroutine 周期 reconcile
- [x] 5.4 实现 `Prober.Shutdown(ctx)`：cancel 根 ctx 停所有 goroutine；遍历关各 consumer；关共享 Producer
- [x] 5.5 在 `probe_test.go` 加用例：stub Admin 第一次返回 `{A,B}`、第二次返回 `{A,C}`，断言 B 被 `ClearHealthCluster`+consumer.Shutdown、C 起了新 probe；discover 失败时不 panic 且不改动现有 probe
- [x] 5.6 `go test ./internal/health/ -run Reconcile -v` 通过

## 6. rocketmq-client-go 适配器

- [x] 6.1 新建 `internal/health/adapter.go`：实现 `rocketmqProducer`（`producer.NewDefaultProducer` + `WithNameServer` + `WithRetry(2)` + `WithInstanceName("rmq-exporter-health")` + ACL 时 `WithCredentials`）与 `rocketmqConsumer`（`consumer.NewPushConsumer` + 同名服务器/凭据 + `WithGroupName` per-cluster），适配 4.1 的接口
- [x] 6.2 确认 `Subscribe` handler 签名与 `probe_live_test.go` 一致；`Start`/`Shutdown` 转发；body 时间戳用 `UnixMilli`
- [x] 6.3 `go build ./internal/health/` 通过

## 7. /healthz HTTP handler

- [x] 7.1 新建 `internal/health/http.go`：`func HealthzHandler(coll *collector.MetricsCollector) http.Handler`，读 `coll.HealthDetail()`，所有集群 `overall=1` -> 200，任一 `=0` 或无 probe -> 503；body 为 JSON（`overall` + `clusters` map + `last_probe_at`）
- [x] 7.2 新建 `internal/health/http_test.go`：stub `HealthDetail` 返回全健康/部分不健康/空，断言 200/503/503 与 JSON 形状
- [x] 7.3 `go test ./internal/health/ -run Healthz -v` 通过

## 8. main.go 装配

- [x] 8.1 在 `cmd/rmq-exporter/main.go`：当 `cfg.HealthCheck.Enabled` 时构造 `health.NewProber(admin, coll, cfg.HealthCheck)` + `Start(ctx)` + `defer Shutdown`
- [x] 8.2 注册 `mux.Handle(cfg.HealthCheck.Path, health.HealthzHandler(coll))`（仅 enabled 时；disabled 时 `/healthz` 自然 404）
- [x] 8.3 `go build ./cmd/rmq-exporter/` 通过；手动 `./rmq-exporter --health-check-enabled=false` 确认 `/healthz` 404、`/metrics` 含保真指标且无 `rocketmq_health_check_*` 样本（空 family 的 # HELP/# TYPE 声明仍存在，符合 Java simpleclient 始终声明 family 的语义与 golden_test）

## 9. Live 测试

- [x] 9.1 新建 `internal/health/probe_live_test.go`（`RMQ_LIVE_TESTS=1` 守卫，复用 `internal/service` 的 `liveNamesrv`/`probeCredentials`）：预建 `HealthCheckTopic-<cluster>` 后跑 `Prober.Start`，断言 `coll.Gather()` 含 `rocketmq_health_check_status{cluster=...,check="overall"} 1`、`HealthzHandler` 返回 200、`consume_total` 在 ~5s 内递增、`latency_seconds{check="consume"}` 为正且合理
- [x] 9.2 ACL 场景：`RMQ_LIVE_TESTS=1 RMQ_ENABLE_ACL=1 RMQ_ACCESS_KEY=rocketmq2 RMQ_SECRET_KEY=<admin secret> go test ./internal/health/ -run TestLiveHealthProbe -v` 通过（验证签名后 produce/consume）
- [x] 9.3 故障注入：live 测试中临时停 consumer 或发到不存在的 topic，断言 `status` 翻 0、恢复后翻 1（自愈）

## 10. 文档与验收

- [x] 10.1 确认 PR 描述显式列出 5 个新增 `rocketmq_health_check_*` 指标（名/类型/标签）及"Go-only 新增、非 Java 对齐"说明（黄金法则）
- [x] 10.2 扩展 `internal/collector/golden_test.go`：确认健康 family 出现在 `/metrics` 末尾，且现有对齐指标 name/type/labels/order 不变
- [x] 10.3 `go test ./...` 全绿（非 live 测试默认跑）
- [x] 10.4 `openspec validate cluster-health-check` 通过
