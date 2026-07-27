
### Requirement: 单个 Prometheus 指标端点
系统 SHALL 暴露恰好一个 HTTP `GET` 端点，位于配置的 telemetry 路径（默认 `/metrics`），输出 Prometheus text exposition 格式。一期 SHALL 不提供其他 HTTP 路由。

#### Scenario: metrics scrape 返回 200 与 Prometheus 文本
- **WHEN** client 发送 `GET /metrics`
- **THEN** 响应状态码为 `200`，`Content-Type` 为 `text/plain; version=0.0.4; charset=utf-8`，body 为合法 Prometheus exposition。

#### Scenario: 自定义 telemetry 路径生效
- **WHEN** `telemetry-path` 配为 `/custom-metrics`
- **THEN** `GET /custom-metrics` 返回 200，`GET /metrics` 返回 404。

### Requirement: 对 Java exporter 的指标保真
系统 SHALL 输出 `collector/RMQMetricsCollector.java` 产生的每个 Prometheus 指标，指标名、类型（全部为 gauge）、HELP 文本、标签名及顺序与 Java 版逐字一致。系统 SHALL NOT 在未于 change 中记录的情况下，相对 Java 版新增、改名、重排标签或删除任何指标。

#### Scenario: group_diff 标签与顺序
- **WHEN** consumer-diff cache 含一条记录
- **THEN** 输出样本为 `rocketmq_group_diff{group="<g>",topic="<t>",countOfOnlineConsumers="<n>",msgModel="<m>"} <v>`，标签顺序精确如此。

#### Scenario: broker runtime 标签与顺序
- **WHEN** 输出一个 broker runtime gauge
- **THEN** 标签顺序为 `cluster,brokerIP,brokerHost,des,boottime,broker_version`。

#### Scenario: 指标不被静默丢弃
- **WHEN** 某 topic/broker 的采集 RPC 失败
- **THEN** 该 scrape 仍输出其余全部指标，失败仅记录日志，不作为错误返回。

### Requirement: TTL 缓存 gauge 存储
系统 SHALL 把采集到的值存于内存 cache，自最后一次写入后 `cache-ttl` 秒淘汰（替代 Guava `expireAfterWrite`），使 `/metrics` scrape 反映最近一次成功采集而非每次 scrape 重新查询集群。

#### Scenario: 值在 TTL 后过期
- **WHEN** 某指标值在 T 时写入且无更新
- **THEN** 在 T + cache-ttl + ε 时该值不再出现于 `/metrics`。

### Requirement: OTLP gRPC 延后
系统 SHALL NOT 在一期实现 OTLP gRPC 服务端。`internal/otlp/` 目录 SHALL 存在但为空/预留。

#### Scenario: 无 grpc 监听
- **WHEN** exporter 启动
- **THEN** 不开放任何 gRPC 端口（含 5559）。

### Requirement: Apache 2.0 源文件头
每个 Go 源文件 SHALL 带 Apache License 2.0 头注释。

#### Scenario: 头注释存在
- **WHEN** 在 `cmd/` 或 `internal/` 下新增 `.go` 文件
- **THEN** 其首行含 ASF license 头。
