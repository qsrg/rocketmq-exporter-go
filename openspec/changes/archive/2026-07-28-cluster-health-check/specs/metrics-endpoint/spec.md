## MODIFIED Requirements

### Requirement: 单个 Prometheus 指标端点
系统 SHALL 暴露 HTTP `GET` 端点位于配置的 telemetry 路径（默认 `/metrics`），输出 Prometheus text exposition 格式。当 `health-check-enabled=true` 时，系统 SHALL 额外暴露健康检查端点（默认 `/healthz`，详见 cluster-health-check 能力）；`health-check-enabled=false` 时 SHALL NOT 提供除 telemetry 路径外的 HTTP 路由。

#### Scenario: metrics scrape 返回 200 与 Prometheus 文本
- **WHEN** client 发送 `GET /metrics`
- **THEN** 响应状态码为 `200`，`Content-Type` 为 `text/plain; version=0.0.4; charset=utf-8`，body 为合法 Prometheus exposition。

#### Scenario: 自定义 telemetry 路径生效
- **WHEN** `telemetry-path` 配为 `/custom-metrics`
- **THEN** `GET /custom-metrics` 返回 200，`GET /metrics` 返回 404。

#### Scenario: health-check-enabled 时注册健康检查端点
- **WHEN** `health-check-enabled=true`
- **THEN** `GET /healthz`（或配置的 `health-check-path`）返回响应；`health-check-enabled=false` 时该路径返回 404。

### Requirement: 对 Java exporter 的指标保真
系统 SHALL 输出 `collector/RMQMetricsCollector.java` 产生的每个 Prometheus 指标，指标名、类型（Java 保真指标全部为 gauge）、HELP 文本、标签名及顺序与 Java 版逐字一致。系统 SHALL NOT 在未于 change 中记录的情况下，相对 Java 版新增、改名、重排标签或删除任何 Java 保真指标。健康检查指标（`rocketmq_health_check_*`，详见 cluster-health-check 能力）为 Go 版新增、非 Java 对齐指标，已在本 change 中记录，其类型（含 counter）与标签由该能力定义。

#### Scenario: group_diff 标签与顺序
- **WHEN** consumer-diff cache 含一条记录
- **THEN** 输出样本为 `rocketmq_group_diff{group="<g>",topic="<t>",countOfOnlineConsumers="<n>",msgModel="<m>"} <v>`，标签顺序精确如此。

#### Scenario: broker runtime 标签与顺序
- **WHEN** 输出一个 broker runtime gauge
- **THEN** 标签顺序为 `cluster,brokerIP,brokerHost,des,boottime,broker_version`。

#### Scenario: 指标不被静默丢弃
- **WHEN** 某 topic/broker 的采集 RPC 失败
- **THEN** 该 scrape 仍输出其余全部指标，失败仅记录日志，不作为错误返回。

#### Scenario: 健康检查指标不破坏保真指标
- **WHEN** `/metrics` 输出含健康检查 family
- **THEN** Java 保真指标的名称/类型/标签/顺序不变，健康检查 family 以 `rocketmq_health_check_` 前缀隔离。

## ADDED Requirements

### Requirement: 健康检查 family 合并与空 family 类型编码
`/metrics` 输出 SHALL 在 Java 保真 family 之后追加 cluster-health-check 能力产生的健康检查 family。对于无样本的空 family，系统 SHALL 按 `MetricFamily.GetType()` 输出 `# TYPE` 行（如 counter family 输出 `# TYPE ... counter`），SHALL NOT 硬编码为 `gauge`。

#### Scenario: 健康检查 family 出现于 /metrics
- **WHEN** 健康检查已产生样本
- **THEN** `/metrics` body 含 `rocketmq_health_check_*` family，位于 Java 保真 family 之后。

#### Scenario: 空 counter family 的 TYPE 行
- **WHEN** 某 counter family（如 `rocketmq_health_check_produce_total`）尚无样本
- **THEN** 其 `# TYPE` 行为 `# TYPE rocketmq_health_check_produce_total counter`，不为 `gauge`。
