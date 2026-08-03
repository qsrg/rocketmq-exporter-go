# RocketMQ Exporter (Go)

用 Go 重写的 [Apache RocketMQ Exporter](https://github.com/apache/rocketmq-exporter)（对齐 Java 版 `v0.0.3-SNAPSHOT`，目标 RocketMQ **4.x**）。提供 Prometheus `/metrics` 端点 + 6 个 cron 采集任务，并新增主动端到端集群健康探针（`/healthz`）。单个静态二进制，无 JVM。

## 功能

- **Prometheus 指标保真**：指标名/类型/标签及顺序与 Java 版逐字一致（Grafana dashboard 10477、`example.rules` 可直接复用）。
- **6 个 cron 采集任务**：topic offset、producer、consumer offset（含 storetime 延迟）、broker stats topic、broker stats、broker runtime stats。
- **ACL 支持**：broker 开启 ACL 时，所有出站 RPC（含健康探针的 producer/consumer）自动 HMAC-SHA1 签名。
- **集群健康探针**（Go-only，无 Java 对应）：常驻 producer/consumer 按 recency 推导 per-cluster 生产/消费链路健康，暴露 `rocketmq_health_check_*` 指标与 `/healthz` 端点。
- 配置支持 CLI flag / 环境变量 / YAML 文件，优先级 flag > env > file > 默认。

## 前置条件

- **Go 1.26+**（`go.mod` 要求 `go 1.26.3`；较新的 Go 工具链会按 `GOTOOLCHAIN=auto` 自动下载对应版本）。
- **RocketMQ 4.x 集群**，namesrv 可达。

## 快速开始

```bash
# 1. 拉取（默认目录名与仓库名一致：rocketmq-exporter-go）
git clone https://github.com/qsrg/rocketmq-exporter-go.git
cd rocketmq-exporter-go

# 2. 编译
go build -o rmq-exporter ./cmd/rmq-exporter

# 3. 准备配置文件（仓库自带示例，字段全可选）
cp config.example.yaml config.yaml
#   按需编辑 config.yaml，至少确认 namesrv 指向你的集群

# 4. 指定配置文件运行
./rmq-exporter --config config.yaml
```

配置文件路径也可用环境变量指定：`RMQ_CONFIG=./config.yaml ./rmq-exporter`。

启动后：
- `GET http://localhost:5557/metrics` — Prometheus 指标
- `GET http://localhost:5557/healthz` — 集群健康 JSON（默认开启）

## 配置

**优先级**：CLI flag > 环境变量（`RMQ_` 前缀）> 配置文件 > 内置默认。配置文件中未写的字段走默认值。

配置文件为 YAML（snake_case），完整字段见 [`config.example.yaml`](config.example.yaml)。常用项：

```yaml
namesrv: "127.0.0.1:9876"     # RocketMQ namesrv 地址（多个用逗号分隔）
listen: ":5557"               # HTTP 监听地址
telemetry_path: "/metrics"
enable_collect: true          # 是否启用采集任务
enable_acl: false             # broker 开了 ACL 就改 true 并填下面两项
access_key: ""
secret_key: ""
cache_ttl: "60s"              # 指标缓存 TTL（Go duration 格式）
pool: { core: 10, max: 10, queue: 5000 }   # worker pool
cron:
  collect_topic_offset: "15 0/1 * * * ?"   # 6 字段 cron，? 会自动转 *
  # collect_producer / collect_consumer_offset / collect_broker_stats_topic
  # collect_broker_stats / collect_broker_runtime_stats 同格式
health_check:
  enabled: true
  topic_prefix: "HealthCheckTopic-"
  group_prefix: "HealthCheckGroup-"
  rate: 2.0                   # 每集群每秒产消息数
  recency: "5s"               # 最近成功在窗口内视为健康
  cluster_refresh: "5m"       # 集群发现刷新间隔
  path: "/healthz"
```

也可以不用配置文件，纯 flag 或环境变量运行：

```bash
# flag
./rmq-exporter --namesrv=10.0.0.1:9876 --listen=:5557

# 环境变量
RMQ_NAMESRV_ADDR=10.0.0.1:9876 ./rmq-exporter
```

所有 flag 均有对应的 `RMQ_` 前缀环境变量（如 `--namesrv` ↔ `RMQ_NAMESRV_ADDR`、`--enable-acl` ↔ `RMQ_ENABLE_ACL`），完整清单见 `config.example.yaml` 注释。

### ACL 场景

broker 开启 ACL 时，在配置文件中：

```yaml
enable_acl: true
access_key: "你的 AccessKey"
secret_key: "你的 SecretKey"
```

凭据需对采集涉及的 topic/group 有读权限；健康探针的 `HealthCheckTopic-<集群>` 需 PUB+SUB 权限。启用后所有出站 RPC 自动签名，非 ACL 集群不受影响（`enable_acl: false` 时为 no-op）。

### 健康探针注意事项

默认 `health_check.enabled: true`，会向 `HealthCheckTopic-<集群名>` 持续产/消消息：

- broker `autoCreateTopicEnable=true`（开发默认）：topic 自动创建，零配置。
- broker `autoCreateTopicEnable=false`（生产常见）：需手动预建 `HealthCheckTopic-<集群名>` 并授 PUB+SUB 权限；或设 `health_check.enabled: false` 关闭。

## HTTP 端点

| 端点 | 方法 | 返回 |
|---|---|---|
| `/metrics` | GET | Prometheus text exposition（89 个 Java 对齐指标族 + 5 个 `rocketmq_health_check_*`） |
| `/healthz` | GET | 集群健康 JSON：所有集群 `overall=ok` 返回 **200**；任一不健康或未完成首次探测返回 **503**；`health_check.enabled=false` 时返回 **404** |

`/healthz` 与 `/metrics` 读同一份缓存，不触发额外探测。

## 与 Java 版对齐

- Prometheus 指标名/类型/标签/顺序逐字一致（黄金法则）。
- 已记录的偏差：大数 exponent 格式（Java `E8` / Go `e+08`，Prometheus 等价）、`# HELP`/`# TYPE` 元数据行（Go 按文本格式标准输出，是 Java 的超集）。
- 健康探针为 Go-only 新增（5 个 `rocketmq_health_check_*` 指标），非 Java 对齐。

## 测试

```bash
go test ./...                          # 单测
RMQ_LIVE_TESTS=1 go test ./...         # 含 live 测试（需可达的 RocketMQ 集群）
```

## 许可证

Apache License 2.0。
