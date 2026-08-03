# RocketMQ Exporter (Go 重写)
使用中文回答

## 目标
用 Go 重写 Apache RocketMQ Exporter（Java 源码 v0.0.3-SNAPSHOT）。
- Java 源码（行为对齐的**唯一事实来源**）：`/Users/wcf/java-project/rocketmq-exporter`
- Go 项目根：`/Users/wcf/go-project/rmq-exporter`
- 模块路径：`github.com/qsrg/rocketmq-exporter-go`（仓库 https://github.com/qsrg/rocketmq-exporter-go）
- 目标 RocketMQ：**4.x**，对齐 Java 4.9.8 协议
- RocketMQ 4.x 源码位置：/Users/wcf/java-project/rocketmq-4.9.8

## 范围分期
- **一期**：Prometheus `/metrics` 端点 + 6 个 cron 采集任务 + admin 客户端封装。行为对齐 Java 版。
- **二期（不在一期范围）**：OTLP gRPC 导出（Java 版端口 5559，供 SkyWalking 等）。一期不实现 gRPC 服务端，目录 `internal/otlp/` 预留。

## 黄金法则：指标保真
Prometheus 指标名、类型（counter/gauge）、标签名及顺序必须与 Java 版**逐字一致**
（Grafana dashboard 10477、`example.rules` 依赖）。HELP 文本保持一致。
任何新增/改名/删除指标必须在 PR 中显式记录理由。不静默丢弃指标。
> 注：指标名保真 ≠ 配置保真。采集的端口/路径/配置格式允许现代化（见下）。

## 技术栈映射（Java -> Go）
| Java                              | Go                                                              |
|-----------------------------------|-----------------------------------------------------------------|
| Spring Boot Web (Jetty)           | `net/http`（单端点，无需框架）                                    |
| io.prometheus simpleclient 0.15.0 | `github.com/prometheus/client_golang`                           |
| rocketmq-tools DefaultMQAdminExt  | `github.com/apache/rocketmq-client-go/v2/admin`（缺口用原生 Remoting 协议补，见下方风险） |
| @Scheduled cron（6 字段，含 `?`）  | `github.com/robfig/cron/v3` + `WithSeconds()`；加载时 `?` → `*`  |
| Spring DI (@Autowired/@Service)   | 手动构造注入（不引入 DI 框架）                                    |
| logback + slf4j                   | `log/slog`（结构化日志）                                         |
| application.yml                   | 标准库 `flag` + 环境变量（可选 `spf13/viper` 读文件）             |
| fastjson / jackson                | `encoding/json`                                                 |
| Guava (Throwables 等)             | 标准库                                                          |
| joor 运行时反射                   | 删除（仅 `viewMessage` hack 用到，非指标路径）                    |
| ThreadPoolExecutor + DiscardOldestPolicy | 有界 worker pool：buffered channel(5000) + 非阻塞投递/丢弃最旧 |
| OTLP gRPC (grpc-netty)            | 二期，一期不做                                                   |

## Java -> Go 翻译约定
- 检查型异常 → `error` 返回值；**不用 panic 做控制流**。
- 多 catch 分支 → `errors.Is` / 哨兵 error / `ResponseCode` 整数比较（RocketMQ 错误码保留）。
- 类与继承 → 结构体 + 组合嵌入；接口隐式实现。
- getter/setter → 导出字段或显式方法。
- Optional → 指针 / `v, ok` 模式。
- 枚举（`MessageModel` 等）→ typed 常量。
- `Map.Entry` 遍历 → `for k, v := range`。
- `StringUtils.isBlank` → 自实现 helper；`String.format` → `fmt.Sprintf`。
- null 语义：注意 Java map 可含 null value，Go 用“缺 key”或 `*T` 表达，勿混淆。
- 采集的“尽力而为”语义保留：单 topic/单 broker 失败只 log 并 continue，不中断整轮采集；
  `ResponseCode` 为 `TOPIC_NOT_EXIST` / `CONSUMER_NOT_ONLINE` / `SYSTEM_ERROR` 时的静默降级逻辑照搬。

## 目录结构
```
cmd/rmq-exporter/   # main.go：装配 + 启动 HTTP/cron
internal/
  config/      # 配置加载（对应 RMQConfigure、ScheduleConfig、ExecutorConfig）
  collector/   # Prometheus 指标注册与采集器（对应 RMQMetricsCollector）
  task/        # 6 个 cron 采集任务 + worker pool（对应 MetricsCollectTask）
  service/     # admin 客户端封装（对应 MQAdminExtImpl/MQAdminInstance）
  model/       # BrokerRuntimeStats 等数据模型
  otlp/        # 预留（二期）
```

## 并发与生命周期
- `context.Context` 贯穿所有采集与 RPC；`SIGTERM` 优雅退出。
- client 指标采集用有界 worker pool（core=max=10, queue=5000, 丢弃最旧）。
- 跨 goroutine 共享状态必须 mutex 保护；优先 channel + 不可变数据。

## 配置（允许现代化）
用 `flag` + 环境变量，默认值可调整，无需沿用 `application.yml` 的 key 命名。保留的**语义调参项**：
- namesrv 地址、enableCollect、enableACL + accessKey/secretKey、outOfTimeSeconds 缓存 TTL
- 6 个采集任务的 cron（默认沿用 `15 0/1 * * * ?` 语义，`?`→`*`）
- worker pool 大小（core/max=10, queue=5000）
- HTTP 监听地址、telemetry 路径（默认 `:5557` `/metrics`，可改）

## 测试
- table-driven 单测优先覆盖纯函数：`Utils.getFixedDouble`、`BrokerRuntimeStats` 解析、
  cron 表达式转换、`buildClientAddresses`，并对照 Java 输出。
- 集成验收：对同一 RocketMQ 集群并行跑 Java 版与 Go 版，diff 排序后的 `/metrics`
  （指标名/标签/值容差），作为行为对齐的核心验收。

## 最大风险：RocketMQ admin 客户端覆盖度
**开工第一步做 spike**：列出 `MetricsCollectTask` 用到的全部 admin 方法，逐个核对
`rocketmq-client-go v2` 的 `admin` 包是否有等价实现。缺口（疑似 `viewBrokerStatsData`、
`getAllProducerInfo`、`queryMsgByOffset` 等）决定走向：原生 `RemotingCommand` 协议自实现，
或砍掉对应指标并显式记录。**此风险不解除，不要大规模铺代码。**

## 不要做
- 不照搬 Java 的深层类层级；不引入 Spring 等价重框架。
- 不改指标命名/类型/标签；不静默丢指标。
- 不保留 joor 反射 hack；不用 panic 控制流。
- 不重发明 cron / Prometheus / RocketMQ 客户端，优先用成熟库。

## 许可证
Apache 2.0，源文件保留 ASF 头注释。
