
# collection-tasks Specification

## Purpose
六个 cron 采集任务镜像 Java `MetricsCollectTask` 的 `@Scheduled` 方法，按调度采集 RocketMQ 集群指标；按 topic/broker 失败隔离的尽力而为语义，通过有界"丢弃最旧"worker pool 采集 client 运行时指标，支持优雅退出。

## Requirements

### Requirement: 六个 cron 采集任务
系统 SHALL 注册六个计划采集任务，镜像 `MetricsCollectTask.java` 的 `@Scheduled` 方法：`collectTopicOffset`、`collectProducer`、`collectConsumerOffset`、`collectBrokerStatsTopic`、`collectBrokerStats`（含 `collectBrokerGroupStats`，共享 `collectBrokerStats` cron）、`collectBrokerRuntimeStats`。调度器 SHALL 使用 `robfig/cron/v3` + `WithSeconds()`，并在加载时将 6 字段表达式中的 `?` 转 `*`。每个任务的默认 cron SHALL 为 Java 的 `15 0/1 * * * ?` → `15 0/1 * * * *` 语义（每分钟第 15 秒）。

#### Scenario: 注册六个任务
- **WHEN** exporter 启用采集启动
- **THEN** 调度器恰好注册六个 cron 条目。

#### Scenario: 问号转星号
- **WHEN** 配置的 cron 表达式含 `?`
- **THEN** 调度器收到将 `?` 替换为 `*` 的等价表达式并成功调度。

#### Scenario: 禁用采集时跳过所有任务
- **WHEN** `enable-collect` 为 false
- **THEN** 无采集任务改动指标存储（每个任务检查 flag 后立即返回）。

### Requirement: 按 topic/broker 失败隔离的尽力而为语义
单个 topic 或 broker 的失败（RPC 错误、解析错误）SHALL 仅记录日志且 SHALL NOT 中断当前采集轮次。系统 SHALL 移植 Java 的 `handleTopicNotExistException` 静默降级逻辑：对 `ResponseCode.TOPIC_NOT_EXIST` 与 `CONSUMER_NOT_ONLINE`，错误被静默吞掉（不发 error 日志）；其余 code 记 error 级日志。

#### Scenario: topic 不存在时静默
- **WHEN** `examineConsumeStats` 返回 `TOPIC_NOT_EXIST`
- **THEN** 该 topic/group 不发 error 日志，采集继续到下一 group。

#### Scenario: broker stats 返回 system error 时静默
- **WHEN** `viewBrokerStatsData` 返回 `SYSTEM_ERROR`
- **THEN** 该 stats 项被静默跳过，其余项/broker 仍被采集。

#### Scenario: 无关 RPC 失败记录日志并继续
- **WHEN** 某 topic 的 RPC 抛出非静默异常
- **THEN** 异常记 error 日志，循环继续到下一 topic。

### Requirement: client 指标的有界"丢弃最旧"worker pool
系统 SHALL 通过有界 worker pool 采集每个 consumer client 的 runtime 指标（按连接调用 `getConsumerRunningInfo`），pool 大小 `pool-max`（默认 10），队列容量 `pool-queue`（默认 5000）。pool 饱和时投递 SHALL 丢弃队列中最旧项（镜像 `ThreadPoolExecutor.DiscardOldestPolicy`），而非最新项，且 SHALL NOT 阻塞调用方。

#### Scenario: 饱和时非阻塞投递
- **WHEN** 队列已满且新 client-metric 任务被投递
- **THEN** submit 立即返回不阻塞，队列最旧任务被丢弃。

#### Scenario: 每轮每 group 一个 client 任务
- **WHEN** 某 consumer group 有在线 consumer
- **THEN** 每轮 `collectConsumerOffset` 至多提交一次该 group 的 client-metric 任务（由每轮去重集合守护）。

### Requirement: 优雅退出
系统 SHALL 在 `SIGTERM`/`SIGINT` 时取消所有进行中的采集与 RPC 工作，排空 worker pool，关闭 remoting client，且不在响应中途丢弃进行中的 scrape。

#### Scenario: SIGTERM 停调度器并排空 pool
- **WHEN** 进程收到 `SIGTERM`
- **THEN** cron 调度器停止接受新 tick，队列中的 client-metric 任务被排空或中止，进程在配置的 drain 超时内退出。
