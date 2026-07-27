# 行为对齐验证（任务 10）

日期：2026-07-15。集群：本地 RocketMQ 4.9.8，namesrv `127.0.0.1:9876`，两个
broker（`broker-a` 127.0.0.1:10911、`broker-b` 127.0.0.1:20911）。

## 步骤

- Java exporter（v0.0.3-SNAPSHOT exec jar）跑在 `:19876/metrics`，默认 cron
  `15 0/1 * * * ?`（每分钟第 15 秒）。
- Go exporter（本模块 `cmd/rmq-exporter`）跑在 `:5557/metrics`，cron
  `*/2 * * * * *`（每 2 秒，测试期加快采样）。
- 两者指向同一集群。等 Java 至少 1 个 cron tick、Go 若干 tick 后，分别抓取
  `/metrics`，对排序后的输出做 diff。

命令：
```bash
curl -s localhost:19876/metrics > /tmp/java_metrics.txt
curl -s localhost:5557/metrics   > /tmp/go_metrics.txt
# 指标名集合：
diff <(grep -oE '^rocketmq_[a-z_]+' /tmp/java_metrics.txt | sort -u) \
     <(grep -oE '^rocketmq_[a-z_]+' /tmp/go_metrics.txt   | sort -u)
# 标签集（name+标签顺序，忽略 value；Java 尾随逗号归一化）：
diff <(grep -E '^rocketmq_' /tmp/java_metrics.txt | sed -E 's/,\}/}/g; s/\}[ ].*//' | sort -u) \
     <(grep -E '^rocketmq_' /tmp/go_metrics.txt   | sed -E 's/\}[ ].*//' | sort -u)
```

## 结果

**指标名集合：一致。** 两侧导出相同的 60 个指标名（其余 29 个 gauge 族在本集群
两侧均为空；Go 仍输出裸 `# HELP`/`# TYPE` 行，Java 的 simpleclient 构建不输出
元数据行 -- 见偏差 D2）。

**标签名 + 顺序：88/89 族一致。** 归一化 Java simpleclient 的尾随逗号
（`label="v",}` -> `label="v"}`）后，每个有数据的采样行两侧标签名与顺序完全相同。

**值：在容差内一致。** 剩余的值行差异为：
- 时序抖动（两次抓取在活集群上相隔片刻；如 `broker_qps` 5.92 vs 5.72、
  `commitlog_maxoffset` 差约 8 KB），符合 CLAUDE.md 的值容差。
- 数字格式（见下文 D1）。

**唯一的标签集差异：** `rocketmq_group_get_latency_by_storetime`
- Java 约 42 个采样，含 retry topic（`%RETRY%…`）；Go 约 5 个（有在线消费者
  的普通 topic）。见偏差 D4。

## 记录的偏差（遵循黄金法则：不静默丢指标）

- **D0 - ACL 签名。** RESOLVED（2026-07-23）：实现于
  `internal/rmqremote/acl.go`（`SignACL` = 对 canonical string 做 HMAC-SHA1，
  逐字移植自 `rocketmq-client-go` 的 `ACLInterceptor`/`calculateSignature`，
  后者经 broker 验证）。通过 `AdminClient.signIfACL` 接入每个出站 RPC
  （namesrv、broker、PULL_MESSAGE 路径）。单测以已知向量
  `tAb/54Rwwcq+pbH8Loi7FWX4QSQ=` 字节级验证（`internal/rmqremote/acl_test.go`）。
  已对 live 4.9.8 broker（ACL 开启，admin 账号 `rocketmq2`）端到端验证：未签名
  broker RPC 被拒（`AclException: No accessKey is configured`）；签名后 RPC 在
  全套 broker 接口上通过（GET_BROKER_RUNTIME_INFO、GET_TOPIC_STATS_INFO、
  GET_CONSUME_STATS、GET_ALL_PRODUCER_INFO、PULL_MESSAGE 等）。无需改白名单 --
  尽管配置了 `globalWhiteRemoteAddresses: 127.0.0.1`，broker 仍对 exporter 的
  连接强制 ACL（4.9.8 地址匹配的一个 nuance）。非 ACL 集群不受影响
  （`enable-acl=false` 时 `signIfACL` 为 no-op）。

- **D1 - 数字 exposition 格式。** Java simpleclient 用大写指数且无符号
  （`4.46510053E8`、`8.14970044416E11`）；Go `expfmt` 用小写带显式符号
  （`4.46518488e+08`、`8.14970044416e+11`）。数值相同；Prometheus 两者等价解析。
  不属于指标定义保真问题（名/类型/标签不受影响）。不照搬（Go 的形式才是文本格式
  标准）。

- **D2 - `# HELP`/`# TYPE` 元数据行。** 本 Java 构建的 simpleclient exposition
  只输出采样行（无元数据）。Go 输出 `# HELP`/`# TYPE`（文本格式标准，也是
  CLAUDE.md 要求保留的）。因此此处 Go 是 Java exposition 的超集；非回归。

- **D3 - `countOfOnlineConsumers` 确定性。** Java 的
  `examineConsumerConnectionInfo` 只查询路由中**随机一个** broker（不确定，可能
  漏掉其他 broker 上的消费者）。Go 合并路由中**所有** broker 的连接集合（确定，
  无论消费者在哪都能找到）。标签名/顺序相同；多 broker 边缘场景下值可能不同
  （Go 报真实总数）。Go 更正确；当 Java 随机命中正确 broker 时与 Java 输出一致。

- **D4 - `rocketmq_group_get_latency_by_storetime` retry-topic 覆盖。** 该
  storetime 延迟指标对每个消费队列 pull 一条消息（PULL_MESSAGE，code 11）并读其
  store timestamp。对有在线消费者的普通 topic，Go 正常填充（已验证：
  `QuickStartTopic9/ConsumerGroup9 = 1522ms`）。对 retry topic（`%RETRY%<group>`），
  broker 在消费者 offset 处对 Go 的 pull 返回 NO_NEW，因此拿不到 store timestamp、
  不记录延迟采样（Java 的 pull 能在那里取到消息，记录约 42 个采样）。指标**名、
  标签、顺序逐字一致**；仅 retry-topic 的**采样数**不同。根因是 broker 侧 retry
  队列 pull 语义（疑为 `%RETRY%` 队列 + `TOOLS_CONSUMER` pull group 的
  group/权限处理）-- 超出一期范围的更深 pull 路径排查。已记录、非静默丢弃：gauge
  族存在（含 `# HELP`/`# TYPE`）且对非 retry topic 有数据。

## 结论

一期保真契约（指标名、类型、标签名、标签顺序 -- 与 Java exporter 逐字一致）对
88/89 个指标族**完全满足**；第 89 个（`group_get_latency_by_storetime`）的
名/标签/顺序一致，仅 retry topic 上有已记录的采样数缺口。Go exporter 提供的
`/metrics` 接口可供现有 Grafana dashboard 10477 与 `example.rules` 对本集群抓取。
偏差 D0–D4 已按黄金法则记录于上。

---

## 归档后更新（2026-07-16）：D4 RESOLVED

D4 的根因**不是** broker 侧 retry 语义 -- 而是 Go 的一个逻辑 bug。Go 的
`collectConsumerOffset` 仅在 `lag > latencyMap[broker]` 时才发射
`rocketmq_group_get_latency_by_storetime` 采样，导致 NO_NEW_MSG pull（lag 0）从未
写入 map、不发射采样。Java 用 `!containsKey -> put(lagTime>0?lagTime:0)`，每个
broker 始终发射一个（可能为 0）的采样。

修复：`internal/task/collect.go` 现在在每个 broker 的首个队列上用 0 预置 map
（仅当 lag 更大时抬升），并在 pull 出错时放弃整组延迟（对齐 Java 的 try/catch）。

用自建的 Go producer + 触发 retry 的 consumer 端到端验证（测试
`internal/task/probe_latency_live_test.go::TestLiveD4RetryTopicLatency`，以及
数据播种 probe `internal/service/probe_live_test.go`）：
`latency samples: probe-group=2 (of which retry-topic=1)` -- 此前缺失的
`%RETRY%<group>` 采样现已发射。剩余标签集差异已闭合；全部 89 个指标族现已在
名/标签/顺序层面与 Java 逐字一致，延迟采样数缺口已消除。

---

## 归档后更新（2026-07-23）：D0 RESOLVED

ACL 签名已实现（`internal/rmqremote/acl.go`）：`SignACL` 对 canonical string
（AccessKey + 可选 SecurityToken + 全部 ExtFields 按 key 字典序拼 value + body）
做 HMAC-SHA1，base64 编码写入 `ExtFields["Signature"]`，并写入
`ExtFields["AccessKey"]`。这是 `rocketmq-client-go` 的
`ACLInterceptor`/`calculateSignature`（本身移植自 `rocketmq-acl`）的逐字移植，
故 canonical string 与 broker 完全一致。

接线：`AdminClient.signIfACL` 在 `invokeSyncNamesrv`、`invokeSyncBroker`、
`QueryMsgByOffset`（PULL_MESSAGE）的每个 `InvokeSync` 前调用；`enable-acl=false`
时为 no-op，故非 ACL 集群不受影响。live 测试套件（`live_test.go`、
`probe_live_test.go`、`probe_latency_live_test.go`）通过 `RMQ_ENABLE_ACL`/
`RMQ_ACCESS_KEY`/`RMQ_SECRET_KEY` 环境变量支持 ACL。

验证：单测以已知向量 `tAb/54Rwwcq+pbH8Loi7FWX4QSQ=` 字节级验证
（`acl_test.go`）。已对 live 4.9.8 broker（ACL 开启，admin 账号 `rocketmq2`）
端到端验证：未签名 broker RPC 被拒（`AclException: No accessKey is configured`），
签名后 RPC 在全套 broker 接口上成功（GET_BROKER_RUNTIME_INFO、
GET_TOPIC_STATS_INFO、GET_CONSUME_STATS、GET_ALL_PRODUCER_INFO、PULL_MESSAGE 等）。
值得注意的是无需改 `plain_acl.yml` 白名单：尽管配置了
`globalWhiteRemoteAddresses: 127.0.0.1`，4.9.8 broker 仍对 exporter 的 loopback
连接强制 ACL（地址匹配的一个 nuance），故签名直接被验证。`TestLiveACLSigning`
自动化此"未签名被拒 / 已签名通过"检查；若连接的 IP 真被白名单放行，则自 skip 并
给出指引。

剩余延后项：二期 OTLP gRPC。（D0 ACL 签名已于 2026-07-23 解决，见上。）
