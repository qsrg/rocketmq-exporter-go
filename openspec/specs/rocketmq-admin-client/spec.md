
# rocketmq-admin-client Specification

## Purpose
基于 vendor `rocketmq-client-go/v2` remoting wire layer 实现 admin 读 RPC，覆盖采集所需的全部读方法；保留 ResponseCode 错误、ACL 签名（HmacSHA1）、admin 客户端生命周期；仅移植读 RPC，不移植写/配置类方法。

## Requirements

### Requirement: 基于 vendor 的 remoting wire layer 实现读 RPC
系统 SHALL 通过 vendor `rocketmq-client-go/v2` 的 `internal/remote` wire layer（重写 import path）至本模块，在 RocketMQ 4.x remoting 协议上实现 `MetricsCollectTask` 与 `ClientMetricTaskRunnable` 依赖的 admin 读 RPC。已实现 RPC SHALL 覆盖：`examineBrokerClusterInfo`、`fetchAllTopicList`、`examineTopicStats`、`examineTopicRouteInfo`、`queryTopicConsumeByWho`、`examineConsumerConnectionInfo`、`examineConsumeStats`、`viewBrokerStatsData`、`fetchBrokerRuntimeStats`、`getAllProducerInfo`、`getConsumerRunningInfo`、`queryMsgByOffset`（PULL_MESSAGE）。

#### Scenario: namesrv RPC 返回解码后的 cluster info
- **WHEN** 对运行中的 namesrv 调用 `examineBrokerClusterInfo`
- **THEN** 返回的 `ClusterInfo` 解码出的 `clusterAddrTable`/`brokerAddrTable` 与 Java `DefaultMQAdminExt` 一致。

#### Scenario: broker RPC 定址 master 地址
- **WHEN** 对某 broker name 调用 per-broker RPC
- **THEN** 请求发往该 broker 的 master 地址（`MASTER_ID`）。

### Requirement: 保留 ResponseCode 的错误
系统 SHALL 将 broker/namesrv 错误响应作为携带 RocketMQ `ResponseCode` 整数的 Go error 暴露，供调用方做静默降级。`TOPIC_NOT_EXIST`、`CONSUMER_NOT_ONLINE`、`SYSTEM_ERROR`、`SUCCESS` SHALL 以命名常量表示。

#### Scenario: error 携带 response code
- **WHEN** broker 返回非 `SUCCESS` 响应
- **THEN** 返回的 error 暴露 `ResponseCode` 整数与 remark 字符串。

### Requirement: ACL 签名（HmacSHA1）
系统 SHALL 解析 `enable-acl` 开关与 `access-key`/`secret-key` 凭据。当 `enable-acl=true` 时，系统 SHALL 在每个出站 `RemotingCommand` 编码前附加 ACL 签名（移植自 `rocketmq-acl` / `rocketmq-client-go` 的 `ACLInterceptor`）：将 `AccessKey`（+可选 `SecurityToken`）与全部 `ExtFields` 按 key 字典序拼接其 value，追加请求 body，HMAC-SHA1 + base64 写入 `ExtFields["Signature"]`，并写入 `AccessKey`。当 `enable-acl=false` 时 SHALL NOT 签名（非 ACL 集群行为不变）。canonical string 必须与 broker 逐字一致（单测以已知向量 `tAb/54Rwwcq+pbH8Loi7FWX4QSQ=` 验证字节级一致）。

#### Scenario: ACL 关闭时不签名
- **WHEN** `enable-acl=false`
- **THEN** 不附加 ACL hook，remoting 行为同普通 client，非 ACL 集群正常工作。

#### Scenario: ACL 开启时签名每个出站请求
- **WHEN** `enable-acl=true`
- **THEN** 每个出站 RPC（namesrv / broker / PULL_MESSAGE）在 `InvokeSync` 前附加 `Signature` + `AccessKey`；已签名请求被开启 ACL 的 broker 接受。

### Requirement: Admin 客户端生命周期
系统 SHALL 通过手动构造注入（无 DI 框架）构造 admin client，启动时（延迟）打开 remoting 连接，context 取消时关闭。admin client SHALL 可被所有 cron 任务与 worker pool 并发安全使用。

#### Scenario: 任务并发访问
- **WHEN** 多个 cron 任务并发调用 RPC
- **THEN** admin client 并发安全（无 data race、无响应串包）；底层 remoting 传输可为 per-call dial 或连接池化，二者皆满足并发安全（一期采用 per-call dial：每次 `InvokeSync` 独立连接，天然隔离，无共享连接状态）。

### Requirement: 删除写/配置类 admin RPC
系统 SHALL NOT 在一期移植 `MQAdminExtImpl.java` 的写或配置类 admin 方法（create/update topic、delete subscription、reset offset 等）——仅移植采集所需的读 RPC。

#### Scenario: 写 RPC 不存在
- **WHEN** 检视 Go admin client 的方法面
- **THEN** 不存在 `createAndUpdateTopicConfig`、`deleteTopicInBroker`、`resetOffset*` 等方法。
