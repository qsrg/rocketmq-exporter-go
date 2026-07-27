# Remoting RequestCode — confirmed against rocketmq-4.9.8

Source of truth: `common/src/main/java/org/apache/rocketmq/common/protocol/RequestCode.java`
in `/Users/wcf/java-project/rocketmq-4.9.8`, cross-checked with the call sites in
`client/src/main/java/org/apache/rocketmq/client/impl/MQClientAPIImpl.java`.

This is the output of task 5.1: every admin read RPC `MetricsCollectTask` (+ `ClientMetricTaskRunnable`)
uses, with its RocketMQ 4.x `RequestCode` integer and remoting target (namesrv vs broker).

| Go method (planned)              | Java method                     | RequestCode constant                | int | target  |
|----------------------------------|----------------------------------|-------------------------------------|----:|---------|
| ExamineBrokerClusterInfo         | examineBrokerClusterInfo         | GET_BROKER_CLUSTER_INFO             | 106 | namesrv |
| FetchAllTopicList               | fetchAllTopicList                | GET_ALL_TOPIC_LIST_FROM_NAMESERVER  | 206 | namesrv |
| ExamineTopicStats               | examineTopicStats                | GET_TOPIC_STATS_INFO                | 202 | broker  |
| ExamineTopicRouteInfo            | examineTopicRouteInfo            | GET_ROUTEINFO_BY_TOPIC              | 105 | namesrv |
| QueryTopicConsumeByWho          | queryTopicConsumeByWho           | QUERY_TOPIC_CONSUME_BY_WHO          | 300 | broker  |
| ExamineConsumerConnectionInfo    | examineConsumerConnectionInfo    | GET_CONSUMER_LIST_BY_GROUP          |  38 | broker  |
| ExamineConsumeStats             | examineConsumeStats              | GET_CONSUME_STATS                   | 208 | broker  |
| ViewBrokerStatsData            | viewBrokerStatsData              | VIEW_BROKER_STATS_DATA              | 315 | broker  |
| FetchBrokerRuntimeStats         | fetchBrokerRuntimeStats          | GET_BROKER_RUNTIME_INFO             |  28 | broker  |
| GetAllProducerInfo              | getAllProducerInfo               | GET_ALL_PRODUCER_INFO                | 328 | broker  |
| GetConsumerRunningInfo          | getConsumerRunningInfo           | GET_CONSUMER_RUNNING_INFO           | 307 | broker  |
| QueryMsgByOffset (pull)         | DefaultMQPullConsumer.pull       | PULL_MESSAGE                         |  11 | broker  |

## Corrections vs. design D2 (pre-spike guesses)

The pre-spike design table guessed several codes; the source confirmed them wrong:

| RPC                       | guessed | confirmed |
|---------------------------|--------:|----------:|
| PULL_MESSAGE              | 10      | **11**    |
| GET_BROKER_RUNTIME_INFO   | 322     | **28**    |
| GET_TOPIC_STATS_INFO      | 15      | **202**   |
| GET_CONSUME_STATS         | 30013   | **208**   |
| GET_CONSUMER_RUNNING_INFO | 321     | **307**   |
| VIEW_BROKER_STATS_DATA   | 20012   | **315**   |

design.md D2 has been updated to the confirmed values.

## Notes

- `GET_ALL_TOPIC_LIST_FROM_NAMESERVER` (206) is the constant Java exposes as
  `fetchAllTopicList`; do not confuse with `GET_ALL_TOPIC_CONFIG` (21), which is
  the per-broker `examineTopicConfig` path (used by MQAdminExtImpl's custom
  remoting, NOT by the collection tasks — not needed in Phase 1).
- `examineConsumerConnectionInfo` reuses `GET_CONSUMER_LIST_BY_GROUP` (38) and
  returns a `ConsumerConnection` (connection set + messageModel).
- ACL is deferred (see proposal Non-goals / defer-acl-phase1): no RPCHook is
  attached in Phase 1, so the codes above are sent unsigned.
