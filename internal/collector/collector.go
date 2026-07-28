// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to You under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

package collector

import (
	"strconv"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/wcf/rmq-exporter/internal/model"
)

// MetricsCollector is the Go port of RMQMetricsCollector. It is both the metric
// store (caches + add* setters, fed by cron tasks) and a prometheus.Gatherer
// (Gather), so empty gauge families still emit #HELP/#TYPE exactly as the Java
// simpleclient Collector does.
type MetricsCollector struct {
	clk clock

	// topic offsets
	topicOffset     *ttlCache[producerKey, float64]
	topicRetryOffset *ttlCache[producerKey, float64]
	topicDLQOffset  *ttlCache[dlqKey, float64]

	producerCounts *ttlCache[producerCountKey, int]

	topicPutNums  *ttlCache[topicPutNumKey, topicPutNumVal]
	topicPutSize  *ttlCache[topicPutNumKey, topicPutNumVal]

	consumerDiff     *ttlCache[diffKey, int64]
	consumerRetryDiff *ttlCache[diffKey, int64]
	consumerDLQDiff   *ttlCache[diffKey, int64]
	consumerCounts   *ttlCache[string, consumerCountVal]

	consumerClientFailedMsgCounts *ttlCache[clientRuntimeKey, clientRuntimeVal]
	consumerClientFailedTPS       *ttlCache[clientRuntimeKey, clientRuntimeVal]
	consumerClientOKTPS           *ttlCache[clientRuntimeKey, clientRuntimeVal]
	consumerClientRT              *ttlCache[clientRuntimeKey, clientRuntimeVal]
	consumerClientPullRT          *ttlCache[clientRuntimeKey, clientRuntimeVal]
	consumerClientPullTPS         *ttlCache[clientRuntimeKey, clientRuntimeVal]

	groupBrokerTotalOffset *ttlCache[consumerKey, int64]
	groupConsumeTPS         *ttlCache[consumerKey, float64]
	groupGetNums            *ttlCache[consumerKey, float64]
	groupGetSize            *ttlCache[consumerKey, float64]
	sendBackNums            *ttlCache[consumerKey, float64]
	groupLatencyByTime      *ttlCache[consumerKey, int64]
	// groupConsumeTotalOffset is an empty family in Java (put commented out).

	brokerPutNums      *ttlCache[brokerKey, brokerVal]
	brokerGetNums      *ttlCache[brokerKey, brokerVal]
	brokerCommitLogDiff *ttlCache[brokerKey, brokerVal]

	// broker runtime: data-driven to keep the ~50 gauges + 13 distribute gauges
	// in sync without per-field boilerplate (and to keep label order consistent).
	runtimeMetrics    []runtimeMetric
	distributeMetrics []distributeMetric

	// health is the non-TTL store for the cluster-health-check capability
	// (Go-only addition; fed by internal/health.Prober).
	health *healthStore
}

// runtimeMetric is one broker-runtime gauge backed by its own cache, so a null
// putMessageDistributeTimeMap leaves the 13 pmdt gauges absent while the other
// ~50 are present (matching Java's separate caches).
type runtimeMetric struct {
	name   string
	help   string
	cache  *ttlCache[brokerRuntimeKey, runtimeEntry]
	getVal func(s *model.BrokerRuntimeStats) float64
}

type distributeMetric struct {
	name    string
	help    string
	distKey string
	cache   *ttlCache[brokerRuntimeKey, runtimeEntry]
}

// New constructs a MetricsCollector with the given cache TTL.
func New(ttl time.Duration) *MetricsCollector {
	return NewWithClock(ttl, realClock{})
}

// NewWithClock lets tests inject a deterministic clock.
func NewWithClock(ttl time.Duration, clk clock) *MetricsCollector {
	c := &MetricsCollector{clk: clk}
	c.topicOffset = newCache[producerKey, float64](ttl, clk)
	c.topicRetryOffset = newCache[producerKey, float64](ttl, clk)
	c.topicDLQOffset = newCache[dlqKey, float64](ttl, clk)
	c.producerCounts = newCache[producerCountKey, int](ttl, clk)
	c.topicPutNums = newCache[topicPutNumKey, topicPutNumVal](ttl, clk)
	c.topicPutSize = newCache[topicPutNumKey, topicPutNumVal](ttl, clk)
	c.consumerDiff = newCache[diffKey, int64](ttl, clk)
	c.consumerRetryDiff = newCache[diffKey, int64](ttl, clk)
	c.consumerDLQDiff = newCache[diffKey, int64](ttl, clk)
	c.consumerCounts = newCache[string, consumerCountVal](ttl, clk)
	c.consumerClientFailedMsgCounts = newCache[clientRuntimeKey, clientRuntimeVal](ttl, clk)
	c.consumerClientFailedTPS = newCache[clientRuntimeKey, clientRuntimeVal](ttl, clk)
	c.consumerClientOKTPS = newCache[clientRuntimeKey, clientRuntimeVal](ttl, clk)
	c.consumerClientRT = newCache[clientRuntimeKey, clientRuntimeVal](ttl, clk)
	c.consumerClientPullRT = newCache[clientRuntimeKey, clientRuntimeVal](ttl, clk)
	c.consumerClientPullTPS = newCache[clientRuntimeKey, clientRuntimeVal](ttl, clk)
	c.groupBrokerTotalOffset = newCache[consumerKey, int64](ttl, clk)
	c.groupConsumeTPS = newCache[consumerKey, float64](ttl, clk)
	c.groupGetNums = newCache[consumerKey, float64](ttl, clk)
	c.groupGetSize = newCache[consumerKey, float64](ttl, clk)
	c.sendBackNums = newCache[consumerKey, float64](ttl, clk)
	c.groupLatencyByTime = newCache[consumerKey, int64](ttl, clk)
	c.brokerPutNums = newCache[brokerKey, brokerVal](ttl, clk)
	c.brokerGetNums = newCache[brokerKey, brokerVal](ttl, clk)
	c.brokerCommitLogDiff = newCache[brokerKey, brokerVal](ttl, clk)

	c.runtimeMetrics = buildRuntimeMetrics(func() *ttlCache[brokerRuntimeKey, runtimeEntry] {
		return newCache[brokerRuntimeKey, runtimeEntry](ttl, clk)
	})
	c.distributeMetrics = buildDistributeMetrics(func() *ttlCache[brokerRuntimeKey, runtimeEntry] {
		return newCache[brokerRuntimeKey, runtimeEntry](ttl, clk)
	})
	c.health = newHealthStore()
	return c
}

// --- Setters: signatures mirror the Java addXxxMetric methods so the cron task
// port is a mechanical translation. ---

const (
	retryTopicPrefix = "%RETRY%" // MixAll.RETRY_GROUP_TOPIC_PREFIX
	dlqTopicPrefix   = "%DLQ%"   // MixAll.DLQ_GROUP_TOPIC_PREFIX
)

// AddTopicOffsetMetric routes RETRY/DLQ topics to their own families, matching
// Java addTopicOffsetMetric (MixAll prefix routing).
func (c *MetricsCollector) AddTopicOffsetMetric(cluster, broker, topic string, lastUpdateTimestamp int64, value float64) {
	switch {
	case strings.HasPrefix(topic, retryTopicPrefix):
		c.topicRetryOffset.put(producerKey{cluster, broker, topic}, value)
	case strings.HasPrefix(topic, dlqTopicPrefix):
		group := strings.TrimPrefix(topic, dlqTopicPrefix)
		c.topicDLQOffset.put(dlqKey{cluster, broker, group}, value)
	default:
		c.topicOffset.put(producerKey{cluster, broker, topic}, value)
	}
}

func (c *MetricsCollector) AddProducerCountMetric(cluster, broker, group string, count int) {
	c.producerCounts.put(producerCountKey{group, cluster, broker}, count)
}

func (c *MetricsCollector) AddGroupCountMetric(group, caddrs, localaddrs string, count int) {
	// Java ConsumerCountMetric key = group only; caddr/localaddr are last-writer.
	c.consumerCounts.put(group, consumerCountVal{caddrs, localaddrs, count})
}

// AddGroupDiffMetric routes RETRY/DLQ topics to their own diff families.
func (c *MetricsCollector) AddGroupDiffMetric(countOfOnlineConsumers, group, topic, msgModel string, value int64) {
	k := diffKey{group, topic, countOfOnlineConsumers, msgModel}
	switch {
	case strings.HasPrefix(topic, retryTopicPrefix):
		c.consumerRetryDiff.put(k, value)
	case strings.HasPrefix(topic, dlqTopicPrefix):
		c.consumerDLQDiff.put(k, value)
	default:
		c.consumerDiff.put(k, value)
	}
}

func (c *MetricsCollector) AddTopicPutNumsMetric(cluster, brokerName, brokerIP, topic string, value float64) {
	c.topicPutNums.put(topicPutNumKey{cluster, brokerIP, topic}, topicPutNumVal{brokerName, value})
}

func (c *MetricsCollector) AddTopicPutSizeMetric(cluster, brokerName, brokerIP, topic string, value float64) {
	c.topicPutSize.put(topicPutNumKey{cluster, brokerIP, topic}, topicPutNumVal{brokerName, value})
}

func (c *MetricsCollector) AddGroupBrokerTotalOffsetMetric(cluster, broker, topic, group string, value int64) {
	c.groupBrokerTotalOffset.put(consumerKey{cluster, broker, topic, group}, value)
}

func (c *MetricsCollector) AddGroupGetLatencyByStoreTimeMetric(cluster, broker, topic, group string, value int64) {
	c.groupLatencyByTime.put(consumerKey{cluster, broker, topic, group}, value)
}

func (c *MetricsCollector) AddGroupConsumeTPSMetric(cluster, broker, topic, group string, value float64) {
	c.groupConsumeTPS.put(consumerKey{cluster, broker, topic, group}, value)
}

func (c *MetricsCollector) AddGroupGetNumsMetric(cluster, broker, topic, group string, value float64) {
	c.groupGetNums.put(consumerKey{cluster, broker, topic, group}, value)
}

func (c *MetricsCollector) AddGroupGetSizeMetric(cluster, broker, topic, group string, value float64) {
	c.groupGetSize.put(consumerKey{cluster, broker, topic, group}, value)
}

func (c *MetricsCollector) AddSendBackNumsMetric(cluster, broker, topic, group string, value float64) {
	c.sendBackNums.put(consumerKey{cluster, broker, topic, group}, value)
}

func addClient(c *ttlCache[clientRuntimeKey, clientRuntimeVal], group, topic, clientAddr, clientId string, value float64) {
	c.put(clientRuntimeKey{group, topic, strings.ToLower(clientAddr)}, clientRuntimeVal{clientAddr, clientId, value})
}

func (c *MetricsCollector) AddConsumerClientFailedMsgCountsMetric(group, topic, clientAddr, clientId string, value int64) {
	addClient(c.consumerClientFailedMsgCounts, group, topic, clientAddr, clientId, float64(value))
}
func (c *MetricsCollector) AddConsumerClientFailedTPSMetric(group, topic, clientAddr, clientId string, value float64) {
	addClient(c.consumerClientFailedTPS, group, topic, clientAddr, clientId, value)
}
func (c *MetricsCollector) AddConsumerClientOKTPSMetric(group, topic, clientAddr, clientId string, value float64) {
	addClient(c.consumerClientOKTPS, group, topic, clientAddr, clientId, value)
}
func (c *MetricsCollector) AddConsumeRTMetricMetric(group, topic, clientAddr, clientId string, value float64) {
	addClient(c.consumerClientRT, group, topic, clientAddr, clientId, value)
}
func (c *MetricsCollector) AddPullRTMetric(group, topic, clientAddr, clientId string, value float64) {
	addClient(c.consumerClientPullRT, group, topic, clientAddr, clientId, value)
}
func (c *MetricsCollector) AddPullTPSMetric(group, topic, clientAddr, clientId string, value float64) {
	addClient(c.consumerClientPullTPS, group, topic, clientAddr, clientId, value)
}

func (c *MetricsCollector) AddBrokerPutNumsMetric(cluster, brokerIP, brokerName string, value float64) {
	c.brokerPutNums.put(brokerKey{cluster, brokerIP}, brokerVal{brokerName, value})
}
func (c *MetricsCollector) AddBrokerGetNumsMetric(cluster, brokerIP, brokerName string, value float64) {
	c.brokerGetNums.put(brokerKey{cluster, brokerIP}, brokerVal{brokerName, value})
}
func (c *MetricsCollector) AddBrokerCommitLogDiffMetric(cluster, brokerIP, brokerName string, value float64) {
	c.brokerCommitLogDiff.put(brokerKey{cluster, brokerIP}, brokerVal{brokerName, value})
}

// AddBrokerRuntimeStatsMetric ports Java addBrokerRuntimeStatsMetric. It sets all
// ~50 numeric runtime gauges; the 13 distribute-time gauges are only set when
// the parsed PutMessageDistributeTimeMap is non-empty (Java early-returns).
func (c *MetricsCollector) AddBrokerRuntimeStatsMetric(stats *model.BrokerRuntimeStats, cluster, brokerAddress, brokerHost string) {
	labels := &brokerRuntimeVal{
		brokerHost:    brokerHost,
		brokerDesc:     stats.BrokerVersionDesc,
		bootTimestamp:  stats.BootTimestamp,
		brokerVersion:  stats.BrokerVersion,
	}
	key := brokerRuntimeKey{cluster, brokerAddress}
	for _, rm := range c.runtimeMetrics {
		rm.cache.put(key, runtimeEntry{labels: labels, num: rm.getVal(stats)})
	}
	if dist := stats.PutMessageDistributeTimeMap; len(dist) > 0 {
		for _, dm := range c.distributeMetrics {
			dm.cache.put(key, runtimeEntry{labels: labels, num: float64(dist[dm.distKey])})
		}
	}
}

// Sweep purges expired entries from every cache; called by the janitor goroutine
// between scrapes to bound memory (mirrors Guava's passive eviction + the TTL).
func (c *MetricsCollector) Sweep() {
	c.topicOffset.sweep()
	c.topicRetryOffset.sweep()
	c.topicDLQOffset.sweep()
	c.producerCounts.sweep()
	c.topicPutNums.sweep()
	c.topicPutSize.sweep()
	c.consumerDiff.sweep()
	c.consumerRetryDiff.sweep()
	c.consumerDLQDiff.sweep()
	c.consumerCounts.sweep()
	c.consumerClientFailedMsgCounts.sweep()
	c.consumerClientFailedTPS.sweep()
	c.consumerClientOKTPS.sweep()
	c.consumerClientRT.sweep()
	c.consumerClientPullRT.sweep()
	c.consumerClientPullTPS.sweep()
	c.groupBrokerTotalOffset.sweep()
	c.groupConsumeTPS.sweep()
	c.groupGetNums.sweep()
	c.groupGetSize.sweep()
	c.sendBackNums.sweep()
	c.groupLatencyByTime.sweep()
	c.brokerPutNums.sweep()
	c.brokerGetNums.sweep()
	c.brokerCommitLogDiff.sweep()
	for _, rm := range c.runtimeMetrics {
		rm.cache.sweep()
	}
	for _, dm := range c.distributeMetrics {
		dm.cache.sweep()
	}
}

// --- Gather: builds the prometheus MetricFamily list, emitting an empty family
// (with #HELP/#TYPE but no samples) when a cache has no live entries, exactly
// as the Java simpleclient Collector does. ---

type sample struct {
	labelValues []string
	value       float64
}

func gaugeFamily(name, help string, labelNames []string, samples []sample) *dto.MetricFamily {
	metrics := make([]*dto.Metric, 0, len(samples))
	for _, s := range samples {
		labels := make([]*dto.LabelPair, len(s.labelValues))
		for i, lv := range s.labelValues {
			labels[i] = &dto.LabelPair{Name: &labelNames[i], Value: &lv}
		}
		v := s.value
		metrics = append(metrics, &dto.Metric{
			Label: labels,
			Gauge: &dto.Gauge{Value: &v},
		})
	}
	t := dto.MetricType_GAUGE
	return &dto.MetricFamily{Name: &name, Help: &help, Type: &t, Metric: metrics}
}

// runtimeLabels returns the 6 broker-runtime label VALUES for a cache entry.
func runtimeLabels(k brokerRuntimeKey, e runtimeEntry) []string {
	return []string{k.cluster, k.brokerAddress, e.labels.brokerHost, e.labels.brokerDesc,
		strconv.FormatInt(e.labels.bootTimestamp, 10), strconv.Itoa(e.labels.brokerVersion)}
}

func runtimeSamples(cache *ttlCache[brokerRuntimeKey, runtimeEntry]) []sample {
	entries := cache.snapshot()
	out := make([]sample, 0, len(entries))
	for _, kv := range entries {
		out = append(out, sample{runtimeLabels(kv.K, kv.V), kv.V.num})
	}
	return out
}

func (c *MetricsCollector) Gather() ([]*dto.MetricFamily, error) {
	var families []*dto.MetricFamily

	// --- collectConsumerMetric ---
	families = append(families,
		gaugeFamily("rocketmq_group_diff", "GroupDiff", groupDiffLabels, diffSamples(c.consumerDiff)),
		gaugeFamily("rocketmq_group_retrydiff", "GroupRetryDiff", groupDiffLabels, diffSamples(c.consumerRetryDiff)),
		gaugeFamily("rocketmq_group_dlqdiff", "GroupDLQDiff", groupDiffLabels, diffSamples(c.consumerDLQDiff)),
		gaugeFamily("rocketmq_group_count", "GroupCount", groupCountLabels, countSamples(c.consumerCounts)),
	)

	// --- collectProducerMetric ---
	families = append(families,
		gaugeFamily("rocketmq_producer_count", "producer instance counter", producerCountLabels, producerCountSamples(c.producerCounts)),
	)

	// --- collectTopicOffsetMetric ---
	families = append(families,
		gaugeFamily("rocketmq_producer_offset", "TopicOffset", topicOffsetLabels, producerOffsetSamples(c.topicOffset)),
		gaugeFamily("rocketmq_topic_retry_offset", "TopicRetryOffset", topicOffsetLabels, producerOffsetSamples(c.topicRetryOffset)),
		// Java HELP for dlq offset is "TopicRetryOffset" (a typo); preserved verbatim.
		gaugeFamily("rocketmq_topic_dlq_offset", "TopicRetryOffset", dlqOffsetLabels, dlqOffsetSamples(c.topicDLQOffset)),
	)

	// --- collectTopicNums ---
	families = append(families,
		gaugeFamily("rocketmq_producer_tps", "TopicPutNums", topicNumsLabels, topicPutSamples(c.topicPutNums)),
		gaugeFamily("rocketmq_producer_message_size", "TopicPutMessageSize", topicNumsLabels, topicPutSamples(c.topicPutSize)),
	)

	// --- collectGroupNums ---
	families = append(families,
		gaugeFamily("rocketmq_consumer_tps", "GroupGetNums", groupNumsLabels, consumerSamplesFloat(c.groupGetNums)),
		gaugeFamily("rocketmq_group_consume_tps", "GroupConsumeTPS", groupNumsLabels, consumerSamplesFloat(c.groupConsumeTPS)),
		gaugeFamily("rocketmq_consumer_offset", "GroupBrokerTotalOffset", groupNumsLabels, consumerSamplesInt64(c.groupBrokerTotalOffset)),
		// Java never populates this cache (put commented out) -> empty family.
		gaugeFamily("rocketmq_group_consume_total_offset", "GroupConsumeTotalOffset", groupNumsLabels, nil),
		gaugeFamily("rocketmq_consumer_message_size", "GroupGetMessageSize", groupNumsLabels, consumerSamplesFloat(c.groupGetSize)),
		gaugeFamily("rocketmq_send_back_nums", "SendBackNums", groupNumsLabels, consumerSamplesFloat(c.sendBackNums)),
		gaugeFamily("rocketmq_group_get_latency_by_storetime", "GroupGetLatencyByStoreTime", groupLatencyLabels, consumerSamplesInt64(c.groupLatencyByTime)),
	)

	// --- collectClientGroupMetric ---
	families = append(families,
		gaugeFamily("rocketmq_client_consume_fail_msg_count", "consumerClientFailedMsgCounts", clientLabels, clientSamples(c.consumerClientFailedMsgCounts)),
		gaugeFamily("rocketmq_client_consume_fail_msg_tps", "consumerClientFailedTPS", clientLabels, clientSamples(c.consumerClientFailedTPS)),
		gaugeFamily("rocketmq_client_consume_ok_msg_tps", "consumerClientOKTPS", clientLabels, clientSamples(c.consumerClientOKTPS)),
		gaugeFamily("rocketmq_client_consume_rt", "consumerClientRT", clientLabels, clientSamples(c.consumerClientRT)),
		gaugeFamily("rocketmq_client_consumer_pull_rt", "consumerClientPullRT", clientLabels, clientSamples(c.consumerClientPullRT)),
		gaugeFamily("rocketmq_client_consumer_pull_tps", "consumerClientPullTPS", clientLabels, clientSamples(c.consumerClientPullTPS)),
	)

	// --- collectBrokerNums ---
	families = append(families,
		gaugeFamily("rocketmq_broker_tps", "BrokerPutNums", brokerNumsLabels, brokerSamples(c.brokerPutNums)),
		gaugeFamily("rocketmq_broker_qps", "BrokerGetNums", brokerNumsLabels, brokerSamples(c.brokerGetNums)),
		gaugeFamily("rocketmq_broker_commitlog_diff", "brokerCommitLogDiffGauge", brokerNumsLabels, brokerSamples(c.brokerCommitLogDiff)),
	)

	// --- collectBrokerRuntimeStats (distribute-time first, then the rest) ---
	for _, dm := range c.distributeMetrics {
		families = append(families, gaugeFamily(dm.name, dm.help, brokerRuntimeLabels, runtimeSamples(dm.cache)))
	}
	for _, rm := range c.runtimeMetrics {
		families = append(families, gaugeFamily(rm.name, rm.help, brokerRuntimeLabels, runtimeSamples(rm.cache)))
	}

	// --- cluster-health-check (Go-only addition; appended AFTER all Java-parity
	// families so the golden / Java-diff surface is untouched). Counters use
	// counterFamily so an empty family still emits `# TYPE ... counter`. ---
	families = append(families,
		counterFamily("rocketmq_health_check_produce_total",
			"RocketMQ health check produce result count", healthProduceLabels, c.health.produceSamples()),
		counterFamily("rocketmq_health_check_consume_total",
			"RocketMQ health check consumed message count", healthConsumeLabels, c.health.consumeSamples()),
		gaugeFamily("rocketmq_health_check_status",
			"RocketMQ health check status (1=healthy, 0=unhealthy)", healthCheckLabels, c.health.statusSamples()),
		gaugeFamily("rocketmq_health_check_latency_seconds",
			"RocketMQ health check latency in seconds", healthCheckLabels, c.health.latencySamples()),
		gaugeFamily("rocketmq_health_check_last_success_timestamp_seconds",
			"Unix timestamp of last successful health check", healthCheckLabels, c.health.lastSuccessSamples()),
	)

	return families, nil
}

// --- label name slices (byte-identical to the Java *_LABEL_NAMES constants) ---

var (
	groupDiffLabels        = []string{"group", "topic", "countOfOnlineConsumers", "msgModel"}
	groupCountLabels       = []string{"caddr", "localaddr", "group"}
	topicOffsetLabels      = []string{"cluster", "broker", "topic"}
	dlqOffsetLabels        = []string{"cluster", "broker", "group"}
	producerCountLabels    = []string{"cluster", "broker", "group"}
	topicNumsLabels        = []string{"cluster", "broker", "topic"}
	groupNumsLabels        = []string{"cluster", "broker", "topic", "group"}
	groupLatencyLabels     = []string{"cluster", "broker", "topic", "group"}
	clientLabels           = []string{"clientAddr", "clientId", "group", "topic"}
	brokerNumsLabels       = []string{"cluster", "brokerIP", "broker"}
	brokerRuntimeLabels    = []string{"cluster", "brokerIP", "brokerHost", "des", "boottime", "broker_version"}
)

// --- per-cache sample extractors ---

func diffSamples(c *ttlCache[diffKey, int64]) []sample {
	entries := c.snapshot()
	out := make([]sample, 0, len(entries))
	for _, kv := range entries {
		out = append(out, sample{[]string{kv.K.group, kv.K.topic, kv.K.countOnline, kv.K.msgModel}, float64(kv.V)})
	}
	return out
}

func countSamples(c *ttlCache[string, consumerCountVal]) []sample {
	entries := c.snapshot()
	out := make([]sample, 0, len(entries))
	for _, kv := range entries {
		out = append(out, sample{[]string{kv.V.caddrs, kv.V.localaddrs, kv.K}, float64(kv.V.count)})
	}
	return out
}

func producerCountSamples(c *ttlCache[producerCountKey, int]) []sample {
	entries := c.snapshot()
	out := make([]sample, 0, len(entries))
	for _, kv := range entries {
		out = append(out, sample{[]string{kv.K.cluster, kv.K.broker, kv.K.group}, float64(kv.V)})
	}
	return out
}

func producerOffsetSamples(c *ttlCache[producerKey, float64]) []sample {
	entries := c.snapshot()
	out := make([]sample, 0, len(entries))
	for _, kv := range entries {
		out = append(out, sample{[]string{kv.K.cluster, kv.K.broker, kv.K.topic}, kv.V})
	}
	return out
}

func dlqOffsetSamples(c *ttlCache[dlqKey, float64]) []sample {
	entries := c.snapshot()
	out := make([]sample, 0, len(entries))
	for _, kv := range entries {
		out = append(out, sample{[]string{kv.K.cluster, kv.K.broker, kv.K.group}, kv.V})
	}
	return out
}

func topicPutSamples(c *ttlCache[topicPutNumKey, topicPutNumVal]) []sample {
	entries := c.snapshot()
	out := make([]sample, 0, len(entries))
	for _, kv := range entries {
		out = append(out, sample{[]string{kv.K.cluster, kv.V.brokerName, kv.K.topic}, kv.V.val})
	}
	return out
}

// consumerSamples handles both int64-backed and float64-backed ConsumerMetric
// caches; isFloat selects the value interpretation.
func consumerSamplesInt64(c *ttlCache[consumerKey, int64]) []sample {
	entries := c.snapshot()
	out := make([]sample, 0, len(entries))
	for _, kv := range entries {
		out = append(out, sample{[]string{kv.K.cluster, kv.K.broker, kv.K.topic, kv.K.group}, float64(kv.V)})
	}
	return out
}
func consumerSamplesFloat(c *ttlCache[consumerKey, float64]) []sample {
	entries := c.snapshot()
	out := make([]sample, 0, len(entries))
	for _, kv := range entries {
		out = append(out, sample{[]string{kv.K.cluster, kv.K.broker, kv.K.topic, kv.K.group}, kv.V})
	}
	return out
}

func clientSamples(c *ttlCache[clientRuntimeKey, clientRuntimeVal]) []sample {
	entries := c.snapshot()
	out := make([]sample, 0, len(entries))
	for _, kv := range entries {
		out = append(out, sample{[]string{kv.V.caddrs, kv.V.localaddrs, kv.K.group, kv.K.topic}, kv.V.val})
	}
	return out
}

func brokerSamples(c *ttlCache[brokerKey, brokerVal]) []sample {
	entries := c.snapshot()
	out := make([]sample, 0, len(entries))
	for _, kv := range entries {
		out = append(out, sample{[]string{kv.K.cluster, kv.K.brokerIP, kv.V.brokerName}, kv.V.val})
	}
	return out
}
