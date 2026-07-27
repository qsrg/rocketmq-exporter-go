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
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/wcf/rmq-exporter/internal/model"
)

// expectedFamily is the golden snapshot of one Java gauge: name, HELP text, and
// the label names in their EXACT order. Derived from RMQMetricsCollector.java.
type expectedFamily struct {
	name   string
	help   string
	labels []string
}

// goldenFamilies is the byte-fidelity contract: any added/renamed/reordered/
// dropped metric breaks this test, which is the point (golden rule).
var goldenFamilies = []expectedFamily{
	// collectConsumerMetric
	{"rocketmq_group_diff", "GroupDiff", []string{"group", "topic", "countOfOnlineConsumers", "msgModel"}},
	{"rocketmq_group_retrydiff", "GroupRetryDiff", []string{"group", "topic", "countOfOnlineConsumers", "msgModel"}},
	{"rocketmq_group_dlqdiff", "GroupDLQDiff", []string{"group", "topic", "countOfOnlineConsumers", "msgModel"}},
	{"rocketmq_group_count", "GroupCount", []string{"caddr", "localaddr", "group"}},
	// collectProducerMetric
	{"rocketmq_producer_count", "producer instance counter", []string{"cluster", "broker", "group"}},
	// collectTopicOffsetMetric
	{"rocketmq_producer_offset", "TopicOffset", []string{"cluster", "broker", "topic"}},
	{"rocketmq_topic_retry_offset", "TopicRetryOffset", []string{"cluster", "broker", "topic"}},
	{"rocketmq_topic_dlq_offset", "TopicRetryOffset", []string{"cluster", "broker", "group"}}, // Java HELP typo preserved
	// collectTopicNums
	{"rocketmq_producer_tps", "TopicPutNums", []string{"cluster", "broker", "topic"}},
	{"rocketmq_producer_message_size", "TopicPutMessageSize", []string{"cluster", "broker", "topic"}},
	// collectGroupNums
	{"rocketmq_consumer_tps", "GroupGetNums", []string{"cluster", "broker", "topic", "group"}},
	{"rocketmq_group_consume_tps", "GroupConsumeTPS", []string{"cluster", "broker", "topic", "group"}},
	{"rocketmq_consumer_offset", "GroupBrokerTotalOffset", []string{"cluster", "broker", "topic", "group"}},
	{"rocketmq_group_consume_total_offset", "GroupConsumeTotalOffset", []string{"cluster", "broker", "topic", "group"}},
	{"rocketmq_consumer_message_size", "GroupGetMessageSize", []string{"cluster", "broker", "topic", "group"}},
	{"rocketmq_send_back_nums", "SendBackNums", []string{"cluster", "broker", "topic", "group"}},
	{"rocketmq_group_get_latency_by_storetime", "GroupGetLatencyByStoreTime", []string{"cluster", "broker", "topic", "group"}},
	// collectClientGroupMetric
	{"rocketmq_client_consume_fail_msg_count", "consumerClientFailedMsgCounts", []string{"clientAddr", "clientId", "group", "topic"}},
	{"rocketmq_client_consume_fail_msg_tps", "consumerClientFailedTPS", []string{"clientAddr", "clientId", "group", "topic"}},
	{"rocketmq_client_consume_ok_msg_tps", "consumerClientOKTPS", []string{"clientAddr", "clientId", "group", "topic"}},
	{"rocketmq_client_consume_rt", "consumerClientRT", []string{"clientAddr", "clientId", "group", "topic"}},
	{"rocketmq_client_consumer_pull_rt", "consumerClientPullRT", []string{"clientAddr", "clientId", "group", "topic"}},
	{"rocketmq_client_consumer_pull_tps", "consumerClientPullTPS", []string{"clientAddr", "clientId", "group", "topic"}},
	// collectBrokerNums
	{"rocketmq_broker_tps", "BrokerPutNums", []string{"cluster", "brokerIP", "broker"}},
	{"rocketmq_broker_qps", "BrokerGetNums", []string{"cluster", "brokerIP", "broker"}},
	{"rocketmq_broker_commitlog_diff", "brokerCommitLogDiffGauge", []string{"cluster", "brokerIP", "broker"}},
}

// runtimeGolden is appended after goldenFamilies for the broker-runtime gauges.
var runtimeGolden = []expectedFamily{
	{"rocketmq_brokeruntime_pmdt_0ms", "PutMessageDistributeTimeMap0ms", runtimeLabelNames},
	{"rocketmq_brokeruntime_pmdt_0to10ms", "PutMessageDistributeTimeMap0to10ms", runtimeLabelNames},
	{"rocketmq_brokeruntime_pmdt_10to50ms", "PutMessageDistributeTimeMap10to50ms", runtimeLabelNames},
	{"rocketmq_brokeruntime_pmdt_50to100ms", "PutMessageDistributeTimeMap50to100ms", runtimeLabelNames},
	{"rocketmq_brokeruntime_pmdt_100to200ms", "PutMessageDistributeTimeMap100to200ms", runtimeLabelNames},
	{"rocketmq_brokeruntime_pmdt_200to500ms", "PutMessageDistributeTimeMap200to500ms", runtimeLabelNames},
	{"rocketmq_brokeruntime_pmdt_500to1s", "PutMessageDistributeTimeMap500to1s", runtimeLabelNames},
	{"rocketmq_brokeruntime_pmdt_1to2s", "PutMessageDistributeTimeMap1to2s", runtimeLabelNames},
	{"rocketmq_brokeruntime_pmdt_2to3s", "PutMessageDistributeTimeMap2to3s", runtimeLabelNames},
	{"rocketmq_brokeruntime_pmdt_3to4s", "PutMessageDistributeTimeMap3to4s", runtimeLabelNames},
	{"rocketmq_brokeruntime_pmdt_4to5s", "PutMessageDistributeTimeMap4to5s", runtimeLabelNames},
	{"rocketmq_brokeruntime_pmdt_5to10s", "PutMessageDistributeTimeMap5to10s", runtimeLabelNames},
	{"rocketmq_brokeruntime_pmdt_10stomore", "PutMessageDistributeTimeMap10toMore", runtimeLabelNames},
	{"rocketmq_brokeruntime_msg_put_total_today_now", "brokerRuntimeMsgPutTotalTodayNow", runtimeLabelNames},
	{"rocketmq_brokeruntime_msg_gettotal_today_now", "brokerRuntimeMsgGetTotalTodayNow", runtimeLabelNames},
	{"rocketmq_brokeruntime_dispatch_behind_bytes", "brokerRuntimeDispatchBehindBytes", runtimeLabelNames},
	{"rocketmq_brokeruntime_put_message_size_total", "brokerRuntimePutMessageSizeTotal", runtimeLabelNames},
	{"rocketmq_brokeruntime_put_message_average_size", "brokerRuntimePutMessageAverageSize", runtimeLabelNames},
	{"rocketmq_brokeruntime_query_threadpool_queue_capacity", "brokerRuntimeQueryThreadPoolQueueCapacity", runtimeLabelNames},
	{"rocketmq_brokeruntime_remain_transientstore_buffer_numbs", "brokerRuntimeRemainTransientStoreBufferNumbs", runtimeLabelNames},
	{"rocketmq_brokeruntime_earliest_message_timestamp", "brokerRuntimeEarliestMessageTimeStamp", runtimeLabelNames},
	{"rocketmq_brokeruntime_putmessage_entire_time_max", "brokerRuntimePutMessageEntireTimeMax", runtimeLabelNames},
	{"rocketmq_brokeruntime_start_accept_sendrequest_time", "brokerRuntimeStartAcceptSendRequestTimeStamp", runtimeLabelNames},
	{"rocketmq_brokeruntime_send_threadpool_queue_size", "brokerRuntimeSendThreadPoolQueueSize", runtimeLabelNames},
	{"rocketmq_brokeruntime_putmessage_times_total", "brokerRuntimePutMessageTimesTotal", runtimeLabelNames},
	{"rocketmq_brokeruntime_getmessage_entire_time_max", "brokerRuntimeGetMessageEntireTimeMax", runtimeLabelNames},
	{"rocketmq_brokeruntime_pagecache_lock_time_mills", "brokerRuntimePageCacheLockTimeMills", runtimeLabelNames},
	{"rocketmq_brokeruntime_commitlog_disk_ratio", "brokerRuntimeCommitLogDiskRatio", runtimeLabelNames},
	{"rocketmq_brokeruntime_consumequeue_disk_ratio", "brokerRuntimeConsumeQueueDiskRatio", runtimeLabelNames},
	{"rocketmq_brokeruntime_getfound_tps600", "brokerRuntimeGetFoundTps600", runtimeLabelNames},
	{"rocketmq_brokeruntime_getfound_tps60", "brokerRuntimeGetFoundTps60", runtimeLabelNames},
	{"rocketmq_brokeruntime_getfound_tps10", "brokerRuntimeGetFoundTps10", runtimeLabelNames},
	{"rocketmq_brokeruntime_gettotal_tps600", "brokerRuntimeGetTotalTps600", runtimeLabelNames},
	{"rocketmq_brokeruntime_gettotal_tps60", "brokerRuntimeGetTotalTps60", runtimeLabelNames},
	{"rocketmq_brokeruntime_gettotal_tps10", "brokerRuntimeGetTotalTps10", runtimeLabelNames},
	{"rocketmq_brokeruntime_gettransfered_tps600", "brokerRuntimeGetTransferedTps600", runtimeLabelNames},
	{"rocketmq_brokeruntime_gettransfered_tps60", "brokerRuntimeGetTransferedTps60", runtimeLabelNames},
	{"rocketmq_brokeruntime_gettransfered_tps10", "brokerRuntimeGetTransferedTps10", runtimeLabelNames},
	{"rocketmq_brokeruntime_getmiss_tps600", "brokerRuntimeGetMissTps600", runtimeLabelNames},
	{"rocketmq_brokeruntime_getmiss_tps60", "brokerRuntimeGetMissTps60", runtimeLabelNames},
	{"rocketmq_brokeruntime_getmiss_tps10", "brokerRuntimeGetMissTps10", runtimeLabelNames},
	{"rocketmq_brokeruntime_put_tps600", "brokerRuntimePutTps600", runtimeLabelNames},
	{"rocketmq_brokeruntime_put_tps60", "brokerRuntimePutTps60", runtimeLabelNames},
	{"rocketmq_brokeruntime_put_tps10", "brokerRuntimePutTps10", runtimeLabelNames},
	{"rocketmq_brokeruntime_put_latency_99", "brokerRuntimePutLatency99", runtimeLabelNames},
	{"rocketmq_brokeruntime_put_latency_999", "brokerRuntimePutLatency999", runtimeLabelNames},
	{"rocketmq_brokeruntime_dispatch_maxbuffer", "brokerRuntimeDispatchMaxBuffer", runtimeLabelNames},
	{"rocketmq_brokeruntime_pull_threadpoolqueue_capacity", "brokerRuntimePullThreadPoolQueueCapacity", runtimeLabelNames},
	{"rocketmq_brokeruntime_send_threadpoolqueue_capacity", "brokerRuntimeSendThreadPoolQueueCapacity", runtimeLabelNames},
	{"rocketmq_brokeruntime_pull_threadpoolqueue_size", "brokerRuntimePullThreadPoolQueueSizeF", runtimeLabelNames}, // Java help typo preserved
	{"rocketmq_brokeruntime_query_threadpoolqueue_size", "brokerRuntimeQueryThreadPoolQueueSize", runtimeLabelNames},
	{"rocketmq_brokeruntime_pull_threadpoolqueue_headwait_timemills", "brokerRuntimePullThreadPoolQueueHeadWaitTimeMills", runtimeLabelNames},
	{"rocketmq_brokeruntime_query_threadpoolqueue_headwait_timemills", "brokerRuntimeQueryThreadPoolQueueHeadWaitTimeMills", runtimeLabelNames},
	{"rocketmq_brokeruntime_send_threadpoolqueue_headwait_timemills", "brokerRuntimeSendThreadPoolQueueHeadWaitTimeMills", runtimeLabelNames},
	{"rocketmq_brokeruntime_msg_gettotal_yesterdaymorning", "brokerRuntimeMsgGetTotalYesterdayMorning", runtimeLabelNames},
	{"rocketmq_brokeruntime_msg_puttotal_yesterdaymorning", "brokerRuntimeMsgPutTotalYesterdayMorning", runtimeLabelNames},
	{"rocketmq_brokeruntime_msg_gettotal_todaymorning", "brokerRuntimeMsgGetTotalTodayMorning", runtimeLabelNames},
	{"rocketmq_brokeruntime_msg_puttotal_todaymorning", "brokerRuntimeMsgPutTotalTodayMorning", runtimeLabelNames},
	{"rocketmq_brokeruntime_commitlogdir_capacity_free", "brokerRuntimeCommitLogDirCapacityFree", runtimeLabelNames},
	{"rocketmq_brokeruntime_commitlogdir_capacity_total", "brokerRuntimeCommitLogDirCapacityTotal", runtimeLabelNames},
	{"rocketmq_brokeruntime_commitlog_maxoffset", "brokerRuntimeCommitLogMaxOffset", runtimeLabelNames},
	{"rocketmq_brokeruntime_commitlog_minoffset", "brokerRuntimeCommitLogMinOffset", runtimeLabelNames},
	{"rocketmq_brokeruntime_remain_howmanydata_toflush", "brokerRuntimeRemainHowManyDataToFlush", runtimeLabelNames},
}

var runtimeLabelNames = []string{"cluster", "brokerIP", "brokerHost", "des", "boottime", "broker_version"}

func TestGoldenMetricFamilies(t *testing.T) {
	c := New(time.Minute) // TTL irrelevant for the golden check; non-zero so entries are live
	populateAll(c)

	families, err := c.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	got := map[string]*dto.MetricFamily{}
	for _, f := range families {
		got[f.GetName()] = f
	}

	want := append(append([]expectedFamily{}, goldenFamilies...), runtimeGolden...)
	if len(want) != len(got) {
		t.Errorf("family count: got %d, want %d", len(got), len(want))
	}

	for _, w := range want {
		f, ok := got[w.name]
		if !ok {
			t.Errorf("missing family %q", w.name)
			continue
		}
		if f.GetHelp() != w.help {
			t.Errorf("family %q HELP = %q, want %q", w.name, f.GetHelp(), w.help)
		}
		if f.GetType() != dto.MetricType_GAUGE {
			t.Errorf("family %q type = %v, want GAUGE", w.name, f.GetType())
		}
		if len(f.Metric) == 0 {
			// group_consume_total_offset is the only expected empty family.
			if w.name != "rocketmq_group_consume_total_offset" {
				t.Errorf("family %q has no samples after populate", w.name)
			}
			continue
		}
		gotLabels := labelNames(f.Metric[0])
		if !equalSlices(gotLabels, w.labels) {
			t.Errorf("family %q label order = %v, want %v", w.name, gotLabels, w.labels)
		}
	}

	// No spurious families beyond the golden set.
	for name := range got {
		found := false
		for _, w := range want {
			if w.name == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("unexpected family %q not in golden set", name)
		}
	}
}

func populateAll(c *MetricsCollector) {
	// topic offsets (plain + retry + dlq routing)
	c.AddTopicOffsetMetric("c", "b", "topicA", 1, 100)
	c.AddTopicOffsetMetric("c", "b", "%RETRY%g", 1, 200)
	c.AddTopicOffsetMetric("c", "b", "%DLQ%g", 1, 300)

	c.AddProducerCountMetric("c", "b", "pg", 2)
	c.AddGroupCountMetric("g", "1.1.1.1:80", "client-1", 3)
	c.AddGroupDiffMetric("3", "g", "topicA", "0", 9)
	c.AddGroupDiffMetric("3", "g", "%RETRY%g", "0", 8)
	c.AddGroupDiffMetric("3", "g", "%DLQ%g", "0", 7)

	c.AddTopicPutNumsMetric("c", "b", "1.1.1.1", "topicA", 1.5)
	c.AddTopicPutSizeMetric("c", "b", "1.1.1.1", "topicA", 2.5)

	c.AddGroupBrokerTotalOffsetMetric("c", "b", "topicA", "g", 1000)
	c.AddGroupGetLatencyByStoreTimeMetric("c", "b", "topicA", "g", 50)
	c.AddGroupConsumeTPSMetric("c", "b", "topicA", "g", 3.3)
	c.AddGroupGetNumsMetric("c", "b", "topicA", "g", 4.4)
	c.AddGroupGetSizeMetric("c", "b", "topicA", "g", 5.5)
	c.AddSendBackNumsMetric("c", "b", "topicA", "g", 6.6)

	c.AddConsumerClientFailedMsgCountsMetric("g", "topicA", "1.1.1.1:80", "client-1", 10)
	c.AddConsumerClientFailedTPSMetric("g", "topicA", "1.1.1.1:80", "client-1", 1.1)
	c.AddConsumerClientOKTPSMetric("g", "topicA", "1.1.1.1:80", "client-1", 2.2)
	c.AddConsumeRTMetricMetric("g", "topicA", "1.1.1.1:80", "client-1", 3.3)
	c.AddPullRTMetric("g", "topicA", "1.1.1.1:80", "client-1", 4.4)
	c.AddPullTPSMetric("g", "topicA", "1.1.1.1:80", "client-1", 5.5)

	c.AddBrokerPutNumsMetric("c", "1.1.1.1", "b", 7.7)
	c.AddBrokerGetNumsMetric("c", "1.1.1.1", "b", 8.8)
	c.AddBrokerCommitLogDiffMetric("c", "1.1.1.1", "b", 9.9)

	// broker runtime: a stats with a non-empty distribute map populates all 63.
	stats := &model.BrokerRuntimeStats{
		PutMessageDistributeTimeMap: map[string]int{
			"<=0ms": 1, "0~10ms": 1, "10~50ms": 1, "50~100ms": 1, "100~200ms": 1,
			"200~500ms": 1, "500ms~1s": 1, "1~2s": 1, "2~3s": 1, "3~4s": 1,
			"4~5s": 1, "5~10s": 1, "10s~": 1,
		},
		BootTimestamp:    1609459200000,
		BrokerVersion:    323,
		BrokerVersionDesc: "V4_9_8",
	}
	c.AddBrokerRuntimeStatsMetric(stats, "c", "1.1.1.1:10911", "brokerHost")
}

func labelNames(m *dto.Metric) []string {
	out := make([]string, len(m.Label))
	for i, lp := range m.Label {
		out[i] = lp.GetName()
	}
	return out
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
