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
	"github.com/qsrg/rocketmq-exporter-go/internal/model"
)

// buildRuntimeMetrics returns the ~50 non-distribute broker-runtime gauges, each
// backed by its own cache (mirroring the per-field caches in
// RMQMetricsCollector.addBrokerRuntimeStatsMetric / addAllKindOfTps /
// addCommitLogDirCapacity). Order matches the Java collect emission order so a
// diff against Java /metrics lines up.
func buildRuntimeMetrics(newCache func() *ttlCache[brokerRuntimeKey, runtimeEntry]) []runtimeMetric {
	specs := []runtimeMetric{
		// collectBrokerRuntimeStatsPutMessageDistributeTime is emitted separately
		// (see distributeMetrics) BEFORE these — so we leave a placeholder note:
		// the 13 pmdt gauges are appended in Gather before the runtimeMetrics.

		{name: "rocketmq_brokeruntime_msg_put_total_today_now", help: "brokerRuntimeMsgPutTotalTodayNow",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.MsgPutTotalTodayNow) }},
		{name: "rocketmq_brokeruntime_msg_gettotal_today_now", help: "brokerRuntimeMsgGetTotalTodayNow",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.MsgGetTotalTodayNow) }},

		{name: "rocketmq_brokeruntime_dispatch_behind_bytes", help: "brokerRuntimeDispatchBehindBytes",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.DispatchBehindBytes) }},
		{name: "rocketmq_brokeruntime_put_message_size_total", help: "brokerRuntimePutMessageSizeTotal",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.PutMessageSizeTotal) }},
		{name: "rocketmq_brokeruntime_put_message_average_size", help: "brokerRuntimePutMessageAverageSize",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.PutMessageAverageSize }},
		{name: "rocketmq_brokeruntime_query_threadpool_queue_capacity", help: "brokerRuntimeQueryThreadPoolQueueCapacity",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.QueryThreadPoolQueueCapacity) }},
		{name: "rocketmq_brokeruntime_remain_transientstore_buffer_numbs", help: "brokerRuntimeRemainTransientStoreBufferNumbs",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.RemainTransientStoreBufferNumbs) }},
		{name: "rocketmq_brokeruntime_earliest_message_timestamp", help: "brokerRuntimeEarliestMessageTimeStamp",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.EarliestMessageTimeStamp) }},
		{name: "rocketmq_brokeruntime_putmessage_entire_time_max", help: "brokerRuntimePutMessageEntireTimeMax",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.PutMessageEntireTimeMax) }},
		{name: "rocketmq_brokeruntime_start_accept_sendrequest_time", help: "brokerRuntimeStartAcceptSendRequestTimeStamp",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.StartAcceptSendRequestTimeStamp) }},
		{name: "rocketmq_brokeruntime_send_threadpool_queue_size", help: "brokerRuntimeSendThreadPoolQueueSize",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.SendThreadPoolQueueSize) }},
		{name: "rocketmq_brokeruntime_putmessage_times_total", help: "brokerRuntimePutMessageTimesTotal",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.PutMessageTimesTotal) }},
		{name: "rocketmq_brokeruntime_getmessage_entire_time_max", help: "brokerRuntimeGetMessageEntireTimeMax",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.GetMessageEntireTimeMax) }},
		{name: "rocketmq_brokeruntime_pagecache_lock_time_mills", help: "brokerRuntimePageCacheLockTimeMills",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.PageCacheLockTimeMills) }},
		{name: "rocketmq_brokeruntime_commitlog_disk_ratio", help: "brokerRuntimeCommitLogDiskRatio",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.CommitLogDiskRatio }},
		{name: "rocketmq_brokeruntime_consumequeue_disk_ratio", help: "brokerRuntimeConsumeQueueDiskRatio",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.ConsumeQueueDiskRatio }},

		// addAllKindOfTps: getFoundTps (10/60/600)
		{name: "rocketmq_brokeruntime_getfound_tps600", help: "brokerRuntimeGetFoundTps600",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.GetFoundTps.SixHundred }},
		{name: "rocketmq_brokeruntime_getfound_tps60", help: "brokerRuntimeGetFoundTps60",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.GetFoundTps.Sixty }},
		{name: "rocketmq_brokeruntime_getfound_tps10", help: "brokerRuntimeGetFoundTps10",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.GetFoundTps.Ten }},
	}
	specs = append(specs,
		runtimeMetric{name: "rocketmq_brokeruntime_gettotal_tps600", help: "brokerRuntimeGetTotalTps600",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.GetTotalTps.SixHundred }},
		runtimeMetric{name: "rocketmq_brokeruntime_gettotal_tps60", help: "brokerRuntimeGetTotalTps60",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.GetTotalTps.Sixty }},
		runtimeMetric{name: "rocketmq_brokeruntime_gettotal_tps10", help: "brokerRuntimeGetTotalTps10",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.GetTotalTps.Ten }},

		runtimeMetric{name: "rocketmq_brokeruntime_gettransfered_tps600", help: "brokerRuntimeGetTransferedTps600",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.GetTransferedTps.SixHundred }},
		runtimeMetric{name: "rocketmq_brokeruntime_gettransfered_tps60", help: "brokerRuntimeGetTransferedTps60",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.GetTransferedTps.Sixty }},
		runtimeMetric{name: "rocketmq_brokeruntime_gettransfered_tps10", help: "brokerRuntimeGetTransferedTps10",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.GetTransferedTps.Ten }},

		runtimeMetric{name: "rocketmq_brokeruntime_getmiss_tps600", help: "brokerRuntimeGetMissTps600",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.GetMissTps.SixHundred }},
		runtimeMetric{name: "rocketmq_brokeruntime_getmiss_tps60", help: "brokerRuntimeGetMissTps60",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.GetMissTps.Sixty }},
		runtimeMetric{name: "rocketmq_brokeruntime_getmiss_tps10", help: "brokerRuntimeGetMissTps10",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.GetMissTps.Ten }},

		runtimeMetric{name: "rocketmq_brokeruntime_put_tps600", help: "brokerRuntimePutTps600",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.PutTps.SixHundred }},
		runtimeMetric{name: "rocketmq_brokeruntime_put_tps60", help: "brokerRuntimePutTps60",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.PutTps.Sixty }},
		runtimeMetric{name: "rocketmq_brokeruntime_put_tps10", help: "brokerRuntimePutTps10",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.PutTps.Ten }},

		// put latency
		runtimeMetric{name: "rocketmq_brokeruntime_put_latency_99", help: "brokerRuntimePutLatency99",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.PutLatency99 }},
		runtimeMetric{name: "rocketmq_brokeruntime_put_latency_999", help: "brokerRuntimePutLatency999",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.PutLatency999 }},

		// dispatch max buffer
		runtimeMetric{name: "rocketmq_brokeruntime_dispatch_maxbuffer", help: "brokerRuntimeDispatchMaxBuffer",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.DispatchMaxBuffer) }},

		// threadpool queue capacity / size / headwait
		runtimeMetric{name: "rocketmq_brokeruntime_pull_threadpoolqueue_capacity", help: "brokerRuntimePullThreadPoolQueueCapacity",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.PullThreadPoolQueueCapacity) }},
		runtimeMetric{name: "rocketmq_brokeruntime_send_threadpoolqueue_capacity", help: "brokerRuntimeSendThreadPoolQueueCapacity",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.SendThreadPoolQueueCapacity) }},
		runtimeMetric{name: "rocketmq_brokeruntime_pull_threadpoolqueue_size", help: "brokerRuntimePullThreadPoolQueueSizeF", // Java help typo preserved
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.PullThreadPoolQueueSize) }},
		runtimeMetric{name: "rocketmq_brokeruntime_query_threadpoolqueue_size", help: "brokerRuntimeQueryThreadPoolQueueSize",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.QueryThreadPoolQueueSize) }},
		runtimeMetric{name: "rocketmq_brokeruntime_pull_threadpoolqueue_headwait_timemills", help: "brokerRuntimePullThreadPoolQueueHeadWaitTimeMills",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.PullThreadPoolQueueHeadWaitTimeMills) }},
		runtimeMetric{name: "rocketmq_brokeruntime_query_threadpoolqueue_headwait_timemills", help: "brokerRuntimeQueryThreadPoolQueueHeadWaitTimeMills",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.QueryThreadPoolQueueHeadWaitTimeMills) }},
		runtimeMetric{name: "rocketmq_brokeruntime_send_threadpoolqueue_headwait_timemills", help: "brokerRuntimeSendThreadPoolQueueHeadWaitTimeMills",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.SendThreadPoolQueueHeadWaitTimeMills) }},

		// msg totals (yesterday/today morning)
		runtimeMetric{name: "rocketmq_brokeruntime_msg_gettotal_yesterdaymorning", help: "brokerRuntimeMsgGetTotalYesterdayMorning",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.MsgGetTotalYesterdayMorning) }},
		runtimeMetric{name: "rocketmq_brokeruntime_msg_puttotal_yesterdaymorning", help: "brokerRuntimeMsgPutTotalYesterdayMorning",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.MsgPutTotalYesterdayMorning) }},
		runtimeMetric{name: "rocketmq_brokeruntime_msg_gettotal_todaymorning", help: "brokerRuntimeMsgGetTotalTodayMorning",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.MsgGetTotalTodayMorning) }},
		runtimeMetric{name: "rocketmq_brokeruntime_msg_puttotal_todaymorning", help: "brokerRuntimeMsgPutTotalTodayMorning",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.MsgPutTotalTodayMorning) }},

		// commitlog dir capacity / offsets / remain-to-flush
		runtimeMetric{name: "rocketmq_brokeruntime_commitlogdir_capacity_free", help: "brokerRuntimeCommitLogDirCapacityFree",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.CommitLogDirCapacityFree }},
		runtimeMetric{name: "rocketmq_brokeruntime_commitlogdir_capacity_total", help: "brokerRuntimeCommitLogDirCapacityTotal",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.CommitLogDirCapacityTotal }},
		runtimeMetric{name: "rocketmq_brokeruntime_commitlog_maxoffset", help: "brokerRuntimeCommitLogMaxOffset",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.CommitLogMaxOffset) }},
		runtimeMetric{name: "rocketmq_brokeruntime_commitlog_minoffset", help: "brokerRuntimeCommitLogMinOffset",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return float64(s.CommitLogMinOffset) }},
		runtimeMetric{name: "rocketmq_brokeruntime_remain_howmanydata_toflush", help: "brokerRuntimeRemainHowManyDataToFlush",
			getVal: func(s *model.BrokerRuntimeStats) float64 { return s.RemainHowManyDataToFlush }},
	)

	// assign a fresh cache to each spec
	for i := range specs {
		specs[i].cache = newCache()
	}
	return specs
}

// buildDistributeMetrics returns the 13 PutMessageDistributeTime gauges, each
// backed by its own cache and keyed by the broker's putMessageDistributeTime
// map key (Java addBrokerRuntimePutMessageDistributeTimeMap).
func buildDistributeMetrics(newCache func() *ttlCache[brokerRuntimeKey, runtimeEntry]) []distributeMetric {
	dm := []distributeMetric{
		{name: "rocketmq_brokeruntime_pmdt_0ms", help: "PutMessageDistributeTimeMap0ms", distKey: "<=0ms"},
		{name: "rocketmq_brokeruntime_pmdt_0to10ms", help: "PutMessageDistributeTimeMap0to10ms", distKey: "0~10ms"},
		{name: "rocketmq_brokeruntime_pmdt_10to50ms", help: "PutMessageDistributeTimeMap10to50ms", distKey: "10~50ms"},
		{name: "rocketmq_brokeruntime_pmdt_50to100ms", help: "PutMessageDistributeTimeMap50to100ms", distKey: "50~100ms"},
		{name: "rocketmq_brokeruntime_pmdt_100to200ms", help: "PutMessageDistributeTimeMap100to200ms", distKey: "100~200ms"},
		{name: "rocketmq_brokeruntime_pmdt_200to500ms", help: "PutMessageDistributeTimeMap200to500ms", distKey: "200~500ms"},
		{name: "rocketmq_brokeruntime_pmdt_500to1s", help: "PutMessageDistributeTimeMap500to1s", distKey: "500ms~1s"},
		{name: "rocketmq_brokeruntime_pmdt_1to2s", help: "PutMessageDistributeTimeMap1to2s", distKey: "1~2s"},
		{name: "rocketmq_brokeruntime_pmdt_2to3s", help: "PutMessageDistributeTimeMap2to3s", distKey: "2~3s"},
		{name: "rocketmq_brokeruntime_pmdt_3to4s", help: "PutMessageDistributeTimeMap3to4s", distKey: "3~4s"},
		{name: "rocketmq_brokeruntime_pmdt_4to5s", help: "PutMessageDistributeTimeMap4to5s", distKey: "4~5s"},
		{name: "rocketmq_brokeruntime_pmdt_5to10s", help: "PutMessageDistributeTimeMap5to10s", distKey: "5~10s"},
		{name: "rocketmq_brokeruntime_pmdt_10stomore", help: "PutMessageDistributeTimeMap10toMore", distKey: "10s~"},
	}
	for i := range dm {
		dm[i].cache = newCache()
	}
	return dm
}
