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

package model

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// NOTE: the fixture in testdata/broker_runtime_stats.json is SYNTHESIZED from
// the confirmed 4.9.8 broker formats (StoreStatsService.putMessageDistributeTimeToString
// et al.), not captured from a live broker (no cluster available offline). A
// real captured KVTable should replace it during the task-10 integration diff.

func TestBrokerRuntimeStatsParse(t *testing.T) {
	kv := loadFixture(t)

	s, err := NewBrokerRuntimeStats(kv)
	if err != nil {
		t.Fatalf("NewBrokerRuntimeStats: %v", err)
	}

	// int64 fields
	assertInt64(t, s.MsgPutTotalTodayNow, 100, "MsgPutTotalTodayNow")
	assertInt64(t, s.MsgGetTotalTodayNow, 200, "MsgGetTotalTodayNow")
	assertInt64(t, s.MsgPutTotalTodayMorning, 10, "MsgPutTotalTodayMorning")
	assertInt64(t, s.MsgGetTotalTodayMorning, 20, "MsgGetTotalTodayMorning")
	assertInt64(t, s.MsgPutTotalYesterdayMorning, 1, "MsgPutTotalYesterdayMorning")
	assertInt64(t, s.MsgGetTotalYesterdayMorning, 2, "MsgGetTotalYesterdayMorning")
	assertInt64(t, s.BootTimestamp, 1609459200000, "BootTimestamp")
	assertInt(t, s.BrokerVersion, 323, "BrokerVersion")
	assertInt64(t, s.CommitLogMinOffset, 1000, "CommitLogMinOffset")
	assertInt64(t, s.CommitLogMaxOffset, 2000, "CommitLogMaxOffset")
	assertInt64(t, s.DispatchMaxBuffer, 4096, "DispatchMaxBuffer")
	assertInt64(t, s.PageCacheLockTimeMills, 1000, "PageCacheLockTimeMills")
	assertInt64(t, s.GetMessageEntireTimeMax, 500, "GetMessageEntireTimeMax")
	assertInt64(t, s.PutMessageTimesTotal, 9999, "PutMessageTimesTotal")
	assertInt64(t, s.SendThreadPoolQueueSize, 21, "SendThreadPoolQueueSize")
	assertInt64(t, s.StartAcceptSendRequestTimeStamp, 1609459200001, "StartAcceptSendRequestTimeStamp")
	assertInt64(t, s.PutMessageEntireTimeMax, 750, "PutMessageEntireTimeMax")
	assertInt64(t, s.EarliestMessageTimeStamp, 1609459200002, "EarliestMessageTimeStamp")
	assertInt64(t, s.RemainTransientStoreBufferNumbs, 8, "RemainTransientStoreBufferNumbs")
	assertInt64(t, s.QueryThreadPoolQueueCapacity, 64, "QueryThreadPoolQueueCapacity")
	assertInt64(t, s.DispatchBehindBytes, 4096, "DispatchBehindBytes")
	assertInt64(t, s.PutMessageSizeTotal, 1048576, "PutMessageSizeTotal")
	assertInt64(t, s.SendThreadPoolQueueCapacity, 16, "SendThreadPoolQueueCapacity")
	assertInt64(t, s.PullThreadPoolQueueCapacity, 17, "PullThreadPoolQueueCapacity")
	assertInt64(t, s.SendThreadPoolQueueHeadWaitTimeMills, 11, "SendThreadPoolQueueHeadWaitTimeMills")
	assertInt64(t, s.QueryThreadPoolQueueHeadWaitTimeMills, 12, "QueryThreadPoolQueueHeadWaitTimeMills")
	assertInt64(t, s.PullThreadPoolQueueHeadWaitTimeMills, 13, "PullThreadPoolQueueHeadWaitTimeMills")
	assertInt64(t, s.QueryThreadPoolQueueSize, 14, "QueryThreadPoolQueueSize")
	assertInt64(t, s.PullThreadPoolQueueSize, 15, "PullThreadPoolQueueSize")

	// string fields
	if s.Runtime != "[runtime]" {
		t.Errorf("Runtime = %q, want [runtime]", s.Runtime)
	}
	if s.BrokerVersionDesc != "V4_9_8" {
		t.Errorf("BrokerVersionDesc = %q, want V4_9_8", s.BrokerVersionDesc)
	}

	// double fields
	assertFloat(t, s.RemainHowManyDataToFlush, 12345.6, "RemainHowManyDataToFlush")
	assertFloat(t, s.ConsumeQueueDiskRatio, 0.25, "ConsumeQueueDiskRatio")
	assertFloat(t, s.CommitLogDiskRatio, 0.5, "CommitLogDiskRatio")
	assertFloat(t, s.PutMessageAverageSize, 1024.5, "PutMessageAverageSize")
	assertFloat(t, s.PutLatency99, 5.5, "PutLatency99")
	assertFloat(t, s.PutLatency999, 10.5, "PutLatency999")
	assertFloat(t, s.CommitLogDirCapacityTotal, 1.5*1024*1024*1024, "CommitLogDirCapacityTotal")
	assertFloat(t, s.CommitLogDirCapacityFree, 500.0*1024*1024, "CommitLogDirCapacityFree")

	// TPS triples (ten, sixty, sixHundred)
	assertTps(t, s.PutTps, 10, 60, 600, "PutTps")
	assertTps(t, s.GetMissTps, 1, 2, 3, "GetMissTps")
	assertTps(t, s.GetTransferedTps, 4, 5, 6, "GetTransferedTps") // getTransferredTps key
	assertTps(t, s.GetTotalTps, 7, 8, 9, "GetTotalTps")
	assertTps(t, s.GetFoundTps, 10, 11, 12, "GetFoundTps")

	// distribute-time map: 13 buckets with bracket-stripped keys
	if len(s.PutMessageDistributeTimeMap) != 13 {
		t.Fatalf("PutMessageDistributeTimeMap len = %d, want 13", len(s.PutMessageDistributeTimeMap))
	}
	want := map[string]int{"<=0ms": 1, "0~10ms": 2, "10~50ms": 3, "50~100ms": 4,
		"100~200ms": 5, "200~500ms": 6, "500ms~1s": 7, "1~2s": 8, "2~3s": 9,
		"3~4s": 10, "4~5s": 11, "5~10s": 12, "10s~": 13}
	for k, v := range want {
		if got := s.PutMessageDistributeTimeMap[k]; got != v {
			t.Errorf("PutMessageDistributeTimeMap[%q] = %d, want %d", k, got, v)
		}
	}

	// schedule message offsets
	if len(s.ScheduleMessageOffsetTables) != 2 {
		t.Fatalf("ScheduleMessageOffsetTables len = %d, want 2", len(s.ScheduleMessageOffsetTables))
	}
}

func TestBrokerRuntimeStatsGetTransferedFallback(t *testing.T) {
	// When only the misspelled getTransferedTps key is present, the parser must
	// still populate GetTransferedTps (Java fallback branch).
	kv := loadFixture(t)
	delete(kv, "getTransferredTps")
	s, err := NewBrokerRuntimeStats(kv)
	if err != nil {
		t.Fatalf("NewBrokerRuntimeStats: %v", err)
	}
	assertTps(t, s.GetTransferedTps, 0, 0, 0, "GetTransferedTps without fallback key")
}

func TestBrokerRuntimeStatsPutLatencyDefault(t *testing.T) {
	// putLatency99/999 default to -1 when absent.
	kv := loadFixture(t)
	delete(kv, "putLatency99")
	delete(kv, "putLatency999")
	s, err := NewBrokerRuntimeStats(kv)
	if err != nil {
		t.Fatalf("NewBrokerRuntimeStats: %v", err)
	}
	assertFloat(t, s.PutLatency99, -1, "PutLatency99 default")
	assertFloat(t, s.PutLatency999, -1, "PutLatency999 default")
}

func loadFixture(t *testing.T) map[string]string {
	t.Helper()
	path := filepath.Join("testdata", "broker_runtime_stats.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var kv map[string]string
	if err := json.Unmarshal(b, &kv); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	return kv
}

func assertInt64(t *testing.T, got, want int64, name string) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %d, want %d", name, got, want)
	}
}

func assertInt(t *testing.T, got, want int, name string) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %d, want %d", name, got, want)
	}
}

func assertFloat(t *testing.T, got, want float64, name string) {
	t.Helper()
	const eps = 1e-6
	if abs(got-want) > eps {
		t.Errorf("%s = %v, want %v", name, got, want)
	}
}

func assertTps(t *testing.T, tp TpsTriple, ten, sixty, sixHundred float64, name string) {
	t.Helper()
	const eps = 1e-6
	if abs(tp.Ten-ten) > eps || abs(tp.Sixty-sixty) > eps || abs(tp.SixHundred-sixHundred) > eps {
		t.Errorf("%s = {%v %v %v}, want {%v %v %v}", name, tp.Ten, tp.Sixty, tp.SixHundred, ten, sixty, sixHundred)
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
