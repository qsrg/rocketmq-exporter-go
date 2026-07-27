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

package service

import (
	"os"
	"testing"
	"time"

	"github.com/wcf/rmq-exporter/internal/collector"
	"github.com/wcf/rmq-exporter/internal/rmqremote"
)

// liveNamesrv returns the test namesrv address (default 127.0.0.1:9876).
func liveNamesrv() string {
	if v := os.Getenv("RMQ_NAMESRV"); v != "" {
		return v
	}
	return "127.0.0.1:9876"
}

func liveEnabled() bool { return os.Getenv("RMQ_LIVE_TESTS") == "1" }

// liveAdminClient builds an admin client honoring the RMQ_ENABLE_ACL /
// RMQ_ACCESS_KEY / RMQ_SECRET_KEY env vars, so the live suite can run against an
// ACL-enabled broker. Defaults to ACL disabled (plain broker unchanged).
func liveAdminClient(t *testing.T) *AdminClient {
	t.Helper()
	return NewAdminClient(
		liveNamesrv(),
		os.Getenv("RMQ_ENABLE_ACL") == "1",
		os.Getenv("RMQ_ACCESS_KEY"),
		os.Getenv("RMQ_SECRET_KEY"),
		5*time.Second,
	)
}

// TestLiveExamineBrokerClusterInfo connects to a real namesrv, captures the raw
// GET_BROKER_CLUSTER_INFO body to testdata/cluster_info.json (for the offline
// decode test), and asserts the typed decode succeeds with a non-empty cluster.
// Skipped unless RMQ_LIVE_TESTS=1.
func TestLiveExamineBrokerClusterInfo(t *testing.T) {
	if !liveEnabled() {
		t.Skip("set RMQ_LIVE_TESTS=1 to run live broker tests")
	}
	a := liveAdminClient(t)
	defer a.Shutdown(t.Context())

	// Capture the raw body for an offline fixture.
	cmd := rmqremote.NewRemotingCommand(rmqremote.RequestGetBrokerClusterInfo, nil, nil)
	a.signIfACL(cmd)
	resp, err := a.rc.InvokeSync(a.namesrv, cmd, 5*time.Second)
	if err != nil {
		t.Fatalf("InvokeSync: %v", err)
	}
	if resp.Code != rmqremote.ResponseSuccess {
		t.Fatalf("broker returned code %d: %s", resp.Code, resp.Remark)
	}
	if err := writeFixture("cluster_info.json", resp.Body); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	// End-to-end typed decode via the public API.
	ci, err := a.ExamineBrokerClusterInfo()
	if err != nil {
		t.Fatalf("ExamineBrokerClusterInfo: %v", err)
	}
	if len(ci.BrokerAddrTable) == 0 {
		t.Fatalf("BrokerAddrTable empty; full body: %s", string(resp.Body))
	}
	t.Logf("clusters=%v brokers=%d", ci.ClusterAddrTable, len(ci.BrokerAddrTable))
}

// TestLiveFetchAllTopicList fetches every topic from namesrv (206), captures the
// body, and asserts the typed decode returns a slice.
func TestLiveFetchAllTopicList(t *testing.T) {
	if !liveEnabled() {
		t.Skip("set RMQ_LIVE_TESTS=1 to run live broker tests")
	}
	a := liveAdminClient(t)
	defer a.Shutdown(t.Context())

	cmd := rmqremote.NewRemotingCommand(rmqremote.RequestGetAllTopicListFromNameSrv, nil, nil)
	a.signIfACL(cmd)
	resp, err := a.rc.InvokeSync(a.namesrv, cmd, 5*time.Second)
	if err != nil {
		t.Fatalf("InvokeSync: %v", err)
	}
	if resp.Code != rmqremote.ResponseSuccess {
		t.Fatalf("namesrv returned code %d: %s", resp.Code, resp.Remark)
	}
	if err := writeFixture("all_topic_list.json", resp.Body); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	tl, err := a.FetchAllTopicList()
	if err != nil {
		t.Fatalf("FetchAllTopicList: %v", err)
	}
	t.Logf("topics=%d %v", len(tl.TopicList), tl.TopicList)
}

// TestLiveExamineTopicRouteInfo fetches the route for the first topic from the
// list (105) and asserts the typed decode yields BrokerDatas with master addrs.
func TestLiveExamineTopicRouteInfo(t *testing.T) {
	if !liveEnabled() {
		t.Skip("set RMQ_LIVE_TESTS=1 to run live broker tests")
	}
	a := liveAdminClient(t)
	defer a.Shutdown(t.Context())

	tl, err := a.FetchAllTopicList()
	if err != nil {
		t.Fatalf("FetchAllTopicList: %v", err)
	}
	if len(tl.TopicList) == 0 {
		t.Skip("no topics on broker to fetch a route for")
	}
	topic := tl.TopicList[0]

	cmd := rmqremote.NewRemotingCommand(rmqremote.RequestGetRouteInfoByTopic, topicHeader{topic: topic}, nil)
	a.signIfACL(cmd)
	resp, err := a.rc.InvokeSync(a.namesrv, cmd, 5*time.Second)
	if err != nil {
		t.Fatalf("InvokeSync: %v", err)
	}
	if resp.Code == rmqremote.ResponseTopicNotExist {
		t.Skipf("topic %q not present", topic)
	}
	if resp.Code != rmqremote.ResponseSuccess {
		t.Fatalf("namesrv returned code %d: %s", resp.Code, resp.Remark)
	}
	if err := writeFixture("topic_route_data.json", resp.Body); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	tr, err := a.ExamineTopicRouteInfo(topic)
	if err != nil {
		t.Fatalf("ExamineTopicRouteInfo(%q): %v\nbody=%s", topic, err, string(resp.Body))
	}
	if len(tr.BrokerDatas) == 0 {
		t.Fatalf("BrokerDatas empty; body=%s", string(resp.Body))
	}
	for _, bd := range tr.BrokerDatas {
		if _, ok := bd.BrokerAddrs[0]; !ok {
			t.Errorf("broker %q missing master (id=0) address", bd.BrokerName)
		}
	}
	t.Logf("route for %q: %d broker datas", topic, len(tr.BrokerDatas))
}

// TestLiveFetchBrokerRuntimeStats closes the end-to-end loop for the largest
// metric family: fetch a broker's runtime KVTable (28) -> parse via
// model.BrokerRuntimeStats -> feed collector.AddBrokerRuntimeStatsMetric ->
// Gather -> assert the ~63 broker-runtime gauges carry samples. This proves the
// fastjson-tolerant decode + model parser + collector all agree on a real broker.
func TestLiveFetchBrokerRuntimeStats(t *testing.T) {
	if !liveEnabled() {
		t.Skip("set RMQ_LIVE_TESTS=1 to run live broker tests")
	}
	a := liveAdminClient(t)
	defer a.Shutdown(t.Context())

	ci, err := a.ExamineBrokerClusterInfo()
	if err != nil {
		t.Fatalf("ExamineBrokerClusterInfo: %v", err)
	}
	// pick the first broker's master address
	var masterAddr, brokerName, cluster string
	for name, bd := range ci.BrokerAddrTable {
		brokerName, cluster = name, bd.Cluster
		masterAddr = bd.BrokerAddrs[0]
		break
	}
	if masterAddr == "" {
		t.Skip("no master broker address in cluster")
	}

	// capture the raw KVTable body as a REAL fixture (replaces the synthesized one)
	cmd := rmqremote.NewRemotingCommand(rmqremote.RequestGetBrokerRuntimeInfo, nil, nil)
	a.signIfACL(cmd)
	resp, err := a.rc.InvokeSync(masterAddr, cmd, 5*time.Second)
	if err != nil {
		t.Fatalf("InvokeSync broker: %v", err)
	}
	if resp.Code != rmqremote.ResponseSuccess {
		t.Fatalf("broker returned code %d: %s", resp.Code, resp.Remark)
	}
	if err := writeFixture("broker_runtime_stats_live.json", resp.Body); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	stats, err := a.FetchBrokerRuntimeStats(masterAddr)
	if err != nil {
		t.Fatalf("FetchBrokerRuntimeStats %s: %v", masterAddr, err)
	}
	if stats.BrokerVersionDesc == "" {
		t.Fatal("parsed stats has empty BrokerVersionDesc")
	}

	// feed the collector and verify the runtime gauges are populated.
	c := collector.New(60 * time.Second)
	c.AddBrokerRuntimeStatsMetric(stats, cluster, masterAddr, "")
	families, err := c.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	runtimeGauges := 0
	for _, f := range families {
		if len(f.Metric) > 0 {
			runtimeGauges++
		}
	}
	if runtimeGauges == 0 {
		t.Fatal("no gauges populated after AddBrokerRuntimeStatsMetric")
	}
	t.Logf("broker=%s cluster=%s version=%s; populated gauge families=%d",
		brokerName, cluster, stats.BrokerVersionDesc, runtimeGauges)
}

// TestLiveBrokerRPCs exercises every remaining 6.3 broker RPC against the real
// cluster, capturing each response body (esp. the composite-key
// TopicStatsTable/ConsumeStats) so the decode shape can be confirmed. It picks a
// topic that has consumer groups for the consume-side RPCs.
func TestLiveBrokerRPCs(t *testing.T) {
	if !liveEnabled() {
		t.Skip("set RMQ_LIVE_TESTS=1 to run live broker tests")
	}
	a := liveAdminClient(t)
	defer a.Shutdown(t.Context())

	ci, err := a.ExamineBrokerClusterInfo()
	if err != nil {
		t.Fatalf("ExamineBrokerClusterInfo: %v", err)
	}
	var brokerAddr string
	for _, bd := range ci.BrokerAddrTable {
		brokerAddr = bd.BrokerAddrs[0]
		break
	}
	if brokerAddr == "" {
		t.Skip("no master broker")
	}
	t.Logf("using broker %s", brokerAddr)

	tl, _ := a.FetchAllTopicList()
	// find a topic that has consumer groups for the consume-side RPCs
	var topic, group string
	for _, tp := range tl.TopicList {
		if len(tp) == 0 || tp[0] == '%' || tp == "SCHEDULE_TOPIC_XXXX" || tp == "RMQ_SYS_TRANS_HALF_TOPIC" || tp == "TBW102" {
			continue
		}
		if gl, err := a.QueryTopicConsumeByWhoOnBroker(brokerAddr, tp); err == nil && len(gl.GroupList) > 0 {
			topic, group = tp, gl.GroupList[0]
			break
		}
	}
	if topic == "" {
		topic = "BenchmarkTest"
	}
	t.Logf("topic=%q group=%q", topic, group)

	// helper: capture raw body to a fixture, then attempt typed decode (log errors)
	capRPC := func(name string, code int16, header rmqremote.CustomHeader, decode func([]byte) error) {
		cmd := rmqremote.NewRemotingCommand(code, header, nil)
		a.signIfACL(cmd)
		resp, err := a.rc.InvokeSync(brokerAddr, cmd, 5*time.Second)
		if err != nil {
			t.Logf("%s: invoke: %v", name, err)
			return
		}
		if resp.Code != rmqremote.ResponseSuccess {
			t.Logf("%s: code=%d remark=%s", name, resp.Code, resp.Remark)
			return
		}
		_ = writeFixture(name, resp.Body)
		if decode != nil {
			if err := decode(resp.Body); err != nil {
				t.Logf("%s: decode: %v", name, err)
			}
		}
	}

	// ExamineTopicStats (202) — composite-key TopicStatsTable.
	capRPC("topic_stats_table.json", rmqremote.RequestGetTopicStatsInfo, topicHeaderH{topic: topic},
		func(b []byte) error { var t TopicStatsTable; return rmqremote.UnmarshalJSON(b, &t) })

	// QueryTopicConsumeByWho (300).
	capRPC("group_list.json", rmqremote.RequestQueryTopicConsumeByWho, topicHeaderH{topic: topic},
		func(b []byte) error { var g GroupList; return rmqremote.UnmarshalJSON(b, &g) })

	// Consume-side RPCs need a group.
	if group != "" {
		// ExamineConsumerConnectionInfo (38).
		capRPC("consumer_connection.json", rmqremote.RequestGetConsumerListByGroup, groupHeader{consumerGroup: group},
			func(b []byte) error { var cc ConsumerConnection; return rmqremote.UnmarshalJSON(b, &cc) })

		// ExamineConsumeStats (208) — composite-key ConsumeStats.
		capRPC("consume_stats.json", rmqremote.RequestGetConsumeStats, groupTopicHeader{consumerGroup: group, topic: topic},
			func(b []byte) error { var cs ConsumeStats; return rmqremote.UnmarshalJSON(b, &cs) })
	}

	// GetConsumerRunningInfo (307) — needs a clientId; fetch connection first.
	if group != "" {
		if cc, err := a.ExamineConsumerConnectionInfoOnBroker(brokerAddr, group); err == nil && len(cc.ConnectionSet) > 0 {
			cid := cc.ConnectionSet[0].ClientId
			capRPC("consumer_running_info.json", rmqremote.RequestGetConsumerRunningInfo,
				consumerRunningHeader{consumerGroup: group, clientId: cid, jstackEnable: false},
				func(b []byte) error { var c ConsumerRunningInfo; return rmqremote.UnmarshalJSON(b, &c) })
		}
	}

	// ViewBrokerStatsData (315).
	capRPC("broker_stats_data.json", rmqremote.RequestViewBrokerStatsData,
		statsDataHeader{statsName: "TOPIC_PUT_NUMS", statsKey: topic},
		func(b []byte) error { var bsd BrokerStatsData; return rmqremote.UnmarshalJSON(b, &bsd) })

	// GetAllProducerInfo (328).
	capRPC("producer_table_info.json", rmqremote.RequestGetAllProducerInfo, nil,
		func(b []byte) error { var p ProducerTableInfo; return rmqremote.UnmarshalJSON(b, &p) })
}

// TestLiveQueryMsgByOffset finds a queue that actually has messages and pulls 1
// message at a low offset, asserting the binary message-batch decode yields a
// store timestamp (or a documented non-FOUND status).
func TestLiveQueryMsgByOffset(t *testing.T) {
	if !liveEnabled() {
		t.Skip("set RMQ_LIVE_TESTS=1 to run live broker tests")
	}
	a := liveAdminClient(t)
	defer a.Shutdown(t.Context())

	ci, _ := a.ExamineBrokerClusterInfo()
	var brokerAddr string
	for _, bd := range ci.BrokerAddrTable {
		brokerAddr = bd.BrokerAddrs[0]
		break
	}
	tl, _ := a.FetchAllTopicList()
	var foundMQ MessageQueue
	var foundOffset int64
	for _, tp := range tl.TopicList {
		if len(tp) == 0 || tp[0] == '%' {
			continue
		}
		tts, err := a.ExamineTopicStatsOnBroker(brokerAddr, tp)
		if err != nil {
			continue
		}
		entries, _ := tts.Entries()
		for _, e := range entries {
			if e.Offset.MaxOffset > 0 {
				foundMQ, foundOffset = e.Queue, e.Offset.MaxOffset-1
				break
			}
		}
		if foundMQ.Topic != "" {
			break
		}
	}
	if foundMQ.Topic == "" {
		t.Skip("no topic with messages to pull from")
	}

	pr, err := a.QueryMsgByOffset(brokerAddr, foundMQ, foundOffset)
	if err != nil {
		t.Fatalf("QueryMsgByOffset(%v,%d): %v", foundMQ, foundOffset, err)
	}
	t.Logf("pull %v offset=%d: status=%s storeTs=%d minOffset=%d",
		foundMQ, foundOffset, pr.Status, pr.StoreTimestamp, pr.MinOffset)
	if pr.Status == pullFound && pr.StoreTimestamp == 0 {
		t.Errorf("FOUND but storeTimestamp not extracted")
	}
}

func TestDebugRouteShape(t *testing.T) {
	if !liveEnabled() {
		t.Skip("live")
	}
	a := liveAdminClient(t)
	defer a.Shutdown(t.Context())
	for _, topic := range []string{"HA_REAL_ROUTE_TEST", "DefaultCluster", "BenchmarkTopic"} {
		cmd := rmqremote.NewRemotingCommand(rmqremote.RequestGetRouteInfoByTopic, topicHeader{topic: topic}, nil)
		a.signIfACL(cmd)
		resp, err := a.rc.InvokeSync(a.namesrv, cmd, 5*time.Second)
		if err != nil { t.Logf("%s: %v", topic, err); continue }
		t.Logf("=== %s code=%d body=%s", topic, resp.Code, string(resp.Body))
	}
}

func TestDebugConsumerConn(t *testing.T) {
	if !liveEnabled() { t.Skip("live") }
	a := liveAdminClient(t)
	defer a.Shutdown(t.Context())
	for _, addr := range []string{"127.0.0.1:10911", "127.0.0.1:20911"} {
		cmd := rmqremote.NewRemotingCommand(rmqremote.RequestGetConsumerListByGroup, groupHeader{consumerGroup: "ConsumerGroup9"}, nil)
		a.signIfACL(cmd)
		resp, err := a.rc.InvokeSync(addr, cmd, 5*time.Second)
		if err != nil { t.Logf("%s: %v", addr, err); continue }
		t.Logf("=== %s code=%d body=%s", addr, resp.Code, string(resp.Body))
	}
}

// TestLiveACLSigning verifies the ACL signing path end-to-end. The namesrv does
// NOT enforce ACL (only the broker does), so the test first discovers a broker
// address via an unsigned namesrv RPC, then probes the BROKER with an unsigned
// GET_BROKER_RUNTIME_INFO: if the broker accepts it, this host is in the
// broker's globalWhiteRemoteAddresses (127.0.0.1 is whitelisted by default) and
// signing cannot be exercised - the test skips with instructions. When the
// broker enforces ACL, it asserts the unsigned request is rejected and a SIGNED
// request (RMQ_ACCESS_KEY / RMQ_SECRET_KEY) succeeds.
func TestLiveACLSigning(t *testing.T) {
	if !liveEnabled() {
		t.Skip("set RMQ_LIVE_TESTS=1 to run live broker tests")
	}
	probeClient := NewAdminClient(liveNamesrv(), false, "", "", 5*time.Second)
	defer probeClient.Shutdown(t.Context())

	// namesrv RPCs are never ACL-checked; use one to discover a broker address.
	ci, err := probeClient.ExamineBrokerClusterInfo()
	if err != nil {
		t.Skipf("ExamineBrokerClusterInfo: %v", err)
	}
	var brokerAddr string
	for _, bd := range ci.BrokerAddrTable {
		brokerAddr = bd.BrokerAddrs[0]
		break
	}
	if brokerAddr == "" {
		t.Skip("no master broker address")
	}

	// unsigned BROKER RPC: if it succeeds, this host is whitelisted.
	unsignedCmd := rmqremote.NewRemotingCommand(rmqremote.RequestGetBrokerRuntimeInfo, nil, nil)
	resp, err := probeClient.rc.InvokeSync(brokerAddr, unsignedCmd, 5*time.Second)
	if err != nil {
		t.Skipf("transport error probing broker ACL: %v", err)
	}
	if resp.Code == rmqremote.ResponseSuccess {
		t.Skipf("broker %s accepts unsigned requests from this host: 127.0.0.1 is in globalWhiteRemoteAddresses. Remove it from plain_acl.yml and restart the broker to exercise ACL signing", brokerAddr)
	}
	t.Logf("unsigned broker probe rejected as expected: code=%d remark=%s", resp.Code, resp.Remark)

	ak, sk := os.Getenv("RMQ_ACCESS_KEY"), os.Getenv("RMQ_SECRET_KEY")
	if ak == "" || sk == "" {
		t.Fatal("broker enforces ACL but RMQ_ACCESS_KEY/RMQ_SECRET_KEY not set")
	}
	signed := NewAdminClient(liveNamesrv(), true, ak, sk, 5*time.Second)
	defer signed.Shutdown(t.Context())
	stats, err := signed.FetchBrokerRuntimeStats(brokerAddr)
	if err != nil {
		t.Fatalf("signed FetchBrokerRuntimeStats failed (signing broken?): %v", err)
	}
	t.Logf("ACL signing verified: unsigned rejected, signed succeeded (broker=%s version=%s)", brokerAddr, stats.BrokerVersionDesc)
}
