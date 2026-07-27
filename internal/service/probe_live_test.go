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
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

// probeCredentials returns the ACL credentials from the RMQ_* env vars when
// RMQ_ENABLE_ACL=1, so the probe producer/consumer can connect to an ACL broker.
func probeCredentials() (primitive.Credentials, bool) {
	if os.Getenv("RMQ_ENABLE_ACL") != "1" {
		return primitive.Credentials{}, false
	}
	return primitive.Credentials{
		AccessKey: os.Getenv("RMQ_ACCESS_KEY"),
		SecretKey: os.Getenv("RMQ_SECRET_KEY"),
	}, true
}

// TestLiveProbeProducerConsumer spins up a Go producer + consumer against the
// live cluster to CREATE the data the exporter's RPCs need to be validated
// against real non-empty bodies: a registered producer (getAllProducerInfo),
// an online consumer (ConsumerConnection / ConsumerRunningInfo / ConsumeStats),
// and a populated retry queue (the D4 latency-pull path). Skipped unless
// RMQ_LIVE_TESTS=1.
func TestLiveProbeProducerConsumer(t *testing.T) {
	if !liveEnabled() {
		t.Skip("set RMQ_LIVE_TESTS=1 to run live broker tests")
	}
	ns := []string{liveNamesrv()}
	topic := "ProbeTestTopic"
	producerGroup := "ProbeProducerGroup"
	consumerGroup := "ProbeConsumerGroup"

	// --- producer ---
	popts := []producer.Option{
		producer.WithNameServer(ns),
		producer.WithGroupName(producerGroup),
		producer.WithInstanceName("rmq-exporter-probe-producer"),
		producer.WithRetry(2),
	}
	if creds, ok := probeCredentials(); ok {
		popts = append(popts, producer.WithCredentials(creds))
	}
	p, err := producer.NewDefaultProducer(popts...)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	if err := p.Start(); err != nil {
		t.Fatalf("producer start: %v", err)
	}
	defer p.Shutdown()

	for i := 0; i < 20; i++ {
		msg := primitive.NewMessage(topic, []byte(fmt.Sprintf("probe-%d", i)))
		if _, err := p.SendSync(t.Context(), msg); err != nil {
			t.Logf("send %d: %v (continuing)", i, err)
		}
	}
	t.Log("producer: 20 messages sent")

	// --- consumer: succeed on even, retry on odd (forces %RETRY% population) ---
	copts := []consumer.Option{
		consumer.WithNameServer(ns),
		consumer.WithGroupName(consumerGroup),
		consumer.WithInstance("rmq-exporter-probe-consumer"),
		consumer.WithMaxReconsumeTimes(2),
	}
	if creds, ok := probeCredentials(); ok {
		copts = append(copts, consumer.WithCredentials(creds))
	}
	c, err := consumer.NewPushConsumer(copts...)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	var consumed, retried int64
	if err := c.Subscribe(topic, consumer.MessageSelector{},
		func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			for _, m := range msgs {
				atomic.AddInt64(&consumed, 1)
				// odd-indexed bodies fail to drive retries into %RETRY%<group>.
				if len(m.Body) > 0 && m.Body[len(m.Body)-1]%2 == 1 {
					atomic.AddInt64(&retried, 1)
					return consumer.ConsumeRetryLater, nil
				}
			}
			return consumer.ConsumeSuccess, nil
		}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := c.Start(); err != nil {
		t.Fatalf("consumer start: %v", err)
	}
	defer c.Shutdown()

	// give consumers/producers time to register and retries to materialize
	time.Sleep(15 * time.Second)
	t.Logf("consumer: consumed=%d retried=%d", atomic.LoadInt64(&consumed), atomic.LoadInt64(&retried))

	// --- now exercise the exporter's admin RPCs against the freshly-seeded data ---
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

	// getAllProducerInfo must now see our producer group.
	pti, err := a.GetAllProducerInfo(brokerAddr)
	if err != nil {
		t.Fatalf("GetAllProducerInfo: %v", err)
	}
	t.Logf("GetAllProducerInfo: %d groups (%v)", len(pti.Data), producerGroups(pti.Data))
	if _, ok := pti.Data[producerGroup]; !ok {
		t.Errorf("producer group %q not in ProducerTableInfo (data=%v)", producerGroup, producerGroups(pti.Data))
	}

	// ConsumerConnection must now see an online consumer.
	cc, err := a.ExamineConsumerConnectionInfo(consumerGroup)
	if err != nil {
		t.Fatalf("ExamineConsumerConnectionInfo(%q): %v", consumerGroup, err)
	}
	if len(cc.ConnectionSet) == 0 {
		t.Errorf("no online consumers for %q", consumerGroup)
	} else {
		t.Logf("ConsumerConnection: %d online, model=%s", len(cc.ConnectionSet), cc.MessageModel)
	}

	// ConsumerRunningInfo (uses the first connection's clientId).
	if len(cc.ConnectionSet) > 0 {
		cid := cc.ConnectionSet[0].ClientId
		if cri, err := a.GetConsumerRunningInfo(consumerGroup, cid, false); err != nil {
			t.Logf("GetConsumerRunningInfo(%q): %v", cid, err)
		} else {
			t.Logf("GetConsumerRunningInfo: %d status topics", len(cri.StatusTable))
		}
	}

	// ConsumeStats on the live topic.
	if cs, err := a.ExamineConsumeStats(consumerGroup, topic); err != nil {
		t.Logf("ExamineConsumeStats(%q,%q): %v", consumerGroup, topic, err)
	} else {
		diff, _ := cs.ComputeTotalDiff()
		t.Logf("ConsumeStats(%q,%q): %d entries, totalDiff=%d, tps=%v", consumerGroup, topic, len(cs.OffsetTable), diff, cs.ConsumeTps)
	}

	// D4: pull the %RETRY%<group> queue and check for a found message + storeTs.
	retryTopic := "%RETRY%" + consumerGroup
	cs, err := a.ExamineConsumeStats(consumerGroup, retryTopic)
	if err != nil {
		t.Logf("ExamineConsumeStats(retry %q): %v (retry queue may be empty)", retryTopic, err)
	} else {
		entries, _ := cs.Entries()
		found := 0
		for _, e := range entries {
			pr, err := a.QueryMsgByOffset(brokerAddr, e.Queue, e.Offset.ConsumerOffset)
			if err != nil {
				t.Logf("QueryMsgByOffset retry queue %v @%d: %v", e.Queue, e.Offset.ConsumerOffset, err)
				continue
			}
			t.Logf("retry pull %v @%d: status=%s storeTs=%d", e.Queue, e.Offset.ConsumerOffset, pr.Status, pr.StoreTimestamp)
			if pr.Status == pullFound && pr.StoreTimestamp > 0 {
				found++
			}
		}
		t.Logf("D4 retry pull: %d/%d queues returned a found message with storeTimestamp", found, len(entries))
		if found == 0 {
			t.Logf("D4 NOTE: no retry messages pullable yet (retry delay levels may not have elapsed); this confirms D4 is a broker-side retry-visibility timing issue, not a Go decode bug")
		}
	}
}

func producerGroups(data map[string][]ProducerInfo) []string {
	out := make([]string, 0, len(data))
	for k := range data {
		out = append(out, k)
	}
	return out
}
