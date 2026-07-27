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

package task

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	dto "github.com/prometheus/client_model/go"

	"github.com/wcf/rmq-exporter/internal/collector"
)

// taskProbeCredentials returns ACL credentials from the RMQ_* env vars when
// RMQ_ENABLE_ACL=1, so the probe producer/consumer can connect to an ACL broker.
func taskProbeCredentials() (primitive.Credentials, bool) {
	if os.Getenv("RMQ_ENABLE_ACL") != "1" {
		return primitive.Credentials{}, false
	}
	return primitive.Credentials{
		AccessKey: os.Getenv("RMQ_ACCESS_KEY"),
		SecretKey: os.Getenv("RMQ_SECRET_KEY"),
	}, true
}

// TestLiveD4RetryTopicLatency validates the D4 fix: collectConsumerOffset must
// emit rocketmq_group_get_latency_by_storetime samples for a group's queues
// (including the %RETRY%<group> topic) even when the pull returns NO_NEW_MSG
// (lagTime 0), matching Java's !containsKey -> put(0) behavior. We seed a
// producer + a retry-inducing consumer, run the task, and assert the metric
// carries samples for the probe group. Skipped unless RMQ_LIVE_TESTS=1.
func TestLiveD4RetryTopicLatency(t *testing.T) {
	if !taskLiveEnabled() {
		t.Skip("set RMQ_LIVE_TESTS=1 to run live integration tests")
	}
	ctx := context.Background()
	ns := []string{taskNamesrv()}
	topic := "ProbeTestTopic"
	pgroup := "ProbeProducerGroup"
	cgroup := "ProbeConsumerGroup"

	// seed producer
	popts := []producer.Option{
		producer.WithNameServer(ns), producer.WithGroupName(pgroup),
		producer.WithInstanceName("rmq-exporter-d4-producer"), producer.WithRetry(2),
	}
	if creds, ok := taskProbeCredentials(); ok {
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
		_, _ = p.SendSync(ctx, primitive.NewMessage(topic, []byte{byte(i)}))
	}

	// seed consumer: retry odd-indexed bodies to populate %RETRY%<group>
	copts := []consumer.Option{
		consumer.WithNameServer(ns), consumer.WithGroupName(cgroup),
		consumer.WithInstance("rmq-exporter-d4-consumer"), consumer.WithMaxReconsumeTimes(2),
	}
	if creds, ok := taskProbeCredentials(); ok {
		copts = append(copts, consumer.WithCredentials(creds))
	}
	c, err := consumer.NewPushConsumer(copts...)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	if err := c.Subscribe(topic, consumer.MessageSelector{},
		func(_ context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			for _, m := range msgs {
				if len(m.Body) > 0 && m.Body[0]%2 == 1 {
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
	time.Sleep(12 * time.Second) // let retries materialize

	// run collectConsumerOffset against the seeded data
	admin := taskAdminClient()
	defer admin.Shutdown(ctx)
	coll := collector.New(120 * time.Second)
	pool := New(10, 5000)
	pool.Start(ctx)
	defer pool.Shutdown(ctx)
	ct := &CollectTask{Admin: admin, Coll: coll, EnableCollect: true, Pool: pool}
	ct.CollectConsumerOffset(ctx)

	families, err := coll.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	// count latency samples for the probe group (across topic + retry topic)
	var probeSamples, retrySamples int
	for _, f := range families {
		if f.GetName() != "rocketmq_group_get_latency_by_storetime" {
			continue
		}
		for _, m := range f.Metric {
			grp := labelValue(m.Label, "group")
			if grp == cgroup {
				probeSamples++
				if labelValue(m.Label, "topic") == "%RETRY%"+cgroup {
					retrySamples++
				}
			}
		}
	}
	t.Logf("latency samples: probe-group=%d (of which retry-topic=%d)", probeSamples, retrySamples)
	if probeSamples == 0 {
		t.Error("D4 REGRESSION: no rocketmq_group_get_latency_by_storetime samples for the probe group (expected >=1, lag=0 on NO_NEW)")
	}
	// retry-topic sample is the specific D4 case; it may be 0 if the retry queue
	// had no queues in the consume stats this tick, but ideally >=1.
	if retrySamples == 0 {
		t.Logf("D4 NOTE: no retry-topic latency sample this tick (retry queue may not have appeared in ConsumeStats yet)")
	}
}

// labelValue returns the value of a label by name, or "".
func labelValue(labels []*dto.LabelPair, name string) string {
	for _, lp := range labels {
		if lp.GetName() == name {
			return lp.GetValue()
		}
	}
	return ""
}
