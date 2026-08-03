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

	"github.com/qsrg/rocketmq-exporter-go/internal/collector"
	"github.com/qsrg/rocketmq-exporter-go/internal/service"
)

func taskLiveEnabled() bool { return os.Getenv("RMQ_LIVE_TESTS") == "1" }
func taskNamesrv() string {
	if v := os.Getenv("RMQ_NAMESRV"); v != "" {
		return v
	}
	return "127.0.0.1:9876"
}

// taskAdminClient builds an admin client honoring the RMQ_ENABLE_ACL /
// RMQ_ACCESS_KEY / RMQ_SECRET_KEY env vars, so the live task suite can run
// against an ACL-enabled broker. Defaults to ACL disabled (plain broker).
func taskAdminClient() *service.AdminClient {
	return service.NewAdminClient(
		taskNamesrv(),
		os.Getenv("RMQ_ENABLE_ACL") == "1",
		os.Getenv("RMQ_ACCESS_KEY"),
		os.Getenv("RMQ_SECRET_KEY"),
		10*time.Second,
	)
}

// TestLiveCollectTasks runs every collection task once against the live cluster,
// then gathers /metrics and reports how many gauge families are populated. This
// is the end-to-end exercise of the cron-task port (task 8.x) before the Java
// diff (task 10).
func TestLiveCollectTasks(t *testing.T) {
	if !taskLiveEnabled() {
		t.Skip("set RMQ_LIVE_TESTS=1 to run live integration tests")
	}
	ctx := context.Background()
	admin := taskAdminClient()
	defer admin.Shutdown(ctx)
	if err := admin.Start(ctx); err != nil {
		t.Fatalf("admin start: %v", err)
	}
	coll := collector.New(120 * time.Second)
	pool := New(10, 5000)
	pool.Start(ctx)
	defer pool.Shutdown(ctx)

	ct := &CollectTask{Admin: admin, Coll: coll, EnableCollect: true, Pool: pool}

	t.Run("collectTopicOffset", func(t *testing.T) { ct.CollectTopicOffset(ctx) })
	t.Run("collectProducer", func(t *testing.T) { ct.CollectProducer(ctx) })
	t.Run("collectConsumerOffset", func(t *testing.T) { ct.CollectConsumerOffset(ctx) })
	t.Run("collectBrokerStatsTopic", func(t *testing.T) { ct.CollectBrokerStatsTopic(ctx) })
	t.Run("collectBrokerStats", func(t *testing.T) { ct.CollectBrokerStats(ctx) })
	t.Run("collectBrokerGroupStats", func(t *testing.T) { ct.CollectBrokerGroupStats(ctx) })
	t.Run("collectBrokerRuntimeStats", func(t *testing.T) { ct.CollectBrokerRuntimeStats(ctx) })

	// let any in-flight client-metric tasks drain
	time.Sleep(time.Second)
	families, err := coll.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	populated, samples := 0, 0
	for _, f := range families {
		if len(f.Metric) > 0 {
			populated++
			samples += len(f.Metric)
		}
	}
	t.Logf("gathered: %d families, %d populated, %d samples", len(families), populated, samples)
	if populated == 0 {
		t.Fatal("no metric families populated — collection produced nothing")
	}
}
