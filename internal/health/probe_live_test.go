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

package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/wcf/rmq-exporter/internal/collector"
	"github.com/wcf/rmq-exporter/internal/config"
	"github.com/wcf/rmq-exporter/internal/service"
)

func liveEnabled() bool { return os.Getenv("RMQ_LIVE_TESTS") == "1" }

func liveNamesrv() string {
	if v := os.Getenv("RMQ_NAMESRV"); v != "" {
		return v
	}
	return "127.0.0.1:9876"
}

// liveConfig builds a config wired to the live broker (RMQ_NAMESRV) with ACL
// credentials from the RMQ_* env vars (mirrors service.liveAdminClient). The
// health-check rate is raised so samples accumulate quickly within the test
// window. The produce loop's sends auto-create HealthCheckTopic-<cluster> via
// the broker's autoCreateTopicEnable; the consumer retries until the topic route
// exists, so no manual topic pre-creation is needed (see probe.go addProbe).
func liveConfig() *config.Config {
	cfg := config.Default()
	cfg.Namesrv = liveNamesrv()
	cfg.EnableACL = os.Getenv("RMQ_ENABLE_ACL") == "1"
	cfg.AccessKey = os.Getenv("RMQ_ACCESS_KEY")
	cfg.SecretKey = os.Getenv("RMQ_SECRET_KEY")
	cfg.HealthCheck.Rate = 5.0
	cfg.HealthCheck.Recency = 5 * time.Second
	cfg.HealthCheck.ClusterRefresh = 30 * time.Second
	return &cfg
}

// waitForHealthy polls HealthDetail until Overall=="ok" or the deadline elapses.
func waitForHealthy(t *testing.T, coll *collector.MetricsCollector, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if coll.HealthDetail().Overall == "ok" {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

// findHealthSample returns the first metric value in family `name` whose labels
// contain all of `want` (subset match; nil matches any single-sample family).
func findHealthSample(t *testing.T, coll *collector.MetricsCollector, name string, want map[string]string) (float64, bool) {
	t.Helper()
	fams, err := coll.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.Metric {
			if want == nil {
				return metricVal(m), true
			}
			// Subset match: every wanted label is present with the wanted value.
			match := true
			for k, v := range want {
				found := false
				for _, lp := range m.Label {
					if lp.GetName() == k && lp.GetValue() == v {
						found = true
						break
					}
				}
				if !found {
					match = false
					break
				}
			}
			if match {
				return metricVal(m), true
			}
		}
	}
	return 0, false
}

func metricVal(m *dto.Metric) float64 {
	if m.Counter != nil {
		return m.Counter.GetValue()
	}
	if m.Gauge != nil {
		return m.Gauge.GetValue()
	}
	return 0
}

// TestLiveHealthProbe runs the prober against a real 4.9.8 broker and asserts the
// end-to-end path: produce+consume succeed, status{overall}==1, /healthz 200,
// consume_total increments, and consume latency is positive and reasonable.
// Also covers the ACL scenario (9.2) when RMQ_ENABLE_ACL=1 is set: the same
// adapter signs produce/consume with the configured credentials. Skipped unless
// RMQ_LIVE_TESTS=1.
func TestLiveHealthProbe(t *testing.T) {
	if !liveEnabled() {
		t.Skip("set RMQ_LIVE_TESTS=1 to run live broker tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	cfg := liveConfig()
	admin := service.NewAdminClient(cfg.Namesrv, cfg.EnableACL, cfg.AccessKey, cfg.SecretKey, 5*time.Second)
	if err := admin.Start(ctx); err != nil {
		t.Fatalf("admin start: %v", err)
	}
	defer admin.Shutdown(context.Background())

	coll := collector.New(time.Minute)
	hca, err := NewAdapter(cfg)
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	prober := NewProber(cfg.HealthCheck, NewClusterLister(admin), coll, hca.Producer(), hca.ConsumerFactory(), nil)
	if err := prober.Start(ctx); err != nil {
		t.Fatalf("prober start: %v", err)
	}
	defer prober.Shutdown(context.Background())

	if !waitForHealthy(t, coll, 40*time.Second) {
		snap := coll.HealthDetail()
		t.Fatalf("cluster did not become healthy; snapshot=%+v", snap)
	}

	// status{overall}==1
	if v, ok := findHealthSample(t, coll, "rocketmq_health_check_status", map[string]string{"check": "overall"}); !ok || v != 1 {
		t.Errorf("status{overall} = %v (ok=%v), want 1", v, ok)
	}

	// /healthz 200 + JSON
	rec := httptest.NewRecorder()
	HealthzHandler(coll).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("/healthz = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	// consume_total > 0
	if v, ok := findHealthSample(t, coll, "rocketmq_health_check_consume_total", nil); !ok || v <= 0 {
		t.Errorf("consume_total = %v (ok=%v), want > 0", v, ok)
	}
	// consume latency positive and reasonable (< 5s)
	if v, ok := findHealthSample(t, coll, "rocketmq_health_check_latency_seconds", map[string]string{"check": "consume"}); !ok || v <= 0 || v > 5 {
		t.Errorf("consume latency = %v (ok=%v), want (0,5]", v, ok)
	}
	t.Logf("live health probe healthy; snapshot=%+v", coll.HealthDetail())
}

// TestLiveHealthProbeConsumerFault verifies the recency->0 path live: after the
// cluster is healthy, stopping its consumer makes last_consume go stale, so
// status{consume} and status{overall} flip to 0 within recency. (Recovery back
// to 1 is covered deterministically by TestProduceFailureRecovery with stubs;
// restarting a live push consumer mid-test is brittle, so only the fault
// direction is asserted here.) Skipped unless RMQ_LIVE_TESTS=1.
func TestLiveHealthProbeConsumerFault(t *testing.T) {
	if !liveEnabled() {
		t.Skip("set RMQ_LIVE_TESTS=1 to run live broker tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	cfg := liveConfig()
	admin := service.NewAdminClient(cfg.Namesrv, cfg.EnableACL, cfg.AccessKey, cfg.SecretKey, 5*time.Second)
	if err := admin.Start(ctx); err != nil {
		t.Fatalf("admin start: %v", err)
	}
	defer admin.Shutdown(context.Background())

	coll := collector.New(time.Minute)
	hca, err := NewAdapter(cfg)
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	prober := NewProber(cfg.HealthCheck, NewClusterLister(admin), coll, hca.Producer(), hca.ConsumerFactory(), nil)
	if err := prober.Start(ctx); err != nil {
		t.Fatalf("prober start: %v", err)
	}
	defer prober.Shutdown(context.Background())

	if !waitForHealthy(t, coll, 40*time.Second) {
		t.Fatalf("cluster did not become healthy: %+v", coll.HealthDetail())
	}

	// Stop the only cluster's consumer -> last_consume goes stale.
	for _, cp := range prober.snapshotProbes() {
		if err := cp.consumer.Shutdown(); err != nil {
			t.Logf("consumer shutdown: %v", err)
		}
	}

	// Wait past recency for the eval tick to flip status to 0.
	deadline := time.Now().Add(cfg.HealthCheck.Recency + 5*time.Second)
	for time.Now().Before(deadline) {
		if v, ok := findHealthSample(t, coll, "rocketmq_health_check_status", map[string]string{"check": "consume"}); ok && v == 0 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if v, ok := findHealthSample(t, coll, "rocketmq_health_check_status", map[string]string{"check": "consume"}); !ok || v != 0 {
		t.Errorf("status{consume} = %v (ok=%v), want 0 after consumer stopped", v, ok)
	}
	if v, ok := findHealthSample(t, coll, "rocketmq_health_check_status", map[string]string{"check": "overall"}); !ok || v != 0 {
		t.Errorf("status{overall} = %v (ok=%v), want 0 after consumer stopped", v, ok)
	}
}
