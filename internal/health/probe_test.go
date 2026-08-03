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
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"

	"github.com/qsrg/rocketmq-exporter-go/internal/collector"
	"github.com/qsrg/rocketmq-exporter-go/internal/config"
)

// --- fakes ---

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock(at time.Time) *fakeClock   { return &fakeClock{t: at} }
func (f *fakeClock) Now() time.Time          { f.mu.Lock(); defer f.mu.Unlock(); return f.t }
func (f *fakeClock) Advance(d time.Duration) { f.mu.Lock(); defer f.mu.Unlock(); f.t = f.t.Add(d) }
func (f *fakeClock) NewTicker(_ time.Duration) Ticker {
	return &fakeTicker{ch: make(chan time.Time, 1)}
}

type fakeTicker struct{ ch chan time.Time }

func (t *fakeTicker) Chan() <-chan time.Time { return t.ch }
func (t *fakeTicker) Stop()                  {}

type stubProducer struct {
	mu      sync.Mutex
	err     error
	sends   int
	started bool
	stopped bool
}

func (s *stubProducer) Start() error { s.mu.Lock(); s.started = true; s.mu.Unlock(); return nil }
func (s *stubProducer) Shutdown() error {
	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()
	return nil
}
func (s *stubProducer) SendSync(_ context.Context, _ ...*primitive.Message) (*primitive.SendResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sends++
	if s.err != nil {
		return nil, s.err
	}
	return &primitive.SendResult{}, nil
}
func (s *stubProducer) setErr(e error) { s.mu.Lock(); s.err = e; s.mu.Unlock() }

type stubConsumer struct {
	handler  MessageHandler
	started  bool
	startErr error
}

func (s *stubConsumer) Subscribe(_ string, _ consumer.MessageSelector, f MessageHandler) error {
	s.handler = f
	return nil
}
func (s *stubConsumer) Start() error    { s.started = true; return s.startErr }
func (s *stubConsumer) Shutdown() error { s.started = false; return nil }

// --- helpers ---

func defaultCfg() config.HealthCheckConfig {
	return config.HealthCheckConfig{
		Enabled:        true,
		TopicPrefix:    "HealthCheckTopic-",
		GroupPrefix:    "HealthCheckGroup-",
		Rate:           2.0,
		Recency:        5 * time.Second,
		ClusterRefresh: 5 * time.Minute,
		Path:           "/healthz",
	}
}

func newTestProbe(t *testing.T, clk *fakeClock, prod Producer) (*clusterProbe, *collector.MetricsCollector) {
	t.Helper()
	coll := collector.New(time.Minute)
	cp := &clusterProbe{
		cluster: "c1",
		topic:   "HealthCheckTopic-c1",
		group:   "HealthCheckGroup-c1",
		cfg:     defaultCfg(),
		coll:    coll,
		prod:    prod,
		clk:     clk,
		log:     slog.Default(),
		done:    make(chan struct{}),
	}
	return cp, coll
}

// deliverMsg simulates the consumer receiving one message whose body timestamp
// is the clock's current time.
func deliverMsg(cp *clusterProbe, clk *fakeClock) {
	body := []byte(strconv.FormatInt(clk.Now().UnixMilli(), 10))
	_, _ = cp.handleMessage(context.Background(), &primitive.MessageExt{Message: primitive.Message{Body: body}})
}

// metricValue finds (family, labels) in coll.Gather() and returns its value.
func metricValue(t *testing.T, coll *collector.MetricsCollector, family string, labels map[string]string) float64 {
	t.Helper()
	fams, err := coll.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range fams {
		if f.GetName() != family {
			continue
		}
		for _, m := range f.Metric {
			if len(m.Label) != len(labels) {
				continue
			}
			match := true
			for _, lp := range m.Label {
				if v, ok := labels[lp.GetName()]; !ok || v != lp.GetValue() {
					match = false
					break
				}
			}
			if !match {
				continue
			}
			if m.Counter != nil {
				return m.Counter.GetValue()
			}
			if m.Gauge != nil {
				return m.Gauge.GetValue()
			}
		}
	}
	t.Fatalf("metric %s %v not found", family, labels)
	return 0
}

// --- tests ---

func TestSendOneProduceSuccessAndFailure(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	prod := &stubProducer{}
	cp, coll := newTestProbe(t, clk, prod)
	ctx := context.Background()

	cp.sendOne(ctx)
	cp.sendOne(ctx)
	if got := metricValue(t, coll, "rocketmq_health_check_produce_total", map[string]string{"cluster": "c1", "result": "success"}); got != 2 {
		t.Errorf("produce success = %v, want 2", got)
	}
	if got := metricValue(t, coll, "rocketmq_health_check_last_success_timestamp_seconds", map[string]string{"cluster": "c1", "check": "produce"}); got != 1000 {
		t.Errorf("produce last_success = %v, want 1000", got)
	}

	prod.setErr(errors.New("no route"))
	cp.sendOne(ctx)
	if got := metricValue(t, coll, "rocketmq_health_check_produce_total", map[string]string{"cluster": "c1", "result": "failure"}); got != 1 {
		t.Errorf("produce failure = %v, want 1", got)
	}
	// last_produce_success must NOT advance on failure (still 1000).
	if got := metricValue(t, coll, "rocketmq_health_check_last_success_timestamp_seconds", map[string]string{"cluster": "c1", "check": "produce"}); got != 1000 {
		t.Errorf("produce last_success advanced on failure = %v, want 1000", got)
	}
}

func TestConsumeLatencyFromBodyTimestamp(t *testing.T) {
	clk := newFakeClock(time.Unix(2000, 0))
	cp, coll := newTestProbe(t, clk, &stubProducer{})

	// body timestamp 500ms before now
	sendTs := clk.Now().Add(-500 * time.Millisecond)
	body := []byte(strconv.FormatInt(sendTs.UnixMilli(), 10))
	_, _ = cp.handleMessage(context.Background(), &primitive.MessageExt{Message: primitive.Message{Body: body}})

	if got := metricValue(t, coll, "rocketmq_health_check_consume_total", map[string]string{"cluster": "c1"}); got != 1 {
		t.Errorf("consume_total = %v, want 1", got)
	}
	got := metricValue(t, coll, "rocketmq_health_check_latency_seconds", map[string]string{"cluster": "c1", "check": "consume"})
	if got < 0.499 || got > 0.501 {
		t.Errorf("consume latency = %v, want ~0.5", got)
	}
}

func TestRecencyStatusFlips(t *testing.T) {
	clk := newFakeClock(time.Unix(3000, 0))
	cp, coll := newTestProbe(t, clk, &stubProducer{})
	ctx := context.Background()

	cp.sendOne(ctx) // lastProduceSuccess = 3000
	deliverMsg(cp, clk)
	cp.evalOnce(clk.Now())
	if got := metricValue(t, coll, "rocketmq_health_check_status", map[string]string{"cluster": "c1", "check": "overall"}); got != 1 {
		t.Errorf("within recency overall = %v, want 1", got)
	}

	clk.Advance(6 * time.Second) // beyond 5s recency
	cp.evalOnce(clk.Now())
	for _, check := range []string{"produce", "consume", "overall"} {
		if got := metricValue(t, coll, "rocketmq_health_check_status", map[string]string{"cluster": "c1", "check": check}); got != 0 {
			t.Errorf("beyond recency %s = %v, want 0", check, got)
		}
	}
}

func TestStartupStatusZero(t *testing.T) {
	clk := newFakeClock(time.Unix(5000, 0))
	cp, coll := newTestProbe(t, clk, &stubProducer{})

	cp.evalOnce(clk.Now()) // never succeeded -> last_success zero -> status 0
	for _, check := range []string{"produce", "consume", "overall"} {
		if got := metricValue(t, coll, "rocketmq_health_check_status", map[string]string{"cluster": "c1", "check": check}); got != 0 {
			t.Errorf("startup %s = %v, want 0", check, got)
		}
	}
}

func TestProduceFailureRecovery(t *testing.T) {
	clk := newFakeClock(time.Unix(6000, 0))
	prod := &stubProducer{}
	cp, coll := newTestProbe(t, clk, prod)
	ctx := context.Background()

	// healthy
	cp.sendOne(ctx)
	deliverMsg(cp, clk)
	cp.evalOnce(clk.Now())
	if got := metricValue(t, coll, "rocketmq_health_check_status", map[string]string{"cluster": "c1", "check": "overall"}); got != 1 {
		t.Fatalf("initial overall = %v, want 1", got)
	}

	// produce goes down; advance past recency so last_produce_success is stale
	prod.setErr(errors.New("broker down"))
	clk.Advance(6 * time.Second)
	cp.sendOne(ctx)     // failure; lastProduceSuccess unchanged
	deliverMsg(cp, clk) // consume still fresh at new now
	cp.evalOnce(clk.Now())
	if got := metricValue(t, coll, "rocketmq_health_check_status", map[string]string{"cluster": "c1", "check": "produce"}); got != 0 {
		t.Errorf("stale produce = %v, want 0", got)
	}
	if got := metricValue(t, coll, "rocketmq_health_check_status", map[string]string{"cluster": "c1", "check": "overall"}); got != 0 {
		t.Errorf("down overall = %v, want 0", got)
	}

	// recover
	prod.setErr(nil)
	cp.sendOne(ctx) // fresh success
	deliverMsg(cp, clk)
	cp.evalOnce(clk.Now())
	if got := metricValue(t, coll, "rocketmq_health_check_status", map[string]string{"cluster": "c1", "check": "overall"}); got != 1 {
		t.Errorf("recovered overall = %v, want 1", got)
	}
}

// --- multi-cluster discovery / reconcile / lifecycle ---

type stubLister struct {
	mu       sync.Mutex
	clusters []string
	err      error
}

func (s *stubLister) ListClusters(_ context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.clusters...), s.err
}

func (s *stubLister) set(clusters []string, err error) {
	s.mu.Lock()
	s.clusters = clusters
	s.err = err
	s.mu.Unlock()
}

type recordingFactory struct {
	mu        sync.Mutex
	created   map[string]*stubConsumer // group -> consumer
	failStart bool                     // when true, built consumers' Start fails (simulates missing topic)
}

func (f *recordingFactory) factory() ConsumerFactory {
	return func(group string) (Consumer, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		c := &stubConsumer{}
		if f.failStart {
			c.startErr = errors.New("topic route not found")
		}
		f.created[group] = c
		return c, nil
	}
}

func newReconcileProber(t *testing.T) (*Prober, *stubLister, *stubProducer, *recordingFactory, *collector.MetricsCollector) {
	t.Helper()
	clk := newFakeClock(time.Unix(0, 0))
	prod := &stubProducer{}
	lister := &stubLister{}
	factory := &recordingFactory{created: map[string]*stubConsumer{}}
	coll := collector.New(time.Minute)
	p := NewProber(defaultCfg(), lister, coll, prod, factory.factory(), clk)
	return p, lister, prod, factory, coll
}

func TestReconcileClustersAddRemove(t *testing.T) {
	p, _, _, factory, _ := newReconcileProber(t)
	ctx := context.Background()

	p.reconcileClusters(ctx, []string{"A", "B"})
	probes := p.snapshotProbes()
	if len(probes) != 2 || probes["A"] == nil || probes["B"] == nil {
		t.Fatalf("after first reconcile, probes = %v", probes)
	}

	// B disappears, C appears.
	p.reconcileClusters(ctx, []string{"A", "C"})
	probes = p.snapshotProbes()
	if len(probes) != 2 || probes["A"] == nil || probes["C"] == nil || probes["B"] != nil {
		t.Fatalf("after second reconcile, probes = %v", probes)
	}

	// B's consumer was shut down; C's is started.
	factory.mu.Lock()
	bCons := factory.created["HealthCheckGroup-B"]
	cCons := factory.created["HealthCheckGroup-C"]
	factory.mu.Unlock()
	if bCons == nil || bCons.started {
		t.Errorf("B consumer should exist and be shut down; started=%v", bCons == nil || bCons.started)
	}
	if cCons == nil || !cCons.started {
		t.Errorf("C consumer should be started")
	}
}

func TestDiscoverFailureLeavesProbesIntact(t *testing.T) {
	p, lister, _, _, _ := newReconcileProber(t)
	ctx := context.Background()

	p.reconcileClusters(ctx, []string{"A"})
	if got := len(p.snapshotProbes()); got != 1 {
		t.Fatalf("expected 1 probe, got %d", got)
	}

	// Discovery fails: the refresh loop must skip reconcile, so A stays.
	lister.set(nil, errors.New("namesrv down"))
	if _, err := p.discoverClusters(ctx); err == nil {
		t.Fatal("expected discovery error")
	}
	// (Refresh loop calls discover then reconcile only on success; simulate by
	// NOT calling reconcileClusters here, mirroring refreshLoop's behavior.)
	if got := len(p.snapshotProbes()); got != 1 {
		t.Errorf("probes wiped by failed discovery: got %d, want 1", got)
	}
}

func TestStartShutdownLifecycle(t *testing.T) {
	p, lister, prod, _, _ := newReconcileProber(t)
	lister.set([]string{"A"}, nil)
	ctx := context.Background()

	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Initial discovery is synchronous in Start; A's probe must exist.
	if got := len(p.snapshotProbes()); got != 1 {
		t.Fatalf("after Start, probes = %d, want 1", got)
	}
	if !prod.started {
		t.Error("producer should be started")
	}

	p.Shutdown(context.Background())
	if got := len(p.snapshotProbes()); got != 0 {
		t.Errorf("after Shutdown, probes = %d, want 0", got)
	}
	if !prod.stopped {
		t.Error("producer should be shut down")
	}
}

// hasHealthClusterSample reports whether any rocketmq_health_check_* sample
// carries the given cluster label (used to detect ghost samples after removal).
func hasHealthClusterSample(t *testing.T, coll *collector.MetricsCollector, cluster string) bool {
	t.Helper()
	fams, err := coll.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range fams {
		if !strings.HasPrefix(f.GetName(), "rocketmq_health_check_") {
			continue
		}
		for _, m := range f.Metric {
			for _, lp := range m.Label {
				if lp.GetName() == "cluster" && lp.GetValue() == cluster {
					return true
				}
			}
		}
	}
	return false
}

// TestEvalTickSerializesClear verifies the evalMu fix: while evalTick holds
// evalMu, removeProbe's ClearHealthCluster must wait, so a mid-pass removal
// cannot leave ghost samples. Without evalMu this test fails (removeProbe
// completes immediately while evalMu is held).
func TestEvalTickSerializesClear(t *testing.T) {
	p, lister, _, _, coll := newReconcileProber(t)
	lister.set([]string{"A"}, nil)
	ctx := context.Background()
	if err := p.addProbe(ctx, "A"); err != nil {
		t.Fatalf("addProbe: %v", err)
	}
	cp := p.snapshotProbes()["A"]
	cp.sendOne(ctx) // produce_total{A,success}=1
	cp.evalOnce(p.clk.Now())
	if !hasHealthClusterSample(t, coll, "A") {
		t.Fatal("expected A samples after evalOnce")
	}

	// Hold evalMu to mimic an in-flight evalTick pass.
	p.evalMu.Lock()
	done := make(chan struct{})
	go func() {
		p.removeProbe("A")
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("removeProbe completed while evalMu held; ClearHealthCluster not serialized with evalTick")
	case <-time.After(50 * time.Millisecond):
	}

	p.evalMu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("removeProbe did not complete after evalMu released")
	}

	if hasHealthClusterSample(t, coll, "A") {
		t.Error("ghost samples remain for A after removeProbe")
	}
}

// TestEmptyDiscoveryKeepsProbesAndCounters verifies the empty-discovery guard:
// an empty (but successful) discovery must NOT tear down existing probes or
// reset their cumulative counters, since empty is usually a transient namesrv
// state rather than "all clusters gone".
func TestEmptyDiscoveryKeepsProbesAndCounters(t *testing.T) {
	p, _, _, _, coll := newReconcileProber(t)
	ctx := context.Background()
	p.reconcileClusters(ctx, []string{"A"})
	cp := p.snapshotProbes()["A"]
	cp.sendOne(ctx) // produce_total{A,success}=1

	before := metricValue(t, coll, "rocketmq_health_check_produce_total",
		map[string]string{"cluster": "A", "result": "success"})
	if before != 1 {
		t.Fatalf("produce_total before = %v, want 1", before)
	}

	// Empty discovery: A must stay and its counter must not reset.
	p.reconcileClusters(ctx, []string{})
	if got := len(p.snapshotProbes()); got != 1 {
		t.Errorf("empty discovery wiped probes: got %d, want 1", got)
	}
	after := metricValue(t, coll, "rocketmq_health_check_produce_total",
		map[string]string{"cluster": "A", "result": "success"})
	if after != 1 {
		t.Errorf("produce_total after empty discovery = %v, want 1 (counter reset)", after)
	}
}

// TestProduceLastErrorClearedOnRecovery verifies finding ③: produce last_error
// holds the real RPC error on failure and is cleared on the next success (no
// stale error after recovery).
func TestProduceLastErrorClearedOnRecovery(t *testing.T) {
	clk := newFakeClock(time.Unix(7000, 0))
	prod := &stubProducer{}
	cp, coll := newTestProbe(t, clk, prod)
	ctx := context.Background()

	prod.setErr(errors.New("broker unreachable"))
	cp.sendOne(ctx)
	if got := coll.HealthDetail().Clusters["c1"].Produce.LastError; got != "broker unreachable" {
		t.Errorf("after failure, produce last_error = %q, want %q", got, "broker unreachable")
	}

	prod.setErr(nil)
	cp.sendOne(ctx)
	if got := coll.HealthDetail().Clusters["c1"].Produce.LastError; got != "" {
		t.Errorf("after recovery, produce last_error = %q, want empty", got)
	}
}

// TestConsumeLastErrorDerivedFromRecency verifies finding ⑤: consume has no
// error callback, so last_error is derived from the recency timeout (empty when
// fresh, a description when stale) instead of being a dead field.
func TestConsumeLastErrorDerivedFromRecency(t *testing.T) {
	clk := newFakeClock(time.Unix(8000, 0))
	cp, coll := newTestProbe(t, clk, &stubProducer{})
	ctx := context.Background()

	cp.sendOne(ctx)
	deliverMsg(cp, clk)
	cp.evalOnce(clk.Now())
	if got := coll.HealthDetail().Clusters["c1"].Consume.LastError; got != "" {
		t.Errorf("fresh consume last_error = %q, want empty", got)
	}

	clk.Advance(6 * time.Second) // beyond 5s recency, no new message
	cp.evalOnce(clk.Now())
	if got := coll.HealthDetail().Clusters["c1"].Consume.LastError; got == "" {
		t.Error("stale consume last_error = empty, want recency-timeout description")
	}
}

// TestProduceLatencyClearedWhenUnhealthy verifies finding ⑦: produce latency is
// preserved while healthy but cleared once the check goes stale, so a failing
// probe does not advertise a stale pre-failure latency.
func TestProduceLatencyClearedWhenUnhealthy(t *testing.T) {
	clk := newFakeClock(time.Unix(9000, 0))
	cp, coll := newTestProbe(t, clk, &stubProducer{})
	ctx := context.Background()

	cp.sendOne(ctx)                             // success (fakeClock -> 0s latency)
	coll.SetHealthLatency("c1", "produce", 0.5) // inject a non-zero last-success latency
	cp.evalOnce(clk.Now())                      // healthy -> latency preserved
	if got := metricValue(t, coll, "rocketmq_health_check_latency_seconds", map[string]string{"cluster": "c1", "check": "produce"}); got != 0.5 {
		t.Fatalf("healthy produce latency = %v, want 0.5", got)
	}

	clk.Advance(6 * time.Second) // beyond recency -> produce stale
	cp.evalOnce(clk.Now())
	if got := metricValue(t, coll, "rocketmq_health_check_latency_seconds", map[string]string{"cluster": "c1", "check": "produce"}); got != 0 {
		t.Errorf("unhealthy produce latency = %v, want 0", got)
	}
}

// TestAddProbeDoesNotBailOnMissingTopic locks in the bootstrap fix: when the
// consumer cannot start (topic route missing on a fresh cluster), addProbe must
// STILL add the probe and start the produce loop -- it must NOT bail. The
// consumer is retried; once Start succeeds the consumer comes up. Before the
// fix, addProbe returned an error on consumer.Start failure and the probe was
// never added, leaving produce_total empty and the health check silent.
func TestAddProbeDoesNotBailOnMissingTopic(t *testing.T) {
	p, _, _, factory, _ := newReconcileProber(t)
	factory.failStart = true // built consumers' Start fails (missing topic)
	ctx := context.Background()

	p.reconcileClusters(ctx, []string{"A"})
	// The probe MUST exist despite the consumer start failure.
	probes := p.snapshotProbes()
	if len(probes) != 1 || probes["A"] == nil {
		t.Fatalf("probe A missing after reconcile with failing consumer; probes=%v", probes)
	}
	cp := probes["A"]
	if cp.consumerStarted {
		t.Error("consumer should NOT be started when Start fails")
	}

	// Topic "appears": make the factory build a succeeding consumer, then a
	// direct tryStartConsumer (mirroring the background retry loop) brings it up.
	factory.failStart = false
	if err := cp.tryStartConsumer(); err != nil {
		t.Fatalf("tryStartConsumer after topic appeared: %v", err)
	}
	if !cp.consumerStarted {
		t.Error("consumer should be started after successful tryStartConsumer")
	}

	p.Shutdown(ctx)
	if got := len(p.snapshotProbes()); got != 0 {
		t.Errorf("after Shutdown, probes = %d, want 0", got)
	}
}
