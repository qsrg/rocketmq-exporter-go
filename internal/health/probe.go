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

// Package health implements the active end-to-end cluster health probe
// (cluster-health-check capability): a long-lived producer sends timestamped
// test messages to a per-cluster topic at a configurable rate; a per-cluster
// push consumer consumes them back. Status is derived from recency, not from a
// per-message timeout table. This is a Go-only addition with no Java equivalent;
// see openspec/changes/cluster-health-check/.
package health

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"

	"github.com/wcf/rmq-exporter/internal/collector"
	"github.com/wcf/rmq-exporter/internal/config"
)

// Clock abstracts time so the prober's recency evaluation is deterministic in
// tests. Production uses realClock; tests use a controllable fake.
type Clock interface {
	Now() time.Time
	NewTicker(d time.Duration) Ticker
}

// Ticker is the clock-driven trigger used by the prober loops.
type Ticker interface {
	Chan() <-chan time.Time
	Stop()
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTicker(d time.Duration) Ticker {
	return realTicker{t: time.NewTicker(d)}
}

type realTicker struct{ t *time.Ticker }

func (r realTicker) Chan() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()                  { r.t.Stop() }

// Producer is the rocketmq-client-go producer subset used by the prober. The
// library's *defaultProducer satisfies it structurally.
type Producer interface {
	Start() error
	Shutdown() error
	SendSync(ctx context.Context, mq ...*primitive.Message) (*primitive.SendResult, error)
}

// MessageHandler is the push-consumer callback signature. It is a type alias
// (not a named type) so the library's *pushConsumer satisfies the Consumer
// interface below without an adapter.
type MessageHandler = func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error)

// Consumer is the rocketmq-client-go push-consumer subset used by the prober.
type Consumer interface {
	Subscribe(topic string, selector consumer.MessageSelector, f MessageHandler) error
	Start() error
	Shutdown() error
}

// ConsumerFactory builds a per-cluster Consumer bound to a consumer group. The
// prober wires the topic + message handler via Subscribe after construction.
// The real factory (adapter.go) builds a rocketmq-client-go push consumer; tests
// inject a stub.
type ConsumerFactory func(group string) (Consumer, error)

// ClusterLister lists RocketMQ cluster names. *service.AdminClient satisfies it
// via a thin adapter (adapter.go), keeping this package decoupled from service.
type ClusterLister interface {
	ListClusters(ctx context.Context) ([]string, error)
}

// clusterProbe is the per-cluster probe state: one produce goroutine + one
// consumer instance. Local recency timestamps drive status evaluation; the
// collector mirrors them as Prometheus metrics.
type clusterProbe struct {
	cluster string
	topic   string
	group   string
	cfg     config.HealthCheckConfig
	coll    *collector.MetricsCollector
	prod    Producer
	clk     Clock
	log     *slog.Logger

	consumerFactory ConsumerFactory // builds a fresh consumer per (re)start attempt
	consumer        Consumer
	consumerStarted bool // under mu; true once Subscribe+Start succeeded

	// consumerRetryDone is closed when the background consumer-(re)start loop
	// exits (consumer started, or ctx canceled). removeProbe waits on it so it
	// never shuts down a consumer while tryStartConsumer is mid-Start.
	consumerRetryDone chan struct{}

	// recency state (protected by mu); zero Time means "never succeeded".
	mu                 sync.Mutex
	lastProduceSuccess time.Time
	lastConsume        time.Time

	cancel context.CancelFunc // stops produceLoop + consumerRetryLoop
	done   chan struct{}      // closed when produceLoop exits
}

// consumerRetryInterval is how often the background loop retries consumer
// Start when the topic route is not available yet (fresh cluster, broker
// mid-restart). Short enough to come up within recency once the topic appears;
// long enough that a permanently-missing topic doesn't build a consumer every
// tick.
const consumerRetryInterval = 5 * time.Second

// sendOne performs a single produce attempt and records the result.
func (p *clusterProbe) sendOne(ctx context.Context) {
	now := p.clk.Now()
	body := []byte(strconv.FormatInt(now.UnixMilli(), 10))
	msg := primitive.NewMessage(p.topic, body)
	start := p.clk.Now()
	_, err := p.prod.SendSync(ctx, msg)
	elapsed := p.clk.Now().Sub(start)
	if err != nil {
		p.coll.AddHealthProduce(p.cluster, "failure")
		p.coll.SetHealthLastError(p.cluster, "produce", err.Error())
		p.log.Warn("health check produce failed",
			"cluster", p.cluster, "topic", p.topic, "err", err)
		return
	}
	p.coll.AddHealthProduce(p.cluster, "success")
	p.coll.SetHealthLatency(p.cluster, "produce", elapsed.Seconds())
	p.coll.SetHealthLastSuccess(p.cluster, "produce", now.Unix())
	p.coll.SetHealthLastError(p.cluster, "produce", "") // clear last error on recovery
	p.mu.Lock()
	p.lastProduceSuccess = now
	p.mu.Unlock()
}

// handleMessage is the push-consumer callback: every message is acked as
// success; the body timestamp yields end-to-end latency.
func (p *clusterProbe) handleMessage(_ context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	now := p.clk.Now()
	for _, m := range msgs {
		p.coll.AddHealthConsume(p.cluster)
		if bt := parseBodyTs(m.Body); !bt.IsZero() {
			p.coll.SetHealthLatency(p.cluster, "consume", now.Sub(bt).Seconds())
		}
		p.coll.SetHealthLastSuccess(p.cluster, "consume", now.Unix())
	}
	p.mu.Lock()
	p.lastConsume = now
	p.mu.Unlock()
	return consumer.ConsumeSuccess, nil
}

// parseBodyTs parses the body (a UnixMilli timestamp string) back to a time.
func parseBodyTs(body []byte) time.Time {
	ms, err := strconv.ParseInt(string(body), 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

// evalOnce computes this cluster's status from recency and writes it to the
// collector. Called every second by Prober.evalTick.
func (p *clusterProbe) evalOnce(now time.Time) {
	p.mu.Lock()
	produceOK := !p.lastProduceSuccess.IsZero() && now.Sub(p.lastProduceSuccess) < p.cfg.Recency
	consumeOK := !p.lastConsume.IsZero() && now.Sub(p.lastConsume) < p.cfg.Recency
	overallOK := produceOK && consumeOK
	p.mu.Unlock()

	p.coll.SetHealthStatus(p.cluster, "produce", boolToInt(produceOK))
	p.coll.SetHealthStatus(p.cluster, "consume", boolToInt(consumeOK))
	p.coll.SetHealthStatus(p.cluster, "overall", boolToInt(overallOK))

	// last_error reflects the current failure cause and is cleared on recovery.
	// produce's error is the real RPC error written by sendOne (cleared on the
	// next success). consume has no error callback, so its cause is derived from
	// the recency timeout.
	if consumeOK {
		p.coll.SetHealthLastError(p.cluster, "consume", "")
	} else {
		p.coll.SetHealthLastError(p.cluster, "consume", "no message consumed within recency")
	}

	// Latency is meaningful only while the check is healthy; clear it when
	// unhealthy so a stale pre-failure value does not mislead /healthz or the
	// latency gauge.
	if !produceOK {
		p.coll.SetHealthLatency(p.cluster, "produce", 0)
	}
	if !consumeOK {
		p.coll.SetHealthLatency(p.cluster, "consume", 0)
	}

	if overallOK {
		p.coll.SetHealthLastSuccess(p.cluster, "overall", now.Unix())
	}
}

// produceLoop sends test messages at cfg.Rate until ctx is canceled.
func (p *clusterProbe) produceLoop(ctx context.Context) {
	defer close(p.done)
	interval := time.Duration(float64(time.Second) / p.cfg.Rate)
	if interval <= 0 {
		interval = time.Second
	}
	ticker := p.clk.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.Chan():
			p.sendOne(ctx)
		}
	}
}

// tryStartConsumer builds, subscribes, and starts the push consumer for this
// probe's topic. It is idempotent: once started it returns nil. On failure the
// half-built consumer is shut down and the error returned; the caller (the
// background retry loop) retries until the topic exists -- the producer's sends
// auto-create it on brokers with autoCreateTopicEnable, or the operator
// pre-creates it. This keeps the produce loop running (emitting
// produce_total{failure}) even while the consumer cannot start, matching the
// design's "natural failure + self-heal" intent instead of bailing the probe.
func (p *clusterProbe) tryStartConsumer() error {
	p.mu.Lock()
	if p.consumerStarted || p.consumerFactory == nil {
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()

	cons, err := p.consumerFactory(p.group)
	if err != nil {
		return fmt.Errorf("health consumer factory for %s: %w", p.cluster, err)
	}
	if err := cons.Subscribe(p.topic, consumer.MessageSelector{}, p.handleMessage); err != nil {
		_ = cons.Shutdown()
		return fmt.Errorf("health subscribe %s for %s: %w", p.topic, p.cluster, err)
	}
	if err := cons.Start(); err != nil {
		_ = cons.Shutdown()
		return fmt.Errorf("health consumer start for %s: %w", p.cluster, err)
	}
	p.mu.Lock()
	p.consumer = cons
	p.consumerStarted = true
	p.mu.Unlock()
	return nil
}

// consumerRetryLoop retries tryStartConsumer every consumerRetryInterval until
// the consumer is up or ctx is canceled. It exits immediately if the consumer
// already started (initial addProbe attempt succeeded). removeProbe waits on
// consumerRetryDone so it never races a mid-flight Start.
func (p *clusterProbe) consumerRetryLoop(ctx context.Context) {
	defer close(p.consumerRetryDone)
	p.mu.Lock()
	started := p.consumerStarted
	p.mu.Unlock()
	if started {
		return
	}
	ticker := p.clk.NewTicker(consumerRetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.Chan():
			if err := p.tryStartConsumer(); err == nil {
				return // started
			}
		}
	}
}

// Prober drives the per-cluster probes: a shared producer, a map of
// clusterProbes, a 1s evaluation tick, and a cluster-refresh loop.
type Prober struct {
	cfg             config.HealthCheckConfig
	lister          ClusterLister
	coll            *collector.MetricsCollector
	prod            Producer
	consumerFactory ConsumerFactory
	clk             Clock
	log             *slog.Logger

	mu     sync.Mutex
	probes map[string]*clusterProbe

	// evalMu serializes the 1s evalOnce pass (evalTick) against removeProbe's
	// ClearHealthCluster, so a probe removed mid-pass cannot have its collector
	// samples written back by a stale snapshot entry and left as ghost metrics.
	evalMu sync.Mutex

	rootCtx    context.Context
	rootCancel context.CancelFunc
	wg         sync.WaitGroup
}

// NewProber assembles a Prober. clk may be nil (defaults to realClock).
func NewProber(cfg config.HealthCheckConfig, lister ClusterLister, coll *collector.MetricsCollector,
	prod Producer, factory ConsumerFactory, clk Clock) *Prober {
	if clk == nil {
		clk = realClock{}
	}
	return &Prober{
		cfg:             cfg,
		lister:          lister,
		coll:            coll,
		prod:            prod,
		consumerFactory: factory,
		clk:             clk,
		log:             slog.Default(),
		probes:          make(map[string]*clusterProbe),
	}
}

// evalTick evaluates every cluster's recency status once.
func (p *Prober) evalTick() {
	now := p.clk.Now()
	// Hold evalMu across the evalOnce pass so it cannot overlap removeProbe's
	// ClearHealthCluster. Without this, a probe removed after the snapshot but
	// before its evalOnce would have its status/latency/lastSuccess/lastError
	// samples written back by evalOnce and left as ghost metrics (the probe is
	// then gone from the map, so no later tick ever clears them).
	p.evalMu.Lock()
	defer p.evalMu.Unlock()
	p.mu.Lock()
	probes := make([]*clusterProbe, 0, len(p.probes))
	for _, cp := range p.probes {
		probes = append(probes, cp)
	}
	p.mu.Unlock()
	for _, cp := range probes {
		cp.evalOnce(now)
	}
}

// addProbe creates and starts a clusterProbe. The produce loop starts FIRST so
// produce metrics flow immediately (and the producer's sends auto-create the
// topic on brokers with autoCreateTopicEnable). The consumer is then started
// best-effort; if the topic route is not available yet, a background loop
// retries until it is. addProbe never fails: a missing topic degrades to
// produce_total{failure} + status 0 (via recency) rather than a silent no-probe,
// matching the design's "natural failure + self-heal" intent.
func (p *Prober) addProbe(ctx context.Context, cluster string) error {
	topic := p.cfg.TopicPrefix + cluster
	group := p.cfg.GroupPrefix + cluster
	cp := &clusterProbe{
		cluster:           cluster,
		topic:             topic,
		group:             group,
		cfg:               p.cfg,
		coll:              p.coll,
		prod:              p.prod,
		clk:               p.clk,
		log:               p.log,
		consumerFactory:   p.consumerFactory,
		done:              make(chan struct{}),
		consumerRetryDone: make(chan struct{}),
	}
	cpctx, cancel := context.WithCancel(ctx)
	cp.cancel = cancel
	p.mu.Lock()
	p.probes[cluster] = cp
	p.mu.Unlock()
	// Produce first: sends auto-create the topic (broker autoCreateTopicEnable)
	// and produce_total counts flow even if the consumer cannot subscribe yet.
	go cp.produceLoop(cpctx)
	if err := cp.tryStartConsumer(); err != nil {
		p.log.Warn("health consumer start deferred; retrying in background",
			"cluster", cluster, "topic", topic, "err", err)
	}
	go cp.consumerRetryLoop(cpctx)
	p.log.Info("health probe started", "cluster", cluster, "topic", topic, "group", group)
	return nil
}

// removeProbe stops a clusterProbe: cancel its loops, wait for produceLoop and
// the consumer retry loop to exit, shut down its consumer (if it ever started),
// and clear the cluster's health samples.
func (p *Prober) removeProbe(cluster string) {
	p.mu.Lock()
	cp, ok := p.probes[cluster]
	if !ok {
		p.mu.Unlock()
		return
	}
	delete(p.probes, cluster)
	p.mu.Unlock()
	if cp.cancel != nil {
		cp.cancel()
	}
	<-cp.done              // produceLoop has exited
	<-cp.consumerRetryDone // consumer retry loop has exited (no mid-Start race)
	cp.mu.Lock()
	cons := cp.consumer
	cp.mu.Unlock()
	if cons != nil {
		if err := cons.Shutdown(); err != nil {
			p.log.Warn("health consumer shutdown", "cluster", cluster, "err", err)
		}
	}
	// Serialize the clear against the evalOnce pass (evalTick) so evalTick
	// cannot re-create this cluster's samples after the clear.
	p.evalMu.Lock()
	p.coll.ClearHealthCluster(cluster)
	p.evalMu.Unlock()
	p.log.Info("health probe removed", "cluster", cluster)
}

// snapshotProbes returns the current cluster->probe map (for tests/inspection).
func (p *Prober) snapshotProbes() map[string]*clusterProbe {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]*clusterProbe, len(p.probes))
	for k, v := range p.probes {
		out[k] = v
	}
	return out
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
