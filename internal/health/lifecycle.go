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
	"fmt"
	"time"
)

// discoverClusters lists cluster names from the broker topology.
func (p *Prober) discoverClusters(ctx context.Context) ([]string, error) {
	return p.lister.ListClusters(ctx)
}

// reconcileClusters adds probes for newly discovered clusters and removes probes
// for clusters that have disappeared. A discovery that returns a subset is
// honored as-is (the caller must not pass a partial list on error - see Start,
// which skips reconcile entirely when discovery fails, so probes are never wiped
// by a transient namesrv failure).
func (p *Prober) reconcileClusters(ctx context.Context, discovered []string) {
	if len(discovered) == 0 {
		// An empty discovery is almost always a transient namesrv state
		// (single-broker cluster mid-restart, namesrv failover), not a real
		// "all clusters gone" signal. Honoring it would tear down every probe
		// and reset cumulative counters via ClearHealthCluster on a flap,
		// breaking Prometheus rate calculations. Skip; the next refresh
		// retries. Genuinely-vanished clusters surface as unhealthy via recency
		// instead. (Discovery *errors* are already skipped upstream by
		// Start/refreshLoop; this guards the empty-success case they miss.)
		p.log.Warn("health cluster discovery returned empty; probes unchanged")
		return
	}
	discoveredSet := make(map[string]struct{}, len(discovered))
	for _, c := range discovered {
		discoveredSet[c] = struct{}{}
	}

	p.mu.Lock()
	existing := make([]string, 0, len(p.probes))
	for name := range p.probes {
		existing = append(existing, name)
	}
	p.mu.Unlock()

	// Add new clusters (best-effort; a failure is logged, not fatal).
	for c := range discoveredSet {
		p.mu.Lock()
		_, exists := p.probes[c]
		p.mu.Unlock()
		if exists {
			continue
		}
		if err := p.addProbe(ctx, c); err != nil {
			p.log.Warn("health probe start failed; will retry next refresh", "cluster", c, "err", err)
		}
	}
	// Remove vanished clusters.
	for _, c := range existing {
		if _, ok := discoveredSet[c]; !ok {
			p.removeProbe(c)
		}
	}
}

// Start launches the shared producer, performs initial cluster discovery, and
// runs the 1s evaluation tick plus the cluster-refresh loop. A failed initial
// discovery is logged but not fatal (the exporter stays up; the six cron tasks
// are unaffected); discovery is retried each cluster-refresh interval.
func (p *Prober) Start(ctx context.Context) error {
	p.rootCtx, p.rootCancel = context.WithCancel(ctx)
	if err := p.prod.Start(); err != nil {
		return fmt.Errorf("health producer start: %w", err)
	}
	if clusters, err := p.discoverClusters(p.rootCtx); err != nil {
		p.log.Warn("health cluster discovery failed at start; retrying on refresh", "err", err)
	} else {
		p.reconcileClusters(p.rootCtx, clusters)
	}

	p.wg.Add(2)
	go p.evalLoop()
	go p.refreshLoop()
	return nil
}

// evalLoop runs the 1s recency evaluation until rootCtx is canceled.
func (p *Prober) evalLoop() {
	defer p.wg.Done()
	ticker := p.clk.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.rootCtx.Done():
			return
		case <-ticker.Chan():
			p.evalTick()
		}
	}
}

// refreshLoop re-discovers clusters every cfg.ClusterRefresh until rootCtx is
// canceled. Discovery failures are logged and skipped (probes untouched).
func (p *Prober) refreshLoop() {
	defer p.wg.Done()
	ticker := p.clk.NewTicker(p.cfg.ClusterRefresh)
	defer ticker.Stop()
	for {
		select {
		case <-p.rootCtx.Done():
			return
		case <-ticker.Chan():
			clusters, err := p.discoverClusters(p.rootCtx)
			if err != nil {
				p.log.Warn("health cluster discovery failed; probes unchanged", "err", err)
				continue
			}
			p.reconcileClusters(p.rootCtx, clusters)
		}
	}
}

// Shutdown stops all loops, removes every cluster probe (stopping produce
// goroutines and consumers), and shuts down the shared producer.
func (p *Prober) Shutdown(_ context.Context) {
	if p.rootCancel != nil {
		p.rootCancel()
	}
	p.wg.Wait() // evalLoop + refreshLoop have exited

	p.mu.Lock()
	names := make([]string, 0, len(p.probes))
	for n := range p.probes {
		names = append(names, n)
	}
	p.mu.Unlock()
	for _, n := range names {
		p.removeProbe(n)
	}

	if err := p.prod.Shutdown(); err != nil {
		p.log.Warn("health producer shutdown", "err", err)
	}
}
