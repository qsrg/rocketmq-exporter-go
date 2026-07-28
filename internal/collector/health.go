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
	"sync"

	dto "github.com/prometheus/client_model/go"
)

// healthStore holds the non-TTL state for the cluster-health-check capability
// (Go-only addition; no Java equivalent). Counters accumulate for the process
// lifetime; gauges are overwritten by the prober's 1s evaluation tick. The
// /healthz handler reads the same store via HealthDetail, so the HTTP endpoint
// and /metrics never diverge and the endpoint triggers no extra probing.
type healthStore struct {
	mu sync.RWMutex

	// counters (cumulative; never swept)
	produceCount map[produceHealthKey]int64 // key = {cluster, result=success|failure}
	consumeCount map[string]int64           // key = cluster

	// gauges (latest value; overwritten by the prober eval tick)
	status      map[clusterCheckKey]int     // check = produce|consume|overall; 1=healthy, 0=unhealthy
	latency     map[clusterCheckKey]float64 // check = produce|consume (seconds)
	lastSuccess map[clusterCheckKey]int64   // check = produce|consume|overall (unix seconds)
	lastError   map[clusterCheckKey]string  // check = produce|consume (latest error string)
}

// produceHealthKey identity = {cluster, result}.
type produceHealthKey struct{ cluster, result string }

// clusterCheckKey identity = {cluster, check}. Used for status / latency /
// lastSuccess / lastError; the set of valid `check` values differs per map
// (status/lastSuccess allow "overall", latency/lastError do not).
type clusterCheckKey struct{ cluster, check string }

func newHealthStore() *healthStore {
	return &healthStore{
		produceCount: make(map[produceHealthKey]int64),
		consumeCount: make(map[string]int64),
		status:       make(map[clusterCheckKey]int),
		latency:      make(map[clusterCheckKey]float64),
		lastSuccess:  make(map[clusterCheckKey]int64),
		lastError:    make(map[clusterCheckKey]string),
	}
}

// AddHealthProduce increments the produce counter for (cluster, result).
func (c *MetricsCollector) AddHealthProduce(cluster, result string) {
	c.health.mu.Lock()
	c.health.produceCount[produceHealthKey{cluster, result}]++
	c.health.mu.Unlock()
}

// AddHealthConsume increments the consume counter for cluster.
func (c *MetricsCollector) AddHealthConsume(cluster string) {
	c.health.mu.Lock()
	c.health.consumeCount[cluster]++
	c.health.mu.Unlock()
}

// SetHealthStatus sets the latest status gauge for (cluster, check).
func (c *MetricsCollector) SetHealthStatus(cluster, check string, v int) {
	c.health.mu.Lock()
	c.health.status[clusterCheckKey{cluster, check}] = v
	c.health.mu.Unlock()
}

// SetHealthLatency sets the latest latency gauge (seconds) for (cluster, check).
func (c *MetricsCollector) SetHealthLatency(cluster, check string, secs float64) {
	c.health.mu.Lock()
	c.health.latency[clusterCheckKey{cluster, check}] = secs
	c.health.mu.Unlock()
}

// SetHealthLastSuccess sets the last-success unix timestamp for (cluster, check).
func (c *MetricsCollector) SetHealthLastSuccess(cluster, check string, ts int64) {
	c.health.mu.Lock()
	c.health.lastSuccess[clusterCheckKey{cluster, check}] = ts
	c.health.mu.Unlock()
}

// SetHealthLastError records the latest error string for (cluster, check),
// surfaced only via /healthz (never exported as a Prometheus metric).
func (c *MetricsCollector) SetHealthLastError(cluster, check, errStr string) {
	c.health.mu.Lock()
	c.health.lastError[clusterCheckKey{cluster, check}] = errStr
	c.health.mu.Unlock()
}

// ClearHealthCluster drops all health state for a cluster (called when a cluster
// disappears at refresh time, so no ghost samples linger).
func (c *MetricsCollector) ClearHealthCluster(cluster string) {
	h := c.health
	h.mu.Lock()
	defer h.mu.Unlock()
	for k := range h.produceCount {
		if k.cluster == cluster {
			delete(h.produceCount, k)
		}
	}
	delete(h.consumeCount, cluster)
	for k := range h.status {
		if k.cluster == cluster {
			delete(h.status, k)
		}
	}
	for k := range h.latency {
		if k.cluster == cluster {
			delete(h.latency, k)
		}
	}
	for k := range h.lastSuccess {
		if k.cluster == cluster {
			delete(h.lastSuccess, k)
		}
	}
	for k := range h.lastError {
		if k.cluster == cluster {
			delete(h.lastError, k)
		}
	}
}

// --- label name slices for the health families (byte-identical to design D4) ---

var (
	healthProduceLabels = []string{"cluster", "result"}
	healthConsumeLabels = []string{"cluster"}
	healthCheckLabels   = []string{"cluster", "check"}
)

// counterFamily is the counter analogue of gaugeFamily: identical label wiring
// but MetricType_COUNTER + dto.Counter. Used by the health-check counters so an
// empty family still emits `# TYPE ... counter` (not gauge) via the /metrics
// encoder.
func counterFamily(name, help string, labelNames []string, samples []sample) *dto.MetricFamily {
	metrics := make([]*dto.Metric, 0, len(samples))
	for _, s := range samples {
		labels := make([]*dto.LabelPair, len(s.labelValues))
		for i, lv := range s.labelValues {
			labels[i] = &dto.LabelPair{Name: &labelNames[i], Value: &lv}
		}
		v := s.value
		metrics = append(metrics, &dto.Metric{
			Label:   labels,
			Counter: &dto.Counter{Value: &v},
		})
	}
	t := dto.MetricType_COUNTER
	return &dto.MetricFamily{Name: &name, Help: &help, Type: &t, Metric: metrics}
}

// --- sample extractors (snapshot the maps under the read lock) ---

func (h *healthStore) produceSamples() []sample {
	h.mu.RLock()
	out := make([]sample, 0, len(h.produceCount))
	for k, v := range h.produceCount {
		out = append(out, sample{[]string{k.cluster, k.result}, float64(v)})
	}
	h.mu.RUnlock()
	return out
}

func (h *healthStore) consumeSamples() []sample {
	h.mu.RLock()
	out := make([]sample, 0, len(h.consumeCount))
	for cluster, v := range h.consumeCount {
		out = append(out, sample{[]string{cluster}, float64(v)})
	}
	h.mu.RUnlock()
	return out
}

func (h *healthStore) statusSamples() []sample {
	h.mu.RLock()
	out := make([]sample, 0, len(h.status))
	for k, v := range h.status {
		out = append(out, sample{[]string{k.cluster, k.check}, float64(v)})
	}
	h.mu.RUnlock()
	return out
}

func (h *healthStore) latencySamples() []sample {
	h.mu.RLock()
	out := make([]sample, 0, len(h.latency))
	for k, v := range h.latency {
		out = append(out, sample{[]string{k.cluster, k.check}, v})
	}
	h.mu.RUnlock()
	return out
}

func (h *healthStore) lastSuccessSamples() []sample {
	h.mu.RLock()
	out := make([]sample, 0, len(h.lastSuccess))
	for k, v := range h.lastSuccess {
		out = append(out, sample{[]string{k.cluster, k.check}, float64(v)})
	}
	h.mu.RUnlock()
	return out
}

// --- HealthDetail: the /healthz view (same store, no extra probing) ---

// HealthSnapshot is the JSON-serializable /healthz body.
type HealthSnapshot struct {
	Overall     string                   `json:"overall"` // "ok" | "fail"
	LastProbeAt int64                    `json:"last_probe_at"`
	Clusters    map[string]ClusterHealth `json:"clusters"`
}

// ClusterHealth is one cluster's produce/consume/overall detail.
type ClusterHealth struct {
	Overall string      `json:"overall"` // "ok" | "fail"
	Produce CheckHealth `json:"produce"`
	Consume CheckHealth `json:"consume"`
}

// CheckHealth is one check's latest result.
type CheckHealth struct {
	Status      string  `json:"status"` // "ok" | "fail"
	Latency     float64 `json:"latency"`
	LastSuccess int64   `json:"last_success"`
	LastError   string  `json:"last_error"`
}

// HealthDetail returns the per-cluster snapshot consumed by /healthz.
func (c *MetricsCollector) HealthDetail() HealthSnapshot {
	h := c.health
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Cluster set = union of clusters seen in any health map (a freshly added
	// cluster may have produce/consume counts before its first eval tick).
	clusterSet := make(map[string]struct{})
	for k := range h.produceCount {
		clusterSet[k.cluster] = struct{}{}
	}
	for cluster := range h.consumeCount {
		clusterSet[cluster] = struct{}{}
	}
	for k := range h.status {
		clusterSet[k.cluster] = struct{}{}
	}

	clusters := make(map[string]ClusterHealth, len(clusterSet))
	var lastProbe int64
	for cluster := range clusterSet {
		produce := h.checkDetail(cluster, "produce")
		consume := h.checkDetail(cluster, "consume")
		overallOK := h.status[clusterCheckKey{cluster, "overall"}] == 1
		clusters[cluster] = ClusterHealth{
			Overall: okFail(overallOK),
			Produce: produce,
			Consume: consume,
		}
		if produce.LastSuccess > lastProbe {
			lastProbe = produce.LastSuccess
		}
		if consume.LastSuccess > lastProbe {
			lastProbe = consume.LastSuccess
		}
	}

	overall := "ok"
	if len(clusters) == 0 {
		overall = "fail" // no probe has completed yet
	} else {
		for _, ch := range clusters {
			if ch.Overall != "ok" {
				overall = "fail"
				break
			}
		}
	}
	return HealthSnapshot{Overall: overall, LastProbeAt: lastProbe, Clusters: clusters}
}

// checkDetail assembles one check's /healthz view from the gauge maps. Caller
// holds h.mu.
func (h *healthStore) checkDetail(cluster, check string) CheckHealth {
	return CheckHealth{
		Status:      okFail(h.status[clusterCheckKey{cluster, check}] == 1),
		Latency:     h.latency[clusterCheckKey{cluster, check}],
		LastSuccess: h.lastSuccess[clusterCheckKey{cluster, check}],
		LastError:   h.lastError[clusterCheckKey{cluster, check}],
	}
}

func okFail(ok bool) string {
	if ok {
		return "ok"
	}
	return "fail"
}
