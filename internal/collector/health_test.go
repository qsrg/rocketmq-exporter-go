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
	"encoding/json"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

func findFamily(fs []*dto.MetricFamily, name string) *dto.MetricFamily {
	for _, f := range fs {
		if f.GetName() == name {
			return f
		}
	}
	return nil
}

// findMetric returns the metric whose labels match exactly, or nil.
func findMetric(f *dto.MetricFamily, labels map[string]string) *dto.Metric {
	if f == nil {
		return nil
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
		if match {
			return m
		}
	}
	return nil
}

func metricValue(m *dto.Metric) float64 {
	if m == nil {
		return 0
	}
	if m.Gauge != nil {
		return m.Gauge.GetValue()
	}
	if m.Counter != nil {
		return m.Counter.GetValue()
	}
	return 0
}

func TestHealthProduceCounterFamily(t *testing.T) {
	c := New(time.Minute)
	c.AddHealthProduce("c1", "success")
	c.AddHealthProduce("c1", "success")
	c.AddHealthProduce("c1", "success")
	c.AddHealthProduce("c1", "failure")
	c.AddHealthProduce("c2", "success")

	families, err := c.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	pf := findFamily(families, "rocketmq_health_check_produce_total")
	if pf == nil {
		t.Fatal("missing produce_total family")
	}
	if pf.GetType() != dto.MetricType_COUNTER {
		t.Errorf("produce_total type = %v, want COUNTER", pf.GetType())
	}
	if pf.GetHelp() != "RocketMQ health check produce result count" {
		t.Errorf("produce_total HELP = %q", pf.GetHelp())
	}
	cases := []struct {
		labels map[string]string
		want   float64
	}{
		{map[string]string{"cluster": "c1", "result": "success"}, 3},
		{map[string]string{"cluster": "c1", "result": "failure"}, 1},
		{map[string]string{"cluster": "c2", "result": "success"}, 1},
	}
	for _, tc := range cases {
		m := findMetric(pf, tc.labels)
		if m == nil {
			t.Errorf("missing sample %v", tc.labels)
			continue
		}
		if got := metricValue(m); got != tc.want {
			t.Errorf("sample %v = %v, want %v", tc.labels, got, tc.want)
		}
	}
	// label order = cluster, result
	if m := findMetric(pf, map[string]string{"cluster": "c1", "result": "success"}); m != nil {
		got := labelNames(m)
		want := []string{"cluster", "result"}
		if !equalSlices(got, want) {
			t.Errorf("produce_total label order = %v, want %v", got, want)
		}
	}
}

func TestHealthConsumeCounterFamily(t *testing.T) {
	c := New(time.Minute)
	c.AddHealthConsume("c1")
	c.AddHealthConsume("c1")
	c.AddHealthConsume("c2")
	families, _ := c.Gather()
	cf := findFamily(families, "rocketmq_health_check_consume_total")
	if cf == nil {
		t.Fatal("missing consume_total family")
	}
	if cf.GetType() != dto.MetricType_COUNTER {
		t.Errorf("consume_total type = %v, want COUNTER", cf.GetType())
	}
	if got := metricValue(findMetric(cf, map[string]string{"cluster": "c1"})); got != 2 {
		t.Errorf("consume_total{c1} = %v, want 2", got)
	}
	if got := metricValue(findMetric(cf, map[string]string{"cluster": "c2"})); got != 1 {
		t.Errorf("consume_total{c2} = %v, want 1", got)
	}
}

func TestHealthGaugeFamilies(t *testing.T) {
	c := New(time.Minute)
	c.SetHealthStatus("c1", "produce", 1)
	c.SetHealthStatus("c1", "consume", 0)
	c.SetHealthStatus("c1", "overall", 1)
	c.SetHealthLatency("c1", "produce", 0.05)
	c.SetHealthLatency("c1", "consume", 0.0)
	c.SetHealthLastSuccess("c1", "produce", 1700000000)
	c.SetHealthLastSuccess("c1", "overall", 1700000005)
	c.SetHealthLastError("c1", "consume", "no messages")

	families, _ := c.Gather()

	// status gauge
	sf := findFamily(families, "rocketmq_health_check_status")
	if sf == nil || sf.GetType() != dto.MetricType_GAUGE {
		t.Fatalf("status family missing or not gauge: %v", sf)
	}
	if got := metricValue(findMetric(sf, map[string]string{"cluster": "c1", "check": "produce"})); got != 1 {
		t.Errorf("status{c1,produce} = %v, want 1", got)
	}
	if got := metricValue(findMetric(sf, map[string]string{"cluster": "c1", "check": "overall"})); got != 1 {
		t.Errorf("status{c1,overall} = %v, want 1", got)
	}

	// latency gauge (no overall sample expected)
	lf := findFamily(families, "rocketmq_health_check_latency_seconds")
	if lf == nil || lf.GetType() != dto.MetricType_GAUGE {
		t.Fatalf("latency family missing or not gauge: %v", lf)
	}
	if got := metricValue(findMetric(lf, map[string]string{"cluster": "c1", "check": "produce"})); got != 0.05 {
		t.Errorf("latency{c1,produce} = %v, want 0.05", got)
	}
	if findMetric(lf, map[string]string{"cluster": "c1", "check": "overall"}) != nil {
		t.Error("latency family should not have an overall sample")
	}

	// last_success gauge
	lsf := findFamily(families, "rocketmq_health_check_last_success_timestamp_seconds")
	if lsf == nil || lsf.GetType() != dto.MetricType_GAUGE {
		t.Fatalf("last_success family missing or not gauge: %v", lsf)
	}
	if got := metricValue(findMetric(lsf, map[string]string{"cluster": "c1", "check": "overall"})); got != 1700000005 {
		t.Errorf("last_success{c1,overall} = %v, want 1700000005", got)
	}
}

func TestClearHealthCluster(t *testing.T) {
	c := New(time.Minute)
	c.AddHealthProduce("c1", "success")
	c.AddHealthProduce("c2", "success")
	c.SetHealthStatus("c1", "overall", 1)
	c.SetHealthStatus("c2", "overall", 1)
	c.SetHealthLatency("c1", "produce", 0.1)
	c.SetHealthLastError("c1", "produce", "err")

	c.ClearHealthCluster("c1")

	families, _ := c.Gather()
	pf := findFamily(families, "rocketmq_health_check_produce_total")
	if findMetric(pf, map[string]string{"cluster": "c1", "result": "success"}) != nil {
		t.Error("c1 produce sample should be cleared")
	}
	if findMetric(pf, map[string]string{"cluster": "c2", "result": "success"}) == nil {
		t.Error("c2 produce sample should remain")
	}
	sf := findFamily(families, "rocketmq_health_check_status")
	if findMetric(sf, map[string]string{"cluster": "c1", "check": "overall"}) != nil {
		t.Error("c1 status sample should be cleared")
	}
	if findMetric(sf, map[string]string{"cluster": "c2", "check": "overall"}) == nil {
		t.Error("c2 status sample should remain")
	}
	lf := findFamily(families, "rocketmq_health_check_latency_seconds")
	if findMetric(lf, map[string]string{"cluster": "c1", "check": "produce"}) != nil {
		t.Error("c1 latency sample should be cleared")
	}
}

func TestHealthDetail(t *testing.T) {
	c := New(time.Minute)
	// empty: no probes yet -> overall fail, no clusters
	snap := c.HealthDetail()
	if snap.Overall != "fail" {
		t.Errorf("empty Overall = %q, want fail", snap.Overall)
	}
	if len(snap.Clusters) != 0 {
		t.Errorf("empty Clusters = %d, want 0", len(snap.Clusters))
	}

	// healthy cluster
	c.AddHealthProduce("c1", "success")
	c.AddHealthConsume("c1")
	c.SetHealthStatus("c1", "produce", 1)
	c.SetHealthStatus("c1", "consume", 1)
	c.SetHealthStatus("c1", "overall", 1)
	c.SetHealthLatency("c1", "produce", 0.1)
	c.SetHealthLatency("c1", "consume", 0.2)
	c.SetHealthLastSuccess("c1", "produce", 100)
	c.SetHealthLastSuccess("c1", "consume", 200)
	c.SetHealthLastError("c1", "consume", "")

	snap = c.HealthDetail()
	if snap.Overall != "ok" {
		t.Errorf("Overall = %q, want ok", snap.Overall)
	}
	ch, ok := snap.Clusters["c1"]
	if !ok {
		t.Fatal("missing cluster c1")
	}
	if ch.Overall != "ok" {
		t.Errorf("c1 Overall = %q, want ok", ch.Overall)
	}
	if ch.Produce.Status != "ok" || ch.Produce.Latency != 0.1 || ch.Produce.LastSuccess != 100 {
		t.Errorf("c1 Produce = %+v", ch.Produce)
	}
	if ch.Consume.Status != "ok" || ch.Consume.Latency != 0.2 || ch.Consume.LastSuccess != 200 {
		t.Errorf("c1 Consume = %+v", ch.Consume)
	}
	if snap.LastProbeAt != 200 {
		t.Errorf("LastProbeAt = %d, want 200", snap.LastProbeAt)
	}

	// one cluster unhealthy -> overall fail
	c.SetHealthStatus("c1", "overall", 0)
	c.SetHealthStatus("c1", "consume", 0)
	snap = c.HealthDetail()
	if snap.Overall != "fail" {
		t.Errorf("Overall = %q, want fail", snap.Overall)
	}
	if snap.Clusters["c1"].Overall != "fail" {
		t.Errorf("c1 Overall = %q, want fail", snap.Clusters["c1"].Overall)
	}

	// JSON serializable (the /healthz body)
	if _, err := json.Marshal(snap); err != nil {
		t.Errorf("HealthDetail not JSON-serializable: %v", err)
	}
}
