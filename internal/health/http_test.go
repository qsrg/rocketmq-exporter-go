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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/qsrg/rocketmq-exporter-go/internal/collector"
)

func mkColl() *collector.MetricsCollector { return collector.New(time.Minute) }

func TestHealthzAllHealthy200(t *testing.T) {
	c := mkColl()
	c.AddHealthProduce("A", "success")
	c.AddHealthConsume("A")
	c.SetHealthStatus("A", "produce", 1)
	c.SetHealthStatus("A", "consume", 1)
	c.SetHealthStatus("A", "overall", 1)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	HealthzHandler(c).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	var snap collector.HealthSnapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("unmarshal: %v; body=%s", err, rec.Body.String())
	}
	if snap.Overall != "ok" {
		t.Errorf("overall = %q, want ok", snap.Overall)
	}
	if snap.Clusters["A"].Overall != "ok" {
		t.Errorf("cluster A overall = %q, want ok", snap.Clusters["A"].Overall)
	}
	if snap.Clusters["A"].Produce.Status != "ok" {
		t.Errorf("cluster A produce status = %q, want ok", snap.Clusters["A"].Produce.Status)
	}
}

func TestHealthzAnyUnhealthy503(t *testing.T) {
	c := mkColl()
	c.SetHealthStatus("A", "overall", 1)
	c.SetHealthStatus("B", "produce", 0)
	c.SetHealthStatus("B", "consume", 1)
	c.SetHealthStatus("B", "overall", 0)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	HealthzHandler(c).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
	var snap collector.HealthSnapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("unmarshal: %v; body=%s", err, rec.Body.String())
	}
	if snap.Overall != "fail" {
		t.Errorf("overall = %q, want fail", snap.Overall)
	}
	if snap.Clusters["B"].Overall != "fail" {
		t.Errorf("cluster B overall = %q, want fail", snap.Clusters["B"].Overall)
	}
}

func TestHealthzNoProbes503(t *testing.T) {
	c := mkColl() // no probes yet
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	HealthzHandler(c).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 (no probes)", rec.Code)
	}
	var snap collector.HealthSnapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if snap.Overall != "fail" || len(snap.Clusters) != 0 {
		t.Errorf("snap = %+v, want overall=fail no clusters", snap)
	}
}
