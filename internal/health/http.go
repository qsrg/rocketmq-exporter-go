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

	"github.com/wcf/rmq-exporter/internal/collector"
)

// HealthzHandler serves the cluster health snapshot as JSON. It returns 200 when
// every cluster is overall-healthy and at least one probe has completed; 503
// when any cluster is unhealthy or no probe has completed yet. It reads the same
// collector store as /metrics, so it triggers no extra probing. The 404 case
// (health-check disabled) is handled by main.go not registering the route.
func HealthzHandler(coll *collector.MetricsCollector) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		snap := coll.HealthDetail()
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		if snap.Overall != "ok" {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(snap)
	})
}
