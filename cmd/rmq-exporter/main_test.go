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

package main

import (
	"bytes"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
)

func ptr[T any](v T) *T { return &v }

func TestEncodeMetricFamiliesEmptyTypeLines(t *testing.T) {
	// An empty counter family (e.g. rocketmq_health_check_produce_total before
	// any probe) MUST emit `# TYPE ... counter`, not the old hardcoded "gauge".
	counterName := "rocketmq_health_check_produce_total"
	ct := dto.MetricType_COUNTER
	counter := &dto.MetricFamily{Name: &counterName, Help: ptr("produce count"), Type: &ct}

	gaugeName := "rocketmq_group_consume_total_offset"
	gt := dto.MetricType_GAUGE
	gauge := &dto.MetricFamily{Name: &gaugeName, Help: ptr("empty gauge"), Type: &gt}

	// A non-empty gauge family must still encode its samples normally.
	filledName := "rocketmq_group_diff"
	v := 9.0
	lblName, lblVal := "group", "g"
	filled := &dto.MetricFamily{
		Name: &filledName, Help: ptr("GroupDiff"), Type: &gt,
		Metric: []*dto.Metric{{Label: []*dto.LabelPair{{Name: &lblName, Value: &lblVal}}, Gauge: &dto.Gauge{Value: &v}}},
	}

	var buf bytes.Buffer
	encodeMetricFamilies(&buf, []*dto.MetricFamily{counter, gauge, filled})
	out := buf.String()

	if !strings.Contains(out, "# TYPE rocketmq_health_check_produce_total counter\n") {
		t.Errorf("empty counter family TYPE line wrong or missing;\n got:\n%s", out)
	}
	if !strings.Contains(out, "# HELP rocketmq_health_check_produce_total produce count\n") {
		t.Errorf("empty counter family HELP line missing;\n got:\n%s", out)
	}
	if !strings.Contains(out, "# TYPE rocketmq_group_consume_total_offset gauge\n") {
		t.Errorf("empty gauge family TYPE line wrong or missing;\n got:\n%s", out)
	}
	if !strings.Contains(out, `rocketmq_group_diff{group="g"} 9`) {
		t.Errorf("non-empty family sample missing;\n got:\n%s", out)
	}
}
