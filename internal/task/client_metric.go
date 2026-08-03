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
	"log/slog"

	"github.com/qsrg/rocketmq-exporter-go/internal/collector"
	"github.com/qsrg/rocketmq-exporter-go/internal/service"
)

// NewClientMetricTask ports ClientMetricTaskRunnable. It fetches each online
// consumer's running info (per clientId, routed via the retry topic) and pushes
// the six client-runtime gauges (failed-msgs/failed-tps/ok-tps/rt/pull-rt/
// pull-tps) for every topic in the running info's status table. Per-connection
// failures are best-effort (logged, then continue) — matching the Java
// catch-continue behavior.
func NewClientMetricTask(
	admin *service.AdminClient,
	coll *collector.MetricsCollector,
	group string,
	cc *service.ConsumerConnection,
) Task {
	return func() {
		if cc == nil || len(cc.ConnectionSet) == 0 {
			return
		}
		for _, conn := range cc.ConnectionSet {
			cri, err := admin.GetConsumerRunningInfo(group, conn.ClientId, false)
			if err != nil {
				slog.Warn("ClientMetricTask: getConsumerRunningInfo ignored",
					"group", group, "clientId", conn.ClientId, "clientAddr", conn.ClientAddr, "err", err)
				continue
			}
			if cri.Jstack != "" {
				slog.Error("group jstack", "group", group, "jstack", cri.Jstack)
			}
			for topic, st := range cri.StatusTable {
				coll.AddConsumerClientFailedMsgCountsMetric(group, topic, conn.ClientAddr, conn.ClientId, st.ConsumeFailedMsgs)
				coll.AddConsumerClientFailedTPSMetric(group, topic, conn.ClientAddr, conn.ClientId, st.ConsumeFailedTPS)
				coll.AddConsumerClientOKTPSMetric(group, topic, conn.ClientAddr, conn.ClientId, st.ConsumeOKTPS)
				coll.AddConsumeRTMetricMetric(group, topic, conn.ClientAddr, conn.ClientId, st.ConsumeRT)
				coll.AddPullRTMetric(group, topic, conn.ClientAddr, conn.ClientId, st.PullRT)
				coll.AddPullTPSMetric(group, topic, conn.ClientAddr, conn.ClientId, st.PullTPS)
			}
		}
	}
}
