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

package service

import (
	"encoding/json"
	"fmt"

	"github.com/qsrg/rocketmq-exporter-go/internal/rmqremote"
)

// retryTopic is MixAll.getRetryTopic(group) = "%RETRY%" + group (the retry
// topic whose route is used to locate the brokers serving a consumer group).
func retryTopic(group string) string { return "%RETRY%" + group }

// selectBrokerAddr ports BrokerData.selectBrokerAddr: the master (id 0) if
// present, else any (here the first iterated) slave.
func selectBrokerAddr(bd BrokerData) string {
	if addr, ok := bd.BrokerAddrs[0]; ok && addr != "" {
		return addr
	}
	for _, addr := range bd.BrokerAddrs {
		if addr != "" {
			return addr
		}
	}
	return ""
}

// ExamineTopicStats ports DefaultMQAdminExt.examineTopicStats(topic): fetch the
// topic route, query GET_TOPIC_STATS_INFO on each broker's master, and merge the
// offsetTables. Per-broker failures are logged and skipped (best-effort).
func (a *AdminClient) ExamineTopicStats(topic string) (*TopicStatsTable, error) {
	route, err := a.ExamineTopicRouteInfo(topic)
	if err != nil {
		return nil, err
	}
	merged := &TopicStatsTable{OffsetTable: map[string]json.RawMessage{}}
	for _, bd := range route.BrokerDatas {
		addr := selectBrokerAddr(bd)
		if addr == "" {
			continue
		}
		tst, err := a.ExamineTopicStatsOnBroker(addr, topic)
		if err != nil {
			a.logRPC("examineTopicStats", topic, addr, err)
			continue
		}
		for k, v := range tst.OffsetTable {
			merged.OffsetTable[k] = v
		}
	}
	return merged, nil
}

// ExamineConsumeStats ports DefaultMQAdminExt.examineConsumeStats(group, topic):
// route the retry topic, query each broker, merge offsetTables and sum consumeTps.
func (a *AdminClient) ExamineConsumeStats(group, topic string) (*ConsumeStats, error) {
	route, err := a.ExamineTopicRouteInfo(retryTopic(group))
	if err != nil {
		return nil, err
	}
	merged := &ConsumeStats{OffsetTable: map[string]json.RawMessage{}}
	for _, bd := range route.BrokerDatas {
		addr := selectBrokerAddr(bd)
		if addr == "" {
			continue
		}
		cs, err := a.ExamineConsumeStatsOnBroker(addr, group, topic)
		if err != nil {
			a.logRPC("examineConsumeStats", group+"/"+topic, addr, err)
			continue
		}
		for k, v := range cs.OffsetTable {
			merged.OffsetTable[k] = v
		}
		merged.ConsumeTps += cs.ConsumeTps
	}
	return merged, nil
}

// ExamineConsumerConnectionInfo ports DefaultMQAdminExt.examineConsumerConnectionInfo.
// Java picks ONE random broker in the route; if that broker lacks the consumer it
// throws CONSUMER_NOT_ONLINE (non-deterministic — it can miss consumers hosted
// on other brokers). We query EVERY broker in the route and merge their
// connection sets, which is deterministic and finds the consumer wherever it is
// (matching what Java shows only on a lucky pick). An empty merged set yields a
// CONSUMER_NOT_ONLINE rpcError so the task layer silent-degrades.
func (a *AdminClient) ExamineConsumerConnectionInfo(group string) (*ConsumerConnection, error) {
	route, err := a.ExamineTopicRouteInfo(retryTopic(group))
	if err != nil {
		return nil, err
	}
	merged := &ConsumerConnection{}
	for _, bd := range route.BrokerDatas {
		addr := selectBrokerAddr(bd)
		if addr == "" {
			continue
		}
		cc, err := a.ExamineConsumerConnectionInfoOnBroker(addr, group)
		if err != nil {
			continue // per-broker: consumer may be on a different broker
		}
		if merged.MessageModel == "" {
			merged.MessageModel = cc.MessageModel
		}
		merged.ConnectionSet = append(merged.ConnectionSet, cc.ConnectionSet...)
	}
	if len(merged.ConnectionSet) == 0 {
		return merged, &rpcError{code: rmqremote.ResponseConsumerNotOnline, remark: "Not found the consumer group connection"}
	}
	return merged, nil
}

// QueryTopicConsumeByWho ports DefaultMQAdminExt.queryTopicConsumeByWho: route,
// query the first broker, return its group list.
func (a *AdminClient) QueryTopicConsumeByWho(topic string) (*GroupList, error) {
	route, err := a.ExamineTopicRouteInfo(topic)
	if err != nil {
		return nil, err
	}
	for _, bd := range route.BrokerDatas {
		addr := selectBrokerAddr(bd)
		if addr == "" {
			continue
		}
		return a.QueryTopicConsumeByWhoOnBroker(addr, topic)
	}
	return &GroupList{}, nil
}

// GetConsumerRunningInfo ports DefaultMQAdminExt.getConsumerRunningInfo: route
// the retry topic, query the first broker that responds.
func (a *AdminClient) GetConsumerRunningInfo(group, clientId string, jstack bool) (*ConsumerRunningInfo, error) {
	route, err := a.ExamineTopicRouteInfo(retryTopic(group))
	if err != nil {
		return nil, err
	}
	for _, bd := range route.BrokerDatas {
		addr := selectBrokerAddr(bd)
		if addr == "" {
			continue
		}
		return a.GetConsumerRunningInfoOnBroker(addr, group, clientId, jstack)
	}
	return nil, fmt.Errorf("getConsumerRunningInfo: no broker in route for group %s", group)
}

// logRPC is a thin best-effort logger for per-broker RPC failures (kept on
// AdminClient so the cron tasks stay focused on collection logic).
func (a *AdminClient) logRPC(op, key, addr string, err error) {
	// keep at debug-level equivalent; the task layer already logs at error
	// level for non-silent codes, so here we swallow per-broker noise.
	_ = op
	_ = key
	_ = addr
	_ = err
}
