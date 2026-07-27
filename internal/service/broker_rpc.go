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

	"github.com/wcf/rmq-exporter/internal/rmqremote"
)

// --- shared request headers (ext fields) ---

type topicHeaderH struct{ topic string }

func (h topicHeaderH) Encode() map[string]string { return map[string]string{"topic": h.topic} }

type groupHeader struct{ consumerGroup string }

func (h groupHeader) Encode() map[string]string { return map[string]string{"consumerGroup": h.consumerGroup} }

type groupTopicHeader struct{ consumerGroup, topic string }

func (h groupTopicHeader) Encode() map[string]string {
	return map[string]string{"consumerGroup": h.consumerGroup, "topic": h.topic}
}

type statsDataHeader struct{ statsName, statsKey string }

func (h statsDataHeader) Encode() map[string]string {
	return map[string]string{"statsName": h.statsName, "statsKey": h.statsKey}
}

type consumerRunningHeader struct{ consumerGroup, clientId string; jstackEnable bool }

func (h consumerRunningHeader) Encode() map[string]string {
	return map[string]string{
		"consumerGroup": h.consumerGroup,
		"clientId":      h.clientId,
		"jstackEnable":  boolToStr(h.jstackEnable),
	}
}

func boolToStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// --- body types ---

// MessageQueue mirrors org.apache.rocketmq.common.message.MessageQueue.
type MessageQueue struct {
	Topic      string `json:"topic"`
	BrokerName string `json:"brokerName"`
	QueueId    int    `json:"queueId"`
}

// TopicOffset mirrors org.apache.rocketmq.common.admin.TopicOffset.
type TopicOffset struct {
	MinOffset          int64 `json:"minOffset"`
	MaxOffset          int64 `json:"maxOffset"`
	LastUpdateTimestamp int64 `json:"lastUpdateTimestamp"`
}

// OffsetWrapper mirrors org.apache.rocketmq.common.admin.OffsetWrapper.
type OffsetWrapper struct {
	BrokerOffset  int64 `json:"brokerOffset"`
	ConsumerOffset int64 `json:"consumerOffset"`
	LastTimestamp  int64 `json:"lastTimestamp"`
}

// GroupList mirrors org.apache.rocketmq.common.protocol.body.GroupList.
type GroupList struct {
	GroupList []string `json:"groupList"`
}

// Connection mirrors org.apache.rocketmq.common.protocol.body.Connection.
type Connection struct {
	ClientAddr string `json:"clientAddr"`
	ClientId   string `json:"clientId"`
	Language   string `json:"language"`
	Version    int    `json:"version"`
}

// ConsumerConnection mirrors org.apache.rocketmq.common.protocol.body.ConsumerConnection.
type ConsumerConnection struct {
	ConnectionSet []Connection `json:"connectionSet"`
	MessageModel  string       `json:"messageModel"`
}

// BrokerStatsItem mirrors org.apache.rocketmq.common.protocol.body.StatsItem
// (the per-window tps/avgTime/sum triple inside BrokerStatsData).
type BrokerStatsItem struct {
	Tps     float64 `json:"tps"`
	AvgTime float64 `json:"avgTime"`
	Sum     float64 `json:"sum"`
}

// BrokerStatsData mirrors org.apache.rocketmq.common.protocol.body.BrokerStatsData.
// The exporter reads StatsMinute.Tps and StatsMinute.Sum.
type BrokerStatsData struct {
	StatsMinute *BrokerStatsItem `json:"statsMinute"`
	StatsHour   *BrokerStatsItem `json:"statsHour"`
	StatsTen    *BrokerStatsItem `json:"statsTen"`
}

// ProducerInfo — the exporter only needs list.size(); fields are not read, so a
// minimal placeholder keeps the slice non-nil for counting.
type ProducerInfo struct{}

// ProducerTableInfo mirrors org.apache.rocketmq.common.protocol.body.ProducerTableInfo.
type ProducerTableInfo struct {
	Data map[string][]ProducerInfo `json:"data"`
}

// ConsumerRunningStatusTable mirrors the inner StatusTable of ConsumerRunningInfo.
type ConsumerRunningStatusTable struct {
	ConsumeFailedMsgs int64   `json:"consumeFailedMsgs"`
	ConsumeFailedTPS  float64 `json:"consumeFailedTPS"`
	ConsumeOKTPS      float64 `json:"consumeOKTPS"`
	ConsumeRT         float64 `json:"consumeRT"`
	PullRT            float64 `json:"pullRT"`
	PullTPS           float64 `json:"pullTPS"`
}

// ConsumerRunningInfo mirrors org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo.
// StatusTable is keyed by topic (string), so no composite-key risk.
type ConsumerRunningInfo struct {
	StatusTable map[string]ConsumerRunningStatusTable `json:"statusTable"`
	Jstack      string                                `json:"jstack"`
}

// --- RPCs ---
//
// TopicStatsTable and ConsumeStats carry a Map<MessageQueue, X> (composite key).
// fastjson's serialization of composite-object keys is non-standard; we capture
// the raw form via json.RawMessage and parse it in the task layer once the live
// format is confirmed (see live test fixtures).

// TopicStatsTable mirrors org.apache.rocketmq.common.admin.TopicStatsTable.
// OffsetTable keys are MessageQueue (composite) — held as raw JSON until Entries().
type TopicStatsTable struct {
	OffsetTable map[string]json.RawMessage `json:"offsetTable"`
}

// TopicStatsEntry is one parsed (MessageQueue, TopicOffset) pair.
type TopicStatsEntry struct {
	Queue  MessageQueue
	Offset TopicOffset
}

// Entries parses the composite-key map into typed pairs.
func (t *TopicStatsTable) Entries() ([]TopicStatsEntry, error) {
	out := make([]TopicStatsEntry, 0, len(t.OffsetTable))
	for keyRaw, valRaw := range t.OffsetTable {
		var q MessageQueue
		if err := json.Unmarshal([]byte(keyRaw), &q); err != nil {
			return nil, fmt.Errorf("parse topic stats key %q: %w", keyRaw, err)
		}
		var o TopicOffset
		if err := json.Unmarshal(valRaw, &o); err != nil {
			return nil, fmt.Errorf("parse topic offset value: %w", err)
		}
		out = append(out, TopicStatsEntry{Queue: q, Offset: o})
	}
	return out, nil
}

// ConsumeStats mirrors org.apache.rocketmq.common.admin.ConsumeStats.
// OffsetTable keys are MessageQueue (composite) — held as raw JSON until Entries().
type ConsumeStats struct {
	OffsetTable map[string]json.RawMessage `json:"offsetTable"`
	ConsumeTps  float64                    `json:"consumeTps"`
}

// ConsumeStatsEntry is one parsed (MessageQueue, OffsetWrapper) pair.
type ConsumeStatsEntry struct {
	Queue  MessageQueue
	Offset OffsetWrapper
}

// Entries parses the composite-key map into typed pairs.
func (c *ConsumeStats) Entries() ([]ConsumeStatsEntry, error) {
	out := make([]ConsumeStatsEntry, 0, len(c.OffsetTable))
	for keyRaw, valRaw := range c.OffsetTable {
		var q MessageQueue
		if err := json.Unmarshal([]byte(keyRaw), &q); err != nil {
			return nil, fmt.Errorf("parse consume stats key %q: %w", keyRaw, err)
		}
		var o OffsetWrapper
		if err := json.Unmarshal(valRaw, &o); err != nil {
			return nil, fmt.Errorf("parse offset wrapper value: %w", err)
		}
		out = append(out, ConsumeStatsEntry{Queue: q, Offset: o})
	}
	return out, nil
}

// ComputeTotalDiff ports ConsumeStats.computeTotalDiff: sum of (brokerOffset -
// consumerOffset) over all entries.
func (c *ConsumeStats) ComputeTotalDiff() (int64, error) {
	entries, err := c.Entries()
	if err != nil {
		return 0, err
	}
	var diff int64
	for _, e := range entries {
		diff += e.Offset.BrokerOffset - e.Offset.ConsumerOffset
	}
	return diff, nil
}

// ExamineTopicStats fetches a topic's stats table (GET_TOPIC_STATS_INFO, 202).
func (a *AdminClient) ExamineTopicStatsOnBroker(brokerAddr, topic string) (*TopicStatsTable, error) {
	var t TopicStatsTable
	if _, err := a.invokeSyncBroker(brokerAddr, rmqremote.RequestGetTopicStatsInfo, topicHeaderH{topic: topic}, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

// QueryTopicConsumeByWho lists consumer groups for a topic (300).
func (a *AdminClient) QueryTopicConsumeByWhoOnBroker(brokerAddr, topic string) (*GroupList, error) {
	var g GroupList
	if _, err := a.invokeSyncBroker(brokerAddr, rmqremote.RequestQueryTopicConsumeByWho, topicHeaderH{topic: topic}, &g); err != nil {
		return nil, err
	}
	return &g, nil
}

// ExamineConsumerConnectionInfoOnBroker fetches a group's consumer connections
// (GET_CONSUMER_CONNECTION_LIST, 203 — NOT 38 which returns only consumerIdList).
func (a *AdminClient) ExamineConsumerConnectionInfoOnBroker(brokerAddr, group string) (*ConsumerConnection, error) {
	var cc ConsumerConnection
	if _, err := a.invokeSyncBroker(brokerAddr, rmqremote.RequestGetConsumerConnectionList, groupHeader{consumerGroup: group}, &cc); err != nil {
		return nil, err
	}
	return &cc, nil
}

// ExamineConsumeStats fetches a group's consume stats (208).
func (a *AdminClient) ExamineConsumeStatsOnBroker(brokerAddr, group, topic string) (*ConsumeStats, error) {
	var cs ConsumeStats
	if _, err := a.invokeSyncBroker(brokerAddr, rmqremote.RequestGetConsumeStats, groupTopicHeader{consumerGroup: group, topic: topic}, &cs); err != nil {
		return nil, err
	}
	return &cs, nil
}

// ViewBrokerStatsData fetches a broker stats item (315).
func (a *AdminClient) ViewBrokerStatsData(brokerAddr, statsName, statsKey string) (*BrokerStatsData, error) {
	var b BrokerStatsData
	if _, err := a.invokeSyncBroker(brokerAddr, rmqremote.RequestViewBrokerStatsData, statsDataHeader{statsName: statsName, statsKey: statsKey}, &b); err != nil {
		return nil, err
	}
	return &b, nil
}

// GetAllProducerInfo fetches a broker's producer table (328).
func (a *AdminClient) GetAllProducerInfo(brokerAddr string) (*ProducerTableInfo, error) {
	var p ProducerTableInfo
	if _, err := a.invokeSyncBroker(brokerAddr, rmqremote.RequestGetAllProducerInfo, nil, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

// GetConsumerRunningInfo fetches a consumer client's running info (307).
func (a *AdminClient) GetConsumerRunningInfoOnBroker(brokerAddr, group, clientId string, jstack bool) (*ConsumerRunningInfo, error) {
	var c ConsumerRunningInfo
	if _, err := a.invokeSyncBroker(brokerAddr, rmqremote.RequestGetConsumerRunningInfo,
		consumerRunningHeader{consumerGroup: group, clientId: clientId, jstackEnable: jstack}, &c); err != nil {
		return nil, err
	}
	return &c, nil
}
