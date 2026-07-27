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
	"github.com/wcf/rmq-exporter/internal/rmqremote"
)

// ClusterInfo is the body of GET_BROKER_CLUSTER_INFO (106), returned by
// examineBrokerClusterInfo. Mirrors org.apache.rocketmq.common.protocol.body.ClusterInfo.
type ClusterInfo struct {
	ClusterAddrTable map[string][]string `json:"clusterAddrTable"` // cluster name -> broker names
	BrokerAddrTable  map[string]BrokerData `json:"brokerAddrTable"`  // broker name -> BrokerData
}

// BrokerData mirrors org.apache.rocketmq.common.protocol.route.BrokerData.
// BrokerAddrs is keyed by broker id (MASTER_ID = 0); JSON keys are strings,
// decoded into int64.
type BrokerData struct {
	Cluster     string            `json:"cluster"`
	BrokerName  string            `json:"brokerName"`
	BrokerAddrs map[int64]string  `json:"brokerAddrs"`
}

// TopicList is the body of GET_ALL_TOPIC_LIST_FROM_NAMESERVER (206).
// Mirrors org.apache.rocketmq.common.protocol.body.TopicList.
type TopicList struct {
	TopicList       []string `json:"topicList"`
	TopicQueueTable map[string]int `json:"topicQueueTable"` // (4.x: present but exporter ignores it)
}

// TopicRouteData is the body of GET_ROUTEINFO_BY_TOPIC (105).
// Mirrors org.apache.rocketmq.common.protocol.route.TopicRouteData.
type TopicRouteData struct {
	QueueDatas   []QueueData   `json:"queueDatas"`
	BrokerDatas  []BrokerData  `json:"brokerDatas"`
}

// QueueData mirrors org.apache.rocketmq.common.protocol.route.QueueData.
type QueueData struct {
	BrokerName     string `json:"brokerName"`
	ReadQueueNums  int    `json:"readQueueNums"`
	WriteQueueNums int    `json:"writeQueueNums"`
	Perm           int    `json:"perm"`
	TopicSynFlag   int    `json:"topicSysFlag"`
}

// --- request headers (ext fields) ---

type topicHeader struct{ topic string }

func (h topicHeader) Encode() map[string]string {
	return map[string]string{"topic": h.topic}
}

// --- RPCs ---

// ExamineBrokerClusterInfo fetches the cluster topology from namesrv (106).
func (a *AdminClient) ExamineBrokerClusterInfo() (*ClusterInfo, error) {
	var ci ClusterInfo
	if err := a.invokeSyncNamesrv(rmqremote.RequestGetBrokerClusterInfo, nil, &ci); err != nil {
		return nil, err
	}
	return &ci, nil
}

// FetchAllTopicList fetches every topic from namesrv (206).
func (a *AdminClient) FetchAllTopicList() (*TopicList, error) {
	var tl TopicList
	if err := a.invokeSyncNamesrv(rmqremote.RequestGetAllTopicListFromNameSrv, nil, &tl); err != nil {
		return nil, err
	}
	return &tl, nil
}

// ExamineTopicRouteInfo fetches the route for a topic from namesrv (105).
func (a *AdminClient) ExamineTopicRouteInfo(topic string) (*TopicRouteData, error) {
	var tr TopicRouteData
	if err := a.invokeSyncNamesrv(rmqremote.RequestGetRouteInfoByTopic, topicHeader{topic: topic}, &tr); err != nil {
		return nil, err
	}
	return &tr, nil
}
