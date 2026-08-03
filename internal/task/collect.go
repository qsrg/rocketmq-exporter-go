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

// Package task holds the six cron collection tasks (a port of
// MetricsCollectTask.java) and the bounded worker pool that runs client-metric
// collection.
package task

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/qsrg/rocketmq-exporter-go/internal/collector"
	"github.com/qsrg/rocketmq-exporter-go/internal/rmqremote"
	"github.com/qsrg/rocketmq-exporter-go/internal/model"
	"github.com/qsrg/rocketmq-exporter-go/internal/service"
	"github.com/qsrg/rocketmq-exporter-go/internal/util"
)

const (
	retryTopicPrefix = "%RETRY%" // MixAll.RETRY_GROUP_TOPIC_PREFIX
	dlqTopicPrefix   = "%DLQ%"   // MixAll.DLQ_GROUP_TOPIC_PREFIX
)

// CollectTask is the Go port of MetricsCollectTask. It holds the admin client,
// the metric store, the enable-collect flag, and the worker pool. The six
// Collect* methods are registered with the cron scheduler.
type CollectTask struct {
	Admin         *service.AdminClient
	Coll          *collector.MetricsCollector
	EnableCollect bool
	Pool          *WorkerPool
	Log           *slog.Logger

	clusterName string // cached first-cluster name (Java's static MetricsCollectTask.clusterName)
}

func (t *CollectTask) logger() *slog.Logger {
	if t.Log != nil {
		return t.Log
	}
	return slog.Default()
}

// cluster returns the cached cluster name, fetching it once (Java sets it in
// @PostConstruct init from examineBrokerClusterInfo).
func (t *CollectTask) cluster() (string, error) {
	if t.clusterName != "" {
		return t.clusterName, nil
	}
	ci, err := t.Admin.ExamineBrokerClusterInfo()
	if err != nil {
		return "", err
	}
	for name := range ci.ClusterAddrTable {
		t.clusterName = name
		break
	}
	return t.clusterName, nil
}

// brokerAddrTable caches brokerName -> master addr from a fresh ClusterInfo.
func (t *CollectTask) brokerAddrTable() (map[string]string, *service.ClusterInfo, error) {
	ci, err := t.Admin.ExamineBrokerClusterInfo()
	if err != nil {
		return nil, nil, err
	}
	m := make(map[string]string, len(ci.BrokerAddrTable))
	for name, bd := range ci.BrokerAddrTable {
		if addr, ok := bd.BrokerAddrs[0]; ok {
			m[name] = addr
		}
	}
	return m, ci, nil
}

// handleTopicNotExistException ports the Java method: TOPIC_NOT_EXIST and
// CONSUMER_NOT_ONLINE are silently swallowed; other codes are logged at error.
func (t *CollectTask) handleTopicNotExist(topic, group string, err error) {
	code := service.ResponseCodeOf(err)
	if code == rmqremote.ResponseTopicNotExist || code == rmqremote.ResponseConsumerNotOnline {
		return // silent
	}
	t.logger().Error("get topic's consumer-stats exception", "topic", topic, "group", group, "err", err)
}

// messageModelOrdinal returns the ordinal of a MessageModel name as a string.
// MessageModel enum order is BROADCASTING(0), CLUSTERING(1) (confirmed from
// rocketmq-4.9.8 common/.../heartbeat/MessageModel.java), so CLUSTERING — the
// Java exporter's default and the only model for which diff is emitted — maps
// to "1". Empty/unknown -> "1" (Java's MessageModel.CLUSTERING default).
func messageModelOrdinal(name string) string {
	if strings.EqualFold(name, "BROADCASTING") {
		return "0"
	}
	return "1" // CLUSTERING (default)
}

// --- 1. collectTopicOffset ---

func (t *CollectTask) CollectTopicOffset(_ context.Context) {
	if !t.EnableCollect {
		return
	}
	start := time.Now()
	log := t.logger()
	tl, err := t.Admin.FetchAllTopicList()
	if err != nil {
		log.Error("collectTopicOffset: getting topic list", "err", err)
		return
	}
	if len(tl.TopicList) == 0 {
		log.Error("collectTopicOffset: the topic list is empty")
		return
	}
	cluster, err := t.cluster()
	if err != nil {
		log.Error("collectTopicOffset: cluster", "err", err)
		return
	}
	for _, topic := range tl.TopicList {
		tts, err := t.Admin.ExamineTopicStats(topic)
		if err != nil {
			log.Error("collectTopicOffset: getting topic stats", "topic", topic, "err", err)
			continue
		}
		entries, err := tts.Entries()
		if err != nil {
			log.Error("collectTopicOffset: parse topic stats", "topic", topic, "err", err)
			continue
		}
		brokerOffset := map[string]int64{}
		brokerTs := map[string]int64{}
		for _, e := range entries {
			bn := e.Queue.BrokerName
			brokerOffset[bn] = brokerOffset[bn] + e.Offset.MaxOffset
			if e.Offset.LastUpdateTimestamp > brokerTs[bn] {
				brokerTs[bn] = e.Offset.LastUpdateTimestamp
			}
		}
		for bn, off := range brokerOffset {
			t.Coll.AddTopicOffsetMetric(cluster, bn, topic, brokerTs[bn], float64(off))
		}
	}
	log.Info("topic offset collection task finished", "cost", time.Since(start))
}

// --- 2. collectProducer ---

func (t *CollectTask) CollectProducer(_ context.Context) {
	if !t.EnableCollect {
		return
	}
	log := t.logger()
	ci, err := t.Admin.ExamineBrokerClusterInfo()
	if err != nil {
		log.Error("collectProducer: cluster info", "err", err)
		return
	}
	if ci.ClusterAddrTable == nil || ci.BrokerAddrTable == nil {
		log.Warn("collectProducer: empty cluster info")
		return
	}
	for clusterName, brokerNames := range ci.ClusterAddrTable {
		if len(brokerNames) == 0 {
			continue
		}
		for _, brokerName := range brokerNames {
			bd := ci.BrokerAddrTable[brokerName]
			masterAddr := bd.BrokerAddrs[0]
			pt, err := t.Admin.GetAllProducerInfo(masterAddr)
			if err != nil {
				log.Error("collectProducer: should not be here", "cluster", clusterName, "broker", brokerName, "err", err)
				continue
			}
			if pt == nil || len(pt.Data) == 0 {
				continue
			}
			for producerGroup, list := range pt.Data {
				count := -1
				if list != nil {
					count = len(list)
				}
				t.Coll.AddProducerCountMetric(clusterName, brokerName, producerGroup, count)
			}
		}
	}
	log.Info("producer metric collection task ended")
}

// --- 3. collectConsumerOffset (the largest) ---

func (t *CollectTask) CollectConsumerOffset(_ context.Context) {
	if !t.EnableCollect {
		return
	}
	start := time.Now()
	log := t.logger()
	tl, err := t.Admin.FetchAllTopicList()
	if err != nil {
		log.Error("collectConsumerOffset: fetch topic list", "err", err)
		return
	}
	cluster, err := t.cluster()
	if err != nil {
		log.Error("collectConsumerOffset: cluster", "err", err)
		return
	}
	brokerAddrs, _, err := t.brokerAddrTable()
	if err != nil {
		log.Error("collectConsumerOffset: broker addrs", "err", err)
		return
	}
	groupCollected := map[string]bool{}
	for _, topic := range tl.TopicList {
		if strings.HasPrefix(topic, dlqTopicPrefix) {
			continue
		}
		gl, err := t.Admin.QueryTopicConsumeByWho(topic)
		if err != nil {
			continue // Java: silently continue
		}
		if gl == nil || len(gl.GroupList) == 0 {
			continue
		}
		for _, group := range gl.GroupList {
			var countOfOnline int
			var cc *service.ConsumerConnection
			cc, err = t.Admin.ExamineConsumerConnectionInfo(group)
			if err != nil {
				t.handleTopicNotExist(topic, group, err)
			} else {
				countOfOnline = len(cc.ConnectionSet)
			}

			cAddrs, localAddrs := "", ""
			if countOfOnline > 0 {
				addrs := make([]string, 0, countOfOnline)
				ids := make([]string, 0, countOfOnline)
				for _, conn := range cc.ConnectionSet {
					addrs = append(addrs, conn.ClientAddr)
					ids = append(ids, conn.ClientId)
				}
				cAddrs, localAddrs = util.ClientAddresses(addrs, ids)
			}
			t.Coll.AddGroupCountMetric(group, cAddrs, localAddrs, countOfOnline)

			if countOfOnline > 0 && !groupCollected[group] {
				t.Pool.Submit(NewClientMetricTask(t.Admin, t.Coll, group, cc))
				groupCollected[group] = true
			}

			cs, err := t.Admin.ExamineConsumeStats(group, topic)
			if err != nil {
				t.handleTopicNotExist(topic, group, err)
				continue
			}
			entries, err := cs.Entries()
			if err != nil || len(entries) == 0 {
				continue
			}
			diff, _ := cs.ComputeTotalDiff()
			ord := messageModelOrdinal("")
			if cc != nil {
				ord = messageModelOrdinal(cc.MessageModel)
			}
			t.Coll.AddGroupDiffMetric(strconv.Itoa(countOfOnline), group, topic, ord, diff)

			// per-broker consumer offset (CLUSTERING)
			consumeOffsetMap := map[string]int64{}
			for _, e := range entries {
				consumeOffsetMap[e.Queue.BrokerName] += e.Offset.ConsumerOffset
			}
			for bn, off := range consumeOffsetMap {
				t.Coll.AddGroupBrokerTotalOffsetMetric(cluster, bn, topic, group, off)
			}

			// storetime latency (CLUSTERING only) via queryMsgByOffset.
			// Ports Java collectConsumerOffset: a latency sample is emitted for
			// every broker that has a queue in the consume stats, with lagTime 0
			// when the pull returns NO_NEW (Java: !containsKey -> put(lagTime>0?
			// lagTime:0)). If any pull errors, the whole group's latency is
			// skipped (Java wraps the loop in try/catch and abandons the map).
			latencyMap := map[string]int64{}
			latencyOK := true
			for _, e := range entries {
				addr, ok := brokerAddrs[e.Queue.BrokerName]
				if !ok || addr == "" {
					continue
				}
				pr, perr := t.Admin.QueryMsgByOffset(addr, e.Queue, e.Offset.ConsumerOffset)
				if perr != nil {
					// Java: an exception in the loop abandons the whole map.
					latencyOK = false
					break
				}
				var lag int64
				if pr.Status == "FOUND" {
					lag = time.Now().UnixMilli() - pr.StoreTimestamp
					if e.Offset.BrokerOffset == e.Offset.ConsumerOffset {
						lag = 0
					}
				} else if pr.Status == "OFFSET_ILLEGAL" {
					pr2, perr2 := t.Admin.QueryMsgByOffset(addr, e.Queue, pr.MinOffset)
					if perr2 != nil {
						latencyOK = false
						break
					}
					if pr2.Status == "FOUND" {
						lag = time.Now().UnixMilli() - pr2.StoreTimestamp
					}
				}
				// Java: first queue for a broker seeds the map (0 if no lag);
				// subsequent queues only raise it.
				cur, exists := latencyMap[e.Queue.BrokerName]
				if !exists {
					if lag > 0 {
						latencyMap[e.Queue.BrokerName] = lag
					} else {
						latencyMap[e.Queue.BrokerName] = 0
					}
				} else if lag > cur {
					latencyMap[e.Queue.BrokerName] = lag
				}
			}
			if !latencyOK {
				log.Warn("addGroupGetLatencyByStoreTimeMetric error: a pull failed; skipping latency for group",
					"topic", topic, "group", group)
			} else {
				for bn, lag := range latencyMap {
					t.Coll.AddGroupGetLatencyByStoreTimeMetric(cluster, bn, topic, group, lag)
				}
			}
		}
	}
	log.Info("consumer offset collection task finished", "cost", time.Since(start))
}

// --- 4. collectBrokerStatsTopic ---

// BrokerStatsManager stats names (org.apache.rocketmq.store.stats.BrokerStatsManager).
const (
	statsTopicPutNums   = "TOPIC_PUT_NUMS"
	statsTopicPutSize   = "TOPIC_PUT_SIZE"
	statsGroupGetNums   = "GROUP_GET_NUMS"
	statsGroupGetSize   = "GROUP_GET_SIZE"
	statsSndBackPutNums = "SNDBCK_PUT_NUMS"
	statsBrokerPutNums  = "BROKER_PUT_NUMS"
	statsBrokerGetNums   = "BROKER_GET_NUMS"
)

func (t *CollectTask) CollectBrokerStatsTopic(_ context.Context) {
	if !t.EnableCollect {
		return
	}
	start := time.Now()
	log := t.logger()
	tl, err := t.Admin.FetchAllTopicList()
	if err != nil {
		log.Error("collectBrokerStatsTopic: fetch topic list", "err", err)
		return
	}
	if len(tl.TopicList) == 0 {
		return
	}
	ci, err := t.Admin.ExamineBrokerClusterInfo()
	if err != nil {
		log.Error("collectBrokerStatsTopic: cluster info", "err", err)
		return
	}
	for _, topic := range tl.TopicList {
		if strings.HasPrefix(topic, retryTopicPrefix) || strings.HasPrefix(topic, dlqTopicPrefix) {
			continue
		}
		route, err := t.Admin.ExamineTopicRouteInfo(topic)
		if err != nil {
			log.Error("collectBrokerStatsTopic: fetch topic route", "topic", topic, "err", err)
			continue
		}
		for _, bd := range route.BrokerDatas {
			masterAddr := ""
			if a, ok := bd.BrokerAddrs[0]; ok {
				masterAddr = a
			}
			if masterAddr == "" {
				continue
			}
			brokerIP := masterAddr
			if mb, ok := ci.BrokerAddrTable[bd.BrokerName]; ok {
				if a, ok2 := mb.BrokerAddrs[0]; ok2 {
					brokerIP = a
				}
			}
			// TOPIC_PUT_NUMS / TOPIC_PUT_SIZE
			if bsd, err := t.Admin.ViewBrokerStatsData(masterAddr, statsTopicPutNums, topic); err == nil && bsd.StatsMinute != nil {
				t.Coll.AddTopicPutNumsMetric(bd.Cluster, bd.BrokerName, brokerIP, topic, util.GetFixedDouble(bsd.StatsMinute.Tps))
			} else if code := service.ResponseCodeOf(err); code != rmqremote.ResponseSystemError && err != nil {
				log.Error("TOPIC_PUT_NUMS-error", "topic", topic, "broker", masterAddr, "err", err)
			}
			if bsd, err := t.Admin.ViewBrokerStatsData(masterAddr, statsTopicPutSize, topic); err == nil && bsd.StatsMinute != nil {
				t.Coll.AddTopicPutSizeMetric(bd.Cluster, bd.BrokerName, brokerIP, topic, util.GetFixedDouble(bsd.StatsMinute.Tps))
			} else if code := service.ResponseCodeOf(err); code != rmqremote.ResponseSystemError && err != nil {
				log.Error("TOPIC_PUT_SIZE-error", "topic", topic, "broker", masterAddr, "err", err)
			}
		}

		gl, err := t.Admin.QueryTopicConsumeByWho(topic)
		if err != nil || gl == nil || len(gl.GroupList) == 0 {
			continue
		}
		for _, group := range gl.GroupList {
			statsKey := topic + "@" + group
			for _, bd := range route.BrokerDatas {
				masterAddr := ""
				if a, ok := bd.BrokerAddrs[0]; ok {
					masterAddr = a
				}
				if masterAddr == "" {
					continue
				}
				if bsd, err := t.Admin.ViewBrokerStatsData(masterAddr, statsGroupGetNums, statsKey); err == nil && bsd.StatsMinute != nil {
					t.Coll.AddGroupGetNumsMetric(bd.Cluster, bd.BrokerName, topic, group, util.GetFixedDouble(bsd.StatsMinute.Tps))
				} else if code := service.ResponseCodeOf(err); code != rmqremote.ResponseSystemError && err != nil {
					log.Error("GROUP_GET_NUMS-error", "topic", topic, "group", group, "broker", masterAddr, "err", err)
				}
				if bsd, err := t.Admin.ViewBrokerStatsData(masterAddr, statsGroupGetSize, statsKey); err == nil && bsd.StatsMinute != nil {
					t.Coll.AddGroupGetSizeMetric(bd.Cluster, bd.BrokerName, topic, group, util.GetFixedDouble(bsd.StatsMinute.Tps))
				} else if code := service.ResponseCodeOf(err); code != rmqremote.ResponseSystemError && err != nil {
					log.Error("GROUP_GET_SIZE-error", "topic", topic, "group", group, "broker", masterAddr, "err", err)
				}
				if bsd, err := t.Admin.ViewBrokerStatsData(masterAddr, statsSndBackPutNums, statsKey); err == nil && bsd.StatsMinute != nil {
					t.Coll.AddSendBackNumsMetric(bd.Cluster, bd.BrokerName, topic, group, bsd.StatsMinute.Sum)
				} else if code := service.ResponseCodeOf(err); code != rmqremote.ResponseSystemError && err != nil {
					log.Error("SNDBCK_PUT_NUMS-error", "topic", topic, "group", group, "broker", masterAddr, "err", err)
				}
			}
		}
	}
	log.Info("broker topic stats collection task finished", "cost", time.Since(start))
}

// --- 5. collectBrokerStats + collectBrokerGroupStats (share one cron slot) ---

func (t *CollectTask) CollectBrokerStats(_ context.Context) {
	if !t.EnableCollect {
		return
	}
	start := time.Now()
	log := t.logger()
	ci, err := t.Admin.ExamineBrokerClusterInfo()
	if err != nil {
		log.Error("collectBrokerStats: cluster info", "err", err)
		return
	}
	for _, bd := range ci.BrokerAddrTable {
		masterAddr := ""
		if a, ok := bd.BrokerAddrs[0]; ok {
			masterAddr = a
		}
		if masterAddr == "" {
			continue
		}
		clusterName := bd.Cluster
		brokerIP := masterAddr
		brokerName := bd.BrokerName
		if bsd, err := t.Admin.ViewBrokerStatsData(masterAddr, statsBrokerPutNums, clusterName); err == nil && bsd.StatsMinute != nil {
			t.Coll.AddBrokerPutNumsMetric(clusterName, brokerIP, brokerName, util.GetFixedDouble(bsd.StatsMinute.Tps))
		} else if code := service.ResponseCodeOf(err); code != rmqremote.ResponseSystemError && err != nil {
			log.Error("BROKER_PUT_NUMS-error", "broker", masterAddr, "err", err)
		}
		if bsd, err := t.Admin.ViewBrokerStatsData(masterAddr, statsBrokerGetNums, clusterName); err == nil && bsd.StatsMinute != nil {
			t.Coll.AddBrokerGetNumsMetric(clusterName, brokerIP, brokerName, util.GetFixedDouble(bsd.StatsMinute.Tps))
		} else if code := service.ResponseCodeOf(err); code != rmqremote.ResponseSystemError && err != nil {
			log.Error("BROKER_GET_NUMS-error", "broker", masterAddr, "err", err)
		}
	}
	log.Info("broker stats collection task finished", "cost", time.Since(start))
}

func (t *CollectTask) CollectBrokerGroupStats(_ context.Context) {
	if !t.EnableCollect {
		return
	}
	log := t.logger()
	ci, err := t.Admin.ExamineBrokerClusterInfo()
	if err != nil {
		log.Error("collectBrokerGroupStats: cluster info", "err", err)
		return
	}
	for _, bd := range ci.BrokerAddrTable {
		clusterName := bd.Cluster
		brokerName := bd.BrokerName
		masterAddr := ""
		if a, ok := bd.BrokerAddrs[0]; ok {
			masterAddr = a
		}
		for brokerID, addr := range bd.BrokerAddrs {
			if brokerID == 0 { // MASTER_ID
				continue
			}
			slave := t.fetchRuntime(addr)
			master := t.fetchRuntime(masterAddr)
			var diff float64
			if master != nil && slave != nil {
				diff = float64(master.CommitLogMaxOffset - slave.CommitLogMaxOffset)
			}
			t.Coll.AddBrokerCommitLogDiffMetric(clusterName, addr, brokerName, diff)
		}
	}
	log.Info("broker group stats collection task finished")
}

func (t *CollectTask) fetchRuntime(addr string) *model.BrokerRuntimeStats {
	if addr == "" {
		return nil
	}
	s, err := t.Admin.FetchBrokerRuntimeStats(addr)
	if err != nil {
		return nil
	}
	return s
}

// --- 6. collectBrokerRuntimeStats ---

func (t *CollectTask) CollectBrokerRuntimeStats(_ context.Context) {
	if !t.EnableCollect {
		return
	}
	start := time.Now()
	log := t.logger()
	ci, err := t.Admin.ExamineBrokerClusterInfo()
	if err != nil {
		log.Error("collectBrokerRuntimeStats: cluster info", "err", err)
		return
	}
	for _, bd := range ci.BrokerAddrTable {
		masterAddr := ""
		if a, ok := bd.BrokerAddrs[0]; ok {
			masterAddr = a
		}
		clusterName := bd.Cluster
		stats, err := t.Admin.FetchBrokerRuntimeStats(masterAddr)
		if err != nil {
			log.Error("collectBrokerRuntimeStats: fetch runtime stats", "broker", masterAddr, "err", err)
			continue
		}
		t.Coll.AddBrokerRuntimeStatsMetric(stats, clusterName, masterAddr, "")
	}
	log.Info("broker runtime stats collection task finished", "cost", time.Since(start))
}
