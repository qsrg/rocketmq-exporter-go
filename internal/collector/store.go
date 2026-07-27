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
	"time"
)

// clock lets tests inject a deterministic now.
type clock interface{ now() time.Time }

type realClock struct{}

func (realClock) now() time.Time { return time.Now() }

// entry holds a cached value plus its last-write time, mirroring Guava's
// expireAfterWrite semantics (evict TTL after the last write to a key).
type entry[V any] struct {
	value     V
	writtenAt time.Time
}

// ttlCache is the Go analogue of a Guava Cache<K, V> with expireAfterWrite.
// A value is considered live from its last write until now-ttl; Sweep deletes
// expired entries, and Snapshot returns only live (key, value) pairs so a
// /metrics scrape reflects the most recent successful collection round.
type ttlCache[K comparable, V any] struct {
	mu  sync.RWMutex
	m   map[K]entry[V]
	ttl time.Duration
	clk clock
}

func newCache[K comparable, V any](ttl time.Duration, clk clock) *ttlCache[K, V] {
	return &ttlCache[K, V]{m: make(map[K]entry[V]), ttl: ttl, clk: clk}
}

func (c *ttlCache[K, V]) put(k K, v V) {
	c.mu.Lock()
	c.m[k] = entry[V]{value: v, writtenAt: c.clk.now()}
	c.mu.Unlock()
}

func (c *ttlCache[K, V]) get(k K) (V, bool) {
	c.mu.RLock()
	e, ok := c.m[k]
	c.mu.RUnlock()
	if !ok || c.clk.now().Sub(e.writtenAt) > c.ttl {
		var zero V
		return zero, false
	}
	return e.value, true
}

func (c *ttlCache[K, V]) snapshot() []kv[K, V] {
	now := c.clk.now()
	c.mu.RLock()
	out := make([]kv[K, V], 0, len(c.m))
	for k, e := range c.m {
		if now.Sub(e.writtenAt) > c.ttl {
			continue
		}
		out = append(out, kv[K, V]{k, e.value})
	}
	c.mu.RUnlock()
	return out
}

// Sweep deletes expired entries. Called by the janitor between scrapes.
func (c *ttlCache[K, V]) sweep() {
	now := c.clk.now()
	c.mu.Lock()
	for k, e := range c.m {
		if now.Sub(e.writtenAt) > c.ttl {
			delete(c.m, k)
		}
	}
	c.mu.Unlock()
}

func (c *ttlCache[K, V]) len() int {
	c.mu.RLock()
	n := len(c.m)
	c.mu.RUnlock()
	return n
}

type kv[K comparable, V any] struct {
	K K
	V V
}

// --- Key types: identity fields mirror the Java equals() of each metric key.
// Fields that are labels but NOT in the Java equals (last-writer-wins labels)
// are carried in the value entry, not the key. ---

// producerKey = ProducerMetric.equals(cluster, broker, topic). lastUpdateTimestamp
// is NOT in the Java key.
type producerKey struct{ cluster, broker, topic string }

// producerCountKey = ProducerCountMetric.equals(group, cluster, broker).
type producerCountKey struct{ group, cluster, broker string }

// dlqKey = DLQTopicOffsetMetric.equals(cluster, broker, group).
type dlqKey struct{ cluster, broker, group string }

// diffKey = ConsumerTopicDiffMetric.equals(group, topic, countOfOnlineConsumers, msgModel).
type diffKey struct{ group, topic, countOnline, msgModel string }

// consumerKey = ConsumerMetric.equals(cluster, broker, topic, group).
type consumerKey struct{ cluster, broker, topic, group string }

// brokerKey = BrokerMetric.equals(cluster, brokerIP). brokerName is a
// last-writer-wins label, carried in the value entry.
type brokerKey struct{ cluster, brokerIP string }

// topicPutNumKey = TopicPutNumMetric.equals(cluster, brokerIP, topic).
// brokerName is a last-writer-wins label, carried in the value entry.
type topicPutNumKey struct{ cluster, brokerIP, topic string }

// brokerRuntimeKey = BrokerRuntimeMetric.equals(cluster, brokerAddress).
// brokerHost/des/boottime/broker_version are last-writer-wins labels, carried
// in the value entry.
type brokerRuntimeKey struct{ cluster, brokerAddress string }

// clientRuntimeKey = ConsumerRuntime*.equals(group, topic, caddrsIgnoreCase).
// caddrs original case + clientId (localaddrs) are last-writer-wins labels.
type clientRuntimeKey struct{ group, topic, caddrsLower string }

// --- Value entries for last-writer-wins metrics. ---

// brokerVal carries the brokerName label (last writer) plus the gauge value.
type brokerVal struct{ brokerName string; val float64 }

type topicPutNumVal struct{ brokerName string; val float64 }

type brokerRuntimeVal struct {
	brokerHost, brokerDesc  string
	bootTimestamp            int64
	brokerVersion            int
}

// For broker-runtime metrics the numeric value lives in a dedicated cache per
// metric (so a null putMessageDistributeTimeMap keeps the 13 pmdt gauges absent
// while the other ~50 are present, matching Java). The label-bearing fields are
// shared; each cache stores brokerRuntimeVal as the value alongside the number.
// To avoid duplicating the label fields per cache, we store a pointer to the
// shared brokerRuntimeVal in each per-metric cache entry.
type runtimeEntry struct {
	labels *brokerRuntimeVal // shared label snapshot for this broker
	num    float64
}

type clientRuntimeVal struct {
	caddrs   string // original case (Java: getCaddrs())
	localaddrs string
	val      float64
}

type consumerCountVal struct {
	caddrs    string
	localaddrs string
	count     int
}
