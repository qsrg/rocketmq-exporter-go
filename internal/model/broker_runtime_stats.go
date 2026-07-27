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

// Package model holds data-model types ported from the Java exporter's
// model/ package. BrokerRuntimeStats is the parsed form of the broker
// GET_BROKER_RUNTIME_INFO KVTable, produced by fetchBrokerRuntimeStats.
package model

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/wcf/rmq-exporter/internal/util"
)

// TpsTriple is the {ten, sixty, sixHundred} triple shared by PutTps and the
// Get*Mis/Transfered/Total/Found Tps inner classes in BrokerRuntimeStats.java.
type TpsTriple struct {
	Ten       float64
	Sixty     float64
	SixHundred float64
}

// ScheduleMessageOffsetTable mirrors the Java inner class of the same name.
type ScheduleMessageOffsetTable struct {
	DelayOffset int64
	MaxOffset   int64
}

// BrokerRuntimeStats mirrors model.BrokerRuntimeStats. Fields and parsing are
// ported verbatim from the Java constructor BrokerRuntimeStats(KVTable).
type BrokerRuntimeStats struct {
	MsgPutTotalTodayNow              int64
	MsgGetTotalTodayNow               int64
	MsgPutTotalTodayMorning           int64
	MsgGetTotalTodayMorning           int64
	MsgPutTotalYesterdayMorning       int64
	MsgGetTotalYesterdayMorning       int64
	ScheduleMessageOffsetTables       []ScheduleMessageOffsetTable
	SendThreadPoolQueueHeadWaitTimeMills int64
	QueryThreadPoolQueueHeadWaitTimeMills int64
	PullThreadPoolQueueHeadWaitTimeMills int64
	QueryThreadPoolQueueSize          int64
	PullThreadPoolQueueSize           int64
	SendThreadPoolQueueCapacity       int64
	PullThreadPoolQueueCapacity       int64
	PutMessageDistributeTimeMap       map[string]int
	RemainHowManyDataToFlush          float64
	CommitLogMinOffset                int64
	CommitLogMaxOffset                int64
	Runtime                           string
	BootTimestamp                     int64
	CommitLogDirCapacityTotal         float64
	CommitLogDirCapacityFree          float64
	BrokerVersion                     int
	DispatchMaxBuffer                 int64
	PutTps                            TpsTriple
	GetMissTps                        TpsTriple
	GetTransferedTps                  TpsTriple
	GetTotalTps                       TpsTriple
	GetFoundTps                       TpsTriple
	ConsumeQueueDiskRatio             float64
	CommitLogDiskRatio                float64
	PageCacheLockTimeMills            int64
	GetMessageEntireTimeMax           int64
	PutMessageTimesTotal              int64
	BrokerVersionDesc                 string
	SendThreadPoolQueueSize           int64
	StartAcceptSendRequestTimeStamp   int64
	PutMessageEntireTimeMax           int64
	EarliestMessageTimeStamp          int64
	RemainTransientStoreBufferNumbs   int64
	QueryThreadPoolQueueCapacity      int64
	PutMessageAverageSize             float64
	PutMessageSizeTotal               int64
	DispatchBehindBytes               int64
	PutLatency99                      float64
	PutLatency999                     float64
}

// NewBrokerRuntimeStats parses the KVTable (a map[string]string) exactly as the
// Java constructor does. It returns an error (rather than throwing, as Java's
// NumberFormatException would) when a required key is missing or unparseable —
// see CLAUDE.md: checked exceptions become error returns, no panic control flow.
func NewBrokerRuntimeStats(kv map[string]string) (*BrokerRuntimeStats, error) {
	if kv == nil {
		return nil, fmt.Errorf("broker runtime stats: nil kv table")
	}
	s := &BrokerRuntimeStats{PutMessageDistributeTimeMap: map[string]int{}}
	g := kvGetter{kv: kv}

	var err error
	if s.MsgPutTotalTodayNow, err = g.parseInt64("msgPutTotalTodayNow"); err != nil {
		return nil, err
	}
	s.loadScheduleMessageOffsets(kv)
	if err = s.loadPutMessageDistributeTime(kv["putMessageDistributeTime"]); err != nil {
		return nil, err
	}
	if err = s.loadTps(&s.PutTps, kv["putTps"]); err != nil {
		return nil, err
	}
	if err = s.loadTps(&s.GetMissTps, kv["getMissTps"]); err != nil {
		return nil, err
	}
	// Java: getTransferredTps key preferred, else getTransferedTps (note spelling).
	transferKey := "getTransferedTps"
	if _, ok := kv["getTransferredTps"]; ok {
		transferKey = "getTransferredTps"
	}
	if err = s.loadTps(&s.GetTransferedTps, kv[transferKey]); err != nil {
		return nil, err
	}
	if err = s.loadTps(&s.GetTotalTps, kv["getTotalTps"]); err != nil {
		return nil, err
	}
	if err = s.loadTps(&s.GetFoundTps, kv["getFoundTps"]); err != nil {
		return nil, err
	}
	if err = s.loadCommitLogDirCapacity(kv["commitLogDirCapacity"]); err != nil {
		return nil, err
	}

	// Remaining direct field reads (order matches the Java constructor).
	int64Fields := []struct {
		dst *int64
		key string
	}{
		{&s.SendThreadPoolQueueHeadWaitTimeMills, "sendThreadPoolQueueHeadWaitTimeMills"},
		{&s.QueryThreadPoolQueueHeadWaitTimeMills, "queryThreadPoolQueueHeadWaitTimeMills"},
		{&s.QueryThreadPoolQueueSize, "queryThreadPoolQueueSize"},
		{&s.BootTimestamp, "bootTimestamp"},
		{&s.MsgPutTotalYesterdayMorning, "msgPutTotalYesterdayMorning"},
		{&s.MsgGetTotalYesterdayMorning, "msgGetTotalYesterdayMorning"},
		{&s.PullThreadPoolQueueSize, "pullThreadPoolQueueSize"},
		{&s.CommitLogMinOffset, "commitLogMinOffset"},
		{&s.PullThreadPoolQueueHeadWaitTimeMills, "pullThreadPoolQueueHeadWaitTimeMills"},
		{&s.DispatchMaxBuffer, "dispatchMaxBuffer"},
		{&s.PageCacheLockTimeMills, "pageCacheLockTimeMills"},
		{&s.CommitLogMaxOffset, "commitLogMaxOffset"},
		{&s.GetMessageEntireTimeMax, "getMessageEntireTimeMax"},
		{&s.MsgPutTotalTodayMorning, "msgPutTotalTodayMorning"},
		{&s.PutMessageTimesTotal, "putMessageTimesTotal"},
		{&s.MsgGetTotalTodayMorning, "msgGetTotalTodayMorning"},
		{&s.SendThreadPoolQueueSize, "sendThreadPoolQueueSize"},
		{&s.StartAcceptSendRequestTimeStamp, "startAcceptSendRequestTimeStamp"},
		{&s.PutMessageEntireTimeMax, "putMessageEntireTimeMax"},
		{&s.EarliestMessageTimeStamp, "earliestMessageTimeStamp"},
		{&s.RemainTransientStoreBufferNumbs, "remainTransientStoreBufferNumbs"},
		{&s.QueryThreadPoolQueueCapacity, "queryThreadPoolQueueCapacity"},
		{&s.DispatchBehindBytes, "dispatchBehindBytes"},
		{&s.PutMessageSizeTotal, "putMessageSizeTotal"},
		{&s.SendThreadPoolQueueCapacity, "sendThreadPoolQueueCapacity"},
		{&s.PullThreadPoolQueueCapacity, "pullThreadPoolQueueCapacity"},
	}
	for _, f := range int64Fields {
		if *f.dst, err = g.parseInt64(f.key); err != nil {
			return nil, err
		}
	}
	if s.Runtime, err = g.getString("runtime"); err != nil {
		return nil, err
	}
	if s.BrokerVersionDesc, err = g.getString("brokerVersionDesc"); err != nil {
		return nil, err
	}
	if s.BrokerVersion, err = g.parseInt("brokerVersion"); err != nil {
		return nil, err
	}
	if s.MsgGetTotalTodayNow, err = g.parseInt64("msgGetTotalTodayNow"); err != nil {
		return nil, err
	}
	// double fields
	if s.RemainHowManyDataToFlush, err = parseFirstFloat64(kv, "remainHowManyDataToFlush"); err != nil {
		return nil, err
	}
	if s.ConsumeQueueDiskRatio, err = g.parseFloat64("consumeQueueDiskRatio"); err != nil {
		return nil, err
	}
	if s.CommitLogDiskRatio, err = g.parseFloat64("commitLogDiskRatio"); err != nil {
		return nil, err
	}
	if s.PutMessageAverageSize, err = g.parseFloat64("putMessageAverageSize"); err != nil {
		return nil, err
	}
	if s.PutLatency99, err = g.parseFloat64OrDefault("putLatency99", "-1"); err != nil {
		return nil, err
	}
	if s.PutLatency999, err = g.parseFloat64OrDefault("putLatency999", "-1"); err != nil {
		return nil, err
	}
	return s, nil
}

// helper accessors mirroring Java's kvTable.getTable().get(...) semantics, where
// a missing key would NPE / throw in Java; here it surfaces as a typed error.
type kvGetter struct {
	kv map[string]string
}

func (g *kvGetter) getString(key string) (string, error) {
	v, ok := g.kv[key]
	if !ok {
		return "", fmt.Errorf("broker runtime stats: missing key %q", key)
	}
	return v, nil
}

func (g *kvGetter) parseInt64(key string) (int64, error) {
	v, err := g.getString(key)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("broker runtime stats: key %q = %q: %w", key, v, err)
	}
	return n, nil
}

func (g *kvGetter) parseInt(key string) (int, error) {
	v, err := g.getString(key)
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("broker runtime stats: key %q = %q: %w", key, v, err)
	}
	return n, nil
}

func (g *kvGetter) parseFloat64(key string) (float64, error) {
	v, err := g.getString(key)
	if err != nil {
		return 0, err
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("broker runtime stats: key %q = %q: %w", key, v, err)
	}
	return f, nil
}

func (g *kvGetter) parseFloat64OrDefault(key, def string) (float64, error) {
	v, ok := g.kv[key]
	if !ok {
		v = def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("broker runtime stats: key %q = %q: %w", key, v, err)
	}
	return f, nil
}

// parseFirstFloat64 mirrors Java: Double.parseDouble(value.split(" ")[0]) —
// used for remainHowManyDataToFlush which carries a unit suffix.
func parseFirstFloat64(kv map[string]string, key string) (float64, error) {
	v, ok := kv[key]
	if !ok {
		return 0, fmt.Errorf("broker runtime stats: missing key %q", key)
	}
	parts := strings.SplitN(v, " ", 2)
	f, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return 0, fmt.Errorf("broker runtime stats: key %q = %q: %w", key, v, err)
	}
	return f, nil
}

// loadTps mirrors Java loadTps: split on " ", arr[0]→ten, arr[1]→sixty,
// arr[2]→sixHundred (tolerant of fewer fields).
func (s *BrokerRuntimeStats) loadTps(t *TpsTriple, value string) error {
	if value == "" {
		return nil
	}
	arr := strings.Split(value, " ")
	var err error
	if len(arr) >= 1 && arr[0] != "" {
		if t.Ten, err = strconv.ParseFloat(arr[0], 64); err != nil {
			return fmt.Errorf("loadTps ten = %q: %w", arr[0], err)
		}
	}
	if len(arr) >= 2 {
		if t.Sixty, err = strconv.ParseFloat(arr[1], 64); err != nil {
			return fmt.Errorf("loadTps sixty = %q: %w", arr[1], err)
		}
	}
	if len(arr) >= 3 {
		if t.SixHundred, err = strconv.ParseFloat(arr[2], 64); err != nil {
			return fmt.Errorf("loadTps sixHundred = %q: %w", arr[2], err)
		}
	}
	return nil
}

// loadPutMessageDistributeTime mirrors the Java parser. The broker emits
// "[<=0ms]:1 [0~10ms]:2 ... " — space-separated pairs whose key token carries
// surrounding brackets; we strip them (Java: tarr[0].replace("[","").replace("]","")).
func (s *BrokerRuntimeStats) loadPutMessageDistributeTime(str string) error {
	if strings.EqualFold(str, "null") {
		return nil // Java: warn and return (map stays empty)
	}
	for _, ar := range strings.Split(str, " ") {
		tarr := strings.SplitN(ar, ":", 2)
		if len(tarr) < 2 {
			continue // Java: warn and continue
		}
		key := strings.NewReplacer("[", "", "]", "").Replace(tarr[0])
		n, err := strconv.Atoi(tarr[1])
		if err != nil {
			return fmt.Errorf("loadPutMessageDistributeTime %q: %w", ar, err)
		}
		s.PutMessageDistributeTimeMap[key] = n
	}
	return nil
}

// loadCommitLogDirCapacity mirrors Java loadCommitLogDirCapacity: the raw value
// looks like "Total : 1.5 GB, Free : 500.0 MB"; total = "1.5 GB" (arr[2]+" "+
// arr[3] without trailing comma), free = "500.0 MB" (arr[6]+" "+arr[7]).
func (s *BrokerRuntimeStats) loadCommitLogDirCapacity(value string) error {
	arr := strings.Split(value, " ")
	if len(arr) < 8 {
		return fmt.Errorf("loadCommitLogDirCapacity: unexpected format %q", value)
	}
	total := fmt.Sprintf("%s %s", arr[2], strings.TrimSuffix(arr[3], ","))
	free := fmt.Sprintf("%s %s", arr[6], strings.TrimSuffix(arr[7], ","))
	s.CommitLogDirCapacityTotal = util.MachineReadableByteCount(total)
	s.CommitLogDirCapacityFree = util.MachineReadableByteCount(free)
	return nil
}

// loadScheduleMessageOffsets mirrors Java loadScheduleMessageOffsets: every key
// starting with "scheduleMessageOffset" has value "delayOffset,maxOffset".
func (s *BrokerRuntimeStats) loadScheduleMessageOffsets(kv map[string]string) {
	for key, v := range kv {
		if !strings.HasPrefix(key, "scheduleMessageOffset") {
			continue
		}
		arr := strings.SplitN(v, ",", 2)
		if len(arr) < 2 {
			continue
		}
		first, err1 := strconv.ParseInt(arr[0], 10, 64)
		second, err2 := strconv.ParseInt(arr[1], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		s.ScheduleMessageOffsetTables = append(s.ScheduleMessageOffsetTables,
			ScheduleMessageOffsetTable{DelayOffset: first, MaxOffset: second})
	}
}
