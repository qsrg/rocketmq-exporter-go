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
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/wcf/rmq-exporter/internal/rmqremote"
)

// PullStatus mirrors org.apache.rocketmq.client.consumer.PullStatus (string
// form). Derived from the PULL_MESSAGE response Code (the response header has
// no pullStatus field in 4.x).
const (
	pullFound        = "FOUND"
	pullNoNewMsg     = "NO_NEW_MSG"
	pullNoMatchedMsg = "NO_MATCHED_MSG"
	pullOffsetIllegal = "OFFSET_ILLEGAL"
	pullUnknown      = "UNKNOWN"
)

// PullResult is the subset of DefaultMQPullConsumer.pull's result the exporter
// needs: the pull status, the first found message's store timestamp (for the
// storetime-latency metric), and min offset (for the OFFSET_ILLEGAL retry path).
type PullResult struct {
	Status          string
	StoreTimestamp  int64
	MinOffset       int64
}

// pull sysFlag bits (PullSysFlag). The exporter's pull(mq,"*",offset,1) sets
// subscription=true, suspend=false, commitOffset=false, classFilter=false.
const flagSubscription = 0x1 << 2 // FLAG_SUBSCRIPTION

// FlagBornHostV6 selects a 20-byte (v6) vs 8-byte (v4) born-host field, which
// shifts the store-timestamp offset in the message record.
const flagBornHostV6 = 0x1 << 4

type pullHeader struct {
	consumerGroup, topic, subscription, expressionType string
	queueId, maxMsgNums, sysFlag                        int
	queueOffset, commitOffset, suspendTimeoutMillis, subVersion int64
}

func (h pullHeader) Encode() map[string]string {
	return map[string]string{
		"consumerGroup":        h.consumerGroup,
		"topic":                h.topic,
		"queueId":              strconv.Itoa(h.queueId),
		"queueOffset":          strconv.FormatInt(h.queueOffset, 10),
		"maxMsgNums":           strconv.Itoa(h.maxMsgNums),
		"sysFlag":              strconv.Itoa(h.sysFlag),
		"commitOffset":        strconv.FormatInt(h.commitOffset, 10),
		"suspendTimeoutMillis": strconv.FormatInt(h.suspendTimeoutMillis, 10),
		"subscription":         h.subscription,
		"subVersion":           strconv.FormatInt(h.subVersion, 10),
		"expressionType":       h.expressionType,
	}
}

// QueryMsgByOffset ports DefaultMQPullConsumer.pull(mq, "*", offset, 1) used by
// collectConsumerOffset to compute storetime latency. The PULL_MESSAGE response
// body is a binary message batch (NOT JSON); we extract only the first message's
// store timestamp via a minimal, dependency-free reader (the field sits in the
// fixed message header, before any compressed payload).
func (a *AdminClient) QueryMsgByOffset(brokerAddr string, mq MessageQueue, offset int64) (*PullResult, error) {
	hdr := pullHeader{
		consumerGroup:       "TOOLS_CONSUMER", // MixAll.TOOLS_CONSUMER_GROUP — matches the Java exporter's pullConsumer
		topic:               mq.Topic,
		queueId:        mq.QueueId,
		queueOffset:    offset,
		maxMsgNums:     1,
		sysFlag:        flagSubscription,
		commitOffset:   0,
		suspendTimeoutMillis: 0,
		subscription:   "*",
		subVersion:     0,
		expressionType: "TAG",
	}
	cmd := rmqremote.NewRemotingCommand(rmqremote.RequestPullMessage, hdr, nil)
	a.signIfACL(cmd)
	resp, err := a.rc.InvokeSync(brokerAddr, cmd, a.timeout)
	if err != nil {
		return nil, fmt.Errorf("pull %s: %w", brokerAddr, err)
	}
	pr := &PullResult{Status: pullStatusFromCode(int(resp.Code)), MinOffset: respExtInt64(resp, "minOffset")}
	if pr.Status == pullFound && len(resp.Body) > 0 {
		if ts, ok := firstStoreTimestamp(resp.Body); ok {
			pr.StoreTimestamp = ts
		}
	}
	return pr, nil
}

func pullStatusFromCode(code int) string {
	switch code {
	case rmqremote.ResponseSuccess:
		return pullFound
	case 19: // PULL_NOT_FOUND
		return pullNoNewMsg
	case 20: // PULL_RETRY_IMMEDIATELY
		return pullNoMatchedMsg
	case 21: // PULL_OFFSET_MOVED
		return pullOffsetIllegal
	default:
		return pullUnknown
	}
}

// firstStoreTimestamp reads the storeTimestamp of the first message in a binary
// message batch (format from rocketmq-client-go DecodeMessage). Field offsets:
// storeSize(4) magic(4) bodyCRC(4) queueId(4) flag(4) queueOffset(8)
// commitLogOffset(8) sysFlag(4) bornTimestamp(8) bornHost(8 v4 | 20 v6)
// storeTimestamp(8). sysFlag bit FlagBornHostV6 selects the born-host size.
func firstStoreTimestamp(body []byte) (int64, bool) {
	const sysFlagOff = 4 + 4 + 4 + 4 + 4 + 8 + 8 // 36
	if len(body) < sysFlagOff+4 {
		return 0, false
	}
	sysFlag := binary.BigEndian.Uint32(body[sysFlagOff:])
	tsOff := sysFlagOff + 4 + 8 // +sysFlag(4) +bornTimestamp(8) = 48
	if sysFlag&flagBornHostV6 != 0 {
		tsOff += 20 // v6 born host (16 addr + 4 port)
	} else {
		tsOff += 8 // v4 born host (4 addr + 4 port)
	}
	if len(body) < tsOff+8 {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(body[tsOff:])), true
}

// respExtInt64 reads an int64 ext field from a RemotingCommand response.
func respExtInt64(resp *rmqremote.RemotingCommand, key string) int64 {
	if resp.ExtFields == nil {
		return 0
	}
	v, ok := resp.ExtFields[key]
	if !ok {
		return 0
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0
	}
	return n
}
