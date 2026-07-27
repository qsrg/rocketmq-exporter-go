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

package rmqremote

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestNormalizeKeys(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		// bare numeric keys (Map<Long,V>)
		{"bare numeric key", `{"brokerAddrs":{0:"127.0.0.1:10911"}}`, `{"brokerAddrs":{"0":"127.0.0.1:10911"}}`},
		{"two numeric keys", `{0:"a",1:"b"}`, `{"0":"a","1":"b"}`},
		{"negative numeric key", `{-1:"x"}`, `{"-1":"x"}`},
		// bare composite-object keys (Map<MessageQueue,X>) — from a real 4.9.8 broker
		{"composite object key",
			`{"offsetTable":{{"brokerName":"b","queueId":0,"topic":"t"}:{"maxOffset":1}}}`,
			`{"offsetTable":{"{\"brokerName\":\"b\",\"queueId\":0,\"topic\":\"t\"}":{"maxOffset":1}}}`},
		// colons/braces inside string values must NOT be treated as keys
		{"colon in string value", `{"brokerAddrs":{0:"127.0.0.1:10911",2:"1,2:3"}}`,
			`{"brokerAddrs":{"0":"127.0.0.1:10911","2":"1,2:3"}}`},
		// already-quoted keys untouched
		{"already-quoted numeric key", `{"k":{"0":"v"}}`, `{"k":{"0":"v"}}`},
		{"string keys untouched", `{"topic":"t","broker":"b"}`, `{"topic":"t","broker":"b"}`},
		// arrays of numbers are values, not keys — must not be mangled
		{"array of numbers not mangled", `{"a":[1,2,3]}`, `{"a":[1,2,3]}`},
		{"escaped quote in string", `{"k":"a\"b:{0:"}`, `{"k":"a\"b:{0:"}`},
		{"no keys at all", `{"a":1}`, `{"a":1}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := string(normalizeKeys([]byte(c.in)))
			if got != c.want {
				t.Errorf("normalizeKeys(%q)\n  got  = %q\n  want = %q", c.in, got, c.want)
			}
		})
	}
}

func TestUnmarshalJSONNumericKeys(t *testing.T) {
	body := []byte(`{"brokerAddrTable":{"broker-a":{"brokerAddrs":{0:"127.0.0.1:10911",1:"127.0.0.1:10912"},"brokerName":"broker-a","cluster":"DefaultCluster"}}}`)
	var v struct {
		BrokerAddrTable map[string]struct {
			BrokerAddrs map[int64]string `json:"brokerAddrs"`
		} `json:"brokerAddrTable"`
	}
	if err := UnmarshalJSON(body, &v); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}
	want := map[int64]string{0: "127.0.0.1:10911", 1: "127.0.0.1:10912"}
	got := v.BrokerAddrTable["broker-a"].BrokerAddrs
	if !reflect.DeepEqual(got, want) {
		t.Errorf("BrokerAddrs = %v, want %v", got, want)
	}
}

// TestUnmarshalJSONCompositeKey decodes a real-shape ConsumeStats body whose
// offsetTable is a Map<MessageQueue,OffsetWrapper> (composite object keys) into
// a map[string]json.RawMessage, then parses one key into a MessageQueue.
func TestUnmarshalJSONCompositeKey(t *testing.T) {
	body := []byte(`{"consumeTps":0.0,"offsetTable":{{"brokerName":"b","queueId":0,"topic":"t"}:{"brokerOffset":0,"consumerOffset":21,"lastTimestamp":0}}}`)
	var cs struct {
		ConsumeTps  float64                  `json:"consumeTps"`
		OffsetTable map[string]json.RawMessage `json:"offsetTable"`
	}
	if err := UnmarshalJSON(body, &cs); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}
	if cs.ConsumeTps != 0.0 {
		t.Errorf("ConsumeTps = %v, want 0", cs.ConsumeTps)
	}
	if len(cs.OffsetTable) != 1 {
		t.Fatalf("OffsetTable len = %d, want 1", len(cs.OffsetTable))
	}
	for keyRaw, valRaw := range cs.OffsetTable {
		var mq struct {
			BrokerName string `json:"brokerName"`
			QueueId    int    `json:"queueId"`
			Topic      string `json:"topic"`
		}
		if err := json.Unmarshal([]byte(keyRaw), &mq); err != nil {
			t.Fatalf("parse key %q: %v", keyRaw, err)
		}
		if mq.BrokerName != "b" || mq.Topic != "t" || mq.QueueId != 0 {
			t.Errorf("key parsed wrong: %+v", mq)
		}
		var ow struct {
			ConsumerOffset int64 `json:"consumerOffset"`
		}
		if err := json.Unmarshal(valRaw, &ow); err != nil {
			t.Fatalf("parse value: %v", err)
		}
		if ow.ConsumerOffset != 21 {
			t.Errorf("ConsumerOffset = %d, want 21", ow.ConsumerOffset)
		}
	}
}

func TestDebugNormalizeRoute(t *testing.T) {
	body := []byte(`{"brokerDatas":[{"brokerAddrs":{0:"127.0.0.1:20911"},"brokerName":"broker-b","cluster":"DefaultCluster"},{"brokerAddrs":{0:"127.0.0.1:10911"},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[]}`)
	got := string(normalizeKeys(body))
	t.Logf("normalized:\n%s", got)
	var tr struct{}
	_ = tr
}
