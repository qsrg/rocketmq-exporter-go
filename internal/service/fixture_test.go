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
	"os"
	"path/filepath"
	"testing"

	"github.com/wcf/rmq-exporter/internal/rmqremote"
)

// writeFixture writes a captured raw response body to testdata/<name>.
func writeFixture(name string, body []byte) error {
	if err := os.MkdirAll("testdata", 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join("testdata", name), body, 0o644)
}

func loadFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Skipf("fixture %s missing (run with RMQ_LIVE_TESTS=1 to capture): %v", name, err)
	}
	return b
}

// TestDecodeClusterInfoFromFixture decodes a captured GET_BROKER_CLUSTER_INFO
// body offline, asserting the Go struct matches the Java ClusterInfo shape.
func TestDecodeClusterInfoFromFixture(t *testing.T) {
	body := loadFixture(t, "cluster_info.json")
	var ci ClusterInfo
	if err := rmqremote.UnmarshalJSON(body, &ci); err != nil {
		t.Fatalf("unmarshal ClusterInfo: %v", err)
	}
	if len(ci.BrokerAddrTable) == 0 {
		t.Fatal("BrokerAddrTable empty in fixture")
	}
	for name, bd := range ci.BrokerAddrTable {
		if bd.BrokerName == "" {
			t.Errorf("broker %q has empty BrokerName", name)
		}
		if len(bd.BrokerAddrs) == 0 {
			t.Errorf("broker %q has no BrokerAddrs", name)
		}
		// MASTER_ID = 0 must be present (Java MixAll.MASTER_ID).
		if _, ok := bd.BrokerAddrs[0]; !ok {
			t.Errorf("broker %q missing master (id=0) address", name)
		}
	}
}
