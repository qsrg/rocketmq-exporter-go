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

import "testing"

func TestDedupConnectionsByClientId(t *testing.T) {
	conns := []Connection{
		{ClientAddr: "127.0.0.1:1", ClientId: "host@1"},
		{ClientAddr: "127.0.0.1:2", ClientId: "host@1"}, // dup: same consumer, other broker
		{ClientAddr: "127.0.0.1:3", ClientId: "host@2"},
		{ClientAddr: "127.0.0.1:4", ClientId: ""}, // blank ClientId kept as-is
	}
	got := dedupConnectionsByClientId(conns)
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3 (host@1 once, host@2, blank): %+v", len(got), got)
	}
	if got[0].ClientAddr != "127.0.0.1:1" {
		t.Errorf("first occurrence kept = %s, want 127.0.0.1:1", got[0].ClientAddr)
	}
	// nil / single-element passthrough.
	if got := dedupConnectionsByClientId(nil); len(got) != 0 {
		t.Errorf("nil -> %d, want 0", len(got))
	}
	if got := dedupConnectionsByClientId(conns[:1]); len(got) != 1 {
		t.Errorf("single -> %d, want 1", len(got))
	}
}
