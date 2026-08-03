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

package health

import "testing"

func TestSplitNamesrv(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []string
	}{
		{"127.0.0.1:9876", []string{"127.0.0.1:9876"}},
		{"127.0.0.1:9876;127.0.0.1:9877", []string{"127.0.0.1:9876", "127.0.0.1:9877"}}, // semicolon
		{"127.0.0.1:9876,127.0.0.1:9877", []string{"127.0.0.1:9876", "127.0.0.1:9877"}}, // comma
		{" 127.0.0.1:9876 ; 127.0.0.1:9877 ", []string{"127.0.0.1:9876", "127.0.0.1:9877"}},
		{"", []string{}},
	} {
		got := splitNamesrv(tc.in)
		if len(got) != len(tc.want) {
			t.Errorf("splitNamesrv(%q) = %v, want %v", tc.in, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("splitNamesrv(%q)[%d] = %q, want %q", tc.in, i, got[i], tc.want[i])
			}
		}
	}
}
