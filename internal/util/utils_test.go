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

package util

import (
	"math"
	"testing"
)

func TestGetFixedDouble(t *testing.T) {
	cases := []struct {
		name string
		in   float64
		want float64
	}{
		// Java DecimalFormat("#.##") is HALF_UP applied to the *exact* value of
		// the double, not the literal. Two consequences worth pinning down:
		//  - 1.235: nearest double is 1.2350000000000000976 (ABOVE 1.235) -> 1.24.
		//  - 1.005: nearest double is 1.0049999999999998935 (BELOW 1.005) -> 1.00,
		//    the counterintuitive result naive HALF_UP would not give.
		{"1.236 -> 1.24", 1.236, 1.24},
		{"1.235 double above -> 1.24", 1.235, 1.24},
		{"1.244 -> 1.24", 1.244, 1.24},
		{"1.245 double above -> 1.25", 1.245, 1.25},
		{"1.005 double below -> 1.00 (fidelity case)", 1.005, 1.00},
		{"2.5 -> 2.5", 2.5, 2.5},
		{"negative half-up away from zero: -1.235 -> -1.24", -1.235, -1.24},
		{"0 -> 0", 0, 0},
		{"1234.567 -> 1234.57", 1234.567, 1234.57},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := GetFixedDouble(c.in)
			if math.Abs(got-c.want) > 1e-9 {
				t.Errorf("GetFixedDouble(%v) = %v, want %v", c.in, got, c.want)
			}
		})
	}
}

func TestMachineReadableByteCount(t *testing.T) {
	cases := []struct {
		in   string
		want float64
	}{
		{"512 B", 512},
		{"1024 KB", 1024 * 1024},
		{"1.5 GB", 1.5 * 1024 * 1024 * 1024},
		{"2 TB", 2 * 1024 * 1024 * 1024 * 1024},
		{"1 MB", 1024 * 1024},
	}
	const eps = 1e-3
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got := MachineReadableByteCount(c.in)
			if math.Abs(got-c.want) > eps {
				t.Errorf("MachineReadableByteCount(%q) = %v, want %v", c.in, got, c.want)
			}
		})
	}
}

func TestClientAddresses(t *testing.T) {
	cases := []struct {
		addrs, ids       []string
		wantAddrs, wantIDs string
	}{
		{[]string{"1.1.1.1:80", "2.2.2.2:80"}, []string{"c1", "c2"}, "1.1.1.1:80,2.2.2.2:80", "c1,c2"},
		{[]string{}, []string{}, "", ""},
		{[]string{"a"}, []string{"b"}, "a", "b"},
	}
	for _, c := range cases {
		a, i := ClientAddresses(c.addrs, c.ids)
		if a != c.wantAddrs || i != c.wantIDs {
			t.Errorf("ClientAddresses(%v,%v) = (%q,%q), want (%q,%q)", c.addrs, c.ids, a, i, c.wantAddrs, c.wantIDs)
		}
	}
}
