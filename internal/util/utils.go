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

// Package util ports the pure helpers from the Java exporter's
// util/Utils.java, verbatim in semantics so that metric values match the
// Java output byte-for-byte at the same precision.
package util

import (
	"math/big"
	"strconv"
	"strings"
)

// GetFixedDouble replicates Java Utils.getFixedDouble: DecimalFormat("#.##"),
// whose default RoundingMode is HALF_UP applied to the *exact* decimal value of
// the double (so e.g. 1.235 -> 1.23, matching the Java exporter, because the
// double 1.235 is actually 1.2349999...). We round half away from zero on the
// exact rational of the input via math/big to avoid Go's round-half-to-even.
func GetFixedDouble(value float64) float64 {
	// Exact rational of the float64 (SetFloat64 is exact for all float64).
	r := new(big.Rat).SetFloat64(value)
	r.Mul(r, big.NewRat(100, 1))
	neg := r.Sign() < 0
	abs := new(big.Rat).Abs(r)
	abs.Add(abs, big.NewRat(1, 2)) // floor(abs + 1/2) == HALF_UP for positive
	n := new(big.Int).Quo(abs.Num(), abs.Denom())
	out := new(big.Rat).SetFrac(n, big.NewInt(100))
	if neg {
		out.Neg(out)
	}
	f, _ := out.Float64()
	return f
}

// unitPrefixes mirrors the Java "KMGTPE" string: K=1, M=2, G=3, T=4, P=5, E=6
// (exponent of 1024).
const unitPrefixes = "KMGTPE"

// MachineReadableByteCount replicates Java Utils.machineReadableByteCount: it
// parses a "<number> <unit>" human-readable byte string (e.g. "1.5 GB",
// "512 B", "1024 KB") into bytes. "B" returns the base value as-is; a unit
// whose first char is one of K M G T P E multiplies by 1024^exp.
func MachineReadableByteCount(humanReadableValue string) float64 {
	parts := strings.Split(humanReadableValue, " ")
	// Java: valueArray[0] base, valueArray[1] unit; panics on malformed input.
	base, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		panic("machineReadableByteCount: invalid base: " + humanReadableValue)
	}
	unit := parts[1]
	if unit == "B" {
		return base
	}
	idx := strings.IndexByte(unitPrefixes, unit[0])
	if idx < 0 {
		panic("machineReadableByteCount: unknown unit: " + unit)
	}
	exp := idx + 1
	var m float64 = 1024
	for i := 1; i < exp; i++ {
		m *= 1024
	}
	return base * m
}
