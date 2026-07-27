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
	"fmt"

	"github.com/wcf/rmq-exporter/internal/model"
	"github.com/wcf/rmq-exporter/internal/rmqremote"
)

// kvTable is the wire wrapper for GET_BROKER_RUNTIME_INFO (28) and other KV
// responses: {"table": { "key": "value", ... }}. The inner map is what
// model.NewBrokerRuntimeStats consumes.
type kvTable struct {
	Table map[string]string `json:"table"`
}

// FetchBrokerRuntimeStats fetches a broker's runtime KVTable (GET_BROKER_RUNTIME_INFO,
// code 28) and parses it into a BrokerRuntimeStats, the input to the ~63
// broker-runtime gauges. Sent to a broker (master) address, no request header.
func (a *AdminClient) FetchBrokerRuntimeStats(brokerAddr string) (*model.BrokerRuntimeStats, error) {
	var kv kvTable
	if _, err := a.invokeSyncBroker(brokerAddr, rmqremote.RequestGetBrokerRuntimeInfo, nil, &kv); err != nil {
		return nil, err
	}
	if kv.Table == nil {
		return nil, fmt.Errorf("fetchBrokerRuntimeStats %s: empty KVTable", brokerAddr)
	}
	return model.NewBrokerRuntimeStats(kv.Table)
}
