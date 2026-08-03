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

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"

	"github.com/qsrg/rocketmq-exporter-go/internal/config"
	"github.com/qsrg/rocketmq-exporter-go/internal/service"
)

// Adapter wires the real rocketmq-client-go producer/consumer and the admin
// client into the Prober's injected interfaces. It is the production counterpart
// to the test stubs in probe_test.go. Producer/consumer usage mirrors the
// verified internal/service/probe_live_test.go pattern (WithRetry(2), ACL
// credentials shared with the admin client).
type Adapter struct {
	namesrv   []string
	enableACL bool
	accessKey string
	secretKey string
	prod      Producer
}

// NewAdapter builds the shared health-check producer. The consumer factory and
// cluster lister are exposed by their methods for wiring into NewProber.
func NewAdapter(cfg *config.Config) (*Adapter, error) {
	namesrv := splitNamesrv(cfg.Namesrv)
	popts := []producer.Option{
		producer.WithNameServer(namesrv),
		producer.WithGroupName("rmq-exporter-health-producer"),
		producer.WithInstanceName("rmq-exporter-health-producer"),
		producer.WithRetry(2),
	}
	if cfg.EnableACL {
		popts = append(popts, producer.WithCredentials(primitive.Credentials{
			AccessKey: cfg.AccessKey,
			SecretKey: cfg.SecretKey,
		}))
	}
	p, err := producer.NewDefaultProducer(popts...)
	if err != nil {
		return nil, fmt.Errorf("health producer: %w", err)
	}
	return &Adapter{
		namesrv:   namesrv,
		enableACL: cfg.EnableACL,
		accessKey: cfg.AccessKey,
		secretKey: cfg.SecretKey,
		prod:      p,
	}, nil
}

// Producer returns the shared producer (satisfies health.Producer).
func (a *Adapter) Producer() Producer { return a.prod }

// ConsumerFactory returns a factory that builds a per-cluster push consumer
// bound to the given group. The prober calls Subscribe + Start after construction.
func (a *Adapter) ConsumerFactory() ConsumerFactory {
	return func(group string) (Consumer, error) {
		copts := []consumer.Option{
			consumer.WithNameServer(a.namesrv),
			consumer.WithGroupName(group),
			consumer.WithInstance("rmq-exporter-health-consumer"),
		}
		if a.enableACL {
			copts = append(copts, consumer.WithCredentials(primitive.Credentials{
				AccessKey: a.accessKey,
				SecretKey: a.secretKey,
			}))
		}
		return consumer.NewPushConsumer(copts...)
	}
}

// adminLister adapts *service.AdminClient to health.ClusterLister by reading the
// ClusterAddrTable from the broker topology RPC.
type adminLister struct{ a *service.AdminClient }

// ListClusters returns the cluster names from ExamineBrokerClusterInfo.
func (l adminLister) ListClusters(_ context.Context) ([]string, error) {
	ci, err := l.a.ExamineBrokerClusterInfo()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(ci.ClusterAddrTable))
	for name := range ci.ClusterAddrTable {
		out = append(out, name)
	}
	return out, nil
}

// NewClusterLister wraps an admin client as a ClusterLister for NewProber.
func NewClusterLister(a *service.AdminClient) ClusterLister { return adminLister{a: a} }

// splitNamesrv splits the namesrv string the config carries into the []string
// rocketmq-client-go expects. Both ',' and ';' are accepted as separators --
// RocketMQ/Java configs commonly use ';' (e.g. "ip1:9876;ip2:9876").
func splitNamesrv(s string) []string {
	s = strings.ReplaceAll(s, ";", ",")
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
