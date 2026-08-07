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

// Command rmq-exporter serves Prometheus metrics for an Apache RocketMQ 4.x
// cluster. Phase 1 scope: a single GET /metrics endpoint plus six cron
// collection tasks over a vendored remoting client. See
// openspec/changes/phase1-metrics-exporter/.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/robfig/cron/v3"

	dto "github.com/prometheus/client_model/go"

	"github.com/qsrg/rocketmq-exporter-go/internal/collector"
	"github.com/qsrg/rocketmq-exporter-go/internal/config"
	"github.com/qsrg/rocketmq-exporter-go/internal/health"
	"github.com/qsrg/rocketmq-exporter-go/internal/service"
	"github.com/qsrg/rocketmq-exporter-go/internal/task"
)

func main() {
	if err := run(); err != nil {
		slog.Error("rmq-exporter exited with error", "err", err)
		os.Exit(1)
	}
}

func run() error {
	// Register --config flag for help text; actual value obtained via
	// FindConfigPath() before flag.Parse() so YAML values can influence
	// the flag defaults.
	var configPathVar string
	flag.StringVar(&configPathVar, "config", os.Getenv("RMQ_CONFIG"), "path to YAML config file (env: RMQ_CONFIG)")

	// Priority chain: flag > env > config file > default.
	configPath := config.FindConfigPath()
	base := config.Default()
	if configPath != "" {
		if err := config.OverlayFile(&base, configPath); err != nil {
			return fmt.Errorf("config file %s: %w", configPath, err)
		}
		slog.Info("loaded config file", "path", configPath)
	}
	cfg := config.RegisterFlags(flag.CommandLine, "RMQ_", &base)
	flag.Parse()
	if err := cfg.ValidateAll(); err != nil {
		return err
	}

	// Go runtime soft memory limit (GOMEMLIMIT equivalent). Off by default; set
	// via --go-mem-limit / RMQ_GO_MEM_LIMIT / go_mem_limit in the config file.
	if limit, err := config.ParseMemLimit(cfg.GoMemLimit); err != nil {
		return fmt.Errorf("go-mem-limit: %w", err)
	} else if limit > 0 {
		debug.SetMemoryLimit(limit)
	}

	slog.Info("rmq-exporter starting",
		"namesrv", cfg.Namesrv, "listen", cfg.Listen, "telemetry", cfg.TelemetryPath,
		"enableCollect", cfg.EnableCollect, "enableACL", cfg.EnableACL, "cacheTTL", cfg.CacheTTL,
		"goMemLimit", cfg.GoMemLimit)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Admin client (read RPCs; ACL deferred — see defer-acl-phase1).
	admin := service.NewAdminClient(cfg.Namesrv, cfg.EnableACL, cfg.AccessKey, cfg.SecretKey, 10*time.Second)
	if err := admin.Start(ctx); err != nil {
		return err
	}
	defer admin.Shutdown(context.Background())

	// Metric store + collector (a prometheus.Gatherer).
	coll := collector.New(cfg.CacheTTL)

	// Bounded worker pool for client-metric collection (discard-oldest).
	pool := task.New(cfg.Pool.Max, cfg.Pool.Queue)
	pool.Start(ctx)
	defer pool.Shutdown(context.Background())

	ct := &task.CollectTask{
		Admin:         admin,
		Coll:          coll,
		EnableCollect: cfg.EnableCollect,
		Pool:          pool,
	}

	// Cron scheduler (6-field, seconds precision; '?' -> '*' at load).
	sched := cron.New(cron.WithSeconds(), cron.WithLogger(cron.PrintfLogger(new(cronLogger))))
	if err := registerJobs(sched, ct, cfg.Cron); err != nil {
		return err
	}
	sched.Start()
	defer sched.Stop()

	// HTTP: single /metrics endpoint. A custom encoder (rather than
	// promhttp.HandlerFor) is used so empty gauge families still emit their
	// # HELP / # TYPE lines, matching the Java exporter's simpleclient output
	// (promhttp.HandlerFor drops empty families, which would lose parity).
	mux := http.NewServeMux()
	mux.HandleFunc(cfg.TelemetryPath, func(w http.ResponseWriter, r *http.Request) {
		families, err := coll.Gather()
		if err != nil {
			slog.Error("gather metrics", "err", err)
			http.Error(w, "gather error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		encodeMetricFamilies(w, families)
	})

	// Active end-to-end cluster health probe (Go-only; cluster-health-check
	// capability). When disabled, /healthz is simply not registered (404).
	if cfg.HealthCheck.Enabled {
		hca, err := health.NewAdapter(cfg)
		if err != nil {
			return fmt.Errorf("health adapter: %w", err)
		}
		prober := health.NewProber(cfg.HealthCheck, health.NewClusterLister(admin), coll,
			hca.Producer(), hca.ConsumerFactory(), nil)
		if err := prober.Start(ctx); err != nil {
			return fmt.Errorf("health prober start: %w", err)
		}
		defer prober.Shutdown(context.Background())
		mux.Handle(cfg.HealthCheck.Path, health.HealthzHandler(coll))
		slog.Info("cluster health check enabled",
			"path", cfg.HealthCheck.Path, "rate", cfg.HealthCheck.Rate,
			"recency", cfg.HealthCheck.Recency, "clusterRefresh", cfg.HealthCheck.ClusterRefresh)
	}

	srv := &http.Server{Addr: cfg.Listen, Handler: mux}
	go func() {
		slog.Info("serving metrics", "addr", cfg.Listen, "path", cfg.TelemetryPath)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server error", "err", err)
		}
	}()
	defer srv.Shutdown(context.Background())

	<-ctx.Done()
	slog.Info("rmq-exporter shutting down")
	return nil
}

func registerJobs(s *cron.Cron, ct *task.CollectTask, c config.CronConfig) error {
	jobs := []struct {
		name, spec string
		fn         func(context.Context)
	}{
		{"collectTopicOffset", c.CollectTopicOffset, ct.CollectTopicOffset},
		{"collectProducer", c.CollectProducer, ct.CollectProducer},
		{"collectConsumerOffset", c.CollectConsumerOffset, ct.CollectConsumerOffset},
		{"collectBrokerStatsTopic", c.CollectBrokerStatsTopic, ct.CollectBrokerStatsTopic},
		{"collectBrokerStats", c.CollectBrokerStats, ct.CollectBrokerStats},
		// collectBrokerGroupStats shares the collectBrokerStats cron slot (Java).
		{"collectBrokerRuntimeStats", c.CollectBrokerRuntimeStats, ct.CollectBrokerRuntimeStats},
	}
	ctx := context.Background()
	for _, j := range jobs {
		spec := config.TranslateCron(j.spec)
		if _, err := s.AddFunc(spec, func() { j.fn(ctx) }); err != nil {
			return err
		}
	}
	// collectBrokerGroupStats shares the collectBrokerStats cron expression.
	if _, err := s.AddFunc(config.TranslateCron(c.CollectBrokerStats), func() { ct.CollectBrokerGroupStats(ctx) }); err != nil {
		return err
	}
	return nil
}

// cronLogger adapts slog to robfig/cron's minimal logger interface.
type cronLogger struct{}

func (c *cronLogger) Printf(format string, args ...any) {
	slog.Info(format, "args", args)
}

// encodeMetricFamilies writes families in Prometheus text exposition format. The
// expfmt text encoder REJECTS families with zero metrics (returns an error), but
// the Java exporter emits empty families as bare # HELP / # TYPE lines; those are
// emitted manually here. The # TYPE line uses MetricFamily.GetType() (not a
// hardcoded "gauge") so an empty counter family is correctly labeled "counter" -
// required now that the cluster-health-check capability adds counter families.
func encodeMetricFamilies(w io.Writer, families []*dto.MetricFamily) {
	enc := expfmt.NewEncoder(w, expfmt.FmtText)
	for _, mf := range families {
		if len(mf.Metric) == 0 {
			typ := strings.ToLower(mf.GetType().String())
			fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", mf.GetName(), mf.GetHelp(), mf.GetName(), typ)
			continue
		}
		if err := enc.Encode(mf); err != nil {
			slog.Error("encode metric family", "name", mf.GetName(), "err", err)
			return
		}
	}
}
