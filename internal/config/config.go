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

// Package config loads exporter configuration from CLI flags with environment
// variable fallbacks, replacing the Java exporter's Spring application.yml.
// It preserves the Java semantic knobs (namesrv, enable-collect, ACL, cache TTL,
// the six cron expressions, pool sizing, listen address, telemetry path) but
// is free to rename the keys — see design D4.
package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

// Config holds all exporter settings. Defaults follow design D4 / the Java
// application.yml (worker pool sizes from the yml, not the Java class defaults).
type Config struct {
	Namesrv        string
	Listen         string
	TelemetryPath  string
	EnableCollect  bool
	EnableACL      bool
	AccessKey      string
	SecretKey      string
	CacheTTL       time.Duration
	Cron           CronConfig
	Pool           PoolConfig
}

// CronConfig holds the six collection-task cron expressions (6-field, seconds
// precision). Expressions are stored verbatim from config and translated (?->*)
// at scheduler-load time by TranslateCron.
type CronConfig struct {
	CollectTopicOffset        string
	CollectProducer            string
	CollectConsumerOffset      string
	CollectBrokerStatsTopic    string
	CollectBrokerStats         string
	CollectBrokerRuntimeStats  string
}

// PoolConfig holds the bounded worker-pool sizing.
type PoolConfig struct {
	Core  int
	Max   int
	Queue int
}

// Default returns the modernized defaults (design D4). The HTTP port differs
// from the Java exporter (19876 -> :5557); this is recorded in the change.
func Default() Config {
	return Config{
		Namesrv:       "127.0.0.1:9876",
		Listen:        ":5557",
		TelemetryPath: "/metrics",
		EnableCollect:  true,
		EnableACL:      false,
		CacheTTL:       60 * time.Second,
		Cron: CronConfig{
			CollectTopicOffset:        "15 0/1 * * * ?",
			CollectProducer:            "15 0/1 * * * ?",
			CollectConsumerOffset:       "15 0/1 * * * ?",
			CollectBrokerStatsTopic:    "15 0/1 * * * ?",
			CollectBrokerStats:         "15 0/1 * * * ?",
			CollectBrokerRuntimeStats:  "15 0/1 * * * ?",
		},
		Pool: PoolConfig{Core: 10, Max: 10, Queue: 5000},
	}
}

// RegisterFlags wires every config field to a flag, with the env-var fallback
// applied as the flag default so `--flag` still overrides the environment.
func RegisterFlags(fs *flag.FlagSet, envPrefix string) *Config {
	c := Default()
	env := func(name, def string) string {
		if v, ok := os.LookupEnv(envPrefix + name); ok {
			return v
		}
		return def
	}
	envBool := func(name string, def bool) bool {
		if v, ok := os.LookupEnv(envPrefix + name); ok {
			b, err := strconv.ParseBool(v)
			if err == nil {
				return b
			}
		}
		return def
	}
	envInt := func(name string, def int) int {
		if v, ok := os.LookupEnv(envPrefix + name); ok {
			if n, err := strconv.Atoi(v); err == nil {
				return n
			}
		}
		return def
	}
	envDur := func(name string, def time.Duration) time.Duration {
		if v, ok := os.LookupEnv(envPrefix + name); ok {
			if d, err := time.ParseDuration(v); err == nil {
				return d
			}
		}
		return def
	}

	fs.StringVar(&c.Namesrv, "namesrv", env("NAMESRV_ADDR", c.Namesrv),
		"RocketMQ nameserver address list (comma-separated)")
	fs.StringVar(&c.Listen, "listen", env("LISTEN", c.Listen),
		"HTTP listen address")
	fs.StringVar(&c.TelemetryPath, "telemetry-path", env("TELEMETRY_PATH", c.TelemetryPath),
		"Prometheus metrics path")
	fs.BoolVar(&c.EnableCollect, "enable-collect", envBool("ENABLE_COLLECT", c.EnableCollect),
		"enable scheduled collection tasks")
	fs.BoolVar(&c.EnableACL, "enable-acl", envBool("ENABLE_ACL", c.EnableACL),
		"attach ACL signature to remoting commands")
	fs.StringVar(&c.AccessKey, "access-key", env("ACCESS_KEY", c.AccessKey),
		"ACL access key (requires --enable-acl)")
	fs.StringVar(&c.SecretKey, "secret-key", env("SECRET_KEY", c.SecretKey),
		"ACL secret key (requires --enable-acl)")
	fs.DurationVar(&c.CacheTTL, "cache-ttl", envDur("CACHE_TTL", c.CacheTTL),
		"TTL of the in-memory metric store")
	fs.IntVar(&c.Pool.Core, "pool-core", envInt("POOL_CORE", c.Pool.Core),
		"worker pool core size")
	fs.IntVar(&c.Pool.Max, "pool-max", envInt("POOL_MAX", c.Pool.Max),
		"worker pool max size")
	fs.IntVar(&c.Pool.Queue, "pool-queue", envInt("POOL_QUEUE", c.Pool.Queue),
		"worker pool queue size")

	fs.StringVar(&c.Cron.CollectTopicOffset, "cron.collectTopicOffset",
		env("CRON_COLLECT_TOPIC_OFFSET", c.Cron.CollectTopicOffset),
		"cron for collectTopicOffset (6-field, ? allowed)")
	fs.StringVar(&c.Cron.CollectProducer, "cron.collectProducer",
		env("CRON_COLLECT_PRODUCER", c.Cron.CollectProducer), "cron for collectProducer")
	fs.StringVar(&c.Cron.CollectConsumerOffset, "cron.collectConsumerOffset",
		env("CRON_COLLECT_CONSUMER_OFFSET", c.Cron.CollectConsumerOffset), "cron for collectConsumerOffset")
	fs.StringVar(&c.Cron.CollectBrokerStatsTopic, "cron.collectBrokerStatsTopic",
		env("CRON_COLLECT_BROKER_STATS_TOPIC", c.Cron.CollectBrokerStatsTopic), "cron for collectBrokerStatsTopic")
	fs.StringVar(&c.Cron.CollectBrokerStats, "cron.collectBrokerStats",
		env("CRON_COLLECT_BROKER_STATS", c.Cron.CollectBrokerStats), "cron for collectBrokerStats / collectBrokerGroupStats")
	fs.StringVar(&c.Cron.CollectBrokerRuntimeStats, "cron.collectBrokerRuntimeStats",
		env("CRON_COLLECT_BROKER_RUNTIME_STATS", c.Cron.CollectBrokerRuntimeStats), "cron for collectBrokerRuntimeStats")
	return &c
}

// TranslateCron converts a 6-field cron expression with '?' (legal in Quartz but
// not in robfig/cron/v3) by substituting '?' -> '*'. This mirrors CLAUDE.md's
// '? -> *' load-time rule. It does NOT validate the expression; use ValidateCron
// (or the scheduler) to fail fast on invalid input.
func TranslateCron(expr string) string {
	return strings.ReplaceAll(expr, "?", "*")
}

// ValidateCron fails fast if a translated cron expression is not parseable by
// robfig/cron/v3 with seconds precision. It names the offending task.
func ValidateCron(taskName, expr string) error {
	parser := cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	if _, err := parser.Parse(TranslateCron(expr)); err != nil {
		return fmt.Errorf("invalid cron for %s (%q): %w", taskName, expr, err)
	}
	return nil
}

// ValidateAll validates every cron expression in the config, failing fast at
// startup with the first offending task named.
func (c *Config) ValidateAll() error {
	tasks := []struct {
		name, expr string
	}{
		{"collectTopicOffset", c.Cron.CollectTopicOffset},
		{"collectProducer", c.Cron.CollectProducer},
		{"collectConsumerOffset", c.Cron.CollectConsumerOffset},
		{"collectBrokerStatsTopic", c.Cron.CollectBrokerStatsTopic},
		{"collectBrokerStats", c.Cron.CollectBrokerStats},
		{"collectBrokerRuntimeStats", c.Cron.CollectBrokerRuntimeStats},
	}
	for _, t := range tasks {
		if err := ValidateCron(t.name, t.expr); err != nil {
			return err
		}
	}
	return nil
}
