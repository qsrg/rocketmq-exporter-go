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
	"gopkg.in/yaml.v3"
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
	HealthCheck    HealthCheckConfig
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

// HealthCheckConfig configures the active end-to-end cluster health probe
// (cluster-health-check capability). Go-only addition; no Java equivalent.
// See openspec/changes/cluster-health-check/.
type HealthCheckConfig struct {
	Enabled        bool          // master switch
	TopicPrefix    string        // topic = TopicPrefix + cluster (per-cluster routing)
	GroupPrefix    string        // consumer group = GroupPrefix + cluster
	Rate           float64       // produce rate (msgs/sec/cluster)
	Recency        time.Duration // status recency threshold
	ClusterRefresh time.Duration // cluster discovery refresh interval
	Path           string        // HTTP path for the /healthz endpoint
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
		HealthCheck: HealthCheckConfig{
			Enabled:        true,
			TopicPrefix:    "HealthCheckTopic-",
			GroupPrefix:    "HealthCheckGroup-",
			Rate:           2.0,
			Recency:        5 * time.Second,
			ClusterRefresh: 5 * time.Minute,
			Path:           "/healthz",
		},
	}
}

// RegisterFlags wires every config field to a flag, with the env-var fallback
// applied as the flag default so `--flag` still overrides the environment.
// If base is non-nil, its values serve as the starting point (typically the
// result of Default() overlaid with a YAML config file), giving the priority
// chain: flag > env > file > default. Passing nil is equivalent to passing
// &Default().
func RegisterFlags(fs *flag.FlagSet, envPrefix string, base *Config) *Config {
	var c Config
	if base != nil {
		c = *base
	} else {
		c = Default()
	}
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
	envFloat := func(name string, def float64) float64 {
		if v, ok := os.LookupEnv(envPrefix + name); ok {
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f
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

	// Health check (cluster-health-check capability; Go-only addition).
	fs.BoolVar(&c.HealthCheck.Enabled, "health-check-enabled", envBool("HEALTH_CHECK_ENABLED", c.HealthCheck.Enabled),
		"enable active end-to-end cluster health probing (produce+consume)")
	fs.StringVar(&c.HealthCheck.TopicPrefix, "health-check-topic-prefix", env("HEALTH_CHECK_TOPIC_PREFIX", c.HealthCheck.TopicPrefix),
		"health check topic = prefix + cluster (per-cluster; user must pre-create each topic)")
	fs.StringVar(&c.HealthCheck.GroupPrefix, "health-check-group-prefix", env("HEALTH_CHECK_GROUP_PREFIX", c.HealthCheck.GroupPrefix),
		"health check consumer group = prefix + cluster (per-cluster)")
	fs.Float64Var(&c.HealthCheck.Rate, "health-check-rate", envFloat("HEALTH_CHECK_RATE", c.HealthCheck.Rate),
		"health check produce rate (msgs/sec/cluster)")
	fs.DurationVar(&c.HealthCheck.Recency, "health-check-recency", envDur("HEALTH_CHECK_RECENCY", c.HealthCheck.Recency),
		"status recency threshold; a check is healthy while its last success is within this window")
	fs.DurationVar(&c.HealthCheck.ClusterRefresh, "health-check-cluster-refresh", envDur("HEALTH_CHECK_CLUSTER_REFRESH", c.HealthCheck.ClusterRefresh),
		"cluster discovery refresh interval (adds/removes per-cluster probes)")
	fs.StringVar(&c.HealthCheck.Path, "health-check-path", env("HEALTH_CHECK_PATH", c.HealthCheck.Path),
		"HTTP path for the health check endpoint")

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

// ---------------------------------------------------------------------------
// YAML config file support
// ---------------------------------------------------------------------------

// yamlConfig mirrors Config but uses pointer fields so that absent YAML keys
// remain nil and do not override the base value. YAML keys use snake_case.
type yamlConfig struct {
	Namesrv       *string          `yaml:"namesrv"`
	Listen        *string          `yaml:"listen"`
	TelemetryPath *string          `yaml:"telemetry_path"`
	EnableCollect *bool            `yaml:"enable_collect"`
	EnableACL     *bool            `yaml:"enable_acl"`
	AccessKey     *string          `yaml:"access_key"`
	SecretKey     *string          `yaml:"secret_key"`
	CacheTTL      *string          `yaml:"cache_ttl"` // Go duration string, e.g. "60s"
	Pool          *yamlPool        `yaml:"pool"`
	Cron          *yamlCron        `yaml:"cron"`
	HealthCheck   *yamlHealthCheck `yaml:"health_check"`
}

type yamlPool struct {
	Core  *int `yaml:"core"`
	Max   *int `yaml:"max"`
	Queue *int `yaml:"queue"`
}

type yamlCron struct {
	CollectTopicOffset       *string `yaml:"collect_topic_offset"`
	CollectProducer           *string `yaml:"collect_producer"`
	CollectConsumerOffset     *string `yaml:"collect_consumer_offset"`
	CollectBrokerStatsTopic  *string `yaml:"collect_broker_stats_topic"`
	CollectBrokerStats        *string `yaml:"collect_broker_stats"`
	CollectBrokerRuntimeStats *string `yaml:"collect_broker_runtime_stats"`
}

type yamlHealthCheck struct {
	Enabled        *bool    `yaml:"enabled"`
	TopicPrefix    *string  `yaml:"topic_prefix"`
	GroupPrefix    *string  `yaml:"group_prefix"`
	Rate           *float64 `yaml:"rate"`
	Recency        *string  `yaml:"recency"`          // Go duration string, e.g. "5s"
	ClusterRefresh *string  `yaml:"cluster_refresh"`   // Go duration string, e.g. "5m"
	Path           *string  `yaml:"path"`
}

// FindConfigPath returns the config file path by scanning os.Args for --config
// (or --config=VALUE) and falling back to the RMQ_CONFIG environment variable.
// Returns "" if none is specified. This must be called before flag.Parse() so
// that the YAML values can influence the flag defaults.
func FindConfigPath() string {
	for i := 1; i < len(os.Args); i++ {
		arg := os.Args[i]
		if arg == "--config" || arg == "-config" {
			if i+1 < len(os.Args) {
				return os.Args[i+1]
			}
		}
		if strings.HasPrefix(arg, "--config=") {
			return strings.TrimPrefix(arg, "--config=")
		}
	}
	if v, ok := os.LookupEnv("RMQ_CONFIG"); ok {
		return v
	}
	return ""
}

// LoadYAML reads and parses a YAML config file. Returns a yamlConfig where
// only keys present in the file are non-nil.
func LoadYAML(path string) (*yamlConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}
	var yc yamlConfig
	if err := yaml.Unmarshal(data, &yc); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}
	return &yc, nil
}

// Overlay copies non-nil fields from src into dst. Duration fields in src are
// string representations (e.g. "60s") and are parsed here. Returns an error if
// any duration string is invalid.
func Overlay(dst *Config, src *yamlConfig) error {
	if src.Namesrv != nil {
		dst.Namesrv = *src.Namesrv
	}
	if src.Listen != nil {
		dst.Listen = *src.Listen
	}
	if src.TelemetryPath != nil {
		dst.TelemetryPath = *src.TelemetryPath
	}
	if src.EnableCollect != nil {
		dst.EnableCollect = *src.EnableCollect
	}
	if src.EnableACL != nil {
		dst.EnableACL = *src.EnableACL
	}
	if src.AccessKey != nil {
		dst.AccessKey = *src.AccessKey
	}
	if src.SecretKey != nil {
		dst.SecretKey = *src.SecretKey
	}
	if src.CacheTTL != nil {
		d, err := time.ParseDuration(*src.CacheTTL)
		if err != nil {
			return fmt.Errorf("invalid cache_ttl %q: %w", *src.CacheTTL, err)
		}
		dst.CacheTTL = d
	}
	if src.Pool != nil {
		if src.Pool.Core != nil {
			dst.Pool.Core = *src.Pool.Core
		}
		if src.Pool.Max != nil {
			dst.Pool.Max = *src.Pool.Max
		}
		if src.Pool.Queue != nil {
			dst.Pool.Queue = *src.Pool.Queue
		}
	}
	if src.Cron != nil {
		if src.Cron.CollectTopicOffset != nil {
			dst.Cron.CollectTopicOffset = *src.Cron.CollectTopicOffset
		}
		if src.Cron.CollectProducer != nil {
			dst.Cron.CollectProducer = *src.Cron.CollectProducer
		}
		if src.Cron.CollectConsumerOffset != nil {
			dst.Cron.CollectConsumerOffset = *src.Cron.CollectConsumerOffset
		}
		if src.Cron.CollectBrokerStatsTopic != nil {
			dst.Cron.CollectBrokerStatsTopic = *src.Cron.CollectBrokerStatsTopic
		}
		if src.Cron.CollectBrokerStats != nil {
			dst.Cron.CollectBrokerStats = *src.Cron.CollectBrokerStats
		}
		if src.Cron.CollectBrokerRuntimeStats != nil {
			dst.Cron.CollectBrokerRuntimeStats = *src.Cron.CollectBrokerRuntimeStats
		}
	}
	if src.HealthCheck != nil {
		if src.HealthCheck.Enabled != nil {
			dst.HealthCheck.Enabled = *src.HealthCheck.Enabled
		}
		if src.HealthCheck.TopicPrefix != nil {
			dst.HealthCheck.TopicPrefix = *src.HealthCheck.TopicPrefix
		}
		if src.HealthCheck.GroupPrefix != nil {
			dst.HealthCheck.GroupPrefix = *src.HealthCheck.GroupPrefix
		}
		if src.HealthCheck.Rate != nil {
			dst.HealthCheck.Rate = *src.HealthCheck.Rate
		}
		if src.HealthCheck.Recency != nil {
			d, err := time.ParseDuration(*src.HealthCheck.Recency)
			if err != nil {
				return fmt.Errorf("invalid health_check.recency %q: %w", *src.HealthCheck.Recency, err)
			}
			dst.HealthCheck.Recency = d
		}
		if src.HealthCheck.ClusterRefresh != nil {
			d, err := time.ParseDuration(*src.HealthCheck.ClusterRefresh)
			if err != nil {
				return fmt.Errorf("invalid health_check.cluster_refresh %q: %w", *src.HealthCheck.ClusterRefresh, err)
			}
			dst.HealthCheck.ClusterRefresh = d
		}
		if src.HealthCheck.Path != nil {
			dst.HealthCheck.Path = *src.HealthCheck.Path
		}
	}
	return nil
}

// OverlayFile loads a YAML config file and overlays non-nil fields onto dst.
// This is the primary entry point for main.go.
func OverlayFile(dst *Config, path string) error {
	yc, err := LoadYAML(path)
	if err != nil {
		return err
	}
	return Overlay(dst, yc)
}
