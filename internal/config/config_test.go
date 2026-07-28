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

package config

import (
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestTranslateCron(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"15 0/1 * * * ?", "15 0/1 * * * *"},
		{"15 0/1 * * * *", "15 0/1 * * * *"}, // no-op when no '?'
		{"0 0 ? * 1L", "0 0 * * 1L"},          // multiple '?'
	}
	for _, c := range cases {
		if got := TranslateCron(c.in); got != c.want {
			t.Errorf("TranslateCron(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestValidateCronRejectsInvalid(t *testing.T) {
	// 'bad' is not parseable -> fail fast naming the task.
	if err := ValidateCron("collectConsumerOffset", "not a cron"); err == nil {
		t.Fatal("expected error for invalid cron, got nil")
	}
}

func TestValidateAllAcceptsDefaults(t *testing.T) {
	c := Default()
	if err := c.ValidateAll(); err != nil {
		t.Fatalf("default crons should validate: %v", err)
	}
}

func TestValidateAllRejectsOneTask(t *testing.T) {
	c := Default()
	c.Cron.CollectConsumerOffset = "garbage"
	err := c.ValidateAll()
	if err == nil {
		t.Fatal("expected error for invalid cron, got nil")
	}
	// Must name the offending task.
	if !contains(err.Error(), "collectConsumerOffset") {
		t.Errorf("error should name collectConsumerOffset, got: %v", err)
	}
}

func TestRegisterFlagsEnvFallback(t *testing.T) {
	t.Setenv("RMQ_TEST_NAMESRV_ADDR", "10.0.0.2:9876")
	t.Setenv("RMQ_TEST_LISTEN", ":9999")
	t.Setenv("RMQ_TEST_ENABLE_COLLECT", "false")
	t.Setenv("RMQ_TEST_POOL_MAX", "20")

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	c := RegisterFlags(fs, "RMQ_TEST_", nil)
	if err := fs.Parse(nil); err != nil { // no --flags -> env defaults apply
		t.Fatal(err)
	}
	if c.Namesrv != "10.0.0.2:9876" {
		t.Errorf("Namesrv env fallback = %q, want 10.0.0.2:9876", c.Namesrv)
	}
	if c.Listen != ":9999" {
		t.Errorf("Listen env fallback = %q, want :9999", c.Listen)
	}
	if c.EnableCollect {
		t.Error("EnableCollect env fallback should be false")
	}
	if c.Pool.Max != 20 {
		t.Errorf("Pool.Max env fallback = %d, want 20", c.Pool.Max)
	}
}

func TestRegisterFlagsFlagOverridesEnv(t *testing.T) {
	t.Setenv("RMQ_TEST_NAMESRV_ADDR", "env-value")
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	c := RegisterFlags(fs, "RMQ_TEST_", nil)
	if err := fs.Parse([]string{"-namesrv=10.0.0.1:9876"}); err != nil {
		t.Fatal(err)
	}
	if c.Namesrv != "10.0.0.1:9876" {
		t.Errorf("flag should override env; got %q", c.Namesrv)
	}
}

func TestDefaultValues(t *testing.T) {
	c := Default()
	if c.Listen != ":5557" {
		t.Errorf("Listen default = %q, want :5557", c.Listen)
	}
	if c.TelemetryPath != "/metrics" {
		t.Errorf("TelemetryPath default = %q, want /metrics", c.TelemetryPath)
	}
	if c.CacheTTL != 60*time.Second {
		t.Errorf("CacheTTL default = %v, want 60s", c.CacheTTL)
	}
	if c.Pool.Core != 10 || c.Pool.Max != 10 || c.Pool.Queue != 5000 {
		t.Errorf("Pool defaults wrong: %+v", c.Pool)
	}
}

func TestHealthCheckDefaults(t *testing.T) {
	c := Default()
	hc := c.HealthCheck
	if !hc.Enabled {
		t.Error("HealthCheck.Enabled default should be true")
	}
	if hc.TopicPrefix != "HealthCheckTopic-" {
		t.Errorf("TopicPrefix default = %q, want HealthCheckTopic-", hc.TopicPrefix)
	}
	if hc.GroupPrefix != "HealthCheckGroup-" {
		t.Errorf("GroupPrefix default = %q, want HealthCheckGroup-", hc.GroupPrefix)
	}
	if hc.Rate != 2.0 {
		t.Errorf("Rate default = %v, want 2.0", hc.Rate)
	}
	if hc.Recency != 5*time.Second {
		t.Errorf("Recency default = %v, want 5s", hc.Recency)
	}
	if hc.ClusterRefresh != 5*time.Minute {
		t.Errorf("ClusterRefresh default = %v, want 5m", hc.ClusterRefresh)
	}
	if hc.Path != "/healthz" {
		t.Errorf("Path default = %q, want /healthz", hc.Path)
	}
}

func TestHealthCheckEnvAndFlagOverrides(t *testing.T) {
	cases := []struct {
		name   string
		env    map[string]string
		args   []string
		check  func(c *Config) string
		want   string
	}{
		{
			name: "env rate override",
			env:  map[string]string{"RMQ_TEST_HEALTH_CHECK_RATE": "5"},
			check: func(c *Config) string {
				return strconv.FormatFloat(c.HealthCheck.Rate, 'f', -1, 64)
			},
			want: "5",
		},
		{
			name: "env recency override",
			env:  map[string]string{"RMQ_TEST_HEALTH_CHECK_RECENCY": "10s"},
			check: func(c *Config) string {
				return c.HealthCheck.Recency.String()
			},
			want: "10s",
		},
		{
			name: "env enabled=false override",
			env:  map[string]string{"RMQ_TEST_HEALTH_CHECK_ENABLED": "false"},
			check: func(c *Config) string {
				return strconv.FormatBool(c.HealthCheck.Enabled)
			},
			want: "false",
		},
		{
			name: "env path override",
			env:  map[string]string{"RMQ_TEST_HEALTH_CHECK_PATH": "/livez"},
			check: func(c *Config) string {
				return c.HealthCheck.Path
			},
			want: "/livez",
		},
		{
			name: "flag recency overrides env",
			env:  map[string]string{"RMQ_TEST_HEALTH_CHECK_RECENCY": "10s"},
			args: []string{"-health-check-recency=15s"},
			check: func(c *Config) string {
				return c.HealthCheck.Recency.String()
			},
			want: "15s",
		},
		{
			name: "flag enabled=false overrides env",
			env:  map[string]string{"RMQ_TEST_HEALTH_CHECK_ENABLED": "true"},
			args: []string{"-health-check-enabled=false"},
			check: func(c *Config) string {
				return strconv.FormatBool(c.HealthCheck.Enabled)
			},
			want: "false",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			c := RegisterFlags(fs, "RMQ_TEST_", nil)
			if err := fs.Parse(tc.args); err != nil {
				t.Fatal(err)
			}
			if got := tc.check(c); got != tc.want {
				t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
			}
		})
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}

// ptr is a generic helper to create a pointer to a literal value.
func ptr[T any](v T) *T { return &v }

func TestLoadYAML(t *testing.T) {
	content := `
namesrv: "10.0.0.1:9876"
listen: ":9090"
enable_collect: false
cache_ttl: "120s"
pool:
  core: 5
  max: 20
cron:
  collect_topic_offset: "0 0/2 * * * ?"
health_check:
  enabled: false
  rate: 3.0
`
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	yc, err := LoadYAML(p)
	if err != nil {
		t.Fatalf("LoadYAML: %v", err)
	}
	// Verify non-nil fields.
	if yc.Namesrv == nil || *yc.Namesrv != "10.0.0.1:9876" {
		t.Error("Namesrv not parsed correctly")
	}
	if yc.Listen == nil || *yc.Listen != ":9090" {
		t.Error("Listen not parsed correctly")
	}
	if yc.EnableCollect == nil || *yc.EnableCollect != false {
		t.Error("EnableCollect not parsed correctly")
	}
	if yc.CacheTTL == nil || *yc.CacheTTL != "120s" {
		t.Error("CacheTTL not parsed correctly")
	}
	if yc.Pool == nil || yc.Pool.Core == nil || *yc.Pool.Core != 5 {
		t.Error("Pool.Core not parsed correctly")
	}
	if yc.Pool == nil || yc.Pool.Max == nil || *yc.Pool.Max != 20 {
		t.Error("Pool.Max not parsed correctly")
	}
	if yc.Cron == nil || yc.Cron.CollectTopicOffset == nil || *yc.Cron.CollectTopicOffset != "0 0/2 * * * ?" {
		t.Error("Cron.CollectTopicOffset not parsed correctly")
	}
	if yc.HealthCheck == nil || yc.HealthCheck.Enabled == nil || *yc.HealthCheck.Enabled != false {
		t.Error("HealthCheck.Enabled not parsed correctly")
	}
	if yc.HealthCheck == nil || yc.HealthCheck.Rate == nil || *yc.HealthCheck.Rate != 3.0 {
		t.Error("HealthCheck.Rate not parsed correctly")
	}
	// Verify absent fields are nil.
	if yc.AccessKey != nil {
		t.Error("AccessKey should be nil (absent in YAML)")
	}
	if yc.SecretKey != nil {
		t.Error("SecretKey should be nil (absent in YAML)")
	}
	if yc.Pool != nil && yc.Pool.Queue != nil {
		t.Error("Pool.Queue should be nil (absent in YAML)")
	}
}

func TestOverlayPartial(t *testing.T) {
	base := Default()
	yc := &yamlConfig{Namesrv: ptr("10.0.0.1:9876")}
	if err := Overlay(&base, yc); err != nil {
		t.Fatalf("Overlay: %v", err)
	}
	if base.Namesrv != "10.0.0.1:9876" {
		t.Errorf("Namesrv = %q, want 10.0.0.1:9876", base.Namesrv)
	}
	// Other fields should remain at defaults.
	if base.Listen != ":5557" {
		t.Errorf("Listen = %q, want :5557 (default)", base.Listen)
	}
	if !base.EnableCollect {
		t.Error("EnableCollect should remain true (default)")
	}
}

func TestOverlayZeroValue(t *testing.T) {
	// enable_collect: false explicitly set to zero value.
	base := Default() // EnableCollect = true
	yc := &yamlConfig{EnableCollect: ptr(false)}
	if err := Overlay(&base, yc); err != nil {
		t.Fatalf("Overlay: %v", err)
	}
	if base.EnableCollect {
		t.Error("EnableCollect should be overridden to false")
	}
}

func TestOverlayDuration(t *testing.T) {
	base := Default()
	yc := &yamlConfig{CacheTTL: ptr("120s")}
	if err := Overlay(&base, yc); err != nil {
		t.Fatalf("Overlay: %v", err)
	}
	if base.CacheTTL != 120*time.Second {
		t.Errorf("CacheTTL = %v, want 120s", base.CacheTTL)
	}
}

func TestOverlayDurationInvalid(t *testing.T) {
	base := Default()
	yc := &yamlConfig{CacheTTL: ptr("not-a-duration")}
	if err := Overlay(&base, yc); err == nil {
		t.Fatal("expected error for invalid duration, got nil")
	}
}

func TestOverlayHealthCheckDuration(t *testing.T) {
	base := Default()
	yc := &yamlConfig{
		HealthCheck: &yamlHealthCheck{
			Recency:        ptr("10s"),
			ClusterRefresh: ptr("10m"),
		},
	}
	if err := Overlay(&base, yc); err != nil {
		t.Fatalf("Overlay: %v", err)
	}
	if base.HealthCheck.Recency != 10*time.Second {
		t.Errorf("Recency = %v, want 10s", base.HealthCheck.Recency)
	}
	if base.HealthCheck.ClusterRefresh != 10*time.Minute {
		t.Errorf("ClusterRefresh = %v, want 10m", base.HealthCheck.ClusterRefresh)
	}
}

func TestOverlayFileNotFound(t *testing.T) {
	base := Default()
	if err := OverlayFile(&base, "/nonexistent/config.yaml"); err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestOverlayFileIntegration(t *testing.T) {
	content := `
namesrv: "yaml-value:9876"
listen: ":9999"
enable_collect: false
cache_ttl: "120s"
pool:
  queue: 1000
cron:
  collect_topic_offset: "0 0/2 * * * ?"
health_check:
  enabled: false
  recency: "10s"
`
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	base := Default()
	if err := OverlayFile(&base, p); err != nil {
		t.Fatalf("OverlayFile: %v", err)
	}
	if base.Namesrv != "yaml-value:9876" {
		t.Errorf("Namesrv = %q, want yaml-value:9876", base.Namesrv)
	}
	if base.Listen != ":9999" {
		t.Errorf("Listen = %q, want :9999", base.Listen)
	}
	if base.EnableCollect {
		t.Error("EnableCollect should be false")
	}
	if base.CacheTTL != 120*time.Second {
		t.Errorf("CacheTTL = %v, want 120s", base.CacheTTL)
	}
	if base.Pool.Queue != 1000 {
		t.Errorf("Pool.Queue = %d, want 1000", base.Pool.Queue)
	}
	// Pool.Core/Max should retain defaults.
	if base.Pool.Core != 10 || base.Pool.Max != 10 {
		t.Errorf("Pool.Core/Max = %d/%d, want 10/10 (default)", base.Pool.Core, base.Pool.Max)
	}
	if base.HealthCheck.Enabled {
		t.Error("HealthCheck.Enabled should be false")
	}
	if base.HealthCheck.Recency != 10*time.Second {
		t.Errorf("HealthCheck.Recency = %v, want 10s", base.HealthCheck.Recency)
	}
}

func TestPriorityEnvOverFile(t *testing.T) {
	content := `namesrv: "yaml-value:9876"`
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RMQ_TEST_NAMESRV_ADDR", "env-value:9876")

	base := Default()
	if err := OverlayFile(&base, p); err != nil {
		t.Fatalf("OverlayFile: %v", err)
	}
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	c := RegisterFlags(fs, "RMQ_TEST_", &base)
	if err := fs.Parse(nil); err != nil {
		t.Fatal(err)
	}
	if c.Namesrv != "env-value:9876" {
		t.Errorf("env should override file; got %q", c.Namesrv)
	}
}

func TestPriorityFlagOverEnvOverFile(t *testing.T) {
	content := `namesrv: "yaml-value:9876"`
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RMQ_TEST_NAMESRV_ADDR", "env-value:9876")

	base := Default()
	if err := OverlayFile(&base, p); err != nil {
		t.Fatalf("OverlayFile: %v", err)
	}
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	c := RegisterFlags(fs, "RMQ_TEST_", &base)
	if err := fs.Parse([]string{"-namesrv=flag-value:9876"}); err != nil {
		t.Fatal(err)
	}
	if c.Namesrv != "flag-value:9876" {
		t.Errorf("flag should override env+file; got %q", c.Namesrv)
	}
}

func TestPriorityFileOverDefault(t *testing.T) {
	content := `namesrv: "yaml-value:9876"`
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	base := Default()
	if err := OverlayFile(&base, p); err != nil {
		t.Fatalf("OverlayFile: %v", err)
	}
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	c := RegisterFlags(fs, "RMQ_TEST_", &base)
	if err := fs.Parse(nil); err != nil {
		t.Fatal(err)
	}
	if c.Namesrv != "yaml-value:9876" {
		t.Errorf("file should override default; got %q", c.Namesrv)
	}
}

func TestFindConfigPath(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  string
		want string
	}{
		{
			name: "no config specified",
			args: []string{"rmq-exporter"},
			want: "",
		},
		{
			name: "--config /path",
			args: []string{"rmq-exporter", "--config", "/path/to/config.yaml"},
			want: "/path/to/config.yaml",
		},
		{
			name: "--config=/path",
			args: []string{"rmq-exporter", "--config=/path/to/config.yaml"},
			want: "/path/to/config.yaml",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			orig := os.Args
			os.Args = tc.args
			defer func() { os.Args = orig }()

			if tc.env != "" {
				t.Setenv("RMQ_CONFIG", tc.env)
			}
			got := FindConfigPath()
			if got != tc.want {
				t.Errorf("FindConfigPath() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFindConfigPathEnvFallback(t *testing.T) {
	orig := os.Args
	os.Args = []string{"rmq-exporter"}
	defer func() { os.Args = orig }()

	t.Setenv("RMQ_CONFIG", "/env/config.yaml")
	got := FindConfigPath()
	if got != "/env/config.yaml" {
		t.Errorf("FindConfigPath() = %q, want /env/config.yaml", got)
	}
}
