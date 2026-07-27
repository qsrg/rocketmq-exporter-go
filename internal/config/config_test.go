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
	c := RegisterFlags(fs, "RMQ_TEST_")
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
	c := RegisterFlags(fs, "RMQ_TEST_")
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
