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

package main

import (
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"

	rlog "github.com/apache/rocketmq-client-go/v2/rlog"

	"github.com/qsrg/rocketmq-exporter-go/internal/config"
)

// setupLogging configures the exporter's slog logger (level + writer) and
// redirects the rocketmq-client-go rlog (logrus) to the same writer + level, so
// all logs go to one place. If cfg.LogFile is empty, both write to stderr.
// If set, both write to a lumberjack-rotated file with the configured retention.
func setupLogging(cfg *config.Config) {
	level := parseSlogLevel(cfg.LogLevel)

	var w io.Writer = os.Stderr
	if cfg.LogFile != "" {
		w = &lumberjack.Logger{
			Filename:   cfg.LogFile,
			MaxSize:    cfg.LogMaxSizeMB,
			MaxBackups: cfg.LogMaxBackups,
			MaxAge:     cfg.LogMaxAgeDays,
			Compress:   cfg.LogCompress,
			LocalTime:  true,
		}
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: level})))

	// Redirect the rocketmq-client-go library's own logger (logrus-based rlog)
	// to the same writer and level, so its verbose info logs respect the
	// configured level and go to the same file (if any).
	rlog.SetLogger(&sharedRlogLogger{l: newLogrusLogger(level, w)})
}

func parseSlogLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func newLogrusLogger(level slog.Level, w io.Writer) *logrus.Logger {
	l := logrus.New()
	l.SetOutput(w)
	l.SetFormatter(&logrus.TextFormatter{DisableColors: true, FullTimestamp: true})
	switch level {
	case slog.LevelDebug:
		l.SetLevel(logrus.DebugLevel)
	case slog.LevelWarn:
		l.SetLevel(logrus.WarnLevel)
	case slog.LevelError:
		l.SetLevel(logrus.ErrorLevel)
	default:
		l.SetLevel(logrus.InfoLevel)
	}
	return l
}

// sharedRlogLogger implements rlog.Logger, forwarding to a logrus.Logger whose
// output is the same writer (lumberjack file or stderr) as the exporter's slog.
// This unifies all logs into one destination and applies the configured level
// to the library's verbose internal logging (route changes, offset fetches, ...).
type sharedRlogLogger struct{ l *logrus.Logger }

func (a *sharedRlogLogger) Debug(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	a.l.WithFields(logrus.Fields(fields)).Debug(msg)
}
func (a *sharedRlogLogger) Info(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	a.l.WithFields(logrus.Fields(fields)).Info(msg)
}
func (a *sharedRlogLogger) Warning(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	a.l.WithFields(logrus.Fields(fields)).Warning(msg)
}
func (a *sharedRlogLogger) Error(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	a.l.WithFields(logrus.Fields(fields)).Error(msg)
}
func (a *sharedRlogLogger) Fatal(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	a.l.WithFields(logrus.Fields(fields)).Fatal(msg)
}
func (a *sharedRlogLogger) Level(level string) {
	switch strings.ToLower(level) {
	case "debug":
		a.l.SetLevel(logrus.DebugLevel)
	case "warn":
		a.l.SetLevel(logrus.WarnLevel)
	case "error":
		a.l.SetLevel(logrus.ErrorLevel)
	case "fatal":
		a.l.SetLevel(logrus.FatalLevel)
	default:
		a.l.SetLevel(logrus.InfoLevel)
	}
}
func (a *sharedRlogLogger) OutputPath(_ string) error { return nil }
