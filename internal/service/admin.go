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

// Package service implements the RocketMQ admin client wrapper (port of
// MQAdminExtImpl/MQAdminInstance) — only the read RPCs the collection tasks
// need. When enable-acl is set, every outbound request is ACL-signed via
// rmqremote.SignACL (HMAC-SHA1, mirroring the Java exporter's aclHook).
package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/wcf/rmq-exporter/internal/rmqremote"
)

// AdminClient wraps a remoting client and namesrv address. It is constructed
// via manual injection (no DI framework) and safe for concurrent use by all
// cron tasks (per-call dial + mutex-guarded connections).
type AdminClient struct {
	rc         *rmqremote.RemotingClient
	namesrv    string
	enableACL  bool
	accessKey  string
	secretKey string
	timeout    time.Duration
}

// NewAdminClient constructs an admin client pointed at namesrv. When enableACL
// is true, every request is signed with accessKey/secretKey (HMAC-SHA1) before
// dispatch; non-ACL brokers are unaffected (signIfACL is a no-op).
func NewAdminClient(namesrv string, enableACL bool, accessKey, secretKey string, timeout time.Duration) *AdminClient {
	return &AdminClient{
		rc:         rmqremote.NewRemotingClient(),
		namesrv:    namesrv,
		enableACL:  enableACL,
		accessKey:  accessKey,
		secretKey:  secretKey,
		timeout:    timeout,
	}
}

// Start opens nothing eagerly (connections dial lazily); it logs the ACL
// deferral when ACL is requested.
func (a *AdminClient) Start(_ context.Context) error {
	if a.enableACL {
		slog.Info("ACL signing enabled; requests will be signed with the configured access key")
	}
	return nil
}

// signIfACL attaches the ACL signature when enable-acl is true; otherwise it is
// a no-op (non-ACL brokers are unaffected). Mirrors the Java exporter wiring an
// aclHook onto DefaultMQAdminExt.
func (a *AdminClient) signIfACL(cmd *rmqremote.RemotingCommand) {
	if a.enableACL {
		rmqremote.SignACL(cmd, a.accessKey, a.secretKey)
	}
}

// Shutdown closes the remoting client's connections.
func (a *AdminClient) Shutdown(_ context.Context) error {
	return a.rc.Shutdown()
}

// rpcError mirrors the Java broker error path: a non-SUCCESS ResponseCode is
// surfaced as a typed error carrying the code (int) + remark, so the task layer
// can apply TOPIC_NOT_EXIST / CONSUMER_NOT_ONLINE / SYSTEM_ERROR silent-degrade.
type rpcError struct {
	code   int
	remark string
}

func (e *rpcError) Error() string { return fmt.Sprintf("rocketmq rpc error: code=%d remark=%s", e.code, e.remark) }

// Code returns the RocketMQ ResponseCode carried by the error (0 if none).
func (e *rpcError) Code() int { return e.code }

// ResponseCodeOf unwraps the ResponseCode from an error returned by an RPC, or
// -1 if the error is not a broker ResponseCode error (e.g. a transport error).
func ResponseCodeOf(err error) int {
	var rerr *rpcError
	if errors.As(err, &rerr) {
		return rerr.code
	}
	return -1
}

// invokeSync sends a request to namesrv and decodes the JSON body into out.
// It returns a ResponseCode error on non-SUCCESS.
func (a *AdminClient) invokeSyncNamesrv(code int16, header rmqremote.CustomHeader, out any) error {
	cmd := rmqremote.NewRemotingCommand(code, header, nil)
	a.signIfACL(cmd)
	resp, err := a.rc.InvokeSync(a.namesrv, cmd, a.timeout)
	if err != nil {
		return err
	}
	if resp.Code != rmqremote.ResponseSuccess {
		return &rpcError{code: int(resp.Code), remark: resp.Remark}
	}
	if out != nil && len(resp.Body) > 0 {
		if err := rmqremote.UnmarshalJSON(resp.Body, out); err != nil {
			return fmt.Errorf("decode response body (code=%d): %w", code, err)
		}
	}
	return nil
}

// invokeSyncBroker sends a request to a broker address and decodes into out.
func (a *AdminClient) invokeSyncBroker(addr string, code int16, header rmqremote.CustomHeader, out any) (*rmqremote.RemotingCommand, error) {
	cmd := rmqremote.NewRemotingCommand(code, header, nil)
	a.signIfACL(cmd)
	resp, err := a.rc.InvokeSync(addr, cmd, a.timeout)
	if err != nil {
		return nil, err
	}
	if resp.Code != rmqremote.ResponseSuccess {
		return resp, &rpcError{code: int(resp.Code), remark: resp.Remark}
	}
	if out != nil && len(resp.Body) > 0 {
		if err := rmqremote.UnmarshalJSON(resp.Body, out); err != nil {
			return resp, fmt.Errorf("decode response body (code=%d): %w", code, err)
		}
	}
	return resp, nil
}
