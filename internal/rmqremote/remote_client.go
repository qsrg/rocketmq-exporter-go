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

package rmqremote

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

// RemotingClient is a minimal TCP client for the RocketMQ 4.x remoting protocol.
//
// It dials a fresh connection per InvokeSync. This is deliberately NOT pooled:
// a shared cached connection would let concurrent InvokeSync callers interleave
// their write/read frames on one TCP stream and desync the framing (garbled
// decode). Per-call dial trades a TCP handshake per RPC for correctness under
// concurrency — acceptable for a metrics exporter whose RPCs are per cron tick,
// not per scrape. Connection pooling with opaque-based correlation is a Phase 1.5
// performance item.
//
// Frame layout matches codec.go: a request is encode(cmd) =
// [4-byte frameSize][markedHeaderLen(4)][header][body]; a response is
// [4-byte length][payload of `length` bytes], decoded by decode().
type RemotingClient struct {
	dialer net.Dialer
}

// NewRemotingClient returns a client.
func NewRemotingClient() *RemotingClient {
	return &RemotingClient{dialer: net.Dialer{Timeout: 10 * time.Second}}
}

// InvokeSync sends cmd to addr and returns the decoded response, failing the
// RPC if the broker returns a non-SUCCESS ResponseCode (see ResponseCodeOf).
func (c *RemotingClient) InvokeSync(addr string, cmd *RemotingCommand, timeout time.Duration) (*RemotingCommand, error) {
	if timeout > 0 {
		c.dialer.Timeout = timeout
	}
	conn, err := c.dialer.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("remoting dial %s: %w", addr, err)
	}
	defer conn.Close()
	if timeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(timeout))
	}

	// Send.
	enc, err := encode(cmd)
	if err != nil {
		return nil, fmt.Errorf("remoting encode: %w", err)
	}
	if _, err := conn.Write(enc); err != nil {
		return nil, fmt.Errorf("remoting write %s: %w", addr, err)
	}

	// Read response: [4-byte length][payload].
	var length int32
	if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
		return nil, fmt.Errorf("remoting read length %s: %w", addr, err)
	}
	if length <= 0 {
		return nil, fmt.Errorf("remoting %s: non-positive response length %d", addr, length)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, fmt.Errorf("remoting read payload %s: %w", addr, err)
	}

	resp, err := decode(payload)
	if err != nil {
		return nil, fmt.Errorf("remoting decode %s: %w", addr, err)
	}
	return resp, nil
}

// Shutdown is a no-op (per-call dial closes connections immediately); kept for
// the AdminClient lifecycle.
func (c *RemotingClient) Shutdown() error { return nil }
