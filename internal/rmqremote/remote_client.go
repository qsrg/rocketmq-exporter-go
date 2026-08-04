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
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// connPoolSizePerAddr is the max cached connections per broker/namesrv address.
// Each connection serves one request at a time (handed out via a channel), so
// this is also the max concurrent in-flight RPCs to one address - the rest wait
// for a free connection. With pooling there is no per-RPC TCP handshake and no
// TIME_WAIT explosion, so this can be set well above the ~14-concurrent ceiling
// that per-call dial hits via ephemeral-port exhaustion.
const connPoolSizePerAddr = 32

// RemotingClient is a minimal TCP client for the RocketMQ 4.x remoting protocol.
//
// Connections are pooled per address. Correctness under concurrency is preserved
// by handing each cached connection to at most one InvokeSync caller at a time
// (via a channel): a caller takes a connection, does its write+read, and returns
// it. Two callers never share a TCP stream mid-RPC, so request/response frames
// cannot interleave or desync (no opaque-based multiplexing needed). This avoids
// the per-RPC TCP handshake of per-call dial -- which at the exporter's RPC
// volume (tens of thousands of RPCs per collection cycle, e.g. one
// queryMsgByOffset per consume queue) capped effective concurrency at ~14 via
// ephemeral-port/TIME_WAIT exhaustion and pushed collection cycles past the
// metric TTL.
//
// Frame layout matches codec.go: a request is encode(cmd) =
// [4-byte frameSize][markedHeaderLen(4)][header][body]; a response is
// [4-byte length][payload of `length` bytes], decoded by decode().
type RemotingClient struct {
	dialer net.Dialer
	mu     sync.Mutex
	pools  map[string]chan net.Conn
}

// NewRemotingClient returns a client.
func NewRemotingClient() *RemotingClient {
	return &RemotingClient{
		dialer: net.Dialer{Timeout: 10 * time.Second},
		pools:  make(map[string]chan net.Conn),
	}
}

// poolFor returns the connection channel for addr, creating it on first use.
func (c *RemotingClient) poolFor(addr string) chan net.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch, ok := c.pools[addr]
	if !ok {
		ch = make(chan net.Conn, connPoolSizePerAddr)
		c.pools[addr] = ch
	}
	return ch
}

// takeConn returns a cached connection for addr if one is free, else nil.
func (c *RemotingClient) takeConn(addr string) net.Conn {
	select {
	case conn := <-c.poolFor(addr):
		return conn
	default:
		return nil
	}
}

// putConn returns a healthy connection to the pool, or closes it if the pool is
// full. A broken connection (caller already closed it) must not be put back.
func (c *RemotingClient) putConn(addr string, conn net.Conn) {
	select {
	case c.poolFor(addr) <- conn:
	default:
		_ = conn.Close()
	}
}

// InvokeSync sends cmd to addr and returns the decoded response, failing the
// RPC if the broker returns a non-SUCCESS ResponseCode (see ResponseCodeOf).
//
// It prefers a pooled connection; if that connection has gone stale (broker
// closed it idle) the write/read fails and the call transparently retries once
// with a fresh dial. Each connection is used by one caller at a time, so frames
// never interleave.
func (c *RemotingClient) InvokeSync(addr string, cmd *RemotingCommand, timeout time.Duration) (*RemotingCommand, error) {
	enc, err := encode(cmd)
	if err != nil {
		return nil, fmt.Errorf("remoting encode: %w", err)
	}

	// Try a pooled connection first.
	if conn := c.takeConn(addr); conn != nil {
		resp, err := c.invokeOnConn(addr, conn, enc, timeout)
		if err == nil {
			c.putConn(addr, conn)
			return resp, nil
		}
		// Stale/broken pooled connection: drop it and fall through to a fresh dial.
		_ = conn.Close()
	}

	// Fresh dial (no free pooled connection, or the pooled one was stale).
	conn, err := c.dial(addr, timeout)
	if err != nil {
		return nil, fmt.Errorf("remoting dial %s: %w", addr, err)
	}
	resp, err := c.invokeOnConn(addr, conn, enc, timeout)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	c.putConn(addr, conn)
	return resp, nil
}

// dial opens a new TCP connection to addr, bounded by timeout (via context so
// the shared dialer is not mutated under concurrency).
func (c *RemotingClient) dial(addr string, timeout time.Duration) (net.Conn, error) {
	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	return c.dialer.DialContext(ctx, "tcp", addr)
}

// invokeOnConn writes the encoded request and reads one response frame on conn.
// The caller owns conn for the duration (no concurrent access), so the deadline
// set here is safe. A non-nil error means conn may be in a bad state and must
// not be returned to the pool.
func (c *RemotingClient) invokeOnConn(addr string, conn net.Conn, enc []byte, timeout time.Duration) (*RemotingCommand, error) {
	if timeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(timeout))
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

// Shutdown closes all pooled connections. Called at exporter shutdown (after the
// cron tasks have stopped, so no InvokeSync is in flight).
func (c *RemotingClient) Shutdown() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ch := range c.pools {
		for {
			select {
			case conn := <-ch:
				_ = conn.Close()
			default:
				goto drained
			}
		}
	drained:
	}
	c.pools = make(map[string]chan net.Conn)
	return nil
}
