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
	"sync"
	"testing"
	"time"
)

// readRequestFrame reads one request frame [4-byte frameSize][frame] from conn
// and decodes it (decode expects the frame with frameSize stripped).
func readRequestFrame(conn net.Conn) (*RemotingCommand, error) {
	var frameSize int32
	if err := binary.Read(conn, binary.BigEndian, &frameSize); err != nil {
		return nil, err
	}
	frame := make([]byte, frameSize)
	if _, err := io.ReadFull(conn, frame); err != nil {
		return nil, err
	}
	return decode(frame)
}

// writeResponseFrame writes one response frame [4-byte length][payload] where
// payload is the serialized command (encode produces [frameSize][payload]; the
// length prefix equals frameSize).
func writeResponseFrame(conn net.Conn, resp *RemotingCommand) error {
	enc, err := encode(resp)
	if err != nil {
		return err
	}
	payload := enc[4:] // strip the leading frameSize; the rest is the payload
	if err := binary.Write(conn, binary.BigEndian, int32(len(payload))); err != nil {
		return err
	}
	_, err = conn.Write(payload)
	return err
}

// TestInvokeSyncConcurrentPooling runs many concurrent InvokeSync calls against
// a mock server that echoes each request's opaque in its response. With
// connection pooling, each cached connection is used by one caller at a time, so
// request/response frames must not interleave; a mixed/corrupted response would
// carry the wrong opaque and fail the assertion. Exercises connection reuse
// (N >> connPoolSizePerAddr) and runs under -race.
func TestInvokeSyncConcurrentPooling(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	addr := ln.Addr().String()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			go func(conn net.Conn) {
				defer conn.Close()
				for {
					req, err := readRequestFrame(conn)
					if err != nil {
						return // connection closed
					}
					resp := NewRemotingCommand(ResponseSuccess, nil, nil)
					resp.Opaque = req.Opaque // echo so the client can detect a mismatched frame
					if err := writeResponseFrame(conn, resp); err != nil {
						return
					}
				}
			}(conn)
		}
	}()

	c := NewRemotingClient()
	defer c.Shutdown()

	const N = 300 // well above connPoolSizePerAddr to force reuse
	var wg sync.WaitGroup
	errCh := make(chan error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd := NewRemotingCommand(RequestGetBrokerClusterInfo, nil, nil)
			want := cmd.Opaque
			resp, err := c.InvokeSync(addr, cmd, 5*time.Second)
			if err != nil {
				errCh <- fmt.Errorf("invoke: %w", err)
				return
			}
			if resp.Opaque != want {
				errCh <- fmt.Errorf("opaque mismatch: got %d want %d (frame corruption / shared connection?)", resp.Opaque, want)
			}
			if resp.Code != ResponseSuccess {
				errCh <- fmt.Errorf("response code = %d, want %d", resp.Code, ResponseSuccess)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}
