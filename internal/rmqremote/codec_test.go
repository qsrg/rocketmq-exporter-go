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
	"bytes"
	"testing"
)

// TestRemotingCommandRoundTrip encodes a RemotingCommand with the JSON codec
// (the 4.x broker default) and decodes it back, asserting every field survives.
// This is the minimum wire-fidelity check (design top risk: Go json-iterator vs
// fastjson). The codec is vendored verbatim from rocketmq-client-go/v2, so a
// green round-trip confirms our copy is intact.
func TestRemotingCommandRoundTrip(t *testing.T) {
	// codecType defaults to JsonCodecs (0) — the RocketMQ 4.x broker default.
	cmd := NewRemotingCommand(int16(106), nil, []byte("payload body"))
	cmd.ExtFields = map[string]string{
		"brokerName":  "broker-a",
		"clusterName": "DefaultCluster",
	}
	cmd.Remark = "ok"
	wantOpaque := cmd.Opaque
	wantBody := append([]byte(nil), cmd.Body...)

	enc, err := encode(cmd)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(enc) == 0 {
		t.Fatal("encoded frame is empty")
	}

	// decode expects the frame with the leading 4-byte frameSize already stripped
	// (the TCP read layer consumes frameSize, then hands the rest to decode).
	dec, err := decode(enc[4:])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if dec.Code != 106 {
		t.Errorf("Code = %d, want 106", dec.Code)
	}
	if dec.Opaque != wantOpaque {
		t.Errorf("Opaque = %d, want %d", dec.Opaque, wantOpaque)
	}
	if dec.Remark != "ok" {
		t.Errorf("Remark = %q, want %q", dec.Remark, "ok")
	}
	if !bytes.Equal(dec.Body, wantBody) {
		t.Errorf("Body = %q, want %q", dec.Body, wantBody)
	}
	if dec.ExtFields["brokerName"] != "broker-a" {
		t.Errorf("ExtFields[brokerName] = %q, want broker-a", dec.ExtFields["brokerName"])
	}
	if dec.ExtFields["clusterName"] != "DefaultCluster" {
		t.Errorf("ExtFields[clusterName] = %q, want DefaultCluster", dec.ExtFields["clusterName"])
	}
}

// TestRemotingCommandFrameFormat asserts the wire framing matches the RocketMQ
// remoting protocol: [4-byte frame size][4-byte marked header len][header][body],
// with the high byte of the marked header length selecting the codec (0 = JSON).
func TestRemotingCommandFrameFormat(t *testing.T) {
	cmd := NewRemotingCommand(int16(105), nil, []byte("X"))
	enc, err := encode(cmd)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(enc) < 8 {
		t.Fatalf("frame too short: %d bytes", len(enc))
	}
	// Frame size (first 4 bytes, big-endian) = bytes FOLLOWING the frame size
	// field (i.e. total len - 4): markedHeaderLen(4) + header + body.
	frameSize := int32(enc[0])<<24 | int32(enc[1])<<16 | int32(enc[2])<<8 | int32(enc[3])
	if int(frameSize) != len(enc)-4 {
		t.Errorf("frameSize = %d, want %d (total len - 4)", frameSize, len(enc)-4)
	}
	// Marked header length: high byte = codec type (0 = JSON), low 3 bytes = header len.
	if codec := enc[4]; codec != JsonCodecs {
		t.Errorf("codec byte = %d, want JsonCodecs (0)", codec)
	}
	headerLen := int32(enc[5])<<16 | int32(enc[6])<<8 | int32(enc[7])
	if headerLen <= 0 {
		t.Errorf("headerLen = %d, want > 0", headerLen)
	}
	// 4 (frameSize) + 4 (marked header len) + headerLen + bodyLen == total.
	bodyLen := (len(enc) - 4) - 4 - int(headerLen)
	if bodyLen != len(cmd.Body) {
		t.Errorf("bodyLen = %d, want %d", bodyLen, len(cmd.Body))
	}
}
