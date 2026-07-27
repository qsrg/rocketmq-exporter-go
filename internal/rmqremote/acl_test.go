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
	"testing"
)

// TestCalculateSignatureVector validates HMAC-SHA1+base64 against the known
// rocketmq-client-go interceptor_test vector.
func TestCalculateSignatureVector(t *testing.T) {
	got := calculateSignature([]byte("Hello RocketMQ Client ACL Feature"), []byte("adiaushdiaushd"))
	const want = "tAb/54Rwwcq+pbH8Loi7FWX4QSQ="
	if got != want {
		t.Errorf("calculateSignature = %q, want %q", got, want)
	}
}

// TestSignACLAttachesFields asserts SignACL populates AccessKey + Signature and
// that the signature is deterministic for a fixed input (regression guard for
// the canonical-string construction).
func TestSignACLAttachesFields(t *testing.T) {
	cmd := NewRemotingCommand(106, nil, []byte("body"))
	cmd.ExtFields["topic"] = "t"
	cmd.ExtFields["consumerGroup"] = "g"
	SignACL(cmd, "RocketMQ", "12345678")

	if cmd.ExtFields[aclAccessKey] != "RocketMQ" {
		t.Errorf("AccessKey = %q, want RocketMQ", cmd.ExtFields[aclAccessKey])
	}
	if cmd.ExtFields[aclSignature] == "" {
		t.Fatal("Signature not set")
	}

	// Deterministic: same input -> same signature.
	cmd2 := NewRemotingCommand(106, nil, []byte("body"))
	cmd2.ExtFields["topic"] = "t"
	cmd2.ExtFields["consumerGroup"] = "g"
	SignACL(cmd2, "RocketMQ", "12345678")
	if cmd.ExtFields[aclSignature] != cmd2.ExtFields[aclSignature] {
		t.Errorf("signature not deterministic: %q vs %q", cmd.ExtFields[aclSignature], cmd2.ExtFields[aclSignature])
	}

	// Different secret -> different signature.
	cmd3 := NewRemotingCommand(106, nil, []byte("body"))
	cmd3.ExtFields["topic"] = "t"
	cmd3.ExtFields["consumerGroup"] = "g"
	SignACL(cmd3, "RocketMQ", "different")
	if cmd.ExtFields[aclSignature] == cmd3.ExtFields[aclSignature] {
		t.Error("signature should change with a different secret key")
	}
}

// TestSignACLCanonicalOrderIndependence asserts ext-field insertion order does
// not change the signature (the canonical string sorts keys).
func TestSignACLCanonicalOrderIndependence(t *testing.T) {
	mk := func() *RemotingCommand {
		c := NewRemotingCommand(208, nil, []byte("b"))
		c.ExtFields = map[string]string{}
		return c
	}
	a := mk(); a.ExtFields["topic"] = "t"; a.ExtFields["group"] = "g"; a.ExtFields["queueId"] = "1"
	b := mk(); b.ExtFields["queueId"] = "1"; b.ExtFields["group"] = "g"; b.ExtFields["topic"] = "t"
	SignACL(a, "k", "s")
	SignACL(b, "k", "s")
	if a.ExtFields[aclSignature] != b.ExtFields[aclSignature] {
		t.Errorf("signature must be order-independent: %q vs %q", a.ExtFields[aclSignature], b.ExtFields[aclSignature])
	}
}
