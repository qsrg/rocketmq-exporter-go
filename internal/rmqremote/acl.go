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
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"hash"
	"sort"
	"strings"
)

// ACL ext-field keys (match org.apache.rocketmq.acl.common.SessionCredentials).
const (
	aclSignature = "Signature"
	aclAccessKey = "AccessKey"
	aclSecurityToken = "SecurityToken"
)

// SignACL computes the RocketMQ ACL signature over cmd and attaches it (plus the
// access key) to cmd.ExtFields. It is a verbatim port of rocketmq-client-go's
// internal/remote.ACLInterceptor + calculateSignature, which is itself a port of
// rocketmq-acl (Java) and is broker-tested. Call after the request header's
// ExtFields are populated and BEFORE encode().
//
// Canonical string (rocketmq-acl SigningBase): collect AccessKey (+ optional
// SecurityToken) and every existing ExtFields entry into a map, sort the keys
// lexicographically, concatenate the VALUES in that order, append cmd.Body, then
// HMAC-SHA1 with the secret key and base64-encode.
func SignACL(cmd *RemotingCommand, accessKey, secretKey string) {
	if cmd.ExtFields == nil {
		cmd.ExtFields = make(map[string]string)
	}
	m := make(map[string]string, len(cmd.ExtFields)+2)
	order := make([]string, 0, len(cmd.ExtFields)+2)

	m[aclAccessKey] = accessKey
	order = append(order, aclAccessKey)
	if tok := cmd.ExtFields[aclSecurityToken]; tok != "" {
		m[aclSecurityToken] = tok
		order = append(order, aclSecurityToken)
	}
	for k, v := range cmd.ExtFields {
		if k == aclAccessKey || k == aclSecurityToken || k == aclSignature {
			continue
		}
		m[k] = v
		order = append(order, k)
	}
	sort.Slice(order, func(i, j int) bool { return strings.Compare(order[i], order[j]) < 0 })

	var content strings.Builder
	for _, k := range order {
		content.WriteString(m[k])
	}
	buf := make([]byte, content.Len()+len(cmd.Body))
	copy(buf, content.String())
	copy(buf[content.Len():], cmd.Body)

	cmd.ExtFields[aclSignature] = calculateSignature(buf, []byte(secretKey))
	cmd.ExtFields[aclAccessKey] = accessKey
}

// calculateSignature = base64(HMAC-SHA1(secretKey, data)). Verbatim from
// rocketmq-client-go internal/remote.calculateSignature.
func calculateSignature(data, secretKey []byte) string {
	mac := hmac.New(func() hash.Hash { return sha1.New() }, secretKey)
	mac.Write(data)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
