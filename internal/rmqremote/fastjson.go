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
	"encoding/json"
	"strings"
)

// UnmarshalJSON decodes a RocketMQ broker/namesrv response body into v.
//
// The 4.x broker serializes bodies with Alibaba fastjson, which emits
// NON-STANDARD JSON for Map<Long, V> and Map<Object, V>: bare (unquoted) object
// keys. For numeric keys: {"brokerAddrs":{0:"..."}}. For composite-object keys
// (e.g. Map<MessageQueue, X> in TopicStatsTable/ConsumeStats):
// {"offsetTable":{{"brokerName":...}:{...}}}. Go's encoding/json (and
// json-iterator) reject unquoted keys. This helper normalizes bare keys (numeric
// or object/array) into quoted string keys — context-aware so bare tokens that
// are VALUES (array elements, after ':') are left alone — then delegates to
// encoding/json. This is the mitigation for the design's #1 risk (Go json vs
// fastjson drift).
func UnmarshalJSON(body []byte, v any) error {
	normalized := normalizeKeys(body)
	return json.Unmarshal(normalized, v)
}

// parse state: whether the next structural token is a key (in an object) or a
// value.
type pstate byte

const (
	pExpectKey    pstate = iota // after '{' (object open) or ',' in object — next is a key
	pExpectColon              // after a key — next is ':'
	pExpectValue              // after ':' or '[' (array open) or ',' in array — next is a value
	pAfterValue               // after a value — next is ',' or '}'/']'
)

// normalizeKeys quotes bare non-string object keys. It is a string-aware,
// container-aware state machine: a bare `{`/`[`/number is treated as a key to
// quote ONLY when it appears in object-key position; as a value (array element
// or after ':') it is emitted verbatim. Strings are copied byte-for-byte so
// colons/braces inside string values are never mistaken for structure.
func normalizeKeys(b []byte) []byte {
	out := make([]byte, 0, len(b)+16)
	stack := make([]byte, 0, 16) // 'o' (object) or 'a' (array)
	st := pExpectValue
	inString := false
	escaped := false
	n := len(b)

	// topKind returns the container we're inside ('o'/'a'/0 if root).
	topKind := func() byte {
		if len(stack) == 0 {
			return 0
		}
		return stack[len(stack)-1]
	}

	emit := func(bs ...byte) { out = append(out, bs...) }

	for i := 0; i < n; {
		c := b[i]
		if inString {
			out = append(out, c)
			if escaped {
				escaped = false
				i++
				continue
			}
			if c == '\\' {
				escaped = true
			} else if c == '"' {
				inString = false
				// a string was a key or a value depending on state
				if st == pExpectKey {
					st = pExpectColon
				} else if st == pExpectValue {
					st = pAfterValue
				}
			}
			i++
			continue
		}
		switch c {
		case ' ', '\t', '\n', '\r':
			emit(c)
			i++
			continue
		case '"':
			inString = true
			emit(c)
			i++
			continue
		case ':':
			emit(c)
			st = pExpectValue
			i++
			continue
		case '{':
			if st == pExpectKey {
				// bare object key (composite MessageQueue). Capture & quote it.
				token, next, ok := captureValue(b, i)
				if !ok {
					emit(b[i:]...)
					return out
				}
				out = append(out, '"')
				out = append(out, escapeForString(token)...)
				out = append(out, '"')
				i = next
				st = pExpectColon
				continue
			}
			// object value opener
			emit(c)
			stack = append(stack, 'o')
			st = pExpectKey
			i++
			continue
		case '[':
			if st == pExpectKey {
				// bare array key (rare). Capture & quote.
				token, next, ok := captureValue(b, i)
				if !ok {
					emit(b[i:]...)
					return out
				}
				out = append(out, '"')
				out = append(out, escapeForString(token)...)
				out = append(out, '"')
				i = next
				st = pExpectColon
				continue
			}
			emit(c)
			stack = append(stack, 'a')
			st = pExpectValue
			i++
			continue
		case '}', ']':
			emit(c)
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
			st = pAfterValue
			i++
			continue
		case ',':
			emit(c)
			if topKind() == 'o' {
				st = pExpectKey
			} else {
				st = pExpectValue
			}
			i++
			continue
		case 't', 'f', 'n':
			// true/false/null token — capture the keyword, emit verbatim.
			token, next := scanKeyword(b, i)
			emit(token...)
			i = next
			st = pAfterValue
			continue
		case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			token, next := scanNumber(b, i)
			if st == pExpectKey {
				out = append(out, '"')
				out = append(out, token...)
				out = append(out, '"')
				st = pExpectColon
			} else {
				emit(token...)
				st = pAfterValue
			}
			i = next
			continue
		default:
			emit(c)
			i++
		}
	}
	return out
}


// captureValue scans one complete JSON value starting at b[i] (which must be '{'
// or '['), bracket- and string-aware, returning the raw token and the index
// just past it.
func captureValue(b []byte, i int) (token []byte, next int, ok bool) {
	n := len(b)
	open := b[i]
	closeB := byte('}')
	if open == '[' {
		closeB = ']'
	}
	depth := 0
	inStr := false
	esc := false
	start := i
	for ; i < n; i++ {
		c := b[i]
		if inStr {
			if esc {
				esc = false
				continue
			}
			if c == '\\' {
				esc = true
				continue
			}
			if c == '"' {
				inStr = false
			}
			continue
		}
		switch c {
		case '"':
			inStr = true
		case open:
			depth++
		case closeB:
			depth--
			if depth == 0 {
				return b[start : i+1], i + 1, true
			}
		}
	}
	return nil, i, false
}

func scanNumber(b []byte, i int) (token []byte, next int) {
	n := len(b)
	start := i
	if i < n && b[i] == '-' {
		i++
	}
	for i < n && (b[i] == '.' || b[i] == 'e' || b[i] == 'E' || b[i] == '+' || b[i] == '-' || (b[i] >= '0' && b[i] <= '9')) {
		i++
	}
	return b[start:i], i
}

func scanKeyword(b []byte, i int) (token []byte, next int) {
	n := len(b)
	start := i
	for i < n && (b[i] >= 'a' && b[i] <= 'z') {
		i++
	}
	return b[start:i], i
}

// escapeForString escapes a raw JSON token so it can sit inside a JSON string
// literal (the key material, e.g. a MessageQueue object, decoded later by the
// caller).
func escapeForString(token []byte) []byte {
	s := string(token)
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return []byte(s)
}
