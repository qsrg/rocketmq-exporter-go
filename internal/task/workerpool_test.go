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

package task

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerPoolRunsTasks(t *testing.T) {
	var done int64
	p := New(4, 16)
	p.Start(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		p.Submit(func() { atomic.AddInt64(&done, 1); wg.Done() })
	}
	wg.Wait()
	p.Shutdown(context.Background())
	if got := atomic.LoadInt64(&done); got != 16 {
		t.Errorf("ran %d tasks, want 16", got)
	}
}

// TestWorkerPoolDiscardsOldest asserts DiscardOldestPolicy: when the buffer is
// saturated the OLDEST queued task is dropped (not the newest), and the caller
// never blocks on Submit.
func TestWorkerPoolDiscardsOldest(t *testing.T) {
	var ranB, ranC, ranD int64
	p := New(1, 2) // 1 worker, buffer cap 2
	p.Start(context.Background())

	// Block the single worker so the buffer fills behind it.
	block := make(chan struct{})
	started := make(chan struct{})
	p.Submit(func() {
		close(started) // signal the worker has taken task A and is now blocked
		<-block
	})
	<-started

	// Buffer is empty but the worker is busy. Enqueue B (oldest), then C — now full.
	p.Submit(func() { atomic.StoreInt64(&ranB, 1) }) // B
	p.Submit(func() { atomic.StoreInt64(&ranC, 1) }) // C — buffer now full (B,C)
	// Submit D while full: must drop the OLDEST (B), keep C and D. Non-blocking.
	start := time.Now()
	p.Submit(func() { atomic.StoreInt64(&ranD, 1) }) // D
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("Submit blocked for %v — must be non-blocking", elapsed)
	}

	close(block) // release the worker; A finishes, then C and D run (B was dropped)
	p.Shutdown(context.Background())

	if atomic.LoadInt64(&ranB) != 0 {
		t.Errorf("oldest task B ran; want dropped (DiscardOldestPolicy)")
	}
	if atomic.LoadInt64(&ranC) != 1 {
		t.Errorf("task C did not run; want ran")
	}
	if atomic.LoadInt64(&ranD) != 1 {
		t.Errorf("task D did not run; want ran")
	}
}

// TestWorkerPoolNonBlockingUnderSaturation: a full queue never blocks Submit.
func TestWorkerPoolNonBlockingUnderSaturation(t *testing.T) {
	p := New(1, 4)
	p.Start(context.Background())
	block := make(chan struct{})
	p.Submit(func() { <-block })
	start := time.Now()
	for i := 0; i < 1000; i++ {
		p.Submit(func() {})
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("1000 saturated submits took %v — expected near-instant", elapsed)
	}
	close(block)
	p.Shutdown(context.Background())
}

func TestWorkerPoolShutdownDropsNewSubmissions(t *testing.T) {
	p := New(2, 4)
	p.Start(context.Background())
	p.Shutdown(context.Background())
	var ran int64
	p.Submit(func() { atomic.AddInt64(&ran, 1) })
	if got := atomic.LoadInt64(&ran); got != 0 {
		t.Errorf("post-Shutdown Submit ran; want dropped")
	}
}
