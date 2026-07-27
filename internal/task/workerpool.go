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
)

// Task is a unit of background work submitted to the pool (a port of
// ClientMetricTaskRunnable.run as a closure).
type Task func()

// WorkerPool is a bounded worker pool that mirrors the Java exporter's
// ClientMetricCollectorFixedThreadPoolExecutor + ThreadPoolExecutor.DiscardOldestPolicy:
//   - at most `workers` goroutines run tasks concurrently;
//   - a FIFO buffer of capacity `queue` holds pending tasks;
//   - Submit is NON-BLOCKING: when the buffer is full, the OLDEST queued task is
//     dropped (not the newest) and the new task is enqueued, matching Java's
//     DiscardOldestPolicy (rejectedExecution: queue.poll(); execute(r)).
type WorkerPool struct {
	mu      sync.Mutex
	cond    *sync.Cond
	buf     []Task
	cap     int
	workers int
	stopped bool
	wg      sync.WaitGroup
}

// New returns a pool with `workers` workers and a `queue`-sized FIFO buffer.
func New(workers, queue int) *WorkerPool {
	if workers < 1 {
		workers = 1
	}
	if queue < 1 {
		queue = 1
	}
	p := &WorkerPool{cap: queue, workers: workers}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// Start spawns the worker goroutines, which run until Shutdown drains the buffer.
func (p *WorkerPool) Start(_ context.Context) {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

func (p *WorkerPool) worker() {
	defer p.wg.Done()
	for {
		p.mu.Lock()
		for !p.stopped && len(p.buf) == 0 {
			p.cond.Wait()
		}
		if len(p.buf) == 0 {
			// stopped and drained
			p.mu.Unlock()
			return
		}
		t := p.buf[0]
		p.buf = p.buf[1:]
		p.mu.Unlock()
		t()
	}
}

// Submit enqueues a task non-blockingly. If the buffer is full the OLDEST queued
// task is dropped (DiscardOldestPolicy). After Shutdown, submissions are dropped.
func (p *WorkerPool) Submit(t Task) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	if len(p.buf) >= p.cap {
		// Drop the oldest (head) and enqueue the new task.
		copy(p.buf, p.buf[1:])
		p.buf = p.buf[:len(p.buf)-1]
	}
	p.buf = append(p.buf, t)
	p.cond.Signal()
}

// Shutdown stops accepting new tasks, lets workers drain the remaining buffer,
// and waits for all in-flight tasks to finish (best-effort within ctx).
func (p *WorkerPool) Shutdown(_ context.Context) {
	p.mu.Lock()
	p.stopped = true
	p.cond.Broadcast()
	p.mu.Unlock()
	p.wg.Wait()
}

// QueueLen returns the current pending-task count (for tests/observability).
func (p *WorkerPool) QueueLen() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.buf)
}
