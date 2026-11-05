package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Sustained traffic exercises the default 30-second client replay window,
// peer history expiry, shared heartbeats and periodic cleanup together.
func TestStressSteadyRouting(t *testing.T) {
	if os.Getenv("WSROUTER_STEADY") != "1" {
		t.Skip("set WSROUTER_STEADY=1")
	}
	servers, urls := startCluster(t, 5)
	cleanup, stopCleanup := context.WithCancel(context.Background())
	defer stopCleanup()
	for _, s := range servers {
		go s.receiptCleanupLoop(cleanup)
	}
	nodes := make([]testNode, 100)
	for i := range nodes {
		nodes[i] = createNode(t, urls[i%5].URL, "steady", 1000000)
		owner, _ := servers[0].owner(nodes[i].id)
		nodes[i].conn = connectNode(t, owner.Addr, nodes[i].token, "")
	}
	payload := strings.Repeat("x", 1024)
	var sent, delivered, feedback atomic.Int64
	var latency [10001]atomic.Int64
	failures := make(chan error, 1)
	fail := func(err error) {
		select {
		case failures <- err:
		default:
		}
	}
	finished := make(chan struct{})
	var readers, writers sync.WaitGroup
	for i := range nodes {
		readers.Add(1)
		go func() {
			defer readers.Done()
			nodes[i].conn.SetReadDeadline(time.Now().Add(55 * time.Second))
			for {
				var p packet
				if err := nodes[i].conn.ReadJSON(&p); err != nil {
					select {
					case <-finished:
					default:
						fail(err)
					}
					return
				}
				if p.Dest != nodes[i].id {
					fail(fmt.Errorf("wrong destination: %s", p.Dest))
					return
				}
				switch p.Type {
				case "data":
					if p.Payload != payload {
						fail(fmt.Errorf("corrupt payload"))
						return
					}
					delivered.Add(1)
				case "status":
					if p.Payload != "success" {
						fail(fmt.Errorf("unexpected feedback: %+v", p))
						return
					}
					_, raw, ok := strings.Cut(p.PacketID, ":")
					stamp, err := strconv.ParseInt(raw, 10, 64)
					if !ok || err != nil {
						fail(fmt.Errorf("invalid correlation ID"))
						return
					}
					elapsed := max(0, time.Since(time.Unix(0, stamp)).Milliseconds())
					latency[min(elapsed, 10000)].Add(1)
					feedback.Add(1)
				default:
					fail(fmt.Errorf("unexpected packet type %q", p.Type))
					return
				}
			}
		}()
	}
	defer func() {
		close(finished)
		for i := range nodes {
			nodes[i].conn.Close()
		}
		readers.Wait()
	}()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	load, stopLoad := context.WithTimeout(context.Background(), 40*time.Second)
	defer stopLoad()
	for i := range nodes {
		writers.Add(1)
		go func() {
			defer writers.Done()
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			sequence := i + 1
			for {
				select {
				case <-load.Done():
					return
				case <-ticker.C:
					dest := sequence % len(nodes)
					sequence++
					if dest == i {
						dest = (dest + 1) % len(nodes)
					}
					p := packet{PacketID: fmt.Sprintf("%d:%d", i, time.Now().UnixNano()), Type: "data", Dest: nodes[dest].id, Payload: payload}
					nodes[i].conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
					if err := nodes[i].conn.WriteJSON(p); err != nil {
						fail(err)
						return
					}
					sent.Add(1)
				}
			}
		}()
	}
	writingDone := make(chan struct{})
	go func() { writers.Wait(); close(writingDone) }()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	peak := before.HeapAlloc
	start := time.Now()
	writing := true
	for writing {
		select {
		case err := <-failures:
			stopLoad()
			writers.Wait()
			t.Fatal(err)
		case <-writingDone:
			writing = false
		case <-ticker.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			peak = max(peak, m.HeapAlloc)
			t.Logf("steady %.0fs: sent=%d delivered=%d feedback=%d heap=%.1f MiB", time.Since(start).Seconds(), sent.Load(), delivered.Load(), feedback.Load(), float64(m.HeapAlloc)/(1<<20))
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for delivered.Load() != sent.Load() || feedback.Load() != sent.Load() {
		select {
		case err := <-failures:
			t.Fatal(err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("undelivered packets: sent=%d delivered=%d feedback=%d", sent.Load(), delivered.Load(), feedback.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	percentile := func(percent int64) int {
		want := (feedback.Load()*percent + 99) / 100
		var count int64
		for i := range latency {
			count += latency[i].Load()
			if count >= want {
				return i
			}
		}
		return 10000
	}
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	peak = max(peak, after.HeapAlloc)
	t.Logf("steady complete: sent=%d p50=%dms p99=%dms sampled_heap_max=%.1f MiB alloc=%.1f MiB", sent.Load(), percentile(50), percentile(99), float64(peak)/(1<<20), float64(after.TotalAlloc-before.TotalAlloc)/(1<<20))
	for _, s := range servers {
		s.cleanupReceipts(time.Now().Add(31 * time.Second))
	}
}
