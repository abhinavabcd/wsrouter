package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSharedConcurrentQuota(t *testing.T) {
	state := &nodeState{rp5: 100}
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl := client{state: state}
			if reason := cl.admit(packet{Dests: []string{"one", "two"}}, 10000); reason == "" {
				accepted.Add(1)
			} else if reason != "rate_limited" {
				t.Errorf("admit: %s", reason)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 100 || state.rateCount != 100 {
		t.Fatalf("accepted/count: %d/%d", accepted.Load(), state.rateCount)
	}
}

func TestNodePolicyAndReconnect(t *testing.T) {
	s, ts := startTestRouter(t, 0, "")
	node := createNode(t, ts.URL, "account", 10)
	first := connectNode(t, ts.URL, node.token, "")
	waitConnections(t, s, node.id, 1)
	s.mu.RLock()
	state := s.nodes[node.id]
	s.mu.RUnlock()
	state.mu.Lock()
	state.rateBucket = time.Now().UnixMilli() / 5000
	state.rateCount = 3
	state.mu.Unlock()
	account := accountFor(node)
	account.mu.Lock()
	account.rate = 2
	account.mu.Unlock()
	second := connectNode(t, ts.URL, node.token, "")
	waitConnections(t, s, node.id, 2)
	state.mu.Lock()
	if state.rp5 != 2 || state.rateCount != 3 {
		t.Errorf("policy update reset counters: %d/%d", state.rp5, state.rateCount)
	}
	state.mu.Unlock()
	beforeClose := time.Now()
	first.Close()
	second.Close()
	waitConnections(t, s, node.id, 0)
	if s.nodeCount.Load() != 0 || s.deliver(packet{Dest: node.id}, nil) {
		t.Fatal("socketless node appears connected")
	}
	state.mu.Lock()
	if state.cleanupAt.Before(beforeClose.Add(10*time.Second)) || !state.cleanupCheckPending {
		t.Error("last close did not retain state")
	}
	state.mu.Unlock()
	account.mu.Lock()
	account.rate = 20
	account.mu.Unlock()
	third := connectNode(t, ts.URL, node.token, "")
	waitConnections(t, s, node.id, 1)
	s.mu.RLock()
	if s.nodes[node.id] != state {
		t.Error("reconnect lost retained state")
	}
	s.mu.RUnlock()
	state.mu.Lock()
	if state.rp5 != 20 || state.rateCount != 3 {
		t.Errorf("reconnect policy/count: %d/%d", state.rp5, state.rateCount)
	}
	state.mu.Unlock()
	s.cleanupNodes(time.Now().Add(time.Minute))
	state.mu.Lock()
	if state.cleanupCheckPending {
		t.Error("active node kept pending check")
	}
	state.mu.Unlock()
	third.Close()
	waitConnections(t, s, node.id, 0)
	s.cleanupNodes(time.Now().Add(time.Minute))
	connectNode(t, ts.URL, node.token, "")
	waitConnections(t, s, node.id, 1)
	s.mu.RLock()
	replacement := s.nodes[node.id]
	s.mu.RUnlock()
	replacement.mu.Lock()
	defer replacement.mu.Unlock()
	if replacement == state || replacement.rp5 != 20 || replacement.rateCount != 0 {
		t.Errorf("replacement policy/count: %d/%d", replacement.rp5, replacement.rateCount)
	}
}

func TestNodeCleanupDeadlines(t *testing.T) {
	s := newServer(0, "https://router.example", "")
	now := time.Now()
	s.mu.Lock()
	state := s.lockNode("node", 5)
	s.scheduleNodeCleanup(state, now.Add(10*time.Second))
	for range 100 {
		s.scheduleNodeCleanup(state, now.Add(60*time.Second))
	}
	state.mu.Unlock()
	s.mu.Unlock()
	if len(s.cleanupQueue) != 1 || !s.cleanupQueue[0].scheduledTime.Equal(now.Add(10*time.Second)) {
		t.Fatal("duplicate check or eager deadline update")
	}
	s.cleanupNodes(now.Add(11 * time.Second))
	if len(s.cleanupQueue) != 1 || !s.cleanupQueue[0].scheduledTime.Equal(now.Add(60*time.Second)) || !state.cleanupCheckPending {
		t.Fatal("extended deadline not requeued")
	}
	s.mu.Lock()
	state.mu.Lock()
	state.connections[&client{}] = struct{}{}
	state.mu.Unlock()
	s.mu.Unlock()
	s.cleanupNodes(now.Add(61 * time.Second))
	if len(s.cleanupQueue) != 0 || state.cleanupCheckPending || s.nodes["node"] != state {
		t.Fatal("active node deleted or pending not cleared")
	}
	s.mu.Lock()
	state.mu.Lock()
	clear(state.connections)
	s.scheduleNodeCleanup(state, now.Add(70*time.Second))
	state.mu.Unlock()
	s.mu.Unlock()
	s.cleanupNodes(now.Add(70 * time.Second))
	if s.nodes["node"] != nil || state.cleanupCheckPending {
		t.Fatal("socketless state not deleted")
	}
	// Stale heap entries must never delete a replacement object with the same ID.
	s.mu.Lock()
	old := s.lockNode("same", 1)
	s.scheduleNodeCleanup(old, now)
	old.mu.Unlock()
	replacement := &nodeState{nodeID: "same", connections: make(map[*client]struct{})}
	s.nodes["same"] = replacement
	s.mu.Unlock()
	s.cleanupNodes(now)
	if s.nodes["same"] != replacement {
		t.Fatal("stale entry deleted replacement")
	}
}

func TestNodeCleanupActivityRace(t *testing.T) {
	s := newServer(0, "https://router.example", "")
	now := time.Now()
	for i := range 500 {
		id := fmt.Sprint(i)
		s.mu.Lock()
		state := s.lockNode(id, 5)
		s.scheduleNodeCleanup(state, now)
		state.mu.Unlock()
		s.mu.Unlock()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); s.cleanupNodes(now) }()
		go func() {
			defer wg.Done()
			s.mu.Lock()
			current := s.lockNode(id, 5)
			s.scheduleNodeCleanup(current, now.Add(time.Minute))
			current.mu.Unlock()
			s.mu.Unlock()
		}()
		wg.Wait()
		s.mu.RLock()
		current := s.nodes[id]
		if current == nil {
			t.Fatal("cleanup deleted renewed activity")
		}
		current.mu.Lock()
		if !current.cleanupCheckPending || current.cleanupAt.Before(now.Add(time.Minute)) {
			t.Error("lost activity deadline")
		}
		current.mu.Unlock()
		s.mu.RUnlock()
	}
	if len(s.cleanupQueue) != 500 {
		t.Fatalf("duplicate or missing checks: %d", len(s.cleanupQueue))
	}
	s.cleanupNodes(now.Add(time.Minute))
	if len(s.nodes) != 0 {
		t.Fatal("expired nodes leaked")
	}
}

func TestNodeCleanupTimerWake(t *testing.T) {
	s := newServer(0, "https://router.example", "")
	defer close(s.routingDone)
	go s.nodeCleanupLoop()
	add := func(id string, d time.Duration) {
		s.mu.Lock()
		state := s.lockNode(id, 1)
		s.scheduleNodeCleanup(state, time.Now().Add(d))
		state.mu.Unlock()
		s.mu.Unlock()
	}
	add("later", time.Hour)
	add("earlier", 20*time.Millisecond)
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.RLock()
		gone := s.nodes["earlier"] == nil
		later := s.nodes["later"] != nil
		s.mu.RUnlock()
		if gone {
			if !later {
				t.Fatal("later node removed early")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("earlier insertion failed to wake timer")
		}
		time.Sleep(time.Millisecond)
	}
}
