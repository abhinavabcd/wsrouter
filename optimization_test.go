package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestEstablishedPeerBypassesDialLock(t *testing.T) {
	servers, _ := startCluster(t, 2)
	owner := peer{ID: servers[1].id, Addr: servers[1].addr}
	want, err := servers[0].getPeerLink(owner)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := servers[0].peerDials.Load(owner.ID)
	lock := raw.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()
	done := make(chan *peerLink, 1)
	go func() { got, _ := servers[0].getPeerLink(owner); done <- got }()
	select {
	case got := <-done:
		if got != want {
			t.Fatal("did not reuse established peer")
		}
	case <-time.After(time.Second):
		t.Fatal("established peer waited for a dial lock")
	}
}

func TestPublishedRingDoesNotAliasInput(t *testing.T) {
	s := newServer(0, "https://one.example", "")
	peers := []peer{{ID: 10, Addr: "https://two.example"}, {ID: 0, Addr: s.addr}}
	s.applySnapshot(peers, s.epoch, 2)
	peers[0].Addr = "https://changed.example"
	published := s.routingRing.Load().([]peer)
	if published[1].Addr != "https://two.example" || peers[0].ID != 10 {
		t.Fatal("published ring aliases or mutates the supplied slice")
	}
}

func TestOutgoingQueueLimitsDoNotRecordRejectedPackets(t *testing.T) {
	cl := &client{done: make(chan struct{}), send: make(chan outbound, 8)}
	data := make([]byte, 1<<20)
	for range 4 {
		if !cl.queue(outbound{data: data}) {
			t.Fatal("queue rejected its byte allowance")
		}
	}
	if cl.queue(outbound{data: []byte("extra")}) || cl.queuedBytes.Load() != 4<<20 {
		t.Fatal("client queue exceeded its byte allowance or leaked accounting")
	}
	link := &peerLink{done: make(chan struct{}), ready: make(chan struct{}, 1)}
	p := packet{Dest: "dest", Type: "data", Payload: "first"}
	if err := link.enqueue(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	link.data.bytes = 64 << 20
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := link.enqueue(ctx, p); err == nil || ctx.Err() != nil {
		t.Fatal("full peer queue did not reject immediately")
	}
	if len(link.history) != 1 {
		t.Fatal("rejected packet entered replay history")
	}
	link.data.bytes = len(link.data.entries[0])
	link.feedback.push([]byte("feedback"))
	if got := link.takeFrame(true); string(got) == "feedback" || link.data.bytes != 0 {
		t.Fatal("data turn was starved or queue bytes were not released")
	}
	if got := link.takeFrame(false); string(got) != "feedback" || link.feedback.bytes != 0 {
		t.Fatal("feedback turn lost its packet")
	}
}

func TestExpiredReplayReleasesReferences(t *testing.T) {
	now := time.Now()
	history := make([]replayRecord, 2050)
	for i := range history {
		at := now
		if i < 1500 {
			at = now.Add(-time.Minute)
		}
		history[i] = replayRecord{packet: &packet{PacketID: strconv.Itoa(i)}, sentAt: at}
	}
	retained, head := pruneHistory(history, 0, now.Add(-time.Second))
	if head != 0 || len(retained) != 550 || retained[0].packet.PacketID != "1500" {
		t.Fatal("history compaction lost live packets")
	}
	for _, entry := range history[len(retained):] {
		if entry.packet != nil {
			t.Fatal("expired history retained a packet")
		}
	}
}

func TestSharedHeartbeatUsesConnectionIntervals(t *testing.T) {
	servers, urls := startCluster(t, 1)
	node := createNode(t, urls[0].URL, "pings")
	var counts [2]atomic.Int64
	var conns []*websocket.Conn
	var readers sync.WaitGroup
	for i, interval := range []string{"0.02", "0.10"} {
		conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(urls[0].URL, "http")+"/connect?"+nodeQuery(node.id, node.token)+"&tcp_keep_alive="+interval, nil)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, conn)
		t.Cleanup(func() { conn.Close() })
		pong := conn.PingHandler()
		conn.SetPingHandler(func(data string) error { counts[i].Add(1); return pong(data) })
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for counts[1].Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if counts[1].Load() < 2 || counts[0].Load() <= counts[1].Load() {
		t.Fatal("connection ping intervals not respected")
	}
	for _, conn := range conns {
		conn.Close()
	}
	readers.Wait()
	waitConnections(t, servers[0], node.id, 0)
	servers[0].pingMu.Lock()
	remaining := len(servers[0].pings)
	servers[0].pingMu.Unlock()
	if remaining != 0 {
		t.Fatal("closed sockets remain in heartbeat scheduler")
	}
}

func TestPeerBatchNegotiationAndLegacyFrames(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprint(batch), func(t *testing.T) {
			servers, urls := startCluster(t, 2)
			var node testNode
			for {
				node = createNode(t, urls[0].URL, "batch-target")
				owner, _ := servers[0].owner(node.id)
				if owner.ID == servers[1].id {
					break
				}
			}
			dst := connectNode(t, urls[1].URL, node.token, "")
			waitConnections(t, servers[1], node.id, 1)
			headers := http.Header{}
			headers.Set("X-Server-ID", strconv.FormatUint(servers[0].id, 10))
			headers.Set("X-Node-ID", servers[0].serverNodeID)
			headers.Set("Authorization", "Bearer "+servers[0].serverToken)
			if batch {
				headers.Set("X-Peer-Batching", "1")
			}
			conn, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(urls[1].URL, "http")+"/peer", headers)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if (resp.Header.Get("X-Peer-Batching") == "1") != batch {
				t.Fatal("unexpected batching negotiation")
			}
			frame := peerFrame{Packet: packet{PacketID: "first", Type: "status", SrcNodeID: servers[0].serverNodeID, Dest: node.id, Payload: "success"}}
			data, _ := json.MarshalIndent(frame, "", "  ")
			want := 1
			if batch {
				data, _ = json.Marshal(frame)
				frame.Packet.PacketID = "second"
				next, _ := json.Marshal(frame)
				data = append(append(data, '\n'), next...)
				want = 2
			}
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < want; i++ {
				dst.SetReadDeadline(time.Now().Add(time.Second))
				var got packet
				if err := dst.ReadJSON(&got); err != nil || got.Payload != "success" {
					t.Fatalf("peer delivery: %+v %v", got, err)
				}
			}
		})
	}
}
