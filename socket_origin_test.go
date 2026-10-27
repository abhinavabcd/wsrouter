package main

import (
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestFanoutSkipsOnlyOriginSocket(t *testing.T) {
	servers, urls := startCluster(t, 2)
	a := createNode(t, urls[0].URL, "a", 1000)
	owner, _ := servers[0].owner(a.id)
	local, remote := servers[0], servers[1]
	if owner.ID != local.id {
		local, remote = remote, local
	}
	var b testNode
	for {
		b = createNode(t, urls[0].URL, "b", 1000)
		p, _ := local.owner(b.id)
		if p.ID == remote.id {
			break
		}
	}
	first := connectNode(t, local.addr, a.token, "")
	other := connectNode(t, local.addr, a.token, "")
	dest := connectNode(t, remote.addr, b.token, "")
	waitConnections(t, local, a.id, 2)
	waitConnections(t, remote, b.id, 1)
	local.mu.RLock()
	state := local.nodes[a.id]
	state.mu.Lock()
	var origin, sibling *client
	for cl := range state.connections {
		if cl.connectionOrder == 1 {
			origin = cl
		} else {
			sibling = cl
		}
	}
	state.mu.Unlock()
	local.mu.RUnlock()
	if origin.socketID == "" || origin.socketID == sibling.socketID {
		t.Fatal("socket IDs are not unique")
	}
	read := func(conn *websocket.Conn) packet {
		t.Helper()
		conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		var p packet
		if err := conn.ReadJSON(&p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	// A client cannot spoof the internal origin to suppress another socket.
	if err := first.WriteJSON(map[string]any{"_id": "fanout", "type": "data", "dest_node_ids": []string{a.id, b.id}, "src_socket_id": sibling.socketID}); err != nil {
		t.Fatal(err)
	}
	outcomes := map[string]bool{}
	for range 2 {
		p := read(first)
		if p.Type != "status" || p.Payload != "success" || p.PacketID != "fanout" {
			t.Fatalf("origin received its data: %+v", p)
		}
		outcomes[p.SrcNodeID] = true
	}
	if !outcomes[a.id] || !outcomes[b.id] {
		t.Fatal("fanout feedback missing")
	}
	dataCount := 0
	for range 3 {
		p := read(other)
		if p.Type == "data" {
			dataCount++
			if p.Dest != a.id || p.SrcNodeID != a.id {
				t.Fatalf("bad self delivery: %+v", p)
			}
		} else if p.Type != "status" {
			t.Fatalf("unexpected packet: %+v", p)
		}
	}
	if dataCount != 1 {
		t.Fatal("sibling socket missed self delivery")
	}
	if p := read(dest); p.Type != "data" || p.Dest != b.id || p.SrcNodeID != a.id {
		t.Fatalf("remote delivery: %+v", p)
	}
	// Replay the self-addressed copy through another router. Peer transport must
	// preserve the origin marker, and router-generated feedback must still arrive.
	local.mu.RLock()
	state.mu.Lock()
	var replay packet
	for _, item := range sibling.sent {
		if item.packet.Type == "data" {
			replay = *item.packet
			break
		}
	}
	state.mu.Unlock()
	local.mu.RUnlock()
	if replay.srcSocketID != origin.socketID {
		t.Fatal("history lost origin socket")
	}
	replay.replays++
	remote.route(replay)
	if p := read(first); p.Type != "status" || p.Payload != "success" {
		t.Fatalf("peer replay echoed to origin: %+v", p)
	}
	if p := read(other); p.Type != "data" {
		t.Fatalf("peer replay missing: %+v", p)
	}
	if p := read(other); p.Type != "status" {
		t.Fatalf("peer replay feedback missing: %+v", p)
	}
	// A reconnect is a different socket and must be eligible for old packets.
	first.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
	first.Close()
	waitConnections(t, local, a.id, 1)
	replacement := connectNode(t, local.addr, a.token, "")
	waitConnections(t, local, a.id, 2)
	local.route(replay)
	if p := read(replacement); p.Type != "data" || p.PacketID != "fanout" {
		t.Fatalf("reconnect reused origin ID: %+v", p)
	}
	if p := read(replacement); p.Type != "status" || p.Payload != "success" {
		t.Fatalf("reconnect feedback: %+v", p)
	}
}

func TestSelfSendWithoutAnotherSocket(t *testing.T) {
	s, ts := startTestRouter(t, 0, "")
	node := createNode(t, ts.URL, "self")
	conn := connectNode(t, ts.URL, node.token, "")
	waitConnections(t, s, node.id, 1)
	if err := conn.WriteJSON(packet{Type: "data", Dest: node.id, PacketID: "only-socket"}); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var p packet
	if err := conn.ReadJSON(&p); err != nil || p.Type != "status" || p.Payload != "failed" || p.PacketID != "only-socket" {
		t.Fatalf("unexpected self-send result: %+v %v", p, err)
	}
	s.mu.RLock()
	state := s.nodes[node.id]
	state.mu.Lock()
	var previousID string
	for cl := range state.connections {
		previousID = cl.socketID
	}
	state.mu.Unlock()
	s.mu.RUnlock()
	s.disconnectNode(node.id)
	waitConnections(t, s, node.id, 0)
	replacement := connectNode(t, ts.URL, node.token, "")
	waitConnections(t, s, node.id, 1)
	s.route(packet{Type: "data", Dest: node.id, SrcNodeID: node.id, PacketID: "old-socket-replay", srcSocketID: previousID})
	replacement.SetReadDeadline(time.Now().Add(3 * time.Second))
	if err := replacement.ReadJSON(&p); err != nil || p.Type != "data" || p.PacketID != "old-socket-replay" {
		t.Fatalf("node recreation reused a socket ID: %+v %v", p, err)
	}
}
