package main

import (
	"bytes"
	"encoding/json"
	"math"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestMalformedClientPacketsAreIsolated(t *testing.T) {
	servers, urls := startCluster(t, 1)
	s := servers[0]
	node := createNode(t, urls[0].URL, "bad-packets", 1000)
	many, _ := json.Marshal(packet{Type: "data", Dests: make([]string, 1025)})
	cases := [][]byte{
		[]byte(`null`), []byte(`[]`), []byte(`{"type":`), []byte(`{}`),
		[]byte(`{"type":"data","dest_node_id":"a"} {}`),
		[]byte(`{"type":"data","dest_node_id":"a","dest_node_ids":["b"]}`),
		[]byte(`{"type":"data","dest_node_ids":[""]}`), many,
		[]byte(`{"type":"data","dest_node_id":"a","timestamp":9223372036854775808}`),
		[]byte(`{"type":"data","dest_node_id":"a","timestamp":"now"}`),
		[]byte(`{"type":"status","dest_node_id":"a","payload":"success"}`),
		[]byte(`{"type":"reconnect","dest_node_id":"a"}`),
		[]byte(`{"type":"data","dest_node_id":"a","src_id":"forged"}`),
		[]byte(`{"type":"data","dest_node_id":"` + strings.Repeat("a", 1025) + `"}`),
		[]byte(`{"type":"data","dest_node_id":"a","_id":"` + strings.Repeat("a", 1025) + `"}`),
		[]byte("{\"type\":\"data\",\"dest_node_id\":\"a\",\"payload\":\"\xff\"}"),
		[]byte(`{"type":"data","dest_node_id":"a","extra":` + strings.Repeat("[", 10001) + `0` + strings.Repeat("]", 10001) + `}`),
		bytes.Repeat([]byte("x"), (1<<20)+1),
	}
	// A maximum-sized input can grow after canonical fields are added to a drop.
	prefix := `{"type":"data","dest_node_id":"a","timestamp":0,"extra":"`
	cases = append(cases, []byte(prefix+strings.Repeat("x", (1<<20)-len(prefix)-2)+`"}`))
	for i, data := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			conn := connectNode(t, urls[0].URL, node.token, "")
			waitConnections(t, s, node.id, 1)
			conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
			conn.WriteMessage(websocket.TextMessage, data)
			conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			if _, _, err := conn.ReadMessage(); err == nil {
				t.Fatal("invalid packet accepted")
			}
			waitConnections(t, s, node.id, 0)
		})
	}
	// Invalid input must not stop the listener or other routing workers.
	conn := connectNode(t, urls[0].URL, node.token, "")
	other := connectNode(t, urls[0].URL, node.token, "")
	waitConnections(t, s, node.id, 2)
	if err := conn.WriteJSON(packet{Type: "block", Dest: node.id, PacketID: "ordinary-client-payload"}); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	other.SetReadDeadline(time.Now().Add(3 * time.Second))
	for _, kind := range []string{"block", "status"} {
		var p packet
		if err := other.ReadJSON(&p); err != nil || p.Type != kind || p.PacketID != "ordinary-client-payload" {
			t.Fatalf("router not healthy: %+v %v", p, err)
		}
	}
}

func TestMalformedPeerPacketsAreIsolated(t *testing.T) {
	servers, urls := startCluster(t, 2)
	good := peerFrame{Packet: packet{Type: "status", SrcNodeID: servers[0].serverNodeID, Dest: "offline", Payload: "success"}}
	raw, _ := json.Marshal(good)
	cases := [][]byte{[]byte(`null`), []byte(`{"packet":null}`), bytes.Repeat(append(append([]byte(nil), raw...), '\n'), 33)}
	for _, mutate := range []func(*peerFrame){
		func(p *peerFrame) { p.Hops = 4 }, func(p *peerFrame) { p.Replays = 255 },
		func(p *peerFrame) { p.Packet.Dest = strings.Repeat("x", 1025) },
		func(p *peerFrame) { p.Packet.Dests = []string{"fanout"} },
	} {
		p := good
		mutate(&p)
		data, _ := json.Marshal(p)
		cases = append(cases, data)
	}
	for i, data := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			headers := http.Header{}
			headers.Set("X-Server-ID", strconv.FormatUint(servers[0].id, 10))
			headers.Set("X-Node-ID", servers[0].serverNodeID)
			headers.Set("Authorization", "Bearer "+servers[0].serverToken)
			headers.Set("X-Peer-Batching", "1")
			conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(urls[1].URL, "http")+"/peer", headers)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				t.Fatal(err)
			}
			conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			if _, _, err := conn.ReadMessage(); err == nil {
				t.Fatal("invalid peer frame accepted")
			} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
				t.Fatal("invalid peer frame left connection open")
			}
		})
	}
}

func TestEmptySnapshotDoesNotReplaceLiveRing(t *testing.T) {
	s := newServer(0, "https://router.test", "")
	s.nodes["live"] = &nodeState{connections: make(map[*client]struct{})}
	if s.applySnapshot(nil, s.epoch, 2) {
		t.Fatal("empty snapshot accepted")
	}
	if owner, ok := s.owner("live"); !ok || owner.Addr != s.addr {
		t.Fatal("live ring lost")
	}
}

func TestCreateRejectsTrailingJSON(t *testing.T) {
	_, urls := startCluster(t, 1)
	body := `{"auth_domain":"auth.client.test","path":"/verify","secret_string":"secret"} {}`
	resp, err := http.Post(urls[0].URL+"/create", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("trailing JSON: %d", resp.StatusCode)
	}
}

func FuzzPacketEnvelope(f *testing.F) {
	for _, raw := range []string{`null`, `{}`, `[]`, `{"type":"data","dest_node_id":"a"}`, `{"type":"data","dest_node_ids":["a","b"],"timestamp":9223372036854775807}`, `{"type":"data","dest_node_id":"a","timestamp":-9223372036854775808}`} {
		f.Add([]byte(raw))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		p, err := decodePacket(data)
		if err != nil || !p.validRoute() {
			return
		}
		cl := client{rp5: math.MaxInt64}
		cl.admit(p, 1700000010000)
		encoded, err := encodePacket(p)
		if err != nil {
			return
		}
		q, err := decodePacket(encoded)
		if err != nil || !reflect.DeepEqual(p, q) {
			t.Fatalf("round trip failed: %v", err)
		}
	})
}

func TestNormalCloseDuringFragmentDoesNotEnableReplay(t *testing.T) {
	servers, urls := startCluster(t, 1)
	node := createNode(t, urls[0].URL, "fragment")
	conn := connectNode(t, urls[0].URL, node.token, "")
	waitConnections(t, servers[0], node.id, 1)
	servers[0].mu.RLock()
	state := servers[0].nodes[node.id]
	state.mu.Lock()
	var cl *client
	for current := range state.connections {
		cl = current
	}
	state.mu.Unlock()
	servers[0].mu.RUnlock()
	writer, err := conn.NextWriter(websocket.TextMessage)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(bytes.Repeat([]byte("x"), 8192)); err != nil {
		t.Fatal(err)
	}
	// Send a normal close while the message still has an unfinished fragment.
	if err := conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	conn.ReadMessage()
	waitConnections(t, servers[0], node.id, 0)
	if !cl.normalClose.Load() {
		t.Fatal("fragmented normal close treated as unexpected disconnect")
	}
}
