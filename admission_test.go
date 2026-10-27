package main

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestAdmissionTimestampBounds(t *testing.T) {
	now := int64(1700000010000)
	for _, tc := range []struct {
		name string
		time int64
		want string
	}{
		{"current", now, ""},
		{"past", now - 1000, ""},
		{"eight-seconds", now - 8000, ""},
		{"too-old", now - 8001, "invalid_time"},
		{"future", now + 1, "invalid_time"},
		{"zero", 0, "invalid_time"},
		{"minimum", -1 << 63, "invalid_time"},
		{"maximum", 1<<63 - 1, "invalid_time"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl := &client{rp5: 10}
			if got := cl.admit(packet{Timestamp: &tc.time}, now); got != tc.want {
				t.Fatalf("admit = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestAdmissionBucketsAndServerBypass(t *testing.T) {
	cl := &client{rp5: 2}
	for _, tc := range []struct {
		now  int64
		want string
	}{
		{10000, ""}, {10001, ""}, {10002, "rate_limited"},
		{14999, "rate_limited"}, {15000, ""}, {15001, ""}, {15002, "rate_limited"},
	} {
		if got := cl.admit(packet{}, tc.now); got != tc.want {
			t.Fatalf("bucket admission at %d = %q, want %q", tc.now, got, tc.want)
		}
	}
	other := &client{rp5: 2}
	if got := other.admit(packet{}, 15002); got != "" {
		t.Fatal("one connection consumed another connection's allowance")
	}
	badTime := int64(20001)
	cl = &client{rp5: 1}
	if got := cl.admit(packet{Timestamp: &badTime}, 20000); got != "invalid_time" {
		t.Fatalf("future timestamp: %s", got)
	}
	if got := cl.admit(packet{}, 20000); got != "rate_limited" {
		t.Fatal("invalid timestamp did not consume its bucket allowance")
	}
	serverConn := &client{nodeType: "server", rp5: 1}
	for range 100 {
		if got := serverConn.admit(packet{Timestamp: &badTime}, 10000); got != "" {
			t.Fatalf("server was subject to node admission: %s", got)
		}
	}
	if serverConn.rateCount != 0 {
		t.Fatal("server traffic changed the node rate counter")
	}
}

func TestInvalidTimeDropPreservesFieldsAndSocket(t *testing.T) {
	servers, urls := startCluster(t, 1)
	node := createNode(t, urls[0].URL, "account")
	conn := connectNode(t, urls[0].URL, node.token, "")
	waitConnections(t, servers[0], node.id, 1)
	for _, suppliedID := range []bool{true, false} {
		stamp := time.Now().UnixMilli() - 60000
		input := map[string]any{
			"type": "data", "payload": "original", "timestamp": stamp,
			"dest_node_ids": []string{"one", "two"},
			"metadata":      json.RawMessage(`{"large":9007199254740993}`),
		}
		if suppliedID {
			input["_id"] = "chosen-id"
		}
		data, _ := json.Marshal(input)
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			t.Fatal(err)
		}
		conn.SetReadDeadline(time.Now().Add(time.Second))
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodePacket(raw)
		if err != nil || got.Type != "drop" || got.Payload != "invalid_time" || got.Timestamp == nil || *got.Timestamp != stamp || got.PacketID == "" || got.SrcNodeID != node.id || got.XSrc != "account" || !reflect.DeepEqual(got.Dests, []string{"one", "two"}) {
			t.Fatalf("invalid drop: %+v, %v", got, err)
		}
		if suppliedID && got.PacketID != "chosen-id" {
			t.Fatal("drop replaced the supplied packet ID")
		}
		var fields map[string]json.RawMessage
		json.Unmarshal(raw, &fields)
		if string(fields["metadata"]) != `{"large":9007199254740993}` {
			t.Fatalf("drop did not preserve extra JSON fields: %s", raw)
		}
	}
	other := connectNode(t, urls[0].URL, node.token, "")
	waitConnections(t, servers[0], node.id, 2)
	other.SetReadDeadline(time.Now().Add(time.Second))
	stamp := time.Now().UnixMilli() - 1000
	data, _ := encodePacket(packet{PacketID: "after-drop", Type: "data", Dest: node.id, Payload: "accepted", Timestamp: &stamp})
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatal(err)
	}
	for _, wantType := range []string{"data", "status"} {
		_, raw, err := other.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodePacket(raw)
		if err != nil || got.PacketID != "after-drop" || got.Type != wantType {
			t.Fatalf("rejected packet was routed or socket closed: %+v, %v", got, err)
		}
		if wantType == "data" && (got.Timestamp == nil || *got.Timestamp != stamp) {
			t.Fatal("accepted client timestamp was replaced")
		}
	}
}

func TestRateDropIsPerConnection(t *testing.T) {
	servers, urls := startCluster(t, 1)
	source := createNode(t, urls[0].URL, "source", 1)
	dest := createNode(t, urls[0].URL, "dest")
	first := connectNode(t, urls[0].URL, source.token, "")
	second := connectNode(t, urls[0].URL, source.token, "")
	dst := connectNode(t, urls[0].URL, dest.token, "")
	waitConnections(t, servers[0], source.id, 2)
	waitConnections(t, servers[0], dest.id, 1)
	for _, id := range []string{"one", "two", "three"} {
		data, _ := encodePacket(packet{PacketID: id, Type: "data", Dest: dest.id, Payload: "burst"})
		if err := first.WriteMessage(websocket.TextMessage, data); err != nil {
			t.Fatal(err)
		}
	}
	seen := make(map[string]bool)
	drops := 0
	for range 3 {
		first.SetReadDeadline(time.Now().Add(time.Second))
		_, raw, err := first.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodePacket(raw)
		if err != nil || seen[got.PacketID] || (got.PacketID != "one" && got.PacketID != "two" && got.PacketID != "three") {
			t.Fatalf("wrong burst feedback: %+v, %v", got, err)
		}
		seen[got.PacketID] = true
		if got.Type == "drop" && got.Payload == "rate_limited" && got.Dest == dest.id {
			drops++
			continue
		}
		if got.Type != "status" || got.Payload != "success" {
			t.Fatalf("wrong rate feedback: %+v", got)
		}
		dst.SetReadDeadline(time.Now().Add(time.Second))
		if _, _, err := dst.ReadMessage(); err != nil {
			t.Fatal(err)
		}
	}
	if drops == 0 {
		t.Fatal("burst was not limited")
	}
	data, _ := encodePacket(packet{PacketID: "independent", Type: "data", Dest: dest.id, Payload: "second socket"})
	if err := second.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatal(err)
	}
	first.SetReadDeadline(time.Now().Add(time.Second))
	_, raw, err := first.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodePacket(raw)
	if err != nil || got.Type != "status" || got.Payload != "success" || got.PacketID != "independent" {
		t.Fatalf("rate allowance was shared between sockets: %+v, %v", got, err)
	}
	dst.SetReadDeadline(time.Now().Add(time.Second))
	_, raw, err = dst.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	got, err = decodePacket(raw)
	if err != nil || got.PacketID != "independent" {
		t.Fatalf("a rate-dropped packet reached the destination: %+v, %v", got, err)
	}
}
