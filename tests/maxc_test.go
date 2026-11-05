package main

import (
	"os"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestMaxConnectionsEvictsOldest(t *testing.T) {
	for _, limit := range []int{5, 2} {
		name := "default"
		if limit == 2 {
			name = "custom"
		}
		t.Run(name, func(t *testing.T) {
			servers, urls := startCluster(t, 1)
			node := createNode(t, urls[0].URL, "limited")
			other := createNode(t, urls[0].URL, "unrelated")
			sender := connectNode(t, urls[0].URL, other.token, "")
			account := accountFor(node)
			if limit == 2 {
				account.mu.Lock()
				account.maxc = limit
				account.mu.Unlock()
			}
			var connections []*websocket.Conn
			for i := 0; i < limit+2; i++ {
				connections = append(connections, connectNode(t, urls[0].URL, node.token, ""))
				waitConnections(t, servers[0], node.id, min(i+1, limit))
			}
			for _, conn := range connections[:2] {
				conn.SetReadDeadline(time.Now().Add(time.Second))
				if _, _, err := conn.ReadMessage(); err == nil || os.IsTimeout(err) {
					t.Fatalf("oldest socket not closed: %v", err)
				}
			}
			if err := sender.WriteJSON(packet{Type: "data", Dest: node.id, Payload: "retained"}); err != nil {
				t.Fatal(err)
			}
			for _, conn := range connections[2:] {
				conn.SetReadDeadline(time.Now().Add(time.Second))
				var got packet
				if err := conn.ReadJSON(&got); err != nil || got.Payload != "retained" {
					t.Fatalf("retained socket delivery: %+v, %v", got, err)
				}
			}
			account.mu.Lock()
			account.maxc = 1
			account.mu.Unlock()
			replacement := connectNode(t, urls[0].URL, node.token, "")
			waitConnections(t, servers[0], node.id, 1)
			for _, conn := range connections[2:] {
				conn.SetReadDeadline(time.Now().Add(time.Second))
				if _, _, err := conn.ReadMessage(); err == nil || os.IsTimeout(err) {
					t.Fatalf("lowered limit kept old socket: %v", err)
				}
			}
			if err := sender.WriteJSON(packet{Type: "data", Dest: node.id, Payload: "replacement"}); err != nil {
				t.Fatal(err)
			}
			replacement.SetReadDeadline(time.Now().Add(time.Second))
			var got packet
			if err := replacement.ReadJSON(&got); err != nil || got.Payload != "replacement" {
				t.Fatalf("replacement socket delivery: %+v, %v", got, err)
			}
			waitConnections(t, servers[0], other.id, 1)
			if servers[0].nodeCount.Load() != 2 {
				t.Fatal("eviction changed the tracked node count")
			}
		})
	}
}
