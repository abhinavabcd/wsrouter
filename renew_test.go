package main

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestRenewAtOwnerClosesOnlyRenewedNode(t *testing.T) {
	servers, urls := startCluster(t, 1)
	node := createNode(t, urls[0].URL, "renewed")
	other := createNode(t, urls[0].URL, "unrelated")
	first := connectNode(t, urls[0].URL, node.token, "")
	second := connectNode(t, urls[0].URL, node.token, "")
	otherConn := connectNode(t, urls[0].URL, other.token, "")
	waitConnections(t, servers[0], node.id, 2)
	waitConnections(t, servers[0], other.id, 1)
	req, _ := http.NewRequest(http.MethodPost, urls[0].URL+"/renew", nil)
	req.Header.Set("Authorization", "Bearer "+node.token)
	req.Header.Set("X-Node-ID", node.id)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("renewal returned %s", resp.Status)
	}
	for _, conn := range []*websocket.Conn{first, second} {
		conn.SetReadDeadline(time.Now().Add(time.Second))
		if _, _, err := conn.ReadMessage(); err == nil || os.IsTimeout(err) {
			t.Fatalf("renewal did not close socket: %v", err)
		}
	}
	if servers[0].nodeCount.Load() != 1 {
		t.Fatal("renewal changed the unrelated node count")
	}
	data, _ := encodePacket(packet{Type: "data", Dest: other.id, Payload: "still-connected"})
	if err := otherConn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatal(err)
	}
	otherConn.SetReadDeadline(time.Now().Add(time.Second))
	_, raw, err := otherConn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodePacket(raw)
	if err != nil || got.Payload != "still-connected" {
		t.Fatalf("unrelated socket changed: %+v, %v", got, err)
	}
}

func TestRenewAtNonOwnerReturnsVerifierDenialWithoutDisconnect(t *testing.T) {
	servers, urls := startCluster(t, 2)
	node := createNode(t, urls[0].URL, "account")
	owner, _ := servers[0].owner(node.id)
	ownerIndex := 0
	if owner.ID == servers[1].id {
		ownerIndex = 1
	}
	conn := connectNode(t, owner.Addr, node.token, "")
	waitConnections(t, servers[ownerIndex], node.id, 1)
	account := accountFor(node)
	account.mu.Lock()
	account.denyRenew = true
	account.mu.Unlock()
	req, _ := http.NewRequest(http.MethodPost, urls[1-ownerIndex].URL+"/renew", strings.NewReader(`{"valid_for":60}`))
	req.Header.Set("Authorization", "Bearer "+node.token)
	req.Header.Set("X-Node-ID", node.id)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized || string(body) != "Unauthorized\n" || resp.Header.Get("Content-Type") != "text/plain; charset=utf-8" || account.renewCalls != 1 {
		t.Fatalf("unexpected verifier denial: %s %s, calls=%d", resp.Status, body, account.renewCalls)
	}
	waitConnections(t, servers[ownerIndex], node.id, 1)
	data, _ := encodePacket(packet{Type: "data", Dest: node.id, Payload: "denial-kept-socket"})
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("denied renewal closed the socket: %v", err)
	}
}

func TestRenewWithoutRing(t *testing.T) {
	servers, urls := startCluster(t, 1)
	node := createNode(t, urls[0].URL, "account")
	servers[0].mu.Lock()
	servers[0].ring = nil
	servers[0].routingRing.Store([]peer(nil))
	servers[0].mu.Unlock()
	req, _ := http.NewRequest(http.MethodPost, urls[0].URL+"/renew", nil)
	req.Header.Set("Authorization", "Bearer "+node.token)
	req.Header.Set("X-Node-ID", node.id)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var renewed domainIdentity
	if err := json.NewDecoder(resp.Body).Decode(&renewed); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || renewed.NodeID != node.id || renewed.Token != node.token {
		t.Fatalf("renewal without ring: %s, %+v", resp.Status, renewed)
	}
}

func TestOldSocketCleanupPreservesReplacementNode(t *testing.T) {
	servers, urls := startCluster(t, 1)
	s := servers[0]
	node := createNode(t, urls[0].URL, "account")
	connectNode(t, urls[0].URL, node.token, "")
	waitConnections(t, s, node.id, 1)
	s.mu.Lock()
	state := s.nodes[node.id]
	state.mu.Lock()
	var old *client
	for cl := range state.connections {
		old = cl
		old.normalClose.Store(true)
	}
	// Model a replacement arriving between renewal's detach and cleanup.
	delete(s.nodes, node.id)
	s.nodeCount.Add(-1)
	state.mu.Unlock()
	s.mu.Unlock()
	newConn := connectNode(t, urls[0].URL, node.token, "")
	waitConnections(t, s, node.id, 1)
	old.close()
	if s.nodeCount.Load() != 1 {
		t.Fatal("old socket cleanup removed the replacement node")
	}
	data, _ := json.Marshal(packet{Type: "data", Dest: node.id, Payload: "replacement"})
	if err := newConn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatal(err)
	}
	newConn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := newConn.ReadMessage(); err != nil {
		t.Fatalf("replacement was removed by old socket cleanup: %v", err)
	}
}
