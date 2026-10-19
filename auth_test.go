package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestDomainAuthenticationLifecycle(t *testing.T) {
	servers, urls := startCluster(t, 2)
	node := createNode(t, urls[0].URL, "account", 37)
	if !strings.HasPrefix(node.id, strings.TrimPrefix(fixtureAuthURL, "https://")+"|") || !strings.HasPrefix(node.token, "/verify|") {
		t.Fatal("wrong credential format")
	}
	owner, _ := servers[0].owner(node.id)
	conn := connectNode(t, owner.Addr, node.token, "")
	for _, s := range servers {
		if s.addr == owner.Addr {
			waitConnections(t, s, node.id, 1)
			s.mu.RLock()
			state := s.nodes[node.id]
			state.mu.Lock()
			for cl := range state.connections {
				if cl.rp5 != 37 || cl.xSrcID != "account" || cl.nodeType != "node" {
					t.Error("verifier metadata not applied")
				}
			}
			state.mu.Unlock()
			s.mu.RUnlock()
		}
	}
	account := accountFor(node)
	account.mu.Lock()
	account.denied = true
	account.mu.Unlock()
	for _, path := range []string{"/locate", "/renew"} {
		method := http.MethodGet
		if path == "/renew" {
			method = http.MethodPost
		}
		req, _ := http.NewRequest(method, urls[0].URL+path, nil)
		req.Header.Set("X-Node-ID", node.id)
		req.Header.Set("Authorization", "Bearer "+node.token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 401 {
			t.Fatalf("revoked %s: %s", path, resp.Status)
		}
	}
	_, response, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(owner.Addr, "http")+"/connect?"+nodeQuery(node.id, node.token), nil)
	if err == nil || response == nil || response.StatusCode != 401 {
		t.Fatal("revoked connection accepted")
	}
	response.Body.Close()
	// Revocation denies new authentication; an existing socket stays until renewal.
	if err := conn.WriteJSON(packet{Type: "data", Dest: node.id, Payload: "still-connected"}); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatal(err)
	}
	account.mu.Lock()
	account.denied = false
	account.rotated = randomTestSecret()
	account.rate = 23
	account.mu.Unlock()
	req, _ := http.NewRequest(http.MethodPost, urls[1].URL+"/renew", strings.NewReader(`{"rp5":1000000}`))
	req.Header.Set("X-Node-ID", node.id)
	req.Header.Set("Authorization", "Bearer "+node.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var result domainIdentity
	err = json.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()
	if err != nil || resp.StatusCode != 200 || result.NodeID != node.id || result.Token == node.token || result.RP5 != 23 {
		t.Fatalf("renewal: %+v, %v", result, err)
	}
	fixtureTokens.Store(result.Token, node.id)
	connectNode(t, owner.Addr, result.Token, "")
	c, err := servers[0].authenticate(context.Background(), node.id, result.Token, "", "")
	if err != nil || c.RP5 != 23 {
		t.Fatalf("rotated token: %+v %v", c, err)
	}
}

func TestCreateRequiresDomainCredentials(t *testing.T) {
	_, urls := startCluster(t, 1)
	for _, body := range []string{
		`{}`, `{"x_src_id":"old"}`, `{"auth_domain":"https://example.com","path":"/verify","secret_string":"secret"}`,
		`{"auth_domain":"example.com","path":"//evil.com/verify","secret_string":"secret"}`,
		`{"auth_domain":"example.com","path":"/verify","secret_string":""}`,
	} {
		resp, err := http.Post(urls[0].URL+"/create", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Fatalf("invalid create returned %s", resp.Status)
		}
	}
	resp, err := http.Post(urls[0].URL+"/generate", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatal("generate was restored")
	}
}
