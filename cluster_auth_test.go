package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

func TestServerVerifierJSONCredentials(t *testing.T) {
	servers, urls := startCluster(t, 1)
	s := servers[0]
	_, secret, _ := strings.Cut(s.serverToken, "|")
	valid, _ := json.Marshal(map[string]string{"node_id": s.serverNodeID, "secret": secret})
	wrong, _ := json.Marshal(map[string]string{"node_id": s.serverNodeID, "secret": "wrong"})
	missing, _ := json.Marshal(map[string]string{"secret": secret})
	query := "?node_id=" + url.QueryEscape(s.serverNodeID)
	for _, tc := range []struct {
		query, body string
		want        int
	}{
		{"", string(valid), 200},
		{query, string(valid), 200},
		{"?node_id=another-node", string(valid), 401},
		{query, string(wrong), 401},
		{query, string(missing), 401},
		{query, secret, 400},
	} {
		resp, err := http.Post(urls[0].URL+"/server"+tc.query, "application/json", strings.NewReader(tc.body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Fatalf("server verifier returned %d, want %d", resp.StatusCode, tc.want)
		}
	}
}

func TestClusterRegistrationHTTPSProof(t *testing.T) {
	servers, urls := startCluster(t, 1)
	leader := servers[0]
	follower, ts := startTestRouter(t, 17, urls[0].URL)
	domain := strings.TrimPrefix(ts.URL, "https://")
	_, secret, _ := strings.Cut(follower.serverToken, "|")
	for _, tc := range []struct {
		domain, secret string
		id             uint64
		want           int
	}{
		{domain, "wrong", 17, 401}, {domain, secret, 18, 401}, {"other.example.com", secret, 17, 403}, {domain, secret, 17, 200},
	} {
		data, _ := json.Marshal(map[string]any{"server_domain": tc.domain, "id": tc.id, "secret": tc.secret})
		resp, err := http.Post(urls[0].URL+"/server", "application/json", bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Fatalf("registration got %d, want %d", resp.StatusCode, tc.want)
		}
	}
	if err := follower.refreshOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, s := range []*server{leader, follower} {
		for _, p := range s.ring {
			nodeID := strings.TrimPrefix(p.Addr, "https://") + "|" + strconv.FormatUint(p.ID, 10)
			c, err := s.authenticate(context.Background(), nodeID, p.AuthToken, "")
			if err != nil || c.NodeType != "server" || c.Expires != 0 {
				t.Fatalf("server verification: %+v, %v", c, err)
			}
		}
	}
	// A restarted process gets a fresh credential; the endpoint proof authorizes replacement.
	restart := newServer(follower.id, follower.addr, leader.addr)
	if restart.serverToken == follower.serverToken || restart.serverNodeID != follower.serverNodeID {
		t.Fatal("restart identity/credential generation")
	}
}

func TestPeerRequiresRegisteredDomainIdentity(t *testing.T) {
	servers, urls := startCluster(t, 2)
	leader, follower := servers[0], servers[1]
	node := createNode(t, urls[0].URL, "client")
	for _, tc := range []struct {
		nodeID, token string
		id            uint64
		allowed       bool
	}{
		{follower.serverNodeID, follower.serverToken, follower.id, true},
		{follower.serverNodeID, follower.serverToken + "x", follower.id, false},
		{leader.serverNodeID, follower.serverToken, follower.id, false},
		{node.id, node.token, follower.id, false},
		{follower.serverNodeID, follower.serverToken, 99, false},
	} {
		r := httptest.NewRequest("GET", "/peer", nil)
		r.Header.Set("X-Node-ID", tc.nodeID)
		r.Header.Set("Authorization", "Bearer "+tc.token)
		if err := leader.verifyServerNode(r, tc.id); (err == nil) != tc.allowed {
			t.Fatalf("peer authentication allowed=%v error=%v", tc.allowed, err)
		}
	}
	// A sibling server cannot publish a snapshot as the configured primary.
	data, _ := json.Marshal(ringSnapshot{Epoch: leader.epoch, Version: 999, Peers: leader.ring})
	req, _ := http.NewRequest("POST", urls[1].URL+"/snapshot", bytes.NewReader(data))
	req.Header.Set("X-Node-ID", follower.serverNodeID)
	req.Header.Set("Authorization", "Bearer "+follower.serverToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatal("non-primary snapshot accepted")
	}
}
