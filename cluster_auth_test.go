package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestStableSharedSecretTokens(t *testing.T) {
	secret := []byte(strings.Repeat("s", 32))
	before := newServer(7, "http://localhost:8080", "", secret)
	after := newServer(7, "http://localhost:8080", "", secret)
	other := newServer(8, "http://localhost:8081", "", secret)
	c := claims{Issuer: before.id, NodeID: strings.Repeat("a", 32), XSrcID: "account", Expires: time.Now().Unix() + 60, RP5: 10}
	token := before.signToken(c)
	if after.signToken(c) != token || before.serverToken != after.serverToken {
		t.Fatal("signing changed across a restart with the same secret")
	}
	for _, s := range []*server{after, other} {
		if _, err := s.verifyToken(token); err != nil {
			t.Fatalf("shared-secret token rejected after restart or on another server: %v", err)
		}
	}
	wrong := newServer(7, "http://localhost:8080", "", []byte(strings.Repeat("x", 32)))
	if _, err := wrong.verifyToken(token); err == nil {
		t.Fatal("wrong signing secret accepted")
	}
	if before.packetPrefix == after.packetPrefix || before.packetPrefix == other.packetPrefix {
		t.Fatal("packet ID prefixes collided across servers or restarts")
	}
	c.Expires = 0
	if _, err := before.verifyToken(before.signToken(c)); err == nil {
		t.Fatal("non-expiring regular node token accepted")
	}
	c.NodeType = "server"
	if _, err := before.verifyToken(before.signToken(c)); err != nil {
		t.Fatalf("non-expiring server token rejected: %v", err)
	}
}

func TestClusterJoinCredentialsAndServerTokens(t *testing.T) {
	servers, urls := startCluster(t, 1)
	leader := servers[0]
	leader.clusterSecret = []byte(strings.Repeat("c", 32))
	for _, tc := range []struct {
		name, signing, joining string
		allowed                bool
	}{
		{"wrong join secret", "s", "x", false},
		{"wrong signing secret", "x", "c", false},
		{"valid credentials", "s", "c", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			follower := newServer(17, "http://placeholder", urls[0].URL, []byte(strings.Repeat(tc.signing, 32)))
			follower.clusterSecret = []byte(strings.Repeat(tc.joining, 32))
			ts := httptest.NewServer(follower.routes())
			defer ts.Close()
			follower.addr = ts.URL
			err := follower.refreshOnce(context.Background())
			if (err == nil) != tc.allowed {
				t.Fatalf("join error = %v, allowed = %v", err, tc.allowed)
			}
			if !tc.allowed {
				if len(leader.ring) != 1 || follower.serverToken != "" {
					t.Fatal("rejected server received membership or a credential")
				}
				return
			}
			for _, s := range []*server{leader, follower} {
				for _, p := range s.ring {
					c, err := s.verifyToken(p.AuthToken)
					if err != nil || c.NodeType != "server" || c.Expires != 0 || c.Issuer != leader.id || c.NodeID != fmt.Sprintf("%032x", p.ID) {
						t.Fatalf("invalid distributed server credential: %+v, %v", c, err)
					}
				}
				data, _ := json.Marshal(s.ring)
				if strings.Contains(string(data), `"auth_key"`) || strings.Contains(string(data), string(leader.serverSecret)) {
					t.Fatal("snapshot contains a signing secret")
				}
			}
			// Once admitted, a registered server token authenticates later heartbeats.
			follower.clusterSecret = []byte(strings.Repeat("x", 32))
			if err := follower.refreshOnce(context.Background()); err != nil {
				t.Fatalf("registered server could not refresh using its token: %v", err)
			}
		})
	}
}

func TestPeerRequiresRegisteredServerToken(t *testing.T) {
	servers, _ := startCluster(t, 2)
	leader, follower := servers[0], servers[1]
	clientClaims := claims{Issuer: leader.id, NodeID: fmt.Sprintf("%032x", follower.id), XSrcID: "client", Expires: time.Now().Unix() + 60, RP5: 10}
	for _, tc := range []struct {
		name, token string
		id          uint64
		allowed     bool
	}{
		{"registered", follower.serverToken, follower.id, true},
		{"missing", "", follower.id, false},
		{"tampered", follower.serverToken + "x", follower.id, false},
		{"wrong identity", follower.serverToken, leader.id, false},
		{"regular node", leader.signToken(clientClaims), follower.id, false},
		{"unregistered", leader.serverNodeToken(99), 99, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/peer", nil)
			r.Header.Set("Authorization", "Bearer "+tc.token)
			if err := leader.verifyServerNode(r, tc.id); (err == nil) != tc.allowed {
				t.Fatalf("authentication error = %v, allowed = %v", err, tc.allowed)
			}
		})
	}
}
