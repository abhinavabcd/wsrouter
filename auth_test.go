package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestExternalAuthorizationAndExpiredRenewal(t *testing.T) {
	var approved atomic.Bool
	approved.Store(true)
	var calls atomic.Int64
	requests := make(chan string, 32)
	verifier := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodGet || r.URL.Path != "/account/verify" || r.URL.Query().Get("account") != "42" || r.Header.Get("Authorization") != "" {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		requests <- r.URL.Query().Get("action")
		if !approved.Load() {
			w.Write([]byte("denied"))
			return
		}
		if r.URL.Query().Get("action") == "renew" {
			w.Write([]byte("OK"))
		} else {
			w.Write([]byte("ok"))
		}
	}))
	t.Cleanup(verifier.Close)
	servers, urls := startCluster(t, 2)
	verifyURL := verifier.URL + "/account/verify?account=42"
	for _, nodeType := range []string{"node", "server"} {
		t.Run(nodeType, func(t *testing.T) {
			approved.Store(true)
			body, _ := json.Marshal(map[string]any{"x_src_id": "account-42", "node_type": nodeType, "auth_verify_url": verifyURL})
			req, _ := http.NewRequest(http.MethodPost, urls[0].URL+"/create", bytes.NewReader(body))
			if nodeType == "server" {
				req.Header.Set("X-Cluster-Signature", signBody(body, servers[0].serverSecret))
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			var node struct {
				NodeID string `json:"node_id"`
				Token  string `json:"token"`
			}
			err = json.NewDecoder(resp.Body).Decode(&node)
			resp.Body.Close()
			if err != nil || resp.StatusCode != http.StatusOK {
				t.Fatalf("create: %s, %v", resp.Status, err)
			}
			c, err := servers[1].verifyToken(node.Token)
			if err != nil || c.AuthVerifyURL != verifyURL {
				t.Fatalf("verifier missing from signed claims: %+v, %v", c, err)
			}
			owner, _ := servers[0].owner(node.NodeID)
			dial := func(token string, want int) {
				t.Helper()
				conn, response, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(owner.Addr, "http")+"/connect?token="+token, nil)
				if conn != nil {
					conn.Close()
				}
				if response != nil {
					response.Body.Close()
				}
				if response == nil || response.StatusCode != want || (want == http.StatusSwitchingProtocols && err != nil) {
					t.Fatalf("connect: response=%v, error=%v, want %d", response, err, want)
				}
			}
			renew := func(token string, want int) string {
				t.Helper()
				req, _ := http.NewRequest(http.MethodPost, urls[1].URL+"/renew", strings.NewReader(`{"valid_for":123}`))
				req.Header.Set("Authorization", "Bearer "+token)
				response, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				if response.StatusCode != want {
					t.Fatalf("renew: %s, want %d", response.Status, want)
				}
				if want != http.StatusOK {
					return ""
				}
				var result struct {
					Token string `json:"token"`
				}
				if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
					t.Fatal(err)
				}
				return result.Token
			}
			assertAction := func(want string) {
				t.Helper()
				select {
				case action := <-requests:
					if action != want {
						t.Fatalf("verifier action = %q, want %q", action, want)
					}
				default:
					t.Fatal("verifier was not called")
				}
			}
			dial(node.Token, http.StatusSwitchingProtocols)
			assertAction("")
			approved.Store(false)
			dial(node.Token, http.StatusUnauthorized)
			assertAction("")
			locateReq, _ := http.NewRequest(http.MethodGet, urls[0].URL+"/locate", nil)
			locateReq.Header.Set("Authorization", "Bearer "+node.Token)
			locateResp, err := http.DefaultClient.Do(locateReq)
			if err != nil {
				t.Fatal(err)
			}
			locateResp.Body.Close()
			if locateResp.StatusCode != http.StatusUnauthorized {
				t.Fatal("revoked token was accepted by locate")
			}
			assertAction("")
			renew(node.Token, http.StatusUnauthorized)
			assertAction("renew")
			approved.Store(true)
			c.Expires = time.Now().Add(-time.Hour).Unix()
			expired := servers[0].signToken(c)
			before := calls.Load()
			dial(expired, http.StatusUnauthorized)
			if calls.Load() != before {
				t.Fatal("expired token reached the connection verifier")
			}
			parts := strings.Split(expired, ".")
			c.AuthVerifyURL = verifier.URL + "/changed"
			forged, _ := json.Marshal(c)
			tampered := base64.RawURLEncoding.EncodeToString(forged) + "." + parts[1]
			renew(tampered, http.StatusUnauthorized)
			if calls.Load() != before {
				t.Fatal("tampered token reached the verifier")
			}
			renewed := renew(expired, http.StatusOK)
			assertAction("renew")
			got, err := servers[0].verifyToken(renewed)
			if err != nil || got.NodeID != node.NodeID || got.NodeType != nodeType || got.AuthVerifyURL != verifyURL || got.Issuer != owner.ID || got.Expires < time.Now().Unix()+120 {
				t.Fatalf("renewed claims changed: %+v, %v", got, err)
			}
			dial(renewed, http.StatusSwitchingProtocols)
			assertAction("")
			approved.Store(false)
			renew(expired, http.StatusUnauthorized)
			assertAction("renew")
		})
	}
	plain := createNode(t, urls[0].URL, "no-verifier")
	c, _ := servers[0].verifyToken(plain.token)
	c.Expires = time.Now().Add(-time.Hour).Unix()
	expired := servers[0].signToken(c)
	req, _ := http.NewRequest(http.MethodPost, urls[1].URL+"/renew", nil)
	req.Header.Set("Authorization", "Bearer "+expired)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatal("expired token without a verifier was renewed")
	}
}

func TestExternalAuthorizationFailsClosed(t *testing.T) {
	servers, _ := startCluster(t, 1)
	for _, tc := range []struct {
		name string
		code int
		body string
	}{
		{"denied", 200, "denied"},
		{"similar", 200, "okay"},
		{"extra-text", 200, "ok\ndenied"},
		{"oversized", 200, strings.Repeat(" ", 32) + "ok"},
		{"http-error", 500, "OK"},
		{"redirect", 302, "ok"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", "/approved")
				w.WriteHeader(tc.code)
				w.Write([]byte(tc.body))
			}))
			defer backend.Close()
			if err := servers[0].verifyAuthorization(context.Background(), backend.URL, ""); err == nil {
				t.Fatal("verifier denial was accepted")
			}
		})
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := servers[0].verifyAuthorization(ctx, backend.URL, "renew"); err == nil {
		t.Fatal("verifier timeout was accepted")
	}
	backend.Close()
	if err := servers[0].verifyAuthorization(context.Background(), backend.URL, ""); err == nil {
		t.Fatal("unavailable verifier was accepted")
	}
}

func TestInvalidAuthorizationURLs(t *testing.T) {
	_, urls := startCluster(t, 1)
	for _, address := range []string{"/relative", "file:///etc/passwd", "ftp://example.com/verify", "http://user:pass@example.com/verify", "https://example.com/verify#fragment", strings.Repeat("x", 2049)} {
		body, _ := json.Marshal(map[string]string{"x_src_id": "account", "auth_verify_url": address})
		resp, err := http.Post(urls[0].URL+"/create", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("invalid verifier URL %q returned %s", address, resp.Status)
		}
	}
}
