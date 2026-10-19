package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDomainCredentialHTTPSVerification(t *testing.T) {
	var nodeID string
	secret := "raw|secret & \"quoted\"\nbody"
	backend := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		err := json.NewDecoder(r.Body).Decode(&body)
		if err != nil || len(body) != 2 || body["node_id"] != nodeID || body["secret"] != secret || r.Header.Get("Content-Type") != "application/json" || r.Method != "POST" || r.URL.Path != "/verify" || r.URL.Query().Get("node_id") != nodeID || len(r.URL.Query()["node_id"]) != 1 || r.URL.Query().Get("account") != "42" || r.Header.Get("Authorization") != "" {
			http.Error(w, "bad request", 400)
			return
		}
		if r.URL.Query().Get("action") == "renew" {
			io.WriteString(w, `{"valid":true,"x_src_id":"account-42","rp5":25,"secret_string":"rotated"}`)
		} else {
			io.WriteString(w, `{"valid":"true","rp5":10}`)
		}
	}))
	defer backend.Close()
	nodeID = strings.TrimPrefix(backend.URL, "https://") + "|random"
	token := "/verify?account=42&node_id=wrong&node_id=duplicate|" + secret
	s := &server{addr: "https://node.router.example", httpClient: backend.Client()}
	c, err := s.authenticate(context.Background(), nodeID, token, "", "")
	if err != nil || c.NodeID != nodeID || c.Token != token || c.XSrcID != nodeID || c.RP5 != 10 || c.NodeType != "node" {
		t.Fatalf("authentication: %+v, %v", c, err)
	}
	c, err = s.authenticate(context.Background(), nodeID, token, "renew", "")
	if err != nil || c.XSrcID != "account-42" || c.RP5 != 25 || !strings.HasSuffix(c.Token, "|rotated") {
		t.Fatalf("renewal: %+v, %v", c, err)
	}
	s.httpClient = &http.Client{}
	if _, err = s.authenticate(context.Background(), nodeID, token, "", ""); err == nil {
		t.Fatal("untrusted certificate accepted")
	}
}

func TestDomainCredentialDenials(t *testing.T) {
	for _, body := range []string{"ok", "denied", "okay", `{"valid":false,"rp5":10}`, `{"valid":"false","rp5":10}`, `{"valid":"true","rp5":0}`, `{"valid":true}`, `{"valid":"true","rp5":10,"expires":1}`, strings.Repeat("x", 8193)} {
		backend := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, body) }))
		s := &server{addr: "https://router.example", httpClient: backend.Client()}
		_, err := s.authenticate(context.Background(), strings.TrimPrefix(backend.URL, "https://")+"|id", "/verify|secret", "", "")
		backend.Close()
		if err == nil {
			t.Fatal("denial accepted")
		}
	}
	backend := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/approved", 302) }))
	defer backend.Close()
	s := &server{addr: "https://router.example", httpClient: backend.Client()}
	if _, err := s.authenticate(context.Background(), strings.TrimPrefix(backend.URL, "https://")+"|id", "/verify|secret", "", ""); err == nil {
		t.Fatal("redirect accepted")
	}
}

func TestServerDomainBoundaries(t *testing.T) {
	for _, tc := range []struct {
		auth, current string
		want          bool
	}{
		{"sub2.domain.com", "sub1.domain.com", true},
		{"domain.com", "sub1.domain.com", true},
		{"sub2.x.y.com", "sub1.x.y.com", true},
		{"x.y.com", "sub1.x.y.com", true},
		{"other.y.com", "sub1.x.y.com", false},
		{"evil.com", "domain.com", false},
		{"domain.com.evil.com", "sub1.domain.com", false},
		{"other.co.uk", "domain.co.uk", false},
		{"other.github.io", "account.github.io", false},
		{"localhost:8444", "localhost:8443", true},
		{"127.0.0.2", "127.0.0.1", false},
	} {
		if got := sameServerDomain(tc.auth, tc.current); got != tc.want {
			t.Errorf("%s vs %s: %v", tc.auth, tc.current, got)
		}
	}
}

func TestDomainCredentialParsing(t *testing.T) {
	for _, tc := range [][2]string{
		{"example.com|id", "//evil.com/path|secret"},
		{"http://example.com|id", "/verify|secret"},
		{"user@example.com|id", "/verify|secret"},
		{"example.com|id", "https://evil.com/verify|secret"},
		{"example.com|id", "/verify#fragment|secret"},
		{"example.com|id", "/verify|"},
		{"example.com|", "/verify|secret"},
		{"example.com:0|id", "/verify|secret"},
		{"example.com|id|extra", "/verify|secret"},
	} {
		if _, _, err := authEndpoint(tc[0], tc[1]); err == nil {
			t.Errorf("invalid credentials accepted: %v", tc)
		}
	}
}
