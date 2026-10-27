package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

func TestCORSAndVerifierOrigin(t *testing.T) {
	servers, urls := startCluster(t, 2)
	origins := make(chan string, 16)
	verifier := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		origins <- origin
		json.NewEncoder(w).Encode(map[string]any{"valid": origin != "https://denied.example", "rp5": 10})
	}))
	verifier.TLS = urls[0].TLS.Clone()
	verifier.StartTLS()
	defer verifier.Close()
	for _, path := range []string{"/create", "/connect"} {
		req, _ := http.NewRequest("OPTIONS", urls[0].URL+path, nil)
		req.Header.Set("Origin", "https://any.example")
		req.Header.Set("Access-Control-Request-Method", "POST")
		req.Header.Set("Access-Control-Request-Headers", "content-type,authorization,x-node-id")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 204 || resp.Header.Get("Access-Control-Allow-Origin") != "*" {
			t.Fatalf("preflight %s: %s", path, resp.Status)
		}
	}
	body, _ := json.Marshal(map[string]string{"auth_domain": strings.TrimPrefix(verifier.URL, "https://"), "path": "/verify", "secret_string": "secret"})
	req, _ := http.NewRequest("POST", urls[0].URL+"/create", strings.NewReader(string(body)))
	req.Header.Set("Origin", "https://any.example")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var node struct {
		ID    string `json:"node_id"`
		Token string `json:"token"`
	}
	err = json.NewDecoder(resp.Body).Decode(&node)
	resp.Body.Close()
	if err != nil || resp.StatusCode != 200 || resp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("create: %s, %v", resp.Status, err)
	}
	if got := <-origins; got != "https://any.example" {
		t.Fatalf("create lost origin: %q", got)
	}
	owner, _ := servers[0].owner(node.ID)
	for _, origin := range []string{"https://any.example", "https://denied.example", "null", ""} {
		headers := http.Header{}
		if origin != "" {
			headers.Set("Origin", origin)
		}
		conn, response, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(owner.Addr, "http")+"/connect?"+nodeQuery(node.ID, node.Token), headers)
		if conn != nil {
			conn.Close()
		}
		if response == nil {
			t.Fatalf("upgrade: %v", err)
		}
		response.Body.Close()
		want := 101
		if origin == "https://denied.example" {
			want = 401
		}
		if response.StatusCode != want {
			t.Fatalf("origin %q: got %s, want %d", origin, response.Status, want)
		}
		if got := <-origins; got != origin {
			t.Fatalf("verifier origin %q, want %q", got, origin)
		}
	}
	// Renewal at a non-owner server must pass the browser's origin to the verifier.
	other := urls[0].URL
	if other == owner.Addr {
		other = urls[1].URL
	}
	for _, path := range []string{"/locate", "/renew"} {
		method := "GET"
		if path == "/renew" {
			method = "POST"
		}
		req, _ := http.NewRequest(method, other+path, nil)
		req.Header.Set("X-Node-ID", node.ID)
		req.Header.Set("Authorization", "Bearer "+node.Token)
		req.Header.Set("Origin", "https://denied.example")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 401 || resp.Header.Get("Access-Control-Allow-Origin") != "*" {
			t.Fatalf("%s denial: %s", path, resp.Status)
		}
		if got := <-origins; got != "https://denied.example" {
			t.Fatalf("%s lost origin: %q", path, got)
		}
	}
}
