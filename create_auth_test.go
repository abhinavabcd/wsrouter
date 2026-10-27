package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCreateRequiresVerifierApproval(t *testing.T) {
	s, router := startTestRouter(t, 0, "")
	for _, tc := range []struct {
		name, body string
		status     int
		allowed    bool
	}{
		{"boolean", `{"valid":true,"rp5":10}`, 200, true},
		{"string", `{"valid":"true","rp5":10}`, 200, true},
		{"denied", `{"valid":false,"rp5":10}`, 200, false},
		{"missing-rate", `{"valid":true}`, 200, false},
		{"bad-rate", `{"valid":true,"rp5":0}`, 200, false},
		{"bad-maxc", `{"valid":true,"rp5":10,"maxc":0}`, 200, false},
		{"expired", `{"valid":true,"rp5":10,"expires":1}`, 200, false},
		{"plain-ok", `ok`, 200, false},
		{"malformed", `{`, 200, false},
		{"unavailable", `{"valid":true,"rp5":10}`, 503, false},
		{"redirect", `{"valid":true,"rp5":10}`, 302, false},
		{"oversized", `{"valid":true,"rp5":10}` + strings.Repeat(" ", 8192), 200, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seen := make(chan string, 1)
			verifier := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var credentials struct {
					NodeID string `json:"node_id"`
					Secret string `json:"secret"`
				}
				if err := json.NewDecoder(r.Body).Decode(&credentials); err != nil {
					t.Error(err)
				}
				if r.Method != "POST" || r.URL.Path != "/verify" || r.Header.Get("Content-Type") != "application/json" || r.Header.Get("Origin") != "https://client.example" || credentials.Secret != "shared|secret" || credentials.NodeID == "" || r.URL.Query().Get("node_id") != credentials.NodeID {
					t.Errorf("incorrect verifier request: %s %s %+v", r.Method, r.URL, credentials)
				}
				seen <- credentials.NodeID
				if tc.status == 302 {
					w.Header().Set("Location", "/must-not-follow")
				}
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			}))
			verifier.TLS = router.TLS.Clone()
			verifier.StartTLS()
			defer verifier.Close()
			domain := strings.TrimPrefix(verifier.URL, "https://")
			body, _ := json.Marshal(map[string]string{"auth_domain": domain, "path": "/verify", "secret_string": "shared|secret"})
			req, _ := http.NewRequest("POST", router.URL+"/create", strings.NewReader(string(body)))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Origin", "https://client.example")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			data, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			candidate := <-seen
			if !strings.HasPrefix(candidate, domain+"|") || len(strings.TrimPrefix(candidate, domain+"|")) != 32 {
				t.Fatalf("bad candidate: %s", candidate)
			}
			if tc.allowed {
				var result map[string]string
				if err := json.Unmarshal(data, &result); err != nil || resp.StatusCode != 200 || result["node_id"] != candidate || result["token"] != "/verify|shared|secret" || result["address"] != s.addr {
					t.Fatalf("approval result: %d %s %v", resp.StatusCode, data, err)
				}
			} else if resp.StatusCode != 401 || strings.Contains(string(data), candidate) || strings.Contains(string(data), "shared|secret") {
				t.Fatalf("denial leaked credentials or succeeded: %d %s", resp.StatusCode, data)
			}
			if s.nodeCount.Load() != 0 {
				t.Fatal("create tracked a socketless node")
			}
		})
	}
}
