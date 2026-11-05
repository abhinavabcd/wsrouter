package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLocalSnapshotView(t *testing.T) {
	s := &server{id: 7, ring: []peer{
		{ID: 7, Addr: "https://one.router.test", Nodes: 1, AuthToken: "private-token"},
		{ID: 42, Addr: "https://two.router.test", Nodes: 12, AuthToken: "other-token"},
	}}
	s.nodeCount.Store(3)
	handler := s.routes()
	for _, remote := range []string{"127.0.0.1:1234", "127.0.0.2:1234", "192.0.2.1:1234", "[::1]:1234", "invalid"} {
		t.Run(remote, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/snapshot", nil)
			r.RemoteAddr = remote
			r.Header.Set("X-Forwarded-For", "127.0.0.1")
			r.Header.Set("X-Real-IP", "127.0.0.1")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if remote != "127.0.0.1:1234" {
				if w.Code != http.StatusUnauthorized || strings.Contains(w.Body.String(), "router.test") {
					t.Fatalf("non-local view: %d %s", w.Code, w.Body.String())
				}
				return
			}
			want := "addr\tnodes\tringid\nhttps://one.router.test\t3\t7\nhttps://two.router.test\t12\t42\n"
			if w.Code != http.StatusOK || w.Header().Get("Content-Type") != "text/plain; charset=utf-8" || w.Body.String() != want {
				t.Fatalf("local view: %d %s", w.Code, w.Body.String())
			}
		})
	}
}
