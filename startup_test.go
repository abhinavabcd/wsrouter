package main

import (
	"strings"
	"testing"
)

func TestListenAddress(t *testing.T) {
	for _, tc := range []struct{ address, listen string }{
		{"http://example.com", ":80"},
		{"https://example.com", ":443"},
		{"http://localhost:8080", ":8080"},
		{"https://example.com:8443/", ":8443"},
		{"http://[::1]", ":80"},
		{"https://[::1]:9443", ":9443"},
	} {
		got, err := listenAddress(tc.address)
		if err != nil || got != tc.listen {
			t.Fatalf("listenAddress(%q) = %q, %v; want %q", tc.address, got, err, tc.listen)
		}
	}
	for _, address := range []string{"", "localhost:8080", "ftp://example.com", "https://example.com/path", "http://user:pass@example.com", "http://example.com:0", "http://example.com:65536", "http://example.com:bad", "https://example.com?query=1", "https://example.com#fragment"} {
		if _, err := listenAddress(address); err == nil {
			t.Fatalf("invalid server address accepted: %q", address)
		}
	}
}

func TestDefaultLeader(t *testing.T) {
	s := newServer(0, "http://localhost:8080", "", []byte(strings.Repeat("s", 32)))
	if !s.leader || s.clusterAddr != s.addr || len(s.ring) != 1 || s.ring[0].Addr != s.addr {
		t.Fatal("omitting clusteraddr did not initialize a leader")
	}
}
