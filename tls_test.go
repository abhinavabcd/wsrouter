package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func testCertificate(t *testing.T, serial int64) (*x509.Certificate, []byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return parsed, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

func TestHTTPSWSSAndCertificateReload(t *testing.T) {
	certDir := t.TempDir()
	first, firstPEM, firstKey := testCertificate(t, 1)
	second, secondPEM, secondKey := testCertificate(t, 2)
	certPath, keyPath := filepath.Join(certDir, "server.pem"), filepath.Join(certDir, "server.key")
	if err := os.WriteFile(certPath, firstPEM, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, firstKey, 0600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	baseURL := "https://" + listener.Addr().String()
	s := newServer(0, baseURL, baseURL)
	s.httpClient.Transport = fixtureTransport.Clone()
	s.sslCerts = certDir
	if err := s.reloadCertificate(); err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: s.routes(), TLSConfig: s.tlsConfig(), ReadHeaderTimeout: 5 * time.Second}
	serveDone := make(chan error, 1)
	go func() { serveDone <- httpServer.ServeTLS(listener, "", "") }()
	t.Cleanup(func() {
		httpServer.Shutdown(context.Background())
		<-serveDone
	})
	pool := x509.NewCertPool()
	pool.AddCert(first)
	pool.AddCert(second)
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}, DisableKeepAlives: true}}
	getSerial := func() int64 {
		t.Helper()
		conn, err := tls.Dial("tcp", listener.Addr().String(), &tls.Config{RootCAs: pool})
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		return conn.ConnectionState().PeerCertificates[0].SerialNumber.Int64()
	}
	if got := getSerial(); got != 1 {
		t.Fatalf("initial certificate serial = %d", got)
	}
	response, err := client.Get(baseURL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("HTTPS health returned %s", response.Status)
	}
	create := func(xSrc string) testNode {
		t.Helper()
		secret := randomTestSecret()
		body, _ := json.Marshal(map[string]string{"auth_domain": strings.TrimPrefix(fixtureAuthURL, "https://"), "path": "/verify", "secret_string": secret})
		response, err := client.Post(baseURL+"/create", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("HTTPS create returned %s", response.Status)
		}
		var node struct {
			NodeID string `json:"node_id"`
			Token  string `json:"token"`
		}
		if err := json.NewDecoder(response.Body).Decode(&node); err != nil {
			t.Fatal(err)
		}
		fixtureAccounts.Store(secret, &fixtureAccount{nodeID: node.NodeID, source: xSrc, rate: 10})
		return testNode{id: node.NodeID, token: node.Token}
	}
	source, dest := create("source"), create("dest")
	dialer := websocket.Dialer{TLSClientConfig: &tls.Config{RootCAs: pool}}
	connect := func(node testNode) *websocket.Conn {
		t.Helper()
		conn, _, err := dialer.Dial("wss://"+listener.Addr().String()+"/connect?"+nodeQuery(node.id, node.token), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	srcConn, dstConn := connect(source), connect(dest)
	waitConnections(t, s, source.id, 1)
	waitConnections(t, s, dest.id, 1)
	sendAndReceive := func() {
		t.Helper()
		data, _ := encodePacket(packet{Type: "data", Dest: dest.id, Payload: "hello"})
		if err := srcConn.WriteMessage(websocket.TextMessage, data); err != nil {
			t.Fatal(err)
		}
		dstConn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, deliveredData, err := dstConn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		delivered, err := decodePacket(deliveredData)
		if err != nil || delivered.Src != source.id || delivered.Payload != "hello" {
			t.Fatalf("WSS delivery: %+v, %v", delivered, err)
		}
		srcConn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, statusData, err := srcConn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		status, err := decodePacket(statusData)
		if err != nil || status.Type != "status" || status.Payload != "success" || status.PacketID != delivered.PacketID {
			t.Fatalf("WSS status: %+v, %v", status, err)
		}
	}
	sendAndReceive()
	reload := func(signature string) int {
		t.Helper()
		request, _ := http.NewRequest(http.MethodPost, baseURL+"/reload", nil)
		request.Header.Set("Authorization", "Bearer "+signature)
		request.Header.Set("X-Node-ID", s.serverNodeID)
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		return response.StatusCode
	}
	if got := reload("invalid"); got != http.StatusUnauthorized {
		t.Fatalf("unauthorized reload returned %d", got)
	}
	signature := s.serverToken
	if err := os.WriteFile(certPath, []byte("invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	if got := reload(signature); got != http.StatusBadGateway {
		t.Fatalf("invalid certificate reload returned %d", got)
	}
	if got := getSerial(); got != 1 {
		t.Fatalf("invalid reload replaced certificate with serial %d", got)
	}
	if err := os.WriteFile(certPath, secondPEM, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, secondKey, 0600); err != nil {
		t.Fatal(err)
	}
	if got := reload(signature); got != http.StatusNoContent {
		t.Fatalf("valid certificate reload returned %d", got)
	}
	if got := getSerial(); got != 2 {
		t.Fatalf("reload left certificate serial %d", got)
	}
	sendAndReceive()
}

func TestCommonPEMCertificateNames(t *testing.T) {
	_, certPEM, keyPEM := testCertificate(t, 1)
	for _, files := range [][2]string{{"fullchain.pem", "privkey.pem"}, {"public.pem", "private.pem"}} {
		t.Run(files[0], func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, files[0]), certPEM, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, files[1]), keyPEM, 0600); err != nil {
				t.Fatal(err)
			}
			s := &server{sslCerts: dir}
			if err := s.reloadCertificate(); err != nil {
				t.Fatal(err)
			}
			if s.certificate.Load() == nil {
				t.Fatal("no TLS certificate loaded")
			}
		})
	}
}

func TestCertificateWatcherFollowsAtomicPublication(t *testing.T) {
	dir := t.TempDir()
	for i, name := range []string{"first", "second"} {
		_, cert, key := testCertificate(t, int64(i+1))
		version := filepath.Join(dir, name)
		if err := os.Mkdir(version, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(version, "server.pem"), cert, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(version, "server.key"), key, 0600); err != nil {
			t.Fatal(err)
		}
	}
	current := filepath.Join(dir, "current")
	if err := os.Symlink("first", current); err != nil {
		t.Fatal(err)
	}
	s := &server{sslCerts: current}
	if err := s.reloadCertificate(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.watchCertificate(ctx, 10*time.Millisecond) }()
	defer func() { cancel(); <-done }()
	if err := os.Symlink("second", filepath.Join(dir, "next")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, "next"), current); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		cert, err := x509.ParseCertificate(s.certificate.Load().Certificate[0])
		if err != nil {
			t.Fatal(err)
		}
		if cert.SerialNumber.Int64() == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("watcher did not load newly published certificate")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
