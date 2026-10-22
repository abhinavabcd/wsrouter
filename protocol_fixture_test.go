package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

var fixtureCertificate tls.Certificate
var fixtureTransport *http.Transport
var fixtureAuthURL string
var fixtureTokens sync.Map   // token -> node ID, used by existing routing helpers
var fixtureAccounts sync.Map // secret -> *fixtureAccount

type fixtureAccount struct {
	maxc                    int
	mu                      sync.Mutex
	nodeID, source, rotated string
	rate                    int64
	denied, denyRenew       bool
	renewCalls              int
	expires                 int64
}

func TestMain(m *testing.M) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(100), Subject: pkix.Name{CommonName: "wsrouter tests"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true, IsCA: true, DNSNames: []string{"*.router.test", "auth.client.test", "localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		panic(err)
	}
	fixtureCertificate, err = tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	if err != nil {
		panic(err)
	}
	parsed, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(parsed)
	fixtureTransport = http.DefaultTransport.(*http.Transport).Clone()
	fixtureTransport.Proxy = nil
	fixtureTransport.TLSClientConfig = &tls.Config{RootCAs: pool}
	fixtureTransport.MaxIdleConns = 1024
	fixtureTransport.MaxIdleConnsPerHost = 64
	fixtureTransport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		if strings.HasSuffix(host, ".test") {
			address = net.JoinHostPort("127.0.0.1", port)
		}
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
	http.DefaultTransport = fixtureTransport
	websocket.DefaultDialer = &websocket.Dialer{TLSClientConfig: fixtureTransport.TLSClientConfig, NetDialContext: fixtureTransport.DialContext, HandshakeTimeout: 5 * time.Second}
	auth := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var credentials struct {
			NodeID string `json:"node_id"`
			Secret string `json:"secret"`
		}
		err := json.NewDecoder(io.LimitReader(r.Body, 65536)).Decode(&credentials)
		raw, ok := fixtureAccounts.Load(credentials.Secret)
		if err != nil || !ok || r.Method != "POST" || r.Header.Get("Content-Type") != "application/json" {
			http.Error(w, "denied", 401)
			return
		}
		account := raw.(*fixtureAccount)
		account.mu.Lock()
		defer account.mu.Unlock()
		if credentials.NodeID != account.nodeID || r.URL.Query().Get("node_id") != account.nodeID || account.denied {
			http.Error(w, "denied", 401)
			return
		}
		approval := map[string]any{"valid": "true", "rp5": account.rate, "x_src_id": account.source, "expires": account.expires}
		if account.maxc != 0 {
			approval["maxc"] = account.maxc
		}
		if r.URL.Query().Get("action") == "renew" {
			account.renewCalls++
			if account.denyRenew {
				http.Error(w, "denied", 401)
				return
			}
			if account.rotated != "" {
				approval["secret_string"] = account.rotated
				fixtureAccounts.Store(account.rotated, account)
			}
		}
		json.NewEncoder(w).Encode(approval)
	}))
	auth.TLS = &tls.Config{Certificates: []tls.Certificate{fixtureCertificate}}
	auth.StartTLS()
	_, port, _ := net.SplitHostPort(auth.Listener.Addr().String())
	fixtureAuthURL = "https://auth.client.test:" + port
	code := m.Run()
	auth.Close()
	fixtureTransport.CloseIdleConnections()
	os.Exit(code)
}

func startTestRouter(t *testing.T, id uint64, cluster string) (*server, *httptest.Server) {
	t.Helper()
	ts := httptest.NewUnstartedServer(nil)
	_, port, _ := net.SplitHostPort(ts.Listener.Addr().String())
	address := fmt.Sprintf("https://s%d.router.test:%s", id, port)
	s := newServer(id, address, cluster)
	s.httpClient.Transport = fixtureTransport.Clone()
	ts.Config.Handler = s.routes()
	ts.TLS = &tls.Config{Certificates: []tls.Certificate{fixtureCertificate}}
	ts.StartTLS()
	ts.URL = address
	t.Cleanup(func() {
		s.peerMu.Lock()
		links := make([]*peerLink, 0, len(s.allPeerLinks))
		for link := range s.allPeerLinks {
			links = append(links, link)
		}
		s.peerMu.Unlock()
		for _, link := range links {
			link.close()
		}
		s.httpClient.CloseIdleConnections()
		ts.Close()
	})
	return s, ts
}

func nodeQuery(nodeID, token string) string {
	return url.Values{"node_id": {nodeID}, "token": {token}}.Encode()
}
func tokenQuery(token string) string {
	id, _ := fixtureTokens.Load(token)
	if id == nil {
		panic("test token has no node ID")
	}
	return nodeQuery(id.(string), token)
}
func accountFor(node testNode) *fixtureAccount {
	_, secret, _ := strings.Cut(node.token, "|")
	raw, _ := fixtureAccounts.Load(secret)
	return raw.(*fixtureAccount)
}
func serverTestNode(s *server) testNode {
	fixtureTokens.Store(s.serverToken, s.serverNodeID)
	return testNode{id: s.serverNodeID, token: s.serverToken}
}
func randomTestSecret() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
