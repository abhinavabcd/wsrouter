package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type peer struct {
	ID      uint64 `json:"id"`
	Addr    string `json:"addr"`
	Nodes   int64  `json:"nodes"`
	AuthKey []byte `json:"auth_key"`
}

type ringSnapshot struct {
	Epoch   int64  `json:"epoch"`
	Version uint64 `json:"version"`
	Peers   []peer `json:"peers"`
}

type member struct {
	peer
	Seen time.Time
}

type claims struct {
	Issuer        uint64 `json:"issuer"`
	NodeID        string `json:"node_id"`
	XSrcID        string `json:"x_src_id"`
	NodeType      string `json:"node_type,omitempty"`
	AuthVerifyURL string `json:"auth_verify_url,omitempty"`
	Nonce         string `json:"nonce,omitempty"`
	Expires       int64  `json:"expires"`
}

type packet struct {
	PacketID  string   `json:"_id,omitempty"`
	Timestamp *int64   `json:"timestamp,omitempty"`
	Type      string   `json:"type"`
	Dest      string   `json:"dest_node_id,omitempty"`
	Dests     []string `json:"dest_node_ids,omitempty"`
	Src       string   `json:"src_id,omitempty"`
	SrcNodeID string   `json:"src_node_id"`
	XSrc      string   `json:"x_src_id,omitempty"`
	Payload   string   `json:"payload"`
	hops      uint8
	replays   uint8
}
type replayRecord struct {
	packet packet
	sentAt time.Time
}

func (p packet) sourceNodeID() string {
	if p.SrcNodeID != "" {
		return p.SrcNodeID
	}
	return p.Src
}

type receipt struct {
	sourceID string
	packetID string
	sentAt   time.Time
}

type receiptQueue struct {
	entries []receipt
	head    int
}

type nodeState struct {
	mu          sync.Mutex
	connections map[*client]struct{}
	recent      receiptQueue
}

type outbound struct {
	data       []byte
	closeAfter bool
}

type routedPacket struct {
	packet packet
	bytes  int
}

type client struct {
	conn         *websocket.Conn
	nodeID       string
	xSrcID       string
	nodeType     string
	keepAlive    time.Duration
	sent         []replayRecord // guarded by state.mu
	sentHead     int
	normalClose  atomic.Bool
	send         chan outbound
	done         chan struct{}
	once         sync.Once
	pendingBytes atomic.Int64
	server       *server
	state        *nodeState
}

type server struct {
	id           uint64
	addr         string
	clusterAddr  string
	serverSecret []byte
	authKey      []byte
	leader       bool
	httpClient   *http.Client
	upgrader     websocket.Upgrader
	sslCerts     string
	keepAlive    time.Duration
	certificate  atomic.Pointer[tls.Certificate]

	refreshMu     sync.Mutex
	peerDialMu    sync.Mutex
	peerMu        sync.Mutex
	peerLinks     map[uint64]*peerLink
	allPeerLinks  map[*peerLink]struct{}
	mu            sync.RWMutex
	ring          []peer
	ringEpoch     int64
	ringVersion   uint64
	epoch         int64
	version       uint64
	members       map[uint64]member
	nodes         map[string]*nodeState
	recent        sync.Map // node IDs with unexpired receipts; values are *nodeState
	recentClients sync.Map // clients with packets inside their replay window
	packetPrefix  string
	nextPacketID  atomic.Uint64
	statusErrors  atomic.Uint64
	routeErrors   atomic.Uint64
	nodeCount     atomic.Int64
}

type peerFrame struct {
	Packet  packet `json:"packet"`
	Hops    uint8  `json:"hops,omitempty"`
	Replays uint8  `json:"replays,omitempty"`
}

type peerLink struct {
	server   *server
	peerID   uint64
	conn     *websocket.Conn
	send     chan []byte
	feedback chan []byte
	incoming chan packet
	done     chan struct{}
	once     sync.Once
	mu       sync.Mutex
	history  []replayRecord
	head     int
}

func main() {
	listen := flag.String("listen", ":8080", "HTTP or HTTPS listen address")
	sslCerts := flag.String("ssl_certs", "", "directory containing a TLS certificate and private key")
	addr := flag.String("addr", "", "public HTTP base URL of this server")
	clusterAddr := flag.String("clusteraddr", "", "public HTTP base URL of the first server")
	id := flag.Uint64("id", 0, "unique position on the 64-bit hash ring")
	flag.Parse()
	secret := os.Getenv("SERVER_SECRET")
	if secret == "" {
		secret = os.Getenv("server_secret")
	}
	if *addr == "" || *clusterAddr == "" || len(secret) < 32 {
		log.Fatal("--addr, --clusteraddr, and SERVER_SECRET (at least 32 bytes) are required")
	}
	if *id > math.MaxInt64 {
		log.Fatal("--id must be between 0 and MaxInt64")
	}
	for _, raw := range []string{*addr, *clusterAddr} {
		u, err := url.Parse(raw)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.Path != "" || u.RawQuery != "" {
			log.Fatalf("invalid server URL: %q", raw)
		}
	}
	s := newServer(*id, strings.TrimRight(*addr, "/"), strings.TrimRight(*clusterAddr, "/"), []byte(secret))
	if value := os.Getenv("TCP_KEEP_ALIVE"); value != "" {
		interval, err := time.ParseDuration(value + "s")
		if err != nil || interval <= 0 || interval > time.Duration(math.MaxInt64)-10*time.Second {
			log.Fatal("TCP_KEEP_ALIVE must be a positive number of seconds")
		}
		s.keepAlive = interval
	}
	if *sslCerts != "" {
		if !strings.HasPrefix(s.addr, "https://") {
			log.Fatal("--addr must use https when --ssl_certs is set")
		}
		s.sslCerts = *sslCerts
		if err := s.reloadCertificate(); err != nil {
			log.Fatal(err)
		}
	}
	go s.refreshLoop(context.Background(), time.Minute)
	go s.receiptCleanupLoop(context.Background())
	log.Printf("listening on %s, ring id %d", *listen, *id)
	httpServer := &http.Server{Addr: *listen, Handler: s.routes(), ReadHeaderTimeout: 5 * time.Second}
	if s.sslCerts != "" {
		httpServer.TLSConfig = s.tlsConfig()
		log.Fatal(httpServer.ListenAndServeTLS("", ""))
	}
	log.Fatal(httpServer.ListenAndServe())
}

func newServer(id uint64, addr, clusterAddr string, secret []byte) *server {
	authKey := make([]byte, 32)
	if _, err := rand.Read(authKey); err != nil {
		panic(err)
	}
	s := &server{
		id: id, addr: addr, clusterAddr: clusterAddr, serverSecret: secret, authKey: authKey,
		leader:       addr == clusterAddr,
		httpClient:   &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{MaxIdleConns: 1024, MaxIdleConnsPerHost: 64, IdleConnTimeout: 90 * time.Second}},
		upgrader:     websocket.Upgrader{ReadBufferSize: 4096, WriteBufferSize: 4096, CheckOrigin: func(*http.Request) bool { return true }},
		keepAlive:    30 * time.Second,
		members:      make(map[uint64]member),
		peerLinks:    make(map[uint64]*peerLink),
		allPeerLinks: make(map[*peerLink]struct{}),
		nodes:        make(map[string]*nodeState),
		packetPrefix: hex.EncodeToString(authKey[:8]) + "-",
	}
	if s.leader {
		s.members[id] = member{peer: peer{ID: id, Addr: addr, AuthKey: authKey}, Seen: time.Now()}
		s.ring = []peer{{ID: id, Addr: addr, AuthKey: authKey}}
		s.epoch = time.Now().UnixNano()
		s.ringEpoch = s.epoch
		s.ringVersion = 1
		s.version = 1
	}
	return s
}

func (s *server) reloadCertificate() error {
	entries, err := os.ReadDir(s.sslCerts)
	if err != nil {
		return err
	}
	files := make(map[string]bool, len(entries))
	var pemFiles, keyFiles []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		files[name] = true
		switch filepath.Ext(name) {
		case ".pem":
			pemFiles = append(pemFiles, name)
		case ".key":
			keyFiles = append(keyFiles, name)
		}
	}
	var certFile, keyFile string
	switch len(keyFiles) {
	case 1:
		keyFile = keyFiles[0]
		matching := strings.TrimSuffix(keyFile, ".key") + ".pem"
		switch {
		case files[matching]:
			certFile = matching
		case len(pemFiles) == 1:
			certFile = pemFiles[0]
		case files["fullchain.pem"]:
			certFile = "fullchain.pem"
		}
	case 0:
		for _, pair := range [][2]string{{"fullchain.pem", "privkey.pem"}, {"public.pem", "private.pem"}, {"cert.pem", "key.pem"}} {
			if files[pair[0]] && files[pair[1]] {
				certFile, keyFile = pair[0], pair[1]
				break
			}
		}
	}
	if certFile == "" || keyFile == "" {
		return errors.New("certificate pair not found")
	}
	cert, err := tls.LoadX509KeyPair(filepath.Join(s.sslCerts, certFile), filepath.Join(s.sslCerts, keyFile))
	if err == nil {
		s.certificate.Store(&cert)
	}
	return err
}

func (s *server) tlsConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			cert := s.certificate.Load()
			if cert == nil {
				return nil, errors.New("TLS certificate unavailable")
			}
			return cert, nil
		},
	}
}

func (s *server) reload(w http.ResponseWriter, r *http.Request) error {
	if s.sslCerts == "" {
		return httpStatus(http.StatusNotFound)
	}
	if !verifyBody([]byte("POST /reload"), r.Header.Get("X-Cluster-Signature"), s.serverSecret) {
		return httpStatus(http.StatusUnauthorized)
	}
	if err := s.reloadCertificate(); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	handle := func(pattern string, handler func(http.ResponseWriter, *http.Request) error) {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			if err := handler(w, r); err != nil {
				writeHTTPError(w, err)
			}
		})
	}
	handle("POST /create", s.create)
	handle("POST /renew", s.renew)
	handle("GET /connect", s.connect)
	handle("GET /locate", s.locate)
	handle("GET /peer", s.peer)
	handle("POST /refresh", s.refresh)
	handle("POST /snapshot", s.receiveSnapshot)
	handle("POST /reload", s.reload)
	mux.HandleFunc("GET /health", s.health)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

type httpStatus int

func (status httpStatus) Error() string { return http.StatusText(int(status)) }

func writeHTTPError(w http.ResponseWriter, err error) {
	code := http.StatusBadGateway
	if status, ok := err.(httpStatus); ok {
		code = int(status)
	}
	http.Error(w, http.StatusText(code), code)
}

func tokenExpiry(validFor *int64) (int64, error) {
	seconds := int64(90 * 86400)
	if validFor != nil {
		seconds = *validFor
	}
	now := time.Now().Unix()
	if seconds <= 0 || seconds > math.MaxInt64-now {
		return 0, errors.New("valid_for must be a positive number of seconds")
	}
	return now + seconds, nil
}

func (s *server) create(w http.ResponseWriter, r *http.Request) error {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4096))
	var req struct {
		XSrcID        string `json:"x_src_id"`
		NodeType      string `json:"node_type"`
		ValidFor      *int64 `json:"valid_for"`
		AuthVerifyURL string `json:"auth_verify_url"`
	}
	if err != nil || json.Unmarshal(body, &req) != nil || len(req.XSrcID) == 0 || len(req.XSrcID) > 256 || (req.NodeType != "" && req.NodeType != "node" && req.NodeType != "server") {
		return httpStatus(http.StatusBadRequest)
	}
	if req.NodeType == "server" && !verifyBody(body, r.Header.Get("X-Cluster-Signature"), s.serverSecret) {
		return httpStatus(http.StatusUnauthorized)
	}
	if req.AuthVerifyURL != "" {
		if _, err := parseVerifyURL(req.AuthVerifyURL); err != nil {
			return httpStatus(http.StatusBadRequest)
		}
	}
	expires, err := tokenExpiry(req.ValidFor)
	if err != nil {
		return httpStatus(http.StatusBadRequest)
	}
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return err
	}
	if req.NodeType == "" {
		req.NodeType = "node"
	}
	c := claims{Issuer: s.id, NodeID: hex.EncodeToString(id), XSrcID: req.XSrcID, NodeType: req.NodeType, Expires: expires, AuthVerifyURL: req.AuthVerifyURL}
	token := s.signToken(c)
	w.Header().Set("Content-Type", "application/json")
	result := map[string]any{"node_id": c.NodeID, "node_type": c.NodeType, "token": token, "expires": c.Expires}
	if owner, ok := s.owner(c.NodeID); ok {
		result["address"] = owner.Addr
	}
	json.NewEncoder(w).Encode(result)
	return nil
}

func (s *server) renew(w http.ResponseWriter, r *http.Request) error {
	authorization := r.Header.Get("Authorization")
	if !strings.HasPrefix(authorization, "Bearer ") {
		return httpStatus(http.StatusUnauthorized)
	}
	c, err := s.verifyTokenSignature(strings.TrimPrefix(authorization, "Bearer "))
	if err != nil || (c.Expires <= time.Now().Unix() && c.AuthVerifyURL == "") {
		return httpStatus(http.StatusUnauthorized)
	}
	owner, ok := s.owner(c.NodeID)
	if !ok {
		return httpStatus(http.StatusBadGateway)
	}
	if owner.Addr != s.addr {
		hops := 0
		if raw := r.Header.Get("X-Renew-Hops"); raw != "" {
			hops, err = strconv.Atoi(raw)
		}
		if err != nil || hops < 0 || hops >= 4 {
			return httpStatus(http.StatusLoopDetected)
		}
		target, err := url.Parse(owner.Addr)
		if err != nil {
			return httpStatus(http.StatusBadGateway)
		}
		proxy := httputil.NewSingleHostReverseProxy(target)
		proxy.Transport = s.httpClient.Transport
		proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			writeHTTPError(w, err)
		}
		r.Header.Set("X-Renew-Hops", strconv.Itoa(hops+1))
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		proxy.ServeHTTP(w, r.WithContext(ctx))
		return nil
	}
	var req struct {
		ValidFor *int64 `json:"valid_for"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&req); err != nil && err != io.EOF {
		return httpStatus(http.StatusBadRequest)
	}
	expires, err := tokenExpiry(req.ValidFor)
	if err != nil {
		return httpStatus(http.StatusBadRequest)
	}
	if err := s.verifyAuthorization(r.Context(), c.AuthVerifyURL, "renew"); err != nil {
		return httpStatus(http.StatusUnauthorized)
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	c.Nonce = hex.EncodeToString(nonce)
	c.Issuer = s.id
	c.Expires = expires
	token := s.signToken(c)
	s.disconnectNode(c.NodeID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"node_id": c.NodeID, "node_type": c.NodeType, "token": token, "expires": c.Expires})
	return nil
}

func (s *server) disconnectNode(nodeID string) {
	var connections []*client
	s.mu.Lock()
	if state := s.nodes[nodeID]; state != nil {
		state.mu.Lock()
		for cl := range state.connections {
			cl.normalClose.Store(true)
			connections = append(connections, cl)
		}
		delete(s.nodes, nodeID)
		s.nodeCount.Add(-1)
		state.mu.Unlock()
	}
	s.mu.Unlock()
	// Remove all sockets from delivery before closing any one of them. Close
	// every transport before receipt cleanup, which can route feedback remotely.
	for _, cl := range connections {
		cl.conn.Close()
	}
	for _, cl := range connections {
		cl.close()
	}
}

func (s *server) locate(w http.ResponseWriter, r *http.Request) error {
	token := r.URL.Query().Get("token")
	if token == "" && strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
		token = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	}
	c, err := s.verifyToken(token)
	if err != nil {
		return httpStatus(http.StatusUnauthorized)
	}
	if err := s.verifyAuthorization(r.Context(), c.AuthVerifyURL, ""); err != nil {
		return httpStatus(http.StatusUnauthorized)
	}
	owner, ok := s.owner(c.NodeID)
	if !ok {
		return httpStatus(http.StatusBadGateway)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"address": owner.Addr})
	return nil
}

func (s *server) connect(w http.ResponseWriter, r *http.Request) error {
	token := r.URL.Query().Get("token")
	if token == "" && strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
		token = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	}
	c, err := s.verifyToken(token)
	if err != nil {
		return httpStatus(http.StatusUnauthorized)
	}
	owner, ok := s.owner(c.NodeID)
	if !ok {
		return httpStatus(http.StatusBadGateway)
	}
	if owner.Addr != s.addr {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMisdirectedRequest)
		json.NewEncoder(w).Encode(map[string]string{"address": owner.Addr})
		return nil
	}
	if err := s.verifyAuthorization(r.Context(), c.AuthVerifyURL, ""); err != nil {
		return httpStatus(http.StatusUnauthorized)
	}
	keepAlive := s.keepAlive
	if c.NodeType == "server" {
		keepAlive = 3 * time.Second
	}
	if raw := r.URL.Query().Get("tcp_keep_alive"); raw != "" {
		interval, err := time.ParseDuration(raw + "s")
		if err != nil || interval <= 0 || interval > time.Duration(math.MaxInt64)-10*time.Second {
			return httpStatus(http.StatusBadRequest)
		}
		keepAlive = interval
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil
	}
	cl := &client{conn: conn, nodeID: c.NodeID, xSrcID: c.XSrcID, nodeType: c.NodeType, keepAlive: keepAlive, send: make(chan outbound, 4096), done: make(chan struct{}), server: s}
	s.mu.Lock()
	state := s.nodes[c.NodeID]
	if state == nil {
		state = &nodeState{connections: make(map[*client]struct{})}
		s.nodes[c.NodeID] = state
		s.nodeCount.Add(1)
	}
	state.mu.Lock()
	state.connections[cl] = struct{}{}
	cl.state = state
	state.mu.Unlock()
	s.mu.Unlock()
	go cl.writeLoop()
	if now, ok := s.owner(c.NodeID); ok && now.Addr != s.addr {
		cl.reconnect(now.Addr)
	}
	cl.readLoop()
	return nil
}

func (s *server) refresh(w http.ResponseWriter, r *http.Request) error {
	if !s.leader {
		return httpStatus(http.StatusNotFound)
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1024))
	if err != nil || !verifyBody(data, r.Header.Get("X-Cluster-Signature"), s.serverSecret) {
		return httpStatus(http.StatusUnauthorized)
	}
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	var p peer
	if json.Unmarshal(data, &p) != nil || p.ID > math.MaxInt64 || p.Addr == "" || p.Nodes < 0 || len(p.AuthKey) != 32 {
		return httpStatus(http.StatusBadRequest)
	}
	u, err := url.Parse(p.Addr)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.Path != "" || u.RawQuery != "" {
		return httpStatus(http.StatusBadRequest)
	}
	s.mu.Lock()
	if existing, ok := s.members[p.ID]; ok && existing.Addr != p.Addr {
		s.mu.Unlock()
		return httpStatus(http.StatusConflict)
	}
	s.members[p.ID] = member{peer: p, Seen: time.Now()}
	snapshot := s.snapshotLocked()
	s.version++
	version := s.version
	s.mu.Unlock()
	state := ringSnapshot{Epoch: s.epoch, Version: version, Peers: snapshot}
	if s.applySnapshot(snapshot, s.epoch, version) {
		s.broadcastSnapshot(r.Context(), state)
	}
	data, _ = json.Marshal(state)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cluster-Signature", signBody(data, s.serverSecret))
	w.Write(data)
	return nil
}

func (s *server) receiveSnapshot(w http.ResponseWriter, r *http.Request) error {
	if s.leader {
		return httpStatus(http.StatusNotFound)
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil || !verifyBody(data, r.Header.Get("X-Cluster-Signature"), s.serverSecret) {
		return httpStatus(http.StatusUnauthorized)
	}
	var state ringSnapshot
	if json.Unmarshal(data, &state) != nil || state.Epoch <= 0 || state.Version == 0 || len(state.Peers) == 0 {
		return httpStatus(http.StatusBadRequest)
	}
	s.applySnapshot(state.Peers, state.Epoch, state.Version)
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *server) broadcastSnapshot(ctx context.Context, state ringSnapshot) {
	data, _ := json.Marshal(state)
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	signature := signBody(data, s.serverSecret)
	var wg sync.WaitGroup
	for _, member := range state.Peers {
		if member.ID == s.id {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+"/snapshot", bytes.NewReader(data))
			if err != nil {
				return
			}
			req.Header.Set("X-Cluster-Signature", signature)
			resp, err := s.httpClient.Do(req)
			if err != nil {
				return
			}
			io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
			resp.Body.Close()
		}(member.Addr)
	}
	wg.Wait()
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	count := len(s.ring)
	s.mu.RUnlock()
	json.NewEncoder(w).Encode(map[string]any{"ring_servers": count, "local_nodes": s.nodeCount.Load(), "status_errors": s.statusErrors.Load(), "route_errors": s.routeErrors.Load()})
}

func (s *server) refreshLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := s.refreshOnce(ctx); err != nil {
			log.Printf("cluster refresh: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *server) refreshOnce(ctx context.Context) error {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	if s.leader {
		s.mu.Lock()
		s.members[s.id] = member{peer: peer{ID: s.id, Addr: s.addr, Nodes: s.nodeCount.Load(), AuthKey: s.authKey}, Seen: time.Now()}
		snapshot := s.snapshotLocked()
		s.version++
		version := s.version
		s.mu.Unlock()
		state := ringSnapshot{Epoch: s.epoch, Version: version, Peers: snapshot}
		if s.applySnapshot(snapshot, s.epoch, version) {
			s.broadcastSnapshot(ctx, state)
		}
		return nil
	}
	data, _ := json.Marshal(peer{ID: s.id, Addr: s.addr, Nodes: s.nodeCount.Load(), AuthKey: s.authKey})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.clusterAddr+"/refresh", bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("X-Cluster-Signature", signBody(data, s.serverSecret))
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("leader returned %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil || !verifyBody(body, resp.Header.Get("X-Cluster-Signature"), s.serverSecret) {
		return errors.New("invalid cluster snapshot")
	}
	var state ringSnapshot
	if err := json.Unmarshal(body, &state); err != nil {
		return err
	}
	if len(state.Peers) == 0 || state.Epoch <= 0 || state.Version == 0 {
		return errors.New("empty cluster snapshot")
	}
	s.applySnapshot(state.Peers, state.Epoch, state.Version)
	return nil
}

func (s *server) snapshotLocked() []peer {
	now := time.Now()
	snapshot := make([]peer, 0, len(s.members))
	for id, m := range s.members {
		if id != s.id && now.Sub(m.Seen) > 3*time.Minute {
			delete(s.members, id)
			continue
		}
		snapshot = append(snapshot, m.peer)
	}
	sort.Slice(snapshot, func(i, j int) bool { return snapshot[i].ID < snapshot[j].ID })
	return snapshot
}

func (s *server) applySnapshot(snapshot []peer, epoch int64, version uint64) bool {
	sort.Slice(snapshot, func(i, j int) bool { return snapshot[i].ID < snapshot[j].ID })
	s.mu.Lock()
	if epoch < s.ringEpoch || (epoch == s.ringEpoch && version <= s.ringVersion) {
		s.mu.Unlock()
		return false
	}
	s.ringEpoch = epoch
	s.ringVersion = version
	changed := len(snapshot) != len(s.ring)
	if !changed {
		for i := range snapshot {
			if snapshot[i].ID != s.ring[i].ID || snapshot[i].Addr != s.ring[i].Addr || !bytes.Equal(snapshot[i].AuthKey, s.ring[i].AuthKey) {
				changed = true
				break
			}
		}
	}
	s.ring = snapshot
	var moved []struct {
		cl   *client
		addr string
	}
	if changed {
		for nodeID, state := range s.nodes {
			owner := ownerIn(snapshot, nodeID)
			if owner.Addr != s.addr {
				state.mu.Lock()
				for cl := range state.connections {
					moved = append(moved, struct {
						cl   *client
						addr string
					}{cl, owner.Addr})
				}
				state.mu.Unlock()
			}
		}
	}
	s.mu.Unlock()
	for _, item := range moved {
		item.cl.reconnect(item.addr)
	}
	return changed
}

func (s *server) owner(nodeID string) (peer, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.ring) == 0 {
		return peer{}, false
	}
	return ownerIn(s.ring, nodeID), true
}

func ownerIn(ring []peer, nodeID string) peer {
	h := sha256.Sum256([]byte(nodeID))
	position := binary.BigEndian.Uint64(h[:8]) >> 1
	i := sort.Search(len(ring), func(i int) bool { return ring[i].ID >= position })
	if i == len(ring) {
		i = 0
	}
	return ring[i]
}

func (s *server) routePacket(ctx context.Context, p packet) error {
	if p.Timestamp == nil {
		now := time.Now().UnixMilli()
		p.Timestamp = &now
	}
	for attempt := 0; attempt < 2; attempt++ {
		owner, ok := s.owner(p.Dest)
		if !ok {
			return errors.New("ring unavailable")
		}
		if owner.Addr == s.addr {
			data, err := encodePacket(p)
			if err != nil {
				return err
			}
			delivered := s.deliver(p, data)
			if p.Type == "status" && !delivered {
				return errors.New("feedback source offline")
			}
			if p.Type != "status" {
				s.sendStatus(p, delivered)
			}
			return nil
		}
		if p.hops >= 4 {
			return errors.New("peer routing exceeded four hops")
		}
		link, err := s.getPeerLink(owner)
		if err == nil {
			err = link.enqueue(ctx, p)
		}
		if err == nil || attempt == 1 {
			return err
		}
		if err := s.refreshOnce(ctx); err != nil {
			return err
		}
	}
	return errors.New("ring changed during forwarding")
}

func (s *server) route(p packet) {
	if err := s.routePacket(context.Background(), p); err != nil {
		s.routeErrors.Add(1)
		if p.Type != "status" {
			s.sendStatus(p, false)
		}
	}
}

func (s *server) deliver(p packet, data []byte) bool {
	var slow []*client
	delivered := false
	s.mu.RLock()
	state := s.nodes[p.Dest]
	if state == nil {
		s.mu.RUnlock()
		return false
	}
	state.mu.Lock()
	for cl := range state.connections {
		select {
		case <-cl.done:
			continue
		default:
		}
		select {
		case cl.send <- outbound{data: data}:
			delivered = true
			if p.replays < 3 {
				cl.sent, cl.sentHead = pruneHistory(cl.sent, cl.sentHead, time.Now().Add(-cl.keepAlive))
				if len(cl.sent) == 0 {
					s.recentClients.Store(cl, struct{}{})
				}
				cl.sent = append(cl.sent, replayRecord{packet: p, sentAt: time.Now()})
			}
		default:
			slow = append(slow, cl)
		}
	}
	if delivered && p.Type != "status" {
		now := time.Now()
		q := &state.recent
		q.prune(now)
		first := len(q.entries) == 0
		q.entries = append(q.entries, receipt{sourceID: p.sourceNodeID(), packetID: p.PacketID, sentAt: now})
		if first {
			s.recent.Store(p.Dest, state)
		}
	}
	state.mu.Unlock()
	s.mu.RUnlock()
	for _, cl := range slow {
		cl.close()
	}
	return delivered
}

func (s *server) sendStatus(p packet, delivered bool) {
	feedback := packet{Type: "status", Dest: p.sourceNodeID(), Src: p.Dest, SrcNodeID: p.Dest, PacketID: p.PacketID, Payload: "failed"}
	if delivered {
		feedback.Payload = "success"
	}
	if err := s.routePacket(context.Background(), feedback); err != nil {
		s.statusErrors.Add(1)
	}
}

func (q *receiptQueue) prune(now time.Time) {
	cutoff := now.Add(-10 * time.Second)
	for q.head < len(q.entries) && q.entries[q.head].sentAt.Before(cutoff) {
		q.head++
	}
	if q.head == len(q.entries) {
		q.entries = nil
		q.head = 0
		return
	}
	if q.head > 1024 && q.head*2 >= len(q.entries) {
		q.entries = append([]receipt(nil), q.entries[q.head:]...)
		q.head = 0
	}
}

func (s *server) cleanupReceipts(now time.Time) {
	s.cleanupPeerLinks(now)
	s.recentClients.Range(func(key, value any) bool {
		cl := key.(*client)
		cl.state.mu.Lock()
		cl.sent, cl.sentHead = pruneHistory(cl.sent, cl.sentHead, now.Add(-cl.keepAlive))
		if len(cl.sent) == 0 {
			s.recentClients.CompareAndDelete(cl, value)
		}
		cl.state.mu.Unlock()
		return true
	})
	s.recent.Range(func(key, value any) bool {
		state := value.(*nodeState)
		state.mu.Lock()
		state.recent.prune(now)
		if len(state.recent.entries) == 0 {
			s.recent.CompareAndDelete(key, state)
		}
		state.mu.Unlock()
		return true
	})
}

func (s *server) receiptCleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.cleanupReceipts(now)
		}
	}
}

func (cl *client) readLoop() {
	defer cl.close()
	jobs := make(chan routedPacket, 1024)
	defer close(jobs)
	go cl.routeLoop(jobs)
	cl.conn.SetReadLimit(1 << 20)
	cl.conn.SetReadDeadline(time.Now().Add(cl.keepAlive + 10*time.Second))
	cl.conn.SetPongHandler(func(string) error {
		return cl.conn.SetReadDeadline(time.Now().Add(cl.keepAlive + 10*time.Second))
	})
	for {
		kind, data, err := cl.conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				cl.normalClose.Store(true)
			}
			return
		}
		if kind != websocket.TextMessage {
			return
		}
		p, err := decodePacket(data)
		if err != nil || p.Type == "" || p.Type == "reconnect" || (p.Dest == "" && len(p.Dests) == 0) || (p.Dest != "" && len(p.Dests) != 0) || len(p.Dests) > 1024 {
			return
		}
		for _, dest := range p.Dests {
			if dest == "" {
				return
			}
		}
		if cl.nodeType == "server" {
			if p.sourceNodeID() == "" {
				return
			}
		} else {
			if p.Type == "status" || p.Src != "" || p.XSrc != "" {
				return
			}
			p.Src, p.XSrc = cl.nodeID, cl.xSrcID
			p.SrcNodeID = cl.nodeID
		}
		if cl.nodeType != "server" || p.Timestamp == nil {
			now := time.Now().UnixMilli()
			p.Timestamp = &now
		}
		if p.PacketID == "" {
			p.PacketID = cl.server.packetPrefix + strconv.FormatUint(cl.server.nextPacketID.Add(1), 36)
		}
		size := int64(len(data))
		if cl.pendingBytes.Add(size) > 4<<20 {
			cl.pendingBytes.Add(-size)
			return
		}
		select {
		case jobs <- routedPacket{packet: p, bytes: len(data)}:
		default:
			cl.pendingBytes.Add(-size)
			return
		}
	}
}

func (cl *client) routeLoop(jobs <-chan routedPacket) {
	for job := range jobs {
		cl.pendingBytes.Add(-int64(job.bytes))
		dests := job.packet.Dests
		if len(dests) == 0 {
			dests = []string{job.packet.Dest}
		}
		for _, dest := range dests {
			p := job.packet
			p.Dest = dest
			p.Dests = nil
			cl.server.route(p)
		}
	}
}

func (cl *client) writeLoop() {
	ticker := time.NewTicker(cl.keepAlive)
	defer ticker.Stop()
	defer cl.close()
	for {
		select {
		case <-cl.done:
			return
		case msg := <-cl.send:
			cl.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := cl.conn.WriteMessage(websocket.TextMessage, msg.data); err != nil {
				return
			}
			if msg.closeAfter {
				return
			}
		case <-ticker.C:
			cl.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := cl.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (cl *client) enqueue(msg outbound) {
	select {
	case <-cl.done:
	case cl.send <- msg:
	default:
		cl.close()
	}
}

func (cl *client) reconnect(addr string) {
	now := time.Now().UnixMilli()
	data, _ := encodePacket(packet{Type: "reconnect", SrcNodeID: strconv.FormatUint(cl.server.id, 10), Payload: addr, Timestamp: &now})
	cl.enqueue(outbound{data: data, closeAfter: true})
}

func (cl *client) close() {
	cl.once.Do(func() {
		close(cl.done)
		cl.conn.Close()
		s := cl.server
		var recent []receipt
		var replay []replayRecord
		s.mu.Lock()
		state := cl.state
		state.mu.Lock()
		delete(state.connections, cl)
		cl.sent, cl.sentHead = pruneHistory(cl.sent, cl.sentHead, time.Now().Add(-cl.keepAlive))
		if !cl.normalClose.Load() {
			replay = append(replay, cl.sent[cl.sentHead:]...)
		}
		cl.sent = nil
		cl.sentHead = 0
		s.recentClients.Delete(cl)
		if len(state.connections) == 0 {
			if s.nodes[cl.nodeID] == state {
				delete(s.nodes, cl.nodeID)
				s.nodeCount.Add(-1)
			}
			state.recent.prune(time.Now())
			recent = state.recent.entries[state.recent.head:]
			s.recent.CompareAndDelete(cl.nodeID, state)
		}
		state.mu.Unlock()
		s.mu.Unlock()
		// Replay owns the outcome for these receipts, including an offline result.
		var retried map[[2]string]struct{}
		if len(recent) != 0 && len(replay) != 0 {
			retried = make(map[[2]string]struct{}, len(replay))
		}
		for _, item := range replay {
			p := item.packet
			if retried != nil {
				retried[[2]string{p.sourceNodeID(), p.PacketID}] = struct{}{}
			}
			p.replays++
			s.route(p)
		}
		if len(recent) == 0 {
			return
		}
		// Older receipts have no payload to replay. Fail them only while this
		// server still owns the node and no replacement connection is available.
		s.mu.RLock()
		localOffline := len(s.ring) != 0 && ownerIn(s.ring, cl.nodeID).Addr == s.addr
		if current := s.nodes[cl.nodeID]; current != nil {
			current.mu.Lock()
			for conn := range current.connections {
				select {
				case <-conn.done:
				default:
					localOffline = false
				}
			}
			current.mu.Unlock()
		}
		s.mu.RUnlock()
		if !localOffline {
			return
		}
		for _, r := range recent {
			if _, ok := retried[[2]string{r.sourceID, r.packetID}]; ok {
				continue
			}
			status := packet{Type: "status", Dest: r.sourceID, Src: cl.nodeID, SrcNodeID: cl.nodeID, PacketID: r.packetID, Payload: "failed"}
			if err := s.routePacket(context.Background(), status); err != nil {
				s.statusErrors.Add(1)
			}
		}
	})
}

func encodePacket(p packet) ([]byte, error) {
	data, _ := json.Marshal(p)
	if len(data) > 1<<20 {
		return nil, errors.New("packet too large")
	}
	return data, nil
}

func decodePacket(data []byte) (packet, error) {
	var p packet
	if len(data) > 1<<20 {
		return p, errors.New("packet too large")
	}
	err := json.Unmarshal(data, &p)
	return p, err
}

func (s *server) peerKey(id uint64) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	i := sort.Search(len(s.ring), func(i int) bool { return s.ring[i].ID >= id })
	if i == len(s.ring) || s.ring[i].ID != id {
		return nil, false
	}
	return s.ring[i].AuthKey, true
}

func (s *server) signToken(c claims) string {
	data, _ := json.Marshal(c)
	payload := base64.RawURLEncoding.EncodeToString(data)
	mac := hmac.New(sha256.New, s.authKey)
	mac.Write([]byte(payload))
	return payload + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (s *server) verifyToken(token string) (claims, error) {
	c, err := s.verifyTokenSignature(token)
	if err != nil {
		return claims{}, err
	}
	if c.Expires <= time.Now().Unix() {
		return claims{}, errors.New("expired token")
	}
	return c, nil
}

func (s *server) verifyTokenSignature(token string) (claims, error) {
	payload, signature, ok := strings.Cut(token, ".")
	if !ok || len(token) > 8192 {
		return claims{}, errors.New("invalid token")
	}
	data, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return claims{}, err
	}
	var c claims
	if json.Unmarshal(data, &c) != nil || len(c.NodeID) != 32 || len(c.XSrcID) == 0 || len(c.XSrcID) > 256 || (c.NodeType != "" && c.NodeType != "node" && c.NodeType != "server") || c.Expires <= 0 || len(c.AuthVerifyURL) > 2048 {
		return claims{}, errors.New("invalid claims")
	}
	if _, err := hex.DecodeString(c.NodeID); err != nil {
		return claims{}, err
	}
	key, ok := s.peerKey(c.Issuer)
	if !ok {
		return claims{}, errors.New("unknown token issuer")
	}
	got, err := base64.RawURLEncoding.DecodeString(signature)
	if err != nil {
		return claims{}, err
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(payload))
	if !hmac.Equal(got, mac.Sum(nil)) {
		return claims{}, errors.New("invalid signature")
	}
	return c, nil
}

func signBody(data, key []byte) string {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return hex.EncodeToString(mac.Sum(nil))
}

func verifyBody(data []byte, signature string, key []byte) bool {
	got, err := hex.DecodeString(signature)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return hmac.Equal(got, mac.Sum(nil))
}

func parseVerifyURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || len(raw) > 2048 || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return nil, errors.New("invalid auth_verify_url")
	}
	return u, nil
}

// verifyAuthorization never sends the node's bearer token to the verifier.
func (s *server) verifyAuthorization(ctx context.Context, rawURL, action string) error {
	if rawURL == "" {
		return nil
	}
	u, err := parseVerifyURL(rawURL)
	if err != nil {
		return err
	}
	if action != "" {
		query := u.Query()
		query.Set("action", action)
		u.RawQuery = query.Encode()
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	client := *s.httpClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 17))
	if err != nil || resp.StatusCode != http.StatusOK || len(body) > 16 || !strings.EqualFold(strings.TrimSpace(string(body)), "ok") {
		return errors.New("authorization denied")
	}
	return nil
}

func (s *server) peer(w http.ResponseWriter, r *http.Request) error {
	id, err := strconv.ParseUint(r.Header.Get("X-Server-ID"), 10, 64)
	key, known := s.peerKey(id)
	if err != nil || !known || id == s.id || !verifyBody([]byte("GET /peer"), r.Header.Get("X-Server-Signature"), key) {
		return httpStatus(http.StatusUnauthorized)
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil
	}
	s.addPeerLink(id, conn)
	return nil
}

func (s *server) getPeerLink(owner peer) (*peerLink, error) {
	s.peerDialMu.Lock()
	defer s.peerDialMu.Unlock()
	s.peerMu.Lock()
	if link := s.peerLinks[owner.ID]; link != nil {
		select {
		case <-link.done:
		default:
			s.peerMu.Unlock()
			return link, nil
		}
	}
	s.peerMu.Unlock()
	address := "ws" + strings.TrimPrefix(owner.Addr, "http") + "/peer"
	headers := http.Header{}
	headers.Set("X-Server-ID", strconv.FormatUint(s.id, 10))
	headers.Set("X-Server-Signature", signBody([]byte("GET /peer"), s.authKey))
	conn, resp, err := (&websocket.Dialer{Proxy: http.ProxyFromEnvironment, HandshakeTimeout: 10 * time.Second}).Dial(address, headers)
	if err != nil {
		if resp != nil {
			resp.Body.Close()
		}
		return nil, err
	}
	return s.addPeerLink(owner.ID, conn), nil
}

func (s *server) addPeerLink(peerID uint64, conn *websocket.Conn) *peerLink {
	link := &peerLink{server: s, peerID: peerID, conn: conn, send: make(chan []byte, 4096), feedback: make(chan []byte, 4096), incoming: make(chan packet, 4096), done: make(chan struct{})}
	s.peerMu.Lock()
	old := s.peerLinks[link.peerID]
	if old == nil {
		s.peerLinks[link.peerID] = link
	} else {
		select {
		case <-old.done:
			s.peerLinks[link.peerID] = link
		default:
		}
	}
	s.allPeerLinks[link] = struct{}{}
	s.peerMu.Unlock()
	go link.writeLoop()
	for range 16 {
		go link.processLoop()
	}
	go link.readLoop()
	return link
}

func (link *peerLink) enqueue(ctx context.Context, p packet) error {
	frame := peerFrame{Packet: p, Hops: p.hops, Replays: p.replays}
	data, _ := json.Marshal(frame)
	if len(data) > 1<<20+4096 {
		return errors.New("peer packet too large")
	}
	link.mu.Lock()
	select {
	case <-link.done:
		link.mu.Unlock()
		return errors.New("peer link closed")
	default:
	}
	link.recordLocked(p, time.Now())
	link.mu.Unlock()
	queue := link.send
	if p.Type == "status" {
		queue = link.feedback
	}
	select {
	case <-link.done:
		return errors.New("peer link closed")
	case <-ctx.Done():
		return ctx.Err()
	case queue <- data:
		return nil
	}
}

func (link *peerLink) readLoop() {
	defer link.close()
	defer close(link.incoming)
	link.conn.SetReadLimit(1<<20 + 4096)
	link.conn.SetReadDeadline(time.Now().Add(13 * time.Second))
	link.conn.SetPongHandler(func(string) error { return link.conn.SetReadDeadline(time.Now().Add(13 * time.Second)) })
	for {
		kind, data, err := link.conn.ReadMessage()
		if err != nil || kind != websocket.TextMessage {
			return
		}
		var frame peerFrame
		if json.Unmarshal(data, &frame) != nil || frame.Packet.Dest == "" || len(frame.Packet.Dests) != 0 || frame.Packet.sourceNodeID() == "" || frame.Packet.Type == "" || frame.Packet.Type == "reconnect" || frame.Hops >= 4 {
			return
		}
		p := frame.Packet
		if p.Timestamp == nil {
			now := time.Now().UnixMilli()
			p.Timestamp = &now
		}
		p.hops = frame.Hops + 1
		p.replays = frame.Replays
		link.mu.Lock()
		link.recordLocked(p, time.Now())
		link.mu.Unlock()
		select {
		case link.incoming <- p:
		case <-link.done:
			return
		}
	}
}

func (link *peerLink) processLoop() {
	for p := range link.incoming {
		link.server.route(p)
	}
}

func (link *peerLink) writeLoop() {
	defer link.close()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		kind := websocket.TextMessage
		var data []byte
		// Pings must stay responsive; feedback takes priority over new data.
		select {
		case <-link.done:
			return
		case <-ticker.C:
			kind = websocket.PingMessage
		default:
			select {
			case data = <-link.feedback:
			default:
				select {
				case <-link.done:
					return
				case <-ticker.C:
					kind = websocket.PingMessage
				case data = <-link.feedback:
				case data = <-link.send:
				}
			}
		}
		link.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := link.conn.WriteMessage(kind, data); err != nil {
			return
		}
	}
}

func (link *peerLink) recordLocked(p packet, now time.Time) {
	if p.replays >= 3 {
		return
	}
	link.history, link.head = pruneHistory(link.history, link.head, now.Add(-3*time.Second))
	link.history = append(link.history, replayRecord{packet: p, sentAt: now})
}

func (link *peerLink) close() {
	link.once.Do(func() {
		close(link.done)
		link.conn.Close()
		s := link.server
		s.peerMu.Lock()
		delete(s.allPeerLinks, link)
		if s.peerLinks[link.peerID] == link {
			delete(s.peerLinks, link.peerID)
			for other := range s.allPeerLinks {
				if other.peerID == link.peerID {
					s.peerLinks[link.peerID] = other
					break
				}
			}
		}
		s.peerMu.Unlock()
		link.mu.Lock()
		link.history, link.head = pruneHistory(link.history, link.head, time.Now().Add(-3*time.Second))
		replay := append([]replayRecord(nil), link.history[link.head:]...)
		link.history, link.head = nil, 0
		link.mu.Unlock()
		if len(replay) != 0 {
			go func() {
				for _, item := range replay {
					p := item.packet
					p.replays++
					s.route(p)
				}
			}()
		}
	})
}

func (s *server) cleanupPeerLinks(now time.Time) {
	s.peerMu.Lock()
	links := make([]*peerLink, 0, len(s.allPeerLinks))
	for link := range s.allPeerLinks {
		links = append(links, link)
	}
	s.peerMu.Unlock()
	for _, link := range links {
		link.mu.Lock()
		link.history, link.head = pruneHistory(link.history, link.head, now.Add(-3*time.Second))
		link.mu.Unlock()
	}
}

func pruneHistory(history []replayRecord, head int, cutoff time.Time) ([]replayRecord, int) {
	for head < len(history) && history[head].sentAt.Before(cutoff) {
		head++
	}
	if head == len(history) {
		return nil, 0
	}
	if head > 1024 && head*2 >= len(history) {
		return append([]replayRecord(nil), history[head:]...), 0
	}
	return history, head
}
