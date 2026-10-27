package main

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
	"golang.org/x/net/publicsuffix"
)

type peer struct {
	ID        uint64 `json:"id"`
	Addr      string `json:"addr"`
	Nodes     int64  `json:"nodes"`
	AuthToken string `json:"auth_token"`
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
	packet *packet
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
	nextConnection uint64
	mu             sync.Mutex
	connections    map[*client]struct{}
	recent         receiptQueue
}

type outbound struct {
	data       []byte
	closeAfter bool
}

type routedPacket struct {
	client *client
	packet packet
	bytes  int
}

type client struct {
	pingIndex       int // guarded by server.pingMu
	nextPing        time.Time
	ping            chan struct{}
	connectionOrder uint64
	conn            *websocket.Conn
	nodeID          string
	xSrcID          string
	nodeType        string
	keepAlive       time.Duration
	rp5             int64
	rateBucket      int64 // readLoop owns the per-connection bucket and counter
	rateCount       int64
	sent            []replayRecord // guarded by state.mu
	sentHead        int
	normalClose     atomic.Bool
	send            chan outbound
	done            chan struct{}
	once            sync.Once
	queuedBytes     atomic.Int64
	pendingBytes    atomic.Int64
	server          *server
	state           *nodeState
}

type server struct {
	id           uint64
	addr         string
	clusterAddr  string
	serverToken  string // local credential, regenerated on restart
	serverNodeID string
	leader       bool
	httpClient   *http.Client
	upgrader     websocket.Upgrader
	sslCerts     string
	keepAlive    time.Duration
	certificate  atomic.Pointer[tls.Certificate]

	refreshMu     sync.Mutex
	peerDials     sync.Map // peer ID -> *sync.Mutex; only used when establishing a link
	peerMu        sync.RWMutex
	peerLinks     map[uint64]*peerLink
	allPeerLinks  map[*peerLink]struct{}
	mu            sync.RWMutex
	ring          []peer
	routingRing   atomic.Value // immutable []peer, published with membership updates
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
	pingMu        sync.Mutex
	pings         pingHeap
	pingWake      chan struct{}
	routingOnce   sync.Once
	routingDone   chan struct{}
	routingQueues []chan *routedPacket
	routingBytes  atomic.Int64
	routeErrors   atomic.Uint64
	nodeCount     atomic.Int64
}

type peerFrame struct {
	Packet  packet `json:"packet"`
	Hops    uint8  `json:"hops,omitempty"`
	Replays uint8  `json:"replays,omitempty"`
}

type frameQueue struct {
	entries [][]byte
	head    int
	bytes   int
}

func (q *frameQueue) push(data []byte) bool {
	if q.bytes+len(data) > 64<<20 {
		return false
	}
	q.entries = append(q.entries, data)
	q.bytes += len(data)
	return true
}

func (q *frameQueue) pop() []byte {
	if q.head == len(q.entries) {
		return nil
	}
	data := q.entries[q.head]
	q.entries[q.head] = nil
	q.head++
	q.bytes -= len(data)
	if q.head == len(q.entries) {
		q.entries, q.head = q.entries[:0], 0
		if cap(q.entries) > 4096 {
			q.entries = nil
		}
	} else if q.head > 1024 && q.head*2 >= len(q.entries) {
		n := copy(q.entries, q.entries[q.head:])
		clear(q.entries[n:])
		q.entries, q.head = q.entries[:n], 0
	}
	return data
}

type peerLink struct {
	batch    bool
	server   *server
	peerID   uint64
	conn     *websocket.Conn
	done     chan struct{}
	once     sync.Once
	mu       sync.Mutex
	history  []replayRecord
	head     int
	data     frameQueue // both queues and history are guarded by mu
	feedback frameQueue
	ready    chan struct{}
}

func main() {
	sslCerts := flag.String("ssl_certs", "", "directory containing a TLS certificate and private key")
	addr := flag.String("addr", "", "public HTTPS base URL of this server")
	clusterAddr := flag.String("clusteraddr", "", "primary server URL; omitted starts a leader")
	id := flag.Uint64("id", 0, "unique position on the 64-bit hash ring")
	flag.Parse()
	if *addr == "" || *sslCerts == "" {
		log.Fatal("--addr (HTTPS) and --ssl_certs are required")
	}
	if *id > math.MaxInt64 {
		log.Fatal("--id must be between 0 and MaxInt64")
	}
	listen, err := listenAddress(*addr)
	if err != nil {
		log.Fatal(err)
	}
	if *clusterAddr != "" {
		if _, err := listenAddress(*clusterAddr); err != nil {
			log.Fatal(err)
		}
	}
	s := newServer(*id, strings.TrimRight(*addr, "/"), strings.TrimRight(*clusterAddr, "/"))
	if value := os.Getenv("TCP_KEEP_ALIVE"); value != "" {
		interval, err := time.ParseDuration(value + "s")
		if err != nil || interval <= 0 || interval > time.Duration(math.MaxInt64)-10*time.Second {
			log.Fatal("TCP_KEEP_ALIVE must be a positive number of seconds")
		}
		s.keepAlive = interval
	}
	s.sslCerts = *sslCerts
	if err := s.reloadCertificate(); err != nil {
		log.Fatal(err)
	}
	// Bind before joining: the primary calls back to verify this server.
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatal(err)
	}
	go s.refreshLoop(context.Background(), time.Minute)
	go s.receiptCleanupLoop(context.Background())
	go s.watchCertificate(context.Background(), 30*time.Second)
	log.Printf("listening on %s, ring id %d", listen, *id)
	httpServer := &http.Server{Addr: listen, Handler: s.routes(), ReadHeaderTimeout: 5 * time.Second, TLSConfig: s.tlsConfig()}
	log.Fatal(httpServer.ServeTLS(listener, "", ""))
}

func listenAddress(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return "", errors.New("invalid server address")
	}
	if _, err := authAuthority(u.Host); err != nil {
		return "", err
	}
	port := u.Port()
	if port == "" {
		port = "443"
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return "", errors.New("invalid server port")
	}
	return ":" + port, nil
}

func newServer(id uint64, addr, clusterAddr string) *server {
	addr = strings.ToLower(strings.TrimRight(addr, "/"))
	clusterAddr = strings.ToLower(strings.TrimRight(clusterAddr, "/"))
	if clusterAddr == "" {
		clusterAddr = addr
	}
	s := &server{
		id: id, addr: addr, clusterAddr: clusterAddr,
		leader:       addr == clusterAddr,
		httpClient:   &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Transport: &http.Transport{MaxIdleConns: 1024, MaxIdleConnsPerHost: 64, IdleConnTimeout: 90 * time.Second}},
		upgrader:     websocket.Upgrader{ReadBufferSize: 4096, WriteBufferSize: 4096, CheckOrigin: func(*http.Request) bool { return true }},
		keepAlive:    30 * time.Second,
		routingDone:  make(chan struct{}),
		pingWake:     make(chan struct{}, 1),
		members:      make(map[uint64]member),
		peerLinks:    make(map[uint64]*peerLink),
		allPeerLinks: make(map[*peerLink]struct{}),
		nodes:        make(map[string]*nodeState),
		packetPrefix: fmt.Sprintf("%x-%x-", id, time.Now().UnixNano()),
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		panic(err)
	}
	s.serverToken = "/server|" + hex.EncodeToString(secret)
	s.serverNodeID = strings.TrimPrefix(addr, "https://") + "|" + strconv.FormatUint(id, 10)
	if s.leader {
		s.members[id] = member{peer: peer{ID: id, Addr: addr, AuthToken: s.serverToken}, Seen: time.Now()}
		s.ring = []peer{{ID: id, Addr: addr, AuthToken: s.serverToken}}
		s.epoch = time.Now().UnixNano()
		s.ringEpoch = s.epoch
		s.ringVersion = 1
		s.version = 1
	}
	s.routingRing.Store(s.ring)
	return s
}

func (s *server) reloadCertificate() error {
	// Resolve a published certificate directory once so a symlink switch cannot
	// mix the old certificate with the new key.
	directory, err := filepath.EvalSymlinks(s.sslCerts)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(directory)
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
	cert, err := tls.LoadX509KeyPair(filepath.Join(directory, certFile), filepath.Join(directory, keyFile))
	if err == nil {
		s.certificate.Store(&cert)
	}
	return err
}

func (s *server) watchCertificate(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			previous := s.certificate.Load()
			if err := s.reloadCertificate(); err != nil {
				log.Printf("certificate reload: %v", err)
				continue
			}
			current := s.certificate.Load()
			if previous == nil || !bytes.Equal(previous.Certificate[0], current.Certificate[0]) {
				log.Print("TLS certificate reloaded")
			}
		}
	}
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
	if r.Header.Get("X-Node-ID") != s.serverNodeID || subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")), []byte(s.serverToken)) != 1 {
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
	handle("POST /server", s.registerServer)
	handle("POST /renew", s.renew)
	handle("GET /connect", s.connect)
	handle("GET /locate", s.locate)
	handle("GET /peer", s.peer)
	handle("POST /refresh", s.refresh)
	handle("GET /snapshot", s.viewSnapshot)
	handle("POST /snapshot", s.receiveSnapshot)
	handle("POST /reload", s.reload)
	mux.HandleFunc("GET /health", s.health)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Node-ID")
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

func (s *server) create(w http.ResponseWriter, r *http.Request) error {
	var req struct {
		AuthDomain string `json:"auth_domain"`
		Path       string `json:"path"`
		Secret     string `json:"secret_string"`
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 12288))
	if err != nil || json.Unmarshal(body, &req) != nil {
		return httpStatus(400)
	}
	domain, err := authAuthority(req.AuthDomain)
	if err != nil {
		return httpStatus(400)
	}
	token := req.Path + "|" + req.Secret
	if _, _, err := authEndpoint(domain+"|new", token); err != nil {
		return httpStatus(400)
	}
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return err
	}
	nodeID := domain + "|" + hex.EncodeToString(id)
	if _, err := s.authenticate(r.Context(), nodeID, token, "", r.Header.Get("Origin")); err != nil {
		return httpStatus(http.StatusUnauthorized)
	}
	result := map[string]any{"node_id": nodeID, "token": token}
	if owner, ok := s.owner(nodeID); ok {
		result["address"] = owner.Addr
	}
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(result)
}

func (s *server) renew(w http.ResponseWriter, r *http.Request) error {
	nodeID, token := requestIdentity(r)
	c, err := s.authenticate(r.Context(), nodeID, token, "renew", r.Header.Get("Origin"))
	if err != nil {
		return httpStatus(401)
	}
	s.disconnectNode(nodeID)
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(c)
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
	nodeID, token := requestIdentity(r)
	c, err := s.authenticate(r.Context(), nodeID, token, "", r.Header.Get("Origin"))
	if err != nil {
		return httpStatus(401)
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
	nodeID, token := requestIdentity(r)
	c, err := s.authenticate(r.Context(), nodeID, token, "", r.Header.Get("Origin"))
	if err != nil {
		return httpStatus(401)
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
	cl := &client{pingIndex: -1, ping: make(chan struct{}, 1), conn: conn, nodeID: c.NodeID, xSrcID: c.XSrcID, nodeType: c.NodeType, rp5: c.RP5, keepAlive: keepAlive, send: make(chan outbound, 1024), done: make(chan struct{}), server: s}
	var evicted []*client
	s.mu.Lock()
	state := s.nodes[c.NodeID]
	if state == nil {
		state = &nodeState{connections: make(map[*client]struct{})}
		s.nodes[c.NodeID] = state
		s.nodeCount.Add(1)
	}
	state.mu.Lock()
	for len(state.connections) >= c.MaxC {
		var oldest *client
		for existing := range state.connections {
			if oldest == nil || existing.connectionOrder < oldest.connectionOrder {
				oldest = existing
			}
		}
		oldest.normalClose.Store(true)
		delete(state.connections, oldest)
		evicted = append(evicted, oldest)
	}
	state.nextConnection++
	cl.connectionOrder = state.nextConnection
	state.connections[cl] = struct{}{}
	cl.state = state
	state.mu.Unlock()
	s.mu.Unlock()
	for _, old := range evicted {
		old.close()
	}
	s.startRouting()
	s.pingMu.Lock()
	select {
	case <-cl.done:
	default:
		cl.nextPing = time.Now().Add(cl.keepAlive)
		heap.Push(&s.pings, cl)
	}
	s.pingMu.Unlock()
	select {
	case s.pingWake <- struct{}{}:
	default:
	}
	go cl.writeLoop()
	if now, ok := s.owner(c.NodeID); ok && now.Addr != s.addr {
		cl.reconnect(now.Addr)
	}
	cl.readLoop()
	return nil
}

// /server verifies this process's credential or registers a joining server.
func (s *server) registerServer(w http.ResponseWriter, r *http.Request) error {
	var req struct {
		NodeID string `json:"node_id"`
		Domain string `json:"server_domain"`
		ID     uint64 `json:"id"`
		Secret string `json:"secret"`
		Nodes  int64  `json:"nodes"`
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 12288))
	if err != nil || json.Unmarshal(body, &req) != nil {
		return httpStatus(400)
	}
	if req.NodeID != "" || r.URL.Query().Has("node_id") {
		_, secret, _ := strings.Cut(s.serverToken, "|")
		if req.NodeID != s.serverNodeID || (r.URL.Query().Has("node_id") && r.URL.Query().Get("node_id") != req.NodeID) || subtle.ConstantTimeCompare([]byte(req.Secret), []byte(secret)) != 1 {
			return httpStatus(401)
		}
		w.Header().Set("Content-Type", "application/json")
		return json.NewEncoder(w).Encode(map[string]any{"valid": true, "rp5": 10})
	}
	if !s.leader {
		return httpStatus(404)
	}
	if req.ID > math.MaxInt64 || req.Nodes < 0 {
		return httpStatus(400)
	}
	domain, err := authAuthority(req.Domain)
	if err != nil || !sameServerDomain(domain, strings.TrimPrefix(s.addr, "https://")) {
		return httpStatus(403)
	}
	nodeID := domain + "|" + strconv.FormatUint(req.ID, 10)
	token := "/server|" + req.Secret
	if _, err := s.authenticate(r.Context(), nodeID, token, "", r.Header.Get("Origin")); err != nil {
		return httpStatus(401)
	}
	return s.updateMember(w, r, peer{ID: req.ID, Addr: "https://" + domain, AuthToken: token, Nodes: req.Nodes})
}

func (s *server) refresh(w http.ResponseWriter, r *http.Request) error {
	if !s.leader {
		return httpStatus(404)
	}
	var p peer
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 8192))
	if err != nil || json.Unmarshal(body, &p) != nil || p.Nodes < 0 {
		return httpStatus(400)
	}
	if err := s.verifyServerNode(r, p.ID); err != nil {
		return err
	}
	s.mu.RLock()
	existing, ok := s.members[p.ID]
	s.mu.RUnlock()
	if !ok || existing.Addr != p.Addr {
		return httpStatus(409)
	}
	p.AuthToken = existing.AuthToken
	return s.updateMember(w, r, p)
}

func (s *server) updateMember(w http.ResponseWriter, r *http.Request, p peer) error {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	s.mu.Lock()
	if p.ID == s.id || (s.members[p.ID].Addr != "" && s.members[p.ID].Addr != p.Addr) {
		s.mu.Unlock()
		return httpStatus(409)
	}
	for id, m := range s.members {
		if id != p.ID && m.Addr == p.Addr {
			s.mu.Unlock()
			return httpStatus(409)
		}
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
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(state)
}

func (s *server) receiveSnapshot(w http.ResponseWriter, r *http.Request) error {
	if s.leader {
		return httpStatus(http.StatusNotFound)
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		return httpStatus(400)
	}
	nodeID, token := requestIdentity(r)
	domain, _, _ := strings.Cut(nodeID, "|")
	if domain != strings.TrimPrefix(s.clusterAddr, "https://") || !strings.HasPrefix(token, "/server|") {
		return httpStatus(401)
	}
	if _, err := s.authenticate(r.Context(), nodeID, token, "", r.Header.Get("Origin")); err != nil {
		return httpStatus(401)
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
			req.Header.Set("X-Node-ID", s.serverNodeID)
			req.Header.Set("Authorization", "Bearer "+s.serverToken)
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
		s.members[s.id] = member{peer: peer{ID: s.id, Addr: s.addr, Nodes: s.nodeCount.Load(), AuthToken: s.serverToken}, Seen: time.Now()}
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
	_, secret, _ := strings.Cut(s.serverToken, "|")
	data, _ := json.Marshal(map[string]any{"server_domain": strings.TrimPrefix(s.addr, "https://"), "id": s.id, "secret": secret, "nodes": s.nodeCount.Load()})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.clusterAddr+"/server", bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("leader returned %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
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

func (s *server) viewSnapshot(w http.ResponseWriter, r *http.Request) error {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil || host != "127.0.0.1" {
		return httpStatus(http.StatusUnauthorized)
	}
	s.mu.RLock()
	peers := append([]peer(nil), s.ring...)
	s.mu.RUnlock()
	var text strings.Builder
	text.WriteString("addr\tnodes\tringid\n")
	for _, p := range peers {
		if p.ID == s.id {
			p.Nodes = s.nodeCount.Load()
		}
		fmt.Fprintf(&text, "%s\t%d\t%d\n", p.Addr, p.Nodes, p.ID)
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, err = io.WriteString(w, text.String())
	return err
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
	if len(snapshot) == 0 {
		return false
	}
	snapshot = append([]peer(nil), snapshot...)
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
			if snapshot[i].ID != s.ring[i].ID || snapshot[i].Addr != s.ring[i].Addr || snapshot[i].AuthToken != s.ring[i].AuthToken {
				changed = true
				break
			}
		}
	}
	s.ring = snapshot
	s.routingRing.Store(snapshot)
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
	ring, _ := s.routingRing.Load().([]peer)
	if len(ring) == 0 {
		return peer{}, false
	}
	return ownerIn(ring, nodeID), true
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
	now := time.Now()
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
		if cl.queue(outbound{data: data}) {
			delivered = true
			if p.replays < 3 {
				cl.sent, cl.sentHead = pruneHistory(cl.sent, cl.sentHead, now.Add(-cl.keepAlive))
				if len(cl.sent) == 0 {
					s.recentClients.Store(cl, struct{}{})
				}
				cl.sent = append(cl.sent, replayRecord{packet: &p, sentAt: now})
			}
		} else {
			slow = append(slow, cl)
		}
	}
	if delivered && p.Type != "status" {
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
		q.entries[q.head] = receipt{}
		q.head++
	}
	if q.head == len(q.entries) {
		q.entries = nil
		q.head = 0
		return
	}
	if q.head > 1024 && q.head*2 >= len(q.entries) {
		n := copy(q.entries, q.entries[q.head:])
		clear(q.entries[n:])
		q.entries, q.head = q.entries[:n], 0
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
	cl.conn.SetReadLimit(1 << 20)
	cl.conn.SetReadDeadline(time.Now().Add(cl.keepAlive + 10*time.Second))
	cl.conn.SetPongHandler(func(string) error {
		return cl.conn.SetReadDeadline(time.Now().Add(cl.keepAlive + 10*time.Second))
	})
	var buffer bytes.Buffer
	for {
		// Do not pin a large one-off message on an otherwise idle connection.
		if buffer.Cap() > 64<<10 {
			buffer = bytes.Buffer{}
		}
		kind, reader, err := cl.conn.NextReader()
		if err == nil {
			if kind != websocket.TextMessage {
				return
			}
			buffer.Reset()
			_, err = buffer.ReadFrom(reader)
		}
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				cl.normalClose.Store(true)
			}
			return
		}
		data := buffer.Bytes()
		p, err := decodePacket(data)
		if err != nil || !p.validRoute() {
			return
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
		now := time.Now().UnixMilli()
		if p.Timestamp == nil {
			p.Timestamp = &now
		}
		if p.PacketID == "" {
			p.PacketID = cl.server.packetPrefix + strconv.FormatUint(cl.server.nextPacketID.Add(1), 36)
		}
		if reason := cl.admit(p, now); reason != "" {
			cl.drop(p, data, reason)
			continue
		}
		dests := p.Dests
		if len(dests) == 0 {
			dests = []string{p.Dest}
		}
		p.Dests = nil
		for _, dest := range dests {
			p.Dest = dest
			if !cl.server.submit(p, cl) {
				return
			}
		}
	}
}

func (cl *client) admit(p packet, now int64) string {
	if cl.nodeType == "server" {
		return ""
	}
	bucket := now / 5000
	if bucket != cl.rateBucket {
		cl.rateBucket, cl.rateCount = bucket, 0
	}
	if cl.rateCount >= cl.rp5 {
		return "rate_limited"
	}
	cl.rateCount++
	if p.Timestamp != nil && (*p.Timestamp < now-8000 || *p.Timestamp > now) {
		return "invalid_time"
	}
	return ""
}

func (cl *client) drop(p packet, original []byte, reason string) {
	var fields map[string]json.RawMessage
	json.Unmarshal(original, &fields)
	p.Type, p.Payload = "drop", reason
	updated, _ := json.Marshal(p)
	// Merge canonical fields while retaining any extra fields from the input.
	json.Unmarshal(updated, &fields)
	data, _ := json.Marshal(fields)
	if len(data) > 1<<20 {
		cl.close()
		return
	}
	cl.enqueue(outbound{data: data})
}

// A bounded set of workers groups destination-node routing without pinning OS threads.
// Connection lifecycle operations retain their locks; workers never own socket I/O.
func (s *server) startRouting() {
	s.routingOnce.Do(func() {
		go s.heartbeatLoop()
		s.routingQueues = make([]chan *routedPacket, min(runtime.GOMAXPROCS(0), 8))
		for i := range s.routingQueues {
			q := make(chan *routedPacket, 131072)
			s.routingQueues[i] = q
			go func() {
				for {
					select {
					case <-s.routingDone:
						return
					case job := <-q:
						s.routingBytes.Add(-int64(job.bytes))
						if job.client != nil {
							job.client.pendingBytes.Add(-int64(job.bytes))
						}
						s.route(job.packet)
					}
				}
			}()
		}
	})
}

func (s *server) submit(p packet, source *client) bool {
	s.startRouting()
	// Count the expanded destination's retained fields, not the entire fanout list.
	size := int64(len(p.Payload) + len(p.Dest) + len(p.Src) + len(p.SrcNodeID) + len(p.XSrc) + len(p.PacketID) + len(p.Type) + 160)
	if source != nil && source.pendingBytes.Add(size) > 4<<20 {
		source.pendingBytes.Add(-size)
		return false
	}
	if s.routingBytes.Add(size) <= 128<<20 {
		hash := sha256.Sum256([]byte(p.Dest))
		q := s.routingQueues[binary.BigEndian.Uint64(hash[:8])%uint64(len(s.routingQueues))]
		select {
		case <-s.routingDone:
		case q <- &routedPacket{packet: p, bytes: int(size), client: source}:
			return true
		default:
		}
	}
	s.routingBytes.Add(-size)
	if source != nil {
		source.pendingBytes.Add(-size)
	}
	return false
}

// One timer per server schedules all client pings. Socket writes stay in writeLoop.
type pingHeap []*client

func (h pingHeap) Len() int           { return len(h) }
func (h pingHeap) Less(i, j int) bool { return h[i].nextPing.Before(h[j].nextPing) }
func (h pingHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].pingIndex = i
	h[j].pingIndex = j
}
func (h *pingHeap) Push(value any) {
	cl := value.(*client)
	cl.pingIndex = len(*h)
	*h = append(*h, cl)
}
func (h *pingHeap) Pop() any {
	last := len(*h) - 1
	cl := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	cl.pingIndex = -1
	return cl
}

func (s *server) heartbeatLoop() {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		now := time.Now()
		wait := time.Hour
		s.pingMu.Lock()
		for len(s.pings) != 0 && !s.pings[0].nextPing.After(now) {
			cl := s.pings[0]
			cl.nextPing = now.Add(cl.keepAlive)
			heap.Fix(&s.pings, 0)
			select {
			case cl.ping <- struct{}{}:
			default:
			}
		}
		if len(s.pings) != 0 {
			wait = time.Until(s.pings[0].nextPing)
		}
		s.pingMu.Unlock()
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(wait)
		select {
		case <-s.routingDone:
			return
		case <-s.pingWake:
		case <-timer.C:
		}
	}
}

func (cl *client) writeLoop() {
	defer cl.close()
	for {
		select {
		case <-cl.done:
			return
		case msg := <-cl.send:
			cl.queuedBytes.Add(-int64(len(msg.data)))
			cl.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := cl.conn.WriteMessage(websocket.TextMessage, msg.data); err != nil {
				return
			}
			if msg.closeAfter {
				return
			}
		case <-cl.ping:
			cl.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := cl.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (cl *client) queue(msg outbound) bool {
	size := int64(len(msg.data))
	if cl.queuedBytes.Add(size) <= 4<<20 {
		select {
		case <-cl.done:
		case cl.send <- msg:
			return true
		default:
		}
	}
	cl.queuedBytes.Add(-size)
	return false
}

func (cl *client) enqueue(msg outbound) {
	if !cl.queue(msg) {
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
		s.pingMu.Lock()
		if cl.pingIndex >= 0 {
			heap.Remove(&s.pings, cl.pingIndex)
		}
		s.pingMu.Unlock()
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
			recent = append([]receipt(nil), state.recent.entries[state.recent.head:]...)
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
			p := *item.packet
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
	if !utf8.Valid(data) {
		return p, errors.New("invalid UTF-8")
	}
	err := json.Unmarshal(data, &p)
	return p, err
}

// Shared routing envelope limits bound fanout, hashing, and feedback size.
func (p packet) validRoute() bool {
	if p.Type == "" || p.Type == "reconnect" || len(p.Type) > 128 || len(p.PacketID) > 1024 || len(p.Src) > 1024 || len(p.SrcNodeID) > 1024 || len(p.XSrc) > 1024 || len(p.Dest) > 1024 || len(p.Dests) > 1024 {
		return false
	}
	if (p.Dest == "") == (len(p.Dests) == 0) {
		return false
	}
	for _, dest := range p.Dests {
		if dest == "" || len(dest) > 1024 {
			return false
		}
	}
	return true
}

func (s *server) peerToken(id uint64) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	i := sort.Search(len(s.ring), func(i int) bool { return s.ring[i].ID >= id })
	if i == len(s.ring) || s.ring[i].ID != id {
		return "", false
	}
	return s.ring[i].AuthToken, true
}

func (s *server) verifyServerNode(r *http.Request, id uint64) error {
	nodeID, token := requestIdentity(r)
	s.mu.RLock()
	i := sort.Search(len(s.ring), func(i int) bool { return s.ring[i].ID >= id })
	var p peer
	if i < len(s.ring) && s.ring[i].ID == id {
		p = s.ring[i]
	}
	s.mu.RUnlock()
	expected := strings.TrimPrefix(p.Addr, "https://") + "|" + strconv.FormatUint(id, 10)
	if p.Addr == "" || nodeID != expected || subtle.ConstantTimeCompare([]byte(token), []byte(p.AuthToken)) != 1 {
		return httpStatus(401)
	}
	c, err := s.authenticate(r.Context(), nodeID, token, "", r.Header.Get("Origin"))
	if err != nil || c.NodeType != "server" {
		return httpStatus(401)
	}
	return nil
}

// Domain credentials are verified by their HTTPS authority, not locally signed.
type domainIdentity struct {
	NodeID   string `json:"node_id"`
	Token    string `json:"token"`
	XSrcID   string `json:"x_src_id"`
	NodeType string `json:"node_type"`
	RP5      int64  `json:"rp5"`
	MaxC     int    `json:"maxc"`
	Expires  int64  `json:"expires,omitempty"`
}

func authAuthority(raw string) (string, error) {
	if raw == "" || len(raw) > 320 || strings.ContainsAny(raw, "/?#@|\\ \t\r\n") || strings.HasSuffix(raw, ":") {
		return "", errors.New("invalid auth_domain")
	}
	u, err := url.Parse("https://" + strings.ToLower(raw))
	if err != nil || u.Hostname() == "" {
		return "", errors.New("invalid auth_domain")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return "", errors.New("invalid port")
		}
	}
	if net.ParseIP(u.Hostname()) == nil {
		for _, label := range strings.Split(u.Hostname(), ".") {
			if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
				return "", errors.New("invalid domain")
			}
			for _, ch := range label {
				if !(ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '-') {
					return "", errors.New("invalid domain")
				}
			}
		}
	}
	return u.Host, nil
}

func sameServerDomain(domain, current string) bool {
	a, err := url.Parse("https://" + domain)
	b, otherErr := url.Parse("https://" + current)
	if err != nil || otherErr != nil {
		return false
	}
	host, own := a.Hostname(), b.Hostname()
	if host == own {
		return true
	}
	if net.ParseIP(host) != nil || net.ParseIP(own) != nil || own == "localhost" {
		return false
	}
	parent := own
	if _, tail, ok := strings.Cut(own, "."); ok {
		suffix, _ := publicsuffix.PublicSuffix(tail)
		if tail != suffix {
			parent = tail
		}
	}
	suffix, _ := publicsuffix.PublicSuffix(parent)
	return parent != suffix && (host == parent || strings.HasSuffix(host, "."+parent))
}

func authEndpoint(nodeID, token string) (*url.URL, string, error) {
	domain, id, ok := strings.Cut(nodeID, "|")
	if !ok || id == "" || len(nodeID) > 1024 || strings.ContainsAny(id, "|\r\n") {
		return nil, "", errors.New("invalid node_id")
	}
	normalized, err := authAuthority(domain)
	if err != nil || normalized != domain {
		return nil, "", errors.New("invalid auth_domain")
	}
	path, secret, ok := strings.Cut(token, "|")
	if !ok || secret == "" || len(token) > 8192 || !strings.HasPrefix(path, "/") || strings.HasPrefix(path, "//") {
		return nil, "", errors.New("invalid token")
	}
	relative, err := url.Parse(path)
	if err != nil || relative.IsAbs() || relative.Host != "" || relative.Fragment != "" || strings.ContainsAny(path, "\\\r\n") {
		return nil, "", errors.New("invalid auth path")
	}
	u := &url.URL{Scheme: "https", Host: domain, Path: relative.Path, RawPath: relative.RawPath, RawQuery: relative.RawQuery}
	query := u.Query()
	query.Set("node_id", nodeID)
	u.RawQuery = query.Encode()
	return u, secret, nil
}

func requestIdentity(r *http.Request) (string, string) {
	nodeID := r.URL.Query().Get("node_id")
	if nodeID == "" {
		nodeID = r.Header.Get("X-Node-ID")
	}
	token := r.URL.Query().Get("token")
	if token == "" && strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
		token = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	}
	return nodeID, token
}

func (s *server) authenticate(ctx context.Context, nodeID, token, action, origin string) (domainIdentity, error) {
	c := domainIdentity{NodeID: nodeID, Token: token, XSrcID: nodeID, NodeType: "node", RP5: 10, MaxC: 5}
	u, secret, err := authEndpoint(nodeID, token)
	if err != nil {
		return c, err
	}
	query := u.Query()
	if action != "" {
		query.Set("action", action)
	} else {
		query.Del("action")
	}
	u.RawQuery = query.Encode()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	requestBody, _ := json.Marshal(map[string]string{"node_id": nodeID, "secret": secret})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(requestBody))
	if err != nil {
		return c, err
	}
	req.Header.Set("Content-Type", "application/json")
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	client := *s.httpClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return c, errors.New("authorization unavailable")
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8193))
	if err != nil || resp.StatusCode != 200 || len(body) > 8192 {
		return c, errors.New("authorization denied")
	}
	{
		var approval struct {
			Valid   json.RawMessage `json:"valid"`
			XSrcID  string          `json:"x_src_id"`
			RP5     int64           `json:"rp5"`
			MaxC    int             `json:"maxc"`
			Expires int64           `json:"expires"`
			Secret  string          `json:"secret_string"`
		}
		approval.MaxC = 5
		if json.Unmarshal(body, &approval) != nil || (string(approval.Valid) != `"true"` && string(approval.Valid) != "true") || approval.RP5 <= 0 || approval.MaxC <= 0 || len(approval.XSrcID) > 256 || approval.Expires < 0 || (approval.Expires != 0 && approval.Expires <= time.Now().Unix()) {
			return c, errors.New("authorization denied")
		}
		if approval.XSrcID != "" {
			c.XSrcID = approval.XSrcID
		}
		c.RP5 = approval.RP5
		c.MaxC = approval.MaxC
		c.Expires = approval.Expires
		if action == "renew" && approval.Secret != "" {
			path, _, _ := strings.Cut(token, "|")
			c.Token = path + "|" + approval.Secret
			if _, _, err := authEndpoint(nodeID, c.Token); err != nil {
				return c, err
			}
		}
	}
	if sameServerDomain(u.Host, strings.TrimPrefix(s.addr, "https://")) {
		c.NodeType = "server"
	}
	return c, nil
}

func (s *server) peer(w http.ResponseWriter, r *http.Request) error {
	id, err := strconv.ParseUint(r.Header.Get("X-Server-ID"), 10, 64)
	if err != nil || id == s.id {
		return httpStatus(http.StatusUnauthorized)
	}
	if err := s.verifyServerNode(r, id); err != nil {
		return err
	}
	batch := r.Header.Get("X-Peer-Batching") == "1"
	response := http.Header{}
	if batch {
		response.Set("X-Peer-Batching", "1")
	}
	conn, err := s.upgrader.Upgrade(w, r, response)
	if err != nil {
		return nil
	}
	s.addPeerLink(id, conn, batch)
	return nil
}

func (s *server) getPeerLink(owner peer) (*peerLink, error) {
	s.peerMu.RLock()
	if link := s.peerLinks[owner.ID]; link != nil {
		select {
		case <-link.done:
		default:
			s.peerMu.RUnlock()
			return link, nil
		}
	}
	s.peerMu.RUnlock()
	dial, _ := s.peerDials.LoadOrStore(owner.ID, &sync.Mutex{})
	lock := dial.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()
	// Another request may have established this peer while we waited.
	s.peerMu.RLock()
	if link := s.peerLinks[owner.ID]; link != nil {
		select {
		case <-link.done:
		default:
			s.peerMu.RUnlock()
			return link, nil
		}
	}
	s.peerMu.RUnlock()
	address := "ws" + strings.TrimPrefix(owner.Addr, "http") + "/peer"
	headers := http.Header{}
	headers.Set("X-Server-ID", strconv.FormatUint(s.id, 10))
	headers.Set("X-Peer-Batching", "1")
	headers.Set("X-Node-ID", s.serverNodeID)
	s.mu.RLock()
	token := s.serverToken
	s.mu.RUnlock()
	headers.Set("Authorization", "Bearer "+token)
	dialer := &websocket.Dialer{Proxy: http.ProxyFromEnvironment, HandshakeTimeout: 10 * time.Second}
	if transport, ok := s.httpClient.Transport.(*http.Transport); ok {
		dialer.TLSClientConfig = transport.TLSClientConfig
		dialer.NetDialContext = transport.DialContext
	}
	conn, resp, err := dialer.Dial(address, headers)
	if err != nil {
		if resp != nil {
			resp.Body.Close()
		}
		return nil, err
	}
	return s.addPeerLink(owner.ID, conn, resp.Header.Get("X-Peer-Batching") == "1"), nil
}

func (s *server) addPeerLink(peerID uint64, conn *websocket.Conn, batch bool) *peerLink {
	link := &peerLink{batch: batch, server: s, peerID: peerID, conn: conn, ready: make(chan struct{}, 1), done: make(chan struct{})}
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
	go link.readLoop()
	return link
}

func (link *peerLink) enqueue(ctx context.Context, p packet) error {
	frame := peerFrame{Packet: p, Hops: p.hops, Replays: p.replays}
	data, _ := json.Marshal(frame)
	if len(data) > 1<<20+4096 {
		return errors.New("peer packet too large")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	link.mu.Lock()
	select {
	case <-link.done:
		link.mu.Unlock()
		return errors.New("peer link closed")
	default:
	}
	queue := &link.data
	if p.Type == "status" {
		queue = &link.feedback
	}
	if !queue.push(data) {
		link.mu.Unlock()
		return errors.New("peer queue full")
	}
	link.recordLocked(p, time.Now())
	link.mu.Unlock()
	select {
	case link.ready <- struct{}{}:
	default:
	}
	return nil
}

func (link *peerLink) readLoop() {
	defer link.close()
	link.conn.SetReadLimit(2<<20 + 8192)
	link.conn.SetReadDeadline(time.Now().Add(13 * time.Second))
	link.conn.SetPongHandler(func(string) error { return link.conn.SetReadDeadline(time.Now().Add(13 * time.Second)) })
	var buffer bytes.Buffer
	for {
		kind, reader, err := link.conn.NextReader()
		if err != nil || kind != websocket.TextMessage {
			return
		}
		buffer.Reset()
		if _, err := buffer.ReadFrom(reader); err != nil {
			return
		}
		if link.batch && bytes.Count(buffer.Bytes(), []byte{'\n'}) >= 32 {
			return
		}
		for data := buffer.Bytes(); len(data) != 0; {
			raw, rest := data, []byte(nil)
			if link.batch {
				raw, rest, _ = bytes.Cut(data, []byte{'\n'})
			}
			data = rest
			var frame peerFrame
			if len(raw) > 1<<20+4096 || !utf8.Valid(raw) || json.Unmarshal(raw, &frame) != nil || !frame.Packet.validRoute() || len(frame.Packet.Dests) != 0 || frame.Packet.sourceNodeID() == "" || frame.Hops >= 4 || frame.Replays > 3 {
				return
			}
			p := frame.Packet
			if p.Timestamp == nil {
				now := time.Now().UnixMilli()
				p.Timestamp = &now
			}
			if p.PacketID == "" {
				p.PacketID = link.server.packetPrefix + strconv.FormatUint(link.server.nextPacketID.Add(1), 36)
			}
			p.hops = frame.Hops + 1
			p.replays = frame.Replays
			link.mu.Lock()
			link.recordLocked(p, time.Now())
			link.mu.Unlock()
			if !link.server.submit(p, nil) {
				return
			}
		}
	}
}

// Reserve regular turns for data so a continuous stream of feedback cannot starve it.
func (link *peerLink) takeFrame(dataFirst bool) []byte {
	link.mu.Lock()
	defer link.mu.Unlock()
	if dataFirst {
		if data := link.data.pop(); data != nil {
			return data
		}
	}
	if data := link.feedback.pop(); data != nil {
		return data
	}
	return link.data.pop()
}

func (link *peerLink) writeLoop() {
	defer link.close()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	var batchBuffer []byte
	turn := 0
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
			data = link.takeFrame(turn%8 == 0)
			turn++
			if data == nil {
				select {
				case <-link.done:
					return
				case <-ticker.C:
					kind = websocket.PingMessage
				case <-link.ready:
					continue
				}
			}
		}
		// Coalesce already queued frames only: no extra latency or client protocol change.
		if link.batch && kind == websocket.TextMessage {
			for n := 1; n < 32 && len(data) < 64<<10; n++ {
				next := link.takeFrame(n%8 == 0)
				if next == nil {
					break
				}
				if n == 1 {
					data = append(batchBuffer[:0], data...)
				}
				data = append(data, '\n')
				data = append(data, next...)
				batchBuffer = data
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
	link.history = append(link.history, replayRecord{packet: &p, sentAt: now})
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
		link.data, link.feedback = frameQueue{}, frameQueue{}
		link.mu.Unlock()
		if len(replay) != 0 {
			go func() {
				for _, item := range replay {
					p := *item.packet
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
		history[head] = replayRecord{}
		head++
	}
	if head == len(history) {
		return nil, 0
	}
	if head > 1024 && head*2 >= len(history) {
		n := copy(history, history[head:])
		clear(history[n:])
		return history[:n], 0
	}
	return history, head
}
