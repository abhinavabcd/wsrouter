package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type testNode struct {
	id    string
	token string
	conn  *websocket.Conn
}

func startCluster(t *testing.T, count int) ([]*server, []*httptest.Server) {
	t.Helper()
	servers := make([]*server, count)
	urls := make([]*httptest.Server, count)
	for i := range servers {
		cluster := ""
		if i > 0 {
			cluster = urls[0].URL
		}
		servers[i], urls[i] = startTestRouter(t, uint64(math.MaxInt64)/uint64(count)*uint64(i), cluster)
		if i > 0 {
			if err := servers[i].refreshOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	}
	return servers, urls
}

func createNode(t *testing.T, baseURL, xSrc string, rp5 ...int64) testNode {
	t.Helper()
	rate := int64(10)
	if len(rp5) > 0 {
		rate = rp5[0]
	}
	secret := randomTestSecret()
	fixtureAccounts.Store(secret, &fixtureAccount{source: xSrc, rate: rate})
	body, _ := json.Marshal(map[string]any{"auth_domain": strings.TrimPrefix(fixtureAuthURL, "https://"), "path": "/verify", "secret_string": secret})
	resp, err := http.Post(baseURL+"/create", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		NodeID string `json:"node_id"`
		Token  string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || resp.StatusCode != 200 {
		t.Fatalf("create: %s, %v", resp.Status, err)
	}
	fixtureTokens.Store(out.Token, out.NodeID)
	return testNode{id: out.NodeID, token: out.Token}
}

func connectNode(t *testing.T, baseURL, token string, origin string) *websocket.Conn {
	t.Helper()
	headers := http.Header{}
	if origin != "" {
		headers.Set("Origin", origin)
	}
	conn, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(baseURL, "http")+"/connect?"+tokenQuery(token), headers)
	if err != nil {
		if resp != nil {
			t.Fatalf("connect: %v (%s)", err, resp.Status)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func waitConnections(t *testing.T, s *server, nodeID string, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.RLock()
		count := 0
		if state := s.nodes[nodeID]; state != nil {
			state.mu.Lock()
			count = len(state.connections)
			state.mu.Unlock()
		}
		s.mu.RUnlock()
		if count == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %s has %d connections, want %d", nodeID, count, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestJSONAndToken(t *testing.T) {
	binaryPayload := base64.StdEncoding.EncodeToString([]byte{0, 1, 255})
	p := packet{Type: "data", Dest: "destination", Src: "sender", XSrc: "account", Payload: binaryPayload}
	data, err := encodePacket(p)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(data) {
		t.Fatal("packet is not JSON")
	}
	got, err := decodePacket(data)
	if err != nil || !reflect.DeepEqual(got, p) {
		t.Fatalf("packet round trip: %+v, %v", got, err)
	}
	if _, err := decodePacket(data[:10]); err == nil {
		t.Fatal("truncated JSON accepted")
	}
	batch, err := encodePacket(packet{Type: "data", Dests: []string{"one", "two"}, Payload: "hello"})
	if err != nil || !bytes.Contains(batch, []byte(`"dest_node_ids"`)) || bytes.Contains(batch, []byte(`"dest_ids"`)) {
		t.Fatalf("wrong multi-destination field: %s, %v", batch, err)
	}
}

func TestMessageTimestampAcrossServers(t *testing.T) {
	servers, urls := startCluster(t, 2)
	source := createNode(t, urls[0].URL, "source")
	sourceOwner, _ := servers[0].owner(source.id)
	var dest testNode
	var destOwner peer
	for i := 0; i < 100; i++ {
		dest = createNode(t, urls[0].URL, "dest")
		destOwner, _ = servers[0].owner(dest.id)
		if destOwner.ID != sourceOwner.ID {
			break
		}
	}
	if destOwner.ID == sourceOwner.ID {
		t.Fatal("could not place destination on another server")
	}
	serverNode := serverTestNode(servers[0])
	serverOwner, _ := servers[0].owner(serverNode.id)
	nodeConn := connectNode(t, sourceOwner.Addr, source.token, "")
	serverConn := connectNode(t, serverOwner.Addr, serverNode.token, "")
	destConn := connectNode(t, destOwner.Addr, dest.token, "")
	for _, s := range servers {
		if s.id == sourceOwner.ID {
			waitConnections(t, s, source.id, 1)
		} else {
			waitConnections(t, s, dest.id, 1)
		}
	}
	for _, nodeType := range []string{"node", "server"} {
		for _, supplied := range []string{"missing", "zero", "existing", "valid", "future", "missing-again"} {
			t.Run(nodeType+"/"+supplied, func(t *testing.T) {
				p := packet{PacketID: nodeType + "-" + supplied, Type: "data", Dest: dest.id, Payload: "hello"}
				if !strings.HasPrefix(supplied, "missing") {
					value := int64(0)
					if supplied == "existing" {
						value = 1700000000123
					}
					if supplied == "valid" {
						value = time.Now().UnixMilli() - 1000
					}
					if supplied == "future" {
						value = time.Now().UnixMilli() + 60000
					}
					p.Timestamp = &value
				}
				conn := nodeConn
				if nodeType == "server" {
					conn = serverConn
					p.Src, p.XSrc = source.id, "source"
				}
				data, _ := encodePacket(p)
				before := time.Now().UnixMilli()
				if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
					t.Fatal(err)
				}
				if nodeType == "node" && (supplied == "zero" || supplied == "existing" || supplied == "future") {
					nodeConn.SetReadDeadline(time.Now().Add(3 * time.Second))
					_, raw, err := nodeConn.ReadMessage()
					if err != nil {
						t.Fatal(err)
					}
					got, err := decodePacket(raw)
					if err != nil || got.Type != "drop" || got.Payload != "invalid_time" || got.PacketID != p.PacketID || got.Timestamp == nil || *got.Timestamp != *p.Timestamp || got.Dest != dest.id {
						t.Fatalf("invalid timestamp was not echoed as a drop: %+v, %v", got, err)
					}
					return
				}
				destConn.SetReadDeadline(time.Now().Add(3 * time.Second))
				_, raw, err := destConn.ReadMessage()
				if err != nil {
					t.Fatal(err)
				}
				got, err := decodePacket(raw)
				if err != nil || got.Timestamp == nil || got.PacketID != p.PacketID {
					t.Fatalf("missing timestamp or wrong packet: %+v, %v", got, err)
				}
				if p.Timestamp != nil {
					if *got.Timestamp != *p.Timestamp {
						t.Fatalf("server timestamp changed: got %d, want %d", *got.Timestamp, *p.Timestamp)
					}
				} else if *got.Timestamp < before || *got.Timestamp > time.Now().UnixMilli() {
					t.Fatalf("timestamp %d is not the server's receive time", *got.Timestamp)
				}
				for _, feedbackConn := range []*websocket.Conn{nodeConn} {
					feedbackConn.SetReadDeadline(time.Now().Add(3 * time.Second))
					_, raw, err := feedbackConn.ReadMessage()
					if err != nil {
						t.Fatal(err)
					}
					feedback, err := decodePacket(raw)
					if err != nil || feedback.Type != "status" || feedback.PacketID != p.PacketID || feedback.Payload != "success" {
						t.Fatalf("wrong timestamp feedback: %+v, %v", feedback, err)
					}
				}
			})
		}
	}
}

func TestTwoServerRoutingKeysAndCORS(t *testing.T) {
	servers, httpServers := startCluster(t, 2)
	leader, follower := servers[0], servers[1]
	leaderURL, followerURL := httpServers[0].URL, httpServers[1].URL
	source := createNode(t, followerURL, "source-account")
	for owner, _ := leader.owner(source.id); owner.Addr != leaderURL; owner, _ = leader.owner(source.id) {
		source = createNode(t, followerURL, "source-account")
	}
	dest := createNode(t, leaderURL, "dest-account")
	for owner, _ := leader.owner(dest.id); owner.Addr != followerURL; owner, _ = leader.owner(dest.id) {
		dest = createNode(t, leaderURL, "dest-account")
	}
	if _, err := leader.authenticate(context.Background(), source.id, source.token, "", ""); err != nil {
		t.Fatalf("leader rejected follower-issued token: %v", err)
	}
	if _, err := follower.authenticate(context.Background(), dest.id, dest.token, "", ""); err != nil {
		t.Fatalf("follower rejected leader-issued token: %v", err)
	}
	if leader.ring[1].AuthToken != follower.serverToken || follower.ring[0].AuthToken != leader.serverToken {
		t.Fatal("server tokens not broadcast")
	}
	locateReq, _ := http.NewRequest(http.MethodGet, leaderURL+"/locate", nil)
	locateReq.Header.Set("Origin", "https://other.example")
	locateReq.Header.Set("Authorization", "Bearer "+dest.token)
	locateReq.Header.Set("X-Node-ID", dest.id)
	locateResp, err := http.DefaultClient.Do(locateReq)
	if err != nil {
		t.Fatal(err)
	}
	var location map[string]string
	if err := json.NewDecoder(locateResp.Body).Decode(&location); err != nil {
		t.Fatal(err)
	}
	locateResp.Body.Close()
	if locateResp.StatusCode != http.StatusOK || locateResp.Header.Get("Access-Control-Allow-Origin") != "*" || location["address"] != followerURL {
		t.Fatalf("locate: %s, %+v", locateResp.Status, location)
	}

	req, _ := http.NewRequest(http.MethodOptions, leaderURL+"/create", nil)
	req.Header.Set("Origin", "https://other.example")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent || resp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("CORS preflight: %s, %s", resp.Status, resp.Header.Get("Access-Control-Allow-Origin"))
	}

	_, wrong, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(leaderURL, "http")+"/connect?"+tokenQuery(dest.token), nil)
	if err == nil || wrong == nil || wrong.StatusCode != http.StatusMisdirectedRequest {
		t.Fatalf("expected 421 redirect, got %v, %v", wrong, err)
	}
	var redirect map[string]string
	if err := json.NewDecoder(wrong.Body).Decode(&redirect); err != nil {
		t.Fatal(err)
	}
	wrong.Body.Close()
	if redirect["address"] != followerURL {
		t.Fatalf("wrong redirect: %+v", redirect)
	}

	src := connectNode(t, leaderURL, source.token, "https://other.example")
	dst := connectNode(t, followerURL, dest.token, "https://another.example")
	dst2 := connectNode(t, followerURL, dest.token, "")
	waitConnections(t, leader, source.id, 1)
	waitConnections(t, follower, dest.id, 2)
	data, _ := encodePacket(packet{Type: "data", Dest: dest.id, Payload: base64.StdEncoding.EncodeToString([]byte{0, 1, 255})})
	if err := src.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatal(err)
	}
	dst.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, received, err := dst.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodePacket(received)
	if err != nil || got.Src != source.id || got.SrcNodeID != source.id || got.XSrc != "source-account" || got.Dest != dest.id || got.Type != "data" || got.Payload != base64.StdEncoding.EncodeToString([]byte{0, 1, 255}) {
		t.Fatalf("wrong forwarded packet: %+v, %v", got, err)
	}
	dst2.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, second, err := dst2.ReadMessage()
	if err != nil || !bytes.Equal(second, received) {
		t.Fatalf("second connection missed packet: %v", err)
	}
	if follower.nodeCount.Load() != 1 {
		t.Fatalf("follower tracks %d nodes", follower.nodeCount.Load())
	}
	src.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, statusData, err := src.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	sent, err := decodePacket(statusData)
	if err != nil || sent.Type != "status" || sent.Payload != "success" || sent.PacketID == "" || sent.PacketID != got.PacketID || sent.Src != dest.id || sent.SrcNodeID != dest.id || sent.Dest != source.id {
		t.Fatalf("wrong sent status: %+v, %v", sent, err)
	}

	local, _ := encodePacket(packet{Type: "local", Dest: source.id, Payload: "self"})
	if err := src.WriteMessage(websocket.TextMessage, local); err != nil {
		t.Fatal(err)
	}
	src.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, localData, err := src.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	localPacket, err := decodePacket(localData)
	if err != nil || localPacket.Type != "local" || localPacket.Src != source.id || localPacket.Payload != "self" {
		t.Fatalf("wrong local packet: %+v, %v", localPacket, err)
	}
	_, localStatusData, err := src.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	localStatus, err := decodePacket(localStatusData)
	if err != nil || localStatus.Type != "status" || localStatus.Payload != "success" || localStatus.PacketID != localPacket.PacketID {
		t.Fatalf("wrong local status: %+v, %v", localStatus, err)
	}

	forged, _ := encodePacket(packet{Type: "data", Dest: dest.id, Src: "forged"})
	if err := src.WriteMessage(websocket.TextMessage, forged); err != nil {
		t.Fatal(err)
	}
	src.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := src.ReadMessage(); err == nil {
		t.Fatal("forged sender did not close connection")
	}
}

func TestRenewOnDifferentServer(t *testing.T) {
	servers, urls := startCluster(t, 2)
	node := createNode(t, urls[0].URL, "account")
	owner, _ := servers[0].owner(node.id)
	ownerIndex := 0
	if owner.ID == servers[1].id {
		ownerIndex = 1
	}
	first := connectNode(t, owner.Addr, node.token, "")
	second := connectNode(t, owner.Addr, node.token, "")
	waitConnections(t, servers[ownerIndex], node.id, 2)
	req, _ := http.NewRequest(http.MethodPost, urls[1-ownerIndex].URL+"/renew", strings.NewReader(`{"valid_for":321}`))
	req.Header.Set("Authorization", "Bearer "+node.token)
	req.Header.Set("X-Node-ID", node.id)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("renew returned %s", resp.Status)
	}
	var renewed struct {
		NodeID string `json:"node_id"`
		Token  string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&renewed); err != nil {
		t.Fatal(err)
	}
	if renewed.NodeID != node.id || renewed.Token != node.token {
		t.Fatal("unexpected renewal identity")
	}
	waitConnections(t, servers[ownerIndex], node.id, 2)
	newConn := connectNode(t, owner.Addr, renewed.Token, "")
	waitConnections(t, servers[ownerIndex], node.id, 3)
	data, _ := encodePacket(packet{Type: "data", Dest: node.id, Payload: "reconnected"})
	if err := newConn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatal(err)
	}
	for _, conn := range []*websocket.Conn{first, second} {
		conn.SetReadDeadline(time.Now().Add(time.Second))
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("renewal at another server closed the owner's socket: %v", err)
		}
		got, err := decodePacket(raw)
		if err != nil || got.Payload != "reconnected" {
			t.Fatalf("existing socket delivery: %+v, %v", got, err)
		}
	}

	newConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, raw, err := newConn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	delivered, err := decodePacket(raw)
	if err != nil || delivered.Payload != "reconnected" || delivered.SrcNodeID != node.id {
		t.Fatalf("renewed token did not reconnect: %+v, %v", delivered, err)
	}
}

func TestRingChangeRequestsReconnect(t *testing.T) {
	_, urls := startCluster(t, 1)
	var node testNode
	for i := 0; i < 1000; i++ {
		node = createNode(t, urls[0].URL, "account")
		if ownerIn([]peer{{ID: 0}, {ID: 1 << 62}}, node.id).ID == 1<<62 {
			break
		}
	}
	if ownerIn([]peer{{ID: 0}, {ID: 1 << 62}}, node.id).ID != 1<<62 {
		t.Fatal("could not find follower-owned node")
	}
	conn := connectNode(t, urls[0].URL, node.token, "")
	follower, ts := startTestRouter(t, 1<<62, urls[0].URL)
	if err := follower.refreshOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	p, err := decodePacket(data)
	if err != nil || p.Type != "reconnect" || p.Payload != ts.URL {
		t.Fatalf("wrong reconnect packet: %+v, %v", p, err)
	}
}

func TestStressFiveServers(t *testing.T) {
	if os.Getenv("WSROUTER_STRESS") != "1" {
		t.Skip("set WSROUTER_STRESS=1 to run the all-to-all stress test")
	}
	count := 1000
	if raw := os.Getenv("WSROUTER_STRESS_NODES"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 2 || n%5 != 0 {
			t.Fatal("WSROUTER_STRESS_NODES must be a multiple of 5 and at least 5")
		}
		count = n
	}
	servers, urls := startCluster(t, 5)
	nodes := make([]testNode, count)
	ids := make([]string, count)
	idIndex := make(map[string]int, count)
	for i := range nodes {
		node := createNode(t, urls[i%5].URL, fmt.Sprintf("account-%d", i), int64(count))
		ids[i], nodes[i].id, nodes[i].token = node.id, node.id, node.token
		idIndex[node.id] = i
	}
	for i := range nodes {
		owner, ok := servers[0].owner(nodes[i].id)
		if !ok {
			t.Fatal("ring unavailable")
		}
		nodes[i].conn = connectNode(t, owner.Addr, nodes[i].token, "https://stress.example")
	}
	for i := range nodes {
		owner, _ := servers[0].owner(nodes[i].id)
		for _, s := range servers {
			if s.addr == owner.Addr {
				waitConnections(t, s, nodes[i].id, 1)
				break
			}
		}
	}
	for i, s := range servers {
		if s.nodeCount.Load() == 0 {
			t.Fatalf("server %d has no connected nodes", i)
		}
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	t.Logf("idle: heap=%.1f MiB goroutines=%d", float64(before.HeapAlloc)/(1<<20), runtime.NumGoroutine())
	start := time.Now()
	var wg sync.WaitGroup
	var receivedData, receivedStatus atomic.Int64
	failures := make(chan error, 1)
	var closeOnce sync.Once
	closeAll := func() {
		closeOnce.Do(func() {
			for i := range nodes {
				nodes[i].conn.Close()
			}
		})
	}
	fail := func(err error) {
		select {
		case failures <- err:
			closeAll()
		default:
		}
	}
	for i := range nodes {
		i := i
		wg.Add(2)
		go func() {
			defer wg.Done()
			seenData := make([]string, count)
			seenStatus := make([]string, count)
			nodes[i].conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
			for received := 0; received < 2*(count-1); {
				kind, data, err := nodes[i].conn.ReadMessage()
				if err != nil {
					fail(fmt.Errorf("node %d read %d/%d: %w", i, received, 2*(count-1), err))
					return
				}
				p, err := decodePacket(data)
				other, exists := idIndex[p.SrcNodeID]
				if err != nil || kind != websocket.TextMessage || !exists || p.SrcNodeID != p.Src || other == i || p.Dest != ids[i] || p.PacketID == "" {
					fail(fmt.Errorf("node %d invalid packet after %d: %+v, %v", i, received, p, err))
					return
				}
				switch p.Type {
				case "data":
					if p.XSrc != fmt.Sprintf("account-%d", other) || p.Payload != strconv.Itoa(other) || (seenData[other] != "" && seenData[other] != p.PacketID) {
						fail(fmt.Errorf("node %d invalid data from %d: %+v", i, other, p))
						return
					}
					if seenData[other] == "" {
						seenData[other] = p.PacketID
						receivedData.Add(1)
						received++
					}
				case "status":
					if p.Payload != "success" || (seenStatus[other] != "" && seenStatus[other] != p.PacketID) {
						fail(fmt.Errorf("node %d invalid status for %d: %+v", i, other, p))
						return
					}
					if seenStatus[other] == "" {
						seenStatus[other] = p.PacketID
						receivedStatus.Add(1)
						received++
					}
				default:
					fail(fmt.Errorf("node %d unexpected packet: %+v", i, p))
					return
				}
			}
			// Keep reading ping frames until the other nodes finish their traffic.
			go func(conn *websocket.Conn) {
				for {
					if _, _, err := conn.ReadMessage(); err != nil {
						return
					}
				}
			}(nodes[i].conn)
		}()
		go func() {
			defer wg.Done()
			order := rand.New(rand.NewSource(int64(i + 1))).Perm(count)
			if i%2 == 0 {
				dests := make([]string, 0, count-1)
				for _, dest := range order {
					if dest != i {
						dests = append(dests, ids[dest])
					}
				}
				data, _ := encodePacket(packet{Type: "data", Dests: dests, Payload: strconv.Itoa(i)})
				nodes[i].conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
				if err := nodes[i].conn.WriteMessage(websocket.TextMessage, data); err != nil {
					fail(fmt.Errorf("node %d sending batch: %w", i, err))
				}
				return
			}
			for _, dest := range order {
				if dest == i {
					continue
				}
				data, _ := encodePacket(packet{Type: "data", Dest: ids[dest], Payload: strconv.Itoa(i)})
				nodes[i].conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
				if err := nodes[i].conn.WriteMessage(websocket.TextMessage, data); err != nil {
					fail(fmt.Errorf("node %d sending to %d: %w", i, dest, err))
					return
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	deadline := time.NewTimer(5 * time.Minute)
	defer deadline.Stop()
	for {
		select {
		case err := <-failures:
			t.Fatal(err)
		case <-done:
			elapsed := time.Since(start)
			var after runtime.MemStats
			runtime.ReadMemStats(&after)
			t.Logf("load: alloc=%.1f MiB heap=%.1f MiB gc=%d", float64(after.TotalAlloc-before.TotalAlloc)/(1<<20), float64(after.HeapAlloc)/(1<<20), after.NumGC-before.NumGC)
			for _, s := range servers {
				s.cleanupReceipts(time.Now().Add(31 * time.Second))
			}
			t.Logf("delivered %d data packets and %d success statuses across five servers in %s", receivedData.Load(), receivedStatus.Load(), elapsed)
			return
		case <-ticker.C:
			var routes, statuses uint64
			var queuedData, queuedFeedback, queuedIncoming int
			for _, s := range servers {
				routes += s.routeErrors.Load()
				statuses += s.statusErrors.Load()
				s.peerMu.Lock()
				for link := range s.allPeerLinks {
					link.mu.Lock()
					queuedFeedback += len(link.feedback.entries) - link.feedback.head
					queuedData += len(link.data.entries) - link.data.head
					link.mu.Unlock()

				}
				s.peerMu.Unlock()
			}
			t.Logf("progress: data=%d status=%d route_errors=%d status_errors=%d peer_queues=%d/%d/%d", receivedData.Load(), receivedStatus.Load(), routes, statuses, queuedData, queuedFeedback, queuedIncoming)
		case <-deadline.C:
			closeAll()
			t.Fatal("stress test timed out before all messages arrived")
		}
	}
}

func TestJoinBroadcastsTokenBeforeForwarding(t *testing.T) {
	servers, urls := startCluster(t, 2)
	existing := servers[1]
	joining, ts := startTestRouter(t, 2*uint64(math.MaxInt64/3), urls[0].URL)
	if err := joining.refreshOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	token, ok := existing.peerToken(joining.id)
	if !ok || token != joining.serverToken {
		t.Fatal("existing server did not receive joining server token")
	}
	source := createNode(t, ts.URL, "new-account")
	for owner, _ := joining.owner(source.id); owner.Addr != ts.URL; owner, _ = joining.owner(source.id) {
		source = createNode(t, ts.URL, "new-account")
	}
	dest := createNode(t, urls[1].URL, "existing-account")
	for owner, _ := joining.owner(dest.id); owner.Addr != urls[1].URL; owner, _ = joining.owner(dest.id) {
		dest = createNode(t, urls[1].URL, "existing-account")
	}
	srcConn := connectNode(t, ts.URL, source.token, "")
	dstConn := connectNode(t, urls[1].URL, dest.token, "")
	data, _ := encodePacket(packet{Type: "join", Dest: dest.id, Payload: "immediate"})
	if err := srcConn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatal(err)
	}
	dstConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, received, err := dstConn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodePacket(received)
	if err != nil || got.Src != source.id || got.SrcNodeID != source.id || got.XSrc != "new-account" || got.Payload != "immediate" {
		t.Fatalf("wrong packet after join: %+v, %v", got, err)
	}
	srcConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, statusData, err := srcConn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	sent, err := decodePacket(statusData)
	if err != nil || sent.Type != "status" || sent.Payload != "success" || sent.PacketID != got.PacketID {
		t.Fatalf("wrong join status: %+v, %v", sent, err)
	}
}

func TestFailedStatusAfterLastConnectionDrops(t *testing.T) {
	servers, urls := startCluster(t, 1)
	source := createNode(t, urls[0].URL, "source")
	dest := createNode(t, urls[0].URL, "dest")
	srcConn := connectNode(t, urls[0].URL, source.token, "")
	first := connectNode(t, urls[0].URL, dest.token, "")
	second := connectNode(t, urls[0].URL, dest.token, "")
	waitConnections(t, servers[0], dest.id, 2)
	data, _ := encodePacket(packet{Type: "data", Dest: dest.id, Payload: "hello"})
	if err := srcConn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatal(err)
	}
	for _, conn := range []*websocket.Conn{first, second} {
		conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, received, err := conn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		p, err := decodePacket(received)
		if err != nil || p.Type != "data" || p.PacketID == "" {
			t.Fatalf("wrong delivered packet: %+v, %v", p, err)
		}
	}
	srcConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, sentData, err := srcConn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	sent, err := decodePacket(sentData)
	if err != nil || sent.Type != "status" || sent.Payload != "success" || sent.PacketID == "" {
		t.Fatalf("wrong sent status: %+v, %v", sent, err)
	}
	if err := first.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		servers[0].mu.RLock()
		state := servers[0].nodes[dest.id]
		remaining := 0
		if state != nil {
			state.mu.Lock()
			remaining = len(state.connections)
			state.mu.Unlock()
		}
		servers[0].mu.RUnlock()
		if remaining == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first connection was not removed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	servers[0].mu.RLock()
	state := servers[0].nodes[dest.id]
	tracked := false
	if state != nil {
		state.mu.Lock()
		tracked = len(state.recent.entries)-state.recent.head == 1
		state.mu.Unlock()
	}
	servers[0].mu.RUnlock()
	if !tracked {
		t.Fatal("receipt was discarded while one destination connection remained")
	}
	second.Close()
	_, failedData, err := srcConn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	failed, err := decodePacket(failedData)
	if err != nil || failed.Type != "status" || failed.Payload != "failed" || failed.PacketID != sent.PacketID || failed.Src != dest.id || failed.SrcNodeID != dest.id || failed.Dest != source.id {
		t.Fatalf("wrong failed status: %+v, %v", failed, err)
	}
	srcConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if _, extra, err := srcConn.ReadMessage(); err == nil {
		t.Fatalf("disconnect emitted duplicate feedback: %s", extra)
	}
}

func TestDisconnectReroutesToNewOwnerBeforeFeedback(t *testing.T) {
	servers, urls := startCluster(t, 2)
	var source, dest testNode
	for i := 0; i < 100 && (source.id == "" || dest.id == ""); i++ {
		node := createNode(t, urls[1].URL, "account")
		owner, _ := servers[0].owner(node.id)
		if owner.ID == servers[1].id && source.id == "" {
			source = node
		} else if owner.ID == servers[0].id && dest.id == "" {
			dest = node
		}
	}
	if source.id == "" || dest.id == "" {
		t.Fatal("could not place source and destination on different owners")
	}
	srcConn := connectNode(t, urls[1].URL, source.token, "")
	oldConn := connectNode(t, urls[0].URL, dest.token, "")
	waitConnections(t, servers[1], source.id, 1)
	waitConnections(t, servers[0], dest.id, 1)
	data, _ := encodePacket(packet{PacketID: "migrating-message", Type: "data", Dest: dest.id, Payload: "retry"})
	if err := srcConn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatal(err)
	}
	for _, conn := range []*websocket.Conn{oldConn, srcConn} {
		conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		if _, _, err := conn.ReadMessage(); err != nil {
			t.Fatal(err)
		}
	}
	// Change ownership without initiating a reconnect, to exercise abrupt close.
	newRing := []peer{{ID: servers[1].id, Addr: urls[1].URL, AuthToken: servers[1].serverToken}}
	servers[1].mu.Lock()
	servers[1].ring = newRing
	servers[1].routingRing.Store(newRing)
	servers[1].mu.Unlock()
	newConn := connectNode(t, urls[1].URL, dest.token, "")
	waitConnections(t, servers[1], dest.id, 1)
	servers[0].mu.Lock()
	servers[0].ring = newRing
	servers[0].routingRing.Store(newRing)
	servers[0].mu.Unlock()
	oldConn.Close()
	newConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, replayData, err := newConn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := decodePacket(replayData)
	if err != nil || replayed.PacketID != "migrating-message" || replayed.Src != source.id || replayed.SrcNodeID != source.id || replayed.Dest != dest.id || replayed.Payload != "retry" {
		t.Fatalf("wrong migrated replay: %+v, %v", replayed, err)
	}
	srcConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, feedbackData, err := srcConn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	feedback, err := decodePacket(feedbackData)
	if err != nil || feedback.Type != "status" || feedback.Payload != "success" || feedback.PacketID != "migrating-message" {
		t.Fatalf("disconnect failed before rerouting: %+v, %v", feedback, err)
	}
}

func TestOfflineDestinationGetsFailedStatus(t *testing.T) {
	_, urls := startCluster(t, 1)
	source := createNode(t, urls[0].URL, "source")
	dest := createNode(t, urls[0].URL, "offline")
	srcConn := connectNode(t, urls[0].URL, source.token, "")
	data, _ := encodePacket(packet{PacketID: "offline-message", Type: "data", Dest: dest.id, Payload: "hello"})
	if err := srcConn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatal(err)
	}
	srcConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, statusData, err := srcConn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	status, err := decodePacket(statusData)
	if err != nil || status.Type != "status" || status.Payload != "failed" || status.PacketID != "offline-message" || status.Src != dest.id || status.SrcNodeID != dest.id || status.Dest != source.id {
		t.Fatalf("wrong offline status: %+v, %v", status, err)
	}
}

func TestReceiptExpiresAfterTenSeconds(t *testing.T) {
	servers, urls := startCluster(t, 1)
	source := createNode(t, urls[0].URL, "source")
	dest := createNode(t, urls[0].URL, "dest")
	srcConn := connectNode(t, urls[0].URL, source.token, "")
	dstConn := connectNode(t, urls[0].URL, dest.token, "")
	data, _ := encodePacket(packet{Type: "data", Dest: dest.id, Payload: "hello"})
	if err := srcConn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatal(err)
	}
	dstConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := dstConn.ReadMessage(); err != nil {
		t.Fatal(err)
	}
	srcConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := srcConn.ReadMessage(); err != nil {
		t.Fatal(err)
	}
	servers[0].mu.RLock()
	state := servers[0].nodes[dest.id]
	if state == nil {
		servers[0].mu.RUnlock()
		t.Fatal("missing node state")
	}
	state.mu.Lock()
	q := &state.recent
	if len(q.entries)-q.head != 1 {
		state.mu.Unlock()
		servers[0].mu.RUnlock()
		t.Fatal("missing receipt")
	}
	q.entries[q.head].sentAt = time.Now().Add(-11 * time.Second)
	state.mu.Unlock()
	servers[0].mu.RUnlock()
	servers[0].cleanupReceipts(time.Now())
	_, retained := servers[0].recent.Load(dest.id)
	if retained {
		t.Fatal("expired receipt remained in queue")
	}
	if err := dstConn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	srcConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if _, _, err := srcConn.ReadMessage(); err == nil {
		t.Fatal("expired receipt emitted a failed status")
	}
}

func TestUnresponsiveWebSocketClosesWithinTenSeconds(t *testing.T) {
	servers, urls := startCluster(t, 1)
	servers[0].keepAlive = 100 * time.Millisecond
	node := createNode(t, urls[0].URL, "idle")
	connectNode(t, urls[0].URL, node.token, "")
	joined := time.Now().Add(time.Second)
	for servers[0].nodeCount.Load() != 1 {
		if time.Now().After(joined) {
			t.Fatal("WebSocket was not tracked")
		}
		time.Sleep(time.Millisecond)
	}
	deadline := time.Now().Add(12 * time.Second)
	for servers[0].nodeCount.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("unresponsive WebSocket remained connected after 10 seconds")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestDomainServerRouting(t *testing.T) {
	servers, urls := startCluster(t, 2)
	defaultNode := createNode(t, urls[0].URL, "default")
	serverNode := serverTestNode(servers[0])
	dest := createNode(t, urls[1].URL, "dest")
	ownerURL := func(nodeID string) string {
		t.Helper()
		owner, ok := servers[0].owner(nodeID)
		if !ok {
			t.Fatal("ring unavailable")
		}
		return owner.Addr
	}
	sourceConn := connectNode(t, ownerURL(defaultNode.id), defaultNode.token, "")
	destConn := connectNode(t, ownerURL(dest.id), dest.token, "")
	serverConn := connectNode(t, ownerURL(serverNode.id), serverNode.token, "")
	for _, node := range []testNode{defaultNode, dest, serverNode} {
		for _, s := range servers {
			if s.addr == ownerURL(node.id) {
				waitConnections(t, s, node.id, 1)
			}
		}
	}
	data, _ := encodePacket(packet{PacketID: "server-owned-1", Type: "data", Dest: dest.id, SrcNodeID: defaultNode.id, XSrc: "delegated-account", Payload: "hello"})
	if err := serverConn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatal(err)
	}
	destConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, deliveredData, err := destConn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	delivered, err := decodePacket(deliveredData)
	if err != nil || delivered.PacketID != "server-owned-1" || delivered.Src != "" || delivered.SrcNodeID != defaultNode.id || delivered.XSrc != "delegated-account" || delivered.Dest != dest.id {
		t.Fatalf("server packet was changed: %+v, %v", delivered, err)
	}
	sourceConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, statusData, err := sourceConn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	status, err := decodePacket(statusData)
	if err != nil || status.PacketID != "server-owned-1" || status.Type != "status" || status.Payload != "success" || status.Dest != defaultNode.id || status.SrcNodeID != dest.id {
		t.Fatalf("wrong delegated status: %+v, %v", status, err)
	}
	manual, _ := encodePacket(packet{PacketID: "manual-status", Type: "status", Dest: dest.id, Src: defaultNode.id, SrcNodeID: "preserved-node", XSrc: "delegated-account", Payload: "custom"})
	if err := serverConn.WriteMessage(websocket.TextMessage, manual); err != nil {
		t.Fatal(err)
	}
	destConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, manualData, err := destConn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodePacket(manualData)
	if err != nil || got.PacketID != "manual-status" || got.Type != "status" || got.Src != defaultNode.id || got.SrcNodeID != "preserved-node" || got.XSrc != "delegated-account" || got.Payload != "custom" {
		t.Fatalf("server status was changed: %+v, %v", got, err)
	}
}

func TestConfiguredKeepAliveSendsPing(t *testing.T) {
	servers, urls := startCluster(t, 1)
	servers[0].keepAlive = 100 * time.Millisecond
	node := createNode(t, urls[0].URL, "ping")
	conn := connectNode(t, urls[0].URL, node.token, "")
	ping := make(chan struct{}, 1)
	conn.SetPingHandler(func(string) error {
		select {
		case ping <- struct{}{}:
		default:
		}
		return conn.WriteControl(websocket.PongMessage, nil, time.Now().Add(time.Second))
	})
	go conn.ReadMessage()
	select {
	case <-ping:
	case <-time.After(2 * time.Second):
		t.Fatal("configured keepalive did not send a ping")
	}
}

func TestMultipleDestinationsAcrossServers(t *testing.T) {
	servers, urls := startCluster(t, 2)
	source := createNode(t, urls[0].URL, "source")
	sourceOwner, _ := servers[0].owner(source.id)
	var local, remote testNode
	for i := 0; i < 100 && (local.id == "" || remote.id == ""); i++ {
		node := createNode(t, urls[i%2].URL, "destination")
		owner, _ := servers[0].owner(node.id)
		if owner.Addr == sourceOwner.Addr && local.id == "" {
			local = node
		} else if owner.Addr != sourceOwner.Addr && remote.id == "" {
			remote = node
		}
	}
	if local.id == "" || remote.id == "" {
		t.Fatal("could not place destinations on both servers")
	}
	offline := createNode(t, urls[1].URL, "offline")
	srcConn := connectNode(t, sourceOwner.Addr, source.token, "")
	localConn := connectNode(t, sourceOwner.Addr, local.token, "")
	remoteOwner, _ := servers[0].owner(remote.id)
	remoteConn := connectNode(t, remoteOwner.Addr, remote.token, "")
	for _, target := range []struct {
		owner peer
		id    string
	}{{sourceOwner, source.id}, {sourceOwner, local.id}, {remoteOwner, remote.id}} {
		index := 0
		if target.owner.Addr == urls[1].URL {
			index = 1
		}
		waitConnections(t, servers[index], target.id, 1)
	}
	data, err := encodePacket(packet{Type: "data", Dests: []string{local.id, remote.id, offline.id}, Payload: "fanout"})
	if err != nil {
		t.Fatal(err)
	}
	if err := srcConn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatal(err)
	}
	var packetID string
	for _, target := range []struct {
		id   string
		conn *websocket.Conn
	}{{local.id, localConn}, {remote.id, remoteConn}} {
		target.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, deliveredData, err := target.conn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodePacket(deliveredData)
		if err != nil || got.Type != "data" || got.Dest != target.id || len(got.Dests) != 0 || got.Src != source.id || got.SrcNodeID != source.id || got.XSrc != "source" || got.Payload != "fanout" || got.PacketID == "" {
			t.Fatalf("bad fanout delivery: %+v, %v", got, err)
		}
		if packetID != "" && got.PacketID != packetID {
			t.Fatalf("fanout used different packet IDs: %q, %q", packetID, got.PacketID)
		}
		packetID = got.PacketID
	}
	seen := make(map[string]bool)
	for range 3 {
		srcConn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, statusData, err := srcConn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		status, err := decodePacket(statusData)
		wantPayload := "success"
		if status.Src == offline.id {
			wantPayload = "failed"
		}
		if err != nil || status.Type != "status" || status.SrcNodeID != status.Src || status.PacketID != packetID || status.Dest != source.id || status.Payload != wantPayload || seen[status.Src] || (status.Src != local.id && status.Src != remote.id && status.Src != offline.id) {
			t.Fatalf("bad fanout status: %+v, %v", status, err)
		}
		seen[status.Src] = true
	}
	if len(seen) != 3 {
		t.Fatalf("only %d destinations returned status", len(seen))
	}
}

func TestInvalidDestinationListsCloseConnection(t *testing.T) {
	_, urls := startCluster(t, 1)
	cases := []packet{
		{Type: "data", Dest: "one", Dests: []string{"two"}},
		{Type: "data", Dests: []string{}},
		{Type: "data", Dests: []string{"one", ""}},
		{Type: "data", Dests: make([]string, 1025)},
	}
	for i, p := range cases {
		node := createNode(t, urls[0].URL, "invalid")
		conn := connectNode(t, urls[0].URL, node.token, "")
		data, err := encodePacket(p)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			t.Fatal(err)
		}
		conn.SetReadDeadline(time.Now().Add(time.Second))
		if _, _, err := conn.ReadMessage(); err == nil {
			t.Fatalf("invalid destination case %d remained connected", i)
		}
	}
}

func TestAutomaticSocketCloseReplaysRecentPacket(t *testing.T) {
	servers, urls := startCluster(t, 1)
	source := createNode(t, urls[0].URL, "source")
	dest := createNode(t, urls[0].URL, "dest")
	srcConn := connectNode(t, urls[0].URL, source.token, "")
	first := connectNode(t, urls[0].URL, dest.token, "")
	second := connectNode(t, urls[0].URL, dest.token, "")
	waitConnections(t, servers[0], source.id, 1)
	waitConnections(t, servers[0], dest.id, 2)
	data, _ := encodePacket(packet{PacketID: "client-chosen", Type: "data", SrcNodeID: "spoofed-node", Dest: dest.id, Payload: "replay"})
	if err := srcConn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatal(err)
	}
	var originalTimestamp int64
	for _, conn := range []*websocket.Conn{first, second} {
		conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodePacket(raw)
		if err != nil || got.Timestamp == nil {
			t.Fatalf("missing original timestamp: %+v, %v", got, err)
		}
		originalTimestamp = *got.Timestamp
	}
	srcConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, initialStatusData, err := srcConn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	initialStatus, err := decodePacket(initialStatusData)
	if err != nil || initialStatus.Type != "status" || initialStatus.Payload != "success" || initialStatus.PacketID != "client-chosen" {
		t.Fatalf("initial status: %+v, %v", initialStatus, err)
	}
	first.Close() // Abrupt close: the server must replay packets queued toward this socket.
	second.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, replayData, err := second.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := decodePacket(replayData)
	if err != nil || replayed.Type != "data" || replayed.PacketID != initialStatus.PacketID || replayed.Src != source.id || replayed.SrcNodeID != source.id || replayed.Dest != dest.id || replayed.Payload != "replay" || replayed.Timestamp == nil || *replayed.Timestamp != originalTimestamp {
		t.Fatalf("replayed packet: %+v, %v", replayed, err)
	}
	srcConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, replayStatusData, err := srcConn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	replayStatus, err := decodePacket(replayStatusData)
	if err != nil || replayStatus.Type != "status" || replayStatus.Payload != "success" || replayStatus.PacketID != replayed.PacketID {
		t.Fatalf("replay status: %+v, %v", replayStatus, err)
	}
}

func TestPeerWebSocketReusesLinkAndReplaysOnClose(t *testing.T) {
	servers, urls := startCluster(t, 2)
	_, response, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(urls[0].URL, "http")+"/peer", nil)
	if err == nil || response == nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unsigned peer connection: %v, %v", response, err)
	}
	response.Body.Close()

	source := createNode(t, urls[0].URL, "source")
	sourceOwner, _ := servers[0].owner(source.id)
	var dest testNode
	for i := 0; i < 100; i++ {
		candidate := createNode(t, urls[1].URL, "dest")
		owner, _ := servers[0].owner(candidate.id)
		if owner.Addr != sourceOwner.Addr {
			dest = candidate
			break
		}
	}
	if dest.id == "" {
		t.Fatal("could not place destination on another server")
	}
	destOwner, _ := servers[0].owner(dest.id)
	srcIndex := 0
	if sourceOwner.Addr == urls[1].URL {
		srcIndex = 1
	}
	dstIndex := 1 - srcIndex
	srcConn := connectNode(t, sourceOwner.Addr, source.token, "")
	dstConn := connectNode(t, destOwner.Addr, dest.token, "")
	waitConnections(t, servers[srcIndex], source.id, 1)
	waitConnections(t, servers[dstIndex], dest.id, 1)
	send := func(id string) {
		t.Helper()
		data, _ := encodePacket(packet{PacketID: id, Type: "data", Dest: dest.id, Payload: "peer-replay"})
		if err := srcConn.WriteMessage(websocket.TextMessage, data); err != nil {
			t.Fatal(err)
		}
		for _, conn := range []*websocket.Conn{dstConn, srcConn} {
			conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			_, raw, err := conn.ReadMessage()
			if err != nil {
				t.Fatal(err)
			}
			got, err := decodePacket(raw)
			if err != nil || got.PacketID != id {
				t.Fatalf("peer delivery: %+v, %v", got, err)
			}
		}
	}
	send("chosen-1")
	servers[srcIndex].peerMu.Lock()
	link := servers[srcIndex].peerLinks[servers[dstIndex].id]
	servers[srcIndex].peerMu.Unlock()
	if link == nil {
		t.Fatal("outbound peer WebSocket was not tracked")
	}
	servers[dstIndex].peerMu.Lock()
	inbound := servers[dstIndex].peerLinks[servers[srcIndex].id]
	servers[dstIndex].peerMu.Unlock()
	if inbound == nil {
		t.Fatal("inbound peer WebSocket was not tracked")
	}
	send("chosen-2")
	servers[srcIndex].peerMu.Lock()
	reused := servers[srcIndex].peerLinks[servers[dstIndex].id] == link
	servers[srcIndex].peerMu.Unlock()
	if !reused {
		t.Fatal("forwarding opened another peer WebSocket")
	}
	link.mu.Lock()
	retainedOutbound := len(link.history) - link.head
	link.mu.Unlock()
	inbound.mu.Lock()
	retainedInbound := len(inbound.history) - inbound.head
	inbound.mu.Unlock()
	if retainedOutbound < 2 || retainedInbound < 2 {
		t.Fatalf("peer replay history missing: outbound %d, inbound %d", retainedOutbound, retainedInbound)
	}
	link.close()
	dstConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, replayRaw, err := dstConn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := decodePacket(replayRaw)
	if err != nil || replayed.Type != "data" || replayed.Src != source.id || replayed.SrcNodeID != source.id || replayed.Dest != dest.id || (replayed.PacketID != "chosen-1" && replayed.PacketID != "chosen-2") {
		t.Fatalf("peer replay changed packet identity: %+v, %v", replayed, err)
	}
}

func TestSocketKeepAliveOverride(t *testing.T) {
	servers, urls := startCluster(t, 1)
	node := createNode(t, urls[0].URL, "keepalive")
	address := "ws" + strings.TrimPrefix(urls[0].URL, "http") + "/connect?" + tokenQuery(node.token)
	conn, _, err := websocket.DefaultDialer.Dial(address+"&tcp_keep_alive=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	waitConnections(t, servers[0], node.id, 1)
	var configured time.Duration
	servers[0].mu.RLock()
	state := servers[0].nodes[node.id]
	state.mu.Lock()
	for cl := range state.connections {
		configured = cl.keepAlive
	}
	state.mu.Unlock()
	servers[0].mu.RUnlock()
	if configured != time.Second {
		t.Fatalf("socket keepalive = %s, want 1s", configured)
	}
	_, response, err := websocket.DefaultDialer.Dial(address+"&tcp_keep_alive=0", nil)
	if err == nil || response == nil || response.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid keepalive accepted: %v, %v", response, err)
	}
	response.Body.Close()
}
