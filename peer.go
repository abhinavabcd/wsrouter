package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type peerFrame struct {
	Packet  packet `json:"packet"`
	Hops    uint8  `json:"hops,omitempty"`
	Replays uint8  `json:"replays,omitempty"`
}

type peerLink struct {
	server   *server
	peerID   uint64
	conn     *websocket.Conn
	outgoing bool
	send     chan []byte
	feedback chan []byte
	incoming chan packet
	done     chan struct{}
	once     sync.Once
	mu       sync.Mutex
	history  []replayRecord
	head     int
}

func (s *server) peer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET required", http.StatusMethodNotAllowed)
		return
	}
	id, err := strconv.ParseUint(r.Header.Get("X-Server-ID"), 10, 64)
	key, known := s.peerKey(id)
	if err != nil || !known || id == s.id || !verifyBody([]byte("GET /peer"), r.Header.Get("X-Server-Signature"), key) {
		http.Error(w, "invalid server signature", http.StatusUnauthorized)
		return
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	link := &peerLink{server: s, peerID: id, conn: conn, send: make(chan []byte, 4096), feedback: make(chan []byte, 4096), incoming: make(chan packet, 4096), done: make(chan struct{})}
	s.registerPeerLink(link)
	link.start()
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
	link := &peerLink{server: s, peerID: owner.ID, conn: conn, outgoing: true, send: make(chan []byte, 4096), feedback: make(chan []byte, 4096), incoming: make(chan packet, 4096), done: make(chan struct{})}
	s.registerPeerLink(link)
	link.start()
	return link, nil
}

func (s *server) registerPeerLink(link *peerLink) {
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
}

func (link *peerLink) start() {
	go link.writeLoop()
	for range 16 {
		go link.processLoop()
	}
	go link.readLoop()
}

func (link *peerLink) enqueue(ctx context.Context, p packet) error {
	frame := peerFrame{Packet: p, Hops: p.hops, Replays: p.replays}
	data, err := json.Marshal(frame)
	if err != nil || len(data) > 1<<20+4096 {
		return errors.New("peer packet too large")
	}
	select {
	case <-link.done:
		return errors.New("peer link closed")
	default:
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
		if err := link.server.routePacket(context.Background(), p); err != nil {
			link.server.routeErrors.Add(1)
			if p.Type != "status" {
				link.server.sendStatus(p, false)
			}
		}
	}
}

func (link *peerLink) writeLoop() {
	defer link.close()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-link.done:
			return
		case <-ticker.C:
			link.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := link.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
			continue
		default:
		}
		var data []byte
		select {
		case <-link.done:
			return
		case data = <-link.feedback:
		default:
			select {
			case <-link.done:
				return
			case data = <-link.feedback:
			case data = <-link.send:
			case <-ticker.C:
				link.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err := link.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
				continue
			}
		}
		link.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := link.conn.WriteMessage(websocket.TextMessage, data); err != nil {
			return
		}
	}
}

func (link *peerLink) recordLocked(p packet, now time.Time) {
	if p.replays >= 3 {
		return
	}
	link.pruneLocked(now)
	link.history = append(link.history, replayRecord{packet: p, sentAt: now})
}

func (link *peerLink) pruneLocked(now time.Time) {
	cutoff := now.Add(-3 * time.Second)
	for link.head < len(link.history) && link.history[link.head].sentAt.Before(cutoff) {
		link.head++
	}
	if link.head == len(link.history) {
		link.history = nil
		link.head = 0
	} else if link.head > 1024 && link.head*2 >= len(link.history) {
		link.history = append([]replayRecord(nil), link.history[link.head:]...)
		link.head = 0
	}
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
		link.pruneLocked(time.Now())
		replay := append([]replayRecord(nil), link.history[link.head:]...)
		link.history = nil
		link.mu.Unlock()
		if len(replay) != 0 {
			go func() {
				for _, item := range replay {
					p := item.packet
					p.replays++
					if err := s.routePacket(context.Background(), p); err != nil {
						s.routeErrors.Add(1)
						if p.Type != "status" {
							s.sendStatus(p, false)
						}
					}
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
		link.pruneLocked(now)
		link.mu.Unlock()
	}
}

func (s *server) forwardOverPeer(ctx context.Context, owner peer, p packet) error {
	link, err := s.getPeerLink(owner)
	if err != nil {
		return fmt.Errorf("connect peer %d: %w", owner.ID, err)
	}
	return link.enqueue(ctx, p)
}
