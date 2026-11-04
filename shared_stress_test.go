package main

import (
	"fmt"
	"github.com/gorilla/websocket"
	"os"
	"sync"
	"testing"
	"time"
)

func TestStressSharedNode(t *testing.T) {
	if os.Getenv("WSROUTER_STRESS") == "" {
		t.Skip("set WSROUTER_STRESS=1")
	}
	servers, urls := startCluster(t, 1)
	src := createNode(t, urls[0].URL, "shared", 1000000)
	account := accountFor(src)
	account.mu.Lock()
	account.maxc = 64
	account.mu.Unlock()
	dst := createNode(t, urls[0].URL, "destination")
	dest := connectNode(t, urls[0].URL, dst.token, "")
	const sockets, messages = 64, 16
	conns := make([]*websocket.Conn, sockets)
	for i := range conns {
		conns[i] = connectNode(t, urls[0].URL, src.token, "")
	}
	waitConnections(t, servers[0], src.id, sockets)
	var readers, writers sync.WaitGroup
	errors := make(chan error, sockets*2+1)
	read := func(c *websocket.Conn, kind string) {
		defer readers.Done()
		c.SetReadDeadline(time.Now().Add(30 * time.Second))
		for range sockets * messages {
			var p packet
			if err := c.ReadJSON(&p); err != nil {
				errors <- err
				return
			}
			if p.Type != kind || (kind == "status" && p.Payload != "success") {
				errors <- fmt.Errorf("unexpected packet: %+v", p)
				return
			}
		}
	}
	readers.Add(sockets + 1)
	go read(dest, "data")
	for _, c := range conns {
		go read(c, "status")
	}
	start := time.Now()
	for i, c := range conns {
		writers.Add(1)
		go func(i int, c *websocket.Conn) {
			defer writers.Done()
			for n := range messages {
				if err := c.WriteJSON(packet{Type: "data", Dest: dst.id, PacketID: fmt.Sprintf("%d-%d", i, n), Payload: "hello"}); err != nil {
					errors <- err
					return
				}
			}
		}(i, c)
	}
	writers.Wait()
	readers.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	t.Logf("64 sockets sharing one node: %d data + %d feedback deliveries in %s", sockets*messages, sockets*sockets*messages, time.Since(start))
}
