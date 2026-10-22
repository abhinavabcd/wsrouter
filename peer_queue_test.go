package main

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"
)

func TestPeerFeedbackDoesNotBlockRouting(t *testing.T) {
	link := &peerLink{done: make(chan struct{}), ready: make(chan struct{}, 1)}
	link.data.bytes = 64 << 20 // Data is full; feedback must still make progress.
	done := make(chan error, 1)
	go func() {
		for i := range 20000 {
			if err := link.enqueue(context.Background(), packet{Type: "status", Payload: "success", PacketID: strconv.Itoa(i)}); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		close(link.done)
		t.Fatal("feedback blocked routing while the writer was busy")
	}
	for i := range 20000 {
		var frame peerFrame
		if err := json.Unmarshal(link.takeFrame(false), &frame); err != nil || frame.Packet.PacketID != strconv.Itoa(i) {
			t.Fatalf("feedback %d was reordered or lost: %v", i, err)
		}
	}
	if link.takeFrame(false) != nil || link.feedback.bytes != 0 || len(link.feedback.entries) != 0 {
		t.Fatal("drained feedback queue retained messages or accounting")
	}
	link.feedback.bytes = 64 << 20
	if err := link.enqueue(context.Background(), packet{Type: "status"}); err == nil {
		t.Fatal("feedback exceeded its memory limit")
	}
}
