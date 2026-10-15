package main

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"
)

func TestPeerFeedbackDoesNotBlockRouting(t *testing.T) {
	link := &peerLink{done: make(chan struct{}), send: make(chan []byte, 1), feedbackReady: make(chan struct{}, 1)}
	link.send <- []byte("full data queue")
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
		if err := json.Unmarshal(link.takeFeedback(), &frame); err != nil || frame.Packet.PacketID != strconv.Itoa(i) {
			t.Fatalf("feedback %d was reordered or lost: %v", i, err)
		}
	}
	if link.takeFeedback() != nil || link.feedbackBytes != 0 || len(link.feedback) != 0 {
		t.Fatal("drained feedback queue retained messages or accounting")
	}
	link.feedbackBytes = 64 << 20
	if err := link.enqueue(context.Background(), packet{Type: "status"}); err == nil {
		t.Fatal("feedback exceeded its memory limit")
	}
}
