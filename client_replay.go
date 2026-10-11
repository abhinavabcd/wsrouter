package main

import "time"

// pruneSentLocked runs while the destination node state is locked.
func (cl *client) pruneSentLocked(now time.Time) {
	cutoff := now.Add(-cl.keepAlive)
	for cl.sentHead < len(cl.sent) && cl.sent[cl.sentHead].sentAt.Before(cutoff) {
		cl.sentHead++
	}
	if cl.sentHead == len(cl.sent) {
		cl.sent = nil
		cl.sentHead = 0
		return
	}
	if cl.sentHead > 1024 && cl.sentHead*2 >= len(cl.sent) {
		cl.sent = append([]replayRecord(nil), cl.sent[cl.sentHead:]...)
		cl.sentHead = 0
	}
}
