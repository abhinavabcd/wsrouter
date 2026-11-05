package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"testing"
)

func BenchmarkOwnerLookup(b *testing.B) {
	for _, n := range []int{5, 100} {
		b.Run(fmt.Sprintf("servers-%d", n), func(b *testing.B) {
			ring := make([]peer, n)
			for i := range ring {
				ring[i] = peer{ID: uint64(math.MaxInt64) / uint64(n) * uint64(i), Addr: fmt.Sprint(i)}
			}
			b.ReportAllocs()
			for b.Loop() {
				_ = ownerIn(ring, "auth.example.com|50faad53703b444091e8c7f6a8074057")
			}
		})
	}
}

func BenchmarkRoutingHashes(b *testing.B) {
	ring := []peer{{ID: 0}, {ID: 1 << 61}, {ID: 1 << 62}, {ID: 3 << 61}, {ID: 1<<63 - 1}}
	dest := "auth.example.com|50faad53703b444091e8c7f6a8074057"
	for _, reuse := range []bool{false, true} {
		b.Run(fmt.Sprint(reuse), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				hash := sha256.Sum256([]byte(dest))
				value := binary.BigEndian.Uint64(hash[:8])
				_ = value % 4
				if reuse {
					_ = benchmarkOwnerAt(ring, value)
				} else {
					_ = ownerIn(ring, dest)
				}
			}
		})
	}
}

// Benchmark-only candidate: reuse the worker-selection hash for owner lookup.
func benchmarkOwnerAt(ring []peer, hash uint64) peer {
	i := sort.Search(len(ring), func(i int) bool { return ring[i].ID >= hash>>1 })
	if i == len(ring) {
		i = 0
	}
	return ring[i]
}
