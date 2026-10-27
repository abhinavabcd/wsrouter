# Routing optimization measurements

Measured locally on 2026-10-22 using `golang:1.27.3-trixie`, a four-CPU Docker
quota, `GOMAXPROCS=4`, and a 6 GiB memory limit. No OS thread pinning or CPU affinity.
Production was not redeployed as part of this work.

## Method

Original baseline commit: `29b38ac`.

Each stage ran the same five-server, 1,000-client HTTPS/WSS all-to-all test three
times, in fresh test processes. Half of the clients use fanout; every destination
must receive its data and every source its success feedback. Each run verifies
999,000 data packets and 999,000 success statuses, including identities and replay
correlation. The table reports medians. The timer covers traffic, not setup or
teardown. Idle heap is measured after GC; allocations are cumulative during traffic.

The client load generators, verifier, and all five routers share the test process
and CPU budget. This is a controlled regression comparison, not a measurement of
five independently deployed production pods. Small timing differences can be noise.

## Incremental results

| Stage | Traffic time | Idle heap | Allocated during traffic |
| --- | ---: | ---: | ---: |
| Original implementation | 8.335 s | 325.8 MiB | 11,029.7 MiB |
| Established-peer fast path, per-peer dialing, immutable ring reads | 8.208 s | 325.8 MiB | 10,989.4 MiB |
| Smaller outgoing client queues | 8.345 s | 232.0 MiB | 11,009.5 MiB |
| Shared destination-affine routing workers | 8.817 s | 75.1 MiB* | 11,662.2 MiB |
| Shared packet references and in-place history compaction | 9.031 s | 75.1 MiB* | 8,305.9 MiB |
| Negotiated peer batching | 7.610 s | 75.1 MiB* | 11,030.3 MiB |
| Reused peer read/write buffers | 7.215 s | 75.1 MiB* | 7,568.3 MiB |
| Byte limits and nonblocking peer enqueue | 7.275 s | 75.1 MiB* | 7,643.6 MiB |
| Shared client heartbeat scheduler; worker queues initialized before measurement | 7.109 s | 99.3 MiB | 7,624.9 MiB |
| Fair turns for peer data alongside feedback | 7.214 s | 99.3 MiB | 7,605.9 MiB |
| Dynamic peer queues using their existing history lock | 7.082 s | 99.3 MiB | 7,575.0 MiB |

*These intermediate versions initialized worker queues on first traffic. The final
version initializes them when sockets connect, so its idle figure includes those
buffers. Compare the final 99.3 MiB against the original 325.8 MiB.

Final versus original: approximately **15% less traffic time**, **18% more data
throughput**, **70% less idle heap**, and **31% fewer allocated bytes**. Idle
steady goroutines fell from 3,074 to 2,099. End-of-burst heap depends on GC timing;
these measurements do not establish an equivalent reduction in peak RSS.

Not every individual change improved throughput. Shared workers primarily saved
connection memory and goroutines; replay references reduced allocation. Batching
and buffer reuse produced the clearest throughput gains.

## Rejected variants

- Reducing client routing channels from 1,024 to 256 entries lost legitimate burst
  traffic and closed connections. This variant was reverted before further work.
- An initially undersized shared queue also rejected the workload. The retained
  implementation has an explicit aggregate byte budget and enough entry capacity.
- Simply making the old 4,096-entry peer data channel nonblocking produced failed
  deliveries. A larger bounded channel passed; it was subsequently replaced with
  a dynamically sized queue under the existing peer-history lock to reduce fixed
  memory and redundant synchronization.

No failed-delivery variant is retained. The load was not reduced to make it pass.

## Design scope

Routing work is grouped by destination. Node lifecycle operations, replay cleanup,
and direct feedback still use their existing synchronization; this is not a fully
lock-free actor system. Standard bounded channels remain between readers and routing
workers. Handwritten atomic queues and a complete lifecycle rewrite were not added:
multiple producers still exist, and replacing these safely would add complexity
without an isolated measured benefit. Peer queues consolidate synchronization under
an already-required lock. Workers perform no socket writes, but initial peer dialing
and membership recovery can still wait on network operations.

Client protocol, verifier policy, maxc eviction, statuses, timestamps, replay IDs,
and retention windows are preserved. Peer batching is negotiated on the WebSocket
upgrade so mixed versions remain compatible. Batch formation drains available work
without sleeping. Client heartbeat intervals are preserved with one shared scheduler;
peer links retain their own three-second ping timers.

Go's module language version is now 1.25, enabling container-aware runtime CPU
defaults with the production compiler. The comparison explicitly set GOMAXPROCS,
so that default change is not counted as a measured throughput improvement.

## Reproduce

```sh
./stress_test.sh
RUNS=0 WSROUTER_STEADY=1 ./stress_test.sh
go test -race ./...
go vet ./...
```

The sustained test sends about 5,000 messages/second for 40 seconds using 100
clients and 1 KiB payloads, covering the default 30-second client replay window and
periodic cleanup. Its latency includes the client's send through success feedback;
`success` still means accepted into destination queues, not application acknowledgment.
Sampled heap is not peak RSS. See the sustained results below.

| Sustained workload | Original | Final |
| --- | ---: | ---: |
| Data messages delivered / success replies | 200,000 / 200,000 | 200,000 / 200,000 |
| Feedback p50 | 1 ms | 1 ms |
| Feedback p99 | 4 ms | 3 ms |
| Sampled maximum heap | 415.6 MiB | 415.9 MiB |
| Allocated during traffic | 4,871.7 MiB | 3,714.6 MiB |

These are single 40-second runs at a controlled offered rate, not saturation tests.
The final sustained allocation fell about 24%; sampled maximum heap was effectively
unchanged. The earlier fixed-peer-channel variant sampled 437.0 MiB, motivating the
final dynamic queues. Larger payloads, hot destinations, more connections, real
network delays, and actual pod CPU/memory limits need separate capacity planning.

Regression checks cover immutable snapshot publication, established peers bypassing
dial locks, byte limits, rejection without recording unsent peer packets, fair data
turns alongside feedback, heartbeat intervals and removal, legacy and batched peer
frames, expiry releasing references, maxc eviction, routing, and replay behavior.

## Core cleanup and packet validation (2026-10-27)

Measured against the reverted router, with no blocking or storage features.
The final change shares routing-envelope validation between client and peer
inputs and reuses client read buffers. Buffers larger than 64 KiB are released
before waiting for another message, limiting retained memory after large inputs.
No runtime dependencies, worker queues, per-node timers, or protocol features
were added.

Three fresh processes per variant, Go 1.27.3, four CPUs/GOMAXPROCS=4, 6 GiB limit,
five routers, and 1,000 clients. Every run delivered 999,000 data messages and
999,000 success replies.

| Measurement | Before | Final |
| --- | --- | --- |
| Individual routing times (s) | 7.397, 7.548, 7.367 | 7.179, 7.252, 7.408 |
| Median routing time (s) | 7.397 | 7.252 |
| Median allocations during traffic (MiB) | 7,546.0 | 7,321.4 |
| Idle heap (MiB) | 99.3 | 99.3 |

Allocations fell about 3%. The observed median runtime fell about 2%, but the
ranges overlap: three runs do not establish a statistically significant speedup.
Both measurements include the load generators. The final sustained 40-second
run completed 199,861 messages and feedback replies, with p50=1 ms, p99=4 ms,
sampled maximum heap=367.8 MiB, and 3,134.7 MiB allocated. No new sustained
baseline was taken for this review; earlier results above used different runs.

Validation passed: full race suite, vet, CGO-disabled production build, three
all-to-all runs, sustained traffic, and a 30-second parser fuzz run with 336,786
executions. Regression tests exercise malformed and oversized client/peer frames,
invalid raw UTF-8, JSON nesting/type/number errors, bad fanout, excessive peer
batches/replay metadata, trailing HTTP JSON, oversized drop replies, empty ring
updates, and normal closure during a fragmented message. Invalid sockets are
closed while unrelated routing remains operational. Fuzzing is bounded testing,
not a proof that arbitrary inputs can never exhaust resources.

Routing fields now have explicit byte limits (1,024 bytes, or 128 for type),
documented in README.md. Empty ring updates are guarded at the state transition
as well as the HTTP boundary. Peer batches exceeding 32 frames are rejected
before processing their frames.

The next capacity concern is replay retention: queue byte limits do not bound
all retained payloads, and rp5 counts one incoming fanout request as one packet.
A global replay-memory budget would require an explicit policy for rejecting
traffic when it cannot preserve the promised lookback window. This review keeps
those existing semantics rather than silently truncating retained messages.

## Origin socket exclusion (2026-10-27)

Connections now receive process-scoped unique socket IDs. In-memory packets
retain the origin, and self-addressed peer frames carry it for replay. Non-self
peer frames omit the metadata because their destination cannot be the origin
socket. IDs are allocated once per connection and do not retain the client
object through replay histories.

Full race tests, vet, and the static build passed. Regression tests cover fanout
including self and a remote node, spoofed client socket IDs, sibling sockets,
status feedback, peer replay, reconnection, complete node-state recreation, and
self-send with no other eligible connection.

One four-CPU five-router stress run delivered all 999,000 data packets and
999,000 success replies in 7.349 seconds, with 99.4 MiB idle heap and 7,693.2 MiB
allocated during traffic. This is a regression/load check, not a new performance
baseline. The origin field adds packet/history bookkeeping; it is not a free
allocation optimization. The all-to-all workload excludes self, so the dedicated
integration tests above exercise the new exclusion behavior.
