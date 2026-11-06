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


## Shared node quotas and delayed cleanup (2026-11-04)

Historical measurement: this comparison predates removal of HTTP sending. The
current router retains shared WebSocket quotas and delayed node cleanup.

Three runs each before and after, using `golang:1.27.3-trixie`, Docker limited
to four CPUs / 6 GiB, and `GOMAXPROCS=4`. Baseline is the stateless HTTP-token
implementation before shared node quotas. Workloads ran serially on each version.

| Workload | Before | After | Median change |
| --- | --- | --- | --- |
| Five servers, 1,000 nodes; 999,000 data + 999,000 success packets | 7.474 / 7.530 / 7.402 s | 7.452 / 7.319 / 7.290 s | 7.474 → 7.319 s (−2.1%) |
| 64 WebSockets sharing one source node; 1,024 data + 65,536 feedback deliveries | 75.1 / 81.4 / 139.7 ms | 81.5 / 88.2 / 85.0 ms | 81.4 → 85.0 ms (+4.4%) |

The ranges overlap; these short runs do not establish a speedup or regression.
Five-server median allocated bytes were 7,755.2 MiB before and 7,769.6 MiB after
(+0.2%). The shared-source test uses a high allowance to measure lock contention
and delivery, while separate concurrent HTTP/WebSocket admission tests enforce
an exact shared quota. Fanout is charged once. Rate updates retain counters and
cached tokens cannot overwrite an existing node's policy.

Validation: full race suite, `go vet`, static build, cleanup extension/identity-race
and wake tests, shared quota and reconnect tests. Cleanup uses one reusable timer
and one pending heap entry per socketless node, rather than per-node timers or a
full node scan. Logs: `/tmp/wsrouter-shared-before.log`,
`/tmp/wsrouter-shared-after.log`, `/tmp/wsrouter-shared-race.log`.


## Multi-server CPU review (2026-11-06)

Profiled the five-server/1,000-node workload with four CPUs and GOMAXPROCS=4.
This profile includes both routers and test clients: system calls accounted for
24.2% flat CPU, memory copying 5.5%, and stacks containing SHA-256 or owner lookup
2.7%. JSON handling, allocation and garbage collection also feature prominently.

The queue hashes each destination to select a worker; routing hashes it again to
find its ring owner. A candidate reused the first hash in an internal packet field.
The isolated benchmark improved from median 175.9 ns / 128 bytes / 2 allocations
to 87.08 ns / 64 bytes / 1 allocation. However, the enlarged packet increased full
workload allocations from 7,788.1 MiB to 7,981.1 MiB. Profiled routing time was
7.479s before and 7.702s after. These single runs do not establish a reliable timing
regression, but the allocation increase defeats the intended improvement. Reverted
the candidate; retained `tests/routing_bench_test.go` for future experiments.

Caching a client's own hash on connection does not eliminate destination hashes
for remote nodes. A global destination cache introduces synchronization, memory
limits and eviction; caching owner addresses additionally requires invalidation
on ring changes. Neither is justified by this profile alone. Investigate allocation
and encoding costs before adding such caches. Production routing remains unchanged.
Candidate validation passed race tests, vet, static build and five-server stress.
Logs: `/tmp/wsrouter-cpu-before.log`, `/tmp/wsrouter-hash-profile.log`, and
`/tmp/wsrouter-hash-reuse.log`.

## Packet copies and JSON work (2026-11-06)

Removed a redundant canonical-packet JSON decode from drop feedback: parse the
original fields once, remove canonical keys from the extras, and join the encoded
extras with the encoded canonical packet. Raw extra values retain their precision.
Disconnect cleanup now transfers the detached replay/receipt slices under their
existing locks instead of cloning them. Peer receive and batch buffers over 64 KiB
are released after use; repeated large packets can therefore allocate more often,
but idle peer links no longer retain those large buffers indefinitely.

Three microbenchmark runs, same four-CPU/6-GiB Docker setup and Go 1.27.3:

| Drop feedback, 1 KiB input payload | Before median | After median |
| --- | --- | --- |
| Canonical fields only | 6,106 ns; 2,836 B; 42 allocations | 3,562 ns; 2,162 B; 14 allocations |
| With extra JSON metadata | 7,181 ns; 3,733 B; 49 allocations | 4,427 ns; 2,547 B; 20 allocations |

Healthy routing already serializes one delivery buffer for all sockets of a node.
Peer forwarding still decodes the peer envelope and encodes the destination client
packet. Avoiding this would require retaining wire bytes alongside parsed packets,
adding memory and cache-invalidation rules. Immutable ring snapshots, bounded peer
batch assembly, and asynchronous queue ownership still require their copies.

One routing stress comparison passed both versions: five-server/1,000-node routing
was 7.640 s before and 8.010 s after; 64 sockets sharing one source were 78.2 ms and
80.7 ms. These changes target drops, disconnects, and retained buffers; this single
sample does not demonstrate an improvement to healthy routing throughput. Allocated
bytes during the five-server run were 7,809.8 MiB before and 7,800.3 MiB after.
Full race suite, vet, and static build passed, including replay, packet safety, and
extra-field preservation tests. Benchmarks live in `tests/memory_bench_test.go`.
Logs: `/tmp/wsrouter-memory-before.log`,
`/tmp/wsrouter-memory-baseline-bench.log`, `/tmp/wsrouter-memory-after.log`.
