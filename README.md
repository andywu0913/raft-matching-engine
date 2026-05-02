# raft-matching-engine

A POC putting a limit-order matching engine behind [hashicorp/raft](https://github.com/hashicorp/raft) so the engine itself is HA — every acked order survives a single-node loss, and a follower takes over as leader in seconds. Using `hashicorp/raft` means no custom WAL, log truncation, snapshot/restore, or replication. The FSM only defines `Apply`/`Snapshot`/`Restore` and the library handles the rest.

> **Note:** real matching engines don't do this. Quorum-commit per order adds milliseconds of latency, so exchanges use single-leader engines with custom WAL shipping. Throughput here is bounded by raft commit, not the engine itself — see [Performance benchmarks](#performance-benchmarks).


## Design

- **Durable-before-ack**: every mutating RPC goes through `raft.Apply`, which blocks until the entry is quorum-committed. The client's success ack means the event is safe across single-node failure.
- **Single FSM goroutine** mutates the order book; reads (`LookupByRequestID`, `GetTopOfBook`) take an `RWMutex` read lock.
- **Idempotency** lives inside the FSM (`(client_id, request_id)` keyed dedup map) so retries are safe and the dedup cache is itself replicated and snapshotted.
- **Order book**: `google/btree` of price levels per side; each level holds an intrusive doubly-linked FIFO of orders. Cancel is O(1) via an `id → *Order` map.
- **Snapshot**: triggered by raft when both `snapshot-interval` has elapsed *and* the log has accumulated `snapshot-threshold` entries since the last snapshot. `FSM.Snapshot` serializes books + dedup + `next_order_id` + `applied_index` into a `pb.Snapshot` proto under the FSM lock; `Persist` writes those bytes to the `SnapshotStore` asynchronously while new `Apply` calls keep flowing. Raft then truncates the log up to the snapshot index.
- **Restore**: on startup, raft reads the newest snapshot from the `SnapshotStore`, calls `FSM.Restore` (which wipes current state and rebuilds books + dedup from the proto), and *then* replays only log entries newer than `snapshot.Index` via `FSM.Apply`. With no snapshot present, the entire WAL is replayed instead.

**Apply path (every accepted order/cancel):**

```
gRPC intake
    │
    ▼
raft.Apply(LogEntry)        ← blocks until quorum-committed → ack
    │
    ▼
FSM.Apply (single-threaded) ← dedup check → match → mutate book
    │
    ▼
ApplyResult                 ← back to the gRPC handler → response
```

**Snapshot path (periodic log compaction):**

```
raft scheduler              ← fires when both interval AND threshold are met
    │
    ▼
FSM.Snapshot()              ← FSM goroutine; deep-copy state → proto.Marshal
    │
    ▼
fsmSnapshot{frozen bytes}
    │
    ▼
Persist(SnapshotSink)       ← writes bytes to disk lock-free
    │
    ▼
LogStore.DeleteRange(0, snapshot.Index)   ← raft truncates the log
```

**Restore path (startup, before serving):**

```
NewRaft startup
    │
    ▼
SnapshotStore.List() → newest (if any)
    │
    ▼
FSM.Restore(rc)             ← wipes state, decodes proto, rebuilds books + dedup
    │
    ▼
LogStore replay: for idx in (snapshot.Index, LastIndex]
    FSM.Apply(log)          ← normal apply path; tail-only
```

## raft.FSM integration

`hashicorp/raft` requires the application to implement three methods on
the `raft.FSM` interface:

| Method | File | What our implementation does |
|---|---|---|
| `Apply(*raft.Log) any` | `internal/fsm/fsm.go` | Decodes the committed `pb.LogEntry`, checks the dedup cache (`(client_id, request_id)` → cached `ApplyResult`), then dispatches to `applyPlace` or `applyCancel`. Mutates the order book (intrusive FIFO + btree of price levels) and returns the `ApplyResult` that the gRPC handler turns into a response. Also advances `applied_index` and `next_order_id`. |
| `Snapshot() (FSMSnapshot, error)` | `internal/fsm/snapshot.go` | Holds the FSM lock briefly, walks every symbol's bid/ask trees plus the entire dedup map, and serializes a `pb.Snapshot` proto containing books + dedup + `next_order_id` + `applied_index`. Returns an `fsmSnapshot` wrapper holding the frozen bytes. |
| `Restore(io.ReadCloser) error` | `internal/fsm/snapshot.go` | Reads the snapshot bytes, unmarshals the proto, **wipes all current FSM state** (per raft contract), then rebuilds books + dedup cache + `next_order_id` + `applied_index`. Called by raft on startup *before* normal log replay. |

The `raft.FSMSnapshot` value returned from `Snapshot()` (our private
`fsmSnapshot` type) provides:

| Method | What our implementation does |
|---|---|
| `Persist(SnapshotSink)` | Writes the captured proto bytes to the `SnapshotSink` and closes it. Runs in its own goroutine and **can race** with subsequent `Apply` calls, which is why `Snapshot()` already serialized into a frozen `[]byte` — `Persist` reads it lock-free. |
| `Release()` | No-op. Nothing to free; the byte slice is GC'd when nothing references it. |

## Project layout

```
proto/matchengine/v1/     .proto definitions (service + log entry + snapshot)
proto_gen/matchengine/v1/ buf-generated Go code
cmd/matching_engine/      raft node binary
cmd/load_test_client/     QPS load generator
internal/
  auth/                   x-client-id header (POC stub)
  fsm/                    raft.FSM, order book, dedup, snapshot
  raftnode/               hashicorp/raft setup (bolt + file snapshot + tcp)
  server/                 gRPC handlers, leader check, validation
```

## Build

```bash
buf generate    # only when proto changes
go build -o ./bin/matching_engine ./cmd/matching_engine
```

## Run (single-node)

```bash
./bin/matching_engine \
  --node-id node \
  --data-dir ./data \
  --raft-addr 127.0.0.1:7000 --grpc-addr 127.0.0.1:9000 \
  --snapshot-threshold 8192 --snapshot-interval 60s \
  --bootstrap
```

State persists in `./data/<node-id>/`. Stop with `Ctrl-C`; restart and the book + dedup cache are rebuilt from the raft log.

## Run (3-node cluster)

```bash
# Terminal 1 — bootstrap leader
./bin/matching_engine \
  --node-id node1 \
  --data-dir ./data \
  --raft-addr 127.0.0.1:7001 --grpc-addr 127.0.0.1:9001 \
  --snapshot-threshold 8192 --snapshot-interval 60s \
  --bootstrap

# Terminal 2 — join via node1's gRPC
./bin/matching_engine \
  --node-id node2 \
  --data-dir ./data \
  --raft-addr 127.0.0.1:7002 --grpc-addr 127.0.0.1:9002 \
  --snapshot-threshold 8192 --snapshot-interval 60s \
  --join 127.0.0.1:9001

# Terminal 3 — join via node1's gRPC
./bin/matching_engine \
  --node-id node3 \
  --data-dir ./data \
  --raft-addr 127.0.0.1:7003 --grpc-addr 127.0.0.1:9003 \
  --snapshot-threshold 8192 --snapshot-interval 60s \
  --join 127.0.0.1:9001
```

`--snapshot-threshold`: Log entries since the last snapshot before raft considers taking a new one. Set lower to snapshot more aggressively (smaller WAL, faster recovery, more I/O).

`--snapshot-interval`: Wall-clock period between snapshot eligibility checks (with jitter). Set lower to react faster to threshold breaches under load; set higher to amortize fsync cost.

`--bootstrap` and `--join` are only consulted on a node's first start; on restart raft picks up the existing config from disk. Writes to a follower return `FailedPrecondition: not_leader` with a `NotLeader` status detail carrying the leader's raft address.

## Disaster recovery (foreign-WAL restore)

If the original cluster is lost but you have a backup of one node's data dir, you can bring up a new single-node cluster from it:

```bash
# 1. Copy the surviving node's data dir.
cp -r ./data/node1 ./data/phoenix

# 2. Start with --recover. raft.RecoverCluster rewrites
#    the cluster configuration to [{phoenix, <new raft addr>}], writes a
#    fresh snapshot, and truncates the log; NewRaft then restores the FSM
#    from that snapshot.
./bin/matching_engine \
  --node-id phoenix \
  --data-dir ./data \
  --raft-addr 10.0.0.99:7000 --grpc-addr 10.0.0.99:9000 \
  --recover
```

The recovered node becomes the leader of a single-node cluster with the order book, dedup cache, and `next_order_id` all intact. New peers can be brought back online via `--join` against the recovered leader's gRPC.

`--recover` is **only** for catastrophic loss; using it on a live cluster will rewrite the cluster configuration and may cause split-brain.
`--bootstrap`, `--join`, and `--recover` are mutually exclusive.

## Usage examples

```bash
# Place a sell
grpcurl -plaintext \
  -H 'x-client-id: alice' \
  -d '{"request_id":"r1","symbol":"BTCUSD","side":"SIDE_SELL","price":50000,"quantity":10}' \
  127.0.0.1:9000 matchengine.v1.MatchEngineService/PlaceOrder

# Cross with a buy
grpcurl -plaintext \
  -H 'x-client-id: bob' \
  -d '{"request_id":"r2","symbol":"BTCUSD","side":"SIDE_BUY","price":50100,"quantity":7}' \
  127.0.0.1:9000 matchengine.v1.MatchEngineService/PlaceOrder

# Top of book
grpcurl -plaintext \
  -d '{"symbol":"BTCUSD","depth":5}' \
  127.0.0.1:9000 matchengine.v1.MatchEngineService/GetTopOfBook

# Idempotent retry: re-send r2; replayed=true, identical result
grpcurl -plaintext \
  -H 'x-client-id: bob' \
  -d '{"request_id":"r2","symbol":"BTCUSD","side":"SIDE_BUY","price":50100,"quantity":7}' \
  127.0.0.1:9000 matchengine.v1.MatchEngineService/PlaceOrder

# Resolve an ambiguous ack
grpcurl -plaintext \
  -H 'x-client-id: bob' \
  -d '{"request_id":"r2"}' \
  127.0.0.1:9000 matchengine.v1.MatchEngineService/LookupByRequestID
```

## Performance benchmarks

Measured on a **MacBook Air 2022 (Apple M2)**, 20-second window after 2s warmup. The whole cluster runs on localhost, so "network" is essentially `memcpy`.

### 1-node (no replication)

| Concurrency | QPS | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| 256  | 11,340 | 22 ms | 35 ms | 42 ms | 74 ms |
| 512  | **13,096** | 38 ms | 59 ms | 72 ms | 109 ms |
| 1024 | 12,304 | 82 ms | 106 ms | 122 ms | 158 ms |

Engine saturates at ~13k QPS by concurrency=512; extra clients just queue in the gRPC layer without producing larger raft commit batches.

### 3-node (quorum-replicated, all on localhost)

| Concurrency | QPS | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| 256  | 549 | 480 ms | 652 ms | 667 ms | 1.17 s |
| 512  | 620 | 866 ms | 1.15 s | 1.22 s | 1.94 s |
| 1024 | 684 | 1.59 s | 2.46 s | 2.50 s | 4.74 s |

Throughput is essentially flat at ~600 QPS regardless of concurrency; latency grows linearly with offered load. Slowdown vs. 1-node is ~20×.

### What's actually slow

On localhost, network is memcpy, so round-trips are sub-millisecond. The cost is quorum-commit latency. The leader cannot unblock raft.Apply until a quorum of nodes have persisted the log entry (fsync) and acknowledged. That means every batch pays the leader's own fsync plus the quorum's slowest fsync-and-round-trip in series. Real-world exchanges avoid this entirely by running a single-leader engine with custom WAL shipping.

### Run your own

The `scripts/e2e_load_test.sh` helper handles build, cluster bring-up, loadgen run, and teardown in one shot. Pass it the cluster size, load duration, and concurrency:

```bash
# Usage: ./scripts/e2e_load_test.sh [nodes] [duration] [concurrency]

# 1-node baseline at concurrency=512
./scripts/e2e_load_test.sh 1 20s 512

# 3-node, c=512
./scripts/e2e_load_test.sh 3 20s 512

# 5-node (quorum=3), c=256
./scripts/e2e_load_test.sh 5 20s 256
```

Each node `i` listens on raft `127.0.0.1:700i` and gRPC `127.0.0.1:900i`,
so `node1`'s gRPC is `127.0.0.1:9001`. The script always points loadgen
at node1 (the bootstrap node, initial leader).

To run a one-off load against an already-running cluster:

```bash
./bin/load_test_client --target 127.0.0.1:9001 --concurrency 512 --duration 20s
```
