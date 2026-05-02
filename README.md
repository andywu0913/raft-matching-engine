# raft-matching-engine

A POC limit-order matching engine in Go, replicated via [hashicorp/raft].
Single FSM goroutine owns the order book; raft handles durability, replication,
and snapshotting.

[hashicorp/raft]: https://github.com/hashicorp/raft

## Design

See the conversation thread for the full design discussion. Quick summary:

- **Durable-before-ack**: every mutating RPC goes through `raft.Apply`, which
  blocks until the entry is quorum-committed. The client's success ack means
  the event is safe across single-node failure.
- **Single FSM goroutine** mutates the order book; reads (`LookupByRequestID`,
  `GetTopOfBook`) take an `RWMutex` read lock.
- **Idempotency** lives inside the FSM (`(client_id, request_id)` keyed dedup
  map) so retries are safe and the dedup cache is itself replicated and
  snapshotted.
- **Order book**: `google/btree` of price levels per side; each level holds an
  intrusive doubly-linked FIFO of orders. Cancel is O(1) via an
  `id → *Order` map.
- **Snapshot/Restore**: protobuf-encoded snapshot of books + dedup +
  `next_order_id` + `applied_index`. Snapshot deep-copies on the FSM
  goroutine, then the slow `Persist` runs lock-free on the frozen view.

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

## Project layout

```
proto/matchengine/v1/     .proto definitions (service + log entry + snapshot)
proto_gen/matchengine/v1/ buf-generated Go code
cmd/                      main binary entry point
internal/
  auth/                   x-client-id header (POC stub)
  fsm/                    raft.FSM, order book, dedup, snapshot
  raftnode/               hashicorp/raft setup (bolt + file snapshot + tcp)
  server/                 gRPC handlers, leader check, validation
```

## Build

```bash
buf generate    # only when proto changes
go build -o ./bin/matching_engine ./cmd
```

## Run (single-node)

```bash
./bin/matching_engine \
  --node-id node1 \
  --raft-addr 127.0.0.1:7000 \
  --grpc-addr 127.0.0.1:9000 \
  --data-dir ./data \
  --bootstrap
```

State persists in `./data/<node-id>/`. Stop with `Ctrl-C`; restart and the
book + dedup cache are rebuilt from the raft log.

## Run (3-node cluster)

```bash
# Terminal 1 — bootstrap leader
./bin/matching_engine --node-id node1 --raft-addr 127.0.0.1:7001 --grpc-addr 127.0.0.1:9001 \
  --snapshot-threshold 8192 --snapshot-interval 60s \
  --bootstrap

# Terminal 2 — join via node1's gRPC
./bin/matching_engine --node-id node2 --raft-addr 127.0.0.1:7002 --grpc-addr 127.0.0.1:9002 \
  --snapshot-threshold 8192 --snapshot-interval 60s \
  --join 127.0.0.1:9001

# Terminal 3 — join via node1's gRPC
./bin/matching_engine --node-id node3 --raft-addr 127.0.0.1:7003 --grpc-addr 127.0.0.1:9003 \
  --snapshot-threshold 8192 --snapshot-interval 60s \
  --join 127.0.0.1:9001
```

`--snapshot-threshold`: Log entries since the last snapshot before raft considers taking a new one. Set lower to snapshot more aggressively (smaller WAL, faster recovery, more I/O).

`--snapshot-interval`: Wall-clock period between snapshot eligibility checks (with jitter). Set lower to react faster to threshold breaches under load; set higher to amortize fsync cost.

`--bootstrap` and `--join` are only consulted on a node's first start; on restart raft picks up the existing config from disk. Writes to a follower return `FailedPrecondition: not_leader` with a `NotLeader` status detail carrying the leader's raft address.

## Disaster recovery (foreign-WAL restore)

If the original cluster is lost (every node's hardware is gone) but you
have a backup of one node's data dir, you can bring up a new single-node
cluster from it:

```bash
# 1. Restore the data dir on the new machine. The subdirectory must match
#    the new node-id you'll use:
cp -R /backup/node1 /var/lib/matching_engine/phoenix

# 2. Start with --recover. raft.RecoverCluster rewrites the cluster
#    configuration to [{phoenix, <new raft addr>}], writes a fresh snapshot,
#    and truncates the log; NewRaft then restores the FSM from that snapshot.
./bin/matching_engine \
  --node-id phoenix \
  --raft-addr 10.0.0.99:7000 \
  --grpc-addr 10.0.0.99:9000 \
  --data-dir /var/lib/matching_engine \
  --recover
```

The recovered node becomes the leader of a single-node cluster with the
order book, dedup cache, and `next_order_id` all intact. New peers can be
brought back online via `--join` against the recovered leader's gRPC.

`--recover` is **only** for catastrophic loss; using it on a live cluster
will rewrite the cluster configuration and may cause split-brain.
`--bootstrap`, `--join`, and `--recover` are mutually exclusive.

## Smoke test

```bash
# Place a sell
grpcurl -plaintext -import-path proto -proto matchengine/v1/matchengine.proto \
  -H 'x-client-id: alice' \
  -d '{"request_id":"r1","symbol":"BTCUSD","side":"SIDE_SELL","price":50000,"quantity":10}' \
  127.0.0.1:9000 matchengine.v1.MatchEngineService/PlaceOrder

# Cross with a buy
grpcurl -plaintext -import-path proto -proto matchengine/v1/matchengine.proto \
  -H 'x-client-id: bob' \
  -d '{"request_id":"r2","symbol":"BTCUSD","side":"SIDE_BUY","price":50100,"quantity":7}' \
  127.0.0.1:9000 matchengine.v1.MatchEngineService/PlaceOrder

# Top of book
grpcurl -plaintext -import-path proto -proto matchengine/v1/matchengine.proto \
  -d '{"symbol":"BTCUSD","depth":5}' \
  127.0.0.1:9000 matchengine.v1.MatchEngineService/GetTopOfBook

# Idempotent retry: re-send r2; replayed=true, identical result
grpcurl -plaintext -import-path proto -proto matchengine/v1/matchengine.proto \
  -H 'x-client-id: bob' \
  -d '{"request_id":"r2","symbol":"BTCUSD","side":"SIDE_BUY","price":50100,"quantity":7}' \
  127.0.0.1:9000 matchengine.v1.MatchEngineService/PlaceOrder

# Resolve an ambiguous ack
grpcurl -plaintext -import-path proto -proto matchengine/v1/matchengine.proto \
  -H 'x-client-id: bob' \
  -d '{"request_id":"r2"}' \
  127.0.0.1:9000 matchengine.v1.MatchEngineService/LookupByRequestID
```

## Status

Implemented:
- Multi-node raft cluster: bootstrap + `Join` RPC + leader failover
- Disaster recovery: `--recover` rebuilds a single-node cluster from a
  surviving data dir (book + dedup + order-id sequence intact)
- `PlaceOrder`, `CancelOrder`, `LookupByRequestID`, `GetTopOfBook`
- Limit + cancel; FIFO price-time priority
- Idempotency (`(client_id, request_id)` dedup, replicated + snapshotted)
- Snapshot + Restore
- Restart recovery from raft log
- Follower redirect with leader hint in `NotLeader` detail

Deferred:
- Snapshot trigger tuning under load
- Object pooling on the hot path (`*Order`, `*PriceLevel`)
- Self-trade prevention, market/IOC/FOK order types
- Client-side leader-hint redirect when the join address isn't the leader
  (currently the joining node fails fast and the operator picks a new target)
