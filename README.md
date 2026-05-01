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
proto/matchengine/v1/    .proto definitions (service + log entry + snapshot)
gen/matchengine/v1/      buf-generated Go code
cmd/                     main binary entry point
internal/
  auth/                  x-client-id header (POC stub)
  fsm/                   raft.FSM, order book, dedup, snapshot
  raftnode/              hashicorp/raft setup (bolt + file snapshot + tcp)
  server/                gRPC handlers, leader check, validation
```

## Build

```bash
buf generate    # only when proto changes
go build -o ./bin/menode ./cmd
```

## Run (single-node)

```bash
./bin/menode \
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
./bin/menode --node-id node1 --raft-addr 127.0.0.1:7001 --grpc-addr 127.0.0.1:9001 --bootstrap

# Terminal 2 — join via node1's gRPC
./bin/menode --node-id node2 --raft-addr 127.0.0.1:7002 --grpc-addr 127.0.0.1:9002 --join 127.0.0.1:9001

# Terminal 3 — join via node1's gRPC
./bin/menode --node-id node3 --raft-addr 127.0.0.1:7003 --grpc-addr 127.0.0.1:9003 --join 127.0.0.1:9001
```

`--bootstrap` and `--join` are only consulted on a node's first start; on
restart raft picks up the existing config from disk. Writes to a follower
return `FailedPrecondition: not_leader` with a `NotLeader` status detail
carrying the leader's raft address.

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
