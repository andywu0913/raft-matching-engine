#!/usr/bin/env bash
# e2e_load_test.sh — boots an N-node local raft cluster, runs the load
# generator against the leader, and tears everything down.
#
# Usage:  ./scripts/e2e_load_test.sh [nodes] [duration] [concurrency]
#
#   nodes        number of cluster nodes (default: 3; minimum: 1)
#   duration     load duration, e.g. 20s, 1m (default: 20s)
#   concurrency  concurrent client workers (default: 64)
#
# Examples:
#   ./scripts/e2e_load_test.sh                # 3-node, 20s, c=64
#   ./scripts/e2e_load_test.sh 1 20s 256      # 1-node baseline
#   ./scripts/e2e_load_test.sh 3 30s 1024     # 3-node, sustained, high concurrency
#   ./scripts/e2e_load_test.sh 5 20s 256      # 5-node (quorum=3)
set -euo pipefail

cd "$(dirname "$0")/.."

NODES="${1:-3}"
DURATION="${2:-20s}"
CONCURRENCY="${3:-64}"
DATA_DIR="./data"

if ! [[ "$NODES" =~ ^[0-9]+$ ]] || [ "$NODES" -lt 1 ]; then
  echo "error: nodes must be a positive integer (got: $NODES)" >&2
  exit 1
fi

# Port plan: nodeN uses raft 7000+N and gRPC 9000+N.
# (so node1 = 7001/9001, node2 = 7002/9002, ...)
RAFT_PORT_BASE=7000
GRPC_PORT_BASE=9000
LEADER_GRPC="127.0.0.1:$((GRPC_PORT_BASE + 1))"

echo "==> build"
go build -o ./bin/matching_engine  ./cmd/matching_engine
go build -o ./bin/load_test_client ./cmd/load_test_client

# Clean previous state so the bootstrap node can re-bootstrap.
rm -rf "$DATA_DIR"

PIDS=()
cleanup() {
  echo "==> teardown"
  for pid in "${PIDS[@]:-}"; do
    kill -INT "$pid" 2>/dev/null || true
  done
  # Give graceful stop a moment, then force.
  sleep 2
  for pid in "${PIDS[@]:-}"; do
    kill -KILL "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

LOG_DIR="$(mktemp -d)"
echo "==> $NODES-node cluster, duration=$DURATION, concurrency=$CONCURRENCY"
echo "==> node logs in $LOG_DIR"

# node1 — bootstrap
echo "==> start node1 (bootstrap)"
./bin/matching_engine \
  --node-id node1 \
  --raft-addr "127.0.0.1:$((RAFT_PORT_BASE + 1))" \
  --grpc-addr "127.0.0.1:$((GRPC_PORT_BASE + 1))" \
  --data-dir "$DATA_DIR" --bootstrap \
  >"$LOG_DIR/node1.log" 2>&1 &
PIDS+=("$!")
sleep 2

# nodes 2..N — join
for ((i=2; i<=NODES; i++)); do
  echo "==> start node$i (join)"
  ./bin/matching_engine \
    --node-id "node$i" \
    --raft-addr "127.0.0.1:$((RAFT_PORT_BASE + i))" \
    --grpc-addr "127.0.0.1:$((GRPC_PORT_BASE + i))" \
    --data-dir "$DATA_DIR" --join "$LEADER_GRPC" \
    >"$LOG_DIR/node$i.log" 2>&1 &
  PIDS+=("$!")
  sleep 2
done

echo "==> wait for cluster to settle"
sleep 4

echo "==> run load_test_client against $LEADER_GRPC"
./bin/load_test_client \
  --target "$LEADER_GRPC" \
  --duration "$DURATION" \
  --concurrency "$CONCURRENCY"

echo "==> done"
