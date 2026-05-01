#!/usr/bin/env bash
# e2e_load_test.sh — boots a 3-node local raft cluster, runs the load generator
# against the leader, and tears everything down.
#
# Usage:  ./scripts/e2e_load_test.sh [duration] [concurrency]
#         duration  default: 20s
#         concurrency default: 64
set -euo pipefail

cd "$(dirname "$0")/.."

DURATION="${1:-20s}"
CONCURRENCY="${2:-64}"
DATA_DIR="./data"

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
echo "==> node logs in $LOG_DIR"

echo "==> start node1 (bootstrap)"
./bin/matching_engine \
  --node-id node1 --raft-addr 127.0.0.1:7001 --grpc-addr 127.0.0.1:9001 \
  --data-dir "$DATA_DIR" --bootstrap >"$LOG_DIR/node1.log" 2>&1 &
PIDS+=("$!")
sleep 2

echo "==> start node2 (join)"
./bin/matching_engine \
  --node-id node2 --raft-addr 127.0.0.1:7002 --grpc-addr 127.0.0.1:9002 \
  --data-dir "$DATA_DIR" --join 127.0.0.1:9001 >"$LOG_DIR/node2.log" 2>&1 &
PIDS+=("$!")
sleep 2

echo "==> start node3 (join)"
./bin/matching_engine \
  --node-id node3 --raft-addr 127.0.0.1:7003 --grpc-addr 127.0.0.1:9003 \
  --data-dir "$DATA_DIR" --join 127.0.0.1:9001 >"$LOG_DIR/node3.log" 2>&1 &
PIDS+=("$!")

echo "==> wait for cluster to settle"
sleep 4

echo "==> run load_test_client"
./bin/load_test_client \
  --target 127.0.0.1:9001 \
  --duration "$DURATION" \
  --concurrency "$CONCURRENCY"

echo "==> done"
