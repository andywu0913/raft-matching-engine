// matching_engine is a single-binary match-engine node.
//
// Cluster formation:
//
//   - The first node starts with --bootstrap to seed a single-node cluster.
//   - Subsequent nodes start with --join=<leader-grpc-addr>; on first start
//     they make a Join RPC to the leader, which calls raft.AddVoter and
//     replicates the existing log to the new follower.
//
// On every restart raft picks up state from the data dir (logs, stable, snaps),
// so --bootstrap and --join are only consulted on the *first* start of a node.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	pb "raft-matching-engine/proto_gen/matchengine/v1"
	"raft-matching-engine/internal/fsm"
	"raft-matching-engine/internal/raftnode"
	"raft-matching-engine/internal/server"
)

func main() {
	var (
		nodeID    = flag.String("node-id", "node1", "raft server ID (unique per node)")
		raftAddr  = flag.String("raft-addr", "127.0.0.1:7000", "raft TCP bind+advertise address")
		grpcAddr  = flag.String("grpc-addr", "127.0.0.1:9000", "gRPC listen address")
		dataDir   = flag.String("data-dir", "./data", "raft data directory root")
		bootstrap = flag.Bool("bootstrap", false, "bootstrap a single-node cluster (first node only)")
		joinAddr  = flag.String("join", "", "gRPC address of an existing leader to join")
		recover   = flag.Bool("recover", false, "force a single-node cluster from existing data-dir (disaster recovery)")

		snapshotThreshold = flag.Uint64("snapshot-threshold", 0, "log entries since last snapshot before raft takes a new one (0 = raft default 8192)")
		snapshotInterval  = flag.Duration("snapshot-interval", 0, "min wall time between snapshot checks (0 = raft default 120s)")
	)
	flag.Parse()

	modes := 0
	if *bootstrap {
		modes++
	}
	if *joinAddr != "" {
		modes++
	}
	if *recover {
		modes++
	}
	if modes > 1 {
		log.Fatal("--bootstrap, --join, and --recover are mutually exclusive")
	}

	absDataDir, err := filepath.Abs(*dataDir)
	if err != nil {
		log.Fatalf("resolve data-dir: %v", err)
	}
	nodeDataDir := filepath.Join(absDataDir, *nodeID)
	if err := os.MkdirAll(nodeDataDir, 0o755); err != nil {
		log.Fatalf("mkdir node data dir: %v", err)
	}

	f := fsm.New()
	rn, err := raftnode.New(raftnode.Config{
		NodeID:            *nodeID,
		BindAddr:          *raftAddr,
		DataDir:           nodeDataDir,
		Bootstrap:         *bootstrap,
		Recover:           *recover,
		SnapshotThreshold: *snapshotThreshold,
		SnapshotInterval:  *snapshotInterval,
	}, f)
	if err != nil {
		log.Fatalf("raft node: %v", err)
	}

	srv := server.New(rn, f)

	gsrv := grpc.NewServer()
	pb.RegisterMatchEngineServiceServer(gsrv, srv)
	reflection.Register(gsrv)

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("grpc listen: %v", err)
	}

	go func() {
		log.Printf("matching_engine %s: raft=%s grpc=%s data=%s", *nodeID, *raftAddr, *grpcAddr, nodeDataDir)
		if err := gsrv.Serve(lis); err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}()

	if *joinAddr != "" {
		// Don't block startup if the leader is briefly unavailable — retry a few times.
		if err := joinCluster(*joinAddr, *nodeID, *raftAddr, 5, time.Second); err != nil {
			log.Fatalf("join cluster: %v", err)
		}
		log.Printf("joined cluster via %s", *joinAddr)
	}

	go reportLeader(rn)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Print("shutting down...")
	gsrv.GracefulStop()
	if err := rn.Shutdown(); err != nil {
		log.Printf("raft shutdown: %v", err)
	}
	log.Print("bye")
}

// joinCluster makes a Join RPC to the leader's gRPC endpoint. Idempotent on
// the leader side (re-joining an existing voter is a no-op), so it's safe to
// retry across restarts. Note: the address must be the *leader's* gRPC; if
// it's not the leader the call returns FailedPrecondition with a NotLeader
// detail that this POC doesn't follow (production would parse + redirect).
func joinCluster(leaderGRPC, nodeID, raftAddr string, attempts int, backoff time.Duration) error {
	conn, err := grpc.NewClient(leaderGRPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	client := pb.NewMatchEngineServiceClient(conn)

	var lastErr error
	for range attempts {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := client.Join(ctx, &pb.JoinRequest{NodeId: nodeID, RaftAddress: raftAddr})
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(backoff)
	}
	return fmt.Errorf("after %d attempts: %w", attempts, lastErr)
}

func reportLeader(rn *raftnode.Node) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	var lastLeader string
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			addr, id, isLeader := rn.Leader()
			cur := id + "@" + addr
			if cur != lastLeader {
				if isLeader {
					fmt.Fprintf(os.Stderr, "[leader] this node is leader (id=%s addr=%s)\n", id, addr)
				} else if id != "" {
					fmt.Fprintf(os.Stderr, "[leader] follower; leader=%s addr=%s\n", id, addr)
				}
				lastLeader = cur
			}
		}
	}
}
