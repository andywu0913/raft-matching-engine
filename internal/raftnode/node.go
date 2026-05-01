// Package raftnode wires up hashicorp/raft with a BoltStore-backed log/stable
// store, a FileSnapshotStore, and a TCP transport. For phase 1 it bootstraps
// a single-node cluster on first start; multi-node Join is deferred.
package raftnode

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
)

type Config struct {
	NodeID       string
	BindAddr     string // raft TCP listen address, e.g. 127.0.0.1:7000
	AdvertiseAddr string // optional: address peers should use to reach us; falls back to BindAddr
	DataDir      string // where logs, stable store, and snapshots live
	Bootstrap    bool   // first-time single-node bootstrap

	// Tuning knobs (optional; sensible defaults applied if zero).
	SnapshotInterval  time.Duration
	SnapshotThreshold uint64
}

type Node struct {
	Raft      *raft.Raft
	Transport *raft.NetworkTransport
	dataDir   string
}

func New(cfg Config, fsm raft.FSM) (*Node, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("raftnode: NodeID required")
	}
	if cfg.BindAddr == "" {
		return nil, errors.New("raftnode: BindAddr required")
	}
	if cfg.DataDir == "" {
		return nil, errors.New("raftnode: DataDir required")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir data dir: %w", err)
	}

	rcfg := raft.DefaultConfig()
	rcfg.LocalID = raft.ServerID(cfg.NodeID)
	if cfg.SnapshotInterval > 0 {
		rcfg.SnapshotInterval = cfg.SnapshotInterval
	}
	if cfg.SnapshotThreshold > 0 {
		rcfg.SnapshotThreshold = cfg.SnapshotThreshold
	}

	// Log + stable stores share a single bolt file for the POC.
	logStore, err := boltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-log.bolt"))
	if err != nil {
		return nil, fmt.Errorf("bolt log store: %w", err)
	}
	stableStore, err := boltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-stable.bolt"))
	if err != nil {
		return nil, fmt.Errorf("bolt stable store: %w", err)
	}

	snapStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("file snapshot store: %w", err)
	}

	advertise := cfg.AdvertiseAddr
	if advertise == "" {
		advertise = cfg.BindAddr
	}
	addr, err := net.ResolveTCPAddr("tcp", advertise)
	if err != nil {
		return nil, fmt.Errorf("resolve advertise: %w", err)
	}
	transport, err := raft.NewTCPTransport(cfg.BindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("tcp transport: %w", err)
	}

	r, err := raft.NewRaft(rcfg, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		return nil, fmt.Errorf("new raft: %w", err)
	}

	if cfg.Bootstrap {
		// HasExistingState is the safety check: if there's already a log on
		// disk we MUST NOT bootstrap again (it would create a divergent log).
		hasState, err := raft.HasExistingState(logStore, stableStore, snapStore)
		if err != nil {
			return nil, fmt.Errorf("check existing state: %w", err)
		}
		if !hasState {
			cluster := raft.Configuration{Servers: []raft.Server{{
				ID:      rcfg.LocalID,
				Address: transport.LocalAddr(),
			}}}
			if err := r.BootstrapCluster(cluster).Error(); err != nil {
				return nil, fmt.Errorf("bootstrap: %w", err)
			}
		}
	}

	return &Node{Raft: r, Transport: transport, dataDir: cfg.DataDir}, nil
}

// Shutdown blocks until the raft node is fully stopped.
func (n *Node) Shutdown() error {
	if n.Raft == nil {
		return nil
	}
	return n.Raft.Shutdown().Error()
}

// Leader returns (leaderAddr, leaderID, isLeader). When isLeader is true the
// caller can safely call Apply. When false, the leaderAddr/leaderID are a
// hint for redirecting clients (may be empty during an election).
func (n *Node) Leader() (string, string, bool) {
	addr, id := n.Raft.LeaderWithID()
	return string(addr), string(id), n.Raft.State() == raft.Leader
}

// AddVoter adds a new voting member to the cluster. Idempotent: re-adding
// an existing (id, address) pair is a no-op; updating the address of an
// existing id replaces it. Must be called on the leader.
func (n *Node) AddVoter(id, addr string) error {
	// prevIndex=0 means "no precondition on cluster config index".
	f := n.Raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 10*time.Second)
	return f.Error()
}
