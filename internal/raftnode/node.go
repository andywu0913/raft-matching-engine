// Package raftnode wires up hashicorp/raft with a BoltStore-backed log/stable
// store, a FileSnapshotStore, and a TCP transport. Supports three first-start
// modes via Config: Bootstrap (single-node seed), Join (handled by main after
// startup), and Recover (force a single-node cluster from existing on-disk
// state — used for catastrophic loss when the original peers are gone).
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

	"raft-matching-engine/internal/fsm"
)

type Config struct {
	NodeID        string
	BindAddr      string // raft TCP listen address, e.g. 127.0.0.1:7000
	AdvertiseAddr string // optional: address peers should use to reach us; falls back to BindAddr
	DataDir       string // where logs, stable store, and snapshots live
	Bootstrap     bool   // first-time single-node bootstrap
	Recover       bool   // force a single-node cluster from existing data dir (DR)

	// Tuning knobs (optional; sensible defaults applied if zero).
	SnapshotInterval  time.Duration
	SnapshotThreshold uint64
}

type Node struct {
	Raft      *raft.Raft
	Transport *raft.NetworkTransport
	dataDir   string
}

func New(cfg Config, runtimeFSM *fsm.FSM) (*Node, error) {
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

	// Recovery runs BEFORE NewRaft. raft.RecoverCluster reads existing state,
	// rewrites the cluster configuration to a single-node config (this node),
	// writes a fresh snapshot, and truncates the log. Per the godoc the FSM
	// it touches is left in an unusable state, so we feed it a throwaway and
	// let NewRaft restore the runtime FSM from the recovered snapshot.
	if cfg.Recover {
		hasState, err := raft.HasExistingState(logStore, stableStore, snapStore)
		if err != nil {
			return nil, fmt.Errorf("recover: check existing state: %w", err)
		}
		if !hasState {
			return nil, errors.New("recover: data dir has no existing raft state")
		}
		recoverConfig := raft.Configuration{Servers: []raft.Server{{
			Suffrage: raft.Voter,
			ID:       rcfg.LocalID,
			Address:  transport.LocalAddr(),
		}}}
		throwaway := fsm.New()
		if err := raft.RecoverCluster(rcfg, throwaway, logStore, stableStore, snapStore, transport, recoverConfig); err != nil {
			return nil, fmt.Errorf("recover cluster: %w", err)
		}
	}

	r, err := raft.NewRaft(rcfg, runtimeFSM, logStore, stableStore, snapStore, transport)
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
