// Package server implements the gRPC intake for the match engine.
//
// Mutating RPCs build a LogEntry, call raft.Apply on the leader, and wait
// for the FSM to return the ApplyResult. Followers reject writes with a
// NOT_LEADER error carrying the leader hint as a status detail; clients
// retry against the leader.
//
// Read RPCs (LookupByRequestID, GetTopOfBook) talk directly to the FSM
// under its read lock and do NOT go through raft.Apply. Per the design
// agreement reads are leader-only for the POC.
//
// File layout — one RPC per file:
//
//	server.go               Server struct + shared helpers (requireLeader, applyEntry, isReplay)
//	place_order.go          PlaceOrder
//	cancel_order.go         CancelOrder
//	lookup_by_request_id.go LookupByRequestID
//	get_top_of_book.go      GetTopOfBook
//	join.go                 Join (cluster admin)
package server

import (
	"errors"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "raft-matching-engine/proto_gen/matchengine/v1"
	"raft-matching-engine/internal/fsm"
	"raft-matching-engine/internal/raftnode"
)

// Compile-time assertion: Server implements the generated service.
var _ pb.MatchEngineServiceServer = (*Server)(nil)

type Server struct {
	pb.UnimplementedMatchEngineServiceServer

	raft         *raft.Raft
	node         *raftnode.Node // for cluster admin (AddVoter)
	fsm          *fsm.FSM
	applyTimeout time.Duration
	now          func() int64 // injectable for tests
}

func New(node *raftnode.Node, f *fsm.FSM) *Server {
	return &Server{
		raft:         node.Raft,
		node:         node,
		fsm:          f,
		applyTimeout: 1 * time.Second,
		now:          func() int64 { return time.Now().UnixNano() },
	}
}

// requireLeader returns nil on the leader, or a NOT_LEADER status with the
// leader-hint detail on followers.
func (s *Server) requireLeader() error {
	if s.raft.State() == raft.Leader {
		return nil
	}
	leaderAddr, leaderID := s.raft.LeaderWithID()
	st := status.New(codes.FailedPrecondition, "not_leader")
	dst, err := st.WithDetails(&pb.NotLeader{
		LeaderId:      string(leaderID),
		LeaderAddress: string(leaderAddr),
	})
	if err != nil {
		return st.Err()
	}
	return dst.Err()
}

// applyEntry serializes a LogEntry, calls raft.Apply, and returns the FSM
// result. Any error here is treated as ambiguous — clients should resolve
// via LookupByRequestID before retrying.
func (s *Server) applyEntry(entry *pb.LogEntry) (fsm.ApplyResult, error) {
	data, err := proto.Marshal(entry)
	if err != nil {
		return fsm.ApplyResult{}, status.Error(codes.Internal, "marshal log entry: "+err.Error())
	}
	f := s.raft.Apply(data, s.applyTimeout)
	if err := f.Error(); err != nil {
		// Could be timeout, lost leadership, raft shutdown. Result is ambiguous.
		if errors.Is(err, raft.ErrNotLeader) {
			return fsm.ApplyResult{}, s.requireLeader()
		}
		return fsm.ApplyResult{}, status.Error(codes.Unavailable, "apply: "+err.Error())
	}
	res, ok := f.Response().(fsm.ApplyResult)
	if !ok {
		return fsm.ApplyResult{}, status.Error(codes.Internal, "unexpected fsm response type")
	}
	return res, nil
}

// isReplay reports whether the FSM result was served from the dedup cache.
// We detect this by comparing the cached WrittenAtNs to the current entry's
// TsNanos: if they differ, this Apply hit an existing dedup entry.
func isReplay(res fsm.ApplyResult, entry *pb.LogEntry) bool {
	return res.WrittenAtNs != entry.TsNanos
}
