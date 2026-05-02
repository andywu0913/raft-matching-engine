package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "raft-matching-engine/gen/matchengine/v1"
)

// Join adds a new voting member to the cluster. Leader-only. Idempotent —
// re-joining an existing (id, address) pair is a no-op.
func (s *Server) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id required")
	}
	if req.RaftAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "raft_address required")
	}
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if err := s.node.AddVoter(req.NodeId, req.RaftAddress); err != nil {
		return nil, status.Error(codes.Internal, "add voter: "+err.Error())
	}
	return &pb.JoinResponse{}, nil
}
