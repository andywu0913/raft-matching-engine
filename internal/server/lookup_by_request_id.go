package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "raft-matching-engine/proto_gen/matchengine/v1"
	"raft-matching-engine/internal/auth"
	"raft-matching-engine/internal/fsm"
)

// LookupByRequestID resolves an ambiguous ack: the client got no response
// (timeout / transport drop / NOT_LEADER) and wants to know whether their
// PlaceOrder/CancelOrder actually committed. Reads the FSM dedup cache
// directly — never goes through raft.Apply.
func (s *Server) LookupByRequestID(ctx context.Context, req *pb.LookupByRequestIDRequest) (*pb.LookupByRequestIDResponse, error) {
	clientID, err := auth.ClientFromCtx(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	if req.RequestId == "" {
		return nil, status.Error(codes.InvalidArgument, "request_id required")
	}
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	res, ok := s.fsm.Lookup(clientID, req.RequestId)
	if !ok {
		return &pb.LookupByRequestIDResponse{Found: false}, nil
	}
	out := &pb.LookupByRequestIDResponse{Found: true}
	switch {
	case res.Err != "":
		out.Result = &pb.LookupByRequestIDResponse_Error{Error: res.Err}
	case res.Type == fsm.EventPlaceOrder:
		out.Result = &pb.LookupByRequestIDResponse_Place{Place: placeResultToProto(res, true)}
	case res.Type == fsm.EventCancelOrder:
		out.Result = &pb.LookupByRequestIDResponse_Cancel{Cancel: &pb.CancelOrderResponse{
			OrderId:  res.OrderID,
			Replayed: true,
		}}
	}
	return out, nil
}
