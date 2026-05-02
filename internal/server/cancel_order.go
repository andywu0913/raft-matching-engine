package server

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "raft-matching-engine/proto_gen/matchengine/v1"
	"raft-matching-engine/internal/auth"
	"raft-matching-engine/internal/fsm"
)

func (s *Server) CancelOrder(ctx context.Context, req *pb.CancelOrderRequest) (*pb.CancelOrderResponse, error) {
	clientID, err := auth.ClientFromCtx(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	if err := validateCancel(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.requireLeader(); err != nil {
		return nil, err
	}

	entry := &pb.LogEntry{
		Type:      pb.EventType_EVENT_TYPE_CANCEL_ORDER,
		ClientId:  clientID,
		RequestId: req.RequestId,
		TsNanos:   s.now(),
		Payload: &pb.LogEntry_Cancel{Cancel: &pb.CancelOrderPayload{
			Symbol:  req.Symbol,
			OrderId: req.OrderId,
		}},
	}
	res, err := s.applyEntry(entry)
	if err != nil {
		return nil, err
	}
	if res.Type != fsm.EventCancelOrder {
		return nil, status.Error(codes.AlreadyExists, "request_id already used for a different op")
	}
	if res.Err != "" {
		switch res.Err {
		case "not_found":
			return nil, status.Error(codes.NotFound, res.Err)
		case "forbidden":
			return nil, status.Error(codes.PermissionDenied, res.Err)
		default:
			return nil, status.Error(codes.FailedPrecondition, res.Err)
		}
	}
	return &pb.CancelOrderResponse{
		OrderId:  res.OrderID,
		Replayed: isReplay(res, entry),
	}, nil
}

func validateCancel(r *pb.CancelOrderRequest) error {
	if r.RequestId == "" {
		return errors.New("request_id required")
	}
	if r.Symbol == "" {
		return errors.New("symbol required")
	}
	if r.OrderId == 0 {
		return errors.New("order_id required")
	}
	return nil
}
