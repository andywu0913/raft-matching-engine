package server

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "raft-matching-engine/gen/matchengine/v1"
	"raft-matching-engine/internal/auth"
	"raft-matching-engine/internal/fsm"
)

func (s *Server) PlaceOrder(ctx context.Context, req *pb.PlaceOrderRequest) (*pb.PlaceOrderResponse, error) {
	clientID, err := auth.ClientFromCtx(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	if err := validatePlace(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.requireLeader(); err != nil {
		return nil, err
	}

	entry := &pb.LogEntry{
		Type:      pb.EventType_EVENT_TYPE_PLACE_ORDER,
		ClientId:  clientID,
		RequestId: req.RequestId,
		TsNanos:   s.now(),
		Payload: &pb.LogEntry_Place{Place: &pb.PlaceOrderPayload{
			Symbol:   req.Symbol,
			Side:     req.Side,
			Price:    req.Price,
			Quantity: req.Quantity,
		}},
	}
	res, err := s.applyEntry(entry)
	if err != nil {
		return nil, err
	}
	if res.Type != fsm.EventPlaceOrder {
		// Idempotency hit on a request_id that was previously used for a
		// different op type. Surface as a structured client error.
		return nil, status.Error(codes.AlreadyExists, "request_id already used for a different op")
	}
	if res.Err != "" {
		return nil, status.Error(codes.FailedPrecondition, res.Err)
	}
	return placeResultToProto(res, isReplay(res, entry)), nil
}

func validatePlace(r *pb.PlaceOrderRequest) error {
	if r.RequestId == "" {
		return errors.New("request_id required")
	}
	if r.Symbol == "" {
		return errors.New("symbol required")
	}
	if r.Side != pb.Side_SIDE_BUY && r.Side != pb.Side_SIDE_SELL {
		return errors.New("invalid side")
	}
	if r.Price <= 0 {
		return errors.New("price must be > 0")
	}
	if r.Quantity <= 0 {
		return errors.New("quantity must be > 0")
	}
	return nil
}

// placeResultToProto is shared between PlaceOrder and LookupByRequestID
// (for the cached-place lookup case).
func placeResultToProto(res fsm.ApplyResult, replayed bool) *pb.PlaceOrderResponse {
	out := &pb.PlaceOrderResponse{
		OrderId:    res.OrderID,
		FilledQty:  res.FilledQty,
		RestingQty: res.RestingQty,
		Replayed:   replayed,
	}
	if len(res.Trades) > 0 {
		out.Trades = make([]*pb.Trade, len(res.Trades))
		for i, t := range res.Trades {
			out.Trades[i] = &pb.Trade{
				MakerOrderId: t.MakerOrderID,
				TakerOrderId: t.TakerOrderID,
				Price:        t.Price,
				Quantity:     t.Qty,
				TsNanos:      t.TsNanos,
			}
		}
	}
	return out
}
