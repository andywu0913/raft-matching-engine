package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "raft-matching-engine/gen/matchengine/v1"
	"raft-matching-engine/internal/fsm"
)

func (s *Server) GetTopOfBook(ctx context.Context, req *pb.GetTopOfBookRequest) (*pb.GetTopOfBookResponse, error) {
	if req.Symbol == "" {
		return nil, status.Error(codes.InvalidArgument, "symbol required")
	}
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	depth := int(req.Depth)
	if depth <= 0 {
		depth = 5
	}
	bids, asks, ok := s.fsm.TopOfBook(req.Symbol, depth)
	if !ok {
		return &pb.GetTopOfBookResponse{Symbol: req.Symbol}, nil
	}
	return &pb.GetTopOfBookResponse{
		Symbol: req.Symbol,
		Bids:   levelsToProto(bids),
		Asks:   levelsToProto(asks),
	}, nil
}

func levelsToProto(lvls []fsm.LevelView) []*pb.PriceLevel {
	if len(lvls) == 0 {
		return nil
	}
	out := make([]*pb.PriceLevel, len(lvls))
	for i, l := range lvls {
		out[i] = &pb.PriceLevel{
			Price:         l.Price,
			TotalQuantity: l.TotalQty,
			OrderCount:    l.OrderCount,
		}
	}
	return out
}
