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
package server

import (
	"context"
	"errors"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "raft-matching-engine/gen/matchengine/v1"
	"raft-matching-engine/internal/auth"
	"raft-matching-engine/internal/fsm"
	"raft-matching-engine/internal/raftnode"
)

const defaultApplyTimeout = 250 * time.Millisecond

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
		applyTimeout: defaultApplyTimeout,
		now:          func() int64 { return time.Now().UnixNano() },
	}
}

// ----------------------------------------------------------------------------
// PlaceOrder
// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------
// CancelOrder
// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------
// LookupByRequestID
// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------
// GetTopOfBook
// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------
// Join — cluster admin
// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------
// Validation (gateway-side; FSM trusts the log)
// ----------------------------------------------------------------------------

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

// Compile-time assertion: Server implements the generated service.
var _ pb.MatchEngineServiceServer = (*Server)(nil)
