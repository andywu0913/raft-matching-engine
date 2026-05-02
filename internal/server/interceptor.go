package server

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "raft-matching-engine/proto_gen/matchengine/v1"
)

// LoggingInterceptor logs every unary RPC: method, duration, gRPC code,
// client_id (from the x-client-id metadata header), request_id (from the
// request body for ops that carry one), and the error message if any.
//
// Wired in main via grpc.NewServer(grpc.UnaryInterceptor(LoggingInterceptor)).
func LoggingInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	dur := time.Since(start)

	code := status.Code(err).String()
	cid := clientIDFromCtx(ctx)
	rid := requestIDFromReq(req)

	if err != nil {
		log.Printf("[rpc] method=%s code=%s duration=%s client_id=%q request_id=%q error=%q",
			info.FullMethod, code, dur, cid, rid, err.Error())
	} else {
		log.Printf("[rpc] method=%s code=%s duration=%s client_id=%q request_id=%q",
			info.FullMethod, code, dur, cid, rid)
	}
	return resp, err
}

// clientIDFromCtx peeks the x-client-id header without erroring (the auth
// package returns an error when missing, which would be too noisy here).
func clientIDFromCtx(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	v := md.Get("x-client-id")
	if len(v) == 0 {
		return ""
	}
	return v[0]
}

// requestIDFromReq pulls the request_id out of the proto when present.
// Each mutating/lookup RPC carries one; reads (GetTopOfBook) and admin
// (Join) don't.
func requestIDFromReq(req any) string {
	switch r := req.(type) {
	case *pb.PlaceOrderRequest:
		return r.RequestId
	case *pb.CancelOrderRequest:
		return r.RequestId
	case *pb.LookupByRequestIDRequest:
		return r.RequestId
	}
	return ""
}
