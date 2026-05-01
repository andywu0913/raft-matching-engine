// Package auth — POC stub. The match engine reads the client_id from an
// `x-client-id` gRPC metadata header set by an upstream gateway/auth proxy.
// In production this would be a JWT/HMAC verifier.
package auth

import (
	"context"
	"errors"

	"google.golang.org/grpc/metadata"
)

const headerClientID = "x-client-id"

var ErrMissingClient = errors.New("missing x-client-id header")

func ClientFromCtx(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", ErrMissingClient
	}
	vals := md.Get(headerClientID)
	if len(vals) == 0 || vals[0] == "" {
		return "", ErrMissingClient
	}
	return vals[0], nil
}
