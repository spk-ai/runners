package server

import (
	"context"
	"testing"

	"github.com/agynio/runners/internal/rpcauth"
	"google.golang.org/grpc/metadata"
)

// The interceptor strips the caller token, and the forwarding allowlist is a
// second guard: a runners caller token must never reach a downstream service.
func TestOutgoingContextNeverForwardsCallerToken(t *testing.T) {
	incoming := metadata.Pairs(rpcauth.CallerTokenMetadataKey, "a.b.c", "authorization", "Bearer x", identityMetadata, "7f2d9b8e-3a51-4c8e-9d0b-5e6f7a8b9c0d")
	out, _ := metadata.FromOutgoingContext(outgoingContext(metadata.NewIncomingContext(context.Background(), incoming)))
	if len(out.Get(rpcauth.CallerTokenMetadataKey)) != 0 || len(out.Get("authorization")) != 0 {
		t.Fatalf("forwarded %v", out)
	}
	if len(out.Get(identityMetadata)) != 1 {
		t.Fatalf("identity not forwarded: %v", out)
	}
}
