package main

import (
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/runners/internal/rpcauth"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// newGRPCServer registers the Runners and health services behind the caller
// authorizer and refuses to return a server with any unclassified method.
// It never sets an UnknownServiceHandler or registers reflection: grpc-go
// answers unknown methods with Unimplemented before interceptors run.
func newGRPCServer(authorizer *rpcauth.Authorizer, runners runnersv1.RunnersServiceServer, health healthpb.HealthServer) (*grpc.Server, error) {
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(authorizer.UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(authorizer.StreamServerInterceptor()),
	)
	runnersv1.RegisterRunnersServiceServer(server, runners)
	healthpb.RegisterHealthServer(server, health)
	if err := authorizer.VerifyServer(server); err != nil {
		server.Stop()
		return nil, err
	}
	return server, nil
}
