package main

import (
	"context"
	"slices"
	"testing"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/runners/internal/rpcauth"
	"google.golang.org/grpc/health"
)

func TestNewGRPCServerRegistersOnlyClassifiedServices(t *testing.T) {
	authorizer, err := rpcauth.Setup(context.Background(), rpcauth.Config{Mode: rpcauth.ModePermissive}, nil)
	if err != nil {
		t.Fatal(err)
	}
	server, err := newGRPCServer(authorizer, runnersv1.UnimplementedRunnersServiceServer{}, health.NewServer())
	if err != nil {
		t.Fatalf("classified registration rejected: %v", err)
	}
	defer server.Stop()
	var services []string
	for name := range server.GetServiceInfo() {
		services = append(services, name)
	}
	slices.Sort(services)
	want := []string{rpcauth.RunnersServiceName, rpcauth.HealthServiceName}
	if !slices.Equal(services, want) {
		t.Fatalf("registered services %v, want %v", services, want)
	}
}
