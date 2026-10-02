package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	agentsv1 "github.com/agynio/runners/.gen/go/agynio/api/agents/v1"
	authorizationv1 "github.com/agynio/runners/.gen/go/agynio/api/authorization/v1"
	identityv1 "github.com/agynio/runners/.gen/go/agynio/api/identity/v1"
	notificationsv1 "github.com/agynio/runners/.gen/go/agynio/api/notifications/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	zitimanagementv1 "github.com/agynio/runners/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/runners/internal/config"
	"github.com/agynio/runners/internal/db"
	"github.com/agynio/runners/internal/rpcauth"
	"github.com/agynio/runners/internal/server"
	"github.com/agynio/runners/internal/zitimanager"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("runners: %v", err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	// Caller authorization is validated before any side effect: a missing
	// policy or TokenReview RBAC must not cost a migration run or a fresh
	// ephemeral Ziti identity on every crash-looping restart.
	authorizer, err := rpcauth.Setup(ctx, cfg.RPCAuth, nil)
	if err != nil {
		return err
	}
	// The same registration with stub handlers proves every method is
	// classified before anything else starts; the real server repeats it.
	probe, err := newGRPCServer(authorizer, runnersv1.UnimplementedRunnersServiceServer{}, health.NewServer())
	if err != nil {
		return err
	}
	probe.Stop()
	log.Printf("runners: rpc caller authorization mode %s", authorizer.Mode())

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("parse database url: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("create connection pool: %w", err)
	}
	defer pool.Close()

	if err := db.ApplyMigrations(ctx, pool); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	identityConn, err := grpc.DialContext(ctx, cfg.IdentityAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial identity service: %w", err)
	}
	defer identityConn.Close()

	authorizationConn, err := grpc.DialContext(ctx, cfg.AuthorizationAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial authorization service: %w", err)
	}
	defer authorizationConn.Close()

	agentsConn, err := grpc.DialContext(ctx, cfg.AgentsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial agents service: %w", err)
	}
	defer agentsConn.Close()

	zitiManagementConn, err := grpc.DialContext(ctx, cfg.ZitiManagementAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial ziti management service: %w", err)
	}
	defer zitiManagementConn.Close()

	zitiMgmtClient := zitimanagementv1.NewZitiManagementServiceClient(zitiManagementConn)
	zitiManager, err := zitimanager.New(ctx, zitiMgmtClient, cfg.ZitiEnrollmentTimeout, cfg.ZitiLeaseRenewalInterval)
	if err != nil {
		return fmt.Errorf("initialize ziti manager: %w", err)
	}
	defer zitiManager.Close()
	// The identity is ephemeral and garbage-collected once its lease lapses.
	// Losing it is unrecoverable in-process, so terminate and let the restart
	// enroll a fresh one.
	go func() {
		if err := <-zitiManager.IdentityLost(); err != nil {
			log.Fatalf("terminating: %v", err)
		}
	}()
	go zitiManager.RunLeaseRenewal(ctx)

	notificationsConn, err := grpc.DialContext(ctx, cfg.NotificationsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial notifications service: %w", err)
	}
	defer notificationsConn.Close()

	srv := server.New(server.Options{
		Pool:                 pool,
		IdentityClient:       identityv1.NewIdentityServiceClient(identityConn),
		AuthorizationClient:  authorizationv1.NewAuthorizationServiceClient(authorizationConn),
		AgentsClient:         agentsv1.NewAgentsServiceClient(agentsConn),
		ZitiManagementClient: zitiMgmtClient,
		NotificationsClient:  notificationsv1.NewNotificationsServiceClient(notificationsConn),
		ZitiDialer:           zitiManager,
	})
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	grpcServer, err := newGRPCServer(authorizer, srv, healthServer)
	if err != nil {
		return err
	}
	go srv.RunWorkloadActivitySweep(ctx, cfg.WorkloadActivitySweepInterval, cfg.WorkloadKeepaliveGrace)

	listener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.GRPCAddr, err)
	}
	defer listener.Close()

	errCh := make(chan error, 1)
	go func() {
		log.Print("runners: ready")
		if err := grpcServer.Serve(listener); err != nil {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return err
	case <-ctx.Done():
		grpcServer.GracefulStop()
		return nil
	}
}
