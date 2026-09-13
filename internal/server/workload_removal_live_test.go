package server

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/runners/internal/db"
	"github.com/agynio/runners/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This test uses a disposable loopback database, never the deployed platform DB.
func TestLiveWorkloadRemovalConfirmation(t *testing.T) {
	dsn := os.Getenv("AGYN_RUNNERS_REMOVAL_TEST_DSN")
	if dsn == "" {
		t.Skip("set AGYN_RUNNERS_REMOVAL_TEST_DSN for disposable PostgreSQL acceptance")
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid test database configuration")
	}
	if config.ConnConfig.Host != "127.0.0.1" || config.ConnConfig.Database != "a2a_removal_acceptance" {
		t.Fatal("requires the disposable a2a_removal_acceptance database on 127.0.0.1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := pgx.ConnectConfig(ctx, config.ConnConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	schema := pgx.Identifier{"removal_" + strings.ReplaceAll(uuid.NewString(), "-", "")}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanup, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	// Build the preceding real schema and insert a terminal record before the
	// new migration. The migration must not turn billing history into proof.
	entries, err := fs.ReadDir(migrations.Files, ".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() >= "0017" {
			continue
		}
		content, err := migrations.Files.ReadFile(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(content)); err != nil {
			t.Fatalf("migration %s: %v", entry.Name(), err)
		}
		if _, err := pool.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", entry.Name()); err != nil {
			t.Fatal(err)
		}
	}
	runnerID, legacyID, ownerID, orgID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	serviceToken := uuid.NewString()
	if _, err := pool.Exec(ctx, "INSERT INTO runners (id, name, identity_id, service_token_hash, status) VALUES ($1, 'removal-test', $2, $3, 'enrolled')", runnerID, uuid.New(), hashServiceToken(serviceToken)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO workloads (id, runner_id, thread_id, agent_id, organization_id, status, owner_kind, owner_id, removed_at) VALUES ($1, $2, $3, $4, $5, 'failed', 'agent_instance', $6, NOW())", legacyID, runnerID, uuid.New(), uuid.New(), orgID, ownerID); err != nil {
		t.Fatal(err)
	}
	if err := db.ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := db.ApplyMigrations(ctx, pool); err != nil {
		t.Fatalf("repeat migration: %v", err)
	}
	s := New(Options{Pool: pool})
	legacy, err := s.getWorkloadByID(ctx, legacyID)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.RemovedAt == nil || legacy.RemovalConfirmedAt != nil {
		t.Fatal("historical removal was inferred")
	}

	threadID, agentID := uuid.New(), uuid.New()
	workload, err := s.insertWorkload(ctx, workloadInsertInput{
		ID: uuid.New(), RunnerID: runnerID, ThreadID: &threadID, AgentID: &agentID,
		OrganizationID: orgID, Status: workloadStatusRunning, ContainersJSON: []byte("[]"),
		OwnerKind: runtimeOwnerKindAgentInstance, OwnerID: ownerID,
	})
	if err != nil {
		t.Fatal(err)
	}
	report, err := s.ReportWorkloadState(ctx, &runnersv1.ReportWorkloadStateRequest{
		ServiceToken: serviceToken, RunnerId: runnerID.String(), WorkloadId: workload.Meta.ID.String(),
		Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !report.Applied {
		t.Fatal("failure report was not applied")
	}
	failed, err := s.getWorkloadByID(ctx, workload.Meta.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Status != workloadStatusFailed || failed.RemovedAt == nil || failed.RemovalConfirmedAt != nil {
		t.Fatal("failure reporting must end metering without confirming removal")
	}
	confirmedAt := time.Now().UTC().Truncate(time.Microsecond)
	for attempt := 0; attempt < 2; attempt++ {
		result, err := s.UpdateWorkload(ctx, &runnersv1.UpdateWorkloadRequest{
			Id: workload.Meta.ID.String(), RemovalConfirmedAt: timestamppb.New(confirmedAt.Add(time.Duration(attempt) * time.Second)),
		})
		if err != nil {
			t.Fatal(err)
		}
		if result.Workload.RemovalConfirmedAt == nil || !result.Workload.RemovalConfirmedAt.AsTime().Equal(confirmedAt) {
			t.Fatal("confirmation was lost or overwritten")
		}
		if !result.Workload.RemovedAt.AsTime().Equal(*failed.RemovedAt) {
			t.Fatal("confirmation changed billing end")
		}
	}
	// The storage constraint closes the race between the RPC's state read and
	// another writer; even an old writer cannot reopen a confirmed workload.
	if _, err := pool.Exec(ctx, "UPDATE workloads SET status = 'running' WHERE id = $1", workload.Meta.ID); err != nil {
		var violation *pgconn.PgError
		if !errors.As(err, &violation) || violation.Code != "23514" || violation.ConstraintName != "workload_removal_confirmation_terminal" {
			t.Fatalf("unexpected reopening failure: %v", err)
		}
	} else {
		t.Fatal("database allowed a confirmed workload to reopen")
	}
	reader, err := pgx.ConnectConfig(ctx, config.ConnConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close(context.Background())
	var persisted time.Time
	if err := reader.QueryRow(ctx, "SELECT removal_confirmed_at FROM workloads WHERE id = $1", workload.Meta.ID).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if !persisted.Equal(confirmedAt) {
		t.Fatal("confirmation did not persist across connections")
	}
	t.Log("real PostgreSQL: historical rows remain unverified; runner failure ends only billing; explicit confirmation is durable and immutable; reopening is rejected")
}
