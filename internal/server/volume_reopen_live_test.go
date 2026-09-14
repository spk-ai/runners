package server

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/runners/internal/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestLiveVolumeReopen(t *testing.T) {
	dsn := os.Getenv("AGYN_RUNNERS_VOLUME_TEST_DSN")
	if dsn == "" {
		t.Skip("set AGYN_RUNNERS_VOLUME_TEST_DSN for disposable PostgreSQL acceptance")
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid test database configuration")
	}
	if config.ConnConfig.Host != "127.0.0.1" || config.ConnConfig.Database != "runners_volume_acceptance" {
		t.Fatal("requires the disposable runners_volume_acceptance database on 127.0.0.1")
	}
	for _, fallback := range config.ConnConfig.Fallbacks {
		if fallback.Host != "127.0.0.1" {
			t.Fatal("non-loopback database fallback is not allowed")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin, err := pgx.ConnectConfig(ctx, config.ConnConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	schemaName := "volume_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	schema := pgx.Identifier{schemaName}.Sanitize()
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
	config.MaxConns = 16
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.ConnConfig.RuntimeParams["application_name"] = schemaName
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := db.ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	reader, err := pgx.ConnectConfig(ctx, config.ConnConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close(context.Background()) })
	srv := New(Options{Pool: pool})
	runnerIDs := []uuid.UUID{uuid.New(), uuid.New()}
	for _, id := range runnerIDs {
		if _, err := pool.Exec(ctx, "INSERT INTO runners (id, name, identity_id, service_token_hash, status) VALUES ($1, 'volume-test', $2, $3, 'enrolled')", id, uuid.New(), hashServiceToken(uuid.NewString())); err != nil {
			t.Fatal(err)
		}
	}
	newRequest := func(sandbox bool) *runnersv1.CreateVolumeRequest {
		req := &runnersv1.CreateVolumeRequest{
			Id: uuid.NewString(), RunnerId: runnerIDs[0].String(), OrganizationId: uuid.NewString(),
			VolumeDefinitionId: ptr(uuid.NewString()), SizeGb: "10", Status: runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING,
			OwnerKind:       runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
			AgentInstanceId: ptr(uuid.NewString()), AgentClassId: ptr(uuid.NewString()), ThreadId: uuid.NewString(),
		}
		if sandbox {
			req.OwnerKind = runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX
			req.OwnerId = req.GetAgentInstanceId()
			req.AgentInstanceId, req.AgentClassId, req.ThreadId = nil, nil, ""
		}
		return req
	}
	clone := func(req *runnersv1.CreateVolumeRequest) *runnersv1.CreateVolumeRequest {
		return proto.Clone(req).(*runnersv1.CreateVolumeRequest)
	}
	snapshot := func(t *testing.T, id string) string {
		t.Helper()
		var value string
		if err := reader.QueryRow(ctx, "SELECT to_jsonb(volumes)::text FROM volumes WHERE id = $1", id).Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	closedVolume := func(t *testing.T, req *runnersv1.CreateVolumeRequest, closed runnersv1.VolumeStatus) *runnersv1.Volume {
		t.Helper()
		if _, err := srv.CreateVolume(ctx, req); err != nil {
			t.Fatal(err)
		}
		old := timestamppb.New(time.Now().Add(-time.Hour).UTC().Truncate(time.Microsecond))
		resp, err := srv.UpdateVolume(ctx, &runnersv1.UpdateVolumeRequest{
			Id: req.Id, Status: &closed, InstanceId: ptr("old-pvc-" + req.Id), RemovedAt: old, LastMeteringSampledAt: old,
		})
		if err != nil {
			t.Fatal(err)
		}
		return resp.Volume
	}
	assertReopened := func(t *testing.T, previous, current *runnersv1.Volume, desired runnersv1.VolumeStatus, size string) {
		t.Helper()
		if current.GetStatus() != desired || current.GetSizeGb() != size || current.InstanceId != nil || current.RemovedAt != nil ||
			current.LastMeteringSampledAt == nil || !current.LastMeteringSampledAt.AsTime().After(previous.LastMeteringSampledAt.AsTime()) {
			t.Fatal("reopen did not reset backing instance and metering for the new generation")
		}
		// Normalize only the fields a new provisioning generation may change.
		expected := proto.Clone(previous).(*runnersv1.Volume)
		expected.Status, expected.SizeGb = desired, size
		expected.InstanceId, expected.RemovedAt = nil, nil
		expected.LastMeteringSampledAt = current.LastMeteringSampledAt
		expected.Meta.UpdatedAt = current.Meta.UpdatedAt
		expected.LifecycleRevision++
		if !proto.Equal(expected, current) {
			t.Fatal("reopen changed persistent volume identity or creation time")
		}
		persisted, err := scanVolume(reader.QueryRow(ctx, "SELECT "+volumeColumns+" FROM volumes WHERE id = $1", current.Meta.Id))
		if err != nil {
			t.Fatal(err)
		}
		stored, err := toProtoVolume(persisted)
		if err != nil || !proto.Equal(stored, current) {
			t.Fatalf("reopened response differs from an independent database read: %v", err)
		}
	}

	for _, closed := range []runnersv1.VolumeStatus{runnersv1.VolumeStatus_VOLUME_STATUS_FAILED, runnersv1.VolumeStatus_VOLUME_STATUS_DELETED} {
		t.Run(closed.String(), func(t *testing.T) {
			for _, kind := range []string{"canonical", "legacy", "unspecified-owner-kind", "sandbox"} {
				for _, desired := range []runnersv1.VolumeStatus{runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING, runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE} {
					t.Run("same-owner/"+kind+"/"+desired.String(), func(t *testing.T) {
						req := newRequest(kind == "sandbox")
						previous := closedVolume(t, req, closed)
						req.Status, req.SizeGb = desired, "12"
						switch kind {
						case "legacy":
							req.VolumeId, req.AgentId, req.OwnerId = req.GetVolumeDefinitionId(), req.GetAgentClassId(), req.GetAgentInstanceId()
							req.VolumeDefinitionId, req.AgentClassId, req.AgentInstanceId = nil, nil, nil
						case "unspecified-owner-kind":
							req.OwnerKind = runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_UNSPECIFIED
						case "canonical":
							// Canonical definition/class fields still take precedence over aliases.
							req.VolumeId, req.AgentId = uuid.NewString(), uuid.NewString()
						}
						resp, err := srv.CreateVolume(ctx, req)
						if err != nil {
							t.Fatal(err)
						}
						assertReopened(t, previous, resp.Volume, desired, "12")
					})
				}
			}
			mismatches := []struct {
				name   string
				mutate func(*runnersv1.CreateVolumeRequest)
			}{
				{"owner", func(r *runnersv1.CreateVolumeRequest) { r.AgentInstanceId = ptr(uuid.NewString()) }},
				{"organization", func(r *runnersv1.CreateVolumeRequest) { r.OrganizationId = uuid.NewString() }},
				{"agent-class", func(r *runnersv1.CreateVolumeRequest) { r.AgentClassId = ptr(uuid.NewString()) }},
				{"thread", func(r *runnersv1.CreateVolumeRequest) { r.ThreadId = uuid.NewString() }},
				{"definition", func(r *runnersv1.CreateVolumeRequest) { r.VolumeDefinitionId = ptr(uuid.NewString()) }},
				{"runner", func(r *runnersv1.CreateVolumeRequest) { r.RunnerId = runnerIDs[1].String() }},
				{"owner-kind", func(r *runnersv1.CreateVolumeRequest) {
					r.OwnerKind, r.OwnerId = runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX, r.GetAgentInstanceId()
					r.AgentInstanceId = nil
				}},
			}
			for _, tt := range mismatches {
				t.Run("reject/"+tt.name, func(t *testing.T) {
					req := newRequest(false)
					closedVolume(t, req, closed)
					before := snapshot(t, req.Id)
					tt.mutate(req)
					_, err := srv.CreateVolume(ctx, req)
					after := snapshot(t, req.Id)
					if status.Code(err) != codes.AlreadyExists || before != after {
						t.Fatalf("identity mismatch: code=%s, row unchanged=%t", status.Code(err), before == after)
					}
				})
			}
			for _, field := range []string{"thread", "agent-class"} {
				for _, remove := range []bool{false, true} {
					name := "null-to-value"
					if remove {
						name = "value-to-null"
					}
					t.Run("reject/sandbox-"+field+"/"+name, func(t *testing.T) {
						req := newRequest(true)
						set := func(value string) {
							if field == "thread" {
								req.ThreadId = value
							} else {
								req.AgentClassId = ptr(value)
							}
						}
						if remove {
							set(uuid.NewString())
						}
						closedVolume(t, req, closed)
						before := snapshot(t, req.Id)
						if remove {
							set("")
						} else {
							set(uuid.NewString())
						}
						_, err := srv.CreateVolume(ctx, req)
						if status.Code(err) != codes.AlreadyExists || before != snapshot(t, req.Id) {
							t.Fatalf("nullable identity mismatch was accepted or mutated the row: %v", err)
						}
					})
				}
			}
		})
	}

	for _, open := range []runnersv1.VolumeStatus{runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING, runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE, runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING} {
		t.Run("open-conflict/"+open.String(), func(t *testing.T) {
			req := newRequest(false)
			req.Status = open
			if _, err := srv.CreateVolume(ctx, req); err != nil {
				t.Fatal(err)
			}
			before := snapshot(t, req.Id)
			for _, otherOwner := range []bool{false, true} {
				retry := clone(req)
				retry.Status = runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING
				if otherOwner {
					retry.AgentInstanceId = ptr(uuid.NewString())
				}
				_, err := srv.CreateVolume(ctx, retry)
				if status.Code(err) != codes.AlreadyExists || before != snapshot(t, req.Id) {
					t.Fatalf("open volume was changed by a create conflict: %v", err)
				}
			}
		})
	}

	t.Run("checked-lifecycle", func(t *testing.T) {
		testCheckedVolumeLifecycle(t, ctx, srv, pool, reader, newRequest)
	})

	t.Run("concurrent-reopen", func(t *testing.T) {
		req := newRequest(false)
		previous := closedVolume(t, req, runnersv1.VolumeStatus_VOLUME_STATUS_FAILED)
		lock, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Rollback(context.Background())
		if _, err := lock.Exec(ctx, "SELECT id FROM volumes WHERE id = $1 FOR UPDATE", req.Id); err != nil {
			t.Fatal(err)
		}
		type outcome struct {
			foreign bool
			resp    *runnersv1.CreateVolumeResponse
			err     error
		}
		const contenders = 8
		results := make(chan outcome, contenders)
		for i := 0; i < contenders; i++ {
			retry := clone(req)
			foreign := i%2 == 1
			if foreign {
				retry.AgentInstanceId = ptr(uuid.NewString())
			}
			go func() {
				resp, err := srv.CreateVolume(ctx, retry)
				results <- outcome{foreign, resp, err}
			}()
		}
		// Hold the row until all legitimate contenders are blocked in PostgreSQL;
		// merely starting goroutines does not prove their SQL overlapped.
		blocked := 0
		var waitErr error
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			waitErr = admin.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND cardinality(pg_blocking_pids(pid)) > 0", schemaName).Scan(&blocked)
			if waitErr != nil || blocked >= contenders/2 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		releaseErr := lock.Rollback(ctx)
		successes := 0
		for i := 0; i < contenders; i++ {
			result := <-results
			if result.err == nil {
				successes++
				if result.foreign {
					t.Error("a different owner won the reopen race")
				} else {
					assertReopened(t, previous, result.resp.Volume, req.Status, req.SizeGb)
				}
			} else if status.Code(result.err) != codes.AlreadyExists {
				t.Errorf("unexpected concurrent result: %v", result.err)
			}
		}
		if waitErr != nil || releaseErr != nil || blocked < contenders/2 || successes != 1 {
			t.Fatalf("concurrency proof: blocked=%d successes=%d wait=%v release=%v", blocked, successes, waitErr, releaseErr)
		}
	})
}
