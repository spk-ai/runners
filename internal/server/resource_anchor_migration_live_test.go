package server

import (
	"context"
	"io/fs"
	"strings"
	"testing"
	"time"

	runnerv1 "github.com/agynio/runners/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/runners/internal/db"
	"github.com/agynio/runners/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/encoding/protojson"
)

func testResourceAnchorMigration(t *testing.T, ctx context.Context, base *pgxpool.Config) {
	config := base.Copy()
	admin, err := pgx.ConnectConfig(ctx, config.ConnConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	schema := pgx.Identifier{"anchors_" + strings.ReplaceAll(uuid.NewString(), "-", "")}.Sanitize()
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
	entries, err := fs.ReadDir(migrations.Files, ".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() >= "0023" {
			continue
		}
		content, err := migrations.Files.ReadFile(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(content)); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", entry.Name()); err != nil {
			t.Fatal(err)
		}
	}
	runner := uuid.NewString()
	if _, err := pool.Exec(ctx, "INSERT INTO runners (id, name, identity_id, service_token_hash, status) VALUES ($1, 'anchor-migration', $2, $3, 'enrolled')", runner, uuid.NewString(), hashServiceToken(uuid.NewString())); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []runnersv1.RuntimeOwnerKind{runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE, runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX} {
		for _, phase := range []string{"reserved", "preparing", "bound", "activating", "active", "removing", "removed"} {
			for _, empty := range []bool{false, true} {
				raw := &runnersv1.CreateVolumeRequest{Id: uuid.NewString(), RunnerId: runner, OrganizationId: uuid.NewString(), OwnerKind: kind,
					OwnerId: uuid.NewString(), VolumeId: uuid.NewString(), SizeGb: "1", Status: runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING}
				if kind == runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE {
					raw.ThreadId, raw.AgentId = uuid.NewString(), uuid.NewString()
				}
				v := &runnersv1.Volume{RunnerId: runner, OrganizationId: raw.OrganizationId, OwnerKind: kind, OwnerId: raw.OwnerId, ThreadId: raw.ThreadId, AgentId: raw.AgentId}
				var instance *runnerv1.VolumeListItem
				if !empty {
					v, err = createMigrationVolume(ctx, pool, raw, true)
					if err != nil {
						t.Fatal(err)
					}
					instance = lifecycleTestInstance(v)
					if err := adoptionBindSQL(ctx, pool, v, instance); err != nil {
						t.Fatal(err)
					}
				}
				request := &runnersv1.CreatePreparedWorkloadRequest{BackendId: lifecycleTestBackend, VolumeIds: []string{}, Workload: &runnersv1.CreateWorkloadRequest{
					Id: uuid.NewString(), RunnerId: runner, OrganizationId: raw.OrganizationId, ThreadId: raw.ThreadId, AgentId: raw.AgentId,
					OwnerKind: kind, OwnerId: raw.OwnerId, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING}}
				if !empty {
					request.VolumeIds = []string{v.Meta.Id}
				}
				if err := preparedInsertSQL(ctx, pool, request); err != nil {
					t.Fatal(err)
				}
				binding := &runnerv1.WorkloadBinding{WorkloadId: request.Workload.Id, InstanceUid: uuid.NewString(), BackendId: lifecycleTestBackend}
				if instance != nil {
					binding.Volumes = []*runnerv1.VolumeListItem{instance}
				}
				encoded, err := protojson.Marshal(binding)
				if err != nil {
					t.Fatal(err)
				}
				observation, err := protojson.Marshal(&runnerv1.RemovePreparedWorkloadResponse{State: runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_ABSENT, Binding: binding})
				if err != nil {
					t.Fatal(err)
				}
				for _, target := range []string{"preparing", "bound", "activating", "active", "removing", "removed"} {
					if phase == "reserved" {
						break
					}
					var native, removed []byte
					if target != "preparing" {
						native = encoded
					}
					if target == "removed" {
						removed = observation
					}
					_, err := pool.Exec(ctx, `UPDATE workloads SET preparation_phase = $2, preparation_revision = preparation_revision + 1,
                        prepared_binding = $3, prepared_removal_observation = $4, instance_id = CASE WHEN $3::jsonb IS NOT NULL THEN id::text END,
                        status = CASE WHEN $2 = 'removed' THEN 'stopped' ELSE status END,
                        removal_confirmed_at = CASE WHEN $2 = 'removed' THEN NOW() END, removed_at = CASE WHEN $2 = 'removed' THEN NOW() END
                        WHERE id = $1`, request.Workload.Id, target, native, removed)
					if err != nil {
						t.Fatalf("historical phase %s: %v", target, err)
					}
					if target == phase {
						break
					}
				}
			}
		}
	}
	snapshot := func() string {
		var data string
		if err := pool.QueryRow(ctx, `SELECT jsonb_build_object(
            'volumes', (SELECT jsonb_agg(to_jsonb(v) - ARRAY['resource_anchor', 'anchor_reservation'] ORDER BY id) FROM volumes v),
            'workloads', (SELECT jsonb_agg(to_jsonb(w) - 'resource_anchors' ORDER BY id) FROM workloads w),
            'guards', (SELECT jsonb_agg(to_jsonb(g) - 'resource_anchors_required' ORDER BY owner_kind, owner_id) FROM runtime_volume_admission_guards g))::text`).Scan(&data); err != nil {
			t.Fatal(err)
		}
		return data
	}
	before := snapshot()
	for i := 0; i < 2; i++ {
		if err := db.ApplyMigrations(ctx, pool); err != nil {
			t.Fatal(err)
		}
		assertNoInferredResourceAnchors(t, ctx, pool)
		var applied, workloads, volumes int
		if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM schema_migrations WHERE version = '0023_resource_anchors.sql'),
            (SELECT count(*) FROM workloads), (SELECT count(*) FROM volumes)`).Scan(&applied, &workloads, &volumes); err != nil {
			t.Fatal(err)
		}
		if applied != 1 || workloads != 28 || volumes != 14 || snapshot() != before {
			t.Fatal("anchor migration changed prepared history or failed to commit exactly once")
		}
	}
}
