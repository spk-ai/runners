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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func newAnchorMigrationPool(t *testing.T, ctx context.Context, base *pgxpool.Config, cutoff string) *pgxpool.Pool {
	t.Helper()
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
		if entry.Name() >= cutoff {
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
	return pool
}

func testResourceAnchorMigration(t *testing.T, ctx context.Context, base *pgxpool.Config) {
	pool := newAnchorMigrationPool(t, ctx, base, "0023")
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
					var err error
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
            'volumes', (SELECT jsonb_agg(to_jsonb(v) - ARRAY['resource_anchor', 'anchor_reservation', 'anchored_removal_observation'] ORDER BY id) FROM volumes v),
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

func testResourceAnchorThreadMigration(t *testing.T, ctx context.Context, base *pgxpool.Config) {
	pool := newAnchorMigrationPool(t, ctx, base, "0024")
	runner, owner := uuid.NewString(), uuid.NewString()
	if _, err := pool.Exec(ctx, "INSERT INTO runners (id, name, identity_id, service_token_hash, status) VALUES ($1, 'anchor-thread-migration', $2, $3, 'enrolled')", runner, uuid.NewString(), hashServiceToken(uuid.NewString())); err != nil {
		t.Fatal(err)
	}
	client, _ := preparedRegistryClient(t, preRetirementMigrationPool{pool})
	created, err := client.CreateVolumeChecked(ctx, &runnersv1.CreateVolumeCheckedRequest{Volume: &runnersv1.CreateVolumeRequest{
		Id: uuid.NewString(), RunnerId: runner, OwnerId: owner, ThreadId: owner, AgentId: uuid.NewString(), OrganizationId: uuid.NewString(),
		OwnerKind: runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE, VolumeId: uuid.NewString(), SizeGb: "1", Status: runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING}})
	if err != nil {
		t.Fatal(err)
	}
	v := created.Volume
	reserved, err := client.CreateAnchoredWorkload(ctx, &runnersv1.CreateAnchoredWorkloadRequest{Preparation: preparedCreateRequest(v)})
	if err != nil {
		t.Fatal(err)
	}
	w := reserved.Workload
	record, err := scanWorkload(pool.QueryRow(ctx, "SELECT "+workloadColumns+" FROM workloads WHERE id = $1", w.Meta.Id))
	if err != nil {
		t.Fatal(err)
	}
	work := registryTestAnchor(record, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD, record.Meta.ID, "")
	volume := registryTestAnchor(record, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME, uuid.MustParse(v.Meta.Id), "")
	if _, err := client.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{Id: v.Meta.Id, ExpectedRevision: 1, Operation: &runnersv1.UpdateVolumeCheckedRequest_BindAnchor{
		BindAnchor: &runnersv1.BindVolumeResourceAnchor{Anchor: volume, WorkloadId: w.Meta.Id, ExpectedPreparationRevision: 1, ExpectedAnchorRevision: 1}}}); err != nil {
		t.Fatal(err)
	}
	bound, err := client.BindWorkloadResourceAnchors(ctx, &runnersv1.BindWorkloadResourceAnchorsRequest{Id: w.Meta.Id, ExpectedPreparationRevision: 1,
		ExpectedAnchorRevision: 1, WorkloadAnchor: work, VolumeAnchors: []*runnerv1.ResourceAnchor{volume}})
	if err != nil {
		t.Fatal(err)
	}
	prior, err := client.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: w.Meta.Id})
	if err != nil || !proto.Equal(prior.GetWorkload().GetPreparation(), bound.Workload.Preparation) {
		t.Fatal("pre-upgrade read changed the bound resource identities")
	}
	actual := proto.Clone(work).(*runnerv1.ResourceAnchor)
	actual.IdentityLabels["thread-id"] = uuid.NewString()
	valid := func(anchor *runnerv1.ResourceAnchor) bool {
		t.Helper()
		data, err := protojson.Marshal(anchor)
		if err != nil {
			t.Fatal(err)
		}
		var accepted bool
		if err := pool.QueryRow(ctx, `SELECT valid_registry_resource_anchor($1::jsonb,$2,$3,$4,$5,$6,$7,$8)`, data,
			anchor.Kind.String(), record.Meta.ID, record.Preparation.BackendId, record.OwnerKind, record.OwnerID, record.AgentID, record.ThreadID).Scan(&accepted); err != nil {
			t.Fatal(err)
		}
		return accepted
	}
	if valid(actual) || !valid(work) {
		t.Fatal("schema 0023 did not reproduce the native thread mismatch")
	}
	// Preserve a bound anchored PVC, not only an unused ownership reservation.
	physical := lifecycleTestInstance(v)
	physical.Anchor = volume
	if _, err := client.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{Id: v.Meta.Id, ExpectedRevision: 2,
		Operation: &runnersv1.UpdateVolumeCheckedRequest_Bind{Bind: &runnersv1.BindVolumeInstance{Instance: physical}}}); err != nil {
		t.Fatal(err)
	}
	snapshot := func() string {
		var data string
		if err := pool.QueryRow(ctx, `SELECT jsonb_build_object(
            'volumes', (SELECT jsonb_agg(to_jsonb(v) - 'anchored_removal_observation' ORDER BY id) FROM volumes v),
            'workloads', (SELECT jsonb_agg(to_jsonb(w) ORDER BY id) FROM workloads w),
            'guards', (SELECT jsonb_agg(to_jsonb(g) ORDER BY owner_kind,owner_id) FROM runtime_volume_admission_guards g))::text`).Scan(&data); err != nil {
			t.Fatal(err)
		}
		return data
	}
	before := snapshot()
	for i := 0; i < 2; i++ {
		if err := db.ApplyMigrations(ctx, pool); err != nil {
			t.Fatal(err)
		}
		if snapshot() != before || !valid(actual) || !valid(work) {
			t.Fatal("thread migration changed prior ownership or rejected a valid inbox thread")
		}
		var invented int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM volumes WHERE anchored_removal_observation IS NOT NULL").Scan(&invented); err != nil || invented != 0 {
			t.Fatal("retirement migration invented native absence")
		}
	}
	for _, malformed := range []string{"", uuid.Nil.String(), "not-a-thread", uuid.NewString() + " "} {
		actual.IdentityLabels["thread-id"] = malformed
		if valid(actual) {
			t.Fatal("database accepted a noncanonical native inbox thread")
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE workloads SET resource_anchors = jsonb_set(resource_anchors,'{workload,identityLabels,thread-id}',to_jsonb($2::text)) WHERE id=$1`, w.Meta.Id, uuid.NewString()); status.Code(toStatusError(err)) != codes.FailedPrecondition {
		t.Fatal("thread migration allowed an already-bound inbox thread to change")
	}
	read, err := client.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: w.Meta.Id})
	if err != nil || !proto.Equal(read.GetWorkload(), prior.Workload) || snapshot() != before {
		t.Fatal("existing anchor or registry legacy identity changed after upgrade")
	}
}
