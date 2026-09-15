package server

import (
	"context"
	"maps"
	"testing"

	runnerv1 "github.com/agynio/runners/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/runners/internal/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func testPreparationRevocationMigration(t *testing.T, ctx context.Context, base *pgxpool.Config) {
	pool := newAnchorMigrationPool(t, ctx, base, "0026")
	runner := uuid.NewString()
	if _, err := pool.Exec(ctx, "INSERT INTO runners (id,name,identity_id,service_token_hash,status) VALUES ($1,'revocation-migration',$2,$3,'enrolled')", runner, uuid.NewString(), hashServiceToken(uuid.NewString())); err != nil {
		t.Fatal(err)
	}
	client, stop := preparedRegistryClient(t, pool)
	defer stop()
	var interrupted []*runnersv1.Workload
	var history []*runnersv1.Workload
	for _, kind := range []runnersv1.RuntimeOwnerKind{runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE, runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX} {
		for _, phase := range []string{"reserved", "unbound-removing", "bound-removing", "removed"} {
			raw := &runnersv1.CreateVolumeRequest{Id: uuid.NewString(), RunnerId: runner, OwnerKind: kind, OrganizationId: uuid.NewString(), OwnerId: uuid.NewString(), VolumeId: uuid.NewString(), SizeGb: "1", Status: runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING}
			if kind == runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE {
				raw.AgentId, raw.ThreadId = uuid.NewString(), uuid.NewString()
			}
			created, err := client.CreateVolumeChecked(ctx, &runnersv1.CreateVolumeCheckedRequest{Volume: raw})
			if err != nil {
				t.Fatal(err)
			}
			v := created.Volume
			reserved, err := client.CreateAnchoredWorkload(ctx, &runnersv1.CreateAnchoredWorkloadRequest{Preparation: preparedCreateRequest(v)})
			if err != nil {
				t.Fatal(err)
			}
			w := reserved.Workload
			record, err := scanWorkload(pool.QueryRow(ctx, "SELECT "+workloadColumns+" FROM workloads WHERE id=$1", w.Meta.Id))
			if err != nil {
				t.Fatal(err)
			}
			human := uuid.NewString()
			anchor := registryTestAnchor(record, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD, record.Meta.ID, human)
			volume := registryTestAnchor(record, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME, uuid.MustParse(v.Meta.Id), human)
			if _, err := client.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{Id: v.Meta.Id, ExpectedRevision: 1, Operation: &runnersv1.UpdateVolumeCheckedRequest_BindAnchor{
				BindAnchor: &runnersv1.BindVolumeResourceAnchor{Anchor: volume, WorkloadId: w.Meta.Id, ExpectedPreparationRevision: 1, ExpectedAnchorRevision: 1}}}); err != nil {
				t.Fatal(err)
			}
			bound, err := client.BindWorkloadResourceAnchors(ctx, &runnersv1.BindWorkloadResourceAnchorsRequest{Id: w.Meta.Id, ExpectedPreparationRevision: 1, ExpectedAnchorRevision: 1, WorkloadAnchor: anchor, VolumeAnchors: []*runnerv1.ResourceAnchor{volume}})
			if err != nil {
				t.Fatal(err)
			}
			w = bound.Workload
			physical := lifecycleTestInstance(v)
			physical.Anchor, physical.IdentityLabels = volume, maps.Clone(volume.IdentityLabels)
			binding := &runnerv1.WorkloadBinding{WorkloadId: w.Meta.Id, BackendId: w.Preparation.BackendId, InstanceUid: uuid.NewString(), Anchor: anchor, Volumes: []*runnerv1.VolumeListItem{physical}}
			step := func(name string) {
				t.Helper()
				op := preparedOperation(name, binding)
				op.Id, op.ExpectedRevision = w.Meta.Id, w.Preparation.Revision
				response, err := client.UpdateAnchoredWorkload(ctx, &runnersv1.UpdateAnchoredWorkloadRequest{Operation: op, ExpectedAnchorRevision: w.Preparation.Resources.Revision})
				if err != nil {
					t.Fatal(err)
				}
				w = response.Workload
			}
			if phase != "reserved" {
				step("prepare")
				if phase != "unbound-removing" {
					if _, err := client.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{Id: v.Meta.Id, ExpectedRevision: 2,
						Operation: &runnersv1.UpdateVolumeCheckedRequest_Bind{Bind: &runnersv1.BindVolumeInstance{Instance: physical}}}); err != nil {
						t.Fatal(err)
					}
					step("bind")
				}
				step("remove")
				if phase == "removed" {
					step("removed")
				}
			}
			if phase == "unbound-removing" {
				interrupted = append(interrupted, w)
			}
			// GetWorkload enriches display names; compare the same read API on
			// both sides of the migration, in addition to the full SQL snapshot.
			read, err := client.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: w.Meta.Id})
			if err != nil || !proto.Equal(read.GetWorkload().GetPreparation(), w.Preparation) {
				t.Fatal("pre-upgrade read changed the prepared lifecycle")
			}
			history = append(history, read.Workload)
		}
	}
	snapshot := func() string {
		t.Helper()
		var data string
		if err := pool.QueryRow(ctx, `SELECT jsonb_build_object(
            'volumes',(SELECT jsonb_agg(to_jsonb(v) ORDER BY id) FROM volumes v),
            'workloads',(SELECT jsonb_agg(to_jsonb(w) ORDER BY id) FROM workloads w),
            'guards',(SELECT jsonb_agg(to_jsonb(g) ORDER BY owner_kind,owner_id) FROM runtime_volume_admission_guards g))::text`).Scan(&data); err != nil {
			t.Fatal(err)
		}
		return data
	}
	before := snapshot()
	proof := func(w *runnersv1.Workload) *runnerv1.PreparationRevocation {
		return &runnerv1.PreparationRevocation{WorkloadAnchor: w.Preparation.Resources.Workload, VolumeAnchors: w.Preparation.Resources.Volumes, InstanceUid: uuid.NewString()}
	}
	recordProof := func(w *runnersv1.Workload, receipt *runnerv1.PreparationRevocation) (*runnersv1.UpdateAnchoredWorkloadResponse, error) {
		return client.UpdateAnchoredWorkload(ctx, &runnersv1.UpdateAnchoredWorkloadRequest{ExpectedAnchorRevision: w.Preparation.Resources.Revision,
			Operation: &runnersv1.UpdatePreparedWorkloadRequest{Id: w.Meta.Id, ExpectedRevision: w.Preparation.Revision, Operation: &runnersv1.UpdatePreparedWorkloadRequest_RecordRevocation{RecordRevocation: &runnersv1.RecordPreparationRevocation{Revocation: receipt}}}})
	}
	if _, err := recordProof(interrupted[0], proof(interrupted[0])); status.Code(err) != codes.FailedPrecondition || snapshot() != before {
		t.Fatalf("schema 0025 accepted an unsupported revocation or mutated history: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := db.ApplyMigrations(ctx, pool); err != nil {
			t.Fatal(err)
		}
		var applied int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations WHERE version='0026_preparation_revocation.sql'").Scan(&applied); err != nil || applied != 1 || snapshot() != before {
			t.Fatal("revocation migration rewrote anchored workload/volume history or did not commit exactly once")
		}
		for _, old := range history {
			response, err := client.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: old.Meta.Id})
			if err != nil || !proto.Equal(response.GetWorkload(), old) {
				t.Fatal("upgrade changed historical native bindings, ownership or removal evidence")
			}
		}
	}
	for _, old := range interrupted {
		receipt := proof(old)
		recorded, err := recordProof(old, receipt)
		if err != nil {
			t.Fatal(err)
		}
		w := recorded.Workload
		observation := &runnerv1.ObservePreparationRevocationResponse{Revocation: receipt, State: runnerv1.RevokedPreparationState_REVOKED_PREPARATION_STATE_POD_ABSENT, AbsentVolumeIds: w.Preparation.VolumeIds}
		confirmed, err := client.UpdateAnchoredWorkload(ctx, &runnersv1.UpdateAnchoredWorkloadRequest{ExpectedAnchorRevision: w.Preparation.Resources.Revision,
			Operation: &runnersv1.UpdatePreparedWorkloadRequest{Id: w.Meta.Id, ExpectedRevision: w.Preparation.Revision, Operation: &runnersv1.UpdatePreparedWorkloadRequest_ConfirmRevocation{ConfirmRevocation: &runnersv1.ConfirmPreparationRevocation{Observation: observation}}}})
		if err != nil || confirmed.GetWorkload().GetRemovalConfirmedAt() == nil || confirmed.GetWorkload().GetPreparation().GetBinding() != nil {
			t.Fatalf("upgraded interrupted workload could not record distinct revocation cleanup: %v", err)
		}
	}
}
