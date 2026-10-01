package server

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"sync"
	"testing"

	runnerv1 "github.com/agynio/runners/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type revocationInterleavingPool struct {
	dbPool
	once   sync.Once
	before func()
}

func (p *revocationInterleavingPool) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	if strings.HasPrefix(strings.TrimSpace(query), "UPDATE workloads SET") {
		p.once.Do(p.before)
	}
	return p.dbPool.QueryRow(ctx, query, args...)
}

// Native receipts are synthetic here. The database, RPC boundary, revision CAS,
// independently read history and competing volume writes are real.
func testPreparationRevocationRegistry(t *testing.T, ctx context.Context, pool *pgxpool.Pool, reader *pgx.Conn, newRequest func(bool) *runnersv1.CreateVolumeRequest) {
	for _, sandbox := range []bool{false, true} {
		for _, mode := range []string{"zero", "absent", "found", "mixed", "late-binding-race"} {
			t.Run(fmt.Sprintf("sandbox=%t/%s", sandbox, mode), func(t *testing.T) {
				client, stop := preparedRegistryClient(t, pool)
				defer func() { stop() }()
				raw := newRequest(sandbox)
				count := 2
				if mode == "zero" {
					count = 0
				}
				var volumes []*runnersv1.Volume
				create := &runnersv1.CreatePreparedWorkloadRequest{BackendId: lifecycleTestBackend,
					Workload: &runnersv1.CreateWorkloadRequest{Id: uuid.NewString(), RunnerId: raw.RunnerId, OrganizationId: raw.OrganizationId,
						ThreadId: raw.ThreadId, AgentClassId: raw.AgentClassId, OwnerKind: raw.OwnerKind, OwnerId: raw.OwnerId,
						AgentInstanceId: raw.AgentInstanceId, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING}}
				for i := 0; i < count; i++ {
					r := proto.Clone(raw).(*runnersv1.CreateVolumeRequest)
					r.Id, r.VolumeDefinitionId = uuid.NewString(), ptr(uuid.NewString())
					response, err := client.CreateVolumeChecked(ctx, &runnersv1.CreateVolumeCheckedRequest{Volume: r})
					if err != nil {
						t.Fatal(err)
					}
					if i == 0 && mode == "absent" {
						failed, err := client.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{Id: response.Volume.Meta.Id, ExpectedRevision: response.Volume.LifecycleRevision,
							Operation: &runnersv1.UpdateVolumeCheckedRequest_FailProvisioning{FailProvisioning: &runnersv1.FailVolumeProvisioning{}}})
						if err != nil {
							t.Fatal(err)
						}
						reopened, err := client.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{Id: response.Volume.Meta.Id, ExpectedRevision: failed.Volume.LifecycleRevision,
							Operation: &runnersv1.UpdateVolumeCheckedRequest_Reopen{Reopen: &runnersv1.ReopenVolume{Volume: r}}})
						if err != nil {
							t.Fatal(err)
						}
						response.Volume = reopened.Volume
					}
					volumes = append(volumes, response.Volume)
					create.VolumeIds = append(create.VolumeIds, response.Volume.Meta.Id)
				}
				created, err := client.CreateAnchoredWorkload(ctx, &runnersv1.CreateAnchoredWorkloadRequest{Preparation: create})
				if err != nil {
					t.Fatal(err)
				}
				w := created.Workload
				read := func() *runnersv1.Workload {
					t.Helper()
					r, err := scanWorkload(reader.QueryRow(ctx, "SELECT "+workloadColumns+" FROM workloads WHERE id=$1", w.Meta.Id))
					if err != nil {
						t.Fatal(err)
					}
					value, err := toProtoWorkload(r)
					if err != nil {
						t.Fatal(err)
					}
					return value
				}
				snapshot := func() string {
					t.Helper()
					var value string
					if err := reader.QueryRow(ctx, "SELECT to_jsonb(workloads)::text FROM workloads WHERE id=$1", w.Meta.Id).Scan(&value); err != nil {
						t.Fatal(err)
					}
					return value
				}
				record, err := scanWorkload(reader.QueryRow(ctx, "SELECT "+workloadColumns+" FROM workloads WHERE id=$1", w.Meta.Id))
				if err != nil {
					t.Fatal(err)
				}
				human := uuid.NewString()
				anchor := registryTestAnchor(record, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD, record.Meta.ID, human)
				var anchors []*runnerv1.ResourceAnchor
				for i, v := range volumes {
					a := registryTestAnchor(record, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME, uuid.MustParse(v.Meta.Id), human)
					binding := &runnersv1.BindVolumeResourceAnchor{Anchor: a, WorkloadId: w.Meta.Id, ExpectedPreparationRevision: 1, ExpectedAnchorRevision: 1}
					request := &runnersv1.UpdateVolumeCheckedRequest{Id: v.Meta.Id, ExpectedRevision: v.LifecycleRevision,
						Operation: &runnersv1.UpdateVolumeCheckedRequest_BindAnchor{BindAnchor: binding}}
					if v.LifecycleRevision > 1 {
						request.Operation = &runnersv1.UpdateVolumeCheckedRequest_BindReopenedAnchor{BindReopenedAnchor: binding}
					}
					bound, err := client.UpdateVolumeChecked(ctx, request)
					if err != nil {
						t.Fatal(err)
					}
					volumes[i], anchors = bound.Volume, append(anchors, a)
				}
				bound, err := client.BindWorkloadResourceAnchors(ctx, &runnersv1.BindWorkloadResourceAnchorsRequest{Id: w.Meta.Id,
					ExpectedPreparationRevision: 1, ExpectedAnchorRevision: 1, WorkloadAnchor: anchor, VolumeAnchors: anchors})
				if err != nil {
					t.Fatal(err)
				}
				w = bound.Workload
				request := func(op *runnersv1.UpdatePreparedWorkloadRequest) *runnersv1.UpdateAnchoredWorkloadRequest {
					op.Id, op.ExpectedRevision = w.Meta.Id, w.Preparation.Revision
					return &runnersv1.UpdateAnchoredWorkloadRequest{Operation: op, ExpectedAnchorRevision: w.Preparation.Resources.Revision}
				}
				step := func(op *runnersv1.UpdatePreparedWorkloadRequest) {
					t.Helper()
					previous := w.Preparation
					response, err := client.UpdateAnchoredWorkload(ctx, request(op))
					if err != nil {
						t.Fatal(err)
					}
					w = response.Workload
					if w.Preparation.Revision != previous.Revision+1 || w.Preparation.Resources.Revision != previous.Resources.Revision+1 || !proto.Equal(w, read()) {
						t.Fatal("checked registry response did not match independently read next revisions")
					}
				}
				step(preparedOperation("prepare", nil))
				step(preparedOperation("remove", nil))
				proof := &runnerv1.PreparationRevocation{WorkloadAnchor: w.Preparation.Resources.Workload,
					VolumeAnchors: w.Preparation.Resources.Volumes, InstanceUid: uuid.NewString()}
				observation := &runnerv1.ObservePreparationRevocationResponse{Revocation: proof, State: runnerv1.RevokedPreparationState_REVOKED_PREPARATION_STATE_POD_ABSENT}
				for i, v := range volumes {
					if mode == "found" || mode == "mixed" && i == 0 {
						item := lifecycleTestInstance(v)
						item.Anchor, item.IdentityLabels = v.ResourceAnchor, maps.Clone(v.ResourceAnchor.IdentityLabels)
						observation.Volumes = append(observation.Volumes, item)
					} else {
						observation.AbsentVolumeIds = append(observation.AbsentVolumeIds, v.Meta.Id)
					}
				}
				recordOp := func() *runnersv1.UpdatePreparedWorkloadRequest {
					return &runnersv1.UpdatePreparedWorkloadRequest{Operation: &runnersv1.UpdatePreparedWorkloadRequest_RecordRevocation{RecordRevocation: &runnersv1.RecordPreparationRevocation{Revocation: proof}}}
				}
				confirmOp := func(obs *runnerv1.ObservePreparationRevocationResponse) *runnersv1.UpdatePreparedWorkloadRequest {
					return &runnersv1.UpdatePreparedWorkloadRequest{Operation: &runnersv1.UpdatePreparedWorkloadRequest_ConfirmRevocation{ConfirmRevocation: &runnersv1.ConfirmPreparationRevocation{Observation: obs}}}
				}
				rawReject := func(next *runnersv1.WorkloadResourceAnchors, phase string) {
					t.Helper()
					next.Revision = w.Preparation.Resources.Revision + 1
					data, err := protojson.Marshal(next)
					if err != nil {
						t.Fatal(err)
					}
					before := snapshot()
					_, err = pool.Exec(ctx, `UPDATE workloads SET resource_anchors=$2, preparation_revision=preparation_revision+1,
						preparation_phase=$3, status=CASE WHEN $3='removed' THEN 'stopped' ELSE status END,
						removal_confirmed_at=CASE WHEN $3='removed' THEN NOW() ELSE removal_confirmed_at END
						WHERE id=$1`, w.Meta.Id, data, phase)
					if status.Code(toStatusError(err)) != codes.FailedPrecondition || snapshot() != before {
						t.Fatalf("raw SQL bypassed revocation guard or failed unexpectedly: %v", err)
					}
				}
				combined := proto.Clone(w.Preparation.Resources).(*runnersv1.WorkloadResourceAnchors)
				combined.PreparationRevocation, combined.RevocationObservation = proof, observation
				rawReject(combined, "removed")
				if _, err := client.UpdateAnchoredWorkload(ctx, request(confirmOp(observation))); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("cleanup skipped durable proof: %v", err)
				}
				if _, err := client.UpdatePreparedWorkload(ctx, request(recordOp()).Operation); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("old API wrote revocation without anchor revision: %v", err)
				}
				stale := request(recordOp())
				step(recordOp())
				stop()
				client, stop = preparedRegistryClient(t, pool)
				w = read()
				if w.RemovalConfirmedAt != nil || w.Preparation.Binding != nil || !proto.Equal(w.Preparation.Resources.PreparationRevocation, proof) {
					t.Fatal("restart lost proof or fabricated completed cleanup")
				}
				if _, err := client.UpdateAnchoredWorkload(ctx, stale); status.Code(err) != codes.Aborted {
					t.Fatal("stale proof replay escaped both revisions")
				}
				create.Workload.Id = uuid.NewString()
				if _, err := client.CreateAnchoredWorkload(ctx, &runnersv1.CreateAnchoredWorkloadRequest{Preparation: create}); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("proof alone released owner admission: %v", err)
				}
				for _, change := range []string{"drop-proof", "replace-proof", "pending", "missing-observation", "bad-partition"} {
					t.Run(change, func(t *testing.T) {
						next := proto.Clone(w.Preparation.Resources).(*runnersv1.WorkloadResourceAnchors)
						phase := "removing"
						switch change {
						case "drop-proof":
							next.PreparationRevocation = nil
						case "replace-proof":
							next.PreparationRevocation.InstanceUid = uuid.NewString()
						case "missing-observation":
							phase = "removed"
						default:
							phase = "removed"
							next.RevocationObservation = proto.Clone(observation).(*runnerv1.ObservePreparationRevocationResponse)
							if change == "pending" {
								next.RevocationObservation.State = runnerv1.RevokedPreparationState_REVOKED_PREPARATION_STATE_PENDING
							} else {
								next.RevocationObservation.AbsentVolumeIds = append(next.RevocationObservation.AbsentVolumeIds, uuid.NewString())
							}
						}
						rawReject(next, phase)
					})
				}
				bindVolume := func(item *runnerv1.VolumeListItem) {
					t.Helper()
					for i, v := range volumes {
						if v.Meta.Id == item.VolumeKey {
							response, err := client.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{Id: v.Meta.Id, ExpectedRevision: v.LifecycleRevision,
								Operation: &runnersv1.UpdateVolumeCheckedRequest_Bind{Bind: &runnersv1.BindVolumeInstance{Instance: item}}})
							if err != nil {
								t.Fatal(err)
							}
							volumes[i] = response.Volume
							return
						}
					}
					t.Fatal("unknown fixture volume")
				}
				if len(observation.Volumes) > 0 {
					next := proto.Clone(w.Preparation.Resources).(*runnersv1.WorkloadResourceAnchors)
					next.RevocationObservation = observation
					rawReject(next, "removed")
					if _, err := client.UpdateAnchoredWorkload(ctx, request(confirmOp(observation))); status.Code(err) != codes.FailedPrecondition {
						t.Fatalf("unbound discovered volume authorized cleanup: %v", err)
					}
				}
				for _, item := range observation.Volumes {
					bindVolume(item)
				}
				if mode == "late-binding-race" {
					item := lifecycleTestInstance(volumes[0])
					item.Anchor, item.IdentityLabels = volumes[0].ResourceAnchor, maps.Clone(volumes[0].ResourceAnchor.IdentityLabels)
					intercepted := false
					race := New(Options{Pool: &revocationInterleavingPool{dbPool: pool, before: func() { intercepted = true; bindVolume(item) }}})
					before := snapshot()
					if _, err := race.UpdateAnchoredWorkload(ctx, request(confirmOp(observation))); status.Code(err) != codes.FailedPrecondition || !intercepted || snapshot() != before {
						t.Fatalf("read-to-CAS race erased a late recorded workspace: %v", err)
					}
					observation.Volumes = append(observation.Volumes, item)
					observation.AbsentVolumeIds = observation.AbsentVolumeIds[1:]
				}
				if len(observation.Volumes) > 0 {
					missing := proto.Clone(observation).(*runnerv1.ObservePreparationRevocationResponse)
					missing.AbsentVolumeIds = append(missing.AbsentVolumeIds, missing.Volumes[0].VolumeKey)
					missing.Volumes = missing.Volumes[1:]
					next := proto.Clone(w.Preparation.Resources).(*runnersv1.WorkloadResourceAnchors)
					next.RevocationObservation = missing
					rawReject(next, "removed")
					if _, err := client.UpdateAnchoredWorkload(ctx, request(confirmOp(missing))); status.Code(err) != codes.FailedPrecondition {
						t.Fatal("known absent workspace silently discarded")
					}
				}
				step(confirmOp(observation))
				stop()
				client, stop = preparedRegistryClient(t, pool)
				w = read()
				if w.RemovalConfirmedAt == nil || w.Preparation.Binding != nil || w.Preparation.RemovalObservation != nil ||
					!proto.Equal(w.Preparation.Resources.RevocationObservation.Revocation, proof) {
					t.Fatal("cleanup did not preserve its distinct durable evidence")
				}
				before := snapshot()
				for _, sql := range []string{
					`UPDATE workloads SET resource_anchors=resource_anchors-'preparationRevocation' WHERE id=$1`,
					`UPDATE workloads SET resource_anchors=resource_anchors-'revocationObservation' WHERE id=$1`,
					`UPDATE workloads SET preparation_phase='preparing', removal_confirmed_at=NULL, status='starting', preparation_revision=preparation_revision+1 WHERE id=$1`,
					`DELETE FROM workloads WHERE id=$1`,
				} {
					if _, err := pool.Exec(ctx, sql, w.Meta.Id); status.Code(toStatusError(err)) != codes.FailedPrecondition || snapshot() != before {
						t.Fatalf("immutable removed history was changed: %v", err)
					}
				}
				admitted, err := client.CreateAnchoredWorkload(ctx, &runnersv1.CreateAnchoredWorkloadRequest{Preparation: create})
				if err != nil {
					t.Fatalf("confirmed revocation did not release owner admission: %v", err)
				}
				abort := preparedOperation("abort", nil)
				abort.Id, abort.ExpectedRevision = admitted.Workload.Meta.Id, 1
				if _, err := client.UpdateAnchoredWorkload(ctx, &runnersv1.UpdateAnchoredWorkloadRequest{Operation: abort, ExpectedAnchorRevision: 1}); err != nil {
					t.Fatal(err)
				}
				for _, expected := range volumes {
					v, err := scanVolume(reader.QueryRow(ctx, "SELECT "+volumeColumns+" FROM volumes WHERE id=$1", expected.Meta.Id))
					if err != nil || !proto.Equal(v.ResourceAnchor, expected.ResourceAnchor) || !proto.Equal(v.AnchorReservation, expected.AnchorReservation) || !proto.Equal(v.BoundInstance, expected.BoundInstance) {
						t.Fatalf("recovery changed retained workspace history: %v", err)
					}
				}
			})
		}
	}
}
