package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	runnerv1 "github.com/agynio/runners/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Real registry RPCs and PostgreSQL, under the parent's disposable database gate.
// Native owner identities and Pod/PVC observations are synthetic in this fixture.
func testResourceAnchors(t *testing.T, ctx context.Context, pool *pgxpool.Pool, reader *pgx.Conn, newRequest func(bool) *runnersv1.CreateVolumeRequest) {
	t.Run("reservation-race", func(t *testing.T) { testResourceAnchorReservationRace(t, ctx, pool, reader, newRequest) })
	for _, sandbox := range []bool{false, true} {
		for _, volumes := range []int{0, 1, 2, 3} {
			t.Run(fmt.Sprintf("sandbox=%t/volumes=%d", sandbox, volumes), func(t *testing.T) {
				client, stop := preparedRegistryClient(t, pool)
				raw := newRequest(sandbox)
				flavor := "admission-" + uuid.NewString()
				if _, err := pool.Exec(ctx, "INSERT INTO workload_flavor_admission (runner_id, flavor, capacity) VALUES ($1,$2,1)", raw.RunnerId, flavor); err != nil {
					t.Fatal(err)
				}
				assertSlot := func(want int) {
					t.Helper()
					var got int
					if err := reader.QueryRow(ctx, "SELECT occupied FROM workload_flavor_admission WHERE runner_id=$1 AND flavor=$2", raw.RunnerId, flavor).Scan(&got); err != nil || got != want {
						t.Fatalf("admission occupied=%d want=%d error=%v", got, want, err)
					}
				}
				var claims []*runnersv1.Volume
				for i := 0; i < volumes; i++ {
					r := proto.Clone(raw).(*runnersv1.CreateVolumeRequest)
					r.Id, r.VolumeDefinitionId = uuid.NewString(), ptr(uuid.NewString())
					v, err := client.CreateVolumeChecked(ctx, &runnersv1.CreateVolumeCheckedRequest{Volume: r})
					if err != nil {
						t.Fatal(err)
					}
					if i == 0 && volumes == 3 {
						failed, err := client.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{Id: v.Volume.Meta.Id, ExpectedRevision: v.Volume.LifecycleRevision,
							Operation: &runnersv1.UpdateVolumeCheckedRequest_FailProvisioning{FailProvisioning: &runnersv1.FailVolumeProvisioning{}}})
						if err != nil {
							t.Fatal(err)
						}
						reopened, err := client.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{Id: v.Volume.Meta.Id, ExpectedRevision: failed.Volume.LifecycleRevision,
							Operation: &runnersv1.UpdateVolumeCheckedRequest_Reopen{Reopen: &runnersv1.ReopenVolume{Volume: r}}})
						if err != nil || reopened.GetVolume().GetLifecycleRevision() != 3 {
							t.Fatalf("unused checked reopen: %v", err)
						}
						v.Volume = reopened.Volume
					}
					claims = append(claims, v.Volume)
				}
				req := &runnersv1.CreatePreparedWorkloadRequest{BackendId: lifecycleTestBackend,
					Workload: &runnersv1.CreateWorkloadRequest{Id: uuid.NewString(), RunnerId: raw.RunnerId, OrganizationId: raw.OrganizationId,
						ThreadId: raw.ThreadId, AgentClassId: raw.AgentClassId, OwnerKind: raw.OwnerKind, OwnerId: raw.OwnerId,
						AgentInstanceId: raw.AgentInstanceId, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING}}
				for _, v := range claims {
					req.VolumeIds = append(req.VolumeIds, v.Meta.Id)
				}
				human := uuid.NewString()
				req.Workload.Flavor = flavor
				for turn := 0; turn < 2; turn++ {
					req.Workload.Id = uuid.NewString()
					created, err := client.CreateAnchoredWorkload(ctx, &runnersv1.CreateAnchoredWorkloadRequest{Preparation: req})
					if err != nil {
						t.Fatal(err)
					}
					w := created.Workload
					assertSlot(1)
					if w.Preparation.Resources.GetRevision() != 1 || w.Preparation.Resources.Workload != nil || w.Preparation.Revision != 1 {
						t.Fatal("reservation invented native authority")
					}
					operation := func(name string, b *runnerv1.WorkloadBinding) *runnersv1.UpdatePreparedWorkloadRequest {
						op := preparedOperation(name, b)
						op.Id, op.ExpectedRevision = w.Meta.Id, w.Preparation.Revision
						return op
					}
					step := func(name string, b *runnerv1.WorkloadBinding) {
						t.Helper()
						previous := w.Preparation
						r, err := client.UpdateAnchoredWorkload(ctx, &runnersv1.UpdateAnchoredWorkloadRequest{Operation: operation(name, b), ExpectedAnchorRevision: previous.Resources.Revision})
						if err != nil {
							t.Fatalf("%s: %v", name, err)
						}
						w = r.Workload
						if w.RemovalConfirmedAt == nil {
							assertSlot(1)
						} else {
							assertSlot(0)
						}
						if w.Preparation.Revision != previous.Revision+1 || w.Preparation.Resources.Revision != previous.Resources.Revision+1 {
							t.Fatal("transition did not advance both revisions")
						}
					}
					if _, err := client.UpdatePreparedWorkload(ctx, operation("prepare", nil)); status.Code(err) != codes.FailedPrecondition {
						t.Fatalf("old API admitted anchored work: %v", err)
					}
					if _, err := client.UpdateAnchoredWorkload(ctx, &runnersv1.UpdateAnchoredWorkloadRequest{Operation: operation("prepare", nil), ExpectedAnchorRevision: 1}); status.Code(err) != codes.FailedPrecondition {
						t.Fatalf("prepare before anchor persistence: %v", err)
					}
					record, err := scanWorkload(reader.QueryRow(ctx, "SELECT "+workloadColumns+" FROM workloads WHERE id = $1", w.Meta.Id))
					if err != nil {
						t.Fatal(err)
					}
					anchor := registryTestAnchor(record, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD, record.Meta.ID, human)
					if !sandbox {
						anchor.IdentityLabels["thread-id"] = uuid.NewString()
					}
					var volumeAnchors []*runnerv1.ResourceAnchor
					for _, v := range claims {
						a := v.ResourceAnchor
						if a == nil {
							a = registryTestAnchor(record, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME, uuid.MustParse(v.Meta.Id), human)
						}
						volumeAnchors = append(volumeAnchors, a)
					}
					bind := &runnersv1.BindWorkloadResourceAnchorsRequest{Id: w.Meta.Id, ExpectedPreparationRevision: 1, ExpectedAnchorRevision: 1, WorkloadAnchor: anchor, VolumeAnchors: volumeAnchors}
					if turn == 0 && volumes > 0 {
						if _, err := client.BindWorkloadResourceAnchors(ctx, bind); status.Code(err) != codes.FailedPrecondition {
							t.Fatalf("workload acquired unpersisted volume owner: %v", err)
						}
					}
					for i, v := range claims {
						if v.ResourceAnchor != nil {
							continue
						}
						oldBinding := &runnersv1.UpdateVolumeCheckedRequest{Id: v.Meta.Id, ExpectedRevision: v.LifecycleRevision, Operation: &runnersv1.UpdateVolumeCheckedRequest_Bind{Bind: &runnersv1.BindVolumeInstance{Instance: lifecycleTestInstance(v)}}}
						if _, err := client.UpdateVolumeChecked(ctx, oldBinding); status.Code(err) != codes.FailedPrecondition {
							t.Fatalf("old volume binding escaped owner pin: %v", err)
						}
						binding := &runnersv1.BindVolumeResourceAnchor{Anchor: volumeAnchors[i], WorkloadId: w.Meta.Id, ExpectedPreparationRevision: 1, ExpectedAnchorRevision: 1}
						request := &runnersv1.UpdateVolumeCheckedRequest{Id: v.Meta.Id, ExpectedRevision: v.LifecycleRevision,
							Operation: &runnersv1.UpdateVolumeCheckedRequest_BindAnchor{BindAnchor: binding}}
						if v.LifecycleRevision > 1 {
							if _, err := client.UpdateVolumeChecked(ctx, request); status.Code(err) != codes.FailedPrecondition {
								t.Fatalf("legacy operation allocated reopened volume: %v", err)
							}
							request.Operation = &runnersv1.UpdateVolumeCheckedRequest_BindReopenedAnchor{BindReopenedAnchor: binding}
						}
						bound, err := client.UpdateVolumeChecked(ctx, request)
						if err != nil {
							t.Fatal(err)
						}
						if i == 0 && volumes == 3 && bound.Volume.AnchorReservation.GetAllocationRevision() != 4 {
							t.Fatal("reopened allocation lost its original revision")
						}
						claims[i] = bound.Volume
						if !proto.Equal(bound.Volume.ResourceAnchor, volumeAnchors[i]) || bound.Volume.BoundInstance != nil {
							t.Fatal("anchor binding invented a PVC")
						}
					}
					bound, err := client.BindWorkloadResourceAnchors(ctx, bind)
					if err != nil {
						t.Fatal(err)
					}
					w = bound.Workload
					if w.Preparation.Revision != 1 || w.Preparation.Resources.Revision != 2 {
						t.Fatal("anchor binding modified preparation revision")
					}
					if _, err := client.BindWorkloadResourceAnchors(ctx, bind); status.Code(err) != codes.Aborted {
						t.Fatalf("stale binding retried: %v", err)
					}
					_, err = pool.Exec(ctx, "UPDATE workloads SET preparation_phase = 'preparing', preparation_revision = preparation_revision + 1 WHERE id = $1", w.Meta.Id)
					if status.Code(toStatusError(err)) != codes.FailedPrecondition {
						t.Fatalf("old SQL writer bypassed anchor revision: %v", err)
					}
					step("prepare", nil)
					binding := &runnerv1.WorkloadBinding{WorkloadId: w.Meta.Id, BackendId: lifecycleTestBackend, InstanceUid: uuid.NewString(), Anchor: anchor}
					for i, v := range claims {
						instance := v.BoundInstance
						if instance == nil {
							instance = lifecycleTestInstance(v)
							instance.Anchor = v.ResourceAnchor
							if sandbox {
								instance.IdentityLabels["sandbox-owner-id"] = human
							}
							r, err := client.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{Id: v.Meta.Id, ExpectedRevision: v.LifecycleRevision,
								Operation: &runnersv1.UpdateVolumeCheckedRequest_Bind{Bind: &runnersv1.BindVolumeInstance{Instance: instance}}})
							if err != nil {
								t.Fatal(err)
							}
							claims[i] = r.Volume
						}
						binding.Volumes = append(binding.Volumes, instance)
					}
					for _, op := range []string{"bind", "activate", "active", "remove", "removed"} {
						step(op, binding)
						if op == "remove" {
							for _, v := range claims {
								_, err := client.UpdateVolumeChecked(ctx, beginRetirementRequest(v))
								if status.Code(err) != codes.FailedPrecondition {
									t.Fatalf("retirement bypassed unconfirmed workload: %v", err)
								}
								stored, err := scanVolume(reader.QueryRow(ctx, "SELECT "+volumeColumns+" FROM volumes WHERE id=$1", v.Meta.Id))
								if err != nil || stored.RemovalIntent != nil || stored.LifecycleRevision != int64(v.LifecycleRevision) {
									t.Fatal("rejected retirement changed persistent state")
								}
							}
						}
					}
					if w.RemovalConfirmedAt == nil || !proto.Equal(w.Preparation.Resources.Workload, anchor) {
						t.Fatal("removed workload lost its ownership history")
					}
					for _, sql := range []string{
						"DELETE FROM workloads WHERE id = $1",
						"UPDATE workloads SET resource_anchors = NULL WHERE id = $1",
						"UPDATE runtime_volume_admission_guards SET resource_anchors_required = FALSE WHERE owner_id = (SELECT owner_id FROM workloads WHERE id = $1)",
					} {
						_, err := pool.Exec(ctx, sql, w.Meta.Id)
						if status.Code(toStatusError(err)) != codes.FailedPrecondition {
							t.Fatalf("immutable anchor history changed: %v", err)
						}
					}
					legacy := proto.Clone(req).(*runnersv1.CreatePreparedWorkloadRequest)
					legacy.Workload.Id = uuid.NewString()
					if _, err := client.CreatePreparedWorkload(ctx, legacy); status.Code(err) != codes.FailedPrecondition {
						t.Fatalf("owner returned to old capability: %v", err)
					}
					for _, v := range claims {
						_, err := client.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{Id: v.Meta.Id, ExpectedRevision: v.LifecycleRevision,
							Operation: &runnersv1.UpdateVolumeCheckedRequest_BeginRemoval{BeginRemoval: &runnersv1.BeginVolumeRemoval{}}})
						if status.Code(err) != codes.FailedPrecondition {
							t.Fatalf("anchored volume retired through old contract: %v", err)
						}
						read, err := scanVolume(reader.QueryRow(ctx, "SELECT "+volumeColumns+" FROM volumes WHERE id = $1", v.Meta.Id))
						if err != nil || !proto.Equal(read.ResourceAnchor, v.ResourceAnchor) || !proto.Equal(read.BoundInstance, v.BoundInstance) {
							t.Fatalf("persistent workspace identity changed: %v", err)
						}
					}
					// Recreate the gRPC server between turns; state comes from PostgreSQL.
					stop()
					client, stop = preparedRegistryClient(t, pool)
				}
				if len(claims) > 0 {
					isolation := []pgx.TxIsoLevel{pgx.ReadCommitted, pgx.RepeatableRead, pgx.Serializable}[volumes-1]
					if !t.Run("retirement-admission", func(t *testing.T) { testAnchoredRetirementAdmission(t, ctx, pool, reader, claims[0], isolation) }) {
						return
					}
				}
				for _, v := range claims {
					t.Run("retirement/"+v.Meta.Id, func(t *testing.T) { testAnchoredVolumeRetirementRPC(t, ctx, pool, reader, v) })
				}
			})
		}
	}
}

func testResourceAnchorReservationRace(t *testing.T, ctx context.Context, pool *pgxpool.Pool, reader *pgx.Conn, newRequest func(bool) *runnersv1.CreateVolumeRequest) {
	client, _ := preparedRegistryClient(t, pool)
	for _, sandbox := range []bool{false, true} {
		for _, isolation := range []pgx.TxIsoLevel{pgx.ReadCommitted, pgx.RepeatableRead, pgx.Serializable} {
			for _, commit := range []bool{false, true} {
				t.Run(fmt.Sprintf("sandbox=%t/%s/commit=%t", sandbox, isolation, commit), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
					defer cancel()
					volume, err := client.CreateVolumeChecked(ctx, &runnersv1.CreateVolumeCheckedRequest{Volume: newRequest(sandbox)})
					if err != nil {
						t.Fatal(err)
					}
					v := volume.Volume
					request := preparedCreateRequest(v)
					created, err := client.CreateAnchoredWorkload(ctx, &runnersv1.CreateAnchoredWorkloadRequest{Preparation: request})
					if err != nil {
						t.Fatal(err)
					}
					w := created.Workload
					record, err := scanWorkload(reader.QueryRow(ctx, "SELECT "+workloadColumns+" FROM workloads WHERE id = $1", w.Meta.Id))
					if err != nil {
						t.Fatal(err)
					}
					anchor := registryTestAnchor(record, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME, uuid.MustParse(v.Meta.Id), uuid.NewString())
					loser, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: isolation})
					if err != nil {
						t.Fatal(err)
					}
					defer loser.Rollback(context.Background())
					var initial int
					if err := loser.QueryRow(ctx, "SELECT preparation_revision FROM workloads WHERE id = $1", w.Meta.Id).Scan(&initial); err != nil || initial != 1 {
						t.Fatalf("initial snapshot: %v", err)
					}
					winner, err := pool.Begin(ctx)
					if err != nil {
						t.Fatal(err)
					}
					defer winner.Rollback(context.Background())
					_, err = winner.Exec(ctx, `UPDATE workloads SET preparation_phase = 'removed', preparation_revision = 2,
                        resource_anchors = '{"revision":"2"}', status = 'stopped', removed_at = NOW(), removal_confirmed_at = NOW() WHERE id = $1`, w.Meta.Id)
					if err != nil {
						t.Fatal(err)
					}
					// A replacement reservation has the same owner but grants no authority to the old request.
					_, err = winner.Exec(ctx, `INSERT INTO workloads (id, runner_id, organization_id, thread_id, agent_id, owner_kind, owner_id, status,
                        preparation_phase, preparation_revision, prepared_backend_id, prepared_volume_ids, resource_anchors)
                        SELECT $2, runner_id, organization_id, thread_id, agent_id, owner_kind, owner_id, 'starting',
                        'reserved', 1, prepared_backend_id, prepared_volume_ids, '{"revision":"1"}' FROM workloads WHERE id = $1`, w.Meta.Id, uuid.NewString())
					if err != nil {
						t.Fatal(err)
					}
					done := make(chan error, 1)
					go func() {
						srv := New(Options{Pool: loser})
						_, err := srv.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{Id: v.Meta.Id, ExpectedRevision: v.LifecycleRevision,
							Operation: &runnersv1.UpdateVolumeCheckedRequest_BindAnchor{BindAnchor: &runnersv1.BindVolumeResourceAnchor{Anchor: anchor,
								WorkloadId: w.Meta.Id, ExpectedPreparationRevision: 1, ExpectedAnchorRevision: 1}}})
						if err == nil {
							err = loser.Commit(ctx)
						} else {
							_ = loser.Rollback(ctx)
						}
						done <- err
					}()
					waitErr := waitAdmissionBlocked(ctx, reader, winner.Conn().PgConn().PID(), loser.Conn().PgConn().PID())
					_, otherErr := client.CreateVolumeChecked(ctx, &runnersv1.CreateVolumeCheckedRequest{Volume: newRequest(sandbox)})
					if commit {
						err = winner.Commit(ctx)
					} else {
						err = winner.Rollback(ctx)
					}
					result := <-done
					if waitErr != nil || otherErr != nil || err != nil {
						t.Fatalf("overlap: wait=%v other=%v release=%v", waitErr, otherErr, err)
					}
					if commit && status.Code(result) != codes.FailedPrecondition && status.Code(result) != codes.Aborted {
						t.Fatalf("old reservation wrote under replacement authority: %v", result)
					}
					if !commit && result != nil {
						t.Fatalf("rollback denied the original reservation: %v", result)
					}
					stored, err := scanVolume(reader.QueryRow(ctx, "SELECT "+volumeColumns+" FROM volumes WHERE id = $1", v.Meta.Id))
					if err != nil {
						t.Fatal(err)
					}
					if commit && (stored.ResourceAnchor != nil || uint64(stored.LifecycleRevision) != v.LifecycleRevision) || !commit && !proto.Equal(stored.ResourceAnchor, anchor) {
						t.Fatal("reservation race changed the wrong volume state")
					}
				})
			}
		}
	}
}
