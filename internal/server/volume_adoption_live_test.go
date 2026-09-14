package server

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	authorizationv1 "github.com/agynio/runners/.gen/go/agynio/api/authorization/v1"
	runnerv1 "github.com/agynio/runners/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Uses only TestLiveVolumeReopen's disposable, loopback PostgreSQL fixture.
func testLegacyVolumeAdoption(t *testing.T, ctx context.Context, pool *pgxpool.Pool, reader *pgx.Conn, newRequest func(bool) *runnersv1.CreateVolumeRequest) {
	srv := New(Options{Pool: pool, AuthorizationClient: fakeAuthorizationClient{
		write: func(context.Context, *authorizationv1.WriteRequest) (*authorizationv1.WriteResponse, error) {
			return &authorizationv1.WriteResponse{}, nil
		},
	}})
	create := func(t *testing.T, sandbox, named bool, phase runnersv1.VolumeStatus) (*runnersv1.CreateVolumeRequest, *runnersv1.Volume) {
		t.Helper()
		req := newRequest(sandbox)
		created, err := srv.CreateVolume(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		update := &runnersv1.UpdateVolumeRequest{Id: req.Id, Status: &phase}
		if named {
			update.InstanceId = ptr(lifecycleTestInstance(created.Volume).InstanceId)
		}
		stored, err := srv.UpdateVolume(ctx, update)
		if err != nil {
			t.Fatal(err)
		}
		return req, stored.Volume
	}
	workload := func(v *runnersv1.Volume) *runnersv1.CreateWorkloadRequest {
		return &runnersv1.CreateWorkloadRequest{
			Id: uuid.NewString(), OwnerKind: v.OwnerKind, OwnerId: v.OwnerId,
			OrganizationId: v.OrganizationId, RunnerId: v.RunnerId, ThreadId: v.ThreadId, AgentId: v.AgentId,
			Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		}
	}
	snapshot := func(t *testing.T, v *runnersv1.Volume) string {
		t.Helper()
		var data string
		if err := reader.QueryRow(ctx, "SELECT to_jsonb(volumes)::text FROM volumes WHERE id = $1", v.Meta.Id).Scan(&data); err != nil {
			t.Fatal(err)
		}
		return data
	}
	bind := func(v *runnersv1.Volume, target *runnerv1.VolumeListItem) (*runnersv1.UpdateVolumeCheckedResponse, error) {
		return srv.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{
			Id: v.Meta.Id, ExpectedRevision: v.LifecycleRevision,
			Operation: &runnersv1.UpdateVolumeCheckedRequest_Bind{Bind: &runnersv1.BindVolumeInstance{Instance: target}},
		})
	}

	for _, sandbox := range []bool{false, true} {
		t.Run(fmt.Sprintf("preserve-legacy-fields/sandbox=%t", sandbox), func(t *testing.T) {
			_, v := create(t, sandbox, true, runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE)
			before := snapshot(t, v)
			for _, field := range []string{"instance", "size_gb = '99'", "removed_at = NOW()", "last_metering_sampled_at = NOW()"} {
				target := lifecycleTestInstance(v)
				assignment := field
				if field == "instance" {
					target.InstanceId = "replacement-" + v.Meta.Id
					assignment = "instance_id = 'replacement-" + v.Meta.Id + "'"
				}
				binding, err := protojson.Marshal(target)
				if err != nil {
					t.Fatal(err)
				}
				_, err = pool.Exec(ctx, `UPDATE volumes SET checked_lifecycle = TRUE, lifecycle_revision = lifecycle_revision + 1,
                    status = 'active', bound_instance = $2, `+assignment+` WHERE id = $1`, v.Meta.Id, binding)
				var pgErr *pgconn.PgError
				if !errors.As(err, &pgErr) || pgErr.Code != "55000" || pgErr.ConstraintName != "volumes_legacy_adoption" || snapshot(t, v) != before {
					t.Fatalf("adoption changed legacy %s: %v", field, err)
				}
			}
		})
		for _, phase := range []runnersv1.WorkloadStatus{
			runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING, runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
			runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING, runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
			runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
		} {
			t.Run(fmt.Sprintf("unconfirmed/sandbox=%t/%s", sandbox, phase), func(t *testing.T) {
				_, v := create(t, sandbox, true, runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE)
				w := workload(v)
				w.Status = phase
				if _, err := srv.CreateWorkload(ctx, w); err != nil {
					t.Fatal(err)
				}
				if _, err := srv.UpdateWorkload(ctx, &runnersv1.UpdateWorkloadRequest{Id: w.Id, RemovedAt: timestamppb.Now()}); err != nil {
					t.Fatal(err)
				}
				before := snapshot(t, v)
				target := lifecycleTestInstance(v)
				resp, err := bind(v, target)
				if status.Code(err) != codes.FailedPrecondition || resp != nil || snapshot(t, v) != before {
					t.Fatalf("adopted with an unconfirmed predecessor: %v", err)
				}
				assertAdmissionConflict(t, adoptionBindSQL(ctx, pool, v, target))
				if snapshot(t, v) != before {
					t.Fatal("raw adoption changed the legacy record")
				}
				stopped := runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED
				if _, err := srv.UpdateWorkload(ctx, &runnersv1.UpdateWorkloadRequest{Id: w.Id, Status: &stopped, RemovalConfirmedAt: timestamppb.Now()}); err != nil {
					t.Fatal(err)
				}
				resp, err = bind(v, target)
				if err != nil || !resp.GetVolume().GetCheckedLifecycle() || !proto.Equal(resp.Volume.BoundInstance, target) {
					t.Fatalf("explicitly confirmed fixture could not be adopted: %v", err)
				}
				if _, err := srv.CreateWorkload(ctx, workload(resp.Volume)); err != nil {
					t.Fatalf("adoption prevented a valid follow-up: %v", err)
				}
				if _, err := bind(resp.Volume, target); err != nil {
					t.Fatalf("legacy-only guard blocked a checked binding retry: %v", err)
				}
			})
		}
		for _, named := range []bool{false, true} {
			for _, phase := range []runnersv1.VolumeStatus{
				runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING, runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE,
				runnersv1.VolumeStatus_VOLUME_STATUS_FAILED, runnersv1.VolumeStatus_VOLUME_STATUS_DELETED,
			} {
				t.Run(fmt.Sprintf("provenance/sandbox=%t/named=%t/%s", sandbox, named, phase), func(t *testing.T) {
					req, v := create(t, sandbox, named, phase)
					before := snapshot(t, v)
					target := lifecycleTestInstance(v)
					resp, err := bind(v, target)
					valid := named && (phase == runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE || phase == runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING)
					if valid {
						if err != nil {
							t.Fatal(err)
						}
						adopted := resp.Volume
						expected := proto.Clone(v).(*runnersv1.Volume)
						expected.CheckedLifecycle, expected.LifecycleRevision = true, v.LifecycleRevision+1
						expected.Status, expected.BoundInstance = runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE, target
						expected.Meta.UpdatedAt = adopted.Meta.UpdatedAt
						if !proto.Equal(expected, adopted) {
							t.Fatal("adoption changed fields other than binding, status, revision and update time")
						}
						stored, readErr := scanVolume(reader.QueryRow(ctx, "SELECT "+volumeColumns+" FROM volumes WHERE id = $1", v.Meta.Id))
						storedProto, convertErr := toProtoVolume(stored)
						if readErr != nil || convertErr != nil || !proto.Equal(storedProto, adopted) {
							t.Fatalf("committed adoption differs from response: read=%v convert=%v", readErr, convertErr)
						}
						if _, err := bind(v, target); status.Code(err) != codes.Aborted {
							t.Fatalf("stale adoption was accepted: %v", err)
						}
						return
					}
					if status.Code(err) != codes.FailedPrecondition || resp != nil || snapshot(t, v) != before {
						t.Fatalf("unproven legacy generation was adopted: %v", err)
					}
					if phase == runnersv1.VolumeStatus_VOLUME_STATUS_FAILED || phase == runnersv1.VolumeStatus_VOLUME_STATUS_DELETED {
						resp, err := srv.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{
							Id: v.Meta.Id, ExpectedRevision: v.LifecycleRevision,
							Operation: &runnersv1.UpdateVolumeCheckedRequest_Reopen{Reopen: &runnersv1.ReopenVolume{Volume: req}},
						})
						if status.Code(err) != codes.FailedPrecondition || resp != nil || snapshot(t, v) != before {
							t.Fatalf("ordinary reopen implicitly adopted legacy history: %v", err)
						}
						_, err = pool.Exec(ctx, "UPDATE volumes SET checked_lifecycle = TRUE, lifecycle_revision = lifecycle_revision + 1, status = 'provisioning' WHERE id = $1", v.Meta.Id)
						if err == nil || snapshot(t, v) != before {
							t.Fatalf("raw reopen implicitly adopted legacy history: %v", err)
						}
					}
					if err := adoptionBindSQL(ctx, pool, v, target); err == nil || snapshot(t, v) != before {
						t.Fatalf("raw SQL adopted an unproven generation: %v", err)
					}
				})
			}
		}
		for _, isolation := range []pgx.TxIsoLevel{pgx.ReadUncommitted, pgx.ReadCommitted, pgx.RepeatableRead, pgx.Serializable} {
			for _, adoptionFirst := range []bool{false, true} {
				for _, commit := range []bool{false, true} {
					t.Run(fmt.Sprintf("blocked/sandbox=%t/%s/adoption-first=%t/commit=%t", sandbox, isolation, adoptionFirst, commit), func(t *testing.T) {
						caseCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
						defer cancel()
						_, v := create(t, sandbox, true, runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE)
						_, other := create(t, sandbox, true, runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE)
						target, w := lifecycleTestInstance(v), workload(v)
						loser, err := pool.BeginTx(caseCtx, pgx.TxOptions{IsoLevel: isolation})
						if err != nil {
							t.Fatal(err)
						}
						defer loser.Rollback(context.Background())
						var count int
						if err := loser.QueryRow(caseCtx, "SELECT count(*) FROM workloads WHERE owner_id = $1", v.OwnerId).Scan(&count); err != nil || count != 0 {
							t.Fatalf("pin pre-race snapshot: count=%d err=%v", count, err)
						}
						winner, err := pool.Begin(caseCtx)
						if err != nil {
							t.Fatal(err)
						}
						defer winner.Rollback(context.Background())
						if adoptionFirst {
							err = adoptionBindSQL(caseCtx, winner, v, target)
						} else {
							err = admissionInsertSQL(caseCtx, winner, w)
						}
						if err != nil {
							t.Fatal(err)
						}
						winnerPID, loserPID := winner.Conn().PgConn().PID(), loser.Conn().PgConn().PID()
						finished := make(chan error, 1)
						go func() {
							var result error
							if adoptionFirst {
								result = admissionInsertSQL(caseCtx, loser, w)
							} else {
								result = adoptionBindSQL(caseCtx, loser, v, target)
							}
							if result == nil {
								result = loser.Commit(caseCtx)
							} else {
								_ = loser.Rollback(caseCtx)
							}
							finished <- result
						}()
						waitErr := waitAdmissionBlocked(caseCtx, reader, winnerPID, loserPID)
						otherCtx, otherCancel := context.WithTimeout(caseCtx, time.Second)
						otherErr := adoptionBindSQL(otherCtx, pool, other, lifecycleTestInstance(other))
						otherCancel()
						var releaseErr error
						if commit {
							releaseErr = winner.Commit(caseCtx)
						} else {
							releaseErr = winner.Rollback(caseCtx)
						}
						result := <-finished
						if waitErr != nil || releaseErr != nil || otherErr != nil {
							t.Fatalf("blocked adoption proof: wait=%v release=%v other=%v result=%v", waitErr, releaseErr, otherErr, result)
						}
						stale := commit && (isolation == pgx.RepeatableRead || isolation == pgx.Serializable)
						if stale {
							var pgErr *pgconn.PgError
							if !errors.As(result, &pgErr) || pgErr.Code != "40001" {
								t.Fatalf("stale snapshot did not abort: %v", result)
							}
						} else if commit && !adoptionFirst {
							assertAdmissionConflict(t, result)
						} else if result != nil {
							t.Fatalf("serialized valid transition failed: %v", result)
						}
						var checked bool
						var binding string
						if err := reader.QueryRow(ctx, `SELECT checked_lifecycle, COALESCE(bound_instance->>'instanceUid', ''),
                            (SELECT count(*) FROM workloads WHERE owner_id = v.owner_id AND removal_confirmed_at IS NULL)
                            FROM volumes v WHERE id = $1`, v.Meta.Id).Scan(&checked, &binding, &count); err != nil {
							t.Fatal(err)
						}
						wantChecked := adoptionFirst == commit
						wantCount := 0
						if !adoptionFirst && commit || adoptionFirst && (!commit || !stale) {
							wantCount = 1
						}
						if checked != wantChecked || checked && binding != target.InstanceUid || !checked && binding != "" || count != wantCount {
							t.Fatalf("committed race state: checked=%t binding=%q workloads=%d wantChecked=%t wantWorkloads=%d", checked, binding, count, wantChecked, wantCount)
						}
					})
				}
			}
		}
	}
}

func adoptionBindSQL(ctx context.Context, db admissionExecer, v *runnersv1.Volume, target *runnerv1.VolumeListItem) error {
	binding, err := protojson.Marshal(target)
	if err != nil {
		return err
	}
	result, err := db.Exec(ctx, `UPDATE volumes SET checked_lifecycle = TRUE, lifecycle_revision = lifecycle_revision + 1,
        status = 'active', instance_id = $3, bound_instance = $4
        WHERE id = $1 AND lifecycle_revision = $2`, v.Meta.Id, int64(v.LifecycleRevision), target.InstanceId, binding)
	if err == nil && result.RowsAffected() != 1 {
		return fmt.Errorf("checked adoption affected %d rows", result.RowsAffected())
	}
	return err
}
