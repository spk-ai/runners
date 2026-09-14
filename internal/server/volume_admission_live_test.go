package server

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	authorizationv1 "github.com/agynio/runners/.gen/go/agynio/api/authorization/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testVolumeWorkloadAdmission(t *testing.T, ctx context.Context, srv *Server, pool *pgxpool.Pool, reader *pgx.Conn, newRequest func(bool) *runnersv1.CreateVolumeRequest) {
	srv = New(Options{Pool: pool, AuthorizationClient: fakeAuthorizationClient{
		write: func(context.Context, *authorizationv1.WriteRequest) (*authorizationv1.WriteResponse, error) {
			return &authorizationv1.WriteResponse{}, nil
		},
	}})
	create := func(t *testing.T, sandbox, bound bool) *runnersv1.Volume {
		t.Helper()
		created, err := srv.CreateVolumeChecked(ctx, &runnersv1.CreateVolumeCheckedRequest{Volume: newRequest(sandbox)})
		if err != nil {
			t.Fatal(err)
		}
		if !bound {
			return created.Volume
		}
		resp, err := srv.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{
			Id: created.Volume.Meta.Id, ExpectedRevision: created.Volume.LifecycleRevision,
			Operation: &runnersv1.UpdateVolumeCheckedRequest_Bind{Bind: &runnersv1.BindVolumeInstance{Instance: lifecycleTestInstance(created.Volume)}},
		})
		if err != nil {
			t.Fatal(err)
		}
		return resp.Volume
	}
	workloadRequest := func(v *runnersv1.Volume) *runnersv1.CreateWorkloadRequest {
		return &runnersv1.CreateWorkloadRequest{
			Id: uuid.NewString(), RunnerId: v.RunnerId, OrganizationId: v.OrganizationId,
			ThreadId: v.ThreadId, AgentId: v.AgentId, OwnerKind: v.OwnerKind, OwnerId: v.OwnerId,
			Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		}
	}
	begin := func(v *runnersv1.Volume) (*runnersv1.UpdateVolumeCheckedResponse, error) {
		return srv.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{
			Id: v.Meta.Id, ExpectedRevision: v.LifecycleRevision,
			Operation: &runnersv1.UpdateVolumeCheckedRequest_BeginRemoval{BeginRemoval: &runnersv1.BeginVolumeRemoval{}},
		})
	}
	snapshot := func(t *testing.T, v *runnersv1.Volume) string {
		t.Helper()
		var data string
		if err := reader.QueryRow(ctx, "SELECT to_jsonb(volumes)::text FROM volumes WHERE id = $1", v.Meta.Id).Scan(&data); err != nil {
			t.Fatal(err)
		}
		return data
	}

	for _, sandbox := range []bool{false, true} {
		for _, phase := range []runnersv1.WorkloadStatus{
			runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
			runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
			runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING,
			runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
			runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
		} {
			t.Run(fmt.Sprintf("unconfirmed/sandbox=%t/%s", sandbox, phase), func(t *testing.T) {
				v := create(t, sandbox, true)
				req := workloadRequest(v)
				req.Status = phase
				if _, err := srv.CreateWorkload(ctx, req); err != nil {
					t.Fatal(err)
				}
				billingEnd := timestamppb.New(time.Now().Add(-time.Hour))
				if _, err := srv.UpdateWorkload(ctx, &runnersv1.UpdateWorkloadRequest{Id: req.Id, RemovedAt: billingEnd}); err != nil {
					t.Fatal(err)
				}
				if _, err := srv.CreateWorkload(ctx, workloadRequest(v)); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("admitted a second unconfirmed workload: %v", err)
				}
				before := snapshot(t, v)
				resp, err := begin(v)
				if status.Code(err) != codes.FailedPrecondition || resp != nil || snapshot(t, v) != before {
					t.Fatalf("deletion admitted an unconfirmed workload: code=%s err=%v", status.Code(err), err)
				}
				stopped := runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED
				if _, err := srv.UpdateWorkload(ctx, &runnersv1.UpdateWorkloadRequest{Id: req.Id, Status: &stopped, RemovalConfirmedAt: timestamppb.Now()}); err != nil {
					t.Fatal(err)
				}
				followup := workloadRequest(v)
				if _, err := srv.CreateWorkload(ctx, followup); err != nil {
					t.Fatalf("confirmed predecessor did not permit a follow-up: %v", err)
				}
				if _, err := begin(v); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("follow-up did not protect the original volume: %v", err)
				}
				if _, err := srv.UpdateWorkload(ctx, &runnersv1.UpdateWorkloadRequest{Id: followup.Id, Status: &stopped, RemovalConfirmedAt: timestamppb.Now()}); err != nil {
					t.Fatal(err)
				}
				if _, err := begin(v); err != nil {
					t.Fatalf("confirmed removal did not release the owner: %v", err)
				}
			})
		}
		for _, phase := range []string{"pending", "deleted", "failed"} {
			t.Run(fmt.Sprintf("reject-start/sandbox=%t/%s", sandbox, phase), func(t *testing.T) {
				v := create(t, sandbox, phase != "failed")
				var resp *runnersv1.UpdateVolumeCheckedResponse
				var err error
				if phase == "failed" {
					resp, err = srv.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{
						Id: v.Meta.Id, ExpectedRevision: v.LifecycleRevision,
						Operation: &runnersv1.UpdateVolumeCheckedRequest_FailProvisioning{FailProvisioning: &runnersv1.FailVolumeProvisioning{}},
					})
				} else {
					resp, err = begin(v)
				}
				if err != nil {
					t.Fatal(err)
				}
				v = resp.Volume
				if phase == "deleted" {
					resp, err = srv.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{
						Id: v.Meta.Id, ExpectedRevision: v.LifecycleRevision,
						Operation: &runnersv1.UpdateVolumeCheckedRequest_ConfirmRemoval{ConfirmRemoval: &runnersv1.ConfirmVolumeRemoval{IntentId: v.RemovalIntent.Id}},
					})
					if err != nil {
						t.Fatal(err)
					}
					v = resp.Volume
				}
				req := workloadRequest(v)
				before := snapshot(t, v)
				workload, err := srv.CreateWorkload(ctx, req)
				var count int
				if readErr := reader.QueryRow(ctx, "SELECT count(*) FROM workloads WHERE id = $1", req.Id).Scan(&count); readErr != nil {
					t.Fatal(readErr)
				}
				if status.Code(err) != codes.FailedPrecondition || workload != nil || count != 0 || snapshot(t, v) != before {
					t.Fatalf("workload admitted on unavailable volume: code=%s rows=%d err=%v", status.Code(err), count, err)
				}
			})
		}
	}

	for _, sandbox := range []bool{false, true} {
		t.Run(fmt.Sprintf("identity/sandbox=%t", sandbox), func(t *testing.T) {
			v := create(t, sandbox, true)
			for field, mutate := range map[string]func(*runnersv1.CreateWorkloadRequest){
				"organization": func(r *runnersv1.CreateWorkloadRequest) { r.OrganizationId = uuid.NewString() },
				"runner":       func(r *runnersv1.CreateWorkloadRequest) { r.RunnerId = uuid.NewString() },
				"thread":       func(r *runnersv1.CreateWorkloadRequest) { r.ThreadId = uuid.NewString() },
				"class":        func(r *runnersv1.CreateWorkloadRequest) { r.AgentId = uuid.NewString() },
			} {
				t.Run(field, func(t *testing.T) {
					req := workloadRequest(v)
					mutate(req)
					if _, err := srv.CreateWorkload(ctx, req); status.Code(err) != codes.FailedPrecondition {
						t.Fatalf("mismatched workload admission: %v", err)
					}
				})
			}
			req := workloadRequest(v)
			if _, err := srv.CreateWorkload(ctx, req); err != nil {
				t.Fatal(err)
			}
			workloadSnapshot := func() string {
				var data string
				if err := reader.QueryRow(ctx, "SELECT to_jsonb(workloads)::text FROM workloads WHERE id = $1", req.Id).Scan(&data); err != nil {
					t.Fatal(err)
				}
				return data
			}
			before := workloadSnapshot()
			for _, field := range []string{"id", "owner_id", "organization_id", "runner_id", "thread_id", "agent_id"} {
				t.Run("raw-update/"+field, func(t *testing.T) {
					_, err := pool.Exec(ctx, "UPDATE workloads SET "+field+" = $2 WHERE id = $1", req.Id, uuid.NewString())
					assertAdmissionConflict(t, err)
					if workloadSnapshot() != before {
						t.Fatal("rejected owner mutation changed the workload")
					}
				})
			}
			_, err := pool.Exec(ctx, "UPDATE workloads SET owner_kind = CASE WHEN owner_kind = 'sandbox' THEN 'agent_instance' ELSE 'sandbox' END WHERE id = $1", req.Id)
			assertAdmissionConflict(t, err)
			_, err = pool.Exec(ctx, "DELETE FROM workloads WHERE id = $1", req.Id)
			assertAdmissionConflict(t, err)
			if workloadSnapshot() != before {
				t.Fatal("old writer discarded protected workload evidence")
			}
			failed := runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED
			if _, err := srv.UpdateWorkload(ctx, &runnersv1.UpdateWorkloadRequest{Id: req.Id, Status: &failed, RemovalConfirmedAt: timestamppb.Now()}); err != nil {
				t.Fatal(err)
			}
			before = workloadSnapshot()
			for _, sql := range []string{
				"UPDATE workloads SET removal_confirmed_at = NULL WHERE id = $1",
				"UPDATE workloads SET removal_confirmed_at = removal_confirmed_at + interval '1 second' WHERE id = $1",
				"UPDATE workloads SET status = 'starting', removal_confirmed_at = NULL WHERE id = $1",
			} {
				_, err := pool.Exec(ctx, sql, req.Id)
				assertAdmissionConflict(t, err)
				if workloadSnapshot() != before {
					t.Fatal("old writer discarded confirmation")
				}
			}
		})
	}

	t.Run("failed-provisioning-recovery-requires-removal", func(t *testing.T) {
		req := newRequest(false)
		created, err := srv.CreateVolumeChecked(ctx, &runnersv1.CreateVolumeCheckedRequest{Volume: req})
		if err != nil {
			t.Fatal(err)
		}
		v := created.Volume
		w := workloadRequest(v)
		if _, err := srv.CreateWorkload(ctx, w); err != nil {
			t.Fatal(err)
		}
		fail := &runnersv1.UpdateVolumeCheckedRequest{Id: v.Meta.Id, ExpectedRevision: v.LifecycleRevision,
			Operation: &runnersv1.UpdateVolumeCheckedRequest_FailProvisioning{FailProvisioning: &runnersv1.FailVolumeProvisioning{}}}
		if _, err := srv.UpdateVolumeChecked(ctx, fail); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("stale compensation failed a running owner's volume: %v", err)
		}
		failed := runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED
		if _, err := srv.UpdateWorkload(ctx, &runnersv1.UpdateWorkloadRequest{Id: w.Id, Status: &failed}); err != nil {
			t.Fatal(err)
		}
		result, err := srv.UpdateVolumeChecked(ctx, fail)
		if err != nil {
			t.Fatalf("failed workload prevented provisioning cleanup: %v", err)
		}
		reopen := &runnersv1.UpdateVolumeCheckedRequest{Id: v.Meta.Id, ExpectedRevision: result.Volume.LifecycleRevision,
			Operation: &runnersv1.UpdateVolumeCheckedRequest_Reopen{Reopen: &runnersv1.ReopenVolume{Volume: req}}}
		if _, err := srv.UpdateVolumeChecked(ctx, reopen); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("reopened before failed workload removal: %v", err)
		}
		if _, err := srv.UpdateWorkload(ctx, &runnersv1.UpdateWorkloadRequest{Id: w.Id, RemovalConfirmedAt: timestamppb.Now()}); err != nil {
			t.Fatal(err)
		}
		if _, err := srv.UpdateVolumeChecked(ctx, reopen); err != nil {
			t.Fatalf("confirmed cleanup prevented explicit recovery: %v", err)
		}
		if _, err := srv.CreateWorkload(ctx, workloadRequest(v)); err != nil {
			t.Fatalf("recovered provisioning did not admit a follow-up: %v", err)
		}
	})

	for _, field := range []string{"organization", "runner", "thread", "class", "multiple-workloads"} {
		t.Run("checked-opt-in/"+field, func(t *testing.T) {
			req := newRequest(false)
			legacy, err := srv.CreateVolume(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			v := legacy.Volume
			w := workloadRequest(v)
			switch field {
			case "organization":
				w.OrganizationId = uuid.NewString()
			case "thread":
				w.ThreadId = uuid.NewString()
			case "class":
				w.AgentId = uuid.NewString()
			case "runner":
				if err := pool.QueryRow(ctx, "SELECT id::text FROM runners WHERE id <> $1 LIMIT 1", v.RunnerId).Scan(&w.RunnerId); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := srv.CreateWorkload(ctx, w); err != nil {
				t.Fatal(err)
			}
			if field == "multiple-workloads" {
				if _, err := srv.CreateWorkload(ctx, workloadRequest(v)); err != nil {
					t.Fatal(err)
				}
			}
			before := snapshot(t, v)
			if _, err := srv.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{
				Id: v.Meta.Id, ExpectedRevision: v.LifecycleRevision,
				Operation: &runnersv1.UpdateVolumeCheckedRequest_Bind{Bind: &runnersv1.BindVolumeInstance{Instance: lifecycleTestInstance(v)}},
			}); status.Code(err) != codes.FailedPrecondition || snapshot(t, v) != before {
				t.Fatalf("unsafe checked opt-in: %v", err)
			}
			second := proto.Clone(req).(*runnersv1.CreateVolumeRequest)
			second.Id = uuid.NewString()
			if _, err := srv.CreateVolumeChecked(ctx, &runnersv1.CreateVolumeCheckedRequest{Volume: second}); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("unsafe checked creation: %v", err)
			}
		})
	}

	for _, sandbox := range []bool{false, true} {
		for _, isolation := range []pgx.TxIsoLevel{pgx.ReadUncommitted, pgx.ReadCommitted, pgx.RepeatableRead, pgx.Serializable} {
			for _, order := range []string{"workload-first", "removal-first", "two-workloads"} {
				for _, commit := range []bool{false, true} {
					t.Run(fmt.Sprintf("blocked/sandbox=%t/%s/%s/commit=%t", sandbox, isolation, order, commit), func(t *testing.T) {
						caseCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
						defer cancel()
						v := create(t, sandbox, true)
						other := create(t, sandbox, true)
						loser, err := pool.BeginTx(caseCtx, pgx.TxOptions{IsoLevel: isolation})
						if err != nil {
							t.Fatal(err)
						}
						defer loser.Rollback(context.Background())
						kind, _ := runtimeOwnerKindToString(v.OwnerKind)
						var observed int
						if err := loser.QueryRow(caseCtx, "SELECT count(*) FROM workloads WHERE owner_kind = $1 AND owner_id = $2", kind, v.OwnerId).Scan(&observed); err != nil || observed != 0 {
							t.Fatalf("pin initial transaction snapshot: count=%d err=%v", observed, err)
						}
						winner, err := pool.Begin(caseCtx)
						if err != nil {
							t.Fatal(err)
						}
						defer winner.Rollback(context.Background())
						if order == "removal-first" {
							err = admissionBeginSQL(caseCtx, winner, v)
						} else {
							err = admissionInsertSQL(caseCtx, winner, workloadRequest(v))
						}
						if err != nil {
							t.Fatal(err)
						}
						winnerPID, loserPID := winner.Conn().PgConn().PID(), loser.Conn().PgConn().PID()
						finished := make(chan error, 1)
						go func() {
							var result error
							if order == "workload-first" {
								result = admissionBeginSQL(caseCtx, loser, v)
							} else {
								result = admissionInsertSQL(caseCtx, loser, workloadRequest(v))
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
						otherErr := admissionInsertSQL(otherCtx, pool, workloadRequest(other))
						otherCancel()
						var releaseErr error
						if commit {
							releaseErr = winner.Commit(caseCtx)
						} else {
							releaseErr = winner.Rollback(caseCtx)
						}
						result := <-finished
						if waitErr != nil || releaseErr != nil || otherErr != nil {
							t.Fatalf("overlap/independence proof failed: wait=%v release=%v other-owner=%v loser=%v", waitErr, releaseErr, otherErr, result)
						}
						if commit {
							if isolation == pgx.RepeatableRead || isolation == pgx.Serializable {
								var pgErr *pgconn.PgError
								if !errors.As(result, &pgErr) || pgErr.Code != "40001" {
									t.Fatalf("stale snapshot was not aborted: %v", result)
								}
							} else {
								assertAdmissionConflict(t, result)
							}
						} else if result != nil {
							t.Fatalf("rolled-back winner blocked admission: %v", result)
						}
						var deleting, intent bool
						var workloads int
						if err := reader.QueryRow(ctx, `SELECT status = 'deprovisioning', removal_intent IS NOT NULL,
                            (SELECT count(*) FROM workloads w WHERE w.owner_kind = v.owner_kind
                                AND w.owner_id = v.owner_id AND w.removal_confirmed_at IS NULL)
                            FROM volumes v WHERE id = $1`, v.Meta.Id).Scan(&deleting, &intent, &workloads); err != nil {
							t.Fatal(err)
						}
						wantDeleting := (order == "removal-first" && commit) || (order == "workload-first" && !commit)
						wantWorkloads := 1
						if wantDeleting {
							wantWorkloads = 0
						}
						if deleting != wantDeleting || intent != wantDeleting || workloads != wantWorkloads {
							t.Fatalf("committed owner state: deleting=%t intent=%t workloads=%d", deleting, intent, workloads)
						}
					})
				}
			}
		}
	}
}

type admissionExecer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

// Raw statements also exercise old writers that do not know the guard table.
func admissionInsertSQL(ctx context.Context, db admissionExecer, req *runnersv1.CreateWorkloadRequest) error {
	kind, err := runtimeOwnerKindToString(req.OwnerKind)
	if err != nil {
		return err
	}
	nullable := func(value string) any {
		if value == "" {
			return nil
		}
		return value
	}
	_, err = db.Exec(ctx, `INSERT INTO workloads (id, runner_id, organization_id, thread_id, agent_id, owner_kind, owner_id, status)
        VALUES ($1, $2, $3, $4, $5, $6, $7, 'starting')`, req.Id, req.RunnerId, req.OrganizationId,
		nullable(req.ThreadId), nullable(req.AgentId), kind, req.OwnerId)
	return err
}

func admissionBeginSQL(ctx context.Context, db admissionExecer, v *runnersv1.Volume) error {
	result, err := db.Exec(ctx, `UPDATE volumes SET status = 'deprovisioning', lifecycle_revision = lifecycle_revision + 1,
        removal_intent = jsonb_build_object('id', $3::text, 'requestedAt', $4::text, 'expected', bound_instance)
        WHERE id = $1 AND lifecycle_revision = $2`, v.Meta.Id, int64(v.LifecycleRevision), uuid.NewString(), time.Now().UTC().Format(time.RFC3339Nano))
	if err == nil && result.RowsAffected() != 1 {
		return fmt.Errorf("checked begin affected %d rows", result.RowsAffected())
	}
	return err
}

func assertAdmissionConflict(t *testing.T, err error) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55000" || pgErr.ConstraintName != "runtime_volume_admission" {
		t.Fatalf("expected database admission conflict, got %v", err)
	}
}

func waitAdmissionBlocked(ctx context.Context, reader *pgx.Conn, winnerPID, loserPID uint32) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var blocked bool
		if err := reader.QueryRow(ctx, "SELECT $1::integer = ANY(pg_blocking_pids($2::integer))", winnerPID, loserPID).Scan(&blocked); err != nil {
			return err
		}
		if blocked {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("workload/volume statement did not block behind the winner")
}
