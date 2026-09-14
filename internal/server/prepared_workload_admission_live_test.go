package server

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func preparedInsertSQL(ctx context.Context, db admissionExecer, req *runnersv1.CreatePreparedWorkloadRequest) error {
	w := req.Workload
	kind, err := runtimeOwnerKindToString(w.OwnerKind)
	if err != nil {
		return err
	}
	nullable := func(value string) any {
		if value == "" {
			return nil
		}
		return value
	}
	_, err = db.Exec(ctx, `INSERT INTO workloads (id, runner_id, organization_id, thread_id, agent_id, owner_kind, owner_id, status,
        preparation_phase, preparation_revision, prepared_backend_id, prepared_volume_ids)
        VALUES ($1, $2, $3, $4, $5, $6, $7, 'starting', 'reserved', 1, $8, $9)`,
		w.Id, w.RunnerId, w.OrganizationId, nullable(w.ThreadId), nullable(w.AgentId), kind, w.OwnerId, req.BackendId, req.VolumeIds)
	return err
}

func testPreparedWorkloadAdmission(t *testing.T, ctx context.Context, pool *pgxpool.Pool, reader *pgx.Conn, newRequest func(bool) *runnersv1.CreateVolumeRequest) {
	client, _ := preparedRegistryClient(t, pool)
	create := func(t *testing.T, sandbox bool) *runnersv1.Volume {
		t.Helper()
		resp, err := client.CreateVolumeChecked(ctx, &runnersv1.CreateVolumeCheckedRequest{Volume: newRequest(sandbox)})
		if err != nil {
			t.Fatal(err)
		}
		return resp.Volume
	}
	for _, sandbox := range []bool{false, true} {
		for _, isolation := range []pgx.TxIsoLevel{pgx.ReadCommitted, pgx.RepeatableRead, pgx.Serializable} {
			for _, preparedFirst := range []bool{false, true} {
				for _, commit := range []bool{false, true} {
					t.Run(fmt.Sprintf("sandbox=%t/%s/prepared-first=%t/commit=%t", sandbox, isolation, preparedFirst, commit), func(t *testing.T) {
						ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
						defer cancel()
						v, other := create(t, sandbox), create(t, sandbox)
						prepared, legacy := preparedCreateRequest(v), preparedCreateRequest(v).Workload
						loser, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: isolation})
						if err != nil {
							t.Fatal(err)
						}
						defer loser.Rollback(context.Background())
						var initial int
						if err := loser.QueryRow(ctx, "SELECT count(*) FROM workloads WHERE owner_id = $1", v.OwnerId).Scan(&initial); err != nil || initial != 0 {
							t.Fatalf("initial snapshot: count=%d err=%v", initial, err)
						}
						winner, err := pool.Begin(ctx)
						if err != nil {
							t.Fatal(err)
						}
						defer winner.Rollback(context.Background())
						if preparedFirst {
							err = preparedInsertSQL(ctx, winner, prepared)
						} else {
							err = admissionInsertSQL(ctx, winner, legacy)
						}
						if err != nil {
							t.Fatal(err)
						}
						done := make(chan error, 1)
						go func() {
							var err error
							if preparedFirst {
								err = admissionInsertSQL(ctx, loser, legacy)
							} else {
								err = preparedInsertSQL(ctx, loser, prepared)
							}
							if err == nil {
								err = loser.Commit(ctx)
							} else {
								_ = loser.Rollback(ctx)
							}
							done <- err
						}()
						waitErr := waitAdmissionBlocked(ctx, reader, winner.Conn().PgConn().PID(), loser.Conn().PgConn().PID())
						otherErr := preparedInsertSQL(ctx, pool, preparedCreateRequest(other))
						if commit {
							err = winner.Commit(ctx)
						} else {
							err = winner.Rollback(ctx)
						}
						result := <-done
						if waitErr != nil || err != nil || otherErr != nil {
							t.Fatalf("overlap/isolation: wait=%v release=%v other=%v", waitErr, err, otherErr)
						}
						if commit && isolation != pgx.ReadCommitted {
							var pgErr *pgconn.PgError
							if !errors.As(result, &pgErr) || pgErr.Code != "40001" {
								t.Fatalf("stale snapshot committed: %v", result)
							}
						} else if commit {
							if status.Code(toStatusError(result)) != codes.FailedPrecondition {
								t.Fatalf("competing admission succeeded: %v", result)
							}
						} else if result != nil {
							t.Fatalf("rollback still excluded owner: %v", result)
						}
						var count, preparedCount int
						var pinned bool
						if err := reader.QueryRow(ctx, `SELECT count(*), count(preparation_phase),
                            (SELECT prepared_backend_id IS NOT NULL FROM runtime_volume_admission_guards WHERE owner_kind = $2 AND owner_id = $1)
                            FROM workloads WHERE owner_id = $1`, v.OwnerId, map[bool]string{false: "agent_instance", true: "sandbox"}[sandbox]).Scan(&count, &preparedCount, &pinned); err != nil {
							t.Fatal(err)
						}
						wantPrepared := preparedFirst == commit
						if count != 1 || (preparedCount == 1) != wantPrepared || pinned != wantPrepared {
							t.Fatal("admission and pin were not committed atomically")
						}
					})
				}
			}
		}
		t.Run(fmt.Sprintf("no-volumes/sandbox=%t", sandbox), func(t *testing.T) {
			raw := newRequest(sandbox)
			w := &runnersv1.CreateWorkloadRequest{Id: uuid.NewString(), RunnerId: raw.RunnerId, OrganizationId: raw.OrganizationId,
				ThreadId: raw.ThreadId, AgentClassId: raw.AgentClassId, OwnerKind: raw.OwnerKind, OwnerId: raw.OwnerId, AgentInstanceId: raw.AgentInstanceId, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING}
			req := &runnersv1.CreatePreparedWorkloadRequest{Workload: w, BackendId: lifecycleTestBackend}
			created, err := client.CreatePreparedWorkload(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			abort := preparedOperation("abort", nil)
			abort.Id, abort.ExpectedRevision = created.Workload.Meta.Id, 1
			if _, err := client.UpdatePreparedWorkload(ctx, abort); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, "DELETE FROM workloads WHERE id = $1", w.Id); err != nil {
				t.Fatal(err)
			}
			w.Id = uuid.NewString()
			if _, err := client.CreateWorkload(ctx, w); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("empty-volume owner lost legacy exclusion: %v", err)
			}
			req.BackendId = "other"
			if _, err := client.CreatePreparedWorkload(ctx, req); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("empty-volume owner lost backend pin: %v", err)
			}
		})
	}
}
