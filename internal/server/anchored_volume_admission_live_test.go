package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Both SQL trigger orderings are forced behind the shared owner guard. The
// last case commits retirement; earlier accepted reservations are aborted
// through the checked API, with no native preparation or execution authority.
func testAnchoredRetirementAdmission(t *testing.T, parent context.Context, pool *pgxpool.Pool, reader *pgx.Conn, v *runnersv1.Volume, isolation pgx.TxIsoLevel) {
	client, _ := preparedRegistryClient(t, pool)
	for _, mode := range []string{"admission-wins", "retirement-rollback", "retirement-wins"} {
		if !t.Run(fmt.Sprint(isolation)+"/"+mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(parent, 10*time.Second)
			defer cancel()
			winner, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer winner.Rollback(context.Background())
			loser, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: isolation})
			if err != nil {
				t.Fatal(err)
			}
			defer loser.Rollback(context.Background())
			var initial int64
			if err := loser.QueryRow(ctx, "SELECT lifecycle_revision FROM volumes WHERE id=$1", v.Meta.Id).Scan(&initial); err != nil || initial != int64(v.LifecycleRevision) {
				t.Fatal("contender snapshot changed")
			}
			workloadID := uuid.NewString()
			admit := func(tx pgx.Tx) error {
				_, err := tx.Exec(ctx, `INSERT INTO workloads (id,runner_id,organization_id,thread_id,agent_id,owner_kind,owner_id,status,
                    preparation_phase,preparation_revision,prepared_backend_id,prepared_volume_ids,resource_anchors)
                    SELECT $2,runner_id,organization_id,thread_id,agent_id,owner_kind,owner_id,'starting',
                    'reserved',1,prepared_backend_id,prepared_volume_ids,'{"revision":"1"}' FROM workloads
                    WHERE id=$1`, v.AnchorReservation.WorkloadId, workloadID)
				return err
			}
			retire := func(tx pgx.Tx) error {
				_, err := New(Options{Pool: tx}).UpdateVolumeChecked(ctx, beginRetirementRequest(v))
				return err
			}
			win, lose := retire, admit
			if mode == "admission-wins" {
				win, lose = admit, retire
			}
			if err := win(winner); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				err := lose(loser)
				if err == nil {
					err = loser.Commit(ctx)
				} else {
					_ = loser.Rollback(ctx)
				}
				done <- err
			}()
			waitErr := waitAdmissionBlocked(ctx, reader, winner.Conn().PgConn().PID(), loser.Conn().PgConn().PID())
			if mode == "retirement-rollback" {
				err = winner.Rollback(ctx)
			} else {
				err = winner.Commit(ctx)
			}
			result := <-done
			if waitErr != nil || err != nil {
				t.Fatalf("contention not established: wait=%v release=%v", waitErr, err)
			}
			if mode == "retirement-rollback" {
				if result != nil {
					t.Fatalf("rollback stranded eligible admission: %v", result)
				}
			} else {
				code := status.Code(result)
				if _, ok := status.FromError(result); !ok {
					code = status.Code(volumeLifecycleStatusError(result))
				}
				if code != codes.FailedPrecondition && code != codes.Aborted {
					t.Fatalf("losing operation escaped admission guard: %v", result)
				}
			}
			stored, err := scanVolume(reader.QueryRow(ctx, "SELECT "+volumeColumns+" FROM volumes WHERE id=$1", v.Meta.Id))
			if err != nil {
				t.Fatal(err)
			}
			if mode == "retirement-wins" {
				if stored.Status != volumeStatusDeprovision || !stored.RemovalIntent.GetAnchored() {
					t.Fatal("winning retirement was not committed")
				}
				var count int
				if err := reader.QueryRow(ctx, "SELECT count(*) FROM workloads WHERE id=$1", workloadID).Scan(&count); err != nil || count != 0 {
					t.Fatal("losing admission persisted")
				}
			} else {
				if stored.Status != volumeStatusActive || stored.RemovalIntent != nil || stored.LifecycleRevision != int64(v.LifecycleRevision) {
					t.Fatal("losing/rolled-back retirement changed volume")
				}
				w, err := client.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: workloadID})
				if err != nil {
					t.Fatal(err)
				}
				op := preparedOperation("abort", nil)
				op.Id, op.ExpectedRevision = workloadID, w.Workload.Preparation.Revision
				if _, err := client.UpdateAnchoredWorkload(ctx, &runnersv1.UpdateAnchoredWorkloadRequest{Operation: op, ExpectedAnchorRevision: w.Workload.Preparation.Resources.Revision}); err != nil {
					t.Fatal(err)
				}
			}
		}) {
			return
		}
	}
}
