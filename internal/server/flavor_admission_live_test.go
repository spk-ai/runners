package server

import (
	"context"
	"testing"
	"time"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Uses TestLiveVolumeReopen's disposable loopback database and real registry RPCs.
func testFlavorAdmission(t *testing.T, ctx context.Context, pool *pgxpool.Pool, reader *pgx.Conn, newOwner func(bool) *runnersv1.CreateVolumeRequest) {
	client, _ := preparedRegistryClient(t, pool)
	flavor := "bounded-" + uuid.NewString()
	runnerID := newOwner(false).RunnerId
	if _, err := pool.Exec(ctx, "INSERT INTO workload_flavor_admission (runner_id,flavor,capacity) VALUES ($1,$2,1)", runnerID, flavor); err != nil {
		t.Fatal(err)
	}
	request := func() *runnersv1.CreateAnchoredWorkloadRequest {
		owner := newOwner(false)
		return &runnersv1.CreateAnchoredWorkloadRequest{Preparation: &runnersv1.CreatePreparedWorkloadRequest{BackendId: lifecycleTestBackend, Workload: &runnersv1.CreateWorkloadRequest{
			Id: uuid.NewString(), RunnerId: runnerID, OrganizationId: owner.OrganizationId, ThreadId: owner.ThreadId, AgentClassId: owner.AgentClassId, OwnerKind: owner.OwnerKind, OwnerId: owner.OwnerId, AgentInstanceId: owner.AgentInstanceId, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING, Flavor: flavor}}}
	}
	occupied := func(want int) {
		t.Helper()
		var got int
		if err := reader.QueryRow(ctx, "SELECT occupied FROM workload_flavor_admission WHERE runner_id=$1 AND flavor=$2", runnerID, flavor).Scan(&got); err != nil || got != want {
			t.Fatalf("occupied=%d want=%d err=%v", got, want, err)
		}
	}
	// Hold the shared policy until every independent request is waiting in SQL.
	lock, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(context.Background())
	if _, err = lock.Exec(ctx, "SELECT 1 FROM workload_flavor_admission WHERE runner_id=$1 AND flavor=$2 FOR UPDATE", runnerID, flavor); err != nil {
		t.Fatal(err)
	}
	type result struct {
		response *runnersv1.CreateAnchoredWorkloadResponse
		err      error
	}
	const contenders = 6
	results := make(chan result, contenders)
	for i := 0; i < contenders; i++ {
		req := request()
		go func() { r, e := client.CreateAnchoredWorkload(ctx, req); results <- result{r, e} }()
	}
	blocked := 0
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		err = reader.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name=current_setting('application_name') AND cardinality(pg_blocking_pids(pid))>0").Scan(&blocked)
		if err != nil || blocked >= contenders {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := lock.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	successes := 0
	var winner *runnersv1.Workload
	for i := 0; i < contenders; i++ {
		r := <-results
		if r.err == nil {
			successes++
			winner = r.response.Workload
		} else if status.Code(r.err) != codes.ResourceExhausted {
			t.Errorf("unexpected admission result: %v", r.err)
		}
	}
	if err != nil || blocked < contenders || successes != 1 {
		t.Fatalf("concurrent admission blocked=%d winners=%d err=%v", blocked, successes, err)
	}
	occupied(1)
	// Billing completion, relabeling, history deletion and counter edits cannot
	// manufacture physical absence or release capacity.
	if _, err := pool.Exec(ctx, "UPDATE workloads SET status='failed',removed_at=now() WHERE id=$1", winner.Meta.Id); err != nil {
		t.Fatal(err)
	}
	occupied(1)
	for _, sql := range []string{
		"UPDATE workloads SET flavor='other' WHERE id=$1",
		"UPDATE workloads SET allocated_ram_bytes=allocated_ram_bytes+1 WHERE id=$1",
		"UPDATE workloads SET removal_confirmed_at=now() WHERE id=$1",
		"DELETE FROM workloads WHERE id=$1",
	} {
		if _, err := pool.Exec(ctx, sql, winner.Meta.Id); err == nil {
			t.Fatalf("unsafe change accepted: %s", sql)
		}
		occupied(1)
	}
	if _, err := pool.Exec(ctx, "UPDATE workload_flavor_admission SET occupied=0 WHERE runner_id=$1 AND flavor=$2", runnerID, flavor); err == nil {
		t.Fatal("counter reset accepted")
	}
	if _, err := pool.Exec(ctx, "DELETE FROM workload_flavor_admission WHERE runner_id=$1 AND flavor=$2", runnerID, flavor); err == nil {
		t.Fatal("active policy deletion accepted")
	}
	if _, err := pool.Exec(ctx, "DELETE FROM workload_flavor_reservations WHERE workload_id=$1", winner.Meta.Id); err == nil {
		t.Fatal("reservation deletion accepted")
	}
	// This reservation never authorized native preparation, so the existing
	// revision-checked abort is the one safe early-release operation.
	op := preparedOperation("abort", nil)
	op.Id = winner.Meta.Id
	op.ExpectedRevision = winner.Preparation.Revision
	if _, err := client.UpdateAnchoredWorkload(ctx, &runnersv1.UpdateAnchoredWorkloadRequest{Operation: op, ExpectedAnchorRevision: winner.Preparation.Resources.Revision}); err != nil {
		t.Fatal(err)
	}
	occupied(0)
	// A transaction that never commits cannot strand capacity, even after the
	// database has acquired the slot and checked the native ownership fences.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	_, err = tx.Exec(ctx, `INSERT INTO workloads (id,runner_id,thread_id,agent_id,organization_id,status,owner_kind,owner_id,flavor,preparation_phase,preparation_revision,prepared_backend_id,prepared_volume_ids,resource_anchors)
	 SELECT gen_random_uuid(),runner_id,thread_id,agent_id,organization_id,'starting',owner_kind,owner_id,flavor,'reserved',1,prepared_backend_id,'{}'::uuid[],'{"revision":"1"}'::jsonb FROM workloads WHERE id=$1`, winner.Meta.Id)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	occupied(0)
	if _, err := client.CreateAnchoredWorkload(ctx, request()); err != nil {
		t.Fatalf("capacity not reusable after confirmed abort: %v", err)
	}
	occupied(1)
}
