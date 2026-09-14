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
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testCheckedVolumeLifecycle(t *testing.T, ctx context.Context, srv *Server, pool *pgxpool.Pool, reader *pgx.Conn, newRequest func(bool) *runnersv1.CreateVolumeRequest) {
	snapshot := func(t *testing.T, id string) string {
		t.Helper()
		var value string
		if err := reader.QueryRow(ctx, "SELECT to_jsonb(volumes)::text FROM volumes WHERE id = $1", id).Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	read := func(t *testing.T, id string) *runnersv1.Volume {
		t.Helper()
		record, err := scanVolume(reader.QueryRow(ctx, "SELECT "+volumeColumns+" FROM volumes WHERE id = $1", id))
		if err != nil {
			t.Fatal(err)
		}
		volume, err := toProtoVolume(record)
		if err != nil {
			t.Fatal(err)
		}
		return volume
	}
	create := func(t *testing.T, sandbox bool) (*runnersv1.CreateVolumeRequest, *runnersv1.Volume) {
		t.Helper()
		req := newRequest(sandbox)
		resp, err := srv.CreateVolumeChecked(ctx, &runnersv1.CreateVolumeCheckedRequest{Volume: req})
		if err != nil {
			t.Fatal(err)
		}
		v := resp.Volume
		if v.GetLifecycleRevision() != 1 || !v.GetCheckedLifecycle() || v.BoundInstance != nil || v.RemovalIntent != nil || v.GetStatus() != req.Status || !proto.Equal(v, read(t, req.Id)) {
			t.Fatal("new checked record is not persisted with an initial revision")
		}
		return req, v
	}
	update := func(t *testing.T, v *runnersv1.Volume, req *runnersv1.UpdateVolumeCheckedRequest) *runnersv1.Volume {
		t.Helper()
		req.Id, req.ExpectedRevision = v.Meta.Id, v.LifecycleRevision
		resp, err := srv.UpdateVolumeChecked(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Volume.LifecycleRevision != v.LifecycleRevision+1 || !resp.Volume.CheckedLifecycle || !proto.Equal(resp.Volume, read(t, v.Meta.Id)) {
			t.Fatal("checked response is not the next independently observed committed revision")
		}
		return resp.Volume
	}
	reject := func(t *testing.T, v *runnersv1.Volume, req *runnersv1.UpdateVolumeCheckedRequest, code codes.Code) {
		t.Helper()
		req.Id = v.Meta.Id
		if req.ExpectedRevision == 0 {
			req.ExpectedRevision = v.LifecycleRevision
		}
		before := snapshot(t, req.Id)
		resp, err := srv.UpdateVolumeChecked(ctx, req)
		if status.Code(err) != code || resp != nil || snapshot(t, req.Id) != before {
			t.Fatalf("rejected update changed state: code=%s want=%s err=%v", status.Code(err), code, err)
		}
	}
	bind := func(t *testing.T, v *runnersv1.Volume) *runnersv1.Volume {
		t.Helper()
		return update(t, v, &runnersv1.UpdateVolumeCheckedRequest{Operation: &runnersv1.UpdateVolumeCheckedRequest_Bind{
			Bind: &runnersv1.BindVolumeInstance{Instance: lifecycleTestInstance(v)},
		}})
	}
	begin := func(v *runnersv1.Volume) *runnersv1.UpdateVolumeCheckedRequest {
		return &runnersv1.UpdateVolumeCheckedRequest{Id: v.Meta.Id, ExpectedRevision: v.LifecycleRevision,
			Operation: &runnersv1.UpdateVolumeCheckedRequest_BeginRemoval{BeginRemoval: &runnersv1.BeginVolumeRemoval{}}}
	}
	reopen := func(req *runnersv1.CreateVolumeRequest) *runnersv1.UpdateVolumeCheckedRequest {
		return &runnersv1.UpdateVolumeCheckedRequest{Operation: &runnersv1.UpdateVolumeCheckedRequest_Reopen{Reopen: &runnersv1.ReopenVolume{Volume: req}}}
	}
	confirm := func(intentID string) *runnersv1.UpdateVolumeCheckedRequest {
		return &runnersv1.UpdateVolumeCheckedRequest{Operation: &runnersv1.UpdateVolumeCheckedRequest_ConfirmRemoval{ConfirmRemoval: &runnersv1.ConfirmVolumeRemoval{IntentId: intentID}}}
	}
	waitBlocked := func(want int) (int, error) {
		blocked := 0
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			err := reader.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND cardinality(pg_blocking_pids(pid)) > 0", pool.Config().ConnConfig.RuntimeParams["application_name"]).Scan(&blocked)
			if err != nil || blocked >= want {
				return blocked, err
			}
			time.Sleep(10 * time.Millisecond)
		}
		return blocked, fmt.Errorf("timed out waiting for %d blocked checked updates", want)
	}

	for _, sandbox := range []bool{false, true} {
		t.Run(fmt.Sprintf("cycle/sandbox=%t", sandbox), func(t *testing.T) {
			req, v := create(t, sandbox)
			reject(t, v, begin(v), codes.FailedPrecondition)
			v = bind(t, v)
			oldBegin := begin(v)
			target := proto.Clone(v.BoundInstance).(*runnerv1.VolumeListItem)
			changed := proto.Clone(target).(*runnerv1.VolumeListItem)
			changed.InstanceUid = uuid.NewString()
			reject(t, v, &runnersv1.UpdateVolumeCheckedRequest{Operation: &runnersv1.UpdateVolumeCheckedRequest_Bind{Bind: &runnersv1.BindVolumeInstance{Instance: changed}}}, codes.FailedPrecondition)
			v = update(t, v, begin(v))
			if v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING || v.RemovalIntent.GetId() == "" || v.RemovalIntent.RequestedAt == nil || v.RemovalIntent.ConfirmedAt != nil || !proto.Equal(target, v.RemovalIntent.Expected) {
				t.Fatal("removal intent is not pinned to the original binding")
			}
			intent := proto.Clone(v.RemovalIntent).(*runnersv1.VolumeRemovalIntent)
			// A new server object, not in-memory coordinator state, recovers the intent.
			restarted := New(Options{Pool: pool})
			persisted, err := restarted.getVolumeByID(ctx, uuid.MustParse(v.Meta.Id))
			if err != nil || !proto.Equal(persisted.RemovalIntent, intent) {
				t.Fatalf("intent recovery: %v", err)
			}
			v = update(t, v, begin(v))
			if !proto.Equal(intent, v.RemovalIntent) {
				t.Fatal("begin retry retargeted the durable intent")
			}
			billingTime := timestamppb.New(time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond))
			billed, err := srv.UpdateVolume(ctx, &runnersv1.UpdateVolumeRequest{Id: v.Meta.Id, RemovedAt: billingTime, LastMeteringSampledAt: billingTime})
			if err != nil || billed.Volume.LifecycleRevision != v.LifecycleRevision || billed.Volume.RemovalIntent.ConfirmedAt != nil {
				t.Fatalf("billing was confused with lifecycle confirmation: %v", err)
			}
			reject(t, v, reopen(req), codes.FailedPrecondition)
			reject(t, v, confirm(uuid.NewString()), codes.FailedPrecondition)
			before := snapshot(t, v.Meta.Id)
			deleted := runnersv1.VolumeStatus_VOLUME_STATUS_DELETED
			if _, err := srv.UpdateVolume(ctx, &runnersv1.UpdateVolumeRequest{Id: v.Meta.Id, Status: &deleted}); status.Code(err) != codes.FailedPrecondition || snapshot(t, v.Meta.Id) != before {
				t.Fatalf("legacy API closed a checked record: %v", err)
			}
			for name, sql := range map[string]string{
				"legacy-failure":  "UPDATE volumes SET status = 'failed' WHERE id = $1",
				"legacy-instance": "UPDATE volumes SET instance_id = 'replacement' WHERE id = $1",
				"legacy-reopen":   "UPDATE volumes SET status = 'provisioning', removed_at = NULL WHERE id = $1",
				"unprotect":       "UPDATE volumes SET checked_lifecycle = FALSE, lifecycle_revision = lifecycle_revision + 1 WHERE id = $1",
				"owner":           "UPDATE volumes SET owner_id = '00000000-0000-0000-0000-000000000001', lifecycle_revision = lifecycle_revision + 1 WHERE id = $1",
				"retarget":        "UPDATE volumes SET bound_instance = jsonb_set(bound_instance, '{instanceUid}', '\"replacement\"'), lifecycle_revision = lifecycle_revision + 1 WHERE id = $1",
				"discard-intent":  "UPDATE volumes SET removal_intent = NULL, lifecycle_revision = lifecycle_revision + 1 WHERE id = $1",
				"delete-record":   "DELETE FROM volumes WHERE id = $1",
			} {
				t.Run("raw-sql/"+name, func(t *testing.T) {
					if _, err := pool.Exec(ctx, sql, v.Meta.Id); err == nil || snapshot(t, v.Meta.Id) != before {
						t.Fatalf("unsafe SQL accepted: %v", err)
					}
				})
			}
			v = update(t, v, confirm(intent.Id))
			if v.Status != deleted || v.RemovalIntent.ConfirmedAt == nil || !proto.Equal(v.RemovalIntent.Expected, target) || !proto.Equal(v.RemovedAt, billingTime) {
				t.Fatal("confirmation lost the target or overwrote metering")
			}
			for _, checked := range []bool{false, true} {
				before := snapshot(t, v.Meta.Id)
				var err error
				if checked {
					_, err = srv.CreateVolumeChecked(ctx, &runnersv1.CreateVolumeCheckedRequest{Volume: req})
				} else {
					_, err = srv.CreateVolume(ctx, req)
				}
				if status.Code(err) != codes.AlreadyExists || snapshot(t, v.Meta.Id) != before {
					t.Fatalf("create implicitly reopened a checked volume: %v", err)
				}
			}
			foreign := proto.Clone(req).(*runnersv1.CreateVolumeRequest)
			foreign.OrganizationId = uuid.NewString()
			reject(t, v, reopen(foreign), codes.FailedPrecondition)
			v = update(t, v, reopen(req))
			if v.BoundInstance != nil || v.RemovalIntent != nil || v.InstanceId != nil || v.RemovedAt != nil || v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING {
				t.Fatal("reopen retained a completed generation's deletion authority")
			}
			reject(t, v, oldBegin, codes.Aborted)
			reject(t, v, confirm(intent.Id), codes.FailedPrecondition)
			v = bind(t, v)
			if v.BoundInstance.InstanceUid == target.InstanceUid {
				t.Fatal("test did not bind a new physical generation")
			}
			reject(t, v, oldBegin, codes.Aborted)
		})
	}

	t.Run("failed-provisioning-is-not-removal", func(t *testing.T) {
		req, v := create(t, false)
		v = update(t, v, &runnersv1.UpdateVolumeCheckedRequest{Operation: &runnersv1.UpdateVolumeCheckedRequest_FailProvisioning{FailProvisioning: &runnersv1.FailVolumeProvisioning{}}})
		if v.RemovedAt == nil || v.RemovalIntent != nil || v.BoundInstance != nil {
			t.Fatal("failed provisioning invented deletion evidence")
		}
		v = update(t, v, reopen(req))
		if v.RemovedAt != nil || v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING {
			t.Fatal("explicit provisioning recovery failed")
		}
	})

	t.Run("all-reopen-identity-fields", func(t *testing.T) {
		mutations := map[string]func(*runnersv1.CreateVolumeRequest){
			"id":           func(r *runnersv1.CreateVolumeRequest) { r.Id = uuid.NewString() },
			"definition":   func(r *runnersv1.CreateVolumeRequest) { r.VolumeDefinitionId = ptr(uuid.NewString()) },
			"owner":        func(r *runnersv1.CreateVolumeRequest) { r.AgentInstanceId = ptr(uuid.NewString()) },
			"thread":       func(r *runnersv1.CreateVolumeRequest) { r.ThreadId = uuid.NewString() },
			"class":        func(r *runnersv1.CreateVolumeRequest) { r.AgentClassId = ptr(uuid.NewString()) },
			"organization": func(r *runnersv1.CreateVolumeRequest) { r.OrganizationId = uuid.NewString() },
			"runner":       func(r *runnersv1.CreateVolumeRequest) { r.RunnerId = uuid.NewString() },
			"owner-kind": func(r *runnersv1.CreateVolumeRequest) {
				r.OwnerKind = runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX
				r.OwnerId = r.GetAgentInstanceId()
				r.AgentInstanceId = nil
			},
		}
		req, v := create(t, false)
		v = update(t, v, &runnersv1.UpdateVolumeCheckedRequest{Operation: &runnersv1.UpdateVolumeCheckedRequest_FailProvisioning{FailProvisioning: &runnersv1.FailVolumeProvisioning{}}})
		for name, mutate := range mutations {
			t.Run(name, func(t *testing.T) {
				foreign := proto.Clone(req).(*runnersv1.CreateVolumeRequest)
				mutate(foreign)
				reject(t, v, reopen(foreign), codes.FailedPrecondition)
			})
		}
	})

	t.Run("concurrent-intent-CAS", func(t *testing.T) {
		_, v := create(t, false)
		v = bind(t, v)
		lock, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Rollback(context.Background())
		if _, err := lock.Exec(ctx, "SELECT id FROM volumes WHERE id = $1 FOR UPDATE", v.Meta.Id); err != nil {
			t.Fatal(err)
		}
		type result struct {
			response *runnersv1.UpdateVolumeCheckedResponse
			err      error
		}
		const contenders = 8
		results := make(chan result, contenders)
		for i := 0; i < contenders; i++ {
			go func() {
				resp, err := srv.UpdateVolumeChecked(ctx, begin(v))
				results <- result{resp, err}
			}()
		}
		blocked, err := waitBlocked(contenders)
		releaseErr := lock.Rollback(ctx)
		successes := 0
		for i := 0; i < contenders; i++ {
			result := <-results
			if result.err == nil {
				successes++
				if result.response.Volume.LifecycleRevision != v.LifecycleRevision+1 || result.response.Volume.RemovalIntent == nil {
					t.Error("invalid CAS winner")
				}
			} else if status.Code(result.err) != codes.Aborted || result.response != nil {
				t.Errorf("unexpected CAS loser: %v", result.err)
			}
		}
		stored := read(t, v.Meta.Id)
		if err != nil || releaseErr != nil || blocked != contenders || successes != 1 || stored.LifecycleRevision != v.LifecycleRevision+1 {
			t.Fatalf("CAS proof: blocked=%d successes=%d wait=%v release=%v", blocked, successes, err, releaseErr)
		}
	})

	t.Run("metering-after-CAS-read", func(t *testing.T) {
		_, v := create(t, false)
		v = bind(t, v)
		lock, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Rollback(context.Background())
		if _, err := lock.Exec(ctx, "SELECT id FROM volumes WHERE id = $1 FOR UPDATE", v.Meta.Id); err != nil {
			t.Fatal(err)
		}
		type result struct {
			response *runnersv1.UpdateVolumeCheckedResponse
			err      error
		}
		resultCh := make(chan result, 1)
		go func() { resp, err := srv.UpdateVolumeChecked(ctx, begin(v)); resultCh <- result{resp, err} }()
		blocked, waitErr := waitBlocked(1)
		billing := time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond)
		var writeErr, releaseErr error
		if waitErr == nil {
			_, writeErr = lock.Exec(ctx, "UPDATE volumes SET removed_at = $2, last_metering_sampled_at = $2 WHERE id = $1", v.Meta.Id, billing)
		}
		if waitErr == nil && writeErr == nil {
			releaseErr = lock.Commit(ctx)
		} else {
			releaseErr = lock.Rollback(ctx)
		}
		outcome := <-resultCh
		if waitErr != nil || writeErr != nil || releaseErr != nil || blocked != 1 || outcome.err != nil {
			t.Fatalf("metering interleaving: blocked=%d wait=%v write=%v release=%v update=%v", blocked, waitErr, writeErr, releaseErr, outcome.err)
		}
		stored := read(t, v.Meta.Id)
		if !proto.Equal(stored, outcome.response.Volume) || stored.LifecycleRevision != v.LifecycleRevision+1 ||
			stored.RemovedAt == nil || !stored.RemovedAt.AsTime().Equal(billing) || stored.LastMeteringSampledAt == nil || !stored.LastMeteringSampledAt.AsTime().Equal(billing) || stored.RemovalIntent.GetConfirmedAt() != nil {
			t.Fatal("checked update overwrote a concurrent billing sample or invented confirmation")
		}
	})
}

func lifecycleTestInstance(volume *runnersv1.Volume) *runnerv1.VolumeListItem {
	identity := map[string]string{
		"app.kubernetes.io/managed-by": "k8s-runner", "agyn.dev/managed-by": "agents-orchestrator",
		"managed-by": "agents-orchestrator", "volume_key": volume.Meta.Id,
	}
	if volume.OwnerKind == runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX {
		identity["sandbox-id"], identity["sandbox-owner-id"] = volume.OwnerId, uuid.NewString()
	} else {
		identity["agent-instance-id"], identity["agent-id"] = volume.OwnerId, volume.GetAgentClassId()
	}
	return &runnerv1.VolumeListItem{InstanceId: "pv-" + volume.Meta.Id, VolumeKey: volume.Meta.Id, InstanceUid: uuid.NewString(), IdentityLabels: identity}
}
