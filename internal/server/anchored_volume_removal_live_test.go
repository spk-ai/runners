package server

import (
	"context"
	"testing"

	runnerv1 "github.com/agynio/runners/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func beginRetirementRequest(v *runnersv1.Volume) *runnersv1.UpdateVolumeCheckedRequest {
	return &runnersv1.UpdateVolumeCheckedRequest{Id: v.Meta.Id, ExpectedRevision: v.LifecycleRevision,
		Operation: &runnersv1.UpdateVolumeCheckedRequest_BeginAnchoredRemoval{BeginAnchoredRemoval: &runnersv1.BeginAnchoredVolumeRemoval{}}}
}

// The parent has executed two complete anchored lifecycle sequences. SQL and
// registry RPCs are real; native observations here are fixtures, not GC evidence.
func testAnchoredVolumeRetirementRPC(t *testing.T, ctx context.Context, pool *pgxpool.Pool, reader *pgx.Conn, initial *runnersv1.Volume) {
	client, stop := preparedRegistryClient(t, pool)
	read := func() *runnersv1.Volume {
		t.Helper()
		r, err := scanVolume(reader.QueryRow(ctx, "SELECT "+volumeColumns+" FROM volumes WHERE id=$1", initial.Meta.Id))
		if err != nil {
			t.Fatal(err)
		}
		v, err := toProtoVolume(r)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	v := read()
	request := beginRetirementRequest(v)
	if _, err := client.UpdateVolumeChecked(ctx, request); err != nil {
		t.Fatal(err)
	}
	// Discard the ACK and replace the service; only a fresh SQL read recovers it.
	stop()
	client, stop = preparedRegistryClient(t, pool)
	defer stop()
	v = read()
	if v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING || !v.RemovalIntent.GetAnchored() || v.AnchoredRemovalObservation != nil {
		t.Fatal("retirement intent was not durable")
	}
	if _, err := client.UpdateVolumeChecked(ctx, request); status.Code(err) != codes.Aborted {
		t.Fatalf("stale begin replay accepted: %v", err)
	}
	intent := proto.Clone(v.RemovalIntent)
	if _, err := client.UpdateVolumeChecked(ctx, beginRetirementRequest(v)); err != nil {
		t.Fatal(err)
	}
	v = read()
	if !proto.Equal(v.RemovalIntent, intent) {
		t.Fatal("fresh begin replaced original intent")
	}
	for _, volumes := range [][]string{nil, {v.Meta.Id}} {
		req := preparedCreateRequest(v)
		req.VolumeIds = volumes
		if _, err := client.CreateAnchoredWorkload(ctx, &runnersv1.CreateAnchoredWorkloadRequest{Preparation: req}); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("workload admitted during retirement: %v", err)
		}
	}
	absent := &runnerv1.RemoveVolumeAnchoredResponse{State: runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT,
		BackendId: v.BoundInstance.BackendId, Anchor: proto.Clone(v.ResourceAnchor).(*runnerv1.ResourceAnchor)}
	confirm := func(obs *runnerv1.RemoveVolumeAnchoredResponse) *runnersv1.UpdateVolumeCheckedRequest {
		return &runnersv1.UpdateVolumeCheckedRequest{Id: v.Meta.Id, ExpectedRevision: v.LifecycleRevision,
			Operation: &runnersv1.UpdateVolumeCheckedRequest_ConfirmAnchoredRemoval{ConfirmAnchoredRemoval: &runnersv1.ConfirmAnchoredVolumeRemoval{IntentId: v.RemovalIntent.Id, Observation: obs}}}
	}
	for name, mutate := range map[string]func(*runnerv1.RemoveVolumeAnchoredResponse){
		"pending": func(o *runnerv1.RemoveVolumeAnchoredResponse) {
			o.State = runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING
		},
		"unknown":       func(o *runnerv1.RemoveVolumeAnchoredResponse) { o.State = 99 },
		"wrong-backend": func(o *runnerv1.RemoveVolumeAnchoredResponse) { o.BackendId = "other" },
		"wrong-owner":   func(o *runnerv1.RemoveVolumeAnchoredResponse) { o.Anchor.InstanceUid = v.BoundInstance.InstanceUid },
		"unknown-wire":  func(o *runnerv1.RemoveVolumeAnchoredResponse) { o.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
	} {
		t.Run(name, func(t *testing.T) {
			bad := proto.Clone(absent).(*runnerv1.RemoveVolumeAnchoredResponse)
			mutate(bad)
			if _, err := client.UpdateVolumeChecked(ctx, confirm(bad)); status.Code(err) != codes.FailedPrecondition || !proto.Equal(read(), v) {
				t.Fatalf("invalid confirmation changed registry: %v", err)
			}
		})
	}
	for _, sql := range []string{
		`UPDATE volumes SET status='deleted', lifecycle_revision=lifecycle_revision+1, removal_intent=removal_intent || '{"confirmedAt":"2026-09-15T00:00:00Z"}' WHERE id=$1`,
		`UPDATE volumes SET removal_intent=removal_intent-'anchored', lifecycle_revision=lifecycle_revision+1 WHERE id=$1`,
		`UPDATE volumes SET removal_intent=NULL, status='active', lifecycle_revision=lifecycle_revision+1 WHERE id=$1`,
		`UPDATE volumes SET anchored_removal_observation=jsonb_build_object('state','VOLUME_REMOVAL_STATE_ABSENT','backendId',bound_instance->>'backendId','anchor',resource_anchor) WHERE id=$1`,
	} {
		if _, err := pool.Exec(ctx, sql, v.Meta.Id); status.Code(volumeLifecycleStatusError(err)) != codes.FailedPrecondition || !proto.Equal(read(), v) {
			t.Fatalf("old SQL bypassed retirement: %v", err)
		}
	}
	if _, err := client.UpdateVolumeChecked(ctx, confirm(absent)); err != nil {
		t.Fatal(err)
	}
	stop()
	client, stop = preparedRegistryClient(t, pool)
	defer stop()
	v = read()
	if v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_DELETED || v.RemovalIntent.ConfirmedAt == nil ||
		!proto.Equal(v.AnchoredRemovalObservation, absent) || !proto.Equal(v.ResourceAnchor, initial.ResourceAnchor) ||
		!proto.Equal(v.BoundInstance, initial.BoundInstance) || !proto.Equal(v.AnchorReservation, initial.AnchorReservation) {
		t.Fatal("retirement did not preserve exact durable evidence")
	}
	for _, sql := range []string{
		`UPDATE volumes SET anchored_removal_observation=NULL, lifecycle_revision=lifecycle_revision+1 WHERE id=$1`,
		`UPDATE volumes SET resource_anchor=NULL, anchor_reservation=NULL, lifecycle_revision=lifecycle_revision+1 WHERE id=$1`,
		`UPDATE volumes SET status='provisioning', instance_id=NULL, bound_instance=NULL, removal_intent=NULL, anchored_removal_observation=NULL, lifecycle_revision=lifecycle_revision+1 WHERE id=$1`,
		`DELETE FROM volumes WHERE id=$1`,
	} {
		if _, err := pool.Exec(ctx, sql, v.Meta.Id); status.Code(volumeLifecycleStatusError(err)) != codes.FailedPrecondition || !proto.Equal(read(), v) {
			t.Fatalf("retired history changed: %v", err)
		}
	}
	if _, err := client.UpdateVolumeChecked(ctx, confirm(absent)); err != nil {
		t.Fatal(err)
	}
	latest := read()
	if !proto.Equal(latest.RemovalIntent, v.RemovalIntent) || !proto.Equal(latest.AnchoredRemovalObservation, v.AnchoredRemovalObservation) {
		t.Fatal("confirmation retry changed immutable evidence")
	}
}
