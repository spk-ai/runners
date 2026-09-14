package server

import (
	"context"
	"math"
	"testing"

	runnerv1 "github.com/agynio/runners/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestCheckedVolumeRequestsValidateBeforeStorage(t *testing.T) {
	srv := New(Options{})
	for _, req := range []*runnersv1.CreateVolumeCheckedRequest{
		nil, {}, {Volume: &runnersv1.CreateVolumeRequest{Status: runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE, SizeGb: "1"}},
		{Volume: &runnersv1.CreateVolumeRequest{Status: runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING, SizeGb: "0"}},
		{Volume: &runnersv1.CreateVolumeRequest{Status: runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING, SizeGb: "-1"}},
		{Volume: &runnersv1.CreateVolumeRequest{Status: runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING, SizeGb: "invalid"}},
	} {
		if _, err := srv.CreateVolumeChecked(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid create: %v", err)
		}
	}
	for _, revision := range []uint64{0, math.MaxInt64, math.MaxUint64} {
		_, err := srv.UpdateVolumeChecked(context.Background(), &runnersv1.UpdateVolumeCheckedRequest{
			Id: uuid.NewString(), ExpectedRevision: revision,
			Operation: &runnersv1.UpdateVolumeCheckedRequest_BeginRemoval{BeginRemoval: &runnersv1.BeginVolumeRemoval{}},
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid revision: %v", err)
		}
	}
	for _, req := range []*runnersv1.UpdateVolumeCheckedRequest{nil, {}, {Id: uuid.NewString(), ExpectedRevision: 1}} {
		if _, err := srv.UpdateVolumeChecked(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid update: %v", err)
		}
	}
}

func TestVolumeBindingVerifiesDurableOwner(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		record := volumeRecord{
			Meta: entityMeta{ID: uuid.New()}, VolumeID: uuid.New(), ThreadID: uuid.New(), AgentID: uuid.New(), OwnerID: uuid.New(),
			OwnerKind: runtimeOwnerKindAgentInstance, Status: volumeStatusProvisioning, LifecycleRevision: 1,
		}
		if sandbox {
			record.OwnerKind = runtimeOwnerKindSandbox
		}
		v, err := toProtoVolume(record)
		if err != nil {
			t.Fatal(err)
		}
		original := lifecycleTestInstance(v)
		if err := validateVolumeBinding(record, original); err != nil {
			t.Fatalf("valid binding: %v", err)
		}
		for name, mutate := range map[string]func(*runnerv1.VolumeListItem){
			"blank name":      func(v *runnerv1.VolumeListItem) { v.InstanceId = "" },
			"blank uid":       func(v *runnerv1.VolumeListItem) { v.InstanceUid = "" },
			"padded uid":      func(v *runnerv1.VolumeListItem) { v.InstanceUid += " " },
			"wrong key":       func(v *runnerv1.VolumeListItem) { v.VolumeKey = uuid.NewString() },
			"wrong manager":   func(v *runnerv1.VolumeListItem) { v.IdentityLabels["managed-by"] = "other" },
			"wrong label key": func(v *runnerv1.VolumeListItem) { v.IdentityLabels["volume_key"] = uuid.NewString() },
			"missing labels":  func(v *runnerv1.VolumeListItem) { v.IdentityLabels = nil },
			"padded label":    func(v *runnerv1.VolumeListItem) { v.IdentityLabels["managed-by"] += " " },
			"wrong instance":  func(v *runnerv1.VolumeListItem) { v.IdentityLabels["agent-instance-id"] = uuid.NewString() },
			"wrong sandbox":   func(v *runnerv1.VolumeListItem) { v.IdentityLabels["sandbox-id"] = uuid.NewString() },
		} {
			t.Run(v.OwnerKind.String()+"/"+name, func(t *testing.T) {
				bad := proto.Clone(original).(*runnerv1.VolumeListItem)
				mutate(bad)
				if err := validateVolumeBinding(record, bad); err == nil {
					t.Fatal("invalid binding accepted")
				}
			})
		}
		record.InstanceID = ptr("already-recorded-name")
		if err := validateVolumeBinding(record, original); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("existing backend name rebound: %v", err)
		}
	}
}

func TestCheckedVolumeOperationRejectsUnsafeTransitions(t *testing.T) {
	for _, state := range []string{volumeStatusProvisioning, volumeStatusActive, volumeStatusDeprovision, volumeStatusDeleted, volumeStatusFailed} {
		for _, op := range []*runnersv1.UpdateVolumeCheckedRequest{
			{Operation: &runnersv1.UpdateVolumeCheckedRequest_BeginRemoval{BeginRemoval: &runnersv1.BeginVolumeRemoval{}}},
			{Operation: &runnersv1.UpdateVolumeCheckedRequest_ConfirmRemoval{ConfirmRemoval: &runnersv1.ConfirmVolumeRemoval{IntentId: uuid.NewString()}}},
		} {
			record := volumeRecord{Status: state, CheckedLifecycle: true, LifecycleRevision: 1}
			if err := applyVolumeOperation(&record, op); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("unbound %s transition: %v", state, err)
			}
		}
		if state != volumeStatusProvisioning {
			record := volumeRecord{Status: state, CheckedLifecycle: true}
			err := applyVolumeOperation(&record, &runnersv1.UpdateVolumeCheckedRequest{Operation: &runnersv1.UpdateVolumeCheckedRequest_FailProvisioning{FailProvisioning: &runnersv1.FailVolumeProvisioning{}}})
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("failure bypass from %s: %v", state, err)
			}
		}
	}
}
