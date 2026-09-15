package server

import (
	"testing"

	runnerv1 "github.com/agynio/runners/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

func retirementTestVolume(t *testing.T, sandbox bool) volumeRecord {
	t.Helper()
	w := registryTestAnchoredWorkload(sandbox, 1)
	a := w.Preparation.Resources.Volumes[0]
	v := volumeRecord{Meta: entityMeta{ID: uuid.MustParse(a.ResourceId)}, OwnerKind: w.OwnerKind, OwnerID: w.OwnerID,
		AgentID: w.AgentID, ThreadID: w.ThreadID, Status: volumeStatusActive, SizeGB: "1", CheckedLifecycle: true,
		ResourceAnchor: a, AnchorReservation: &runnersv1.VolumeAnchorReservation{WorkloadId: w.Meta.ID.String(), PreparationRevision: 1, ResourceRevision: 1}}
	v.BoundInstance = &runnerv1.VolumeListItem{VolumeKey: a.ResourceId, InstanceId: "pv-" + a.ResourceId,
		InstanceUid: uuid.NewString(), BackendId: a.BackendId, IdentityLabels: a.IdentityLabels, Anchor: a}
	v.InstanceID = ptr(v.BoundInstance.InstanceId)
	return v
}

func retirementTestObservation(v volumeRecord) *runnerv1.RemoveVolumeAnchoredResponse {
	return &runnerv1.RemoveVolumeAnchoredResponse{State: runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT,
		BackendId: v.BoundInstance.BackendId, Anchor: proto.Clone(v.ResourceAnchor).(*runnerv1.ResourceAnchor)}
}

func TestAnchoredVolumeRetirement(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		v := retirementTestVolume(t, sandbox)
		t.Run(v.OwnerKind, func(t *testing.T) {
			begin := &runnersv1.UpdateVolumeCheckedRequest{Operation: &runnersv1.UpdateVolumeCheckedRequest_BeginAnchoredRemoval{BeginAnchoredRemoval: &runnersv1.BeginAnchoredVolumeRemoval{}}}
			if err := applyVolumeOperation(&v, begin); err != nil || v.Status != volumeStatusDeprovision || !v.RemovalIntent.GetAnchored() {
				t.Fatalf("begin: %v", err)
			}
			intent := proto.Clone(v.RemovalIntent)
			if err := applyVolumeOperation(&v, begin); err != nil || !proto.Equal(intent, v.RemovalIntent) {
				t.Fatalf("begin retry replaced intent: %v", err)
			}
			for name, mutate := range map[string]func(*runnersv1.ConfirmAnchoredVolumeRemoval){
				"wrong-intent": func(op *runnersv1.ConfirmAnchoredVolumeRemoval) { op.IntentId = uuid.NewString() },
				"nil-receipt":  func(op *runnersv1.ConfirmAnchoredVolumeRemoval) { op.Observation = nil },
				"pending": func(op *runnersv1.ConfirmAnchoredVolumeRemoval) {
					op.Observation.State = runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING
				},
				"unknown-state": func(op *runnersv1.ConfirmAnchoredVolumeRemoval) { op.Observation.State = 97 },
				"wrong-backend": func(op *runnersv1.ConfirmAnchoredVolumeRemoval) { op.Observation.BackendId = "other" },
				"new-owner":     func(op *runnersv1.ConfirmAnchoredVolumeRemoval) { op.Observation.Anchor.InstanceUid = uuid.NewString() },
				"new-volume":    func(op *runnersv1.ConfirmAnchoredVolumeRemoval) { op.Observation.Anchor.ResourceId = uuid.NewString() },
				"no-owner":      func(op *runnersv1.ConfirmAnchoredVolumeRemoval) { op.Observation.Anchor = nil },
				"unknown-receipt": func(op *runnersv1.ConfirmAnchoredVolumeRemoval) {
					op.Observation.ProtoReflect().SetUnknown([]byte{0x78, 1})
				},
				"unknown-operation": func(op *runnersv1.ConfirmAnchoredVolumeRemoval) { op.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
			} {
				t.Run(name, func(t *testing.T) {
					op := &runnersv1.ConfirmAnchoredVolumeRemoval{IntentId: v.RemovalIntent.Id, Observation: retirementTestObservation(v)}
					mutate(op)
					copy := v
					if err := confirmAnchoredVolumeRemoval(&copy, op); err == nil || !proto.Equal(copy.RemovalIntent, intent) || copy.AnchoredRemovalObservation != nil {
						t.Fatal("mismatched absence was accepted or changed history")
					}
				})
			}
			op := &runnersv1.ConfirmAnchoredVolumeRemoval{IntentId: v.RemovalIntent.Id, Observation: retirementTestObservation(v)}
			if err := confirmAnchoredVolumeRemoval(&v, op); err != nil || v.Status != volumeStatusDeleted || !proto.Equal(v.AnchoredRemovalObservation, op.Observation) {
				t.Fatalf("confirm: %v", err)
			}
			confirmed := proto.Clone(v.RemovalIntent)
			if err := confirmAnchoredVolumeRemoval(&v, op); err != nil || !proto.Equal(confirmed, v.RemovalIntent) {
				t.Fatal("confirmation retry changed receipt")
			}
			for _, old := range []*runnersv1.UpdateVolumeCheckedRequest{
				begin,
				{Operation: &runnersv1.UpdateVolumeCheckedRequest_BeginRemoval{BeginRemoval: &runnersv1.BeginVolumeRemoval{}}},
				{Operation: &runnersv1.UpdateVolumeCheckedRequest_ConfirmRemoval{ConfirmRemoval: &runnersv1.ConfirmVolumeRemoval{IntentId: v.RemovalIntent.Id, BackendId: v.BoundInstance.BackendId}}},
				{Operation: &runnersv1.UpdateVolumeCheckedRequest_Reopen{Reopen: &runnersv1.ReopenVolume{}}},
				{Operation: &runnersv1.UpdateVolumeCheckedRequest_FailProvisioning{FailProvisioning: &runnersv1.FailVolumeProvisioning{}}},
			} {
				if err := applyVolumeOperation(&v, old); err == nil || !proto.Equal(confirmed, v.RemovalIntent) {
					t.Fatal("retired history reopened or old API accepted")
				}
			}
		})
	}
}

func TestAnchoredVolumeRetirementRejectsIncompleteBinding(t *testing.T) {
	for name, mutate := range map[string]func(*volumeRecord){
		"unbound":        func(v *volumeRecord) { v.BoundInstance = nil },
		"no-anchor":      func(v *volumeRecord) { v.ResourceAnchor = nil },
		"no-receipt":     func(v *volumeRecord) { v.AnchorReservation = nil },
		"bad-receipt":    func(v *volumeRecord) { v.AnchorReservation.ResourceRevision = 0 },
		"no-instance":    func(v *volumeRecord) { v.InstanceID = nil },
		"wrong-instance": func(v *volumeRecord) { v.InstanceID = ptr("other") },
		"no-uid":         func(v *volumeRecord) { v.BoundInstance.InstanceUid = "" },
		"legacy":         func(v *volumeRecord) { v.CheckedLifecycle = false },
		"provisioning":   func(v *volumeRecord) { v.Status = volumeStatusProvisioning },
		"failed":         func(v *volumeRecord) { v.Status = volumeStatusFailed },
	} {
		t.Run(name, func(t *testing.T) {
			v := retirementTestVolume(t, false)
			mutate(&v)
			if err := beginAnchoredVolumeRemoval(&v); err == nil || v.RemovalIntent != nil {
				t.Fatal("invalid binding gained retirement intent")
			}
		})
	}
}
