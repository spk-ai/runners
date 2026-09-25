package server

import (
	"math"
	"time"

	runnerv1 "github.com/agynio/runners/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func validateBoundAnchoredVolume(volume *volumeRecord) error {
	r := volume.AnchorReservation
	if !volume.CheckedLifecycle || volume.ResourceAnchor == nil || volume.BoundInstance == nil || r == nil ||
		volume.InstanceID == nil || *volume.InstanceID != volume.BoundInstance.InstanceId || !canonicalPreparedUUID(volume.BoundInstance.InstanceUid) ||
		len(volume.BoundInstance.ProtoReflect().GetUnknown()) != 0 ||
		!canonicalPreparedUUID(r.WorkloadId) || r.PreparationRevision == 0 || r.PreparationRevision > math.MaxInt64 ||
		r.ResourceRevision == 0 || r.ResourceRevision > math.MaxInt64 || len(r.ProtoReflect().GetUnknown()) != 0 {
		return status.Error(codes.FailedPrecondition, "bound_anchored_volume_required")
	}
	return validateVolumeBinding(*volume, volume.BoundInstance)
}

func validAnchoredRemovalIntent(volume *volumeRecord) bool {
	i := volume.RemovalIntent
	return i != nil && i.Anchored && canonicalPreparedUUID(i.Id) && i.RequestedAt != nil && i.RequestedAt.CheckValid() == nil &&
		len(i.ProtoReflect().GetUnknown()) == 0 && proto.Equal(i.Expected, volume.BoundInstance)
}

// beginAnchoredVolumeRemoval reserves workspace retirement, not idle compute
// release. The current validator requires allocation provenance; adoption does not
// fabricate that reservation. Persist via UpdateVolumeChecked before native removal.
// ../../migrations/0025_anchored_volume_removal.sql shares owner-admission guards,
// excluding every unconfirmed predecessor, including starts with no mounts.
func beginAnchoredVolumeRemoval(volume *volumeRecord) error {
	if err := validateBoundAnchoredVolume(volume); err != nil {
		return err
	}
	if volume.AnchoredRemovalObservation != nil {
		return status.Error(codes.FailedPrecondition, "volume_already_retired")
	}
	if volume.Status == volumeStatusDeprovision && volume.RemovalIntent != nil {
		if !validAnchoredRemovalIntent(volume) || volume.RemovalIntent.ConfirmedAt != nil {
			return status.Error(codes.FailedPrecondition, "anchored_removal_intent_required")
		}
		return nil
	}
	if volume.Status != volumeStatusActive || volume.RemovalIntent != nil {
		return status.Error(codes.FailedPrecondition, "active_anchored_volume_required")
	}
	volume.RemovalIntent = &runnersv1.VolumeRemovalIntent{Id: uuid.NewString(),
		Expected:    proto.Clone(volume.BoundInstance).(*runnerv1.VolumeListItem),
		RequestedAt: timestamppb.New(time.Now().UTC()), Anchored: true}
	volume.Status = volumeStatusDeprovision
	return nil
}

func validateAnchoredVolumeRemovalObservation(expected *runnerv1.VolumeListItem, observation *runnerv1.RemoveVolumeAnchoredResponse) error {
	if expected.GetAnchor() == nil || observation == nil || len(observation.ProtoReflect().GetUnknown()) != 0 ||
		observation.State != runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT ||
		!validVolumeBackend(observation.BackendId) || observation.BackendId != expected.BackendId || !proto.Equal(observation.Anchor, expected.Anchor) {
		return status.Error(codes.FailedPrecondition, "matching_anchored_volume_absence_required")
	}
	return nil
}

// confirmAnchoredVolumeRemoval accepts the original intent and exact native
// PVC-and-owner ABSENT. Retries retain the first receipt/time, binding, anchor and
// reservation. Migration 0025 rejects old confirmation, history deletion and reopen;
// current absence is not future-write fencing.
// @see k8s-runner::internal/server/anchored_volume_removal
func confirmAnchoredVolumeRemoval(volume *volumeRecord, op *runnersv1.ConfirmAnchoredVolumeRemoval) error {
	intent := volume.RemovalIntent
	if !validAnchoredRemovalIntent(volume) || op.GetIntentId() != intent.Id || op == nil || len(op.ProtoReflect().GetUnknown()) != 0 ||
		volume.Status != volumeStatusDeprovision && volume.Status != volumeStatusDeleted {
		return status.Error(codes.FailedPrecondition, "matching_anchored_removal_intent_required")
	}
	if err := validateBoundAnchoredVolume(volume); err != nil {
		return err
	}
	if volume.Status == volumeStatusDeprovision && (intent.ConfirmedAt != nil || volume.AnchoredRemovalObservation != nil) ||
		volume.Status == volumeStatusDeleted && (intent.ConfirmedAt == nil || intent.ConfirmedAt.CheckValid() != nil || volume.AnchoredRemovalObservation == nil) {
		return status.Error(codes.FailedPrecondition, "anchored_retirement_history_inconsistent")
	}
	if err := validateAnchoredVolumeRemovalObservation(intent.Expected, op.GetObservation()); err != nil {
		return err
	}
	if volume.AnchoredRemovalObservation != nil && !proto.Equal(volume.AnchoredRemovalObservation, op.Observation) {
		return status.Error(codes.FailedPrecondition, "anchored_volume_absence_is_immutable")
	}
	volume.RemovalIntent = proto.Clone(intent).(*runnersv1.VolumeRemovalIntent)
	if intent.ConfirmedAt == nil {
		volume.RemovalIntent.ConfirmedAt = timestamppb.New(time.Now().UTC())
	}
	volume.AnchoredRemovalObservation = proto.Clone(op.Observation).(*runnerv1.RemoveVolumeAnchoredResponse)
	volume.Status = volumeStatusDeleted
	return nil
}
