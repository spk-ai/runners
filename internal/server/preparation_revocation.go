package server

import (
	"context"
	"maps"
	"slices"
	"strings"

	runnerv1 "github.com/agynio/runners/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation"
)

func canonicalRegistryPreparationRevocation(w workloadRecord, value *runnerv1.PreparationRevocation) (*runnerv1.PreparationRevocation, error) {
	p := w.Preparation
	if p == nil || p.Resources == nil || p.Resources.Workload == nil || value == nil ||
		len(value.ProtoReflect().GetUnknown()) != 0 || !canonicalPreparedUUID(value.InstanceUid) ||
		value.SelectedPodUid != "" && !canonicalPreparedUUID(value.SelectedPodUid) ||
		!proto.Equal(value.WorkloadAnchor, p.Resources.Workload) || len(value.VolumeAnchors) != len(p.Resources.Volumes) || len(value.VolumeAnchors) > 64 {
		return nil, status.Error(codes.InvalidArgument, "complete_matching_preparation_revocation_required")
	}
	seen := map[string]bool{}
	for _, anchor := range value.VolumeAnchors {
		if anchor == nil || seen[anchor.ResourceId] || !slices.ContainsFunc(p.Resources.Volumes, func(expected *runnerv1.ResourceAnchor) bool { return proto.Equal(expected, anchor) }) {
			return nil, status.Error(codes.FailedPrecondition, "preparation_revocation_anchor_set_changed")
		}
		seen[anchor.ResourceId] = true
	}
	copy := proto.Clone(value).(*runnerv1.PreparationRevocation)
	slices.SortFunc(copy.VolumeAnchors, func(a, b *runnerv1.ResourceAnchor) int { return strings.Compare(a.ResourceId, b.ResourceId) })
	if data, err := protojson.Marshal(copy); err != nil || len(data) > 128*1024 {
		return nil, status.Error(codes.InvalidArgument, "preparation_revocation_too_large")
	}
	return copy, nil
}

func canonicalRegistryRevocationObservation(w workloadRecord, expected *runnerv1.PreparationRevocation, value *runnerv1.ObservePreparationRevocationResponse) (*runnerv1.ObservePreparationRevocationResponse, error) {
	if expected == nil || value == nil || len(value.ProtoReflect().GetUnknown()) != 0 ||
		value.State != runnerv1.RevokedPreparationState_REVOKED_PREPARATION_STATE_POD_ABSENT ||
		len(value.Volumes)+len(value.AbsentVolumeIds) != len(expected.VolumeAnchors) {
		return nil, status.Error(codes.FailedPrecondition, "complete_revoked_preparation_observation_required")
	}
	receipt, err := canonicalRegistryPreparationRevocation(w, value.Revocation)
	if err != nil {
		return nil, err
	}
	if !proto.Equal(receipt, expected) {
		return nil, status.Error(codes.FailedPrecondition, "persisted_preparation_revocation_required")
	}
	anchors := map[string]*runnerv1.ResourceAnchor{}
	for _, a := range expected.VolumeAnchors {
		anchors[a.ResourceId] = a
	}
	seen, names, uids := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, volume := range value.Volumes {
		if volume == nil || len(volume.ProtoReflect().GetUnknown()) != 0 ||
			!canonicalPreparedUUID(volume.InstanceUid) || volume.InstanceId == "" || len(validation.IsDNS1123Subdomain(volume.InstanceId)) != 0 ||
			seen[volume.VolumeKey] || names[volume.InstanceId] || uids[volume.InstanceUid] || anchors[volume.VolumeKey] == nil ||
			volume.BackendId != expected.WorkloadAnchor.BackendId || !proto.Equal(volume.Anchor, anchors[volume.VolumeKey]) ||
			!maps.Equal(volume.IdentityLabels, volume.Anchor.IdentityLabels) {
			return nil, status.Error(codes.FailedPrecondition, "revocation_workspace_identity_invalid")
		}
		seen[volume.VolumeKey], names[volume.InstanceId], uids[volume.InstanceUid] = true, true, true
	}
	for _, id := range value.AbsentVolumeIds {
		if anchors[id] == nil || seen[id] {
			return nil, status.Error(codes.FailedPrecondition, "revocation_workspace_partition_invalid")
		}
		seen[id] = true
	}
	copy := proto.Clone(value).(*runnerv1.ObservePreparationRevocationResponse)
	copy.Revocation = receipt
	slices.SortFunc(copy.Volumes, func(a, b *runnerv1.VolumeListItem) int { return strings.Compare(a.VolumeKey, b.VolumeKey) })
	slices.Sort(copy.AbsentVolumeIds)
	if data, err := protojson.Marshal(copy); err != nil || len(data) > 256*1024 {
		return nil, status.Error(codes.InvalidArgument, "preparation_revocation_observation_too_large")
	}
	return copy, nil
}

func validateStoredPreparationRevocation(w workloadRecord, resources *runnersv1.WorkloadResourceAnchors) error {
	receipt, observation := resources.PreparationRevocation, resources.RevocationObservation
	if receipt == nil && observation == nil {
		return nil
	}
	p := w.Preparation
	if receipt == nil || p.Binding != nil || p.RemovalObservation != nil ||
		p.Phase != preparationPhases["removing"] && p.Phase != preparationPhases["removed"] ||
		(p.Phase == preparationPhases["removed"]) != (observation != nil) {
		return status.Error(codes.FailedPrecondition, "invalid_stored_preparation_revocation")
	}
	value, err := canonicalRegistryPreparationRevocation(w, receipt)
	if err != nil {
		return err
	}
	if !proto.Equal(value, receipt) {
		return status.Error(codes.FailedPrecondition, "noncanonical_stored_preparation_revocation")
	}
	if observation != nil {
		value, err := canonicalRegistryRevocationObservation(w, receipt, observation)
		if err != nil {
			return err
		}
		if !proto.Equal(value, observation) {
			return status.Error(codes.FailedPrecondition, "noncanonical_stored_revocation_observation")
		}
	}
	return nil
}

func recordPreparationRevocation(current workloadRecord, next *runnersv1.PreparedWorkloadLifecycle, op *runnersv1.RecordPreparationRevocation) error {
	if op == nil || len(op.ProtoReflect().GetUnknown()) != 0 || next.Phase != preparationPhases["removing"] || next.Binding != nil ||
		next.Resources == nil || next.Resources.PreparationRevocation != nil || next.Resources.RevocationObservation != nil {
		return status.Error(codes.FailedPrecondition, "unbound_anchored_removal_required")
	}
	receipt, err := canonicalRegistryPreparationRevocation(current, op.Revocation)
	if err != nil {
		return err
	}
	next.Resources.PreparationRevocation = receipt
	return nil
}

func confirmPreparationRevocation(current workloadRecord, next *runnersv1.PreparedWorkloadLifecycle, op *runnersv1.ConfirmPreparationRevocation) error {
	if op == nil || len(op.ProtoReflect().GetUnknown()) != 0 || next.Phase != preparationPhases["removing"] || next.Binding != nil ||
		next.Resources == nil || next.Resources.PreparationRevocation == nil || next.Resources.RevocationObservation != nil {
		return status.Error(codes.FailedPrecondition, "persisted_preparation_revocation_required")
	}
	observation, err := canonicalRegistryRevocationObservation(current, next.Resources.PreparationRevocation, op.Observation)
	if err != nil {
		return err
	}
	next.Resources.RevocationObservation, next.Phase = observation, preparationPhases["removed"]
	return nil
}

func validateRevocationVolumeRecord(w workloadRecord, anchor *runnerv1.ResourceAnchor, found *runnerv1.VolumeListItem, v volumeRecord) error {
	if !v.CheckedLifecycle || v.RemovalIntent != nil || !proto.Equal(v.ResourceAnchor, anchor) ||
		v.Meta.ID.String() != anchor.ResourceId || v.OwnerKind != w.OwnerKind || v.OwnerID != w.OwnerID ||
		v.OrganizationID != w.OrganizationID || v.RunnerID != w.RunnerID || v.ThreadID != w.ThreadID || v.AgentID != w.AgentID {
		return status.Error(codes.FailedPrecondition, "revocation_workspace_record_changed")
	}
	if found != nil {
		if v.Status != volumeStatusActive || v.InstanceID == nil || *v.InstanceID != found.InstanceId || !proto.Equal(v.BoundInstance, found) {
			return status.Error(codes.FailedPrecondition, "recovered_workspace_must_be_bound_first")
		}
		return validateVolumeBinding(v, found)
	}
	// Absence cannot erase a known workspace, or reset an older allocation.
	reservation := v.AnchorReservation
	if v.Status != volumeStatusProvisioning || v.LifecycleRevision != 2 || v.BoundInstance != nil || v.InstanceID != nil ||
		reservation == nil || !canonicalPreparedUUID(reservation.WorkloadId) || reservation.PreparationRevision != 1 || reservation.ResourceRevision != 1 ||
		len(reservation.ProtoReflect().GetUnknown()) != 0 {
		return status.Error(codes.FailedPrecondition, "absent_workspace_requires_original_unbound_reservation")
	}
	return nil
}

func (s *Server) checkRevocationVolumeRecords(ctx context.Context, w workloadRecord, observation *runnerv1.ObservePreparationRevocationResponse) error {
	found := map[string]*runnerv1.VolumeListItem{}
	for _, v := range observation.Volumes {
		found[v.VolumeKey] = v
	}
	for _, anchor := range observation.Revocation.VolumeAnchors {
		v, err := s.getVolumeByID(ctx, uuid.MustParse(anchor.ResourceId))
		if err != nil {
			return toStatusError(err)
		}
		if err := validateRevocationVolumeRecord(w, anchor, found[anchor.ResourceId], v); err != nil {
			return err
		}
	}
	return nil
}
