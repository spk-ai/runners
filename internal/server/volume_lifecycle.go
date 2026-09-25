package server

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"strings"
	"time"

	runnerv1 "github.com/agynio/runners/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/util/validation"
)

func volumeLifecycleStatusError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "55000" {
		switch pgErr.ConstraintName {
		case "volumes_checked_lifecycle":
			return status.Error(codes.FailedPrecondition, "checked_volume_lifecycle_required")
		case "volumes_legacy_adoption":
			return status.Error(codes.FailedPrecondition, "legacy_volume_reconciliation_required")
		}
	}
	return toStatusError(err)
}

func validateCheckedVolumeCreate(req *runnersv1.CreateVolumeRequest) error {
	if req.GetStatus() != runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING {
		return status.Error(codes.InvalidArgument, "checked_volume_must_start_provisioning")
	}
	size, err := parseVolumeSize(req.GetSizeGb())
	if err != nil || size.Sign() <= 0 {
		return status.Error(codes.InvalidArgument, "positive_volume_size_required")
	}
	return nil
}

// CreateVolumeChecked reserves an unbound PROVISIONING generation with a positive
// revision and sticky checked protection. It never implicitly reopens a record.
func (s *Server) CreateVolumeChecked(ctx context.Context, req *runnersv1.CreateVolumeCheckedRequest) (*runnersv1.CreateVolumeCheckedResponse, error) {
	if err := validateCheckedVolumeCreate(req.GetVolume()); err != nil {
		return nil, err
	}
	resp, err := s.createVolume(ctx, req.GetVolume(), true)
	if err != nil {
		return nil, err
	}
	return &runnersv1.CreateVolumeCheckedResponse{Volume: resp.Volume}, nil
}

// UpdateVolumeChecked persists one lifecycle transition by CAS, separate from
// metering. Bind pins backend/name/UID/owner; begin-removal must commit that target
// before native deletion. Confirmation trusts the caller's matching ABSENT evidence.
// Failed provisioning is not absence; only confirmed unanchored deletion clears
// identity on explicit reopen. Anchored retirement retains its history permanently.
// ../../migrations/0018_checked_volume_lifecycle.sql protects checked history;
// ../../migrations/0019_volume_workload_admission.sql serializes owner admission
// against removal even after billing ends. Audited legacy bind needs a recorded
// name and idle owner under ../../migrations/0020_legacy_volume_adoption.sql.
// ../../migrations/0021_volume_backend_identity.sql rejects backend-less history
// without backfill. These SQL guards also constrain old binaries, not only this CAS.
// @see orchestrator::internal/reconciler/checked_volumes
func (s *Server) UpdateVolumeChecked(ctx context.Context, req *runnersv1.UpdateVolumeCheckedRequest) (*runnersv1.UpdateVolumeCheckedResponse, error) {
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}
	if req.GetExpectedRevision() == 0 || req.GetExpectedRevision() >= math.MaxInt64 || req.GetOperation() == nil {
		return nil, status.Error(codes.InvalidArgument, "valid_volume_revision_and_operation_required")
	}
	current, err := s.getVolumeByID(ctx, id)
	if err != nil {
		return nil, toStatusError(err)
	}
	if current.LifecycleRevision != int64(req.ExpectedRevision) {
		return nil, status.Error(codes.Aborted, "volume_lifecycle_revision_changed")
	}
	next := current
	if err := applyVolumeOperation(&next, req); err != nil {
		return nil, err
	}
	if op := req.GetBindAnchor(); op != nil {
		if err := s.checkVolumeAnchorReservation(ctx, current, op); err != nil {
			return nil, err
		}
	}
	var boundJSON, intentJSON []byte
	if next.BoundInstance != nil {
		boundJSON, err = protojson.Marshal(next.BoundInstance)
		if err != nil {
			return nil, status.Error(codes.Internal, "encode_volume_binding")
		}
	}
	if next.RemovalIntent != nil {
		intentJSON, err = protojson.Marshal(next.RemovalIntent)
		if err != nil {
			return nil, status.Error(codes.Internal, "encode_volume_removal_intent")
		}
	}
	// Do not overwrite metering changes made after our read; they deliberately
	// do not advance lifecycle_revision. Only reopen resets the billing clock.
	reopen := req.GetReopen() != nil
	closed := next.Status == volumeStatusDeleted || next.Status == volumeStatusFailed
	extraSet, extraWhere := "", ""
	args := []any{id, int64(req.ExpectedRevision), next.Status, next.InstanceID, next.SizeGB, boundJSON, intentJSON, reopen, closed}
	if op := req.GetBindAnchor(); op != nil {
		data, err := protojson.Marshal(next.ResourceAnchor)
		if err != nil {
			return nil, status.Error(codes.Internal, "encode_volume_anchor")
		}
		reservation, err := protojson.Marshal(next.AnchorReservation)
		if err != nil {
			return nil, status.Error(codes.Internal, "encode_volume_anchor_reservation")
		}
		extraSet = ", resource_anchor = $10, anchor_reservation = $14"
		extraWhere = ` AND EXISTS (SELECT 1 FROM workloads w WHERE w.id = $11
            AND w.preparation_revision = $12 AND w.resource_anchors->>'revision' = $13
            AND w.preparation_phase = 'reserved' AND w.status = 'starting'
            AND w.resource_anchors->'workload' IS NULL)`
		args = append(args, data, uuid.MustParse(op.WorkloadId), int64(op.ExpectedPreparationRevision), fmt.Sprint(op.ExpectedAnchorRevision), reservation)
	} else if req.GetConfirmAnchoredRemoval() != nil {
		data, err := protojson.Marshal(next.AnchoredRemovalObservation)
		if err != nil {
			return nil, status.Error(codes.Internal, "encode_anchored_volume_removal")
		}
		extraSet = ", anchored_removal_observation = $10"
		args = append(args, data)
	}
	row := s.pool.QueryRow(ctx, fmt.Sprintf(`UPDATE volumes
        SET lifecycle_revision = lifecycle_revision + 1, checked_lifecycle = TRUE,
            status = $3, instance_id = $4, size_gb = $5, bound_instance = $6, removal_intent = $7,
            removed_at = CASE WHEN $8 THEN NULL WHEN $9 THEN COALESCE(removed_at, NOW()) ELSE removed_at END,
            last_metering_sampled_at = CASE WHEN $8 THEN NOW() ELSE last_metering_sampled_at END,
            updated_at = NOW()%s
        WHERE id = $1 AND lifecycle_revision = $2%s
        RETURNING %s`, extraSet, extraWhere, volumeColumns), args...)
	updated, err := scanVolume(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Error(codes.Aborted, "volume_lifecycle_revision_changed")
	}
	if err != nil {
		return nil, volumeLifecycleStatusError(err)
	}
	s.publishVolumeUpdateNotification(ctx, updated)
	volume, err := toProtoVolume(updated)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert volume: %v", err)
	}
	return &runnersv1.UpdateVolumeCheckedResponse{Volume: volume}, nil
}

func applyVolumeOperation(volume *volumeRecord, req *runnersv1.UpdateVolumeCheckedRequest) error {
	fail := func(reason string) error { return status.Error(codes.FailedPrecondition, reason) }
	if volume.BoundInstance != nil && !validVolumeBackend(volume.BoundInstance.BackendId) {
		return fail("volume_backend_reconciliation_required")
	}
	if volume.ResourceAnchor != nil && req.GetBind() == nil && req.GetBeginAnchoredRemoval() == nil && req.GetConfirmAnchoredRemoval() == nil {
		return fail("anchored_volume_retirement_contract_required")
	}
	switch op := req.GetOperation().(type) {
	case *runnersv1.UpdateVolumeCheckedRequest_BeginAnchoredRemoval:
		return beginAnchoredVolumeRemoval(volume)
	case *runnersv1.UpdateVolumeCheckedRequest_ConfirmAnchoredRemoval:
		return confirmAnchoredVolumeRemoval(volume, op.ConfirmAnchoredRemoval)
	case *runnersv1.UpdateVolumeCheckedRequest_BindAnchor:
		if !volume.CheckedLifecycle || volume.Status != volumeStatusProvisioning || volume.BoundInstance != nil || volume.InstanceID != nil || volume.RemovalIntent != nil {
			return fail("unbound_checked_volume_required")
		}
		a := op.BindAnchor.GetAnchor()
		if err := validateRegistryResourceAnchor(a, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME, volume.Meta.ID, a.GetBackendId(), volume.OwnerKind, volume.OwnerID, volume.AgentID, volume.ThreadID); err != nil {
			return err
		}
		volume.ResourceAnchor = proto.Clone(a).(*runnerv1.ResourceAnchor)
		volume.AnchorReservation = &runnersv1.VolumeAnchorReservation{WorkloadId: op.BindAnchor.GetWorkloadId(),
			PreparationRevision: op.BindAnchor.GetExpectedPreparationRevision(), ResourceRevision: op.BindAnchor.GetExpectedAnchorRevision()}
	case *runnersv1.UpdateVolumeCheckedRequest_Bind:
		if volume.Status != volumeStatusProvisioning && volume.Status != volumeStatusActive || volume.RemovalIntent != nil {
			return fail("volume_not_bindable")
		}
		if !volume.CheckedLifecycle && (volume.InstanceID == nil || *volume.InstanceID == "") {
			return fail("legacy_volume_recorded_instance_required")
		}
		instance := op.Bind.GetInstance()
		if err := validateVolumeBinding(*volume, instance); err != nil {
			return err
		}
		if volume.BoundInstance != nil && !proto.Equal(volume.BoundInstance, instance) {
			return fail("volume_incarnation_already_bound")
		}
		volume.BoundInstance = proto.Clone(instance).(*runnerv1.VolumeListItem)
		name := instance.InstanceId
		volume.InstanceID = &name
		volume.Status = volumeStatusActive
	case *runnersv1.UpdateVolumeCheckedRequest_BeginRemoval:
		if !volume.CheckedLifecycle || volume.BoundInstance == nil {
			return fail("checked_volume_binding_required")
		}
		if volume.Status == volumeStatusDeprovision && volume.RemovalIntent != nil {
			break
		}
		if volume.Status != volumeStatusActive && volume.Status != volumeStatusProvisioning || volume.RemovalIntent != nil {
			return fail("volume_not_removable")
		}
		volume.RemovalIntent = &runnersv1.VolumeRemovalIntent{
			Id: uuid.NewString(), Expected: proto.Clone(volume.BoundInstance).(*runnerv1.VolumeListItem),
			RequestedAt: timestamppb.New(time.Now().UTC()),
		}
		volume.Status = volumeStatusDeprovision
	case *runnersv1.UpdateVolumeCheckedRequest_ConfirmRemoval:
		intent := volume.RemovalIntent
		if !volume.CheckedLifecycle || intent == nil || op.ConfirmRemoval.GetIntentId() != intent.Id ||
			(volume.Status != volumeStatusDeprovision && volume.Status != volumeStatusDeleted) {
			return fail("matching_volume_removal_intent_required")
		}
		if !validVolumeBackend(op.ConfirmRemoval.GetBackendId()) || op.ConfirmRemoval.GetBackendId() != intent.GetExpected().GetBackendId() ||
			!proto.Equal(intent.Expected, volume.BoundInstance) {
			return fail("matching_volume_backend_required")
		}
		volume.RemovalIntent = proto.Clone(intent).(*runnersv1.VolumeRemovalIntent)
		if intent.ConfirmedAt == nil {
			volume.RemovalIntent.ConfirmedAt = timestamppb.New(time.Now().UTC())
		}
		volume.Status = volumeStatusDeleted
	case *runnersv1.UpdateVolumeCheckedRequest_FailProvisioning:
		if !volume.CheckedLifecycle || volume.Status != volumeStatusProvisioning || volume.RemovalIntent != nil {
			return fail("only_unremoved_provisioning_can_fail")
		}
		volume.Status = volumeStatusFailed
	case *runnersv1.UpdateVolumeCheckedRequest_Reopen:
		if !volume.CheckedLifecycle {
			return fail("legacy_volume_reconciliation_required")
		}
		request := op.Reopen.GetVolume()
		if err := validateCheckedVolumeCreate(request); err != nil {
			return err
		}
		input, err := parseVolumeCreate(request)
		if err != nil {
			return err
		}
		if !sameVolumeOwner(*volume, input) {
			return fail("volume_reopen_identity_mismatch")
		}
		if !isClosedVolumeStatus(volume.Status) {
			return fail("volume_not_closed")
		}
		if volume.Status == volumeStatusDeleted {
			if !volume.CheckedLifecycle || volume.RemovalIntent.GetConfirmedAt() == nil {
				return fail("volume_removal_confirmation_required")
			}
			volume.BoundInstance, volume.InstanceID, volume.RemovalIntent = nil, nil, nil
		} else if volume.RemovalIntent != nil {
			return fail("volume_has_removal_intent")
		}
		volume.Status, volume.SizeGB = volumeStatusProvisioning, input.SizeGB
	default:
		return status.Error(codes.InvalidArgument, "volume_operation_required")
	}
	volume.CheckedLifecycle = true
	return nil
}

func sameVolumeOwner(record volumeRecord, input volumeInsertInput) bool {
	uuidMatches := func(value uuid.UUID, requested *uuid.UUID) bool {
		return requested == nil && value == uuid.Nil || requested != nil && value == *requested
	}
	return record.Meta.ID == input.ID && uuidMatches(record.VolumeID, input.VolumeID) && uuidMatches(record.ThreadID, input.ThreadID) &&
		record.RunnerID == input.RunnerID && uuidMatches(record.AgentID, input.AgentID) && record.OrganizationID == input.OrganizationID &&
		record.OwnerKind == input.OwnerKind && record.OwnerID == input.OwnerID
}

func validVolumeBackend(backend string) bool {
	return backend != "" && strings.TrimSpace(backend) == backend && len(backend) <= 512
}

func validateVolumeBinding(record volumeRecord, instance *runnerv1.VolumeListItem) error {
	// The current checked profile uses k8s-runner's persistent identity contract.
	// Reject targets the backend cannot remove before making the binding immutable.
	if instance == nil || instance.GetInstanceId() == "" || len(validation.IsDNS1123Subdomain(instance.GetInstanceId())) != 0 ||
		instance.GetInstanceUid() == "" || strings.TrimSpace(instance.GetInstanceUid()) != instance.GetInstanceUid() || len(instance.GetInstanceUid()) > 256 ||
		len(instance.GetIdentityLabels()) == 0 || len(instance.GetIdentityLabels()) > 8 || !validVolumeBackend(instance.GetBackendId()) {
		return status.Error(codes.InvalidArgument, "complete_volume_instance_required")
	}
	if !proto.Equal(record.ResourceAnchor, instance.Anchor) {
		return status.Error(codes.FailedPrecondition, "persisted_volume_anchor_required")
	}
	if a := record.ResourceAnchor; a != nil {
		if err := validateRegistryResourceAnchor(a, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME, record.Meta.ID, instance.BackendId, record.OwnerKind, record.OwnerID, record.AgentID, record.ThreadID); err != nil {
			return err
		}
		if !maps.Equal(a.IdentityLabels, instance.IdentityLabels) {
			return status.Error(codes.FailedPrecondition, "volume_anchor_identity_mismatch")
		}
	}
	for key, value := range instance.IdentityLabels {
		switch key {
		case "app.kubernetes.io/managed-by", "agyn.dev/managed-by", "volume_key", "managed-by", "agent-instance-id", "agent-id", "sandbox-id", "sandbox-owner-id":
		default:
			return status.Error(codes.InvalidArgument, "nonpersistent_volume_identity_label")
		}
		if value == "" || len(validation.IsValidLabelValue(value)) != 0 {
			return status.Error(codes.InvalidArgument, "invalid_volume_identity_label")
		}
	}
	size, err := parseVolumeSize(record.SizeGB)
	if err != nil || size.Sign() <= 0 {
		return status.Error(codes.FailedPrecondition, "volume_size_requires_reconciliation")
	}
	labels := instance.IdentityLabels
	ownerMatches := false
	switch record.OwnerKind {
	case runtimeOwnerKindAgentInstance:
		ownerMatches = labels["agent-instance-id"] == record.OwnerID.String() && labels["agent-id"] == record.AgentID.String() &&
			labels["sandbox-id"] == "" && labels["sandbox-owner-id"] == ""
	case runtimeOwnerKindSandbox:
		ownerMatches = labels["sandbox-id"] == record.OwnerID.String() && labels["sandbox-owner-id"] != "" &&
			labels["agent-instance-id"] == "" && labels["agent-id"] == ""
	}
	if !ownerMatches || instance.VolumeKey != record.Meta.ID.String() || labels["volume_key"] != record.Meta.ID.String() ||
		labels["managed-by"] != "agents-orchestrator" || labels["app.kubernetes.io/managed-by"] != "k8s-runner" ||
		labels["agyn.dev/managed-by"] != "" && labels["agyn.dev/managed-by"] != "agents-orchestrator" ||
		record.InstanceID != nil && *record.InstanceID != instance.InstanceId {
		return status.Error(codes.FailedPrecondition, "volume_instance_owner_mismatch")
	}
	return nil
}
