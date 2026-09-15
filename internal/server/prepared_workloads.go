package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	runnerv1 "github.com/agynio/runners/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var preparationPhases = map[string]runnersv1.PreparedWorkloadPhase{
	"reserved":   runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_RESERVED,
	"preparing":  runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_PREPARING,
	"bound":      runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_BOUND,
	"activating": runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_ACTIVATING,
	"active":     runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_ACTIVE,
	"removing":   runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING,
	"removed":    runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED,
}

func canonicalPreparedUUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}

func (s *Server) CreatePreparedWorkload(ctx context.Context, req *runnersv1.CreatePreparedWorkloadRequest) (*runnersv1.CreatePreparedWorkloadResponse, error) {
	return s.createPreparedWorkload(ctx, req, false)
}

func (s *Server) createPreparedWorkload(ctx context.Context, req *runnersv1.CreatePreparedWorkloadRequest, anchored bool) (*runnersv1.CreatePreparedWorkloadResponse, error) {
	w := req.GetWorkload()
	if w == nil || w.Status != runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING || !validVolumeBackend(req.GetBackendId()) || len(req.GetVolumeIds()) > 64 {
		return nil, status.Error(codes.InvalidArgument, "prepared_starting_workload_backend_and_volumes_required")
	}
	for _, id := range []string{w.Id, w.RunnerId, w.OrganizationId} {
		if !canonicalPreparedUUID(id) {
			return nil, status.Error(codes.InvalidArgument, "canonical_prepared_workload_ids_required")
		}
	}
	ids := append([]string{}, req.GetVolumeIds()...)
	slices.Sort(ids)
	for i, id := range ids {
		if !canonicalPreparedUUID(id) || i > 0 && id == ids[i-1] {
			return nil, status.Error(codes.InvalidArgument, "unique_canonical_prepared_volume_ids_required")
		}
	}
	preparation := &runnersv1.PreparedWorkloadLifecycle{
		Phase: preparationPhases["reserved"], Revision: 1, BackendId: req.BackendId, VolumeIds: ids,
	}
	if anchored {
		preparation.Resources = &runnersv1.WorkloadResourceAnchors{Revision: 1}
	}
	created, err := s.createWorkload(ctx, w, preparation)
	if err != nil {
		return nil, err
	}
	return &runnersv1.CreatePreparedWorkloadResponse{Workload: created.Workload}, nil
}

func (s *Server) UpdatePreparedWorkload(ctx context.Context, req *runnersv1.UpdatePreparedWorkloadRequest) (*runnersv1.UpdatePreparedWorkloadResponse, error) {
	return s.updatePreparedWorkload(ctx, req, 0)
}

func (s *Server) updatePreparedWorkload(ctx context.Context, req *runnersv1.UpdatePreparedWorkloadRequest, anchorRevision uint64) (*runnersv1.UpdatePreparedWorkloadResponse, error) {
	if !canonicalPreparedUUID(req.GetId()) || req.GetExpectedRevision() == 0 || req.GetExpectedRevision() >= math.MaxInt64 || req.GetOperation() == nil {
		return nil, status.Error(codes.InvalidArgument, "prepared_workload_id_revision_and_operation_required")
	}
	id := uuid.MustParse(req.Id)
	current, err := s.getWorkloadByID(ctx, id)
	if err != nil {
		return nil, toStatusError(err)
	}
	if current.Preparation == nil {
		return nil, status.Error(codes.FailedPrecondition, "prepared_workload_required")
	}
	if current.Preparation.Revision != req.ExpectedRevision {
		return nil, status.Error(codes.Aborted, "workload_preparation_revision_changed")
	}
	resources := current.Preparation.Resources
	if (resources == nil) != (anchorRevision == 0) {
		return nil, status.Error(codes.FailedPrecondition, "matching_preparation_capability_required")
	}
	if resources != nil && resources.Revision != anchorRevision {
		return nil, status.Error(codes.Aborted, "workload_anchor_revision_changed")
	}
	next := proto.Clone(current.Preparation).(*runnersv1.PreparedWorkloadLifecycle)
	if err := applyPreparedWorkloadOperation(current, next, req); err != nil {
		return nil, err
	}
	// Validate through the same volume contract as checked bind. The database
	// repeats set/binding/ownership checks after its owner write, closing this
	// read-to-CAS window without cross-table row-lock deadlocks.
	if req.GetBind() != nil {
		for _, instance := range next.Binding.Volumes {
			v, err := s.getVolumeByID(ctx, uuid.MustParse(instance.VolumeKey))
			if err != nil {
				return nil, toStatusError(err)
			}
			if err := validateVolumeBinding(v, instance); err != nil {
				return nil, err
			}
			if !v.CheckedLifecycle || v.Status != volumeStatusActive || v.RemovalIntent != nil || !proto.Equal(v.BoundInstance, instance) ||
				v.OwnerKind != current.OwnerKind || v.OwnerID != current.OwnerID || v.OrganizationID != current.OrganizationID ||
				v.RunnerID != current.RunnerID || v.ThreadID != current.ThreadID || v.AgentID != current.AgentID {
				return nil, status.Error(codes.FailedPrecondition, "prepared_volume_binding_mismatch")
			}
		}
	}
	var binding, observation []byte
	if next.Binding != nil {
		binding, err = protojson.Marshal(next.Binding)
		if err != nil {
			return nil, status.Error(codes.Internal, "encode_workload_binding")
		}
	}
	if next.RemovalObservation != nil {
		observation, err = protojson.Marshal(next.RemovalObservation)
		if err != nil {
			return nil, status.Error(codes.Internal, "encode_workload_removal")
		}
	}
	phase := strings.ToLower(strings.TrimPrefix(next.Phase.String(), "PREPARED_WORKLOAD_PHASE_"))
	extraSet, extraWhere := "", ""
	args := []any{id, int64(req.ExpectedRevision), phase, binding, observation}
	if next.Resources != nil {
		if anchorRevision >= math.MaxInt64 {
			return nil, status.Error(codes.FailedPrecondition, "workload_anchor_revision_exhausted")
		}
		next.Resources.Revision++
		data, err := protojson.Marshal(next.Resources)
		if err != nil {
			return nil, status.Error(codes.Internal, "encode_workload_anchors")
		}
		extraSet, extraWhere = ", resource_anchors = $6", " AND resource_anchors->>'revision' = $7"
		args = append(args, data, fmt.Sprint(anchorRevision))
	}
	row := s.pool.QueryRow(ctx, fmt.Sprintf(`UPDATE workloads SET
        preparation_phase = $3, preparation_revision = preparation_revision + 1,
        prepared_binding = $4, prepared_removal_observation = $5,
        instance_id = CASE WHEN $4::jsonb IS NOT NULL THEN id::text ELSE instance_id END,
        status = CASE WHEN $3 = 'removed' AND status <> 'failed' THEN 'stopped' ELSE status END,
        removed_at = CASE WHEN $3 = 'removed' THEN COALESCE(removed_at, NOW()) ELSE removed_at END,
        removal_confirmed_at = CASE WHEN $3 = 'removed' THEN COALESCE(removal_confirmed_at, NOW()) ELSE removal_confirmed_at END,
        updated_at = NOW()%s
        WHERE id = $1 AND preparation_revision = $2%s RETURNING %s`, extraSet, extraWhere, workloadColumns), args...)
	updated, err := scanWorkload(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Error(codes.Aborted, "workload_preparation_revision_changed")
	}
	if err != nil {
		return nil, toStatusError(err)
	}
	s.publishWorkloadUpdateNotifications(ctx, updated, updated.Status != current.Status, false, false, updated.AgentState != current.AgentState)
	w, err := toProtoWorkload(updated)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert workload: %v", err)
	}
	return &runnersv1.UpdatePreparedWorkloadResponse{Workload: w}, nil
}

func applyPreparedWorkloadOperation(current workloadRecord, next *runnersv1.PreparedWorkloadLifecycle, req *runnersv1.UpdatePreparedWorkloadRequest) error {
	fail := func() error { return status.Error(codes.FailedPrecondition, "invalid_prepared_workload_transition") }
	phase := next.Phase
	switch op := req.GetOperation().(type) {
	case *runnersv1.UpdatePreparedWorkloadRequest_BeginPreparation:
		if op.BeginPreparation == nil || phase != preparationPhases["reserved"] || current.Status != workloadStatusStarting {
			return fail()
		}
		if next.Resources != nil && next.Resources.Workload == nil {
			return status.Error(codes.FailedPrecondition, "durable_resource_anchors_required")
		}
		next.Phase = preparationPhases["preparing"]
	case *runnersv1.UpdatePreparedWorkloadRequest_Bind:
		if next.Binding != nil || phase != preparationPhases["preparing"] && phase != preparationPhases["removing"] {
			return fail()
		}
		binding, err := validatePreparedWorkloadBinding(current, op.Bind.GetBinding())
		if err != nil {
			return err
		}
		next.Binding = binding
		if phase == preparationPhases["preparing"] {
			next.Phase = preparationPhases["bound"]
		}
	case *runnersv1.UpdatePreparedWorkloadRequest_BeginActivation:
		if op.BeginActivation == nil || phase != preparationPhases["bound"] || current.Status != workloadStatusStarting || next.Binding == nil {
			return fail()
		}
		next.Phase = preparationPhases["activating"]
	case *runnersv1.UpdatePreparedWorkloadRequest_ConfirmActivation:
		if phase != preparationPhases["activating"] {
			return fail()
		}
		binding, err := validatePreparedWorkloadBinding(current, op.ConfirmActivation.GetBinding())
		if err != nil {
			return err
		}
		if !proto.Equal(next.Binding, binding) {
			return fail()
		}
		next.Phase = preparationPhases["active"]
	case *runnersv1.UpdatePreparedWorkloadRequest_BeginRemoval:
		if op.BeginRemoval == nil || phase == preparationPhases["reserved"] || phase == preparationPhases["removing"] || phase == preparationPhases["removed"] {
			return fail()
		}
		next.Phase = preparationPhases["removing"]
	case *runnersv1.UpdatePreparedWorkloadRequest_ConfirmRemoval:
		observation := op.ConfirmRemoval.GetObservation()
		if phase != preparationPhases["removing"] || next.Binding == nil || observation.GetState() != runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_ABSENT {
			return fail()
		}
		binding, err := validatePreparedWorkloadBinding(current, observation.GetBinding())
		if err != nil {
			return err
		}
		if !proto.Equal(next.Binding, binding) {
			return fail()
		}
		next.RemovalObservation = proto.Clone(observation).(*runnerv1.RemovePreparedWorkloadResponse)
		next.RemovalObservation.Binding = binding
		next.Phase = preparationPhases["removed"]
	case *runnersv1.UpdatePreparedWorkloadRequest_AbortReservation:
		if op.AbortReservation == nil || phase != preparationPhases["reserved"] || next.Binding != nil {
			return fail()
		}
		next.Phase = preparationPhases["removed"]
	default:
		return status.Error(codes.InvalidArgument, "prepared_workload_operation_required")
	}
	return nil
}

func validatePreparedWorkloadBinding(current workloadRecord, value *runnerv1.WorkloadBinding) (*runnerv1.WorkloadBinding, error) {
	p := current.Preparation
	if value == nil || value.WorkloadId != current.Meta.ID.String() || !canonicalPreparedUUID(value.InstanceUid) || p == nil ||
		!validVolumeBackend(value.BackendId) || value.BackendId != p.BackendId || len(value.Volumes) != len(p.VolumeIds) || len(value.Volumes) > 64 {
		return nil, status.Error(codes.InvalidArgument, "complete_matching_workload_binding_required")
	}
	copy := proto.Clone(value).(*runnerv1.WorkloadBinding)
	if p.Resources == nil {
		if copy.Anchor != nil {
			return nil, status.Error(codes.FailedPrecondition, "anchored_preparation_required")
		}
	} else {
		if err := validateWorkloadResourceAnchors(current, p.Resources); err != nil {
			return nil, err
		}
		if p.Resources.Workload == nil || !proto.Equal(copy.Anchor, p.Resources.Workload) {
			return nil, status.Error(codes.FailedPrecondition, "persisted_workload_anchor_required")
		}
	}
	slices.SortFunc(copy.Volumes, func(a, b *runnerv1.VolumeListItem) int { return strings.Compare(a.GetInstanceId(), b.GetInstanceId()) })
	ids := make(map[string]bool, len(p.VolumeIds))
	for _, id := range p.VolumeIds {
		ids[id] = true
	}
	names := make(map[string]bool, len(ids))
	for _, v := range copy.Volumes {
		if v == nil || !canonicalPreparedUUID(v.VolumeKey) || !ids[v.VolumeKey] || v.BackendId != p.BackendId || v.InstanceId == "" || names[v.InstanceId] {
			return nil, status.Error(codes.InvalidArgument, "complete_unique_prepared_volume_set_required")
		}
		var anchor *runnerv1.ResourceAnchor
		if p.Resources != nil {
			for _, expected := range p.Resources.Volumes {
				if expected.ResourceId == v.VolumeKey {
					anchor = expected
				}
			}
		}
		if !proto.Equal(v.Anchor, anchor) {
			return nil, status.Error(codes.FailedPrecondition, "persisted_volume_anchor_required")
		}
		delete(ids, v.VolumeKey)
		names[v.InstanceId] = true
	}
	return copy, nil
}
