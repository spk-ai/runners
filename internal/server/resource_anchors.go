package server

import (
	"context"
	"errors"
	"fmt"
	"maps"
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

func validateRegistryResourceAnchor(a *runnerv1.ResourceAnchor, kind runnerv1.ResourceAnchorKind, id uuid.UUID, backend, ownerKind string, owner, agent, thread uuid.UUID) error {
	if a == nil || a.Kind != kind || a.ResourceId != id.String() || !canonicalPreparedUUID(a.ResourceId) ||
		!canonicalPreparedUUID(a.InstanceUid) || !validVolumeBackend(a.BackendId) || a.BackendId != backend || len(a.ProtoReflect().GetUnknown()) != 0 {
		return status.Error(codes.InvalidArgument, "matching_native_resource_anchor_required")
	}
	labels := map[string]string{"app.kubernetes.io/managed-by": "k8s-runner", "agyn.dev/managed-by": "agents-orchestrator", "managed-by": "agents-orchestrator"}
	switch ownerKind {
	case runtimeOwnerKindAgentInstance:
		if owner == uuid.Nil || agent == uuid.Nil || kind == runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD && thread == uuid.Nil {
			return status.Error(codes.FailedPrecondition, "anchor_owner_identity_missing")
		}
		labels["agent-instance-id"], labels["agent-id"] = owner.String(), agent.String()
		if kind == runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD {
			// Registry thread_id is a legacy instance alias. The native inbox
			// thread belongs to this immutable workload anchor, not the volume.
			if !canonicalPreparedUUID(a.IdentityLabels["thread-id"]) {
				return status.Error(codes.InvalidArgument, "canonical_anchor_thread_required")
			}
			labels["thread-id"] = a.IdentityLabels["thread-id"]
		}
	case runtimeOwnerKindSandbox:
		if owner == uuid.Nil || !canonicalPreparedUUID(a.IdentityLabels["sandbox-owner-id"]) {
			return status.Error(codes.InvalidArgument, "sandbox_anchor_owner_required")
		}
		labels["sandbox-id"], labels["sandbox-owner-id"] = owner.String(), a.IdentityLabels["sandbox-owner-id"]
	default:
		return status.Error(codes.InvalidArgument, "resource_anchor_owner_kind_invalid")
	}
	if kind == runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME {
		labels["volume_key"] = id.String()
	}
	if !maps.Equal(labels, a.IdentityLabels) {
		return status.Error(codes.InvalidArgument, "resource_anchor_owner_mismatch")
	}
	return nil
}

func validateWorkloadResourceAnchors(w workloadRecord, resources *runnersv1.WorkloadResourceAnchors) error {
	p := w.Preparation
	if p == nil || resources == nil || resources.Revision == 0 || resources.Revision > math.MaxInt64 || len(resources.ProtoReflect().GetUnknown()) != 0 {
		return status.Error(codes.InvalidArgument, "valid_workload_resource_revision_required")
	}
	if resources.Workload == nil {
		if len(resources.Volumes) != 0 || p.Binding != nil || p.Phase != preparationPhases["reserved"] && p.Phase != preparationPhases["removed"] {
			return status.Error(codes.FailedPrecondition, "durable_resource_anchors_required")
		}
		return nil
	}
	if err := validateRegistryResourceAnchor(resources.Workload, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD, w.Meta.ID, p.BackendId, w.OwnerKind, w.OwnerID, w.AgentID, w.ThreadID); err != nil {
		return err
	}
	if len(resources.Volumes) != len(p.VolumeIds) || len(resources.Volumes) > 64 {
		return status.Error(codes.InvalidArgument, "complete_volume_anchor_set_required")
	}
	seen := map[string]bool{}
	for _, a := range resources.Volumes {
		if a == nil || !canonicalPreparedUUID(a.ResourceId) || seen[a.ResourceId] || !slices.Contains(p.VolumeIds, a.ResourceId) {
			return status.Error(codes.InvalidArgument, "unique_volume_anchor_set_required")
		}
		if err := validateRegistryResourceAnchor(a, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME, uuid.MustParse(a.ResourceId), p.BackendId, w.OwnerKind, w.OwnerID, w.AgentID, w.ThreadID); err != nil {
			return err
		}
		if a.IdentityLabels["sandbox-owner-id"] != resources.Workload.IdentityLabels["sandbox-owner-id"] {
			return status.Error(codes.InvalidArgument, "sandbox_anchor_owner_mismatch")
		}
		seen[a.ResourceId] = true
	}
	return nil
}

func (s *Server) CreateAnchoredWorkload(ctx context.Context, req *runnersv1.CreateAnchoredWorkloadRequest) (*runnersv1.CreateAnchoredWorkloadResponse, error) {
	response, err := s.createPreparedWorkload(ctx, req.GetPreparation(), true)
	if err != nil {
		return nil, err
	}
	return &runnersv1.CreateAnchoredWorkloadResponse{Workload: response.Workload}, nil
}

func (s *Server) UpdateAnchoredWorkload(ctx context.Context, req *runnersv1.UpdateAnchoredWorkloadRequest) (*runnersv1.UpdateAnchoredWorkloadResponse, error) {
	if req.GetExpectedAnchorRevision() == 0 || req.GetExpectedAnchorRevision() >= math.MaxInt64 {
		return nil, status.Error(codes.InvalidArgument, "workload_anchor_revision_required")
	}
	response, err := s.updatePreparedWorkload(ctx, req.GetOperation(), req.ExpectedAnchorRevision)
	if err != nil {
		return nil, err
	}
	return &runnersv1.UpdateAnchoredWorkloadResponse{Workload: response.Workload}, nil
}

func (s *Server) BindWorkloadResourceAnchors(ctx context.Context, req *runnersv1.BindWorkloadResourceAnchorsRequest) (*runnersv1.BindWorkloadResourceAnchorsResponse, error) {
	if !canonicalPreparedUUID(req.GetId()) || req.GetExpectedPreparationRevision() == 0 || req.GetExpectedPreparationRevision() >= math.MaxInt64 || req.GetExpectedAnchorRevision() == 0 || req.GetExpectedAnchorRevision() >= math.MaxInt64 {
		return nil, status.Error(codes.InvalidArgument, "workload_anchor_id_and_revisions_required")
	}
	id := uuid.MustParse(req.Id)
	current, err := s.getWorkloadByID(ctx, id)
	if err != nil {
		return nil, toStatusError(err)
	}
	p := current.Preparation
	if p == nil || p.Resources == nil {
		return nil, status.Error(codes.FailedPrecondition, "anchored_workload_required")
	}
	if p.Revision != req.ExpectedPreparationRevision || p.Resources.Revision != req.ExpectedAnchorRevision {
		return nil, status.Error(codes.Aborted, "workload_anchor_revision_changed")
	}
	if p.Phase != preparationPhases["reserved"] || current.Status != workloadStatusStarting || p.Resources.Workload != nil {
		return nil, status.Error(codes.FailedPrecondition, "unused_anchor_reservation_required")
	}
	next := proto.Clone(&runnersv1.WorkloadResourceAnchors{Revision: req.ExpectedAnchorRevision + 1, Workload: req.WorkloadAnchor, Volumes: req.VolumeAnchors}).(*runnersv1.WorkloadResourceAnchors)
	if next.Workload == nil {
		return nil, status.Error(codes.InvalidArgument, "workload_anchor_required")
	}
	if err := validateWorkloadResourceAnchors(current, next); err != nil {
		return nil, err
	}
	slices.SortFunc(next.Volumes, func(a, b *runnerv1.ResourceAnchor) int { return strings.Compare(a.ResourceId, b.ResourceId) })
	for _, anchor := range next.Volumes {
		v, err := s.getVolumeByID(ctx, uuid.MustParse(anchor.ResourceId))
		if err != nil {
			return nil, toStatusError(err)
		}
		if !v.CheckedLifecycle || v.Status != volumeStatusProvisioning && v.Status != volumeStatusActive || v.RemovalIntent != nil ||
			!proto.Equal(v.ResourceAnchor, anchor) || v.OwnerKind != current.OwnerKind || v.OwnerID != current.OwnerID ||
			v.RunnerID != current.RunnerID || v.OrganizationID != current.OrganizationID || v.AgentID != current.AgentID || v.ThreadID != current.ThreadID {
			return nil, status.Error(codes.FailedPrecondition, "persisted_volume_anchor_required")
		}
	}
	data, err := protojson.Marshal(next)
	if err != nil {
		return nil, status.Error(codes.Internal, "encode_workload_anchors")
	}
	updated, err := scanWorkload(s.pool.QueryRow(ctx, fmt.Sprintf(`UPDATE workloads SET resource_anchors = $4, updated_at = NOW()
        WHERE id = $1 AND preparation_revision = $2 AND resource_anchors->>'revision' = $3
        AND preparation_phase = 'reserved' AND resource_anchors->'workload' IS NULL RETURNING %s`, workloadColumns),
		id, int64(req.ExpectedPreparationRevision), fmt.Sprint(req.ExpectedAnchorRevision), data))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Error(codes.Aborted, "workload_anchor_revision_changed")
	}
	if err != nil {
		return nil, toStatusError(err)
	}
	w, err := toProtoWorkload(updated)
	if err != nil {
		return nil, status.Error(codes.Internal, "convert_anchored_workload")
	}
	s.publishWorkloadUpdateNotifications(ctx, updated, false, false, false, false)
	return &runnersv1.BindWorkloadResourceAnchorsResponse{Workload: w}, nil
}

func (s *Server) checkVolumeAnchorReservation(ctx context.Context, v volumeRecord, op *runnersv1.BindVolumeResourceAnchor) error {
	if !canonicalPreparedUUID(op.GetWorkloadId()) || op.GetExpectedPreparationRevision() == 0 || op.GetExpectedPreparationRevision() >= math.MaxInt64 || op.GetExpectedAnchorRevision() == 0 || op.GetExpectedAnchorRevision() >= math.MaxInt64 {
		return status.Error(codes.InvalidArgument, "volume_anchor_reservation_required")
	}
	w, err := s.getWorkloadByID(ctx, uuid.MustParse(op.WorkloadId))
	if err != nil {
		return toStatusError(err)
	}
	p := w.Preparation
	if p == nil || p.Resources == nil || p.Phase != preparationPhases["reserved"] || w.Status != workloadStatusStarting || p.Resources.Workload != nil ||
		!slices.Contains(p.VolumeIds, v.Meta.ID.String()) || w.OwnerKind != v.OwnerKind || w.OwnerID != v.OwnerID || w.RunnerID != v.RunnerID ||
		w.OrganizationID != v.OrganizationID || w.AgentID != v.AgentID || w.ThreadID != v.ThreadID || p.BackendId != op.GetAnchor().GetBackendId() {
		return status.Error(codes.FailedPrecondition, "unused_matching_anchor_reservation_required")
	}
	if p.Revision != op.ExpectedPreparationRevision || p.Resources.Revision != op.ExpectedAnchorRevision {
		return status.Error(codes.Aborted, "workload_anchor_revision_changed")
	}
	return nil
}
