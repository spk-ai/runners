package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	authorizationv1 "github.com/agynio/runners/.gen/go/agynio/api/authorization/v1"
	notificationsv1 "github.com/agynio/runners/.gen/go/agynio/api/notifications/v1"
	runnerv1 "github.com/agynio/runners/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	workloadStatusStarting = "starting"
	workloadStatusRunning  = "running"
	workloadStatusStopping = "stopping"
	workloadStatusStopped  = "stopped"
	workloadStatusFailed   = "failed"

	workloadAgentStateProcessing = "processing"
	workloadAgentStateIdle       = "idle"

	workloadFailureReasonStartFailed     = "start_failed"
	workloadFailureReasonImagePullFailed = "image_pull_failed"
	workloadFailureReasonConfigInvalid   = "config_invalid"
	workloadFailureReasonCrashloop       = "crashloop"
	workloadFailureReasonRuntimeLost     = "runtime_lost"

	containerRoleMain    = "main"
	containerRoleSidecar = "sidecar"
	containerRoleInit    = "init"

	containerStatusRunning    = "running"
	containerStatusTerminated = "terminated"
	containerStatusWaiting    = "waiting"

	workloadColumns = `id, runner_id, thread_id, agent_id, organization_id, status, agent_state, failure_reason, failure_message, containers, ziti_identity_id, allocated_cpu_millicores, allocated_ram_bytes, flavor, persistent_shells, instance_id, last_activity_at, last_metering_sampled_at, removed_at, owner_kind, owner_id, created_at, updated_at, removal_confirmed_at, preparation_phase, preparation_revision, prepared_backend_id, prepared_volume_ids, prepared_binding, prepared_removal_observation, resource_anchors`
)

type workloadRecord struct {
	Meta                   entityMeta
	RunnerID               uuid.UUID
	ThreadID               uuid.UUID
	AgentID                uuid.UUID
	OrganizationID         uuid.UUID
	Status                 string
	AgentState             string
	FailureReason          *string
	FailureMessage         *string
	Containers             []containerRecord
	ZitiIdentityID         string
	AllocatedCPUMillicores int32
	AllocatedRAMBytes      int64
	Flavor                 string
	PersistentShells       bool
	InstanceID             *string
	LastActivityAt         time.Time
	RemovedAt              *time.Time
	RemovalConfirmedAt     *time.Time
	LastMeteringAt         *time.Time
	OwnerKind              string
	OwnerID                uuid.UUID
	Preparation            *runnersv1.PreparedWorkloadLifecycle
}

type workloadInsertInput struct {
	ID                     uuid.UUID
	RunnerID               uuid.UUID
	ThreadID               *uuid.UUID
	AgentID                *uuid.UUID
	OrganizationID         uuid.UUID
	Status                 string
	ContainersJSON         []byte
	ZitiIdentityID         string
	AllocatedCPUMillicores int32
	AllocatedRAMBytes      int64
	Flavor                 string
	PersistentShells       bool
	OwnerKind              string
	OwnerID                uuid.UUID
	Preparation            *runnersv1.PreparedWorkloadLifecycle
}

type workloadUpdateInput struct {
	ID                 uuid.UUID
	Status             *string
	FailureReason      *string
	FailureMessage     *string
	ContainersJSON     *[]byte
	InstanceID         *string
	RemovedAt          *time.Time
	RemovalConfirmedAt *time.Time
	LastMeteringAt     *time.Time
	ResetLastActivity  bool
}

type workloadListFilter struct {
	OrganizationID *uuid.UUID
	AgentIDs       []uuid.UUID
	RunnerIDs      []uuid.UUID
	OwnerKinds     []string
	OwnerIDs       []uuid.UUID
	Statuses       []string
	StartedAfter   *time.Time
	StartedBefore  *time.Time
	PendingSample  bool
}

type workloadSortField int

const (
	workloadSortStarted workloadSortField = iota
	workloadSortAgent
	workloadSortRunner
	workloadSortStatus
	workloadSortDuration
)

type workloadListSort struct {
	Field     workloadSortField
	Direction sortDirection
}

type containerRecord struct {
	ContainerID  string     `json:"container_id"`
	Name         string     `json:"name"`
	Role         string     `json:"role"`
	Image        string     `json:"image"`
	Status       string     `json:"status"`
	Reason       *string    `json:"reason,omitempty"`
	Message      *string    `json:"message,omitempty"`
	ExitCode     *int32     `json:"exit_code,omitempty"`
	RestartCount int32      `json:"restart_count"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
}

func (s *Server) CreateWorkload(ctx context.Context, req *runnersv1.CreateWorkloadRequest) (*runnersv1.CreateWorkloadResponse, error) {
	return s.createWorkload(ctx, req, nil)
}

func (s *Server) createWorkload(ctx context.Context, req *runnersv1.CreateWorkloadRequest, preparation *runnersv1.PreparedWorkloadLifecycle) (*runnersv1.CreateWorkloadResponse, error) {
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}
	runnerID, err := parseUUID(req.GetRunnerId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "runner_id: %v", err)
	}
	threadID, err := parseOptionalUUID(req.GetThreadId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "thread_id: %v", err)
	}
	agentValue := req.GetAgentId()
	if req.AgentClassId != nil {
		agentValue = req.GetAgentClassId()
	}
	agentID, err := parseOptionalUUID(agentValue)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "agent_class_id: %v", err)
	}
	organizationID, err := parseUUID(req.GetOrganizationId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "organization_id: %v", err)
	}
	ownerKind, err := runtimeOwnerKindToString(req.GetOwnerKind())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "owner_kind: %v", err)
	}
	ownerID, err := runtimeOwnerIDFromRequest(req.GetOwnerId(), req.AgentInstanceId, ownerKind)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "owner_id: %v", err)
	}
	if ownerKind == runtimeOwnerKindAgentInstance {
		if threadID == nil {
			return nil, status.Error(codes.InvalidArgument, "thread_id: value is empty")
		}
		if agentID == nil {
			return nil, status.Error(codes.InvalidArgument, "agent_class_id: value is empty")
		}
	}

	statusValue, err := workloadStatusToString(req.GetStatus())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "status: %v", err)
	}
	containers, err := containersFromProto(req.GetContainers())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "containers: %v", err)
	}
	containersJSON, err := json.Marshal(containers)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal containers: %v", err)
	}

	workload, err := s.insertWorkload(ctx, workloadInsertInput{
		ID:                     id,
		RunnerID:               runnerID,
		ThreadID:               threadID,
		AgentID:                agentID,
		OrganizationID:         organizationID,
		Status:                 statusValue,
		ContainersJSON:         containersJSON,
		ZitiIdentityID:         strings.TrimSpace(req.GetZitiIdentityId()),
		AllocatedCPUMillicores: req.GetAllocatedCpuMillicores(),
		AllocatedRAMBytes:      req.GetAllocatedRamBytes(),
		Flavor:                 req.GetFlavor(),
		PersistentShells:       req.GetPersistentShells(),
		OwnerKind:              ownerKind,
		OwnerID:                *ownerID,
		Preparation:            preparation,
	})
	if err != nil {
		return nil, toStatusError(err)
	}
	if err := s.writeWorkloadAuthorization(ctx, id, organizationID, agentID, ownerID); err != nil {
		return nil, err
	}

	protoWorkload, err := toProtoWorkload(workload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert workload: %v", err)
	}
	return &runnersv1.CreateWorkloadResponse{Workload: protoWorkload}, nil
}

func (s *Server) UpdateWorkload(ctx context.Context, req *runnersv1.UpdateWorkloadRequest) (*runnersv1.UpdateWorkloadResponse, error) {
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}

	var statusValue *string
	if req.Status != nil {
		value, err := workloadStatusToString(req.GetStatus())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "status: %v", err)
		}
		statusValue = &value
	}

	var failureReason *string
	if req.FailureReason != nil {
		value, err := workloadFailureReasonToString(req.GetFailureReason())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "failure_reason: %v", err)
		}
		failureReason = &value
	}

	var failureMessage *string
	if req.FailureMessage != nil {
		value := strings.TrimSpace(req.GetFailureMessage())
		failureMessage = &value
	}

	var containerRecords []containerRecord
	var containersJSON *[]byte
	if req.Containers != nil {
		containers, err := containersFromProto(req.GetContainers())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "containers: %v", err)
		}
		payload, err := json.Marshal(containers)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "marshal containers: %v", err)
		}
		containerRecords = containers
		containersJSON = &payload
	}

	var instanceID *string
	if req.InstanceId != nil {
		trimmed := strings.TrimSpace(req.GetInstanceId())
		if trimmed == "" {
			return nil, status.Error(codes.InvalidArgument, "instance_id must not be empty")
		}
		instanceID = &trimmed
	}

	var removedAt *time.Time
	if req.RemovedAt != nil {
		if err := req.GetRemovedAt().CheckValid(); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "removed_at: %v", err)
		}
		value := req.GetRemovedAt().AsTime()
		removedAt = &value
	}

	var lastMeteringAt *time.Time
	if req.LastMeteringSampledAt != nil {
		if err := req.GetLastMeteringSampledAt().CheckValid(); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "last_metering_sampled_at: %v", err)
		}
		value := req.GetLastMeteringSampledAt().AsTime()
		lastMeteringAt = &value
	}

	var removalConfirmedAt *time.Time
	if req.RemovalConfirmedAt != nil {
		if err := req.GetRemovalConfirmedAt().CheckValid(); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "removal_confirmed_at: %v", err)
		}
		if statusValue != nil && !isTerminalWorkloadStatus(*statusValue) {
			return nil, status.Error(codes.InvalidArgument, "removal_confirmed_at requires a terminal workload")
		}
		value := req.GetRemovalConfirmedAt().AsTime()
		removalConfirmedAt = &value
	}

	if statusValue == nil && containersJSON == nil && instanceID == nil && removedAt == nil && removalConfirmedAt == nil && lastMeteringAt == nil && failureReason == nil && failureMessage == nil {
		return nil, status.Error(codes.InvalidArgument, "at least one field must be provided")
	}

	needsExisting := false
	if s.notificationsClient != nil && (statusValue != nil || containersJSON != nil || failureReason != nil || failureMessage != nil) {
		needsExisting = true
	}
	if statusValue != nil && *statusValue == workloadStatusRunning {
		needsExisting = true
	}
	if removalConfirmedAt != nil && statusValue == nil {
		needsExisting = true
	}

	var existingWorkload *workloadRecord
	if needsExisting {
		workload, err := s.getWorkloadByID(ctx, id)
		if err != nil {
			return nil, toStatusError(err)
		}
		existingWorkload = &workload
	}
	if removalConfirmedAt != nil && statusValue == nil && !isTerminalWorkloadStatus(existingWorkload.Status) {
		return nil, status.Error(codes.FailedPrecondition, "removal_confirmed_at requires a terminal workload")
	}

	resetLastActivity := existingWorkload != nil && statusValue != nil && *statusValue == workloadStatusRunning && existingWorkload.Status == workloadStatusStarting

	workload, err := s.updateWorkload(ctx, workloadUpdateInput{
		ID:                 id,
		Status:             statusValue,
		FailureReason:      failureReason,
		FailureMessage:     failureMessage,
		ContainersJSON:     containersJSON,
		InstanceID:         instanceID,
		RemovedAt:          removedAt,
		RemovalConfirmedAt: removalConfirmedAt,
		LastMeteringAt:     lastMeteringAt,
		ResetLastActivity:  resetLastActivity,
	})
	if err != nil {
		return nil, toStatusError(err)
	}
	statusChanged := existingWorkload != nil && statusValue != nil && *statusValue != existingWorkload.Status
	containersChanged := existingWorkload != nil && containersJSON != nil && !containersEqualByName(existingWorkload.Containers, containerRecords)
	failureReasonChanged := existingWorkload != nil && failureReason != nil && (existingWorkload.FailureReason == nil || *existingWorkload.FailureReason != *failureReason)
	failureMessageChanged := existingWorkload != nil && failureMessage != nil && (existingWorkload.FailureMessage == nil || *existingWorkload.FailureMessage != *failureMessage)
	agentStateChanged := existingWorkload != nil && workload.AgentState != existingWorkload.AgentState
	if statusChanged || containersChanged || failureReasonChanged || failureMessageChanged || agentStateChanged {
		s.publishWorkloadUpdateNotifications(ctx, workload, statusChanged, containersChanged, failureReasonChanged || failureMessageChanged, agentStateChanged)
	}
	protoWorkload, err := toProtoWorkload(workload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert workload: %v", err)
	}
	return &runnersv1.UpdateWorkloadResponse{Workload: protoWorkload}, nil
}

func (s *Server) publishWorkloadUpdateNotifications(ctx context.Context, workload workloadRecord, statusChanged, containersChanged, failureChanged, agentStateChanged bool) {
	if s.notificationsClient == nil {
		return
	}
	payloadFields := map[string]any{
		"workload_id": workload.Meta.ID.String(),
		"status":      workload.Status,
		"agent_state": workload.AgentState,
	}
	if workload.FailureReason != nil {
		payloadFields["failure_reason"] = *workload.FailureReason
	}
	if workload.FailureMessage != nil {
		payloadFields["failure_message"] = *workload.FailureMessage
	}
	payload, err := structpb.NewStruct(payloadFields)
	if err != nil {
		log.Printf("runners: build workload notification payload: %v", err)
		return
	}
	workloadRoom := fmt.Sprintf("workload:%s", workload.Meta.ID.String())
	updatedRooms := []string{
		fmt.Sprintf("organization:%s", workload.OrganizationID.String()),
		workloadRoom,
	}
	if statusChanged || containersChanged || failureChanged || agentStateChanged {
		s.publishWorkloadNotification(ctx, "workload.updated", updatedRooms, payload)
	}
	if statusChanged {
		// The flat room as well: the Orchestrator reconciles every workload in
		// the cluster and cannot subscribe per workload without racing the
		// creation of the next one.
		s.publishWorkloadNotification(ctx, "workload.status_changed", []string{workloadRoom, platformWorkloadsRoom}, payload)
	}
}

// platformWorkloadsRoom is cluster-wide and held by the platform alone. See
// architecture/notifications.md#cluster-wide-rooms.
const platformWorkloadsRoom = "workloads"

func (s *Server) publishWorkloadNotification(ctx context.Context, event string, rooms []string, payload *structpb.Struct) {
	if s.notificationsClient == nil {
		return
	}
	_, err := s.notificationsClient.Publish(ctx, &notificationsv1.PublishRequest{
		Event:   event,
		Rooms:   rooms,
		Payload: payload,
		Source:  "runners",
	})
	if err != nil {
		log.Printf("runners: publish %s notification: %v", event, err)
	}
}

func (s *Server) UpdateWorkloadStatus(ctx context.Context, req *runnersv1.UpdateWorkloadStatusRequest) (*runnersv1.UpdateWorkloadStatusResponse, error) {
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}
	statusValue, err := workloadStatusToString(req.GetStatus())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "status: %v", err)
	}
	containers, err := containersFromProto(req.GetContainers())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "containers: %v", err)
	}
	containersJSON, err := json.Marshal(containers)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal containers: %v", err)
	}

	var existingWorkload *workloadRecord
	if statusValue == workloadStatusRunning {
		workload, err := s.getWorkloadByID(ctx, id)
		if err != nil {
			return nil, toStatusError(err)
		}
		existingWorkload = &workload
	}
	resetLastActivity := existingWorkload != nil && existingWorkload.Status == workloadStatusStarting

	workload, err := s.updateWorkload(ctx, workloadUpdateInput{
		ID:                id,
		Status:            &statusValue,
		ContainersJSON:    &containersJSON,
		ResetLastActivity: resetLastActivity,
	})
	if err != nil {
		return nil, toStatusError(err)
	}
	protoWorkload, err := toProtoWorkload(workload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert workload: %v", err)
	}
	return &runnersv1.UpdateWorkloadStatusResponse{Workload: protoWorkload}, nil
}

func (s *Server) TouchWorkload(ctx context.Context, req *runnersv1.TouchWorkloadRequest) (*runnersv1.TouchWorkloadResponse, error) {
	callerID, err := identityFromMetadataOptional(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}
	workload, err := s.touchWorkloadForOwner(ctx, id, callerID)
	if err != nil {
		return nil, toStatusError(err)
	}
	if workload != nil {
		s.publishWorkloadUpdateNotifications(ctx, *workload, false, false, false, true)
	}
	return &runnersv1.TouchWorkloadResponse{}, nil
}

func (s *Server) DeleteWorkload(ctx context.Context, req *runnersv1.DeleteWorkloadRequest) (*runnersv1.DeleteWorkloadResponse, error) {
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}
	if err := s.softDeleteWorkload(ctx, id); err != nil {
		return nil, toStatusError(err)
	}
	return &runnersv1.DeleteWorkloadResponse{}, nil
}

func (s *Server) GetWorkload(ctx context.Context, req *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
	callerID, err := identityFromMetadataOptional(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}
	workload, err := s.getWorkloadByID(ctx, id)
	if err != nil {
		return nil, toStatusError(err)
	}
	// Internal callers (Terminal Proxy resolving runner_id after validating its
	// own ticket) forward no identity; the mesh AuthorizationPolicy gates them.
	if callerID != nil {
		if err := s.requireRelation(ctx, *callerID, workloadCanViewRelation, workloadObject(workload.Meta.ID)); err != nil {
			return nil, err
		}
	}
	workloads, err := s.buildWorkloadProtos(ctx, []workloadRecord{workload})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert workload: %v", err)
	}
	return &runnersv1.GetWorkloadResponse{Workload: workloads[0]}, nil
}

func (s *Server) ListWorkloadsByThread(ctx context.Context, req *runnersv1.ListWorkloadsByThreadRequest) (*runnersv1.ListWorkloadsByThreadResponse, error) {
	callerID, err := identityFromMetadataOptional(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	threadID, err := parseUUID(req.GetThreadId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "thread_id: %v", err)
	}
	var agentID *uuid.UUID
	if req.AgentId != nil {
		parsed, err := parseUUID(req.GetAgentId())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "agent_id: %v", err)
		}
		agentID = &parsed
	}
	statuses, err := workloadStatusesToStrings(req.GetStatuses())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "statuses: %v", err)
	}
	workloads, nextToken, err := s.listWorkloadsByThread(ctx, threadID, agentID, statuses, req.GetPageSize(), req.GetPageToken())
	if err != nil {
		var invalidToken *InvalidPageTokenError
		if errors.As(err, &invalidToken) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", invalidToken.Err)
		}
		return nil, status.Errorf(codes.Internal, "list workloads: %v", err)
	}
	if callerID != nil {
		viewWorkloadsCache := map[uuid.UUID]bool{}
		filtered := make([]workloadRecord, 0, len(workloads))
		for _, workload := range workloads {
			allowed, err := s.orgRelationAllowedCached(ctx, *callerID, workload.OrganizationID, organizationViewWorkloads, viewWorkloadsCache)
			if err != nil {
				return nil, err
			}
			if allowed {
				filtered = append(filtered, workload)
			}
		}
		workloads = filtered
	}
	protoWorkloads, err := toProtoWorkloadList(workloads)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert workloads: %v", err)
	}
	return &runnersv1.ListWorkloadsByThreadResponse{Workloads: protoWorkloads, NextPageToken: nextToken}, nil
}

func (s *Server) ListWorkloadsByAgentInstance(ctx context.Context, req *runnersv1.ListWorkloadsByAgentInstanceRequest) (*runnersv1.ListWorkloadsByAgentInstanceResponse, error) {
	callerID, err := identityFromMetadataOptional(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	agentInstanceID, err := parseUUID(req.GetAgentInstanceId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "agent_instance_id: %v", err)
	}
	statuses, err := workloadStatusesToStrings(req.GetStatuses())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "statuses: %v", err)
	}
	workloads, nextToken, err := s.listWorkloadsByAgentInstance(ctx, agentInstanceID, statuses, req.GetPageSize(), req.GetPageToken())
	if err != nil {
		var invalidToken *InvalidPageTokenError
		if errors.As(err, &invalidToken) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", invalidToken.Err)
		}
		return nil, status.Errorf(codes.Internal, "list workloads: %v", err)
	}
	if callerID != nil {
		memberCache := map[uuid.UUID]bool{}
		filtered := make([]workloadRecord, 0, len(workloads))
		for _, workload := range workloads {
			allowed, err := s.orgRelationAllowedCached(ctx, *callerID, workload.OrganizationID, organizationMemberRelation, memberCache)
			if err != nil {
				return nil, err
			}
			if allowed {
				filtered = append(filtered, workload)
			}
		}
		workloads = filtered
	}
	protoWorkloads, err := toProtoWorkloadList(workloads)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert workloads: %v", err)
	}
	return &runnersv1.ListWorkloadsByAgentInstanceResponse{Workloads: protoWorkloads, NextPageToken: nextToken}, nil
}

func (s *Server) ListWorkloads(ctx context.Context, req *runnersv1.ListWorkloadsRequest) (*runnersv1.ListWorkloadsResponse, error) {
	callerID, err := identityFromMetadataOptional(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	orgValue := strings.TrimSpace(req.GetOrganizationId())
	var organizationID *uuid.UUID
	if callerID != nil {
		if orgValue == "" {
			return nil, status.Error(codes.InvalidArgument, "organization_id: value is empty")
		}
		parsed, err := parseUUID(orgValue)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "organization_id: %v", err)
		}
		organizationID = &parsed
		if err := s.requireRelation(ctx, *callerID, organizationViewWorkloads, organizationObject(*organizationID)); err != nil {
			return nil, err
		}
	} else if orgValue != "" {
		parsed, err := parseUUID(orgValue)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "organization_id: %v", err)
		}
		organizationID = &parsed
	}

	filter := workloadListFilter{OrganizationID: organizationID}
	pendingSampleSet := false
	if req.Filter != nil {
		filter.AgentIDs = make([]uuid.UUID, 0, len(req.Filter.AgentIdIn))
		for _, agentValue := range req.Filter.AgentIdIn {
			parsed, err := parseUUID(agentValue)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "filter.agent_id_in: %v", err)
			}
			filter.AgentIDs = append(filter.AgentIDs, parsed)
		}
		filter.RunnerIDs = make([]uuid.UUID, 0, len(req.Filter.RunnerIdIn))
		for _, runnerValue := range req.Filter.RunnerIdIn {
			parsed, err := parseUUID(runnerValue)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "filter.runner_id_in: %v", err)
			}
			filter.RunnerIDs = append(filter.RunnerIDs, parsed)
		}
		if req.Filter.StartedAfter != nil {
			if err := req.Filter.StartedAfter.CheckValid(); err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "filter.started_after: %v", err)
			}
			startedAfter := req.Filter.StartedAfter.AsTime()
			filter.StartedAfter = &startedAfter
		}
		if req.Filter.StartedBefore != nil {
			if err := req.Filter.StartedBefore.CheckValid(); err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "filter.started_before: %v", err)
			}
			startedBefore := req.Filter.StartedBefore.AsTime()
			filter.StartedBefore = &startedBefore
		}
		if req.Filter.PendingSample != nil {
			filter.PendingSample = req.Filter.GetPendingSample()
			pendingSampleSet = true
		}
		filter.OwnerKinds = make([]string, 0, len(req.Filter.OwnerKindIn))
		for _, kind := range req.Filter.OwnerKindIn {
			value, err := runtimeOwnerKindToString(kind)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "filter.owner_kind_in: %v", err)
			}
			if kind == runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_UNSPECIFIED {
				return nil, status.Error(codes.InvalidArgument, "filter.owner_kind_in: unspecified kind")
			}
			filter.OwnerKinds = append(filter.OwnerKinds, value)
		}
		filter.OwnerIDs = make([]uuid.UUID, 0, len(req.Filter.OwnerIdIn))
		for _, ownerValue := range req.Filter.OwnerIdIn {
			parsed, err := parseUUID(ownerValue)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "filter.owner_id_in: %v", err)
			}
			filter.OwnerIDs = append(filter.OwnerIDs, parsed)
		}
	}

	if req.RunnerId != nil {
		parsed, err := parseUUID(req.GetRunnerId())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "runner_id: %v", err)
		}
		filter.RunnerIDs = append(filter.RunnerIDs, parsed)
	}
	if req.PendingSample != nil && !pendingSampleSet {
		filter.PendingSample = req.GetPendingSample()
	}

	statusFilters := make([]runnersv1.WorkloadStatus, 0)
	if req.Filter != nil {
		statusFilters = append(statusFilters, req.Filter.StatusIn...)
	}
	statusFilters = append(statusFilters, req.GetStatuses()...)
	statuses, err := workloadStatusesToStrings(statusFilters)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "statuses: %v", err)
	}
	filter.Statuses = statuses

	sort, err := parseWorkloadSort(req.GetSort())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "sort: %v", err)
	}

	workloads, nextToken, err := s.listWorkloads(ctx, filter, sort, req.GetPageSize(), req.GetPageToken())
	if err != nil {
		var invalidToken *InvalidPageTokenError
		if errors.As(err, &invalidToken) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", invalidToken.Err)
		}
		return nil, status.Errorf(codes.Internal, "list workloads: %v", err)
	}
	protoWorkloads, err := s.buildWorkloadProtos(ctx, workloads)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert workloads: %v", err)
	}
	return &runnersv1.ListWorkloadsResponse{Workloads: protoWorkloads, NextPageToken: nextToken}, nil
}

func (s *Server) BatchUpdateWorkloadSampledAt(ctx context.Context, req *runnersv1.BatchUpdateWorkloadSampledAtRequest) (*runnersv1.BatchUpdateWorkloadSampledAtResponse, error) {
	entries := req.GetEntries()
	if len(entries) == 0 {
		return &runnersv1.BatchUpdateWorkloadSampledAtResponse{}, nil
	}
	updates, err := parseSampledAtEntries(entries)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.batchUpdateWorkloadSampledAt(ctx, updates); err != nil {
		return nil, status.Errorf(codes.Internal, "batch update workloads: %v", err)
	}
	return &runnersv1.BatchUpdateWorkloadSampledAtResponse{}, nil
}

func (s *Server) writeWorkloadAuthorization(ctx context.Context, workloadID, organizationID uuid.UUID, agentID, ownerID *uuid.UUID) error {
	tuples := workloadAuthorizationTuples(workloadID, organizationID, agentID, ownerID)
	if len(tuples) == 0 {
		return nil
	}
	if _, err := s.authorizationClient.Write(ctx, &authorizationv1.WriteRequest{Writes: tuples}); err != nil {
		return status.Errorf(codes.Internal, "authorization write: %v", err)
	}
	return nil
}

// workloadAuthorizationTuples names who may see a workload.
//
// The owner is here as well as the agent class because the owner is who
// actually asks. A workload authenticates as its own runtime identity -- the
// agent instance, or the sandbox -- so a check made from inside one carried an
// id the class tuple never matched, and `agyn expose list` in a workload was
// refused a view of the workload it was running in.
func workloadAuthorizationTuples(workloadID, organizationID uuid.UUID, agentID, ownerID *uuid.UUID) []*authorizationv1.TupleKey {
	object := workloadObject(workloadID)
	tuples := []*authorizationv1.TupleKey{
		{
			User:     organizationObject(organizationID),
			Relation: workloadOrgRelation,
			Object:   object,
		},
	}
	seen := map[uuid.UUID]struct{}{}
	for _, viewer := range []*uuid.UUID{agentID, ownerID} {
		if viewer == nil {
			continue
		}
		if _, already := seen[*viewer]; already {
			continue
		}
		seen[*viewer] = struct{}{}
		tuples = append(tuples, &authorizationv1.TupleKey{
			User:     identityObject(*viewer),
			Relation: workloadOwnerAgentRelation,
			Object:   object,
		})
	}
	return tuples
}

func (s *Server) insertWorkload(ctx context.Context, input workloadInsertInput) (workloadRecord, error) {
	containersJSON := input.ContainersJSON
	if len(containersJSON) == 0 {
		containersJSON = []byte("[]")
	}
	query := fmt.Sprintf(`INSERT INTO workloads (id, runner_id, thread_id, agent_id, organization_id, status, containers, ziti_identity_id, allocated_cpu_millicores, allocated_ram_bytes, flavor, persistent_shells, owner_kind, owner_id, last_activity_at, created_at, updated_at)
	    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW(), NOW(), NOW())
	    RETURNING %s`, workloadColumns)
	args := []any{
		input.ID,
		input.RunnerID,
		nullableUUIDValue(input.ThreadID),
		nullableUUIDValue(input.AgentID),
		input.OrganizationID,
		input.Status,
		containersJSON,
		input.ZitiIdentityID,
		input.AllocatedCPUMillicores,
		input.AllocatedRAMBytes,
		input.Flavor,
		input.PersistentShells,
		input.OwnerKind,
		input.OwnerID,
	}
	if input.Preparation != nil {
		query = fmt.Sprintf(`INSERT INTO workloads (id, runner_id, thread_id, agent_id, organization_id, status, containers, ziti_identity_id, allocated_cpu_millicores, allocated_ram_bytes, flavor, persistent_shells, owner_kind, owner_id, last_activity_at, created_at, updated_at, preparation_phase, preparation_revision, prepared_backend_id, prepared_volume_ids)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW(), NOW(), NOW(), 'reserved', 1, $15, $16)
            RETURNING %s`, workloadColumns)
		args = append(args, input.Preparation.BackendId, input.Preparation.VolumeIds)
		if input.Preparation.Resources != nil {
			data, err := protojson.Marshal(input.Preparation.Resources)
			if err != nil {
				return workloadRecord{}, err
			}
			query = fmt.Sprintf(`INSERT INTO workloads (id, runner_id, thread_id, agent_id, organization_id, status, containers, ziti_identity_id, allocated_cpu_millicores, allocated_ram_bytes, flavor, persistent_shells, owner_kind, owner_id, last_activity_at, created_at, updated_at, preparation_phase, preparation_revision, prepared_backend_id, prepared_volume_ids, resource_anchors)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW(), NOW(), NOW(), 'reserved', 1, $15, $16, $17)
            RETURNING %s`, workloadColumns)
			args = append(args, data)
		}
	}
	row := s.pool.QueryRow(ctx, query, args...)
	workload, err := scanWorkload(row)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.Code == "23505" {
				return workloadRecord{}, AlreadyExists("workload")
			}
			if pgErr.Code == "23503" {
				return workloadRecord{}, NotFound("runner")
			}
		}
		return workloadRecord{}, err
	}
	workload.OwnerKind = input.OwnerKind
	workload.OwnerID = input.OwnerID
	return workload, nil
}

// updateWorkload separates billing end from physical-removal confirmation. Only
// an explicit terminal-workload lifecycle write sets the first confirmation;
// runner status reports and logical deletion do not infer it.
// ../../migrations/0017_workload_removal_confirmation.sql leaves old rows unverified
// and prevents reopening confirmed work. Migration 0019 retains admission for
// unconfirmed failures/stops and prevents identity/deletion bypasses.
// @see orchestrator::internal/reconciler/workload_reconcile
// @see gateway::internal/gateway/runners
func (s *Server) updateWorkload(ctx context.Context, input workloadUpdateInput) (workloadRecord, error) {
	clauses := make([]string, 0, 6)
	args := make([]any, 0, 6)

	if input.Status != nil {
		addUpdateClause(&clauses, &args, "status", *input.Status)
	}
	if input.FailureReason != nil {
		addUpdateClause(&clauses, &args, "failure_reason", *input.FailureReason)
	}
	if input.FailureMessage != nil {
		addUpdateClause(&clauses, &args, "failure_message", *input.FailureMessage)
	}
	if input.ContainersJSON != nil {
		payload := *input.ContainersJSON
		if len(payload) == 0 {
			payload = []byte("[]")
		}
		addUpdateClause(&clauses, &args, "containers", payload)
	}
	if input.InstanceID != nil {
		addUpdateClause(&clauses, &args, "instance_id", *input.InstanceID)
	}
	if input.RemovedAt != nil {
		addUpdateClause(&clauses, &args, "removed_at", *input.RemovedAt)
	}
	if input.RemovalConfirmedAt != nil {
		args = append(args, *input.RemovalConfirmedAt)
		clauses = append(clauses, fmt.Sprintf("removal_confirmed_at = COALESCE(removal_confirmed_at, $%d)", len(args)))
	}
	// A terminal status always carries removed_at, whoever wrote it.
	if input.RemovedAt == nil && input.Status != nil && isTerminalWorkloadStatus(*input.Status) {
		clauses = append(clauses, "removed_at = COALESCE(removed_at, NOW())")
	}
	if input.LastMeteringAt != nil {
		addUpdateClause(&clauses, &args, "last_metering_sampled_at", *input.LastMeteringAt)
	}
	if input.ResetLastActivity {
		clauses = append(clauses, "last_activity_at = NOW()")
	}
	query, args := buildUpdateQuery("workloads", workloadColumns, clauses, args, input.ID)
	row := s.pool.QueryRow(ctx, query, args...)
	workload, err := scanWorkload(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return workloadRecord{}, NotFound("workload")
		}
		return workloadRecord{}, err
	}
	return workload, nil
}

func (s *Server) getWorkloadByID(ctx context.Context, id uuid.UUID) (workloadRecord, error) {
	row := s.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT %s FROM workloads WHERE id = $1`, workloadColumns),
		id,
	)
	workload, err := scanWorkload(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return workloadRecord{}, NotFound("workload")
		}
		return workloadRecord{}, err
	}
	return workload, nil
}

func (s *Server) softDeleteWorkload(ctx context.Context, id uuid.UUID) error {
	statusValue := workloadStatusStopped
	clauses := make([]string, 0, 2)
	args := make([]any, 0, 2)
	addUpdateClause(&clauses, &args, "status", statusValue)
	clauses = append(clauses, "removed_at = NOW()")
	query, args := buildUpdateQuery("workloads", workloadColumns, clauses, args, id)
	row := s.pool.QueryRow(ctx, query, args...)
	_, err := scanWorkload(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return NotFound("workload")
		}
		return err
	}
	return nil
}

func (s *Server) touchWorkloadForOwner(ctx context.Context, id uuid.UUID, callerID *uuid.UUID) (*workloadRecord, error) {
	workload, err := s.getWorkloadByID(ctx, id)
	if err != nil {
		return nil, err
	}
	switch workload.OwnerKind {
	case runtimeOwnerKindAgentInstance:
		if callerID == nil {
			return nil, PermissionDenied()
		}
		if *callerID != workload.OwnerID {
			return nil, PermissionDenied()
		}
		return s.touchAgentInstanceWorkload(ctx, workload.Meta.ID, workload.OwnerID)
	case runtimeOwnerKindSandbox:
		if callerID != nil {
			return nil, PermissionDenied()
		}
		return nil, s.touchSandboxWorkload(ctx, workload.Meta.ID, workload.OwnerID)
	default:
		return nil, fmt.Errorf("unknown runtime owner kind %q", workload.OwnerKind)
	}
}

func (s *Server) touchAgentInstanceWorkload(ctx context.Context, id uuid.UUID, ownerID uuid.UUID) (*workloadRecord, error) {
	query := fmt.Sprintf("UPDATE workloads SET agent_state = $1, last_activity_at = NOW(), updated_at = NOW() WHERE id = $2 AND owner_kind = $3 AND owner_id = $4 AND agent_state = $5 RETURNING %s", workloadColumns)
	row := s.pool.QueryRow(ctx, query, workloadAgentStateProcessing, id, runtimeOwnerKindAgentInstance, ownerID, workloadAgentStateIdle)
	workload, err := scanWorkload(row)
	if err == nil {
		return &workload, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	return nil, s.touchRuntimeOwnerWorkload(ctx, id, runtimeOwnerKindAgentInstance, ownerID)
}

func (s *Server) touchSandboxWorkload(ctx context.Context, id uuid.UUID, ownerID uuid.UUID) error {
	return s.touchRuntimeOwnerWorkload(ctx, id, runtimeOwnerKindSandbox, ownerID)
}

func (s *Server) touchRuntimeOwnerWorkload(ctx context.Context, id uuid.UUID, ownerKind string, ownerID uuid.UUID) error {
	result, err := s.pool.Exec(ctx, `UPDATE workloads SET last_activity_at = NOW(), updated_at = NOW() WHERE id = $1 AND owner_kind = $2 AND owner_id = $3`, id, ownerKind, ownerID)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return NotFound("workload")
	}
	return nil
}

func (s *Server) listWorkloadsByThread(ctx context.Context, threadID uuid.UUID, agentID *uuid.UUID, statuses []string, pageSize int32, pageToken string) ([]workloadRecord, string, error) {
	clauses := []string{"thread_id = $1"}
	args := []any{threadID}

	if agentID != nil {
		clauses = append(clauses, fmt.Sprintf("agent_id = $%d", len(args)+1))
		args = append(args, *agentID)
	}
	return s.listWorkloadsByClauses(ctx, clauses, args, statuses, pageSize, pageToken)
}

func (s *Server) listWorkloadsByAgentInstance(ctx context.Context, agentInstanceID uuid.UUID, statuses []string, pageSize int32, pageToken string) ([]workloadRecord, string, error) {
	clauses := []string{"owner_kind = $1", "owner_id = $2"}
	args := []any{runtimeOwnerKindAgentInstance, agentInstanceID}
	return s.listWorkloadsByClauses(ctx, clauses, args, statuses, pageSize, pageToken)
}

func (s *Server) listWorkloadsByClauses(ctx context.Context, clauses []string, args []any, statuses []string, pageSize int32, pageToken string) ([]workloadRecord, string, error) {
	limit := normalizePageSize(pageSize)
	if len(statuses) > 0 {
		clauses = append(clauses, fmt.Sprintf("status = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[string](statuses))
	}

	if pageToken != "" {
		cursor, err := decodeWorkloadCursor(pageToken)
		if err != nil {
			return nil, "", InvalidPageToken(err)
		}
		clauses = append(clauses, fmt.Sprintf("(created_at < $%d OR (created_at = $%d AND id < $%d))", len(args)+1, len(args)+1, len(args)+2))
		args = append(args, cursor.CreatedAt, cursor.ID)
	}

	query := strings.Builder{}
	query.WriteString(fmt.Sprintf("SELECT %s FROM workloads", workloadColumns))
	query.WriteString(" WHERE ")
	query.WriteString(strings.Join(clauses, " AND "))
	query.WriteString(fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", len(args)+1))
	args = append(args, int(limit)+1)

	rows, err := s.pool.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	workloads := make([]workloadRecord, 0, limit)
	var (
		lastID  uuid.UUID
		lastAt  time.Time
		hasMore bool
	)
	for rows.Next() {
		if int32(len(workloads)) == limit {
			hasMore = true
			break
		}
		workload, err := scanWorkload(rows)
		if err != nil {
			return nil, "", err
		}
		workloads = append(workloads, workload)
		lastID = workload.Meta.ID
		lastAt = workload.Meta.CreatedAt
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	nextToken := ""
	if hasMore {
		nextToken = encodeWorkloadCursor(lastAt, lastID)
	}
	return workloads, nextToken, nil
}

type workloadCursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

func encodeWorkloadCursor(createdAt time.Time, id uuid.UUID) string {
	payload := fmt.Sprintf("%s|%s", createdAt.UTC().Format(time.RFC3339Nano), id.String())
	return base64.RawURLEncoding.EncodeToString([]byte(payload))
}

func decodeWorkloadCursor(token string) (workloadCursor, error) {
	if token == "" {
		return workloadCursor{}, errors.New("empty token")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return workloadCursor{}, fmt.Errorf("decode token: %w", err)
	}
	parts := strings.SplitN(string(decoded), "|", 2)
	if len(parts) != 2 {
		return workloadCursor{}, errors.New("invalid token")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return workloadCursor{}, fmt.Errorf("parse created_at: %w", err)
	}
	id, err := uuid.Parse(parts[1])
	if err != nil {
		return workloadCursor{}, fmt.Errorf("parse id: %w", err)
	}
	return workloadCursor{CreatedAt: createdAt, ID: id}, nil
}

func parseWorkloadSort(sort *runnersv1.ListWorkloadsSort) (workloadListSort, error) {
	field := workloadSortStarted
	if sort != nil {
		switch sort.GetField() {
		case runnersv1.ListWorkloadsSortField_LIST_WORKLOADS_SORT_FIELD_UNSPECIFIED:
			field = workloadSortStarted
		case runnersv1.ListWorkloadsSortField_LIST_WORKLOADS_SORT_FIELD_STARTED:
			field = workloadSortStarted
		case runnersv1.ListWorkloadsSortField_LIST_WORKLOADS_SORT_FIELD_AGENT:
			field = workloadSortAgent
		case runnersv1.ListWorkloadsSortField_LIST_WORKLOADS_SORT_FIELD_RUNNER:
			field = workloadSortRunner
		case runnersv1.ListWorkloadsSortField_LIST_WORKLOADS_SORT_FIELD_STATUS:
			field = workloadSortStatus
		case runnersv1.ListWorkloadsSortField_LIST_WORKLOADS_SORT_FIELD_DURATION:
			field = workloadSortDuration
		default:
			return workloadListSort{}, fmt.Errorf("invalid sort field: %s", sort.GetField().String())
		}
	}
	defaultDirection := sortDesc
	if sort == nil {
		return workloadListSort{Field: field, Direction: defaultDirection}, nil
	}
	direction, err := parseSortDirection(sort.GetDirection(), defaultDirection)
	if err != nil {
		return workloadListSort{}, err
	}
	if field == workloadSortDuration {
		direction = direction.invert()
	}
	return workloadListSort{Field: field, Direction: direction}, nil
}

func workloadSortColumn(field workloadSortField) string {
	switch field {
	case workloadSortStatus:
		return "workloads.status"
	case workloadSortDuration:
		return "workloads.created_at"
	case workloadSortStarted:
		return "workloads.created_at"
	default:
		panic("invalid sort field")
	}
}

func workloadCursorPrimary(field workloadSortField, cursor listCursor) (any, error) {
	switch field {
	case workloadSortAgent:
		// An empty primary is legitimate here: it is the sort key of
		// sandbox-owned workloads, which have no agent class.
		return strings.ToLower(strings.TrimSpace(cursor.Primary)), nil
	case workloadSortRunner:
		primary := strings.TrimSpace(cursor.Primary)
		if primary == "" {
			return nil, errors.New("cursor primary is empty")
		}
		return strings.ToLower(primary), nil
	case workloadSortStatus:
		if strings.TrimSpace(cursor.Primary) == "" {
			return nil, errors.New("cursor primary is empty")
		}
		return cursor.Primary, nil
	case workloadSortDuration, workloadSortStarted:
		createdAt, err := time.Parse(time.RFC3339Nano, cursor.Primary)
		if err != nil {
			return nil, fmt.Errorf("parse created_at: %w", err)
		}
		return createdAt, nil
	default:
		return nil, errors.New("invalid sort field")
	}
}

func workloadPrimaryValue(record workloadRecord, field workloadSortField, agentNames map[uuid.UUID]string, runnerNames map[uuid.UUID]string) (string, error) {
	switch field {
	case workloadSortAgent:
		if record.AgentID == uuid.Nil {
			return workloadAgentSortKeyless, nil
		}
		name, ok := agentNames[record.AgentID]
		if !ok {
			return "", fmt.Errorf("agent name missing for %s", record.AgentID)
		}
		return strings.ToLower(strings.TrimSpace(name)), nil
	case workloadSortRunner:
		name, ok := runnerNames[record.RunnerID]
		if !ok {
			return "", fmt.Errorf("runner name missing for %s", record.RunnerID)
		}
		return strings.ToLower(strings.TrimSpace(name)), nil
	case workloadSortStatus:
		return record.Status, nil
	case workloadSortDuration, workloadSortStarted:
		return record.Meta.CreatedAt.UTC().Format(time.RFC3339Nano), nil
	default:
		return "", errors.New("invalid sort field")
	}
}

func addCursorClause(clauses *[]string, args *[]any, column string, direction sortDirection, primary any, id uuid.UUID) {
	operator := ">"
	if direction == sortDesc {
		operator = "<"
	}
	clause := fmt.Sprintf("(%s %s $%d OR (%s = $%d AND workloads.id > $%d))", column, operator, len(*args)+1, column, len(*args)+1, len(*args)+2)
	*clauses = append(*clauses, clause)
	*args = append(*args, primary, id)
}

func (s *Server) listWorkloadAgentIDs(ctx context.Context, clauses []string, args []any) ([]uuid.UUID, error) {
	query := strings.Builder{}
	query.WriteString("SELECT DISTINCT agent_id FROM workloads")
	if len(clauses) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(clauses, " AND "))
	}
	rows, err := s.pool.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	agentIDs := []uuid.UUID{}
	for rows.Next() {
		// Sandbox-owned workloads carry a NULL agent_id, so DISTINCT yields a
		// NULL row whenever the scope contains one.
		var agentID nullableUUIDScanner
		if err := rows.Scan(&agentID); err != nil {
			return nil, err
		}
		if !agentID.Valid {
			continue
		}
		agentIDs = append(agentIDs, agentID.UUID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return agentIDs, nil
}

// workloadAgentSortKeyless is the agent sort key given to workloads with no
// agent class (sandbox owners). It buckets them together deterministically —
// before every named agent ascending, after every named agent descending — so
// ordering and cursor pagination stay stable on mixed pages.
const workloadAgentSortKeyless = ""

func buildWorkloadAgentSortExpr(agentNames map[uuid.UUID]string, startIndex int) (string, []any) {
	if len(agentNames) == 0 {
		return "''::text", nil
	}
	ids := make([]uuid.UUID, 0, len(agentNames))
	for id := range agentNames {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].String() < ids[j].String()
	})

	parts := make([]string, 0, len(ids))
	args := make([]any, 0, len(ids)*2)
	idx := startIndex
	for _, id := range ids {
		name := strings.ToLower(strings.TrimSpace(agentNames[id]))
		parts = append(parts, fmt.Sprintf("WHEN $%d THEN $%d", idx, idx+1))
		args = append(args, id, name)
		idx += 2
	}
	return fmt.Sprintf("CASE workloads.agent_id %s ELSE ''::text END", strings.Join(parts, " ")), args
}

func (s *Server) listWorkloads(ctx context.Context, filter workloadListFilter, sort workloadListSort, pageSize int32, pageToken string) ([]workloadRecord, string, error) {
	limit := normalizePageSize(pageSize)
	clauses := []string{}
	args := []any{}
	if filter.OrganizationID != nil {
		clauses = append(clauses, fmt.Sprintf("workloads.organization_id = $%d", len(args)+1))
		args = append(args, *filter.OrganizationID)
	}

	if len(filter.AgentIDs) > 0 {
		clauses = append(clauses, fmt.Sprintf("workloads.agent_id = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[uuid.UUID](filter.AgentIDs))
	}
	if len(filter.RunnerIDs) > 0 {
		clauses = append(clauses, fmt.Sprintf("workloads.runner_id = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[uuid.UUID](filter.RunnerIDs))
	}
	if len(filter.OwnerKinds) > 0 {
		clauses = append(clauses, fmt.Sprintf("workloads.owner_kind = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[string](filter.OwnerKinds))
	}
	if len(filter.OwnerIDs) > 0 {
		clauses = append(clauses, fmt.Sprintf("workloads.owner_id = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[uuid.UUID](filter.OwnerIDs))
	}
	if len(filter.Statuses) > 0 {
		clauses = append(clauses, fmt.Sprintf("workloads.status = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[string](filter.Statuses))
	}
	if filter.StartedAfter != nil {
		clauses = append(clauses, fmt.Sprintf("workloads.created_at >= $%d", len(args)+1))
		args = append(args, *filter.StartedAfter)
	}
	if filter.StartedBefore != nil {
		clauses = append(clauses, fmt.Sprintf("workloads.created_at < $%d", len(args)+1))
		args = append(args, *filter.StartedBefore)
	}
	if filter.PendingSample {
		clauses = append(clauses, pendingSampleClause)
	}

	var (
		joinClause  string
		sortColumn  string
		agentNames  map[uuid.UUID]string
		runnerNames map[uuid.UUID]string
	)
	switch sort.Field {
	case workloadSortRunner:
		joinClause = " JOIN runners ON workloads.runner_id = runners.id"
		sortColumn = "LOWER(runners.name)"
	case workloadSortAgent:
		agentIDs, err := s.listWorkloadAgentIDs(ctx, clauses, args)
		if err != nil {
			return nil, "", err
		}
		agentNames, err = s.resolveAgentNames(ctx, uniqueUUIDs(agentIDs))
		if err != nil {
			return nil, "", err
		}
		sortExpr, sortArgs := buildWorkloadAgentSortExpr(agentNames, len(args)+1)
		sortColumn = sortExpr
		args = append(args, sortArgs...)
	default:
		sortColumn = workloadSortColumn(sort.Field)
	}

	if pageToken != "" {
		cursor, cursorID, err := decodeListCursor(pageToken)
		if err != nil {
			return nil, "", InvalidPageToken(err)
		}
		primaryValue, err := workloadCursorPrimary(sort.Field, cursor)
		if err != nil {
			return nil, "", InvalidPageToken(err)
		}
		addCursorClause(&clauses, &args, sortColumn, sort.Direction, primaryValue, cursorID)
	}

	query := strings.Builder{}
	query.WriteString(fmt.Sprintf("SELECT %s FROM workloads", workloadColumns))
	query.WriteString(joinClause)
	if len(clauses) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(clauses, " AND "))
	}
	query.WriteString(fmt.Sprintf(" ORDER BY %s %s, workloads.id ASC LIMIT $%d", sortColumn, sort.Direction, len(args)+1))
	args = append(args, int(limit)+1)

	rows, err := s.pool.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	workloads := make([]workloadRecord, 0, limit)
	var (
		lastRecord workloadRecord
		hasMore    bool
	)
	for rows.Next() {
		if int32(len(workloads)) == limit {
			hasMore = true
			break
		}
		workload, err := scanWorkload(rows)
		if err != nil {
			return nil, "", err
		}
		workloads = append(workloads, workload)
		lastRecord = workload
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	nextToken := ""
	if hasMore {
		if sort.Field == workloadSortRunner {
			runnerIDs := []uuid.UUID{lastRecord.RunnerID}
			resolved, err := s.resolveRunnerNames(ctx, runnerIDs)
			if err != nil {
				return nil, "", err
			}
			runnerNames = resolved
		}
		primary, err := workloadPrimaryValue(lastRecord, sort.Field, agentNames, runnerNames)
		if err != nil {
			return nil, "", err
		}
		nextToken, err = encodeListCursor(primary, lastRecord.Meta.ID)
		if err != nil {
			return nil, "", err
		}
	}
	return workloads, nextToken, nil
}

func (s *Server) buildWorkloadProtos(ctx context.Context, records []workloadRecord) ([]*runnersv1.Workload, error) {
	if len(records) == 0 {
		return []*runnersv1.Workload{}, nil
	}
	agentIDs := make([]uuid.UUID, 0, len(records))
	runnerIDs := make([]uuid.UUID, 0, len(records))
	sandboxIDs := make([]uuid.UUID, 0, len(records))
	for _, record := range records {
		if record.AgentID != uuid.Nil {
			agentIDs = append(agentIDs, record.AgentID)
		}
		if record.OwnerKind == runtimeOwnerKindSandbox {
			sandboxIDs = append(sandboxIDs, record.OwnerID)
		}
		runnerIDs = append(runnerIDs, record.RunnerID)
	}
	uniqueAgents := uniqueUUIDs(agentIDs)
	uniqueRunners := uniqueUUIDs(runnerIDs)
	uniqueSandboxes := uniqueUUIDs(sandboxIDs)

	agentNames, err := s.resolveAgentNames(ctx, uniqueAgents)
	if err != nil {
		return nil, err
	}
	runnerNames, err := s.resolveRunnerNames(ctx, uniqueRunners)
	if err != nil {
		return nil, err
	}
	sandboxNames, err := s.resolveSandboxNames(ctx, uniqueSandboxes)
	if err != nil {
		return nil, err
	}

	workloads := make([]*runnersv1.Workload, 0, len(records))
	for _, record := range records {
		agentName := ""
		if record.AgentID != uuid.Nil {
			name, ok := agentNames[record.AgentID]
			if !ok {
				return nil, fmt.Errorf("agent name missing for %s", record.AgentID)
			}
			agentName = name
		}
		ownerName := ""
		if record.OwnerKind == runtimeOwnerKindSandbox {
			name, ok := sandboxNames[record.OwnerID]
			if !ok {
				return nil, fmt.Errorf("sandbox name missing for %s", record.OwnerID)
			}
			ownerName = name
		}
		runnerName, ok := runnerNames[record.RunnerID]
		if !ok {
			return nil, fmt.Errorf("runner name missing for %s", record.RunnerID)
		}
		workload, err := toProtoWorkloadWithNames(record, agentName, ownerName, runnerName)
		if err != nil {
			return nil, err
		}
		workloads = append(workloads, workload)
	}
	return workloads, nil
}

func (s *Server) batchUpdateWorkloadSampledAt(ctx context.Context, entries []sampledAtEntry) error {
	query, args := buildBatchSampledAtUpdateQuery("workloads", entries)
	if _, err := s.pool.Exec(ctx, query, args...); err != nil {
		return err
	}
	return nil
}

func scanWorkload(row pgx.Row) (workloadRecord, error) {
	var (
		workload            workloadRecord
		containersData      []byte
		threadID            nullableUUIDScanner
		agentID             nullableUUIDScanner
		failureReason       pgtype.Text
		failureMessage      pgtype.Text
		instanceID          pgtype.Text
		removedAt           pgtype.Timestamptz
		removalConfirmedAt  pgtype.Timestamptz
		lastMeteringAt      pgtype.Timestamptz
		ownerKindRaw        any
		ownerIDRaw          any
		preparationPhase    pgtype.Text
		preparationRevision int64
		preparedBackend     pgtype.Text
		preparedVolumeIDs   []string
		preparedBinding     []byte
		preparedRemoval     []byte
		resourceAnchors     []byte
	)
	if err := row.Scan(
		&workload.Meta.ID,
		&workload.RunnerID,
		&threadID,
		&agentID,
		&workload.OrganizationID,
		&workload.Status,
		&workload.AgentState,
		&failureReason,
		&failureMessage,
		&containersData,
		&workload.ZitiIdentityID,
		&workload.AllocatedCPUMillicores,
		&workload.AllocatedRAMBytes,
		&workload.Flavor,
		&workload.PersistentShells,
		&instanceID,
		&workload.LastActivityAt,
		&lastMeteringAt,
		&removedAt,
		&ownerKindRaw,
		&ownerIDRaw,
		&workload.Meta.CreatedAt,
		&workload.Meta.UpdatedAt,
		&removalConfirmedAt,
		&preparationPhase,
		&preparationRevision,
		&preparedBackend,
		&preparedVolumeIDs,
		&preparedBinding,
		&preparedRemoval,
		&resourceAnchors,
	); err != nil {
		return workloadRecord{}, err
	}
	ownerKind, ok := ownerKindRaw.(string)
	if !ok || ownerKind == "" {
		return workloadRecord{}, fmt.Errorf("owner_kind missing")
	}
	ownerID, err := scanNullableUUID(ownerIDRaw)
	if err != nil {
		return workloadRecord{}, err
	}
	if ownerID == nil {
		return workloadRecord{}, fmt.Errorf("owner_id missing")
	}
	workload.OwnerKind = ownerKind
	workload.OwnerID = *ownerID
	if threadID.Valid {
		workload.ThreadID = threadID.UUID
	}
	if agentID.Valid {
		workload.AgentID = agentID.UUID
	}
	if len(containersData) == 0 {
		containersData = []byte("[]")
	}
	if err := json.Unmarshal(containersData, &workload.Containers); err != nil {
		return workloadRecord{}, err
	}
	if failureReason.Valid {
		value := failureReason.String
		workload.FailureReason = &value
	}
	if failureMessage.Valid {
		value := failureMessage.String
		workload.FailureMessage = &value
	}
	if instanceID.Valid {
		value := instanceID.String
		workload.InstanceID = &value
	}
	if removedAt.Valid {
		value := removedAt.Time
		workload.RemovedAt = &value
	}
	if removalConfirmedAt.Valid {
		value := removalConfirmedAt.Time
		workload.RemovalConfirmedAt = &value
	}
	if lastMeteringAt.Valid {
		value := lastMeteringAt.Time
		workload.LastMeteringAt = &value
	}
	if preparationPhase.Valid {
		phase, ok := preparationPhases[preparationPhase.String]
		if !ok || preparationRevision < 1 || !preparedBackend.Valid || !validVolumeBackend(preparedBackend.String) {
			return workloadRecord{}, fmt.Errorf("invalid stored workload preparation")
		}
		workload.Preparation = &runnersv1.PreparedWorkloadLifecycle{
			Phase: phase, Revision: uint64(preparationRevision), BackendId: preparedBackend.String, VolumeIds: preparedVolumeIDs,
		}
		if len(preparedBinding) > 0 {
			workload.Preparation.Binding = &runnerv1.WorkloadBinding{}
			if err := protojson.Unmarshal(preparedBinding, workload.Preparation.Binding); err != nil {
				return workloadRecord{}, fmt.Errorf("decode workload binding: %w", err)
			}
		}
		if len(preparedRemoval) > 0 {
			workload.Preparation.RemovalObservation = &runnerv1.RemovePreparedWorkloadResponse{}
			if err := protojson.Unmarshal(preparedRemoval, workload.Preparation.RemovalObservation); err != nil {
				return workloadRecord{}, fmt.Errorf("decode workload removal: %w", err)
			}
		}
		if len(resourceAnchors) > 0 {
			workload.Preparation.Resources = &runnersv1.WorkloadResourceAnchors{}
			if err := protojson.Unmarshal(resourceAnchors, workload.Preparation.Resources); err != nil {
				return workloadRecord{}, fmt.Errorf("decode workload anchors: %w", err)
			}
			if err := validateWorkloadResourceAnchors(workload, workload.Preparation.Resources); err != nil {
				return workloadRecord{}, fmt.Errorf("invalid stored workload anchors: %w", err)
			}
		}
	} else if len(resourceAnchors) != 0 {
		return workloadRecord{}, fmt.Errorf("anchors require prepared workload")
	}
	return workload, nil
}

func toProtoWorkload(record workloadRecord) (*runnersv1.Workload, error) {
	statusValue, err := workloadStatusFromString(record.Status)
	if err != nil {
		return nil, err
	}
	agentStateValue, err := workloadAgentStateFromString(record.AgentState)
	if err != nil {
		return nil, err
	}
	containers, err := containersToProto(record.Containers)
	if err != nil {
		return nil, err
	}
	protoWorkload := &runnersv1.Workload{
		Meta:                   toProtoEntityMeta(record.Meta),
		RunnerId:               record.RunnerID.String(),
		OrganizationId:         record.OrganizationID.String(),
		Status:                 statusValue,
		AgentState:             agentStateValue,
		Containers:             containers,
		ZitiIdentityId:         record.ZitiIdentityID,
		LastActivityAt:         timestamppb.New(record.LastActivityAt),
		AllocatedCpuMillicores: record.AllocatedCPUMillicores,
		AllocatedRamBytes:      record.AllocatedRAMBytes,
		Flavor:                 record.Flavor,
		PersistentShells:       record.PersistentShells,
		OwnerId:                record.OwnerID.String(),
		Preparation:            record.Preparation,
	}
	ownerKind, err := runtimeOwnerKindFromString(record.OwnerKind)
	if err != nil {
		return nil, err
	}
	protoWorkload.OwnerKind = ownerKind
	if record.OwnerKind == runtimeOwnerKindAgentInstance {
		agentInstanceID := record.OwnerID.String()
		protoWorkload.AgentInstanceId = &agentInstanceID
	}
	if record.ThreadID != uuid.Nil {
		protoWorkload.ThreadId = record.ThreadID.String()
	}
	if record.AgentID != uuid.Nil {
		agentID := record.AgentID.String()
		protoWorkload.AgentId = agentID
		protoWorkload.AgentClassId = &agentID
	}
	if record.InstanceID != nil {
		protoWorkload.InstanceId = record.InstanceID
	}
	if record.RemovedAt != nil {
		protoWorkload.RemovedAt = timestamppb.New(*record.RemovedAt)
	}
	if record.RemovalConfirmedAt != nil {
		protoWorkload.RemovalConfirmedAt = timestamppb.New(*record.RemovalConfirmedAt)
	}
	if record.LastMeteringAt != nil {
		protoWorkload.LastMeteringSampledAt = timestamppb.New(*record.LastMeteringAt)
	}
	if record.FailureReason != nil {
		failureReason, err := workloadFailureReasonFromString(*record.FailureReason)
		if err != nil {
			return nil, err
		}
		protoWorkload.FailureReason = &failureReason
	}
	if record.FailureMessage != nil {
		protoWorkload.FailureMessage = record.FailureMessage
	}
	return protoWorkload, nil
}

func toProtoWorkloadWithNames(record workloadRecord, agentName, ownerName, runnerName string) (*runnersv1.Workload, error) {
	workload, err := toProtoWorkload(record)
	if err != nil {
		return nil, err
	}
	workload.AgentName = agentName
	workload.RunnerName = runnerName
	if ownerName != "" {
		workload.OwnerName = &ownerName
	}
	return workload, nil
}

func toProtoWorkloadList(records []workloadRecord) ([]*runnersv1.Workload, error) {
	workloads := make([]*runnersv1.Workload, 0, len(records))
	for _, record := range records {
		workload, err := toProtoWorkload(record)
		if err != nil {
			return nil, err
		}
		workloads = append(workloads, workload)
	}
	return workloads, nil
}

func containersFromProto(containers []*runnersv1.Container) ([]containerRecord, error) {
	if len(containers) == 0 {
		return []containerRecord{}, nil
	}
	records := make([]containerRecord, len(containers))
	for i, container := range containers {
		if container == nil {
			return nil, fmt.Errorf("container %d is nil", i)
		}
		role, err := containerRoleToString(container.GetRole())
		if err != nil {
			return nil, err
		}
		statusValue, err := containerStatusToString(container.GetStatus())
		if err != nil {
			return nil, err
		}
		name := container.GetName()
		startedAt, err := containerTimestamp(container.GetStartedAt(), name, "started_at")
		if err != nil {
			return nil, err
		}
		finishedAt, err := containerTimestamp(container.GetFinishedAt(), name, "finished_at")
		if err != nil {
			return nil, err
		}
		var reason *string
		if container.Reason != nil {
			value := container.GetReason()
			reason = &value
		}
		var message *string
		if container.Message != nil {
			value := container.GetMessage()
			message = &value
		}
		var exitCode *int32
		if container.ExitCode != nil {
			value := container.GetExitCode()
			exitCode = &value
		}
		records[i] = containerRecord{
			ContainerID:  container.GetContainerId(),
			Name:         name,
			Role:         role,
			Image:        container.GetImage(),
			Status:       statusValue,
			Reason:       reason,
			Message:      message,
			ExitCode:     exitCode,
			RestartCount: container.GetRestartCount(),
			StartedAt:    startedAt,
			FinishedAt:   finishedAt,
		}
	}
	return records, nil
}

func containersToProto(records []containerRecord) ([]*runnersv1.Container, error) {
	if len(records) == 0 {
		return []*runnersv1.Container{}, nil
	}
	containers := make([]*runnersv1.Container, len(records))
	for i, record := range records {
		role, err := containerRoleFromString(record.Role)
		if err != nil {
			return nil, err
		}
		statusValue, err := containerStatusFromString(record.Status)
		if err != nil {
			return nil, err
		}
		containers[i] = &runnersv1.Container{
			ContainerId:  record.ContainerID,
			Name:         record.Name,
			Role:         role,
			Image:        record.Image,
			Status:       statusValue,
			Reason:       record.Reason,
			Message:      record.Message,
			ExitCode:     record.ExitCode,
			RestartCount: record.RestartCount,
			StartedAt:    timestampProto(record.StartedAt),
			FinishedAt:   timestampProto(record.FinishedAt),
		}
	}
	return containers, nil
}

func containersEqualByName(existing, updated []containerRecord) bool {
	if len(existing) != len(updated) {
		return false
	}
	byName := make(map[string]containerRecord, len(existing))
	for _, container := range existing {
		if _, ok := byName[container.Name]; ok {
			return false
		}
		byName[container.Name] = container
	}
	for _, container := range updated {
		current, ok := byName[container.Name]
		if !ok {
			return false
		}
		if !containerRecordEqual(current, container) {
			return false
		}
	}
	return true
}

func containerRecordEqual(left, right containerRecord) bool {
	if left.ContainerID != right.ContainerID ||
		left.Name != right.Name ||
		left.Role != right.Role ||
		left.Image != right.Image ||
		left.Status != right.Status ||
		left.RestartCount != right.RestartCount {
		return false
	}
	if !optionalStringEqual(left.Reason, right.Reason) || !optionalStringEqual(left.Message, right.Message) {
		return false
	}
	if !optionalInt32Equal(left.ExitCode, right.ExitCode) {
		return false
	}
	if !optionalTimeEqual(left.StartedAt, right.StartedAt) || !optionalTimeEqual(left.FinishedAt, right.FinishedAt) {
		return false
	}
	return true
}

func optionalStringEqual(left, right *string) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func optionalInt32Equal(left, right *int32) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func optionalTimeEqual(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Equal(*right)
}

func containerTimestamp(value *timestamppb.Timestamp, name, field string) (*time.Time, error) {
	if value == nil {
		return nil, nil
	}
	if err := value.CheckValid(); err != nil {
		if name == "" {
			return nil, fmt.Errorf("%s: %w", field, err)
		}
		return nil, fmt.Errorf("container %s %s: %w", name, field, err)
	}
	timestamp := value.AsTime()
	return &timestamp, nil
}

func timestampProto(value *time.Time) *timestamppb.Timestamp {
	if value == nil {
		return nil
	}
	return timestamppb.New(*value)
}

func isTerminalWorkloadStatus(status string) bool {
	return status == workloadStatusStopped || status == workloadStatusFailed
}

func workloadStatusToString(status runnersv1.WorkloadStatus) (string, error) {
	switch status {
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING:
		return workloadStatusStarting, nil
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING:
		return workloadStatusRunning, nil
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING:
		return workloadStatusStopping, nil
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED:
		return workloadStatusStopped, nil
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED:
		return workloadStatusFailed, nil
	default:
		return "", fmt.Errorf("invalid workload status: %s", status.String())
	}
}

func workloadAgentStateToString(state runnersv1.WorkloadAgentState) (string, error) {
	switch state {
	case runnersv1.WorkloadAgentState_WORKLOAD_AGENT_STATE_PROCESSING:
		return workloadAgentStateProcessing, nil
	case runnersv1.WorkloadAgentState_WORKLOAD_AGENT_STATE_IDLE:
		return workloadAgentStateIdle, nil
	default:
		return "", fmt.Errorf("invalid workload agent state: %s", state.String())
	}
}

func workloadFailureReasonToString(reason runnersv1.WorkloadFailureReason) (string, error) {
	switch reason {
	case runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_START_FAILED:
		return workloadFailureReasonStartFailed, nil
	case runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_IMAGE_PULL_FAILED:
		return workloadFailureReasonImagePullFailed, nil
	case runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CONFIG_INVALID:
		return workloadFailureReasonConfigInvalid, nil
	case runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CRASHLOOP:
		return workloadFailureReasonCrashloop, nil
	case runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_RUNTIME_LOST:
		return workloadFailureReasonRuntimeLost, nil
	default:
		return "", fmt.Errorf("invalid workload failure reason: %s", reason.String())
	}
}

func workloadStatusFromString(value string) (runnersv1.WorkloadStatus, error) {
	switch value {
	case workloadStatusStarting:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING, nil
	case workloadStatusRunning:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, nil
	case workloadStatusStopping:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING, nil
	case workloadStatusStopped:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED, nil
	case workloadStatusFailed:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED, nil
	default:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED, fmt.Errorf("invalid workload status: %s", value)
	}
}

func workloadAgentStateFromString(value string) (runnersv1.WorkloadAgentState, error) {
	switch value {
	case workloadAgentStateProcessing:
		return runnersv1.WorkloadAgentState_WORKLOAD_AGENT_STATE_PROCESSING, nil
	case workloadAgentStateIdle:
		return runnersv1.WorkloadAgentState_WORKLOAD_AGENT_STATE_IDLE, nil
	default:
		return runnersv1.WorkloadAgentState_WORKLOAD_AGENT_STATE_UNSPECIFIED, fmt.Errorf("invalid workload agent state: %s", value)
	}
}

func workloadFailureReasonFromString(value string) (runnersv1.WorkloadFailureReason, error) {
	switch value {
	case workloadFailureReasonStartFailed:
		return runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_START_FAILED, nil
	case workloadFailureReasonImagePullFailed:
		return runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_IMAGE_PULL_FAILED, nil
	case workloadFailureReasonConfigInvalid:
		return runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CONFIG_INVALID, nil
	case workloadFailureReasonCrashloop:
		return runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CRASHLOOP, nil
	case workloadFailureReasonRuntimeLost:
		return runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_RUNTIME_LOST, nil
	default:
		return runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_UNSPECIFIED, fmt.Errorf("invalid workload failure reason: %s", value)
	}
}

func workloadStatusesToStrings(statuses []runnersv1.WorkloadStatus) ([]string, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	values := make([]string, len(statuses))
	for i, statusValue := range statuses {
		value, err := workloadStatusToString(statusValue)
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	return values, nil
}

func containerRoleToString(role runnersv1.ContainerRole) (string, error) {
	switch role {
	case runnersv1.ContainerRole_CONTAINER_ROLE_MAIN:
		return containerRoleMain, nil
	case runnersv1.ContainerRole_CONTAINER_ROLE_SIDECAR:
		return containerRoleSidecar, nil
	case runnersv1.ContainerRole_CONTAINER_ROLE_INIT:
		return containerRoleInit, nil
	default:
		return "", fmt.Errorf("invalid container role: %s", role.String())
	}
}

func containerRoleFromString(value string) (runnersv1.ContainerRole, error) {
	switch value {
	case containerRoleMain:
		return runnersv1.ContainerRole_CONTAINER_ROLE_MAIN, nil
	case containerRoleSidecar:
		return runnersv1.ContainerRole_CONTAINER_ROLE_SIDECAR, nil
	case containerRoleInit:
		return runnersv1.ContainerRole_CONTAINER_ROLE_INIT, nil
	default:
		return runnersv1.ContainerRole_CONTAINER_ROLE_UNSPECIFIED, fmt.Errorf("invalid container role: %s", value)
	}
}

func containerStatusToString(status runnersv1.ContainerStatus) (string, error) {
	switch status {
	case runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING:
		return containerStatusRunning, nil
	case runnersv1.ContainerStatus_CONTAINER_STATUS_TERMINATED:
		return containerStatusTerminated, nil
	case runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING:
		return containerStatusWaiting, nil
	default:
		return "", fmt.Errorf("invalid container status: %s", status.String())
	}
}

func containerStatusFromString(value string) (runnersv1.ContainerStatus, error) {
	switch value {
	case containerStatusRunning:
		return runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING, nil
	case containerStatusTerminated:
		return runnersv1.ContainerStatus_CONTAINER_STATUS_TERMINATED, nil
	case containerStatusWaiting:
		return runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING, nil
	default:
		return runnersv1.ContainerStatus_CONTAINER_STATUS_UNSPECIFIED, fmt.Errorf("invalid container status: %s", value)
	}
}
