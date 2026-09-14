package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/big"
	"sort"
	"strings"
	"time"

	agentsv1 "github.com/agynio/runners/.gen/go/agynio/api/agents/v1"
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
	volumeStatusProvisioning = "provisioning"
	volumeStatusActive       = "active"
	volumeStatusDeprovision  = "deprovisioning"
	volumeStatusDeleted      = "deleted"
	volumeStatusFailed       = "failed"

	volumeColumns = `id, instance_id, volume_id, thread_id, runner_id, agent_id, organization_id, size_gb, status, removed_at, last_metering_sampled_at, owner_kind, owner_id, created_at, updated_at, lifecycle_revision, checked_lifecycle, bound_instance, removal_intent`
)

type volumeRecord struct {
	Meta              entityMeta
	InstanceID        *string
	VolumeID          uuid.UUID
	ThreadID          uuid.UUID
	RunnerID          uuid.UUID
	AgentID           uuid.UUID
	OrganizationID    uuid.UUID
	SizeGB            string
	Status            string
	RemovedAt         *time.Time
	LastMeteringAt    *time.Time
	OwnerKind         string
	OwnerID           uuid.UUID
	LifecycleRevision int64
	CheckedLifecycle  bool
	BoundInstance     *runnerv1.VolumeListItem
	RemovalIntent     *runnersv1.VolumeRemovalIntent
}

type volumeInsertInput struct {
	ID               uuid.UUID
	VolumeID         *uuid.UUID
	ThreadID         *uuid.UUID
	RunnerID         uuid.UUID
	AgentID          *uuid.UUID
	OrganizationID   uuid.UUID
	SizeGB           string
	Status           string
	OwnerKind        string
	OwnerID          uuid.UUID
	CheckedLifecycle bool
}

type volumeUpdateInput struct {
	ID             uuid.UUID
	Status         *string
	InstanceID     *string
	RemovedAt      *time.Time
	LastMeteringAt *time.Time
}

type volumeListFilter struct {
	OrganizationID     *uuid.UUID
	RunnerIDs          []uuid.UUID
	OwnerKinds         []string
	OwnerIDs           []uuid.UUID
	Statuses           []string
	AttachedKinds      []runnersv1.VolumeAttachmentFilterKind
	PendingSample      bool
	VolumeNameContains string
}

type volumeSortField int

const (
	volumeSortName volumeSortField = iota
	volumeSortSize
	volumeSortStatus
	volumeSortCreated
)

type volumeListSort struct {
	Field     volumeSortField
	Direction sortDirection
}

type volumeListItem struct {
	record      volumeRecord
	volumeName  string
	ownerName   string
	attachments []*runnersv1.Attachment
}

type volumeEnrichmentCache struct {
	volumeNames  map[uuid.UUID]string
	attachments  map[uuid.UUID][]*runnersv1.Attachment
	agentNames   map[uuid.UUID]string
	sandboxNames map[uuid.UUID]string
	mcpNames     map[uuid.UUID]string
}

func newVolumeEnrichmentCache() *volumeEnrichmentCache {
	return &volumeEnrichmentCache{
		volumeNames:  map[uuid.UUID]string{},
		attachments:  map[uuid.UUID][]*runnersv1.Attachment{},
		agentNames:   map[uuid.UUID]string{},
		sandboxNames: map[uuid.UUID]string{},
		mcpNames:     map[uuid.UUID]string{},
	}
}

func (s *Server) CreateVolume(ctx context.Context, req *runnersv1.CreateVolumeRequest) (*runnersv1.CreateVolumeResponse, error) {
	return s.createVolume(ctx, req, false)
}

func (s *Server) createVolume(ctx context.Context, req *runnersv1.CreateVolumeRequest, checked bool) (*runnersv1.CreateVolumeResponse, error) {
	input, err := parseVolumeCreate(req)
	if err != nil {
		return nil, err
	}
	input.CheckedLifecycle = checked
	volume, err := s.insertVolume(ctx, input)
	if err != nil {
		return nil, volumeLifecycleStatusError(err)
	}
	protoVolume, err := toProtoVolume(volume)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert volume: %v", err)
	}
	return &runnersv1.CreateVolumeResponse{Volume: protoVolume}, nil
}

func parseVolumeCreate(req *runnersv1.CreateVolumeRequest) (volumeInsertInput, error) {
	if req == nil {
		return volumeInsertInput{}, status.Error(codes.InvalidArgument, "volume required")
	}
	id, err := parseUUID(req.GetId())
	if err != nil {
		return volumeInsertInput{}, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}
	volumeValue := req.GetVolumeId()
	if req.VolumeDefinitionId != nil {
		volumeValue = req.GetVolumeDefinitionId()
	}
	volumeID, err := parseOptionalUUID(volumeValue)
	if err != nil {
		return volumeInsertInput{}, status.Errorf(codes.InvalidArgument, "volume_definition_id: %v", err)
	}
	threadID, err := parseOptionalUUID(req.GetThreadId())
	if err != nil {
		return volumeInsertInput{}, status.Errorf(codes.InvalidArgument, "thread_id: %v", err)
	}
	runnerID, err := parseUUID(req.GetRunnerId())
	if err != nil {
		return volumeInsertInput{}, status.Errorf(codes.InvalidArgument, "runner_id: %v", err)
	}
	agentValue := req.GetAgentId()
	if req.AgentClassId != nil {
		agentValue = req.GetAgentClassId()
	}
	agentID, err := parseOptionalUUID(agentValue)
	if err != nil {
		return volumeInsertInput{}, status.Errorf(codes.InvalidArgument, "agent_class_id: %v", err)
	}
	organizationID, err := parseUUID(req.GetOrganizationId())
	if err != nil {
		return volumeInsertInput{}, status.Errorf(codes.InvalidArgument, "organization_id: %v", err)
	}
	ownerKind, err := runtimeOwnerKindToString(req.GetOwnerKind())
	if err != nil {
		return volumeInsertInput{}, status.Errorf(codes.InvalidArgument, "owner_kind: %v", err)
	}
	ownerID, err := runtimeOwnerIDFromRequest(req.GetOwnerId(), req.AgentInstanceId, ownerKind)
	if err != nil {
		return volumeInsertInput{}, status.Errorf(codes.InvalidArgument, "owner_id: %v", err)
	}
	// Every disk is made from a definition an operator declared, whatever owns
	// it. The implicit sandbox workspace was the one exception and is gone.
	if volumeID == nil {
		return volumeInsertInput{}, status.Error(codes.InvalidArgument, "volume_definition_id: value is empty")
	}
	if ownerKind == runtimeOwnerKindAgentInstance {
		if threadID == nil {
			return volumeInsertInput{}, status.Error(codes.InvalidArgument, "thread_id: value is empty")
		}
		if agentID == nil {
			return volumeInsertInput{}, status.Error(codes.InvalidArgument, "agent_class_id: value is empty")
		}
	}

	statusValue, err := volumeStatusToString(req.GetStatus())
	if err != nil {
		return volumeInsertInput{}, status.Errorf(codes.InvalidArgument, "status: %v", err)
	}
	sizeGB := strings.TrimSpace(req.GetSizeGb())
	if sizeGB == "" {
		return volumeInsertInput{}, status.Error(codes.InvalidArgument, "size_gb must be provided")
	}

	return volumeInsertInput{
		ID:             id,
		VolumeID:       volumeID,
		ThreadID:       threadID,
		RunnerID:       runnerID,
		AgentID:        agentID,
		OrganizationID: organizationID,
		SizeGB:         sizeGB,
		Status:         statusValue,
		OwnerKind:      ownerKind,
		OwnerID:        *ownerID,
	}, nil
}

func (s *Server) UpdateVolume(ctx context.Context, req *runnersv1.UpdateVolumeRequest) (*runnersv1.UpdateVolumeResponse, error) {
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}

	var statusValue *string
	if req.Status != nil {
		value, err := volumeStatusToString(req.GetStatus())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "status: %v", err)
		}
		statusValue = &value
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

	if statusValue == nil && instanceID == nil && removedAt == nil && lastMeteringAt == nil {
		return nil, status.Error(codes.InvalidArgument, "at least one field must be provided")
	}

	var existingVolume *volumeRecord
	if s.notificationsClient != nil && (statusValue != nil || instanceID != nil || removedAt != nil) {
		volume, err := s.getVolumeByID(ctx, id)
		if err != nil {
			return nil, toStatusError(err)
		}
		existingVolume = &volume
	}

	volume, err := s.updateVolume(ctx, volumeUpdateInput{
		ID:             id,
		Status:         statusValue,
		InstanceID:     instanceID,
		RemovedAt:      removedAt,
		LastMeteringAt: lastMeteringAt,
	})
	if err != nil {
		return nil, volumeLifecycleStatusError(err)
	}
	if existingVolume != nil {
		statusChanged := statusValue != nil && *statusValue != existingVolume.Status
		instanceChanged := instanceID != nil && (existingVolume.InstanceID == nil || *instanceID != *existingVolume.InstanceID)
		removedChanged := removedAt != nil && (existingVolume.RemovedAt == nil || !existingVolume.RemovedAt.Equal(*removedAt))
		if statusChanged || instanceChanged || removedChanged {
			s.publishVolumeUpdateNotification(ctx, volume)
		}
	}
	protoVolume, err := toProtoVolume(volume)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert volume: %v", err)
	}
	return &runnersv1.UpdateVolumeResponse{Volume: protoVolume}, nil
}

func (s *Server) publishVolumeUpdateNotification(ctx context.Context, volume volumeRecord) {
	if s.notificationsClient == nil {
		return
	}
	payloadFields := map[string]any{
		"volume_id": volume.Meta.ID.String(),
		"status":    volume.Status,
	}
	payload, err := structpb.NewStruct(payloadFields)
	if err != nil {
		log.Printf("runners: build volume notification payload: %v", err)
		return
	}
	rooms := []string{
		fmt.Sprintf("organization:%s", volume.OrganizationID.String()),
		fmt.Sprintf("volume:%s", volume.Meta.ID.String()),
	}
	s.publishVolumeNotification(ctx, "volume.updated", rooms, payload)
}

func (s *Server) publishVolumeNotification(ctx context.Context, event string, rooms []string, payload *structpb.Struct) {
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

// GetVolume reads one volume on the same terms the list RPCs read many: an
// identified caller must be allowed to view its organization's volumes, while
// an internal caller holds no identity and is served. Requiring one here left
// the Orchestrator, which strips its identity for exactly these reads, unable
// to inspect a volume record it had just been told already exists -- so every
// retried start failed on the read rather than the write.
func (s *Server) GetVolume(ctx context.Context, req *runnersv1.GetVolumeRequest) (*runnersv1.GetVolumeResponse, error) {
	callerID, err := identityFromMetadataOptional(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}
	volume, err := s.getVolumeByID(ctx, id)
	if err != nil {
		return nil, toStatusError(err)
	}
	if callerID != nil {
		if err := s.requireRelation(ctx, *callerID, organizationViewVolumes, organizationObject(volume.OrganizationID)); err != nil {
			return nil, err
		}
	}
	cache := newVolumeEnrichmentCache()
	items, err := s.buildVolumeItems(ctx, []volumeRecord{volume}, cache, true)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert volume: %v", err)
	}
	protoVolume, err := toProtoVolumeWithEnrichment(items[0].record, items[0].volumeName, items[0].ownerName, items[0].attachments)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert volume: %v", err)
	}
	return &runnersv1.GetVolumeResponse{Volume: protoVolume}, nil
}

func (s *Server) ListVolumesByThread(ctx context.Context, req *runnersv1.ListVolumesByThreadRequest) (*runnersv1.ListVolumesByThreadResponse, error) {
	callerID, err := identityFromMetadataOptional(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	threadID, err := parseUUID(req.GetThreadId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "thread_id: %v", err)
	}
	volumes, nextToken, err := s.listVolumesByThread(ctx, threadID, req.GetPageSize(), req.GetPageToken())
	if err != nil {
		var invalidToken *InvalidPageTokenError
		if errors.As(err, &invalidToken) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", invalidToken.Err)
		}
		return nil, status.Errorf(codes.Internal, "list volumes: %v", err)
	}
	if callerID != nil {
		memberCache := map[uuid.UUID]bool{}
		filtered := make([]volumeRecord, 0, len(volumes))
		for _, volume := range volumes {
			allowed, err := s.memberAllowed(ctx, *callerID, volume.OrganizationID, memberCache)
			if err != nil {
				return nil, err
			}
			if allowed {
				filtered = append(filtered, volume)
			}
		}
		volumes = filtered
	}
	protoVolumes, err := toProtoVolumeList(volumes)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert volumes: %v", err)
	}
	return &runnersv1.ListVolumesByThreadResponse{Volumes: protoVolumes, NextPageToken: nextToken}, nil
}

func (s *Server) ListVolumesByAgentInstance(ctx context.Context, req *runnersv1.ListVolumesByAgentInstanceRequest) (*runnersv1.ListVolumesByAgentInstanceResponse, error) {
	callerID, err := identityFromMetadataOptional(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	agentInstanceID, err := parseUUID(req.GetAgentInstanceId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "agent_instance_id: %v", err)
	}
	statuses, err := volumeStatusesToStrings(req.GetStatuses())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "statuses: %v", err)
	}
	volumes, nextToken, err := s.listVolumesByAgentInstance(ctx, agentInstanceID, statuses, req.GetPageSize(), req.GetPageToken())
	if err != nil {
		var invalidToken *InvalidPageTokenError
		if errors.As(err, &invalidToken) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", invalidToken.Err)
		}
		return nil, status.Errorf(codes.Internal, "list volumes: %v", err)
	}
	if callerID != nil {
		memberCache := map[uuid.UUID]bool{}
		filtered := make([]volumeRecord, 0, len(volumes))
		for _, volume := range volumes {
			allowed, err := s.memberAllowed(ctx, *callerID, volume.OrganizationID, memberCache)
			if err != nil {
				return nil, err
			}
			if allowed {
				filtered = append(filtered, volume)
			}
		}
		volumes = filtered
	}
	protoVolumes, err := toProtoVolumeList(volumes)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert volumes: %v", err)
	}
	return &runnersv1.ListVolumesByAgentInstanceResponse{Volumes: protoVolumes, NextPageToken: nextToken}, nil
}

func (s *Server) ListVolumes(ctx context.Context, req *runnersv1.ListVolumesRequest) (*runnersv1.ListVolumesResponse, error) {
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
		if err := s.requireRelation(ctx, *callerID, organizationViewVolumes, organizationObject(*organizationID)); err != nil {
			return nil, err
		}
	} else if orgValue != "" {
		parsed, err := parseUUID(orgValue)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "organization_id: %v", err)
		}
		organizationID = &parsed
	}

	filter := volumeListFilter{OrganizationID: organizationID}
	pendingSampleSet := false
	if req.Filter != nil {
		filter.RunnerIDs = make([]uuid.UUID, 0, len(req.Filter.RunnerIdIn))
		for _, runnerValue := range req.Filter.RunnerIdIn {
			parsed, err := parseUUID(runnerValue)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "filter.runner_id_in: %v", err)
			}
			filter.RunnerIDs = append(filter.RunnerIDs, parsed)
		}
		filter.AttachedKinds = make([]runnersv1.VolumeAttachmentFilterKind, 0, len(req.Filter.AttachedToKindIn))
		for _, kind := range req.Filter.AttachedToKindIn {
			switch kind {
			case runnersv1.VolumeAttachmentFilterKind_VOLUME_ATTACHMENT_FILTER_KIND_UNSPECIFIED:
				return nil, status.Error(codes.InvalidArgument, "filter.attached_to_kind_in: unspecified kind")
			case runnersv1.VolumeAttachmentFilterKind_VOLUME_ATTACHMENT_FILTER_KIND_AGENT,
				runnersv1.VolumeAttachmentFilterKind_VOLUME_ATTACHMENT_FILTER_KIND_MCP,
				runnersv1.VolumeAttachmentFilterKind_VOLUME_ATTACHMENT_FILTER_KIND_UNATTACHED:
				filter.AttachedKinds = append(filter.AttachedKinds, kind)
			default:
				return nil, status.Errorf(codes.InvalidArgument, "filter.attached_to_kind_in: %s", kind.String())
			}
		}
		if req.Filter.PendingSample != nil {
			filter.PendingSample = req.Filter.GetPendingSample()
			pendingSampleSet = true
		}
		if req.Filter.VolumeNameSubstring != nil {
			filter.VolumeNameContains = strings.TrimSpace(req.Filter.GetVolumeNameSubstring())
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

	statusFilters := make([]runnersv1.VolumeStatus, 0)
	if req.Filter != nil {
		statusFilters = append(statusFilters, req.Filter.StatusIn...)
	}
	statusFilters = append(statusFilters, req.GetStatuses()...)
	statuses, err := volumeStatusesToStrings(statusFilters)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "statuses: %v", err)
	}
	filter.Statuses = statuses

	sort, err := parseVolumeSort(req.GetSort())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "sort: %v", err)
	}

	cache := newVolumeEnrichmentCache()
	var (
		pageItems []volumeListItem
		nextToken string
	)
	if sort.Field == volumeSortName {
		pageItems, nextToken, err = s.listVolumesByName(ctx, filter, sort, req.GetPageSize(), req.GetPageToken(), cache)
	} else {
		pageItems, nextToken, err = s.listVolumesPaged(ctx, filter, sort, req.GetPageSize(), req.GetPageToken(), cache)
	}
	if err != nil {
		var invalidToken *InvalidPageTokenError
		if errors.As(err, &invalidToken) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", invalidToken.Err)
		}
		return nil, status.Errorf(codes.Internal, "list volumes: %v", err)
	}
	protoVolumes := make([]*runnersv1.Volume, 0, len(pageItems))
	for _, item := range pageItems {
		volume, err := toProtoVolumeWithEnrichment(item.record, item.volumeName, item.ownerName, item.attachments)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "convert volume: %v", err)
		}
		protoVolumes = append(protoVolumes, volume)
	}
	return &runnersv1.ListVolumesResponse{Volumes: protoVolumes, NextPageToken: nextToken}, nil
}

func (s *Server) listVolumesByName(ctx context.Context, filter volumeListFilter, sort volumeListSort, pageSize int32, pageToken string, cache *volumeEnrichmentCache) ([]volumeListItem, string, error) {
	limit := normalizePageSize(pageSize)
	volumeIDs, err := s.listVolumeIDs(ctx, filter)
	if err != nil {
		return nil, "", err
	}
	if len(volumeIDs) == 0 {
		return s.listVolumesPaged(ctx, filter, sort, pageSize, pageToken, cache)
	}
	if err := s.ensureVolumeNames(ctx, cache, volumeIDs); err != nil {
		return nil, "", err
	}
	volumeNames := make(map[uuid.UUID]string, len(volumeIDs))
	for _, volumeID := range volumeIDs {
		name, ok := cache.volumeNames[volumeID]
		if !ok {
			return nil, "", fmt.Errorf("volume name missing for %s", volumeID)
		}
		volumeNames[volumeID] = name
	}

	nameFilter := strings.ToLower(strings.TrimSpace(filter.VolumeNameContains))
	kindFilter := map[runnersv1.VolumeAttachmentFilterKind]bool{}
	for _, kind := range filter.AttachedKinds {
		kindFilter[kind] = true
	}

	collected := make([]volumeListItem, 0, limit+1)
	currentToken := pageToken
	for {
		records, nextToken, err := s.listVolumesByNamePage(ctx, filter, sort, pageSize, currentToken, volumeNames)
		if err != nil {
			return nil, "", err
		}
		if len(records) == 0 {
			break
		}
		items, err := s.buildVolumeItems(ctx, records, cache, false)
		if err != nil {
			return nil, "", err
		}
		for _, item := range items {
			if nameFilter != "" && !strings.Contains(strings.ToLower(item.volumeName), nameFilter) {
				continue
			}
			if item.record.VolumeID != uuid.Nil {
				if err := s.ensureVolumeAttachments(ctx, cache, []uuid.UUID{item.record.VolumeID}); err != nil {
					return nil, "", err
				}
				item.attachments = cache.attachments[item.record.VolumeID]
			}
			if len(kindFilter) > 0 && !matchesVolumeAttachmentKinds(item.attachments, kindFilter) {
				continue
			}
			collected = append(collected, item)
			if len(collected) == int(limit)+1 {
				break
			}
		}
		if len(collected) == int(limit)+1 {
			break
		}
		if nextToken == "" {
			break
		}
		currentToken = nextToken
	}
	if len(collected) <= int(limit) {
		return collected, "", nil
	}
	page := collected[:limit]
	lastItem := page[len(page)-1]
	primary, err := volumePrimaryValue(lastItem, sort.Field)
	if err != nil {
		return nil, "", err
	}
	nextToken, err := encodeListCursor(primary, lastItem.record.Meta.ID)
	if err != nil {
		return nil, "", err
	}
	return page, nextToken, nil
}

func (s *Server) listVolumesPaged(ctx context.Context, filter volumeListFilter, sort volumeListSort, pageSize int32, pageToken string, cache *volumeEnrichmentCache) ([]volumeListItem, string, error) {
	limit := normalizePageSize(pageSize)
	collected := make([]volumeListItem, 0, limit+1)
	currentToken := pageToken
	kindFilter := map[runnersv1.VolumeAttachmentFilterKind]bool{}
	for _, kind := range filter.AttachedKinds {
		kindFilter[kind] = true
	}

	for {
		records, nextToken, err := s.listVolumesPage(ctx, filter, sort, pageSize, currentToken)
		if err != nil {
			return nil, "", err
		}
		if len(records) == 0 {
			break
		}
		items, err := s.buildVolumeItems(ctx, records, cache, false)
		if err != nil {
			return nil, "", err
		}
		items = filterVolumeItemsByName(items, filter.VolumeNameContains)
		for _, item := range items {
			if item.record.VolumeID != uuid.Nil {
				if err := s.ensureVolumeAttachments(ctx, cache, []uuid.UUID{item.record.VolumeID}); err != nil {
					return nil, "", err
				}
				item.attachments = cache.attachments[item.record.VolumeID]
			}
			if len(kindFilter) > 0 && !matchesVolumeAttachmentKinds(item.attachments, kindFilter) {
				continue
			}
			collected = append(collected, item)
			if len(collected) == int(limit)+1 {
				break
			}
		}
		if len(collected) == int(limit)+1 {
			break
		}
		if nextToken == "" {
			break
		}
		currentToken = nextToken
	}
	if len(collected) <= int(limit) {
		return collected, "", nil
	}
	page := collected[:limit]
	lastItem := page[len(page)-1]
	primary, err := volumePrimaryValue(lastItem, sort.Field)
	if err != nil {
		return nil, "", err
	}
	nextToken, err := encodeListCursor(primary, lastItem.record.Meta.ID)
	if err != nil {
		return nil, "", err
	}
	return page, nextToken, nil
}

func (s *Server) BatchUpdateVolumeSampledAt(ctx context.Context, req *runnersv1.BatchUpdateVolumeSampledAtRequest) (*runnersv1.BatchUpdateVolumeSampledAtResponse, error) {
	entries := req.GetEntries()
	if len(entries) == 0 {
		return &runnersv1.BatchUpdateVolumeSampledAtResponse{}, nil
	}
	updates, err := parseSampledAtEntries(entries)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.batchUpdateVolumeSampledAt(ctx, updates); err != nil {
		return nil, status.Errorf(codes.Internal, "batch update volumes: %v", err)
	}
	return &runnersv1.BatchUpdateVolumeSampledAtResponse{}, nil
}

func (s *Server) insertVolume(ctx context.Context, input volumeInsertInput) (volumeRecord, error) {
	row := s.pool.QueryRow(ctx,
		fmt.Sprintf(`INSERT INTO volumes (id, volume_id, thread_id, runner_id, agent_id, organization_id, size_gb, status, owner_kind, owner_id, checked_lifecycle)
	    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	    RETURNING %s`, volumeColumns),
		input.ID,
		nullableUUIDValue(input.VolumeID),
		nullableUUIDValue(input.ThreadID),
		input.RunnerID,
		nullableUUIDValue(input.AgentID),
		input.OrganizationID,
		input.SizeGB,
		input.Status,
		input.OwnerKind,
		input.OwnerID,
		input.CheckedLifecycle,
	)
	volume, err := scanVolume(row)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.Code == "23505" {
				if input.CheckedLifecycle {
					return volumeRecord{}, AlreadyExists("volume")
				}
				return s.reopenClosedVolume(ctx, input)
			}
			if pgErr.Code == "23503" {
				return volumeRecord{}, NotFound("runner")
			}
		}
		return volumeRecord{}, err
	}
	volume.OwnerKind = input.OwnerKind
	volume.OwnerID = input.OwnerID
	return volume, nil
}

// reopenClosedVolume gives a create that collided with a closed row a fresh
// provisioning generation: same disk slot and owner, metering resumed from now.
// Match identity in the update itself so concurrent creates cannot take over a
// closed slot. Open rows and identity mismatches keep the create conflict.
func (s *Server) reopenClosedVolume(ctx context.Context, input volumeInsertInput) (volumeRecord, error) {
	row := s.pool.QueryRow(ctx,
		fmt.Sprintf(`UPDATE volumes
	    SET size_gb = $7, status = $8,
	        removed_at = NULL, instance_id = NULL, last_metering_sampled_at = NOW(), updated_at = NOW()
	    WHERE id = $1 AND NOT checked_lifecycle AND status IN ('%s', '%s')
	        AND volume_id = $2 AND thread_id IS NOT DISTINCT FROM $3
	        AND runner_id = $4 AND agent_id IS NOT DISTINCT FROM $5
	        AND organization_id = $6 AND owner_kind = $9 AND owner_id = $10
	    RETURNING %s`, volumeStatusDeleted, volumeStatusFailed, volumeColumns),
		input.ID,
		nullableUUIDValue(input.VolumeID),
		nullableUUIDValue(input.ThreadID),
		input.RunnerID,
		nullableUUIDValue(input.AgentID),
		input.OrganizationID,
		input.SizeGB,
		input.Status,
		input.OwnerKind,
		input.OwnerID,
	)
	volume, err := scanVolume(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return volumeRecord{}, AlreadyExists("volume")
		}
		return volumeRecord{}, err
	}
	return volume, nil
}

func isClosedVolumeStatus(status string) bool {
	return status == volumeStatusDeleted || status == volumeStatusFailed
}

func (s *Server) updateVolume(ctx context.Context, input volumeUpdateInput) (volumeRecord, error) {
	clauses := make([]string, 0, 5)
	args := make([]any, 0, 5)

	if input.Status != nil {
		addUpdateClause(&clauses, &args, "status", *input.Status)
	}
	if input.InstanceID != nil {
		addUpdateClause(&clauses, &args, "instance_id", *input.InstanceID)
	}
	if input.RemovedAt != nil {
		addUpdateClause(&clauses, &args, "removed_at", *input.RemovedAt)
	}
	// A closed status always carries removed_at, whoever wrote it.
	if input.RemovedAt == nil && input.Status != nil && isClosedVolumeStatus(*input.Status) {
		clauses = append(clauses, "removed_at = COALESCE(removed_at, NOW())")
	}
	if input.LastMeteringAt != nil {
		addUpdateClause(&clauses, &args, "last_metering_sampled_at", *input.LastMeteringAt)
	}
	query, args := buildUpdateQuery("volumes", volumeColumns, clauses, args, input.ID)
	row := s.pool.QueryRow(ctx, query, args...)
	volume, err := scanVolume(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return volumeRecord{}, NotFound("volume")
		}
		return volumeRecord{}, err
	}
	return volume, nil
}

func (s *Server) getVolumeByID(ctx context.Context, id uuid.UUID) (volumeRecord, error) {
	row := s.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT %s FROM volumes WHERE id = $1`, volumeColumns),
		id,
	)
	volume, err := scanVolume(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return volumeRecord{}, NotFound("volume")
		}
		return volumeRecord{}, err
	}
	return volume, nil
}

func (s *Server) listVolumesByThread(ctx context.Context, threadID uuid.UUID, pageSize int32, pageToken string) ([]volumeRecord, string, error) {
	clauses := []string{"thread_id = $1"}
	args := []any{threadID}
	return s.listVolumesByClauses(ctx, clauses, args, nil, pageSize, pageToken)
}

func (s *Server) listVolumesByAgentInstance(ctx context.Context, agentInstanceID uuid.UUID, statuses []string, pageSize int32, pageToken string) ([]volumeRecord, string, error) {
	clauses := []string{"owner_kind = $1", "owner_id = $2"}
	args := []any{runtimeOwnerKindAgentInstance, agentInstanceID}
	return s.listVolumesByClauses(ctx, clauses, args, statuses, pageSize, pageToken)
}

func (s *Server) listVolumesByClauses(ctx context.Context, clauses []string, args []any, statuses []string, pageSize int32, pageToken string) ([]volumeRecord, string, error) {
	limit := normalizePageSize(pageSize)
	if len(statuses) > 0 {
		clauses = append(clauses, fmt.Sprintf("status = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[string](statuses))
	}

	if pageToken != "" {
		afterID, err := decodePageToken(pageToken)
		if err != nil {
			return nil, "", InvalidPageToken(err)
		}
		clauses = append(clauses, fmt.Sprintf("id > $%d", len(args)+1))
		args = append(args, afterID)
	}

	query := strings.Builder{}
	query.WriteString(fmt.Sprintf("SELECT %s FROM volumes", volumeColumns))
	query.WriteString(" WHERE ")
	query.WriteString(strings.Join(clauses, " AND "))
	query.WriteString(fmt.Sprintf(" ORDER BY id ASC LIMIT $%d", len(args)+1))
	args = append(args, int(limit)+1)

	rows, err := s.pool.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	volumes := make([]volumeRecord, 0, limit)
	var (
		lastID  uuid.UUID
		hasMore bool
	)
	for rows.Next() {
		if int32(len(volumes)) == limit {
			hasMore = true
			break
		}
		volume, err := scanVolume(rows)
		if err != nil {
			return nil, "", err
		}
		volumes = append(volumes, volume)
		lastID = volume.Meta.ID
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	nextToken := ""
	if hasMore {
		nextToken = encodePageToken(lastID)
	}
	return volumes, nextToken, nil
}

func parseVolumeSort(sort *runnersv1.ListVolumesSort) (volumeListSort, error) {
	field := volumeSortName
	if sort != nil {
		switch sort.GetField() {
		case runnersv1.ListVolumesSortField_LIST_VOLUMES_SORT_FIELD_UNSPECIFIED:
			field = volumeSortName
		case runnersv1.ListVolumesSortField_LIST_VOLUMES_SORT_FIELD_NAME:
			field = volumeSortName
		case runnersv1.ListVolumesSortField_LIST_VOLUMES_SORT_FIELD_SIZE:
			field = volumeSortSize
		case runnersv1.ListVolumesSortField_LIST_VOLUMES_SORT_FIELD_STATUS:
			field = volumeSortStatus
		case runnersv1.ListVolumesSortField_LIST_VOLUMES_SORT_FIELD_CREATED:
			field = volumeSortCreated
		default:
			return volumeListSort{}, fmt.Errorf("invalid sort field: %s", sort.GetField().String())
		}
	}
	defaultDirection := sortAsc
	if sort == nil {
		return volumeListSort{Field: field, Direction: defaultDirection}, nil
	}
	direction, err := parseSortDirection(sort.GetDirection(), defaultDirection)
	if err != nil {
		return volumeListSort{}, err
	}
	return volumeListSort{Field: field, Direction: direction}, nil
}

func volumeSortColumn(field volumeSortField) (string, error) {
	switch field {
	case volumeSortName:
		return "COALESCE(volumes.volume_id::text, '')", nil
	case volumeSortSize:
		return "volumes.size_gb::numeric", nil
	case volumeSortStatus:
		return "volumes.status", nil
	case volumeSortCreated:
		return "volumes.created_at", nil
	default:
		return "", errors.New("invalid sort field")
	}
}

func volumeCursorCast(field volumeSortField) string {
	if field == volumeSortSize {
		return "::numeric"
	}
	return ""
}

func buildVolumeNameSortExpr(volumeNames map[uuid.UUID]string, startIndex int) (string, []any, error) {
	if len(volumeNames) == 0 {
		return "COALESCE(volumes.volume_id::text, '')", nil, nil
	}
	ids := make([]uuid.UUID, 0, len(volumeNames))
	for id := range volumeNames {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].String() < ids[j].String()
	})

	parts := make([]string, 0, len(ids))
	args := make([]any, 0, len(ids)*2)
	idx := startIndex
	for _, id := range ids {
		name := strings.ToLower(strings.TrimSpace(volumeNames[id]))
		parts = append(parts, fmt.Sprintf("WHEN $%d THEN $%d", idx, idx+1))
		args = append(args, id, name)
		idx += 2
	}
	return fmt.Sprintf("COALESCE(CASE volumes.volume_id %s END, '')", strings.Join(parts, " ")), args, nil
}

func parseVolumeSize(value string) (*big.Rat, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil, errors.New("size_gb is empty")
	}
	size, ok := new(big.Rat).SetString(trimmed)
	if !ok {
		return nil, fmt.Errorf("invalid size_gb %q", value)
	}
	return size, nil
}

func volumeCursorPrimary(field volumeSortField, cursor listCursor) (any, error) {
	switch field {
	case volumeSortName:
		return strings.ToLower(strings.TrimSpace(cursor.Primary)), nil
	case volumeSortSize:
		primary := strings.TrimSpace(cursor.Primary)
		if primary == "" {
			return nil, errors.New("cursor primary is empty")
		}
		if _, err := parseVolumeSize(primary); err != nil {
			return nil, err
		}
		return primary, nil
	case volumeSortStatus:
		if strings.TrimSpace(cursor.Primary) == "" {
			return nil, errors.New("cursor primary is empty")
		}
		return cursor.Primary, nil
	case volumeSortCreated:
		createdAt, err := time.Parse(time.RFC3339Nano, cursor.Primary)
		if err != nil {
			return nil, fmt.Errorf("parse created_at: %w", err)
		}
		return createdAt, nil
	default:
		return nil, errors.New("invalid sort field")
	}
}

func volumePrimaryValue(item volumeListItem, field volumeSortField) (string, error) {
	switch field {
	case volumeSortName:
		return strings.ToLower(strings.TrimSpace(item.volumeName)), nil
	case volumeSortSize:
		value := strings.TrimSpace(item.record.SizeGB)
		if _, err := parseVolumeSize(value); err != nil {
			return "", err
		}
		return value, nil
	case volumeSortStatus:
		return item.record.Status, nil
	case volumeSortCreated:
		return item.record.Meta.CreatedAt.UTC().Format(time.RFC3339Nano), nil
	default:
		return "", errors.New("invalid sort field")
	}
}

func (s *Server) listVolumeIDs(ctx context.Context, filter volumeListFilter) ([]uuid.UUID, error) {
	clauses := []string{}
	args := []any{}
	if filter.OrganizationID != nil {
		clauses = append(clauses, fmt.Sprintf("volumes.organization_id = $%d", len(args)+1))
		args = append(args, *filter.OrganizationID)
	}

	if len(filter.RunnerIDs) > 0 {
		clauses = append(clauses, fmt.Sprintf("volumes.runner_id = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[uuid.UUID](filter.RunnerIDs))
	}
	if len(filter.OwnerKinds) > 0 {
		clauses = append(clauses, fmt.Sprintf("volumes.owner_kind = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[string](filter.OwnerKinds))
	}
	if len(filter.OwnerIDs) > 0 {
		clauses = append(clauses, fmt.Sprintf("volumes.owner_id = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[uuid.UUID](filter.OwnerIDs))
	}
	if len(filter.Statuses) > 0 {
		clauses = append(clauses, fmt.Sprintf("volumes.status = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[string](filter.Statuses))
	}
	if filter.PendingSample {
		clauses = append(clauses, pendingSampleClause)
	}

	query := strings.Builder{}
	query.WriteString("SELECT DISTINCT volume_id FROM volumes")
	if len(clauses) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(clauses, " AND "))
	}

	rows, err := s.pool.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	volumeIDs := []uuid.UUID{}
	for rows.Next() {
		var volumeID any
		if err := rows.Scan(&volumeID); err != nil {
			return nil, err
		}
		parsed, err := scanNullableUUID(volumeID)
		if err != nil {
			return nil, err
		}
		if parsed != nil {
			volumeIDs = append(volumeIDs, *parsed)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return volumeIDs, nil
}

func addVolumeCursorClause(clauses *[]string, args *[]any, column string, direction sortDirection, primary any, id uuid.UUID, cast string) {
	operator := ">"
	if direction == sortDesc {
		operator = "<"
	}
	clause := fmt.Sprintf("(%s %s $%d%s OR (%s = $%d%s AND volumes.id > $%d))", column, operator, len(*args)+1, cast, column, len(*args)+1, cast, len(*args)+2)
	*clauses = append(*clauses, clause)
	*args = append(*args, primary, id)
}

func (s *Server) listVolumesPage(ctx context.Context, filter volumeListFilter, sort volumeListSort, pageSize int32, pageToken string) ([]volumeRecord, string, error) {
	limit := normalizePageSize(pageSize)
	clauses := []string{}
	args := []any{}
	if filter.OrganizationID != nil {
		clauses = append(clauses, fmt.Sprintf("volumes.organization_id = $%d", len(args)+1))
		args = append(args, *filter.OrganizationID)
	}

	if len(filter.RunnerIDs) > 0 {
		clauses = append(clauses, fmt.Sprintf("volumes.runner_id = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[uuid.UUID](filter.RunnerIDs))
	}
	if len(filter.OwnerKinds) > 0 {
		clauses = append(clauses, fmt.Sprintf("volumes.owner_kind = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[string](filter.OwnerKinds))
	}
	if len(filter.OwnerIDs) > 0 {
		clauses = append(clauses, fmt.Sprintf("volumes.owner_id = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[uuid.UUID](filter.OwnerIDs))
	}
	if len(filter.Statuses) > 0 {
		clauses = append(clauses, fmt.Sprintf("volumes.status = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[string](filter.Statuses))
	}
	if filter.PendingSample {
		clauses = append(clauses, pendingSampleClause)
	}

	sortColumn, err := volumeSortColumn(sort.Field)
	if err != nil {
		return nil, "", err
	}

	if pageToken != "" {
		cursor, cursorID, err := decodeListCursor(pageToken)
		if err != nil {
			return nil, "", InvalidPageToken(err)
		}
		primaryValue, err := volumeCursorPrimary(sort.Field, cursor)
		if err != nil {
			return nil, "", InvalidPageToken(err)
		}
		addVolumeCursorClause(&clauses, &args, sortColumn, sort.Direction, primaryValue, cursorID, volumeCursorCast(sort.Field))
	}

	query := strings.Builder{}
	query.WriteString(fmt.Sprintf("SELECT %s FROM volumes", volumeColumns))
	if len(clauses) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(clauses, " AND "))
	}
	query.WriteString(fmt.Sprintf(" ORDER BY %s %s, volumes.id ASC LIMIT $%d", sortColumn, sort.Direction, len(args)+1))
	args = append(args, int(limit)+1)

	rows, err := s.pool.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	volumes := make([]volumeRecord, 0, limit)
	var (
		lastRecord volumeRecord
		hasMore    bool
	)
	for rows.Next() {
		if int32(len(volumes)) == limit {
			hasMore = true
			break
		}
		volume, err := scanVolume(rows)
		if err != nil {
			return nil, "", err
		}
		volumes = append(volumes, volume)
		lastRecord = volume
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	nextToken := ""
	if hasMore {
		primary, err := volumePrimaryValue(volumeListItem{record: lastRecord}, sort.Field)
		if err != nil {
			return nil, "", err
		}
		nextToken, err = encodeListCursor(primary, lastRecord.Meta.ID)
		if err != nil {
			return nil, "", err
		}
	}
	return volumes, nextToken, nil
}

func (s *Server) listVolumesByNamePage(ctx context.Context, filter volumeListFilter, sort volumeListSort, pageSize int32, pageToken string, volumeNames map[uuid.UUID]string) ([]volumeRecord, string, error) {
	limit := normalizePageSize(pageSize)
	clauses := []string{}
	args := []any{}
	if filter.OrganizationID != nil {
		clauses = append(clauses, fmt.Sprintf("volumes.organization_id = $%d", len(args)+1))
		args = append(args, *filter.OrganizationID)
	}

	if len(filter.RunnerIDs) > 0 {
		clauses = append(clauses, fmt.Sprintf("volumes.runner_id = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[uuid.UUID](filter.RunnerIDs))
	}
	if len(filter.OwnerKinds) > 0 {
		clauses = append(clauses, fmt.Sprintf("volumes.owner_kind = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[string](filter.OwnerKinds))
	}
	if len(filter.OwnerIDs) > 0 {
		clauses = append(clauses, fmt.Sprintf("volumes.owner_id = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[uuid.UUID](filter.OwnerIDs))
	}
	if len(filter.Statuses) > 0 {
		clauses = append(clauses, fmt.Sprintf("volumes.status = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[string](filter.Statuses))
	}
	if filter.PendingSample {
		clauses = append(clauses, pendingSampleClause)
	}

	sortColumn, sortArgs, err := buildVolumeNameSortExpr(volumeNames, len(args)+1)
	if err != nil {
		return nil, "", err
	}
	args = append(args, sortArgs...)

	if pageToken != "" {
		cursor, cursorID, err := decodeListCursor(pageToken)
		if err != nil {
			return nil, "", InvalidPageToken(err)
		}
		primaryValue, err := volumeCursorPrimary(sort.Field, cursor)
		if err != nil {
			return nil, "", InvalidPageToken(err)
		}
		addVolumeCursorClause(&clauses, &args, sortColumn, sort.Direction, primaryValue, cursorID, "")
	}

	query := strings.Builder{}
	query.WriteString(fmt.Sprintf("SELECT %s FROM volumes", volumeColumns))
	if len(clauses) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(clauses, " AND "))
	}
	query.WriteString(fmt.Sprintf(" ORDER BY %s %s, volumes.id ASC LIMIT $%d", sortColumn, sort.Direction, len(args)+1))
	args = append(args, int(limit)+1)

	rows, err := s.pool.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	volumes := make([]volumeRecord, 0, limit)
	var (
		lastRecord volumeRecord
		hasMore    bool
	)
	for rows.Next() {
		if int32(len(volumes)) == limit {
			hasMore = true
			break
		}
		volume, err := scanVolume(rows)
		if err != nil {
			return nil, "", err
		}
		volumes = append(volumes, volume)
		lastRecord = volume
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	nextToken := ""
	if hasMore {
		name := ""
		if lastRecord.VolumeID != uuid.Nil {
			resolvedName, ok := volumeNames[lastRecord.VolumeID]
			if !ok {
				return nil, "", fmt.Errorf("volume name missing for %s", lastRecord.VolumeID)
			}
			name = resolvedName
		}
		primary, err := volumePrimaryValue(volumeListItem{record: lastRecord, volumeName: name}, sort.Field)
		if err != nil {
			return nil, "", err
		}
		nextToken, err = encodeListCursor(primary, lastRecord.Meta.ID)
		if err != nil {
			return nil, "", err
		}
	}
	return volumes, nextToken, nil
}

// volumeTargets reports what mounts a disk. A volume used to reach a workload
// through a separate attachment record; it now carries its own target, so the
// definition is the only thing to read.
func (s *Server) volumeTargets(ctx context.Context, volumeID uuid.UUID) ([]*agentsv1.Volume, error) {
	if s.agentsClient == nil {
		return nil, errors.New("agents client not configured")
	}
	resp, err := s.agentsClient.GetVolume(outgoingContext(ctx), &agentsv1.GetVolumeRequest{Id: volumeID.String()})
	if err != nil {
		if isNotFoundGrpcError(err) {
			return nil, nil
		}
		return nil, err
	}
	if resp.GetVolume() == nil {
		return nil, nil
	}
	return []*agentsv1.Volume{resp.GetVolume()}, nil
}

func (s *Server) ensureVolumeNames(ctx context.Context, cache *volumeEnrichmentCache, volumeIDs []uuid.UUID) error {
	missing := []uuid.UUID{}
	for _, volumeID := range uniqueUUIDs(volumeIDs) {
		if _, ok := cache.volumeNames[volumeID]; ok {
			continue
		}
		missing = append(missing, volumeID)
	}
	if len(missing) == 0 {
		return nil
	}
	resolved, err := s.resolveVolumeNames(ctx, missing)
	if err != nil {
		return err
	}
	for id, name := range resolved {
		cache.volumeNames[id] = name
	}
	return nil
}

func (s *Server) ensureAgentNames(ctx context.Context, cache *volumeEnrichmentCache, agentIDs []uuid.UUID) error {
	missing := []uuid.UUID{}
	for _, agentID := range uniqueUUIDs(agentIDs) {
		if _, ok := cache.agentNames[agentID]; ok {
			continue
		}
		missing = append(missing, agentID)
	}
	if len(missing) == 0 {
		return nil
	}
	resolved, err := s.resolveAgentNames(ctx, missing)
	if err != nil {
		return err
	}
	for id, name := range resolved {
		cache.agentNames[id] = name
	}
	return nil
}

func (s *Server) ensureSandboxNames(ctx context.Context, cache *volumeEnrichmentCache, sandboxIDs []uuid.UUID) error {
	missing := []uuid.UUID{}
	for _, sandboxID := range uniqueUUIDs(sandboxIDs) {
		if _, ok := cache.sandboxNames[sandboxID]; ok {
			continue
		}
		missing = append(missing, sandboxID)
	}
	if len(missing) == 0 {
		return nil
	}
	resolved, err := s.resolveSandboxNames(ctx, missing)
	if err != nil {
		return err
	}
	for id, name := range resolved {
		cache.sandboxNames[id] = name
	}
	return nil
}

func (s *Server) ensureMcpNames(ctx context.Context, cache *volumeEnrichmentCache, mcpIDs []uuid.UUID) error {
	for _, mcpID := range uniqueUUIDs(mcpIDs) {
		if _, ok := cache.mcpNames[mcpID]; ok {
			continue
		}
		name, err := s.resolveMcpName(ctx, mcpID)
		if err != nil {
			return err
		}
		cache.mcpNames[mcpID] = name
	}
	return nil
}

func (s *Server) ensureVolumeAttachments(ctx context.Context, cache *volumeEnrichmentCache, volumeIDs []uuid.UUID) error {
	missing := []uuid.UUID{}
	for _, volumeID := range uniqueUUIDs(volumeIDs) {
		if _, ok := cache.attachments[volumeID]; ok {
			continue
		}
		missing = append(missing, volumeID)
	}
	if len(missing) == 0 {
		return nil
	}

	attachmentsByVolume := make(map[uuid.UUID][]*agentsv1.Volume, len(missing))
	attachmentAgentIDs := []uuid.UUID{}
	mcpIDs := []uuid.UUID{}

	for _, volumeID := range missing {
		attachments, err := s.volumeTargets(ctx, volumeID)
		if err != nil {
			return fmt.Errorf("list volume attachments: %w", err)
		}
		attachmentsByVolume[volumeID] = attachments
		for _, definition := range attachments {
			if definition.GetMcpId() != "" {
				parsed, err := parseUUID(definition.GetMcpId())
				if err != nil {
					return fmt.Errorf("volume mcp_id: %w", err)
				}
				mcpIDs = append(mcpIDs, parsed)
			}
		}
	}

	if err := s.ensureAgentNames(ctx, cache, attachmentAgentIDs); err != nil {
		return err
	}
	if err := s.ensureMcpNames(ctx, cache, mcpIDs); err != nil {
		return err
	}

	for _, volumeID := range missing {
		attachments := attachmentsByVolume[volumeID]
		protoAttachments := make([]*runnersv1.Attachment, 0, len(attachments))
		for _, definition := range attachments {
			switch {
			case definition.GetMcpId() != "":
				parsed, err := parseUUID(definition.GetMcpId())
				if err != nil {
					return fmt.Errorf("volume mcp_id: %w", err)
				}
				protoAttachments = append(protoAttachments, &runnersv1.Attachment{
					Kind: runnersv1.AttachmentKind_ATTACHMENT_KIND_MCP,
					Id:   parsed.String(),
					Name: cache.mcpNames[parsed],
				})
			case definition.GetEnvironmentId() != "":
				parsed, err := parseUUID(definition.GetEnvironmentId())
				if err != nil {
					return fmt.Errorf("volume environment_id: %w", err)
				}
				// An environment volume is mounted by whatever runs the
				// environment, which the workload record already names.
				protoAttachments = append(protoAttachments, &runnersv1.Attachment{
					Kind: runnersv1.AttachmentKind_ATTACHMENT_KIND_AGENT,
					Id:   parsed.String(),
					Name: definition.GetName(),
				})
			}
		}
		cache.attachments[volumeID] = protoAttachments
	}
	return nil
}

func (s *Server) buildVolumeItems(ctx context.Context, records []volumeRecord, cache *volumeEnrichmentCache, includeAttachments bool) ([]volumeListItem, error) {
	if len(records) == 0 {
		return []volumeListItem{}, nil
	}
	volumeIDs := make([]uuid.UUID, 0, len(records))
	sandboxIDs := make([]uuid.UUID, 0, len(records))
	for _, record := range records {
		if record.VolumeID != uuid.Nil {
			volumeIDs = append(volumeIDs, record.VolumeID)
		}
		if record.OwnerKind == runtimeOwnerKindSandbox {
			sandboxIDs = append(sandboxIDs, record.OwnerID)
		}
	}
	if len(volumeIDs) > 0 {
		if err := s.ensureVolumeNames(ctx, cache, volumeIDs); err != nil {
			return nil, err
		}
	}
	if includeAttachments && len(volumeIDs) > 0 {
		if err := s.ensureVolumeAttachments(ctx, cache, volumeIDs); err != nil {
			return nil, err
		}
	}
	if len(sandboxIDs) > 0 {
		if err := s.ensureSandboxNames(ctx, cache, sandboxIDs); err != nil {
			return nil, err
		}
	}

	items := make([]volumeListItem, 0, len(records))
	for _, record := range records {
		name := ""
		ownerName := ""
		attachments := []*runnersv1.Attachment{}
		if record.VolumeID != uuid.Nil {
			resolvedName, ok := cache.volumeNames[record.VolumeID]
			if !ok {
				return nil, fmt.Errorf("volume name missing for %s", record.VolumeID)
			}
			name = resolvedName
			if includeAttachments {
				attachments = cache.attachments[record.VolumeID]
			}
		}
		if record.OwnerKind == runtimeOwnerKindSandbox {
			resolvedOwner, ok := cache.sandboxNames[record.OwnerID]
			if !ok {
				return nil, fmt.Errorf("sandbox name missing for %s", record.OwnerID)
			}
			ownerName = resolvedOwner
		}
		items = append(items, volumeListItem{record: record, volumeName: name, ownerName: ownerName, attachments: attachments})
	}
	return items, nil
}

func filterVolumeItemsByName(items []volumeListItem, nameFilter string) []volumeListItem {
	if len(items) == 0 {
		return items
	}
	nameFilter = strings.ToLower(strings.TrimSpace(nameFilter))
	if nameFilter == "" {
		return items
	}
	filtered := make([]volumeListItem, 0, len(items))
	for _, item := range items {
		if strings.Contains(strings.ToLower(item.volumeName), nameFilter) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func matchesVolumeAttachmentKinds(attachments []*runnersv1.Attachment, kindFilter map[runnersv1.VolumeAttachmentFilterKind]bool) bool {
	if len(kindFilter) == 0 {
		return true
	}
	if len(attachments) == 0 {
		return kindFilter[runnersv1.VolumeAttachmentFilterKind_VOLUME_ATTACHMENT_FILTER_KIND_UNATTACHED]
	}
	for _, attachment := range attachments {
		switch attachment.GetKind() {
		case runnersv1.AttachmentKind_ATTACHMENT_KIND_AGENT:
			if kindFilter[runnersv1.VolumeAttachmentFilterKind_VOLUME_ATTACHMENT_FILTER_KIND_AGENT] {
				return true
			}
		case runnersv1.AttachmentKind_ATTACHMENT_KIND_MCP:
			if kindFilter[runnersv1.VolumeAttachmentFilterKind_VOLUME_ATTACHMENT_FILTER_KIND_MCP] {
				return true
			}
		}
	}
	return false
}

func paginateVolumeItems(items []volumeListItem, sort volumeListSort, pageSize int32) ([]volumeListItem, string, error) {
	limit := normalizePageSize(pageSize)
	if len(items) <= int(limit) {
		return items, "", nil
	}
	page := items[:limit]
	lastItem := page[len(page)-1]
	primary, err := volumePrimaryValue(lastItem, sort.Field)
	if err != nil {
		return nil, "", err
	}
	nextToken, err := encodeListCursor(primary, lastItem.record.Meta.ID)
	if err != nil {
		return nil, "", err
	}
	return page, nextToken, nil
}

func (s *Server) batchUpdateVolumeSampledAt(ctx context.Context, entries []sampledAtEntry) error {
	query, args := buildBatchSampledAtUpdateQuery("volumes", entries)
	if _, err := s.pool.Exec(ctx, query, args...); err != nil {
		return err
	}
	return nil
}

func scanVolume(row pgx.Row) (volumeRecord, error) {
	var (
		volume         volumeRecord
		instanceID     pgtype.Text
		volumeID       nullableUUIDScanner
		threadID       nullableUUIDScanner
		agentID        nullableUUIDScanner
		removedAt      pgtype.Timestamptz
		lastMeteringAt pgtype.Timestamptz
		ownerID        nullableUUIDScanner
		boundJSON      []byte
		intentJSON     []byte
	)
	if err := row.Scan(
		&volume.Meta.ID,
		&instanceID,
		&volumeID,
		&threadID,
		&volume.RunnerID,
		&agentID,
		&volume.OrganizationID,
		&volume.SizeGB,
		&volume.Status,
		&removedAt,
		&lastMeteringAt,
		&volume.OwnerKind,
		&ownerID,
		&volume.Meta.CreatedAt,
		&volume.Meta.UpdatedAt,
		&volume.LifecycleRevision,
		&volume.CheckedLifecycle,
		&boundJSON,
		&intentJSON,
	); err != nil {
		return volumeRecord{}, err
	}
	if !ownerID.Valid {
		return volumeRecord{}, fmt.Errorf("owner_id missing")
	}
	if volume.LifecycleRevision <= 0 {
		return volumeRecord{}, fmt.Errorf("volume lifecycle revision missing")
	}
	if len(boundJSON) != 0 {
		volume.BoundInstance = &runnerv1.VolumeListItem{}
		if err := protojson.Unmarshal(boundJSON, volume.BoundInstance); err != nil {
			return volumeRecord{}, fmt.Errorf("decode volume binding: %w", err)
		}
	}
	if len(intentJSON) != 0 {
		volume.RemovalIntent = &runnersv1.VolumeRemovalIntent{}
		if err := protojson.Unmarshal(intentJSON, volume.RemovalIntent); err != nil {
			return volumeRecord{}, fmt.Errorf("decode volume removal intent: %w", err)
		}
	}
	volume.OwnerID = ownerID.UUID
	if volumeID.Valid {
		volume.VolumeID = volumeID.UUID
	}
	if threadID.Valid {
		volume.ThreadID = threadID.UUID
	}
	if agentID.Valid {
		volume.AgentID = agentID.UUID
	}
	if instanceID.Valid {
		value := instanceID.String
		volume.InstanceID = &value
	}
	if removedAt.Valid {
		value := removedAt.Time
		volume.RemovedAt = &value
	}
	if lastMeteringAt.Valid {
		value := lastMeteringAt.Time
		volume.LastMeteringAt = &value
	}
	return volume, nil
}

func toProtoVolume(record volumeRecord) (*runnersv1.Volume, error) {
	statusValue, err := volumeStatusFromString(record.Status)
	if err != nil {
		return nil, err
	}
	protoVolume := &runnersv1.Volume{
		Meta:              toProtoEntityMeta(record.Meta),
		RunnerId:          record.RunnerID.String(),
		OrganizationId:    record.OrganizationID.String(),
		SizeGb:            record.SizeGB,
		Status:            statusValue,
		OwnerId:           record.OwnerID.String(),
		LifecycleRevision: uint64(record.LifecycleRevision),
		CheckedLifecycle:  record.CheckedLifecycle,
		BoundInstance:     record.BoundInstance,
		RemovalIntent:     record.RemovalIntent,
	}
	ownerKind, err := runtimeOwnerKindFromString(record.OwnerKind)
	if err != nil {
		return nil, err
	}
	protoVolume.OwnerKind = ownerKind
	if record.OwnerKind == runtimeOwnerKindAgentInstance {
		agentInstanceID := record.OwnerID.String()
		protoVolume.AgentInstanceId = &agentInstanceID
	}
	if record.VolumeID != uuid.Nil {
		volumeID := record.VolumeID.String()
		protoVolume.VolumeId = volumeID
		protoVolume.VolumeDefinitionId = &volumeID
	}
	if record.ThreadID != uuid.Nil {
		protoVolume.ThreadId = record.ThreadID.String()
	}
	if record.AgentID != uuid.Nil {
		agentID := record.AgentID.String()
		protoVolume.AgentId = agentID
		protoVolume.AgentClassId = &agentID
	}
	if record.InstanceID != nil {
		protoVolume.InstanceId = record.InstanceID
	}
	if record.RemovedAt != nil {
		protoVolume.RemovedAt = timestamppb.New(*record.RemovedAt)
	}
	if record.LastMeteringAt != nil {
		protoVolume.LastMeteringSampledAt = timestamppb.New(*record.LastMeteringAt)
	}
	return protoVolume, nil
}

func toProtoVolumeWithEnrichment(record volumeRecord, volumeName, ownerName string, attachments []*runnersv1.Attachment) (*runnersv1.Volume, error) {
	volume, err := toProtoVolume(record)
	if err != nil {
		return nil, err
	}
	volume.VolumeName = volumeName
	volume.Attachments = attachments
	if ownerName != "" {
		volume.OwnerName = &ownerName
	}
	return volume, nil
}

func toProtoVolumeList(records []volumeRecord) ([]*runnersv1.Volume, error) {
	volumes := make([]*runnersv1.Volume, 0, len(records))
	for _, record := range records {
		volume, err := toProtoVolume(record)
		if err != nil {
			return nil, err
		}
		volumes = append(volumes, volume)
	}
	return volumes, nil
}

func volumeStatusToString(status runnersv1.VolumeStatus) (string, error) {
	switch status {
	case runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING:
		return volumeStatusProvisioning, nil
	case runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE:
		return volumeStatusActive, nil
	case runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING:
		return volumeStatusDeprovision, nil
	case runnersv1.VolumeStatus_VOLUME_STATUS_DELETED:
		return volumeStatusDeleted, nil
	case runnersv1.VolumeStatus_VOLUME_STATUS_FAILED:
		return volumeStatusFailed, nil
	default:
		return "", fmt.Errorf("invalid volume status: %s", status.String())
	}
}

func volumeStatusFromString(value string) (runnersv1.VolumeStatus, error) {
	switch value {
	case volumeStatusProvisioning:
		return runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING, nil
	case volumeStatusActive:
		return runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE, nil
	case volumeStatusDeprovision:
		return runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING, nil
	case volumeStatusDeleted:
		return runnersv1.VolumeStatus_VOLUME_STATUS_DELETED, nil
	case volumeStatusFailed:
		return runnersv1.VolumeStatus_VOLUME_STATUS_FAILED, nil
	default:
		return runnersv1.VolumeStatus_VOLUME_STATUS_UNSPECIFIED, fmt.Errorf("invalid volume status: %s", value)
	}
}

func volumeStatusesToStrings(statuses []runnersv1.VolumeStatus) ([]string, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	values := make([]string, len(statuses))
	for i, statusValue := range statuses {
		value, err := volumeStatusToString(statusValue)
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	return values, nil
}
