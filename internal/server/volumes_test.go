package server

import (
	"context"
	"encoding/base64"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	agentsv1 "github.com/agynio/runners/.gen/go/agynio/api/agents/v1"
	authorizationv1 "github.com/agynio/runners/.gen/go/agynio/api/authorization/v1"
	notificationsv1 "github.com/agynio/runners/.gen/go/agynio/api/notifications/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pashagolub/pgxmock/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var volumeRowColumns = []string{
	"id",
	"instance_id",
	"volume_id",
	"thread_id",
	"runner_id",
	"agent_id",
	"organization_id",
	"size_gb",
	"status",
	"removed_at",
	"last_metering_sampled_at",
	"owner_kind",
	"owner_id",
	"created_at",
	"updated_at",
	"lifecycle_revision",
	"checked_lifecycle",
	"bound_instance",
	"removal_intent",
	"resource_anchor",
	"anchor_reservation",
	"anchored_removal_observation",
}

func TestListVolumesFiltersOrganization(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	volumeResourceID := uuid.New()
	threadID := uuid.New()
	runnerID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()

	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, int64(1), false, nil, nil, nil, nil, nil)
	volumeIDRows := pgxmock.NewRows([]string{"volume_id"}).AddRow(volumeResourceID)
	volumeIDQuery := "SELECT DISTINCT volume_id FROM volumes WHERE volumes.organization_id = $1"
	mockPool.ExpectQuery(regexp.QuoteMeta(volumeIDQuery)).
		WithArgs(organizationID).
		WillReturnRows(volumeIDRows)

	volumeName := "volume-name"
	limit := normalizePageSize(0)
	sortExpr := "COALESCE(CASE volumes.volume_id WHEN $2 THEN $3 END, '')"
	query := fmt.Sprintf("SELECT %s FROM volumes WHERE volumes.organization_id = $1 ORDER BY %s ASC, volumes.id ASC LIMIT $4", volumeColumns, sortExpr)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(organizationID, volumeResourceID, strings.ToLower(volumeName), int(limit)+1).
		WillReturnRows(rows)

	agentsClient := fakeAgentsClient{
		getVolume: func(ctx context.Context, req *agentsv1.GetVolumeRequest) (*agentsv1.GetVolumeResponse, error) {
			return &agentsv1.GetVolumeResponse{Volume: &agentsv1.Volume{Name: volumeName, Target: &agentsv1.Volume_EnvironmentId{EnvironmentId: agentID.String()}}}, nil
		},
	}

	var gotCheckReqs []*authorizationv1.CheckRequest
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			gotCheckReqs = append(gotCheckReqs, req)
			allowed := req.GetTupleKey().GetRelation() == organizationViewVolumes
			return &authorizationv1.CheckResponse{Allowed: allowed}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient, AgentsClient: agentsClient})
	organizationIDValue := organizationID.String()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	resp, err := srv.ListVolumes(ctx, &runnersv1.ListVolumesRequest{OrganizationId: &organizationIDValue})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	if len(resp.GetVolumes()) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(resp.GetVolumes()))
	}
	if resp.GetVolumes()[0].GetOrganizationId() != organizationID.String() {
		t.Fatalf("expected organization id %q, got %q", organizationID.String(), resp.GetVolumes()[0].GetOrganizationId())
	}
	if resp.GetVolumes()[0].GetVolumeName() != volumeName {
		t.Fatalf("expected volume name %q, got %q", volumeName, resp.GetVolumes()[0].GetVolumeName())
	}
	if len(gotCheckReqs) != 1 {
		t.Fatalf("expected 1 authorization check, got %d", len(gotCheckReqs))
	}
	if gotCheckReqs[0].GetTupleKey().GetRelation() != organizationViewVolumes {
		t.Fatalf("expected view volumes relation, got %s", gotCheckReqs[0].GetTupleKey().GetRelation())
	}
	if gotCheckReqs[0].GetTupleKey().GetObject() != organizationObject(organizationID) {
		t.Fatalf("expected organization object %q, got %q", organizationObject(organizationID), gotCheckReqs[0].GetTupleKey().GetObject())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListVolumesInternalNoIdentity(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	volumeResourceID := uuid.New()
	threadID := uuid.New()
	runnerID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()

	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, int64(1), false, nil, nil, nil, nil, nil)
	volumeIDRows := pgxmock.NewRows([]string{"volume_id"}).AddRow(volumeResourceID)
	volumeIDQuery := "SELECT DISTINCT volume_id FROM volumes"
	mockPool.ExpectQuery(regexp.QuoteMeta(volumeIDQuery)).
		WillReturnRows(volumeIDRows)

	volumeName := "volume-name"
	limit := normalizePageSize(0)
	sortExpr := "COALESCE(CASE volumes.volume_id WHEN $1 THEN $2 END, '')"
	query := fmt.Sprintf("SELECT %s FROM volumes ORDER BY %s ASC, volumes.id ASC LIMIT $3", volumeColumns, sortExpr)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(volumeResourceID, strings.ToLower(volumeName), int(limit)+1).
		WillReturnRows(rows)

	agentsClient := fakeAgentsClient{
		getVolume: func(ctx context.Context, req *agentsv1.GetVolumeRequest) (*agentsv1.GetVolumeResponse, error) {
			return &agentsv1.GetVolumeResponse{Volume: &agentsv1.Volume{Name: volumeName, Target: &agentsv1.Volume_EnvironmentId{EnvironmentId: agentID.String()}}}, nil
		},
	}

	checkCalls := 0
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			checkCalls++
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient, AgentsClient: agentsClient})
	resp, err := srv.ListVolumes(context.Background(), &runnersv1.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	if len(resp.GetVolumes()) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(resp.GetVolumes()))
	}
	if resp.GetVolumes()[0].GetOrganizationId() != organizationID.String() {
		t.Fatalf("expected organization id %q, got %q", organizationID.String(), resp.GetVolumes()[0].GetOrganizationId())
	}
	if resp.GetVolumes()[0].GetVolumeName() != volumeName {
		t.Fatalf("expected volume name %q, got %q", volumeName, resp.GetVolumes()[0].GetVolumeName())
	}
	if checkCalls != 0 {
		t.Fatalf("expected no authorization checks, got %d", checkCalls)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListVolumesFiltersRunner(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	volumeResourceID := uuid.New()
	threadID := uuid.New()
	runnerID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()

	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, int64(1), false, nil, nil, nil, nil, nil)
	volumeIDRows := pgxmock.NewRows([]string{"volume_id"}).AddRow(volumeResourceID)
	volumeIDQuery := "SELECT DISTINCT volume_id FROM volumes WHERE volumes.organization_id = $1 AND volumes.runner_id = ANY($2)"
	mockPool.ExpectQuery(regexp.QuoteMeta(volumeIDQuery)).
		WithArgs(organizationID, pgtype.FlatArray[uuid.UUID]([]uuid.UUID{runnerID})).
		WillReturnRows(volumeIDRows)

	volumeName := "volume-name"
	limit := normalizePageSize(0)
	sortExpr := "COALESCE(CASE volumes.volume_id WHEN $3 THEN $4 END, '')"
	query := fmt.Sprintf("SELECT %s FROM volumes WHERE volumes.organization_id = $1 AND volumes.runner_id = ANY($2) ORDER BY %s ASC, volumes.id ASC LIMIT $5", volumeColumns, sortExpr)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(organizationID, pgtype.FlatArray[uuid.UUID]([]uuid.UUID{runnerID}), volumeResourceID, strings.ToLower(volumeName), int(limit)+1).
		WillReturnRows(rows)

	agentsClient := fakeAgentsClient{
		getVolume: func(ctx context.Context, req *agentsv1.GetVolumeRequest) (*agentsv1.GetVolumeResponse, error) {
			return &agentsv1.GetVolumeResponse{Volume: &agentsv1.Volume{Name: volumeName, Target: &agentsv1.Volume_EnvironmentId{EnvironmentId: agentID.String()}}}, nil
		},
	}

	var gotCheckReq *authorizationv1.CheckRequest
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			gotCheckReq = req
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient, AgentsClient: agentsClient})
	runnerIDValue := runnerID.String()
	organizationIDValue := organizationID.String()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	resp, err := srv.ListVolumes(ctx, &runnersv1.ListVolumesRequest{OrganizationId: &organizationIDValue, RunnerId: &runnerIDValue})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	if len(resp.GetVolumes()) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(resp.GetVolumes()))
	}
	if resp.GetVolumes()[0].GetRunnerId() != runnerID.String() {
		t.Fatalf("expected runner id %q, got %q", runnerID.String(), resp.GetVolumes()[0].GetRunnerId())
	}
	if resp.GetVolumes()[0].GetVolumeName() != volumeName {
		t.Fatalf("expected volume name %q, got %q", volumeName, resp.GetVolumes()[0].GetVolumeName())
	}
	if gotCheckReq == nil {
		t.Fatal("expected authorization Check to be called")
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListVolumesPendingSample(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	volumeResourceID := uuid.New()
	threadID := uuid.New()
	runnerID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()

	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, int64(1), false, nil, nil, nil, nil, nil)
	volumeIDRows := pgxmock.NewRows([]string{"volume_id"}).AddRow(volumeResourceID)
	volumeIDQuery := fmt.Sprintf("SELECT DISTINCT volume_id FROM volumes WHERE volumes.organization_id = $1 AND %s", pendingSampleClause)
	mockPool.ExpectQuery(regexp.QuoteMeta(volumeIDQuery)).
		WithArgs(organizationID).
		WillReturnRows(volumeIDRows)

	volumeName := "volume-name"
	limit := normalizePageSize(0)
	sortExpr := "COALESCE(CASE volumes.volume_id WHEN $2 THEN $3 END, '')"
	query := fmt.Sprintf("SELECT %s FROM volumes WHERE volumes.organization_id = $1 AND %s ORDER BY %s ASC, volumes.id ASC LIMIT $4", volumeColumns, pendingSampleClause, sortExpr)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(organizationID, volumeResourceID, strings.ToLower(volumeName), int(limit)+1).
		WillReturnRows(rows)

	agentsClient := fakeAgentsClient{
		getVolume: func(ctx context.Context, req *agentsv1.GetVolumeRequest) (*agentsv1.GetVolumeResponse, error) {
			return &agentsv1.GetVolumeResponse{Volume: &agentsv1.Volume{Name: volumeName, Target: &agentsv1.Volume_EnvironmentId{EnvironmentId: agentID.String()}}}, nil
		},
	}

	var gotCheckReq *authorizationv1.CheckRequest
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			gotCheckReq = req
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient, AgentsClient: agentsClient})
	pendingSample := true
	organizationIDValue := organizationID.String()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	resp, err := srv.ListVolumes(ctx, &runnersv1.ListVolumesRequest{OrganizationId: &organizationIDValue, PendingSample: &pendingSample})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	if len(resp.GetVolumes()) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(resp.GetVolumes()))
	}
	if gotCheckReq == nil {
		t.Fatal("expected authorization Check to be called")
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListVolumesFiltersAttachments(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	volumeResourceID := uuid.New()
	otherVolumeID := uuid.New()
	otherResourceID := uuid.New()
	threadID := uuid.New()
	runnerID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()

	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, int64(1), false, nil, nil, nil, nil, nil).
		AddRow(otherVolumeID, nil, otherResourceID, threadID, runnerID, agentID, organizationID, "20", volumeStatusActive, nil, nil, runtimeOwnerKindAgentInstance, otherVolumeID, now, now, int64(1), false, nil, nil, nil, nil, nil)
	volumeIDRows := pgxmock.NewRows([]string{"volume_id"}).AddRow(volumeResourceID).AddRow(otherResourceID)
	volumeIDQuery := "SELECT DISTINCT volume_id FROM volumes WHERE volumes.organization_id = $1"
	mockPool.ExpectQuery(regexp.QuoteMeta(volumeIDQuery)).
		WithArgs(organizationID).
		WillReturnRows(volumeIDRows)

	volumeName := "data-volume"
	otherName := "other-volume"
	ids := []uuid.UUID{volumeResourceID, otherResourceID}
	nameMap := map[uuid.UUID]string{volumeResourceID: volumeName, otherResourceID: otherName}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].String() < ids[j].String()
	})
	parts := make([]string, 0, len(ids))
	args := []any{organizationID}
	idx := 2
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("WHEN $%d THEN $%d", idx, idx+1))
		args = append(args, id, strings.ToLower(nameMap[id]))
		idx += 2
	}
	sortExpr := fmt.Sprintf("COALESCE(CASE volumes.volume_id %s END, '')", strings.Join(parts, " "))
	limit := normalizePageSize(0)
	query := fmt.Sprintf("SELECT %s FROM volumes WHERE volumes.organization_id = $1 ORDER BY %s ASC, volumes.id ASC LIMIT $%d", volumeColumns, sortExpr, len(args)+1)
	args = append(args, int(limit)+1)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(args...).
		WillReturnRows(rows)
	agentsClient := fakeAgentsClient{
		getVolume: func(ctx context.Context, req *agentsv1.GetVolumeRequest) (*agentsv1.GetVolumeResponse, error) {
			switch req.GetId() {
			case volumeResourceID.String():
				return &agentsv1.GetVolumeResponse{Volume: &agentsv1.Volume{Name: volumeName, Target: &agentsv1.Volume_EnvironmentId{EnvironmentId: agentID.String()}}}, nil
			case otherResourceID.String():
				return &agentsv1.GetVolumeResponse{Volume: &agentsv1.Volume{Description: otherName}}, nil
			default:
				return &agentsv1.GetVolumeResponse{Volume: &agentsv1.Volume{}}, nil
			}
		},
		getAgent: func(ctx context.Context, req *agentsv1.GetAgentRequest) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Name: "agent"}}, nil
		},
	}

	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient, AgentsClient: agentsClient})
	organizationIDValue := organizationID.String()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	resp, err := srv.ListVolumes(ctx, &runnersv1.ListVolumesRequest{
		OrganizationId: &organizationIDValue,
		Filter: &runnersv1.ListVolumesFilter{
			AttachedToKindIn: []runnersv1.VolumeAttachmentFilterKind{
				runnersv1.VolumeAttachmentFilterKind_VOLUME_ATTACHMENT_FILTER_KIND_AGENT,
			},
			VolumeNameSubstring: &volumeName,
		},
	})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	if len(resp.GetVolumes()) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(resp.GetVolumes()))
	}
	volume := resp.GetVolumes()[0]
	if volume.GetVolumeName() != volumeName {
		t.Fatalf("expected volume name %q, got %q", volumeName, volume.GetVolumeName())
	}
	attachments := volume.GetAttachments()
	if len(attachments) != 1 {
		t.Fatalf("expected 1 attachment, got %d", len(attachments))
	}
	if attachments[0].GetKind() != runnersv1.AttachmentKind_ATTACHMENT_KIND_AGENT {
		t.Fatalf("expected agent attachment, got %v", attachments[0].GetKind())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListVolumesPaginationByName(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	volumeResourceID := uuid.New()
	otherVolumeID := uuid.New()
	otherResourceID := uuid.New()
	threadID := uuid.New()
	runnerID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()

	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, int64(1), false, nil, nil, nil, nil, nil).
		AddRow(otherVolumeID, nil, otherResourceID, threadID, runnerID, agentID, organizationID, "20", volumeStatusActive, nil, nil, runtimeOwnerKindAgentInstance, otherVolumeID, now, now, int64(1), false, nil, nil, nil, nil, nil)
	volumeIDRows := pgxmock.NewRows([]string{"volume_id"}).AddRow(volumeResourceID).AddRow(otherResourceID)
	volumeIDQuery := "SELECT DISTINCT volume_id FROM volumes WHERE volumes.organization_id = $1"
	mockPool.ExpectQuery(regexp.QuoteMeta(volumeIDQuery)).WithArgs(organizationID).WillReturnRows(volumeIDRows)

	volumeName := "alpha"
	otherName := "beta"
	ids := []uuid.UUID{volumeResourceID, otherResourceID}
	nameMap := map[uuid.UUID]string{volumeResourceID: volumeName, otherResourceID: otherName}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].String() < ids[j].String()
	})
	parts := make([]string, 0, len(ids))
	args := []any{organizationID}
	idx := 2
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("WHEN $%d THEN $%d", idx, idx+1))
		args = append(args, id, strings.ToLower(nameMap[id]))
		idx += 2
	}
	sortExpr := fmt.Sprintf("COALESCE(CASE volumes.volume_id %s END, '')", strings.Join(parts, " "))
	pageSize := int32(1)
	limit := normalizePageSize(pageSize)
	query := fmt.Sprintf("SELECT %s FROM volumes WHERE volumes.organization_id = $1 ORDER BY %s ASC, volumes.id ASC LIMIT $%d", volumeColumns, sortExpr, len(args)+1)
	argsWithLimit := append(args, int(limit)+1)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(argsWithLimit...).WillReturnRows(rows)
	cursorIndex := len(args) + 1
	cursorIDIndex := len(args) + 2
	limitIndex := len(args) + 3
	queryWithCursor := fmt.Sprintf("SELECT %s FROM volumes WHERE volumes.organization_id = $1 AND (%s > $%d OR (%s = $%d AND volumes.id > $%d)) ORDER BY %s ASC, volumes.id ASC LIMIT $%d", volumeColumns, sortExpr, cursorIndex, sortExpr, cursorIndex, cursorIDIndex, sortExpr, limitIndex)
	argsWithCursor := append(args, strings.ToLower(volumeName), volumeID, int(limit)+1)
	rowsSecond := pgxmock.NewRows(volumeRowColumns).
		AddRow(otherVolumeID, nil, otherResourceID, threadID, runnerID, agentID, organizationID, "20", volumeStatusActive, nil, nil, runtimeOwnerKindAgentInstance, otherVolumeID, now, now, int64(1), false, nil, nil, nil, nil, nil)
	mockPool.ExpectQuery(regexp.QuoteMeta(queryWithCursor)).WithArgs(argsWithCursor...).WillReturnRows(rowsSecond)

	volumeIDRowsSecond := pgxmock.NewRows([]string{"volume_id"}).AddRow(volumeResourceID).AddRow(otherResourceID)
	mockPool.ExpectQuery(regexp.QuoteMeta(volumeIDQuery)).WithArgs(organizationID).WillReturnRows(volumeIDRowsSecond)
	rowsThird := pgxmock.NewRows(volumeRowColumns).
		AddRow(otherVolumeID, nil, otherResourceID, threadID, runnerID, agentID, organizationID, "20", volumeStatusActive, nil, nil, runtimeOwnerKindAgentInstance, otherVolumeID, now, now, int64(1), false, nil, nil, nil, nil, nil)
	mockPool.ExpectQuery(regexp.QuoteMeta(queryWithCursor)).WithArgs(argsWithCursor...).WillReturnRows(rowsThird)
	agentsClient := fakeAgentsClient{
		getVolume: func(ctx context.Context, req *agentsv1.GetVolumeRequest) (*agentsv1.GetVolumeResponse, error) {
			switch req.GetId() {
			case volumeResourceID.String():
				return &agentsv1.GetVolumeResponse{Volume: &agentsv1.Volume{Name: volumeName, Target: &agentsv1.Volume_EnvironmentId{EnvironmentId: agentID.String()}}}, nil
			case otherResourceID.String():
				return &agentsv1.GetVolumeResponse{Volume: &agentsv1.Volume{Description: otherName}}, nil
			default:
				return &agentsv1.GetVolumeResponse{Volume: &agentsv1.Volume{}}, nil
			}
		},
	}

	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient, AgentsClient: agentsClient})
	organizationIDValue := organizationID.String()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	resp, err := srv.ListVolumes(ctx, &runnersv1.ListVolumesRequest{OrganizationId: &organizationIDValue, PageSize: pageSize})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	if len(resp.GetVolumes()) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(resp.GetVolumes()))
	}
	if resp.GetVolumes()[0].GetVolumeName() != volumeName {
		t.Fatalf("expected first volume name %q, got %q", volumeName, resp.GetVolumes()[0].GetVolumeName())
	}
	if resp.GetNextPageToken() == "" {
		t.Fatal("expected next page token")
	}

	secondResp, err := srv.ListVolumes(ctx, &runnersv1.ListVolumesRequest{OrganizationId: &organizationIDValue, PageSize: pageSize, PageToken: resp.GetNextPageToken()})
	if err != nil {
		t.Fatalf("ListVolumes page 2 failed: %v", err)
	}
	if len(secondResp.GetVolumes()) != 1 {
		t.Fatalf("expected 1 volume on second page, got %d", len(secondResp.GetVolumes()))
	}
	if secondResp.GetVolumes()[0].GetVolumeName() != otherName {
		t.Fatalf("expected second volume name %q, got %q", otherName, secondResp.GetVolumes()[0].GetVolumeName())
	}
	if secondResp.GetNextPageToken() != "" {
		t.Fatalf("expected empty next page token, got %q", secondResp.GetNextPageToken())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListVolumesInvalidUUID(t *testing.T) {
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
	}
	srv := New(Options{AuthorizationClient: authorizationClient})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, uuid.NewString()))
	organizationIDValue := uuid.NewString()

	cases := []struct {
		name string
		req  *runnersv1.ListVolumesRequest
	}{
		{
			name: "organization_id_missing",
			req:  &runnersv1.ListVolumesRequest{},
		},
		{
			name: "organization_id",
			req: func() *runnersv1.ListVolumesRequest {
				value := "not-a-uuid"
				return &runnersv1.ListVolumesRequest{OrganizationId: &value}
			}(),
		},
		{
			name: "runner_id",
			req: func() *runnersv1.ListVolumesRequest {
				value := "not-a-uuid"
				return &runnersv1.ListVolumesRequest{OrganizationId: &organizationIDValue, RunnerId: &value}
			}(),
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := srv.ListVolumes(ctx, testCase.req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument error, got %v", err)
			}
		})
	}
}

func TestListVolumesInvalidPageToken(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	organizationID := uuid.New()
	callerID := uuid.New()
	rows := pgxmock.NewRows([]string{"volume_id"})
	query := "SELECT DISTINCT volume_id FROM volumes WHERE volumes.organization_id = $1"
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(organizationID).WillReturnRows(rows)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(organizationID).WillReturnRows(pgxmock.NewRows([]string{"volume_id"}))
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(organizationID).WillReturnRows(pgxmock.NewRows([]string{"volume_id"}))

	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	organizationIDValue := organizationID.String()
	validID := uuid.NewString()

	invalidJSONToken := base64.RawURLEncoding.EncodeToString([]byte("not-json"))
	wrongPrimaryToken := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"primary":10,"id":"%s"}`, validID)))

	cases := []string{"not-a-token", invalidJSONToken, wrongPrimaryToken}
	for _, token := range cases {
		_, err := srv.ListVolumes(ctx, &runnersv1.ListVolumesRequest{OrganizationId: &organizationIDValue, PageToken: token})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected InvalidArgument error for token %q, got %v", token, err)
		}
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListVolumesRequiresViewVolumes(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	organizationID := uuid.New()
	callerID := uuid.New()

	var gotCheckReqs []*authorizationv1.CheckRequest
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			gotCheckReqs = append(gotCheckReqs, req)
			return &authorizationv1.CheckResponse{Allowed: false}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	organizationIDValue := organizationID.String()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	_, err = srv.ListVolumes(ctx, &runnersv1.ListVolumesRequest{OrganizationId: &organizationIDValue})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied error, got %v", err)
	}
	if len(gotCheckReqs) != 1 {
		t.Fatalf("expected 1 authorization check, got %d", len(gotCheckReqs))
	}
	if gotCheckReqs[0].GetTupleKey().GetRelation() != organizationViewVolumes {
		t.Fatalf("expected view volumes relation, got %s", gotCheckReqs[0].GetTupleKey().GetRelation())
	}
	if gotCheckReqs[0].GetTupleKey().GetObject() != organizationObject(organizationID) {
		t.Fatalf("expected organization object %q, got %q", organizationObject(organizationID), gotCheckReqs[0].GetTupleKey().GetObject())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListVolumesByThreadInternalNoIdentity(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	volumeResourceID := uuid.New()
	threadID := uuid.New()
	runnerID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	limit := normalizePageSize(0)

	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, int64(1), false, nil, nil, nil, nil, nil)

	query := fmt.Sprintf("SELECT %s FROM volumes WHERE thread_id = $1 ORDER BY id ASC LIMIT $2", volumeColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(threadID, int(limit)+1).
		WillReturnRows(rows)

	checkCalls := 0
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			checkCalls++
			return &authorizationv1.CheckResponse{Allowed: false}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	resp, err := srv.ListVolumesByThread(context.Background(), &runnersv1.ListVolumesByThreadRequest{ThreadId: threadID.String()})
	if err != nil {
		t.Fatalf("ListVolumesByThread failed: %v", err)
	}
	if len(resp.GetVolumes()) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(resp.GetVolumes()))
	}
	if resp.GetVolumes()[0].GetOrganizationId() != organizationID.String() {
		t.Fatalf("expected organization id %q, got %q", organizationID.String(), resp.GetVolumes()[0].GetOrganizationId())
	}
	if checkCalls != 0 {
		t.Fatalf("expected no authorization checks, got %d", checkCalls)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListVolumesByAgentInstanceFiltersOwner(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	volumeResourceID := uuid.New()
	threadID := uuid.New()
	runnerID := uuid.New()
	agentID := uuid.New()
	agentInstanceID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	limit := normalizePageSize(0)

	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, "runner-volume-instance", volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, runtimeOwnerKindAgentInstance, agentInstanceID, now, now, int64(1), false, nil, nil, nil, nil, nil)

	query := fmt.Sprintf("SELECT %s FROM volumes WHERE owner_kind = $1 AND owner_id = $2 AND status = ANY($3) ORDER BY id ASC LIMIT $4", volumeColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(runtimeOwnerKindAgentInstance, agentInstanceID, pgtype.FlatArray[string]{volumeStatusActive}, int(limit)+1).
		WillReturnRows(rows)

	checkCalls := 0
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			checkCalls++
			return &authorizationv1.CheckResponse{Allowed: false}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	resp, err := srv.ListVolumesByAgentInstance(context.Background(), &runnersv1.ListVolumesByAgentInstanceRequest{
		AgentInstanceId: agentInstanceID.String(),
		Statuses:        []runnersv1.VolumeStatus{runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE},
	})
	if err != nil {
		t.Fatalf("ListVolumesByAgentInstance failed: %v", err)
	}
	if len(resp.GetVolumes()) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(resp.GetVolumes()))
	}
	volume := resp.GetVolumes()[0]
	if volume.GetAgentInstanceId() != agentInstanceID.String() {
		t.Fatalf("expected agent instance id %s, got %s", agentInstanceID, volume.GetAgentInstanceId())
	}
	if volume.GetOwnerId() != agentInstanceID.String() {
		t.Fatalf("expected owner id %s, got %s", agentInstanceID, volume.GetOwnerId())
	}
	if volume.GetInstanceId() != "runner-volume-instance" {
		t.Fatalf("expected runner-local volume instance id, got %q", volume.GetInstanceId())
	}
	if checkCalls != 0 {
		t.Fatalf("expected no authorization checks, got %d", checkCalls)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestGetVolumeRequiresViewVolumes(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	volumeResourceID := uuid.New()
	threadID := uuid.New()
	runnerID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()

	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, int64(1), false, nil, nil, nil, nil, nil)

	query := fmt.Sprintf("SELECT %s FROM volumes WHERE id = $1", volumeColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(volumeID).WillReturnRows(rows)

	var gotCheckReq *authorizationv1.CheckRequest
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			gotCheckReq = req
			return &authorizationv1.CheckResponse{Allowed: false}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	_, err = srv.GetVolume(ctx, &runnersv1.GetVolumeRequest{Id: volumeID.String()})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied error, got %v", err)
	}
	if gotCheckReq == nil {
		t.Fatal("expected authorization Check to be called")
	}
	if gotCheckReq.GetTupleKey().GetRelation() != organizationViewVolumes {
		t.Fatalf("expected view volumes relation, got %s", gotCheckReq.GetTupleKey().GetRelation())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUpdateVolume(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	volumeResourceID := uuid.New()
	threadID := uuid.New()
	runnerID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	instanceID := "instance-1"
	now := time.Now().UTC()

	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, instanceID, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, int64(1), false, nil, nil, nil, nil, nil)

	query := fmt.Sprintf("UPDATE volumes SET status = $1, instance_id = $2, updated_at = NOW() WHERE id = $3 RETURNING %s", volumeColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(volumeStatusActive, instanceID, volumeID).
		WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	resp, err := srv.UpdateVolume(context.Background(), &runnersv1.UpdateVolumeRequest{
		Id:         volumeID.String(),
		Status:     runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE.Enum(),
		InstanceId: &instanceID,
	})
	if err != nil {
		t.Fatalf("UpdateVolume failed: %v", err)
	}
	if resp.GetVolume().GetInstanceId() != instanceID {
		t.Fatalf("expected instance id %q, got %q", instanceID, resp.GetVolume().GetInstanceId())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUpdateVolumePublishesNotification(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	volumeResourceID := uuid.New()
	threadID := uuid.New()
	runnerID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()

	selectRows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusProvisioning, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, int64(1), false, nil, nil, nil, nil, nil)
	selectQuery := fmt.Sprintf("SELECT %s FROM volumes WHERE id = $1", volumeColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(selectQuery)).
		WithArgs(volumeID).
		WillReturnRows(selectRows)

	updateRows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, int64(1), false, nil, nil, nil, nil, nil)
	updateQuery := fmt.Sprintf("UPDATE volumes SET status = $1, updated_at = NOW() WHERE id = $2 RETURNING %s", volumeColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(updateQuery)).
		WithArgs(volumeStatusActive, volumeID).
		WillReturnRows(updateRows)

	published := []*notificationsv1.PublishRequest{}
	notificationsClient := fakeNotificationsClient{publish: func(ctx context.Context, req *notificationsv1.PublishRequest) (*notificationsv1.PublishResponse, error) {
		published = append(published, req)
		return &notificationsv1.PublishResponse{}, nil
	}}

	srv := New(Options{Pool: mockPool, NotificationsClient: notificationsClient})
	_, err = srv.UpdateVolume(context.Background(), &runnersv1.UpdateVolumeRequest{
		Id:     volumeID.String(),
		Status: runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE.Enum(),
	})
	if err != nil {
		t.Fatalf("UpdateVolume failed: %v", err)
	}
	if len(published) != 1 {
		t.Fatalf("expected 1 notification, got %d", len(published))
	}
	if published[0].GetEvent() != "volume.updated" {
		t.Fatalf("expected volume.updated event, got %q", published[0].GetEvent())
	}
	rooms := published[0].GetRooms()
	if len(rooms) != 2 {
		t.Fatalf("expected 2 rooms, got %d", len(rooms))
	}
	if rooms[0] != fmt.Sprintf("organization:%s", organizationID.String()) {
		t.Fatalf("unexpected organization room %q", rooms[0])
	}
	if rooms[1] != fmt.Sprintf("volume:%s", volumeID.String()) {
		t.Fatalf("unexpected volume room %q", rooms[1])
	}
	payloadFields := published[0].GetPayload().AsMap()
	if payloadFields["volume_id"] != volumeID.String() {
		t.Fatalf("expected payload volume_id %q, got %v", volumeID.String(), payloadFields["volume_id"])
	}
	if payloadFields["status"] != volumeStatusActive {
		t.Fatalf("expected payload status %q, got %v", volumeStatusActive, payloadFields["status"])
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUpdateVolumeRequiresFields(t *testing.T) {
	srv := New(Options{})

	_, err := srv.UpdateVolume(context.Background(), &runnersv1.UpdateVolumeRequest{Id: uuid.NewString()})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument error, got %v", err)
	}
}

func TestBatchUpdateVolumeSampledAt(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	firstID := uuid.New()
	secondID := uuid.New()
	firstSampledAt := time.Now().UTC()
	secondSampledAt := firstSampledAt.Add(3 * time.Minute)

	query := "UPDATE volumes AS target SET last_metering_sampled_at = v.sampled_at, updated_at = NOW() FROM (VALUES ($1::uuid, $2::timestamptz), ($3::uuid, $4::timestamptz)) AS v(id, sampled_at) WHERE target.id = v.id"
	mockPool.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs(firstID, firstSampledAt, secondID, secondSampledAt).
		WillReturnResult(pgxmock.NewResult("UPDATE", 2))

	srv := New(Options{Pool: mockPool})
	_, err = srv.BatchUpdateVolumeSampledAt(context.Background(), &runnersv1.BatchUpdateVolumeSampledAtRequest{
		Entries: []*runnersv1.SampledAtEntry{
			{Id: firstID.String(), SampledAt: timestamppb.New(firstSampledAt)},
			{Id: secondID.String(), SampledAt: timestamppb.New(secondSampledAt)},
		},
	})
	if err != nil {
		t.Fatalf("BatchUpdateVolumeSampledAt failed: %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestBatchUpdateVolumeSampledAtInvalid(t *testing.T) {
	srv := New(Options{})

	_, err := srv.BatchUpdateVolumeSampledAt(context.Background(), &runnersv1.BatchUpdateVolumeSampledAtRequest{
		Entries: []*runnersv1.SampledAtEntry{{Id: "not-a-uuid", SampledAt: timestamppb.Now()}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument error, got %v", err)
	}
}

// A sandbox volume names the definition it was made from, like every other
// disk. It carries no thread or class, which is what distinguishes it from an
// agent-instance volume -- not the absence of a definition.
func TestCreateVolumeSandboxNamesItsDefinition(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}
	defer mockPool.Close()

	volumeID := uuid.New()
	definitionID := uuid.New()
	runnerID := uuid.New()
	organizationID := uuid.New()
	sandboxID := uuid.New()
	now := time.Now()

	query := fmt.Sprintf("INSERT INTO volumes (id, volume_id, thread_id, runner_id, agent_id, organization_id, size_gb, status, owner_kind, owner_id, checked_lifecycle)\n\t    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)\n\t    RETURNING %s", volumeColumns)
	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, definitionID, nil, runnerID, nil, organizationID, "10", volumeStatusActive, nil, nil, runtimeOwnerKindSandbox, sandboxID, now, now, int64(1), false, nil, nil, nil, nil, nil)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(volumeID, definitionID, nil, runnerID, nil, organizationID, "10", volumeStatusActive, runtimeOwnerKindSandbox, sandboxID, false).
		WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	definition := definitionID.String()
	resp, err := srv.CreateVolume(context.Background(), &runnersv1.CreateVolumeRequest{
		Id:                 volumeID.String(),
		RunnerId:           runnerID.String(),
		OrganizationId:     organizationID.String(),
		SizeGb:             "10",
		Status:             runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE,
		OwnerKind:          runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX,
		OwnerId:            sandboxID.String(),
		VolumeDefinitionId: &definition,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	if resp.GetVolume().GetOwnerKind() != runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX {
		t.Fatalf("expected sandbox owner kind, got %s", resp.GetVolume().GetOwnerKind())
	}
	if resp.GetVolume().GetVolumeDefinitionId() != definitionID.String() {
		t.Fatalf("expected definition %s, got %q", definitionID, resp.GetVolume().GetVolumeDefinitionId())
	}
	if resp.GetVolume().GetAgentId() != "" || resp.GetVolume().GetThreadId() != "" {
		t.Fatalf("expected a sandbox volume to carry no class or thread")
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// Every disk is made from a definition, whatever owns it.
func TestCreateVolumeRequiresADefinition(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}
	defer mockPool.Close()

	srv := New(Options{Pool: mockPool})
	_, err = srv.CreateVolume(context.Background(), &runnersv1.CreateVolumeRequest{
		Id:             uuid.NewString(),
		RunnerId:       uuid.NewString(),
		OrganizationId: uuid.NewString(),
		SizeGb:         "10",
		Status:         runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE,
		OwnerKind:      runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX,
		OwnerId:        uuid.NewString(),
	})
	if err == nil {
		t.Fatal("expected a volume without a definition to be refused")
	}
}

func TestCreateVolumeMapsAgentInstanceIDToOwnerID(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	volumeResourceID := uuid.New()
	threadID := uuid.New()
	runnerID := uuid.New()
	agentID := uuid.New()
	agentInstanceID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	query := fmt.Sprintf("INSERT INTO volumes (id, volume_id, thread_id, runner_id, agent_id, organization_id, size_gb, status, owner_kind, owner_id, checked_lifecycle)\n\t    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)\n\t    RETURNING %s", volumeColumns)
	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, runtimeOwnerKindAgentInstance, agentInstanceID, now, now, int64(1), false, nil, nil, nil, nil, nil)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(volumeID, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, runtimeOwnerKindAgentInstance, agentInstanceID, false).
		WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	resp, err := srv.CreateVolume(context.Background(), &runnersv1.CreateVolumeRequest{
		Id:                 volumeID.String(),
		VolumeDefinitionId: ptr(volumeResourceID.String()),
		ThreadId:           threadID.String(),
		RunnerId:           runnerID.String(),
		AgentClassId:       ptr(agentID.String()),
		OrganizationId:     organizationID.String(),
		SizeGb:             "10",
		Status:             runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE,
		OwnerKind:          runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
		AgentInstanceId:    ptr(agentInstanceID.String()),
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	if resp.GetVolume().GetOwnerId() != agentInstanceID.String() {
		t.Fatalf("expected owner id %s, got %s", agentInstanceID, resp.GetVolume().GetOwnerId())
	}
	if resp.GetVolume().GetAgentInstanceId() != agentInstanceID.String() {
		t.Fatalf("expected agent instance id %s, got %s", agentInstanceID, resp.GetVolume().GetAgentInstanceId())
	}
	if resp.GetVolume().GetInstanceId() != "" {
		t.Fatalf("expected runner-local instance id to stay empty, got %q", resp.GetVolume().GetInstanceId())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestGetVolumeSandboxRuntimeOnlySkipsDefinitionEnrichment(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	runnerID := uuid.New()
	organizationID := uuid.New()
	sandboxID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()

	query := fmt.Sprintf(`SELECT %s FROM volumes WHERE id = $1`, volumeColumns)
	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, nil, nil, runnerID, nil, organizationID, "10", volumeStatusActive, nil, nil, runtimeOwnerKindSandbox, sandboxID, now, now, int64(1), false, nil, nil, nil, nil, nil)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(volumeID).WillReturnRows(rows)

	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			if req.GetTupleKey().GetRelation() != organizationViewVolumes {
				t.Fatalf("expected view volumes relation, got %s", req.GetTupleKey().GetRelation())
			}
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
	}

	sandboxName := "sandbox-name"
	agentsClient := fakeAgentsClient{getSandbox: func(ctx context.Context, req *agentsv1.GetSandboxRequest) (*agentsv1.GetSandboxResponse, error) {
		if req.GetId() != sandboxID.String() {
			t.Fatalf("expected sandbox id %s, got %s", sandboxID, req.GetId())
		}
		return &agentsv1.GetSandboxResponse{Sandbox: &agentsv1.Sandbox{Name: sandboxName}}, nil
	}}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient, AgentsClient: agentsClient})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	resp, err := srv.GetVolume(ctx, &runnersv1.GetVolumeRequest{Id: volumeID.String()})
	if err != nil {
		t.Fatalf("GetVolume failed: %v", err)
	}
	volume := resp.GetVolume()
	if volume.GetOwnerKind() != runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX {
		t.Fatalf("expected sandbox owner kind, got %s", volume.GetOwnerKind())
	}
	if volume.GetOwnerId() != sandboxID.String() {
		t.Fatalf("expected owner id %s, got %s", sandboxID, volume.GetOwnerId())
	}
	if volume.GetOwnerName() != sandboxName {
		t.Fatalf("expected owner name %q, got %q", sandboxName, volume.GetOwnerName())
	}
	if volume.GetVolumeId() != "" || volume.GetVolumeDefinitionId() != "" || volume.GetVolumeName() != "" {
		t.Fatalf("expected empty definition fields, got id=%q definition_id=%q name=%q", volume.GetVolumeId(), volume.GetVolumeDefinitionId(), volume.GetVolumeName())
	}
	if len(volume.GetAttachments()) != 0 {
		t.Fatalf("expected no attachments, got %d", len(volume.GetAttachments()))
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListVolumesSandboxRuntimeOnlySkipsDefinitionEnrichment(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	runnerID := uuid.New()
	organizationID := uuid.New()
	sandboxID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()

	volumeIDRows := pgxmock.NewRows([]string{"volume_id"}).AddRow(nil)
	volumeIDQuery := "SELECT DISTINCT volume_id FROM volumes WHERE volumes.organization_id = $1 AND volumes.owner_kind = ANY($2) AND volumes.owner_id = ANY($3)"
	mockPool.ExpectQuery(regexp.QuoteMeta(volumeIDQuery)).
		WithArgs(organizationID, pgtype.FlatArray[string]{runtimeOwnerKindSandbox}, pgtype.FlatArray[uuid.UUID]{sandboxID}).
		WillReturnRows(volumeIDRows)

	limit := normalizePageSize(0)
	query := fmt.Sprintf("SELECT %s FROM volumes WHERE volumes.organization_id = $1 AND volumes.owner_kind = ANY($2) AND volumes.owner_id = ANY($3) ORDER BY COALESCE(volumes.volume_id::text, '') ASC, volumes.id ASC LIMIT $4", volumeColumns)
	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, nil, nil, runnerID, nil, organizationID, "10", volumeStatusActive, nil, nil, runtimeOwnerKindSandbox, sandboxID, now, now, int64(1), false, nil, nil, nil, nil, nil)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(organizationID, pgtype.FlatArray[string]{runtimeOwnerKindSandbox}, pgtype.FlatArray[uuid.UUID]{sandboxID}, int(limit)+1).
		WillReturnRows(rows)

	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
	}

	sandboxName := "sandbox-name"
	agentsClient := fakeAgentsClient{getSandbox: func(ctx context.Context, req *agentsv1.GetSandboxRequest) (*agentsv1.GetSandboxResponse, error) {
		return &agentsv1.GetSandboxResponse{Sandbox: &agentsv1.Sandbox{Name: sandboxName}}, nil
	}}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient, AgentsClient: agentsClient})
	organizationIDValue := organizationID.String()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	resp, err := srv.ListVolumes(ctx, &runnersv1.ListVolumesRequest{
		OrganizationId: &organizationIDValue,
		Filter: &runnersv1.ListVolumesFilter{
			OwnerKindIn: []runnersv1.RuntimeOwnerKind{runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX},
			OwnerIdIn:   []string{sandboxID.String()},
		},
	})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	if len(resp.GetVolumes()) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(resp.GetVolumes()))
	}
	volume := resp.GetVolumes()[0]
	if volume.GetOwnerKind() != runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX {
		t.Fatalf("expected sandbox owner kind, got %s", volume.GetOwnerKind())
	}
	if volume.GetOwnerId() != sandboxID.String() {
		t.Fatalf("expected owner id %s, got %s", sandboxID, volume.GetOwnerId())
	}
	if volume.GetOwnerName() != sandboxName {
		t.Fatalf("expected owner name %q, got %q", sandboxName, volume.GetOwnerName())
	}
	if volume.GetVolumeId() != "" || volume.GetVolumeDefinitionId() != "" || volume.GetVolumeName() != "" {
		t.Fatalf("expected empty definition fields, got id=%q definition_id=%q name=%q", volume.GetVolumeId(), volume.GetVolumeDefinitionId(), volume.GetVolumeName())
	}
	if len(volume.GetAttachments()) != 0 {
		t.Fatalf("expected no attachments, got %d", len(volume.GetAttachments()))
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// The Orchestrator reads volume records over the mesh with no identity, the
// same way it lists them: when CreateVolume reports the record already exists
// it reads the existing one back to check it matches before reusing it.
// Demanding an identity here failed that read, so a start that had already
// created its volumes could never be retried.
func TestGetVolumeServesTheInternalCallerWithoutAnIdentity(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	runnerID := uuid.New()
	organizationID := uuid.New()
	sandboxID := uuid.New()
	now := time.Now().UTC()

	query := fmt.Sprintf(`SELECT %s FROM volumes WHERE id = $1`, volumeColumns)
	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, nil, nil, runnerID, nil, organizationID, "10", volumeStatusActive, nil, nil, runtimeOwnerKindSandbox, sandboxID, now, now, int64(1), false, nil, nil, nil, nil, nil)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(volumeID).WillReturnRows(rows)

	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			t.Fatal("expected no authorization check for a caller with no identity")
			return nil, nil
		},
	}
	agentsClient := fakeAgentsClient{getSandbox: func(ctx context.Context, req *agentsv1.GetSandboxRequest) (*agentsv1.GetSandboxResponse, error) {
		return &agentsv1.GetSandboxResponse{Sandbox: &agentsv1.Sandbox{Name: "sandbox-name"}}, nil
	}}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient, AgentsClient: agentsClient})
	resp, err := srv.GetVolume(context.Background(), &runnersv1.GetVolumeRequest{Id: volumeID.String()})
	if err != nil {
		t.Fatalf("GetVolume failed: %v", err)
	}
	if resp.GetVolume().GetOwnerId() != sandboxID.String() {
		t.Fatalf("expected owner %s, got %s", sandboxID, resp.GetVolume().GetOwnerId())
	}
	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// A malformed identity is a broken caller, not an internal one.
func TestGetVolumeRejectsAMalformedIdentity(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}
	srv := New(Options{Pool: mockPool, AuthorizationClient: fakeAuthorizationClient{}})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, "not-a-uuid"))
	if _, err := srv.GetVolume(ctx, &runnersv1.GetVolumeRequest{Id: uuid.New().String()}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", err)
	}
}

func TestClosedVolumeStatuses(t *testing.T) {
	for status, closed := range map[string]bool{
		volumeStatusProvisioning: false,
		volumeStatusActive:       false,
		volumeStatusDeprovision:  false,
		volumeStatusDeleted:      true,
		volumeStatusFailed:       true,
	} {
		if isClosedVolumeStatus(status) != closed {
			t.Errorf("expected isClosedVolumeStatus(%q) = %v", status, closed)
		}
	}
}
