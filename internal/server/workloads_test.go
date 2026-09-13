package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var workloadRowColumns = []string{
	"id",
	"runner_id",
	"thread_id",
	"agent_id",
	"organization_id",
	"status",
	"agent_state",
	"failure_reason",
	"failure_message",
	"containers",
	"ziti_identity_id",
	"allocated_cpu_millicores",
	"allocated_ram_bytes",
	"flavor",
	"persistent_shells",
	"instance_id",
	"last_activity_at",
	"last_metering_sampled_at",
	"removed_at",
	"owner_kind",
	"owner_id",
	"created_at",
	"updated_at",
	"removal_confirmed_at",
}

type fakeNotificationsClient struct {
	publish func(ctx context.Context, req *notificationsv1.PublishRequest) (*notificationsv1.PublishResponse, error)
}

func (f fakeNotificationsClient) Publish(ctx context.Context, req *notificationsv1.PublishRequest, opts ...grpc.CallOption) (*notificationsv1.PublishResponse, error) {
	if f.publish == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return f.publish(ctx, req)
}

func (f fakeNotificationsClient) Subscribe(ctx context.Context, req *notificationsv1.SubscribeRequest, opts ...grpc.CallOption) (notificationsv1.NotificationsService_SubscribeClient, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func expectWorkloadInsert(t *testing.T, mockPool pgxmock.PgxPoolIface, input workloadInsertInput, workload workloadRecord) {
	matcher := regexp.QuoteMeta(fmt.Sprintf("INSERT INTO workloads (id, runner_id, thread_id, agent_id, organization_id, status, containers, ziti_identity_id, allocated_cpu_millicores, allocated_ram_bytes, flavor, persistent_shells, owner_kind, owner_id, last_activity_at, created_at, updated_at)\n\t    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW(), NOW(), NOW())\n\t    RETURNING %s", workloadColumns))
	mockPool.ExpectQuery(matcher).
		WithArgs(input.ID, input.RunnerID, nullableUUIDValue(input.ThreadID), nullableUUIDValue(input.AgentID), input.OrganizationID, input.Status, input.ContainersJSON, input.ZitiIdentityID, input.AllocatedCPUMillicores, input.AllocatedRAMBytes, input.Flavor, input.PersistentShells, input.OwnerKind, input.OwnerID).
		WillReturnRows(workloadRows(t, workload))
}

func workloadRows(t *testing.T, records ...workloadRecord) *pgxmock.Rows {
	t.Helper()
	rows := pgxmock.NewRows(workloadRowColumns)
	for _, record := range records {
		containersJSON, err := json.Marshal(record.Containers)
		if err != nil {
			t.Fatalf("marshal containers: %v", err)
		}
		failureReason := pgtype.Text{}
		if record.FailureReason != nil {
			failureReason = pgtype.Text{String: *record.FailureReason, Valid: true}
		}
		failureMessage := pgtype.Text{}
		if record.FailureMessage != nil {
			failureMessage = pgtype.Text{String: *record.FailureMessage, Valid: true}
		}
		instanceID := pgtype.Text{}
		if record.InstanceID != nil {
			instanceID = pgtype.Text{String: *record.InstanceID, Valid: true}
		}
		lastMeteringAt := pgtype.Timestamptz{}
		if record.LastMeteringAt != nil {
			lastMeteringAt = pgtype.Timestamptz{Time: *record.LastMeteringAt, Valid: true}
		}
		removedAt := pgtype.Timestamptz{}
		if record.RemovedAt != nil {
			removedAt = pgtype.Timestamptz{Time: *record.RemovedAt, Valid: true}
		}
		confirmation := pgtype.Timestamptz{}
		if record.RemovalConfirmedAt != nil {
			confirmation = pgtype.Timestamptz{Time: *record.RemovalConfirmedAt, Valid: true}
		}
		rows.AddRow(record.Meta.ID, record.RunnerID, pgUUIDValue(record.ThreadID), pgUUIDValue(record.AgentID), record.OrganizationID, record.Status, record.AgentState, failureReason, failureMessage, containersJSON, record.ZitiIdentityID, record.AllocatedCPUMillicores, record.AllocatedRAMBytes, record.Flavor, record.PersistentShells, instanceID, record.LastActivityAt, lastMeteringAt, removedAt, record.OwnerKind, record.OwnerID, record.Meta.CreatedAt, record.Meta.UpdatedAt, confirmation)
	}
	return rows
}

func defaultWorkloadRecord(workloadID, runnerID, threadID, agentID, organizationID uuid.UUID, now time.Time) workloadRecord {
	return workloadRecord{
		Meta: entityMeta{
			ID:        workloadID,
			CreatedAt: now,
			UpdatedAt: now,
		},
		RunnerID:               runnerID,
		ThreadID:               threadID,
		AgentID:                agentID,
		OwnerKind:              runtimeOwnerKindAgentInstance,
		OwnerID:                threadID,
		OrganizationID:         organizationID,
		Status:                 workloadStatusRunning,
		AgentState:             workloadAgentStateProcessing,
		Containers:             []containerRecord{},
		ZitiIdentityID:         "ziti-id",
		AllocatedCPUMillicores: 0,
		AllocatedRAMBytes:      0,
		LastActivityAt:         now,
	}
}

func TestCreateWorkloadWritesAuthorizationTuples(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")
	input := workloadInsertInput{
		ID:                     workloadID,
		RunnerID:               runnerID,
		ThreadID:               &threadID,
		AgentID:                &agentID,
		OwnerKind:              runtimeOwnerKindAgentInstance,
		OwnerID:                threadID,
		OrganizationID:         organizationID,
		Status:                 workloadStatusRunning,
		ContainersJSON:         containersJSON,
		ZitiIdentityID:         "ziti-id",
		AllocatedCPUMillicores: 250,
		AllocatedRAMBytes:      512,
	}
	expectWorkloadInsert(t, mockPool, input, defaultWorkloadRecord(workloadID, runnerID, threadID, agentID, organizationID, now))

	var gotWriteReq *authorizationv1.WriteRequest
	authorizationClient := fakeAuthorizationClient{write: func(ctx context.Context, req *authorizationv1.WriteRequest) (*authorizationv1.WriteResponse, error) {
		gotWriteReq = req
		return &authorizationv1.WriteResponse{}, nil
	}}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	resp, err := srv.CreateWorkload(context.Background(), &runnersv1.CreateWorkloadRequest{
		Id:                     workloadID.String(),
		RunnerId:               runnerID.String(),
		ThreadId:               threadID.String(),
		AgentId:                agentID.String(),
		OrganizationId:         organizationID.String(),
		Status:                 runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		OwnerKind:              runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
		OwnerId:                threadID.String(),
		ZitiIdentityId:         "ziti-id",
		AllocatedCpuMillicores: 250,
		AllocatedRamBytes:      512,
	})
	if err != nil {
		t.Fatalf("CreateWorkload failed: %v", err)
	}
	if resp.GetWorkload().GetMeta().GetId() != workloadID.String() {
		t.Fatalf("expected workload id %s, got %s", workloadID, resp.GetWorkload().GetMeta().GetId())
	}
	if gotWriteReq == nil {
		t.Fatal("expected authorization Write to be called")
	}
	assertWorkloadAuthorizationWrites(t, gotWriteReq, workloadID, organizationID, agentID, threadID)

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestCreateWorkloadReturnsInternalWhenAuthorizationWriteFails(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")
	input := workloadInsertInput{
		ID:             workloadID,
		RunnerID:       runnerID,
		ThreadID:       &threadID,
		AgentID:        &agentID,
		OwnerKind:      runtimeOwnerKindAgentInstance,
		OwnerID:        threadID,
		OrganizationID: organizationID,
		Status:         workloadStatusRunning,
		ContainersJSON: containersJSON,
	}
	expectWorkloadInsert(t, mockPool, input, defaultWorkloadRecord(workloadID, runnerID, threadID, agentID, organizationID, now))

	authorizationClient := fakeAuthorizationClient{write: func(ctx context.Context, req *authorizationv1.WriteRequest) (*authorizationv1.WriteResponse, error) {
		return nil, status.Error(codes.Unavailable, "authorization unavailable")
	}}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	_, err = srv.CreateWorkload(context.Background(), &runnersv1.CreateWorkloadRequest{
		Id:             workloadID.String(),
		RunnerId:       runnerID.String(),
		ThreadId:       threadID.String(),
		AgentId:        agentID.String(),
		OrganizationId: organizationID.String(),
		Status:         runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		OwnerKind:      runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
		OwnerId:        threadID.String(),
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal error, got %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestCreateWorkloadDoesNotWriteAuthorizationWhenInsertFails(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	containersJSON := []byte("[]")
	input := workloadInsertInput{
		ID:             workloadID,
		RunnerID:       runnerID,
		ThreadID:       &threadID,
		AgentID:        &agentID,
		OwnerKind:      runtimeOwnerKindAgentInstance,
		OwnerID:        threadID,
		OrganizationID: organizationID,
		Status:         workloadStatusRunning,
		ContainersJSON: containersJSON,
	}
	matcher := regexp.QuoteMeta(fmt.Sprintf("INSERT INTO workloads (id, runner_id, thread_id, agent_id, organization_id, status, containers, ziti_identity_id, allocated_cpu_millicores, allocated_ram_bytes, flavor, persistent_shells, owner_kind, owner_id, last_activity_at, created_at, updated_at)\n\t    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW(), NOW(), NOW())\n\t    RETURNING %s", workloadColumns))
	mockPool.ExpectQuery(matcher).
		WithArgs(input.ID, input.RunnerID, nullableUUIDValue(input.ThreadID), nullableUUIDValue(input.AgentID), input.OrganizationID, input.Status, input.ContainersJSON, input.ZitiIdentityID, input.AllocatedCPUMillicores, input.AllocatedRAMBytes, input.Flavor, input.PersistentShells, input.OwnerKind, input.OwnerID).
		WillReturnError(AlreadyExists("workload"))

	authorizationClient := fakeAuthorizationClient{write: func(ctx context.Context, req *authorizationv1.WriteRequest) (*authorizationv1.WriteResponse, error) {
		t.Fatal("authorization Write must not be called when workload insert fails")
		return &authorizationv1.WriteResponse{}, nil
	}}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	_, err = srv.CreateWorkload(context.Background(), &runnersv1.CreateWorkloadRequest{
		Id:             workloadID.String(),
		RunnerId:       runnerID.String(),
		ThreadId:       threadID.String(),
		AgentId:        agentID.String(),
		OrganizationId: organizationID.String(),
		Status:         runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		OwnerKind:      runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
		OwnerId:        threadID.String(),
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists error, got %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestCreateWorkloadMapsAgentInstanceIDToOwnerID(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	agentInstanceID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")
	input := workloadInsertInput{
		ID:             workloadID,
		RunnerID:       runnerID,
		ThreadID:       &threadID,
		AgentID:        &agentID,
		OwnerKind:      runtimeOwnerKindAgentInstance,
		OwnerID:        agentInstanceID,
		OrganizationID: organizationID,
		Status:         workloadStatusRunning,
		ContainersJSON: containersJSON,
	}
	workload := defaultWorkloadRecord(workloadID, runnerID, threadID, agentID, organizationID, now)
	workload.OwnerID = agentInstanceID
	expectWorkloadInsert(t, mockPool, input, workload)

	var gotWriteReq *authorizationv1.WriteRequest
	authorizationClient := fakeAuthorizationClient{write: func(ctx context.Context, req *authorizationv1.WriteRequest) (*authorizationv1.WriteResponse, error) {
		gotWriteReq = req
		return &authorizationv1.WriteResponse{}, nil
	}}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	resp, err := srv.CreateWorkload(context.Background(), &runnersv1.CreateWorkloadRequest{
		Id:              workloadID.String(),
		RunnerId:        runnerID.String(),
		ThreadId:        threadID.String(),
		AgentClassId:    ptr(agentID.String()),
		OrganizationId:  organizationID.String(),
		Status:          runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		OwnerKind:       runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
		AgentInstanceId: ptr(agentInstanceID.String()),
	})
	if err != nil {
		t.Fatalf("CreateWorkload failed: %v", err)
	}
	if resp.GetWorkload().GetOwnerId() != agentInstanceID.String() {
		t.Fatalf("expected owner id %s, got %s", agentInstanceID, resp.GetWorkload().GetOwnerId())
	}
	if resp.GetWorkload().GetAgentInstanceId() != agentInstanceID.String() {
		t.Fatalf("expected agent instance id %s, got %s", agentInstanceID, resp.GetWorkload().GetAgentInstanceId())
	}
	if resp.GetWorkload().GetInstanceId() != "" {
		t.Fatalf("expected runner-local instance id to stay empty, got %q", resp.GetWorkload().GetInstanceId())
	}
	if gotWriteReq == nil {
		t.Fatal("expected authorization Write to be called")
	}
	assertWorkloadAuthorizationWrites(t, gotWriteReq, workloadID, organizationID, agentID, agentInstanceID)

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestCreateWorkloadRejectsMismatchedAgentInstanceID(t *testing.T) {
	srv := New(Options{})
	agentInstanceID := uuid.New()
	ownerID := uuid.New()

	_, err := srv.CreateWorkload(context.Background(), &runnersv1.CreateWorkloadRequest{
		Id:              uuid.New().String(),
		RunnerId:        uuid.New().String(),
		ThreadId:        uuid.New().String(),
		AgentClassId:    ptr(uuid.New().String()),
		OrganizationId:  uuid.New().String(),
		Status:          runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		OwnerKind:       runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
		OwnerId:         ownerID.String(),
		AgentInstanceId: ptr(agentInstanceID.String()),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument error, got %v", err)
	}
}

// viewers are the identities expected to hold owner_agent: the agent class and
// the workload's own runtime identity, which is who asks from inside it.
func assertWorkloadAuthorizationWrites(t *testing.T, req *authorizationv1.WriteRequest, workloadID, organizationID uuid.UUID, viewers ...uuid.UUID) {
	t.Helper()
	expected := []*authorizationv1.TupleKey{
		{
			User:     organizationObject(organizationID),
			Relation: workloadOrgRelation,
			Object:   workloadObject(workloadID),
		},
	}
	for _, viewer := range viewers {
		expected = append(expected, &authorizationv1.TupleKey{
			User:     identityObject(viewer),
			Relation: workloadOwnerAgentRelation,
			Object:   workloadObject(workloadID),
		})
	}
	writes := req.GetWrites()
	if len(writes) != len(expected) {
		t.Fatalf("expected %d authorization writes, got %d", len(expected), len(writes))
	}
	for idx, tuple := range expected {
		if writes[idx].GetUser() != tuple.GetUser() || writes[idx].GetRelation() != tuple.GetRelation() || writes[idx].GetObject() != tuple.GetObject() {
			t.Fatalf("expected write %d to be %v, got %v", idx, tuple, writes[idx])
		}
	}
	if len(req.GetDeletes()) != 0 {
		t.Fatalf("expected no authorization deletes, got %d", len(req.GetDeletes()))
	}
}

func TestListWorkloadsFiltersOrganization(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)

	query := fmt.Sprintf("SELECT %s FROM workloads WHERE workloads.organization_id = $1 ORDER BY workloads.created_at DESC, workloads.id ASC LIMIT $2", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(organizationID, 51).
		WillReturnRows(rows)

	runnerName := "runner-name"
	runnerRows := pgxmock.NewRows([]string{"id", "name"}).AddRow(runnerID, runnerName)
	mockPool.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM runners WHERE id = ANY($1)")).
		WithArgs(pgtype.FlatArray[uuid.UUID]([]uuid.UUID{runnerID})).
		WillReturnRows(runnerRows)

	agentName := "agent-name"
	agentsClient := fakeAgentsClient{getAgent: func(ctx context.Context, req *agentsv1.GetAgentRequest) (*agentsv1.GetAgentResponse, error) {
		return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Name: agentName}}, nil
	}}

	var gotCheckReqs []*authorizationv1.CheckRequest
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			gotCheckReqs = append(gotCheckReqs, req)
			allowed := req.GetTupleKey().GetRelation() == organizationViewWorkloads
			return &authorizationv1.CheckResponse{Allowed: allowed}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient, AgentsClient: agentsClient})
	organizationIDValue := organizationID.String()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	resp, err := srv.ListWorkloads(ctx, &runnersv1.ListWorkloadsRequest{OrganizationId: &organizationIDValue})
	if err != nil {
		t.Fatalf("ListWorkloads failed: %v", err)
	}
	if len(resp.GetWorkloads()) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(resp.GetWorkloads()))
	}
	if resp.GetWorkloads()[0].GetOrganizationId() != organizationID.String() {
		t.Fatalf("expected organization id %q, got %q", organizationID.String(), resp.GetWorkloads()[0].GetOrganizationId())
	}
	if resp.GetWorkloads()[0].GetAgentName() != agentName {
		t.Fatalf("expected agent name %q, got %q", agentName, resp.GetWorkloads()[0].GetAgentName())
	}
	if resp.GetWorkloads()[0].GetRunnerName() != runnerName {
		t.Fatalf("expected runner name %q, got %q", runnerName, resp.GetWorkloads()[0].GetRunnerName())
	}
	if len(gotCheckReqs) != 1 {
		t.Fatalf("expected 1 authorization check, got %d", len(gotCheckReqs))
	}
	if gotCheckReqs[0].GetTupleKey().GetRelation() != organizationViewWorkloads {
		t.Fatalf("expected view workloads relation, got %s", gotCheckReqs[0].GetTupleKey().GetRelation())
	}
	if gotCheckReqs[0].GetTupleKey().GetObject() != organizationObject(organizationID) {
		t.Fatalf("expected organization object %q, got %q", organizationObject(organizationID), gotCheckReqs[0].GetTupleKey().GetObject())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListWorkloadsInternalNoIdentity(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)

	limit := normalizePageSize(0)
	query := fmt.Sprintf("SELECT %s FROM workloads ORDER BY workloads.created_at DESC, workloads.id ASC LIMIT $1", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(int(limit) + 1).
		WillReturnRows(rows)

	runnerName := "runner-name"
	runnerRows := pgxmock.NewRows([]string{"id", "name"}).AddRow(runnerID, runnerName)
	mockPool.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM runners WHERE id = ANY($1)")).
		WithArgs(pgtype.FlatArray[uuid.UUID]([]uuid.UUID{runnerID})).
		WillReturnRows(runnerRows)

	agentName := "agent-name"
	agentsClient := fakeAgentsClient{getAgent: func(ctx context.Context, req *agentsv1.GetAgentRequest) (*agentsv1.GetAgentResponse, error) {
		return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Name: agentName}}, nil
	}}

	checkCalls := 0
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			checkCalls++
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient, AgentsClient: agentsClient})
	resp, err := srv.ListWorkloads(context.Background(), &runnersv1.ListWorkloadsRequest{})
	if err != nil {
		t.Fatalf("ListWorkloads failed: %v", err)
	}
	if len(resp.GetWorkloads()) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(resp.GetWorkloads()))
	}
	if resp.GetWorkloads()[0].GetOrganizationId() != organizationID.String() {
		t.Fatalf("expected organization id %q, got %q", organizationID.String(), resp.GetWorkloads()[0].GetOrganizationId())
	}
	if resp.GetWorkloads()[0].GetAgentName() != agentName {
		t.Fatalf("expected agent name %q, got %q", agentName, resp.GetWorkloads()[0].GetAgentName())
	}
	if resp.GetWorkloads()[0].GetRunnerName() != runnerName {
		t.Fatalf("expected runner name %q, got %q", runnerName, resp.GetWorkloads()[0].GetRunnerName())
	}
	if checkCalls != 0 {
		t.Fatalf("expected no authorization checks, got %d", checkCalls)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListWorkloadsFiltersRunner(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)

	query := fmt.Sprintf("SELECT %s FROM workloads WHERE workloads.organization_id = $1 AND workloads.runner_id = ANY($2) ORDER BY workloads.created_at DESC, workloads.id ASC LIMIT $3", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(organizationID, pgtype.FlatArray[uuid.UUID]([]uuid.UUID{runnerID}), 51).
		WillReturnRows(rows)

	runnerName := "runner-name"
	runnerRows := pgxmock.NewRows([]string{"id", "name"}).AddRow(runnerID, runnerName)
	mockPool.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM runners WHERE id = ANY($1)")).
		WithArgs(pgtype.FlatArray[uuid.UUID]([]uuid.UUID{runnerID})).
		WillReturnRows(runnerRows)

	agentName := "agent-name"
	agentsClient := fakeAgentsClient{getAgent: func(ctx context.Context, req *agentsv1.GetAgentRequest) (*agentsv1.GetAgentResponse, error) {
		return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Name: agentName}}, nil
	}}

	var gotCheckReqs []*authorizationv1.CheckRequest
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			gotCheckReqs = append(gotCheckReqs, req)
			allowed := req.GetTupleKey().GetRelation() == organizationViewWorkloads
			return &authorizationv1.CheckResponse{Allowed: allowed}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient, AgentsClient: agentsClient})
	runnerIDValue := runnerID.String()
	organizationIDValue := organizationID.String()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	resp, err := srv.ListWorkloads(ctx, &runnersv1.ListWorkloadsRequest{OrganizationId: &organizationIDValue, RunnerId: &runnerIDValue})
	if err != nil {
		t.Fatalf("ListWorkloads failed: %v", err)
	}
	if len(resp.GetWorkloads()) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(resp.GetWorkloads()))
	}
	if resp.GetWorkloads()[0].GetRunnerId() != runnerID.String() {
		t.Fatalf("expected runner id %q, got %q", runnerID.String(), resp.GetWorkloads()[0].GetRunnerId())
	}
	if resp.GetWorkloads()[0].GetAgentName() != agentName {
		t.Fatalf("expected agent name %q, got %q", agentName, resp.GetWorkloads()[0].GetAgentName())
	}
	if resp.GetWorkloads()[0].GetRunnerName() != runnerName {
		t.Fatalf("expected runner name %q, got %q", runnerName, resp.GetWorkloads()[0].GetRunnerName())
	}
	if len(gotCheckReqs) != 1 {
		t.Fatalf("expected 1 authorization check, got %d", len(gotCheckReqs))
	}
	if gotCheckReqs[0].GetTupleKey().GetRelation() != organizationViewWorkloads {
		t.Fatalf("expected view workloads relation, got %s", gotCheckReqs[0].GetTupleKey().GetRelation())
	}
	if gotCheckReqs[0].GetTupleKey().GetObject() != organizationObject(organizationID) {
		t.Fatalf("expected organization object %q, got %q", organizationObject(organizationID), gotCheckReqs[0].GetTupleKey().GetObject())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListWorkloadsPendingSample(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)

	query := fmt.Sprintf("SELECT %s FROM workloads WHERE workloads.organization_id = $1 AND %s ORDER BY workloads.created_at DESC, workloads.id ASC LIMIT $2", workloadColumns, pendingSampleClause)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(organizationID, 51).
		WillReturnRows(rows)

	runnerName := "runner-name"
	runnerRows := pgxmock.NewRows([]string{"id", "name"}).AddRow(runnerID, runnerName)
	mockPool.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM runners WHERE id = ANY($1)")).
		WithArgs(pgtype.FlatArray[uuid.UUID]([]uuid.UUID{runnerID})).
		WillReturnRows(runnerRows)

	agentName := "agent-name"
	agentsClient := fakeAgentsClient{getAgent: func(ctx context.Context, req *agentsv1.GetAgentRequest) (*agentsv1.GetAgentResponse, error) {
		return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Name: agentName}}, nil
	}}

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
	resp, err := srv.ListWorkloads(ctx, &runnersv1.ListWorkloadsRequest{OrganizationId: &organizationIDValue, PendingSample: &pendingSample})
	if err != nil {
		t.Fatalf("ListWorkloads failed: %v", err)
	}
	if len(resp.GetWorkloads()) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(resp.GetWorkloads()))
	}
	if gotCheckReq == nil {
		t.Fatal("expected authorization Check to be called")
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListWorkloadsCursorPagination(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	cursorTime := now.Add(-5 * time.Minute)
	cursorID := uuid.New()
	primary := cursorTime.Format(time.RFC3339Nano)
	pageToken, err := encodeListCursor(primary, cursorID)
	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}

	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)

	pageSize := int32(2)
	limit := normalizePageSize(pageSize)
	query := fmt.Sprintf("SELECT %s FROM workloads WHERE workloads.organization_id = $1 AND (workloads.created_at < $2 OR (workloads.created_at = $2 AND workloads.id > $3)) ORDER BY workloads.created_at DESC, workloads.id ASC LIMIT $4", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(organizationID, cursorTime, cursorID, int(limit)+1).
		WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	filter := workloadListFilter{OrganizationID: &organizationID}
	sort, err := parseWorkloadSort(nil)
	if err != nil {
		t.Fatalf("parse sort: %v", err)
	}
	workloads, nextToken, err := srv.listWorkloads(context.Background(), filter, sort, pageSize, pageToken)
	if err != nil {
		t.Fatalf("listWorkloads failed: %v", err)
	}
	if len(workloads) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(workloads))
	}
	if nextToken != "" {
		t.Fatalf("expected empty next token, got %q", nextToken)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListWorkloadsSortByAgentQuery(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	agentRows := pgxmock.NewRows([]string{"agent_id"}).AddRow(agentID)
	agentQuery := "SELECT DISTINCT agent_id FROM workloads WHERE workloads.organization_id = $1"
	mockPool.ExpectQuery(regexp.QuoteMeta(agentQuery)).WithArgs(organizationID).WillReturnRows(agentRows)

	agentName := "Agent Alpha"
	primary := strings.ToLower(agentName)
	cursorID := uuid.New()
	pageToken, err := encodeListCursor(primary, cursorID)
	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}

	pageSize := int32(1)
	limit := normalizePageSize(pageSize)
	sortExpr := "CASE workloads.agent_id WHEN $2 THEN $3 ELSE ''::text END"
	query := fmt.Sprintf("SELECT %s FROM workloads WHERE workloads.organization_id = $1 AND (%s > $4 OR (%s = $4 AND workloads.id > $5)) ORDER BY %s ASC, workloads.id ASC LIMIT $6", workloadColumns, sortExpr, sortExpr, sortExpr)
	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(organizationID, agentID, primary, primary, cursorID, int(limit)+1).
		WillReturnRows(rows)

	agentsClient := fakeAgentsClient{
		getAgent: func(ctx context.Context, req *agentsv1.GetAgentRequest) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Name: agentName}}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AgentsClient: agentsClient})
	filter := workloadListFilter{OrganizationID: &organizationID}
	sort := workloadListSort{Field: workloadSortAgent, Direction: sortAsc}
	workloads, nextToken, err := srv.listWorkloads(context.Background(), filter, sort, pageSize, pageToken)
	if err != nil {
		t.Fatalf("listWorkloads failed: %v", err)
	}
	if len(workloads) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(workloads))
	}
	if nextToken != "" {
		t.Fatalf("expected empty next token, got %q", nextToken)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestWorkloadAgentSortPrimaryValues(t *testing.T) {
	agentID := uuid.New()
	sandboxID := uuid.New()
	agentNames := map[uuid.UUID]string{agentID: "  Agent Alpha  "}

	cases := []struct {
		name    string
		record  workloadRecord
		want    string
		wantErr bool
	}{
		{
			name:   "agent instance owner uses lowercased agent name",
			record: workloadRecord{Meta: entityMeta{ID: uuid.New()}, AgentID: agentID, OwnerKind: runtimeOwnerKindAgentInstance, OwnerID: uuid.New()},
			want:   "agent alpha",
		},
		{
			name:   "sandbox owner falls into the keyless bucket",
			record: workloadRecord{Meta: entityMeta{ID: uuid.New()}, OwnerKind: runtimeOwnerKindSandbox, OwnerID: sandboxID},
			want:   workloadAgentSortKeyless,
		},
		{
			name:    "unresolved agent name is an error",
			record:  workloadRecord{Meta: entityMeta{ID: uuid.New()}, AgentID: uuid.New(), OwnerKind: runtimeOwnerKindAgentInstance, OwnerID: uuid.New()},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := workloadPrimaryValue(tc.record, workloadSortAgent, agentNames, nil)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("workloadPrimaryValue failed: %v", err)
			}
			if got != tc.want {
				t.Fatalf("expected primary %q, got %q", tc.want, got)
			}

			token, err := encodeListCursor(got, tc.record.Meta.ID)
			if err != nil {
				t.Fatalf("encode cursor: %v", err)
			}
			cursor, cursorID, err := decodeListCursor(token)
			if err != nil {
				t.Fatalf("decode cursor: %v", err)
			}
			if cursorID != tc.record.Meta.ID {
				t.Fatalf("expected cursor id %s, got %s", tc.record.Meta.ID, cursorID)
			}
			primary, err := workloadCursorPrimary(workloadSortAgent, cursor)
			if err != nil {
				t.Fatalf("workloadCursorPrimary failed: %v", err)
			}
			if primary != tc.want {
				t.Fatalf("expected cursor primary %q, got %q", tc.want, primary)
			}
		})
	}
}

func TestBuildWorkloadAgentSortExpr(t *testing.T) {
	first := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	second := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	cases := []struct {
		name       string
		agentNames map[uuid.UUID]string
		wantExpr   string
		wantArgs   []any
	}{
		{
			name:       "no agent classes in scope",
			agentNames: map[uuid.UUID]string{},
			wantExpr:   "''::text",
		},
		{
			name:       "agent classes bucket sandbox rows into the ELSE branch",
			agentNames: map[uuid.UUID]string{first: " Agent Alpha ", second: "Agent Beta"},
			wantExpr:   "CASE workloads.agent_id WHEN $2 THEN $3 WHEN $4 THEN $5 ELSE ''::text END",
			wantArgs:   []any{first, "agent alpha", second, "agent beta"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expr, args := buildWorkloadAgentSortExpr(tc.agentNames, 2)
			if expr != tc.wantExpr {
				t.Fatalf("expected expr %q, got %q", tc.wantExpr, expr)
			}
			if len(args) != len(tc.wantArgs) {
				t.Fatalf("expected %d args, got %v", len(tc.wantArgs), args)
			}
			for i, want := range tc.wantArgs {
				if args[i] != want {
					t.Fatalf("expected arg %d to be %v, got %v", i, want, args[i])
				}
			}
		})
	}
}

func TestListWorkloadsSortByAgentMixedOwnerPage(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	agentWorkloadID := uuid.MustParse("cccccccc-cccc-cccc-cccc-cccccccccccc")
	firstSandboxWorkloadID := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	secondSandboxWorkloadID := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	sandboxID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	agentName := "Agent Alpha"
	agentPrimary := strings.ToLower(agentName)
	sortExpr := "CASE workloads.agent_id WHEN $2 THEN $3 ELSE ''::text END"

	// DISTINCT agent_id yields a NULL row for the sandbox-owned workloads.
	agentRows := pgxmock.NewRows([]string{"agent_id"}).AddRow(nil).AddRow(agentID)
	agentQuery := "SELECT DISTINCT agent_id FROM workloads WHERE workloads.organization_id = $1"
	mockPool.ExpectQuery(regexp.QuoteMeta(agentQuery)).WithArgs(organizationID).WillReturnRows(agentRows)

	pageSize := int32(2)
	limit := normalizePageSize(pageSize)
	firstQuery := fmt.Sprintf("SELECT %s FROM workloads WHERE workloads.organization_id = $1 ORDER BY %s ASC, workloads.id ASC LIMIT $4", workloadColumns, sortExpr)
	firstRows := pgxmock.NewRows(workloadRowColumns).
		AddRow(firstSandboxWorkloadID, runnerID, nil, nil, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindSandbox, sandboxID, now, now, nil).
		AddRow(secondSandboxWorkloadID, runnerID, nil, nil, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindSandbox, sandboxID, now, now, nil).
		AddRow(agentWorkloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)
	mockPool.ExpectQuery(regexp.QuoteMeta(firstQuery)).
		WithArgs(organizationID, agentID, agentPrimary, int(limit)+1).
		WillReturnRows(firstRows)

	agentsClient := fakeAgentsClient{
		getAgent: func(ctx context.Context, req *agentsv1.GetAgentRequest) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Name: agentName}}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AgentsClient: agentsClient})
	filter := workloadListFilter{OrganizationID: &organizationID}
	sort := workloadListSort{Field: workloadSortAgent, Direction: sortAsc}
	workloads, nextToken, err := srv.listWorkloads(context.Background(), filter, sort, pageSize, "")
	if err != nil {
		t.Fatalf("listWorkloads failed: %v", err)
	}
	if len(workloads) != 2 {
		t.Fatalf("expected 2 workloads, got %d", len(workloads))
	}
	if nextToken == "" {
		t.Fatal("expected a next page token for a sandbox-owned last row")
	}

	cursor, cursorID, err := decodeListCursor(nextToken)
	if err != nil {
		t.Fatalf("decode next token: %v", err)
	}
	if cursorID != secondSandboxWorkloadID {
		t.Fatalf("expected cursor id %s, got %s", secondSandboxWorkloadID, cursorID)
	}
	if cursor.Primary != workloadAgentSortKeyless {
		t.Fatalf("expected keyless cursor primary, got %q", cursor.Primary)
	}

	// Round-trip: the token from the sandbox-owned row drives the next page.
	mockPool.ExpectQuery(regexp.QuoteMeta(agentQuery)).WithArgs(organizationID).
		WillReturnRows(pgxmock.NewRows([]string{"agent_id"}).AddRow(nil).AddRow(agentID))

	secondQuery := fmt.Sprintf("SELECT %s FROM workloads WHERE workloads.organization_id = $1 AND (%s > $4 OR (%s = $4 AND workloads.id > $5)) ORDER BY %s ASC, workloads.id ASC LIMIT $6", workloadColumns, sortExpr, sortExpr, sortExpr)
	secondRows := pgxmock.NewRows(workloadRowColumns).
		AddRow(agentWorkloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)
	mockPool.ExpectQuery(regexp.QuoteMeta(secondQuery)).
		WithArgs(organizationID, agentID, agentPrimary, workloadAgentSortKeyless, secondSandboxWorkloadID, int(limit)+1).
		WillReturnRows(secondRows)

	workloads, nextToken, err = srv.listWorkloads(context.Background(), filter, sort, pageSize, nextToken)
	if err != nil {
		t.Fatalf("listWorkloads second page failed: %v", err)
	}
	if len(workloads) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(workloads))
	}
	if workloads[0].Meta.ID != agentWorkloadID {
		t.Fatalf("expected agent workload %s, got %s", agentWorkloadID, workloads[0].Meta.ID)
	}
	if nextToken != "" {
		t.Fatalf("expected empty next token, got %q", nextToken)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListWorkloadsSortByRunnerQuery(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	primary := "runner-omega"
	cursorID := uuid.New()
	pageToken, err := encodeListCursor(primary, cursorID)
	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}

	pageSize := int32(1)
	limit := normalizePageSize(pageSize)
	sortColumn := "LOWER(runners.name)"
	query := fmt.Sprintf("SELECT %s FROM workloads JOIN runners ON workloads.runner_id = runners.id WHERE workloads.organization_id = $1 AND (%s < $2 OR (%s = $2 AND workloads.id > $3)) ORDER BY %s DESC, workloads.id ASC LIMIT $4", workloadColumns, sortColumn, sortColumn, sortColumn)
	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(organizationID, primary, cursorID, int(limit)+1).
		WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	filter := workloadListFilter{OrganizationID: &organizationID}
	sort := workloadListSort{Field: workloadSortRunner, Direction: sortDesc}
	workloads, nextToken, err := srv.listWorkloads(context.Background(), filter, sort, pageSize, pageToken)
	if err != nil {
		t.Fatalf("listWorkloads failed: %v", err)
	}
	if len(workloads) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(workloads))
	}
	if nextToken != "" {
		t.Fatalf("expected empty next token, got %q", nextToken)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListWorkloadsInvalidUUID(t *testing.T) {
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
		req  *runnersv1.ListWorkloadsRequest
	}{
		{
			name: "organization_id_missing",
			req:  &runnersv1.ListWorkloadsRequest{},
		},
		{
			name: "organization_id",
			req: func() *runnersv1.ListWorkloadsRequest {
				value := "not-a-uuid"
				return &runnersv1.ListWorkloadsRequest{OrganizationId: &value}
			}(),
		},
		{
			name: "runner_id",
			req: func() *runnersv1.ListWorkloadsRequest {
				value := "not-a-uuid"
				return &runnersv1.ListWorkloadsRequest{OrganizationId: &organizationIDValue, RunnerId: &value}
			}(),
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := srv.ListWorkloads(ctx, testCase.req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument error, got %v", err)
			}
		})
	}
}

func TestListWorkloadsInvalidPageToken(t *testing.T) {
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
	}
	srv := New(Options{AuthorizationClient: authorizationClient})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, uuid.NewString()))
	organizationIDValue := uuid.NewString()
	validID := uuid.NewString()

	invalidJSONToken := base64.RawURLEncoding.EncodeToString([]byte("not-json"))
	wrongPrimaryToken := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"primary":10,"id":"%s"}`, validID)))

	cases := []string{"not-a-token", invalidJSONToken, wrongPrimaryToken}
	for _, token := range cases {
		_, err := srv.ListWorkloads(ctx, &runnersv1.ListWorkloadsRequest{OrganizationId: &organizationIDValue, PageToken: token})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected InvalidArgument error for token %q, got %v", token, err)
		}
	}
}

func TestListWorkloadsRequiresViewWorkloads(t *testing.T) {
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
	_, err = srv.ListWorkloads(ctx, &runnersv1.ListWorkloadsRequest{OrganizationId: &organizationIDValue})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied error, got %v", err)
	}
	if len(gotCheckReqs) != 1 {
		t.Fatalf("expected 1 authorization check, got %d", len(gotCheckReqs))
	}
	if gotCheckReqs[0].GetTupleKey().GetRelation() != organizationViewWorkloads {
		t.Fatalf("expected view workloads relation, got %s", gotCheckReqs[0].GetTupleKey().GetRelation())
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

func TestListWorkloadsByThreadFilters(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")
	statuses := []string{workloadStatusRunning, workloadStatusFailed}
	pageSize := int32(5)
	limit := normalizePageSize(pageSize)

	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)

	query := fmt.Sprintf("SELECT %s FROM workloads WHERE thread_id = $1 AND agent_id = $2 AND status = ANY($3) ORDER BY created_at DESC, id DESC LIMIT $4", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(threadID, agentID, pgtype.FlatArray[string](statuses), int(limit)+1).
		WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	workloads, nextToken, err := srv.listWorkloadsByThread(context.Background(), threadID, &agentID, statuses, pageSize, "")
	if err != nil {
		t.Fatalf("listWorkloadsByThread failed: %v", err)
	}
	if len(workloads) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(workloads))
	}
	if nextToken != "" {
		t.Fatalf("expected empty next token, got %q", nextToken)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListWorkloadsByThreadInternalNoIdentity(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")
	limit := normalizePageSize(0)

	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)

	query := fmt.Sprintf("SELECT %s FROM workloads WHERE thread_id = $1 ORDER BY created_at DESC, id DESC LIMIT $2", workloadColumns)
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
	resp, err := srv.ListWorkloadsByThread(context.Background(), &runnersv1.ListWorkloadsByThreadRequest{ThreadId: threadID.String()})
	if err != nil {
		t.Fatalf("ListWorkloadsByThread failed: %v", err)
	}
	if len(resp.GetWorkloads()) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(resp.GetWorkloads()))
	}
	if resp.GetWorkloads()[0].GetOrganizationId() != organizationID.String() {
		t.Fatalf("expected organization id %q, got %q", organizationID.String(), resp.GetWorkloads()[0].GetOrganizationId())
	}
	if resp.GetWorkloads()[0].GetAgentState() != runnersv1.WorkloadAgentState_WORKLOAD_AGENT_STATE_PROCESSING {
		t.Fatalf("expected agent state %v, got %v", runnersv1.WorkloadAgentState_WORKLOAD_AGENT_STATE_PROCESSING, resp.GetWorkloads()[0].GetAgentState())
	}
	if checkCalls != 0 {
		t.Fatalf("expected no authorization checks, got %d", checkCalls)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListWorkloadsByThreadUsesViewWorkloadsRelation(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")
	limit := normalizePageSize(0)

	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)

	query := fmt.Sprintf("SELECT %s FROM workloads WHERE thread_id = $1 ORDER BY created_at DESC, id DESC LIMIT $2", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(threadID, int(limit)+1).
		WillReturnRows(rows)

	var gotCheckReqs []*authorizationv1.CheckRequest
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			gotCheckReqs = append(gotCheckReqs, req)
			allowed := req.GetTupleKey().GetRelation() == organizationViewWorkloads
			return &authorizationv1.CheckResponse{Allowed: allowed}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	resp, err := srv.ListWorkloadsByThread(ctx, &runnersv1.ListWorkloadsByThreadRequest{ThreadId: threadID.String()})
	if err != nil {
		t.Fatalf("ListWorkloadsByThread failed: %v", err)
	}
	if len(resp.GetWorkloads()) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(resp.GetWorkloads()))
	}
	if resp.GetWorkloads()[0].GetOrganizationId() != organizationID.String() {
		t.Fatalf("expected organization id %q, got %q", organizationID.String(), resp.GetWorkloads()[0].GetOrganizationId())
	}
	if len(gotCheckReqs) != 1 {
		t.Fatalf("expected 1 authorization check, got %d", len(gotCheckReqs))
	}
	if gotCheckReqs[0].GetTupleKey().GetRelation() != organizationViewWorkloads {
		t.Fatalf("expected view workloads relation, got %s", gotCheckReqs[0].GetTupleKey().GetRelation())
	}
	if gotCheckReqs[0].GetTupleKey().GetObject() != organizationObject(organizationID) {
		t.Fatalf("expected organization object %q, got %q", organizationObject(organizationID), gotCheckReqs[0].GetTupleKey().GetObject())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListWorkloadsByThreadPagination(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	threadID := uuid.New()
	runnerID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	containersJSON := []byte("[]")
	pageSize := int32(2)
	limit := normalizePageSize(pageSize)

	cursorTime := time.Now().UTC().Add(-10 * time.Minute)
	cursorID := uuid.New()
	pageToken := encodeWorkloadCursor(cursorTime, cursorID)

	firstAt := cursorTime.Add(-1 * time.Minute)
	secondAt := cursorTime.Add(-2 * time.Minute)
	thirdAt := cursorTime.Add(-3 * time.Minute)
	firstID := uuid.New()
	secondID := uuid.New()
	thirdID := uuid.New()

	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(firstID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, firstAt, nil, nil, runtimeOwnerKindAgentInstance, firstID, firstAt, firstAt, nil).
		AddRow(secondID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, secondAt, nil, nil, runtimeOwnerKindAgentInstance, secondID, secondAt, secondAt, nil).
		AddRow(thirdID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, thirdAt, nil, nil, runtimeOwnerKindAgentInstance, thirdID, thirdAt, thirdAt, nil)

	query := fmt.Sprintf("SELECT %s FROM workloads WHERE thread_id = $1 AND (created_at < $2 OR (created_at = $2 AND id < $3)) ORDER BY created_at DESC, id DESC LIMIT $4", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(threadID, cursorTime, cursorID, int(limit)+1).
		WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	workloads, nextToken, err := srv.listWorkloadsByThread(context.Background(), threadID, nil, nil, pageSize, pageToken)
	if err != nil {
		t.Fatalf("listWorkloadsByThread failed: %v", err)
	}
	if len(workloads) != int(limit) {
		t.Fatalf("expected %d workloads, got %d", limit, len(workloads))
	}
	expectedToken := encodeWorkloadCursor(secondAt, secondID)
	if nextToken != expectedToken {
		t.Fatalf("expected next token %q, got %q", expectedToken, nextToken)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListWorkloadsByThreadPaginationTieBreak(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	threadID := uuid.New()
	runnerID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	containersJSON := []byte("[]")
	pageSize := int32(1)
	limit := normalizePageSize(pageSize)

	createdAt := time.Now().UTC()
	firstID := uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
	secondID := uuid.MustParse("00000000-0000-0000-0000-000000000000")

	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(firstID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, createdAt, nil, nil, runtimeOwnerKindAgentInstance, firstID, createdAt, createdAt, nil).
		AddRow(secondID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, createdAt, nil, nil, runtimeOwnerKindAgentInstance, secondID, createdAt, createdAt, nil)

	query := fmt.Sprintf("SELECT %s FROM workloads WHERE thread_id = $1 ORDER BY created_at DESC, id DESC LIMIT $2", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(threadID, int(limit)+1).
		WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	workloads, nextToken, err := srv.listWorkloadsByThread(context.Background(), threadID, nil, nil, pageSize, "")
	if err != nil {
		t.Fatalf("listWorkloadsByThread failed: %v", err)
	}
	if len(workloads) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(workloads))
	}
	if workloads[0].Meta.ID != firstID {
		t.Fatalf("expected first workload id %s, got %s", firstID, workloads[0].Meta.ID)
	}
	expectedToken := encodeWorkloadCursor(createdAt, firstID)
	if nextToken != expectedToken {
		t.Fatalf("expected next token %q, got %q", expectedToken, nextToken)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListWorkloadsByThreadInvalidPageToken(t *testing.T) {
	srv := New(Options{})
	threadID := uuid.New()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, uuid.NewString()))
	_, err := srv.ListWorkloadsByThread(ctx, &runnersv1.ListWorkloadsByThreadRequest{ThreadId: threadID.String(), PageToken: "not-a-token"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument error, got %v", err)
	}
}

func TestListWorkloadsByAgentInstanceFiltersOwner(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	agentInstanceID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	limit := normalizePageSize(0)
	workload := defaultWorkloadRecord(workloadID, runnerID, threadID, agentID, organizationID, now)
	workload.OwnerID = agentInstanceID
	workload.InstanceID = ptr("runner-local-instance")

	query := fmt.Sprintf("SELECT %s FROM workloads WHERE owner_kind = $1 AND owner_id = $2 AND status = ANY($3) ORDER BY created_at DESC, id DESC LIMIT $4", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(runtimeOwnerKindAgentInstance, agentInstanceID, pgtype.FlatArray[string]{workloadStatusRunning}, int(limit)+1).
		WillReturnRows(workloadRows(t, workload))

	checkCalls := 0
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			checkCalls++
			return &authorizationv1.CheckResponse{Allowed: false}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	resp, err := srv.ListWorkloadsByAgentInstance(context.Background(), &runnersv1.ListWorkloadsByAgentInstanceRequest{
		AgentInstanceId: agentInstanceID.String(),
		Statuses:        []runnersv1.WorkloadStatus{runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING},
	})
	if err != nil {
		t.Fatalf("ListWorkloadsByAgentInstance failed: %v", err)
	}
	if len(resp.GetWorkloads()) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(resp.GetWorkloads()))
	}
	gotWorkload := resp.GetWorkloads()[0]
	if gotWorkload.GetAgentInstanceId() != agentInstanceID.String() {
		t.Fatalf("expected agent instance id %s, got %s", agentInstanceID, gotWorkload.GetAgentInstanceId())
	}
	if gotWorkload.GetOwnerId() != agentInstanceID.String() {
		t.Fatalf("expected owner id %s, got %s", agentInstanceID, gotWorkload.GetOwnerId())
	}
	if gotWorkload.GetInstanceId() != "runner-local-instance" {
		t.Fatalf("expected runner-local instance id, got %q", gotWorkload.GetInstanceId())
	}
	if checkCalls != 0 {
		t.Fatalf("expected no authorization checks, got %d", checkCalls)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListWorkloadsByAgentInstanceUsesMemberRelation(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	agentInstanceID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()
	limit := normalizePageSize(0)
	workload := defaultWorkloadRecord(workloadID, runnerID, threadID, agentID, organizationID, now)
	workload.OwnerID = agentInstanceID

	query := fmt.Sprintf("SELECT %s FROM workloads WHERE owner_kind = $1 AND owner_id = $2 ORDER BY created_at DESC, id DESC LIMIT $3", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(runtimeOwnerKindAgentInstance, agentInstanceID, int(limit)+1).
		WillReturnRows(workloadRows(t, workload))

	var gotCheckReqs []*authorizationv1.CheckRequest
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			gotCheckReqs = append(gotCheckReqs, req)
			allowed := req.GetTupleKey().GetRelation() == organizationMemberRelation
			return &authorizationv1.CheckResponse{Allowed: allowed}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	resp, err := srv.ListWorkloadsByAgentInstance(ctx, &runnersv1.ListWorkloadsByAgentInstanceRequest{AgentInstanceId: agentInstanceID.String()})
	if err != nil {
		t.Fatalf("ListWorkloadsByAgentInstance failed: %v", err)
	}
	if len(resp.GetWorkloads()) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(resp.GetWorkloads()))
	}
	if len(gotCheckReqs) != 1 {
		t.Fatalf("expected 1 authorization check, got %d", len(gotCheckReqs))
	}
	gotCheckReq := gotCheckReqs[0]
	if gotCheckReq.GetTupleKey().GetRelation() != organizationMemberRelation {
		t.Fatalf("expected member relation, got %s", gotCheckReq.GetTupleKey().GetRelation())
	}
	if gotCheckReq.GetTupleKey().GetObject() != organizationObject(organizationID) {
		t.Fatalf("expected organization object %q, got %q", organizationObject(organizationID), gotCheckReq.GetTupleKey().GetObject())
	}
	if gotCheckReq.GetTupleKey().GetUser() != identityObject(callerID) {
		t.Fatalf("expected identity user %q, got %q", identityObject(callerID), gotCheckReq.GetTupleKey().GetUser())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestGetWorkloadRequiresCanViewWorkload(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)

	query := fmt.Sprintf("SELECT %s FROM workloads WHERE id = $1", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(workloadID).WillReturnRows(rows)

	var gotCheckReq *authorizationv1.CheckRequest
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			gotCheckReq = req
			return &authorizationv1.CheckResponse{Allowed: false}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	_, err = srv.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: workloadID.String()})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied error, got %v", err)
	}
	if gotCheckReq == nil {
		t.Fatal("expected authorization Check to be called")
	}
	if gotCheckReq.GetTupleKey().GetRelation() != workloadCanViewRelation {
		t.Fatalf("expected workload can view relation, got %s", gotCheckReq.GetTupleKey().GetRelation())
	}
	if gotCheckReq.GetTupleKey().GetObject() != workloadObject(workloadID) {
		t.Fatalf("expected workload object %q, got %q", workloadObject(workloadID), gotCheckReq.GetTupleKey().GetObject())
	}
	if gotCheckReq.GetTupleKey().GetUser() != identityObject(callerID) {
		t.Fatalf("expected identity user %q, got %q", identityObject(callerID), gotCheckReq.GetTupleKey().GetUser())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestGetWorkloadReturnsAgentState(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateIdle, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)

	query := fmt.Sprintf("SELECT %s FROM workloads WHERE id = $1", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(workloadID).WillReturnRows(rows)

	runnerName := "runner-name"
	runnerRows := pgxmock.NewRows([]string{"id", "name"}).AddRow(runnerID, runnerName)
	mockPool.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM runners WHERE id = ANY($1)")).
		WithArgs(pgtype.FlatArray[uuid.UUID]([]uuid.UUID{runnerID})).
		WillReturnRows(runnerRows)

	agentName := "agent-name"
	agentsClient := fakeAgentsClient{getAgent: func(ctx context.Context, req *agentsv1.GetAgentRequest) (*agentsv1.GetAgentResponse, error) {
		return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Name: agentName}}, nil
	}}

	authorizationClient := fakeAuthorizationClient{check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
		return &authorizationv1.CheckResponse{Allowed: true}, nil
	}}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient, AgentsClient: agentsClient})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	resp, err := srv.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: workloadID.String()})
	if err != nil {
		t.Fatalf("GetWorkload failed: %v", err)
	}
	if resp.GetWorkload().GetAgentState() != runnersv1.WorkloadAgentState_WORKLOAD_AGENT_STATE_IDLE {
		t.Fatalf("expected agent state %v, got %v", runnersv1.WorkloadAgentState_WORKLOAD_AGENT_STATE_IDLE, resp.GetWorkload().GetAgentState())
	}
	if resp.GetWorkload().GetAgentName() != agentName {
		t.Fatalf("expected agent name %q, got %q", agentName, resp.GetWorkload().GetAgentName())
	}
	if resp.GetWorkload().GetRunnerName() != runnerName {
		t.Fatalf("expected runner name %q, got %q", runnerName, resp.GetWorkload().GetRunnerName())
	}
	if resp.GetWorkload().OwnerName != nil {
		t.Fatalf("expected owner name to stay unset for agent instance owners, got %q", resp.GetWorkload().GetOwnerName())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestGetWorkloadInternalNoIdentity(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	sandboxID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, nil, nil, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindSandbox, sandboxID, now, now, nil)

	query := fmt.Sprintf("SELECT %s FROM workloads WHERE id = $1", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(workloadID).WillReturnRows(rows)

	runnerName := "runner-name"
	runnerRows := pgxmock.NewRows([]string{"id", "name"}).AddRow(runnerID, runnerName)
	mockPool.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM runners WHERE id = ANY($1)")).
		WithArgs(pgtype.FlatArray[uuid.UUID]([]uuid.UUID{runnerID})).
		WillReturnRows(runnerRows)

	checkCalls := 0
	authorizationClient := fakeAuthorizationClient{check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
		checkCalls++
		return &authorizationv1.CheckResponse{Allowed: true}, nil
	}}

	sandboxName := "sandbox-name"
	agentsClient := fakeAgentsClient{getSandbox: func(ctx context.Context, req *agentsv1.GetSandboxRequest) (*agentsv1.GetSandboxResponse, error) {
		if req.GetId() != sandboxID.String() {
			t.Fatalf("expected sandbox id %s, got %s", sandboxID, req.GetId())
		}
		return &agentsv1.GetSandboxResponse{Sandbox: &agentsv1.Sandbox{Name: sandboxName}}, nil
	}}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient, AgentsClient: agentsClient})
	resp, err := srv.GetWorkload(context.Background(), &runnersv1.GetWorkloadRequest{Id: workloadID.String()})
	if err != nil {
		t.Fatalf("GetWorkload failed: %v", err)
	}
	if resp.GetWorkload().GetRunnerId() != runnerID.String() {
		t.Fatalf("expected runner id %q, got %q", runnerID, resp.GetWorkload().GetRunnerId())
	}
	if resp.GetWorkload().GetOwnerKind() != runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX {
		t.Fatalf("expected sandbox owner kind, got %v", resp.GetWorkload().GetOwnerKind())
	}
	if resp.GetWorkload().GetOwnerName() != sandboxName {
		t.Fatalf("expected owner name %q, got %q", sandboxName, resp.GetWorkload().GetOwnerName())
	}
	if resp.GetWorkload().GetAgentName() != "" {
		t.Fatalf("expected empty agent name, got %q", resp.GetWorkload().GetAgentName())
	}
	if checkCalls != 0 {
		t.Fatalf("expected no authorization checks, got %d", checkCalls)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestGetWorkloadOwningAgentAllowed(t *testing.T) {
	assertGetWorkloadAllowedByAuthorization(t, true)
}

func TestGetWorkloadOrgOwnerAllowed(t *testing.T) {
	assertGetWorkloadAllowedByAuthorization(t, false)
}

func TestGetWorkloadNonOwningAgentDenied(t *testing.T) {
	assertGetWorkloadDeniedByAuthorization(t, false)
}

func TestGetWorkloadOwningAgentDeniedWhenAuthorizationDenies(t *testing.T) {
	assertGetWorkloadDeniedByAuthorization(t, true)
}

func assertGetWorkloadDeniedByAuthorization(t *testing.T, callerIsOwnerAgent bool) {
	t.Helper()
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	if callerIsOwnerAgent {
		callerID = threadID
	}
	now := time.Now().UTC()
	workload := defaultWorkloadRecord(workloadID, runnerID, threadID, agentID, organizationID, now)
	query := fmt.Sprintf("SELECT %s FROM workloads WHERE id = $1", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(workloadID).WillReturnRows(workloadRows(t, workload))

	var gotCheckReq *authorizationv1.CheckRequest
	authorizationClient := fakeAuthorizationClient{check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
		gotCheckReq = req
		return &authorizationv1.CheckResponse{Allowed: false}, nil
	}}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	_, err = srv.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: workloadID.String()})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied error, got %v", err)
	}
	if gotCheckReq == nil {
		t.Fatal("expected authorization Check to be called")
	}
	if gotCheckReq.GetTupleKey().GetRelation() != workloadCanViewRelation {
		t.Fatalf("expected workload can view relation, got %s", gotCheckReq.GetTupleKey().GetRelation())
	}
	if gotCheckReq.GetTupleKey().GetObject() != workloadObject(workloadID) {
		t.Fatalf("expected workload object %q, got %q", workloadObject(workloadID), gotCheckReq.GetTupleKey().GetObject())
	}
	if gotCheckReq.GetTupleKey().GetUser() != identityObject(callerID) {
		t.Fatalf("expected identity user %q, got %q", identityObject(callerID), gotCheckReq.GetTupleKey().GetUser())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func assertGetWorkloadAllowedByAuthorization(t *testing.T, callerIsOwnerAgent bool) {
	t.Helper()
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	if callerIsOwnerAgent {
		callerID = threadID
	}
	now := time.Now().UTC()
	workload := defaultWorkloadRecord(workloadID, runnerID, threadID, agentID, organizationID, now)
	query := fmt.Sprintf("SELECT %s FROM workloads WHERE id = $1", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(workloadID).WillReturnRows(workloadRows(t, workload))

	runnerName := "runner-name"
	runnerRows := pgxmock.NewRows([]string{"id", "name"}).AddRow(runnerID, runnerName)
	mockPool.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM runners WHERE id = ANY($1)")).
		WithArgs(pgtype.FlatArray[uuid.UUID]([]uuid.UUID{runnerID})).
		WillReturnRows(runnerRows)

	agentName := "agent-name"
	agentsClient := fakeAgentsClient{getAgent: func(ctx context.Context, req *agentsv1.GetAgentRequest) (*agentsv1.GetAgentResponse, error) {
		return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Name: agentName}}, nil
	}}

	var gotCheckReq *authorizationv1.CheckRequest
	authorizationClient := fakeAuthorizationClient{check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
		gotCheckReq = req
		return &authorizationv1.CheckResponse{Allowed: true}, nil
	}}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient, AgentsClient: agentsClient})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	resp, err := srv.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: workloadID.String()})
	if err != nil {
		t.Fatalf("GetWorkload failed: %v", err)
	}
	if resp.GetWorkload().GetMeta().GetId() != workloadID.String() {
		t.Fatalf("expected workload id %s, got %s", workloadID, resp.GetWorkload().GetMeta().GetId())
	}
	if gotCheckReq == nil {
		t.Fatal("expected authorization Check to be called")
	}
	if gotCheckReq.GetTupleKey().GetRelation() != workloadCanViewRelation {
		t.Fatalf("expected workload can view relation, got %s", gotCheckReq.GetTupleKey().GetRelation())
	}
	if gotCheckReq.GetTupleKey().GetObject() != workloadObject(workloadID) {
		t.Fatalf("expected workload object %q, got %q", workloadObject(workloadID), gotCheckReq.GetTupleKey().GetObject())
	}
	if gotCheckReq.GetTupleKey().GetUser() != identityObject(callerID) {
		t.Fatalf("expected identity user %q, got %q", identityObject(callerID), gotCheckReq.GetTupleKey().GetUser())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestTouchWorkload(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	getQuery := fmt.Sprintf(`SELECT %s FROM workloads WHERE id = $1`, workloadColumns)
	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)
	mockPool.ExpectQuery(regexp.QuoteMeta(getQuery)).WithArgs(workloadID).WillReturnRows(rows)

	updateQuery := fmt.Sprintf("UPDATE workloads SET agent_state = $1, last_activity_at = NOW(), updated_at = NOW() WHERE id = $2 AND owner_kind = $3 AND owner_id = $4 AND agent_state = $5 RETURNING %s", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(updateQuery)).
		WithArgs(workloadAgentStateProcessing, workloadID, runtimeOwnerKindAgentInstance, threadID, workloadAgentStateIdle).
		WillReturnRows(pgxmock.NewRows(workloadRowColumns))

	touchQuery := "UPDATE workloads SET last_activity_at = NOW(), updated_at = NOW() WHERE id = $1 AND owner_kind = $2 AND owner_id = $3"
	mockPool.ExpectExec(regexp.QuoteMeta(touchQuery)).
		WithArgs(workloadID, runtimeOwnerKindAgentInstance, threadID).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	srv := New(Options{Pool: mockPool})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, threadID.String()))
	_, err = srv.TouchWorkload(ctx, &runnersv1.TouchWorkloadRequest{Id: workloadID.String()})
	if err != nil {
		t.Fatalf("TouchWorkload failed: %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestTouchWorkloadRejectsAgentClassIdentity(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	ownerID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	getQuery := fmt.Sprintf(`SELECT %s FROM workloads WHERE id = $1`, workloadColumns)
	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, ownerID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, ownerID, now, now, nil)
	mockPool.ExpectQuery(regexp.QuoteMeta(getQuery)).WithArgs(workloadID).WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, agentID.String()))
	_, err = srv.TouchWorkload(ctx, &runnersv1.TouchWorkloadRequest{Id: workloadID.String()})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied error, got %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestTouchWorkloadNoPublishWhenProcessing(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	getQuery := fmt.Sprintf(`SELECT %s FROM workloads WHERE id = $1`, workloadColumns)
	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)
	mockPool.ExpectQuery(regexp.QuoteMeta(getQuery)).WithArgs(workloadID).WillReturnRows(rows)

	updateQuery := fmt.Sprintf("UPDATE workloads SET agent_state = $1, last_activity_at = NOW(), updated_at = NOW() WHERE id = $2 AND owner_kind = $3 AND owner_id = $4 AND agent_state = $5 RETURNING %s", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(updateQuery)).
		WithArgs(workloadAgentStateProcessing, workloadID, runtimeOwnerKindAgentInstance, threadID, workloadAgentStateIdle).
		WillReturnRows(pgxmock.NewRows(workloadRowColumns))

	touchQuery := "UPDATE workloads SET last_activity_at = NOW(), updated_at = NOW() WHERE id = $1 AND owner_kind = $2 AND owner_id = $3"
	mockPool.ExpectExec(regexp.QuoteMeta(touchQuery)).
		WithArgs(workloadID, runtimeOwnerKindAgentInstance, threadID).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	publishCalls := 0
	notificationsClient := fakeNotificationsClient{publish: func(ctx context.Context, req *notificationsv1.PublishRequest) (*notificationsv1.PublishResponse, error) {
		publishCalls++
		return &notificationsv1.PublishResponse{}, nil
	}}

	srv := New(Options{Pool: mockPool, NotificationsClient: notificationsClient})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, threadID.String()))
	_, err = srv.TouchWorkload(ctx, &runnersv1.TouchWorkloadRequest{Id: workloadID.String()})
	if err != nil {
		t.Fatalf("TouchWorkload failed: %v", err)
	}
	if publishCalls != 0 {
		t.Fatalf("expected no notifications, got %d", publishCalls)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestTouchWorkloadPublishesUpdateWhenIdle(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	getQuery := fmt.Sprintf(`SELECT %s FROM workloads WHERE id = $1`, workloadColumns)
	getRows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateIdle, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)
	mockPool.ExpectQuery(regexp.QuoteMeta(getQuery)).WithArgs(workloadID).WillReturnRows(getRows)

	updateQuery := fmt.Sprintf("UPDATE workloads SET agent_state = $1, last_activity_at = NOW(), updated_at = NOW() WHERE id = $2 AND owner_kind = $3 AND owner_id = $4 AND agent_state = $5 RETURNING %s", workloadColumns)
	updateRows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)
	mockPool.ExpectQuery(regexp.QuoteMeta(updateQuery)).
		WithArgs(workloadAgentStateProcessing, workloadID, runtimeOwnerKindAgentInstance, threadID, workloadAgentStateIdle).
		WillReturnRows(updateRows)

	published := make([]*notificationsv1.PublishRequest, 0, 1)
	notificationsClient := fakeNotificationsClient{publish: func(ctx context.Context, req *notificationsv1.PublishRequest) (*notificationsv1.PublishResponse, error) {
		published = append(published, req)
		return &notificationsv1.PublishResponse{}, nil
	}}

	srv := New(Options{Pool: mockPool, NotificationsClient: notificationsClient})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, threadID.String()))
	_, err = srv.TouchWorkload(ctx, &runnersv1.TouchWorkloadRequest{Id: workloadID.String()})
	if err != nil {
		t.Fatalf("TouchWorkload failed: %v", err)
	}
	if len(published) != 1 {
		t.Fatalf("expected 1 notification, got %d", len(published))
	}
	request := published[0]
	if request.GetEvent() != "workload.updated" {
		t.Fatalf("unexpected event: %s", request.GetEvent())
	}
	workloadRoom := fmt.Sprintf("workload:%s", workloadID)
	orgRoom := fmt.Sprintf("organization:%s", organizationID)
	rooms := request.GetRooms()
	if len(rooms) != 2 || !hasRoom(rooms, workloadRoom) || !hasRoom(rooms, orgRoom) {
		t.Fatalf("unexpected workload.updated rooms: %v", rooms)
	}
	payload := request.GetPayload().AsMap()
	if payload["workload_id"] != workloadID.String() {
		t.Fatalf("unexpected workload_id payload: %v", payload["workload_id"])
	}
	statusValue, ok := payload["status"].(string)
	if !ok || statusValue != workloadStatusRunning {
		t.Fatalf("unexpected status payload: %v", payload["status"])
	}
	agentStateValue, ok := payload["agent_state"].(string)
	if !ok || agentStateValue != workloadAgentStateProcessing {
		t.Fatalf("unexpected agent_state payload: %v", payload["agent_state"])
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestTouchWorkloadRequiresOwnerIdentity(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	agentID := uuid.New()
	callerID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	getQuery := fmt.Sprintf(`SELECT %s FROM workloads WHERE id = $1`, workloadColumns)
	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)
	mockPool.ExpectQuery(regexp.QuoteMeta(getQuery)).WithArgs(workloadID).WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	_, err = srv.TouchWorkload(ctx, &runnersv1.TouchWorkloadRequest{Id: workloadID.String()})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied error, got %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestTouchSandboxWorkloadAllowsInternalTouch(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	organizationID := uuid.New()
	sandboxID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	getQuery := fmt.Sprintf(`SELECT %s FROM workloads WHERE id = $1`, workloadColumns)
	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, nil, nil, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindSandbox, sandboxID, now, now, nil)
	mockPool.ExpectQuery(regexp.QuoteMeta(getQuery)).WithArgs(workloadID).WillReturnRows(rows)

	touchQuery := "UPDATE workloads SET last_activity_at = NOW(), updated_at = NOW() WHERE id = $1 AND owner_kind = $2 AND owner_id = $3"
	mockPool.ExpectExec(regexp.QuoteMeta(touchQuery)).
		WithArgs(workloadID, runtimeOwnerKindSandbox, sandboxID).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	srv := New(Options{Pool: mockPool})
	_, err = srv.TouchWorkload(context.Background(), &runnersv1.TouchWorkloadRequest{Id: workloadID.String()})
	if err != nil {
		t.Fatalf("TouchWorkload failed: %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestTouchSandboxWorkloadDeniesForwardedIdentity(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	organizationID := uuid.New()
	sandboxID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	getQuery := fmt.Sprintf(`SELECT %s FROM workloads WHERE id = $1`, workloadColumns)
	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, nil, nil, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindSandbox, sandboxID, now, now, nil)
	mockPool.ExpectQuery(regexp.QuoteMeta(getQuery)).WithArgs(workloadID).WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	_, err = srv.TouchWorkload(ctx, &runnersv1.TouchWorkloadRequest{Id: workloadID.String()})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied error, got %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestSweepWorkloadActivityPublishesUpdates(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	firstID := uuid.New()
	secondID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	keepaliveGrace := 25 * time.Second
	cutoff := now.Add(-keepaliveGrace)
	lastActivity := cutoff.Add(-2 * time.Second)
	containersJSON := []byte("[]")

	updateQuery := fmt.Sprintf("UPDATE workloads SET agent_state = $1, updated_at = NOW() WHERE status = $2 AND agent_state = $3 AND owner_kind = $4 AND last_activity_at < $5 AND removed_at IS NULL RETURNING %s", workloadColumns)
	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(firstID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateIdle, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, lastActivity, nil, nil, runtimeOwnerKindAgentInstance, firstID, now, now, nil).
		AddRow(secondID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateIdle, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, lastActivity, nil, nil, runtimeOwnerKindAgentInstance, secondID, now, now, nil)
	mockPool.ExpectQuery(regexp.QuoteMeta(updateQuery)).
		WithArgs(workloadAgentStateIdle, workloadStatusRunning, workloadAgentStateProcessing, runtimeOwnerKindAgentInstance, cutoff).
		WillReturnRows(rows)

	published := []*notificationsv1.PublishRequest{}
	notificationsClient := fakeNotificationsClient{publish: func(ctx context.Context, req *notificationsv1.PublishRequest) (*notificationsv1.PublishResponse, error) {
		published = append(published, req)
		return &notificationsv1.PublishResponse{}, nil
	}}

	srv := New(Options{Pool: mockPool, NotificationsClient: notificationsClient})
	if err := srv.sweepWorkloadActivity(context.Background(), now, keepaliveGrace); err != nil {
		t.Fatalf("sweepWorkloadActivity failed: %v", err)
	}
	if len(published) != 2 {
		t.Fatalf("expected 2 notifications, got %d", len(published))
	}

	for _, req := range published {
		if req.GetEvent() != "workload.updated" {
			t.Fatalf("unexpected event: %s", req.GetEvent())
		}
		payload := req.GetPayload().AsMap()
		agentStateValue, ok := payload["agent_state"].(string)
		if !ok || agentStateValue != workloadAgentStateIdle {
			t.Fatalf("unexpected agent_state payload: %v", payload["agent_state"])
		}
		statusValue, ok := payload["status"].(string)
		if !ok || statusValue != workloadStatusRunning {
			t.Fatalf("unexpected status payload: %v", payload["status"])
		}
		workloadID, ok := payload["workload_id"].(string)
		if !ok {
			t.Fatalf("expected workload_id payload, got %v", payload["workload_id"])
		}
		rooms := req.GetRooms()
		workloadRoom := fmt.Sprintf("workload:%s", workloadID)
		orgRoom := fmt.Sprintf("organization:%s", organizationID)
		if len(rooms) != 2 || !hasRoom(rooms, workloadRoom) || !hasRoom(rooms, orgRoom) {
			t.Fatalf("unexpected workload.updated rooms: %v", rooms)
		}
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUpdateWorkload(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	instanceID := "instance-1"
	now := time.Now().UTC()
	startedAt := now.Add(-2 * time.Minute)
	finishedAt := now.Add(-1 * time.Minute)
	reason := "CrashLoopBackOff"
	message := "back-off"
	exitCode := int32(137)
	containers := []*runnersv1.Container{{
		ContainerId:  "container-1",
		Name:         "name",
		Role:         runnersv1.ContainerRole_CONTAINER_ROLE_MAIN,
		Image:        "image",
		Status:       runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING,
		Reason:       &reason,
		Message:      &message,
		ExitCode:     &exitCode,
		RestartCount: 2,
		StartedAt:    timestamppb.New(startedAt),
		FinishedAt:   timestamppb.New(finishedAt),
	}}
	containerRecords, err := containersFromProto(containers)
	if err != nil {
		t.Fatalf("failed to build container records: %v", err)
	}
	containersJSON, err := json.Marshal(containerRecords)
	if err != nil {
		t.Fatalf("failed to marshal containers: %v", err)
	}

	selectRows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, instanceID, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)
	selectQuery := fmt.Sprintf("SELECT %s FROM workloads WHERE id = $1", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(selectQuery)).
		WithArgs(workloadID).
		WillReturnRows(selectRows)

	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, instanceID, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)

	query := fmt.Sprintf("UPDATE workloads SET status = $1, containers = $2, instance_id = $3, updated_at = NOW() WHERE id = $4 RETURNING %s", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(workloadStatusRunning, containersJSON, instanceID, workloadID).
		WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	resp, err := srv.UpdateWorkload(context.Background(), &runnersv1.UpdateWorkloadRequest{
		Id:         workloadID.String(),
		Status:     runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING.Enum(),
		Containers: containers,
		InstanceId: &instanceID,
	})
	if err != nil {
		t.Fatalf("UpdateWorkload failed: %v", err)
	}
	if resp.GetWorkload().GetInstanceId() != instanceID {
		t.Fatalf("expected instance id %q, got %q", instanceID, resp.GetWorkload().GetInstanceId())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUpdateWorkloadPublishesNotifications(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	selectRows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusStarting, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)

	selectQuery := fmt.Sprintf("SELECT %s FROM workloads WHERE id = $1", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(selectQuery)).
		WithArgs(workloadID).
		WillReturnRows(selectRows)

	startedAt := now.Add(-3 * time.Minute)
	finishedAt := now.Add(-2 * time.Minute)
	reason := "Completed"
	message := "done"
	exitCode := int32(0)
	containers := []*runnersv1.Container{{
		ContainerId:  "container-1",
		Name:         "main",
		Role:         runnersv1.ContainerRole_CONTAINER_ROLE_MAIN,
		Image:        "image",
		Status:       runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING,
		Reason:       &reason,
		Message:      &message,
		ExitCode:     &exitCode,
		RestartCount: 1,
		StartedAt:    timestamppb.New(startedAt),
		FinishedAt:   timestamppb.New(finishedAt),
	}}
	containerRecords, err := containersFromProto(containers)
	if err != nil {
		t.Fatalf("failed to build container records: %v", err)
	}
	updatedContainersJSON, err := json.Marshal(containerRecords)
	if err != nil {
		t.Fatalf("failed to marshal containers: %v", err)
	}

	updateRows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, updatedContainersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)

	updateQuery := fmt.Sprintf("UPDATE workloads SET status = $1, containers = $2, last_activity_at = NOW(), updated_at = NOW() WHERE id = $3 RETURNING %s", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(updateQuery)).
		WithArgs(workloadStatusRunning, updatedContainersJSON, workloadID).
		WillReturnRows(updateRows)

	published := make([]*notificationsv1.PublishRequest, 0, 2)
	notificationsClient := fakeNotificationsClient{publish: func(ctx context.Context, req *notificationsv1.PublishRequest) (*notificationsv1.PublishResponse, error) {
		published = append(published, req)
		return &notificationsv1.PublishResponse{}, nil
	}}

	srv := New(Options{Pool: mockPool, NotificationsClient: notificationsClient})
	_, err = srv.UpdateWorkload(context.Background(), &runnersv1.UpdateWorkloadRequest{
		Id:         workloadID.String(),
		Status:     runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING.Enum(),
		Containers: containers,
	})
	if err != nil {
		t.Fatalf("UpdateWorkload failed: %v", err)
	}
	if len(published) != 2 {
		t.Fatalf("expected 2 notifications, got %d", len(published))
	}
	workloadRoom := fmt.Sprintf("workload:%s", workloadID)
	orgRoom := fmt.Sprintf("organization:%s", organizationID)
	events := map[string]bool{}
	for _, req := range published {
		events[req.GetEvent()] = true
		if req.GetSource() != "runners" {
			t.Fatalf("expected source runners, got %q", req.GetSource())
		}
		switch req.GetEvent() {
		case "workload.updated":
			rooms := req.GetRooms()
			if len(rooms) != 2 || !hasRoom(rooms, workloadRoom) || !hasRoom(rooms, orgRoom) {
				t.Fatalf("unexpected workload.updated rooms: %v", rooms)
			}
		case "workload.status_changed":
			// The workload's own room for whoever is watching that one, and the
			// cluster-wide room the Orchestrator holds -- it reconciles every
			// workload and cannot subscribe per workload without racing the
			// creation of the next one.
			rooms := req.GetRooms()
			if len(rooms) != 2 || !hasRoom(rooms, workloadRoom) || !hasRoom(rooms, "workloads") {
				t.Fatalf("unexpected workload.status_changed rooms: %v", rooms)
			}
		default:
			t.Fatalf("unexpected event: %s", req.GetEvent())
		}
		payload := req.GetPayload().AsMap()
		if payload["workload_id"] != workloadID.String() {
			t.Fatalf("unexpected workload_id payload: %v", payload["workload_id"])
		}
		statusValue, ok := payload["status"].(string)
		if !ok || statusValue != workloadStatusRunning {
			t.Fatalf("unexpected status payload: %v", payload["status"])
		}
	}
	if !events["workload.updated"] {
		t.Fatal("expected workload.updated notification")
	}
	if !events["workload.status_changed"] {
		t.Fatal("expected workload.status_changed notification")
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUpdateWorkloadFailureMetadata(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")
	failureReason := workloadFailureReasonCrashloop
	failureMessage := "back-off"

	selectRows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)

	selectQuery := fmt.Sprintf("SELECT %s FROM workloads WHERE id = $1", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(selectQuery)).
		WithArgs(workloadID).
		WillReturnRows(selectRows)

	updateRows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, failureReason, failureMessage, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)

	updateQuery := fmt.Sprintf("UPDATE workloads SET failure_reason = $1, failure_message = $2, updated_at = NOW() WHERE id = $3 RETURNING %s", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(updateQuery)).
		WithArgs(failureReason, failureMessage, workloadID).
		WillReturnRows(updateRows)

	published := make([]*notificationsv1.PublishRequest, 0, 1)
	notificationsClient := fakeNotificationsClient{publish: func(ctx context.Context, req *notificationsv1.PublishRequest) (*notificationsv1.PublishResponse, error) {
		published = append(published, req)
		return &notificationsv1.PublishResponse{}, nil
	}}

	srv := New(Options{Pool: mockPool, NotificationsClient: notificationsClient})
	reasonEnum := runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CRASHLOOP
	resp, err := srv.UpdateWorkload(context.Background(), &runnersv1.UpdateWorkloadRequest{
		Id:             workloadID.String(),
		FailureReason:  &reasonEnum,
		FailureMessage: &failureMessage,
	})
	if err != nil {
		t.Fatalf("UpdateWorkload failed: %v", err)
	}
	if resp.GetWorkload().GetFailureReason() != reasonEnum {
		t.Fatalf("expected failure reason %v, got %v", reasonEnum, resp.GetWorkload().GetFailureReason())
	}
	if resp.GetWorkload().GetFailureMessage() != failureMessage {
		t.Fatalf("expected failure message %q, got %q", failureMessage, resp.GetWorkload().GetFailureMessage())
	}
	if len(published) != 1 {
		t.Fatalf("expected 1 notification, got %d", len(published))
	}
	request := published[0]
	if request.GetEvent() != "workload.updated" {
		t.Fatalf("unexpected event: %s", request.GetEvent())
	}
	workloadRoom := fmt.Sprintf("workload:%s", workloadID)
	orgRoom := fmt.Sprintf("organization:%s", organizationID)
	rooms := request.GetRooms()
	if len(rooms) != 2 || !hasRoom(rooms, workloadRoom) || !hasRoom(rooms, orgRoom) {
		t.Fatalf("unexpected workload.updated rooms: %v", rooms)
	}
	payload := request.GetPayload().AsMap()
	if payload["workload_id"] != workloadID.String() {
		t.Fatalf("unexpected workload_id payload: %v", payload["workload_id"])
	}
	if payload["failure_reason"] != failureReason {
		t.Fatalf("unexpected failure_reason payload: %v", payload["failure_reason"])
	}
	if payload["failure_message"] != failureMessage {
		t.Fatalf("unexpected failure_message payload: %v", payload["failure_message"])
	}
	statusValue, ok := payload["status"].(string)
	if !ok || statusValue != workloadStatusRunning {
		t.Fatalf("unexpected status payload: %v", payload["status"])
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func hasRoom(rooms []string, target string) bool {
	for _, room := range rooms {
		if room == target {
			return true
		}
	}
	return false
}

func TestUpdateWorkloadSkipsNotificationsWhenContainersUnchanged(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()

	containerMain := &runnersv1.Container{
		ContainerId:  "container-1",
		Name:         "main",
		Role:         runnersv1.ContainerRole_CONTAINER_ROLE_MAIN,
		Image:        "image",
		Status:       runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING,
		RestartCount: 0,
	}
	containerSidecar := &runnersv1.Container{
		ContainerId:  "container-2",
		Name:         "sidecar",
		Role:         runnersv1.ContainerRole_CONTAINER_ROLE_SIDECAR,
		Image:        "image",
		Status:       runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING,
		RestartCount: 0,
	}
	existingContainers := []*runnersv1.Container{containerMain, containerSidecar}
	existingRecords, err := containersFromProto(existingContainers)
	if err != nil {
		t.Fatalf("failed to build container records: %v", err)
	}
	existingContainersJSON, err := json.Marshal(existingRecords)
	if err != nil {
		t.Fatalf("failed to marshal containers: %v", err)
	}

	selectRows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, existingContainersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)

	selectQuery := fmt.Sprintf("SELECT %s FROM workloads WHERE id = $1", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(selectQuery)).
		WithArgs(workloadID).
		WillReturnRows(selectRows)

	requestContainers := []*runnersv1.Container{containerSidecar, containerMain}
	requestRecords, err := containersFromProto(requestContainers)
	if err != nil {
		t.Fatalf("failed to build container records: %v", err)
	}
	requestContainersJSON, err := json.Marshal(requestRecords)
	if err != nil {
		t.Fatalf("failed to marshal containers: %v", err)
	}

	updateRows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, requestContainersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, threadID, now, now, nil)

	updateQuery := fmt.Sprintf("UPDATE workloads SET containers = $1, updated_at = NOW() WHERE id = $2 RETURNING %s", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(updateQuery)).
		WithArgs(requestContainersJSON, workloadID).
		WillReturnRows(updateRows)

	publishCount := 0
	notificationsClient := fakeNotificationsClient{publish: func(ctx context.Context, req *notificationsv1.PublishRequest) (*notificationsv1.PublishResponse, error) {
		publishCount++
		return &notificationsv1.PublishResponse{}, nil
	}}

	srv := New(Options{Pool: mockPool, NotificationsClient: notificationsClient})
	_, err = srv.UpdateWorkload(context.Background(), &runnersv1.UpdateWorkloadRequest{
		Id:         workloadID.String(),
		Containers: requestContainers,
	})
	if err != nil {
		t.Fatalf("UpdateWorkload failed: %v", err)
	}
	if publishCount != 0 {
		t.Fatalf("expected no notifications, got %d", publishCount)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUpdateWorkloadRequiresFields(t *testing.T) {
	srv := New(Options{})

	_, err := srv.UpdateWorkload(context.Background(), &runnersv1.UpdateWorkloadRequest{Id: uuid.NewString()})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument error, got %v", err)
	}
}

func TestBatchUpdateWorkloadSampledAt(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	firstID := uuid.New()
	secondID := uuid.New()
	firstSampledAt := time.Now().UTC()
	secondSampledAt := firstSampledAt.Add(2 * time.Minute)

	query := "UPDATE workloads AS target SET last_metering_sampled_at = v.sampled_at, updated_at = NOW() FROM (VALUES ($1::uuid, $2::timestamptz), ($3::uuid, $4::timestamptz)) AS v(id, sampled_at) WHERE target.id = v.id"
	mockPool.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs(firstID, firstSampledAt, secondID, secondSampledAt).
		WillReturnResult(pgxmock.NewResult("UPDATE", 2))

	srv := New(Options{Pool: mockPool})
	_, err = srv.BatchUpdateWorkloadSampledAt(context.Background(), &runnersv1.BatchUpdateWorkloadSampledAtRequest{
		Entries: []*runnersv1.SampledAtEntry{
			{Id: firstID.String(), SampledAt: timestamppb.New(firstSampledAt)},
			{Id: secondID.String(), SampledAt: timestamppb.New(secondSampledAt)},
		},
	})
	if err != nil {
		t.Fatalf("BatchUpdateWorkloadSampledAt failed: %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestBatchUpdateWorkloadSampledAtInvalid(t *testing.T) {
	srv := New(Options{})

	_, err := srv.BatchUpdateWorkloadSampledAt(context.Background(), &runnersv1.BatchUpdateWorkloadSampledAtRequest{
		Entries: []*runnersv1.SampledAtEntry{{Id: "not-a-uuid", SampledAt: timestamppb.Now()}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument error, got %v", err)
	}
}

func TestSoftDeleteWorkload(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusStopped, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, now, runtimeOwnerKindAgentInstance, threadID, now, now, nil)

	query := fmt.Sprintf("UPDATE workloads SET status = $1, removed_at = NOW(), updated_at = NOW() WHERE id = $2 RETURNING %s", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(workloadStatusStopped, workloadID).
		WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	if err := srv.softDeleteWorkload(context.Background(), workloadID); err != nil {
		t.Fatalf("softDeleteWorkload failed: %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestCreateWorkloadSandboxOwner(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	organizationID := uuid.New()
	sandboxID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")
	input := workloadInsertInput{
		ID:             workloadID,
		RunnerID:       runnerID,
		OrganizationID: organizationID,
		Status:         workloadStatusRunning,
		ContainersJSON: containersJSON,
		OwnerKind:      runtimeOwnerKindSandbox,
		OwnerID:        sandboxID,
	}
	workload := workloadRecord{
		Meta: entityMeta{
			ID:        workloadID,
			CreatedAt: now,
			UpdatedAt: now,
		},
		RunnerID:       runnerID,
		OrganizationID: organizationID,
		Status:         workloadStatusRunning,
		AgentState:     workloadAgentStateProcessing,
		Containers:     []containerRecord{},
		LastActivityAt: now,
		OwnerKind:      runtimeOwnerKindSandbox,
		OwnerID:        sandboxID,
	}
	expectWorkloadInsert(t, mockPool, input, workload)

	var gotWriteReq *authorizationv1.WriteRequest
	authorizationClient := fakeAuthorizationClient{write: func(ctx context.Context, req *authorizationv1.WriteRequest) (*authorizationv1.WriteResponse, error) {
		gotWriteReq = req
		return &authorizationv1.WriteResponse{}, nil
	}}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	resp, err := srv.CreateWorkload(context.Background(), &runnersv1.CreateWorkloadRequest{
		Id:             workloadID.String(),
		RunnerId:       runnerID.String(),
		OrganizationId: organizationID.String(),
		Status:         runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		OwnerKind:      runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX,
		OwnerId:        sandboxID.String(),
	})
	if err != nil {
		t.Fatalf("CreateWorkload failed: %v", err)
	}
	if resp.GetWorkload().GetOwnerKind() != runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX {
		t.Fatalf("expected sandbox owner kind, got %s", resp.GetWorkload().GetOwnerKind())
	}
	if resp.GetWorkload().GetOwnerId() != sandboxID.String() {
		t.Fatalf("expected owner id %s, got %s", sandboxID, resp.GetWorkload().GetOwnerId())
	}
	if resp.GetWorkload().GetAgentId() != "" || resp.GetWorkload().GetThreadId() != "" {
		t.Fatalf("expected sandbox workload without agent/thread ids")
	}
	// A sandbox workload names no agent class, so the only identity that can
	// view it is its own -- the sandbox the workload runs for.
	assertWorkloadAuthorizationWrites(t, gotWriteReq, workloadID, organizationID, sandboxID)

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// The flavor a workload starts on is what metering bills, so it has to survive
// the round trip rather than being resolved again later.
func TestCreateWorkloadPersistsFlavor(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()

	input := workloadInsertInput{
		ID:             workloadID,
		RunnerID:       runnerID,
		ThreadID:       &threadID,
		AgentID:        &agentID,
		OwnerKind:      runtimeOwnerKindAgentInstance,
		OwnerID:        threadID,
		OrganizationID: organizationID,
		Status:         workloadStatusRunning,
		ContainersJSON: []byte("[]"),
		ZitiIdentityID: "ziti-id",
		Flavor:         "ram-4gb",
	}
	record := defaultWorkloadRecord(workloadID, runnerID, threadID, agentID, organizationID, now)
	record.Flavor = "ram-4gb"
	expectWorkloadInsert(t, mockPool, input, record)

	srv := New(Options{Pool: mockPool, AuthorizationClient: fakeAuthorizationClient{
		write: func(context.Context, *authorizationv1.WriteRequest) (*authorizationv1.WriteResponse, error) {
			return &authorizationv1.WriteResponse{}, nil
		},
	}})
	resp, err := srv.CreateWorkload(context.Background(), &runnersv1.CreateWorkloadRequest{
		Id:             workloadID.String(),
		RunnerId:       runnerID.String(),
		ThreadId:       threadID.String(),
		AgentId:        agentID.String(),
		OrganizationId: organizationID.String(),
		Status:         runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		OwnerKind:      runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
		OwnerId:        threadID.String(),
		ZitiIdentityId: "ziti-id",
		Flavor:         "ram-4gb",
	})
	if err != nil {
		t.Fatalf("CreateWorkload failed: %v", err)
	}
	if resp.GetWorkload().GetFlavor() != "ram-4gb" {
		t.Fatalf("expected flavor ram-4gb, got %q", resp.GetWorkload().GetFlavor())
	}
}

func TestTerminalWorkloadStatuses(t *testing.T) {
	for status, terminal := range map[string]bool{
		workloadStatusStarting: false,
		workloadStatusRunning:  false,
		workloadStatusStopping: false,
		workloadStatusStopped:  true,
		workloadStatusFailed:   true,
	} {
		if isTerminalWorkloadStatus(status) != terminal {
			t.Errorf("expected isTerminalWorkloadStatus(%q) = %v", status, terminal)
		}
	}
}
