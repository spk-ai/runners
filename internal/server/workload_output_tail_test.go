package server

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	agentsv1 "github.com/agynio/runners/.gen/go/agynio/api/agents/v1"
	authorizationv1 "github.com/agynio/runners/.gen/go/agynio/api/authorization/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pashagolub/pgxmock/v3"
)

func outputTailContainers(tails ...*string) []*runnersv1.Container {
	containers := make([]*runnersv1.Container, 0, len(tails))
	for i, tail := range tails {
		containers = append(containers, &runnersv1.Container{
			Name:       fmt.Sprintf("c%d", i),
			Role:       runnersv1.ContainerRole_CONTAINER_ROLE_MAIN,
			Status:     runnersv1.ContainerStatus_CONTAINER_STATUS_TERMINATED,
			OutputTail: tail,
		})
	}
	return containers
}

func TestContainerOutputTailRoundTrip(t *testing.T) {
	tail := "panic: boom\n[REDACTED:jwt]\n"
	records, err := containersFromProto(outputTailContainers(&tail, nil))
	if err != nil {
		t.Fatalf("containersFromProto: %v", err)
	}
	payload, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(payload), "output_tail") != 1 {
		t.Fatalf("absent tails must not be stored: %s", payload)
	}
	var stored []containerRecord
	if err := json.Unmarshal(payload, &stored); err != nil {
		t.Fatal(err)
	}
	containers, err := containersToProto(stored)
	if err != nil {
		t.Fatalf("containersToProto: %v", err)
	}
	if containers[0].OutputTail == nil || containers[0].GetOutputTail() != tail {
		t.Fatalf("tail not preserved: %v", containers[0].OutputTail)
	}
	if containers[1].OutputTail != nil {
		t.Fatalf("absent tail became %q", containers[1].GetOutputTail())
	}
	// Change detection for notifications sees a tail change.
	other := "other"
	changed, err := containersFromProto(outputTailContainers(&other, nil))
	if err != nil {
		t.Fatal(err)
	}
	if containersEqualByName(records, changed) {
		t.Fatal("output_tail change not detected")
	}
}

func TestContainerOutputTailBounds(t *testing.T) {
	tooLong := strings.Repeat("x", maxContainerOutputTailBytes+1)
	if _, err := containersFromProto(outputTailContainers(&tooLong)); err == nil || !strings.Contains(err.Error(), "output_tail") {
		t.Fatalf("expected per-container bound error, got %v", err)
	}
	atBound := strings.Repeat("x", maxContainerOutputTailBytes)
	if _, err := containersFromProto(outputTailContainers(&atBound)); err != nil {
		t.Fatalf("tail at the bound rejected: %v", err)
	}
	tails := make([]*string, 0, 5)
	for i := 0; i < 5; i++ {
		tails = append(tails, &atBound)
	}
	if _, err := containersFromProto(outputTailContainers(tails...)); err == nil || !strings.Contains(err.Error(), "per workload") {
		t.Fatalf("expected per-workload bound error, got %v", err)
	}
}

func outputTailRecord(t *testing.T, workloadID, runnerID, ownerID, organizationID uuid.UUID, now time.Time) workloadRecord {
	t.Helper()
	tail := "fatal: listen tcp: address already in use\n"
	records, err := containersFromProto(outputTailContainers(&tail))
	if err != nil {
		t.Fatal(err)
	}
	record := defaultWorkloadRecord(workloadID, runnerID, ownerID, uuid.New(), organizationID, now)
	record.OwnerID = ownerID
	record.Status = workloadStatusFailed
	record.Containers = records
	return record
}

func TestListWorkloadsByAgentInstanceOmitsContainerOutput(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}
	instanceID := uuid.New()
	record := outputTailRecord(t, uuid.New(), uuid.New(), instanceID, uuid.New(), time.Now().UTC())
	query := fmt.Sprintf("SELECT %s FROM workloads WHERE owner_kind = $1 AND owner_id = $2 ORDER BY created_at DESC, id DESC LIMIT $3", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(runtimeOwnerKindAgentInstance, instanceID, int(normalizePageSize(0))+1).
		WillReturnRows(workloadRows(t, record))

	srv := New(Options{Pool: mockPool})
	resp, err := srv.ListWorkloadsByAgentInstance(context.Background(), &runnersv1.ListWorkloadsByAgentInstanceRequest{AgentInstanceId: instanceID.String()})
	if err != nil {
		t.Fatalf("ListWorkloadsByAgentInstance: %v", err)
	}
	containers := resp.GetWorkloads()[0].GetContainers()
	if len(containers) != 1 || containers[0].GetName() != "c0" {
		t.Fatalf("container status must stay listed: %v", containers)
	}
	if containers[0].OutputTail != nil {
		t.Fatalf("list response carried output_tail %q", containers[0].GetOutputTail())
	}
	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestGetWorkloadKeepsContainerOutput(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}
	workloadID, runnerID, sandboxID := uuid.New(), uuid.New(), uuid.New()
	record := outputTailRecord(t, workloadID, runnerID, sandboxID, uuid.New(), time.Now().UTC())
	record.OwnerKind = runtimeOwnerKindSandbox
	query := fmt.Sprintf("SELECT %s FROM workloads WHERE id = $1", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(workloadID).WillReturnRows(workloadRows(t, record))
	mockPool.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM runners WHERE id = ANY($1)")).
		WithArgs(pgtype.FlatArray[uuid.UUID]([]uuid.UUID{runnerID})).
		WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(runnerID, "runner"))
	agentsClient := fakeAgentsClient{
		getSandbox: func(ctx context.Context, req *agentsv1.GetSandboxRequest) (*agentsv1.GetSandboxResponse, error) {
			return &agentsv1.GetSandboxResponse{Sandbox: &agentsv1.Sandbox{Name: "sandbox"}}, nil
		},
		getAgent: func(ctx context.Context, req *agentsv1.GetAgentRequest) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Name: "agent"}}, nil
		},
	}
	authorizationClient := fakeAuthorizationClient{check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
		return &authorizationv1.CheckResponse{Allowed: true}, nil
	}}

	srv := New(Options{Pool: mockPool, AgentsClient: agentsClient, AuthorizationClient: authorizationClient})
	resp, err := srv.GetWorkload(context.Background(), &runnersv1.GetWorkloadRequest{Id: workloadID.String()})
	if err != nil {
		t.Fatalf("GetWorkload: %v", err)
	}
	containers := resp.GetWorkload().GetContainers()
	if len(containers) != 1 || !strings.Contains(containers[0].GetOutputTail(), "address already in use") {
		t.Fatalf("GetWorkload dropped output_tail: %v", containers)
	}
	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// The orchestrator reads every tracked record through ListWorkloads.
func TestListWorkloadsOmitsContainerOutput(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}
	runnerID := uuid.New()
	record := outputTailRecord(t, uuid.New(), runnerID, uuid.New(), uuid.New(), time.Now().UTC())
	query := fmt.Sprintf("SELECT %s FROM workloads ORDER BY workloads.created_at DESC, workloads.id ASC LIMIT $1", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(int(normalizePageSize(0)) + 1).WillReturnRows(workloadRows(t, record))
	mockPool.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM runners WHERE id = ANY($1)")).
		WithArgs(pgtype.FlatArray[uuid.UUID]([]uuid.UUID{runnerID})).
		WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(runnerID, "runner"))
	agentsClient := fakeAgentsClient{getAgent: func(ctx context.Context, req *agentsv1.GetAgentRequest) (*agentsv1.GetAgentResponse, error) {
		return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Name: "agent"}}, nil
	}}

	srv := New(Options{Pool: mockPool, AgentsClient: agentsClient})
	resp, err := srv.ListWorkloads(context.Background(), &runnersv1.ListWorkloadsRequest{})
	if err != nil {
		t.Fatalf("ListWorkloads: %v", err)
	}
	containers := resp.GetWorkloads()[0].GetContainers()
	if len(containers) != 1 || containers[0].OutputTail != nil {
		t.Fatalf("list response must keep status and drop output_tail: %v", containers)
	}
	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
