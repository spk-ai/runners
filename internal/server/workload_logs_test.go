package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"regexp"
	"strings"
	"testing"
	"time"

	authorizationv1 "github.com/agynio/runners/.gen/go/agynio/api/authorization/v1"
	runnerv1 "github.com/agynio/runners/.gen/go/agynio/api/runner/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pashagolub/pgxmock/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var runnerRowColumns = []string{
	"id",
	"name",
	"organization_id",
	"identity_id",
	"ziti_identity_id",
	"ziti_service_id",
	"ziti_service_name",
	"status",
	"labels",
	"capabilities",
	"created_at",
	"updated_at",
}

type fakeRunnerLogStream struct {
	ctx  context.Context
	sent []*runnerv1.StreamWorkloadLogsResponse
}

func (f *fakeRunnerLogStream) Send(resp *runnerv1.StreamWorkloadLogsResponse) error {
	f.sent = append(f.sent, resp)
	return nil
}

func (f *fakeRunnerLogStream) SetHeader(metadata.MD) error {
	return nil
}

func (f *fakeRunnerLogStream) SendHeader(metadata.MD) error {
	return nil
}

func (f *fakeRunnerLogStream) SetTrailer(metadata.MD) {}

func (f *fakeRunnerLogStream) SendMsg(any) error {
	return nil
}

func (f *fakeRunnerLogStream) RecvMsg(any) error {
	return nil
}

func (f *fakeRunnerLogStream) Context() context.Context {
	if f.ctx != nil {
		return f.ctx
	}
	return context.Background()
}

type fakeRunnerServer struct {
	runnerv1.UnimplementedRunnerServiceServer
	streamWorkloadLogs func(*runnerv1.StreamWorkloadLogsRequest, grpc.ServerStreamingServer[runnerv1.StreamWorkloadLogsResponse]) error
}

func (f *fakeRunnerServer) StreamWorkloadLogs(req *runnerv1.StreamWorkloadLogsRequest, stream grpc.ServerStreamingServer[runnerv1.StreamWorkloadLogsResponse]) error {
	if f.streamWorkloadLogs == nil {
		return status.Error(codes.Unimplemented, "StreamWorkloadLogs not implemented")
	}
	return f.streamWorkloadLogs(req, stream)
}

func startRunnerServer(t *testing.T, streamWorkloadLogs func(*runnerv1.StreamWorkloadLogsRequest, grpc.ServerStreamingServer[runnerv1.StreamWorkloadLogsResponse]) error) *bufconn.Listener {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	runnerv1.RegisterRunnerServiceServer(server, &fakeRunnerServer{streamWorkloadLogs: streamWorkloadLogs})
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return listener
}

func TestStreamWorkloadLogsProxiesRunnerStream(t *testing.T) {
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
	identityID := uuid.New()
	now := time.Now().UTC()
	instanceID := "instance-1"
	serviceName := "runner-service"
	containersJSON := []byte("[]")
	requestChan := make(chan *runnerv1.StreamWorkloadLogsRequest, 1)
	sinceTime := timestamppb.New(time.Unix(1723123123, 0))

	labelsJSON, err := json.Marshal(map[string]string{})
	if err != nil {
		t.Fatalf("failed to marshal labels: %v", err)
	}
	capabilitiesJSON, err := json.Marshal([]string{})
	if err != nil {
		t.Fatalf("failed to marshal capabilities: %v", err)
	}

	workloadRows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, instanceID, now, nil, nil, runtimeOwnerKindAgentInstance, workloadID, now, now, nil, nil, int64(0), nil, []string(nil), []byte(nil), []byte(nil), nil)
	workloadQuery := fmt.Sprintf("SELECT %s FROM workloads WHERE id = $1", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(workloadQuery)).
		WithArgs(workloadID).
		WillReturnRows(workloadRows)

	runnerRows := pgxmock.NewRows(runnerRowColumns).
		AddRow(runnerID, "runner-1", pgtype.UUID{Bytes: organizationID, Valid: true}, identityID, "", "service-id", serviceName, runnerStatusEnrolled, labelsJSON, capabilitiesJSON, now, now)
	runnerQuery := fmt.Sprintf("SELECT %s FROM runners WHERE id = $1", runnerColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(runnerQuery)).
		WithArgs(runnerID).
		WillReturnRows(runnerRows)

	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
	}

	listener := startRunnerServer(t, func(req *runnerv1.StreamWorkloadLogsRequest, stream grpc.ServerStreamingServer[runnerv1.StreamWorkloadLogsResponse]) error {
		requestChan <- req
		return stream.Send(&runnerv1.StreamWorkloadLogsResponse{
			Event: &runnerv1.StreamWorkloadLogsResponse_Chunk{
				Chunk: &runnerv1.LogChunk{Data: []byte("hello")},
			},
		})
	})

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	var gotServiceName string
	srv.dialRunnerConn = func(ctx context.Context, name string) (*grpc.ClientConn, error) {
		gotServiceName = name
		return grpc.NewClient(
			"passthrough:///bufnet",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return listener.DialContext(ctx)
			}),
		)
	}

	streamCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	stream := &fakeRunnerLogStream{ctx: streamCtx}
	containerName := "  container-1 "
	err = srv.StreamWorkloadLogs(&runnerv1.StreamWorkloadLogsRequest{
		WorkloadId:    workloadID.String(),
		ContainerName: containerName,
		Follow:        true,
		TailLines:     3,
		SinceTime:     sinceTime,
	}, stream)
	if err != nil {
		t.Fatalf("StreamWorkloadLogs failed: %v", err)
	}

	if gotServiceName != serviceName {
		t.Fatalf("expected service name %q, got %q", serviceName, gotServiceName)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("expected 1 log chunk, got %d", len(stream.sent))
	}
	if string(stream.sent[0].GetChunk().GetData()) != "hello" {
		t.Fatalf("unexpected log data %q", string(stream.sent[0].GetChunk().GetData()))
	}

	select {
	case gotReq := <-requestChan:
		if gotReq.GetWorkloadId() != instanceID {
			t.Fatalf("expected instance id %q, got %q", instanceID, gotReq.GetWorkloadId())
		}
		if gotReq.GetContainerName() != strings.TrimSpace(containerName) {
			t.Fatalf("expected container name %q, got %q", strings.TrimSpace(containerName), gotReq.GetContainerName())
		}
		if gotReq.GetTailLines() != 3 {
			t.Fatalf("expected tail lines 3, got %d", gotReq.GetTailLines())
		}
		if gotReq.GetFollow() != true {
			t.Fatalf("expected follow true")
		}
		if gotReq.GetSinceTime() == nil || gotReq.GetSinceTime().AsTime().Unix() != sinceTime.AsTime().Unix() {
			t.Fatalf("expected since time %v, got %v", sinceTime, gotReq.GetSinceTime())
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for runner request")
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestStreamWorkloadLogsRequiresViewWorkloads(t *testing.T) {
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

	workloadRows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, "instance-1", now, nil, nil, runtimeOwnerKindAgentInstance, workloadID, now, now, nil, nil, int64(0), nil, []string(nil), []byte(nil), []byte(nil), nil)
	workloadQuery := fmt.Sprintf("SELECT %s FROM workloads WHERE id = $1", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(workloadQuery)).
		WithArgs(workloadID).
		WillReturnRows(workloadRows)

	var gotCheckReq *authorizationv1.CheckRequest
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			gotCheckReq = req
			return &authorizationv1.CheckResponse{Allowed: false}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	srv.dialRunnerConn = func(context.Context, string) (*grpc.ClientConn, error) {
		t.Fatal("unexpected runner dial")
		return nil, nil
	}

	streamCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	stream := &fakeRunnerLogStream{ctx: streamCtx}
	err = srv.StreamWorkloadLogs(&runnerv1.StreamWorkloadLogsRequest{
		WorkloadId:    workloadID.String(),
		ContainerName: "container-1",
	}, stream)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected permission denied, got %v", err)
	}
	if gotCheckReq == nil {
		t.Fatal("expected authorization Check to be called")
	}
	if gotCheckReq.GetTupleKey().GetRelation() != organizationViewWorkloads {
		t.Fatalf("expected view workloads relation, got %s", gotCheckReq.GetTupleKey().GetRelation())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestStreamWorkloadLogsMissingInstanceID(t *testing.T) {
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

	workloadRows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, workloadAgentStateProcessing, nil, nil, containersJSON, "ziti-id", int32(0), int64(0), "", true, nil, now, nil, nil, runtimeOwnerKindAgentInstance, workloadID, now, now, nil, nil, int64(0), nil, []string(nil), []byte(nil), []byte(nil), nil)
	workloadQuery := fmt.Sprintf("SELECT %s FROM workloads WHERE id = $1", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(workloadQuery)).
		WithArgs(workloadID).
		WillReturnRows(workloadRows)

	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	srv.dialRunnerConn = func(context.Context, string) (*grpc.ClientConn, error) {
		t.Fatal("unexpected runner dial")
		return nil, nil
	}

	streamCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	stream := &fakeRunnerLogStream{ctx: streamCtx}
	err = srv.StreamWorkloadLogs(&runnerv1.StreamWorkloadLogsRequest{
		WorkloadId:    workloadID.String(),
		ContainerName: "container-1",
	}, stream)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected failed precondition, got %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
