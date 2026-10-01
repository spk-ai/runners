package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"testing"
	"time"

	agentsv1 "github.com/agynio/runners/.gen/go/agynio/api/agents/v1"
	authorizationv1 "github.com/agynio/runners/.gen/go/agynio/api/authorization/v1"
	identityv1 "github.com/agynio/runners/.gen/go/agynio/api/identity/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	zitimanagementv1 "github.com/agynio/runners/.gen/go/agynio/api/ziti_management/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pashagolub/pgxmock/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type fakeZitiManagementClient struct {
	createRunnerIdentity func(ctx context.Context, req *zitimanagementv1.CreateRunnerIdentityRequest) (*zitimanagementv1.CreateRunnerIdentityResponse, error)
	createService        func(ctx context.Context, req *zitimanagementv1.CreateServiceRequest) (*zitimanagementv1.CreateServiceResponse, error)
	deleteRunnerIdentity func(ctx context.Context, req *zitimanagementv1.DeleteRunnerIdentityRequest) (*zitimanagementv1.DeleteRunnerIdentityResponse, error)
}

func (f fakeZitiManagementClient) CreateAgentIdentity(ctx context.Context, req *zitimanagementv1.CreateAgentIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.CreateAgentIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) CreateAppIdentity(ctx context.Context, req *zitimanagementv1.CreateAppIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.CreateAppIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) CreateSandboxIdentity(ctx context.Context, req *zitimanagementv1.CreateSandboxIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.CreateSandboxIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) CreateService(ctx context.Context, req *zitimanagementv1.CreateServiceRequest, opts ...grpc.CallOption) (*zitimanagementv1.CreateServiceResponse, error) {
	if f.createService == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return f.createService(ctx, req)
}

func (f fakeZitiManagementClient) GetService(ctx context.Context, req *zitimanagementv1.GetServiceRequest, opts ...grpc.CallOption) (*zitimanagementv1.GetServiceResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) ListServices(ctx context.Context, req *zitimanagementv1.ListServicesRequest, opts ...grpc.CallOption) (*zitimanagementv1.ListServicesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) DeleteIdentity(ctx context.Context, req *zitimanagementv1.DeleteIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.DeleteIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) DeleteAppIdentity(ctx context.Context, req *zitimanagementv1.DeleteAppIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.DeleteAppIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) CreateRunnerIdentity(ctx context.Context, req *zitimanagementv1.CreateRunnerIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.CreateRunnerIdentityResponse, error) {
	if f.createRunnerIdentity == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return f.createRunnerIdentity(ctx, req)
}

func (f fakeZitiManagementClient) DeleteRunnerIdentity(ctx context.Context, req *zitimanagementv1.DeleteRunnerIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.DeleteRunnerIdentityResponse, error) {
	if f.deleteRunnerIdentity == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return f.deleteRunnerIdentity(ctx, req)
}

func (f fakeZitiManagementClient) ListManagedIdentities(ctx context.Context, req *zitimanagementv1.ListManagedIdentitiesRequest, opts ...grpc.CallOption) (*zitimanagementv1.ListManagedIdentitiesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) ResolveIdentity(ctx context.Context, req *zitimanagementv1.ResolveIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.ResolveIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) RequestServiceIdentity(ctx context.Context, req *zitimanagementv1.RequestServiceIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.RequestServiceIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) ExtendIdentityLease(ctx context.Context, req *zitimanagementv1.ExtendIdentityLeaseRequest, opts ...grpc.CallOption) (*zitimanagementv1.ExtendIdentityLeaseResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) CreateServicePolicy(ctx context.Context, req *zitimanagementv1.CreateServicePolicyRequest, opts ...grpc.CallOption) (*zitimanagementv1.CreateServicePolicyResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) GetServicePolicy(ctx context.Context, req *zitimanagementv1.GetServicePolicyRequest, opts ...grpc.CallOption) (*zitimanagementv1.GetServicePolicyResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) ListServicePolicies(ctx context.Context, req *zitimanagementv1.ListServicePoliciesRequest, opts ...grpc.CallOption) (*zitimanagementv1.ListServicePoliciesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) DeleteServicePolicy(ctx context.Context, req *zitimanagementv1.DeleteServicePolicyRequest, opts ...grpc.CallOption) (*zitimanagementv1.DeleteServicePolicyResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) DeleteService(ctx context.Context, req *zitimanagementv1.DeleteServiceRequest, opts ...grpc.CallOption) (*zitimanagementv1.DeleteServiceResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) CreateDeviceIdentity(ctx context.Context, req *zitimanagementv1.CreateDeviceIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.CreateDeviceIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) DeleteDeviceIdentity(ctx context.Context, req *zitimanagementv1.DeleteDeviceIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.DeleteDeviceIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) CreateTunnelIdentity(ctx context.Context, req *zitimanagementv1.CreateTunnelIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.CreateTunnelIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) DeleteTunnelIdentity(ctx context.Context, req *zitimanagementv1.DeleteTunnelIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.DeleteTunnelIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) PatchIdentityRoleAttributes(ctx context.Context, req *zitimanagementv1.PatchIdentityRoleAttributesRequest, opts ...grpc.CallOption) (*zitimanagementv1.PatchIdentityRoleAttributesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) GetIdentityLiveness(ctx context.Context, req *zitimanagementv1.GetIdentityLivenessRequest, opts ...grpc.CallOption) (*zitimanagementv1.GetIdentityLivenessResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) ListServicesByTag(ctx context.Context, req *zitimanagementv1.ListServicesByTagRequest, opts ...grpc.CallOption) (*zitimanagementv1.ListServicesByTagResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) ListIdentitiesByTag(ctx context.Context, req *zitimanagementv1.ListIdentitiesByTagRequest, opts ...grpc.CallOption) (*zitimanagementv1.ListIdentitiesByTagResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) ListServicePoliciesByTag(ctx context.Context, req *zitimanagementv1.ListServicePoliciesByTagRequest, opts ...grpc.CallOption) (*zitimanagementv1.ListServicePoliciesByTagResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) UpdateService(ctx context.Context, req *zitimanagementv1.UpdateServiceRequest, opts ...grpc.CallOption) (*zitimanagementv1.UpdateServiceResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

type fakeIdentityClient struct {
	registerIdentity func(ctx context.Context, req *identityv1.RegisterIdentityRequest) (*identityv1.RegisterIdentityResponse, error)
}

func (f fakeIdentityClient) RegisterIdentity(ctx context.Context, req *identityv1.RegisterIdentityRequest, opts ...grpc.CallOption) (*identityv1.RegisterIdentityResponse, error) {
	if f.registerIdentity == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return f.registerIdentity(ctx, req)
}

func (f fakeIdentityClient) GetIdentityType(ctx context.Context, req *identityv1.GetIdentityTypeRequest, opts ...grpc.CallOption) (*identityv1.GetIdentityTypeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeIdentityClient) BatchGetIdentityTypes(ctx context.Context, req *identityv1.BatchGetIdentityTypesRequest, opts ...grpc.CallOption) (*identityv1.BatchGetIdentityTypesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeIdentityClient) SetNickname(ctx context.Context, req *identityv1.SetNicknameRequest, opts ...grpc.CallOption) (*identityv1.SetNicknameResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeIdentityClient) RemoveNickname(ctx context.Context, req *identityv1.RemoveNicknameRequest, opts ...grpc.CallOption) (*identityv1.RemoveNicknameResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeIdentityClient) ResolveNickname(ctx context.Context, req *identityv1.ResolveNicknameRequest, opts ...grpc.CallOption) (*identityv1.ResolveNicknameResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeIdentityClient) DeleteOrganizationResources(ctx context.Context, req *identityv1.DeleteOrganizationResourcesRequest, opts ...grpc.CallOption) (*identityv1.DeleteOrganizationResourcesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeIdentityClient) BatchGetNicknames(ctx context.Context, req *identityv1.BatchGetNicknamesRequest, opts ...grpc.CallOption) (*identityv1.BatchGetNicknamesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

type fakeAgentsClient struct {
	getAgent              func(ctx context.Context, req *agentsv1.GetAgentRequest) (*agentsv1.GetAgentResponse, error)
	getSandbox            func(ctx context.Context, req *agentsv1.GetSandboxRequest) (*agentsv1.GetSandboxResponse, error)
	getVolume             func(ctx context.Context, req *agentsv1.GetVolumeRequest) (*agentsv1.GetVolumeResponse, error)
	listVolumes           func(ctx context.Context, req *agentsv1.ListVolumesRequest) (*agentsv1.ListVolumesResponse, error)
	listVolumeAttachments func(ctx context.Context, req *agentsv1.ListVolumeAttachmentsRequest) (*agentsv1.ListVolumeAttachmentsResponse, error)
	getMcp                func(ctx context.Context, req *agentsv1.GetMcpRequest) (*agentsv1.GetMcpResponse, error)
}

func (f fakeAgentsClient) GetAgent(ctx context.Context, req *agentsv1.GetAgentRequest, opts ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
	if f.getAgent == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return f.getAgent(ctx, req)
}

func (f fakeAgentsClient) GetSandbox(ctx context.Context, req *agentsv1.GetSandboxRequest, opts ...grpc.CallOption) (*agentsv1.GetSandboxResponse, error) {
	if f.getSandbox == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return f.getSandbox(ctx, req)
}

func (f fakeAgentsClient) GetVolume(ctx context.Context, req *agentsv1.GetVolumeRequest, opts ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error) {
	if f.getVolume == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return f.getVolume(ctx, req)
}

func (f fakeAgentsClient) ListVolumes(ctx context.Context, req *agentsv1.ListVolumesRequest, opts ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error) {
	if f.listVolumes == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return f.listVolumes(ctx, req)
}

func (f fakeAgentsClient) ListVolumeAttachments(ctx context.Context, req *agentsv1.ListVolumeAttachmentsRequest, opts ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
	if f.listVolumeAttachments == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return f.listVolumeAttachments(ctx, req)
}

func (f fakeAgentsClient) GetMcp(ctx context.Context, req *agentsv1.GetMcpRequest, opts ...grpc.CallOption) (*agentsv1.GetMcpResponse, error) {
	if f.getMcp == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return f.getMcp(ctx, req)
}

type fakeAuthorizationClient struct {
	check func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error)
	write func(ctx context.Context, req *authorizationv1.WriteRequest) (*authorizationv1.WriteResponse, error)
}

func (f fakeAuthorizationClient) Check(ctx context.Context, req *authorizationv1.CheckRequest, opts ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {
	if f.check == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return f.check(ctx, req)
}

func (f fakeAuthorizationClient) BatchCheck(ctx context.Context, req *authorizationv1.BatchCheckRequest, opts ...grpc.CallOption) (*authorizationv1.BatchCheckResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeAuthorizationClient) Write(ctx context.Context, req *authorizationv1.WriteRequest, opts ...grpc.CallOption) (*authorizationv1.WriteResponse, error) {
	if f.write == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return f.write(ctx, req)
}

func (f fakeAuthorizationClient) Read(ctx context.Context, req *authorizationv1.ReadRequest, opts ...grpc.CallOption) (*authorizationv1.ReadResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeAuthorizationClient) ListObjects(ctx context.Context, req *authorizationv1.ListObjectsRequest, opts ...grpc.CallOption) (*authorizationv1.ListObjectsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeAuthorizationClient) ListUsers(ctx context.Context, req *authorizationv1.ListUsersRequest, opts ...grpc.CallOption) (*authorizationv1.ListUsersResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func TestRegisterRunnerPersistsLabels(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	labels := map[string]string{"type": "gpu", "region": "us-east-1"}
	labelsJSON, err := json.Marshal(labels)
	if err != nil {
		t.Fatalf("failed to marshal labels: %v", err)
	}
	capabilities := []string{"gpu", "docker"}
	capabilitiesJSON, err := json.Marshal(capabilities)
	if err != nil {
		t.Fatalf("failed to marshal capabilities: %v", err)
	}

	runnerID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	identityID := uuid.New()
	now := time.Now().UTC()
	zitiServiceID := "service-id"
	zitiServiceName := "runner-service"
	rows := pgxmock.NewRows([]string{"id", "name", "organization_id", "identity_id", "ziti_identity_id", "ziti_service_id", "ziti_service_name", "status", "labels", "capabilities", "created_at", "updated_at"}).
		AddRow(runnerID, "runner-1", pgtype.UUID{Bytes: organizationID, Valid: true}, identityID, "", zitiServiceID, zitiServiceName, runnerStatusPending, labelsJSON, capabilitiesJSON, now, now)

	matcher := regexp.QuoteMeta(fmt.Sprintf("INSERT INTO runners (id, name, organization_id, identity_id, ziti_identity_id, ziti_service_id, ziti_service_name, service_token_hash, status, labels, capabilities)\n\t    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)\n\t    RETURNING %s", runnerColumns))
	mockPool.ExpectQuery(matcher).
		WithArgs(pgxmock.AnyArg(), "runner-1", pgtype.UUID{Bytes: organizationID, Valid: true}, pgxmock.AnyArg(), "", zitiServiceID, zitiServiceName, pgxmock.AnyArg(), runnerStatusPending, labelsJSON, capabilitiesJSON).
		WillReturnRows(rows)

	var gotIdentityReq *identityv1.RegisterIdentityRequest
	identityClient := fakeIdentityClient{
		registerIdentity: func(ctx context.Context, req *identityv1.RegisterIdentityRequest) (*identityv1.RegisterIdentityResponse, error) {
			gotIdentityReq = req
			return &identityv1.RegisterIdentityResponse{}, nil
		},
	}
	var gotCheckReq *authorizationv1.CheckRequest
	var gotWriteReq *authorizationv1.WriteRequest
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			gotCheckReq = req
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
		write: func(ctx context.Context, req *authorizationv1.WriteRequest) (*authorizationv1.WriteResponse, error) {
			gotWriteReq = req
			return &authorizationv1.WriteResponse{}, nil
		},
	}
	var gotServiceReq *zitimanagementv1.CreateServiceRequest
	zitiClient := fakeZitiManagementClient{
		createService: func(ctx context.Context, req *zitimanagementv1.CreateServiceRequest) (*zitimanagementv1.CreateServiceResponse, error) {
			gotServiceReq = req
			return &zitimanagementv1.CreateServiceResponse{
				ZitiServiceId:   zitiServiceID,
				ZitiServiceName: zitiServiceName,
			}, nil
		},
	}

	srv := New(Options{
		Pool:                 mockPool,
		IdentityClient:       identityClient,
		AuthorizationClient:  authorizationClient,
		ZitiManagementClient: zitiClient,
	})

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	organizationValue := organizationID.String()
	resp, err := srv.RegisterRunner(ctx, &runnersv1.RegisterRunnerRequest{
		Name:           "runner-1",
		OrganizationId: &organizationValue,
		Labels:         labels,
		Capabilities:   capabilities,
	})
	if err != nil {
		t.Fatalf("RegisterRunner failed: %v", err)
	}
	if resp.GetRunner() == nil {
		t.Fatal("expected runner in response")
	}
	if resp.GetRunner().GetName() != "runner-1" {
		t.Fatalf("expected runner name %q, got %q", "runner-1", resp.GetRunner().GetName())
	}
	if !maps.Equal(resp.GetRunner().GetLabels(), labels) {
		t.Fatalf("expected labels %v, got %v", labels, resp.GetRunner().GetLabels())
	}
	if !slices.Equal(resp.GetRunner().GetCapabilities(), capabilities) {
		t.Fatalf("expected capabilities %v, got %v", capabilities, resp.GetRunner().GetCapabilities())
	}
	if resp.GetServiceToken() == "" {
		t.Fatal("expected service token")
	}
	if gotIdentityReq == nil {
		t.Fatal("expected RegisterIdentity to be called")
	}
	if gotCheckReq == nil {
		t.Fatal("expected authorization Check to be called")
	}
	if gotWriteReq == nil {
		t.Fatal("expected authorization Write to be called")
	}
	if gotCheckReq.GetTupleKey().GetUser() != fmt.Sprintf("identity:%s", callerID) {
		t.Fatalf("expected check user identity:%s, got %s", callerID, gotCheckReq.GetTupleKey().GetUser())
	}
	if gotCheckReq.GetTupleKey().GetRelation() != "owner" {
		t.Fatalf("expected owner relation, got %s", gotCheckReq.GetTupleKey().GetRelation())
	}
	if gotCheckReq.GetTupleKey().GetObject() != fmt.Sprintf("organization:%s", organizationID) {
		t.Fatalf("expected organization:%s object, got %s", organizationID, gotCheckReq.GetTupleKey().GetObject())
	}
	if len(gotWriteReq.GetWrites()) != 1 {
		t.Fatalf("expected one write tuple, got %d", len(gotWriteReq.GetWrites()))
	}
	if gotWriteReq.GetWrites()[0].GetRelation() != organizationMemberRelation {
		t.Fatalf("expected member relation, got %s", gotWriteReq.GetWrites()[0].GetRelation())
	}
	if gotWriteReq.GetWrites()[0].GetObject() != fmt.Sprintf("organization:%s", organizationID) {
		t.Fatalf("expected organization:%s write object, got %s", organizationID, gotWriteReq.GetWrites()[0].GetObject())
	}
	if gotServiceReq == nil {
		t.Fatal("expected CreateService to be called")
	}
	if gotServiceReq.GetName() != fmt.Sprintf("runner-%s", gotIdentityReq.GetIdentityId()) {
		t.Fatalf("expected service name %q, got %q", fmt.Sprintf("runner-%s", gotIdentityReq.GetIdentityId()), gotServiceReq.GetName())
	}
	if len(gotServiceReq.GetRoleAttributes()) != 1 || gotServiceReq.GetRoleAttributes()[0] != zitiRunnerServiceRole {
		t.Fatalf("unexpected role attributes: %v", gotServiceReq.GetRoleAttributes())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestRegisterRunnerClusterScopedRequiresAdmin(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	labels := map[string]string{"tier": "gpu"}
	labelsJSON, err := json.Marshal(labels)
	if err != nil {
		t.Fatalf("failed to marshal labels: %v", err)
	}
	capabilities := []string{"gpu"}
	capabilitiesJSON, err := json.Marshal(capabilities)
	if err != nil {
		t.Fatalf("failed to marshal capabilities: %v", err)
	}

	runnerID := uuid.New()
	callerID := uuid.New()
	identityID := uuid.New()
	now := time.Now().UTC()
	zitiServiceID := "service-id"
	zitiServiceName := "runner-service"
	rows := pgxmock.NewRows([]string{"id", "name", "organization_id", "identity_id", "ziti_identity_id", "ziti_service_id", "ziti_service_name", "status", "labels", "capabilities", "created_at", "updated_at"}).
		AddRow(runnerID, "runner-1", pgtype.UUID{Valid: false}, identityID, "", zitiServiceID, zitiServiceName, runnerStatusPending, labelsJSON, capabilitiesJSON, now, now)

	matcher := regexp.QuoteMeta(fmt.Sprintf("INSERT INTO runners (id, name, organization_id, identity_id, ziti_identity_id, ziti_service_id, ziti_service_name, service_token_hash, status, labels, capabilities)\n\t    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)\n\t    RETURNING %s", runnerColumns))
	mockPool.ExpectQuery(matcher).
		WithArgs(pgxmock.AnyArg(), "runner-1", pgxmock.AnyArg(), pgxmock.AnyArg(), "", zitiServiceID, zitiServiceName, pgxmock.AnyArg(), runnerStatusPending, labelsJSON, capabilitiesJSON).
		WillReturnRows(rows)

	identityClient := fakeIdentityClient{
		registerIdentity: func(ctx context.Context, req *identityv1.RegisterIdentityRequest) (*identityv1.RegisterIdentityResponse, error) {
			return &identityv1.RegisterIdentityResponse{}, nil
		},
	}

	var gotCheckReq *authorizationv1.CheckRequest
	writeCalled := false
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			gotCheckReq = req
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
		write: func(ctx context.Context, req *authorizationv1.WriteRequest) (*authorizationv1.WriteResponse, error) {
			writeCalled = true
			return &authorizationv1.WriteResponse{}, nil
		},
	}

	zitiClient := fakeZitiManagementClient{
		createService: func(ctx context.Context, req *zitimanagementv1.CreateServiceRequest) (*zitimanagementv1.CreateServiceResponse, error) {
			return &zitimanagementv1.CreateServiceResponse{
				ZitiServiceId:   zitiServiceID,
				ZitiServiceName: zitiServiceName,
			}, nil
		},
	}

	srv := New(Options{
		Pool:                 mockPool,
		IdentityClient:       identityClient,
		AuthorizationClient:  authorizationClient,
		ZitiManagementClient: zitiClient,
	})

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	_, err = srv.RegisterRunner(ctx, &runnersv1.RegisterRunnerRequest{
		Name:         "runner-1",
		Labels:       labels,
		Capabilities: capabilities,
	})
	if err != nil {
		t.Fatalf("RegisterRunner failed: %v", err)
	}
	if gotCheckReq == nil {
		t.Fatal("expected authorization Check to be called")
	}
	if gotCheckReq.GetTupleKey().GetRelation() != clusterAdminRelation {
		t.Fatalf("expected admin relation, got %s", gotCheckReq.GetTupleKey().GetRelation())
	}
	if gotCheckReq.GetTupleKey().GetObject() != clusterObject {
		t.Fatalf("expected cluster object %s, got %s", clusterObject, gotCheckReq.GetTupleKey().GetObject())
	}
	if writeCalled {
		t.Fatal("did not expect authorization Write for cluster-scoped runner")
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestGetRunnerRequiresMember(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	runnerID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	identityID := uuid.New()
	now := time.Now().UTC()
	labelsJSON := []byte("{}")
	capabilitiesJSON := []byte("[]")
	rows := pgxmock.NewRows([]string{"id", "name", "organization_id", "identity_id", "ziti_identity_id", "ziti_service_id", "ziti_service_name", "status", "labels", "capabilities", "created_at", "updated_at"}).
		AddRow(runnerID, "runner-1", pgtype.UUID{Bytes: organizationID, Valid: true}, identityID, "", "service-id", "runner-service", runnerStatusOffline, labelsJSON, capabilitiesJSON, now, now)

	query := fmt.Sprintf(`SELECT %s FROM runners WHERE id = $1`, runnerColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(runnerID).WillReturnRows(rows)

	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			return &authorizationv1.CheckResponse{Allowed: false}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	_, err = srv.GetRunner(ctx, &runnersv1.GetRunnerRequest{Id: runnerID.String()})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied error, got %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestGetRunnerInternalNoIdentity(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	runnerID := uuid.New()
	organizationID := uuid.New()
	identityID := uuid.New()
	now := time.Now().UTC()
	labelsJSON := []byte("{}")
	capabilitiesJSON := []byte("[]")
	rows := pgxmock.NewRows([]string{"id", "name", "organization_id", "identity_id", "ziti_identity_id", "ziti_service_id", "ziti_service_name", "status", "labels", "capabilities", "created_at", "updated_at"}).
		AddRow(runnerID, "runner-1", pgtype.UUID{Bytes: organizationID, Valid: true}, identityID, "", "service-id", "runner-service", runnerStatusOffline, labelsJSON, capabilitiesJSON, now, now)

	query := fmt.Sprintf(`SELECT %s FROM runners WHERE id = $1`, runnerColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(runnerID).WillReturnRows(rows)

	checkCalls := 0
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			checkCalls++
			return &authorizationv1.CheckResponse{Allowed: false}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	resp, err := srv.GetRunner(context.Background(), &runnersv1.GetRunnerRequest{Id: runnerID.String()})
	if err != nil {
		t.Fatalf("GetRunner failed: %v", err)
	}
	if resp.GetRunner() == nil {
		t.Fatal("expected runner in response")
	}
	if resp.GetRunner().GetOrganizationId() != organizationID.String() {
		t.Fatalf("expected organization id %q, got %q", organizationID.String(), resp.GetRunner().GetOrganizationId())
	}
	if checkCalls != 0 {
		t.Fatalf("expected no authorization checks, got %d", checkCalls)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListRunnersRequiresMember(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	organizationID := uuid.New()
	callerID := uuid.New()

	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			return &authorizationv1.CheckResponse{Allowed: false}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	organizationIDValue := organizationID.String()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	_, err = srv.ListRunners(ctx, &runnersv1.ListRunnersRequest{OrganizationId: &organizationIDValue})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied error, got %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListRunnersInternalNoIdentity(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	runnerID := uuid.New()
	organizationID := uuid.New()
	identityID := uuid.New()
	now := time.Now().UTC()
	labelsJSON := []byte("{}")
	capabilitiesJSON := []byte("[]")
	rows := pgxmock.NewRows([]string{"id", "name", "organization_id", "identity_id", "ziti_identity_id", "ziti_service_id", "ziti_service_name", "status", "labels", "capabilities", "created_at", "updated_at"}).
		AddRow(runnerID, "runner-1", pgtype.UUID{Bytes: organizationID, Valid: true}, identityID, "", "service-id", "runner-service", runnerStatusOffline, labelsJSON, capabilitiesJSON, now, now)

	limit := normalizePageSize(0)
	query := fmt.Sprintf("SELECT %s FROM runners ORDER BY id ASC LIMIT $1", runnerColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(int(limit) + 1).WillReturnRows(rows)

	checkCalls := 0
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			checkCalls++
			return &authorizationv1.CheckResponse{Allowed: false}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	resp, err := srv.ListRunners(context.Background(), &runnersv1.ListRunnersRequest{})
	if err != nil {
		t.Fatalf("ListRunners failed: %v", err)
	}
	if len(resp.GetRunners()) != 1 {
		t.Fatalf("expected 1 runner, got %d", len(resp.GetRunners()))
	}
	if resp.GetRunners()[0].GetOrganizationId() != organizationID.String() {
		t.Fatalf("expected organization id %q, got %q", organizationID.String(), resp.GetRunners()[0].GetOrganizationId())
	}
	if checkCalls != 0 {
		t.Fatalf("expected no authorization checks, got %d", checkCalls)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUpdateRunnerUpdatesLabels(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	runnerID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	identityID := uuid.New()
	labels := map[string]string{"env": "prod", "type": "cpu"}
	labelsJSON, err := json.Marshal(labels)
	if err != nil {
		t.Fatalf("failed to marshal labels: %v", err)
	}
	capabilities := []string{"containerd", "gpu"}
	capabilitiesJSON, err := json.Marshal(capabilities)
	if err != nil {
		t.Fatalf("failed to marshal capabilities: %v", err)
	}
	now := time.Now().UTC()
	getRows := pgxmock.NewRows([]string{"id", "name", "organization_id", "identity_id", "ziti_identity_id", "ziti_service_id", "ziti_service_name", "status", "labels", "capabilities", "created_at", "updated_at"}).
		AddRow(runnerID, "runner-updated", pgtype.UUID{Bytes: organizationID, Valid: true}, identityID, "", "service-id", "runner-service", runnerStatusOffline, labelsJSON, capabilitiesJSON, now, now)
	updateRows := pgxmock.NewRows([]string{"id", "name", "organization_id", "identity_id", "ziti_identity_id", "ziti_service_id", "ziti_service_name", "status", "labels", "capabilities", "created_at", "updated_at"}).
		AddRow(runnerID, "runner-updated", pgtype.UUID{Bytes: organizationID, Valid: true}, identityID, "", "service-id", "runner-service", runnerStatusOffline, labelsJSON, capabilitiesJSON, now, now)

	getQuery := regexp.QuoteMeta(fmt.Sprintf(`SELECT %s FROM runners WHERE id = $1`, runnerColumns))
	mockPool.ExpectQuery(getQuery).WithArgs(runnerID).WillReturnRows(getRows)

	matcher := regexp.QuoteMeta(fmt.Sprintf(`UPDATE runners SET name = COALESCE($1, name), labels = COALESCE($2::jsonb, labels), capabilities = COALESCE($3::jsonb, capabilities), updated_at = NOW() WHERE id = $4 RETURNING %s`, runnerColumns))
	mockPool.ExpectQuery(matcher).
		WithArgs(
			pgtype.Text{String: "runner-updated", Valid: true},
			pgtype.Text{String: string(labelsJSON), Valid: true},
			pgtype.Text{String: string(capabilitiesJSON), Valid: true},
			runnerID,
		).
		WillReturnRows(updateRows)

	var gotCheckReq *authorizationv1.CheckRequest
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			gotCheckReq = req
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	name := "runner-updated"
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	resp, err := srv.UpdateRunner(ctx, &runnersv1.UpdateRunnerRequest{
		Id:           runnerID.String(),
		Name:         &name,
		Labels:       labels,
		Capabilities: capabilities,
	})
	if err != nil {
		t.Fatalf("UpdateRunner failed: %v", err)
	}
	if resp.GetRunner() == nil {
		t.Fatal("expected runner in response")
	}
	if resp.GetRunner().GetName() != name {
		t.Fatalf("expected runner name %q, got %q", name, resp.GetRunner().GetName())
	}
	if !maps.Equal(resp.GetRunner().GetLabels(), labels) {
		t.Fatalf("expected labels %v, got %v", labels, resp.GetRunner().GetLabels())
	}
	if !slices.Equal(resp.GetRunner().GetCapabilities(), capabilities) {
		t.Fatalf("expected capabilities %v, got %v", capabilities, resp.GetRunner().GetCapabilities())
	}
	if gotCheckReq == nil {
		t.Fatal("expected authorization Check to be called")
	}
	if gotCheckReq.GetTupleKey().GetRelation() != "owner" {
		t.Fatalf("expected owner relation, got %s", gotCheckReq.GetTupleKey().GetRelation())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestEnrollRunnerRequiresToken(t *testing.T) {
	srv := New(Options{})

	_, err := srv.EnrollRunner(context.Background(), &runnersv1.EnrollRunnerRequest{ServiceToken: "   "})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument error, got %v", err)
	}
}

func TestEnrollRunnerNotFound(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	token := "missing-token"
	query := fmt.Sprintf(`SELECT %s FROM runners WHERE service_token_hash = $1`, runnerColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(hashServiceToken(token)).WillReturnError(pgx.ErrNoRows)

	srv := New(Options{
		Pool:                 mockPool,
		ZitiManagementClient: fakeZitiManagementClient{},
	})

	_, err = srv.EnrollRunner(context.Background(), &runnersv1.EnrollRunnerRequest{ServiceToken: token})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound error, got %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestEnrollRunnerSuccess(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	matcher := regexp.QuoteMeta(fmt.Sprintf(`SELECT %s FROM runners WHERE service_token_hash = $1`, runnerColumns))
	updateQuery := regexp.QuoteMeta(`UPDATE runners SET status = $1, ziti_identity_id = $2, updated_at = NOW() WHERE id = $3`)

	runnerID := uuid.New()
	identityID := uuid.New()
	now := time.Now().UTC()
	labelsJSON := []byte("{}")
	capabilitiesJSON := []byte("[]")
	serviceName := "runner-service"
	rows := pgxmock.NewRows([]string{"id", "name", "organization_id", "identity_id", "ziti_identity_id", "ziti_service_id", "ziti_service_name", "status", "labels", "capabilities", "created_at", "updated_at"}).
		AddRow(runnerID, "runner-1", pgtype.UUID{Valid: false}, identityID, "", "service-id", serviceName, runnerStatusPending, labelsJSON, capabilitiesJSON, now, now)

	token := "enroll-token"
	mockPool.ExpectQuery(matcher).WithArgs(hashServiceToken(token)).WillReturnRows(rows)
	mockPool.ExpectExec(updateQuery).WithArgs(runnerStatusEnrolled, "ziti-id", runnerID).WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	identityJSON := []byte("identity-json")
	var gotReq *zitimanagementv1.CreateRunnerIdentityRequest
	fakeClient := fakeZitiManagementClient{
		createRunnerIdentity: func(ctx context.Context, req *zitimanagementv1.CreateRunnerIdentityRequest) (*zitimanagementv1.CreateRunnerIdentityResponse, error) {
			gotReq = req
			return &zitimanagementv1.CreateRunnerIdentityResponse{
				IdentityJson:   identityJSON,
				ZitiIdentityId: "ziti-id",
			}, nil
		},
	}

	srv := New(Options{
		Pool:                 mockPool,
		ZitiManagementClient: fakeClient,
	})

	resp, err := srv.EnrollRunner(context.Background(), &runnersv1.EnrollRunnerRequest{ServiceToken: token})
	if err != nil {
		t.Fatalf("EnrollRunner failed: %v", err)
	}
	if resp.GetServiceName() != serviceName {
		t.Fatalf("expected service name %q, got %q", serviceName, resp.GetServiceName())
	}
	if string(resp.GetIdentityJson()) != string(identityJSON) {
		t.Fatalf("expected identity json %q, got %q", identityJSON, resp.GetIdentityJson())
	}
	if resp.GetIdentityId() != "ziti-id" {
		t.Fatalf("expected identity id %q, got %q", "ziti-id", resp.GetIdentityId())
	}

	if gotReq == nil {
		t.Fatal("expected CreateRunnerIdentity to be called")
	}
	if gotReq.GetRunnerId() != runnerID.String() {
		t.Fatalf("expected runner id %q, got %q", runnerID.String(), gotReq.GetRunnerId())
	}
	if len(gotReq.GetRoleAttributes()) != 1 || gotReq.GetRoleAttributes()[0] != zitiRunnerRoleAttribute {
		t.Fatalf("unexpected role attributes: %v", gotReq.GetRoleAttributes())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestEnrollRunnerZitiFailure(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	matcher := regexp.QuoteMeta(fmt.Sprintf(`SELECT %s FROM runners WHERE service_token_hash = $1`, runnerColumns))
	runnerID := uuid.New()
	identityID := uuid.New()
	now := time.Now().UTC()
	labelsJSON := []byte("{}")
	capabilitiesJSON := []byte("[]")
	rows := pgxmock.NewRows([]string{"id", "name", "organization_id", "identity_id", "ziti_identity_id", "ziti_service_id", "ziti_service_name", "status", "labels", "capabilities", "created_at", "updated_at"}).
		AddRow(runnerID, "runner-1", pgtype.UUID{Valid: false}, identityID, "", "service-id", "runner-service", runnerStatusPending, labelsJSON, capabilitiesJSON, now, now)

	token := "bad-ziti"
	mockPool.ExpectQuery(matcher).WithArgs(hashServiceToken(token)).WillReturnRows(rows)

	fakeClient := fakeZitiManagementClient{
		createRunnerIdentity: func(ctx context.Context, req *zitimanagementv1.CreateRunnerIdentityRequest) (*zitimanagementv1.CreateRunnerIdentityResponse, error) {
			return nil, errors.New("ziti failure")
		},
	}

	srv := New(Options{
		Pool:                 mockPool,
		ZitiManagementClient: fakeClient,
	})

	_, err = srv.EnrollRunner(context.Background(), &runnersv1.EnrollRunnerRequest{ServiceToken: token})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal error, got %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestDeleteRunnerCallsZitiCleanup(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	query := fmt.Sprintf(`SELECT %s FROM runners WHERE id = $1`, runnerColumns)
	deleteQuery := regexp.QuoteMeta(`DELETE FROM runners WHERE id = $1`)

	runnerID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	identityID := uuid.New()
	now := time.Now().UTC()
	labelsJSON := []byte("{}")
	capabilitiesJSON := []byte("[]")
	zitiServiceID := "service-id"
	zitiServiceName := "runner-service"
	zitiIdentityID := "ziti-identity"
	rows := pgxmock.NewRows([]string{"id", "name", "organization_id", "identity_id", "ziti_identity_id", "ziti_service_id", "ziti_service_name", "status", "labels", "capabilities", "created_at", "updated_at"}).
		AddRow(runnerID, "runner-1", pgtype.UUID{Bytes: organizationID, Valid: true}, identityID, zitiIdentityID, zitiServiceID, zitiServiceName, runnerStatusOffline, labelsJSON, capabilitiesJSON, now, now)

	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(runnerID).WillReturnRows(rows)
	mockPool.ExpectExec(deleteQuery).WithArgs(runnerID).WillReturnResult(pgxmock.NewResult("DELETE", 1))

	var gotDeleteReq *zitimanagementv1.DeleteRunnerIdentityRequest
	zitiClient := fakeZitiManagementClient{
		deleteRunnerIdentity: func(ctx context.Context, req *zitimanagementv1.DeleteRunnerIdentityRequest) (*zitimanagementv1.DeleteRunnerIdentityResponse, error) {
			gotDeleteReq = req
			return &zitimanagementv1.DeleteRunnerIdentityResponse{}, nil
		},
	}

	var gotCheckReq *authorizationv1.CheckRequest
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			gotCheckReq = req
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
		write: func(ctx context.Context, req *authorizationv1.WriteRequest) (*authorizationv1.WriteResponse, error) {
			return &authorizationv1.WriteResponse{}, nil
		},
	}

	srv := New(Options{
		Pool:                 mockPool,
		AuthorizationClient:  authorizationClient,
		ZitiManagementClient: zitiClient,
	})

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	_, err = srv.DeleteRunner(ctx, &runnersv1.DeleteRunnerRequest{Id: runnerID.String()})
	if err != nil {
		t.Fatalf("DeleteRunner failed: %v", err)
	}

	if gotDeleteReq == nil {
		t.Fatal("expected DeleteRunnerIdentity to be called")
	}
	if gotDeleteReq.GetIdentityId() != identityID.String() {
		t.Fatalf("expected identity id %q, got %q", identityID.String(), gotDeleteReq.GetIdentityId())
	}
	if gotDeleteReq.GetZitiServiceId() != zitiServiceID {
		t.Fatalf("expected ziti service id %q, got %q", zitiServiceID, gotDeleteReq.GetZitiServiceId())
	}
	if gotCheckReq == nil {
		t.Fatal("expected authorization Check to be called")
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestDeleteRunnerBestEffortZitiFailure(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	query := fmt.Sprintf(`SELECT %s FROM runners WHERE id = $1`, runnerColumns)
	deleteQuery := regexp.QuoteMeta(`DELETE FROM runners WHERE id = $1`)

	runnerID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	identityID := uuid.New()
	now := time.Now().UTC()
	labelsJSON := []byte("{}")
	capabilitiesJSON := []byte("[]")
	rows := pgxmock.NewRows([]string{"id", "name", "organization_id", "identity_id", "ziti_identity_id", "ziti_service_id", "ziti_service_name", "status", "labels", "capabilities", "created_at", "updated_at"}).
		AddRow(runnerID, "runner-1", pgtype.UUID{Bytes: organizationID, Valid: true}, identityID, "ziti-identity", "service-id", "runner-service", runnerStatusOffline, labelsJSON, capabilitiesJSON, now, now)

	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(runnerID).WillReturnRows(rows)
	mockPool.ExpectExec(deleteQuery).WithArgs(runnerID).WillReturnResult(pgxmock.NewResult("DELETE", 1))

	var gotDeleteReq *zitimanagementv1.DeleteRunnerIdentityRequest
	zitiClient := fakeZitiManagementClient{
		deleteRunnerIdentity: func(ctx context.Context, req *zitimanagementv1.DeleteRunnerIdentityRequest) (*zitimanagementv1.DeleteRunnerIdentityResponse, error) {
			gotDeleteReq = req
			return nil, errors.New("delete failure")
		},
	}

	var gotCheckReq *authorizationv1.CheckRequest
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			gotCheckReq = req
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
		write: func(ctx context.Context, req *authorizationv1.WriteRequest) (*authorizationv1.WriteResponse, error) {
			return &authorizationv1.WriteResponse{}, nil
		},
	}

	srv := New(Options{
		Pool:                 mockPool,
		AuthorizationClient:  authorizationClient,
		ZitiManagementClient: zitiClient,
	})

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	_, err = srv.DeleteRunner(ctx, &runnersv1.DeleteRunnerRequest{Id: runnerID.String()})
	if err != nil {
		t.Fatalf("DeleteRunner failed: %v", err)
	}
	if gotDeleteReq == nil {
		t.Fatal("expected DeleteRunnerIdentity to be called")
	}
	if gotCheckReq == nil {
		t.Fatal("expected authorization Check to be called")
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestDeleteRunnerRetentionFailurePreservesExternalIdentity(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	query := fmt.Sprintf(`SELECT %s FROM runners WHERE id = $1`, runnerColumns)
	deleteQuery := regexp.QuoteMeta(`DELETE FROM runners WHERE id = $1`)

	runnerID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	identityID := uuid.New()
	now := time.Now().UTC()
	labelsJSON := []byte("{}")
	capabilitiesJSON := []byte("[]")
	zitiServiceID := "service-id"
	zitiServiceName := "runner-service"
	zitiIdentityID := "ziti-identity"
	rows := pgxmock.NewRows([]string{"id", "name", "organization_id", "identity_id", "ziti_identity_id", "ziti_service_id", "ziti_service_name", "status", "labels", "capabilities", "created_at", "updated_at"}).
		AddRow(runnerID, "runner-1", pgtype.UUID{Bytes: organizationID, Valid: true}, identityID, zitiIdentityID, zitiServiceID, zitiServiceName, runnerStatusOffline, labelsJSON, capabilitiesJSON, now, now)

	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(runnerID).WillReturnRows(rows)
	mockPool.ExpectExec(deleteQuery).WithArgs(runnerID).WillReturnError(errors.New("retained admission history"))

	var gotDeleteReq *zitimanagementv1.DeleteRunnerIdentityRequest
	zitiClient := fakeZitiManagementClient{
		deleteRunnerIdentity: func(ctx context.Context, req *zitimanagementv1.DeleteRunnerIdentityRequest) (*zitimanagementv1.DeleteRunnerIdentityResponse, error) {
			gotDeleteReq = req
			return &zitimanagementv1.DeleteRunnerIdentityResponse{}, nil
		},
	}

	var gotCheckReq *authorizationv1.CheckRequest
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			gotCheckReq = req
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
		write: func(ctx context.Context, req *authorizationv1.WriteRequest) (*authorizationv1.WriteResponse, error) {
			t.Fatal("rejected database deletion changed authorization")
			return nil, nil
		},
	}

	srv := New(Options{
		Pool:                 mockPool,
		AuthorizationClient:  authorizationClient,
		ZitiManagementClient: zitiClient,
	})

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	_, err = srv.DeleteRunner(ctx, &runnersv1.DeleteRunnerRequest{Id: runnerID.String()})
	if err == nil || gotDeleteReq != nil {
		t.Fatal("rejected database deletion changed external identity")
	}
	if gotCheckReq == nil {
		t.Fatal("missing authorization check")
	}
	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
