package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

	authorizationv1 "github.com/agynio/runners/.gen/go/agynio/api/authorization/v1"
	identityv1 "github.com/agynio/runners/.gen/go/agynio/api/identity/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	zitimanagementv1 "github.com/agynio/runners/.gen/go/agynio/api/ziti_management/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	runnerStatusPending  = "pending"
	runnerStatusEnrolled = "enrolled"
	runnerStatusOffline  = "offline"

	zitiRunnerRoleAttribute = "runners"
	zitiRunnerServiceRole   = "runner-services"

	runnerColumns = `id, name, organization_id, identity_id, ziti_identity_id, ziti_service_id, ziti_service_name, status, labels, capabilities, created_at, updated_at`
)

type runnerRecord struct {
	Meta            entityMeta
	Name            string
	OrganizationID  *uuid.UUID
	IdentityID      uuid.UUID
	ZitiIdentityID  string
	ZitiServiceID   string
	ZitiServiceName string
	Status          string
	Labels          map[string]string
	Capabilities    []string
}

type runnerInsertInput struct {
	ID               uuid.UUID
	Name             string
	OrganizationID   *uuid.UUID
	IdentityID       uuid.UUID
	ZitiIdentityID   string
	ZitiServiceID    string
	ZitiServiceName  string
	ServiceTokenHash string
	Status           string
	Labels           map[string]string
	Capabilities     []string
}

type runnerUpdateInput struct {
	ID           uuid.UUID
	Name         *string
	Labels       *map[string]string
	Capabilities *[]string
}

func decodeCapabilities(value []byte) ([]string, error) {
	if value == nil {
		return nil, fmt.Errorf("capabilities is NULL")
	}
	var capabilities []string
	if err := json.Unmarshal(value, &capabilities); err != nil {
		return nil, fmt.Errorf("decode capabilities: %w", err)
	}
	if capabilities == nil {
		return nil, fmt.Errorf("capabilities must be a JSON array")
	}
	return capabilities, nil
}

func encodeCapabilities(capabilities []string) ([]byte, error) {
	if capabilities == nil {
		capabilities = []string{}
	}
	data, err := json.Marshal(capabilities)
	if err != nil {
		return nil, fmt.Errorf("encode capabilities: %w", err)
	}
	return data, nil
}

func (s *Server) RegisterRunner(ctx context.Context, req *runnersv1.RegisterRunnerRequest) (*runnersv1.RegisterRunnerResponse, error) {
	callerID, err := identityFromMetadata(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	name := strings.TrimSpace(req.GetName())
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name must be provided")
	}
	labels := req.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	capabilities := append([]string(nil), req.GetCapabilities()...)

	var organizationID *uuid.UUID
	if req.OrganizationId != nil {
		parsed, err := parseUUID(req.GetOrganizationId())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "organization_id: %v", err)
		}
		organizationID = &parsed
	}

	if organizationID == nil {
		if err := s.requireClusterAdmin(ctx, callerID); err != nil {
			return nil, err
		}
	} else if err := s.requireOrgOwner(ctx, callerID, *organizationID); err != nil {
		return nil, err
	}

	runnerID := uuid.New()

	if _, err := s.identityClient.RegisterIdentity(ctx, &identityv1.RegisterIdentityRequest{
		IdentityId:   runnerID.String(),
		IdentityType: identityv1.IdentityType_IDENTITY_TYPE_RUNNER,
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "register identity: %v", err)
	}

	serviceName := fmt.Sprintf("runner-%s", runnerID.String())
	serviceResp, err := s.zitiManagementClient.CreateService(ctx, &zitimanagementv1.CreateServiceRequest{
		Name:           serviceName,
		RoleAttributes: []string{zitiRunnerServiceRole},
	})
	if err != nil {
		s.cleanupRunnerAuthorization(ctx, runnerID, organizationID)
		return nil, status.Errorf(codes.Internal, "create ziti service: %v", err)
	}

	if err := s.writeRunnerAuthorization(ctx, runnerID, organizationID); err != nil {
		return nil, err
	}

	token, tokenHash, err := generateServiceToken()
	if err != nil {
		s.cleanupRunnerAuthorization(ctx, runnerID, organizationID)
		return nil, status.Errorf(codes.Internal, "generate service token: %v", err)
	}

	runner, err := s.insertRunner(ctx, runnerInsertInput{
		ID:               runnerID,
		Name:             name,
		OrganizationID:   organizationID,
		IdentityID:       runnerID,
		ZitiIdentityID:   "",
		ZitiServiceID:    serviceResp.GetZitiServiceId(),
		ZitiServiceName:  serviceResp.GetZitiServiceName(),
		ServiceTokenHash: tokenHash,
		Status:           runnerStatusPending,
		Labels:           labels,
		Capabilities:     capabilities,
	})
	if err != nil {
		s.cleanupRunnerAuthorization(ctx, runnerID, organizationID)
		return nil, toStatusError(err)
	}

	protoRunner, err := toProtoRunner(runner)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert runner: %v", err)
	}
	return &runnersv1.RegisterRunnerResponse{Runner: protoRunner, ServiceToken: token}, nil
}

func (s *Server) GetRunner(ctx context.Context, req *runnersv1.GetRunnerRequest) (*runnersv1.GetRunnerResponse, error) {
	callerID, err := identityFromMetadataOptional(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}
	runner, err := s.getRunnerByID(ctx, id)
	if err != nil {
		return nil, toStatusError(err)
	}
	if callerID != nil && runner.OrganizationID != nil {
		if err := s.requireOrgMember(ctx, *callerID, *runner.OrganizationID); err != nil {
			return nil, err
		}
	}
	protoRunner, err := toProtoRunner(runner)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert runner: %v", err)
	}
	return &runnersv1.GetRunnerResponse{Runner: protoRunner}, nil
}

func (s *Server) UpdateRunner(ctx context.Context, req *runnersv1.UpdateRunnerRequest) (*runnersv1.UpdateRunnerResponse, error) {
	callerID, err := identityFromMetadata(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}

	var name *string
	if req.Name != nil {
		trimmed := strings.TrimSpace(req.GetName())
		if trimmed == "" {
			return nil, status.Error(codes.InvalidArgument, "name must be provided")
		}
		name = &trimmed
	}

	var labels *map[string]string
	if req.Labels != nil {
		value := req.Labels
		labels = &value
	}
	// NOTE: proto3 repeated fields do not track presence on the wire. A nil
	// slice indicates the caller did not set capabilities; when provided, the
	// list replaces existing capabilities.
	capabilitiesProvided := req.Capabilities != nil
	var capabilities *[]string
	if capabilitiesProvided {
		value := append([]string(nil), req.GetCapabilities()...)
		capabilities = &value
	}

	runnerRecord, err := s.getRunnerByID(ctx, id)
	if err != nil {
		return nil, toStatusError(err)
	}
	if runnerRecord.OrganizationID == nil {
		if err := s.requireClusterAdmin(ctx, callerID); err != nil {
			return nil, err
		}
	} else if err := s.requireOrgOwner(ctx, callerID, *runnerRecord.OrganizationID); err != nil {
		return nil, err
	}

	runner, err := s.updateRunner(ctx, runnerUpdateInput{ID: id, Name: name, Labels: labels, Capabilities: capabilities})
	if err != nil {
		return nil, toStatusError(err)
	}
	protoRunner, err := toProtoRunner(runner)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert runner: %v", err)
	}
	return &runnersv1.UpdateRunnerResponse{Runner: protoRunner}, nil
}

func (s *Server) ListRunners(ctx context.Context, req *runnersv1.ListRunnersRequest) (*runnersv1.ListRunnersResponse, error) {
	callerID, err := identityFromMetadataOptional(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	var organizationID *uuid.UUID
	if req.OrganizationId != nil {
		parsed, err := parseUUID(req.GetOrganizationId())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "organization_id: %v", err)
		}
		organizationID = &parsed
	}
	if callerID != nil && organizationID != nil {
		if err := s.requireOrgMember(ctx, *callerID, *organizationID); err != nil {
			return nil, err
		}
	}

	runners, nextToken, err := s.listRunners(ctx, organizationID, req.GetPageSize(), req.GetPageToken())
	if err != nil {
		var invalidToken *InvalidPageTokenError
		if errors.As(err, &invalidToken) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", invalidToken.Err)
		}
		return nil, status.Errorf(codes.Internal, "list runners: %v", err)
	}

	if callerID != nil && organizationID == nil {
		memberCache := map[uuid.UUID]bool{}
		filtered := make([]runnerRecord, 0, len(runners))
		for _, runner := range runners {
			if runner.OrganizationID == nil {
				filtered = append(filtered, runner)
				continue
			}
			allowed, err := s.memberAllowed(ctx, *callerID, *runner.OrganizationID, memberCache)
			if err != nil {
				return nil, err
			}
			if allowed {
				filtered = append(filtered, runner)
			}
		}
		runners = filtered
	}

	protoRunners := make([]*runnersv1.Runner, 0, len(runners))
	for _, runner := range runners {
		protoRunner, err := toProtoRunner(runner)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "convert runner: %v", err)
		}
		protoRunners = append(protoRunners, protoRunner)
	}
	return &runnersv1.ListRunnersResponse{Runners: protoRunners, NextPageToken: nextToken}, nil
}

func (s *Server) DeleteRunner(ctx context.Context, req *runnersv1.DeleteRunnerRequest) (*runnersv1.DeleteRunnerResponse, error) {
	callerID, err := identityFromMetadata(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}

	runner, err := s.getRunnerByID(ctx, id)
	if err != nil {
		return nil, toStatusError(err)
	}
	if runner.OrganizationID == nil {
		if err := s.requireClusterAdmin(ctx, callerID); err != nil {
			return nil, err
		}
	} else if err := s.requireOrgOwner(ctx, callerID, *runner.OrganizationID); err != nil {
		return nil, err
	}

	// Database retention/admission guards must succeed before external identity
	// or authorization cleanup. A rejected deletion must leave the runner usable.
	if err := s.deleteRunner(ctx, id); err != nil {
		return nil, toStatusError(err)
	}
	if runner.ZitiServiceID != "" || runner.ZitiIdentityID != "" {
		if _, err := s.zitiManagementClient.DeleteRunnerIdentity(ctx, &zitimanagementv1.DeleteRunnerIdentityRequest{
			IdentityId:    runner.IdentityID.String(),
			ZitiServiceId: runner.ZitiServiceID,
		}); err != nil {
			log.Printf("delete runner identity: %v", err)
		}
	}

	s.cleanupRunnerAuthorization(ctx, runner.IdentityID, runner.OrganizationID)

	return &runnersv1.DeleteRunnerResponse{}, nil
}

func (s *Server) ValidateServiceToken(ctx context.Context, req *runnersv1.ValidateServiceTokenRequest) (*runnersv1.ValidateServiceTokenResponse, error) {
	token := strings.TrimSpace(req.GetTokenHash())
	if token == "" {
		return nil, status.Error(codes.InvalidArgument, "token_hash must be provided")
	}
	tokenHash := hashServiceToken(token)
	runner, err := s.getRunnerByServiceTokenHash(ctx, tokenHash)
	if err != nil {
		return nil, toStatusError(err)
	}
	protoRunner, err := toProtoRunner(runner)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert runner: %v", err)
	}
	return &runnersv1.ValidateServiceTokenResponse{Runner: protoRunner}, nil
}

func (s *Server) EnrollRunner(ctx context.Context, req *runnersv1.EnrollRunnerRequest) (*runnersv1.EnrollRunnerResponse, error) {
	serviceToken := strings.TrimSpace(req.GetServiceToken())
	if serviceToken == "" {
		return nil, status.Error(codes.InvalidArgument, "service_token must be provided")
	}

	tokenHash := hashServiceToken(serviceToken)
	runner, err := s.getRunnerByServiceTokenHash(ctx, tokenHash)
	if err != nil {
		return nil, toStatusError(err)
	}

	zitiResp, err := s.zitiManagementClient.CreateRunnerIdentity(ctx, &zitimanagementv1.CreateRunnerIdentityRequest{
		RunnerId:       runner.Meta.ID.String(),
		RoleAttributes: []string{zitiRunnerRoleAttribute},
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create ziti identity: %v", err)
	}

	if err := s.updateRunnerEnrollment(ctx, runner.Meta.ID, runnerStatusEnrolled, zitiResp.GetZitiIdentityId()); err != nil {
		return nil, toStatusError(err)
	}

	return &runnersv1.EnrollRunnerResponse{
		IdentityJson: zitiResp.GetIdentityJson(),
		ServiceName:  runner.ZitiServiceName,
		IdentityId:   zitiResp.GetZitiIdentityId(),
	}, nil
}

func (s *Server) writeRunnerAuthorization(ctx context.Context, runnerID uuid.UUID, organizationID *uuid.UUID) error {
	tuple := runnerAuthorizationTuple(runnerID, organizationID)
	if tuple == nil {
		return nil
	}
	if _, err := s.authorizationClient.Write(ctx, &authorizationv1.WriteRequest{Writes: []*authorizationv1.TupleKey{tuple}}); err != nil {
		return status.Errorf(codes.Internal, "authorization write: %v", err)
	}
	return nil
}

func (s *Server) cleanupRunnerAuthorization(ctx context.Context, runnerID uuid.UUID, organizationID *uuid.UUID) {
	tuple := runnerAuthorizationTuple(runnerID, organizationID)
	if tuple == nil {
		return
	}
	_, _ = s.authorizationClient.Write(ctx, &authorizationv1.WriteRequest{Deletes: []*authorizationv1.TupleKey{tuple}})
}

func runnerAuthorizationTuple(runnerID uuid.UUID, organizationID *uuid.UUID) *authorizationv1.TupleKey {
	if organizationID != nil {
		return &authorizationv1.TupleKey{
			User:     identityObject(runnerID),
			Relation: organizationMemberRelation,
			Object:   organizationObject(*organizationID),
		}
	}
	return nil
}

func (s *Server) insertRunner(ctx context.Context, input runnerInsertInput) (runnerRecord, error) {
	labelsJSON, err := json.Marshal(input.Labels)
	if err != nil {
		return runnerRecord{}, err
	}
	capabilitiesJSON, err := encodeCapabilities(input.Capabilities)
	if err != nil {
		return runnerRecord{}, err
	}
	row := s.pool.QueryRow(ctx,
		fmt.Sprintf(`INSERT INTO runners (id, name, organization_id, identity_id, ziti_identity_id, ziti_service_id, ziti_service_name, service_token_hash, status, labels, capabilities)
	    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	    RETURNING %s`, runnerColumns),
		input.ID,
		input.Name,
		pgtype.UUID{Bytes: input.OrganizationIDBytes(), Valid: input.OrganizationID != nil},
		input.IdentityID,
		input.ZitiIdentityID,
		input.ZitiServiceID,
		input.ZitiServiceName,
		input.ServiceTokenHash,
		input.Status,
		labelsJSON,
		capabilitiesJSON,
	)
	runner, err := scanRunner(row)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return runnerRecord{}, AlreadyExists("runner")
		}
		return runnerRecord{}, err
	}
	return runner, nil
}

func (input runnerInsertInput) OrganizationIDBytes() uuid.UUID {
	if input.OrganizationID == nil {
		return uuid.UUID{}
	}
	return *input.OrganizationID
}

func (s *Server) getRunnerByID(ctx context.Context, id uuid.UUID) (runnerRecord, error) {
	row := s.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT %s FROM runners WHERE id = $1`, runnerColumns),
		id,
	)
	runner, err := scanRunner(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return runnerRecord{}, NotFound("runner")
		}
		return runnerRecord{}, err
	}
	return runner, nil
}

func (s *Server) getRunnerByServiceTokenHash(ctx context.Context, tokenHash string) (runnerRecord, error) {
	row := s.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT %s FROM runners WHERE service_token_hash = $1`, runnerColumns),
		tokenHash,
	)
	runner, err := scanRunner(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return runnerRecord{}, NotFound("runner")
		}
		return runnerRecord{}, err
	}
	return runner, nil
}

func (s *Server) listRunners(ctx context.Context, organizationID *uuid.UUID, pageSize int32, pageToken string) ([]runnerRecord, string, error) {
	var (
		clauses []string
		args    []any
	)
	if organizationID != nil {
		clauses = append(clauses, fmt.Sprintf("(organization_id = $%d OR organization_id IS NULL)", len(args)+1))
		args = append(args, *organizationID)
	}
	return listWithPagination(ctx, s.pool, fmt.Sprintf("SELECT %s FROM runners", runnerColumns), clauses, args, pageSize, pageToken, scanRunner, func(record runnerRecord) uuid.UUID {
		return record.Meta.ID
	})
}

func (s *Server) deleteRunner(ctx context.Context, id uuid.UUID) error {
	result, err := s.pool.Exec(ctx, `DELETE FROM runners WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return NotFound("runner")
	}
	return nil
}

func (s *Server) updateRunner(ctx context.Context, input runnerUpdateInput) (runnerRecord, error) {
	labelsValue := pgtype.Text{Valid: false}
	if input.Labels != nil {
		labelsJSON, err := json.Marshal(*input.Labels)
		if err != nil {
			return runnerRecord{}, err
		}
		labelsValue = pgtype.Text{String: string(labelsJSON), Valid: true}
	}

	capabilitiesValue := pgtype.Text{Valid: false}
	if input.Capabilities != nil {
		capabilitiesJSON, err := encodeCapabilities(*input.Capabilities)
		if err != nil {
			return runnerRecord{}, err
		}
		capabilitiesValue = pgtype.Text{String: string(capabilitiesJSON), Valid: true}
	}

	nameValue := pgtype.Text{Valid: false}
	if input.Name != nil {
		nameValue = pgtype.Text{String: *input.Name, Valid: true}
	}

	row := s.pool.QueryRow(ctx,
		fmt.Sprintf(`UPDATE runners SET name = COALESCE($1, name), labels = COALESCE($2::jsonb, labels), capabilities = COALESCE($3::jsonb, capabilities), updated_at = NOW() WHERE id = $4 RETURNING %s`, runnerColumns),
		nameValue,
		labelsValue,
		capabilitiesValue,
		input.ID,
	)
	runner, err := scanRunner(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return runnerRecord{}, NotFound("runner")
		}
		return runnerRecord{}, err
	}
	return runner, nil
}

func (s *Server) updateRunnerEnrollment(ctx context.Context, id uuid.UUID, statusValue string, zitiIdentityID string) error {
	result, err := s.pool.Exec(ctx, `UPDATE runners SET status = $1, ziti_identity_id = $2, updated_at = NOW() WHERE id = $3`, statusValue, zitiIdentityID, id)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return NotFound("runner")
	}
	return nil
}

func scanRunner(row pgx.Row) (runnerRecord, error) {
	var (
		runner     runnerRecord
		orgID      pgtype.UUID
		labelsData []byte
		capData    []byte
	)
	if err := row.Scan(
		&runner.Meta.ID,
		&runner.Name,
		&orgID,
		&runner.IdentityID,
		&runner.ZitiIdentityID,
		&runner.ZitiServiceID,
		&runner.ZitiServiceName,
		&runner.Status,
		&labelsData,
		&capData,
		&runner.Meta.CreatedAt,
		&runner.Meta.UpdatedAt,
	); err != nil {
		return runnerRecord{}, err
	}
	if err := json.Unmarshal(labelsData, &runner.Labels); err != nil {
		return runnerRecord{}, err
	}
	if runner.Labels == nil {
		runner.Labels = map[string]string{}
	}
	capabilities, err := decodeCapabilities(capData)
	if err != nil {
		return runnerRecord{}, err
	}
	runner.Capabilities = capabilities
	if orgID.Valid {
		value := uuid.UUID(orgID.Bytes)
		runner.OrganizationID = &value
	}
	return runner, nil
}

func toProtoRunner(record runnerRecord) (*runnersv1.Runner, error) {
	status, err := runnerStatusToProto(record.Status)
	if err != nil {
		return nil, err
	}
	runner := &runnersv1.Runner{
		Meta:                toProtoEntityMeta(record.Meta),
		Name:                record.Name,
		IdentityId:          record.IdentityID.String(),
		Status:              status,
		Labels:              record.Labels,
		Capabilities:        append([]string(nil), record.Capabilities...),
		OpenzitiServiceName: record.ZitiServiceName,
	}
	if record.OrganizationID != nil {
		value := record.OrganizationID.String()
		runner.OrganizationId = &value
	}
	return runner, nil
}

func runnerStatusToProto(value string) (runnersv1.RunnerStatus, error) {
	switch value {
	case runnerStatusPending:
		return runnersv1.RunnerStatus_RUNNER_STATUS_PENDING, nil
	case runnerStatusEnrolled:
		return runnersv1.RunnerStatus_RUNNER_STATUS_ENROLLED, nil
	case runnerStatusOffline:
		return runnersv1.RunnerStatus_RUNNER_STATUS_OFFLINE, nil
	default:
		return runnersv1.RunnerStatus_RUNNER_STATUS_UNSPECIFIED, fmt.Errorf("invalid runner status: %s", value)
	}
}
