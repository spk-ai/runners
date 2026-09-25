package server

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"regexp"
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

// Keep revisions within the SQL document's bounded decimal representation.
const maxVolumeMigrationRevision = 999999999999999998

var migrationSpecHash = regexp.MustCompile(`^[0-9a-f]{64}$`)

func migrationOwner(kind runnersv1.RuntimeOwnerKind, id string) (string, uuid.UUID, error) {
	name, err := runtimeOwnerKindToString(kind)
	if err != nil || !canonicalPreparedUUID(id) {
		return "", uuid.Nil, status.Error(codes.InvalidArgument, "canonical_migration_owner_required")
	}
	return name, uuid.MustParse(id), nil
}

func readVolumeMigration(row pgx.Row) (*runnersv1.VolumeAnchorMigration, error) {
	var data []byte
	if err := row.Scan(&data); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "volume_anchor_migration_not_found")
		}
		return nil, toStatusError(err)
	}
	if len(data) == 0 {
		return nil, status.Error(codes.NotFound, "volume_anchor_migration_not_found")
	}
	value := &runnersv1.VolumeAnchorMigration{}
	if err := protojson.Unmarshal(data, value); err != nil {
		return nil, status.Error(codes.FailedPrecondition, "invalid_stored_volume_anchor_migration")
	}
	return value, nil
}

func (s *Server) GetVolumeAnchorMigration(ctx context.Context, req *runnersv1.GetVolumeAnchorMigrationRequest) (*runnersv1.GetVolumeAnchorMigrationResponse, error) {
	kind, owner, err := migrationOwner(req.GetOwnerKind(), req.GetOwnerId())
	if err != nil {
		return nil, err
	}
	migration, err := readVolumeMigration(s.pool.QueryRow(ctx, `SELECT volume_anchor_migration FROM runtime_volume_admission_guards WHERE owner_kind=$1 AND owner_id=$2`, kind, owner))
	if err != nil {
		return nil, err
	}
	return &runnersv1.GetVolumeAnchorMigrationResponse{Migration: migration}, nil
}

// BeginVolumeAnchorMigration commits the complete drained owner plan and admission
// block together with observed legacy-to-checked bindings. The same plan reads
// committed progress; an unbound failed legacy generation stays quarantined.
// SQL serialization and old-writer guards live in the immutable
// ../../migrations/0027_volume_anchor_migration.sql, building on
// ../../migrations/0019_volume_workload_admission.sql. The guard performs a real
// owner-row write before cross-table reads; stale transactions must not admit work.
// @see api::proto/agynio/api/runners/v1/runners
// @see orchestrator::internal/volumemigration/coordinator
func (s *Server) BeginVolumeAnchorMigration(ctx context.Context, req *runnersv1.BeginVolumeAnchorMigrationRequest) (*runnersv1.BeginVolumeAnchorMigrationResponse, error) {
	kind, owner, err := migrationOwner(req.GetOwnerKind(), req.GetOwnerId())
	if err != nil {
		return nil, err
	}
	if !canonicalPreparedUUID(req.GetId()) || !canonicalPreparedUUID(req.GetRunnerId()) || !canonicalPreparedUUID(req.GetOrganizationId()) ||
		!validVolumeBackend(req.GetBackendId()) || len(req.GetSources()) == 0 || len(req.Sources) > 64 ||
		len(req.ProtoReflect().GetUnknown()) != 0 || proto.Size(req) > 256*1024 {
		return nil, status.Error(codes.InvalidArgument, "complete_owner_migration_plan_required")
	}
	sources := append([]*runnersv1.VolumeAnchorMigrationSource(nil), req.Sources...)
	seen := map[string]bool{}
	for _, source := range sources {
		if !canonicalPreparedUUID(source.GetVolumeId()) || source.GetExpectedRevision() == 0 || source.ExpectedRevision >= maxVolumeMigrationRevision ||
			seen[source.VolumeId] || len(source.ProtoReflect().GetUnknown()) != 0 {
			return nil, status.Error(codes.InvalidArgument, "unique_revisioned_migration_sources_required")
		}
		seen[source.VolumeId] = true
	}
	slices.SortFunc(sources, func(a, b *runnersv1.VolumeAnchorMigrationSource) int { return strings.Compare(a.VolumeId, b.VolumeId) })
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, toStatusError(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, "SELECT lock_runtime_volume_owner($1,$2)", kind, owner); err != nil {
		return nil, toStatusError(err)
	}
	previous, err := readVolumeMigration(tx.QueryRow(ctx, `SELECT volume_anchor_migration FROM runtime_volume_admission_guards WHERE owner_kind=$1 AND owner_id=$2`, kind, owner))
	if err == nil {
		if !sameMigrationRequest(previous, req, sources) {
			return nil, status.Error(codes.AlreadyExists, "owner_has_another_immutable_migration")
		}
		return &runnersv1.BeginVolumeAnchorMigrationResponse{Migration: previous}, nil
	}
	if status.Code(err) != codes.NotFound {
		return nil, err
	}
	var volumeCount, unconfirmed int
	if err := tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM volumes WHERE owner_kind=$1 AND owner_id=$2),
		(SELECT count(*) FROM workloads WHERE owner_kind=$1 AND owner_id=$2 AND removal_confirmed_at IS NULL)`, kind, owner).Scan(&volumeCount, &unconfirmed); err != nil {
		return nil, toStatusError(err)
	}
	if volumeCount != len(sources) || unconfirmed != 0 {
		return nil, status.Error(codes.FailedPrecondition, "complete_drained_owner_inventory_required")
	}
	migration := &runnersv1.VolumeAnchorMigration{Id: req.Id, OwnerKind: req.OwnerKind, OwnerId: req.OwnerId,
		RunnerId: req.RunnerId, OrganizationId: req.OrganizationId, BackendId: req.BackendId, Revision: 1}
	var records []volumeRecord
	for _, source := range sources {
		v, err := scanVolume(tx.QueryRow(ctx, "SELECT "+volumeColumns+" FROM volumes WHERE id=$1", uuid.MustParse(source.VolumeId)))
		if err != nil {
			return nil, toStatusError(err)
		}
		entry, err := migrationEntry(req, kind, owner, v, source)
		if err != nil {
			return nil, err
		}
		if len(records) > 0 && (v.ThreadID != records[0].ThreadID || v.AgentID != records[0].AgentID) {
			return nil, status.Error(codes.FailedPrecondition, "owner_volume_identity_conflict")
		}
		records = append(records, v)
		migration.Entries = append(migration.Entries, entry)
	}
	data, err := protojson.Marshal(migration)
	if err != nil {
		return nil, status.Error(codes.Internal, "encode_volume_anchor_migration")
	}
	if _, err := tx.Exec(ctx, `UPDATE runtime_volume_admission_guards SET volume_anchor_migration=$3
		WHERE owner_kind=$1 AND owner_id=$2 AND volume_anchor_migration IS NULL`, kind, owner, data); err != nil {
		return nil, toStatusError(err)
	}
	// Legacy checked adoption and the owner block commit together. No gap admits
	// a competing workload between native observation and durable provenance.
	var changed []volumeRecord
	for i, v := range records {
		entry := migration.Entries[i]
		if v.CheckedLifecycle || entry.Source.Previous == nil {
			continue
		}
		binding, err := protojson.Marshal(entry.Source.Previous)
		if err != nil {
			return nil, status.Error(codes.Internal, "encode_migration_source")
		}
		updated, err := scanVolume(tx.QueryRow(ctx, `UPDATE volumes SET checked_lifecycle=TRUE,lifecycle_revision=lifecycle_revision+1,
			status='active',bound_instance=$3,updated_at=NOW() WHERE id=$1 AND lifecycle_revision=$2 RETURNING `+volumeColumns,
			v.Meta.ID, v.LifecycleRevision, binding))
		if err != nil {
			return nil, volumeLifecycleStatusError(err)
		}
		changed = append(changed, updated)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, toStatusError(err)
	}
	for _, v := range changed {
		s.publishVolumeUpdateNotification(ctx, v)
	}
	return &runnersv1.BeginVolumeAnchorMigrationResponse{Migration: migration}, nil
}

func sameMigrationRequest(m *runnersv1.VolumeAnchorMigration, req *runnersv1.BeginVolumeAnchorMigrationRequest, sources []*runnersv1.VolumeAnchorMigrationSource) bool {
	if m.Id != req.Id || m.OwnerKind != req.OwnerKind || m.OwnerId != req.OwnerId || m.RunnerId != req.RunnerId ||
		m.OrganizationId != req.OrganizationId || m.BackendId != req.BackendId || len(m.Entries) != len(sources) {
		return false
	}
	for i, source := range sources {
		if !proto.Equal(m.Entries[i].Source, source) {
			return false
		}
	}
	return true
}

func migrationEntry(req *runnersv1.BeginVolumeAnchorMigrationRequest, kind string, owner uuid.UUID, v volumeRecord, source *runnersv1.VolumeAnchorMigrationSource) (*runnersv1.VolumeAnchorMigrationEntry, error) {
	if v.OwnerKind != kind || v.OwnerID != owner || v.RunnerID.String() != req.RunnerId || v.OrganizationID.String() != req.OrganizationId ||
		v.ResourceAnchor != nil || v.AnchorAdoption != nil || v.AnchorReservation != nil || v.RemovalIntent != nil {
		return nil, status.Error(codes.FailedPrecondition, "matching_unanchored_volume_required")
	}
	if uint64(v.LifecycleRevision) != source.ExpectedRevision {
		return nil, status.Error(codes.Aborted, "migration_source_revision_changed")
	}
	e := &runnersv1.VolumeAnchorMigrationEntry{Source: proto.Clone(source).(*runnersv1.VolumeAnchorMigrationSource)}
	if source.Previous == nil {
		if v.Status != volumeStatusFailed || v.CheckedLifecycle || v.InstanceID != nil || v.BoundInstance != nil {
			return nil, status.Error(codes.FailedPrecondition, "native_source_observation_required")
		}
		e.UnresolvedReason = "unbound_failed_generation"
		return e, nil
	}
	if v.Status != volumeStatusActive && v.Status != volumeStatusProvisioning || v.InstanceID == nil ||
		!canonicalPreparedUUID(source.Previous.InstanceUid) || source.Previous.BackendId != req.BackendId ||
		len(source.Previous.ProtoReflect().GetUnknown()) != 0 || v.CheckedLifecycle && !proto.Equal(v.BoundInstance, source.Previous) {
		return nil, status.Error(codes.FailedPrecondition, "original_bound_volume_required")
	}
	if err := validateVolumeBinding(v, source.Previous); err != nil {
		return nil, err
	}
	e.CheckedRevision = source.ExpectedRevision
	if !v.CheckedLifecycle {
		e.CheckedRevision++
	}
	e.AdoptionId = uuid.NewSHA1(uuid.MustParse(req.Id), []byte(v.Meta.ID.String())).String()
	e.Intent = &runnerv1.ResourceAnchor{Kind: runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME,
		ResourceId: v.Meta.ID.String(), BackendId: req.BackendId, IdentityLabels: maps.Clone(source.Previous.IdentityLabels)}
	check := proto.Clone(e.Intent).(*runnerv1.ResourceAnchor)
	check.InstanceUid = e.AdoptionId
	if err := validateRegistryResourceAnchor(check, check.Kind, v.Meta.ID, req.BackendId, kind, owner, v.AgentID, v.ThreadID); err != nil {
		return nil, err
	}
	return e, nil
}

func advanceMigration(m *runnersv1.VolumeAnchorMigration, req *runnersv1.AdvanceVolumeAnchorMigrationRequest) (*runnersv1.VolumeAnchorMigration, error) {
	if m.Id != req.Id || m.Revision != req.ExpectedRevision {
		return nil, status.Error(codes.Aborted, "volume_migration_revision_changed")
	}
	if m.Complete {
		return nil, status.Error(codes.FailedPrecondition, "volume_migration_already_complete")
	}
	next := proto.Clone(m).(*runnersv1.VolumeAnchorMigration)
	next.Revision++
	if req.GetComplete() != nil {
		if req.VolumeId != "" || len(req.GetComplete().ProtoReflect().GetUnknown()) != 0 {
			return nil, status.Error(codes.InvalidArgument, "complete_owner_migration_required")
		}
		for _, e := range next.Entries {
			if e.Ready == nil || e.UnresolvedReason != "" {
				return nil, status.Error(codes.FailedPrecondition, "all_original_volumes_must_be_ready")
			}
		}
		next.Complete = true
		return next, nil
	}
	index := slices.IndexFunc(next.Entries, func(e *runnersv1.VolumeAnchorMigrationEntry) bool { return e.GetSource().GetVolumeId() == req.VolumeId })
	if index < 0 || next.Entries[index].UnresolvedReason != "" {
		return nil, status.Error(codes.FailedPrecondition, "planned_adoptable_volume_required")
	}
	e := next.Entries[index]
	switch op := req.Operation.(type) {
	case *runnersv1.AdvanceVolumeAnchorMigrationRequest_Reserve:
		a := op.Reserve
		if e.Adoption != nil || a == nil || !canonicalPreparedUUID(a.InstanceUid) || !canonicalPreparedUUID(a.GetAnchor().GetInstanceUid()) ||
			!migrationSpecHash.MatchString(a.PvcSpecSha256) || a.Id != e.AdoptionId || !proto.Equal(a.Previous, e.Source.Previous) ||
			len(a.ProtoReflect().GetUnknown()) != 0 {
			return nil, status.Error(codes.FailedPrecondition, "matching_native_adoption_receipt_required")
		}
		intent := proto.Clone(a.Anchor).(*runnerv1.ResourceAnchor)
		intent.InstanceUid = ""
		if !proto.Equal(intent, e.Intent) {
			return nil, status.Error(codes.FailedPrecondition, "native_adoption_owner_changed")
		}
		e.Adoption = proto.Clone(a).(*runnerv1.VolumeAnchorAdoption)
	case *runnersv1.AdvanceVolumeAnchorMigrationRequest_Apply:
		if e.Adoption == nil || e.Applied != nil || op.Apply == nil {
			return nil, status.Error(codes.FailedPrecondition, "reserved_native_adoption_required")
		}
		binding := proto.Clone(e.Source.Previous).(*runnerv1.VolumeListItem)
		binding.Anchor = proto.Clone(e.Adoption.Anchor).(*runnerv1.ResourceAnchor)
		expected := &runnerv1.ApplyVolumeAnchorAdoptionResponse{Adoption: e.Adoption, Volume: binding, State: runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_APPLIED}
		if !proto.Equal(expected, op.Apply) {
			return nil, status.Error(codes.FailedPrecondition, "original_applied_native_binding_required")
		}
		e.Applied = proto.Clone(op.Apply).(*runnerv1.ApplyVolumeAnchorAdoptionResponse)
	case *runnersv1.AdvanceVolumeAnchorMigrationRequest_Ready:
		if e.Applied == nil || e.Ready != nil || op.Ready == nil {
			return nil, status.Error(codes.FailedPrecondition, "persisted_applied_binding_required")
		}
		expected := &runnerv1.ObserveVolumeAnchorAdoptionResponse{Adoption: e.Adoption, Volume: e.Applied.Volume, State: runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_READY}
		if !proto.Equal(expected, op.Ready) {
			return nil, status.Error(codes.FailedPrecondition, "matching_independent_native_readiness_required")
		}
		e.Ready = proto.Clone(op.Ready).(*runnerv1.ObserveVolumeAnchorAdoptionResponse)
	default:
		return nil, status.Error(codes.InvalidArgument, "migration_operation_required")
	}
	return next, nil
}

// AdvanceVolumeAnchorMigration appends one reserve/apply/ready receipt per CAS.
// Apply commits the original PVC binding, anchor and journal together, never an
// allocation reservation. Completion checks every READY entry against volume rows
// and permanently pins prepared anchored admission. Migration 0027 guards immutable
// progress from old writers while allowing metering-only updates. Lost replies
// require GetVolumeAnchorMigration, not a new operation ID or force-unblock.
func (s *Server) AdvanceVolumeAnchorMigration(ctx context.Context, req *runnersv1.AdvanceVolumeAnchorMigrationRequest) (*runnersv1.AdvanceVolumeAnchorMigrationResponse, error) {
	kind, owner, err := migrationOwner(req.GetOwnerKind(), req.GetOwnerId())
	if err != nil {
		return nil, err
	}
	if !canonicalPreparedUUID(req.GetId()) || req.GetExpectedRevision() == 0 || req.ExpectedRevision >= maxVolumeMigrationRevision ||
		len(req.ProtoReflect().GetUnknown()) != 0 || proto.Size(req) > 256*1024 {
		return nil, status.Error(codes.InvalidArgument, "checked_migration_id_and_revision_required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, toStatusError(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, "SELECT lock_runtime_volume_owner($1,$2)", kind, owner); err != nil {
		return nil, toStatusError(err)
	}
	current, err := readVolumeMigration(tx.QueryRow(ctx, `SELECT volume_anchor_migration FROM runtime_volume_admission_guards WHERE owner_kind=$1 AND owner_id=$2`, kind, owner))
	if err != nil {
		return nil, err
	}
	next, err := advanceMigration(current, req)
	if err != nil {
		return nil, err
	}
	data, err := protojson.Marshal(next)
	if err != nil {
		return nil, status.Error(codes.Internal, "encode_volume_migration_progress")
	}
	query := `UPDATE runtime_volume_admission_guards SET volume_anchor_migration=$3 WHERE owner_kind=$1 AND owner_id=$2`
	if next.Complete {
		query = `UPDATE runtime_volume_admission_guards g SET volume_anchor_migration=$3,resource_anchors_required=TRUE,
			prepared_backend_id=$4,prepared_runner_id=$5,prepared_organization_id=$6,
			prepared_thread_id=v.thread_id,prepared_agent_id=v.agent_id FROM volumes v
			WHERE g.owner_kind=$1 AND g.owner_id=$2 AND v.id=$7`
		_, err = tx.Exec(ctx, query, kind, owner, data, next.BackendId, uuid.MustParse(next.RunnerId), uuid.MustParse(next.OrganizationId), uuid.MustParse(next.Entries[0].Source.VolumeId))
	} else {
		_, err = tx.Exec(ctx, query, kind, owner, data)
	}
	if err != nil {
		return nil, toStatusError(err)
	}
	var changed *volumeRecord
	if req.GetApply() != nil {
		index := slices.IndexFunc(next.Entries, func(e *runnersv1.VolumeAnchorMigrationEntry) bool { return e.Source.VolumeId == req.VolumeId })
		e := next.Entries[index]
		binding, err := protojson.Marshal(e.Applied.Volume)
		if err != nil {
			return nil, status.Error(codes.Internal, "encode_applied_migration_binding")
		}
		anchor, err := protojson.Marshal(e.Adoption.Anchor)
		if err != nil {
			return nil, status.Error(codes.Internal, "encode_adopted_volume_anchor")
		}
		adoption, err := protojson.Marshal(e.Adoption)
		if err != nil {
			return nil, status.Error(codes.Internal, "encode_volume_adoption")
		}
		v, err := scanVolume(tx.QueryRow(ctx, fmt.Sprintf(`UPDATE volumes SET lifecycle_revision=lifecycle_revision+1,
			bound_instance=$3,resource_anchor=$4,anchor_adoption=$5,updated_at=NOW()
			WHERE id=$1 AND lifecycle_revision=$2 RETURNING %s`, volumeColumns), uuid.MustParse(e.Source.VolumeId), int64(e.CheckedRevision), binding, anchor, adoption))
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.Aborted, "applied_volume_revision_changed")
		}
		if err != nil {
			return nil, volumeLifecycleStatusError(err)
		}
		changed = &v
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, toStatusError(err)
	}
	if changed != nil {
		s.publishVolumeUpdateNotification(ctx, *changed)
	}
	return &runnersv1.AdvanceVolumeAnchorMigrationResponse{Migration: next}, nil
}
