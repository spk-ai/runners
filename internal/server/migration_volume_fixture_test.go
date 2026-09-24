package server

import (
	"context"
	"strings"
	"testing"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Historical fixtures must seed the old schema, not run current projection SQL
// against columns that only the migration under test will add. NULLs are read
// placeholders for new DTO fields; no anchor columns or values are backfilled.
const preAnchorVolumeColumns = `id, instance_id, volume_id, thread_id, runner_id, agent_id, organization_id,
    size_gb, status, removed_at, last_metering_sampled_at, owner_kind, owner_id, created_at, updated_at,
    lifecycle_revision, checked_lifecycle, bound_instance, removal_intent, NULL, NULL, NULL, NULL`

// Seed/read schema 0023/0024 without pretending the new observation column
// exists. Only the historical projection changes; all old writes run unchanged.
type preRetirementMigrationPool struct{ dbPool }

func (p preRetirementMigrationPool) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	projection := strings.TrimSuffix(volumeColumns, ", anchored_removal_observation, anchor_adoption") + ", NULL, NULL"
	return p.dbPool.QueryRow(ctx, strings.ReplaceAll(query, volumeColumns, projection), args...)
}

type preAdoptionMigrationPool struct{ dbPool }

func (p preAdoptionMigrationPool) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	projection := strings.TrimSuffix(volumeColumns, ", anchor_adoption") + ", NULL"
	return p.dbPool.QueryRow(ctx, strings.ReplaceAll(query, volumeColumns, projection), args...)
}

func createMigrationVolume(ctx context.Context, pool *pgxpool.Pool, req *runnersv1.CreateVolumeRequest, checked bool) (*runnersv1.Volume, error) {
	in, err := parseVolumeCreate(req)
	if err != nil {
		return nil, err
	}
	row := pool.QueryRow(ctx, `INSERT INTO volumes (id, volume_id, thread_id, runner_id, agent_id, organization_id,
        size_gb, status, owner_kind, owner_id, checked_lifecycle)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING `+preAnchorVolumeColumns,
		in.ID, nullableUUIDValue(in.VolumeID), nullableUUIDValue(in.ThreadID), in.RunnerID, nullableUUIDValue(in.AgentID),
		in.OrganizationID, in.SizeGB, in.Status, in.OwnerKind, in.OwnerID, checked)
	v, err := scanVolume(row)
	if err != nil {
		return nil, err
	}
	return toProtoVolume(v)
}

func readMigrationVolume(ctx context.Context, pool *pgxpool.Pool, id string) (*runnersv1.Volume, error) {
	v, err := scanVolume(pool.QueryRow(ctx, "SELECT "+preAnchorVolumeColumns+" FROM volumes WHERE id = $1", id))
	if err != nil {
		return nil, err
	}
	return toProtoVolume(v)
}

func assertNoInferredResourceAnchors(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	var invented int
	if err := pool.QueryRow(ctx, `SELECT
        (SELECT count(*) FROM workloads WHERE resource_anchors IS NOT NULL) +
        (SELECT count(*) FROM volumes WHERE resource_anchor IS NOT NULL OR anchor_reservation IS NOT NULL OR anchored_removal_observation IS NOT NULL OR anchor_adoption IS NOT NULL) +
        (SELECT count(*) FROM runtime_volume_admission_guards WHERE resource_anchors_required OR volume_anchor_migration IS NOT NULL)`).Scan(&invented); err != nil {
		t.Fatal(err)
	}
	if invented != 0 {
		t.Fatal("migration invented native anchor authority")
	}
}
