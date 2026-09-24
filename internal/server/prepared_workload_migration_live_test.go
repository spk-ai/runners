package server

import (
	"context"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/agynio/runners/internal/db"
	"github.com/agynio/runners/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testPreparedWorkloadMigration(t *testing.T, ctx context.Context, base *pgxpool.Config) {
	config := base.Copy()
	admin, err := pgx.ConnectConfig(ctx, config.ConnConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	schema := pgx.Identifier{"preparation_" + strings.ReplaceAll(uuid.NewString(), "-", "")}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanup, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	entries, err := fs.ReadDir(migrations.Files, ".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() >= "0022" {
			continue
		}
		content, err := migrations.Files.ReadFile(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(content)); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", entry.Name()); err != nil {
			t.Fatal(err)
		}
	}
	runner := uuid.NewString()
	if _, err := pool.Exec(ctx, "INSERT INTO runners (id, name, identity_id, service_token_hash, status) VALUES ($1, 'preparation-migration', $2, $3, 'enrolled')", runner, uuid.NewString(), hashServiceToken(uuid.NewString())); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"agent_instance", "sandbox"} {
		for _, phase := range []string{"starting", "failed", "stopped"} {
			if _, err := pool.Exec(ctx, `INSERT INTO workloads (id, runner_id, organization_id, thread_id, agent_id, owner_kind, owner_id, status, removal_confirmed_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CASE WHEN $8 = 'stopped' THEN NOW() END)`, uuid.NewString(), runner, uuid.NewString(), uuid.NewString(), uuid.NewString(), kind, uuid.NewString(), phase); err != nil {
				t.Fatal(err)
			}
		}
	}
	snapshot := func() string {
		var data string
		if err := pool.QueryRow(ctx, `SELECT jsonb_build_object(
            'workloads', (SELECT jsonb_agg(to_jsonb(w) - ARRAY['resource_anchors', 'preparation_phase', 'preparation_revision', 'prepared_backend_id', 'prepared_volume_ids', 'prepared_binding', 'prepared_removal_observation'] ORDER BY id) FROM workloads w),
            'guards', (SELECT jsonb_agg((to_jsonb(g) - 'volume_anchor_migration') - ARRAY['resource_anchors_required', 'prepared_backend_id', 'prepared_runner_id', 'prepared_organization_id', 'prepared_thread_id', 'prepared_agent_id'] ORDER BY owner_kind, owner_id) FROM runtime_volume_admission_guards g))::text`).Scan(&data); err != nil {
			t.Fatal(err)
		}
		return data
	}
	before := snapshot()
	for attempt := 0; attempt < 2; attempt++ {
		if err := db.ApplyMigrations(ctx, pool); err != nil {
			t.Fatal(err)
		}
		assertNoInferredResourceAnchors(t, ctx, pool)
		var invented, pins, applied int
		if err := pool.QueryRow(ctx, `SELECT
            (SELECT count(*) FROM workloads WHERE preparation_phase IS NOT NULL OR preparation_revision <> 0 OR prepared_backend_id IS NOT NULL OR prepared_volume_ids IS NOT NULL OR prepared_binding IS NOT NULL OR prepared_removal_observation IS NOT NULL),
            (SELECT count(*) FROM runtime_volume_admission_guards WHERE prepared_backend_id IS NOT NULL OR prepared_runner_id IS NOT NULL OR prepared_organization_id IS NOT NULL OR prepared_thread_id IS NOT NULL OR prepared_agent_id IS NOT NULL),
            (SELECT count(*) FROM schema_migrations WHERE version = '0022_prepared_workloads.sql')`).Scan(&invented, &pins, &applied); err != nil {
			t.Fatal(err)
		}
		if invented != 0 || pins != 0 || applied != 1 || snapshot() != before {
			t.Fatal("migration rewrote history, inferred preparation, or did not commit")
		}
	}
}
