package server

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"
	"time"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/runners/internal/db"
	"github.com/agynio/runners/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Runs only inside the parent test's disposable, loopback PostgreSQL gate.
func testVolumeBackendMigration(t *testing.T, ctx context.Context, base *pgxpool.Config) {
	for _, history := range []string{"identified", "unidentified"} {
		t.Run(history, func(t *testing.T) {
			config := base.Copy()
			admin, err := pgx.ConnectConfig(ctx, config.ConnConfig)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = admin.Close(context.Background()) })
			schema := pgx.Identifier{"backend_" + strings.ReplaceAll(uuid.NewString(), "-", "")}.Sanitize()
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
				if entry.Name() >= "0021" {
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
			if _, err := pool.Exec(ctx, "INSERT INTO runners (id, name, identity_id, service_token_hash, status) VALUES ($1, 'backend-test', $2, $3, 'enrolled')", runner, uuid.NewString(), hashServiceToken(uuid.NewString())); err != nil {
				t.Fatal(err)
			}
			create := func(kind runnersv1.RuntimeOwnerKind) *runnersv1.Volume {
				t.Helper()
				req := &runnersv1.CreateVolumeRequest{Id: uuid.NewString(), RunnerId: runner, OrganizationId: uuid.NewString(),
					OwnerKind: kind, OwnerId: uuid.NewString(), VolumeId: uuid.NewString(), SizeGb: "1", Status: runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING}
				if kind == runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE {
					req.AgentId, req.ThreadId = uuid.NewString(), uuid.NewString()
				}
				created, err := createMigrationVolume(ctx, pool, req, true)
				if err != nil {
					t.Fatal(err)
				}
				return created
			}
			for _, kind := range []runnersv1.RuntimeOwnerKind{runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE, runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX} {
				v := create(kind)
				target := lifecycleTestInstance(v)
				if history == "unidentified" {
					target.BackendId = ""
				}
				if err := adoptionBindSQL(ctx, pool, v, target); err != nil {
					t.Fatal(err)
				}
			}
			snapshot := func() string {
				var result string
				if err := pool.QueryRow(ctx, "SELECT jsonb_agg((to_jsonb(v) - 'anchor_adoption') - ARRAY['resource_anchor', 'anchor_reservation', 'anchored_removal_observation'] ORDER BY id)::text FROM volumes v").Scan(&result); err != nil {
					t.Fatal(err)
				}
				return result
			}
			before := snapshot()
			for attempt := 0; attempt < 2; attempt++ {
				err := db.ApplyMigrations(ctx, pool)
				if history == "unidentified" {
					var pgErr *pgconn.PgError
					if !errors.As(err, &pgErr) || pgErr.Code != "23514" || pgErr.ConstraintName != "volumes_checked_backend" {
						t.Fatalf("unidentified history was not rejected atomically: %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				if history == "identified" {
					assertNoInferredResourceAnchors(t, ctx, pool)
				}
				var applied, guarded bool
				if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = '0021_volume_backend_identity.sql'),
                    EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'volumes'::regclass AND conname = 'volumes_checked_backend' AND convalidated)`).Scan(&applied, &guarded); err != nil {
					t.Fatal(err)
				}
				if applied != (history == "identified") || guarded != applied || snapshot() != before {
					t.Fatal("migration rewrote history or left an unverified guard")
				}
			}
			if history == "identified" {
				for _, backend := range []string{"", " padded ", strings.Repeat("x", 513)} {
					v := create(runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE)
					target := lifecycleTestInstance(v)
					target.BackendId = backend
					before := snapshot()
					var pgErr *pgconn.PgError
					if err := adoptionBindSQL(ctx, pool, v, target); !errors.As(err, &pgErr) || pgErr.Code != "23514" || pgErr.ConstraintName != "volumes_checked_backend" {
						t.Fatalf("old SQL writer bypassed backend guard: %v", err)
					}
					if snapshot() != before {
						t.Fatal("rejected old SQL writer changed the record")
					}
				}
			}
		})
	}
}
