package server

import (
	"context"
	"io/fs"
	"strings"
	"testing"
	"time"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/runners/internal/db"
	"github.com/agynio/runners/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
)

// Called only inside the loopback/disposable-database gate of TestLiveVolumeReopen.
func testVolumeAdmissionMigration(t *testing.T, ctx context.Context, base *pgxpool.Config) {
	for _, scenario := range []string{"valid-active", "pending-workload", "multiple-workloads", "workload-identity", "volume-identity"} {
		t.Run(scenario, func(t *testing.T) {
			config := base.Copy()
			admin, err := pgx.ConnectConfig(ctx, config.ConnConfig)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = admin.Close(context.Background()) })
			schemaName := "admission_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			schema := pgx.Identifier{schemaName}.Sanitize()
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
				if entry.Name() >= "0019" {
					continue
				}
				content, err := migrations.Files.ReadFile(entry.Name())
				if err != nil {
					t.Fatal(err)
				}
				if _, err := pool.Exec(ctx, string(content)); err != nil {
					t.Fatalf("preceding migration %s: %v", entry.Name(), err)
				}
				if _, err := pool.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", entry.Name()); err != nil {
					t.Fatal(err)
				}
			}
			runnerID := uuid.NewString()
			if _, err := pool.Exec(ctx, "INSERT INTO runners (id, name, identity_id, service_token_hash, status) VALUES ($1, 'admission-upgrade', $2, $3, 'enrolled')", runnerID, uuid.NewString(), hashServiceToken(uuid.NewString())); err != nil {
				t.Fatal(err)
			}
			req := &runnersv1.CreateVolumeRequest{
				Id: uuid.NewString(), RunnerId: runnerID, OrganizationId: uuid.NewString(),
				ThreadId: uuid.NewString(), AgentId: uuid.NewString(), VolumeId: uuid.NewString(),
				SizeGb: "1", Status: runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING,
				OwnerKind: runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE, OwnerId: uuid.NewString(),
			}
			created, err := createMigrationVolume(ctx, pool, req, true)
			if err != nil {
				t.Fatal(err)
			}
			err = adoptionBindSQL(ctx, pool, created, lifecycleTestInstance(created))
			if err != nil {
				t.Fatal(err)
			}
			v, err := readMigrationVolume(ctx, pool, req.Id)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "pending-workload" {
				if err := admissionBeginSQL(ctx, pool, v); err != nil {
					t.Fatal(err)
				}
			}
			w := &runnersv1.CreateWorkloadRequest{
				Id: uuid.NewString(), RunnerId: runnerID, OrganizationId: req.OrganizationId,
				ThreadId: req.ThreadId, AgentId: req.AgentId, OwnerKind: req.OwnerKind, OwnerId: req.OwnerId,
			}
			if scenario == "workload-identity" {
				w.OrganizationId = uuid.NewString()
			}
			if err := admissionInsertSQL(ctx, pool, w); err != nil {
				t.Fatal(err)
			}
			if scenario == "multiple-workloads" {
				w.Id = uuid.NewString()
				if err := admissionInsertSQL(ctx, pool, w); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "volume-identity" {
				second := proto.Clone(req).(*runnersv1.CreateVolumeRequest)
				second.Id, second.OrganizationId = uuid.NewString(), uuid.NewString()
				if _, err := createMigrationVolume(ctx, pool, second, true); err != nil {
					t.Fatal(err)
				}
			}
			snapshot := func() string {
				var data string
				if err := pool.QueryRow(ctx, `SELECT jsonb_build_object(
                    'volumes', (SELECT jsonb_agg(to_jsonb(v) - ARRAY['resource_anchor', 'anchor_reservation'] ORDER BY id) FROM volumes v),
                    'workloads', (SELECT jsonb_agg(to_jsonb(w) - ARRAY['resource_anchors', 'preparation_phase', 'preparation_revision', 'prepared_backend_id', 'prepared_volume_ids', 'prepared_binding', 'prepared_removal_observation'] ORDER BY id) FROM workloads w))::text`).Scan(&data); err != nil {
					t.Fatal(err)
				}
				return data
			}
			before := snapshot()
			valid := scenario == "valid-active"
			for attempt := 0; attempt < 2; attempt++ {
				err := db.ApplyMigrations(ctx, pool)
				if valid && err != nil {
					t.Fatal(err)
				}
				if valid {
					assertNoInferredResourceAnchors(t, ctx, pool)
				}
				if !valid {
					assertAdmissionConflict(t, err)
				}
				var applied, table bool
				if err := pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = '0019_volume_workload_admission.sql'), to_regclass(current_schema() || '.runtime_volume_admission_guards') IS NOT NULL").Scan(&applied, &table); err != nil {
					t.Fatal(err)
				}
				if applied != valid || table != valid || snapshot() != before {
					t.Fatalf("migration changed historical rows or partially committed: applied=%t table=%t", applied, table)
				}
			}
			if valid {
				assertAdmissionConflict(t, admissionBeginSQL(ctx, pool, v))
				if _, err := pool.Exec(ctx, "UPDATE workloads SET status = 'failed' WHERE id = $1", w.Id); err != nil {
					t.Fatalf("cleanup transition after upgrade: %v", err)
				}
				assertAdmissionConflict(t, admissionBeginSQL(ctx, pool, v))
				if _, err := pool.Exec(ctx, "UPDATE workloads SET removal_confirmed_at = NOW() WHERE id = $1", w.Id); err != nil {
					t.Fatal(err)
				}
				if err := admissionBeginSQL(ctx, pool, v); err != nil {
					t.Fatalf("explicit fixture confirmation did not release deletion: %v", err)
				}
			}
		})
	}
}
