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

// Called only inside TestLiveVolumeReopen's disposable database gate.
func testVolumeAdoptionMigration(t *testing.T, ctx context.Context, base *pgxpool.Config) {
	config := base.Copy()
	admin, err := pgx.ConnectConfig(ctx, config.ConnConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	schema := pgx.Identifier{"adoption_" + strings.ReplaceAll(uuid.NewString(), "-", "")}.Sanitize()
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
		if entry.Name() >= "0020" {
			continue
		}
		content, err := migrations.Files.ReadFile(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(content)); err != nil {
			t.Fatalf("prior migration %s: %v", entry.Name(), err)
		}
		if _, err := pool.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", entry.Name()); err != nil {
			t.Fatal(err)
		}
	}
	runnerID := uuid.NewString()
	if _, err := pool.Exec(ctx, "INSERT INTO runners (id, name, identity_id, service_token_hash, status) VALUES ($1, 'adoption-upgrade', $2, $3, 'enrolled')", runnerID, uuid.NewString(), hashServiceToken(uuid.NewString())); err != nil {
		t.Fatal(err)
	}
	srv := New(Options{Pool: pool})
	var volumes []*runnersv1.Volume
	for _, kind := range []runnersv1.RuntimeOwnerKind{runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE, runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX} {
		for _, phase := range []runnersv1.VolumeStatus{runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE, runnersv1.VolumeStatus_VOLUME_STATUS_FAILED} {
			req := &runnersv1.CreateVolumeRequest{Id: uuid.NewString(), RunnerId: runnerID, OrganizationId: uuid.NewString(),
				VolumeId: uuid.NewString(), SizeGb: "1", Status: runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING,
				OwnerKind: kind, OwnerId: uuid.NewString()}
			if kind == runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE {
				req.ThreadId, req.AgentId = uuid.NewString(), uuid.NewString()
			}
			created, err := srv.CreateVolume(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			update := &runnersv1.UpdateVolumeRequest{Id: req.Id, Status: &phase}
			if phase == runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE {
				update.InstanceId = ptr(lifecycleTestInstance(created.Volume).InstanceId)
			}
			stored, err := srv.UpdateVolume(ctx, update)
			if err != nil {
				t.Fatal(err)
			}
			v := stored.Volume
			volumes = append(volumes, v)
			if phase == runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE {
				if err := admissionInsertSQL(ctx, pool, &runnersv1.CreateWorkloadRequest{Id: uuid.NewString(), RunnerId: v.RunnerId,
					OrganizationId: v.OrganizationId, ThreadId: v.ThreadId, AgentId: v.AgentId, OwnerKind: v.OwnerKind, OwnerId: v.OwnerId}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	snapshot := func() string {
		var data string
		if err := pool.QueryRow(ctx, `SELECT jsonb_build_object(
            'volumes', (SELECT jsonb_agg(to_jsonb(v) ORDER BY id) FROM volumes v),
            'workloads', (SELECT jsonb_agg(to_jsonb(w) ORDER BY id) FROM workloads w),
            'guards', (SELECT jsonb_agg(to_jsonb(g) ORDER BY owner_kind, owner_id) FROM runtime_volume_admission_guards g))::text`).Scan(&data); err != nil {
			t.Fatal(err)
		}
		return data
	}
	before := snapshot()
	for attempt := 0; attempt < 2; attempt++ {
		if err := db.ApplyMigrations(ctx, pool); err != nil {
			t.Fatal(err)
		}
		var applied, guarded bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = '0020_legacy_volume_adoption.sql'),
            EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = 'volumes'::regclass
                AND tgname = 'volumes_legacy_adoption' AND tgenabled = 'O')`).Scan(&applied, &guarded); err != nil {
			t.Fatal(err)
		}
		if !applied || !guarded || snapshot() != before {
			t.Fatal("upgrade rewrote history or did not install the adoption guard")
		}
	}
	for _, v := range volumes {
		t.Run(v.OwnerKind.String()+"/"+v.Status.String(), func(t *testing.T) {
			before := snapshot()
			if v.Status == runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE {
				assertAdmissionConflict(t, adoptionBindSQL(ctx, pool, v, lifecycleTestInstance(v)))
			} else {
				_, err := pool.Exec(ctx, "UPDATE volumes SET checked_lifecycle = TRUE, lifecycle_revision = lifecycle_revision + 1, status = 'provisioning' WHERE id = $1", v.Meta.Id)
				var pgErr *pgconn.PgError
				if !errors.As(err, &pgErr) || pgErr.Code != "55000" || pgErr.ConstraintName != "volumes_legacy_adoption" {
					t.Fatalf("legacy failure reopened through pre-upgrade SQL: %v", err)
				}
			}
			if snapshot() != before {
				t.Fatal("rejected post-upgrade transition changed historical state")
			}
		})
	}
}
