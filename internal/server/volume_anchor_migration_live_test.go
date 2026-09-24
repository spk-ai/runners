package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	runnerv1 "github.com/agynio/runners/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Real PostgreSQL and registry RPCs, synthetic native receipts. The operator
// acceptance suite separately verifies receipts against actual retained PVCs.
func testVolumeAnchorMigration(t *testing.T, ctx context.Context, pool *pgxpool.Pool, reader *pgx.Conn, newRequest func(bool) *runnersv1.CreateVolumeRequest) {
	fixture := t
	client, stop := preparedRegistryClient(t, pool)
	t.Cleanup(func() { stop() })
	create := func(t *testing.T, sandbox, quarantine bool) (*runnersv1.BeginVolumeAnchorMigrationRequest, []*runnersv1.Volume) {
		t.Helper()
		raw := newRequest(sandbox)
		plan := &runnersv1.BeginVolumeAnchorMigrationRequest{Id: uuid.NewString(), OwnerKind: raw.OwnerKind,
			RunnerId: raw.RunnerId, OrganizationId: raw.OrganizationId, BackendId: lifecycleTestBackend}
		var originals []*runnersv1.Volume
		for i := 0; i < 2; i++ {
			r := proto.Clone(raw).(*runnersv1.CreateVolumeRequest)
			r.Id, r.VolumeDefinitionId = uuid.NewString(), ptr(uuid.NewString())
			var v *runnersv1.Volume
			if i == 0 && !quarantine {
				resp, err := client.CreateVolumeChecked(ctx, &runnersv1.CreateVolumeCheckedRequest{Volume: r})
				if err != nil {
					t.Fatal(err)
				}
				v = resp.Volume
			} else {
				resp, err := client.CreateVolume(ctx, r)
				if err != nil {
					t.Fatal(err)
				}
				v = resp.Volume
			}
			previous := lifecycleTestInstance(v)
			if quarantine {
				phase := runnersv1.VolumeStatus_VOLUME_STATUS_FAILED
				resp, err := client.UpdateVolume(ctx, &runnersv1.UpdateVolumeRequest{Id: r.Id, Status: &phase})
				if err != nil {
					t.Fatal(err)
				}
				v, previous = resp.Volume, nil
			} else if v.CheckedLifecycle {
				resp, err := client.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{Id: r.Id, ExpectedRevision: v.LifecycleRevision,
					Operation: &runnersv1.UpdateVolumeCheckedRequest_Bind{Bind: &runnersv1.BindVolumeInstance{Instance: previous}}})
				if err != nil {
					t.Fatal(err)
				}
				v = resp.Volume
			} else {
				phase := runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE
				resp, err := client.UpdateVolume(ctx, &runnersv1.UpdateVolumeRequest{Id: r.Id, Status: &phase, InstanceId: &previous.InstanceId})
				if err != nil {
					t.Fatal(err)
				}
				v = resp.Volume
			}
			plan.OwnerId = v.OwnerId
			plan.Sources = append(plan.Sources, &runnersv1.VolumeAnchorMigrationSource{VolumeId: v.Meta.Id, ExpectedRevision: v.LifecycleRevision, Previous: previous})
			originals = append(originals, v)
		}
		return plan, originals
	}
	workload := func(v *runnersv1.Volume) *runnersv1.CreateWorkloadRequest {
		return &runnersv1.CreateWorkloadRequest{Id: uuid.NewString(), OwnerKind: v.OwnerKind, OwnerId: v.OwnerId,
			RunnerId: v.RunnerId, OrganizationId: v.OrganizationId, ThreadId: v.ThreadId, AgentId: v.AgentId,
			Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING}
	}
	snapshot := func(t *testing.T, owner string) string {
		t.Helper()
		var s string
		if err := reader.QueryRow(ctx, `SELECT jsonb_build_object('volumes',(SELECT jsonb_agg(to_jsonb(v) ORDER BY id) FROM volumes v WHERE owner_id=$1),
			'guard',(SELECT to_jsonb(g) FROM runtime_volume_admission_guards g WHERE owner_id=$1))::text`, owner).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	assertSQLBlocked := func(t *testing.T, query string, args ...any) {
		t.Helper()
		_, err := pool.Exec(ctx, query, args...)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "55000" {
			t.Fatalf("unguarded migration write: %v", err)
		}
	}
	for _, sandbox := range []bool{false, true} {
		t.Run(fmt.Sprintf("resume-every-step/sandbox=%t", sandbox), func(t *testing.T) {
			plan, originals := create(t, sandbox, false)
			before := snapshot(t, plan.OwnerId)
			bad := proto.Clone(plan).(*runnersv1.BeginVolumeAnchorMigrationRequest)
			bad.Sources = bad.Sources[:1]
			if _, err := client.BeginVolumeAnchorMigration(ctx, bad); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("incomplete owner accepted: %v", err)
			}
			bad = proto.Clone(plan).(*runnersv1.BeginVolumeAnchorMigrationRequest)
			bad.Sources[0].ExpectedRevision++
			if _, err := client.BeginVolumeAnchorMigration(ctx, bad); status.Code(err) != codes.Aborted {
				t.Fatalf("stale source accepted: %v", err)
			}
			if snapshot(t, plan.OwnerId) != before {
				t.Fatal("rejected plan changed original state")
			}
			begun, err := client.BeginVolumeAnchorMigration(ctx, plan)
			if err != nil {
				t.Fatal(err)
			}
			m := begun.Migration
			read := func() {
				t.Helper()
				stop()
				client, stop = preparedRegistryClient(fixture, pool)
				stored, err := client.GetVolumeAnchorMigration(ctx, &runnersv1.GetVolumeAnchorMigrationRequest{OwnerKind: m.OwnerKind, OwnerId: m.OwnerId})
				if err != nil || !proto.Equal(stored.GetMigration(), m) {
					t.Fatalf("restart lost committed migration: %v", err)
				}
				retry, err := client.BeginVolumeAnchorMigration(ctx, plan)
				if err != nil || !proto.Equal(retry.GetMigration(), m) {
					t.Fatalf("lost Begin reply retargeted migration: %v", err)
				}
			}
			request := func() *runnersv1.AdvanceVolumeAnchorMigrationRequest {
				return &runnersv1.AdvanceVolumeAnchorMigrationRequest{OwnerKind: m.OwnerKind, OwnerId: m.OwnerId, Id: m.Id, ExpectedRevision: m.Revision}
			}
			step := func(r *runnersv1.AdvanceVolumeAnchorMigrationRequest) {
				t.Helper()
				resp, err := client.AdvanceVolumeAnchorMigration(ctx, r)
				if err != nil {
					t.Fatal(err)
				}
				if resp.Migration.Revision != m.Revision+1 {
					t.Fatal("CAS revision did not advance once")
				}
				m = resp.Migration
				if _, err := client.AdvanceVolumeAnchorMigration(ctx, r); status.Code(err) != codes.Aborted {
					t.Fatalf("stale retry was not rejected: %v", err)
				}
				read()
			}
			blocked := func() {
				t.Helper()
				if _, err := client.CreateWorkload(ctx, workload(originals[0])); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("old client admitted workload: %v", err)
				}
				for _, v := range originals {
					assertSQLBlocked(t, `UPDATE volumes SET size_gb='99',lifecycle_revision=lifecycle_revision+1 WHERE id=$1`, v.Meta.Id)
					assertSQLBlocked(t, `DELETE FROM volumes WHERE id=$1`, v.Meta.Id)
				}
				assertSQLBlocked(t, `UPDATE runtime_volume_admission_guards SET volume_anchor_migration=NULL WHERE owner_id=$1`, plan.OwnerId)
				assertSQLBlocked(t, `DELETE FROM runtime_volume_admission_guards WHERE owner_id=$1`, plan.OwnerId)
				r := request()
				r.Operation = &runnersv1.AdvanceVolumeAnchorMigrationRequest_Complete{Complete: &runnersv1.CompleteVolumeAnchorMigration{}}
				if _, err := client.AdvanceVolumeAnchorMigration(ctx, r); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("early completion opened owner: %v", err)
				}
			}
			read()
			blocked()
			for _, expression := range []string{
				`volume_anchor_migration || '{"complete":false}'::jsonb`,
				`volume_anchor_migration || '{"complete":null}'::jsonb`,
				`volume_anchor_migration || '{"unexpected":true}'::jsonb`,
				`volume_anchor_migration || '{"revision":"99999999999999999999999"}'::jsonb`,
				`jsonb_set(volume_anchor_migration,'{entries,0,source,expectedRevision}','"100"') || '{"revision":"2"}'::jsonb`,
				`jsonb_set(volume_anchor_migration,'{entries,0,adoption}','null') || '{"revision":"2"}'::jsonb`,
			} {
				assertSQLBlocked(t, `UPDATE runtime_volume_admission_guards SET volume_anchor_migration=`+expression+` WHERE owner_id=$1`, plan.OwnerId)
			}
			for i := range m.Entries {
				e := m.Entries[i]
				a := &runnerv1.VolumeAnchorAdoption{Id: e.AdoptionId, Previous: e.Source.Previous,
					Anchor: proto.Clone(e.Intent).(*runnerv1.ResourceAnchor), InstanceUid: uuid.NewString(), PvcSpecSha256: strings.Repeat("a", 64)}
				a.Anchor.InstanceUid = uuid.NewString()
				wrong := proto.Clone(a).(*runnerv1.VolumeAnchorAdoption)
				wrong.Previous.InstanceUid = uuid.NewString()
				r := request()
				r.VolumeId = e.Source.VolumeId
				r.Operation = &runnersv1.AdvanceVolumeAnchorMigrationRequest_Reserve{Reserve: wrong}
				if _, err := client.AdvanceVolumeAnchorMigration(ctx, r); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("replacement PVC accepted: %v", err)
				}
				r.Operation = &runnersv1.AdvanceVolumeAnchorMigrationRequest_Reserve{Reserve: a}
				step(r)
				blocked()
				binding := proto.Clone(a.Previous).(*runnerv1.VolumeListItem)
				binding.Anchor = a.Anchor
				ready := &runnerv1.ObserveVolumeAnchorAdoptionResponse{Adoption: a, Volume: binding, State: runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_READY}
				r = request()
				r.VolumeId, r.Operation = e.Source.VolumeId, &runnersv1.AdvanceVolumeAnchorMigrationRequest_Ready{Ready: ready}
				if _, err := client.AdvanceVolumeAnchorMigration(ctx, r); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("READY before persisted APPLIED accepted: %v", err)
				}
				r.Operation = &runnersv1.AdvanceVolumeAnchorMigrationRequest_Apply{Apply: &runnerv1.ApplyVolumeAnchorAdoptionResponse{Adoption: a, Volume: binding, State: runnerv1.VolumeAnchorAdoptionState_VOLUME_ANCHOR_ADOPTION_STATE_APPLIED}}
				step(r)
				blocked()
				r = request()
				r.VolumeId, r.Operation = e.Source.VolumeId, &runnersv1.AdvanceVolumeAnchorMigrationRequest_Ready{Ready: ready}
				step(r)
				if i+1 < len(m.Entries) {
					blocked()
				}
			}
			r := request()
			r.Operation = &runnersv1.AdvanceVolumeAnchorMigrationRequest_Complete{Complete: &runnersv1.CompleteVolumeAnchorMigration{}}
			step(r)
			if !m.Complete {
				t.Fatal("migration did not complete")
			}
			for _, original := range originals {
				record, err := scanVolume(reader.QueryRow(ctx, "SELECT "+volumeColumns+" FROM volumes WHERE id=$1", original.Meta.Id))
				if err != nil {
					t.Fatal(err)
				}
				v, err := toProtoVolume(record)
				if err != nil {
					t.Fatal(err)
				}
				expected := proto.Clone(original).(*runnersv1.Volume)
				expected.CheckedLifecycle = true
				expected.LifecycleRevision++
				if !original.CheckedLifecycle {
					expected.LifecycleRevision++
				}
				expected.BoundInstance, expected.ResourceAnchor, expected.AnchorAdoption = v.BoundInstance, v.ResourceAnchor, v.AnchorAdoption
				expected.Meta.UpdatedAt = v.Meta.UpdatedAt
				if !proto.Equal(v, expected) || v.AnchorReservation != nil || !proto.Equal(v.AnchorAdoption.Anchor, v.ResourceAnchor) {
					t.Fatal("migration changed data identity or fabricated allocation provenance")
				}
				if _, err := pool.Exec(ctx, `UPDATE volumes SET last_metering_sampled_at=NOW() WHERE id=$1`, v.Meta.Id); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := client.CreateWorkload(ctx, workload(originals[0])); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("completion admitted unanchored writer: %v", err)
			}
			prep := &runnersv1.CreatePreparedWorkloadRequest{Workload: workload(originals[0]), BackendId: m.BackendId}
			for _, v := range originals {
				prep.VolumeIds = append(prep.VolumeIds, v.Meta.Id)
			}
			if _, err := client.CreateAnchoredWorkload(ctx, &runnersv1.CreateAnchoredWorkloadRequest{Preparation: prep}); err != nil {
				t.Fatalf("migrated owner rejected checked followup: %v", err)
			}
			assertSQLBlocked(t, `UPDATE runtime_volume_admission_guards SET volume_anchor_migration=NULL WHERE owner_id=$1`, plan.OwnerId)
		})
		t.Run(fmt.Sprintf("quarantine/sandbox=%t", sandbox), func(t *testing.T) {
			plan, originals := create(t, sandbox, true)
			resp, err := client.BeginVolumeAnchorMigration(ctx, plan)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range resp.Migration.Entries {
				if e.UnresolvedReason != "unbound_failed_generation" || e.Adoption != nil || e.Intent != nil {
					t.Fatal("invented unbound generation authority")
				}
			}
			for _, v := range originals {
				r, err := scanVolume(reader.QueryRow(ctx, "SELECT "+volumeColumns+" FROM volumes WHERE id=$1", v.Meta.Id))
				if err != nil {
					t.Fatal(err)
				}
				stored, err := toProtoVolume(r)
				if err != nil || !proto.Equal(v, stored) {
					t.Fatalf("quarantine reset historical record: %v", err)
				}
				assertSQLBlocked(t, `UPDATE volumes SET owner_id=$2 WHERE id=$1`, v.Meta.Id, uuid.New())
				assertSQLBlocked(t, `UPDATE volumes SET status='provisioning' WHERE id=$1`, v.Meta.Id)
			}
			if _, err := client.CreateWorkload(ctx, workload(originals[0])); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("quarantined owner admitted: %v", err)
			}
			_, err = client.AdvanceVolumeAnchorMigration(ctx, &runnersv1.AdvanceVolumeAnchorMigrationRequest{OwnerKind: plan.OwnerKind, OwnerId: plan.OwnerId, Id: plan.Id, ExpectedRevision: 1,
				Operation: &runnersv1.AdvanceVolumeAnchorMigrationRequest_Complete{Complete: &runnersv1.CompleteVolumeAnchorMigration{}}})
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("quarantine completed without storage: %v", err)
			}
		})
		for _, isolation := range []pgx.TxIsoLevel{pgx.ReadCommitted, pgx.RepeatableRead, pgx.Serializable} {
			t.Run(fmt.Sprintf("stale-owner-write/sandbox=%t/%s", sandbox, isolation), func(t *testing.T) {
				plan, originals := create(t, sandbox, true)
				tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: isolation})
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback(context.Background())
				var count int
				if err := tx.QueryRow(ctx, `SELECT count(*) FROM runtime_volume_admission_guards WHERE owner_id=$1`, plan.OwnerId).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if _, err := client.BeginVolumeAnchorMigration(ctx, plan); err != nil {
					t.Fatal(err)
				}
				_, err = tx.Exec(ctx, `UPDATE volumes SET owner_id=$2 WHERE id=$1`, originals[0].Meta.Id, uuid.New())
				var pgErr *pgconn.PgError
				code := "55000"
				if isolation != pgx.ReadCommitted {
					code = "40001"
				}
				if !errors.As(err, &pgErr) || pgErr.Code != code {
					t.Fatalf("stale ownership escape: %v", err)
				}
			})
		}
		t.Run(fmt.Sprintf("requires-drain/sandbox=%t", sandbox), func(t *testing.T) {
			plan, originals := create(t, sandbox, false)
			if _, err := client.CreateWorkload(ctx, workload(originals[0])); err != nil {
				t.Fatal(err)
			}
			before := snapshot(t, plan.OwnerId)
			if _, err := client.BeginVolumeAnchorMigration(ctx, plan); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("undrained owner migrated: %v", err)
			}
			if snapshot(t, plan.OwnerId) != before {
				t.Fatal("rejected undrained migration changed state")
			}
		})
	}
}
