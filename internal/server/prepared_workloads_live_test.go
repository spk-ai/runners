package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	agentsv1 "github.com/agynio/runners/.gen/go/agynio/api/agents/v1"
	authorizationv1 "github.com/agynio/runners/.gen/go/agynio/api/authorization/v1"
	runnerv1 "github.com/agynio/runners/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Only called under TestLiveVolumeReopen's disposable loopback database gate.
// Registry RPCs and SQL are real; authorization and native receipts are fixtures.
func preparedRegistryClient(t *testing.T, pool dbPool) (runnersv1.RunnersServiceClient, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	runnersv1.RegisterRunnersServiceServer(srv, New(Options{Pool: pool, AgentsClient: fakeAgentsClient{
		getAgent: func(context.Context, *agentsv1.GetAgentRequest) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Name: "fixture-agent"}}, nil
		},
		getSandbox: func(context.Context, *agentsv1.GetSandboxRequest) (*agentsv1.GetSandboxResponse, error) {
			return &agentsv1.GetSandboxResponse{Sandbox: &agentsv1.Sandbox{Name: "fixture-sandbox"}}, nil
		},
	}, AuthorizationClient: fakeAuthorizationClient{
		write: func(context.Context, *authorizationv1.WriteRequest) (*authorizationv1.WriteResponse, error) {
			return &authorizationv1.WriteResponse{}, nil
		},
	}}))
	served := make(chan error, 1)
	go func() { served <- srv.Serve(listener) }()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		<-served
		t.Fatal(err)
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		_ = conn.Close()
		srv.Stop()
		if err := <-served; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Error(err)
		}
	}
	t.Cleanup(stop)
	return runnersv1.NewRunnersServiceClient(conn), stop
}

func preparedCreateRequest(v *runnersv1.Volume) *runnersv1.CreatePreparedWorkloadRequest {
	return &runnersv1.CreatePreparedWorkloadRequest{
		Workload: &runnersv1.CreateWorkloadRequest{Id: uuid.NewString(), RunnerId: v.RunnerId, OrganizationId: v.OrganizationId,
			ThreadId: v.ThreadId, AgentId: v.AgentId, OwnerKind: v.OwnerKind, OwnerId: v.OwnerId, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING},
		BackendId: lifecycleTestBackend, VolumeIds: []string{v.Meta.Id},
	}
}

func testPreparedWorkloads(t *testing.T, ctx context.Context, pool *pgxpool.Pool, reader *pgx.Conn, newRequest func(bool) *runnersv1.CreateVolumeRequest) {
	client, _ := preparedRegistryClient(t, pool)
	createVolume := func(t *testing.T, sandbox, bind bool) *runnersv1.Volume {
		t.Helper()
		created, err := client.CreateVolumeChecked(ctx, &runnersv1.CreateVolumeCheckedRequest{Volume: newRequest(sandbox)})
		if err != nil {
			t.Fatal(err)
		}
		if !bind {
			return created.Volume
		}
		resp, err := client.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{Id: created.Volume.Meta.Id, ExpectedRevision: created.Volume.LifecycleRevision,
			Operation: &runnersv1.UpdateVolumeCheckedRequest_Bind{Bind: &runnersv1.BindVolumeInstance{Instance: lifecycleTestInstance(created.Volume)}}})
		if err != nil {
			t.Fatal(err)
		}
		return resp.Volume
	}
	reserve := func(t *testing.T, v *runnersv1.Volume) *runnersv1.Workload {
		t.Helper()
		resp, err := client.CreatePreparedWorkload(ctx, preparedCreateRequest(v))
		if err != nil {
			t.Fatal(err)
		}
		return resp.Workload
	}
	step := func(t *testing.T, w *runnersv1.Workload, op string, b *runnerv1.WorkloadBinding) *runnersv1.Workload {
		t.Helper()
		req := preparedOperation(op, b)
		req.Id, req.ExpectedRevision = w.Meta.Id, w.Preparation.Revision
		resp, err := client.UpdatePreparedWorkload(ctx, req)
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		if resp.Workload.Preparation.Revision != w.Preparation.Revision+1 {
			t.Fatal("revision did not advance once")
		}
		stored, err := scanWorkload(reader.QueryRow(ctx, "SELECT "+workloadColumns+" FROM workloads WHERE id = $1", w.Meta.Id))
		if err != nil {
			t.Fatal(err)
		}
		p, err := toProtoWorkload(stored)
		if err != nil || !proto.Equal(p, resp.Workload) {
			t.Fatalf("response differs from independent SQL: %v", err)
		}
		return resp.Workload
	}
	binding := func(w *runnersv1.Workload, v *runnersv1.Volume) *runnerv1.WorkloadBinding {
		instance := v.BoundInstance
		if instance == nil {
			instance = lifecycleTestInstance(v)
		}
		return &runnerv1.WorkloadBinding{WorkloadId: w.Meta.Id, InstanceUid: uuid.NewString(), BackendId: w.Preparation.BackendId, Volumes: []*runnerv1.VolumeListItem{proto.Clone(instance).(*runnerv1.VolumeListItem)}}
	}
	snapshot := func(t *testing.T, id string) string {
		t.Helper()
		var data string
		if err := reader.QueryRow(ctx, "SELECT to_jsonb(w)::text FROM workloads w WHERE id = $1", id).Scan(&data); err != nil {
			t.Fatal(err)
		}
		return data
	}
	for _, sandbox := range []bool{false, true} {
		t.Run(fmt.Sprintf("lifecycle/sandbox=%t", sandbox), func(t *testing.T) {
			v := createVolume(t, sandbox, true)
			w := reserve(t, v)
			duplicate := preparedCreateRequest(v)
			duplicate.Workload.Id = w.Meta.Id
			beforeDuplicate := snapshot(t, w.Meta.Id)
			if _, err := client.CreatePreparedWorkload(ctx, duplicate); status.Code(err) != codes.AlreadyExists || beforeDuplicate != snapshot(t, w.Meta.Id) {
				t.Fatalf("lost create reply did not preserve the original reservation: %v", err)
			}
			for turn := 0; turn < 2; turn++ {
				w = step(t, w, "prepare", nil)
				b := binding(w, v)
				w = step(t, w, "bind", b)
				// A new server and connection recover only committed state.
				fresh, stop := preparedRegistryClient(t, pool)
				recovered, err := fresh.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: w.Meta.Id})
				stop()
				if err != nil || !proto.Equal(recovered.GetWorkload().GetPreparation(), w.Preparation) {
					t.Fatalf("registry restart lost binding: %v", err)
				}
				w = step(t, w, "activate", nil)
				w = step(t, w, "active", b)
				before := snapshot(t, w.Meta.Id)
				for _, sql := range []string{
					"UPDATE workloads SET status = 'stopped', removal_confirmed_at = NOW() WHERE id = $1",
					"UPDATE workloads SET instance_id = 'replacement' WHERE id = $1",
					"UPDATE workloads SET prepared_backend_id = 'replacement' WHERE id = $1",
					"UPDATE workloads SET preparation_phase = 'removed', preparation_revision = preparation_revision + 1 WHERE id = $1",
					"UPDATE workloads SET prepared_volume_ids = '{}' WHERE id = $1",
					"UPDATE workloads SET prepared_binding = jsonb_set(prepared_binding, '{instanceUid}', to_jsonb('11111111-1111-1111-1111-111111111111'::text)) WHERE id = $1",
					"DELETE FROM workloads WHERE id = $1",
				} {
					_, err := pool.Exec(ctx, sql, w.Meta.Id)
					if status.Code(toStatusError(err)) != codes.FailedPrecondition || before != snapshot(t, w.Meta.Id) {
						t.Fatalf("old SQL bypass/partial write: %s err=%v", sql, err)
					}
				}
				w = step(t, w, "remove", nil)
				if _, err := client.CreatePreparedWorkload(ctx, preparedCreateRequest(v)); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("unconfirmed predecessor admitted: %v", err)
				}
				if err := admissionBeginSQL(ctx, pool, v); err == nil {
					t.Fatal("unconfirmed workload allowed volume deletion")
				}
				w = step(t, w, "removed", b)
				if w.RemovalConfirmedAt == nil || w.Status != runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED || w.Preparation.RemovalObservation == nil {
					t.Fatal("absence evidence not retained")
				}
				if turn == 0 {
					w = reserve(t, v)
				}
			}
			// The immutable pin, not the history row, excludes old clients.
			if _, err := pool.Exec(ctx, "DELETE FROM workloads WHERE owner_id = $1", v.OwnerId); err != nil {
				t.Fatal(err)
			}
			if _, err := client.CreateWorkload(ctx, preparedCreateRequest(v).Workload); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("legacy fallback after history GC: %v", err)
			}
			for _, sql := range []string{"DELETE FROM runtime_volume_admission_guards WHERE owner_id = $1", "UPDATE runtime_volume_admission_guards SET prepared_backend_id = NULL WHERE owner_id = $1"} {
				if _, err := pool.Exec(ctx, sql, v.OwnerId); status.Code(toStatusError(err)) != codes.FailedPrecondition {
					t.Fatalf("pin was discarded: %v", err)
				}
			}
		})
		t.Run(fmt.Sprintf("late-prepare/sandbox=%t", sandbox), func(t *testing.T) {
			v := createVolume(t, sandbox, false)
			w := step(t, reserve(t, v), "prepare", nil)
			w = step(t, w, "remove", nil)
			b := binding(w, v)
			for _, op := range []string{"activate", "removed", "abort"} {
				r := preparedOperation(op, b)
				r.Id, r.ExpectedRevision = w.Meta.Id, w.Preparation.Revision
				if _, err := client.UpdatePreparedWorkload(ctx, r); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("unknown prepare allowed %s: %v", op, err)
				}
			}
			if _, err := pool.Exec(ctx, "UPDATE workloads SET status = 'failed' WHERE id = $1", w.Meta.Id); err != nil {
				t.Fatal(err)
			}
			if _, err := client.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{Id: v.Meta.Id, ExpectedRevision: v.LifecycleRevision,
				Operation: &runnersv1.UpdateVolumeCheckedRequest_FailProvisioning{FailProvisioning: &runnersv1.FailVolumeProvisioning{}}}); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("lost prepare volume was closed: %v", err)
			}
			if _, err := client.UpdateVolumeChecked(ctx, &runnersv1.UpdateVolumeCheckedRequest{Id: v.Meta.Id, ExpectedRevision: v.LifecycleRevision,
				Operation: &runnersv1.UpdateVolumeCheckedRequest_Bind{Bind: &runnersv1.BindVolumeInstance{Instance: b.Volumes[0]}}}); err != nil {
				t.Fatal(err)
			}
			w = step(t, w, "bind", b)
			if w.Preparation.Phase != preparationPhases["removing"] {
				t.Fatal("late prepare reauthorized activation")
			}
			w = step(t, w, "removed", b)
			if w.Status != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
				t.Fatal("removal overwrote failure status")
			}
			step(t, reserve(t, v), "abort", nil)
		})
		t.Run(fmt.Sprintf("volume-set/sandbox=%t", sandbox), func(t *testing.T) {
			v, other := createVolume(t, sandbox, true), createVolume(t, sandbox, true)
			req := preparedCreateRequest(v)
			for _, ids := range [][]string{{uuid.NewString()}, {other.Meta.Id}} {
				req.VolumeIds = ids
				if _, err := client.CreatePreparedWorkload(ctx, req); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("foreign/missing volume admitted: %v", err)
				}
			}
			w := step(t, reserve(t, v), "prepare", nil)
			b := binding(w, v)
			b.Volumes[0].InstanceUid = uuid.NewString()
			r := preparedOperation("bind", b)
			r.Id, r.ExpectedRevision = w.Meta.Id, w.Preparation.Revision
			if _, err := client.UpdatePreparedWorkload(ctx, r); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("replacement volume bound: %v", err)
			}
			before := snapshot(t, w.Meta.Id)
			for _, mutate := range []func(*runnerv1.WorkloadBinding){
				func(b *runnerv1.WorkloadBinding) { b.Volumes[0].InstanceUid = uuid.NewString() },
				func(b *runnerv1.WorkloadBinding) { b.Volumes = nil },
				func(b *runnerv1.WorkloadBinding) { b.Volumes = append(b.Volumes, b.Volumes[0]) },
				func(b *runnerv1.WorkloadBinding) { b.BackendId = "other" },
				func(b *runnerv1.WorkloadBinding) { b.WorkloadId = uuid.NewString() },
				func(b *runnerv1.WorkloadBinding) { b.InstanceUid = "not-a-uid" },
				func(b *runnerv1.WorkloadBinding) { b.Volumes[0] = other.BoundInstance },
			} {
				b := binding(w, v)
				mutate(b)
				data, err := protojson.Marshal(b)
				if err != nil {
					t.Fatal(err)
				}
				_, err = pool.Exec(ctx, `UPDATE workloads SET preparation_phase = 'bound', preparation_revision = preparation_revision + 1,
                    instance_id = id::text, prepared_binding = $2 WHERE id = $1`, w.Meta.Id, data)
				if status.Code(toStatusError(err)) != codes.FailedPrecondition || before != snapshot(t, w.Meta.Id) {
					t.Fatalf("raw binding bypass: %v", err)
				}
			}
		})
		t.Run(fmt.Sprintf("empty-mount-set/sandbox=%t", sandbox), func(t *testing.T) {
			v := createVolume(t, sandbox, true)
			req := preparedCreateRequest(v)
			req.VolumeIds = nil
			resp, err := client.CreatePreparedWorkload(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			w := step(t, resp.Workload, "prepare", nil)
			b := &runnerv1.WorkloadBinding{WorkloadId: w.Meta.Id, InstanceUid: uuid.NewString(), BackendId: req.BackendId}
			w = step(t, w, "bind", b)
			w = step(t, w, "remove", nil)
			step(t, w, "removed", b)
			req.Workload.Id, req.BackendId = uuid.NewString(), "other-backend"
			if _, err := client.CreatePreparedWorkload(ctx, req); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("owner backend changed: %v", err)
			}
		})
		for _, isolation := range []pgx.TxIsoLevel{pgx.ReadCommitted, pgx.RepeatableRead, pgx.Serializable} {
			for _, cancelFirst := range []bool{false, true} {
				for _, commit := range []bool{false, true} {
					t.Run(fmt.Sprintf("cas/sandbox=%t/%s/cancel-first=%t/commit=%t", sandbox, isolation, cancelFirst, commit), func(t *testing.T) {
						ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
						defer cancel()
						v := createVolume(t, sandbox, true)
						w := step(t, reserve(t, v), "prepare", nil)
						w = step(t, w, "bind", binding(w, v))
						loser, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: isolation})
						if err != nil {
							t.Fatal(err)
						}
						defer loser.Rollback(context.Background())
						var revision int64
						if err := loser.QueryRow(ctx, "SELECT preparation_revision FROM workloads WHERE id = $1", w.Meta.Id).Scan(&revision); err != nil {
							t.Fatal(err)
						}
						winner, err := pool.Begin(ctx)
						if err != nil {
							t.Fatal(err)
						}
						defer winner.Rollback(context.Background())
						first, second := "activating", "removing"
						if cancelFirst {
							first, second = second, first
						}
						query := "UPDATE workloads SET preparation_phase = $3, preparation_revision = preparation_revision + 1 WHERE id = $1 AND preparation_revision = $2"
						if tag, err := winner.Exec(ctx, query, w.Meta.Id, revision, first); err != nil || tag.RowsAffected() != 1 {
							t.Fatalf("first CAS: %v", err)
						}
						type result struct {
							tag pgconn.CommandTag
							err error
						}
						done := make(chan result, 1)
						go func() {
							tag, err := loser.Exec(ctx, query, w.Meta.Id, revision, second)
							if err == nil {
								err = loser.Commit(ctx)
							} else {
								_ = loser.Rollback(ctx)
							}
							done <- result{tag, err}
						}()
						waitErr := waitAdmissionBlocked(ctx, reader, winner.Conn().PgConn().PID(), loser.Conn().PgConn().PID())
						other := createVolume(t, sandbox, true)
						step(t, reserve(t, other), "abort", nil)
						if commit {
							err = winner.Commit(ctx)
						} else {
							err = winner.Rollback(ctx)
						}
						got := <-done
						if waitErr != nil || err != nil {
							t.Fatalf("overlap: wait=%v release=%v", waitErr, err)
						}
						if commit && isolation != pgx.ReadCommitted {
							var pgErr *pgconn.PgError
							if !errors.As(got.err, &pgErr) || pgErr.Code != "40001" {
								t.Fatalf("stale snapshot: %v", got.err)
							}
						} else if got.err != nil || got.tag.RowsAffected() != map[bool]int64{true: 0, false: 1}[commit] {
							t.Fatalf("CAS loser: tag=%v err=%v", got.tag, got.err)
						}
						var phase string
						if err := reader.QueryRow(ctx, "SELECT preparation_phase, preparation_revision FROM workloads WHERE id = $1", w.Meta.Id).Scan(&phase, &revision); err != nil {
							t.Fatal(err)
						}
						want := first
						if !commit {
							want = second
						}
						if phase != want || revision != int64(w.Preparation.Revision)+1 {
							t.Fatal("committed CAS state differs")
						}
					})
				}
			}
		}
	}
}
