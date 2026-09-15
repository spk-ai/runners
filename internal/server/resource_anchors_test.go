package server

import (
	"context"
	"math"
	"testing"
	"time"

	runnerv1 "github.com/agynio/runners/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/pashagolub/pgxmock/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func registryTestAnchor(w workloadRecord, kind runnerv1.ResourceAnchorKind, id uuid.UUID, human string) *runnerv1.ResourceAnchor {
	labels := map[string]string{"app.kubernetes.io/managed-by": "k8s-runner", "agyn.dev/managed-by": "agents-orchestrator", "managed-by": "agents-orchestrator"}
	if w.OwnerKind == runtimeOwnerKindSandbox {
		labels["sandbox-id"], labels["sandbox-owner-id"] = w.OwnerID.String(), human
	} else {
		labels["agent-instance-id"], labels["agent-id"] = w.OwnerID.String(), w.AgentID.String()
		if kind == runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD {
			labels["thread-id"] = w.ThreadID.String()
		}
	}
	if kind == runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME {
		labels["volume_key"] = id.String()
	}
	return &runnerv1.ResourceAnchor{Kind: kind, ResourceId: id.String(), BackendId: w.Preparation.BackendId, InstanceUid: uuid.NewString(), IdentityLabels: labels}
}

func registryTestAnchoredWorkload(sandbox bool, count int) workloadRecord {
	w := defaultWorkloadRecord(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), time.Now())
	w.Status, w.OwnerID = workloadStatusStarting, uuid.New()
	if sandbox {
		w.OwnerKind, w.AgentID, w.ThreadID = runtimeOwnerKindSandbox, uuid.Nil, uuid.Nil
	}
	w.Preparation = &runnersv1.PreparedWorkloadLifecycle{Phase: preparationPhases["reserved"], Revision: 1, BackendId: lifecycleTestBackend,
		Resources: &runnersv1.WorkloadResourceAnchors{Revision: 2}}
	human := uuid.NewString()
	w.Preparation.Resources.Workload = registryTestAnchor(w, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD, w.Meta.ID, human)
	for i := 0; i < count; i++ {
		id := uuid.New()
		w.Preparation.VolumeIds = append(w.Preparation.VolumeIds, id.String())
		w.Preparation.Resources.Volumes = append(w.Preparation.Resources.Volumes, registryTestAnchor(w, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME, id, human))
	}
	return w
}

func TestRegistryResourceAnchorIdentity(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		w := registryTestAnchoredWorkload(sandbox, 1)
		for _, original := range []*runnerv1.ResourceAnchor{w.Preparation.Resources.Workload, w.Preparation.Resources.Volumes[0]} {
			validate := func(a *runnerv1.ResourceAnchor) error {
				return validateRegistryResourceAnchor(a, original.Kind, uuid.MustParse(original.ResourceId), w.Preparation.BackendId, w.OwnerKind, w.OwnerID, w.AgentID, w.ThreadID)
			}
			if err := validate(original); err != nil {
				t.Fatal(err)
			}
			for name, mutate := range map[string]func(*runnerv1.ResourceAnchor){
				"wrong-resource":     func(a *runnerv1.ResourceAnchor) { a.ResourceId = uuid.NewString() },
				"wrong-kind":         func(a *runnerv1.ResourceAnchor) { a.Kind = 0 },
				"wrong-backend":      func(a *runnerv1.ResourceAnchor) { a.BackendId = "other" },
				"missing-uid":        func(a *runnerv1.ResourceAnchor) { a.InstanceUid = "" },
				"nil-uid":            func(a *runnerv1.ResourceAnchor) { a.InstanceUid = uuid.Nil.String() },
				"padded-uid":         func(a *runnerv1.ResourceAnchor) { a.InstanceUid += " " },
				"missing-label":      func(a *runnerv1.ResourceAnchor) { delete(a.IdentityLabels, "managed-by") },
				"extra-label":        func(a *runnerv1.ResourceAnchor) { a.IdentityLabels["extra"] = "owner" },
				"unknown-wire-field": func(a *runnerv1.ResourceAnchor) { a.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
			} {
				t.Run(w.OwnerKind+"/"+original.Kind.String()+"/"+name, func(t *testing.T) {
					a := proto.Clone(original).(*runnerv1.ResourceAnchor)
					mutate(a)
					if err := validate(a); err == nil {
						t.Fatal("invalid native identity accepted")
					}
				})
			}
			if err := validate(nil); err == nil {
				t.Fatal("nil anchor accepted")
			}
		}
	}
}

func TestResourceAnchorThreadIsNotLegacyInstanceAlias(t *testing.T) {
	w := registryTestAnchoredWorkload(false, 1)
	w.ThreadID = w.OwnerID
	a := w.Preparation.Resources.Workload
	a.IdentityLabels["thread-id"] = uuid.NewString()
	if a.IdentityLabels["thread-id"] == w.ThreadID.String() {
		t.Fatal("fixture did not distinguish inbox thread from legacy instance alias")
	}
	if err := validateWorkloadResourceAnchors(w, w.Preparation.Resources); err != nil {
		t.Fatalf("actual inbox thread was confused with registry thread_id: %v", err)
	}
	for _, invalid := range []string{"", uuid.Nil.String(), "not-a-thread", uuid.NewString() + " "} {
		a.IdentityLabels["thread-id"] = invalid
		if err := validateWorkloadResourceAnchors(w, w.Preparation.Resources); err == nil {
			t.Fatal("noncanonical native inbox thread accepted")
		}
	}
}

func TestRegistryResourceAnchorSet(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		w := registryTestAnchoredWorkload(sandbox, 2)
		if err := validateWorkloadResourceAnchors(w, w.Preparation.Resources); err != nil {
			t.Fatal(err)
		}
		for name, mutate := range map[string]func(*runnersv1.WorkloadResourceAnchors){
			"zero-revision":     func(a *runnersv1.WorkloadResourceAnchors) { a.Revision = 0 },
			"overflow-revision": func(a *runnersv1.WorkloadResourceAnchors) { a.Revision = math.MaxUint64 },
			"missing-workload":  func(a *runnersv1.WorkloadResourceAnchors) { a.Workload = nil },
			"missing-volume":    func(a *runnersv1.WorkloadResourceAnchors) { a.Volumes = a.Volumes[:1] },
			"duplicate-volume":  func(a *runnersv1.WorkloadResourceAnchors) { a.Volumes[1] = a.Volumes[0] },
			"nil-volume":        func(a *runnersv1.WorkloadResourceAnchors) { a.Volumes[1] = nil },
			"extra-wire-field":  func(a *runnersv1.WorkloadResourceAnchors) { a.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		} {
			t.Run(w.OwnerKind+"/"+name, func(t *testing.T) {
				a := proto.Clone(w.Preparation.Resources).(*runnersv1.WorkloadResourceAnchors)
				mutate(a)
				if err := validateWorkloadResourceAnchors(w, a); err == nil {
					t.Fatal("incomplete anchor set accepted")
				}
			})
		}
		if sandbox {
			a := proto.Clone(w.Preparation.Resources).(*runnersv1.WorkloadResourceAnchors)
			a.Volumes[0].IdentityLabels["sandbox-owner-id"] = uuid.NewString()
			if err := validateWorkloadResourceAnchors(w, a); err == nil {
				t.Fatal("different human owner accepted")
			}
		}
	}
}

func TestAnchoredBindingCannotLearnMissingOwner(t *testing.T) {
	w := registryTestAnchoredWorkload(false, 1)
	a := w.Preparation.Resources.Volumes[0]
	b := &runnerv1.WorkloadBinding{WorkloadId: w.Meta.ID.String(), InstanceUid: uuid.NewString(), BackendId: w.Preparation.BackendId,
		Anchor: w.Preparation.Resources.Workload, Volumes: []*runnerv1.VolumeListItem{{VolumeKey: a.ResourceId, InstanceId: "pv-test", BackendId: a.BackendId, Anchor: a}}}
	if _, err := validatePreparedWorkloadBinding(w, b); err != nil {
		t.Fatal(err)
	}
	w.Preparation.Resources.Volumes = nil
	b.Volumes[0].Anchor = nil
	if _, err := validatePreparedWorkloadBinding(w, b); err == nil {
		t.Fatal("missing persisted volume anchor accepted")
	}
}

func TestAnchoredVolumeBindingKeepsHumanOwner(t *testing.T) {
	w := registryTestAnchoredWorkload(true, 1)
	a := w.Preparation.Resources.Volumes[0]
	v := volumeRecord{Meta: entityMeta{ID: uuid.MustParse(a.ResourceId)}, OwnerID: w.OwnerID, OwnerKind: w.OwnerKind,
		Status: volumeStatusProvisioning, CheckedLifecycle: true, SizeGB: "1", ResourceAnchor: a}
	p, err := toProtoVolume(v)
	if err != nil {
		t.Fatal(err)
	}
	b := lifecycleTestInstance(p)
	b.Anchor = a
	b.IdentityLabels["sandbox-owner-id"] = a.IdentityLabels["sandbox-owner-id"]
	if err := validateVolumeBinding(v, b); err != nil {
		t.Fatal(err)
	}
	b.IdentityLabels["sandbox-owner-id"] = uuid.NewString()
	if err := validateVolumeBinding(v, b); err == nil {
		t.Fatal("PVC human owner differs from persistent anchor")
	}
}

func TestAnchoredWorkloadCapabilityAndRevision(t *testing.T) {
	for _, mode := range []string{"legacy-api", "stale-anchor", "missing-anchors", "lost-cas"} {
		t.Run(mode, func(t *testing.T) {
			mock, err := pgxmock.NewPool()
			if err != nil {
				t.Fatal(err)
			}
			defer mock.Close()
			w := registryTestAnchoredWorkload(false, 0)
			if mode == "missing-anchors" {
				w.Preparation.Resources = &runnersv1.WorkloadResourceAnchors{Revision: 1}
			}
			mock.ExpectQuery("SELECT .* FROM workloads WHERE id = \\$1").WithArgs(w.Meta.ID).WillReturnRows(workloadRows(t, w))
			op := preparedOperation("prepare", nil)
			op.Id, op.ExpectedRevision = w.Meta.ID.String(), w.Preparation.Revision
			srv := New(Options{Pool: mock})
			want := codes.FailedPrecondition
			if mode == "legacy-api" {
				_, err = srv.UpdatePreparedWorkload(context.Background(), op)
			} else {
				revision := w.Preparation.Resources.Revision
				if mode == "stale-anchor" {
					revision++
					want = codes.Aborted
				}
				if mode == "lost-cas" {
					mock.ExpectQuery("UPDATE workloads SET").WithArgs(w.Meta.ID, int64(1), "preparing", []byte(nil), []byte(nil), pgxmock.AnyArg(), "2").WillReturnRows(workloadRows(t))
					want = codes.Aborted
				}
				_, err = srv.UpdateAnchoredWorkload(context.Background(), &runnersv1.UpdateAnchoredWorkloadRequest{Operation: op, ExpectedAnchorRevision: revision})
			}
			if status.Code(err) != want {
				t.Fatalf("got %v, want %s", err, want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
