package server

import (
	"context"
	"math"
	"strings"
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

func preparedOperation(name string, binding *runnerv1.WorkloadBinding) *runnersv1.UpdatePreparedWorkloadRequest {
	r := &runnersv1.UpdatePreparedWorkloadRequest{}
	switch name {
	case "prepare":
		r.Operation = &runnersv1.UpdatePreparedWorkloadRequest_BeginPreparation{BeginPreparation: &runnersv1.BeginWorkloadPreparation{}}
	case "bind":
		r.Operation = &runnersv1.UpdatePreparedWorkloadRequest_Bind{Bind: &runnersv1.BindPreparedWorkload{Binding: binding}}
	case "activate":
		r.Operation = &runnersv1.UpdatePreparedWorkloadRequest_BeginActivation{BeginActivation: &runnersv1.BeginWorkloadActivation{}}
	case "active":
		r.Operation = &runnersv1.UpdatePreparedWorkloadRequest_ConfirmActivation{ConfirmActivation: &runnersv1.ConfirmWorkloadActivation{Binding: binding}}
	case "remove":
		r.Operation = &runnersv1.UpdatePreparedWorkloadRequest_BeginRemoval{BeginRemoval: &runnersv1.BeginPreparedWorkloadRemoval{}}
	case "removed":
		r.Operation = &runnersv1.UpdatePreparedWorkloadRequest_ConfirmRemoval{ConfirmRemoval: &runnersv1.ConfirmPreparedWorkloadRemoval{Observation: &runnerv1.RemovePreparedWorkloadResponse{
			State: runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_ABSENT, Binding: binding,
		}}}
	case "abort":
		r.Operation = &runnersv1.UpdatePreparedWorkloadRequest_AbortReservation{AbortReservation: &runnersv1.AbortWorkloadReservation{}}
	}
	return r
}

func TestPreparedWorkloadTransitions(t *testing.T) {
	allowed := map[string]map[string]string{
		"reserved":   {"prepare": "preparing", "abort": "removed"},
		"preparing":  {"bind": "bound", "remove": "removing"},
		"bound":      {"activate": "activating", "remove": "removing"},
		"activating": {"active": "active", "remove": "removing"},
		"active":     {"remove": "removing"},
		"removing":   {"removed": "removed"},
		"removed":    {},
	}
	for phase, valid := range allowed {
		for _, operation := range []string{"prepare", "bind", "activate", "active", "remove", "removed", "abort"} {
			t.Run(phase+"/"+operation, func(t *testing.T) {
				current := defaultWorkloadRecord(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), time.Now())
				current.Status = workloadStatusStarting
				binding := &runnerv1.WorkloadBinding{WorkloadId: current.Meta.ID.String(), InstanceUid: uuid.NewString(), BackendId: "backend"}
				current.Preparation = &runnersv1.PreparedWorkloadLifecycle{Phase: preparationPhases[phase], Revision: 3, BackendId: "backend"}
				if phase != "reserved" && phase != "preparing" {
					current.Preparation.Binding = proto.Clone(binding).(*runnerv1.WorkloadBinding)
				}
				before := proto.Clone(current.Preparation)
				next := proto.Clone(current.Preparation).(*runnersv1.PreparedWorkloadLifecycle)
				err := applyPreparedWorkloadOperation(current, next, preparedOperation(operation, binding))
				if expected, ok := valid[operation]; ok {
					if err != nil || next.Phase != preparationPhases[expected] {
						t.Fatalf("valid transition: phase=%s err=%v", next.Phase, err)
					}
				} else if status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("invalid transition accepted: %v", err)
				}
				if !proto.Equal(before, current.Preparation) {
					t.Fatal("operation mutated the current record")
				}
			})
		}
	}
}

func TestPreparedWorkloadUncertainAndStaleEvidence(t *testing.T) {
	current := defaultWorkloadRecord(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), time.Now())
	current.Status = workloadStatusStarting
	current.Preparation = &runnersv1.PreparedWorkloadLifecycle{Phase: preparationPhases["removing"], Revision: 5, BackendId: "backend"}
	binding := &runnerv1.WorkloadBinding{WorkloadId: current.Meta.ID.String(), InstanceUid: uuid.NewString(), BackendId: "backend"}
	for _, op := range []string{"abort", "removed", "prepare", "activate", "active"} {
		next := proto.Clone(current.Preparation).(*runnersv1.PreparedWorkloadLifecycle)
		if err := applyPreparedWorkloadOperation(current, next, preparedOperation(op, binding)); err == nil {
			t.Fatalf("uncertain preparation allowed %s", op)
		}
	}
	next := proto.Clone(current.Preparation).(*runnersv1.PreparedWorkloadLifecycle)
	if err := applyPreparedWorkloadOperation(current, next, preparedOperation("bind", binding)); err != nil || next.Phase != preparationPhases["removing"] {
		t.Fatalf("late cleanup binding: %v", err)
	}
	current.Preparation = next
	for _, mutate := range []func(*runnerv1.WorkloadBinding){
		func(b *runnerv1.WorkloadBinding) { b.WorkloadId = uuid.NewString() },
		func(b *runnerv1.WorkloadBinding) { b.InstanceUid = uuid.NewString() },
		func(b *runnerv1.WorkloadBinding) { b.BackendId = "other" },
		func(b *runnerv1.WorkloadBinding) { b.InstanceUid = uuid.Nil.String() },
	} {
		other := proto.Clone(binding).(*runnerv1.WorkloadBinding)
		mutate(other)
		next := proto.Clone(current.Preparation).(*runnersv1.PreparedWorkloadLifecycle)
		if err := applyPreparedWorkloadOperation(current, next, preparedOperation("removed", other)); err == nil {
			t.Fatal("accepted wrong removal identity")
		}
	}
	request := preparedOperation("removed", binding)
	request.GetConfirmRemoval().Observation.State = runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_PENDING
	if err := applyPreparedWorkloadOperation(current, next, request); err == nil {
		t.Fatal("accepted pending deletion")
	}
}

func TestPreparedWorkloadInvalidRequests(t *testing.T) {
	s := New(Options{})
	for _, req := range []*runnersv1.CreatePreparedWorkloadRequest{
		nil, {}, {Workload: &runnersv1.CreateWorkloadRequest{}},
	} {
		if _, err := s.CreatePreparedWorkload(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid create: %v", err)
		}
	}
	for _, rev := range []uint64{0, math.MaxInt64, math.MaxUint64} {
		req := preparedOperation("prepare", nil)
		req.Id, req.ExpectedRevision = uuid.NewString(), rev
		if _, err := s.UpdatePreparedWorkload(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid revision: %v", err)
		}
	}
	for _, ids := range [][]string{{"bad"}, {uuid.Nil.String()}, {strings.ToUpper(uuid.NewString())}, {"11111111-1111-1111-1111-111111111111", "11111111-1111-1111-1111-111111111111"}} {
		req := &runnersv1.CreatePreparedWorkloadRequest{Workload: &runnersv1.CreateWorkloadRequest{Id: uuid.NewString(), RunnerId: uuid.NewString(), OrganizationId: uuid.NewString(), Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING}, BackendId: "backend", VolumeIds: ids}
		if _, err := s.CreatePreparedWorkload(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid volume set: %v", err)
		}
	}
}

func TestPreparedWorkloadStaleRevision(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()
	s := New(Options{Pool: mock})
	w := defaultWorkloadRecord(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), time.Now())
	w.Preparation = &runnersv1.PreparedWorkloadLifecycle{Phase: preparationPhases["bound"], Revision: 4, BackendId: "backend", Binding: &runnerv1.WorkloadBinding{WorkloadId: w.Meta.ID.String(), InstanceUid: uuid.NewString(), BackendId: "backend"}}
	mock.ExpectQuery("SELECT .* FROM workloads WHERE id = \\$1").WithArgs(w.Meta.ID).WillReturnRows(workloadRows(t, w))
	req := preparedOperation("activate", nil)
	req.Id, req.ExpectedRevision = w.Meta.ID.String(), 3
	if _, err := s.UpdatePreparedWorkload(context.Background(), req); status.Code(err) != codes.Aborted {
		t.Fatalf("stale revision: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPreparedWorkloadCASLostAfterRead(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()
	s := New(Options{Pool: mock})
	w := defaultWorkloadRecord(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), time.Now())
	w.Status = workloadStatusStarting
	w.Preparation = &runnersv1.PreparedWorkloadLifecycle{Phase: preparationPhases["bound"], Revision: 4, BackendId: "backend", Binding: &runnerv1.WorkloadBinding{WorkloadId: w.Meta.ID.String(), InstanceUid: uuid.NewString(), BackendId: "backend"}}
	mock.ExpectQuery("SELECT .* FROM workloads WHERE id = \\$1").WithArgs(w.Meta.ID).WillReturnRows(workloadRows(t, w))
	mock.ExpectQuery("UPDATE workloads SET").WithArgs(w.Meta.ID, int64(4), "activating", pgxmock.AnyArg(), []byte(nil)).WillReturnRows(workloadRows(t))
	req := preparedOperation("activate", nil)
	req.Id, req.ExpectedRevision = w.Meta.ID.String(), 4
	if _, err := s.UpdatePreparedWorkload(context.Background(), req); status.Code(err) != codes.Aborted {
		t.Fatalf("lost CAS: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
