package server

import (
	"fmt"
	"maps"
	"testing"

	runnerv1 "github.com/agynio/runners/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

func registryRevocationFixture(t *testing.T, sandbox bool, count int) (workloadRecord, *runnerv1.PreparationRevocation, *runnerv1.ObservePreparationRevocationResponse) {
	t.Helper()
	w := registryTestAnchoredWorkload(sandbox, count)
	w.Preparation.Phase, w.Preparation.Revision, w.Preparation.Resources.Revision = preparationPhases["removing"], 3, 4
	p := w.Preparation
	receipt, err := canonicalRegistryPreparationRevocation(w, &runnerv1.PreparationRevocation{
		WorkloadAnchor: p.Resources.Workload, VolumeAnchors: p.Resources.Volumes, InstanceUid: uuid.NewString(),
	})
	if err != nil {
		t.Fatal(err)
	}
	observation := &runnerv1.ObservePreparationRevocationResponse{Revocation: receipt, State: runnerv1.RevokedPreparationState_REVOKED_PREPARATION_STATE_POD_ABSENT}
	for i, a := range receipt.VolumeAnchors {
		if i%2 == 0 {
			observation.Volumes = append(observation.Volumes, &runnerv1.VolumeListItem{InstanceId: "pvc-" + a.ResourceId,
				InstanceUid: uuid.NewString(), VolumeKey: a.ResourceId, BackendId: a.BackendId, Anchor: a, IdentityLabels: maps.Clone(a.IdentityLabels)})
		} else {
			observation.AbsentVolumeIds = append(observation.AbsentVolumeIds, a.ResourceId)
		}
	}
	return w, receipt, observation
}

func TestRegistryPreparationRevocationTransitions(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, count := range []int{0, 2} {
			t.Run(fmt.Sprintf("sandbox=%t/volumes=%d", sandbox, count), func(t *testing.T) {
				w, receipt, observation := registryRevocationFixture(t, sandbox, count)
				record := &runnersv1.UpdatePreparedWorkloadRequest{Operation: &runnersv1.UpdatePreparedWorkloadRequest_RecordRevocation{RecordRevocation: &runnersv1.RecordPreparationRevocation{Revocation: receipt}}}
				confirm := &runnersv1.UpdatePreparedWorkloadRequest{Operation: &runnersv1.UpdatePreparedWorkloadRequest_ConfirmRevocation{ConfirmRevocation: &runnersv1.ConfirmPreparationRevocation{Observation: observation}}}
				before := proto.Clone(w.Preparation)
				next := proto.Clone(w.Preparation).(*runnersv1.PreparedWorkloadLifecycle)
				if err := applyPreparedWorkloadOperation(w, next, confirm); err == nil || !proto.Equal(next, before) {
					t.Fatal("cleanup bypassed the durable revocation step")
				}
				if err := applyPreparedWorkloadOperation(w, next, record); err != nil {
					t.Fatal(err)
				}
				if next.Phase != preparationPhases["removing"] || next.Binding != nil || next.Resources.RevocationObservation != nil || !proto.Equal(w.Preparation, before) {
					t.Fatal("recording revocation fabricated binding or cleanup")
				}
				w.Preparation = next
				if err := validateWorkloadResourceAnchors(w, next.Resources); err != nil {
					t.Fatal(err)
				}
				for _, invalid := range []*runnersv1.UpdatePreparedWorkloadRequest{record, preparedOperation("bind", &runnerv1.WorkloadBinding{}), preparedOperation("activate", nil), preparedOperation("abort", nil)} {
					if err := applyPreparedWorkloadOperation(w, proto.Clone(next).(*runnersv1.PreparedWorkloadLifecycle), invalid); err == nil {
						t.Fatal("revoked preparation regained authority")
					}
				}
				next = proto.Clone(next).(*runnersv1.PreparedWorkloadLifecycle)
				if err := applyPreparedWorkloadOperation(w, next, confirm); err != nil {
					t.Fatal(err)
				}
				w.Preparation = next
				if err := validateWorkloadResourceAnchors(w, next.Resources); err != nil || next.Phase != preparationPhases["removed"] || next.Binding != nil || next.RemovalObservation != nil {
					t.Fatalf("revocation cleanup was confused with bound Pod removal: %v", err)
				}
			})
		}
	}

}

func TestRegistryPreparationRevocationRejectsIdentityChanges(t *testing.T) {
	for _, change := range []string{"uid", "workload", "volume", "duplicate", "pod", "unknown"} {
		t.Run(change, func(t *testing.T) {
			w, receipt, _ := registryRevocationFixture(t, false, 2)
			switch change {
			case "uid":
				receipt.InstanceUid = uuid.Nil.String()
			case "workload":
				receipt.WorkloadAnchor.InstanceUid = uuid.NewString()
			case "volume":
				receipt.VolumeAnchors = receipt.VolumeAnchors[:1]
			case "duplicate":
				receipt.VolumeAnchors[1] = receipt.VolumeAnchors[0]
			case "pod":
				receipt.SelectedPodUid = "not-an-incarnation"
			case "unknown":
				receipt.ProtoReflect().SetUnknown([]byte{0x78, 1})
			}
			if _, err := canonicalRegistryPreparationRevocation(w, receipt); err == nil {
				t.Fatal("unverified revocation identity accepted")
			}
		})
	}
}

func TestRegistryPreparationRevocationRejectsIncompleteObservation(t *testing.T) {
	for _, change := range []string{"pending", "proof", "uid", "owner", "partition", "missing", "foreign", "unknown", "volume-unknown"} {
		t.Run(change, func(t *testing.T) {
			w, receipt, observation := registryRevocationFixture(t, true, 2)
			observation = proto.Clone(observation).(*runnerv1.ObservePreparationRevocationResponse)
			switch change {
			case "pending":
				observation.State = runnerv1.RevokedPreparationState_REVOKED_PREPARATION_STATE_PENDING
			case "proof":
				observation.Revocation.InstanceUid = uuid.NewString()
			case "uid":
				observation.Volumes[0].InstanceUid = ""
			case "owner":
				observation.Volumes[0].Anchor.InstanceUid = uuid.NewString()
			case "partition":
				observation.AbsentVolumeIds[0] = observation.Volumes[0].VolumeKey
			case "missing":
				observation.AbsentVolumeIds = nil
			case "foreign":
				observation.AbsentVolumeIds[0] = uuid.NewString()
			case "unknown":
				observation.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "volume-unknown":
				observation.Volumes[0].ProtoReflect().SetUnknown([]byte{0x78, 1})
			}
			if _, err := canonicalRegistryRevocationObservation(w, receipt, observation); err == nil {
				t.Fatal("unverified observation accepted")
			}
		})
	}
}

func TestRegistryRevocationVolumeIdentity(t *testing.T) {
	w, receipt, observation := registryRevocationFixture(t, false, 1)
	a := receipt.VolumeAnchors[0]
	v := volumeRecord{Meta: entityMeta{ID: uuid.MustParse(a.ResourceId)}, OwnerKind: w.OwnerKind, OwnerID: w.OwnerID, OrganizationID: w.OrganizationID,
		RunnerID: w.RunnerID, ThreadID: w.ThreadID, AgentID: w.AgentID, SizeGB: "1", Status: volumeStatusProvisioning,
		CheckedLifecycle: true, LifecycleRevision: 2, ResourceAnchor: a,
		AnchorReservation: &runnersv1.VolumeAnchorReservation{WorkloadId: w.Meta.ID.String(), PreparationRevision: 1, ResourceRevision: 1}}
	if err := validateRevocationVolumeRecord(w, a, nil, v); err != nil {
		t.Fatalf("original absent first allocation: %v", err)
	}
	reopened := v
	reopened.LifecycleRevision = 4
	reopened.AnchorReservation = proto.Clone(v.AnchorReservation).(*runnersv1.VolumeAnchorReservation)
	reopened.AnchorReservation.AllocationRevision = 4
	if err := validateRevocationVolumeRecord(w, a, nil, reopened); err != nil {
		t.Fatalf("reopened original allocation: %v", err)
	}
	reopened.LifecycleRevision++
	if err := validateRevocationVolumeRecord(w, a, nil, reopened); err == nil {
		t.Fatal("later revision accepted as original allocation")
	}
	for _, change := range []string{"revision", "reservation", "owner", "removal"} {
		changed := v
		switch change {
		case "revision":
			changed.LifecycleRevision++
		case "reservation":
			changed.AnchorReservation = nil
		case "owner":
			changed.OwnerID = uuid.New()
		case "removal":
			changed.RemovalIntent = &runnersv1.VolumeRemovalIntent{}
		}
		if err := validateRevocationVolumeRecord(w, a, nil, changed); err == nil {
			t.Fatalf("changed unbound allocation accepted: %s", change)
		}
	}
	found := observation.Volumes[0]
	if err := validateRevocationVolumeRecord(w, a, found, v); err == nil {
		t.Fatal("unpersisted discovered PVC allowed confirmation")
	}
	v.Status, v.LifecycleRevision, v.BoundInstance, v.InstanceID = volumeStatusActive, 3, found, &found.InstanceId
	if err := validateRevocationVolumeRecord(w, a, found, v); err != nil {
		t.Fatal(err)
	}
	if err := validateRevocationVolumeRecord(w, a, nil, v); err == nil {
		t.Fatal("missing known workspace accepted")
	}
	changed := proto.Clone(found).(*runnerv1.VolumeListItem)
	changed.InstanceUid = uuid.NewString()
	if err := validateRevocationVolumeRecord(w, a, changed, v); err == nil {
		t.Fatal("replacement workspace accepted")
	}
}
