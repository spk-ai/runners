package server

import (
	"strings"
	"testing"

	runnerv1 "github.com/agynio/runners/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

const lifecycleTestBackend = "kubernetes-namespace/v1/workloads/namespace-original"

func TestVolumeBackendBindingAndConfirmation(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		record := volumeRecord{Meta: entityMeta{ID: uuid.New()}, VolumeID: uuid.New(), ThreadID: uuid.New(), AgentID: uuid.New(), OwnerID: uuid.New(),
			OwnerKind: runtimeOwnerKindAgentInstance, Status: volumeStatusProvisioning, SizeGB: "1", CheckedLifecycle: true, LifecycleRevision: 1}
		if sandbox {
			record.OwnerKind = runtimeOwnerKindSandbox
		}
		v, err := toProtoVolume(record)
		if err != nil {
			t.Fatal(err)
		}
		instance := lifecycleTestInstance(v)
		for _, backend := range []string{"", " padded ", strings.Repeat("x", 513), lifecycleTestBackend} {
			instance.BackendId = backend
			if err := validateVolumeBinding(record, instance); (err == nil) != (backend == lifecycleTestBackend) {
				t.Errorf("binding backend=%q: %v", backend, err)
			}
		}
		bind := &runnersv1.UpdateVolumeCheckedRequest{Operation: &runnersv1.UpdateVolumeCheckedRequest_Bind{Bind: &runnersv1.BindVolumeInstance{Instance: instance}}}
		if err := applyVolumeOperation(&record, bind); err != nil {
			t.Fatal(err)
		}
		if err := applyVolumeOperation(&record, &runnersv1.UpdateVolumeCheckedRequest{Operation: &runnersv1.UpdateVolumeCheckedRequest_BeginRemoval{BeginRemoval: &runnersv1.BeginVolumeRemoval{}}}); err != nil {
			t.Fatal(err)
		}
		for _, backend := range []string{"", "other-backend", lifecycleTestBackend} {
			copy := record
			copy.BoundInstance = proto.Clone(record.BoundInstance).(*runnerv1.VolumeListItem)
			copy.RemovalIntent = proto.Clone(record.RemovalIntent).(*runnersv1.VolumeRemovalIntent)
			err := applyVolumeOperation(&copy, &runnersv1.UpdateVolumeCheckedRequest{Operation: &runnersv1.UpdateVolumeCheckedRequest_ConfirmRemoval{
				ConfirmRemoval: &runnersv1.ConfirmVolumeRemoval{IntentId: record.RemovalIntent.Id, BackendId: backend}}})
			if backend != lifecycleTestBackend {
				if err == nil || copy.Status != record.Status || !proto.Equal(copy.RemovalIntent, record.RemovalIntent) {
					t.Errorf("mismatched confirmation changed state: backend=%q err=%v", backend, err)
				}
			} else if err != nil || copy.Status != volumeStatusDeleted || copy.RemovalIntent.ConfirmedAt == nil {
				t.Fatalf("matching confirmation failed: %v", err)
			}
		}
	}
}
