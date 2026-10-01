package server

import (
	"testing"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestLegacyRegistryCannotDecodeReopenedAnchorOperation(t *testing.T) {
	request := &runnersv1.UpdateVolumeCheckedRequest{Id: "fixture", ExpectedRevision: 3,
		Operation: &runnersv1.UpdateVolumeCheckedRequest_BindReopenedAnchor{BindReopenedAnchor: &runnersv1.BindVolumeResourceAnchor{}}}
	// Reconstruct the actual pre-extension wire descriptor. Older registries
	// must observe no known operation, which their entrypoint rejects before SQL.
	file := protodesc.ToFileDescriptorProto(request.ProtoReflect().Descriptor().ParentFile())
	for _, message := range file.MessageType {
		if message.GetName() == "UpdateVolumeCheckedRequest" {
			for i, field := range message.Field {
				if field.GetNumber() == 11 {
					message.Field = append(message.Field[:i], message.Field[i+1:]...)
					break
				}
			}
		}
	}
	legacyFile, err := protodesc.NewFile(file, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatal(err)
	}
	legacy := dynamicpb.NewMessage(legacyFile.Messages().ByName("UpdateVolumeCheckedRequest"))
	wire, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(wire, legacy); err != nil {
		t.Fatal(err)
	}
	if field := legacy.WhichOneof(legacy.Descriptor().Oneofs().ByName("operation")); field != nil {
		t.Fatalf("new operation downgraded to legacy field %s", field.Name())
	}
}
