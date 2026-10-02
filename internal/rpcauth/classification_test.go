package rpcauth

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestClassificationCoversRunnersServiceExactly(t *testing.T) {
	c := mustClassification(t)
	desc := runnersv1.RunnersService_ServiceDesc
	if desc.ServiceName != RunnersServiceName {
		t.Fatalf("service name %q, want %q", desc.ServiceName, RunnersServiceName)
	}
	var registered []string
	for _, method := range desc.Methods {
		registered = append(registered, method.MethodName)
	}
	for _, stream := range desc.Streams {
		registered = append(registered, stream.StreamName)
	}
	classified := 0
	for _, class := range append(grantableClasses, ClassDenied) {
		classified += len(c.Methods(class))
	}
	if classified != len(registered) {
		t.Fatalf("classified %d RunnersService methods, the API registers %d", classified, len(registered))
	}
	for _, name := range registered {
		if _, ok := c.Lookup(FullMethod(RunnersServiceName, name)); !ok {
			t.Errorf("RunnersService/%s is not classified", name)
		}
	}
	if class, _ := c.Lookup(runnersv1.RunnersService_StreamWorkloadLogs_FullMethodName); class != ClassIdentity {
		t.Errorf("StreamWorkloadLogs class %q, want identity", class)
	}
}

func TestClassificationHealthMethods(t *testing.T) {
	c := mustClassification(t)
	for _, method := range healthpb.Health_ServiceDesc.Methods {
		if _, ok := c.Lookup(FullMethod(HealthServiceName, method.MethodName)); !ok {
			t.Errorf("health %s unclassified", method.MethodName)
		}
	}
	for _, stream := range healthpb.Health_ServiceDesc.Streams {
		if _, ok := c.Lookup(FullMethod(HealthServiceName, stream.StreamName)); !ok {
			t.Errorf("health %s unclassified", stream.StreamName)
		}
	}
	for method, want := range map[string]Class{
		healthpb.Health_Check_FullMethodName: ClassHealth,
		healthpb.Health_Watch_FullMethodName: ClassHealth,
		healthpb.Health_List_FullMethodName:  ClassDenied,
	} {
		if got, _ := c.Lookup(method); got != want {
			t.Errorf("%s class %q, want %q", method, got, want)
		}
	}
}

func TestClassificationIsPinnedToGeneratedAPI(t *testing.T) {
	c := mustClassification(t)
	data, err := os.ReadFile("../../buf.gen.yaml")
	if err != nil {
		t.Fatalf("read buf.gen.yaml: %v", err)
	}
	match := regexp.MustCompile(`ref:\s*([0-9a-f]{40})`).FindSubmatch(data)
	if match == nil {
		t.Fatal("buf.gen.yaml has no pinned API ref")
	}
	if string(match[1]) != c.APIRevision {
		t.Fatalf("classification apiRevision %s, buf.gen.yaml ref %s: reclassify after an API bump", c.APIRevision, match[1])
	}
}

func TestVerifyCoverageRejectsUnclassifiedMethods(t *testing.T) {
	c := mustClassification(t)
	if err := c.VerifyCoverage([]ServiceMethods{
		{Service: RunnersServiceName, Methods: []string{"GetWorkload", "StreamWorkloadLogs"}},
		{Service: HealthServiceName, Methods: []string{"Check", "Watch", "List"}},
	}); err != nil {
		t.Fatalf("classified methods rejected: %v", err)
	}
	err := c.VerifyCoverage([]ServiceMethods{
		{Service: RunnersServiceName, Methods: []string{"GetWorkload", "FutureMutation"}},
		{Service: "grpc.reflection.v1.ServerReflection", Methods: []string{"ServerReflectionInfo"}},
	})
	if err == nil {
		t.Fatal("unclassified methods accepted")
	}
	for _, want := range []string{"/agynio.api.runners.v1.RunnersService/FutureMutation", "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s", err, want)
		}
	}
}

func TestParseClassificationRejectsOverlap(t *testing.T) {
	_, err := parseClassification([]byte(`{"apiRevision":"x","orchestrator":["A"],"identity":["A"],"internal":["B"],"runnerToken":["C"],"denied":["D"]}`))
	if err == nil {
		t.Fatal("a method in two classes was accepted")
	}
	_, err = parseClassification([]byte(`{"apiRevision":"x","orchestrator":["A"],"identity":["B"],"internal":["C"],"runnerToken":["D"],"denied":["E"],"extra":[]}`))
	if err == nil {
		t.Fatal("an unknown class was accepted")
	}
}

func TestTokenlessEligibleMethodsAreWorkloadReads(t *testing.T) {
	c := mustClassification(t)
	var eligible []string
	for name := range tokenlessEligible {
		eligible = append(eligible, name)
		class, _ := c.Lookup(FullMethod(RunnersServiceName, name))
		if class != ClassIdentity && class != ClassInternal {
			t.Errorf("%s is %s", name, class)
		}
	}
	slices.Sort(eligible)
	want := []string{"GetWorkload", "ListWorkloadsByAgentInstance", "ListWorkloadsByThread"}
	if !slices.Equal(eligible, want) {
		t.Fatalf("tokenless-eligible %v, want %v", eligible, want)
	}
}
