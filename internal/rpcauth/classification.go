// Package rpcauth authorizes callers of the Runners control-plane gRPC API.
//
// Every registered method belongs to exactly one class. Classification is a
// code contract pinned to the API revision in buf.gen.yaml, because it depends
// on handler semantics: optional-identity handlers become unscoped reads when
// the caller sends no x-identity-id. Who may call each class is deployment
// configuration (see Policy), never hardcoded here.
//
// The RunnersService table is classification.json. It is byte-for-byte the
// same document as smartphonekey/infra packages/native-agyn-security/rpc-access.json,
// which renders the deployment policy; a parity test there guards drift.
package rpcauth

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Class names a group of methods with one authorization rule.
type Class string

const (
	// ClassOrchestrator holds runner-lifecycle and workload/volume mutations.
	// Only principals granted this class (the agents-orchestrator) may call it.
	ClassOrchestrator Class = "orchestrator"
	// ClassIdentity holds methods whose handlers scope results by x-identity-id
	// when it is present and serve unscoped results when it is absent.
	ClassIdentity Class = "identity"
	// ClassInternal holds platform-internal reads and keepalives.
	ClassInternal Class = "internal"
	// ClassRunnerToken holds runner-originated calls. The handler validates
	// the runner's service token from the request body; the caller token
	// additionally proves the call came through an approved forwarder.
	ClassRunnerToken Class = "runnerToken"
	// ClassDenied is never reachable. Nothing can grant it.
	ClassDenied Class = "denied"
	// ClassHealth needs no caller token. It is the only unauthenticated class.
	ClassHealth Class = "health"
)

const (
	// RunnersServiceName is the fully qualified Runners gRPC service.
	RunnersServiceName = "agynio.api.runners.v1.RunnersService"
	// HealthServiceName is the standard gRPC health service registered by main.
	HealthServiceName = "grpc.health.v1.Health"
)

//go:embed classification.json
var classificationJSON []byte

// grantableClasses may appear in a Policy grant.
var grantableClasses = []Class{ClassOrchestrator, ClassIdentity, ClassInternal, ClassRunnerToken}

// healthMethods classifies every method grpc-go's health server registers.
// Check and Watch are status probes and carry no data, so they need no token;
// the NetworkPolicy in front of runners still applies. List enumerates
// registered services and nothing needs it, so it is denied. A health method
// missing from this table fails startup like any other unclassified method.
var healthMethods = map[string]Class{
	"Check": ClassHealth,
	"Watch": ClassHealth,
	"List":  ClassDenied,
}

// Server reflection (grpc.reflection.*) is deliberately absent: runners does
// not register it, and if it ever were registered the startup coverage check
// would refuse to serve because its methods are unclassified.

// tokenlessEligible lists the only methods a deployment may open to callers
// that present no caller token (Policy.Tokenless). They are workload reads the
// unforked upstream callers need: chat (ListWorkloadsByThread,
// ListWorkloadsByAgentInstance with identity), expose (GetWorkload with
// identity) and notifications (GetWorkload without identity). Every
// runner-lifecycle method and every mutation always requires a validated caller
// token; the policy loader rejects any other tokenless method.
var tokenlessEligible = map[string]bool{
	"GetWorkload":                  true,
	"ListWorkloadsByThread":        true,
	"ListWorkloadsByAgentInstance": true,
}

// Classification maps fully qualified gRPC method names to classes.
type Classification struct {
	APIRevision string
	methods     map[string]Class
	byClass     map[Class][]string
}

type classificationDocument struct {
	APIRevision  string   `json:"apiRevision"`
	Orchestrator []string `json:"orchestrator"`
	Identity     []string `json:"identity"`
	Internal     []string `json:"internal"`
	RunnerToken  []string `json:"runnerToken"`
	Denied       []string `json:"denied"`
}

// DefaultClassification returns the embedded, API-pinned classification.
func DefaultClassification() (*Classification, error) {
	return parseClassification(classificationJSON)
}

func parseClassification(data []byte) (*Classification, error) {
	var document classificationDocument
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode classification: %w", err)
	}
	if strings.TrimSpace(document.APIRevision) == "" {
		return nil, fmt.Errorf("classification apiRevision is required")
	}
	c := &Classification{
		APIRevision: document.APIRevision,
		methods:     map[string]Class{},
		byClass:     map[Class][]string{},
	}
	add := func(service string, class Class, names []string) error {
		for _, name := range names {
			if name == "" || strings.Contains(name, "/") {
				return fmt.Errorf("invalid method name %q", name)
			}
			full := FullMethod(service, name)
			if existing, ok := c.methods[full]; ok {
				return fmt.Errorf("method %s classified as both %s and %s", full, existing, class)
			}
			c.methods[full] = class
			if service == RunnersServiceName {
				c.byClass[class] = append(c.byClass[class], name)
			}
		}
		return nil
	}
	for _, entry := range []struct {
		class Class
		names []string
	}{
		{ClassOrchestrator, document.Orchestrator},
		{ClassIdentity, document.Identity},
		{ClassInternal, document.Internal},
		{ClassRunnerToken, document.RunnerToken},
		{ClassDenied, document.Denied},
	} {
		if len(entry.names) == 0 {
			return nil, fmt.Errorf("classification class %s is empty", entry.class)
		}
		if err := add(RunnersServiceName, entry.class, entry.names); err != nil {
			return nil, err
		}
	}
	healthNames := make([]string, 0, len(healthMethods))
	for name := range healthMethods {
		healthNames = append(healthNames, name)
	}
	sort.Strings(healthNames)
	for _, name := range healthNames {
		if err := add(HealthServiceName, healthMethods[name], []string{name}); err != nil {
			return nil, err
		}
	}
	for name := range tokenlessEligible {
		class := c.methods[FullMethod(RunnersServiceName, name)]
		if class != ClassIdentity && class != ClassInternal {
			return nil, fmt.Errorf("tokenless-eligible method %s must be an identity or internal read, got %q", name, class)
		}
	}
	return c, nil
}

// FullMethod joins a service and method into the gRPC FullMethod form.
func FullMethod(service, method string) string {
	return "/" + service + "/" + method
}

// Lookup returns the class of a fully qualified method.
func (c *Classification) Lookup(fullMethod string) (Class, bool) {
	class, ok := c.methods[fullMethod]
	return class, ok
}

// Methods returns the RunnersService method names in a class.
func (c *Classification) Methods(class Class) []string {
	return append([]string(nil), c.byClass[class]...)
}

// ServiceMethods describes the methods a gRPC server registered for one service.
type ServiceMethods struct {
	Service string
	Methods []string
}

// VerifyCoverage fails if any registered method is unclassified. It is run
// against grpc.Server.GetServiceInfo before the listener opens, so a new API
// RPC (RunnersService embeds UnimplementedRunnersServiceServer, so every API
// method is registered) or a newly registered service such as reflection can
// never become reachable without review.
func (c *Classification) VerifyCoverage(services []ServiceMethods) error {
	var missing []string
	for _, service := range services {
		for _, method := range service.Methods {
			if _, ok := c.methods[FullMethod(service.Service, method)]; !ok {
				missing = append(missing, FullMethod(service.Service, method))
			}
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("rpcauth: registered methods are not classified: %s", strings.Join(missing, ", "))
	}
	return nil
}
