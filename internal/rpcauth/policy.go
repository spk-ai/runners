package rpcauth

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"
)

// IdentityRule states whether a grant needs a forwarded end-user identity.
type IdentityRule string

const (
	// IdentityRequired needs exactly one non-nil UUID x-identity-id, so the
	// handler scopes the call to that identity.
	IdentityRequired IdentityRule = "required"
	// IdentityOptional lets the caller omit x-identity-id. Identity and internal
	// handlers then serve unscoped (privileged) results.
	IdentityOptional IdentityRule = "optional"
)

const (
	policyVersion = 1
	// DefaultCacheTTL bounds how long a successful TokenReview is reused.
	DefaultCacheTTL = 30 * time.Second
	// MaxCacheTTL is the longest a revoked token (deleted pod) stays accepted.
	MaxCacheTTL    = 60 * time.Second
	maxPolicyBytes = 64 * 1024
)

// Policy is the deployment-supplied grant table. It is decoded strictly: an
// unknown field, a denied-class grant, a method outside its class or a
// duplicate (principal, method) is a startup error, never a silent widening.
//
//	{
//	  "version": 1,
//	  "audience": "agyn-runners",
//	  "cacheTTLSeconds": 30,
//	  "callers": [
//	    {"namespace": "agyn-platform", "serviceAccount": "agents-orchestrator",
//	     "grants": [{"class": "orchestrator"},
//	                {"class": "identity", "identity": "optional", "methods": ["GetWorkload"]}]}
//	  ],
//	  "tokenless": [{"method": "ListWorkloadsByThread", "identity": "required"}]
//	}
type Policy struct {
	Version         int              `json:"version"`
	Audience        string           `json:"audience"`
	CacheTTLSeconds int              `json:"cacheTTLSeconds,omitempty"`
	Callers         []CallerPolicy   `json:"callers"`
	Tokenless       []TokenlessGrant `json:"tokenless,omitempty"`
}

// CallerPolicy grants classes to one ServiceAccount principal.
type CallerPolicy struct {
	Namespace      string  `json:"namespace"`
	ServiceAccount string  `json:"serviceAccount"`
	Grants         []Grant `json:"grants"`
}

// Grant allows a class, or the listed subset of its methods.
type Grant struct {
	Class    Class        `json:"class"`
	Identity IdentityRule `json:"identity,omitempty"`
	Methods  []string     `json:"methods,omitempty"`
}

// TokenlessGrant opens one read to callers that present no caller token.
//
// Residual risk: a tokenless grant authenticates nobody. Any pod that can reach
// runners on its gRPC port (the NetworkPolicy decides which) can make this call,
// and x-identity-id is whatever that pod claims. With identity "required" the
// handler scopes results to the claimed identity, so a forger reads what that
// identity can read; with "optional" an identity-less GetWorkload returns any
// workload's metadata. Keep the list empty unless an unforked caller needs it.
type TokenlessGrant struct {
	Method   string       `json:"method"`
	Identity IdentityRule `json:"identity"`
}

// rule is the compiled authorization for one (principal or tokenless, method).
type rule struct {
	class    Class
	identity IdentityRule
}

// CompiledPolicy is a validated Policy indexed for lookups.
type CompiledPolicy struct {
	Audience  string
	CacheTTL  time.Duration
	grants    map[string]map[string]rule // principal key -> full method -> rule
	tokenless map[string]rule            // full method -> rule
}

// ParsePolicy decodes and validates a policy document against a classification.
func ParsePolicy(data []byte, classification *Classification) (*CompiledPolicy, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("rpc policy is empty")
	}
	if len(data) > maxPolicyBytes {
		return nil, fmt.Errorf("rpc policy exceeds %d bytes", maxPolicyBytes)
	}
	var policy Policy
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&policy); err != nil {
		return nil, fmt.Errorf("decode rpc policy: %w", err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("decode rpc policy: trailing data")
	}
	return compilePolicy(policy, classification)
}

func compilePolicy(policy Policy, classification *Classification) (*CompiledPolicy, error) {
	if policy.Version != policyVersion {
		return nil, fmt.Errorf("rpc policy version must be %d", policyVersion)
	}
	audience := policy.Audience
	if audience == "" || strings.TrimSpace(audience) != audience || len(audience) > 253 {
		return nil, fmt.Errorf("rpc policy audience must be a non-empty trimmed string of at most 253 characters")
	}
	// The API server's own audiences would let runners accept (and replay)
	// ordinary API tokens. Runners needs a dedicated audience.
	lowered := strings.ToLower(audience)
	if strings.Contains(lowered, "kubernetes") || strings.Contains(lowered, "k3s") {
		return nil, fmt.Errorf("rpc policy audience %q must be dedicated to runners, not an API server audience", audience)
	}
	compiled := &CompiledPolicy{
		Audience:  audience,
		CacheTTL:  DefaultCacheTTL,
		grants:    map[string]map[string]rule{},
		tokenless: map[string]rule{},
	}
	if policy.CacheTTLSeconds != 0 {
		ttl := time.Duration(policy.CacheTTLSeconds) * time.Second
		if ttl < time.Second || ttl > MaxCacheTTL {
			return nil, fmt.Errorf("rpc policy cacheTTLSeconds must be between 1 and %d", int(MaxCacheTTL/time.Second))
		}
		compiled.CacheTTL = ttl
	}
	if len(policy.Callers) == 0 && len(policy.Tokenless) == 0 {
		return nil, fmt.Errorf("rpc policy grants nothing")
	}
	for _, caller := range policy.Callers {
		if errs := validation.IsDNS1123Label(caller.Namespace); len(errs) > 0 {
			return nil, fmt.Errorf("rpc policy caller namespace %q: %s", caller.Namespace, strings.Join(errs, "; "))
		}
		if errs := validation.IsDNS1123Subdomain(caller.ServiceAccount); len(errs) > 0 {
			return nil, fmt.Errorf("rpc policy caller serviceAccount %q: %s", caller.ServiceAccount, strings.Join(errs, "; "))
		}
		key := principalKey(caller.Namespace, caller.ServiceAccount)
		if _, exists := compiled.grants[key]; exists {
			return nil, fmt.Errorf("rpc policy caller %s is listed twice", key)
		}
		if len(caller.Grants) == 0 {
			return nil, fmt.Errorf("rpc policy caller %s has no grants", key)
		}
		methods := map[string]rule{}
		for _, grant := range caller.Grants {
			if err := validateGrant(grant); err != nil {
				return nil, fmt.Errorf("rpc policy caller %s: %w", key, err)
			}
			names, err := grantMethods(grant, classification)
			if err != nil {
				return nil, fmt.Errorf("rpc policy caller %s: %w", key, err)
			}
			for _, name := range names {
				full := FullMethod(RunnersServiceName, name)
				if _, exists := methods[full]; exists {
					return nil, fmt.Errorf("rpc policy caller %s grants %s twice", key, name)
				}
				methods[full] = rule{class: grant.Class, identity: grant.Identity}
			}
		}
		compiled.grants[key] = methods
	}
	for _, grant := range policy.Tokenless {
		if !tokenlessEligible[grant.Method] {
			return nil, fmt.Errorf("rpc policy tokenless method %q is not an eligible workload read", grant.Method)
		}
		if grant.Identity != IdentityRequired && grant.Identity != IdentityOptional {
			return nil, fmt.Errorf("rpc policy tokenless method %s: identity must be %q or %q", grant.Method, IdentityRequired, IdentityOptional)
		}
		full := FullMethod(RunnersServiceName, grant.Method)
		if _, exists := compiled.tokenless[full]; exists {
			return nil, fmt.Errorf("rpc policy tokenless method %s is listed twice", grant.Method)
		}
		class, _ := classification.Lookup(full)
		compiled.tokenless[full] = rule{class: class, identity: grant.Identity}
	}
	return compiled, nil
}

func validateGrant(grant Grant) error {
	switch grant.Class {
	case ClassIdentity, ClassInternal:
		if grant.Identity != IdentityRequired && grant.Identity != IdentityOptional {
			return fmt.Errorf("class %s grant needs identity %q or %q", grant.Class, IdentityRequired, IdentityOptional)
		}
	case ClassOrchestrator, ClassRunnerToken:
		if grant.Identity != "" {
			return fmt.Errorf("class %s grant must not set identity", grant.Class)
		}
	case ClassDenied, ClassHealth:
		return fmt.Errorf("class %s cannot be granted", grant.Class)
	default:
		return fmt.Errorf("unknown class %q", grant.Class)
	}
	return nil
}

func grantMethods(grant Grant, classification *Classification) ([]string, error) {
	all := classification.Methods(grant.Class)
	if grant.Methods == nil {
		return all, nil
	}
	if len(grant.Methods) == 0 {
		return nil, fmt.Errorf("class %s grant lists an empty methods array; omit it to grant the whole class", grant.Class)
	}
	inClass := map[string]bool{}
	for _, name := range all {
		inClass[name] = true
	}
	for _, name := range grant.Methods {
		if !inClass[name] {
			return nil, fmt.Errorf("method %q is not in class %s", name, grant.Class)
		}
	}
	return grant.Methods, nil
}

func principalKey(namespace, serviceAccount string) string {
	return namespace + "/" + serviceAccount
}

func (p *CompiledPolicy) lookup(principal Principal, fullMethod string) (rule, bool) {
	methods, ok := p.grants[principalKey(principal.Namespace, principal.ServiceAccount)]
	if !ok {
		return rule{}, false
	}
	r, ok := methods[fullMethod]
	return r, ok
}

func (p *CompiledPolicy) lookupTokenless(fullMethod string) (rule, bool) {
	r, ok := p.tokenless[fullMethod]
	return r, ok
}
