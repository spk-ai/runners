package rpcauth

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"sigs.k8s.io/yaml"
)

// The chart renders RUNNERS_RPC_POLICY from values.yaml (templates/_rpcauth.tpl),
// defaulting an empty caller namespace to the release namespace. Its defaults
// must be an enforcing, loadable policy that opens nothing tokenless.
func TestChartDefaultsAreAnEnforcingValidPolicy(t *testing.T) {
	data, err := os.ReadFile("../../charts/runners/values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var values struct {
		RPCAuth struct {
			Mode            string           `json:"mode"`
			Audience        string           `json:"audience"`
			CacheTTLSeconds int              `json:"cacheTTLSeconds"`
			Callers         []CallerPolicy   `json:"callers"`
			Tokenless       []TokenlessGrant `json:"tokenless"`
			TokenReviewRBAC struct {
				Create bool `json:"create"`
			} `json:"tokenReviewRBAC"`
		} `json:"rpcAuth"`
		AutomountServiceAccountToken bool `json:"automountServiceAccountToken"`
	}
	if err := yaml.Unmarshal(data, &values); err != nil {
		t.Fatal(err)
	}
	rpc := values.RPCAuth
	if rpc.Mode != string(ModeEnforce) || len(rpc.Tokenless) != 0 || !rpc.TokenReviewRBAC.Create || !values.AutomountServiceAccountToken {
		t.Fatalf("chart defaults must enforce, open nothing tokenless, bind TokenReview RBAC and mount the SA token: %+v", rpc)
	}
	for i := range rpc.Callers {
		if rpc.Callers[i].Namespace == "" {
			rpc.Callers[i].Namespace = "agyn-platform"
		}
	}
	document, err := json.Marshal(Policy{Version: 1, Audience: rpc.Audience, CacheTTLSeconds: rpc.CacheTTLSeconds, Callers: rpc.Callers})
	if err != nil {
		t.Fatal(err)
	}
	chart := mustPolicy(t, string(document))
	test := mustPolicy(t, testPolicy)
	if len(chart.grants) != len(test.grants) {
		t.Fatalf("chart callers %d, test policy %d", len(chart.grants), len(test.grants))
	}
	for principal, methods := range test.grants {
		if len(chart.grants[principal]) != len(methods) {
			t.Fatalf("chart grants for %s differ from the reviewed production grants", principal)
		}
		for method, r := range methods {
			if chart.grants[principal][method] != r {
				t.Fatalf("chart grant %s %s differs", principal, method)
			}
		}
	}
}

// The disposable-VM E2E enforces .github/e2e/rpcauth/policy.json. It must keep
// the production grants, differing only in a short cache TTL.
func TestE2EPolicyMatchesProductionGrants(t *testing.T) {
	data, err := os.ReadFile("../../.github/e2e/rpcauth/policy.json")
	if err != nil {
		t.Fatal(err)
	}
	e2e := mustPolicy(t, string(data))
	test := mustPolicy(t, testPolicy)
	if e2e.Audience != test.Audience || e2e.CacheTTL != 5*time.Second {
		t.Fatalf("e2e audience %q ttl %s", e2e.Audience, e2e.CacheTTL)
	}
	if !reflect.DeepEqual(e2e.grants, test.grants) || !reflect.DeepEqual(e2e.tokenless, test.tokenless) {
		t.Fatal("e2e grants differ from the reviewed production grants")
	}
}
