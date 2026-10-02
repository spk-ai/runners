package rpcauth

import (
	"strings"
	"testing"
	"time"
)

func TestParsePolicyAcceptsProductionShape(t *testing.T) {
	p := mustPolicy(t, testPolicy)
	if p.Audience != testAudience || p.CacheTTL != 30*time.Second {
		t.Fatalf("audience %q ttl %s", p.Audience, p.CacheTTL)
	}
	orchestrator := Principal{Namespace: testNamespace, ServiceAccount: "agents-orchestrator"}
	gateway := Principal{Namespace: testNamespace, ServiceAccount: "gateway"}
	for _, tc := range []struct {
		principal Principal
		method    string
		allowed   bool
		identity  IdentityRule
	}{
		{orchestrator, "CreateWorkload", true, ""},
		{orchestrator, "GetWorkload", true, IdentityOptional},
		{orchestrator, "ListWorkloads", true, IdentityOptional},
		{orchestrator, "RegisterRunner", false, ""},
		{orchestrator, "DeleteRunner", false, ""},
		{orchestrator, "TouchWorkload", false, ""},
		{orchestrator, "EnrollRunner", false, ""},
		{gateway, "RegisterRunner", true, IdentityRequired},
		{gateway, "StreamWorkloadLogs", true, IdentityRequired},
		{gateway, "TouchWorkload", true, IdentityRequired},
		{gateway, "EnrollRunner", true, ""},
		{gateway, "CreateWorkload", false, ""},
		{gateway, "ValidateServiceToken", false, ""},
		{Principal{Namespace: "agyn-workloads", ServiceAccount: "gateway"}, "GetWorkload", false, ""},
	} {
		r, ok := p.lookup(tc.principal, FullMethod(RunnersServiceName, tc.method))
		if ok != tc.allowed || (ok && r.identity != tc.identity) {
			t.Errorf("%s %s: allowed=%v identity=%q, want %v %q", tc.principal, tc.method, ok, r.identity, tc.allowed, tc.identity)
		}
	}
	if r, ok := p.lookupTokenless(FullMethod(RunnersServiceName, "ListWorkloadsByThread")); !ok || r.identity != IdentityRequired {
		t.Fatal("tokenless ListWorkloadsByThread not compiled")
	}
	if _, ok := p.lookupTokenless(FullMethod(RunnersServiceName, "GetWorkload")); ok {
		t.Fatal("unlisted tokenless method compiled")
	}
}

func TestParsePolicyDefaults(t *testing.T) {
	p := mustPolicy(t, `{"version":1,"audience":"agyn-runners","callers":[{"namespace":"ns","serviceAccount":"sa","grants":[{"class":"orchestrator"}]}]}`)
	if p.CacheTTL != DefaultCacheTTL || len(p.tokenless) != 0 {
		t.Fatalf("defaults: ttl %s tokenless %d", p.CacheTTL, len(p.tokenless))
	}
}

func TestParsePolicyRejections(t *testing.T) {
	caller := func(grants string) string {
		return `{"version":1,"audience":"agyn-runners","callers":[{"namespace":"agyn-platform","serviceAccount":"gateway","grants":[` + grants + `]}]}`
	}
	for name, document := range map[string]string{
		"empty":                   ``,
		"unknown field":           `{"version":1,"audience":"agyn-runners","callers":[],"extra":true}`,
		"trailing data":           caller(`{"class":"orchestrator"}`) + `{}`,
		"wrong version":           `{"version":2,"audience":"agyn-runners","callers":[{"namespace":"a","serviceAccount":"b","grants":[{"class":"orchestrator"}]}]}`,
		"empty audience":          `{"version":1,"audience":"","callers":[{"namespace":"a","serviceAccount":"b","grants":[{"class":"orchestrator"}]}]}`,
		"api server audience":     `{"version":1,"audience":"https://kubernetes.default.svc.cluster.local","callers":[{"namespace":"a","serviceAccount":"b","grants":[{"class":"orchestrator"}]}]}`,
		"k3s audience":            `{"version":1,"audience":"k3s","callers":[{"namespace":"a","serviceAccount":"b","grants":[{"class":"orchestrator"}]}]}`,
		"grants nothing":          `{"version":1,"audience":"agyn-runners","callers":[]}`,
		"ttl above 60s":           `{"version":1,"audience":"agyn-runners","cacheTTLSeconds":61,"callers":[{"namespace":"a","serviceAccount":"b","grants":[{"class":"orchestrator"}]}]}`,
		"negative ttl":            `{"version":1,"audience":"agyn-runners","cacheTTLSeconds":-1,"callers":[{"namespace":"a","serviceAccount":"b","grants":[{"class":"orchestrator"}]}]}`,
		"wildcard namespace":      `{"version":1,"audience":"agyn-runners","callers":[{"namespace":"*","serviceAccount":"b","grants":[{"class":"orchestrator"}]}]}`,
		"invalid service account": `{"version":1,"audience":"agyn-runners","callers":[{"namespace":"a","serviceAccount":"B:C","grants":[{"class":"orchestrator"}]}]}`,
		"duplicate caller":        `{"version":1,"audience":"agyn-runners","callers":[{"namespace":"a","serviceAccount":"b","grants":[{"class":"orchestrator"}]},{"namespace":"a","serviceAccount":"b","grants":[{"class":"runnerToken"}]}]}`,
		"no grants":               caller(``),
		"denied class":            caller(`{"class":"denied"}`),
		"health class":            caller(`{"class":"health"}`),
		"unknown class":           caller(`{"class":"admin"}`),
		"identity missing":        caller(`{"class":"identity"}`),
		"identity invalid":        caller(`{"class":"internal","identity":"sometimes"}`),
		"identity forbidden":      caller(`{"class":"orchestrator","identity":"optional"}`),
		"method outside class":    caller(`{"class":"identity","identity":"required","methods":["CreateWorkload"]}`),
		"denied method":           caller(`{"class":"identity","identity":"required","methods":["ValidateServiceToken"]}`),
		"empty methods":           caller(`{"class":"identity","identity":"required","methods":[]}`),
		"duplicate method":        caller(`{"class":"identity","identity":"required"},{"class":"identity","identity":"optional","methods":["GetWorkload"]}`),
		"tokenless mutation":      `{"version":1,"audience":"agyn-runners","tokenless":[{"method":"RegisterRunner","identity":"required"}]}`,
		"tokenless lifecycle":     `{"version":1,"audience":"agyn-runners","tokenless":[{"method":"CreateWorkload","identity":"required"}]}`,
		"tokenless other read":    `{"version":1,"audience":"agyn-runners","tokenless":[{"method":"ListRunners","identity":"required"}]}`,
		"tokenless no identity":   `{"version":1,"audience":"agyn-runners","tokenless":[{"method":"GetWorkload"}]}`,
		"tokenless duplicate":     `{"version":1,"audience":"agyn-runners","tokenless":[{"method":"GetWorkload","identity":"required"},{"method":"GetWorkload","identity":"optional"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParsePolicy([]byte(document), mustClassification(t)); err == nil {
				t.Fatalf("accepted %s", document)
			}
		})
	}
}

func TestParsePolicyRejectsOversizedDocument(t *testing.T) {
	document := `{"version":1,"audience":"` + strings.Repeat("a", maxPolicyBytes) + `"}`
	if _, err := ParsePolicy([]byte(document), mustClassification(t)); err == nil {
		t.Fatal("oversized policy accepted")
	}
}
