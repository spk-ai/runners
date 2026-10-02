package rpcauth

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	testAudience  = "agyn-runners"
	testNamespace = "agyn-platform"
)

// testPolicy mirrors the production grants: the orchestrator owns lifecycle
// mutations and the reads it performs, gateway forwards authenticated
// identities and runner-token calls, and one tokenless read is opened.
const testPolicy = `{
  "version": 1,
  "audience": "agyn-runners",
  "cacheTTLSeconds": 30,
  "callers": [
    {"namespace": "agyn-platform", "serviceAccount": "agents-orchestrator", "grants": [
      {"class": "orchestrator"},
      {"class": "identity", "identity": "optional", "methods": ["GetRunner", "ListRunners", "ListFlavors", "GetWorkload",
        "ListWorkloadsByThread", "ListWorkloadsByAgentInstance", "GetVolume", "ListVolumes", "ListVolumesByThread",
        "ListVolumesByAgentInstance"]},
      {"class": "internal", "identity": "optional", "methods": ["ListWorkloads"]}
    ]},
    {"namespace": "agyn-platform", "serviceAccount": "gateway", "grants": [
      {"class": "identity", "identity": "required"},
      {"class": "internal", "identity": "required"},
      {"class": "runnerToken"}
    ]}
  ],
  "tokenless": [{"method": "ListWorkloadsByThread", "identity": "required"}]
}`

func mustClassification(t *testing.T) *Classification {
	t.Helper()
	c, err := DefaultClassification()
	if err != nil {
		t.Fatalf("classification: %v", err)
	}
	return c
}

func mustPolicy(t *testing.T, document string) *CompiledPolicy {
	t.Helper()
	p, err := ParsePolicy([]byte(document), mustClassification(t))
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	return p
}

// testJWT builds a JWT-shaped token. Only the API server verifies signatures;
// runners reads exp to bound its cache.
func testJWT(subject string, exp time.Time) string {
	enc := base64.RawURLEncoding.EncodeToString
	header := enc([]byte(`{"alg":"RS256","typ":"JWT"}`))
	payload := enc([]byte(fmt.Sprintf(`{"sub":%q,"exp":%d}`, subject, exp.Unix())))
	return header + "." + payload + "." + enc([]byte("signature-"+subject))
}

func serviceAccountStatus(namespace, serviceAccount string, audiences ...string) authenticationv1.TokenReviewStatus {
	return authenticationv1.TokenReviewStatus{
		Authenticated: true,
		Audiences:     audiences,
		User: authenticationv1.UserInfo{
			Username: "system:serviceaccount:" + namespace + ":" + serviceAccount,
			Extra: map[string]authenticationv1.ExtraValue{
				podNameExtra: {serviceAccount + "-pod"},
				podUIDExtra:  {"0d6c2f55-5d2c-4e0b-9a43-7d6a8bf1b001"},
			},
		},
	}
}

// fakeTokenReviews stands in for the API server's TokenReview endpoint.
type fakeTokenReviews struct {
	mu        sync.Mutex
	calls     int
	audiences [][]string
	statuses  map[string]authenticationv1.TokenReviewStatus
	err       error
	block     chan struct{}
}

func newFakeTokenReviews() *fakeTokenReviews {
	return &fakeTokenReviews{statuses: map[string]authenticationv1.TokenReviewStatus{}}
}

func (f *fakeTokenReviews) Create(ctx context.Context, review *authenticationv1.TokenReview, _ metav1.CreateOptions) (*authenticationv1.TokenReview, error) {
	f.mu.Lock()
	f.calls++
	f.audiences = append(f.audiences, append([]string(nil), review.Spec.Audiences...))
	status, known := f.statuses[review.Spec.Token]
	err := f.err
	block := f.block
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err != nil {
		return nil, err
	}
	if !known {
		status = authenticationv1.TokenReviewStatus{Authenticated: false, Error: "invalid bearer token"}
	}
	return &authenticationv1.TokenReview{Status: status}, nil
}

func (f *fakeTokenReviews) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeTokenReviews) setError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}
