package rpcauth

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var tokenReviewsResource = schema.GroupResource{Group: "authentication.k8s.io", Resource: "tokenreviews"}

func TestReviewerAcceptsBoundServiceAccountToken(t *testing.T) {
	fake := newFakeTokenReviews()
	token := testJWT("gateway", time.Now().Add(10*time.Minute))
	fake.statuses[token] = serviceAccountStatus(testNamespace, "gateway", testAudience)
	principal, err := NewReviewer(fake, testAudience, time.Second).Review(context.Background(), token)
	if err != nil {
		t.Fatalf("review: %v", err)
	}
	if principal.Namespace != testNamespace || principal.ServiceAccount != "gateway" || principal.PodName != "gateway-pod" || principal.PodUID == "" {
		t.Fatalf("principal %+v", principal)
	}
	if len(fake.audiences) != 1 || !slices.Equal(fake.audiences[0], []string{testAudience}) {
		t.Fatalf("spec.audiences %v, want [%s]", fake.audiences, testAudience)
	}
}

func TestReviewerRejectsDefinitively(t *testing.T) {
	base := serviceAccountStatus(testNamespace, "gateway", testAudience)
	withUser := func(username string) authenticationv1.TokenReviewStatus {
		s := serviceAccountStatus(testNamespace, "gateway", testAudience)
		s.User.Username = username
		return s
	}
	unbound := serviceAccountStatus(testNamespace, "gateway", testAudience)
	unbound.User.Extra = nil
	errored := base
	errored.Error = "token expired"
	for name, status := range map[string]authenticationv1.TokenReviewStatus{
		"unauthenticated (expired or tampered)": {Authenticated: false, Error: "token has expired"},
		"authenticated with error":              errored,
		"no audiences echoed":                   serviceAccountStatus(testNamespace, "gateway"),
		"wrong audience":                        serviceAccountStatus(testNamespace, "gateway", "other"),
		"api server audience":                   serviceAccountStatus(testNamespace, "gateway", "https://kubernetes.default.svc.cluster.local"),
		"namespace only":                        withUser("system:serviceaccount:agyn-platform"),
		"extra segment":                         withUser("system:serviceaccount:a:b:c"),
		"node":                                  withUser("system:node:x"),
		"user":                                  withUser("admin"),
		"invalid namespace":                     withUser("system:serviceaccount:Agyn:gateway"),
		"unbound token":                         unbound,
	} {
		t.Run(name, func(t *testing.T) {
			fake := newFakeTokenReviews()
			token := testJWT("t", time.Now().Add(time.Hour))
			fake.statuses[token] = status
			_, err := NewReviewer(fake, testAudience, time.Second).Review(context.Background(), token)
			if !errors.Is(err, errNotAuthenticated) {
				t.Fatalf("error %v, want definitive rejection", err)
			}
		})
	}
}

func TestReviewerAPIErrorsAreTransient(t *testing.T) {
	for name, apiErr := range map[string]error{
		"forbidden (missing RBAC)": apierrors.NewForbidden(tokenReviewsResource, "", errors.New("no create")),
		"internal":                 apierrors.NewInternalError(errors.New("etcd")),
		"throttled":                apierrors.NewTooManyRequests("slow down", 1),
		"timeout":                  context.DeadlineExceeded,
	} {
		t.Run(name, func(t *testing.T) {
			fake := newFakeTokenReviews()
			fake.err = apiErr
			_, err := NewReviewer(fake, testAudience, time.Second).Review(context.Background(), testJWT("t", time.Now().Add(time.Hour)))
			if err == nil || errors.Is(err, errNotAuthenticated) {
				t.Fatalf("error %v, want transient", err)
			}
		})
	}
}

func TestReviewerTimesOut(t *testing.T) {
	fake := newFakeTokenReviews()
	fake.block = make(chan struct{})
	defer close(fake.block)
	start := time.Now()
	_, err := NewReviewer(fake, testAudience, 50*time.Millisecond).Review(context.Background(), testJWT("t", time.Now().Add(time.Hour)))
	if err == nil || errors.Is(err, errNotAuthenticated) {
		t.Fatalf("error %v, want transient timeout", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("review ignored its deadline")
	}
}

func TestReviewerSelfCheck(t *testing.T) {
	fake := newFakeTokenReviews()
	if err := NewReviewer(fake, testAudience, time.Second).SelfCheck(context.Background()); err != nil {
		t.Fatalf("self-check against a working API: %v", err)
	}
	fake.err = apierrors.NewForbidden(tokenReviewsResource, "", errors.New("no create"))
	if err := NewReviewer(fake, testAudience, time.Second).SelfCheck(context.Background()); err == nil {
		t.Fatal("self-check passed without RBAC")
	}
	fake.err = nil
	fake.statuses[selfCheckToken] = serviceAccountStatus(testNamespace, "gateway", testAudience)
	if err := NewReviewer(fake, testAudience, time.Second).SelfCheck(context.Background()); err == nil {
		t.Fatal("self-check passed although an unsigned token authenticated")
	}
}
