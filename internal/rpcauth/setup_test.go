package rpcauth

import (
	"context"
	"errors"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func reviewerFactory(fake *fakeTokenReviews, factoryErr error) ReviewerFactory {
	return func(audience string, timeout time.Duration) (*Reviewer, error) {
		if factoryErr != nil {
			return nil, factoryErr
		}
		return NewReviewer(fake, audience, timeout), nil
	}
}

func TestSetupEnforce(t *testing.T) {
	ctx := context.Background()
	fake := newFakeTokenReviews()
	authorizer, err := Setup(ctx, Config{Mode: ModeEnforce, Policy: []byte(testPolicy)}, reviewerFactory(fake, nil))
	if err != nil || authorizer.Mode() != ModeEnforce {
		t.Fatalf("setup: %v", err)
	}
	if fake.callCount() != 1 {
		t.Fatalf("self-check reviews %d, want 1", fake.callCount())
	}
	if _, err := Setup(ctx, Config{Mode: ModeEnforce}, reviewerFactory(fake, nil)); err == nil {
		t.Fatal("enforce without a policy started")
	}
	if _, err := Setup(ctx, Config{Mode: ModeEnforce, Policy: []byte(`{"version":1}`)}, reviewerFactory(fake, nil)); err == nil {
		t.Fatal("enforce with an invalid policy started")
	}
	if _, err := Setup(ctx, Config{Mode: ModeEnforce, Policy: []byte(testPolicy)}, reviewerFactory(nil, errors.New("not in cluster"))); err == nil {
		t.Fatal("enforce without an API client started")
	}
	forbidden := newFakeTokenReviews()
	forbidden.err = apierrors.NewForbidden(tokenReviewsResource, "", errors.New("no create"))
	if _, err := Setup(ctx, Config{Mode: ModeEnforce, Policy: []byte(testPolicy)}, reviewerFactory(forbidden, nil)); err == nil {
		t.Fatal("enforce without TokenReview RBAC started")
	}
}

func TestSetupPermissive(t *testing.T) {
	ctx := context.Background()
	if a, err := Setup(ctx, Config{Mode: ModePermissive}, reviewerFactory(nil, errors.New("unused"))); err != nil || a.Mode() != ModePermissive {
		t.Fatalf("permissive without policy: %v", err)
	}
	forbidden := newFakeTokenReviews()
	forbidden.err = apierrors.NewForbidden(tokenReviewsResource, "", errors.New("no create"))
	if _, err := Setup(ctx, Config{Mode: ModePermissive, Policy: []byte(testPolicy)}, reviewerFactory(forbidden, nil)); err != nil {
		t.Fatalf("permissive must tolerate missing RBAC: %v", err)
	}
	if _, err := Setup(ctx, Config{Mode: ModePermissive, Policy: []byte(testPolicy)}, reviewerFactory(nil, errors.New("not in cluster"))); err != nil {
		t.Fatalf("permissive must tolerate a missing API client: %v", err)
	}
	if _, err := Setup(ctx, Config{Mode: ModePermissive, Policy: []byte(`{"version":1,"audience":"k3s"}`)}, reviewerFactory(forbidden, nil)); err == nil {
		t.Fatal("permissive accepted an invalid policy")
	}
}
