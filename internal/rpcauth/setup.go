package rpcauth

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Config is the deployment input, read from the environment by
// internal/config:
//
//	RUNNERS_RPC_AUTH_MODE        enforce (default) | permissive
//	RUNNERS_RPC_POLICY           inline Policy JSON, or
//	RUNNERS_RPC_POLICY_FILE      path to a Policy JSON file (not both)
//	RUNNERS_TOKENREVIEW_TIMEOUT  per-review deadline, default 3s
type Config struct {
	Mode          Mode
	Policy        []byte
	ReviewTimeout time.Duration
}

// ReviewerFactory builds the TokenReview client for an audience.
type ReviewerFactory func(audience string, timeout time.Duration) (*Reviewer, error)

// Setup validates the configuration and returns a ready Authorizer. It must
// run before any side effect (database migrations, Ziti enrollment, listening):
// in enforce mode a missing or invalid policy, an unusable in-cluster client
// or a failed TokenReview self-check (typically missing RBAC) stops startup.
// Permissive mode still rejects an invalid policy, but only warns when token
// review is unavailable, because it never blocks a call.
func Setup(ctx context.Context, config Config, newReviewer ReviewerFactory) (*Authorizer, error) {
	classification, err := DefaultClassification()
	if err != nil {
		return nil, err
	}
	if newReviewer == nil {
		newReviewer = NewInClusterReviewer
	}
	if len(config.Policy) == 0 {
		if config.Mode == ModeEnforce {
			return nil, errors.New("rpcauth: enforce mode requires RUNNERS_RPC_POLICY or RUNNERS_RPC_POLICY_FILE")
		}
		log.Printf("rpcauth: WARNING permissive mode without a policy; runners serves every caller unauthenticated")
		return New(Options{Mode: config.Mode, Classification: classification})
	}
	policy, err := ParsePolicy(config.Policy, classification)
	if err != nil {
		return nil, fmt.Errorf("rpcauth: %w", err)
	}
	reviewer, err := newReviewer(policy.Audience, config.ReviewTimeout)
	if err == nil {
		err = reviewer.SelfCheck(ctx)
	}
	if err != nil {
		if config.Mode == ModeEnforce {
			return nil, fmt.Errorf("rpcauth: %w", err)
		}
		log.Printf("rpcauth: WARNING permissive mode cannot review tokens: %v", err)
		if reviewer == nil {
			reviewer = NewReviewer(unavailableReviews{err: err}, policy.Audience, config.ReviewTimeout)
		}
	}
	if config.Mode == ModePermissive {
		log.Printf("rpcauth: WARNING permissive mode; policy decisions are logged, not enforced")
	}
	return New(Options{Mode: config.Mode, Classification: classification, Policy: policy, Reviewer: reviewer})
}

type unavailableReviews struct{ err error }

func (u unavailableReviews) Create(context.Context, *authenticationv1.TokenReview, metav1.CreateOptions) (*authenticationv1.TokenReview, error) {
	return nil, u.err
}
