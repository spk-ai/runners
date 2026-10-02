package rpcauth

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	authenticationclient "k8s.io/client-go/kubernetes/typed/authentication/v1"
	"k8s.io/client-go/rest"
)

const (
	serviceAccountUsernamePrefix = "system:serviceaccount:"
	podNameExtra                 = "authentication.kubernetes.io/pod-name"
	podUIDExtra                  = "authentication.kubernetes.io/pod-uid"
	// DefaultReviewTimeout bounds one TokenReview call.
	DefaultReviewTimeout = 3 * time.Second
)

// Principal is the authenticated caller.
type Principal struct {
	Namespace      string
	ServiceAccount string
	// PodName and PodUID identify the pod the projected token is bound to.
	PodName string
	PodUID  string
	// Tokenless marks a call admitted by a Policy.Tokenless grant. Nothing
	// about such a caller is authenticated.
	Tokenless bool
}

func (p Principal) String() string {
	if p.Tokenless {
		return "tokenless"
	}
	return principalKey(p.Namespace, p.ServiceAccount)
}

// errNotAuthenticated means the API server definitively rejected the token.
// It is safe to cache briefly. Any other reviewer error is transient.
var errNotAuthenticated = errors.New("token not authenticated")

// TokenReviewCreator is the slice of the client-go TokenReview client used here.
type TokenReviewCreator interface {
	Create(ctx context.Context, review *authenticationv1.TokenReview, opts metav1.CreateOptions) (*authenticationv1.TokenReview, error)
}

// Reviewer validates caller tokens with the Kubernetes TokenReview API.
type Reviewer struct {
	client   TokenReviewCreator
	audience string
	timeout  time.Duration
}

// NewReviewer builds a reviewer around an explicit client (tests use fakes).
func NewReviewer(client TokenReviewCreator, audience string, timeout time.Duration) *Reviewer {
	if timeout <= 0 {
		timeout = DefaultReviewTimeout
	}
	return &Reviewer{client: client, audience: audience, timeout: timeout}
}

// NewInClusterReviewer uses the pod's own ServiceAccount, which needs
// `create` on `tokenreviews.authentication.k8s.io` and nothing else.
func NewInClusterReviewer(audience string, timeout time.Duration) (*Reviewer, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load in-cluster config: %w", err)
	}
	// client-go defaults to QPS 5 / burst 10. The cache and the in-flight limit
	// bound the steady rate to roughly one review per caller token rotation;
	// a little headroom covers restarts. Throttling waits inside the request
	// deadline and then fails closed as Unavailable.
	config.QPS = 20
	config.Burst = 40
	config.Timeout = timeout
	client, err := authenticationclient.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("build tokenreview client: %w", err)
	}
	return NewReviewer(client.TokenReviews(), audience, timeout), nil
}

// Review returns the principal for a token. A definitive rejection wraps
// errNotAuthenticated; every other error is transient and must fail closed.
func (r *Reviewer) Review(ctx context.Context, token string) (Principal, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	result, err := r.client.Create(ctx, &authenticationv1.TokenReview{
		Spec: authenticationv1.TokenReviewSpec{
			Token:     token,
			Audiences: []string{r.audience},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return Principal{}, fmt.Errorf("tokenreview: %w", err)
	}
	if result == nil {
		return Principal{}, fmt.Errorf("tokenreview: empty response")
	}
	return principalFromStatus(result.Status, r.audience)
}

// principalFromStatus applies every check the TokenReview contract leaves to
// the client: authenticated, no error, the requested audience echoed back
// (an authenticator that ignores audiences returns none), an exact
// ServiceAccount username and a pod binding (kubelet-projected tokens always
// carry one; long-lived Secret tokens and unbound TokenRequest tokens do not).
func principalFromStatus(status authenticationv1.TokenReviewStatus, audience string) (Principal, error) {
	if status.Error != "" || !status.Authenticated {
		return Principal{}, fmt.Errorf("%w: rejected by the API server", errNotAuthenticated)
	}
	if !slices.Contains(status.Audiences, audience) {
		return Principal{}, fmt.Errorf("%w: audience not confirmed", errNotAuthenticated)
	}
	namespace, serviceAccount, ok := parseServiceAccountUsername(status.User.Username)
	if !ok {
		return Principal{}, fmt.Errorf("%w: not a service account", errNotAuthenticated)
	}
	podName := singleExtra(status.User.Extra, podNameExtra)
	podUID := singleExtra(status.User.Extra, podUIDExtra)
	if podName == "" || podUID == "" {
		return Principal{}, fmt.Errorf("%w: token is not bound to a pod", errNotAuthenticated)
	}
	return Principal{Namespace: namespace, ServiceAccount: serviceAccount, PodName: podName, PodUID: podUID}, nil
}

func parseServiceAccountUsername(username string) (string, string, bool) {
	rest, ok := strings.CutPrefix(username, serviceAccountUsernamePrefix)
	if !ok {
		return "", "", false
	}
	parts := strings.Split(rest, ":")
	if len(parts) != 2 {
		return "", "", false
	}
	if len(validation.IsDNS1123Label(parts[0])) > 0 || len(validation.IsDNS1123Subdomain(parts[1])) > 0 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func singleExtra(extra map[string]authenticationv1.ExtraValue, key string) string {
	values := extra[key]
	if len(values) != 1 {
		return ""
	}
	return values[0]
}

// SelfCheck proves the API server answers this ServiceAccount's TokenReviews,
// so missing RBAC fails startup instead of every RPC. A syntactically valid but
// unsigned token must come back unauthenticated.
func (r *Reviewer) SelfCheck(ctx context.Context) error {
	_, err := r.Review(ctx, selfCheckToken)
	if err == nil {
		return fmt.Errorf("tokenreview self-check: an unsigned token was authenticated")
	}
	if errors.Is(err, errNotAuthenticated) {
		return nil
	}
	return fmt.Errorf("tokenreview self-check (runners needs create on tokenreviews.authentication.k8s.io): %w", err)
}

// selfCheckToken is header {"alg":"RS256","typ":"JWT"}, payload {"sub":"rpcauth-self-check"}
// and a junk signature. It carries no secret.
const selfCheckToken = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJycGNhdXRoLXNlbGYtY2hlY2sifQ.c2VsZi1jaGVjaw"
