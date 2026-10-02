package rpcauth

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// CallerTokenMetadataKey carries the caller's audience-bound projected
// ServiceAccount token. It is dedicated to this check (not "authorization"),
// must hold exactly one value, and is stripped before any handler runs, so it
// is never forwarded downstream.
const CallerTokenMetadataKey = "x-agyn-caller-token"

const (
	identityMetadataKey = "x-identity-id"
	maxTokenBytes       = 8 * 1024
	logsPerSecond       = 10
)

// Mode selects whether decisions are enforced.
type Mode string

const (
	// ModeEnforce rejects every call the policy does not allow. It is the
	// default and the only mode for production.
	ModeEnforce Mode = "enforce"
	// ModePermissive evaluates and logs decisions but serves every call. It
	// exists only for deployments whose callers do not send caller tokens yet
	// (upstream E2E suites); it must be configured explicitly.
	ModePermissive Mode = "permissive"
)

// ParseMode validates a mode string; empty means enforce.
func ParseMode(value string) (Mode, error) {
	switch Mode(strings.TrimSpace(value)) {
	case "", ModeEnforce:
		return ModeEnforce, nil
	case ModePermissive:
		return ModePermissive, nil
	default:
		return "", fmt.Errorf("rpc auth mode must be %q or %q", ModeEnforce, ModePermissive)
	}
}

// Authorizer is a unary and stream server interceptor pair.
type Authorizer struct {
	mode           Mode
	classification *Classification
	policy         *CompiledPolicy
	reviewer       tokenReviewer
	logger         *limitedLogger
}

// Options configures an Authorizer.
type Options struct {
	Mode           Mode
	Classification *Classification
	// Policy may be nil only in permissive mode, which then logs nothing but
	// still strips caller tokens.
	Policy *CompiledPolicy
	// Reviewer validates tokens; results are cached for Policy.CacheTTL.
	Reviewer tokenReviewer
	// Logf receives rate-limited decision logs; defaults to log.Printf.
	Logf func(format string, args ...any)
}

// New builds an Authorizer.
func New(options Options) (*Authorizer, error) {
	if options.Classification == nil {
		return nil, errors.New("rpcauth: classification is required")
	}
	switch options.Mode {
	case ModeEnforce:
		if options.Policy == nil || options.Reviewer == nil {
			return nil, errors.New("rpcauth: enforce mode needs a policy and a token reviewer")
		}
	case ModePermissive:
		if options.Policy != nil && options.Reviewer == nil {
			return nil, errors.New("rpcauth: a policy needs a token reviewer")
		}
	default:
		return nil, fmt.Errorf("rpcauth: unknown mode %q", options.Mode)
	}
	logf := options.Logf
	if logf == nil {
		logf = log.Printf
	}
	a := &Authorizer{
		mode:           options.Mode,
		classification: options.Classification,
		policy:         options.Policy,
		logger:         &limitedLogger{printf: logf, now: time.Now},
	}
	if options.Reviewer != nil {
		ttl := DefaultCacheTTL
		if options.Policy != nil {
			ttl = options.Policy.CacheTTL
		}
		a.reviewer = newCachingReviewer(options.Reviewer, ttl)
	}
	return a, nil
}

// Mode reports the configured mode.
func (a *Authorizer) Mode() Mode { return a.mode }

// VerifyServer fails if the server registered any unclassified method.
func (a *Authorizer) VerifyServer(server *grpc.Server) error {
	info := server.GetServiceInfo()
	services := make([]ServiceMethods, 0, len(info))
	for name, service := range info {
		methods := make([]string, 0, len(service.Methods))
		for _, method := range service.Methods {
			methods = append(methods, method.Name)
		}
		services = append(services, ServiceMethods{Service: name, Methods: methods})
	}
	return a.classification.VerifyCoverage(services)
}

// UnaryServerInterceptor authorizes unary calls.
func (a *Authorizer) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		authorized, err := a.authorize(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		return handler(authorized, req)
	}
}

// StreamServerInterceptor authorizes streaming calls (StreamWorkloadLogs,
// health Watch) with the same decision as unary calls.
func (a *Authorizer) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		authorized, err := a.authorize(stream.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		return handler(srv, &authorizedStream{ServerStream: stream, ctx: authorized})
	}
}

type authorizedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *authorizedStream) Context() context.Context { return s.ctx }

type principalContextKey struct{}

// PrincipalFromContext returns the caller admitted by the interceptor.
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(principalContextKey{}).(Principal)
	return principal, ok
}

// authorize returns the handler context, or the error to send. Permissive mode
// computes the same decision, logs a would-be denial and serves the call.
func (a *Authorizer) authorize(ctx context.Context, fullMethod string) (context.Context, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	tokens := md.Get(CallerTokenMetadataKey)
	stripped := stripCallerToken(ctx, md)
	if a.policy == nil {
		// Permissive without a policy: nothing to evaluate.
		return stripped, nil
	}
	principal, err := a.decide(ctx, fullMethod, md, tokens)
	if err != nil {
		a.logDenial(ctx, fullMethod, principal, err)
		if a.mode == ModePermissive {
			return stripped, nil
		}
		return nil, err
	}
	return context.WithValue(stripped, principalContextKey{}, principal), nil
}

func (a *Authorizer) decide(ctx context.Context, fullMethod string, md metadata.MD, tokens []string) (Principal, error) {
	class, ok := a.classification.Lookup(fullMethod)
	if !ok {
		return Principal{}, status.Error(codes.PermissionDenied, "rpcauth: method is not classified")
	}
	switch class {
	case ClassHealth:
		return Principal{Tokenless: true}, nil
	case ClassDenied:
		return Principal{}, status.Error(codes.PermissionDenied, "rpcauth: method is denied")
	}
	if len(tokens) == 0 {
		r, ok := a.policy.lookupTokenless(fullMethod)
		if !ok {
			return Principal{}, status.Error(codes.Unauthenticated, "rpcauth: caller token required")
		}
		principal := Principal{Tokenless: true}
		if err := checkIdentity(r.identity, md); err != nil {
			return principal, err
		}
		return principal, nil
	}
	if len(tokens) > 1 {
		return Principal{}, status.Error(codes.Unauthenticated, "rpcauth: exactly one caller token is allowed")
	}
	token := tokens[0]
	if !wellFormedJWT(token) {
		return Principal{}, status.Error(codes.Unauthenticated, "rpcauth: malformed caller token")
	}
	principal, err := a.reviewer.Review(ctx, token)
	if err != nil {
		if errors.Is(err, errNotAuthenticated) {
			return Principal{}, status.Error(codes.Unauthenticated, "rpcauth: caller token rejected")
		}
		// API errors, missing RBAC, throttling and timeouts fail closed.
		a.logger.logf("rpcauth: token review failed for %s: %v", fullMethod, err)
		return Principal{}, status.Error(codes.Unavailable, "rpcauth: caller token review unavailable")
	}
	r, ok := a.policy.lookup(principal, fullMethod)
	if !ok {
		return principal, status.Errorf(codes.PermissionDenied, "rpcauth: %s is not granted this method", principal)
	}
	if err := checkIdentity(r.identity, md); err != nil {
		return principal, err
	}
	return principal, nil
}

func checkIdentity(requirement IdentityRule, md metadata.MD) error {
	if requirement != IdentityRequired {
		return nil
	}
	values := md.Get(identityMetadataKey)
	if len(values) != 1 {
		return status.Error(codes.PermissionDenied, "rpcauth: exactly one x-identity-id is required")
	}
	parsed, err := uuid.Parse(strings.TrimSpace(values[0]))
	if err != nil || parsed == uuid.Nil {
		return status.Error(codes.PermissionDenied, "rpcauth: x-identity-id must be a non-nil UUID")
	}
	return nil
}

// wellFormedJWT rejects obviously invalid tokens before any API call.
func wellFormedJWT(token string) bool {
	if token == "" || len(token) > maxTokenBytes {
		return false
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		if _, err := base64.RawURLEncoding.DecodeString(part); err != nil {
			return false
		}
	}
	return true
}

func stripCallerToken(ctx context.Context, md metadata.MD) context.Context {
	if md == nil || len(md.Get(CallerTokenMetadataKey)) == 0 {
		return ctx
	}
	copied := md.Copy()
	copied.Delete(CallerTokenMetadataKey)
	return metadata.NewIncomingContext(ctx, copied)
}

func (a *Authorizer) logDenial(ctx context.Context, fullMethod string, principal Principal, err error) {
	caller := "unauthenticated"
	if principal.Namespace != "" || principal.Tokenless {
		caller = principal.String()
		if principal.PodName != "" {
			caller += " pod=" + principal.PodName
		}
	}
	remote := "unknown"
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		remote = p.Addr.String()
	}
	verdict := "denied"
	if a.mode == ModePermissive {
		verdict = "would deny (permissive)"
	}
	a.logger.logf("rpcauth: %s method=%s caller=%s peer=%s code=%s reason=%q",
		verdict, fullMethod, caller, remote, status.Code(err), status.Convert(err).Message())
}

// limitedLogger caps decision logs: any reachable peer can send bad calls at
// request rate, and pod logs share the node's eviction-sensitive filesystem.
type limitedLogger struct {
	printf func(format string, args ...any)
	now    func() time.Time

	mu          sync.Mutex
	windowStart time.Time
	count       int
	suppressed  int
}

func (l *limitedLogger) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if now.Sub(l.windowStart) >= time.Second {
		if l.suppressed > 0 {
			l.printf("rpcauth: suppressed %d decision log lines", l.suppressed)
		}
		l.windowStart = now
		l.count = 0
		l.suppressed = 0
	}
	if l.count >= logsPerSecond {
		l.suppressed++
		return
	}
	l.count++
	l.printf(format, args...)
}
