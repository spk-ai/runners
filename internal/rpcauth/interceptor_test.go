package rpcauth

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	runnerv1 "github.com/agynio/runners/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	testIdentity = "7f2d9b8e-3a51-4c8e-9d0b-5e6f7a8b9c0d"
	otherID      = "1b2c3d4e-5f60-4718-8a9b-0c1d2e3f4a5b"
)

// stubRunners records the context each handler saw. Unimplemented methods
// answer codes.Unimplemented without the "rpcauth:" prefix, which proves the
// call passed the interceptor.
type stubRunners struct {
	runnersv1.UnimplementedRunnersServiceServer
	mu   sync.Mutex
	seen []context.Context
}

func (s *stubRunners) record(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen = append(s.seen, ctx)
}

func (s *stubRunners) last() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seen[len(s.seen)-1]
}

func (s *stubRunners) GetWorkload(ctx context.Context, _ *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
	s.record(ctx)
	return &runnersv1.GetWorkloadResponse{}, nil
}

func (s *stubRunners) CreateWorkload(ctx context.Context, _ *runnersv1.CreateWorkloadRequest) (*runnersv1.CreateWorkloadResponse, error) {
	s.record(ctx)
	return &runnersv1.CreateWorkloadResponse{}, nil
}

func (s *stubRunners) StreamWorkloadLogs(_ *runnerv1.StreamWorkloadLogsRequest, stream grpc.ServerStreamingServer[runnerv1.StreamWorkloadLogsResponse]) error {
	s.record(stream.Context())
	return stream.Send(&runnerv1.StreamWorkloadLogsResponse{})
}

type harness struct {
	t       *testing.T
	fake    *fakeTokenReviews
	stub    *stubRunners
	conn    *grpc.ClientConn
	logs    *logCapture
	tokens  map[string]string
	server  *grpc.Server
	expires time.Time
}

type logCapture struct {
	mu    sync.Mutex
	lines []string
}

func (l *logCapture) printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *logCapture) joined() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

func newHarness(t *testing.T, mode Mode) *harness {
	t.Helper()
	h := &harness{t: t, fake: newFakeTokenReviews(), stub: &stubRunners{}, logs: &logCapture{}, tokens: map[string]string{}, expires: time.Now().Add(10 * time.Minute)}
	for name, principal := range map[string][2]string{
		"orchestrator": {testNamespace, "agents-orchestrator"},
		"gateway":      {testNamespace, "gateway"},
		"intruder":     {testNamespace, "rpcauth-intruder"},
		"workloads":    {"agyn-workloads", "gateway"},
	} {
		token := testJWT(name, h.expires)
		h.tokens[name] = token
		h.fake.statuses[token] = serviceAccountStatus(principal[0], principal[1], testAudience)
	}
	h.tokens["wrong-audience"] = testJWT("wrong-audience", h.expires)
	h.fake.statuses[h.tokens["wrong-audience"]] = serviceAccountStatus(testNamespace, "gateway", "other")
	h.tokens["expired"] = testJWT("expired", time.Now().Add(-time.Minute))
	h.tokens["forged"] = testJWT("forged", h.expires) // unknown to the API server

	authorizer, err := New(Options{
		Mode:           mode,
		Classification: mustClassification(t),
		Policy:         mustPolicy(t, testPolicy),
		Reviewer:       NewReviewer(h.fake, testAudience, time.Second),
		Logf:           h.logs.printf,
	})
	if err != nil {
		t.Fatalf("authorizer: %v", err)
	}
	listener := bufconn.Listen(1 << 20)
	h.server = grpc.NewServer(
		grpc.ChainUnaryInterceptor(authorizer.UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(authorizer.StreamServerInterceptor()),
	)
	runnersv1.RegisterRunnersServiceServer(h.server, h.stub)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(h.server, healthServer)
	if err := authorizer.VerifyServer(h.server); err != nil {
		t.Fatalf("coverage: %v", err)
	}
	go func() { _ = h.server.Serve(listener) }()
	h.conn, err = grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		_ = h.conn.Close()
		h.server.Stop()
	})
	return h
}

type call struct {
	token      string   // harness token name, "" for none
	rawTokens  []string // raw caller-token values, overriding token
	identities []string
}

func (h *harness) context(c call) context.Context {
	md := metadata.MD{}
	if c.rawTokens != nil {
		md[CallerTokenMetadataKey] = c.rawTokens
	} else if c.token != "" {
		md[CallerTokenMetadataKey] = []string{h.tokens[c.token]}
	}
	if len(c.identities) > 0 {
		md[identityMetadataKey] = c.identities
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	h.t.Cleanup(cancel)
	return metadata.NewOutgoingContext(ctx, md)
}

func (h *harness) unary(method string, c call) error {
	return h.conn.Invoke(h.context(c), FullMethod(RunnersServiceName, method), &emptypb.Empty{}, &emptypb.Empty{})
}

func (h *harness) stream(c call) error {
	stream, err := runnersv1.NewRunnersServiceClient(h.conn).StreamWorkloadLogs(h.context(c), &runnerv1.StreamWorkloadLogsRequest{})
	if err != nil {
		return err
	}
	for {
		if _, err := stream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

// outcome reduces an error to allowed or the rpcauth code.
func outcome(t *testing.T, err error) string {
	t.Helper()
	if err == nil {
		return "allowed"
	}
	st := status.Convert(err)
	if !strings.HasPrefix(st.Message(), "rpcauth:") {
		return "allowed" // the handler answered
	}
	return st.Code().String()
}

func TestInterceptorMatrix(t *testing.T) {
	h := newHarness(t, ModeEnforce)
	const (
		allowed  = "allowed"
		unauthn  = "Unauthenticated"
		denied   = "PermissionDenied"
		identity = testIdentity
	)
	for _, tc := range []struct {
		name   string
		method string
		call   call
		want   string
	}{
		{"orchestrator lifecycle", "CreateWorkload", call{token: "orchestrator"}, allowed},
		{"orchestrator volume migration", "AdvanceVolumeAnchorMigration", call{token: "orchestrator"}, allowed},
		{"orchestrator unscoped read", "GetWorkload", call{token: "orchestrator"}, allowed},
		{"orchestrator internal read", "ListWorkloads", call{token: "orchestrator"}, allowed},
		{"orchestrator cannot register runners", "RegisterRunner", call{token: "orchestrator"}, denied},
		{"orchestrator cannot delete runners", "DeleteRunner", call{token: "orchestrator", identities: []string{identity}}, denied},
		{"orchestrator cannot touch", "TouchWorkload", call{token: "orchestrator"}, denied},
		{"orchestrator cannot enroll", "EnrollRunner", call{token: "orchestrator"}, denied},
		{"gateway lifecycle denied", "CreateWorkload", call{token: "gateway", identities: []string{identity}}, denied},
		{"gateway read needs identity", "GetWorkload", call{token: "gateway"}, denied},
		{"gateway read with identity", "GetWorkload", call{token: "gateway", identities: []string{identity}}, allowed},
		{"gateway read with two identities", "GetWorkload", call{token: "gateway", identities: []string{identity, otherID}}, denied},
		{"gateway read with nil identity", "GetWorkload", call{token: "gateway", identities: []string{"00000000-0000-0000-0000-000000000000"}}, denied},
		{"gateway read with non-uuid identity", "GetWorkload", call{token: "gateway", identities: []string{"admin"}}, denied},
		{"gateway runner registration", "RegisterRunner", call{token: "gateway", identities: []string{identity}}, allowed},
		{"gateway internal needs identity", "ListWorkloads", call{token: "gateway"}, denied},
		{"gateway internal with identity", "TouchWorkload", call{token: "gateway", identities: []string{identity}}, allowed},
		{"gateway runner-token class", "EnrollRunner", call{token: "gateway"}, allowed},
		{"gateway report", "ReportWorkloadState", call{token: "gateway"}, allowed},
		{"unknown service account", "GetWorkload", call{token: "intruder", identities: []string{identity}}, denied},
		{"right name, wrong namespace", "GetWorkload", call{token: "workloads", identities: []string{identity}}, denied},
		{"denied class for orchestrator", "ValidateServiceToken", call{token: "orchestrator"}, denied},
		{"denied class for gateway", "CreateFlavor", call{token: "gateway", identities: []string{identity}}, denied},
		{"denied org teardown", "DeleteOrganizationResources", call{token: "orchestrator"}, denied},
		{"denied class without token", "ValidateServiceToken", call{}, denied},
		{"no token", "CreateWorkload", call{}, unauthn},
		{"no token lifecycle with identity", "RegisterRunner", call{identities: []string{identity}}, unauthn},
		{"wrong audience", "GetWorkload", call{token: "wrong-audience", identities: []string{identity}}, unauthn},
		{"expired", "CreateWorkload", call{token: "expired"}, unauthn},
		{"forged", "CreateWorkload", call{token: "forged"}, unauthn},
		{"two tokens", "CreateWorkload", call{rawTokens: []string{"a.b.c", "a.b.c"}}, unauthn},
		{"bearer prefix", "CreateWorkload", call{rawTokens: []string{"Bearer a.b.c"}}, unauthn},
		{"not a jwt", "CreateWorkload", call{rawTokens: []string{"opaque"}}, unauthn},
		{"oversized", "CreateWorkload", call{rawTokens: []string{strings.Repeat("a", maxTokenBytes) + ".b.c"}}, unauthn},
		{"tokenless allowlisted read", "ListWorkloadsByThread", call{identities: []string{identity}}, allowed},
		{"tokenless allowlisted read without identity", "ListWorkloadsByThread", call{}, denied},
		{"tokenless read not allowlisted", "ListWorkloadsByAgentInstance", call{identities: []string{identity}}, unauthn},
		{"tokenless lifecycle", "UpdateRunner", call{identities: []string{identity}}, unauthn},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := outcome(t, h.unary(tc.method, tc.call)); got != tc.want {
				t.Fatalf("%s: %s, want %s", tc.method, got, tc.want)
			}
		})
	}
	if !strings.Contains(h.logs.joined(), "rpcauth: denied method=/agynio.api.runners.v1.RunnersService/CreateWorkload caller=agyn-platform/gateway pod=gateway-pod") {
		t.Errorf("denials are not logged with principal and pod:\n%s", h.logs.joined())
	}
	if strings.Contains(h.logs.joined(), h.tokens["gateway"]) {
		t.Fatal("a token was logged")
	}
}

func TestInterceptorShapeChecksMakeNoReview(t *testing.T) {
	h := newHarness(t, ModeEnforce)
	for _, raw := range [][]string{{"a.b.c", "d.e.f"}, {"opaque"}, {""}, {"a..c"}, {strings.Repeat("x", maxTokenBytes+1)}} {
		_ = h.unary("CreateWorkload", call{rawTokens: raw})
	}
	_ = h.unary("CreateWorkload", call{})
	if n := h.fake.callCount(); n != 0 {
		t.Fatalf("%d TokenReviews for malformed or missing tokens", n)
	}
}

func TestInterceptorStreaming(t *testing.T) {
	h := newHarness(t, ModeEnforce)
	for _, tc := range []struct {
		name string
		call call
		want string
	}{
		{"gateway with identity", call{token: "gateway", identities: []string{testIdentity}}, "allowed"},
		{"gateway without identity", call{token: "gateway"}, "PermissionDenied"},
		{"orchestrator not granted logs", call{token: "orchestrator"}, "PermissionDenied"},
		{"no token", call{identities: []string{testIdentity}}, "Unauthenticated"},
		{"wrong audience", call{token: "wrong-audience", identities: []string{testIdentity}}, "Unauthenticated"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := outcome(t, h.stream(tc.call)); got != tc.want {
				t.Fatalf("StreamWorkloadLogs: %s, want %s", got, tc.want)
			}
		})
	}
	h.fake.setError(errors.New("apiserver down"))
	if got := outcome(t, h.stream(call{token: "intruder", identities: []string{testIdentity}})); got != "Unavailable" {
		t.Fatalf("stream with API down: %s, want Unavailable", got)
	}
}

func TestInterceptorFailsClosedOnAPIErrors(t *testing.T) {
	h := newHarness(t, ModeEnforce)
	h.fake.setError(errors.New("apiserver down"))
	if got := outcome(t, h.unary("CreateWorkload", call{token: "orchestrator"})); got != "Unavailable" {
		t.Fatalf("API error: %s, want Unavailable", got)
	}
	h.fake.setError(nil)
	if got := outcome(t, h.unary("CreateWorkload", call{token: "orchestrator"})); got != "allowed" {
		t.Fatalf("after recovery: %s, want allowed (errors are not cached)", got)
	}
}

func TestInterceptorHealthAndUnknownMethods(t *testing.T) {
	h := newHarness(t, ModeEnforce)
	ctx := h.context(call{})
	resp, err := healthpb.NewHealthClient(h.conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("tokenless health check: %v %v", resp, err)
	}
	watch, err := healthpb.NewHealthClient(h.conn).Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("health watch: %v", err)
	}
	if update, err := watch.Recv(); err != nil || update.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("tokenless health watch: %v %v", update, err)
	}
	if _, err := healthpb.NewHealthClient(h.conn).List(ctx, &healthpb.HealthListRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("health list: %v, want PermissionDenied", err)
	}
	for _, method := range []string{
		FullMethod(RunnersServiceName, "NoSuchMethod"),
		"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
	} {
		err := h.conn.Invoke(h.context(call{token: "orchestrator"}), method, &emptypb.Empty{}, &emptypb.Empty{})
		if status.Code(err) != codes.Unimplemented {
			t.Errorf("%s: %v, want Unimplemented", method, err)
		}
	}
	if n := h.fake.callCount(); n != 0 {
		t.Fatalf("%d TokenReviews for health or unknown methods", n)
	}
}

func TestInterceptorHandlerContext(t *testing.T) {
	h := newHarness(t, ModeEnforce)
	if err := h.unary("GetWorkload", call{token: "gateway", identities: []string{testIdentity}}); err != nil {
		t.Fatalf("call: %v", err)
	}
	assertHandlerContext(t, h.stub.last(), "agyn-platform/gateway")
	if err := h.stream(call{token: "gateway", identities: []string{testIdentity}}); err != nil {
		t.Fatalf("stream: %v", err)
	}
	assertHandlerContext(t, h.stub.last(), "agyn-platform/gateway")
}

func assertHandlerContext(t *testing.T, ctx context.Context, want string) {
	t.Helper()
	md, _ := metadata.FromIncomingContext(ctx)
	if len(md.Get(CallerTokenMetadataKey)) != 0 {
		t.Fatal("handler received the caller token")
	}
	if got := md.Get(identityMetadataKey); len(got) != 1 || got[0] != testIdentity {
		t.Fatalf("handler identity %v", got)
	}
	principal, ok := PrincipalFromContext(ctx)
	if !ok || principal.String() != want {
		t.Fatalf("handler principal %v %v, want %s", principal, ok, want)
	}
}

func TestInterceptorRejectionsArePrefixed(t *testing.T) {
	h := newHarness(t, ModeEnforce)
	for _, c := range []call{{}, {token: "forged"}, {token: "intruder"}, {rawTokens: []string{"x"}}} {
		err := h.unary("CreateWorkload", c)
		if err == nil || !strings.HasPrefix(status.Convert(err).Message(), "rpcauth:") {
			t.Fatalf("rejection %v lacks the rpcauth: prefix", err)
		}
	}
}

func TestPermissiveModeServesButLogs(t *testing.T) {
	h := newHarness(t, ModePermissive)
	for _, tc := range []struct {
		method string
		call   call
	}{
		{"CreateWorkload", call{}},
		{"ValidateServiceToken", call{}},
		{"CreateWorkload", call{token: "gateway"}},
		{"GetWorkload", call{token: "forged"}},
	} {
		if got := outcome(t, h.unary(tc.method, tc.call)); got != "allowed" {
			t.Fatalf("permissive %s: %s", tc.method, got)
		}
	}
	if err := h.stream(call{}); err != nil {
		t.Fatalf("permissive stream: %v", err)
	}
	assertNoToken := func(ctx context.Context) {
		md, _ := metadata.FromIncomingContext(ctx)
		if len(md.Get(CallerTokenMetadataKey)) != 0 {
			t.Fatal("permissive handler received the caller token")
		}
	}
	_ = h.unary("GetWorkload", call{token: "forged"})
	assertNoToken(h.stub.last())
	logs := h.logs.joined()
	if !strings.Contains(logs, "would deny (permissive) method=/agynio.api.runners.v1.RunnersService/CreateWorkload caller=unauthenticated") {
		t.Fatalf("permissive decisions not logged:\n%s", logs)
	}
}

func TestPermissiveWithoutPolicyStripsTokens(t *testing.T) {
	authorizer, err := New(Options{Mode: ModePermissive, Classification: mustClassification(t)})
	if err != nil {
		t.Fatal(err)
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(CallerTokenMetadataKey, "a.b.c", identityMetadataKey, testIdentity))
	out, err := authorizer.authorize(ctx, FullMethod(RunnersServiceName, "CreateWorkload"))
	if err != nil {
		t.Fatal(err)
	}
	md, _ := metadata.FromIncomingContext(out)
	if len(md.Get(CallerTokenMetadataKey)) != 0 || len(md.Get(identityMetadataKey)) != 1 {
		t.Fatalf("metadata %v", md)
	}
}

func TestNewRejectsIncompleteEnforcement(t *testing.T) {
	c := mustClassification(t)
	if _, err := New(Options{Mode: ModeEnforce, Classification: c}); err == nil {
		t.Fatal("enforce without policy accepted")
	}
	if _, err := New(Options{Mode: "audit", Classification: c}); err == nil {
		t.Fatal("unknown mode accepted")
	}
}

func TestParseMode(t *testing.T) {
	for value, want := range map[string]Mode{"": ModeEnforce, "enforce": ModeEnforce, " permissive ": ModePermissive} {
		if got, err := ParseMode(value); err != nil || got != want {
			t.Errorf("ParseMode(%q) = %q, %v", value, got, err)
		}
	}
	for _, value := range []string{"off", "Enforce", "audit"} {
		if _, err := ParseMode(value); err == nil {
			t.Errorf("ParseMode(%q) accepted", value)
		}
	}
}

func TestLimitedLoggerCapsRate(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_800_000_000, 0)}
	var lines []string
	l := &limitedLogger{printf: func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }, now: clock.Now}
	for i := 0; i < 100; i++ {
		l.logf("line %d", i)
	}
	if len(lines) != logsPerSecond {
		t.Fatalf("%d lines in one second, want %d", len(lines), logsPerSecond)
	}
	clock.Advance(time.Second)
	l.logf("next")
	if lines[logsPerSecond] != "rpcauth: suppressed 90 decision log lines" || lines[logsPerSecond+1] != "next" {
		t.Fatalf("summary lines %v", lines[logsPerSecond:])
	}
}
