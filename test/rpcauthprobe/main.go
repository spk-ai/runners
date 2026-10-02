// Command rpcauthprobe checks runners caller authorization from inside a pod.
// The disposable-VM E2E copies it into probe pods that hold projected tokens
// and runs a matrix of expected outcomes against the runners Service and Pod
// addresses. It uses raw method names with empty messages, so it needs no
// generated bindings; an empty request reaches a handler only as a no-op or
// a validation error.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	runnersService = "/agynio.api.runners.v1.RunnersService/"
	tokenKey       = "x-agyn-caller-token"
	identityKey    = "x-identity-id"
)

// row is one expected outcome. Expect is "allowed" (no error, or an error the
// handler produced, i.e. without the "rpcauth:" prefix), "serving" (health),
// or a gRPC code name for an rpcauth or transport rejection.
type row struct {
	Name       string   `json:"name"`
	Method     string   `json:"method"`
	TokenFile  string   `json:"tokenFile,omitempty"`
	Identities []string `json:"identities,omitempty"`
	Stream     bool     `json:"stream,omitempty"`
	Expect     string   `json:"expect"`
}

func main() {
	targets := flag.String("targets", "", "comma-separated host:port targets")
	flag.Parse()
	if *targets == "" {
		fmt.Fprintln(os.Stderr, "usage: rpcauthprobe -targets host:port[,host:port] < matrix.json")
		os.Exit(2)
	}
	var rows []row
	if err := json.NewDecoder(os.Stdin).Decode(&rows); err != nil || len(rows) == 0 {
		fmt.Fprintf(os.Stderr, "read matrix: %v\n", err)
		os.Exit(2)
	}
	failures := 0
	for _, target := range strings.Split(*targets, ",") {
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Fprintf(os.Stderr, "dial %s: %v\n", target, err)
			os.Exit(1)
		}
		for _, r := range rows {
			got, detail := run(conn, r)
			verdict := "PASS"
			if got != r.Expect {
				verdict = "FAIL"
				failures++
			}
			fmt.Printf("%s target=%s row=%q method=%s expect=%s got=%s %s\n", verdict, target, r.Name, r.Method, r.Expect, got, detail)
		}
		_ = conn.Close()
	}
	if failures > 0 {
		fmt.Printf("%d rows failed\n", failures)
		os.Exit(1)
	}
}

func run(conn *grpc.ClientConn, r row) (string, string) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	md := metadata.MD{}
	if r.TokenFile != "" {
		token, err := os.ReadFile(r.TokenFile)
		if err != nil {
			return "probe-error", fmt.Sprintf("read token: %v", err)
		}
		md.Set(tokenKey, strings.TrimSpace(string(token)))
	}
	if len(r.Identities) > 0 {
		md[identityKey] = r.Identities
	}
	ctx = metadata.NewOutgoingContext(ctx, md)

	if r.Method == "HealthCheck" {
		resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
		if err != nil {
			return classify(err)
		}
		if resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			return "serving", ""
		}
		return resp.GetStatus().String(), ""
	}
	method := r.Method
	if !strings.HasPrefix(method, "/") {
		method = runnersService + method
	}
	if r.Stream {
		stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, method)
		if err != nil {
			return classify(err)
		}
		if err := stream.SendMsg(&emptypb.Empty{}); err != nil && !errors.Is(err, io.EOF) {
			return classify(err)
		}
		if err := stream.CloseSend(); err != nil {
			return classify(err)
		}
		if err := stream.RecvMsg(&emptypb.Empty{}); err != nil && !errors.Is(err, io.EOF) {
			return classify(err)
		}
		return "allowed", ""
	}
	return classify(conn.Invoke(ctx, method, &emptypb.Empty{}, &emptypb.Empty{}))
}

func classify(err error) (string, string) {
	if err == nil {
		return "allowed", ""
	}
	st := status.Convert(err)
	detail := fmt.Sprintf("code=%s message=%q", st.Code(), st.Message())
	switch {
	case strings.HasPrefix(st.Message(), "rpcauth:"):
		return st.Code().String(), detail
	case st.Code() == codes.Unimplemented && strings.HasPrefix(st.Message(), "unknown "):
		// grpc-go's answer for an unregistered service or method.
		return st.Code().String(), detail
	case st.Code() == codes.Unavailable || st.Code() == codes.DeadlineExceeded:
		return "transport-" + st.Code().String(), detail
	default:
		// The handler answered, so the interceptor admitted the call.
		return "allowed", detail
	}
}
