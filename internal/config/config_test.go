package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agynio/runners/internal/rpcauth"
)

func setRPCAuthEnv(t *testing.T, values map[string]string) {
	t.Helper()
	for _, key := range []string{"RUNNERS_RPC_AUTH_MODE", "RUNNERS_RPC_POLICY", "RUNNERS_RPC_POLICY_FILE", "RUNNERS_TOKENREVIEW_TIMEOUT"} {
		t.Setenv(key, values[key])
	}
}

func TestLoadRPCAuthDefaultsToEnforce(t *testing.T) {
	setRPCAuthEnv(t, nil)
	cfg, err := loadRPCAuth()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Mode != rpcauth.ModeEnforce || cfg.Policy != nil || cfg.ReviewTimeout != rpcauth.DefaultReviewTimeout {
		t.Fatalf("defaults %+v", cfg)
	}
}

func TestLoadRPCAuthSources(t *testing.T) {
	setRPCAuthEnv(t, map[string]string{"RUNNERS_RPC_AUTH_MODE": "permissive", "RUNNERS_RPC_POLICY": ` {"version":1} `, "RUNNERS_TOKENREVIEW_TIMEOUT": "2s"})
	cfg, err := loadRPCAuth()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Mode != rpcauth.ModePermissive || string(cfg.Policy) != `{"version":1}` || cfg.ReviewTimeout != 2*time.Second {
		t.Fatalf("inline %+v", cfg)
	}

	path := filepath.Join(t.TempDir(), "policy.json")
	if err := os.WriteFile(path, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	setRPCAuthEnv(t, map[string]string{"RUNNERS_RPC_POLICY_FILE": path})
	cfg, err = loadRPCAuth()
	if err != nil || string(cfg.Policy) != `{"version":1}` {
		t.Fatalf("file %+v %v", cfg, err)
	}
}

func TestLoadRPCAuthRejections(t *testing.T) {
	big := filepath.Join(t.TempDir(), "big.json")
	if err := os.WriteFile(big, []byte(strings.Repeat(" ", maxRPCPolicyBytes+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, env := range map[string]map[string]string{
		"unknown mode":     {"RUNNERS_RPC_AUTH_MODE": "off"},
		"both sources":     {"RUNNERS_RPC_POLICY": "{}", "RUNNERS_RPC_POLICY_FILE": "/policy.json"},
		"missing file":     {"RUNNERS_RPC_POLICY_FILE": filepath.Join(t.TempDir(), "absent.json")},
		"oversized file":   {"RUNNERS_RPC_POLICY_FILE": big},
		"bad timeout":      {"RUNNERS_TOKENREVIEW_TIMEOUT": "soon"},
		"zero timeout":     {"RUNNERS_TOKENREVIEW_TIMEOUT": "0s"},
		"too long timeout": {"RUNNERS_TOKENREVIEW_TIMEOUT": "1m"},
	} {
		t.Run(name, func(t *testing.T) {
			setRPCAuthEnv(t, env)
			if _, err := loadRPCAuth(); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}
