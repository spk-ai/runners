package config

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/agynio/runners/internal/rpcauth"
)

const (
	defaultIdentityAddress       = "identity:50051"
	defaultAuthorizationAddress  = "authorization:50051"
	defaultAgentsAddress         = "agents:50051"
	defaultZitiManagementAddress = "ziti-management:50051"
	defaultNotificationsAddress  = "notifications:50051"
	defaultGRPCAddr              = ":50051"
	defaultWorkloadActivitySweep = 5 * time.Second
	defaultKeepaliveGrace        = 25 * time.Second
)

// Config captures runtime configuration derived from the environment.
type Config struct {
	DatabaseURL                   string
	IdentityAddress               string
	AuthorizationAddress          string
	AgentsAddress                 string
	ZitiManagementAddress         string
	NotificationsAddress          string
	ZitiLeaseRenewalInterval      time.Duration
	ZitiEnrollmentTimeout         time.Duration
	WorkloadActivitySweepInterval time.Duration
	WorkloadKeepaliveGrace        time.Duration
	GRPCAddr                      string
	// RPCAuth configures caller authorization; see rpcauth.Config.
	RPCAuth rpcauth.Config
}

// maxRPCPolicyBytes bounds RUNNERS_RPC_POLICY_FILE before parsing.
const maxRPCPolicyBytes = 64 * 1024

// Load reads configuration from environment variables, applying defaults when
// values are not provided. Returns an error when supplied values are invalid.
func Load() (Config, error) {
	var cfg Config

	cfg.DatabaseURL = strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL must be set")
	}

	cfg.IdentityAddress = readEnv("IDENTITY_ADDRESS", defaultIdentityAddress)
	cfg.AuthorizationAddress = readEnv("AUTHORIZATION_ADDRESS", defaultAuthorizationAddress)
	cfg.AgentsAddress = readEnv("AGENTS_ADDRESS", defaultAgentsAddress)
	cfg.ZitiManagementAddress = readEnv("ZITI_MANAGEMENT_ADDRESS", defaultZitiManagementAddress)
	cfg.NotificationsAddress = readEnv("NOTIFICATIONS_ADDRESS", defaultNotificationsAddress)
	leaseRenewalInterval := strings.TrimSpace(os.Getenv("ZITI_LEASE_RENEWAL_INTERVAL"))
	if leaseRenewalInterval == "" {
		cfg.ZitiLeaseRenewalInterval = 2 * time.Minute
	} else {
		parsed, err := time.ParseDuration(leaseRenewalInterval)
		if err != nil {
			return Config{}, fmt.Errorf("parse ZITI_LEASE_RENEWAL_INTERVAL: %w", err)
		}
		cfg.ZitiLeaseRenewalInterval = parsed
	}
	if cfg.ZitiLeaseRenewalInterval <= 0 {
		return Config{}, fmt.Errorf("ZITI_LEASE_RENEWAL_INTERVAL must be greater than 0")
	}

	enrollmentTimeout := strings.TrimSpace(os.Getenv("ZITI_ENROLLMENT_TIMEOUT"))
	if enrollmentTimeout == "" {
		cfg.ZitiEnrollmentTimeout = 2 * time.Minute
	} else {
		parsed, err := time.ParseDuration(enrollmentTimeout)
		if err != nil {
			return Config{}, fmt.Errorf("parse ZITI_ENROLLMENT_TIMEOUT: %w", err)
		}
		cfg.ZitiEnrollmentTimeout = parsed
	}
	if cfg.ZitiEnrollmentTimeout <= 0 {
		return Config{}, fmt.Errorf("ZITI_ENROLLMENT_TIMEOUT must be greater than 0")
	}

	activitySweepInterval := strings.TrimSpace(os.Getenv("WORKLOAD_ACTIVITY_SWEEP_INTERVAL"))
	if activitySweepInterval == "" {
		cfg.WorkloadActivitySweepInterval = defaultWorkloadActivitySweep
	} else {
		parsed, err := time.ParseDuration(activitySweepInterval)
		if err != nil {
			return Config{}, fmt.Errorf("parse WORKLOAD_ACTIVITY_SWEEP_INTERVAL: %w", err)
		}
		cfg.WorkloadActivitySweepInterval = parsed
	}
	if cfg.WorkloadActivitySweepInterval <= 0 {
		return Config{}, fmt.Errorf("WORKLOAD_ACTIVITY_SWEEP_INTERVAL must be greater than 0")
	}

	keepaliveGrace := strings.TrimSpace(os.Getenv("KEEPALIVE_GRACE"))
	if keepaliveGrace == "" {
		cfg.WorkloadKeepaliveGrace = defaultKeepaliveGrace
	} else {
		parsed, err := time.ParseDuration(keepaliveGrace)
		if err != nil {
			return Config{}, fmt.Errorf("parse KEEPALIVE_GRACE: %w", err)
		}
		cfg.WorkloadKeepaliveGrace = parsed
	}
	if cfg.WorkloadKeepaliveGrace <= 0 {
		return Config{}, fmt.Errorf("KEEPALIVE_GRACE must be greater than 0")
	}
	cfg.GRPCAddr = readEnv("GRPC_ADDR", defaultGRPCAddr)

	rpcAuth, err := loadRPCAuth()
	if err != nil {
		return Config{}, err
	}
	cfg.RPCAuth = rpcAuth

	return cfg, nil
}

// loadRPCAuth reads the caller-authorization settings. Enforcement is the
// default; permissive mode must be requested explicitly. Policy validation
// happens in rpcauth.Setup, before any other startup side effect.
func loadRPCAuth() (rpcauth.Config, error) {
	mode, err := rpcauth.ParseMode(os.Getenv("RUNNERS_RPC_AUTH_MODE"))
	if err != nil {
		return rpcauth.Config{}, fmt.Errorf("RUNNERS_RPC_AUTH_MODE: %w", err)
	}
	cfg := rpcauth.Config{Mode: mode, ReviewTimeout: rpcauth.DefaultReviewTimeout}

	inline := strings.TrimSpace(os.Getenv("RUNNERS_RPC_POLICY"))
	path := strings.TrimSpace(os.Getenv("RUNNERS_RPC_POLICY_FILE"))
	switch {
	case inline != "" && path != "":
		return rpcauth.Config{}, fmt.Errorf("set only one of RUNNERS_RPC_POLICY and RUNNERS_RPC_POLICY_FILE")
	case inline != "":
		cfg.Policy = []byte(inline)
	case path != "":
		file, err := os.Open(path)
		if err != nil {
			return rpcauth.Config{}, fmt.Errorf("open RUNNERS_RPC_POLICY_FILE: %w", err)
		}
		defer file.Close()
		data, err := io.ReadAll(io.LimitReader(file, maxRPCPolicyBytes+1))
		if err != nil {
			return rpcauth.Config{}, fmt.Errorf("read RUNNERS_RPC_POLICY_FILE: %w", err)
		}
		if len(data) > maxRPCPolicyBytes {
			return rpcauth.Config{}, fmt.Errorf("RUNNERS_RPC_POLICY_FILE exceeds %d bytes", maxRPCPolicyBytes)
		}
		cfg.Policy = data
	}

	if timeout := strings.TrimSpace(os.Getenv("RUNNERS_TOKENREVIEW_TIMEOUT")); timeout != "" {
		parsed, err := time.ParseDuration(timeout)
		if err != nil {
			return rpcauth.Config{}, fmt.Errorf("parse RUNNERS_TOKENREVIEW_TIMEOUT: %w", err)
		}
		if parsed <= 0 || parsed > 30*time.Second {
			return rpcauth.Config{}, fmt.Errorf("RUNNERS_TOKENREVIEW_TIMEOUT must be in (0, 30s]")
		}
		cfg.ReviewTimeout = parsed
	}
	return cfg, nil
}

func readEnv(key, def string) string {
	if value, ok := os.LookupEnv(key); ok {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return def
}
