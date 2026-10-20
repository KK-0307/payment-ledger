// Package config reads service configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"paymentledger/internal/payments"
	"paymentledger/internal/stripe"
)

type Config struct {
	ListenAddr     string
	DatabaseURL    string
	DBMaxConns     int32
	MigrateOnStart bool

	APIKeys []string

	StripeBaseURL        string
	StripeSecretKey      string
	StripeWebhookSecrets []string
	ProcessorTimeout     time.Duration

	OutboxURL         string
	OutboxSecret      string
	OutboxInterval    time.Duration
	OutboxClaimFor    time.Duration
	RecoveryInterval  time.Duration
	RecoveryBatch     int
	IdempotencyKeyTTL time.Duration

	Payments payments.Config
	LogLevel string
}

// Load reads the environment. See README.md for the full list.
func Load() (*Config, error) {
	p := payments.DefaultConfig()
	c := &Config{
		ListenAddr:           env("LISTEN_ADDR", ":8080"),
		DatabaseURL:          os.Getenv("DATABASE_URL"),
		DBMaxConns:           int32(envInt("DB_MAX_CONNS", 32)),
		MigrateOnStart:       envBool("MIGRATE_ON_START", true),
		APIKeys:              split(os.Getenv("API_KEYS")),
		StripeBaseURL:        env("STRIPE_API_BASE", stripe.DefaultBaseURL),
		StripeSecretKey:      os.Getenv("STRIPE_SECRET_KEY"),
		StripeWebhookSecrets: split(os.Getenv("STRIPE_WEBHOOK_SECRET")),
		ProcessorTimeout:     envDur("PROCESSOR_TIMEOUT", 10*time.Second),
		OutboxURL:            os.Getenv("OUTBOX_WEBHOOK_URL"),
		OutboxSecret:         os.Getenv("OUTBOX_WEBHOOK_SECRET"),
		OutboxInterval:       envDur("OUTBOX_INTERVAL", 200*time.Millisecond),
		OutboxClaimFor:       envDur("OUTBOX_CLAIM_FOR", 30*time.Second),
		RecoveryInterval:     envDur("RECOVERY_INTERVAL", time.Second),
		RecoveryBatch:        envInt("RECOVERY_BATCH", 100),
		IdempotencyKeyTTL:    envDur("IDEMPOTENCY_KEY_TTL", 24*time.Hour),
		LogLevel:             env("LOG_LEVEL", "info"),
	}
	p.FeeBps = int64(envInt("FEE_BPS", int(p.FeeBps)))
	p.FeeFixed = int64(envInt("FEE_FIXED", int(p.FeeFixed)))
	p.LedgerShards = envInt("LEDGER_SHARDS", p.LedgerShards)
	p.PayableShardRate = envFloat("LEDGER_PAYABLE_SHARD_RATE", p.PayableShardRate)
	p.PayableMaxShards = envInt("LEDGER_PAYABLE_MAX_SHARDS", p.PayableMaxShards)
	p.Lease = envDur("LEASE", p.Lease)
	p.InflightWait = envDur("INFLIGHT_WAIT", p.InflightWait)
	p.RetryBase = envDur("RECOVERY_BACKOFF_BASE", p.RetryBase)
	p.RetryMax = envDur("RECOVERY_BACKOFF_MAX", p.RetryMax)
	p.MaxAttempts = envInt("RECOVERY_MAX_ATTEMPTS", p.MaxAttempts)
	p.LookupAfter = envInt("LOOKUP_AFTER_ATTEMPTS", p.LookupAfter)
	p.InlineRetries = envInt("PROCESSOR_INLINE_RETRIES", p.InlineRetries)
	p.InlineDelay = envDur("PROCESSOR_INLINE_DELAY", p.InlineDelay)
	p.RequestBudget = envDur("REQUEST_BUDGET", p.RequestBudget)
	switch m := env("IDEMPOTENCY_MODE", "on"); m {
	case "on":
	case "unsafe-off":
		p.Unsafe = true
	default:
		return nil, fmt.Errorf("IDEMPOTENCY_MODE must be on or unsafe-off, got %q", m)
	}
	c.Payments = p

	if c.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	if len(c.APIKeys) == 0 {
		return nil, fmt.Errorf("API_KEYS is required (comma-separated bearer tokens)")
	}
	if err := stripe.CheckKey(c.StripeBaseURL, c.StripeSecretKey); err != nil {
		return nil, err
	}
	if p.LedgerShards < 1 {
		return nil, fmt.Errorf("LEDGER_SHARDS must be at least 1")
	}
	if p.PayableShardRate < 0 || p.PayableMaxShards < 1 {
		return nil, fmt.Errorf("LEDGER_PAYABLE_SHARD_RATE must be >= 0 and LEDGER_PAYABLE_MAX_SHARDS at least 1")
	}
	// Holders renew their lease every LEASE/3, so LEASE need not exceed the
	// processor timeout; it only has to leave room for a few heartbeats.
	if p.Lease < time.Second {
		return nil, fmt.Errorf("LEASE must be at least 1s")
	}
	return c, nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envBool(k string, def bool) bool {
	if v := os.Getenv(k); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func split(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
