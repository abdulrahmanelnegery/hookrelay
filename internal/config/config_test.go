package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	clearEnv(t)
	t.Setenv("DATABASE_URL", "postgres://localhost/hookrelay")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want :8080", cfg.HTTPAddr)
	}
	if cfg.Workers != 4 {
		t.Errorf("Workers = %d, want 4", cfg.Workers)
	}
	if cfg.MaxAttempts != 5 {
		t.Errorf("MaxAttempts = %d, want 5", cfg.MaxAttempts)
	}
	if cfg.BackoffBase != 10*time.Second {
		t.Errorf("BackoffBase = %s, want 10s", cfg.BackoffBase)
	}
	if cfg.BackoffCap != time.Hour {
		t.Errorf("BackoffCap = %s, want 1h", cfg.BackoffCap)
	}
	if cfg.HTTPTimeout != 10*time.Second {
		t.Errorf("HTTPTimeout = %s, want 10s", cfg.HTTPTimeout)
	}
}

func TestLoadOverrides(t *testing.T) {
	clearEnv(t)
	t.Setenv("DATABASE_URL", "postgres://db/hr")
	t.Setenv("HTTP_ADDR", ":9000")
	t.Setenv("WORKERS", "16")
	t.Setenv("MAX_ATTEMPTS", "3")
	t.Setenv("BACKOFF_BASE", "5s")
	t.Setenv("BACKOFF_FACTOR", "2.5")
	t.Setenv("BACKOFF_CAP", "30m")
	t.Setenv("POLL_INTERVAL", "250ms")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.HTTPAddr != ":9000" {
		t.Errorf("HTTPAddr = %q, want :9000", cfg.HTTPAddr)
	}
	if cfg.Workers != 16 {
		t.Errorf("Workers = %d, want 16", cfg.Workers)
	}
	if cfg.MaxAttempts != 3 {
		t.Errorf("MaxAttempts = %d, want 3", cfg.MaxAttempts)
	}
	if cfg.BackoffBase != 5*time.Second {
		t.Errorf("BackoffBase = %s, want 5s", cfg.BackoffBase)
	}
	if cfg.BackoffFactor != 2.5 {
		t.Errorf("BackoffFactor = %v, want 2.5", cfg.BackoffFactor)
	}
	if cfg.BackoffCap != 30*time.Minute {
		t.Errorf("BackoffCap = %s, want 30m", cfg.BackoffCap)
	}
	if cfg.PollInterval != 250*time.Millisecond {
		t.Errorf("PollInterval = %s, want 250ms", cfg.PollInterval)
	}
}

func TestLoadRequiresDatabaseURL(t *testing.T) {
	clearEnv(t)
	if _, err := Load(); err == nil {
		t.Fatal("Load() succeeded without DATABASE_URL, want error")
	}
}

func TestLoadRejectsBadWorkers(t *testing.T) {
	clearEnv(t)
	t.Setenv("DATABASE_URL", "postgres://db/hr")
	t.Setenv("WORKERS", "0")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted WORKERS=0, want error")
	}
}

func TestLoadIgnoresMalformedDurationAndFallsBackToDefault(t *testing.T) {
	clearEnv(t)
	t.Setenv("DATABASE_URL", "postgres://db/hr")
	t.Setenv("BACKOFF_BASE", "not-a-duration")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.BackoffBase != 10*time.Second {
		t.Errorf("BackoffBase = %s, want default 10s on malformed input", cfg.BackoffBase)
	}
}

// clearEnv unsets every variable Load reads so each test starts from a known
// blank slate. t.Setenv restores originals at test end.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"HTTP_ADDR", "DATABASE_URL", "WORKERS", "CLAIM_BATCH", "POLL_INTERVAL",
		"MAX_ATTEMPTS", "BACKOFF_BASE", "BACKOFF_FACTOR", "BACKOFF_CAP",
		"HTTP_TIMEOUT", "MAX_RESPONSE_BYTES", "SHUTDOWN_GRACE",
	} {
		t.Setenv(k, "")
	}
	// t.Setenv with "" sets an empty value, which Load treats as unset for all
	// getters. Good enough and auto-restored.
}
