// Package config loads hookrelay's runtime configuration from the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds everything the service needs to run. Every field has a default so
// the zero-effort path (just set DATABASE_URL) works.
type Config struct {
	// HTTPAddr is the listen address for the admin/API server.
	HTTPAddr string
	// DatabaseURL is a libpq-style connection string for Postgres.
	DatabaseURL string

	// Workers is the number of delivery worker goroutines.
	Workers int
	// ClaimBatch is how many deliveries a worker claims per database round trip.
	ClaimBatch int
	// PollInterval is how long a worker waits after finding no work before
	// looking again.
	PollInterval time.Duration

	// MaxAttempts is the number of delivery attempts before a delivery is moved
	// to the dead-letter state.
	MaxAttempts int
	// BackoffBase is the first retry delay; each subsequent retry multiplies it
	// by BackoffFactor.
	BackoffBase   time.Duration
	BackoffFactor float64
	// BackoffCap clamps the retry delay so late attempts do not wait absurdly
	// long.
	BackoffCap time.Duration

	// HTTPTimeout bounds a single POST to a subscriber endpoint.
	HTTPTimeout time.Duration
	// MaxResponseBytes caps how much of a subscriber response body we read.
	MaxResponseBytes int64

	// ShutdownGrace bounds how long we wait for in-flight deliveries to drain.
	ShutdownGrace time.Duration
}

// Load reads configuration from the environment, applying defaults. It returns an
// error only when a required value is missing or a provided value is malformed.
func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:         getString("HTTP_ADDR", ":8080"),
		DatabaseURL:      os.Getenv("DATABASE_URL"),
		Workers:          getInt("WORKERS", 4),
		ClaimBatch:       getInt("CLAIM_BATCH", 10),
		PollInterval:     getDuration("POLL_INTERVAL", time.Second),
		MaxAttempts:      getInt("MAX_ATTEMPTS", 5),
		BackoffBase:      getDuration("BACKOFF_BASE", 10*time.Second),
		BackoffFactor:    getFloat("BACKOFF_FACTOR", 4),
		BackoffCap:       getDuration("BACKOFF_CAP", time.Hour),
		HTTPTimeout:      getDuration("HTTP_TIMEOUT", 10*time.Second),
		MaxResponseBytes: int64(getInt("MAX_RESPONSE_BYTES", 64*1024)),
		ShutdownGrace:    getDuration("SHUTDOWN_GRACE", 25*time.Second),
	}

	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("config: DATABASE_URL is required")
	}
	if cfg.Workers < 1 {
		return Config{}, fmt.Errorf("config: WORKERS must be >= 1, got %d", cfg.Workers)
	}
	if cfg.MaxAttempts < 1 {
		return Config{}, fmt.Errorf("config: MAX_ATTEMPTS must be >= 1, got %d", cfg.MaxAttempts)
	}
	return cfg, nil
}

func getString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func getFloat(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}

func getDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
