// Package pgtest spins up a throwaway Postgres for the DB-backed tests using
// testcontainers. It is import-guarded by the callers: every test that uses it
// skips when -short is set or when there is no reachable Docker daemon, so
// `go test ./...` still passes on a machine without Docker.
package pgtest

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/abdulrahman-muhammed-07/hookrelay/internal/store"
)

// DockerAvailable reports whether a Docker daemon looks reachable.
func DockerAvailable() bool {
	if os.Getenv("DOCKER_HOST") != "" {
		return true
	}
	_, err := os.Stat("/var/run/docker.sock")
	return err == nil
}

// SkipIfNoDocker skips the calling test when the DB-backed path cannot run.
func SkipIfNoDocker(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping DB-backed test in -short mode")
	}
	if !DockerAvailable() {
		t.Skip("skipping DB-backed test: no Docker daemon (set DOCKER_HOST or run with a docker socket)")
	}
}

// NewStore starts a Postgres container, runs migrations, and returns a ready
// Store. The container and pool are torn down via t.Cleanup.
func NewStore(t *testing.T) *store.Store {
	t.Helper()
	SkipIfNoDocker(t)

	ctx := context.Background()
	container, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("hookrelay"),
		postgres.WithUsername("hookrelay"),
		postgres.WithPassword("hookrelay"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		_ = container.Terminate(context.Background())
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("container connection string: %v", err)
	}

	if err := store.Migrate(ctx, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	st, err := store.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(st.Close)
	return st
}
