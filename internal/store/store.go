// Package store is hookrelay's data-access layer. It owns the pgx connection
// pool and every SQL query the service runs. Nothing outside this package builds
// SQL. Schema migrations live in migrations/ and are applied with goose.
package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver "pgx", used only for migrations
	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// ErrNotFound is returned by lookups when no row matches.
var ErrNotFound = errors.New("store: not found")

// ErrNotReplayable is returned by Replay when the delivery exists but is not in
// a state that may be replayed (only dead and delivered rows can be).
var ErrNotReplayable = errors.New("store: delivery is not in a replayable state")

// Store wraps a pgx pool with hookrelay's queries.
type Store struct {
	pool *pgxpool.Pool
}

// New builds a connection pool from a libpq-style URL and pings it.
func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("store: open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// FromPool wraps an existing pool. Tests use this to share a container pool.
func FromPool(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Pool exposes the underlying pool for tests that need raw access.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases all pooled connections.
func (s *Store) Close() { s.pool.Close() }

// Migrate applies every embedded migration that has not run yet, in filename
// order, using goose. It opens its own short-lived database/sql connection
// because goose works against *sql.DB, not a pgx pool. Safe to call on every boot.
func Migrate(ctx context.Context, databaseURL string) error {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("store: open migration db: %w", err)
	}
	defer func() { _ = db.Close() }()

	goose.SetBaseFS(migrationFiles)
	goose.SetLogger(goose.NopLogger())
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("store: goose dialect: %w", err)
	}
	if err := goose.UpContext(ctx, db, "migrations"); err != nil {
		return fmt.Errorf("store: run migrations: %w", err)
	}
	return nil
}

// notFound maps pgx's no-rows sentinel to the package sentinel.
func notFound(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}
