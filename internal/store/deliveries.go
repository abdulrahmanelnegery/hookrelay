package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const deliveryColumns = `id, event_id, subscription_id, status, attempts, max_attempts,
	last_status_code, last_error, next_retry_at, delivered_at, created_at, updated_at`

func scanDelivery(row pgx.Row) (Delivery, error) {
	var d Delivery
	err := row.Scan(
		&d.ID, &d.EventID, &d.SubscriptionID, &d.Status, &d.Attempts, &d.MaxAttempts,
		&d.LastStatusCode, &d.LastError, &d.NextRetryAt, &d.DeliveredAt, &d.CreatedAt, &d.UpdatedAt,
	)
	return d, err
}

// ClaimDue atomically claims up to limit pending deliveries whose next_retry_at
// has passed. It marks the claimed rows "processing" and returns everything a
// worker needs to attempt them, joined from the subscription and the event so
// the worker issues no further reads.
//
// The claim SELECT uses FOR UPDATE ... SKIP LOCKED, so two workers polling at the
// same time always get disjoint row sets: a row another transaction has locked is
// skipped rather than waited on.
func (s *Store) ClaimDue(ctx context.Context, limit int) ([]Claim, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: begin claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT d.id, d.attempts, d.max_attempts,
		       s.target_url, s.secret,
		       e.id, e.type, e.payload,
		       s.id
		FROM deliveries d
		JOIN subscriptions s ON s.id = d.subscription_id
		JOIN events e ON e.id = d.event_id
		WHERE d.status = 'pending' AND d.next_retry_at <= now()
		ORDER BY d.next_retry_at
		FOR UPDATE OF d SKIP LOCKED
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: select due deliveries: %w", err)
	}

	var (
		claims []Claim
		ids    []uuid.UUID
	)
	for rows.Next() {
		var c Claim
		if err := rows.Scan(
			&c.DeliveryID, &c.Attempts, &c.MaxAttempts,
			&c.TargetURL, &c.Secret,
			&c.EventID, &c.EventType, &c.EventPayload,
			&c.SubscriptionID,
		); err != nil {
			rows.Close()
			return nil, fmt.Errorf("store: scan claim: %w", err)
		}
		claims = append(claims, c)
		ids = append(ids, c.DeliveryID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate claims: %w", err)
	}

	if len(ids) > 0 {
		if _, err := tx.Exec(ctx,
			`UPDATE deliveries SET status = 'processing', updated_at = now() WHERE id = ANY ($1)`,
			ids,
		); err != nil {
			return nil, fmt.Errorf("store: mark processing: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("store: commit claim tx: %w", err)
	}
	return claims, nil
}

// MarkDelivered records a successful attempt: status "delivered", attempt counter
// bumped, error cleared, delivered_at stamped.
func (s *Store) MarkDelivered(ctx context.Context, id uuid.UUID, statusCode int) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE deliveries
		SET status = 'delivered',
		    attempts = attempts + 1,
		    last_status_code = $2,
		    last_error = NULL,
		    delivered_at = now(),
		    updated_at = now()
		WHERE id = $1`, id, statusCode)
	if err != nil {
		return fmt.Errorf("store: mark delivered: %w", err)
	}
	return nil
}

// RecordFailure records a failed attempt. It bumps the attempt counter and stores
// the status code (nil when the request never got a response) and error text. If
// dead is true the row moves to "dead"; otherwise it goes back to "pending" with
// next_retry_at set to nextRetry so a worker picks it up again after the backoff.
func (s *Store) RecordFailure(ctx context.Context, id uuid.UUID, statusCode *int, errMsg string, nextRetry time.Time, dead bool) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE deliveries
		SET status = CASE WHEN $5 THEN 'dead' ELSE 'pending' END,
		    attempts = attempts + 1,
		    last_status_code = $2,
		    last_error = $3,
		    next_retry_at = $4,
		    updated_at = now()
		WHERE id = $1`, id, statusCode, errMsg, nextRetry, dead)
	if err != nil {
		return fmt.Errorf("store: record failure: %w", err)
	}
	return nil
}

// GetDelivery returns one delivery by id, or ErrNotFound.
func (s *Store) GetDelivery(ctx context.Context, id uuid.UUID) (Delivery, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+deliveryColumns+` FROM deliveries WHERE id = $1`, id)
	d, err := scanDelivery(row)
	if err != nil {
		return Delivery{}, fmt.Errorf("store: get delivery: %w", notFound(err))
	}
	return d, nil
}

// ListDeliveries returns deliveries newest first, optionally filtered by status.
// An empty status means "any". limit caps the result size.
func (s *Store) ListDeliveries(ctx context.Context, status string, limit int) ([]Delivery, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+deliveryColumns+`
		FROM deliveries
		WHERE ($1 = '' OR status = $1)
		ORDER BY created_at DESC
		LIMIT $2`, status, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list deliveries: %w", err)
	}
	defer rows.Close()

	out := make([]Delivery, 0)
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan delivery: %w", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate deliveries: %w", err)
	}
	return out, nil
}

// Replay requeues a delivery that has finished (dead or delivered): status back
// to "pending", attempt budget reset, retry time set to now. It returns
// ErrNotFound when the id is unknown and ErrNotReplayable when the delivery is
// still pending or processing.
func (s *Store) Replay(ctx context.Context, id uuid.UUID) (Delivery, error) {
	current, err := s.GetDelivery(ctx, id)
	if err != nil {
		return Delivery{}, err
	}
	if current.Status != StatusDead && current.Status != StatusDelivered {
		return Delivery{}, fmt.Errorf("store: replay %s: %w", current.Status, ErrNotReplayable)
	}

	row := s.pool.QueryRow(ctx, `
		UPDATE deliveries
		SET status = 'pending',
		    attempts = 0,
		    last_status_code = NULL,
		    last_error = NULL,
		    delivered_at = NULL,
		    next_retry_at = now(),
		    updated_at = now()
		WHERE id = $1
		RETURNING `+deliveryColumns, id)
	d, err := scanDelivery(row)
	if err != nil {
		return Delivery{}, fmt.Errorf("store: replay update: %w", notFound(err))
	}
	return d, nil
}
