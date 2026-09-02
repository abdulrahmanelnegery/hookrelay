package store

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const subscriptionColumns = `id, target_url, event_types, secret, created_at, updated_at`

func scanSubscription(row pgx.Row) (Subscription, error) {
	var s Subscription
	err := row.Scan(&s.ID, &s.TargetURL, &s.EventTypes, &s.Secret, &s.CreatedAt, &s.UpdatedAt)
	return s, err
}

// CreateSubscription inserts a subscription and returns the stored row.
func (s *Store) CreateSubscription(ctx context.Context, targetURL string, eventTypes []string, secret string) (Subscription, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO subscriptions (target_url, event_types, secret)
		VALUES ($1, $2, $3)
		RETURNING `+subscriptionColumns,
		targetURL, eventTypes, secret)
	sub, err := scanSubscription(row)
	if err != nil {
		return Subscription{}, fmt.Errorf("store: create subscription: %w", err)
	}
	return sub, nil
}

// GetSubscription looks up one subscription by id. It returns ErrNotFound when
// there is no match.
func (s *Store) GetSubscription(ctx context.Context, id uuid.UUID) (Subscription, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+subscriptionColumns+` FROM subscriptions WHERE id = $1`, id)
	sub, err := scanSubscription(row)
	if err != nil {
		return Subscription{}, fmt.Errorf("store: get subscription: %w", notFound(err))
	}
	return sub, nil
}

// ListSubscriptions returns all subscriptions, newest first.
func (s *Store) ListSubscriptions(ctx context.Context) ([]Subscription, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+subscriptionColumns+` FROM subscriptions ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("store: list subscriptions: %w", err)
	}
	defer rows.Close()

	out := make([]Subscription, 0)
	for rows.Next() {
		sub, err := scanSubscription(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan subscription: %w", err)
		}
		out = append(out, sub)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate subscriptions: %w", err)
	}
	return out, nil
}

// DeleteSubscription removes a subscription. It returns ErrNotFound when the id
// does not exist. Kept for symmetry and test cleanup; the HTTP layer does not
// expose deletion yet.
func (s *Store) DeleteSubscription(ctx context.Context, id uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM subscriptions WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("store: delete subscription: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
