package store

import (
	"context"
	"encoding/json"
	"fmt"
)

// CreateEventWithDeliveries is the transactional outbox write. In a single
// transaction it inserts the event and one delivery row for every subscription
// whose filter matches the event type (an exact match, or the subscription
// listing "*"). The event and its full fan-out commit together or not at all, so
// a crash can never leave an accepted event with a partial set of deliveries.
//
// It returns the stored event and the number of delivery rows created.
func (s *Store) CreateEventWithDeliveries(ctx context.Context, eventType string, payload json.RawMessage, maxAttempts int) (Event, int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Event{}, 0, fmt.Errorf("store: begin event tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var ev Event
	err = tx.QueryRow(ctx, `
		INSERT INTO events (type, payload)
		VALUES ($1, $2)
		RETURNING id, type, payload, created_at`,
		eventType, []byte(payload),
	).Scan(&ev.ID, &ev.Type, &ev.Payload, &ev.CreatedAt)
	if err != nil {
		return Event{}, 0, fmt.Errorf("store: insert event: %w", err)
	}

	tag, err := tx.Exec(ctx, `
		INSERT INTO deliveries (event_id, subscription_id, max_attempts)
		SELECT $1, id, $2
		FROM subscriptions
		WHERE $3 = ANY (event_types) OR $4 = ANY (event_types)`,
		ev.ID, maxAttempts, eventType, MatchAll,
	)
	if err != nil {
		return Event{}, 0, fmt.Errorf("store: fan out deliveries: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Event{}, 0, fmt.Errorf("store: commit event tx: %w", err)
	}
	return ev, int(tag.RowsAffected()), nil
}
