package store

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// DeliveryStatus enumerates the states an outbox row moves through.
type DeliveryStatus string

const (
	StatusPending    DeliveryStatus = "pending"
	StatusProcessing DeliveryStatus = "processing"
	StatusDelivered  DeliveryStatus = "delivered"
	StatusDead       DeliveryStatus = "dead"
)

// Valid reports whether s is a known delivery status.
func (s DeliveryStatus) Valid() bool {
	switch s {
	case StatusPending, StatusProcessing, StatusDelivered, StatusDead:
		return true
	default:
		return false
	}
}

// MatchAll is the event type that, when present in a subscription's filter,
// matches every event regardless of type.
const MatchAll = "*"

// Subscription is a registered receiver: a target URL, an event-type filter, and
// the secret used to sign requests sent to it. An event type of "*" in
// EventTypes matches every event.
type Subscription struct {
	ID         uuid.UUID `json:"id"`
	TargetURL  string    `json:"target_url"`
	EventTypes []string  `json:"event_types"`
	Secret     string    `json:"secret"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// Event is an accepted event awaiting fan-out to matching subscriptions.
type Event struct {
	ID        uuid.UUID       `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

// Delivery is one outbox row: the intent to deliver one event to one subscriber.
type Delivery struct {
	ID             uuid.UUID      `json:"id"`
	EventID        uuid.UUID      `json:"event_id"`
	SubscriptionID uuid.UUID      `json:"subscription_id"`
	Status         DeliveryStatus `json:"status"`
	Attempts       int            `json:"attempts"`
	MaxAttempts    int            `json:"max_attempts"`
	LastStatusCode *int           `json:"last_status_code"`
	LastError      *string        `json:"last_error"`
	NextRetryAt    time.Time      `json:"next_retry_at"`
	DeliveredAt    *time.Time     `json:"delivered_at"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

// Claim is the slim projection a worker needs to attempt a delivery. It carries
// the subscriber details joined from the subscription and the raw event so the
// worker never issues a second query.
type Claim struct {
	DeliveryID     uuid.UUID
	Attempts       int
	MaxAttempts    int
	TargetURL      string
	Secret         string
	EventID        uuid.UUID
	EventType      string
	EventPayload   json.RawMessage
	SubscriptionID uuid.UUID
}
