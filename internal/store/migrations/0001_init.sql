-- hookrelay initial schema.
--
-- Three tables:
--   subscriptions : who wants which events, and the secret used to sign to them
--   events        : the raw events we accepted over HTTP
--   deliveries    : the outbox. one row per (event, matching subscription).
--                   the delivery worker pool drains this table.

-- +goose Up
CREATE TABLE subscriptions (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    target_url  text        NOT NULL,
    event_types text[]      NOT NULL,
    secret      text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Fan-out matches with "= ANY(event_types)", so index the array for containment.
CREATE INDEX subscriptions_event_types_idx ON subscriptions USING GIN (event_types);

CREATE TABLE events (
    id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    type       text        NOT NULL,
    payload    jsonb       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE deliveries (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id         uuid        NOT NULL REFERENCES events (id) ON DELETE RESTRICT,
    subscription_id  uuid        NOT NULL REFERENCES subscriptions (id) ON DELETE RESTRICT,
    status           text        NOT NULL DEFAULT 'pending',
    attempts         int         NOT NULL DEFAULT 0,
    max_attempts     int         NOT NULL,
    last_status_code int,
    last_error       text,
    next_retry_at    timestamptz NOT NULL DEFAULT now(),
    delivered_at     timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT deliveries_status_check
        CHECK (status IN ('pending', 'processing', 'delivered', 'dead'))
);

-- The worker claim query filters on (status, next_retry_at) and orders by
-- next_retry_at, so index exactly that.
CREATE INDEX deliveries_claim_idx
    ON deliveries (next_retry_at)
    WHERE status = 'pending';

CREATE INDEX deliveries_subscription_idx ON deliveries (subscription_id);
CREATE INDEX deliveries_status_idx ON deliveries (status);

-- +goose Down
DROP TABLE deliveries;
DROP TABLE events;
DROP TABLE subscriptions;
