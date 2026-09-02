# hookrelay

A small webhook delivery service: accept an event once, fan it out to every
subscriber, and keep retrying each delivery until it succeeds or runs out of
attempts.

## Why this exists

I built this to learn Go and Gin properly, end to end: the standard library
HTTP server and `log/slog`, `context` for cancellation and timeouts, goroutine
pools and graceful shutdown, `pgx` against real Postgres, the transactional
outbox pattern, and `SELECT ... FOR UPDATE SKIP LOCKED` for safe concurrent job
claiming. The scope is deliberately narrow so the code stays readable. It is not
a product and it borrows no code from anywhere; it is a from-scratch practice
project with a real quality bar (tests, CI, linting, one-command run).

## What it does

1. You register subscriptions: a target URL, a list of event types to receive,
   and a secret used to sign requests sent to that target.
2. You post events (a type plus a JSON payload). For every subscription whose
   filter matches, hookrelay writes one delivery row into an outbox table, in
   the same database transaction as the event insert.
3. A pool of worker goroutines drains the outbox. Each worker claims a batch of
   due deliveries with `FOR UPDATE SKIP LOCKED`, POSTs the payload to the target
   with an `X-Hookrelay-Signature: sha256=<hmac>` header, and records the result.
4. A failed delivery is retried on an exponential backoff schedule. After the
   last attempt it is marked `dead`.
5. A `dead` or `delivered` delivery can be replayed, which requeues it.

## Architecture

```
                 POST /events                        POST /subscriptions
                      |                                     |
                      v                                     v
        +-----------------------------+        +---------------------------+
        |         Gin HTTP API        |        |   subscriptions table     |
        |  (internal/api)             |        +---------------------------+
        +--------------+--------------+                     ^
                       |  one transaction                  | filter match
                       v                                   |
        +---------------------------------------------------+-----------+
        |  events (raw)  +  deliveries (outbox, one row per subscriber) |
        |                        Postgres                               |
        +------------------------------+-------------------------------- +
                                       ^  SELECT ... FOR UPDATE SKIP LOCKED
                                       |  UPDATE status, attempts, next_retry_at
                       +---------------+----------------+
                       |     worker pool (internal/worker)|
                       |  N goroutines, poll -> POST ->    |
                       |  sign -> record result / backoff  |
                       +---------------+------------------+
                                       |  POST body + HMAC-SHA256 signature
                                       v
                              subscriber target URLs
```

### The transactional outbox

The write path for `POST /events` is a single transaction:

```
BEGIN;
  INSERT INTO events (...) RETURNING id;
  INSERT INTO deliveries (event_id, subscription_id, max_attempts)
    SELECT :event_id, id, :max_attempts
    FROM subscriptions
    WHERE :type = ANY(event_types) OR '*' = ANY(event_types);
COMMIT;
```

Either the event and its full set of delivery rows are all committed, or none of
them are. There is no window where an event is accepted but its deliveries are
missing, and no second system (a queue broker) that could disagree with the
database about what was accepted. The outbox table is the queue.

### The worker pool

Each worker runs the same loop: claim, attempt, record, repeat; sleep briefly
when there is nothing due. Claiming is one short transaction:

```
BEGIN;
  SELECT d.id, d.attempts, ..., s.target_url, s.secret, e.type, e.payload
  FROM deliveries d
  JOIN subscriptions s ON s.id = d.subscription_id
  JOIN events e ON e.id = d.event_id
  WHERE d.status = 'pending' AND d.next_retry_at <= now()
  ORDER BY d.next_retry_at
  FOR UPDATE OF d SKIP LOCKED
  LIMIT :batch;
  UPDATE deliveries SET status = 'processing' WHERE id = ANY(:claimed);
COMMIT;
```

`SKIP LOCKED` means a row another worker has already locked is passed over
instead of blocked on, so two workers polling at the same moment always get
disjoint sets of rows. The claim query joins in everything an attempt needs, so
a worker never issues a second read before making the HTTP call.

After the attempt:

- 2xx response: status `delivered`, `delivered_at` stamped.
- anything else (non-2xx, timeout, connection error): the attempt counter is
  incremented and the error text stored. If attempts are left, status goes back
  to `pending` with `next_retry_at = now() + backoff(attempt)`. If not, status
  becomes `dead`.

### Backoff schedule

`internal/backoff` computes `base * factor^(n-1)`, clamped to a ceiling. With the
defaults (`BACKOFF_BASE=10s`, `BACKOFF_FACTOR=4`, `BACKOFF_CAP=1h`,
`MAX_ATTEMPTS=5`) the waits between the 5 attempts are:

```
10s, 40s, 2m40s, 10m40s, 42m40s
```

All four knobs are environment variables, so you can set a schedule like
`10s, 30s, 2m, 10m, 1h` by tuning base, factor, and cap.

### Signing

`X-Hookrelay-Signature` is `sha256=` followed by the hex HMAC-SHA256 of the exact
request body under the subscription's secret. Receivers should recompute it over
the raw bytes they receive and compare in constant time. Note that payloads are
stored as Postgres `jsonb`, so key order and whitespace are normalised; the
signature always matches the bytes hookrelay actually sends, which is what a
receiver verifies against.

## Running it

```
docker compose up --build
```

That starts Postgres and the service. The service runs migrations on boot and
listens on `localhost:8080`. Configuration is all environment variables with
defaults; see `internal/config/config.go`. The only required one is
`DATABASE_URL`, which `docker-compose.yml` sets for you.

Run locally without Docker for the app itself:

```
export DATABASE_URL='postgres://hookrelay:hookrelay@localhost:5432/hookrelay?sslmode=disable'
go run ./cmd/hookrelay
```

## Endpoints

| Method | Path                        | Body                                  | Success | Notes |
|--------|-----------------------------|---------------------------------------|---------|-------|
| GET    | `/healthz`                  | none                                  | 200     | Pings the database. |
| POST   | `/subscriptions`            | `{target_url, event_types[], secret}` | 201     | `target_url` must be http/https; `secret` at least 8 chars; use `"*"` in `event_types` to match everything. |
| GET    | `/subscriptions`            | none                                  | 200     | Newest first. |
| POST   | `/events`                   | `{type, payload}`                     | 202     | `payload` is any JSON value. Returns the event and `deliveries_created`. |
| GET    | `/deliveries`               | none                                  | 200     | Query: `status` (`pending`/`processing`/`delivered`/`dead`), `limit` (default 100, max 500). Each row carries `attempts`, `last_status_code`, `last_error`. |
| POST   | `/deliveries/{id}/replay`   | none                                  | 200     | Requeues a `dead` or `delivered` delivery. 409 if it is still pending/processing, 404 if unknown. |

Example:

```
curl -X POST localhost:8080/subscriptions \
  -H 'content-type: application/json' \
  -d '{"target_url":"https://example.com/hook","event_types":["order.created"],"secret":"supersecret"}'

curl -X POST localhost:8080/events \
  -H 'content-type: application/json' \
  -d '{"type":"order.created","payload":{"order_id":42}}'

curl 'localhost:8080/deliveries?status=dead'
```

## Tests

```
go test ./...
```

- Pure-logic tests always run: HMAC signing against known digests
  (`internal/signing`), the backoff schedule shape and cap
  (`internal/backoff`), config parsing and defaults (`internal/config`), and the
  worker's single HTTP attempt against an `httptest` server, including the
  signature header, timeout, and non-2xx handling (`internal/worker`).
- DB-backed tests (`internal/store`, `internal/api`) use
  `testcontainers-go` to run a real Postgres in Docker. They skip automatically
  under `go test -short` or when no Docker daemon is reachable, so
  `go test ./...` still passes without Docker.
- The outbox concurrency test starts two workers claiming the same set of
  due deliveries at once and asserts every row is handed to exactly one worker
  (the `SKIP LOCKED` guarantee).

## Layout

```
cmd/hookrelay/        process entrypoint: wires config, store, worker pool, HTTP server, graceful shutdown
internal/config/      environment configuration with defaults
internal/signing/     HMAC-SHA256 request signing and verification
internal/backoff/     exponential retry schedule
internal/store/       pgx pool, goose migrations, all SQL (subscriptions, events+outbox, deliveries)
internal/worker/      worker pool: claim, POST, sign, record result, schedule retry
internal/api/         Gin router and handlers
internal/pgtest/      testcontainers Postgres helper for the DB-backed tests
```

## Known limits

Deliberately out of scope for a learning project: authentication on the admin
API, per-subscription rate limiting, a dead-letter inspection UI, payload size
limits beyond what Postgres accepts, and horizontal scaling of the worker beyond
one process (the `SKIP LOCKED` claim already makes multi-process safe, but it is
not deployed or tested that way here). A `processing` row whose worker crashes
mid-attempt stays `processing`; a reaper to reset stale ones is a natural next
step and is not implemented.
