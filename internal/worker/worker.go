// Package worker drains the delivery outbox. A pool of goroutines polls for due
// deliveries with SELECT ... FOR UPDATE SKIP LOCKED, POSTs each event payload to
// its subscription target with an HMAC signature header, and records the result:
// delivered on a 2xx, otherwise a retry scheduled by the backoff package, or
// "dead" once the attempt budget runs out.
package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/abdulrahman-muhammed-07/hookrelay/internal/backoff"
	"github.com/abdulrahman-muhammed-07/hookrelay/internal/config"
	"github.com/abdulrahman-muhammed-07/hookrelay/internal/signing"
	"github.com/abdulrahman-muhammed-07/hookrelay/internal/store"
)

// EventTypeHeader carries the event type on an outgoing delivery.
const EventTypeHeader = "X-Hookrelay-Event"

// DeliveryIDHeader carries the delivery row id on an outgoing delivery, so a
// receiver can dedupe replays.
const DeliveryIDHeader = "X-Hookrelay-Delivery"

// Pool is the delivery worker pool.
type Pool struct {
	store  *store.Store
	cfg    config.Config
	client *http.Client
	log    *slog.Logger
}

// New builds a pool. The HTTP client has no timeout of its own; each attempt is
// bounded by a per-request context deadline (cfg.HTTPTimeout) instead, so a slow
// body read cannot outlast the deadline.
func New(st *store.Store, cfg config.Config, log *slog.Logger) *Pool {
	return &Pool{
		store:  st,
		cfg:    cfg,
		client: &http.Client{},
		log:    log,
	}
}

// Run starts cfg.Workers goroutines and blocks until ctx is cancelled, then
// waits for the in-flight attempts to finish.
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < p.cfg.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			p.loop(ctx, id)
		}(i)
	}
	wg.Wait()
}

func (p *Pool) loop(ctx context.Context, id int) {
	log := p.log.With("worker", id)
	for {
		if ctx.Err() != nil {
			return
		}

		claims, err := p.store.ClaimDue(ctx, p.cfg.ClaimBatch)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("claim failed", "err", err)
			if !sleep(ctx, p.cfg.PollInterval) {
				return
			}
			continue
		}

		if len(claims) == 0 {
			if !sleep(ctx, p.cfg.PollInterval) {
				return
			}
			continue
		}

		for _, c := range claims {
			p.process(ctx, log, c)
		}
	}
}

// process runs one attempt and writes the outcome back to the store.
func (p *Pool) process(ctx context.Context, log *slog.Logger, c store.Claim) {
	code, err := deliver(ctx, p.client, c, p.cfg.HTTPTimeout, p.cfg.MaxResponseBytes)
	if err == nil {
		if mErr := p.store.MarkDelivered(ctx, c.DeliveryID, code); mErr != nil {
			log.Error("mark delivered failed", "delivery", c.DeliveryID, "err", mErr)
		}
		log.Info("delivered", "delivery", c.DeliveryID, "status", code, "attempt", c.Attempts+1)
		return
	}

	attemptsSoFar := c.Attempts + 1
	dead := attemptsSoFar >= c.MaxAttempts

	var codePtr *int
	if code != 0 {
		codePtr = &code
	}

	delay := backoff.Delay(p.cfg.BackoffBase, p.cfg.BackoffFactor, p.cfg.BackoffCap, attemptsSoFar)
	nextRetry := time.Now().Add(delay)

	if rfErr := p.store.RecordFailure(ctx, c.DeliveryID, codePtr, err.Error(), nextRetry, dead); rfErr != nil {
		log.Error("record failure failed", "delivery", c.DeliveryID, "err", rfErr)
		return
	}

	if dead {
		log.Warn("delivery dead", "delivery", c.DeliveryID, "attempts", attemptsSoFar, "err", err)
		return
	}
	log.Info("delivery retry scheduled", "delivery", c.DeliveryID,
		"attempt", attemptsSoFar, "retry_in", delay.String(), "err", err)
}

// deliver performs one HTTP POST attempt for a claim. It returns the response
// status code (0 when no response was received) and a non-nil error on any
// failure, a transport error or a non-2xx response alike. The response body is
// read (bounded by maxRespBytes) and, on failure, a short prefix of it is folded
// into the error so it shows up in GET /deliveries.
func deliver(ctx context.Context, client *http.Client, c store.Claim, timeout time.Duration, maxRespBytes int64) (int, error) {
	body := []byte(c.EventPayload)

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.TargetURL, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "hookrelay/1")
	req.Header.Set(signing.Header, signing.Sign(c.Secret, body))
	req.Header.Set(EventTypeHeader, c.EventType)
	req.Header.Set(DeliveryIDHeader, c.DeliveryID.String())

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("post: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxRespBytes))
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("target returned %d: %s", resp.StatusCode, preview(snippet))
	}
	return resp.StatusCode, nil
}

func preview(b []byte) string {
	const max = 200
	s := string(bytes.TrimSpace(b))
	if len(s) > max {
		return s[:max] + "..."
	}
	return s
}

// sleep waits d or until ctx is done. It reports false when ctx ended.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
