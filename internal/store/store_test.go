package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/abdulrahman-muhammed-07/hookrelay/internal/pgtest"
	"github.com/abdulrahman-muhammed-07/hookrelay/internal/store"
)

func TestCreateEventWithDeliveriesFansOutToMatchingSubscriptions(t *testing.T) {
	st := pgtest.NewStore(t)
	ctx := context.Background()

	mustSub(t, st, "https://a.example/hook", []string{"order.created"})
	mustSub(t, st, "https://b.example/hook", []string{"order.created", "order.paid"})
	mustSub(t, st, "https://c.example/hook", []string{store.MatchAll})
	mustSub(t, st, "https://d.example/hook", []string{"user.signup"})

	_, count, err := st.CreateEventWithDeliveries(ctx, "order.created", json.RawMessage(`{"id":1}`), 5)
	if err != nil {
		t.Fatalf("CreateEventWithDeliveries: %v", err)
	}
	// a (exact), b (exact in list), c (wildcard). Not d.
	if count != 3 {
		t.Fatalf("deliveries_created = %d, want 3", count)
	}

	pending, err := st.ListDeliveries(ctx, string(store.StatusPending), 100)
	if err != nil {
		t.Fatalf("ListDeliveries: %v", err)
	}
	if len(pending) != 3 {
		t.Fatalf("pending deliveries = %d, want 3", len(pending))
	}
	for _, d := range pending {
		if d.MaxAttempts != 5 {
			t.Errorf("delivery %s MaxAttempts = %d, want 5", d.ID, d.MaxAttempts)
		}
		if d.Attempts != 0 {
			t.Errorf("delivery %s Attempts = %d, want 0", d.ID, d.Attempts)
		}
	}
}

func TestCreateEventWithNoMatchCreatesNoDeliveries(t *testing.T) {
	st := pgtest.NewStore(t)
	ctx := context.Background()

	mustSub(t, st, "https://a.example/hook", []string{"order.created"})

	_, count, err := st.CreateEventWithDeliveries(ctx, "nobody.listening", json.RawMessage(`{}`), 5)
	if err != nil {
		t.Fatalf("CreateEventWithDeliveries: %v", err)
	}
	if count != 0 {
		t.Fatalf("deliveries_created = %d, want 0", count)
	}
}

// TestClaimDueSkipLockedNeverHandsTheSameRowToTwoWorkers is the core concurrency
// guarantee: two workers polling the outbox at the same time must get disjoint
// row sets, and between them they must claim every due row exactly once.
func TestClaimDueSkipLockedNeverHandsTheSameRowToTwoWorkers(t *testing.T) {
	st := pgtest.NewStore(t)
	ctx := context.Background()

	const n = 40
	for i := 0; i < n; i++ {
		mustSub(t, st, "https://sink.example/hook", []string{store.MatchAll})
	}
	_, count, err := st.CreateEventWithDeliveries(ctx, "load.test", json.RawMessage(`{"n":1}`), 5)
	if err != nil {
		t.Fatalf("CreateEventWithDeliveries: %v", err)
	}
	if count != n {
		t.Fatalf("seeded %d deliveries, want %d", count, n)
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		claimed = map[string]int{}
	)
	worker := func() {
		defer wg.Done()
		// Loop so that whichever worker runs second still drains what is left.
		for {
			batch, err := st.ClaimDue(ctx, n)
			if err != nil {
				t.Errorf("ClaimDue: %v", err)
				return
			}
			if len(batch) == 0 {
				return
			}
			mu.Lock()
			for _, c := range batch {
				claimed[c.DeliveryID.String()]++
			}
			mu.Unlock()
		}
	}

	wg.Add(2)
	go worker()
	go worker()
	wg.Wait()

	if len(claimed) != n {
		t.Fatalf("distinct claimed rows = %d, want %d", len(claimed), n)
	}
	for id, times := range claimed {
		if times != 1 {
			t.Errorf("row %s claimed %d times, want exactly 1", id, times)
		}
	}

	processing, err := st.ListDeliveries(ctx, string(store.StatusProcessing), 100)
	if err != nil {
		t.Fatalf("ListDeliveries: %v", err)
	}
	if len(processing) != n {
		t.Errorf("processing rows = %d, want %d", len(processing), n)
	}
}

func TestRecordFailureSchedulesRetryThenMarksDead(t *testing.T) {
	st := pgtest.NewStore(t)
	ctx := context.Background()

	mustSub(t, st, "https://a.example/hook", []string{store.MatchAll})
	_, _, err := st.CreateEventWithDeliveries(ctx, "x", json.RawMessage(`{}`), 2)
	if err != nil {
		t.Fatalf("CreateEventWithDeliveries: %v", err)
	}

	claims, err := st.ClaimDue(ctx, 10)
	if err != nil || len(claims) != 1 {
		t.Fatalf("ClaimDue: got %d claims, err %v", len(claims), err)
	}
	id := claims[0].DeliveryID

	code := 500
	future := time.Now().Add(time.Hour)
	if err := st.RecordFailure(ctx, id, &code, "target returned 500", future, false); err != nil {
		t.Fatalf("RecordFailure(retry): %v", err)
	}

	d, err := st.GetDelivery(ctx, id)
	if err != nil {
		t.Fatalf("GetDelivery: %v", err)
	}
	if d.Status != store.StatusPending {
		t.Errorf("status = %s, want pending", d.Status)
	}
	if d.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", d.Attempts)
	}
	if d.LastStatusCode == nil || *d.LastStatusCode != 500 {
		t.Errorf("last_status_code = %v, want 500", d.LastStatusCode)
	}
	if d.NextRetryAt.Before(time.Now().Add(30 * time.Minute)) {
		t.Errorf("next_retry_at = %s, want ~1h out", d.NextRetryAt)
	}

	// Second failure exhausts the budget (max_attempts = 2).
	if err := st.RecordFailure(ctx, id, &code, "target returned 500", time.Now(), true); err != nil {
		t.Fatalf("RecordFailure(dead): %v", err)
	}
	d, err = st.GetDelivery(ctx, id)
	if err != nil {
		t.Fatalf("GetDelivery: %v", err)
	}
	if d.Status != store.StatusDead {
		t.Errorf("status = %s, want dead", d.Status)
	}
	if d.Attempts != 2 {
		t.Errorf("attempts = %d, want 2", d.Attempts)
	}
}

func TestReplayRequeuesDeadDeliveryAndRejectsPending(t *testing.T) {
	st := pgtest.NewStore(t)
	ctx := context.Background()

	mustSub(t, st, "https://a.example/hook", []string{store.MatchAll})
	_, _, err := st.CreateEventWithDeliveries(ctx, "x", json.RawMessage(`{}`), 3)
	if err != nil {
		t.Fatalf("CreateEventWithDeliveries: %v", err)
	}
	list, err := st.ListDeliveries(ctx, "", 10)
	if err != nil || len(list) != 1 {
		t.Fatalf("ListDeliveries: got %d, err %v", len(list), err)
	}
	id := list[0].ID

	// A brand new delivery is pending, not replayable.
	if _, err := st.Replay(ctx, id); !errors.Is(err, store.ErrNotReplayable) {
		t.Fatalf("Replay of a pending delivery: err = %v, want ErrNotReplayable", err)
	}

	// Drive it to dead, then replay.
	code := 500
	if err := st.RecordFailure(ctx, id, &code, "boom", time.Now(), true); err != nil {
		t.Fatalf("RecordFailure: %v", err)
	}
	d, err := st.Replay(ctx, id)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if d.Status != store.StatusPending {
		t.Errorf("status = %s, want pending", d.Status)
	}
	if d.Attempts != 0 {
		t.Errorf("attempts = %d, want 0 after replay", d.Attempts)
	}
	if d.LastError != nil {
		t.Errorf("last_error = %v, want nil after replay", *d.LastError)
	}
}

func TestReplayUnknownIDIsNotFound(t *testing.T) {
	st := pgtest.NewStore(t)
	_, err := st.Replay(context.Background(), uuid.New())
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Replay of unknown id: err = %v, want ErrNotFound", err)
	}
}

func mustSub(t *testing.T, st *store.Store, url string, types []string) store.Subscription {
	t.Helper()
	sub, err := st.CreateSubscription(context.Background(), url, types, "supersecret")
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	return sub
}
