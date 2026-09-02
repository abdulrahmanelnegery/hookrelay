package api_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/abdulrahman-muhammed-07/hookrelay/internal/api"
	"github.com/abdulrahman-muhammed-07/hookrelay/internal/config"
	"github.com/abdulrahman-muhammed-07/hookrelay/internal/pgtest"
)

func newServer(t *testing.T) http.Handler {
	t.Helper()
	st := pgtest.NewStore(t)
	cfg := config.Config{MaxAttempts: 5}
	return api.New(st, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func do(t *testing.T, h http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestSubscriptionCreateAndList(t *testing.T) {
	h := newServer(t)

	rec := do(t, h, http.MethodPost, "/subscriptions", map[string]any{
		"target_url":  "https://example.com/webhook",
		"event_types": []string{"order.created", "order.paid"},
		"secret":      "supersecret",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /subscriptions = %d, body %s", rec.Code, rec.Body)
	}
	var created map[string]any
	mustJSON(t, rec.Body.Bytes(), &created)
	if created["id"] == "" || created["id"] == nil {
		t.Fatalf("created subscription has no id: %v", created)
	}

	rec = do(t, h, http.MethodGet, "/subscriptions", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /subscriptions = %d", rec.Code)
	}
	var list []map[string]any
	mustJSON(t, rec.Body.Bytes(), &list)
	if len(list) != 1 {
		t.Fatalf("subscription list len = %d, want 1", len(list))
	}
}

func TestSubscriptionValidationRejectsBadInput(t *testing.T) {
	h := newServer(t)

	cases := []struct {
		name string
		body map[string]any
	}{
		{"bad url", map[string]any{"target_url": "not-a-url", "event_types": []string{"x"}, "secret": "supersecret"}},
		{"ftp url", map[string]any{"target_url": "ftp://example.com", "event_types": []string{"x"}, "secret": "supersecret"}},
		{"no event types", map[string]any{"target_url": "https://e.com/h", "event_types": []string{}, "secret": "supersecret"}},
		{"short secret", map[string]any{"target_url": "https://e.com/h", "event_types": []string{"x"}, "secret": "short"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(t, h, http.MethodPost, "/subscriptions", tc.body)
			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("POST /subscriptions = %d, want 422 (body %s)", rec.Code, rec.Body)
			}
		})
	}
}

func TestEventFansOutAndDeliveriesListed(t *testing.T) {
	h := newServer(t)

	do(t, h, http.MethodPost, "/subscriptions", map[string]any{
		"target_url":  "https://a.example/h",
		"event_types": []string{"order.created"},
		"secret":      "supersecret",
	})
	do(t, h, http.MethodPost, "/subscriptions", map[string]any{
		"target_url":  "https://b.example/h",
		"event_types": []string{"*"},
		"secret":      "supersecret",
	})

	rec := do(t, h, http.MethodPost, "/events", map[string]any{
		"type":    "order.created",
		"payload": map[string]any{"order_id": 7},
	})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST /events = %d, body %s", rec.Code, rec.Body)
	}
	var accepted struct {
		DeliveriesCreated int `json:"deliveries_created"`
	}
	mustJSON(t, rec.Body.Bytes(), &accepted)
	if accepted.DeliveriesCreated != 2 {
		t.Fatalf("deliveries_created = %d, want 2", accepted.DeliveriesCreated)
	}

	rec = do(t, h, http.MethodGet, "/deliveries?status=pending", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /deliveries = %d", rec.Code)
	}
	var deliveries []map[string]any
	mustJSON(t, rec.Body.Bytes(), &deliveries)
	if len(deliveries) != 2 {
		t.Fatalf("pending deliveries = %d, want 2", len(deliveries))
	}
}

func TestEventRejectsMissingFields(t *testing.T) {
	h := newServer(t)

	rec := do(t, h, http.MethodPost, "/events", map[string]any{"payload": map[string]any{"a": 1}})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("POST /events without type = %d, want 422", rec.Code)
	}

	rec = do(t, h, http.MethodPost, "/events", map[string]any{"type": "x"})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("POST /events without payload = %d, want 422", rec.Code)
	}
}

func TestReplayUnknownAndMalformedID(t *testing.T) {
	h := newServer(t)

	rec := do(t, h, http.MethodPost, "/deliveries/"+uuid.NewString()+"/replay", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("replay unknown id = %d, want 404", rec.Code)
	}

	rec = do(t, h, http.MethodPost, "/deliveries/not-a-uuid/replay", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("replay malformed id = %d, want 400", rec.Code)
	}
}

func TestListDeliveriesRejectsUnknownStatus(t *testing.T) {
	h := newServer(t)
	rec := do(t, h, http.MethodGet, "/deliveries?status=banana", nil)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("GET /deliveries?status=banana = %d, want 422", rec.Code)
	}
}

func mustJSON(t *testing.T, raw []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatalf("unmarshal %s: %v", raw, err)
	}
}
