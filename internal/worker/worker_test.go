package worker

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/abdulrahman-muhammed-07/hookrelay/internal/signing"
	"github.com/abdulrahman-muhammed-07/hookrelay/internal/store"
)

func testClaim(target string) store.Claim {
	return store.Claim{
		DeliveryID:   uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Attempts:     0,
		MaxAttempts:  5,
		TargetURL:    target,
		Secret:       "the-secret",
		EventID:      uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		EventType:    "order.created",
		EventPayload: []byte(`{"hello":"world"}`),
	}
}

func TestDeliverSuccess(t *testing.T) {
	var (
		gotBody []byte
		gotSig  string
		gotType string
		gotID   string
		gotCT   string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		gotSig = r.Header.Get(signing.Header)
		gotType = r.Header.Get(EventTypeHeader)
		gotID = r.Header.Get(DeliveryIDHeader)
		gotCT = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := testClaim(srv.URL)
	code, err := deliver(context.Background(), srv.Client(), c, 2*time.Second, 4096)
	if err != nil {
		t.Fatalf("deliver() error = %v", err)
	}
	if code != http.StatusOK {
		t.Errorf("code = %d, want 200", code)
	}
	if string(gotBody) != `{"hello":"world"}` {
		t.Errorf("target got body %q, want the raw payload", gotBody)
	}
	if want := signing.Sign("the-secret", c.EventPayload); gotSig != want {
		t.Errorf("signature header = %q, want %q", gotSig, want)
	}
	if gotType != "order.created" {
		t.Errorf("event header = %q, want order.created", gotType)
	}
	if gotID != c.DeliveryID.String() {
		t.Errorf("delivery header = %q, want %q", gotID, c.DeliveryID)
	}
	if gotCT != "application/json" {
		t.Errorf("content-type = %q, want application/json", gotCT)
	}
}

func TestDeliverNon2xxIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	defer srv.Close()

	code, err := deliver(context.Background(), srv.Client(), testClaim(srv.URL), 2*time.Second, 4096)
	if err == nil {
		t.Fatal("deliver() error = nil, want non-nil for a 500")
	}
	if code != http.StatusInternalServerError {
		t.Errorf("code = %d, want 500", code)
	}
}

func TestDeliverTransportErrorHasZeroCode(t *testing.T) {
	// Nothing is listening on this port after Close.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()

	code, err := deliver(context.Background(), http.DefaultClient, testClaim(url), time.Second, 4096)
	if err == nil {
		t.Fatal("deliver() error = nil, want a transport error")
	}
	if code != 0 {
		t.Errorf("code = %d, want 0 on transport failure", code)
	}
}

func TestDeliverRespectsTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	start := time.Now()
	_, err := deliver(context.Background(), srv.Client(), testClaim(srv.URL), 50*time.Millisecond, 4096)
	if err == nil {
		t.Fatal("deliver() error = nil, want a timeout error")
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("deliver blocked %s, want it to give up near the 50ms deadline", elapsed)
	}
}
