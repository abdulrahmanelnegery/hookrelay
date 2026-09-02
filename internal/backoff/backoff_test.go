package backoff

import (
	"testing"
	"time"
)

func TestScheduleShape(t *testing.T) {
	base := 10 * time.Second
	factor := 3.0
	ceiling := time.Hour
	count := 5

	got := Schedule(base, factor, ceiling, count)

	if len(got) != count {
		t.Fatalf("Schedule length = %d, want %d", len(got), count)
	}

	// First term is base.
	if got[0] != base {
		t.Errorf("got[0] = %s, want %s", got[0], base)
	}

	// Monotonic non-decreasing.
	for i := 1; i < len(got); i++ {
		if got[i] < got[i-1] {
			t.Errorf("not monotonic at %d: %s < %s", i, got[i], got[i-1])
		}
	}

	// Never exceeds the ceiling.
	for i, d := range got {
		if d > ceiling {
			t.Errorf("got[%d] = %s exceeds ceiling %s", i, d, ceiling)
		}
	}

	// Exact geometric progression until the ceiling bites.
	want := []time.Duration{
		10 * time.Second,
		30 * time.Second,
		90 * time.Second,
		270 * time.Second,
		810 * time.Second,
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("got[%d] = %s, want %s", i, got[i], want[i])
		}
	}
}

func TestScheduleCaps(t *testing.T) {
	// factor 10 blows past a 1m ceiling immediately after the first term.
	got := Schedule(10*time.Second, 10, time.Minute, 4)
	want := []time.Duration{
		10 * time.Second,
		time.Minute,
		time.Minute,
		time.Minute,
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("got[%d] = %s, want %s", i, got[i], want[i])
		}
	}
}

func TestDelayClampsAttemptNumber(t *testing.T) {
	base := 2 * time.Second
	if d := Delay(base, 2, time.Hour, 0); d != base {
		t.Errorf("Delay(n=0) = %s, want base %s", d, base)
	}
	if d := Delay(base, 2, time.Hour, -5); d != base {
		t.Errorf("Delay(n=-5) = %s, want base %s", d, base)
	}
}

func TestDelayHugeExponentDoesNotOverflow(t *testing.T) {
	// n large enough that base*factor^(n-1) overflows int64 nanoseconds.
	d := Delay(time.Second, 2, time.Hour, 200)
	if d != time.Hour {
		t.Errorf("Delay with huge exponent = %s, want ceiling 1h", d)
	}
}

func TestDelayFlatWhenFactorAtMostOne(t *testing.T) {
	base := 7 * time.Second
	for n := 1; n <= 5; n++ {
		if d := Delay(base, 1, time.Hour, n); d != base {
			t.Errorf("Delay(factor=1, n=%d) = %s, want %s", n, d, base)
		}
		if d := Delay(base, 0.5, time.Hour, n); d != base {
			t.Errorf("Delay(factor=0.5, n=%d) = %s, want %s", n, d, base)
		}
	}
}

func TestZeroBaseIsZero(t *testing.T) {
	if d := Delay(0, 2, time.Hour, 3); d != 0 {
		t.Errorf("Delay(base=0) = %s, want 0", d)
	}
}
