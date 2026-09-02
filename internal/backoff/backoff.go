// Package backoff computes the exponential retry schedule hookrelay uses between
// failed delivery attempts.
package backoff

import (
	"math"
	"time"
)

// Delay returns how long to wait before retry attempt n (n is 1-indexed: n == 1
// is the wait after the first failed attempt). The series is
//
//	base, base*factor, base*factor^2, ...
//
// with every term clamped to ceiling. n below 1 is treated as 1. A non-positive base
// yields 0. A factor at or below 1 makes the series flat at base (still clamped
// to ceiling).
func Delay(base time.Duration, factor float64, ceiling time.Duration, n int) time.Duration {
	if base <= 0 {
		return 0
	}
	if n < 1 {
		n = 1
	}
	if factor < 1 {
		factor = 1
	}

	// Work in float seconds to keep the exponent from overflowing int64 nanos.
	grown := float64(base) * math.Pow(factor, float64(n-1))
	if math.IsInf(grown, 1) || grown >= float64(ceiling) {
		return ceiling
	}
	d := time.Duration(grown)
	if ceiling > 0 && d > ceiling {
		return ceiling
	}
	return d
}

// Schedule returns Delay for n = 1..count as a slice. It is the retry plan in
// full: handy for logging at startup and for asserting the shape in tests. The
// result is always non-decreasing and never exceeds ceiling.
func Schedule(base time.Duration, factor float64, ceiling time.Duration, count int) []time.Duration {
	if count < 0 {
		count = 0
	}
	out := make([]time.Duration, count)
	for i := 0; i < count; i++ {
		out[i] = Delay(base, factor, ceiling, i+1)
	}
	return out
}
