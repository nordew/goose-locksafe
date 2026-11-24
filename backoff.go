package locksafe

import (
	"math/rand/v2"
	"time"
)

// backoff returns the wait after the given failed attempt (1-based):
// exponential growth from base, capped at maxWait, with "equal jitter"
// (half fixed, half random) so concurrent runners spread out but never retry
// immediately.
func backoff(attempt int, base, maxWait time.Duration, randN func(int64) int64) time.Duration {
	d := base
	for i := 1; i < attempt && d < maxWait; i++ {
		d *= 2
	}
	if d > maxWait || d <= 0 {
		d = maxWait
	}
	half := d / 2
	return half + time.Duration(randN(int64(d-half)+1))
}

func defaultRandN(n int64) int64 { return rand.Int64N(n) } //nolint:gosec // jitter, not security.
