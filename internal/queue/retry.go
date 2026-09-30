package queue

import (
	"math"
	"math/rand/v2"
	"time"
)

// RetryPolicy computes exponential backoff with jitter.
//
// The delay before retry n (n = 1 for the first retry) is
// Base * Factor^(n-1), capped at Max, then randomised by +/- Jitter so that a
// burst of failures does not retry in lockstep.
type RetryPolicy struct {
	Base   time.Duration // default 5s
	Max    time.Duration // default 15m; applied before jitter
	Factor float64       // default 2
	Jitter float64       // fraction in [0,1); default 0.2. Use a negative value to disable.

	// Rand returns a float in [0,1). Defaults to math/rand/v2; tests inject a
	// fixed function.
	Rand func() float64
}

// DefaultRetryPolicy returns a policy with the defaults spelled out.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{Base: 5 * time.Second, Max: 15 * time.Minute, Factor: 2, Jitter: 0.2}
}

func (p RetryPolicy) withDefaults() RetryPolicy {
	d := DefaultRetryPolicy()
	if p.Base <= 0 {
		p.Base = d.Base
	}
	if p.Max <= 0 {
		p.Max = d.Max
	}
	if p.Factor < 1 {
		p.Factor = d.Factor
	}
	if p.Jitter == 0 {
		p.Jitter = d.Jitter
	}
	if p.Jitter < 0 {
		p.Jitter = 0
	}
	if p.Jitter >= 1 {
		p.Jitter = 0.99
	}
	if p.Rand == nil {
		p.Rand = rand.Float64
	}
	return p
}

// Backoff returns the delay before the retry that follows the given attempt
// (1-based). attempt <= 1 yields roughly Base.
func (p RetryPolicy) Backoff(attempt int) time.Duration {
	p = p.withDefaults()
	if attempt < 1 {
		attempt = 1
	}
	d := float64(p.Base) * math.Pow(p.Factor, float64(attempt-1))
	if d > float64(p.Max) || math.IsInf(d, 0) || math.IsNaN(d) {
		d = float64(p.Max)
	}
	if p.Jitter > 0 {
		d *= 1 + p.Jitter*(2*p.Rand()-1)
	}
	return time.Duration(d)
}
