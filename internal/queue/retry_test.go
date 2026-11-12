package queue_test

// Specification of RetryPolicy: how long to wait before retrying.
//
//  - Delay before retry n is Base * Factor^(n-1), capped at Max, then spread
//    by +/- Jitter so simultaneous failures do not retry in lockstep.
//  - Whatever the inputs (zero values, nonsense factors, a million attempts)
//    the delay is positive, never shrinks as attempts increase (without
//    jitter), never overflows, and never exceeds Max*(1+Jitter).
//  - The zero value is usable and uses the documented defaults.

import (
	"math"
	"testing"
	"time"

	"github.com/djenkins-1/automation-hub/internal/queue"
)

func noJitter(base, max time.Duration, factor float64) queue.RetryPolicy {
	return queue.RetryPolicy{Base: base, Max: max, Factor: factor, Jitter: -1}
}

func TestBackoffGrowsExponentiallyUntilTheCap(t *testing.T) {
	p := noJitter(time.Second, 10*time.Second, 2)
	want := []time.Duration{1, 2, 4, 8, 10, 10, 10}
	for i, w := range want {
		if got := p.Backoff(i + 1); got != w*time.Second {
			t.Errorf("Backoff(%d) = %v, want %v", i+1, got, w*time.Second)
		}
	}
	p3 := noJitter(100*time.Millisecond, time.Hour, 3)
	for i, w := range []time.Duration{100, 300, 900, 2700} {
		if got := p3.Backoff(i + 1); got != w*time.Millisecond {
			t.Errorf("factor 3: Backoff(%d) = %v, want %v", i+1, got, w*time.Millisecond)
		}
	}
	// Factor 1 means a constant delay.
	for n := 1; n < 20; n++ {
		if got := noJitter(3*time.Second, time.Hour, 1).Backoff(n); got != 3*time.Second {
			t.Fatalf("factor 1: Backoff(%d) = %v", n, got)
		}
	}
}

func TestAttemptsBelowOneCountAsTheFirst(t *testing.T) {
	p := noJitter(time.Second, time.Minute, 2)
	for _, n := range []int{1, 0, -1, -1000, math.MinInt32} {
		if got := p.Backoff(n); got != time.Second {
			t.Errorf("Backoff(%d) = %v, want 1s", n, got)
		}
	}
}

func TestBackoffIsAlwaysSaneWhateverTheInputs(t *testing.T) {
	factors := []float64{0, -3, 0.5, 0.999, 1, 1.0001, 1.5, 2, 10, 1e6, math.Inf(1), math.NaN()}
	attempts := []int{1, 2, 3, 10, 50, 100, 1000, 100_000, math.MaxInt32, math.MaxInt}
	for _, f := range factors {
		for _, max := range []time.Duration{time.Second, time.Minute, 24 * time.Hour, 0} {
			p := noJitter(time.Second, max, f)
			effectiveMax := max
			if effectiveMax <= 0 {
				effectiveMax = 15 * time.Minute // the documented default ceiling
			}
			var prev time.Duration
			for _, n := range attempts {
				d := p.Backoff(n)
				switch {
				case d <= 0:
					t.Fatalf("factor %v max %v: Backoff(%d) = %v, must be positive", f, max, n, d)
				case d > effectiveMax:
					t.Fatalf("factor %v max %v: Backoff(%d) = %v exceeds the cap", f, max, n, d)
				case d < prev:
					t.Fatalf("factor %v max %v: Backoff(%d) = %v is shorter than the previous %v: delays must never shrink", f, max, n, d, prev)
				}
				prev = d
			}
		}
	}
}

func TestBaseAboveMaxIsCappedNotExceeded(t *testing.T) {
	if got := noJitter(time.Hour, time.Minute, 2).Backoff(1); got != time.Minute {
		t.Fatalf("Backoff(1) = %v; the cap applies from the very first retry", got)
	}
}

func TestJitterStaysWithinItsBounds(t *testing.T) {
	for _, j := range []float64{0.05, 0.2, 0.5, 0.99} {
		for _, draw := range []float64{0, 1e-9, 0.25, 0.5, 0.75, 1 - 1e-12} {
			draw := draw
			p := queue.RetryPolicy{Base: 10 * time.Second, Max: time.Minute, Factor: 2, Jitter: j, Rand: func() float64 { return draw }}
			for _, c := range []struct {
				attempt int
				nominal time.Duration
			}{{1, 10 * time.Second}, {2, 20 * time.Second}, {10, time.Minute}} { // the last is capped
				lo := time.Duration(float64(c.nominal) * (1 - j))
				hi := time.Duration(float64(c.nominal) * (1 + j))
				if d := p.Backoff(c.attempt); d < lo-time.Millisecond || d > hi+time.Millisecond {
					t.Errorf("jitter %v draw %v attempt %d: %v outside [%v, %v]", j, draw, c.attempt, d, lo, hi)
				}
			}
		}
	}
	// The extremes of the random draw hit the extremes of the range.
	lowP := queue.RetryPolicy{Base: 10 * time.Second, Jitter: 0.2, Rand: func() float64 { return 0 }}
	if d := lowP.Backoff(1); d != 8*time.Second {
		t.Errorf("draw 0 gives %v, want exactly 8s", d)
	}
}

func TestJitterActuallySpreadsTheDelays(t *testing.T) {
	p := queue.RetryPolicy{Base: 10 * time.Second, Max: time.Hour, Factor: 2, Jitter: 0.2}
	distinct := map[time.Duration]bool{}
	var sum time.Duration
	const n = 400
	for i := 0; i < n; i++ {
		d := p.Backoff(3) // nominal 40s
		distinct[d] = true
		sum += d
		if d < 32*time.Second || d > 48*time.Second {
			t.Fatalf("delay %v outside 40s +/- 20%%", d)
		}
	}
	if len(distinct) < 50 {
		t.Fatalf("only %d distinct delays in %d samples: jitter is not spreading retries", len(distinct), n)
	}
	if mean := sum / n; mean < 36*time.Second || mean > 44*time.Second {
		t.Fatalf("mean delay %v is biased away from the nominal 40s", mean)
	}
}

func TestDisabledJitterIsExactlyReproducible(t *testing.T) {
	p := noJitter(time.Second, time.Minute, 2)
	for i := 0; i < 50; i++ {
		if p.Backoff(4) != 8*time.Second {
			t.Fatal("with jitter disabled the delay must be exact every time")
		}
	}
}

func TestTheZeroPolicyUsesDocumentedDefaults(t *testing.T) {
	def := queue.DefaultRetryPolicy()
	if def.Base != 5*time.Second || def.Max != 15*time.Minute || def.Factor != 2 || def.Jitter != 0.2 {
		t.Fatalf("DefaultRetryPolicy() = %+v; its documented values changed", def)
	}
	var zero queue.RetryPolicy
	for i := 0; i < 100; i++ {
		if d := zero.Backoff(1); d < 4*time.Second || d > 6*time.Second {
			t.Fatalf("zero policy, first retry: %v, want 5s +/- 20%%", d)
		}
		if d := zero.Backoff(50); d < 12*time.Minute || d > 18*time.Minute {
			t.Fatalf("zero policy, late retry: %v, want about the 15m cap +/- 20%%", d)
		}
	}
}

func TestOutOfRangeJitterIsClamped(t *testing.T) {
	for _, j := range []float64{1, 2, 100, math.Inf(1)} {
		p := queue.RetryPolicy{Base: time.Second, Jitter: j, Rand: func() float64 { return 0 }}
		if d := p.Backoff(1); d <= 0 {
			t.Errorf("jitter %v produced a non-positive delay %v", j, d)
		}
	}
}
