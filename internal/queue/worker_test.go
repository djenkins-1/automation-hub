package queue_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djenkins-1/automation-hub/internal/queue"
	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

// These tests use the real clock and real (short) durations.

var quietLog = slog.New(slog.NewTextHandler(io.Discard, nil))

type rig struct {
	q *queue.SQLite
}

func newRig(t *testing.T) rig {
	t.Helper()
	return rig{q: queue.NewSQLite(openDB(t), queue.Options{DefaultMaxAttempts: 4})}
}

func (r rig) worker(t *testing.T, mod func(*queue.WorkerConfig)) *queue.Worker {
	t.Helper()
	cfg := queue.WorkerConfig{
		Queue:          r.q,
		Admin:          r.q,
		Concurrency:    4,
		LeaseFor:       time.Second,
		PollEvery:      10 * time.Millisecond,
		ReapEvery:      20 * time.Millisecond,
		HandlerTimeout: 5 * time.Second,
		ShutdownGrace:  2 * time.Second,
		Policy:         queue.RetryPolicy{Base: 5 * time.Millisecond, Max: 20 * time.Millisecond, Jitter: -1},
		Log:            quietLog,
	}
	if mod != nil {
		mod(&cfg)
	}
	w, err := queue.NewWorker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// start runs w in the background and returns a stop func that cancels it and
// returns Run's error. stop is also registered as cleanup.
func start(t *testing.T, w *queue.Worker) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	var stopped bool
	var result error
	stop = func() error {
		if !stopped {
			stopped = true
			cancel()
			select {
			case result = <-done:
			case <-time.After(10 * time.Second):
				t.Error("worker did not stop within 10s")
			}
		}
		return result
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for: %s", timeout, what)
}

func (r rig) state(t *testing.T, id string) queue.JobInfo {
	t.Helper()
	ji, err := r.q.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return ji
}

func (r rig) waitState(t *testing.T, id, state string) queue.JobInfo {
	t.Helper()
	eventually(t, 5*time.Second, "job "+id+" to reach "+state, func() bool {
		return r.state(t, id).State == state
	})
	return r.state(t, id)
}

func (r rig) enqueue(t *testing.T, j queue.NewJob) string {
	t.Helper()
	id, _, err := r.q.Enqueue(context.Background(), j)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestWorkerProcessesAllJobs(t *testing.T) {
	r := newRig(t)
	var handled atomic.Int64
	w := r.worker(t, nil)
	w.Handle("k", func(ctx context.Context, j queue.Job) error {
		handled.Add(1)
		return nil
	})

	ids := make([]string, 30)
	for i := range ids {
		ids[i] = r.enqueue(t, queue.NewJob{Kind: "k"})
	}
	start(t, w)
	for _, id := range ids {
		r.waitState(t, id, queue.StateDone)
	}
	if got := handled.Load(); got != int64(len(ids)) {
		t.Fatalf("handler ran %d times for %d jobs", got, len(ids))
	}
}

func TestWorkerOnlyLeasesRegisteredKinds(t *testing.T) {
	r := newRig(t)
	other := r.enqueue(t, queue.NewJob{Kind: "someone-elses"})
	mine := r.enqueue(t, queue.NewJob{Kind: "mine"})
	w := r.worker(t, nil)
	w.Handle("mine", func(context.Context, queue.Job) error { return nil })
	start(t, w)

	r.waitState(t, mine, queue.StateDone)
	if s := r.state(t, other); s.State != queue.StateReady || s.Attempts != 0 {
		t.Fatalf("unregistered kind was touched: %+v", s)
	}
}

func TestWorkerRetriesThenSucceeds(t *testing.T) {
	r := newRig(t)
	var calls atomic.Int64
	w := r.worker(t, nil)
	w.Handle("k", func(ctx context.Context, j queue.Job) error {
		if calls.Add(1) < 3 {
			return errors.New("transient")
		}
		return nil
	})
	id := r.enqueue(t, queue.NewJob{Kind: "k"})
	start(t, w)

	ji := r.waitState(t, id, queue.StateDone)
	if ji.Attempts != 3 || calls.Load() != 3 {
		t.Fatalf("attempts=%d calls=%d, want 3", ji.Attempts, calls.Load())
	}
}

func TestWorkerPermanentErrorDeadLettersImmediately(t *testing.T) {
	r := newRig(t)
	var calls atomic.Int64
	w := r.worker(t, nil)
	w.Handle("k", func(ctx context.Context, j queue.Job) error {
		calls.Add(1)
		return sdk.Permanent(errors.New("404 not found"))
	})
	id := r.enqueue(t, queue.NewJob{Kind: "k", MaxAttempts: 5})
	start(t, w)

	ji := r.waitState(t, id, queue.StateDead)
	if ji.Attempts != 1 || calls.Load() != 1 || ji.LastError != "404 not found" {
		t.Fatalf("job=%+v calls=%d", ji, calls.Load())
	}
}

func TestWorkerExhaustsAttempts(t *testing.T) {
	r := newRig(t)
	var calls atomic.Int64
	w := r.worker(t, nil)
	w.Handle("k", func(ctx context.Context, j queue.Job) error {
		calls.Add(1)
		return errors.New("always")
	})
	id := r.enqueue(t, queue.NewJob{Kind: "k", MaxAttempts: 3})
	start(t, w)

	ji := r.waitState(t, id, queue.StateDead)
	time.Sleep(100 * time.Millisecond) // and it must stay dead
	if ji.Attempts != 3 || calls.Load() != 3 {
		t.Fatalf("attempts=%d calls=%d, want 3", ji.Attempts, calls.Load())
	}
}

func TestWorkerPanicIsRetried(t *testing.T) {
	r := newRig(t)
	var calls atomic.Int64
	w := r.worker(t, nil)
	w.Handle("k", func(ctx context.Context, j queue.Job) error {
		if calls.Add(1) == 1 {
			panic("kaboom")
		}
		return nil
	})
	id := r.enqueue(t, queue.NewJob{Kind: "k"})
	start(t, w)

	ji := r.waitState(t, id, queue.StateDone)
	if ji.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", ji.Attempts)
	}
}

func TestWorkerHonoursRetryAfter(t *testing.T) {
	r := newRig(t)
	var first, second atomic.Int64
	w := r.worker(t, nil)
	w.Handle("k", func(ctx context.Context, j queue.Job) error {
		if j.Attempt == 1 {
			first.Store(time.Now().UnixMilli())
			return sdk.RetryAfter(errors.New("rate limited"), 300*time.Millisecond)
		}
		second.Store(time.Now().UnixMilli())
		return nil
	})
	id := r.enqueue(t, queue.NewJob{Kind: "k"})
	start(t, w)

	r.waitState(t, id, queue.StateDone)
	if gap := time.Duration(second.Load()-first.Load()) * time.Millisecond; gap < 290*time.Millisecond {
		t.Fatalf("retried after %v, want >= 300ms", gap)
	}
}

func TestWorkerHeartbeatKeepsLongJobAlive(t *testing.T) {
	r := newRig(t)
	var calls atomic.Int64
	w := r.worker(t, func(c *queue.WorkerConfig) { c.LeaseFor = 300 * time.Millisecond })
	w.Handle("k", func(ctx context.Context, j queue.Job) error {
		calls.Add(1)
		select { // three lease lengths of work
		case <-time.After(900 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	id := r.enqueue(t, queue.NewJob{Kind: "k"})
	start(t, w) // the reaper runs every 20ms, so a missed heartbeat would show up as a second attempt

	ji := r.waitState(t, id, queue.StateDone)
	if ji.Attempts != 1 || calls.Load() != 1 {
		t.Fatalf("job was reclaimed mid-run: attempts=%d calls=%d", ji.Attempts, calls.Load())
	}
}

// failingExtend makes every heartbeat report that the lease is gone.
type failingExtend struct{ queue.Queue }

func (failingExtend) Extend(context.Context, queue.Job, time.Duration) (time.Time, error) {
	return time.Time{}, queue.ErrLeaseLost
}

func TestWorkerLeaseLossCancelsHandler(t *testing.T) {
	r := newRig(t)
	var cause atomic.Value
	var calls atomic.Int64
	w := r.worker(t, func(c *queue.WorkerConfig) {
		c.Queue = failingExtend{r.q}
		c.LeaseFor = 90 * time.Millisecond // heartbeat every 30ms
	})
	w.Handle("k", func(ctx context.Context, j queue.Job) error {
		if calls.Add(1) > 1 {
			return nil
		}
		select {
		case <-ctx.Done():
			cause.Store(context.Cause(ctx))
			return ctx.Err()
		case <-time.After(3 * time.Second):
			return errors.New("handler was never cancelled")
		}
	})
	id := r.enqueue(t, queue.NewJob{Kind: "k"})
	start(t, w)

	r.waitState(t, id, queue.StateDone)
	if c, _ := cause.Load().(error); !errors.Is(c, queue.ErrLeaseLost) {
		t.Fatalf("handler context cause = %v, want ErrLeaseLost", c)
	}
}

func TestWorkerGracefulShutdownLetsJobsFinish(t *testing.T) {
	r := newRig(t)
	started := make(chan struct{})
	w := r.worker(t, nil)
	w.Handle("k", func(ctx context.Context, j queue.Job) error {
		close(started)
		time.Sleep(300 * time.Millisecond) // deliberately ignores ctx: finishing is the point
		return nil
	})
	id := r.enqueue(t, queue.NewJob{Kind: "k"})
	stop := start(t, w)

	<-started
	if err := stop(); err != nil {
		t.Fatalf("Run returned %v", err)
	}
	if s := r.state(t, id); s.State != queue.StateDone {
		t.Fatalf("in-flight job was abandoned: %+v", s)
	}
}

func TestWorkerShutdownGraceExpiryReleasesJob(t *testing.T) {
	r := newRig(t)
	started := make(chan struct{})
	w := r.worker(t, func(c *queue.WorkerConfig) { c.ShutdownGrace = 100 * time.Millisecond })
	w.Handle("k", func(ctx context.Context, j queue.Job) error {
		close(started)
		<-ctx.Done() // cooperative handler: exits when cancelled
		return ctx.Err()
	})
	id := r.enqueue(t, queue.NewJob{Kind: "k"})
	stop := start(t, w)

	<-started
	began := time.Now()
	if err := stop(); err != nil {
		t.Fatalf("Run returned %v", err)
	}
	if d := time.Since(began); d > 3*time.Second {
		t.Fatalf("shutdown took %v", d)
	}
	// Released for immediate retry, not left leased until the lease expires.
	if s := r.state(t, id); s.State != queue.StateReady {
		t.Fatalf("job state after forced shutdown = %q, want ready", s.State)
	}
}

func TestWorkerRecoversJobsFromCrashedWorker(t *testing.T) {
	r := newRig(t)
	id := r.enqueue(t, queue.NewJob{Kind: "k"})

	// A worker that leased the job and then died without a word.
	if jobs, err := r.q.Lease(context.Background(), "dead-worker", nil, 1, 50*time.Millisecond); err != nil || len(jobs) != 1 {
		t.Fatalf("setup lease: %v %d", err, len(jobs))
	}

	var attempt atomic.Int64
	w := r.worker(t, nil)
	w.Handle("k", func(ctx context.Context, j queue.Job) error {
		attempt.Store(int64(j.Attempt))
		return nil
	})
	start(t, w)

	r.waitState(t, id, queue.StateDone)
	if attempt.Load() != 2 {
		t.Fatalf("handled on attempt %d, want 2", attempt.Load())
	}
}

func TestWorkerRespectsConcurrencyLimit(t *testing.T) {
	r := newRig(t)
	var running, peak atomic.Int64
	w := r.worker(t, func(c *queue.WorkerConfig) { c.Concurrency = 3 })
	w.Handle("k", func(ctx context.Context, j queue.Job) error {
		n := running.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		running.Add(-1)
		return nil
	})
	ids := make([]string, 12)
	for i := range ids {
		ids[i] = r.enqueue(t, queue.NewJob{Kind: "k"})
	}
	start(t, w)
	for _, id := range ids {
		r.waitState(t, id, queue.StateDone)
	}
	if p := peak.Load(); p > 3 || p < 2 {
		t.Fatalf("peak concurrency = %d, want between 2 and 3", p)
	}
}

func TestWorkerConfigValidation(t *testing.T) {
	if _, err := queue.NewWorker(queue.WorkerConfig{}); err == nil {
		t.Error("NewWorker without a Queue should fail")
	}
	r := newRig(t)
	w := r.worker(t, nil)
	if err := w.Run(context.Background()); err == nil {
		t.Error("Run with no handlers should fail")
	}
}
