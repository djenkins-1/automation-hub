package queue_test

// Specification of the Worker: what a handler author and an operator can rely on.
//
//  - Every job of a registered kind is run; kinds nobody handles are left alone.
//  - A job that succeeds runs once. A job that fails is retried according to
//    how the handler classified the failure: plain error -> backoff retry,
//    RetryAfter -> not before that delay, Permanent -> dead at once (and
//    Permanent wins if both are present), panic -> retried like any error.
//  - Attempts are bounded; the concurrency limit is never exceeded (and is used).
//  - Long jobs keep their lease via heartbeats; losing the lease cancels the
//    handler; a handler cannot run past its timeout.
//  - Several workers on one queue never run the same job at the same time.
//  - Shutdown stops taking work, lets in-flight jobs finish for the grace
//    period, then cancels them and hands them back immediately.
//  - Jobs stranded by a crashed worker are recovered; old finished jobs are purged.
//  - A broken database never crashes the worker or prevents clean shutdown.
//
// These tests use real time with short durations; "must not happen" checks wait
// briefly, "must happen" checks wait generously.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djenkins-1/automation-hub/internal/queue"
	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

var quietLog = slog.New(slog.NewTextHandler(io.Discard, nil))

type rig struct {
	t  *testing.T
	q  *queue.SQLite
	db interface{ Close() error }
}

func newRig(t *testing.T) *rig {
	t.Helper()
	db := openDB(t)
	return &rig{t: t, q: queue.NewSQLite(db, queue.Options{DefaultMaxAttempts: 3}), db: db}
}

// fastConfig is a worker configuration whose delays are small enough for tests.
func (r *rig) fastConfig() queue.WorkerConfig {
	return queue.WorkerConfig{
		Queue: r.q, Admin: r.q,
		Concurrency:    4,
		LeaseFor:       time.Second,
		PollEvery:      5 * time.Millisecond,
		ReapEvery:      20 * time.Millisecond,
		PurgeEvery:     time.Hour,
		HandlerTimeout: 10 * time.Second,
		ShutdownGrace:  2 * time.Second,
		RetainDone:     -1, RetainDead: -1, // keep everything unless a test says otherwise
		Policy: queue.RetryPolicy{Base: 2 * time.Millisecond, Max: 5 * time.Millisecond, Jitter: -1},
		Log:    quietLog,
	}
}

func (r *rig) worker(mod func(*queue.WorkerConfig)) *queue.Worker {
	r.t.Helper()
	cfg := r.fastConfig()
	if mod != nil {
		mod(&cfg)
	}
	w, err := queue.NewWorker(cfg)
	if err != nil {
		r.t.Fatal(err)
	}
	return w
}

func (r *rig) enqueue(j queue.NewJob) string {
	r.t.Helper()
	id, _, err := r.q.Enqueue(bg, j)
	if err != nil {
		r.t.Fatal(err)
	}
	return id
}

func (r *rig) info(id string) queue.JobInfo {
	r.t.Helper()
	ji, err := r.q.Get(bg, id)
	if err != nil {
		r.t.Fatal(err)
	}
	return ji
}

func (r *rig) waitState(id, state string) queue.JobInfo {
	r.t.Helper()
	eventually(r.t, 10*time.Second, fmt.Sprintf("job %s to be %s", id, state), func() bool { return r.info(id).State == state })
	return r.info(id)
}

// start runs w in the background. stop cancels it and returns Run's result.
func start(t *testing.T, w *queue.Worker) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(bg)
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	var (
		once   sync.Once
		result error
	)
	stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case result = <-done:
			case <-time.After(15 * time.Second):
				t.Error("worker did not stop within 15s of cancellation")
			}
		})
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
		time.Sleep(3 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for: %s", timeout, what)
}

func ok(context.Context, queue.Job) error { return nil }

// ---------------------------------------------------------------------------
// Running jobs

func TestEveryJobIsRunOnceWhenHandlersSucceed(t *testing.T) {
	r := newRig(t)
	var mu sync.Mutex
	runs := map[string]int{}
	w := r.worker(nil)
	w.Handle("k", func(_ context.Context, j queue.Job) error {
		mu.Lock()
		runs[j.ID]++
		mu.Unlock()
		return nil
	})
	var ids []string
	for i := 0; i < 60; i++ {
		ids = append(ids, r.enqueue(queue.NewJob{Kind: "k"}))
	}
	start(t, w)
	for _, id := range ids {
		if ji := r.waitState(id, queue.StateDone); ji.Attempts != 1 {
			t.Fatalf("job %s took %d attempts", id, ji.Attempts)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for _, id := range ids {
		if runs[id] != 1 {
			t.Errorf("job %s ran %d times, want exactly once", id, runs[id])
		}
	}
}

func TestJobsGoToTheHandlerOfTheirKindAndUnhandledKindsAreLeftAlone(t *testing.T) {
	r := newRig(t)
	var a, b atomic.Int64
	w := r.worker(nil)
	w.Handle("a", func(_ context.Context, j queue.Job) error {
		if j.Kind != "a" {
			t.Errorf("handler a received a %q job", j.Kind)
		}
		a.Add(1)
		return nil
	})
	w.Handle("b", func(_ context.Context, j queue.Job) error {
		if j.Kind != "b" {
			t.Errorf("handler b received a %q job", j.Kind)
		}
		b.Add(1)
		return nil
	})
	ia1, ia2 := r.enqueue(queue.NewJob{Kind: "a"}), r.enqueue(queue.NewJob{Kind: "a"})
	ib := r.enqueue(queue.NewJob{Kind: "b"})
	other := r.enqueue(queue.NewJob{Kind: "belongs-to-another-worker"})
	start(t, w)

	for _, id := range []string{ia1, ia2, ib} {
		r.waitState(id, queue.StateDone)
	}
	time.Sleep(60 * time.Millisecond)
	if a.Load() != 2 || b.Load() != 1 {
		t.Fatalf("handler a ran %d times (want 2), handler b %d (want 1)", a.Load(), b.Load())
	}
	if ji := r.info(other); ji.State != queue.StateReady || ji.Attempts != 0 {
		t.Fatalf("a job of a kind this worker does not handle must be untouched: %+v", ji)
	}
}

func TestTheHandlerReceivesTheJobExactlyAsEnqueued(t *testing.T) {
	r := newRig(t)
	payload := []byte{0, 1, 2, 0xff, 0xfe, '\n', '"', 0}
	got := make(chan queue.Job, 1)
	w := r.worker(nil)
	w.Handle("k", func(_ context.Context, j queue.Job) error { got <- j; return nil })
	id := r.enqueue(queue.NewJob{Kind: "k", Payload: payload, MaxAttempts: 7})
	start(t, w)

	select {
	case j := <-got:
		if j.ID != id || j.Kind != "k" || string(j.Payload) != string(payload) || j.Attempt != 1 || j.MaxAttempts != 7 {
			t.Fatalf("handler saw %+v", j)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the handler was never called")
	}
}

func TestConcurrencyIsUsedButNeverExceeded(t *testing.T) {
	for _, limit := range []int{1, 3} {
		t.Run(fmt.Sprintf("limit %d", limit), func(t *testing.T) {
			r := newRig(t)
			var running, peak atomic.Int64
			w := r.worker(func(c *queue.WorkerConfig) { c.Concurrency = limit })
			w.Handle("k", func(context.Context, queue.Job) error {
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
			var ids []string
			for i := 0; i < 14; i++ {
				ids = append(ids, r.enqueue(queue.NewJob{Kind: "k"}))
			}
			start(t, w)
			for _, id := range ids {
				r.waitState(id, queue.StateDone)
			}
			if got := int(peak.Load()); got != limit {
				t.Fatalf("peak concurrency was %d, want exactly the limit %d (never above it, and actually reaching it)", got, limit)
			}
		})
	}
}

func TestOneBusyKindDoesNotStopOthersWhenSlotsAreFree(t *testing.T) {
	r := newRig(t)
	block := make(chan struct{})
	w := r.worker(func(c *queue.WorkerConfig) { c.Concurrency = 2 })
	w.Handle("slow", func(ctx context.Context, _ queue.Job) error {
		select {
		case <-block:
		case <-ctx.Done():
		}
		return nil
	})
	w.Handle("fast", ok)
	slow := r.enqueue(queue.NewJob{Kind: "slow"})
	start(t, w)
	r.waitState(slow, queue.StateLeased)

	fast := r.enqueue(queue.NewJob{Kind: "fast"})
	r.waitState(fast, queue.StateDone) // while the slow one is still running
	close(block)
	r.waitState(slow, queue.StateDone)
}

// ---------------------------------------------------------------------------
// Failure handling

type outcome struct {
	name      string
	handler   func(attempt int) error
	maxAtt    int
	wantState string
	wantTries int
	wantErr   string // substring of last_error when relevant
}

func TestHowHandlerResultsDecideAJobsFate(t *testing.T) {
	boom := errors.New("boom")
	tests := []outcome{
		{"success", func(int) error { return nil }, 3, queue.StateDone, 1, ""},
		{"plain error forever", func(int) error { return boom }, 3, queue.StateDead, 3, "boom"},
		{"plain error forever, one attempt allowed", func(int) error { return boom }, 1, queue.StateDead, 1, "boom"},
		{"fails twice then succeeds", func(a int) error {
			if a < 3 {
				return boom
			}
			return nil
		}, 5, queue.StateDone, 3, ""},
		{"permanent error", func(int) error { return sdk.Permanent(boom) }, 5, queue.StateDead, 1, "boom"},
		{"permanent error, wrapped afterwards", func(int) error { return fmt.Errorf("sending: %w", sdk.Permanent(boom)) }, 5, queue.StateDead, 1, "boom"},
		{"permanent beats retry-after", func(int) error { return sdk.Permanent(sdk.RetryAfter(boom, time.Hour)) }, 5, queue.StateDead, 1, "boom"},
		{"permanent on the last allowed attempt", func(a int) error {
			if a < 2 {
				return boom
			}
			return sdk.Permanent(boom)
		}, 2, queue.StateDead, 2, "boom"},
		{"panic with a string", func(a int) error {
			if a == 1 {
				panic("kaboom")
			}
			return nil
		}, 3, queue.StateDone, 2, ""},
		{"panic with an error", func(a int) error {
			if a == 1 {
				panic(errors.New("kaboom"))
			}
			return nil
		}, 3, queue.StateDone, 2, ""},
		{"panic with a number", func(a int) error {
			if a == 1 {
				panic(42)
			}
			return nil
		}, 3, queue.StateDone, 2, ""},
		{"panics forever", func(int) error { panic("always") }, 3, queue.StateDead, 3, "panic"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t)
			var calls atomic.Int64
			w := r.worker(nil)
			w.Handle("k", func(_ context.Context, j queue.Job) error {
				calls.Add(1)
				return tc.handler(j.Attempt)
			})
			id := r.enqueue(queue.NewJob{Kind: "k", MaxAttempts: tc.maxAtt})
			start(t, w)

			ji := r.waitState(id, tc.wantState)
			time.Sleep(60 * time.Millisecond) // the end state must be final
			ji = r.info(id)
			if ji.State != tc.wantState || ji.Attempts != tc.wantTries || int(calls.Load()) != tc.wantTries {
				t.Fatalf("state=%s attempts=%d handler calls=%d; want %s / %d / %d", ji.State, ji.Attempts, calls.Load(), tc.wantState, tc.wantTries, tc.wantTries)
			}
			if tc.wantErr != "" && !strings.Contains(ji.LastError, tc.wantErr) {
				t.Fatalf("last_error = %q, want it to mention %q", ji.LastError, tc.wantErr)
			}
		})
	}
}

func TestRetryAfterIsHonoured(t *testing.T) {
	r := newRig(t)
	var first, second atomic.Int64
	w := r.worker(nil)
	w.Handle("k", func(_ context.Context, j queue.Job) error {
		if j.Attempt == 1 {
			first.Store(time.Now().UnixMilli())
			return sdk.RetryAfter(errors.New("rate limited"), 300*time.Millisecond)
		}
		second.Store(time.Now().UnixMilli())
		return nil
	})
	id := r.enqueue(queue.NewJob{Kind: "k"})
	start(t, w)
	r.waitState(id, queue.StateDone)
	if gap := time.Duration(second.Load()-first.Load()) * time.Millisecond; gap < 280*time.Millisecond {
		t.Fatalf("retried after %v, want at least the requested 300ms", gap)
	}
}

func TestPlainRetriesBackOffExponentially(t *testing.T) {
	r := newRig(t)
	var mu sync.Mutex
	var at []time.Time
	w := r.worker(func(c *queue.WorkerConfig) {
		c.Policy = queue.RetryPolicy{Base: 50 * time.Millisecond, Max: 5 * time.Second, Factor: 2, Jitter: -1}
	})
	w.Handle("k", func(context.Context, queue.Job) error {
		mu.Lock()
		at = append(at, time.Now())
		mu.Unlock()
		return errors.New("fail")
	})
	id := r.enqueue(queue.NewJob{Kind: "k", MaxAttempts: 3})
	start(t, w)
	r.waitState(id, queue.StateDead)

	mu.Lock()
	defer mu.Unlock()
	if len(at) != 3 {
		t.Fatalf("%d attempts", len(at))
	}
	// Bounds are deliberately a little loose (times are stored in whole
	// milliseconds and the scheduler adds noise), but still tell exponential
	// backoff (50ms then 100ms) from constant backoff (50ms then 50ms).
	if gap := at[1].Sub(at[0]); gap < 40*time.Millisecond {
		t.Errorf("first retry came after %v, want about the 50ms base", gap)
	}
	if gap := at[2].Sub(at[1]); gap < 80*time.Millisecond {
		t.Errorf("second retry came after %v, want about double the base (100ms): the backoff is not growing", gap)
	}
}

func TestFailingJobsDoNotStarveHealthyOnes(t *testing.T) {
	r := newRig(t)
	w := r.worker(func(c *queue.WorkerConfig) { c.Concurrency = 2 })
	w.Handle("bad", func(context.Context, queue.Job) error { return errors.New("always failing") })
	w.Handle("good", ok)
	for i := 0; i < 6; i++ {
		r.enqueue(queue.NewJob{Kind: "bad", MaxAttempts: 40})
	}
	var good []string
	for i := 0; i < 20; i++ {
		good = append(good, r.enqueue(queue.NewJob{Kind: "good"}))
	}
	start(t, w)
	for _, id := range good {
		r.waitState(id, queue.StateDone)
	}
}

func TestAHandlerTimeoutCancelsItAndTheJobIsRetried(t *testing.T) {
	r := newRig(t)
	var firstErr atomic.Value
	w := r.worker(func(c *queue.WorkerConfig) { c.HandlerTimeout = 100 * time.Millisecond })
	w.Handle("k", func(ctx context.Context, j queue.Job) error {
		if j.Attempt == 1 {
			<-ctx.Done()
			firstErr.Store(ctx.Err())
			return ctx.Err()
		}
		return nil
	})
	id := r.enqueue(queue.NewJob{Kind: "k"})
	start(t, w)
	ji := r.waitState(id, queue.StateDone)
	if err, _ := firstErr.Load().(error); !errors.Is(err, context.DeadlineExceeded) || ji.Attempts != 2 {
		t.Fatalf("first attempt ended with %v, job took %d attempts", err, ji.Attempts)
	}
}

// ---------------------------------------------------------------------------
// Leases

func TestHeartbeatsKeepALongJobFromBeingReclaimed(t *testing.T) {
	r := newRig(t)
	var calls atomic.Int64
	w := r.worker(func(c *queue.WorkerConfig) { c.LeaseFor = 300 * time.Millisecond }) // reaper runs every 20ms
	w.Handle("k", func(ctx context.Context, _ queue.Job) error {
		calls.Add(1)
		select { // three lease lengths of work
		case <-time.After(900 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	id := r.enqueue(queue.NewJob{Kind: "k"})
	start(t, w)
	ji := r.waitState(id, queue.StateDone)
	if ji.Attempts != 1 || calls.Load() != 1 {
		t.Fatalf("the job was reclaimed and rerun while still working: attempts=%d calls=%d", ji.Attempts, calls.Load())
	}
}

// lostLeases makes every heartbeat report that the lease is gone.
type lostLeases struct{ queue.Queue }

func (lostLeases) Extend(context.Context, queue.Job, time.Duration) (time.Time, error) {
	return time.Time{}, queue.ErrLeaseLost
}

func TestLosingTheLeaseCancelsTheHandler(t *testing.T) {
	r := newRig(t)
	var cause atomic.Value
	var calls atomic.Int64
	w := r.worker(func(c *queue.WorkerConfig) {
		c.Queue = lostLeases{r.q}
		c.LeaseFor = 90 * time.Millisecond // heartbeat every 30ms
	})
	w.Handle("k", func(ctx context.Context, _ queue.Job) error {
		if calls.Add(1) > 1 {
			return nil
		}
		select {
		case <-ctx.Done():
			cause.Store(context.Cause(ctx))
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return errors.New("the handler was never told its lease was lost")
		}
	})
	id := r.enqueue(queue.NewJob{Kind: "k"})
	start(t, w)
	r.waitState(id, queue.StateDone)
	if c, _ := cause.Load().(error); !errors.Is(c, queue.ErrLeaseLost) {
		t.Fatalf("handler context cause = %v, want ErrLeaseLost so handlers can tell why", c)
	}
}

func TestJobsStrandedByACrashedWorkerAreRecovered(t *testing.T) {
	r := newRig(t)
	id := r.enqueue(queue.NewJob{Kind: "k"})
	// A worker that took the job and then vanished, leaving only a short lease behind.
	if jobs, err := r.q.Lease(bg, "dead-worker", nil, 1, 50*time.Millisecond); err != nil || len(jobs) != 1 {
		t.Fatalf("setup: %v %d", err, len(jobs))
	}
	var attempt atomic.Int64
	w := r.worker(nil)
	w.Handle("k", func(_ context.Context, j queue.Job) error { attempt.Store(int64(j.Attempt)); return nil })
	start(t, w)
	r.waitState(id, queue.StateDone)
	if attempt.Load() != 2 {
		t.Fatalf("recovered job ran as attempt %d, want 2", attempt.Load())
	}
}

func TestAtStartupJobsAbandonedLongAgoAreReclaimedImmediately(t *testing.T) {
	r := newRig(t)
	id := r.enqueue(queue.NewJob{Kind: "k"})
	if _, err := r.q.Lease(bg, "dead-worker", nil, 1, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	w := r.worker(func(c *queue.WorkerConfig) { c.ReapEvery = time.Hour }) // the periodic sweep will not help
	w.Handle("k", ok)
	start(t, w)
	r.waitState(id, queue.StateDone)
}

func TestWorkersSharingAQueueNeverRunOneJobAtTheSameTime(t *testing.T) {
	r := newRig(t)
	var mu sync.Mutex
	active := map[string]bool{}
	runs := map[string]int{}
	handler := func(_ context.Context, j queue.Job) error {
		mu.Lock()
		if active[j.ID] {
			t.Errorf("job %s is being run by two workers at once", j.ID)
		}
		active[j.ID] = true
		runs[j.ID]++
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		mu.Lock()
		active[j.ID] = false
		mu.Unlock()
		return nil
	}
	var ws []*queue.Worker
	for i := 0; i < 3; i++ {
		w := r.worker(func(c *queue.WorkerConfig) { c.Concurrency = 3; c.Owner = fmt.Sprintf("worker-%d", i) })
		w.Handle("k", handler)
		ws = append(ws, w)
	}
	var ids []string
	for i := 0; i < 120; i++ {
		ids = append(ids, r.enqueue(queue.NewJob{Kind: "k"}))
	}
	for _, w := range ws {
		start(t, w)
	}
	for _, id := range ids {
		r.waitState(id, queue.StateDone)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, id := range ids {
		if runs[id] != 1 {
			t.Errorf("job %s ran %d times across the workers", id, runs[id])
		}
	}
}

// ---------------------------------------------------------------------------
// Shutdown

func TestShutdownLetsInFlightJobsFinish(t *testing.T) {
	r := newRig(t)
	started := make(chan struct{})
	w := r.worker(nil)
	w.Handle("k", func(context.Context, queue.Job) error {
		close(started)
		time.Sleep(300 * time.Millisecond) // ignores cancellation on purpose; finishing is the point
		return nil
	})
	id := r.enqueue(queue.NewJob{Kind: "k"})
	stop := start(t, w)
	<-started
	if err := stop(); err != nil {
		t.Fatalf("Run returned %v", err)
	}
	if ji := r.info(id); ji.State != queue.StateDone {
		t.Fatalf("an in-flight job was abandoned during a graceful shutdown: %+v", ji)
	}
}

func TestShutdownStopsTakingNewWork(t *testing.T) {
	r := newRig(t)
	w := r.worker(nil)
	w.Handle("k", ok)
	stop := start(t, w)
	first := r.enqueue(queue.NewJob{Kind: "k"})
	r.waitState(first, queue.StateDone)
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	later := r.enqueue(queue.NewJob{Kind: "k"})
	time.Sleep(100 * time.Millisecond)
	if ji := r.info(later); ji.State != queue.StateReady || ji.Attempts != 0 {
		t.Fatalf("a stopped worker must not pick up jobs: %+v", ji)
	}
}

func TestAnIdleWorkerStopsPromptly(t *testing.T) {
	r := newRig(t)
	w := r.worker(func(c *queue.WorkerConfig) { c.PollEvery = time.Hour; c.ReapEvery = time.Hour })
	w.Handle("k", ok)
	stop := start(t, w)
	time.Sleep(50 * time.Millisecond)
	began := time.Now()
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(began); d > time.Second {
		t.Fatalf("an idle worker took %v to stop; it must not wait out its poll interval", d)
	}
}

func TestRunWithAnAlreadyCancelledContextTakesNothing(t *testing.T) {
	r := newRig(t)
	id := r.enqueue(queue.NewJob{Kind: "k"})
	w := r.worker(nil)
	w.Handle("k", func(context.Context, queue.Job) error { t.Error("handler ran"); return nil })
	ctx, cancel := context.WithCancel(bg)
	cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
	if ji := r.info(id); ji.State != queue.StateReady || ji.Attempts != 0 {
		t.Fatalf("job was touched: %+v", ji)
	}
}

func TestWhenTheGracePeriodRunsOutHandlersAreCancelledAndJobsHandedBack(t *testing.T) {
	r := newRig(t)
	started := make(chan struct{})
	w := r.worker(func(c *queue.WorkerConfig) { c.ShutdownGrace = 100 * time.Millisecond })
	w.Handle("k", func(ctx context.Context, _ queue.Job) error {
		close(started)
		<-ctx.Done() // a well-behaved handler that stops when told to
		return ctx.Err()
	})
	id := r.enqueue(queue.NewJob{Kind: "k", MaxAttempts: 5})
	stop := start(t, w)
	<-started

	began := time.Now()
	if err := stop(); err != nil {
		t.Fatalf("Run returned %v", err)
	}
	if d := time.Since(began); d > 3*time.Second {
		t.Fatalf("shutdown took %v", d)
	}
	// Handed back for immediate retry elsewhere, not left leased until the lease expires.
	ji := r.info(id)
	if ji.State != queue.StateReady {
		t.Fatalf("job state after a forced shutdown = %q, want ready", ji.State)
	}
	jobs, _ := r.q.Lease(bg, "next-worker", nil, 1, time.Minute)
	if len(jobs) != 1 || jobs[0].ID != id {
		t.Fatalf("the interrupted job must be immediately available to another worker: %+v", jobs)
	}
}

// ---------------------------------------------------------------------------
// Housekeeping and robustness

func TestOldFinishedJobsArePurgedAccordingToRetention(t *testing.T) {
	r := newRig(t)
	w := r.worker(func(c *queue.WorkerConfig) {
		c.PurgeEvery = 20 * time.Millisecond
		c.RetainDone = time.Millisecond // purge done jobs almost immediately
		c.RetainDead = -1               // never purge dead letters
	})
	w.Handle("good", ok)
	w.Handle("bad", func(context.Context, queue.Job) error { return sdk.Permanent(errors.New("no")) })
	done := r.enqueue(queue.NewJob{Kind: "good"})
	dead := r.enqueue(queue.NewJob{Kind: "bad"})
	start(t, w)

	eventually(t, 10*time.Second, "the finished job to be purged", func() bool {
		_, err := r.q.Get(bg, done)
		return errors.Is(err, queue.ErrNotFound)
	})
	time.Sleep(100 * time.Millisecond)
	if ji := r.info(dead); ji.State != queue.StateDead {
		t.Fatalf("dead letters must survive when their retention is disabled: %+v", ji)
	}
}

func TestDeadLettersArePurgedOnlyWhenTheirOwnRetentionSaysSo(t *testing.T) {
	r := newRig(t)
	w := r.worker(func(c *queue.WorkerConfig) {
		c.PurgeEvery = 20 * time.Millisecond
		c.RetainDone = -1
		c.RetainDead = time.Millisecond
	})
	w.Handle("good", ok)
	w.Handle("bad", func(context.Context, queue.Job) error { return sdk.Permanent(errors.New("no")) })
	done := r.enqueue(queue.NewJob{Kind: "good"})
	dead := r.enqueue(queue.NewJob{Kind: "bad"})
	start(t, w)

	eventually(t, 10*time.Second, "the dead letter to be purged", func() bool {
		_, err := r.q.Get(bg, dead)
		return errors.Is(err, queue.ErrNotFound)
	})
	if ji := r.info(done); ji.State != queue.StateDone {
		t.Fatalf("done jobs must survive when their retention is disabled: %+v", ji)
	}
}

func TestADatabaseFailureNeverCrashesTheWorkerOrBlocksShutdown(t *testing.T) {
	r := newRig(t)
	w := r.worker(nil)
	w.Handle("k", ok)
	stop := start(t, w)
	r.enqueue(queue.NewJob{Kind: "k"})
	time.Sleep(50 * time.Millisecond)

	_ = r.db.Close() // the database vanishes under a running worker
	time.Sleep(150 * time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- stop() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the worker could not shut down after the database failed")
	}
}

func TestAHandlerIgnoringCancellationDoesNotCorruptOtherJobs(t *testing.T) {
	r := newRig(t)
	release := make(chan struct{})
	w := r.worker(nil)
	w.Handle("stubborn", func(context.Context, queue.Job) error { <-release; return nil })
	w.Handle("k", ok)
	stubborn := r.enqueue(queue.NewJob{Kind: "stubborn"})
	start(t, w)
	r.waitState(stubborn, queue.StateLeased)

	fine := r.enqueue(queue.NewJob{Kind: "k"})
	r.waitState(fine, queue.StateDone)
	close(release)
	r.waitState(stubborn, queue.StateDone)
}

// ---------------------------------------------------------------------------
// Setup mistakes fail loudly and early

func TestRegisteringHandlersIncorrectlyPanicsAtSetupTime(t *testing.T) {
	r := newRig(t)
	mustPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s should panic", name)
			}
		}()
		fn()
	}
	w := r.worker(nil)
	mustPanic("empty kind", func() { w.Handle("", ok) })
	mustPanic("nil handler", func() { w.Handle("k", nil) })
	w.Handle("k", ok)
	mustPanic("duplicate kind", func() { w.Handle("k", ok) })

	handled := make(chan struct{}, 1)
	w2 := r.worker(nil)
	w2.Handle("go", func(context.Context, queue.Job) error {
		select {
		case handled <- struct{}{}:
		default:
		}
		return nil
	})
	r.enqueue(queue.NewJob{Kind: "go"})
	start(t, w2)
	<-handled
	mustPanic("registration after Run", func() { w2.Handle("late", ok) })
}

func TestRunMisuseReturnsErrors(t *testing.T) {
	r := newRig(t)
	if _, err := queue.NewWorker(queue.WorkerConfig{}); err == nil {
		t.Error("a worker without a queue must be rejected")
	}
	empty := r.worker(nil)
	if err := empty.Run(bg); err == nil {
		t.Error("Run with no handlers must fail (it would idle forever)")
	}

	w := r.worker(nil)
	w.Handle("k", ok)
	stop := start(t, w)
	time.Sleep(30 * time.Millisecond)
	if err := w.Run(bg); err == nil {
		t.Error("a second Run on the same worker must fail")
	}
	_ = stop()
}

func TestAWorkerWithOnlyAQueueWorksWithSensibleDefaults(t *testing.T) {
	r := newRig(t)
	w, err := queue.NewWorker(queue.WorkerConfig{Queue: r.q, Log: quietLog})
	if err != nil {
		t.Fatal(err)
	}
	w.Handle("k", ok)
	id := r.enqueue(queue.NewJob{Kind: "k"})
	start(t, w)
	eventually(t, 10*time.Second, "the default-configured worker to process a job", func() bool {
		return r.info(id).State == queue.StateDone
	})
}
