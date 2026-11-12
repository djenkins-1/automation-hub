package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

// Handler processes one leased job. Returning nil acks the job. Errors are
// classified with the sdk helpers:
//
//	sdk.Permanent(err)       dead-letter immediately
//	sdk.RetryAfter(err, d)   retry no sooner than d from now
//	any other error          retry after RetryPolicy backoff
//
// The context is cancelled if the lease is lost, the handler timeout elapses,
// or the worker's shutdown grace period runs out. Handlers should honour it.
// A panic is recovered and treated as a retryable error.
//
// Handlers must be idempotent: delivery is at-least-once.
type Handler func(ctx context.Context, j Job) error

// WorkerConfig configures a Worker. Only Queue is required.
type WorkerConfig struct {
	Queue Queue
	Admin Admin // optional; when set the worker also purges old finished jobs

	Owner          string        // recorded on leases; default "<hostname>-<pid>"
	Concurrency    int           // max jobs in flight; default 4
	LeaseFor       time.Duration // lease length; heartbeats fire every LeaseFor/3; default 30s
	PollEvery      time.Duration // idle polling interval; default 500ms
	ReapEvery      time.Duration // expired-lease sweep interval; default 10s
	PurgeEvery     time.Duration // retention sweep interval; default 1h
	HandlerTimeout time.Duration // hard cap on one handler run; default 2m
	ShutdownGrace  time.Duration // how long to let in-flight jobs finish on shutdown; default 20s

	// Retention for finished jobs (only used when Admin is set). Zero means
	// the default (done: 7 days, dead: 30 days); negative disables purging.
	RetainDone time.Duration
	RetainDead time.Duration

	Policy RetryPolicy // backoff for retryable errors
	Clock  Clock       // must match the queue's clock; default SystemClock
	Log    *slog.Logger
}

func (c *WorkerConfig) applyDefaults() error {
	if c.Queue == nil {
		return errors.New("queue: worker requires a Queue")
	}
	if c.Owner == "" {
		host, _ := os.Hostname()
		if host == "" {
			host = "worker"
		}
		c.Owner = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	def := func(v *time.Duration, d time.Duration) {
		if *v <= 0 {
			*v = d
		}
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 4
	}
	def(&c.LeaseFor, 30*time.Second)
	def(&c.PollEvery, 500*time.Millisecond)
	def(&c.ReapEvery, 10*time.Second)
	def(&c.PurgeEvery, time.Hour)
	def(&c.HandlerTimeout, 2*time.Minute)
	def(&c.ShutdownGrace, 20*time.Second)
	if c.RetainDone == 0 {
		c.RetainDone = 7 * 24 * time.Hour
	}
	if c.RetainDead == 0 {
		c.RetainDead = 30 * 24 * time.Hour
	}
	if c.Clock == nil {
		c.Clock = SystemClock{}
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
	c.Policy = c.Policy.withDefaults()
	return nil
}

// Worker leases jobs and runs them through registered handlers.
//
// It runs three loops: polling (lease up to the free concurrency slots, run
// each job in its own goroutine with a heartbeat), reaping (return expired
// leases to the queue) and, if Admin is set, purging. Every node in a cluster
// can run a Worker; leases keep them from stepping on each other.
type Worker struct {
	cfg WorkerConfig

	mu       sync.Mutex
	handlers map[string]Handler
	started  bool
}

// NewWorker validates cfg and returns a Worker with no handlers registered.
func NewWorker(cfg WorkerConfig) (*Worker, error) {
	if err := cfg.applyDefaults(); err != nil {
		return nil, err
	}
	return &Worker{cfg: cfg, handlers: make(map[string]Handler)}, nil
}

// Handle registers the handler for a job kind. It panics on an empty kind, a
// nil handler, a duplicate registration, or if called after Run has started;
// all of these are programming errors best caught at startup.
func (w *Worker) Handle(kind string, h Handler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch {
	case kind == "" || h == nil:
		panic("queue: Handle requires a kind and a handler")
	case w.started:
		panic("queue: Handle called after Run")
	}
	if _, dup := w.handlers[kind]; dup {
		panic(fmt.Sprintf("queue: duplicate handler for kind %q", kind))
	}
	w.handlers[kind] = h
}

// Run processes jobs until ctx is cancelled, then shuts down gracefully:
// it stops leasing, gives in-flight handlers ShutdownGrace to finish, cancels
// any still running, and returns. Jobs abandoned by a hard stop are recovered
// by the reaper on the next start. Run may be called only once.
func (w *Worker) Run(ctx context.Context) error {
	w.mu.Lock()
	if w.started {
		w.mu.Unlock()
		return errors.New("queue: worker already started")
	}
	if len(w.handlers) == 0 {
		w.mu.Unlock()
		return errors.New("queue: worker has no handlers")
	}
	w.started = true
	handlers := make(map[string]Handler, len(w.handlers))
	kinds := make([]string, 0, len(w.handlers))
	for k, h := range w.handlers {
		handlers[k] = h
		kinds = append(kinds, k)
	}
	w.mu.Unlock()
	sort.Strings(kinds)

	cfg := w.cfg
	log := cfg.Log

	// Handlers run under jobCtx, which is NOT cancelled when ctx is: that is
	// what lets in-flight work drain during the grace period.
	jobCtx, cancelJobs := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelJobs()

	var bg sync.WaitGroup
	bg.Add(1)
	go func() { defer bg.Done(); w.reapLoop(ctx) }()
	if cfg.Admin != nil {
		bg.Add(1)
		go func() { defer bg.Done(); w.purgeLoop(ctx) }()
	}

	var inflight sync.WaitGroup
	sem := make(chan struct{}, cfg.Concurrency)
	wake := make(chan struct{}, 1) // a finished job pokes the poller so slots refill quickly
	timer := time.NewTimer(cfg.PollEvery)
	defer timer.Stop()

	log.Info("queue worker started", "owner", cfg.Owner, "kinds", kinds, "concurrency", cfg.Concurrency)

	for ctx.Err() == nil {
		if free := cap(sem) - len(sem); free > 0 {
			// Not tied to ctx: if the lease commits we want to run the jobs
			// rather than abandon them to the reaper.
			lctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			jobs, err := cfg.Queue.Lease(lctx, cfg.Owner, kinds, free, cfg.LeaseFor)
			cancel()
			if err != nil {
				log.Error("queue lease failed", "err", err)
			}
			for _, j := range jobs {
				sem <- struct{}{} // cannot block: this loop is the only acquirer and free slots were reserved
				inflight.Add(1)
				go func(j Job) {
					defer func() {
						<-sem
						inflight.Done()
						select {
						case wake <- struct{}{}:
						default:
						}
					}()
					w.execute(jobCtx, ctx, handlers[j.Kind], j)
				}(j)
			}
		}

		timer.Reset(cfg.PollEvery)
		select {
		case <-ctx.Done():
		case <-wake:
		case <-timer.C:
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}

	log.Info("queue worker draining", "grace", cfg.ShutdownGrace)
	drained := make(chan struct{})
	go func() { inflight.Wait(); close(drained) }()

	var runErr error
	select {
	case <-drained:
	case <-time.After(cfg.ShutdownGrace):
		log.Warn("shutdown grace expired; cancelling in-flight handlers")
		cancelJobs()
		select {
		case <-drained:
		case <-time.After(5 * time.Second):
			runErr = errors.New("queue: handlers did not stop after cancellation")
			log.Error("handlers ignored cancellation", "err", runErr)
		}
	}
	bg.Wait()
	log.Info("queue worker stopped")
	return runErr
}

// execute runs one job: heartbeat while the handler works, then translate the
// handler's result into Ack, Nack or Fail.
//
// base is the context handlers run under; runCtx is the worker's own context
// (cancelled at shutdown), consulted only to tell a shutdown-induced
// cancellation from an ordinary failure.
func (w *Worker) execute(base, runCtx context.Context, h Handler, j Job) {
	log := w.cfg.Log.With("job", j.ID, "kind", j.Kind, "attempt", j.Attempt)

	if h == nil { // unreachable: we only lease kinds that have handlers
		w.finish(base, runCtx, j, sdk.Permanent(fmt.Errorf("no handler for kind %q", j.Kind)), log)
		return
	}

	hctx, cancelCause := context.WithCancelCause(base)
	defer cancelCause(nil)
	hctx, cancelTimeout := context.WithTimeout(hctx, w.cfg.HandlerTimeout)
	defer cancelTimeout()

	// Heartbeat: keep extending the lease while the handler runs. If the lease
	// is gone, cancel the handler; its result can no longer be recorded.
	hbCtx, stopHB := context.WithCancel(hctx)
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		t := time.NewTicker(w.cfg.LeaseFor / 3)
		defer t.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-t.C:
				_, err := w.cfg.Queue.Extend(hbCtx, j, w.cfg.LeaseFor)
				switch {
				case err == nil:
				case errors.Is(err, ErrLeaseLost):
					log.Warn("lease lost; cancelling handler")
					cancelCause(ErrLeaseLost)
					return
				case hbCtx.Err() != nil:
					return
				default:
					log.Warn("lease heartbeat failed; will retry", "err", err)
				}
			}
		}
	}()

	err := safeCall(hctx, h, j, log)

	stopHB()
	<-hbDone
	w.finish(base, runCtx, j, err, log)
}

// finish records the outcome of a handler run on the queue. It uses a context
// detached from cancellation so that outcomes are recorded during shutdown.
func (w *Worker) finish(base, runCtx context.Context, j Job, herr error, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(base), 5*time.Second)
	defer cancel()

	var qerr error
	switch {
	case herr == nil:
		qerr = w.cfg.Queue.Ack(ctx, j)
		if qerr == nil {
			log.Debug("job done")
		}
	case sdk.IsPermanent(herr):
		log.Warn("job failed permanently", "err", herr)
		qerr = w.cfg.Queue.Fail(ctx, j, herr)
	default:
		delay := w.cfg.Policy.Backoff(j.Attempt)
		if d, ok := sdk.RetryDelay(herr); ok {
			delay = d
		}
		if runCtx.Err() != nil && errors.Is(herr, context.Canceled) {
			delay = 0 // interrupted by shutdown: hand the job straight back
		}
		if j.Attempt >= j.MaxAttempts {
			// Nack dead-letters a job that has used its last attempt.
			log.Warn("job failed; out of attempts", "err", herr, "max_attempts", j.MaxAttempts)
		} else {
			log.Warn("job failed; will retry", "err", herr, "retry_in", delay)
		}
		qerr = w.cfg.Queue.Nack(ctx, j, w.cfg.Clock.Now().Add(delay), herr)
	}

	switch {
	case qerr == nil:
	case errors.Is(qerr, ErrLeaseLost):
		log.Warn("outcome discarded: lease lost (another worker owns this job now)")
	default:
		// The lease will expire and the reaper will recover the job.
		log.Error("could not record job outcome", "err", qerr)
	}
}

// safeCall runs h, converting a panic into an error.
func safeCall(ctx context.Context, h Handler, j Job, log *slog.Logger) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("handler panicked", "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return h(ctx, j)
}

func (w *Worker) reapLoop(ctx context.Context) {
	sweep := func() {
		n, err := w.cfg.Queue.ReapExpired(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			w.cfg.Log.Error("reap failed", "err", err)
		case n > 0:
			w.cfg.Log.Warn("reclaimed jobs with expired leases", "count", n)
		}
	}
	sweep() // recover jobs orphaned by a previous crash right at startup
	t := time.NewTicker(w.cfg.ReapEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			sweep()
		}
	}
}

func (w *Worker) purgeLoop(ctx context.Context) {
	sweep := func() {
		now := w.cfg.Clock.Now()
		for _, p := range []struct {
			state  string
			retain time.Duration
		}{
			{StateDone, w.cfg.RetainDone},
			{StateDead, w.cfg.RetainDead},
		} {
			if p.retain < 0 {
				continue
			}
			n, err := w.cfg.Admin.Purge(ctx, p.state, now.Add(-p.retain))
			switch {
			case err != nil && ctx.Err() == nil:
				w.cfg.Log.Error("purge failed", "state", p.state, "err", err)
			case n > 0:
				w.cfg.Log.Info("purged finished jobs", "state", p.state, "count", n)
			}
		}
	}
	sweep()
	t := time.NewTicker(w.cfg.PurgeEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			sweep()
		}
	}
}
