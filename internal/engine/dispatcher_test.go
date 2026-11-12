package engine_test

// Specification of the Dispatcher: sending one delivery and keeping an honest record.
//
//  - A delivery is sent to its sink once per successful attempt, with
//    a stable identity (so the destination can drop duplicates).
//  - The error the sink returns reaches the queue worker UNCHANGED, so its
//    classification (permanent / retry-after / plain) still decides the job's
//    fate; the dispatcher only adds the audit trail.
//  - The delivery's status always tells the truth about what the queue will do:
//    pending while retries remain, failed once they cannot, delivered stays delivered.
//  - Replays and overlapping workers cannot undo a success or cause needless resends.
//  - A misbehaving sink (panic, hang) or a failing audit store cannot corrupt the record or
//    cause a duplicate send.
//  - Deliveries are independent of each other.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djenkins-1/automation-hub/internal/engine"
	"github.com/djenkins-1/automation-hub/internal/queue"
	"github.com/djenkins-1/automation-hub/internal/rules"
	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

func TestASuccessfulDeliveryIsSentOnceAndRecorded(t *testing.T) {
	h := newHarness(t)
	eventID, job := h.routed(diskEvent(95))

	if err := h.dispatcher.Handle(ctx, tryAttempt(job, 1, 3)); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if h.rec.Count() != 1 {
		t.Fatalf("the sink was called %d times", h.rec.Count())
	}
	d := h.onlyDelivery(eventID)
	if d.Status != engine.DeliveryDelivered || d.Attempts != 1 || d.LastError != "" || d.DeliveredAt.IsZero() {
		t.Fatalf("delivery after success: %+v", d)
	}
	as := h.attempts(d.ID)
	if len(as) != 1 || as[0].Outcome != engine.OutcomeDelivered || as[0].Number != 1 || as[0].Detail == "" || as[0].Error != "" ||
		as[0].FinishedAt.Before(as[0].StartedAt) {
		t.Fatalf("attempt log: %+v (the sink's result detail should be kept)", as)
	}
}

func TestTheSinkReceivesTheRenderedMessageAndAStableIdentity(t *testing.T) {
	h := newHarness(t)
	h.rec.Behavior = func(sdk.Notification, int) error { return errors.New("not yet") }
	eventID, job := h.routed(diskEvent(95))
	d := h.onlyDelivery(eventID)

	for attempt := 1; attempt <= 3; attempt++ {
		_ = h.dispatcher.Handle(ctx, tryAttempt(job, attempt, 9))
	}
	got := h.rec.Notifications()
	if len(got) != 3 {
		t.Fatalf("%d sink calls", len(got))
	}
	for i, n := range got {
		if n.Title != "Disk /var" || n.Body != "95% used" {
			t.Errorf("call %d: message %q / %q", i+1, n.Title, n.Body)
		}
		if n.Attempt != i+1 {
			t.Errorf("call %d reported attempt %d", i+1, n.Attempt)
		}
		if n.ID != d.ID || n.IdempotencyKey != d.IdempotencyKey || n.IdempotencyKey == "" {
			t.Errorf("call %d: id=%q key=%q; the identity must be identical on every retry and match the delivery (%q / %q)", i+1, n.ID, n.IdempotencyKey, d.ID, d.IdempotencyKey)
		}
		if n.Event.ID != eventID || n.Event.Source != "monitor" || n.Event.Type != "disk.full" || n.Event.Subject != "/var" || n.Event.Data["percent"] != 95.0 {
			t.Errorf("call %d: the sink should get the full triggering event, got %+v", i+1, n.Event)
		}
	}
}

func TestTheDeliverysStatusMatchesWhatTheQueueWillDo(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name        string
		err         error
		attempt     int
		max         int
		wantStatus  engine.DeliveryStatus
		wantOutcome engine.AttemptOutcome
	}{
		{"plain error, attempts remain", boom, 1, 3, engine.DeliveryPending, engine.OutcomeRetry},
		{"plain error, one attempt left", boom, 2, 3, engine.DeliveryPending, engine.OutcomeRetry},
		{"plain error, the last attempt", boom, 3, 3, engine.DeliveryFailed, engine.OutcomeFailed},
		{"plain error, beyond the last attempt (a replay)", boom, 4, 3, engine.DeliveryFailed, engine.OutcomeFailed},
		{"plain error, only one attempt allowed", boom, 1, 1, engine.DeliveryFailed, engine.OutcomeFailed},
		{"permanent, attempts remain", sdk.Permanent(boom), 1, 5, engine.DeliveryFailed, engine.OutcomeFailed},
		{"permanent, wrapped afterwards", fmt.Errorf("ctx: %w", sdk.Permanent(boom)), 1, 5, engine.DeliveryFailed, engine.OutcomeFailed},
		{"permanent beats retry-after", sdk.Permanent(sdk.RetryAfter(boom, time.Hour)), 1, 5, engine.DeliveryFailed, engine.OutcomeFailed},
		{"retry-after, attempts remain", sdk.RetryAfter(boom, time.Minute), 1, 3, engine.DeliveryPending, engine.OutcomeRetry},
		{"retry-after, the last attempt", sdk.RetryAfter(boom, time.Minute), 3, 3, engine.DeliveryFailed, engine.OutcomeFailed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.rec.Behavior = func(sdk.Notification, int) error { return tc.err }
			eventID, job := h.routed(diskEvent(95))

			got := h.dispatcher.Handle(ctx, tryAttempt(job, tc.attempt, tc.max))

			// the error reaches the worker exactly as the sink produced it
			if got == nil || got.Error() != tc.err.Error() || !errors.Is(got, boom) ||
				sdk.IsPermanent(got) != sdk.IsPermanent(tc.err) {
				t.Fatalf("returned %v; the sink's error (and its classification) must reach the worker unchanged", got)
			}
			wantDelay, wantHas := sdk.RetryDelay(tc.err)
			if gotDelay, gotHas := sdk.RetryDelay(got); gotHas != wantHas || gotDelay != wantDelay {
				t.Fatalf("retry-after changed from (%v,%v) to (%v,%v)", wantDelay, wantHas, gotDelay, gotHas)
			}

			d := h.onlyDelivery(eventID)
			if d.Status != tc.wantStatus || d.Attempts != 1 || d.LastError != tc.err.Error() || !d.DeliveredAt.IsZero() {
				t.Fatalf("delivery = %+v, want status %s", d, tc.wantStatus)
			}
			if as := h.attempts(d.ID); len(as) != 1 || as[0].Outcome != tc.wantOutcome || as[0].Error != tc.err.Error() || as[0].Number != tc.attempt {
				t.Fatalf("attempt log = %+v", as)
			}
		})
	}
}

func TestAFlakySinkEventuallyDeliversAndTheLogTellsTheStory(t *testing.T) {
	h := newHarness(t)
	h.rec.Behavior = func(_ sdk.Notification, call int) error {
		if call < 3 {
			return fmt.Errorf("flaky #%d", call)
		}
		return nil
	}
	eventID, job := h.routed(diskEvent(95))
	for attempt := 1; attempt <= 3; attempt++ {
		_ = h.dispatcher.Handle(ctx, tryAttempt(job, attempt, 6))
	}
	d := h.onlyDelivery(eventID)
	if d.Status != engine.DeliveryDelivered || d.Attempts != 3 || d.LastError != "" {
		t.Fatalf("delivery = %+v", d)
	}
	as := h.attempts(d.ID)
	if len(as) != 3 || as[0].Outcome != engine.OutcomeRetry || as[0].Error != "flaky #1" ||
		as[1].Outcome != engine.OutcomeRetry || as[1].Error != "flaky #2" || as[2].Outcome != engine.OutcomeDelivered {
		t.Fatalf("log = %+v", as)
	}
}

func TestAReplayAfterSuccessDoesNotSendAgain(t *testing.T) {
	h := newHarness(t)
	eventID, job := h.routed(diskEvent(95))
	if err := h.dispatcher.Handle(ctx, tryAttempt(job, 1, 3)); err != nil {
		t.Fatal(err)
	}
	for attempt := 2; attempt <= 4; attempt++ { // the ack was lost, so the queue keeps handing the job out
		if err := h.dispatcher.Handle(ctx, tryAttempt(job, attempt, 6)); err != nil {
			t.Fatalf("replay %d: %v (it must be a quiet success so the job finally gets acked)", attempt, err)
		}
	}
	if h.rec.Count() != 1 {
		t.Fatalf("the sink was called %d times; a replay after success must never send again", h.rec.Count())
	}
	if d := h.onlyDelivery(eventID); d.Attempts != 1 || d.Status != engine.DeliveryDelivered {
		t.Fatalf("a replay must not change the record: %+v", d)
	}
}

// Two workers can overlap on one delivery when a lease expires mid-send. If the
// slower one then fails, the success must stand.
func TestDeliveredStaysDeliveredWhenAnOverlappingWorkerFails(t *testing.T) {
	h := newHarness(t)
	entered := make(chan struct{}, 2)
	release1, release2 := make(chan struct{}), make(chan struct{})
	h.rec.Behavior = func(_ sdk.Notification, call int) error {
		entered <- struct{}{}
		if call == 1 {
			<-release1
			return nil
		}
		<-release2
		return errors.New("late failure")
	}
	eventID, job := h.routed(diskEvent(95))

	results := make(chan error, 2)
	go func() { results <- h.dispatcher.Handle(ctx, tryAttempt(job, 1, 5)) }()
	go func() { results <- h.dispatcher.Handle(ctx, tryAttempt(job, 2, 5)) }()
	<-entered
	<-entered // both are inside the sink, so both passed the "already delivered?" check

	close(release1)
	if err := <-results; err != nil {
		t.Fatalf("the successful worker returned %v", err)
	}
	close(release2)
	if err := <-results; err == nil {
		t.Fatal("the failing worker should report its failure")
	}

	d := h.onlyDelivery(eventID)
	if d.Status != engine.DeliveryDelivered || d.LastError != "" || d.DeliveredAt.IsZero() {
		t.Fatalf("a late failure undid a success: %+v", d)
	}
	if d.Attempts != 2 || len(h.attempts(d.ID)) != 2 {
		t.Fatalf("both attempts must still be on record: attempts=%d log=%d", d.Attempts, len(h.attempts(d.ID)))
	}
}

func TestADeadLetteredDeliveryCanStillSucceedAfterARequeue(t *testing.T) {
	h := newHarness(t)
	h.rec.Behavior = func(sdk.Notification, int) error { return sdk.Permanent(errors.New("HTTP 401")) }
	eventID, job := h.routed(diskEvent(95))
	_ = h.dispatcher.Handle(ctx, tryAttempt(job, 1, 3))
	if d := h.onlyDelivery(eventID); d.Status != engine.DeliveryFailed {
		t.Fatalf("setup: %+v", d)
	}

	h.rec.Behavior = nil // the operator fixed the credentials and requeued the dead job
	if err := h.dispatcher.Handle(ctx, tryAttempt(job, 1, 3)); err != nil {
		t.Fatal(err)
	}
	d := h.onlyDelivery(eventID)
	if d.Status != engine.DeliveryDelivered || d.Attempts != 2 || d.LastError != "" {
		t.Fatalf("delivery = %+v", d)
	}
}

func TestDeliveriesAreIndependentOfEachOther(t *testing.T) {
	good, bad := &recorderSink{}, &recorderSink{err: sdk.Permanent(errors.New("bad sink"))}
	h := newHarness(t, rules.Rule{Name: "both", Actions: []rules.Action{act("good", "to good"), act("bad", "to bad")}})
	d := engine.NewDispatcher(h.deliveries, h.events, h.sinkSet(map[string]sdk.Sink{"good": good, "bad": bad}), h.opts)

	res := h.accept(diskEvent(95))
	h.route()
	jobs := h.lease(engine.KindDeliver)
	if len(jobs) != 2 {
		t.Fatalf("%d deliver jobs", len(jobs))
	}
	for _, j := range jobs {
		_ = d.Handle(ctx, tryAttempt(j, 1, 3))
	}
	status := map[string]engine.DeliveryStatus{}
	for _, del := range h.deliveriesFor(res.EventID) {
		status[del.Sink] = del.Status
	}
	if status["good"] != engine.DeliveryDelivered || status["bad"] != engine.DeliveryFailed {
		t.Fatalf("statuses = %v; one sink's failure must not affect the other delivery", status)
	}
	if good.calls.Load() != 1 || bad.calls.Load() != 1 {
		t.Fatalf("calls: good=%d bad=%d", good.calls.Load(), bad.calls.Load())
	}
}

type recorderSink struct {
	err   error
	calls atomic.Int32
}

func (r *recorderSink) Deliver(context.Context, sdk.Notification) (sdk.Result, error) {
	r.calls.Add(1)
	return sdk.Result{Detail: "ok"}, r.err
}

func TestUnknownSinksFailPermanentlyAndSayWhich(t *testing.T) {
	h := newHarness(t)
	res := h.accept(diskEvent(95))
	if _, _, err := h.deliveries.CreateDelivery(ctx, engine.NewDelivery{
		ID: "d-ghost", EventID: res.EventID, Rule: "r", Sink: "removed-from-config", Title: "t", IdempotencyKey: "k", CreatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	err := h.dispatcher.Handle(ctx, queue.Job{Payload: []byte(`{"delivery_id":"d-ghost"}`), Attempt: 1, MaxAttempts: 5})
	if !sdk.IsPermanent(err) || !strings.Contains(err.Error(), "removed-from-config") {
		t.Fatalf("err = %v, want a permanent error naming the missing sink", err)
	}
	d, _ := h.deliveries.GetDelivery(ctx, "d-ghost")
	if d.Status != engine.DeliveryFailed || !strings.Contains(d.LastError, "removed-from-config") || d.Attempts != 1 {
		t.Fatalf("delivery = %+v", d)
	}
}

func TestJobsThatCannotBeDispatchedArePermanentFailures(t *testing.T) {
	h := newHarness(t)
	for name, payload := range map[string]string{
		"not json": `}{`, "empty": ``, "no id": `{}`, "id of the wrong type": `{"delivery_id": 7}`, "empty id": `{"delivery_id": ""}`,
		"unknown delivery": `{"delivery_id":"nope"}`,
	} {
		err := h.dispatcher.Handle(ctx, queue.Job{Payload: []byte(payload), Attempt: 1, MaxAttempts: 3})
		if !sdk.IsPermanent(err) {
			t.Errorf("%s: err = %v, want permanent", name, err)
		}
	}
	if h.rec.Count() != 0 {
		t.Fatal("the sink must not be called for a job that points at nothing")
	}
}

func TestAFailureToLoadTheDeliveryIsRetryableAndSendsNothing(t *testing.T) {
	h := newHarness(t)
	_, job := h.routed(diskEvent(95))
	d := engine.NewDispatcher(&brokenDeliveries{DeliveryStore: h.deliveries, failGet: errInjected}, h.events, h.sinkSet(map[string]sdk.Sink{"rec": h.rec}), h.opts)
	err := d.Handle(ctx, tryAttempt(job, 1, 3))
	if !errors.Is(err, errInjected) || sdk.IsPermanent(err) {
		t.Fatalf("err = %v, want a retryable error", err)
	}
	if h.rec.Count() != 0 {
		t.Fatal("nothing may be sent when the delivery could not be loaded")
	}
}

func TestADeliveryWhoseEventWasPurgedIsStillDelivered(t *testing.T) {
	h := newHarness(t)
	_, job := h.routed(diskEvent(95))
	// The message was rendered at routing time, so the event is only extra context.
	broken := brokenEvents{EventStore: h.events, failGet: engine.ErrNotFound}
	d := engine.NewDispatcher(h.deliveries, broken, h.sinkSet(map[string]sdk.Sink{"rec": h.rec}), h.opts)
	if err := d.Handle(ctx, tryAttempt(job, 1, 3)); err != nil {
		t.Fatalf("err = %v", err)
	}
	got := h.rec.Notifications()
	if len(got) != 1 || got[0].Title != "Disk /var" || got[0].Event.ID == "" {
		t.Fatalf("notification = %+v; the message must still go out, with at least the event's ID", got)
	}
}

func TestAnEventStoreFailureWhileLoadingContextIsRetryable(t *testing.T) {
	h := newHarness(t)
	_, job := h.routed(diskEvent(95))
	broken := brokenEvents{EventStore: h.events, failGet: errInjected}
	d := engine.NewDispatcher(h.deliveries, broken, h.sinkSet(map[string]sdk.Sink{"rec": h.rec}), h.opts)
	err := d.Handle(ctx, tryAttempt(job, 1, 3))
	if !errors.Is(err, errInjected) || sdk.IsPermanent(err) || h.rec.Count() != 0 {
		t.Fatalf("err=%v calls=%d: a database hiccup must not be mistaken for a missing event", err, h.rec.Count())
	}
}

func TestAPanickingSinkIsContainedAndRecorded(t *testing.T) {
	h := newHarness(t)
	h.rec.Behavior = func(sdk.Notification, int) error { panic("plugin bug: nil map") }
	eventID, job := h.routed(diskEvent(95))

	err := h.dispatcher.Handle(ctx, tryAttempt(job, 1, 3)) // must return, not crash the process
	if err == nil || !strings.Contains(err.Error(), "panic") || sdk.IsPermanent(err) {
		t.Fatalf("err = %v, want a retryable error that says the sink panicked", err)
	}
	d := h.onlyDelivery(eventID)
	if d.Status != engine.DeliveryPending || d.Attempts != 1 || !strings.Contains(d.LastError, "plugin bug") {
		t.Fatalf("the attempt must still be on record: %+v", d)
	}
}

type contextSink struct {
	deadline atomic.Value // time.Time
	block    bool
	started  chan struct{}
	once     sync.Once
}

func (s *contextSink) Deliver(c context.Context, _ sdk.Notification) (sdk.Result, error) {
	if dl, ok := c.Deadline(); ok {
		s.deadline.Store(dl)
	}
	if s.started != nil {
		s.once.Do(func() { close(s.started) })
	}
	if s.block {
		<-c.Done()
		return sdk.Result{}, c.Err()
	}
	return sdk.Result{}, nil
}

func TestASinkIsGivenADeadlineAndAHangingSinkIsCutOff(t *testing.T) {
	h := newHarness(t)
	_, job := h.routed(diskEvent(95))
	sink := &contextSink{block: true}
	opts := h.opts
	opts.SinkTimeout = 50 * time.Millisecond
	d := engine.NewDispatcher(h.deliveries, h.events, h.sinkSet(map[string]sdk.Sink{"rec": sink}), opts)

	began := time.Now()
	err := d.Handle(ctx, tryAttempt(job, 1, 3))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if since := time.Since(began); since > 5*time.Second {
		t.Fatalf("a hanging sink held the worker for %v", since)
	}
	dl, _ := sink.deadline.Load().(time.Time)
	if dl.IsZero() || dl.Sub(began) > time.Second {
		t.Fatalf("the sink's context deadline was %v after start; it should reflect the 50ms timeout", dl.Sub(began))
	}
	if as := h.attempts(h.deliveriesFor(h.firstEvent())[0].ID); len(as) != 1 || as[0].Outcome != engine.OutcomeRetry {
		t.Fatalf("the timed-out attempt must be recorded as a retry: %+v", as)
	}
}

func (h *harness) firstEvent() string {
	h.t.Helper()
	var id string
	if err := h.db.Read.QueryRowContext(ctx, `SELECT id FROM events ORDER BY id LIMIT 1`).Scan(&id); err != nil {
		h.t.Fatal(err)
	}
	return id
}

func TestShutdownDuringASendStillRecordsTheAttempt(t *testing.T) {
	h := newHarness(t)
	eventID, job := h.routed(diskEvent(95))
	sink := &contextSink{block: true, started: make(chan struct{})}
	d := engine.NewDispatcher(h.deliveries, h.events, h.sinkSet(map[string]sdk.Sink{"rec": sink}), h.opts)

	c, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- d.Handle(c, tryAttempt(job, 1, 3)) }()
	<-sink.started
	cancel() // the hub is shutting down while the sink is mid-send

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Handle did not return after cancellation")
	}
	dl := h.onlyDelivery(eventID)
	if dl.Attempts != 1 || dl.Status != engine.DeliveryPending {
		t.Fatalf("the interrupted attempt vanished from the record: %+v", dl)
	}
}

func TestAFailingAuditStoreNeverCausesADuplicateSend(t *testing.T) {
	h := newHarness(t)
	_, job := h.routed(diskEvent(95))
	d := engine.NewDispatcher(&brokenDeliveries{DeliveryStore: h.deliveries, failRecord: true}, h.events, h.sinkSet(map[string]sdk.Sink{"rec": h.rec}), h.opts)

	// The message went out but the record could not be written. Failing the
	// job would make the queue send it again, so the dispatcher reports success.
	if err := d.Handle(ctx, tryAttempt(job, 1, 3)); err != nil {
		t.Fatalf("err = %v; a delivered message must not be re-sent just because bookkeeping failed", err)
	}
	if h.rec.Count() != 1 {
		t.Fatalf("sink calls = %d", h.rec.Count())
	}

	// And if the send itself failed, the sink's error (not the bookkeeping error) is what the worker sees.
	h2 := newHarness(t)
	h2.rec.Behavior = func(sdk.Notification, int) error { return sdk.Permanent(errors.New("sink said no")) }
	_, job2 := h2.routed(diskEvent(95))
	d2 := engine.NewDispatcher(&brokenDeliveries{DeliveryStore: h2.deliveries, failRecord: true}, h2.events, h2.sinkSet(map[string]sdk.Sink{"rec": h2.rec}), h2.opts)
	err := d2.Handle(ctx, tryAttempt(job2, 1, 3))
	if err == nil || !sdk.IsPermanent(err) || !strings.Contains(err.Error(), "sink said no") {
		t.Fatalf("err = %v, want the sink's own permanent error", err)
	}
}

func TestHugeSinkErrorsAreHandled(t *testing.T) {
	h := newHarness(t)
	huge := strings.Repeat("x", 500_000)
	h.rec.Behavior = func(sdk.Notification, int) error { return errors.New(huge) }
	eventID, job := h.routed(diskEvent(95))
	if err := h.dispatcher.Handle(ctx, tryAttempt(job, 1, 3)); err == nil {
		t.Fatal("expected the error")
	}
	if d := h.onlyDelivery(eventID); d.Attempts != 1 || !strings.HasPrefix(d.LastError, "xxxx") {
		t.Fatalf("a huge error message broke the record: attempts=%d, %d bytes of error", d.Attempts, len(d.LastError))
	}
}
