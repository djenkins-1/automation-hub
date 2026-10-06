package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/djenkins-1/automation-hub/internal/queue"
	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

// Dispatcher sends one delivery to its sink. It is the handler for "deliver"
// jobs.
//
// It does not decide what happens to a failed job; it reports the outcome by
// returning the sink's error unchanged, and the queue worker turns
// sdk.Permanent into an immediate dead letter, sdk.RetryAfter into a timed
// retry, and anything else into a backed-off retry. What the dispatcher adds is
// the audit trail: every attempt is recorded, and the delivery's status tracks
// reality.
type Dispatcher struct {
	deliveries DeliveryStore
	events     EventStore
	sinks      SinkSet
	opts       Options
	log        *slog.Logger
}

// NewDispatcher builds a Dispatcher.
func NewDispatcher(deliveries DeliveryStore, events EventStore, sinks SinkSet, opts Options) *Dispatcher {
	opts = opts.withDefaults()
	return &Dispatcher{
		deliveries: deliveries, events: events, sinks: sinks,
		opts: opts, log: opts.Log.With("component", "dispatcher"),
	}
}

// Handle implements queue.Handler for KindDeliver jobs.
func (d *Dispatcher) Handle(ctx context.Context, j queue.Job) error {
	var p deliverPayload
	if err := json.Unmarshal(j.Payload, &p); err != nil || p.DeliveryID == "" {
		return sdk.Permanent(fmt.Errorf("deliver: malformed payload %q", j.Payload))
	}
	log := d.log.With("delivery", p.DeliveryID, "attempt", j.Attempt)

	del, err := d.deliveries.GetDelivery(ctx, p.DeliveryID)
	switch {
	case errors.Is(err, ErrNotFound):
		return sdk.Permanent(fmt.Errorf("deliver: delivery %s not found", p.DeliveryID))
	case err != nil:
		return fmt.Errorf("deliver: load delivery: %w", err)
	}

	// A crash between "sink accepted it" and "job acked" replays the job.
	// The delivery row remembers the success, so the replay does not send again.
	if del.Status == DeliveryDelivered {
		log.Info("already delivered; nothing to do")
		return nil
	}

	sink, ok := d.sinks.Get(del.Sink)
	if !ok {
		err := sdk.Permanent(fmt.Errorf("unknown sink %q", del.Sink))
		d.record(ctx, del, j, d.opts.Clock.Now(), sdk.Result{}, err, OutcomeFailed, log)
		return err
	}

	ev, err := d.events.GetEvent(ctx, del.EventID)
	switch {
	case errors.Is(err, ErrNotFound):
		ev = sdk.Event{ID: del.EventID} // the message is already rendered; the event is only extra context
	case err != nil:
		return fmt.Errorf("deliver: load event: %w", err)
	}

	started := d.opts.Clock.Now()
	res, serr := d.send(ctx, sink, sdk.Notification{
		ID:             del.ID,
		IdempotencyKey: del.IdempotencyKey,
		Title:          del.Title,
		Body:           del.Body,
		Event:          ev,
		Attempt:        j.Attempt,
	})

	outcome := classify(serr, j)
	recErr := d.record(ctx, del, j, started, res, serr, outcome, log)

	if serr == nil {
		if recErr != nil {
			// The message went out. Failing the job would send it again.
			log.Error("delivered, but could not record the attempt", "err", recErr)
		} else {
			log.Info("delivered", "sink", del.Sink, "detail", res.Detail)
		}
		return nil
	}
	log.Warn("delivery attempt failed", "sink", del.Sink, "outcome", outcome, "err", serr)
	return serr
}

// classify mirrors what the queue will do with the job, so the delivery's
// status matches the job's fate: permanent errors and exhausted attempts are
// final, everything else will be retried.
func classify(err error, j queue.Job) AttemptOutcome {
	switch {
	case err == nil:
		return OutcomeDelivered
	case sdk.IsPermanent(err), j.Attempt >= j.MaxAttempts:
		return OutcomeFailed
	default:
		return OutcomeRetry
	}
}

// send calls the sink under a timeout and converts a panic into an error, so a
// misbehaving sink still gets its attempt recorded.
func (d *Dispatcher) send(ctx context.Context, sink sdk.Sink, n sdk.Notification) (res sdk.Result, err error) {
	ctx, cancel := context.WithTimeout(ctx, d.opts.SinkTimeout)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("sink panicked: %v", r)
		}
	}()
	return sink.Deliver(ctx, n)
}

// record appends the attempt to the audit log. It runs on a context detached
// from cancellation so that an attempt interrupted by shutdown is still written.
func (d *Dispatcher) record(ctx context.Context, del Delivery, j queue.Job, started time.Time,
	res sdk.Result, serr error, outcome AttemptOutcome, log *slog.Logger) error {

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	a := Attempt{
		DeliveryID: del.ID,
		Number:     j.Attempt,
		StartedAt:  started,
		FinishedAt: d.opts.Clock.Now(),
		Outcome:    outcome,
		Detail:     res.Detail,
	}
	if serr != nil {
		a.Error = serr.Error()
	}
	err := d.deliveries.RecordAttempt(ctx, a)
	if err != nil && serr != nil {
		log.Error("could not record failed attempt", "err", err)
	}
	return err
}
