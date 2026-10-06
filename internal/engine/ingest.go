package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/djenkins-1/automation-hub/internal/queue"
	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

// Ingest accepts events from any producer (the HTTP API now; pollers and the
// scheduler later).
//
// This is the transactional outbox: the event row and the job that will route
// it are written in one transaction. Either both exist or neither does, so an
// accepted event is never lost between "saved" and "queued", and a crash here
// cannot leave an event that nothing will ever process.
type Ingest struct {
	tx     Transactor
	events EventStore
	queue  Enqueuer
	opts   Options
	log    *slog.Logger
}

// NewIngest builds an Ingest.
func NewIngest(tx Transactor, events EventStore, q Enqueuer, opts Options) *Ingest {
	opts = opts.withDefaults()
	return &Ingest{tx: tx, events: events, queue: q, opts: opts, log: opts.Log.With("component", "ingest")}
}

// IngestResult describes what happened to an accepted event.
type IngestResult struct {
	EventID string
	// Duplicate is true when the event's idempotency key had been seen
	// before. Nothing new was stored or queued; EventID is the original's.
	Duplicate bool
}

// Accept validates ev, stores it and queues it for routing. Validation
// failures wrap sdk.ErrInvalidEvent. Returning without error means the event
// is durably stored and will be routed at least once.
func (s *Ingest) Accept(ctx context.Context, ev sdk.Event) (IngestResult, error) {
	if err := ev.Validate(); err != nil {
		return IngestResult{}, err
	}

	now := s.opts.Clock.Now()
	ev.ID = s.opts.NewID() // always ours, whatever the producer sent
	if ev.Time.IsZero() {
		ev.Time = now
	}
	ev.Time = ev.Time.UTC()

	var res IngestResult
	err := s.tx.InTx(ctx, func(ctx context.Context) error {
		id, created, err := s.events.InsertEvent(ctx, ev, now)
		if err != nil {
			return fmt.Errorf("store event: %w", err)
		}
		res = IngestResult{EventID: id, Duplicate: !created}
		if !created {
			return nil
		}

		payload, err := json.Marshal(routePayload{EventID: id})
		if err != nil {
			return err
		}
		// The dedupe key is the event ID, so even a retried transaction
		// cannot enqueue the routing job twice.
		if _, _, err := s.queue.Enqueue(ctx, queue.NewJob{
			Kind:      KindRoute,
			Payload:   payload,
			DedupeKey: id,
		}); err != nil {
			return fmt.Errorf("enqueue route job: %w", err)
		}
		return nil
	})
	if err != nil {
		return IngestResult{}, err
	}

	if res.Duplicate {
		s.log.Info("duplicate event ignored", "event", res.EventID, "source", ev.Source, "type", ev.Type)
	} else {
		s.log.Debug("event accepted", "event", res.EventID, "source", ev.Source, "type", ev.Type)
	}
	return res, nil
}
