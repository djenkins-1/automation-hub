package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/djenkins-1/automation-hub/internal/queue"
	"github.com/djenkins-1/automation-hub/internal/rules"
	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

// Router turns an event into deliveries. It is the handler for "route" jobs.
//
// For every action of every matching rule it creates a delivery row and a
// "deliver" job, all in one transaction. Both carry the same idempotency key,
// <event id>:<rule>:<action index>, so if the route job is retried (a crash
// halfway through, a lost ack) the second run finds what the first created and
// adds only what is missing.
type Router struct {
	tx         Transactor
	events     EventStore
	deliveries DeliveryStore
	queue      Enqueuer
	rules      *rules.Set
	opts       Options
	log        *slog.Logger
}

// NewRouter builds a Router.
func NewRouter(tx Transactor, events EventStore, deliveries DeliveryStore, q Enqueuer, rs *rules.Set, opts Options) *Router {
	opts = opts.withDefaults()
	return &Router{
		tx: tx, events: events, deliveries: deliveries, queue: q, rules: rs,
		opts: opts, log: opts.Log.With("component", "router"),
	}
}

// Handle implements queue.Handler for KindRoute jobs.
func (r *Router) Handle(ctx context.Context, j queue.Job) error {
	var p routePayload
	if err := json.Unmarshal(j.Payload, &p); err != nil || p.EventID == "" {
		return sdk.Permanent(fmt.Errorf("route: malformed payload %q", j.Payload))
	}

	ev, err := r.events.GetEvent(ctx, p.EventID)
	switch {
	case errors.Is(err, ErrNotFound):
		return sdk.Permanent(fmt.Errorf("route: event %s not found", p.EventID))
	case err != nil:
		return fmt.Errorf("route: load event: %w", err)
	}

	matched, problems := r.rules.Match(ev)
	for _, err := range problems {
		// Retrying would not help; surface it and carry on with the rest.
		r.log.Warn("rule problem", "event", ev.ID, "err", err)
	}
	if len(matched) == 0 {
		r.log.Debug("no rule matched", "event", ev.ID, "source", ev.Source, "type", ev.Type)
		return nil
	}

	now := r.opts.Clock.Now()
	err = r.tx.InTx(ctx, func(ctx context.Context) error {
		for _, m := range matched {
			key := deliveryKey(ev.ID, m.Rule, m.ActionIndex)
			id, _, err := r.deliveries.CreateDelivery(ctx, NewDelivery{
				ID:             r.opts.NewID(),
				EventID:        ev.ID,
				Rule:           m.Rule,
				ActionIndex:    m.ActionIndex,
				Sink:           m.Sink,
				Title:          m.Title,
				Body:           m.Body,
				IdempotencyKey: key,
				CreatedAt:      now,
			})
			if err != nil {
				return fmt.Errorf("create delivery: %w", err)
			}
			payload, err := json.Marshal(deliverPayload{DeliveryID: id})
			if err != nil {
				return err
			}
			if _, _, err := r.queue.Enqueue(ctx, queue.NewJob{
				Kind:      KindDeliver,
				Payload:   payload,
				DedupeKey: key,
			}); err != nil {
				return fmt.Errorf("enqueue deliver job: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("route: %w", err)
	}

	r.log.Info("event routed", "event", ev.ID, "source", ev.Source, "type", ev.Type, "deliveries", len(matched))
	return nil
}

// deliveryKey identifies "this action of this rule for this event". It is the
// delivery's idempotency key, the deliver job's dedupe key, and is handed to
// sinks so a destination that supports idempotency keys can drop duplicates.
func deliveryKey(eventID, rule string, actionIndex int) string {
	return fmt.Sprintf("%s:%s:%d", eventID, rule, actionIndex)
}
