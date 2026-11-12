package engine_test

// Shared test setup. Everything is real (SQLite, the queue, the stores, the
// engine) except where a test deliberately substitutes a failing collaborator
// to see what the engine does when part of the world breaks.
//
// Tests drive the handlers directly with jobs leased from the real queue, so
// each stage of the pipeline can be observed on its own.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djenkins-1/automation-hub/internal/engine"
	"github.com/djenkins-1/automation-hub/internal/queue"
	"github.com/djenkins-1/automation-hub/internal/rules"
	"github.com/djenkins-1/automation-hub/internal/sinks"
	"github.com/djenkins-1/automation-hub/internal/sinks/sinktest"
	"github.com/djenkins-1/automation-hub/internal/store"
	"github.com/djenkins-1/automation-hub/internal/store/sqlite"
	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

var ctx = context.Background()

type harness struct {
	t          *testing.T
	db         *store.DB
	q          *queue.SQLite
	events     *sqlite.Events
	deliveries *sqlite.Deliveries
	rec        *sinktest.Recorder
	set        *rules.Set
	opts       engine.Options
	ingest     *engine.Ingest
	router     *engine.Router
	dispatcher *engine.Dispatcher
}

// diskRule is the default rule: alert on a full disk.
var diskRule = rules.Rule{
	Name:  "disk",
	Match: rules.Match{Source: "monitor", Type: "disk.*", When: "data.percent > 90"},
	Actions: []rules.Action{
		{Sink: "rec", Title: "Disk {{.Event.Subject}}", Body: "{{.Event.Data.percent}}% used"},
	},
}

func newHarness(t *testing.T, rs ...rules.Rule) *harness {
	t.Helper()
	if len(rs) == 0 {
		rs = []rules.Rule{diskRule}
	}
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	set, err := rules.NewSet(rs)
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{
		t: t, db: db, set: set,
		q:          queue.NewSQLite(db, queue.Options{DefaultMaxAttempts: 3}),
		events:     sqlite.NewEvents(db),
		deliveries: sqlite.NewDeliveries(db),
		rec:        &sinktest.Recorder{},
		opts:       engine.Options{Log: slog.New(slog.NewTextHandler(io.Discard, nil))},
	}
	h.ingest = engine.NewIngest(db, h.events, h.q, h.opts)
	h.router = engine.NewRouter(db, h.events, h.deliveries, h.q, set, h.opts)
	h.dispatcher = engine.NewDispatcher(h.deliveries, h.events, h.sinkSet(map[string]sdk.Sink{"rec": h.rec}), h.opts)
	return h
}

func (h *harness) sinkSet(m map[string]sdk.Sink) engine.SinkSet { return sinks.NewRegistry(m) }

func diskEvent(percent float64) sdk.Event {
	return sdk.Event{Source: "monitor", Type: "disk.full", Subject: "/var", Data: map[string]any{"percent": percent}}
}

func (h *harness) accept(e sdk.Event) engine.IngestResult {
	h.t.Helper()
	res, err := h.ingest.Accept(ctx, e)
	if err != nil {
		h.t.Fatalf("accept: %v", err)
	}
	return res
}

func (h *harness) lease(kind string) []queue.Job {
	h.t.Helper()
	jobs, err := h.q.Lease(ctx, "test", []string{kind}, 1000, time.Minute)
	if err != nil {
		h.t.Fatal(err)
	}
	return jobs
}

// count reports rows without consuming anything.
func (h *harness) count(query string, args ...any) int {
	h.t.Helper()
	var n int
	if err := h.db.Read.QueryRowContext(ctx, query, args...).Scan(&n); err != nil {
		h.t.Fatalf("%s: %v", query, err)
	}
	return n
}

func (h *harness) jobsOfKind(kind string) int {
	h.t.Helper()
	return h.count(`SELECT COUNT(*) FROM jobs WHERE kind = ?`, kind)
}

func (h *harness) eventCount() int { h.t.Helper(); return h.count(`SELECT COUNT(*) FROM events`) }

func (h *harness) deliveriesFor(eventID string) []engine.Delivery {
	h.t.Helper()
	ds, err := h.deliveries.ListDeliveries(ctx, engine.DeliveryQuery{EventID: eventID})
	if err != nil {
		h.t.Fatal(err)
	}
	return ds
}

func (h *harness) onlyDelivery(eventID string) engine.Delivery {
	h.t.Helper()
	ds := h.deliveriesFor(eventID)
	if len(ds) != 1 {
		h.t.Fatalf("expected exactly one delivery for the event, got %d", len(ds))
	}
	return ds[0]
}

func (h *harness) attempts(deliveryID string) []engine.Attempt {
	h.t.Helper()
	as, err := h.deliveries.ListAttempts(ctx, deliveryID)
	if err != nil {
		h.t.Fatal(err)
	}
	return as
}

// route runs every pending route job through the router.
func (h *harness) route() {
	h.t.Helper()
	for _, j := range h.lease(engine.KindRoute) {
		if err := h.router.Handle(ctx, j); err != nil {
			h.t.Fatalf("route: %v", err)
		}
	}
}

// routed ingests and routes an event, returning its ID and the single deliver job it produced.
func (h *harness) routed(e sdk.Event) (string, queue.Job) {
	h.t.Helper()
	res := h.accept(e)
	h.route()
	jobs := h.lease(engine.KindDeliver)
	if len(jobs) != 1 {
		h.t.Fatalf("expected exactly one deliver job, got %d", len(jobs))
	}
	return res.EventID, jobs[0]
}

// tryAttempt returns j as it looks on the nth try of max.
func tryAttempt(j queue.Job, n, max int) queue.Job {
	j.Attempt, j.MaxAttempts = n, max
	return j
}

// ---------------------------------------------------------------------------
// Failing collaborators

var errInjected = errors.New("injected failure")

// flakyEnqueuer fails the nth Enqueue call (1-based) and otherwise delegates.
type flakyEnqueuer struct {
	inner  engine.Enqueuer
	failOn int
	calls  atomic.Int32
}

func (f *flakyEnqueuer) Enqueue(c context.Context, j queue.NewJob) (string, bool, error) {
	if int(f.calls.Add(1)) == f.failOn {
		return "", false, errInjected
	}
	return f.inner.Enqueue(c, j)
}

// brokenEvents injects failures into the event store.
type brokenEvents struct {
	engine.EventStore
	failInsert bool
	failGet    error
}

func (b brokenEvents) InsertEvent(c context.Context, e sdk.Event, at time.Time) (string, bool, error) {
	if b.failInsert {
		return "", false, errInjected
	}
	return b.EventStore.InsertEvent(c, e, at)
}

func (b brokenEvents) GetEvent(c context.Context, id string) (sdk.Event, error) {
	if b.failGet != nil {
		return sdk.Event{}, b.failGet
	}
	return b.EventStore.GetEvent(c, id)
}

// brokenDeliveries injects failures into the delivery store.
type brokenDeliveries struct {
	engine.DeliveryStore
	failCreateOn int // 1-based call index; 0 = never
	failRecord   bool
	failGet      error
	creates      atomic.Int32
}

func (b *brokenDeliveries) CreateDelivery(c context.Context, d engine.NewDelivery) (string, bool, error) {
	if int(b.creates.Add(1)) == b.failCreateOn {
		return "", false, errInjected
	}
	return b.DeliveryStore.CreateDelivery(c, d)
}

func (b *brokenDeliveries) RecordAttempt(c context.Context, a engine.Attempt) error {
	if b.failRecord {
		return errInjected
	}
	return b.DeliveryStore.RecordAttempt(c, a)
}

func (b *brokenDeliveries) GetDelivery(c context.Context, id string) (engine.Delivery, error) {
	if b.failGet != nil {
		return engine.Delivery{}, b.failGet
	}
	return b.DeliveryStore.GetDelivery(c, id)
}
