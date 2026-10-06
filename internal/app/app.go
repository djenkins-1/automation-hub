// Package app is the composition root: the only package that knows every
// concrete type. It builds the database, queue, stores, rules, sinks and engine
// from a config, connects them, and runs them.
package app

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/djenkins-1/automation-hub/internal/config"
	"github.com/djenkins-1/automation-hub/internal/engine"
	"github.com/djenkins-1/automation-hub/internal/queue"
	"github.com/djenkins-1/automation-hub/internal/rules"
	"github.com/djenkins-1/automation-hub/internal/sinks"
	"github.com/djenkins-1/automation-hub/internal/store"
	"github.com/djenkins-1/automation-hub/internal/store/sqlite"
)

// App is a fully wired hub.
type App struct {
	cfg *config.Config
	log *slog.Logger

	db         *store.DB
	queue      *queue.SQLite
	events     *sqlite.Events
	deliveries *sqlite.Deliveries
	sinks      *sinks.Registry

	ingest *engine.Ingest
	worker *queue.Worker
}

type options struct {
	log   *slog.Logger
	sinks *sinks.Registry
}

// Option customises New. Options exist mainly so tests can swap pieces.
type Option func(*options)

// WithLogger sets the logger instead of building one from the config.
func WithLogger(l *slog.Logger) Option { return func(o *options) { o.log = l } }

// WithSinks replaces the sinks built from the config (the `sinks:` section is
// still validated, but the registry given here is what rules deliver to).
func WithSinks(r *sinks.Registry) Option { return func(o *options) { o.sinks = r } }

// New wires everything up: opens (and migrates) the database and checks that
// every rule refers to a sink that exists. It starts nothing; call Run.
func New(ctx context.Context, cfg *config.Config, opts ...Option) (*App, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	log := o.log
	if log == nil {
		log = NewLogger(cfg.Log, os.Stderr)
	}

	ruleSet, err := rules.NewSet(cfg.Rules)
	if err != nil {
		return nil, fmt.Errorf("rules: %w", err)
	}

	registry := o.sinks
	if registry == nil {
		if registry, err = sinks.Build(cfg.Sinks); err != nil {
			return nil, fmt.Errorf("sinks: %w", err)
		}
	}
	var missing []string
	for _, name := range ruleSet.SinkNames() {
		if _, ok := registry.Get(name); !ok {
			missing = append(missing, fmt.Sprintf("%q", name))
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("rules refer to undefined sink(s): %s", strings.Join(missing, ", "))
	}

	db, err := store.Open(ctx, cfg.Storage.Path)
	if err != nil {
		return nil, err
	}

	q := queue.NewSQLite(db, queue.Options{DefaultMaxAttempts: cfg.Queue.MaxAttempts})
	events := sqlite.NewEvents(db)
	deliveries := sqlite.NewDeliveries(db)
	engOpts := engine.Options{Log: log}

	ingest := engine.NewIngest(db, events, q, engOpts)
	router := engine.NewRouter(db, events, deliveries, q, ruleSet, engOpts)
	dispatcher := engine.NewDispatcher(deliveries, events, registry, engOpts)

	worker, err := queue.NewWorker(queue.WorkerConfig{
		Queue:       q,
		Admin:       q,
		Concurrency: cfg.Queue.Workers,
		LeaseFor:    cfg.Queue.Lease,
		PollEvery:   cfg.Queue.PollEvery,
		Policy:      queue.RetryPolicy{Base: cfg.Queue.RetryBase, Max: cfg.Queue.RetryMax},
		Log:         log,
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	worker.Handle(engine.KindRoute, router.Handle)
	worker.Handle(engine.KindDeliver, dispatcher.Handle)

	return &App{
		cfg: cfg, log: log, db: db, queue: q, events: events, deliveries: deliveries,
		sinks: registry, ingest: ingest, worker: worker,
	}, nil
}

// Ingest returns the entry point for events. The HTTP API and, later, pollers
// and the scheduler feed the hub through it.
func (a *App) Ingest() *engine.Ingest { return a.ingest }

// Run processes jobs until ctx is cancelled, then drains in-flight work and
// returns. Components added later (the HTTP server, the scheduler) start here.
func (a *App) Run(ctx context.Context) error {
	a.log.Info("hub started", "database", a.cfg.Storage.Path, "rules", len(a.cfg.Rules), "sinks", a.sinks.Names())
	return a.worker.Run(ctx)
}

// Close releases the database. Call it after Run has returned.
func (a *App) Close() error { return a.db.Close() }

// NewLogger builds a slog.Logger from the log config.
func NewLogger(c config.LogConfig, w io.Writer) *slog.Logger {
	var level slog.Level
	if err := level.UnmarshalText([]byte(c.Level)); err != nil {
		level = slog.LevelInfo
	}
	ho := &slog.HandlerOptions{Level: level}
	if strings.EqualFold(c.Format, "json") {
		return slog.New(slog.NewJSONHandler(w, ho))
	}
	return slog.New(slog.NewTextHandler(w, ho))
}
