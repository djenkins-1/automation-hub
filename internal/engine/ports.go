// Package engine is the notification pipeline:
//
//	ingest  -> [event + "route" job, one transaction]
//	router  -> [delivery rows + "deliver" jobs, one transaction]
//	dispatcher -> sink
//
// Delivery is at-least-once. Each stage is idempotent, so a crash or retry at
// any point cannot lose a notification, and duplicates are suppressed wherever
// the pipeline can do so (see the dedupe keys in router.go and the
// already-delivered check in dispatcher.go).
//
// The engine knows nothing about SQL or HTTP. It declares the small interfaces
// it needs in this file; internal/store/sqlite implements the stores, and
// internal/app wires everything together.
package engine

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/djenkins-1/automation-hub/internal/queue"
	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

// Job kinds handled by the engine.
const (
	KindRoute   = "route"   // payload: routePayload
	KindDeliver = "deliver" // payload: deliverPayload
)

// ErrNotFound is returned by stores when a row does not exist.
var ErrNotFound = errors.New("engine: not found")

// Transactor runs fn in a transaction. Stores and the queue called with the
// context passed to fn join that transaction.
type Transactor interface {
	InTx(ctx context.Context, fn func(ctx context.Context) error) error
}

// Enqueuer is the part of queue.Queue the engine produces work with.
type Enqueuer interface {
	Enqueue(ctx context.Context, j queue.NewJob) (id string, created bool, err error)
}

// Clock lets tests control time. queue.FakeClock satisfies it.
type Clock interface {
	Now() time.Time
}

// EventStore persists events.
type EventStore interface {
	// InsertEvent stores ev. If ev.IdempotencyKey is set and an event from
	// the same source with that key already exists, nothing is stored and the
	// existing event's ID is returned with created == false.
	InsertEvent(ctx context.Context, ev sdk.Event, receivedAt time.Time) (id string, created bool, err error)
	// GetEvent returns ErrNotFound if there is no such event.
	GetEvent(ctx context.Context, id string) (sdk.Event, error)
}

// DeliveryStatus is the state of a delivery.
type DeliveryStatus string

// Delivery states.
const (
	DeliveryPending   DeliveryStatus = "pending"   // not yet delivered; may be retrying
	DeliveryDelivered DeliveryStatus = "delivered" // the sink accepted it
	DeliveryFailed    DeliveryStatus = "failed"    // gave up: permanent error or attempts exhausted
)

// NewDelivery describes a delivery to create.
type NewDelivery struct {
	ID             string
	EventID        string
	Rule           string
	ActionIndex    int
	Sink           string
	Title          string
	Body           string
	IdempotencyKey string // unique; see deliveryKey in router.go
	CreatedAt      time.Time
}

// Delivery is one message destined for one sink, and what has happened to it.
type Delivery struct {
	ID             string
	EventID        string
	Rule           string
	ActionIndex    int
	Sink           string
	Title          string
	Body           string
	IdempotencyKey string
	Status         DeliveryStatus
	Attempts       int
	LastError      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	DeliveredAt    time.Time // zero unless delivered
}

// AttemptOutcome is how one attempt ended.
type AttemptOutcome string

// Attempt outcomes.
const (
	OutcomeDelivered AttemptOutcome = "delivered"
	OutcomeRetry     AttemptOutcome = "retry"  // failed; another attempt is scheduled
	OutcomeFailed    AttemptOutcome = "failed" // failed; no more attempts
)

// Attempt is the record of one try at a delivery.
type Attempt struct {
	DeliveryID string
	Number     int // the queue's attempt number (1-based)
	StartedAt  time.Time
	FinishedAt time.Time
	Outcome    AttemptOutcome
	Error      string
	Detail     string
}

// DeliveryQuery filters ListDeliveries. Zero fields mean "no filter".
type DeliveryQuery struct {
	EventID string
	Status  DeliveryStatus
	Limit   int    // default 50, maximum 500
	AfterID string // pagination cursor: results have ID > AfterID, ordered by ID
}

// DeliveryStore persists deliveries and their attempts.
type DeliveryStore interface {
	// CreateDelivery inserts d. If a delivery with the same IdempotencyKey
	// exists, nothing is inserted and the existing delivery's ID is returned
	// with created == false.
	CreateDelivery(ctx context.Context, d NewDelivery) (id string, created bool, err error)
	// GetDelivery returns ErrNotFound if there is no such delivery.
	GetDelivery(ctx context.Context, id string) (Delivery, error)
	ListDeliveries(ctx context.Context, q DeliveryQuery) ([]Delivery, error)
	// RecordAttempt appends a to the delivery's attempt log and, in the same
	// transaction, updates the delivery: attempts+1, status derived from the
	// outcome, last_error, delivered_at. Returns ErrNotFound for an unknown delivery.
	RecordAttempt(ctx context.Context, a Attempt) error
	ListAttempts(ctx context.Context, deliveryID string) ([]Attempt, error)
}

// SinkSet looks up configured sinks by name.
type SinkSet interface {
	Get(name string) (sdk.Sink, bool)
}

// Options are the knobs shared by the engine's components. The zero value
// uses the system clock, ULIDs, the default logger and a 30s sink timeout.
type Options struct {
	Clock       Clock
	NewID       func() string
	Log         *slog.Logger
	SinkTimeout time.Duration // per Deliver call; dispatcher only
}

func (o Options) withDefaults() Options {
	if o.Clock == nil {
		o.Clock = systemClock{}
	}
	if o.NewID == nil {
		o.NewID = func() string { return ulid.Make().String() }
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.SinkTimeout <= 0 {
		o.SinkTimeout = 30 * time.Second
	}
	return o
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// Job payloads. They carry IDs, never bodies: the rows they point at are the
// source of truth, and small payloads keep the jobs table light.
type routePayload struct {
	EventID string `json:"event_id"`
}

type deliverPayload struct {
	DeliveryID string `json:"delivery_id"`
}
