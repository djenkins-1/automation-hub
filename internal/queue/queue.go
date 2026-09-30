package queue

import (
	"context"
	"errors"
	"time"
)

// Job states
const (
	StateReady  = "ready"  // waiting for run_at, then eligible for Lease
	StateLeased = "leased" // held by a worker until lease expiry
	StateDone   = "done"   // handler succeeded
	StateDead   = "dead"   // out of attempts, or failed permanently
)

var (
	// ErrLeaseLost means the lease presented no longer matches: it expired and
	// was reaped or reassigned, or the job already reached a final state.
	ErrLeaseLost = errors.New("queue: lease lost (expired or reassigned)")
	// ErrNotFound means no job has the given ID.
	ErrNotFound = errors.New("queue: job not found")
	// ErrNotDead means the operation requires a dead job.
	ErrNotDead = errors.New("queue: job is not in the dead state")
)

// NewJob is what producers hand to Enqueue.
type NewJob struct {
	Kind        string    // required, e.g. "route" or "deliver"
	Payload     []byte    // small; IDs rather than bodies
	DedupeKey   string    // empty = no dedupe; otherwise unique per Kind
	RunAt       time.Time // zero = now
	MaxAttempts int       // zero = queue default
}

// Job is a leased job. Handlers get this and must pass it back, unchanged, to Ack/Nack/Fail/Extend.
type Job struct {
	ID             string
	Kind           string
	Payload        []byte
	Attempt        int // 1-based: this is the Nth execution attempt
	MaxAttempts    int
	LeaseToken     int64
	LeaseExpiresAt time.Time
	CreatedAt      time.Time
}

// Queue is the core contract. Every implementation must pass queuetest.Run.
type Queue interface {
	// Enqueue inserts a job and returns its ID. If ctx carries a transaction the
	// insert joins it, which is how ingest gets event+job atomicity. If DedupeKey
	// matches an existing job of the same kind, nothing is inserted,
	// created is false, and id is the existing job's ID (this is not an error).
	//
	// Dedupe keys are remembered only while the job row exists, so retention
	// purging bounds the dedupe window.
	Enqueue(ctx context.Context, j NewJob) (id string, created bool, err error)

	// Lease atomically claims up to n ready jobs of the given kinds whose run_at
	// has passed, oldest run_at first. Empty kinds means any kind. Each returned job
	// has its attempt count incremented and a fresh, higher fencing token.
	Lease(ctx context.Context, owner string, kinds []string, n int, d time.Duration) ([]Job, error)

	// Extend pushes the lease expiry to now+d (heartbeat for long handlers)
	// and returns the new expiry. Returns ErrLeaseLost if the lease is gone.
	Extend(ctx context.Context, j Job, d time.Duration) (time.Time, error)

	// Ack marks the job done. Returns ErrLeaseLost if the lease is gone.
	//
	// An Ack that arrives after the lease expired but before the reaper ran
	// still succeeds: nobody else holds the job, and the work was done.
	Ack(ctx context.Context, j Job) error

	// Nack releases the job for retry at retryAt. If j.Attempt has reached
	// j.MaxAttempts the job is dead-lettered instead. Cause is recorded.
	Nack(ctx context.Context, j Job, retryAt time.Time, cause error) error

	// Fail dead-letters the job immediately (a permanent failure).
	Fail(ctx context.Context, j Job, cause error) error

	// ReapExpired returns jobs whose lease has expired to ready, or to dead if
	// their attempts are exhausted. Returns how many jobs were touched. Safe
	// to call from any number of nodes concurrently.
	ReapExpired(ctx context.Context) (int, error)
}

// Admin holds the operations that only operators and the API need. It is a
// separate interface so workers and handlers cannot touch dead-letters.
type Admin interface {
	// Get returns one job, or ErrNotFound.
	Get(ctx context.Context, id string) (JobInfo, error)

	// ListDead returns dead jobs ordered by ID, starting after afterID
	// (empty for the first page). limit <= 0 means 50; the maximum is 500.
	ListDead(ctx context.Context, limit int, afterID string) ([]JobInfo, error)

	// Requeue moves a dead job back to ready with its attempts reset to zero.
	// Returns ErrNotFound or ErrNotDead as appropriate.
	Requeue(ctx context.Context, id string) error

	// Purge deletes jobs in the given final state (StateDone or StateDead)
	// that finished before olderThan, and returns how many were deleted.
	// The states are purged separately so that routine clean-up of finished
	// work can never silently discard dead letters.
	Purge(ctx context.Context, state string, olderThan time.Time) (int, error)
}

// JobInfo is the read-only view of a job for the API and operators.
// It does not expose lease tokens.
type JobInfo struct {
	ID, Kind, State, LastError   string
	Payload                      []byte
	Attempts, MaxAttempts        int
	RunAt, CreatedAt, FinishedAt time.Time // FinishedAt is zero unless done or dead
}
