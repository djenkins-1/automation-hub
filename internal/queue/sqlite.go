package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/djenkins-1/automation-hub/internal/store"
)

const (
	defaultMaxAttempts = 6
	defaultListLimit   = 50
	maxListLimit       = 500
	maxErrorLen        = 2000 // last_error is truncated so one bad handler cannot bloat rows
)

// Options configures a SQLite queue. The zero value is usable.
type Options struct {
	Clock              Clock         // default SystemClock
	DefaultMaxAttempts int           // default 6
	NewID              func() string // default: ULID. Tests may inject a deterministic source.
}

// SQLite implements Queue and Admin on top of store.DB.
//
// SQLite serialises writers, so each state transition below is a single
// statement that is atomic without any explicit locking. The Postgres
// implementation will use the same statements, adding FOR UPDATE SKIP LOCKED
// to the sub-select in Lease.
type SQLite struct {
	db         *store.DB
	clock      Clock
	newID      func() string
	defaultMax int
}

var (
	_ Queue = (*SQLite)(nil)
	_ Admin = (*SQLite)(nil)
)

// NewSQLite returns a queue backed by db, whose schema must already have been
// migrated (store.Open does this).
func NewSQLite(db *store.DB, opts Options) *SQLite {
	q := &SQLite{db: db, clock: opts.Clock, newID: opts.NewID, defaultMax: opts.DefaultMaxAttempts}
	if q.clock == nil {
		q.clock = SystemClock{}
	}
	if q.newID == nil {
		q.newID = func() string { return ulid.Make().String() }
	}
	if q.defaultMax <= 0 {
		q.defaultMax = defaultMaxAttempts
	}
	return q
}

func (q *SQLite) now() int64 { return q.clock.Now().UnixMilli() }

// Enqueue implements Queue.
func (q *SQLite) Enqueue(ctx context.Context, j NewJob) (string, bool, error) {
	if j.Kind == "" {
		return "", false, errors.New("queue: enqueue: kind is required")
	}
	if j.MaxAttempts < 0 {
		return "", false, errors.New("queue: enqueue: MaxAttempts must not be negative")
	}
	maxAttempts := j.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = q.defaultMax
	}
	now := q.now()
	runAt := now
	if !j.RunAt.IsZero() {
		runAt = j.RunAt.UnixMilli()
	}
	payload := j.Payload
	if payload == nil {
		payload = []byte{} // the column is NOT NULL
	}
	var dedupe any // nil binds as SQL NULL
	if j.DedupeKey != "" {
		dedupe = j.DedupeKey
	}

	exec := store.Executor(ctx, q.db.Write)
	id := q.newID()
	res, err := exec.ExecContext(ctx, `
		INSERT INTO jobs (id, kind, payload, dedupe_key, state, attempts, max_attempts,
		                  run_at, lease_token, created_at, updated_at)
		VALUES (?, ?, ?, ?, 'ready', 0, ?, ?, 0, ?, ?)
		ON CONFLICT (kind, dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING`,
		id, j.Kind, payload, dedupe, maxAttempts, runAt, now, now)
	if err != nil {
		return "", false, fmt.Errorf("queue: enqueue: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return "", false, fmt.Errorf("queue: enqueue: %w", err)
	}
	if n == 1 {
		return id, true, nil
	}

	// Conflict on the dedupe key: report the job that already exists.
	var existing string
	err = exec.QueryRowContext(ctx,
		`SELECT id FROM jobs WHERE kind = ? AND dedupe_key = ?`, j.Kind, j.DedupeKey).Scan(&existing)
	if err != nil {
		return "", false, fmt.Errorf("queue: enqueue: look up deduplicated job: %w", err)
	}
	return existing, false, nil
}

// Lease implements Queue.
func (q *SQLite) Lease(ctx context.Context, owner string, kinds []string, n int, d time.Duration) ([]Job, error) {
	if n <= 0 {
		return nil, nil
	}
	if d <= 0 {
		return nil, errors.New("queue: lease: duration must be positive")
	}
	now := q.now()

	var b strings.Builder
	args := []any{owner, now + d.Milliseconds(), now, now}
	b.WriteString(`
		UPDATE jobs
		SET state = 'leased',
		    attempts = attempts + 1,
		    lease_owner = ?,
		    lease_token = lease_token + 1,
		    lease_expires_at = ?,
		    updated_at = ?
		WHERE id IN (
		    SELECT id FROM jobs
		    WHERE state = 'ready' AND run_at <= ?`)
	if len(kinds) > 0 {
		b.WriteString(` AND kind IN (?` + strings.Repeat(`, ?`, len(kinds)-1) + `)`)
		for _, k := range kinds {
			args = append(args, k)
		}
	}
	b.WriteString(`
		    ORDER BY run_at, id
		    LIMIT ?
		)
		RETURNING id, kind, payload, attempts, max_attempts, lease_token,
		          lease_expires_at, created_at, run_at`)
	args = append(args, n)

	rows, err := store.Executor(ctx, q.db.Write).QueryContext(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("queue: lease: %w", err)
	}
	defer rows.Close()

	type leased struct {
		job   Job
		runAt int64
	}
	var got []leased
	for rows.Next() {
		var (
			l                    leased
			expiresMs, createdMs int64
		)
		if err := rows.Scan(&l.job.ID, &l.job.Kind, &l.job.Payload, &l.job.Attempt, &l.job.MaxAttempts,
			&l.job.LeaseToken, &expiresMs, &createdMs, &l.runAt); err != nil {
			return nil, fmt.Errorf("queue: lease: scan: %w", err)
		}
		l.job.LeaseExpiresAt = time.UnixMilli(expiresMs)
		l.job.CreatedAt = time.UnixMilli(createdMs)
		got = append(got, l)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("queue: lease: %w", err)
	}

	// RETURNING makes no ordering promise; hand jobs back oldest-first.
	sort.Slice(got, func(i, j int) bool {
		if got[i].runAt != got[j].runAt {
			return got[i].runAt < got[j].runAt
		}
		return got[i].job.ID < got[j].job.ID
	})
	jobs := make([]Job, len(got))
	for i := range got {
		jobs[i] = got[i].job
	}
	return jobs, nil
}

// Extend implements Queue.
func (q *SQLite) Extend(ctx context.Context, j Job, d time.Duration) (time.Time, error) {
	if d <= 0 {
		return time.Time{}, errors.New("queue: extend: duration must be positive")
	}
	now := q.now()
	expires := now + d.Milliseconds()
	res, err := store.Executor(ctx, q.db.Write).ExecContext(ctx, `
		UPDATE jobs SET lease_expires_at = ?, updated_at = ?
		WHERE id = ? AND lease_token = ? AND state = 'leased'`,
		expires, now, j.ID, j.LeaseToken)
	if err := requireOne(res, err, "extend"); err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(expires), nil
}

// Ack implements Queue.
func (q *SQLite) Ack(ctx context.Context, j Job) error {
	now := q.now()
	res, err := store.Executor(ctx, q.db.Write).ExecContext(ctx, `
		UPDATE jobs
		SET state = 'done', finished_at = ?, updated_at = ?,
		    lease_owner = NULL, lease_expires_at = NULL
		WHERE id = ? AND lease_token = ? AND state = 'leased'`,
		now, now, j.ID, j.LeaseToken)
	return requireOne(res, err, "ack")
}

// Nack implements Queue. Whether the job retries or dies is decided here, in
// SQL, from the attempts recorded at lease time, so callers cannot get it wrong.
func (q *SQLite) Nack(ctx context.Context, j Job, retryAt time.Time, cause error) error {
	now := q.now()
	res, err := store.Executor(ctx, q.db.Write).ExecContext(ctx, `
		UPDATE jobs
		SET state       = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'ready' END,
		    run_at      = ?,
		    last_error  = ?,
		    finished_at = CASE WHEN attempts >= max_attempts THEN ? END,
		    lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
		WHERE id = ? AND lease_token = ? AND state = 'leased'`,
		retryAt.UnixMilli(), errText(cause), now, now, j.ID, j.LeaseToken)
	return requireOne(res, err, "nack")
}

// Fail implements Queue.
func (q *SQLite) Fail(ctx context.Context, j Job, cause error) error {
	now := q.now()
	res, err := store.Executor(ctx, q.db.Write).ExecContext(ctx, `
		UPDATE jobs
		SET state = 'dead', last_error = ?, finished_at = ?, updated_at = ?,
		    lease_owner = NULL, lease_expires_at = NULL
		WHERE id = ? AND lease_token = ? AND state = 'leased'`,
		errText(cause), now, now, j.ID, j.LeaseToken)
	return requireOne(res, err, "fail")
}

// ReapExpired implements Queue.
func (q *SQLite) ReapExpired(ctx context.Context) (int, error) {
	now := q.now()
	res, err := store.Executor(ctx, q.db.Write).ExecContext(ctx, `
		UPDATE jobs
		SET state       = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'ready' END,
		    run_at      = ?,
		    last_error  = 'lease expired',
		    finished_at = CASE WHEN attempts >= max_attempts THEN ? END,
		    lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
		WHERE state = 'leased' AND lease_expires_at <= ?`,
		now, now, now, now)
	if err != nil {
		return 0, fmt.Errorf("queue: reap: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("queue: reap: %w", err)
	}
	return int(n), nil
}

// ---------------------------------------------------------------------------
// Admin

const infoColumns = `id, kind, state, payload, last_error, attempts, max_attempts, run_at, created_at, finished_at`

type scanner interface{ Scan(dest ...any) error }

func scanInfo(s scanner) (JobInfo, error) {
	var (
		ji               JobInfo
		lastErr          sql.NullString
		runMs, createdMs int64
		finishedMs       sql.NullInt64
	)
	if err := s.Scan(&ji.ID, &ji.Kind, &ji.State, &ji.Payload, &lastErr, &ji.Attempts,
		&ji.MaxAttempts, &runMs, &createdMs, &finishedMs); err != nil {
		return JobInfo{}, err
	}
	ji.LastError = lastErr.String
	ji.RunAt = time.UnixMilli(runMs)
	ji.CreatedAt = time.UnixMilli(createdMs)
	if finishedMs.Valid {
		ji.FinishedAt = time.UnixMilli(finishedMs.Int64)
	}
	return ji, nil
}

// Get implements Admin.
func (q *SQLite) Get(ctx context.Context, id string) (JobInfo, error) {
	row := store.Executor(ctx, q.db.Read).QueryRowContext(ctx,
		`SELECT `+infoColumns+` FROM jobs WHERE id = ?`, id)
	ji, err := scanInfo(row)
	if errors.Is(err, sql.ErrNoRows) {
		return JobInfo{}, ErrNotFound
	}
	if err != nil {
		return JobInfo{}, fmt.Errorf("queue: get: %w", err)
	}
	return ji, nil
}

// ListDead implements Admin.
func (q *SQLite) ListDead(ctx context.Context, limit int, afterID string) ([]JobInfo, error) {
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}
	rows, err := store.Executor(ctx, q.db.Read).QueryContext(ctx,
		`SELECT `+infoColumns+` FROM jobs WHERE state = 'dead' AND id > ? ORDER BY id LIMIT ?`,
		afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("queue: list dead: %w", err)
	}
	defer rows.Close()

	var out []JobInfo
	for rows.Next() {
		ji, err := scanInfo(rows)
		if err != nil {
			return nil, fmt.Errorf("queue: list dead: scan: %w", err)
		}
		out = append(out, ji)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("queue: list dead: %w", err)
	}
	return out, nil
}

// Requeue implements Admin. last_error is kept so the history of why the job
// died survives until it next succeeds or fails.
func (q *SQLite) Requeue(ctx context.Context, id string) error {
	now := q.now()
	exec := store.Executor(ctx, q.db.Write)
	res, err := exec.ExecContext(ctx, `
		UPDATE jobs
		SET state = 'ready', attempts = 0, run_at = ?, finished_at = NULL,
		    lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
		WHERE id = ? AND state = 'dead'`,
		now, now, id)
	if err != nil {
		return fmt.Errorf("queue: requeue: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("queue: requeue: %w", err)
	}
	if n == 1 {
		return nil
	}
	var state string
	err = exec.QueryRowContext(ctx, `SELECT state FROM jobs WHERE id = ?`, id).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("queue: requeue: %w", err)
	}
	return ErrNotDead
}

// Purge implements Admin. Two literal queries (rather than one with a bound
// state) let SQLite use the partial indexes on finished_at.
func (q *SQLite) Purge(ctx context.Context, state string, olderThan time.Time) (int, error) {
	var query string
	switch state {
	case StateDone:
		query = `DELETE FROM jobs WHERE state = 'done' AND finished_at < ?`
	case StateDead:
		query = `DELETE FROM jobs WHERE state = 'dead' AND finished_at < ?`
	default:
		return 0, fmt.Errorf("queue: purge: state must be %q or %q, got %q", StateDone, StateDead, state)
	}
	res, err := store.Executor(ctx, q.db.Write).ExecContext(ctx, query, olderThan.UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("queue: purge: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("queue: purge: %w", err)
	}
	return int(n), nil
}

// ---------------------------------------------------------------------------
// helpers

// requireOne converts "0 rows updated" into ErrLeaseLost. Used by every
// transition guarded by (id, lease_token, state = 'leased').
func requireOne(res sql.Result, err error, op string) error {
	if err != nil {
		return fmt.Errorf("queue: %s: %w", op, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("queue: %s: %w", op, err)
	}
	if n == 0 {
		return ErrLeaseLost
	}
	return nil
}

// errText renders an error for last_error: NULL for nil, truncated otherwise.
func errText(err error) any {
	if err == nil {
		return nil
	}
	s := err.Error()
	if len(s) > maxErrorLen {
		s = strings.ToValidUTF8(s[:maxErrorLen], "") + "…"
	}
	return s
}
