package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/djenkins-1/automation-hub/internal/engine"
	"github.com/djenkins-1/automation-hub/internal/store"
)

const (
	defaultListLimit = 50
	maxListLimit     = 500
)

// Deliveries implements engine.DeliveryStore.
type Deliveries struct{ db *store.DB }

var _ engine.DeliveryStore = (*Deliveries)(nil)

// NewDeliveries returns a delivery store on db.
func NewDeliveries(db *store.DB) *Deliveries { return &Deliveries{db: db} }

// CreateDelivery implements engine.DeliveryStore.
func (s *Deliveries) CreateDelivery(ctx context.Context, d engine.NewDelivery) (string, bool, error) {
	ms := d.CreatedAt.UnixMilli()
	exec := store.Executor(ctx, s.db.Write)
	res, err := exec.ExecContext(ctx, `
		INSERT INTO deliveries (id, event_id, rule, action_index, sink, title, body,
		                        idempotency_key, status, attempts, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?)
		ON CONFLICT (idempotency_key) DO NOTHING`,
		d.ID, d.EventID, d.Rule, d.ActionIndex, d.Sink, d.Title, d.Body, d.IdempotencyKey, ms, ms)
	if err != nil {
		return "", false, fmt.Errorf("sqlite: create delivery: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return "", false, fmt.Errorf("sqlite: create delivery: %w", err)
	}
	if n == 1 {
		return d.ID, true, nil
	}

	var existing string
	err = exec.QueryRowContext(ctx,
		`SELECT id FROM deliveries WHERE idempotency_key = ?`, d.IdempotencyKey).Scan(&existing)
	if err != nil {
		return "", false, fmt.Errorf("sqlite: look up existing delivery: %w", err)
	}
	return existing, false, nil
}

const deliveryColumns = `id, event_id, rule, action_index, sink, title, body, idempotency_key,
	status, attempts, last_error, created_at, updated_at, delivered_at`

type scanner interface{ Scan(dest ...any) error }

func scanDelivery(s scanner) (engine.Delivery, error) {
	var (
		d                    engine.Delivery
		status               string
		lastErr              sql.NullString
		createdMs, updatedMs int64
		deliveredMs          sql.NullInt64
	)
	if err := s.Scan(&d.ID, &d.EventID, &d.Rule, &d.ActionIndex, &d.Sink, &d.Title, &d.Body,
		&d.IdempotencyKey, &status, &d.Attempts, &lastErr, &createdMs, &updatedMs, &deliveredMs); err != nil {
		return engine.Delivery{}, err
	}
	d.Status = engine.DeliveryStatus(status)
	d.LastError = lastErr.String
	d.CreatedAt = time.UnixMilli(createdMs)
	d.UpdatedAt = time.UnixMilli(updatedMs)
	if deliveredMs.Valid {
		d.DeliveredAt = time.UnixMilli(deliveredMs.Int64)
	}
	return d, nil
}

// GetDelivery implements engine.DeliveryStore.
func (s *Deliveries) GetDelivery(ctx context.Context, id string) (engine.Delivery, error) {
	row := store.Executor(ctx, s.db.Read).QueryRowContext(ctx,
		`SELECT `+deliveryColumns+` FROM deliveries WHERE id = ?`, id)
	d, err := scanDelivery(row)
	if errors.Is(err, sql.ErrNoRows) {
		return engine.Delivery{}, engine.ErrNotFound
	}
	if err != nil {
		return engine.Delivery{}, fmt.Errorf("sqlite: get delivery: %w", err)
	}
	return d, nil
}

// ListDeliveries implements engine.DeliveryStore.
func (s *Deliveries) ListDeliveries(ctx context.Context, q engine.DeliveryQuery) ([]engine.Delivery, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	where := []string{"id > ?"}
	args := []any{q.AfterID}
	if q.EventID != "" {
		where = append(where, "event_id = ?")
		args = append(args, q.EventID)
	}
	if q.Status != "" {
		where = append(where, "status = ?")
		args = append(args, string(q.Status))
	}
	args = append(args, limit)

	rows, err := store.Executor(ctx, s.db.Read).QueryContext(ctx,
		`SELECT `+deliveryColumns+` FROM deliveries WHERE `+strings.Join(where, " AND ")+` ORDER BY id LIMIT ?`,
		args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list deliveries: %w", err)
	}
	defer rows.Close()

	var out []engine.Delivery
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, fmt.Errorf("sqlite: list deliveries: scan: %w", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: list deliveries: %w", err)
	}
	return out, nil
}

// RecordAttempt implements engine.DeliveryStore. The attempt row and the
// delivery update commit together, so the log and the status never disagree.
func (s *Deliveries) RecordAttempt(ctx context.Context, a engine.Attempt) error {
	var status engine.DeliveryStatus
	switch a.Outcome {
	case engine.OutcomeDelivered:
		status = engine.DeliveryDelivered
	case engine.OutcomeRetry:
		status = engine.DeliveryPending
	case engine.OutcomeFailed:
		status = engine.DeliveryFailed
	default:
		return fmt.Errorf("sqlite: record attempt: unknown outcome %q", a.Outcome)
	}
	var errText, detail any // nil binds as SQL NULL
	if a.Error != "" {
		errText = a.Error
	}
	if a.Detail != "" {
		detail = a.Detail
	}
	finished := a.FinishedAt.UnixMilli()

	return store.InTx(ctx, s.db.Write, func(ctx context.Context) error {
		exec := store.Executor(ctx, s.db.Write)

		// Delivered is sticky. Delivery is at-least-once, so two workers can
		// overlap on one delivery; if the slower one fails after the other
		// succeeded, its attempt is logged and counted but must not undo the
		// success. Every CASE below sees the row's values from before the update.
		res, err := exec.ExecContext(ctx, `
			UPDATE deliveries
			SET attempts     = attempts + 1,
			    updated_at   = ?,
			    status       = CASE WHEN status = 'delivered' THEN status ELSE ? END,
			    last_error   = CASE WHEN status = 'delivered' THEN last_error ELSE ? END,
			    delivered_at = CASE WHEN status = 'delivered' THEN delivered_at
			                        WHEN ? = 'delivered' THEN ? ELSE delivered_at END
			WHERE id = ?`,
			finished, string(status), errText, string(status), finished, a.DeliveryID)
		if err != nil {
			return fmt.Errorf("sqlite: update delivery: %w", err)
		}
		if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("sqlite: update delivery: %w", err)
		} else if n == 0 {
			return engine.ErrNotFound
		}

		if _, err := exec.ExecContext(ctx, `
			INSERT INTO delivery_attempts (delivery_id, number, started_at, finished_at, outcome, error, detail)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			a.DeliveryID, a.Number, a.StartedAt.UnixMilli(), finished, string(a.Outcome), errText, detail); err != nil {
			return fmt.Errorf("sqlite: insert attempt: %w", err)
		}
		return nil
	})
}

// ListAttempts implements engine.DeliveryStore, oldest first.
func (s *Deliveries) ListAttempts(ctx context.Context, deliveryID string) ([]engine.Attempt, error) {
	rows, err := store.Executor(ctx, s.db.Read).QueryContext(ctx, `
		SELECT delivery_id, number, started_at, finished_at, outcome, error, detail
		FROM delivery_attempts WHERE delivery_id = ? ORDER BY id`, deliveryID)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list attempts: %w", err)
	}
	defer rows.Close()

	var out []engine.Attempt
	for rows.Next() {
		var (
			a                     engine.Attempt
			startedMs, finishedMs int64
			outcome               string
			errText, detail       sql.NullString
		)
		if err := rows.Scan(&a.DeliveryID, &a.Number, &startedMs, &finishedMs, &outcome, &errText, &detail); err != nil {
			return nil, fmt.Errorf("sqlite: list attempts: scan: %w", err)
		}
		a.StartedAt = time.UnixMilli(startedMs)
		a.FinishedAt = time.UnixMilli(finishedMs)
		a.Outcome = engine.AttemptOutcome(outcome)
		a.Error = errText.String
		a.Detail = detail.String
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: list attempts: %w", err)
	}
	return out, nil
}
