// Package sqlite holds the SQL implementations of the engine's stores. It sits
// beside store (connections, transactions, migrations) rather than inside it
// because it imports the engine, which imports the queue, which imports store;
// keeping it separate avoids an import cycle. A future store/postgres package
// would mirror this one.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/djenkins-1/automation-hub/internal/engine"
	"github.com/djenkins-1/automation-hub/internal/store"
	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

// Events implements engine.EventStore.
type Events struct{ db *store.DB }

var _ engine.EventStore = (*Events)(nil)

// NewEvents returns an event store on db.
func NewEvents(db *store.DB) *Events { return &Events{db: db} }

// InsertEvent implements engine.EventStore. Like queue.Enqueue it uses the
// transaction carried by ctx if there is one.
func (s *Events) InsertEvent(ctx context.Context, ev sdk.Event, receivedAt time.Time) (string, bool, error) {
	data := ev.Data
	if data == nil {
		data = map[string]any{}
	}
	blob, err := json.Marshal(data)
	if err != nil {
		return "", false, fmt.Errorf("sqlite: encode event data: %w", err)
	}
	var key any // nil binds as SQL NULL
	if ev.IdempotencyKey != "" {
		key = ev.IdempotencyKey
	}

	exec := store.Executor(ctx, s.db.Write)
	res, err := exec.ExecContext(ctx, `
		INSERT INTO events (id, source, type, subject, occurred_at, data, idempotency_key, received_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (source, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`,
		ev.ID, ev.Source, ev.Type, ev.Subject, ev.Time.UnixMilli(), string(blob), key, receivedAt.UnixMilli())
	if err != nil {
		return "", false, fmt.Errorf("sqlite: insert event: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return "", false, fmt.Errorf("sqlite: insert event: %w", err)
	}
	if n == 1 {
		return ev.ID, true, nil
	}

	var existing string
	err = exec.QueryRowContext(ctx,
		`SELECT id FROM events WHERE source = ? AND idempotency_key = ?`, ev.Source, ev.IdempotencyKey).Scan(&existing)
	if err != nil {
		return "", false, fmt.Errorf("sqlite: look up duplicate event: %w", err)
	}
	return existing, false, nil
}

// GetEvent implements engine.EventStore.
func (s *Events) GetEvent(ctx context.Context, id string) (sdk.Event, error) {
	var (
		ev       sdk.Event
		occurred int64
		data     string
		key      sql.NullString
	)
	err := store.Executor(ctx, s.db.Read).QueryRowContext(ctx, `
		SELECT id, source, type, subject, occurred_at, data, idempotency_key
		FROM events WHERE id = ?`, id).
		Scan(&ev.ID, &ev.Source, &ev.Type, &ev.Subject, &occurred, &data, &key)
	if errors.Is(err, sql.ErrNoRows) {
		return sdk.Event{}, engine.ErrNotFound
	}
	if err != nil {
		return sdk.Event{}, fmt.Errorf("sqlite: get event: %w", err)
	}
	ev.Time = time.UnixMilli(occurred).UTC()
	ev.IdempotencyKey = key.String
	if err := json.Unmarshal([]byte(data), &ev.Data); err != nil {
		return sdk.Event{}, fmt.Errorf("sqlite: decode event data: %w", err)
	}
	return ev, nil
}
