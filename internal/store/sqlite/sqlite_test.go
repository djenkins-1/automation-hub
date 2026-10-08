package sqlite_test

// Runs the shared store contract against the SQLite implementation, plus what
// is specific to a file shared by several handles and restarts.

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/djenkins-1/automation-hub/internal/engine"
	"github.com/djenkins-1/automation-hub/internal/engine/storetest"
	"github.com/djenkins-1/automation-hub/internal/store"
	"github.com/djenkins-1/automation-hub/internal/store/sqlite"
	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

var bg = context.Background()

func harness(t *testing.T) storetest.Harness {
	t.Helper()
	db, err := store.Open(bg, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return storetest.Harness{
		Events:     sqlite.NewEvents(db),
		Deliveries: sqlite.NewDeliveries(db),
		InTx:       db.InTx,
	}
}

func TestSQLiteSatisfiesTheStoreContract(t *testing.T) {
	storetest.Run(t, harness)
}

func TestDataSurvivesARestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.db")
	ms := time.UnixMilli(1_767_268_800_000)

	db, err := store.Open(bg, path)
	if err != nil {
		t.Fatal(err)
	}
	events, deliveries := sqlite.NewEvents(db), sqlite.NewDeliveries(db)
	_, _, _ = events.InsertEvent(bg, sdk.Event{ID: "e1", Source: "s", Type: "t", Time: ms, IdempotencyKey: "k", Data: map[string]any{"a": 1.5}}, ms)
	_, _, _ = deliveries.CreateDelivery(bg, engine.NewDelivery{ID: "d1", EventID: "e1", Rule: "r", Sink: "s", Title: "t", IdempotencyKey: "dk", CreatedAt: ms})
	_ = deliveries.RecordAttempt(bg, engine.Attempt{DeliveryID: "d1", Number: 1, Outcome: engine.OutcomeDelivered, Detail: "ok", StartedAt: ms, FinishedAt: ms})
	_ = db.Close()

	db, err = store.Open(bg, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	events, deliveries = sqlite.NewEvents(db), sqlite.NewDeliveries(db)
	if e, err := events.GetEvent(bg, "e1"); err != nil || e.Data["a"] != 1.5 {
		t.Fatalf("event after restart: %+v %v", e, err)
	}
	if d, err := deliveries.GetDelivery(bg, "d1"); err != nil || d.Status != engine.DeliveryDelivered || d.Attempts != 1 {
		t.Fatalf("delivery after restart: %+v %v", d, err)
	}
	if _, created, _ := events.InsertEvent(bg, sdk.Event{ID: "e2", Source: "s", Type: "t", Time: ms, IdempotencyKey: "k"}, ms); created {
		t.Fatal("idempotency keys must survive a restart")
	}
}

// Two handles stand in for two processes sharing one database file.
func TestSeparateHandlesAgreeOnWhoWonARace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared.db")
	ms := time.UnixMilli(1_767_268_800_000)
	var handles []*store.DB
	for i := 0; i < 2; i++ {
		db, err := store.Open(bg, path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		handles = append(handles, db)
	}

	const n = 20
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		created int
		winners = map[string]bool{}
	)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			events := sqlite.NewEvents(handles[i%2])
			id, c, err := events.InsertEvent(bg, sdk.Event{ID: fmt.Sprintf("e%d", i), Source: "s", Type: "t", Time: ms, IdempotencyKey: "race"}, ms)
			if err != nil {
				t.Errorf("insert: %v", err)
				return
			}
			mu.Lock()
			winners[id] = true
			if c {
				created++
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	if created != 1 || len(winners) != 1 {
		t.Fatalf("created=%d winners=%d across two handles; exactly one insert may win", created, len(winners))
	}
}
