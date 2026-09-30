package queue_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/djenkins-1/automation-hub/internal/queue"
	"github.com/djenkins-1/automation-hub/internal/queue/queuetest"
	"github.com/djenkins-1/automation-hub/internal/store"
)

func openDB(t *testing.T) *store.DB {
	t.Helper()
	db, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newSQLiteHarness(t *testing.T) queuetest.Harness {
	t.Helper()
	db := openDB(t)
	clk := queue.NewFakeClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	q := queue.NewSQLite(db, queue.Options{Clock: clk, DefaultMaxAttempts: 5})
	return queuetest.Harness{
		Queue: q,
		Admin: q,
		Clock: clk,
		InTx: func(ctx context.Context, fn func(ctx context.Context) error) error {
			return store.InTx(ctx, db.Write, fn)
		},
	}
}

func TestSQLiteContract(t *testing.T) {
	queuetest.Run(t, newSQLiteHarness)
}

// Reopening an existing database must not re-run migrations or lose data.
func TestSQLiteReopenKeepsJobs(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "reopen.db")

	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	q := queue.NewSQLite(db, queue.Options{})
	id, _, err := q.Enqueue(ctx, queue.NewJob{Kind: "k", Payload: []byte("p")})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = store.Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	ji, err := queue.NewSQLite(db, queue.Options{}).Get(ctx, id)
	if err != nil || ji.State != queue.StateReady || string(ji.Payload) != "p" {
		t.Fatalf("after reopen: %+v, %v", ji, err)
	}
}

func TestRetryPolicyBackoff(t *testing.T) {
	p := queue.RetryPolicy{Base: time.Second, Max: 10 * time.Second, Factor: 2, Jitter: -1}
	want := []time.Duration{1, 2, 4, 8, 10, 10}
	for i, w := range want {
		if got := p.Backoff(i + 1); got != w*time.Second {
			t.Errorf("Backoff(%d) = %v, want %v", i+1, got, w*time.Second)
		}
	}
	if got := p.Backoff(10_000); got != 10*time.Second {
		t.Errorf("huge attempt should clamp to Max, got %v", got)
	}

	// Jitter stays within +/-20% of the base delay.
	j := queue.RetryPolicy{Base: 10 * time.Second, Jitter: 0.2, Rand: func() float64 { return 0 }}
	if got := j.Backoff(1); got != 8*time.Second {
		t.Errorf("min jitter = %v, want 8s", got)
	}
	j.Rand = func() float64 { return 0.999999 }
	if got := j.Backoff(1); got < 11900*time.Millisecond || got > 12*time.Second {
		t.Errorf("max jitter = %v, want ~12s", got)
	}
}
