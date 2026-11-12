package queue_test

// Runs the shared queue contract against the SQLite implementation, plus the
// promises that only make sense for a database file: it survives restarts, it
// can be shared by several handles (processes) without handing a job out twice,
// and it fails cleanly when the database is closed or the context cancelled.

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/djenkins-1/automation-hub/internal/queue"
	"github.com/djenkins-1/automation-hub/internal/queue/queuetest"
	"github.com/djenkins-1/automation-hub/internal/store"
)

var bg = context.Background()

func openDB(t *testing.T) *store.DB {
	t.Helper()
	db, err := store.Open(bg, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func newSQLiteHarness(t *testing.T) queuetest.Harness {
	t.Helper()
	db := openDB(t)
	clk := queue.NewFakeClock(t0)
	q := queue.NewSQLite(db, queue.Options{Clock: clk, DefaultMaxAttempts: 5})
	return queuetest.Harness{
		Queue: q, Admin: q, Clock: clk,
		InTx: func(ctx context.Context, fn func(ctx context.Context) error) error { return db.InTx(ctx, fn) },
	}
}

func TestSQLiteSatisfiesTheQueueContract(t *testing.T) {
	queuetest.Run(t, newSQLiteHarness)
}

func TestStateSurvivesARestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.db")
	open := func(clk queue.Clock) (*store.DB, *queue.SQLite) {
		db, err := store.Open(bg, path)
		if err != nil {
			t.Fatal(err)
		}
		return db, queue.NewSQLite(db, queue.Options{Clock: clk})
	}

	db, q := open(queue.NewFakeClock(t0))
	ready, _, _ := q.Enqueue(bg, queue.NewJob{Kind: "ready", Payload: []byte("keep me"), DedupeKey: "k"})
	done, _, _ := q.Enqueue(bg, queue.NewJob{Kind: "done"})
	held, _, _ := q.Enqueue(bg, queue.NewJob{Kind: "held", MaxAttempts: 4})
	dead, _, _ := q.Enqueue(bg, queue.NewJob{Kind: "dead"})
	for _, kind := range []string{"done", "held", "dead"} {
		jobs, err := q.Lease(bg, "w", []string{kind}, 1, 30*time.Second)
		if err != nil || len(jobs) != 1 {
			t.Fatalf("lease %s: %v %v", kind, jobs, err)
		}
		switch kind {
		case "done":
			_ = q.Ack(bg, jobs[0])
		case "dead":
			_ = q.Fail(bg, jobs[0], errors.New("why"))
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// The process "restarts" two minutes later.
	db, q = open(queue.NewFakeClock(t0.Add(2 * time.Minute)))
	defer db.Close()
	for id, want := range map[string]string{ready: queue.StateReady, done: queue.StateDone, held: queue.StateLeased, dead: queue.StateDead} {
		ji, err := q.Get(bg, id)
		if err != nil || ji.State != want {
			t.Fatalf("job %s after restart: %+v %v, want %s", id, ji, err, want)
		}
	}
	if ji, _ := q.Get(bg, ready); string(ji.Payload) != "keep me" {
		t.Fatal("payload lost")
	}
	if _, created, _ := q.Enqueue(bg, queue.NewJob{Kind: "ready", DedupeKey: "k"}); created {
		t.Fatal("dedupe keys must survive a restart")
	}

	// The lease the old process held has lapsed meanwhile, so the new one reclaims it.
	if n, err := q.ReapExpired(bg); err != nil || n != 1 {
		t.Fatalf("reap after restart: %d %v", n, err)
	}
	jobs, _ := q.Lease(bg, "w2", []string{"held"}, 1, time.Minute)
	if len(jobs) != 1 || jobs[0].ID != held || jobs[0].Attempt != 2 {
		t.Fatalf("the abandoned job should be available again as attempt 2: %+v", jobs)
	}
}

// Two handles on one file stand in for two processes sharing a database. A job
// must still never be leased to both.
func TestSeparateHandlesNeverLeaseTheSameJob(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared.db")
	var queues []*queue.SQLite
	for i := 0; i < 2; i++ {
		db, err := store.Open(bg, path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		queues = append(queues, queue.NewSQLite(db, queue.Options{}))
	}

	const total = 200
	for i := 0; i < total; i++ {
		if _, _, err := queues[i%2].Enqueue(bg, queue.NewJob{Kind: "k"}); err != nil {
			t.Fatal(err)
		}
	}
	var (
		mu   sync.Mutex
		seen = map[string]int{}
		wg   sync.WaitGroup
	)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(q *queue.SQLite) {
			defer wg.Done()
			for {
				jobs, err := q.Lease(bg, "w", nil, 3, time.Minute)
				if err != nil {
					t.Errorf("lease: %v", err)
					return
				}
				if len(jobs) == 0 {
					return
				}
				for _, j := range jobs {
					mu.Lock()
					seen[j.ID]++
					mu.Unlock()
					if err := q.Ack(bg, j); err != nil {
						t.Errorf("ack: %v", err)
					}
				}
			}
		}(queues[w%2])
	}
	wg.Wait()
	if len(seen) != total {
		t.Fatalf("%d of %d jobs were processed", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %s was leased %d times across the two handles", id, n)
		}
	}
}

func TestOptionsDefaults(t *testing.T) {
	db := openDB(t)
	q := queue.NewSQLite(db, queue.Options{}) // zero value must be usable
	id, _, err := q.Enqueue(bg, queue.NewJob{Kind: "k"})
	if err != nil {
		t.Fatal(err)
	}
	ji, _ := q.Get(bg, id)
	if ji.MaxAttempts < 1 {
		t.Fatalf("default MaxAttempts = %d", ji.MaxAttempts)
	}
	if time.Since(ji.CreatedAt) > time.Minute || ji.CreatedAt.After(time.Now().Add(time.Minute)) {
		t.Fatalf("with no clock given the queue must use real time, CreatedAt = %v", ji.CreatedAt)
	}

	q9 := queue.NewSQLite(db, queue.Options{DefaultMaxAttempts: 9})
	id, _, _ = q9.Enqueue(bg, queue.NewJob{Kind: "k"})
	if ji, _ := q9.Get(bg, id); ji.MaxAttempts != 9 {
		t.Fatalf("configured default ignored: %d", ji.MaxAttempts)
	}
	id, _, _ = q9.Enqueue(bg, queue.NewJob{Kind: "k", MaxAttempts: 2})
	if ji, _ := q9.Get(bg, id); ji.MaxAttempts != 2 {
		t.Fatalf("per-job MaxAttempts must beat the default: %d", ji.MaxAttempts)
	}
}

func TestACancelledContextFailsWithoutSideEffects(t *testing.T) {
	h := newSQLiteHarness(t)
	id, _, _ := h.Queue.Enqueue(bg, queue.NewJob{Kind: "k"})
	cancelled, cancel := context.WithCancel(bg)
	cancel()

	if _, err := h.Queue.Lease(cancelled, "w", nil, 1, time.Minute); err == nil {
		t.Fatal("Lease with a cancelled context must fail")
	}
	if _, _, err := h.Queue.Enqueue(cancelled, queue.NewJob{Kind: "other"}); err == nil {
		t.Fatal("Enqueue with a cancelled context must fail")
	}
	if _, err := h.Admin.Get(cancelled, id); err == nil {
		t.Fatal("Get with a cancelled context must fail")
	}

	// Nothing was consumed or created by the failed calls.
	jobs, err := h.Queue.Lease(bg, "w", nil, 10, time.Minute)
	if err != nil || len(jobs) != 1 || jobs[0].ID != id || jobs[0].Attempt != 1 {
		t.Fatalf("after the failed calls: %+v %v; the job must be untouched (attempt 1) and no stray job created", jobs, err)
	}
}

func TestOperationsOnAClosedDatabaseFailInsteadOfPanicking(t *testing.T) {
	db := openDB(t)
	q := queue.NewSQLite(db, queue.Options{})
	_ = db.Close()
	calls := map[string]func() error{
		"enqueue": func() error { _, _, err := q.Enqueue(bg, queue.NewJob{Kind: "k"}); return err },
		"lease":   func() error { _, err := q.Lease(bg, "w", nil, 1, time.Minute); return err },
		"ack":     func() error { return q.Ack(bg, queue.Job{ID: "x", LeaseToken: 1}) },
		"nack":    func() error { return q.Nack(bg, queue.Job{ID: "x", LeaseToken: 1}, time.Now(), nil) },
		"fail":    func() error { return q.Fail(bg, queue.Job{ID: "x", LeaseToken: 1}, nil) },
		"extend":  func() error { _, err := q.Extend(bg, queue.Job{ID: "x", LeaseToken: 1}, time.Minute); return err },
		"reap":    func() error { _, err := q.ReapExpired(bg); return err },
		"get":     func() error { _, err := q.Get(bg, "x"); return err },
		"list":    func() error { _, err := q.ListDead(bg, 10, ""); return err },
		"requeue": func() error { return q.Requeue(bg, "x") },
		"purge":   func() error { _, err := q.Purge(bg, queue.StateDone, time.Now()); return err },
	}
	for name, call := range calls {
		err := call()
		if err == nil {
			t.Errorf("%s on a closed database must return an error", name)
		}
		if errors.Is(err, queue.ErrLeaseLost) || errors.Is(err, queue.ErrNotFound) {
			t.Errorf("%s: a database failure must not masquerade as %v (callers would drop the job)", name, err)
		}
	}
}
