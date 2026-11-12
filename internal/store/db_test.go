package store_test

// Specification under test: the database layer everything else stands on.
//
//  - Open creates a ready-to-use database, applies migrations exactly once,
//    and is safe to repeat; a bad path fails cleanly.
//  - Connection settings: WAL journaling, foreign keys enforced on EVERY
//    connection, a busy timeout, a single writer connection, a read pool
//    that cannot write.
//  - Transactions: commit on success, roll back on error AND panic (and the
//    connection is usable afterwards), nested calls join the outer one,
//    writers never lose updates, not even across separate handles
//    (processes) on the same file.
//  - The schema itself rejects invalid data: bad states, duplicate keys,
//    orphan rows; deleting an event removes everything hanging off it.
//  - Migration files are numbered sequentially, so ordering is unambiguous.
//
// Several of these double as a check of driver.go's connection string: they
// fail if the DSN stops applying a setting the system depends on.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djenkins-1/automation-hub/internal/store"
)

var ctx = context.Background()

func open(t *testing.T) (*store.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, path
}

func count(t *testing.T, db store.DBTX, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// ---------------------------------------------------------------------------
// Open and migrations

func TestOpenCreatesAReadyDatabase(t *testing.T) {
	db, path := open(t)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("database file was not created: %v", err)
	}
	for _, table := range []string{"jobs", "events", "deliveries", "delivery_attempts", "schema_migrations"} {
		if n := count(t, db.Read, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table); n != 1 {
			t.Errorf("table %q is missing after Open", table)
		}
	}
}

func TestReopeningKeepsDataAndDoesNotReapplyMigrations(t *testing.T) {
	db, path := open(t)
	mustExec(t, db, `INSERT INTO jobs (id, kind, payload, max_attempts, run_at, created_at, updated_at) VALUES ('j1','k',x'',3,0,0,0)`)
	applied := count(t, db.Read, `SELECT COUNT(*) FROM schema_migrations`)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		db2, err := store.Open(ctx, path)
		if err != nil {
			t.Fatalf("reopen %d: %v", i, err)
		}
		if n := count(t, db2.Read, `SELECT COUNT(*) FROM jobs WHERE id='j1'`); n != 1 {
			t.Fatalf("reopen %d lost data", i)
		}
		if n := count(t, db2.Read, `SELECT COUNT(*) FROM schema_migrations`); n != applied {
			t.Fatalf("reopen %d: %d migration records, want %d", i, n, applied)
		}
		_ = db2.Close()
	}
}

func TestEveryMigrationFileIsRecordedExactlyOnce(t *testing.T) {
	db, _ := open(t)
	files := migrationFiles(t)
	rows, err := db.Read.QueryContext(ctx, `SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var recorded []string
	for rows.Next() {
		var v string
		_ = rows.Scan(&v)
		recorded = append(recorded, v)
	}
	if strings.Join(recorded, ",") != strings.Join(files, ",") {
		t.Fatalf("recorded migrations %v do not match the files %v", recorded, files)
	}
}

func migrationFiles(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir("migrations")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names
}

// Ordering is by filename, so a skipped, duplicated or badly named number would
// silently change what runs when.
func TestMigrationFilesAreNumberedSequentially(t *testing.T) {
	names := migrationFiles(t)
	if len(names) == 0 {
		t.Fatal("no migrations found")
	}
	pattern := regexp.MustCompile(`^(\d{4})_[a-z0-9_]+\.sql$`)
	for i, name := range names {
		m := pattern.FindStringSubmatch(name)
		if m == nil {
			t.Errorf("%q must be named NNNN_description.sql (lowercase, underscores)", name)
			continue
		}
		if n, _ := strconv.Atoi(m[1]); n != i+1 {
			t.Errorf("%q is migration number %d but sits at position %d: numbers must start at 0001 and have no gaps or repeats", name, n, i+1)
		}
	}
}

func TestOpenFailsCleanly(t *testing.T) {
	dir := t.TempDir()
	for name, path := range map[string]string{
		"directory does not exist": filepath.Join(dir, "no", "such", "dir", "x.db"),
		"path is a directory":      dir,
	} {
		db, err := store.Open(ctx, path)
		if err == nil {
			_ = db.Close()
			t.Errorf("%s: expected an error", name)
		} else if db != nil {
			t.Errorf("%s: a failed Open must not return a DB", name)
		}
	}
}

func TestOpenRespectsACancelledContext(t *testing.T) {
	c, cancel := context.WithCancel(ctx)
	cancel()
	db, err := store.Open(c, filepath.Join(t.TempDir(), "x.db"))
	if err == nil {
		_ = db.Close()
		t.Fatal("Open with a cancelled context should fail")
	}
}

func TestCloseIsFinal(t *testing.T) {
	db, _ := open(t)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Write.ExecContext(ctx, `SELECT 1`); err == nil {
		t.Fatal("the write pool should be closed")
	}
	if _, err := db.Read.QueryContext(ctx, `SELECT 1`); err == nil {
		t.Fatal("the read pool should be closed")
	}
}

// ---------------------------------------------------------------------------
// Connection settings

func pragma(t *testing.T, c *sql.Conn, name string) string {
	t.Helper()
	var v string
	if err := c.QueryRowContext(ctx, "PRAGMA "+name).Scan(&v); err != nil {
		t.Fatalf("PRAGMA %s: %v", name, err)
	}
	return strings.ToLower(v)
}

func TestEveryConnectionHasTheRequiredSettings(t *testing.T) {
	db, _ := open(t)

	// Hold several read connections at once so the pool really opens more than one.
	var conns []*sql.Conn
	for i := 0; i < 3; i++ {
		c, err := db.Read.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
	}
	w, err := db.Write.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	conns = append(conns, w)
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()

	for i, c := range conns {
		name := fmt.Sprintf("read connection %d", i)
		if c == w {
			name = "write connection"
		}
		if got := pragma(t, c, "journal_mode"); got != "wal" {
			t.Errorf("%s: journal_mode = %q, want wal (readers must not block the writer)", name, got)
		}
		if got := pragma(t, c, "foreign_keys"); got != "1" {
			t.Errorf("%s: foreign_keys = %q, want 1 (it is per connection and off by default)", name, got)
		}
		if ms, _ := strconv.Atoi(pragma(t, c, "busy_timeout")); ms < 1000 {
			t.Errorf("%s: busy_timeout = %dms, want at least 1000", name, ms)
		}
	}
}

func TestWritePoolHasExactlyOneConnectionAndReadPoolCannotWrite(t *testing.T) {
	db, _ := open(t)
	if got := db.Write.Stats().MaxOpenConnections; got != 1 {
		t.Errorf("write pool allows %d connections, want exactly 1 (single writer)", got)
	}
	if got := db.Read.Stats().MaxOpenConnections; got < 2 {
		t.Errorf("read pool allows %d connections; concurrent readers are the point of having it", got)
	}
	_, err := db.Read.ExecContext(ctx, `INSERT INTO jobs (id, kind, payload, max_attempts, run_at, created_at, updated_at) VALUES ('x','k',x'',1,0,0,0)`)
	if err == nil {
		t.Fatal("a write through the read pool must fail")
	}
	mustExec(t, db, `INSERT INTO jobs (id, kind, payload, max_attempts, run_at, created_at, updated_at) VALUES ('x','k',x'',1,0,0,0)`)
}

func TestReadsSeeCommittedWritesImmediately(t *testing.T) {
	db, _ := open(t)
	for i := 0; i < 200; i++ {
		id := fmt.Sprintf("j%d", i)
		mustExec(t, db, `INSERT INTO jobs (id, kind, payload, max_attempts, run_at, created_at, updated_at) VALUES (?,'k',x'',1,0,0,0)`, id)
		if count(t, db.Read, `SELECT COUNT(*) FROM jobs WHERE id=?`, id) != 1 {
			t.Fatalf("a read right after the commit of %s did not see it", id)
		}
	}
}

func TestReadersAreNeitherBlockedByNorSeeAnOpenWriteTransaction(t *testing.T) {
	db, _ := open(t)
	mustExec(t, db, `INSERT INTO jobs (id, kind, payload, max_attempts, run_at, created_at, updated_at) VALUES ('before','k',x'',1,0,0,0)`)

	inside := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- db.InTx(ctx, func(c context.Context) error {
			tx, _ := store.TxFrom(c)
			if _, err := tx.ExecContext(c, `INSERT INTO jobs (id, kind, payload, max_attempts, run_at, created_at, updated_at) VALUES ('uncommitted','k',x'',1,0,0,0)`); err != nil {
				return err
			}
			close(inside)
			<-release
			return nil
		})
	}()
	<-inside

	readDone := make(chan int, 1)
	go func() {
		var n int
		_ = db.Read.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs`).Scan(&n)
		readDone <- n
	}()
	select {
	case n := <-readDone:
		if n != 1 {
			t.Errorf("reader saw %d jobs; it must see only the committed one", n)
		}
	case <-time.After(3 * time.Second):
		t.Error("a reader was blocked by an open write transaction")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if n := count(t, db.Read, `SELECT COUNT(*) FROM jobs`); n != 2 {
		t.Fatalf("after commit there should be 2 jobs, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// Transactions

func TestInTxCommitsOnSuccess(t *testing.T) {
	db, _ := open(t)
	err := db.InTx(ctx, func(c context.Context) error {
		tx, ok := store.TxFrom(c)
		if !ok {
			t.Error("the function must receive a context carrying the transaction")
		}
		_, err := tx.ExecContext(c, `INSERT INTO jobs (id, kind, payload, max_attempts, run_at, created_at, updated_at) VALUES ('j','k',x'',1,0,0,0)`)
		return err
	})
	if err != nil || count(t, db.Read, `SELECT COUNT(*) FROM jobs`) != 1 {
		t.Fatalf("err=%v", err)
	}
}

func TestInTxRollsBackOnErrorAndReturnsThatError(t *testing.T) {
	db, _ := open(t)
	boom := errors.New("boom")
	err := db.InTx(ctx, func(c context.Context) error {
		tx, _ := store.TxFrom(c)
		_, _ = tx.ExecContext(c, `INSERT INTO jobs (id, kind, payload, max_attempts, run_at, created_at, updated_at) VALUES ('j','k',x'',1,0,0,0)`)
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if n := count(t, db.Read, `SELECT COUNT(*) FROM jobs`); n != 0 {
		t.Fatalf("%d rows survived a rolled-back transaction", n)
	}
	assertWriterStillUsable(t, db)
}

func TestInTxRollsBackAndRepanicsOnPanic(t *testing.T) {
	db, _ := open(t)
	func() {
		defer func() {
			if r := recover(); r != "kaboom" {
				t.Errorf("recovered %v, want the original panic value to propagate", r)
			}
		}()
		_ = db.InTx(ctx, func(c context.Context) error {
			tx, _ := store.TxFrom(c)
			_, _ = tx.ExecContext(c, `INSERT INTO jobs (id, kind, payload, max_attempts, run_at, created_at, updated_at) VALUES ('j','k',x'',1,0,0,0)`)
			panic("kaboom")
		})
	}()
	if n := count(t, db.Read, `SELECT COUNT(*) FROM jobs`); n != 0 {
		t.Fatalf("%d rows survived a panicking transaction", n)
	}
	assertWriterStillUsable(t, db)
}

// A leaked transaction would hold the only write connection forever and hang
// the whole system, so "usable afterwards" is checked with a deadline.
func assertWriterStillUsable(t *testing.T, db *store.DB) {
	t.Helper()
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.InTx(c, func(c context.Context) error {
		tx, _ := store.TxFrom(c)
		_, err := tx.ExecContext(c, `SELECT 1`)
		return err
	}); err != nil {
		t.Fatalf("the write connection is stuck after a failed transaction: %v", err)
	}
}

func TestNestedInTxJoinsTheOuterTransaction(t *testing.T) {
	db, _ := open(t)
	insert := func(c context.Context, id string) error {
		tx, _ := store.TxFrom(c)
		_, err := tx.ExecContext(c, `INSERT INTO jobs (id, kind, payload, max_attempts, run_at, created_at, updated_at) VALUES (?,'k',x'',1,0,0,0)`, id)
		return err
	}

	var outerTx, innerTx *sql.Tx
	err := db.InTx(ctx, func(c context.Context) error {
		outerTx, _ = store.TxFrom(c)
		if err := insert(c, "outer"); err != nil {
			return err
		}
		return db.InTx(c, func(c2 context.Context) error { // must not deadlock on the single connection
			innerTx, _ = store.TxFrom(c2)
			return insert(c2, "inner")
		})
	})
	if err != nil || outerTx == nil || outerTx != innerTx {
		t.Fatalf("err=%v: a nested InTx must reuse the outer transaction", err)
	}
	if count(t, db.Read, `SELECT COUNT(*) FROM jobs`) != 2 {
		t.Fatal("both inserts should be committed together")
	}

	// An error in the inner call, propagated, undoes the outer work as well.
	boom := errors.New("inner failed")
	err = db.InTx(ctx, func(c context.Context) error {
		if err := insert(c, "outer2"); err != nil {
			return err
		}
		return db.InTx(c, func(c2 context.Context) error {
			_ = insert(c2, "inner2")
			return boom
		})
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if n := count(t, db.Read, `SELECT COUNT(*) FROM jobs WHERE id IN ('outer2','inner2')`); n != 0 {
		t.Fatalf("%d rows survived", n)
	}
}

func TestContextHelpers(t *testing.T) {
	db, _ := open(t)
	if _, ok := store.TxFrom(ctx); ok {
		t.Fatal("a plain context carries no transaction")
	}
	if got := store.Executor(ctx, db.Write); got != store.DBTX(db.Write) {
		t.Fatal("without a transaction Executor must return the fallback unchanged")
	}
	tx, err := db.Write.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	c := store.WithTx(ctx, tx)
	if got, ok := store.TxFrom(c); !ok || got != tx {
		t.Fatal("WithTx / TxFrom must round-trip")
	}
	if got := store.Executor(c, db.Write); got != store.DBTX(tx) {
		t.Fatal("with a transaction in the context Executor must return it, not the fallback")
	}
}

func TestInTxHonoursAnAlreadyCancelledContext(t *testing.T) {
	db, _ := open(t)
	c, cancel := context.WithCancel(ctx)
	cancel()
	called := false
	err := db.InTx(c, func(context.Context) error { called = true; return nil })
	if err == nil || called {
		t.Fatalf("err=%v called=%v: nothing should run under a cancelled context", err, called)
	}
	assertWriterStillUsable(t, db)
}

func TestConcurrentTransactionsNeverLoseUpdates(t *testing.T) {
	db, _ := open(t)
	mustExec(t, db, `INSERT INTO jobs (id, kind, payload, max_attempts, run_at, created_at, updated_at) VALUES ('counter','k',x'',1000000,0,0,0)`)
	const workers, perWorker = 8, 25
	increment(t, []*store.DB{db}, workers, perWorker)
	if got := count(t, db.Read, `SELECT attempts FROM jobs WHERE id='counter'`); got != workers*perWorker {
		t.Fatalf("counter = %d, want %d: updates were lost", got, workers*perWorker)
	}
}

// Two handles on one file behave like two processes (the server and an admin
// tool, or two nodes sharing a volume). They contend for real SQLite locks, so
// this is also the check that write transactions take the lock up front and wait
// for it, rather than failing half-way with "database is locked".
func TestSeparateHandlesOnOneFileNeverLoseUpdatesOrFailWithBusy(t *testing.T) {
	a, path := open(t)
	b, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	mustExec(t, a, `INSERT INTO jobs (id, kind, payload, max_attempts, run_at, created_at, updated_at) VALUES ('counter','k',x'',1000000,0,0,0)`)

	const workersPerHandle, perWorker = 4, 25
	increment(t, []*store.DB{a, b}, workersPerHandle, perWorker)
	if got := count(t, a.Read, `SELECT attempts FROM jobs WHERE id='counter'`); got != 2*workersPerHandle*perWorker {
		t.Fatalf("counter = %d, want %d", got, 2*workersPerHandle*perWorker)
	}
}

// The dangerous interleaving, forced: A reads, then B reads, then A writes and
// commits, then B writes. If transactions only took the write lock when they
// first wrote, B would be updating a snapshot that A has since invalidated and
// fail with "database is locked". Taking the lock when the transaction BEGINS
// makes B wait its turn instead, so both succeed and nothing is lost.
func TestWriteTransactionsOnSeparateHandlesTakeTheWriteLockUpFront(t *testing.T) {
	a, path := open(t)
	b, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	mustExec(t, a, `INSERT INTO jobs (id, kind, payload, max_attempts, run_at, created_at, updated_at) VALUES ('counter','k',x'',100,0,0,0)`)

	readAndBump := func(db *store.DB, afterRead func()) error {
		return db.InTx(ctx, func(c context.Context) error {
			tx, _ := store.TxFrom(c)
			var n int
			if err := tx.QueryRowContext(c, `SELECT attempts FROM jobs WHERE id='counter'`).Scan(&n); err != nil {
				return err
			}
			afterRead()
			_, err := tx.ExecContext(c, `UPDATE jobs SET attempts = ? WHERE id='counter'`, n+1)
			return err
		})
	}

	aRead, bRead := make(chan struct{}), make(chan struct{})
	errs := make(chan error, 2)
	go func() {
		errs <- readAndBump(a, func() {
			close(aRead)
			select { // give B the chance to read too; if B is correctly kept waiting, stop waiting for it
			case <-bRead:
			case <-time.After(400 * time.Millisecond):
			}
		})
	}()
	go func() {
		<-aRead
		errs <- readAndBump(b, func() { close(bRead) })
	}()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("a write transaction failed because it did not take the write lock up front: %v", err)
		}
	}
	if got := count(t, a.Read, `SELECT attempts FROM jobs WHERE id='counter'`); got != 2 {
		t.Fatalf("counter = %d, want 2: an update was lost", got)
	}
}

// increment runs read-modify-write transactions (the pattern that loses updates
// when transactions are not serialised) from many goroutines across handles.
func increment(t *testing.T, handles []*store.DB, workersPerHandle, perWorker int) {
	t.Helper()
	var wg sync.WaitGroup
	var failures atomic.Int64
	for _, db := range handles {
		for w := 0; w < workersPerHandle; w++ {
			wg.Add(1)
			go func(db *store.DB) {
				defer wg.Done()
				for i := 0; i < perWorker; i++ {
					err := db.InTx(ctx, func(c context.Context) error {
						tx, _ := store.TxFrom(c)
						var n int
						if err := tx.QueryRowContext(c, `SELECT attempts FROM jobs WHERE id='counter'`).Scan(&n); err != nil {
							return err
						}
						_, err := tx.ExecContext(c, `UPDATE jobs SET attempts = ? WHERE id='counter'`, n+1)
						return err
					})
					if err != nil {
						failures.Add(1)
						t.Errorf("transaction failed: %v", err)
						return
					}
				}
			}(db)
		}
	}
	wg.Wait()
	if failures.Load() != 0 {
		t.FailNow()
	}
}

// ---------------------------------------------------------------------------
// Schema guarantees: the database refuses data that would break the system's
// invariants, whichever code path (or operator with a sqlite3 prompt) tries.

func mustExec(t *testing.T, db *store.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Write.ExecContext(ctx, q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func mustReject(t *testing.T, db *store.DB, why, q string, args ...any) {
	t.Helper()
	if _, err := db.Write.ExecContext(ctx, q, args...); err == nil {
		t.Errorf("the schema should reject: %s", why)
	}
}

const (
	insJob      = `INSERT INTO jobs (id, kind, payload, dedupe_key, state, attempts, max_attempts, run_at, created_at, updated_at) VALUES (?, ?, x'', ?, ?, ?, ?, 0, 0, 0)`
	insEvent    = `INSERT INTO events (id, source, type, occurred_at, data, idempotency_key, received_at) VALUES (?, ?, 't', 0, '{}', ?, 0)`
	insDelivery = `INSERT INTO deliveries (id, event_id, rule, action_index, sink, title, body, idempotency_key, status, created_at, updated_at) VALUES (?, ?, 'r', 0, 's', 't', 'b', ?, ?, 0, 0)`
	insAttempt  = `INSERT INTO delivery_attempts (delivery_id, number, started_at, finished_at, outcome) VALUES (?, 1, 0, 0, ?)`
)

func TestSchemaProtectsJobs(t *testing.T) {
	db, _ := open(t)
	mustExec(t, db, insJob, "ok", "k", nil, "ready", 0, 3)
	for _, state := range []string{"ready", "leased", "done", "dead"} {
		mustExec(t, db, insJob, "state-"+state, "k", nil, state, 0, 3)
	}
	mustReject(t, db, "an unknown job state", insJob, "bad-state", "k", nil, "running", 0, 3)
	mustReject(t, db, "max_attempts of zero", insJob, "zero-max", "k", nil, "ready", 0, 0)
	mustReject(t, db, "negative max_attempts", insJob, "neg-max", "k", nil, "ready", 0, -1)
	mustReject(t, db, "a duplicate job id", insJob, "ok", "k", nil, "ready", 0, 3)
	mustReject(t, db, "text in an integer column (tables are STRICT)", insJob, "strict", "k", nil, "ready", "many", 3)

	// dedupe keys: unique per kind, optional
	mustExec(t, db, insJob, "d1", "kindA", "key", "ready", 0, 3)
	mustReject(t, db, "the same dedupe key twice for one kind", insJob, "d2", "kindA", "key", "ready", 0, 3)
	mustExec(t, db, insJob, "d3", "kindB", "key", "ready", 0, 3) // another kind may reuse it
	mustExec(t, db, insJob, "n1", "kindA", nil, "ready", 0, 3)   // no key: no dedupe
	mustExec(t, db, insJob, "n2", "kindA", nil, "ready", 0, 3)
}

func TestSchemaProtectsEventsAndTheirIdempotencyKeys(t *testing.T) {
	db, _ := open(t)
	mustExec(t, db, insEvent, "e1", "src", "key")
	mustReject(t, db, "the same idempotency key from the same source", insEvent, "e2", "src", "key")
	mustExec(t, db, insEvent, "e3", "other-src", "key") // scoped to the source
	mustExec(t, db, insEvent, "e4", "src", nil)         // keys are optional
	mustExec(t, db, insEvent, "e5", "src", nil)
	mustReject(t, db, "a duplicate event id", insEvent, "e1", "x", nil)
}

func TestSchemaProtectsDeliveriesAndAttempts(t *testing.T) {
	db, _ := open(t)
	mustExec(t, db, insEvent, "e1", "src", nil)

	mustExec(t, db, insDelivery, "d1", "e1", "key1", "pending")
	for i, status := range []string{"delivered", "failed"} {
		mustExec(t, db, insDelivery, fmt.Sprintf("ds%d", i), "e1", fmt.Sprintf("k-%s", status), status)
	}
	mustReject(t, db, "an unknown delivery status", insDelivery, "bad", "e1", "kbad", "sent")
	mustReject(t, db, "a duplicate delivery idempotency key", insDelivery, "dup", "e1", "key1", "pending")
	mustReject(t, db, "a delivery for an event that does not exist", insDelivery, "orphan", "ghost", "korphan", "pending")

	for _, outcome := range []string{"delivered", "retry", "failed"} {
		mustExec(t, db, insAttempt, "d1", outcome)
	}
	mustReject(t, db, "an unknown attempt outcome", insAttempt, "d1", "maybe")
	mustReject(t, db, "an attempt for a delivery that does not exist", insAttempt, "ghost", "retry")
}

func TestDeletingAnEventRemovesEverythingHangingOffIt(t *testing.T) {
	db, _ := open(t)
	mustExec(t, db, insEvent, "keep", "src", nil)
	mustExec(t, db, insEvent, "gone", "src", nil)
	mustExec(t, db, insDelivery, "d-keep", "keep", "k1", "pending")
	mustExec(t, db, insDelivery, "d-gone1", "gone", "k2", "pending")
	mustExec(t, db, insDelivery, "d-gone2", "gone", "k3", "failed")
	mustExec(t, db, insAttempt, "d-keep", "retry")
	mustExec(t, db, insAttempt, "d-gone1", "retry")
	mustExec(t, db, insAttempt, "d-gone2", "failed")

	mustExec(t, db, `DELETE FROM events WHERE id='gone'`) // what a retention job will do

	if got := count(t, db.Read, `SELECT COUNT(*) FROM deliveries WHERE event_id='gone'`); got != 0 {
		t.Errorf("%d deliveries left behind", got)
	}
	if got := count(t, db.Read, `SELECT COUNT(*) FROM delivery_attempts WHERE delivery_id IN ('d-gone1','d-gone2')`); got != 0 {
		t.Errorf("%d attempts left behind", got)
	}
	if count(t, db.Read, `SELECT COUNT(*) FROM deliveries WHERE id='d-keep'`) != 1 ||
		count(t, db.Read, `SELECT COUNT(*) FROM delivery_attempts WHERE delivery_id='d-keep'`) != 1 {
		t.Error("deleting one event must not touch another's rows")
	}
}
