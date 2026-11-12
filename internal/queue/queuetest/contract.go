// Package queuetest is the behavioural specification of a job queue, written as
// an executable test suite. Any queue.Queue / queue.Admin implementation (SQLite
// today, Postgres tomorrow) must pass it unchanged:
//
//	func TestPostgresContract(t *testing.T) { queuetest.Run(t, newPostgresHarness) }
//
// The suite is organised by promise, not by method:
//
//	Enqueue    jobs are stored faithfully, are scheduled for when asked, and
//	           duplicates are suppressed per kind
//	Lease      a job is held by at most one worker at a time, oldest first, and
//	           every lease carries a fresh, higher fencing token
//	Outcomes   only the CURRENT holder can ack, nack, fail or extend; anyone
//	           else (a zombie) is told ErrLeaseLost and changes nothing
//	Retries    attempts are counted when leased, so a job that kills its worker
//	           every time still ends up dead
//	Recovery   crashed workers' jobs come back; nothing is lost, nothing runs twice
//	           at once
//	Admin      dead letters can be listed, requeued and purged, safely
//	Model      random operation sequences, replayed against a small reference
//	           model of the rules above, agree with the queue at every step
package queuetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djenkins-1/automation-hub/internal/queue"
)

const leaseFor = 30 * time.Second

// Harness is one fresh, empty queue under test.
type Harness struct {
	Queue queue.Queue
	Admin queue.Admin
	// Clock must be the clock the queue was constructed with.
	Clock *queue.FakeClock
	// InTx runs fn in a transaction (commit on nil, roll back on error). The
	// context handed to fn is what must be passed to Enqueue to join it.
	InTx func(ctx context.Context, fn func(ctx context.Context) error) error
}

// Factory builds a fresh Harness for each test, registering its own cleanup.
type Factory func(t *testing.T) Harness

// Run executes the whole suite as subtests.
func Run(t *testing.T, newHarness Factory) {
	t.Helper()
	suites := []struct {
		name  string
		tests []namedTest
	}{
		{"Enqueue", []namedTest{
			{"StoresJobsFaithfully", testStoresFaithfully},
			{"ArbitraryBytesAndTextAreSafe", testArbitraryContent},
			{"RejectsInvalidJobs", testRejectsInvalid},
			{"RespectsRunAt", testRunAt},
			{"DedupeIsPerKindAndSurvivesCompletion", testDedupe},
			{"DedupeUnderConcurrency", testDedupeConcurrent},
			{"JoinsTransactions", testTransactionalEnqueue},
		}},
		{"Lease", []namedTest{
			{"ClaimsOnlyReadyDueJobs", testLeaseEligibility},
			{"HonoursKindsLimitAndOrder", testLeaseSelection},
			{"MetadataIsAccurate", testLeaseMetadata},
			{"FencingTokensOnlyIncrease", testTokensIncrease},
			{"NeverHandsOutOneJobTwice", testConcurrentLease},
			{"DrainsEverythingExactlyOnce", testDrain},
		}},
		{"Outcomes", []namedTest{
			{"AckFinishesTheJob", testAck},
			{"OnlyTheCurrentHolderMayReport", testZombieRejected},
			{"WrongOrUnknownLeasesAreRejectedWithoutEffect", testWrongLease},
			{"NackSchedulesRetryThenDeadLetters", testNack},
			{"FailDeadLettersImmediately", testFail},
			{"ErrorTextIsStoredAndBounded", testErrorText},
			{"ExtendKeepsALeaseAlive", testExtend},
			{"LateReportsSucceedUntilTheJobIsReclaimed", testLateReports},
		}},
		{"Recovery", []namedTest{
			{"ExpiredLeasesAreReclaimed", testReap},
			{"PoisonPillsDieWithoutAnyNack", testPoisonPill},
			{"ReapTouchesOnlyExpiredLeases", testReapSelective},
		}},
		{"Admin", []namedTest{
			{"GetReportsJobsAndMissingOnes", testGet},
			{"ListDeadPaginatesCompletely", testListDead},
			{"RequeueGivesDeadJobsAFreshStart", testRequeue},
			{"PurgeIsPerStateAndStrictlyOlder", testPurge},
		}},
		{"Model", nil}, // one subtest per seed, added below, each on a fresh queue
	}
	for seed := int64(1); seed <= 8; seed++ {
		seed := seed
		suites[len(suites)-1].tests = append(suites[len(suites)-1].tests, namedTest{
			fmt.Sprintf("Seed%d", seed), func(t *testing.T, h Harness) { runModel(t, h, seed) },
		})
	}
	for _, s := range suites {
		t.Run(s.name, func(t *testing.T) {
			for _, tc := range s.tests {
				t.Run(tc.name, func(t *testing.T) { tc.fn(t, newHarness(t)) })
			}
		})
	}
}

type namedTest struct {
	name string
	fn   func(*testing.T, Harness)
}

// ---------------------------------------------------------------------------
// helpers

var ctx = context.Background()

func enqueue(t *testing.T, h Harness, j queue.NewJob) string {
	t.Helper()
	id, created, err := h.Queue.Enqueue(ctx, j)
	if err != nil {
		t.Fatalf("enqueue %+v: %v", j, err)
	}
	if !created {
		t.Fatalf("enqueue %+v: expected a new job", j)
	}
	return id
}

func lease(t *testing.T, h Harness, kinds []string, n int) []queue.Job {
	t.Helper()
	jobs, err := h.Queue.Lease(ctx, "worker", kinds, n, leaseFor)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	return jobs
}

func leaseOne(t *testing.T, h Harness) queue.Job {
	t.Helper()
	jobs := lease(t, h, nil, 1)
	if len(jobs) != 1 {
		t.Fatalf("expected one job to lease, got %d", len(jobs))
	}
	return jobs[0]
}

func wantNothingToLease(t *testing.T, h Harness) {
	t.Helper()
	if jobs := lease(t, h, nil, 100); len(jobs) != 0 {
		t.Fatalf("expected nothing to lease, got %d job(s): %+v", len(jobs), jobs)
	}
}

func info(t *testing.T, h Harness, id string) queue.JobInfo {
	t.Helper()
	ji, err := h.Admin.Get(ctx, id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	return ji
}

func wantState(t *testing.T, h Harness, id, state string) queue.JobInfo {
	t.Helper()
	ji := info(t, h, id)
	if ji.State != state {
		t.Fatalf("job %s is %q, want %q (%+v)", id, ji.State, state, ji)
	}
	return ji
}

func wantLost(t *testing.T, err error, what string) {
	t.Helper()
	if !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("%s: error = %v, want ErrLeaseLost", what, err)
	}
}

func reap(t *testing.T, h Harness) int {
	t.Helper()
	n, err := h.Queue.ReapExpired(ctx)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	return n
}

func expire(h Harness) { h.Clock.Advance(leaseFor + time.Second) }

func makeDead(t *testing.T, h Harness, kind string) string {
	t.Helper()
	id := enqueue(t, h, queue.NewJob{Kind: kind})
	j := leaseOne(t, h)
	if j.ID != id {
		t.Fatalf("leased %s while making %s dead; the queue should hold nothing else", j.ID, id)
	}
	if err := h.Queue.Fail(ctx, j, errors.New("failed on purpose")); err != nil {
		t.Fatal(err)
	}
	wantState(t, h, id, queue.StateDead)
	return id
}

func makeDone(t *testing.T, h Harness, kind string) string {
	t.Helper()
	id := enqueue(t, h, queue.NewJob{Kind: kind})
	j := leaseOne(t, h)
	if err := h.Queue.Ack(ctx, j); err != nil {
		t.Fatal(err)
	}
	return id
}

// ---------------------------------------------------------------------------
// Enqueue

func testStoresFaithfully(t *testing.T, h Harness) {
	now := h.Clock.Now()
	id := enqueue(t, h, queue.NewJob{Kind: "route", Payload: []byte(`{"event":"e1"}`), MaxAttempts: 7})
	ji := wantState(t, h, id, queue.StateReady)
	if ji.Kind != "route" || string(ji.Payload) != `{"event":"e1"}` || ji.MaxAttempts != 7 ||
		ji.Attempts != 0 || !ji.CreatedAt.Equal(now) || !ji.RunAt.Equal(now) || !ji.FinishedAt.IsZero() || ji.LastError != "" {
		t.Fatalf("stored job differs from what was enqueued: %+v", ji)
	}

	// Distinct jobs get distinct IDs; a job without MaxAttempts gets a sensible default.
	id2 := enqueue(t, h, queue.NewJob{Kind: "route"})
	if id2 == id || id2 == "" {
		t.Fatalf("IDs must be unique and non-empty: %q %q", id, id2)
	}
	if ji := info(t, h, id2); ji.MaxAttempts < 1 {
		t.Fatalf("default MaxAttempts = %d, want at least 1", ji.MaxAttempts)
	}
}

func testArbitraryContent(t *testing.T, h Harness) {
	allBytes := make([]byte, 256)
	for i := range allBytes {
		allBytes[i] = byte(i)
	}
	big := bytes.Repeat([]byte("0123456789abcdef"), 16*1024) // 256 KiB
	nasty := `'; DROP TABLE jobs; -- "quoted" \ back\slash % _ ? $1 ` + "\n\t\x00 日本語 🚨"

	payloads := map[string][]byte{
		"nil": nil, "empty": {}, "single NUL": {0}, "high bytes": {0xff, 0xfe, 0x80}, "all byte values": allBytes,
		"large": big, "sql-looking text": []byte(nasty), "json": []byte(`{"a":[1,2,{"b":null}]}`),
	}
	ids := map[string]string{}
	for name, p := range payloads {
		ids[name] = enqueue(t, h, queue.NewJob{Kind: nasty, Payload: p, DedupeKey: name + nasty})
	}
	for name, p := range payloads {
		ji := info(t, h, ids[name])
		if !bytes.Equal(ji.Payload, p) {
			t.Errorf("payload %q changed in storage (len %d -> %d)", name, len(p), len(ji.Payload))
		}
		if ji.Kind != nasty {
			t.Errorf("kind %q was altered to %q", nasty, ji.Kind)
		}
	}
	leased := lease(t, h, []string{nasty}, 100)
	if len(leased) != len(payloads) {
		t.Fatalf("leased %d of %d jobs", len(leased), len(payloads))
	}
	for _, j := range leased {
		for name, id := range ids {
			if j.ID == id && !bytes.Equal(j.Payload, payloads[name]) {
				t.Errorf("leased payload %q differs from what was enqueued", name)
			}
		}
	}
}

func testRejectsInvalid(t *testing.T, h Harness) {
	if _, _, err := h.Queue.Enqueue(ctx, queue.NewJob{}); err == nil {
		t.Error("a job without a kind must be rejected")
	}
	if _, _, err := h.Queue.Enqueue(ctx, queue.NewJob{Kind: "k", MaxAttempts: -1}); err == nil {
		t.Error("negative MaxAttempts must be rejected")
	}
	if _, err := h.Queue.Lease(ctx, "w", nil, 1, 0); err == nil {
		t.Error("a zero lease duration must be rejected")
	}
	if _, err := h.Queue.Lease(ctx, "w", nil, 1, -time.Second); err == nil {
		t.Error("a negative lease duration must be rejected")
	}
	wantNothingToLease(t, h) // and rejected jobs left nothing behind
}

func testRunAt(t *testing.T, h Harness) {
	now := h.Clock.Now()
	enqueue(t, h, queue.NewJob{Kind: "k", RunAt: now.Add(time.Minute)})
	wantNothingToLease(t, h)
	h.Clock.Advance(time.Minute - time.Millisecond)
	wantNothingToLease(t, h) // not due yet, by the thinnest margin
	h.Clock.Advance(time.Millisecond)
	if got := lease(t, h, nil, 10); len(got) != 1 { // due exactly now
		t.Fatalf("a job is due at its RunAt: leased %d", len(got))
	}

	// A RunAt in the past is simply due, and nothing else is affected by it.
	enqueue(t, h, queue.NewJob{Kind: "k", RunAt: now.Add(-24 * time.Hour)})
	if got := lease(t, h, nil, 10); len(got) != 1 {
		t.Fatalf("a job scheduled in the past should run immediately, leased %d", len(got))
	}
}

func testDedupe(t *testing.T, h Harness) {
	id1, created, err := h.Queue.Enqueue(ctx, queue.NewJob{Kind: "k1", DedupeKey: "x"})
	if err != nil || !created {
		t.Fatalf("first enqueue: created=%v err=%v", created, err)
	}
	for i := 0; i < 3; i++ {
		id, created, err := h.Queue.Enqueue(ctx, queue.NewJob{Kind: "k1", DedupeKey: "x", Payload: []byte("different")})
		if err != nil || created || id != id1 {
			t.Fatalf("repeat %d: id=%s created=%v err=%v; want the original job, nothing new", i, id, created, err)
		}
	}
	if ji := info(t, h, id1); len(ji.Payload) != 0 {
		t.Fatalf("a duplicate enqueue must not modify the original job (payload is now %q)", ji.Payload)
	}

	id3, created, err := h.Queue.Enqueue(ctx, queue.NewJob{Kind: "k2", DedupeKey: "x"})
	if err != nil || !created || id3 == id1 {
		t.Fatalf("the same key under another kind is a different job: id=%s created=%v err=%v", id3, created, err)
	}
	enqueue(t, h, queue.NewJob{Kind: "k1"}) // no key: never deduplicated
	enqueue(t, h, queue.NewJob{Kind: "k1"})
	if got := lease(t, h, []string{"k1"}, 100); len(got) != 3 {
		t.Fatalf("expected 1 keyed + 2 unkeyed k1 jobs, leased %d", len(got))
	}

	// The key stays reserved through completion and dead-lettering...
	for _, j := range lease(t, h, []string{"k2"}, 10) {
		if err := h.Queue.Ack(ctx, j); err != nil {
			t.Fatal(err)
		}
	}
	if _, created, _ := h.Queue.Enqueue(ctx, queue.NewJob{Kind: "k2", DedupeKey: "x"}); created {
		t.Fatal("a finished job's key must stay reserved until it is purged")
	}
	// ...and is released by purging.
	h.Clock.Advance(time.Hour)
	if n, err := h.Admin.Purge(ctx, queue.StateDone, h.Clock.Now()); err != nil || n < 1 {
		t.Fatalf("purge: %d %v", n, err)
	}
	if _, created, err := h.Queue.Enqueue(ctx, queue.NewJob{Kind: "k2", DedupeKey: "x"}); err != nil || !created {
		t.Fatalf("after purging, the key should be reusable: created=%v err=%v", created, err)
	}
}

func testDedupeConcurrent(t *testing.T, h Harness) {
	const goroutines = 24
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		ids     = map[string]int{}
		created int
	)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, c, err := h.Queue.Enqueue(ctx, queue.NewJob{Kind: "k", DedupeKey: "same"})
			if err != nil {
				t.Errorf("enqueue: %v", err)
				return
			}
			mu.Lock()
			ids[id]++
			if c {
				created++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if created != 1 || len(ids) != 1 {
		t.Fatalf("created=%d distinct ids=%d among %d racing enqueues; exactly one job must win", created, len(ids), goroutines)
	}
	if got := lease(t, h, nil, 100); len(got) != 1 {
		t.Fatalf("%d jobs exist, want 1", len(got))
	}
}

func testTransactionalEnqueue(t *testing.T, h Harness) {
	rollback := errors.New("rollback")
	var inTx string
	err := h.InTx(ctx, func(c context.Context) error {
		id, created, err := h.Queue.Enqueue(c, queue.NewJob{Kind: "k", DedupeKey: "d"})
		if err != nil || !created {
			t.Errorf("enqueue inside tx: created=%v err=%v", created, err)
		}
		inTx = id
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatalf("err = %v", err)
	}
	wantNothingToLease(t, h)
	if _, err := h.Admin.Get(ctx, inTx); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("a rolled-back job must not exist (err=%v)", err)
	}

	// Rolling back also releases the dedupe key; committing persists the job.
	var committed string
	err = h.InTx(ctx, func(c context.Context) error {
		id, created, err := h.Queue.Enqueue(c, queue.NewJob{Kind: "k", DedupeKey: "d"})
		if err == nil && !created {
			err = errors.New("the rolled-back transaction leaked its dedupe key")
		}
		committed = id
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := lease(t, h, nil, 10); len(got) != 1 || got[0].ID != committed {
		t.Fatalf("the committed job should be leasable: %+v", got)
	}
}

// ---------------------------------------------------------------------------
// Lease

func testLeaseEligibility(t *testing.T, h Harness) {
	wantNothingToLease(t, h) // empty queue: no error, no jobs

	ready := enqueue(t, h, queue.NewJob{Kind: "k"})
	future := enqueue(t, h, queue.NewJob{Kind: "k", RunAt: h.Clock.Now().Add(time.Hour)})
	leased := enqueue(t, h, queue.NewJob{Kind: "k", RunAt: h.Clock.Now().Add(-2 * time.Hour)})
	first := leaseOne(t, h)
	if first.ID != leased {
		t.Fatalf("expected the oldest due job first, got %s", first.ID)
	}
	makeDoneFrom(t, h, ready)

	// At this point: `leased` is held, `ready` is done, `future` is not due.
	wantNothingToLease(t, h)
	wantState(t, h, future, queue.StateReady)
	wantState(t, h, leased, queue.StateLeased)
	wantState(t, h, ready, queue.StateDone)

	makeDead(t, h, "k-dead")
	wantNothingToLease(t, h)
}

// makeDoneFrom leases and acks the (single) due job that is expected to be id.
func makeDoneFrom(t *testing.T, h Harness, id string) string {
	t.Helper()
	jobs := lease(t, h, nil, 1)
	if len(jobs) != 1 || jobs[0].ID != id {
		t.Fatalf("expected to lease %s, got %+v", id, jobs)
	}
	if err := h.Queue.Ack(ctx, jobs[0]); err != nil {
		t.Fatal(err)
	}
	return id
}

func testLeaseSelection(t *testing.T, h Harness) {
	base := h.Clock.Now()
	// Enqueue out of order: run_at, not insertion order, decides who goes first.
	order := []struct {
		kind    string
		secsAgo int
	}{{"a", 3}, {"b", 1}, {"a", 5}, {"b", 2}, {"c", 4}}
	ids := map[int]string{}
	for _, o := range order {
		ids[o.secsAgo] = enqueue(t, h, queue.NewJob{Kind: o.kind, RunAt: base.Add(-time.Duration(o.secsAgo) * time.Second)})
	}

	// limit and order
	got := lease(t, h, nil, 2)
	if len(got) != 2 || got[0].ID != ids[5] || got[1].ID != ids[4] {
		t.Fatalf("n=2 should return the two oldest, oldest first: %+v", got)
	}
	// kind filter: only matching kinds, still oldest first
	got = lease(t, h, []string{"b"}, 10)
	if len(got) != 2 || got[0].ID != ids[2] || got[1].ID != ids[1] {
		t.Fatalf("kinds=[b]: %+v", got)
	}
	// kinds that match nothing, or only prefixes/lookalikes of real kinds, match nothing
	for _, kinds := range [][]string{{"zzz"}, {"ab"}, {"A"}, {""}} {
		if got := lease(t, h, kinds, 10); len(got) != 0 {
			t.Fatalf("kinds=%q leased %+v", kinds, got)
		}
	}
	// n <= 0 leases nothing and changes nothing
	for _, n := range []int{0, -1} {
		if got := lease(t, h, nil, n); len(got) != 0 {
			t.Fatalf("n=%d leased %d jobs", n, len(got))
		}
	}
	if got := lease(t, h, []string{"a", "x"}, 10); len(got) != 1 || got[0].ID != ids[3] {
		t.Fatalf("kinds=[a x] should find the one remaining a-job: %+v", got)
	}
}

func testLeaseMetadata(t *testing.T, h Harness) {
	id := enqueue(t, h, queue.NewJob{Kind: "route", Payload: []byte("p"), MaxAttempts: 4})
	created := h.Clock.Now()
	h.Clock.Advance(5 * time.Second)

	j := leaseOne(t, h)
	if j.ID != id || j.Kind != "route" || string(j.Payload) != "p" || j.MaxAttempts != 4 || !j.CreatedAt.Equal(created) {
		t.Fatalf("leased job lost information: %+v", j)
	}
	if j.Attempt != 1 || j.LeaseToken < 1 {
		t.Fatalf("attempt=%d token=%d", j.Attempt, j.LeaseToken)
	}
	if want := h.Clock.Now().Add(leaseFor); !j.LeaseExpiresAt.Equal(want) {
		t.Fatalf("LeaseExpiresAt = %v, want now+lease = %v", j.LeaseExpiresAt, want)
	}
	ji := wantState(t, h, id, queue.StateLeased)
	if ji.Attempts != 1 {
		t.Fatalf("the admin view must agree about attempts: %+v", ji)
	}
	// A leased job is invisible to every other worker.
	wantNothingToLease(t, h)
}

func testTokensIncrease(t *testing.T, h Harness) {
	enqueue(t, h, queue.NewJob{Kind: "k", MaxAttempts: 50})
	var last int64
	for i := 1; i <= 8; i++ {
		j := leaseOne(t, h)
		if j.LeaseToken <= last {
			t.Fatalf("lease %d: token %d did not increase past %d", i, j.LeaseToken, last)
		}
		if j.Attempt != i {
			t.Fatalf("lease %d reported attempt %d", i, j.Attempt)
		}
		last = j.LeaseToken
		if i%2 == 0 {
			expire(h)
			reap(t, h)
		} else if err := h.Queue.Nack(ctx, j, h.Clock.Now(), errors.New("again")); err != nil {
			t.Fatal(err)
		}
	}
}

func testConcurrentLease(t *testing.T, h Harness) {
	const total, workers = 300, 40
	if err := h.InTx(ctx, func(c context.Context) error {
		for i := 0; i < total; i++ {
			if _, _, err := h.Queue.Enqueue(c, queue.NewJob{Kind: "k", Payload: []byte(fmt.Sprint(i))}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	var (
		mu   sync.Mutex
		seen = map[string]int{}
		wg   sync.WaitGroup
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				jobs, err := h.Queue.Lease(ctx, fmt.Sprintf("w%d", w), nil, 1+w%4, leaseFor)
				if err != nil {
					t.Errorf("worker %d: %v", w, err)
					return
				}
				if len(jobs) == 0 {
					return
				}
				for _, j := range jobs {
					mu.Lock()
					seen[j.ID]++
					mu.Unlock()
					if err := h.Queue.Ack(ctx, j); err != nil {
						t.Errorf("worker %d ack %s: %v", w, j.ID, err)
					}
				}
			}
		}(w)
	}
	wg.Wait()

	if len(seen) != total {
		t.Errorf("%d distinct jobs were leased, want %d (jobs were lost)", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("job %s was handed out %d times while its lease was valid", id, n)
		}
	}
}

func testDrain(t *testing.T, h Harness) {
	const total = 500
	base := h.Clock.Now()
	want := make([]string, total)
	for i := 0; i < total; i++ {
		// distinct run_at, enqueued in shuffled order
		want[i] = ""
	}
	perm := rand.New(rand.NewSource(7)).Perm(total)
	idAt := map[int]string{}
	for _, i := range perm {
		idAt[i] = enqueue(t, h, queue.NewJob{Kind: "k", RunAt: base.Add(-time.Duration(total-i) * time.Millisecond)})
	}
	var got []string
	for {
		jobs := lease(t, h, nil, 37) // an awkward batch size
		if len(jobs) == 0 {
			break
		}
		for _, j := range jobs {
			got = append(got, j.ID)
		}
	}
	if len(got) != total {
		t.Fatalf("drained %d of %d jobs", len(got), total)
	}
	for i := 0; i < total; i++ {
		if got[i] != idAt[i] {
			t.Fatalf("position %d: jobs must come out in run_at order regardless of enqueue order", i)
		}
	}
}

// ---------------------------------------------------------------------------
// Outcomes

func testAck(t *testing.T, h Harness) {
	id := enqueue(t, h, queue.NewJob{Kind: "k"})
	j := leaseOne(t, h)
	h.Clock.Advance(time.Second)
	if err := h.Queue.Ack(ctx, j); err != nil {
		t.Fatalf("ack: %v", err)
	}
	ji := wantState(t, h, id, queue.StateDone)
	if !ji.FinishedAt.Equal(h.Clock.Now()) || ji.Attempts != 1 {
		t.Fatalf("done job: %+v", ji)
	}
	wantNothingToLease(t, h)
	expire(h)
	if reap(t, h) != 0 {
		t.Fatal("a finished job must never be reclaimed")
	}
	wantState(t, h, id, queue.StateDone)
}

func testZombieRejected(t *testing.T, h Harness) {
	id := enqueue(t, h, queue.NewJob{Kind: "k", MaxAttempts: 10})
	zombie := leaseOne(t, h)
	expire(h)
	if reap(t, h) != 1 {
		t.Fatal("expected the stale lease to be reclaimed")
	}
	current := leaseOne(t, h)
	if current.ID != id || current.LeaseToken <= zombie.LeaseToken {
		t.Fatalf("new holder %+v must have a higher token than the zombie %+v", current, zombie)
	}

	// The zombie wakes up and tries every way of reporting. None may work, none may disturb the owner.
	wantLost(t, h.Queue.Ack(ctx, zombie), "zombie ack")
	wantLost(t, h.Queue.Nack(ctx, zombie, h.Clock.Now(), errors.New("zombie")), "zombie nack")
	wantLost(t, h.Queue.Fail(ctx, zombie, errors.New("zombie")), "zombie fail")
	_, err := h.Queue.Extend(ctx, zombie, leaseFor)
	wantLost(t, err, "zombie extend")
	ji := wantState(t, h, id, queue.StateLeased)
	if ji.LastError == "zombie" {
		t.Fatal("a rejected report must not leave its error text behind")
	}

	if err := h.Queue.Ack(ctx, current); err != nil {
		t.Fatalf("the current holder must still be able to finish: %v", err)
	}
	wantState(t, h, id, queue.StateDone)
}

func testWrongLease(t *testing.T, h Harness) {
	id := enqueue(t, h, queue.NewJob{Kind: "k"})
	j := leaseOne(t, h)

	for name, forged := range map[string]queue.Job{
		"token too high": withToken(j, j.LeaseToken+1),
		"token too low":  withToken(j, j.LeaseToken-1),
		"token zero":     withToken(j, 0),
		"unknown job":    withID(j, "no-such-job"),
		"empty id":       withID(j, ""),
	} {
		wantLost(t, h.Queue.Ack(ctx, forged), name+" ack")
		wantLost(t, h.Queue.Nack(ctx, forged, h.Clock.Now(), nil), name+" nack")
		wantLost(t, h.Queue.Fail(ctx, forged, nil), name+" fail")
		_, err := h.Queue.Extend(ctx, forged, leaseFor)
		wantLost(t, err, name+" extend")
	}
	wantState(t, h, id, queue.StateLeased) // none of it had any effect
	if err := h.Queue.Ack(ctx, j); err != nil {
		t.Fatal(err)
	}

	// Once a job has finished, its old lease is worthless in every way.
	wantLost(t, h.Queue.Ack(ctx, j), "second ack")
	wantLost(t, h.Queue.Nack(ctx, j, h.Clock.Now(), nil), "nack after ack")
	wantLost(t, h.Queue.Fail(ctx, j, nil), "fail after ack")
	_, err := h.Queue.Extend(ctx, j, leaseFor)
	wantLost(t, err, "extend after ack")
	wantState(t, h, id, queue.StateDone)
}

func withToken(j queue.Job, tok int64) queue.Job { j.LeaseToken = tok; return j }
func withID(j queue.Job, id string) queue.Job    { j.ID = id; return j }

func testNack(t *testing.T, h Harness) {
	id := enqueue(t, h, queue.NewJob{Kind: "k", MaxAttempts: 3})

	// attempt 1 of 3: retried at the requested time, and not before
	j := leaseOne(t, h)
	retryAt := h.Clock.Now().Add(10 * time.Second)
	if err := h.Queue.Nack(ctx, j, retryAt, errors.New("first")); err != nil {
		t.Fatal(err)
	}
	ji := wantState(t, h, id, queue.StateReady)
	if ji.LastError != "first" || !ji.RunAt.Equal(retryAt) || !ji.FinishedAt.IsZero() || ji.Attempts != 1 {
		t.Fatalf("after first nack: %+v", ji)
	}
	wantNothingToLease(t, h)
	h.Clock.Advance(10*time.Second - time.Millisecond)
	wantNothingToLease(t, h)
	h.Clock.Advance(time.Millisecond)

	// attempt 2 of 3: a retry time in the past means "right away"
	j = leaseOne(t, h)
	if j.Attempt != 2 {
		t.Fatalf("attempt = %d", j.Attempt)
	}
	if err := h.Queue.Nack(ctx, j, h.Clock.Now().Add(-time.Hour), errors.New("second")); err != nil {
		t.Fatal(err)
	}
	wantState(t, h, id, queue.StateReady)
	j = leaseOne(t, h)

	// attempt 3 of 3: out of attempts, so the QUEUE (not the caller) dead-letters it
	if j.Attempt != 3 {
		t.Fatalf("attempt = %d", j.Attempt)
	}
	if err := h.Queue.Nack(ctx, j, h.Clock.Now().Add(time.Second), errors.New("third")); err != nil {
		t.Fatal(err)
	}
	ji = wantState(t, h, id, queue.StateDead)
	if ji.LastError != "third" || ji.FinishedAt.IsZero() || ji.Attempts != 3 {
		t.Fatalf("after final nack: %+v", ji)
	}
	h.Clock.Advance(24 * time.Hour)
	wantNothingToLease(t, h) // dead jobs stay dead
}

func testFail(t *testing.T, h Harness) {
	id := enqueue(t, h, queue.NewJob{Kind: "k", MaxAttempts: 9})
	j := leaseOne(t, h)
	if err := h.Queue.Fail(ctx, j, errors.New("HTTP 404")); err != nil {
		t.Fatal(err)
	}
	ji := wantState(t, h, id, queue.StateDead)
	if ji.Attempts != 1 || ji.LastError != "HTTP 404" || ji.FinishedAt.IsZero() {
		t.Fatalf("failed job: %+v (attempts remaining are irrelevant for a permanent failure)", ji)
	}
	wantNothingToLease(t, h)
	dead, err := h.Admin.ListDead(ctx, 10, "")
	if err != nil || len(dead) != 1 || dead[0].ID != id {
		t.Fatalf("it should appear among the dead letters: %+v %v", dead, err)
	}
}

func testErrorText(t *testing.T, h Harness) {
	huge := strings.Repeat("x", 100_000)
	id := enqueue(t, h, queue.NewJob{Kind: "k", MaxAttempts: 9})
	j := leaseOne(t, h)
	if err := h.Queue.Nack(ctx, j, h.Clock.Now(), errors.New(huge)); err != nil {
		t.Fatalf("a huge error message must not make Nack fail: %v", err)
	}
	ji := info(t, h, id)
	if len(ji.LastError) == 0 || len(ji.LastError) >= len(huge) || !strings.HasPrefix(ji.LastError, "xxxx") {
		t.Fatalf("stored error is %d bytes; want a non-empty truncation of the original", len(ji.LastError))
	}

	// Awkward text is stored exactly; a nil cause is allowed and clears nothing odd.
	for _, msg := range []string{"it's \"quoted\"; DROP TABLE jobs;--", "日本語 🚨 \n multi\nline", "   "} {
		j = leaseOne(t, h)
		if err := h.Queue.Nack(ctx, j, h.Clock.Now(), errors.New(msg)); err != nil {
			t.Fatal(err)
		}
		if got := info(t, h, id).LastError; got != msg {
			t.Fatalf("error text %q came back as %q", msg, got)
		}
	}
	j = leaseOne(t, h)
	if err := h.Queue.Nack(ctx, j, h.Clock.Now(), nil); err != nil {
		t.Fatalf("a nil cause must be accepted: %v", err)
	}
}

func testExtend(t *testing.T, h Harness) {
	id := enqueue(t, h, queue.NewJob{Kind: "k"})
	j := leaseOne(t, h)

	h.Clock.Advance(20 * time.Second)
	expires, err := h.Queue.Extend(ctx, j, leaseFor)
	if err != nil {
		t.Fatalf("extend: %v", err)
	}
	if want := h.Clock.Now().Add(leaseFor); !expires.Equal(want) {
		t.Fatalf("new expiry %v, want %v", expires, want)
	}

	h.Clock.Advance(20 * time.Second) // 40s in: past the ORIGINAL expiry, inside the extended one
	if reap(t, h) != 0 {
		t.Fatal("an extended lease must not be reclaimed")
	}
	wantState(t, h, id, queue.StateLeased)

	if _, err := h.Queue.Extend(ctx, j, 0); err == nil {
		t.Fatal("extending by zero must be rejected")
	}
	h.Clock.Advance(leaseFor) // now genuinely expired
	if reap(t, h) != 1 {
		t.Fatal("expected the lapsed lease to be reclaimed")
	}
	_, err = h.Queue.Extend(ctx, j, leaseFor)
	wantLost(t, err, "extend after reclaim")
}

// If the lease has lapsed but nobody has reclaimed the job, the worker still
// owns it: finishing the work is better than throwing it away.
func testLateReports(t *testing.T, h Harness) {
	variants := []struct {
		name   string
		report func(queue.Job) error
		state  string
	}{
		{"ack", func(j queue.Job) error { return h.Queue.Ack(ctx, j) }, queue.StateDone},
		{"nack", func(j queue.Job) error { return h.Queue.Nack(ctx, j, h.Clock.Now(), errors.New("late")) }, queue.StateReady},
		{"fail", func(j queue.Job) error { return h.Queue.Fail(ctx, j, errors.New("late")) }, queue.StateDead},
		{"extend", func(j queue.Job) error { _, err := h.Queue.Extend(ctx, j, leaseFor); return err }, queue.StateLeased},
	}
	for _, v := range variants {
		kind := "k-" + v.name
		id := enqueue(t, h, queue.NewJob{Kind: kind, MaxAttempts: 5})
		j := lease(t, h, []string{kind}, 1)[0]
		h.Clock.Advance(leaseFor + time.Hour) // lapsed, but no reaper has run
		if err := v.report(j); err != nil {
			t.Errorf("late %s of a job nobody has reclaimed should be accepted: %v", v.name, err)
		}
		wantState(t, h, id, v.state)
	}
}

// ---------------------------------------------------------------------------
// Recovery

func testReap(t *testing.T, h Harness) {
	id := enqueue(t, h, queue.NewJob{Kind: "k", MaxAttempts: 3})
	first := leaseOne(t, h)

	if reap(t, h) != 0 {
		t.Fatal("a live lease must not be reclaimed")
	}
	h.Clock.Advance(leaseFor - time.Millisecond)
	if reap(t, h) != 0 {
		t.Fatal("a lease one millisecond from expiry must not be reclaimed")
	}
	h.Clock.Advance(time.Millisecond) // exactly at expiry
	if n := reap(t, h); n != 1 {
		t.Fatalf("a lease at its expiry time is expired: reaped %d", n)
	}
	if reap(t, h) != 0 {
		t.Fatal("reaping twice must not touch anything the second time")
	}

	ji := wantState(t, h, id, queue.StateReady)
	if ji.Attempts != 1 || !strings.Contains(strings.ToLower(ji.LastError), "expired") {
		t.Fatalf("reclaimed job: %+v", ji)
	}
	second := leaseOne(t, h) // available again immediately, to anyone
	if second.ID != id || second.Attempt != 2 || second.LeaseToken <= first.LeaseToken {
		t.Fatalf("re-lease: %+v after %+v", second, first)
	}
}

func testPoisonPill(t *testing.T, h Harness) {
	// A job that crashes its worker every time never calls Nack. It must still
	// run out of attempts, or it would loop forever.
	id := enqueue(t, h, queue.NewJob{Kind: "k", MaxAttempts: 3})
	for attempt := 1; attempt <= 3; attempt++ {
		j := leaseOne(t, h)
		if j.Attempt != attempt {
			t.Fatalf("attempt = %d, want %d", j.Attempt, attempt)
		}
		expire(h) // the worker died
		if reap(t, h) != 1 {
			t.Fatalf("attempt %d: nothing reclaimed", attempt)
		}
		if attempt < 3 {
			wantState(t, h, id, queue.StateReady)
		}
	}
	ji := wantState(t, h, id, queue.StateDead)
	if ji.Attempts != 3 || ji.FinishedAt.IsZero() || !strings.Contains(strings.ToLower(ji.LastError), "expired") {
		t.Fatalf("dead job: %+v", ji)
	}
	wantNothingToLease(t, h)
}

func testReapSelective(t *testing.T, h Harness) {
	ready := enqueue(t, h, queue.NewJob{Kind: "ready", RunAt: h.Clock.Now().Add(time.Hour)})
	done := makeDone(t, h, "done")
	dead := makeDead(t, h, "dead")
	oldLease := enqueue(t, h, queue.NewJob{Kind: "old"})
	old := lease(t, h, []string{"old"}, 1)[0]

	h.Clock.Advance(20 * time.Second)
	newLease := enqueue(t, h, queue.NewJob{Kind: "new"})
	fresh := lease(t, h, []string{"new"}, 1)[0]
	h.Clock.Advance(15 * time.Second) // old lease is 35s old (expired); new one 15s (alive)

	if n := reap(t, h); n != 1 {
		t.Fatalf("reaped %d, want exactly the one expired lease", n)
	}
	wantState(t, h, oldLease, queue.StateReady)
	wantState(t, h, newLease, queue.StateLeased)
	wantState(t, h, ready, queue.StateReady)
	wantState(t, h, done, queue.StateDone)
	wantState(t, h, dead, queue.StateDead)
	if err := h.Queue.Ack(ctx, fresh); err != nil {
		t.Fatalf("the live lease must be untouched: %v", err)
	}
	wantLost(t, h.Queue.Ack(ctx, old), "ack of the reclaimed lease")
}

// ---------------------------------------------------------------------------
// Admin

func testGet(t *testing.T, h Harness) {
	if _, err := h.Admin.Get(ctx, "no-such-job"); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if _, err := h.Admin.Get(ctx, ""); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("empty id: err = %v, want ErrNotFound", err)
	}
	id := enqueue(t, h, queue.NewJob{Kind: "k"})
	if info(t, h, id).ID != id {
		t.Fatal("Get returned a different job")
	}
}

func testListDead(t *testing.T, h Harness) {
	if got, err := h.Admin.ListDead(ctx, 10, ""); err != nil || len(got) != 0 {
		t.Fatalf("an empty queue has no dead letters: %v %v", got, err)
	}

	const dead = 23
	want := map[string]bool{}
	for i := 0; i < dead; i++ {
		want[makeDead(t, h, "k")] = true
	}
	enqueue(t, h, queue.NewJob{Kind: "ready-job"}) // never listed
	makeDone(t, h, "done-job")

	for _, pageSize := range []int{1, 2, 5, 7, 23, 24, 100} {
		var got []string
		after := ""
		for pages := 0; pages < 100; pages++ {
			page, err := h.Admin.ListDead(ctx, pageSize, after)
			if err != nil {
				t.Fatal(err)
			}
			if len(page) == 0 {
				break
			}
			if len(page) > pageSize {
				t.Fatalf("page size %d returned %d rows", pageSize, len(page))
			}
			for _, ji := range page {
				if ji.State != queue.StateDead {
					t.Fatalf("a %s job was listed as dead", ji.State)
				}
				got = append(got, ji.ID)
			}
			after = page[len(page)-1].ID
		}
		if len(got) != dead {
			t.Fatalf("page size %d: listed %d dead jobs, want %d", pageSize, len(got), dead)
		}
		if !sort.StringsAreSorted(got) {
			t.Fatalf("page size %d: listing is not in a stable order", pageSize)
		}
		seen := map[string]bool{}
		for _, id := range got {
			if seen[id] || !want[id] {
				t.Fatalf("page size %d: job %s repeated or unexpected", pageSize, id)
			}
			seen[id] = true
		}
	}

	if page, err := h.Admin.ListDead(ctx, 0, ""); err != nil || len(page) == 0 || len(page) > 500 {
		t.Fatalf("limit 0 should mean a sensible default page: %d rows, err %v", len(page), err)
	}
	if page, err := h.Admin.ListDead(ctx, 1_000_000, ""); err != nil || len(page) != dead {
		t.Fatalf("an enormous limit must be clamped, not rejected: %d rows, err %v", len(page), err)
	}
	if page, err := h.Admin.ListDead(ctx, 10, "~~~~after-everything"); err != nil || len(page) != 0 {
		t.Fatalf("a cursor beyond the end returns nothing: %v %v", page, err)
	}
}

func testRequeue(t *testing.T, h Harness) {
	id := enqueue(t, h, queue.NewJob{Kind: "k", MaxAttempts: 2})
	for i := 0; i < 2; i++ {
		j := leaseOne(t, h)
		if err := h.Queue.Nack(ctx, j, h.Clock.Now(), errors.New("nope")); err != nil {
			t.Fatal(err)
		}
	}
	wantState(t, h, id, queue.StateDead)

	h.Clock.Advance(time.Hour)
	if err := h.Admin.Requeue(ctx, id); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	ji := wantState(t, h, id, queue.StateReady)
	if ji.Attempts != 0 || !ji.FinishedAt.IsZero() || !ji.RunAt.Equal(h.Clock.Now()) {
		t.Fatalf("a requeued job gets a fresh start, due now: %+v", ji)
	}
	if err := h.Admin.Requeue(ctx, id); !errors.Is(err, queue.ErrNotDead) {
		t.Fatalf("requeueing twice: err = %v, want ErrNotDead", err)
	}

	// It gets its full allowance of attempts again, then dies again.
	for want := 1; want <= 2; want++ {
		j := leaseOne(t, h)
		if j.Attempt != want {
			t.Fatalf("attempt after requeue = %d, want %d", j.Attempt, want)
		}
		if err := h.Queue.Nack(ctx, j, h.Clock.Now(), errors.New("still nope")); err != nil {
			t.Fatal(err)
		}
	}
	wantState(t, h, id, queue.StateDead)

	// Only dead jobs can be requeued.
	ready := enqueue(t, h, queue.NewJob{Kind: "r", RunAt: h.Clock.Now().Add(time.Hour)})
	if err := h.Admin.Requeue(ctx, ready); !errors.Is(err, queue.ErrNotDead) {
		t.Errorf("ready job: %v", err)
	}
	doneID := makeDone(t, h, "d")
	if err := h.Admin.Requeue(ctx, doneID); !errors.Is(err, queue.ErrNotDead) {
		t.Errorf("done job: %v", err)
	}
	enqueue(t, h, queue.NewJob{Kind: "l"})
	held := lease(t, h, []string{"l"}, 1)[0]
	if err := h.Admin.Requeue(ctx, held.ID); !errors.Is(err, queue.ErrNotDead) {
		t.Errorf("leased job: %v", err)
	}
	wantState(t, h, held.ID, queue.StateLeased) // and the failed attempts changed nothing
	if err := h.Admin.Requeue(ctx, "no-such-job"); !errors.Is(err, queue.ErrNotFound) {
		t.Errorf("unknown job: %v", err)
	}
}

func testPurge(t *testing.T, h Harness) {
	oldDone, oldDead := makeDone(t, h, "a"), makeDead(t, h, "b")
	h.Clock.Advance(48 * time.Hour)
	newDone, newDead := makeDone(t, h, "c"), makeDead(t, h, "d")
	ready := enqueue(t, h, queue.NewJob{Kind: "r", RunAt: h.Clock.Now().Add(time.Hour)})
	enqueue(t, h, queue.NewJob{Kind: "l"})
	held := lease(t, h, []string{"l"}, 1)[0]
	cutoff := h.Clock.Now().Add(-24 * time.Hour)

	for _, state := range []string{queue.StateReady, queue.StateLeased, "", "everything", "DONE"} {
		if n, err := h.Admin.Purge(ctx, state, h.Clock.Now().Add(time.Hour)); err == nil || n != 0 {
			t.Errorf("purging state %q must be rejected (n=%d err=%v): only finished jobs may be deleted", state, n, err)
		}
	}
	wantState(t, h, ready, queue.StateReady)
	wantState(t, h, held.ID, queue.StateLeased)

	n, err := h.Admin.Purge(ctx, queue.StateDone, cutoff)
	if err != nil || n != 1 {
		t.Fatalf("purge done: n=%d err=%v", n, err)
	}
	if _, err := h.Admin.Get(ctx, oldDone); !errors.Is(err, queue.ErrNotFound) {
		t.Errorf("old done job should be gone: %v", err)
	}
	wantState(t, h, oldDead, queue.StateDead) // purging done jobs must never touch dead letters
	wantState(t, h, newDone, queue.StateDone)

	if n, err := h.Admin.Purge(ctx, queue.StateDone, cutoff); err != nil || n != 0 {
		t.Errorf("purging again finds nothing: n=%d err=%v", n, err)
	}
	if n, err = h.Admin.Purge(ctx, queue.StateDead, cutoff); err != nil || n != 1 {
		t.Fatalf("purge dead: n=%d err=%v", n, err)
	}
	wantState(t, h, newDead, queue.StateDead)

	// "Older than" is strict: a job finished exactly at the cutoff stays.
	finishedAt := info(t, h, newDone).FinishedAt
	if n, _ := h.Admin.Purge(ctx, queue.StateDone, finishedAt); n != 0 {
		t.Error("a job finished exactly at the cutoff must not be purged")
	}
	if n, _ := h.Admin.Purge(ctx, queue.StateDone, finishedAt.Add(time.Millisecond)); n != 1 {
		t.Error("a job finished before the cutoff must be purged")
	}
}
