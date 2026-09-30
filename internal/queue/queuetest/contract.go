// Package queuetest is the contract suite every queue.Queue / queue.Admin
// implementation must pass. Point it at a new backend (Postgres, ...) with a
// Factory and Run:
//
//	func TestPostgresContract(t *testing.T) { queuetest.Run(t, newPostgresHarness) }
package queuetest

import (
	"context"
	"errors"
	"fmt"
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
	// InTx runs fn inside a transaction (commit on nil, rollback on error).
	// The ctx given to fn is what must be passed to Enqueue to join it.
	InTx func(ctx context.Context, fn func(ctx context.Context) error) error
}

// Factory builds a fresh Harness for each test (including cleanup via t.Cleanup).
type Factory func(t *testing.T) Harness

// Run executes the whole suite as subtests.
func Run(t *testing.T, newHarness Factory) {
	t.Helper()
	tests := []struct {
		name string
		fn   func(*testing.T, Harness)
	}{
		{"EnqueueLeaseAck", testEnqueueLeaseAck},
		{"EmptyPayloadRoundTrip", testEmptyPayload},
		{"EnqueueValidation", testEnqueueValidation},
		{"FutureRunAtNotLeasedUntilDue", testFutureRunAt},
		{"KindsFilter", testKindsFilter},
		{"LeaseOrderAndLimit", testLeaseOrderAndLimit},
		{"ConcurrentLeaseNoDuplicatesNoLoss", testConcurrentLease},
		{"CrashRecoveryAndFencing", testCrashRecovery},
		{"PoisonPillDeadLettersWithoutNack", testPoisonPill},
		{"NackRetriesThenDeadLetters", testNack},
		{"FailDeadLettersImmediately", testFail},
		{"ExtendKeepsLeaseAlive", testExtend},
		{"AckAfterExpiryBeforeReapSucceeds", testAckAfterExpiry},
		{"DoubleAckFails", testDoubleAck},
		{"DedupeScopedPerKind", testDedupe},
		{"TransactionalEnqueue", testTransactionalEnqueue},
		{"RequeueDeadJob", testRequeue},
		{"ListDeadPagination", testListDead},
		{"PurgeRetention", testPurge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, newHarness(t)) })
	}
}

// ---------------------------------------------------------------------------
// helpers

var ctx = context.Background()

func enqueue(t *testing.T, h Harness, j queue.NewJob) string {
	t.Helper()
	id, created, err := h.Queue.Enqueue(ctx, j)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if !created {
		t.Fatalf("enqueue %+v: expected created=true", j)
	}
	return id
}

func lease(t *testing.T, h Harness, kinds []string, n int) []queue.Job {
	t.Helper()
	jobs, err := h.Queue.Lease(ctx, "test-worker", kinds, n, leaseFor)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	return jobs
}

func leaseOne(t *testing.T, h Harness) queue.Job {
	t.Helper()
	jobs := lease(t, h, nil, 1)
	if len(jobs) != 1 {
		t.Fatalf("expected to lease exactly 1 job, got %d", len(jobs))
	}
	return jobs[0]
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
		t.Fatalf("job %s: state = %q, want %q (%+v)", id, ji.State, state, ji)
	}
	return ji
}

func wantErr(t *testing.T, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Fatalf("error = %v, want %v", got, want)
	}
}

func wantLeased(t *testing.T, h Harness, n int) {
	t.Helper()
	if got := len(lease(t, h, nil, 10)); got != n {
		t.Fatalf("leased %d jobs, want %d", got, n)
	}
}

func reap(t *testing.T, h Harness, want int) {
	t.Helper()
	n, err := h.Queue.ReapExpired(ctx)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if n != want {
		t.Fatalf("reaped %d jobs, want %d", n, want)
	}
}

func makeDead(t *testing.T, h Harness, kind string) string {
	t.Helper()
	id := enqueue(t, h, queue.NewJob{Kind: kind})
	j := leaseOne(t, h)
	if err := h.Queue.Fail(ctx, j, errors.New("boom")); err != nil {
		t.Fatalf("fail: %v", err)
	}
	wantState(t, h, id, queue.StateDead)
	return id
}

// ---------------------------------------------------------------------------
// tests

func testEnqueueLeaseAck(t *testing.T, h Harness) {
	id := enqueue(t, h, queue.NewJob{Kind: "route", Payload: []byte(`{"event":"e1"}`)})
	ji := wantState(t, h, id, queue.StateReady)
	if ji.Attempts != 0 || ji.MaxAttempts <= 0 || !ji.CreatedAt.Equal(h.Clock.Now()) {
		t.Fatalf("unexpected fresh job: %+v", ji)
	}

	j := leaseOne(t, h)
	if j.ID != id || j.Kind != "route" || string(j.Payload) != `{"event":"e1"}` {
		t.Fatalf("leased wrong job: %+v", j)
	}
	if j.Attempt != 1 || j.LeaseToken <= 0 || j.MaxAttempts != ji.MaxAttempts {
		t.Fatalf("bad lease metadata: %+v", j)
	}
	if want := h.Clock.Now().Add(leaseFor); !j.LeaseExpiresAt.Equal(want) {
		t.Fatalf("LeaseExpiresAt = %v, want %v", j.LeaseExpiresAt, want)
	}
	wantState(t, h, id, queue.StateLeased)
	wantLeased(t, h, 0) // a leased job is invisible to other workers

	h.Clock.Advance(time.Second)
	if err := h.Queue.Ack(ctx, j); err != nil {
		t.Fatalf("ack: %v", err)
	}
	ji = wantState(t, h, id, queue.StateDone)
	if !ji.FinishedAt.Equal(h.Clock.Now()) {
		t.Fatalf("FinishedAt = %v, want %v", ji.FinishedAt, h.Clock.Now())
	}
	wantLeased(t, h, 0)
}

func testEmptyPayload(t *testing.T, h Harness) {
	id := enqueue(t, h, queue.NewJob{Kind: "k"}) // nil payload
	j := leaseOne(t, h)
	if j.ID != id || len(j.Payload) != 0 {
		t.Fatalf("got %+v, want empty payload", j)
	}
}

func testEnqueueValidation(t *testing.T, h Harness) {
	if _, _, err := h.Queue.Enqueue(ctx, queue.NewJob{}); err == nil {
		t.Error("enqueue with empty kind should fail")
	}
	if _, _, err := h.Queue.Enqueue(ctx, queue.NewJob{Kind: "k", MaxAttempts: -1}); err == nil {
		t.Error("enqueue with negative MaxAttempts should fail")
	}
	if _, err := h.Queue.Lease(ctx, "w", nil, 1, 0); err == nil {
		t.Error("lease with zero duration should fail")
	}
	jobs, err := h.Queue.Lease(ctx, "w", nil, 0, leaseFor)
	if err != nil || len(jobs) != 0 {
		t.Errorf("lease n=0: jobs=%d err=%v, want none", len(jobs), err)
	}
}

func testFutureRunAt(t *testing.T, h Harness) {
	enqueue(t, h, queue.NewJob{Kind: "k", RunAt: h.Clock.Now().Add(time.Minute)})
	wantLeased(t, h, 0)
	h.Clock.Advance(59 * time.Second)
	wantLeased(t, h, 0)
	h.Clock.Advance(time.Second)
	wantLeased(t, h, 1)
}

func testKindsFilter(t *testing.T, h Harness) {
	enqueue(t, h, queue.NewJob{Kind: "a"})
	enqueue(t, h, queue.NewJob{Kind: "b"})
	enqueue(t, h, queue.NewJob{Kind: "c"})

	got := lease(t, h, []string{"b"}, 10)
	if len(got) != 1 || got[0].Kind != "b" {
		t.Fatalf("lease [b] = %+v", got)
	}
	got = lease(t, h, []string{"a", "x"}, 10)
	if len(got) != 1 || got[0].Kind != "a" {
		t.Fatalf("lease [a x] = %+v", got)
	}
	got = lease(t, h, nil, 10) // empty kinds = any kind
	if len(got) != 1 || got[0].Kind != "c" {
		t.Fatalf("lease [] = %+v", got)
	}
}

func testLeaseOrderAndLimit(t *testing.T, h Harness) {
	base := h.Clock.Now()
	// Enqueue out of order; RunAt decides the order, not insertion order.
	offsets := []int{3, 1, 5, 2, 4} // seconds in the past
	ids := map[int]string{}
	for _, off := range offsets {
		ids[off] = enqueue(t, h, queue.NewJob{Kind: "k", RunAt: base.Add(-time.Duration(off) * time.Second)})
	}
	got := lease(t, h, nil, 3)
	want := []string{ids[5], ids[4], ids[3]} // oldest run_at first
	if len(got) != 3 {
		t.Fatalf("leased %d, want 3", len(got))
	}
	for i := range want {
		if got[i].ID != want[i] {
			t.Fatalf("position %d: got %s, want %s", i, got[i].ID, want[i])
		}
	}
	rest := lease(t, h, nil, 10)
	if len(rest) != 2 || rest[0].ID != ids[2] || rest[1].ID != ids[1] {
		t.Fatalf("remaining leased in wrong order: %+v", rest)
	}
}

func testConcurrentLease(t *testing.T, h Harness) {
	const total, workers = 200, 50

	err := h.InTx(ctx, func(ctx context.Context) error {
		for i := 0; i < total; i++ {
			if _, _, err := h.Queue.Enqueue(ctx, queue.NewJob{Kind: "k", Payload: []byte(fmt.Sprint(i))}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
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
			owner := fmt.Sprintf("w%d", w)
			for {
				jobs, err := h.Queue.Lease(ctx, owner, nil, 1+w%3, leaseFor)
				if err != nil {
					t.Errorf("worker %d lease: %v", w, err)
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
		t.Errorf("leased %d distinct jobs, want %d (jobs lost)", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("job %s leased %d times, want exactly once", id, n)
		}
	}
}

func testCrashRecovery(t *testing.T, h Harness) {
	id := enqueue(t, h, queue.NewJob{Kind: "k"})
	first := leaseOne(t, h)

	reap(t, h, 0) // lease still valid
	wantLeased(t, h, 0)

	h.Clock.Advance(leaseFor + time.Second) // worker "crashes": no ack, no heartbeat
	reap(t, h, 1)
	ji := wantState(t, h, id, queue.StateReady)
	if ji.LastError != "lease expired" || ji.Attempts != 1 {
		t.Fatalf("after reap: %+v", ji)
	}

	second := leaseOne(t, h)
	if second.ID != id || second.Attempt != 2 {
		t.Fatalf("re-lease: %+v", second)
	}
	if second.LeaseToken <= first.LeaseToken {
		t.Fatalf("fencing token did not increase: %d then %d", first.LeaseToken, second.LeaseToken)
	}

	// The zombie worker wakes up and tries to report. It must be rejected on
	// every path, and must not disturb the new holder.
	wantErr(t, h.Queue.Ack(ctx, first), queue.ErrLeaseLost)
	wantErr(t, h.Queue.Nack(ctx, first, h.Clock.Now(), errors.New("zombie")), queue.ErrLeaseLost)
	wantErr(t, h.Queue.Fail(ctx, first, errors.New("zombie")), queue.ErrLeaseLost)
	_, err := h.Queue.Extend(ctx, first, leaseFor)
	wantErr(t, err, queue.ErrLeaseLost)
	wantState(t, h, id, queue.StateLeased)

	if err := h.Queue.Ack(ctx, second); err != nil {
		t.Fatalf("ack by current holder: %v", err)
	}
	wantState(t, h, id, queue.StateDone)
}

func testPoisonPill(t *testing.T, h Harness) {
	id := enqueue(t, h, queue.NewJob{Kind: "k", MaxAttempts: 3})
	for attempt := 1; attempt <= 3; attempt++ {
		j := leaseOne(t, h)
		if j.Attempt != attempt {
			t.Fatalf("attempt = %d, want %d", j.Attempt, attempt)
		}
		h.Clock.Advance(leaseFor + time.Second) // worker dies every time, never calls Nack
		reap(t, h, 1)
		if attempt < 3 {
			wantState(t, h, id, queue.StateReady)
		}
	}
	ji := wantState(t, h, id, queue.StateDead)
	if ji.Attempts != 3 || !strings.Contains(ji.LastError, "lease expired") || ji.FinishedAt.IsZero() {
		t.Fatalf("dead job: %+v", ji)
	}
	wantLeased(t, h, 0)
	dead, err := h.Admin.ListDead(ctx, 10, "")
	if err != nil || len(dead) != 1 || dead[0].ID != id {
		t.Fatalf("ListDead = %+v, %v", dead, err)
	}
}

func testNack(t *testing.T, h Harness) {
	id := enqueue(t, h, queue.NewJob{Kind: "k", MaxAttempts: 2})

	j := leaseOne(t, h)
	retryAt := h.Clock.Now().Add(10 * time.Second)
	if err := h.Queue.Nack(ctx, j, retryAt, errors.New("boom")); err != nil {
		t.Fatalf("nack: %v", err)
	}
	ji := wantState(t, h, id, queue.StateReady)
	if ji.LastError != "boom" || !ji.RunAt.Equal(retryAt) || !ji.FinishedAt.IsZero() {
		t.Fatalf("after first nack: %+v", ji)
	}
	wantLeased(t, h, 0) // not due yet
	h.Clock.Advance(10 * time.Second)

	j = leaseOne(t, h)
	if j.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", j.Attempt)
	}
	if err := h.Queue.Nack(ctx, j, h.Clock.Now().Add(time.Second), errors.New("boom again")); err != nil {
		t.Fatalf("nack: %v", err)
	}
	ji = wantState(t, h, id, queue.StateDead) // attempts exhausted: the queue decides, not the caller
	if ji.LastError != "boom again" || ji.FinishedAt.IsZero() {
		t.Fatalf("after final nack: %+v", ji)
	}
	h.Clock.Advance(time.Hour)
	wantLeased(t, h, 0)
}

func testFail(t *testing.T, h Harness) {
	id := enqueue(t, h, queue.NewJob{Kind: "k", MaxAttempts: 5})
	j := leaseOne(t, h)
	if err := h.Queue.Fail(ctx, j, errors.New("bad request")); err != nil {
		t.Fatalf("fail: %v", err)
	}
	ji := wantState(t, h, id, queue.StateDead)
	if ji.Attempts != 1 || ji.LastError != "bad request" || ji.FinishedAt.IsZero() {
		t.Fatalf("failed job: %+v", ji)
	}
	wantErr(t, h.Queue.Ack(ctx, j), queue.ErrLeaseLost)
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
		t.Fatalf("new expiry = %v, want %v", expires, want)
	}

	h.Clock.Advance(20 * time.Second) // t=40s: past the original expiry, inside the extended one
	reap(t, h, 0)
	wantState(t, h, id, queue.StateLeased)

	bad := j
	bad.LeaseToken++
	_, err = h.Queue.Extend(ctx, bad, leaseFor)
	wantErr(t, err, queue.ErrLeaseLost)

	h.Clock.Advance(leaseFor) // now truly expired
	reap(t, h, 1)
	_, err = h.Queue.Extend(ctx, j, leaseFor)
	wantErr(t, err, queue.ErrLeaseLost)
}

func testAckAfterExpiry(t *testing.T, h Harness) {
	id := enqueue(t, h, queue.NewJob{Kind: "k"})
	j := leaseOne(t, h)
	h.Clock.Advance(leaseFor + time.Minute) // expired, but nobody reaped or re-leased it
	if err := h.Queue.Ack(ctx, j); err != nil {
		t.Fatalf("late ack of an unreassigned job should succeed: %v", err)
	}
	wantState(t, h, id, queue.StateDone)
	reap(t, h, 0)
}

func testDoubleAck(t *testing.T, h Harness) {
	enqueue(t, h, queue.NewJob{Kind: "k"})
	j := leaseOne(t, h)
	if err := h.Queue.Ack(ctx, j); err != nil {
		t.Fatalf("first ack: %v", err)
	}
	wantErr(t, h.Queue.Ack(ctx, j), queue.ErrLeaseLost)
}

func testDedupe(t *testing.T, h Harness) {
	id1, created, err := h.Queue.Enqueue(ctx, queue.NewJob{Kind: "k1", DedupeKey: "x"})
	if err != nil || !created {
		t.Fatalf("first enqueue: created=%v err=%v", created, err)
	}
	id2, created, err := h.Queue.Enqueue(ctx, queue.NewJob{Kind: "k1", DedupeKey: "x"})
	if err != nil || created || id2 != id1 {
		t.Fatalf("duplicate: id=%s (want %s) created=%v err=%v", id2, id1, created, err)
	}
	id3, created, err := h.Queue.Enqueue(ctx, queue.NewJob{Kind: "k2", DedupeKey: "x"})
	if err != nil || !created || id3 == id1 {
		t.Fatalf("same key, other kind: id=%s created=%v err=%v", id3, created, err)
	}
	// No key means no dedupe.
	enqueue(t, h, queue.NewJob{Kind: "k1"})
	enqueue(t, h, queue.NewJob{Kind: "k1"})

	// The key stays reserved after the job finishes (until purged).
	j := lease(t, h, []string{"k1"}, 1)[0]
	if err := h.Queue.Ack(ctx, j); err != nil {
		t.Fatal(err)
	}
	_, created, err = h.Queue.Enqueue(ctx, queue.NewJob{Kind: "k1", DedupeKey: "x"})
	if err != nil || created {
		t.Fatalf("dedupe after completion: created=%v err=%v", created, err)
	}
}

func testTransactionalEnqueue(t *testing.T, h Harness) {
	rollback := errors.New("rollback please")
	var inTxID string
	err := h.InTx(ctx, func(ctx context.Context) error {
		id, created, err := h.Queue.Enqueue(ctx, queue.NewJob{Kind: "k", DedupeKey: "d"})
		if err != nil || !created {
			t.Errorf("enqueue in tx: created=%v err=%v", created, err)
		}
		inTxID = id
		return rollback
	})
	wantErr(t, err, rollback)

	wantLeased(t, h, 0)
	if _, err := h.Admin.Get(ctx, inTxID); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("rolled-back job still exists (err=%v)", err)
	}

	// Rolled back means the dedupe key is free again, and a commit persists.
	err = h.InTx(ctx, func(ctx context.Context) error {
		_, created, err := h.Queue.Enqueue(ctx, queue.NewJob{Kind: "k", DedupeKey: "d"})
		if err == nil && !created {
			err = errors.New("dedupe key leaked out of the rolled-back transaction")
		}
		return err
	})
	if err != nil {
		t.Fatalf("committed tx: %v", err)
	}
	wantLeased(t, h, 1)
}

func testRequeue(t *testing.T, h Harness) {
	id := makeDead(t, h, "k")
	if err := h.Admin.Requeue(ctx, id); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	ji := wantState(t, h, id, queue.StateReady)
	if ji.Attempts != 0 || !ji.FinishedAt.IsZero() {
		t.Fatalf("after requeue: %+v", ji)
	}
	j := leaseOne(t, h)
	if j.ID != id || j.Attempt != 1 {
		t.Fatalf("re-lease after requeue: %+v", j)
	}

	wantErr(t, h.Admin.Requeue(ctx, id), queue.ErrNotDead) // leased now
	wantErr(t, h.Admin.Requeue(ctx, "no-such-id"), queue.ErrNotFound)
}

func testListDead(t *testing.T, h Harness) {
	var ids []string
	for i := 0; i < 5; i++ {
		ids = append(ids, makeDead(t, h, "k"))
	}
	enqueue(t, h, queue.NewJob{Kind: "k"}) // ready, must not appear

	var got []string
	after := ""
	for pages := 0; pages < 10; pages++ {
		page, err := h.Admin.ListDead(ctx, 2, after)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(page) == 0 {
			break
		}
		if len(page) > 2 {
			t.Fatalf("page of %d exceeds limit 2", len(page))
		}
		for _, ji := range page {
			got = append(got, ji.ID)
		}
		after = page[len(page)-1].ID
	}
	if len(got) != 5 {
		t.Fatalf("listed %d dead jobs, want 5: %v", len(got), got)
	}
	seen := map[string]bool{}
	for _, id := range got {
		seen[id] = true
	}
	for _, id := range ids {
		if !seen[id] {
			t.Errorf("dead job %s missing from listing", id)
		}
	}
}

func testPurge(t *testing.T, h Harness) {
	finish := func(fail bool) string {
		id := enqueue(t, h, queue.NewJob{Kind: "k"})
		j := leaseOne(t, h)
		var err error
		if fail {
			err = h.Queue.Fail(ctx, j, errors.New("x"))
		} else {
			err = h.Queue.Ack(ctx, j)
		}
		if err != nil {
			t.Fatal(err)
		}
		return id
	}

	oldDone, oldDead := finish(false), finish(true)
	h.Clock.Advance(48 * time.Hour)
	newDone, newDead := finish(false), finish(true)
	cutoff := h.Clock.Now().Add(-24 * time.Hour)

	if _, err := h.Admin.Purge(ctx, queue.StateReady, cutoff); err == nil {
		t.Error("purging non-final states must be rejected")
	}

	n, err := h.Admin.Purge(ctx, queue.StateDone, cutoff)
	if err != nil || n != 1 {
		t.Fatalf("purge done: n=%d err=%v, want 1", n, err)
	}
	if _, err := h.Admin.Get(ctx, oldDone); !errors.Is(err, queue.ErrNotFound) {
		t.Errorf("old done job should be gone (err=%v)", err)
	}
	wantState(t, h, oldDead, queue.StateDead) // purging done must never touch dead letters
	wantState(t, h, newDone, queue.StateDone)

	n, err = h.Admin.Purge(ctx, queue.StateDead, cutoff)
	if err != nil || n != 1 {
		t.Fatalf("purge dead: n=%d err=%v, want 1", n, err)
	}
	wantState(t, h, newDead, queue.StateDead)
}
