package queuetest

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/djenkins-1/automation-hub/internal/queue"
)

// The model test drives the queue with long, random sequences of operations
// (enqueue, lease, ack, nack, fail, extend, time passing, reaping, requeueing,
// purging, and stale workers reporting late) and, after every step, checks the
// queue against a tiny in-memory model of the rules written down in the
// contract. It finds the interactions no hand-written scenario thinks of: a
// requeue racing a stale ack, a purge after a reap, a lease expiring exactly on
// a tick.
//
// The model deliberately knows nothing about SQL. If the intended semantics
// change, change the model.

type mjob struct {
	kind       string
	state      string
	attempts   int
	max        int
	runAt      time.Time
	token      int64
	expires    time.Time
	finishedAt time.Time
}

type modelRun struct {
	t       *testing.T
	h       Harness
	rng     *rand.Rand
	seed    int64
	jobs    map[string]*mjob
	holders []queue.Job // every lease ever handed out, so stale ones can report late
	seq     int         // keeps run_at values distinct, so lease order is unambiguous
	step    int
	recent  []string
}

func runModel(t *testing.T, h Harness, seed int64) {
	m := &modelRun{t: t, h: h, rng: rand.New(rand.NewSource(seed)), seed: seed, jobs: map[string]*mjob{}}
	const steps = 500
	for m.step = 1; m.step <= steps; m.step++ {
		switch r := m.rng.Intn(100); {
		case r < 25:
			m.opEnqueue()
		case r < 50:
			m.opLease()
		case r < 75:
			m.opOutcome()
		case r < 87:
			m.opAdvance()
		case r < 93:
			m.opReap()
		case r < 97:
			m.opRequeue()
		default:
			m.opPurge()
		}
		if m.step%25 == 0 {
			m.verifyAll()
		}
	}
	m.verifyAll()

	// Whatever happened, the model must be able to finish: drain it.
	expire(h)
	reap(t, h)
	for i := 0; i < 20; i++ {
		jobs := lease(t, h, nil, 50)
		if len(jobs) == 0 {
			h.Clock.Advance(time.Hour)
			if reap(t, h) == 0 {
				break
			}
			continue
		}
		for _, j := range jobs {
			if err := h.Queue.Ack(ctx, j); err != nil {
				t.Fatalf("seed %d: drain ack: %v", seed, err)
			}
		}
	}
	if got := lease(t, h, nil, 10); len(got) != 0 {
		t.Fatalf("seed %d: the queue still hands out work after being drained", seed)
	}
}

func (m *modelRun) logf(format string, args ...any) {
	m.recent = append(m.recent, fmt.Sprintf("#%d "+format, append([]any{m.step}, args...)...))
	if len(m.recent) > 12 {
		m.recent = m.recent[1:]
	}
}

func (m *modelRun) failf(format string, args ...any) {
	m.t.Helper()
	m.t.Fatalf("seed %d, step %d: %s\nrecent operations:\n  %s", m.seed, m.step, fmt.Sprintf(format, args...), strings.Join(m.recent, "\n  "))
}

func (m *modelRun) now() time.Time { return m.h.Clock.Now() }

// uniq returns a small, strictly increasing offset so no two scheduled times collide.
func (m *modelRun) uniq() time.Duration { m.seq++; return time.Duration(m.seq) * time.Millisecond }

func (m *modelRun) opEnqueue() {
	kind := []string{"a", "b"}[m.rng.Intn(2)]
	runAt := m.now().Add(time.Duration(m.rng.Intn(61)-30)*time.Second + m.uniq())
	max := 1 + m.rng.Intn(4)
	id, created, err := m.h.Queue.Enqueue(ctx, queue.NewJob{Kind: kind, RunAt: runAt, MaxAttempts: max})
	if err != nil || !created {
		m.failf("enqueue: created=%v err=%v", created, err)
	}
	m.jobs[id] = &mjob{kind: kind, state: queue.StateReady, max: max, runAt: runAt}
	m.logf("enqueue %s kind=%s max=%d", short(id), kind, max)
	m.verify(id)
}

func (m *modelRun) opLease() {
	kindSets := [][]string{nil, {"a"}, {"b"}, {"a", "b"}}
	kinds := kindSets[m.rng.Intn(len(kindSets))]
	n := 1 + m.rng.Intn(4)
	now := m.now()

	eligible := map[string]bool{}
	for id, j := range m.jobs {
		if j.state == queue.StateReady && !j.runAt.After(now) && kindMatches(kinds, j.kind) {
			eligible[id] = true
		}
	}
	got, err := m.h.Queue.Lease(ctx, "model", kinds, n, leaseFor)
	if err != nil {
		m.failf("lease: %v", err)
	}
	m.logf("lease kinds=%v n=%d -> %d job(s) (%d eligible)", kinds, n, len(got), len(eligible))

	want := n
	if len(eligible) < want {
		want = len(eligible)
	}
	if len(got) != want {
		m.failf("leased %d jobs, want %d (n=%d, eligible=%d)", len(got), want, n, len(eligible))
	}

	returned := map[string]bool{}
	var maxRun, prevRun time.Time
	for i, j := range got {
		mj := m.jobs[j.ID]
		if !eligible[j.ID] || returned[j.ID] {
			m.failf("leased job %s which was not eligible, or leased it twice in one call", short(j.ID))
		}
		returned[j.ID] = true
		if i > 0 && mj.runAt.Before(prevRun) {
			m.failf("jobs were not returned oldest-first")
		}
		prevRun = mj.runAt
		if mj.runAt.After(maxRun) {
			maxRun = mj.runAt
		}
		mj.attempts++
		mj.token++
		mj.state = queue.StateLeased
		mj.expires = now.Add(leaseFor)
		if j.Attempt != mj.attempts || j.LeaseToken != mj.token || j.MaxAttempts != mj.max || j.Kind != mj.kind || !j.LeaseExpiresAt.Equal(mj.expires) {
			m.failf("lease metadata for %s: got attempt=%d token=%d max=%d kind=%s expires=%v; model says attempt=%d token=%d max=%d kind=%s expires=%v",
				short(j.ID), j.Attempt, j.LeaseToken, j.MaxAttempts, j.Kind, j.LeaseExpiresAt, mj.attempts, mj.token, mj.max, mj.kind, mj.expires)
		}
		m.holders = append(m.holders, j)
	}
	for id := range eligible {
		if !returned[id] && m.jobs[id].runAt.Before(maxRun) {
			m.failf("job %s (older than something that was leased) was skipped", short(id))
		}
	}
	for id := range returned {
		m.verify(id)
	}
}

func (m *modelRun) opOutcome() {
	if len(m.holders) == 0 {
		return
	}
	// Half the time pick a lease that is genuinely current (the normal case);
	// otherwise any lease ever issued, which is usually long stale (the zombie case).
	j := m.holders[m.rng.Intn(len(m.holders))]
	if m.rng.Intn(2) == 0 {
		var live []queue.Job
		for _, h := range m.holders {
			if mj := m.jobs[h.ID]; mj != nil && mj.state == queue.StateLeased && mj.token == h.LeaseToken {
				live = append(live, h)
			}
		}
		if len(live) > 0 {
			j = live[m.rng.Intn(len(live))]
		}
	}
	mj := m.jobs[j.ID]
	current := mj != nil && mj.state == queue.StateLeased && mj.token == j.LeaseToken
	now := m.now()

	var (
		err  error
		name string
	)
	retryAt := now.Add(time.Duration(m.rng.Intn(26)-5)*time.Second + m.uniq())
	var newExpiry time.Time
	switch m.rng.Intn(4) {
	case 0:
		name, err = "ack", m.h.Queue.Ack(ctx, j)
	case 1:
		name, err = "nack", m.h.Queue.Nack(ctx, j, retryAt, errors.New("model failure"))
	case 2:
		name, err = "fail", m.h.Queue.Fail(ctx, j, errors.New("model failure"))
	case 3:
		name = "extend"
		newExpiry, err = m.h.Queue.Extend(ctx, j, leaseFor)
	}
	m.logf("%s %s (token %d, current=%v) -> err=%v", name, short(j.ID), j.LeaseToken, current, err)

	if !current {
		if !errors.Is(err, queue.ErrLeaseLost) {
			m.failf("%s with a stale or foreign lease returned %v, want ErrLeaseLost", name, err)
		}
		if mj != nil {
			m.verify(j.ID) // and it changed nothing
		}
		return
	}
	if err != nil {
		m.failf("%s by the current holder failed: %v", name, err)
	}
	switch name {
	case "ack":
		mj.state, mj.finishedAt = queue.StateDone, now
	case "fail":
		mj.state, mj.finishedAt = queue.StateDead, now
	case "nack":
		if mj.attempts >= mj.max {
			mj.state, mj.finishedAt = queue.StateDead, now
		} else {
			mj.state, mj.runAt = queue.StateReady, retryAt
		}
	case "extend":
		mj.expires = now.Add(leaseFor)
		if !newExpiry.Equal(mj.expires) {
			m.failf("extend returned expiry %v, want %v", newExpiry, mj.expires)
		}
	}
	m.verify(j.ID)
}

func (m *modelRun) opAdvance() {
	d := time.Duration(1+m.rng.Intn(45)) * time.Second
	m.h.Clock.Advance(d)
	m.logf("advance %v", d)
}

func (m *modelRun) opReap() {
	now := m.now()
	var expired []string
	for id, j := range m.jobs {
		if j.state == queue.StateLeased && !j.expires.After(now) {
			expired = append(expired, id)
		}
	}
	n, err := m.h.Queue.ReapExpired(ctx)
	m.logf("reap -> %d (model expects %d)", n, len(expired))
	if err != nil || n != len(expired) {
		m.failf("reap returned (%d, %v), want %d", n, err, len(expired))
	}
	for _, id := range expired {
		j := m.jobs[id]
		if j.attempts >= j.max {
			j.state, j.finishedAt = queue.StateDead, now
		} else {
			j.state, j.runAt = queue.StateReady, now
		}
		m.verify(id)
	}
}

func (m *modelRun) opRequeue() {
	id := "no-such-job"
	var ids []string
	for k := range m.jobs {
		ids = append(ids, k)
	}
	sort.Strings(ids)
	if len(ids) > 0 && m.rng.Intn(10) != 0 {
		id = ids[m.rng.Intn(len(ids))]
		if m.rng.Intn(10) < 7 { // mostly aim at jobs that can actually be requeued
			var dead []string
			for _, k := range ids {
				if m.jobs[k].state == queue.StateDead {
					dead = append(dead, k)
				}
			}
			if len(dead) > 0 {
				id = dead[m.rng.Intn(len(dead))]
			}
		}
	}
	err := m.h.Admin.Requeue(ctx, id)
	mj := m.jobs[id]
	m.logf("requeue %s -> %v", short(id), err)
	switch {
	case mj == nil:
		if !errors.Is(err, queue.ErrNotFound) {
			m.failf("requeue of an unknown job returned %v, want ErrNotFound", err)
		}
	case mj.state != queue.StateDead:
		if !errors.Is(err, queue.ErrNotDead) {
			m.failf("requeue of a %s job returned %v, want ErrNotDead", mj.state, err)
		}
		m.verify(id)
	default:
		if err != nil {
			m.failf("requeue of a dead job failed: %v", err)
		}
		mj.state, mj.attempts, mj.runAt, mj.finishedAt = queue.StateReady, 0, m.now(), time.Time{}
		m.verify(id)
	}
}

func (m *modelRun) opPurge() {
	state := []string{queue.StateDone, queue.StateDead}[m.rng.Intn(2)]
	cutoff := m.now().Add(-time.Duration(m.rng.Intn(60)) * time.Second)
	var doomed []string
	for id, j := range m.jobs {
		if j.state == state && j.finishedAt.Before(cutoff) {
			doomed = append(doomed, id)
		}
	}
	n, err := m.h.Admin.Purge(ctx, state, cutoff)
	m.logf("purge %s before %s -> %d (model expects %d)", state, cutoff.Format("15:04:05"), n, len(doomed))
	if err != nil || n != len(doomed) {
		m.failf("purge returned (%d, %v), want %d", n, err, len(doomed))
	}
	for _, id := range doomed {
		delete(m.jobs, id)
		if _, err := m.h.Admin.Get(ctx, id); !errors.Is(err, queue.ErrNotFound) {
			m.failf("purged job %s is still retrievable (err=%v)", short(id), err)
		}
	}
}

// verify checks one job's observable state against the model.
func (m *modelRun) verify(id string) {
	m.t.Helper()
	mj := m.jobs[id]
	ji, err := m.h.Admin.Get(ctx, id)
	if err != nil {
		m.failf("get %s: %v", short(id), err)
	}
	if ji.State != mj.state || ji.Attempts != mj.attempts || ji.MaxAttempts != mj.max || ji.Kind != mj.kind {
		m.failf("job %s: queue says state=%s attempts=%d max=%d kind=%s; model says state=%s attempts=%d max=%d kind=%s",
			short(id), ji.State, ji.Attempts, ji.MaxAttempts, ji.Kind, mj.state, mj.attempts, mj.max, mj.kind)
	}
	switch mj.state {
	case queue.StateReady:
		if !ji.RunAt.Equal(mj.runAt) || !ji.FinishedAt.IsZero() {
			m.failf("ready job %s: run_at=%v finished=%v; model says run_at=%v and not finished", short(id), ji.RunAt, ji.FinishedAt, mj.runAt)
		}
	case queue.StateDone, queue.StateDead:
		if !ji.FinishedAt.Equal(mj.finishedAt) {
			m.failf("%s job %s: finished_at=%v, model says %v", mj.state, short(id), ji.FinishedAt, mj.finishedAt)
		}
	case queue.StateLeased:
		if !ji.FinishedAt.IsZero() {
			m.failf("leased job %s has a finish time", short(id))
		}
	}
	if ji.Attempts > ji.MaxAttempts {
		m.failf("job %s has used %d of %d attempts", short(id), ji.Attempts, ji.MaxAttempts)
	}
}

func (m *modelRun) verifyAll() {
	m.t.Helper()
	var dead []string
	for id, j := range m.jobs {
		m.verify(id)
		if j.state == queue.StateDead {
			dead = append(dead, id)
		}
	}
	sort.Strings(dead)
	var listed []string
	after := ""
	for {
		page, err := m.h.Admin.ListDead(ctx, 500, after)
		if err != nil {
			m.failf("list dead: %v", err)
		}
		if len(page) == 0 {
			break
		}
		for _, ji := range page {
			listed = append(listed, ji.ID)
		}
		after = page[len(page)-1].ID
	}
	if strings.Join(listed, ",") != strings.Join(dead, ",") {
		m.failf("the dead-letter listing (%d) differs from the model (%d)", len(listed), len(dead))
	}
}

func kindMatches(kinds []string, kind string) bool {
	if len(kinds) == 0 {
		return true
	}
	for _, k := range kinds {
		if k == kind {
			return true
		}
	}
	return false
}

func short(id string) string {
	if len(id) > 8 {
		return id[len(id)-8:]
	}
	return id
}
