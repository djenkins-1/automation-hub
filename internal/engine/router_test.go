package engine_test

// Specification of the Router: turning an event into deliveries.
//
//  - Each action of each matching rule becomes exactly one delivery and one
//    deliver job; a non-matching event produces nothing and is acknowledged.
//  - Routing the same event twice (a replayed job, a lost ack, racing
//    workers) never creates duplicates: delivery identity is stable.
//  - The fan-out is all-or-nothing: if anything fails part-way, nothing is
//    kept, and the retry produces the complete set.
//  - Failures that retrying can fix are returned as ordinary errors; those it
//    cannot fix (garbage job, vanished event) are permanent.
//  - A rule that cannot be evaluated never blocks the others.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/djenkins-1/automation-hub/internal/engine"
	"github.com/djenkins-1/automation-hub/internal/queue"
	"github.com/djenkins-1/automation-hub/internal/rules"
	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

func act(sink, title string) rules.Action { return rules.Action{Sink: sink, Title: title} }

// threeActions: two rules, three actions in total, all matching a disk event.
func threeActions() []rules.Rule {
	return []rules.Rule{
		{Name: "a", Match: rules.Match{Type: "disk.*"}, Actions: []rules.Action{act("rec", "a0 {{.Event.Subject}}"), act("rec", "a1")}},
		{Name: "b", Match: rules.Match{Source: "monitor"}, Actions: []rules.Action{act("rec", "b0")}},
	}
}

func TestMatchingEventsBecomeDeliveriesInTheirOwnRightAndDeliverJobs(t *testing.T) {
	h := newHarness(t)
	res := h.accept(diskEvent(95))
	h.route()

	d := h.onlyDelivery(res.EventID)
	if d.Title != "Disk /var" || d.Body != "95% used" || d.Sink != "rec" || d.Rule != "disk" || d.ActionIndex != 0 || d.EventID != res.EventID {
		t.Fatalf("delivery = %+v", d)
	}
	if d.Status != engine.DeliveryPending || d.Attempts != 0 || d.LastError != "" || !d.DeliveredAt.IsZero() {
		t.Fatalf("a fresh delivery must be pending with no history: %+v", d)
	}
	if n := h.jobsOfKind(engine.KindDeliver); n != 1 {
		t.Fatalf("%d deliver jobs, want 1", n)
	}
}

func TestNonMatchingEventsProduceNothingAndAreAcknowledged(t *testing.T) {
	h := newHarness(t)
	for name, e := range map[string]sdk.Event{
		"condition false": diskEvent(50),
		"wrong source":    {Source: "other", Type: "disk.full", Data: map[string]any{"percent": 99.0}},
		"wrong type":      {Source: "monitor", Type: "cpu.high", Data: map[string]any{"percent": 99.0}},
	} {
		res := h.accept(e)
		jobs := h.lease(engine.KindRoute)
		if len(jobs) != 1 {
			t.Fatalf("%s: %d route jobs", name, len(jobs))
		}
		if err := h.router.Handle(ctx, jobs[0]); err != nil {
			t.Errorf("%s: an event that matches nothing is not an error (the job must be acked): %v", name, err)
		}
		if n := len(h.deliveriesFor(res.EventID)); n != 0 {
			t.Errorf("%s: %d deliveries created", name, n)
		}
	}
	if h.jobsOfKind(engine.KindDeliver) != 0 {
		t.Fatal("deliver jobs exist for events nothing matched")
	}
}

func TestEveryActionOfEveryMatchingRuleBecomesExactlyOneDelivery(t *testing.T) {
	h := newHarness(t, append(threeActions(), rules.Rule{Name: "c", Match: rules.Match{Type: "cpu.*"}, Actions: []rules.Action{act("rec", "never")}})...)
	res := h.accept(diskEvent(95))
	h.route()

	ds := h.deliveriesFor(res.EventID)
	if len(ds) != 3 {
		t.Fatalf("%d deliveries, want 3 (rule c does not match)", len(ds))
	}
	byTitle := map[string]engine.Delivery{}
	keys := map[string]bool{}
	for _, d := range ds {
		byTitle[d.Title] = d
		keys[d.IdempotencyKey] = true
	}
	if len(keys) != 3 {
		t.Fatalf("deliveries must have distinct idempotency keys, got %v", keys)
	}
	for title, want := range map[string][2]any{"a0 /var": {"a", 0}, "a1": {"a", 1}, "b0": {"b", 0}} {
		d, ok := byTitle[title]
		if !ok || d.Rule != want[0] || d.ActionIndex != want[1] {
			t.Errorf("delivery %q = %+v, want rule %v action %v", title, d, want[0], want[1])
		}
	}
	if n := h.jobsOfKind(engine.KindDeliver); n != 3 {
		t.Fatalf("%d deliver jobs, want 3", n)
	}
}

func TestReplayingARouteJobNeverCreatesDuplicates(t *testing.T) {
	h := newHarness(t, threeActions()...)
	res := h.accept(diskEvent(95))
	job := h.lease(engine.KindRoute)[0]

	var firstIDs []string
	for run := 1; run <= 4; run++ { // a crash after commit but before ack replays the job
		if err := h.router.Handle(ctx, job); err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		ds := h.deliveriesFor(res.EventID)
		if len(ds) != 3 || h.jobsOfKind(engine.KindDeliver) != 3 {
			t.Fatalf("run %d: %d deliveries and %d deliver jobs; want 3 and 3 however often the job is replayed", run, len(ds), h.jobsOfKind(engine.KindDeliver))
		}
		var ids []string
		for _, d := range ds {
			ids = append(ids, d.ID+"|"+d.IdempotencyKey)
		}
		if run == 1 {
			firstIDs = ids
		} else if strings.Join(ids, ",") != strings.Join(firstIDs, ",") {
			t.Fatalf("run %d changed the deliveries' identities:\n first %v\n now   %v", run, firstIDs, ids)
		}
	}
}

func TestRacingReplaysOfOneRouteJobCreateNoDuplicates(t *testing.T) {
	h := newHarness(t, threeActions()...)
	res := h.accept(diskEvent(95))
	job := h.lease(engine.KindRoute)[0]

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := h.router.Handle(ctx, job); err != nil {
				t.Errorf("Handle: %v", err)
			}
		}()
	}
	wg.Wait()
	if n := len(h.deliveriesFor(res.EventID)); n != 3 || h.jobsOfKind(engine.KindDeliver) != 3 {
		t.Fatalf("%d deliveries and %d deliver jobs after racing replays; want 3 and 3", n, h.jobsOfKind(engine.KindDeliver))
	}
}

func TestDeliveryIdentityIsStablePerEventAndDistinctAcrossEvents(t *testing.T) {
	h := newHarness(t, threeActions()...)
	a, b := h.accept(diskEvent(95)), h.accept(diskEvent(95))
	h.route()

	keys := map[string]bool{}
	for _, id := range []string{a.EventID, b.EventID} {
		ds := h.deliveriesFor(id)
		if len(ds) != 3 {
			t.Fatalf("event %s: %d deliveries", id, len(ds))
		}
		for _, d := range ds {
			if keys[d.IdempotencyKey] {
				t.Fatalf("idempotency key %q is shared between deliveries; a sink would drop one as a duplicate", d.IdempotencyKey)
			}
			keys[d.IdempotencyKey] = true
		}
	}
	if len(keys) != 6 {
		t.Fatalf("%d distinct keys for 6 deliveries", len(keys))
	}
}

func TestAFailureMidFanOutKeepsNothingAndTheRetryCompletesIt(t *testing.T) {
	for name, tc := range map[string]struct {
		build func(h *harness) *engine.Router
	}{
		"the queue fails on the second deliver job": {func(h *harness) *engine.Router {
			return engine.NewRouter(h.db, h.events, h.deliveries, &flakyEnqueuer{inner: h.q, failOn: 2}, h.set, h.opts)
		}},
		"the queue fails on the last deliver job": {func(h *harness) *engine.Router {
			return engine.NewRouter(h.db, h.events, h.deliveries, &flakyEnqueuer{inner: h.q, failOn: 3}, h.set, h.opts)
		}},
		"the delivery store fails on the third delivery": {func(h *harness) *engine.Router {
			return engine.NewRouter(h.db, h.events, &brokenDeliveries{DeliveryStore: h.deliveries, failCreateOn: 3}, h.q, h.set, h.opts)
		}},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, threeActions()...)
			res := h.accept(diskEvent(95))
			job := h.lease(engine.KindRoute)[0]

			err := tc.build(h).Handle(ctx, job)
			if !errors.Is(err, errInjected) {
				t.Fatalf("err = %v, want the underlying failure", err)
			}
			if sdk.IsPermanent(err) {
				t.Fatal("a transient failure must not be permanent: the job has to be retried")
			}
			if n := len(h.deliveriesFor(res.EventID)); n != 0 {
				t.Fatalf("%d deliveries survived a failed route; a partial fan-out would be re-sent inconsistently", n)
			}
			if n := h.jobsOfKind(engine.KindDeliver); n != 0 {
				t.Fatalf("%d deliver jobs survived a failed route", n)
			}

			if err := h.router.Handle(ctx, job); err != nil { // the retry, with everything healthy again
				t.Fatalf("retry: %v", err)
			}
			if n := len(h.deliveriesFor(res.EventID)); n != 3 || h.jobsOfKind(engine.KindDeliver) != 3 {
				t.Fatalf("after the retry: %d deliveries, %d jobs; want the complete 3 and 3", n, h.jobsOfKind(engine.KindDeliver))
			}
		})
	}
}

func TestFailuresThatRetryingCanFixAreOrdinaryErrors(t *testing.T) {
	h := newHarness(t)
	res := h.accept(diskEvent(95))
	job := h.lease(engine.KindRoute)[0]

	flaky := engine.NewRouter(h.db, brokenEvents{EventStore: h.events, failGet: errInjected}, h.deliveries, h.q, h.set, h.opts)
	err := flaky.Handle(ctx, job)
	if !errors.Is(err, errInjected) || sdk.IsPermanent(err) {
		t.Fatalf("a failing event store: err = %v (permanent=%v), want a retryable error", err, sdk.IsPermanent(err))
	}
	if len(h.deliveriesFor(res.EventID)) != 0 {
		t.Fatal("nothing may be created when the event could not be loaded")
	}

	c, cancel := context.WithCancel(ctx)
	cancel()
	err = h.router.Handle(c, job)
	if err == nil || sdk.IsPermanent(err) {
		t.Fatalf("a cancelled context: err = %v, want a retryable error", err)
	}
	if len(h.deliveriesFor(res.EventID)) != 0 {
		t.Fatal("a cancelled route created deliveries")
	}
}

func TestFailuresThatRetryingCannotFixArePermanent(t *testing.T) {
	h := newHarness(t)
	for name, payload := range map[string]string{
		"not json":                `not json`,
		"empty":                   ``,
		"json without an id":      `{}`,
		"wrong shape":             `["event"]`,
		"an id of the wrong type": `{"event_id": 5}`,
		"empty id":                `{"event_id": ""}`,
		"an event that is gone":   `{"event_id": "01HNOSUCHEVENT"}`,
	} {
		err := h.router.Handle(ctx, queue.Job{Payload: []byte(payload), Attempt: 1, MaxAttempts: 3})
		if err == nil || !sdk.IsPermanent(err) {
			t.Errorf("%s: err = %v, want a permanent error (retrying cannot help)", name, err)
		}
	}
}

func TestAnEventPurgedBeforeRoutingIsAPermanentFailureNotAnEndlessRetry(t *testing.T) {
	h := newHarness(t)
	res := h.accept(diskEvent(95))
	job := h.lease(engine.KindRoute)[0]
	if _, err := h.db.Write.ExecContext(ctx, `DELETE FROM events WHERE id = ?`, res.EventID); err != nil { // retention removed it
		t.Fatal(err)
	}
	if err := h.router.Handle(ctx, job); !sdk.IsPermanent(err) {
		t.Fatalf("err = %v, want permanent", err)
	}
}

func TestBrokenRulesNeverBlockHealthyOnes(t *testing.T) {
	h := newHarness(t,
		rules.Rule{Name: "needs-missing-field", Match: rules.Match{When: "data.nothing > 1"}, Actions: []rules.Action{act("rec", "never")}},
		rules.Rule{Name: "healthy-before", Actions: []rules.Action{act("rec", "before")}},
		rules.Rule{Name: "bad-template", Actions: []rules.Action{act("rec", "{{index .Event.Data.list 9}}"), act("rec", "good action of a bad rule")}},
		rules.Rule{Name: "healthy-after", Actions: []rules.Action{act("rec", "after")}},
	)
	e := diskEvent(95)
	e.Data["list"] = []any{1.0}
	res := h.accept(e)
	jobs := h.lease(engine.KindRoute)
	if err := h.router.Handle(ctx, jobs[0]); err != nil {
		t.Fatalf("rule problems must not fail (and so endlessly retry) the route job: %v", err)
	}
	var titles []string
	for _, d := range h.deliveriesFor(res.EventID) {
		titles = append(titles, d.Title)
	}
	got := strings.Join(sortedStrings(titles), "|")
	if want := "after|before|good action of a bad rule"; got != want {
		t.Fatalf("deliveries = %q, want %q", got, want)
	}
}

func sortedStrings(in []string) []string {
	out := append([]string(nil), in...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func TestEventsWithoutDataAndInternationalContentRouteCorrectly(t *testing.T) {
	h := newHarness(t, rules.Rule{Name: "all", Actions: []rules.Action{{Sink: "rec", Title: "{{.Event.Subject}} {{.Event.Data.who | default \"nobody\"}}", Body: "{{.Event.Type}}"}}})
	bare := h.accept(sdk.Event{Source: "s", Type: "t"})
	intl := h.accept(sdk.Event{Source: "s", Type: "t2", Subject: "日本語 🚨", Data: map[string]any{"who": "Zoë \"Z\""}})
	h.route()
	if d := h.onlyDelivery(bare.EventID); d.Title != " nobody" || d.Body != "t" {
		t.Fatalf("bare event delivery: %+v", d)
	}
	if d := h.onlyDelivery(intl.EventID); d.Title != "日本語 🚨 Zoë \"Z\"" || d.Body != "t2" {
		t.Fatalf("international event delivery: %+v", d)
	}
}

func TestManyEventsRouteIndependently(t *testing.T) {
	h := newHarness(t)
	const n = 40
	want := map[string]string{}
	for i := 0; i < n; i++ {
		e := diskEvent(float64(91 + i%9))
		e.Subject = fmt.Sprintf("/mnt/%d", i)
		want[h.accept(e).EventID] = fmt.Sprintf("Disk /mnt/%d", i)
	}
	h.route()
	for id, title := range want {
		if d := h.onlyDelivery(id); d.Title != title {
			t.Fatalf("event %s produced %q, want %q (output crossed between events)", id, d.Title, title)
		}
	}
	if h.jobsOfKind(engine.KindDeliver) != n {
		t.Fatalf("%d deliver jobs, want %d", h.jobsOfKind(engine.KindDeliver), n)
	}
}
