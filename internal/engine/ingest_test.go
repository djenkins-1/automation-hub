package engine_test

// Specification of Ingest, the single door every event comes through.
//
//  - Accept validates, stores the event and queues it for routing; success
//    means the event is durable and WILL be routed.
//  - Storing and queueing are one atomic step: never an event nothing will
//    process, never a job for an event that does not exist.
//  - The hub assigns the event's identity; the producer keeps its own time.
//  - An idempotency key makes retries by the producer harmless, even when
//    they race.
//  - Bad input is rejected completely and leaves no trace (not even a burned key).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djenkins-1/automation-hub/internal/engine"
	"github.com/djenkins-1/automation-hub/internal/queue"
	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

func TestAcceptedEventsAreStoredAndQueuedForRouting(t *testing.T) {
	h := newHarness(t)
	in := diskEvent(95)
	in.IdempotencyKey = "k1"
	res := h.accept(in)
	if res.EventID == "" || res.Duplicate {
		t.Fatalf("result = %+v", res)
	}

	got, err := h.events.GetEvent(ctx, res.EventID)
	if err != nil {
		t.Fatalf("an accepted event must be retrievable: %v", err)
	}
	if got.ID != res.EventID || got.Source != "monitor" || got.Type != "disk.full" || got.Subject != "/var" ||
		got.IdempotencyKey != "k1" || got.Data["percent"] != 95.0 {
		t.Fatalf("stored event differs from the accepted one: %+v", got)
	}
	if n := h.jobsOfKind(engine.KindRoute); n != 1 {
		t.Fatalf("%d route jobs queued, want exactly 1", n)
	}
}

// The route job Ingest queues must be one the Router understands: this is the
// contract between the two stages.
func TestWhatIngestQueuesIsWhatTheRouterConsumes(t *testing.T) {
	h := newHarness(t)
	res := h.accept(diskEvent(95))
	jobs := h.lease(engine.KindRoute)
	if len(jobs) != 1 {
		t.Fatalf("%d route jobs", len(jobs))
	}
	if err := h.router.Handle(ctx, jobs[0]); err != nil {
		t.Fatalf("the router rejected the job ingest produced: %v", err)
	}
	if d := h.onlyDelivery(res.EventID); d.EventID != res.EventID {
		t.Fatalf("the delivery belongs to event %s, not %s", d.EventID, res.EventID)
	}
}

func TestTheHubAssignsIdentityAndTheProducerKeepsItsTime(t *testing.T) {
	h := newHarness(t)

	plain := h.accept(diskEvent(95))
	other := h.accept(diskEvent(95)) // identical content, no key: two separate events
	if plain.EventID == other.EventID || plain.Duplicate || other.Duplicate {
		t.Fatalf("identical events without a key are still distinct: %+v %+v", plain, other)
	}
	if h.eventCount() != 2 || h.jobsOfKind(engine.KindRoute) != 2 {
		t.Fatal("each distinct event needs its own row and its own route job")
	}

	spoof := diskEvent(95)
	spoof.ID = "producer-chose-this"
	res := h.accept(spoof)
	if res.EventID == "producer-chose-this" {
		t.Fatal("event IDs belong to the hub; a producer must not be able to choose (or collide with) them")
	}
	if _, err := h.events.GetEvent(ctx, "producer-chose-this"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatal("nothing may be stored under a producer-chosen ID")
	}

	// time: defaults to receipt in UTC; a supplied time is kept as the same instant
	before := time.Now().Add(-time.Second)
	got, _ := h.events.GetEvent(ctx, plain.EventID)
	if got.Time.Before(before) || got.Time.After(time.Now().Add(time.Second)) || got.Time.Location() != time.UTC {
		t.Fatalf("default time = %v (%v); want about now, in UTC", got.Time, got.Time.Location())
	}
	supplied := diskEvent(95)
	supplied.Time = time.Date(2020, 1, 2, 3, 4, 5, 0, time.FixedZone("x", 3600))
	got, _ = h.events.GetEvent(ctx, h.accept(supplied).EventID)
	if !got.Time.Equal(supplied.Time) {
		t.Fatalf("the producer's time was changed: %v vs %v", got.Time, supplied.Time)
	}
}

func TestTheInjectedClockIsUsedForDefaultTimes(t *testing.T) {
	h := newHarness(t)
	fake := queue.NewFakeClock(time.Date(2030, 6, 7, 8, 9, 10, 0, time.UTC))
	opts := h.opts
	opts.Clock = fake
	ing := engine.NewIngest(h.db, h.events, h.q, opts)
	res, err := ing.Accept(ctx, diskEvent(95))
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := h.events.GetEvent(ctx, res.EventID); !got.Time.Equal(fake.Now()) {
		t.Fatalf("time = %v, want the injected clock's %v", got.Time, fake.Now())
	}
}

func TestInvalidEventsAreRejectedWithoutAnySideEffect(t *testing.T) {
	h := newHarness(t)
	bad := map[string]sdk.Event{
		"no source":          {Type: "t"},
		"no type":            {Source: "s"},
		"bad characters":     {Source: "bad source!", Type: "t"},
		"glob in type":       {Source: "s", Type: "disk.*"},
		"huge data":          {Source: "s", Type: "t", Data: map[string]any{"x": strings.Repeat("x", sdk.MaxEventDataBytes+1)}},
		"unserialisable":     {Source: "s", Type: "t", Data: map[string]any{"c": make(chan int)}},
		"control in key":     {Source: "s", Type: "t", IdempotencyKey: "a\nb"},
		"invalid with a key": {Source: "bad source", Type: "t", IdempotencyKey: "burned?"},
	}
	for name, e := range bad {
		res, err := h.ingest.Accept(ctx, e)
		if !errors.Is(err, sdk.ErrInvalidEvent) {
			t.Errorf("%s: err = %v, want ErrInvalidEvent", name, err)
		}
		if res != (engine.IngestResult{}) {
			t.Errorf("%s: a rejected event must not return a result: %+v", name, res)
		}
	}
	if h.eventCount() != 0 || h.jobsOfKind(engine.KindRoute) != 0 {
		t.Fatal("rejected events left rows behind")
	}
	// and a rejected event does not burn its idempotency key
	ok := diskEvent(95)
	ok.IdempotencyKey = "burned?"
	if res := h.accept(ok); res.Duplicate {
		t.Fatal("an invalid event consumed its idempotency key")
	}
}

func TestRepeatingAnEventWithTheSameKeyIsHarmless(t *testing.T) {
	h := newHarness(t)
	first := diskEvent(95)
	first.IdempotencyKey = "nightly-2026-01-01"
	a := h.accept(first)

	retry := diskEvent(99) // even if the retry's content differs, it is the same logical event
	retry.IdempotencyKey = "nightly-2026-01-01"
	retry.Subject = "different"
	for i := 0; i < 3; i++ {
		b := h.accept(retry)
		if !b.Duplicate || b.EventID != a.EventID {
			t.Fatalf("retry %d: %+v; want Duplicate=true and the original id %s", i, b, a.EventID)
		}
	}
	if h.eventCount() != 1 || h.jobsOfKind(engine.KindRoute) != 1 {
		t.Fatalf("events=%d route jobs=%d after duplicates; want 1 and 1", h.eventCount(), h.jobsOfKind(engine.KindRoute))
	}
	if stored, _ := h.events.GetEvent(ctx, a.EventID); stored.Subject != "/var" || stored.Data["percent"] != 95.0 {
		t.Fatalf("a duplicate must not alter the original event: %+v", stored)
	}

	// a key means "same producer, same event": other sources and other keys are unaffected
	other := first
	other.Source = "backup"
	if res := h.accept(other); res.Duplicate {
		t.Fatal("keys are scoped to the source")
	}
	other.IdempotencyKey = "nightly-2026-01-02"
	if res := h.accept(other); res.Duplicate {
		t.Fatal("a different key is a different event")
	}
}

func TestAnEventIsAcceptedOnlyIfItCanAlsoBeQueued(t *testing.T) {
	h := newHarness(t)
	in := diskEvent(95)
	in.IdempotencyKey = "k"

	broken := engine.NewIngest(h.db, h.events, &flakyEnqueuer{inner: h.q, failOn: 1}, h.opts)
	if res, err := broken.Accept(ctx, in); !errors.Is(err, errInjected) || res != (engine.IngestResult{}) {
		t.Fatalf("res=%+v err=%v; the queue's failure must be reported", res, err)
	}
	if h.eventCount() != 0 {
		t.Fatalf("the event was stored although it could not be queued; nothing would ever route it")
	}
	// The producer's retry then works, and its key was not consumed.
	if res := h.accept(in); res.Duplicate {
		t.Fatal("the key of an event that failed to queue must be reusable")
	}
	if h.eventCount() != 1 || h.jobsOfKind(engine.KindRoute) != 1 {
		t.Fatal("the retry should produce exactly one event and one job")
	}
}

func TestAJobIsNeverQueuedForAnEventThatWasNotStored(t *testing.T) {
	h := newHarness(t)
	broken := engine.NewIngest(h.db, brokenEvents{EventStore: h.events, failInsert: true}, h.q, h.opts)
	if _, err := broken.Accept(ctx, diskEvent(95)); !errors.Is(err, errInjected) {
		t.Fatalf("err = %v", err)
	}
	if h.jobsOfKind(engine.KindRoute) != 0 || h.eventCount() != 0 {
		t.Fatal("a failed store must not leave a dangling job")
	}
}

func TestRacingDuplicatesProduceExactlyOneEventAndOneJob(t *testing.T) {
	h := newHarness(t)
	const n = 30
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		fresh   int
		eventID = map[string]bool{}
	)
	e := diskEvent(95)
	e.IdempotencyKey = "same"
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := h.ingest.Accept(ctx, e)
			if err != nil {
				t.Errorf("accept: %v", err)
				return
			}
			mu.Lock()
			eventID[res.EventID] = true
			if !res.Duplicate {
				fresh++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if fresh != 1 || len(eventID) != 1 || h.eventCount() != 1 || h.jobsOfKind(engine.KindRoute) != 1 {
		t.Fatalf("fresh=%d distinct ids=%d events=%d route jobs=%d among %d racing accepts; want 1/1/1/1",
			fresh, len(eventID), h.eventCount(), h.jobsOfKind(engine.KindRoute), n)
	}
}

func TestManyConcurrentDistinctEventsAreAllKept(t *testing.T) {
	h := newHarness(t)
	const n = 100
	var wg sync.WaitGroup
	ids := make(chan string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e := diskEvent(float64(91 + i%9))
			e.IdempotencyKey = fmt.Sprintf("k%d", i)
			res, err := h.ingest.Accept(ctx, e)
			if err != nil || res.Duplicate {
				t.Errorf("accept %d: %+v %v", i, res, err)
				return
			}
			ids <- res.EventID
		}(i)
	}
	wg.Wait()
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		seen[id] = true
	}
	if len(seen) != n || h.eventCount() != n || h.jobsOfKind(engine.KindRoute) != n {
		t.Fatalf("distinct ids=%d events=%d route jobs=%d, want %d of each", len(seen), h.eventCount(), h.jobsOfKind(engine.KindRoute), n)
	}
}

func TestACancelledContextAcceptsNothing(t *testing.T) {
	h := newHarness(t)
	c, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := h.ingest.Accept(c, diskEvent(95)); err == nil {
		t.Fatal("expected an error")
	}
	if h.eventCount() != 0 || h.jobsOfKind(engine.KindRoute) != 0 {
		t.Fatal("a cancelled Accept left rows behind")
	}
}

func TestAcceptDoesNotModifyTheCallersEvent(t *testing.T) {
	h := newHarness(t)
	e := sdk.Event{
		ID: "mine", Source: "monitor", Type: "disk.full", Subject: "/var", IdempotencyKey: "k",
		Data: map[string]any{"percent": 95.0, "list": []any{1.0, "x"}, "m": map[string]any{"k": "v"}},
	}
	snapshot, _ := json.Marshal(e)
	h.accept(e)
	h.accept(e) // duplicate path too
	after, _ := json.Marshal(e)
	if string(snapshot) != string(after) || !e.Time.IsZero() || e.ID != "mine" {
		t.Fatalf("Accept modified the caller's event:\n before %s\n after  %s", snapshot, after)
	}
}

func TestLargeAndInternationalEventsSurviveIntact(t *testing.T) {
	h := newHarness(t)
	big := diskEvent(95)
	big.Data = map[string]any{"percent": 95.0, "blob": strings.Repeat("日本語", (sdk.MaxEventDataBytes-64)/10)}
	if err := big.Validate(); err != nil {
		t.Fatalf("test setup: %v", err)
	}
	res := h.accept(big)
	got, err := h.events.GetEvent(ctx, res.EventID)
	if err != nil || !reflect.DeepEqual(got.Data, big.Data) {
		t.Fatalf("a near-limit event was altered in storage (err %v)", err)
	}

	intl := diskEvent(95)
	intl.Subject = "日本語 🚨 ünï — \"quoted\" \n second line"
	got, _ = h.events.GetEvent(ctx, h.accept(intl).EventID)
	if got.Subject != intl.Subject {
		t.Fatalf("subject %q came back as %q", intl.Subject, got.Subject)
	}
}

func TestEventsAreAcceptedEvenIfNoRuleWouldMatchThem(t *testing.T) {
	// Ingest does not know about rules; routing decides. An unmatched event is still stored.
	h := newHarness(t)
	res := h.accept(sdk.Event{Source: "unrelated", Type: "nothing.interesting"})
	if _, err := h.events.GetEvent(ctx, res.EventID); err != nil {
		t.Fatalf("event not stored: %v", err)
	}
	if h.jobsOfKind(engine.KindRoute) != 1 {
		t.Fatal("it should still be queued for routing")
	}
}
