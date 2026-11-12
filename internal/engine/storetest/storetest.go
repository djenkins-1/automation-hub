// Package storetest is the behavioural specification of engine.EventStore and
// engine.DeliveryStore, as an executable suite any implementation must pass:
//
//	func TestPostgresStores(t *testing.T) { storetest.Run(t, newPostgresHarness) }
//
// Promises covered:
//
//	Events      stored and returned faithfully; first writer wins per
//	            (source, idempotency key), also under concurrency
//	Deliveries  created once per idempotency key; every attempt is logged;
//	            counters and status always agree with the log; "delivered" is
//	            sticky; invalid input changes nothing
//	Listing     filters, stable order, exhaustive pagination
//	Transactions every operation joins a transaction carried in its context,
//	            so rolling back leaves no trace
package storetest

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djenkins-1/automation-hub/internal/engine"
	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

// Harness is a fresh, empty pair of stores.
type Harness struct {
	Events     engine.EventStore
	Deliveries engine.DeliveryStore
	// InTx runs fn in a transaction: commit on nil, roll back on error. The
	// context given to fn carries the transaction.
	InTx func(ctx context.Context, fn func(ctx context.Context) error) error
}

// Factory builds a fresh Harness, registering its own cleanup.
type Factory func(t *testing.T) Harness

type namedTest struct {
	name string
	fn   func(*testing.T, Harness)
}

// Run executes the whole suite as subtests.
func Run(t *testing.T, newHarness Factory) {
	t.Helper()
	groups := []struct {
		name  string
		tests []namedTest
	}{
		{"Events", []namedTest{
			{"RoundTripIsFaithful", testEventRoundTrip},
			{"TimeKeepsMillisecondsAndTheInstant", testEventTime},
			{"MissingDataReadsBackAsAnEmptyMap", testEventNoData},
			{"HostileTextIsStoredVerbatim", testEventHostileText},
			{"FirstWriterWinsPerSourceAndKey", testEventDedupe},
			{"DuplicateInsertsRacingExactlyOneWins", testEventDedupeConcurrent},
			{"ConcurrentDistinctInsertsAreAllKept", testEventConcurrentDistinct},
			{"IDsAreUnique", testEventIDUnique},
			{"MissingEventsAreReportedAsNotFound", testEventNotFound},
			{"ReturnedAndSuppliedDataAreNotShared", testEventNoAliasing},
		}},
		{"Deliveries", []namedTest{
			{"RoundTripIsFaithful", testDeliveryRoundTrip},
			{"CreationIsIdempotentPerKey", testDeliveryIdempotent},
			{"CreationRacingExactlyOneWins", testDeliveryConcurrent},
			{"RequiresAnExistingEvent", testDeliveryNeedsEvent},
			{"MissingDeliveriesAreReportedAsNotFound", testDeliveryNotFound},
		}},
		{"Attempts", []namedTest{
			{"StatusFollowsTheOutcome", testAttemptStatus},
			{"DeliveredIsSticky", testDeliveredSticky},
			{"LogRoundTripsInOrder", testAttemptLog},
			{"InvalidAttemptsChangeNothing", testAttemptRejections},
			{"HugeErrorTextIsAccepted", testAttemptHugeError},
			{"CountersAlwaysAgreeWithTheLog", testAttemptModel},
		}},
		{"Listing", []namedTest{
			{"FiltersAndOrder", testListFilters},
			{"PaginationIsExhaustiveForAnyPageSize", testListPagination},
			{"LimitsAreClamped", testListLimits},
		}},
		{"Transactions", []namedTest{
			{"EverythingJoinsTheCallersTransaction", testTransactions},
		}},
	}
	for _, g := range groups {
		t.Run(g.name, func(t *testing.T) {
			for _, tc := range g.tests {
				t.Run(tc.name, func(t *testing.T) { tc.fn(t, newHarness(t)) })
			}
		})
	}
}

var ctx = context.Background()

// ms is a time with whole-millisecond precision (the stores' documented resolution).
var ms = time.UnixMilli(1_767_268_800_000)

func ev(id, source, key string) sdk.Event {
	return sdk.Event{ID: id, Source: source, Type: "disk.full", Subject: "/var", Time: ms,
		Data: map[string]any{"percent": 95.5}, IdempotencyKey: key}
}

func insertEvent(t *testing.T, h Harness, e sdk.Event) {
	t.Helper()
	id, created, err := h.Events.InsertEvent(ctx, e, ms)
	if err != nil || !created || id != e.ID {
		t.Fatalf("insert %s: id=%q created=%v err=%v", e.ID, id, created, err)
	}
}

func nd(id, eventID, key string) engine.NewDelivery {
	return engine.NewDelivery{ID: id, EventID: eventID, Rule: "rule", ActionIndex: 2, Sink: "phone",
		Title: "title", Body: "body", IdempotencyKey: key, CreatedAt: ms}
}

func createDelivery(t *testing.T, h Harness, d engine.NewDelivery) {
	t.Helper()
	id, created, err := h.Deliveries.CreateDelivery(ctx, d)
	if err != nil || !created || id != d.ID {
		t.Fatalf("create %s: id=%q created=%v err=%v", d.ID, id, created, err)
	}
}

func attempt(deliveryID string, n int, outcome engine.AttemptOutcome, errText, detail string) engine.Attempt {
	return engine.Attempt{DeliveryID: deliveryID, Number: n, Outcome: outcome, Error: errText, Detail: detail,
		StartedAt: ms.Add(time.Duration(n) * time.Second), FinishedAt: ms.Add(time.Duration(n)*time.Second + 250*time.Millisecond)}
}

func record(t *testing.T, h Harness, a engine.Attempt) {
	t.Helper()
	if err := h.Deliveries.RecordAttempt(ctx, a); err != nil {
		t.Fatalf("record %+v: %v", a, err)
	}
}

func getDelivery(t *testing.T, h Harness, id string) engine.Delivery {
	t.Helper()
	d, err := h.Deliveries.GetDelivery(ctx, id)
	if err != nil {
		t.Fatalf("get delivery %s: %v", id, err)
	}
	return d
}

func attemptsOf(t *testing.T, h Harness, id string) []engine.Attempt {
	t.Helper()
	as, err := h.Deliveries.ListAttempts(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return as
}

func newDeliveryWithEvent(t *testing.T, h Harness) string {
	t.Helper()
	insertEvent(t, h, ev("e1", "src", ""))
	createDelivery(t, h, nd("d1", "e1", "k1"))
	return "d1"
}

// ---------------------------------------------------------------------------
// Events

func testEventRoundTrip(t *testing.T, h Harness) {
	in := sdk.Event{
		ID: "e1", Source: "imap:me@example.com", Type: "mail:received", Time: ms, IdempotencyKey: "k-ü-日本-🔑",
		Subject: "Re: invoice — due Friday ✉\nsecond line\ttab \"quoted\" 'single' \\ back",
		Data: map[string]any{
			"string": "日本語 🚨 \"q\" \n", "float": 95.5, "whole": 95.0, "negative": -0.25, "big": 1e300, "bool": true, "off": false,
			"null": nil, "empty": "", "list": []any{"x", 2.0, nil, true, map[string]any{"deep": []any{1.0}}},
			"nested": map[string]any{"a": map[string]any{"b": map[string]any{"c": "d"}}}, "emptyList": []any{}, "emptyMap": map[string]any{},
		},
	}
	insertEvent(t, h, in)
	got, err := h.Events.GetEvent(ctx, "e1")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != in.ID || got.Source != in.Source || got.Type != in.Type || got.Subject != in.Subject ||
		got.IdempotencyKey != in.IdempotencyKey || !got.Time.Equal(in.Time) {
		t.Fatalf("scalar fields changed:\n got  %+v\n want %+v", got, in)
	}
	if err := equalJSON(in.Data, got.Data); err != nil {
		t.Fatalf("data changed in storage: %v", err)
	}
}

func testEventTime(t *testing.T, h Harness) {
	// Sub-millisecond detail is dropped (the documented resolution)...
	precise := ev("e1", "s", "")
	precise.Time = time.Date(2026, 1, 2, 3, 4, 5, 123_456_789, time.UTC)
	insertEvent(t, h, precise)
	got, _ := h.Events.GetEvent(ctx, "e1")
	if !got.Time.Equal(time.Date(2026, 1, 2, 3, 4, 5, 123_000_000, time.UTC)) {
		t.Errorf("time = %v, want it truncated to the millisecond", got.Time)
	}
	// ...a time in another zone is the same instant, reported in UTC...
	zoned := ev("e2", "s", "")
	zoned.Time = time.Date(2026, 6, 1, 12, 0, 0, 0, time.FixedZone("x", 5*3600+1800))
	insertEvent(t, h, zoned)
	got, _ = h.Events.GetEvent(ctx, "e2")
	if !got.Time.Equal(zoned.Time) || got.Time.Location() != time.UTC {
		t.Errorf("zoned time %v came back as %v (%v)", zoned.Time, got.Time, got.Time.Location())
	}
	// ...and times far from now survive too.
	for i, tm := range []time.Time{time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC), time.Date(2999, 12, 31, 23, 59, 59, 0, time.UTC)} {
		e := ev(fmt.Sprintf("far%d", i), "s", "")
		e.Time = tm
		insertEvent(t, h, e)
		if got, _ := h.Events.GetEvent(ctx, e.ID); !got.Time.Equal(tm) {
			t.Errorf("time %v came back as %v", tm, got.Time)
		}
	}
}

func testEventNoData(t *testing.T, h Harness) {
	for i, data := range []map[string]any{nil, {}} {
		e := ev(fmt.Sprintf("e%d", i), "s", "")
		e.Data = data
		insertEvent(t, h, e)
		got, err := h.Events.GetEvent(ctx, e.ID)
		if err != nil || got.Data == nil || len(got.Data) != 0 {
			t.Errorf("data %#v came back as %#v (err %v); rules index into it, so it must be an empty non-nil map", data, got.Data, err)
		}
	}
}

func testEventHostileText(t *testing.T, h Harness) {
	nasty := `'); DROP TABLE events; -- " \ % _ ? $1 ` + "\x00\n\r\t 日本"
	e := sdk.Event{ID: "e1", Source: "s", Type: "t", Subject: nasty, Time: ms, IdempotencyKey: nasty,
		Data: map[string]any{nasty: nasty, "list": []any{nasty}}}
	insertEvent(t, h, e)
	got, err := h.Events.GetEvent(ctx, "e1")
	if err != nil || got.Subject != nasty || got.IdempotencyKey != nasty || got.Data[nasty] != nasty {
		t.Fatalf("hostile text was altered: %+v %v", got, err)
	}
	// and the store is still intact afterwards
	insertEvent(t, h, ev("e2", "s", ""))
}

func testEventDedupe(t *testing.T, h Harness) {
	first := ev("e1", "monitor", "same")
	first.Data = map[string]any{"v": "first"}
	insertEvent(t, h, first)

	second := ev("e2", "monitor", "same")
	second.Data = map[string]any{"v": "second"}
	id, created, err := h.Events.InsertEvent(ctx, second, ms)
	if err != nil || created || id != "e1" {
		t.Fatalf("duplicate: id=%q created=%v err=%v; want the original id and created=false", id, created, err)
	}
	if _, err := h.Events.GetEvent(ctx, "e2"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("the duplicate must not be stored (err=%v)", err)
	}
	if got, _ := h.Events.GetEvent(ctx, "e1"); got.Data["v"] != "first" {
		t.Fatalf("a duplicate must not overwrite the original: %+v", got.Data)
	}

	// scoped to the source; absent or empty keys never collide
	insertEvent(t, h, ev("e3", "other", "same"))
	insertEvent(t, h, ev("e4", "monitor", ""))
	insertEvent(t, h, ev("e5", "monitor", ""))
	// keys are exact strings
	insertEvent(t, h, ev("e6", "monitor", "SAME"))
	insertEvent(t, h, ev("e7", "monitor", "same "))
	insertEvent(t, h, ev("e8", "monitor", strings.Repeat("k", 256)))
	if _, created, _ := h.Events.InsertEvent(ctx, ev("e9", "monitor", strings.Repeat("k", 256)), ms); created {
		t.Fatal("long keys must dedupe too")
	}
}

func testEventDedupeConcurrent(t *testing.T, h Harness) {
	const n = 24
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		created int
		ids     = map[string]bool{}
	)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, c, err := h.Events.InsertEvent(ctx, ev(fmt.Sprintf("e%d", i), "src", "race"), ms)
			if err != nil {
				t.Errorf("insert: %v", err)
				return
			}
			mu.Lock()
			ids[id] = true
			if c {
				created++
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	if created != 1 || len(ids) != 1 {
		t.Fatalf("created=%d distinct ids=%d among %d racing inserts; exactly one must win and all must agree on it", created, len(ids), n)
	}
}

func testEventConcurrentDistinct(t *testing.T, h Harness) {
	const n = 60
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, c, err := h.Events.InsertEvent(ctx, ev(fmt.Sprintf("e%03d", i), "src", fmt.Sprintf("k%d", i)), ms); err != nil || !c {
				t.Errorf("insert %d: created=%v err=%v", i, c, err)
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		if _, err := h.Events.GetEvent(ctx, fmt.Sprintf("e%03d", i)); err != nil {
			t.Errorf("event %d lost: %v", i, err)
		}
	}
}

func testEventIDUnique(t *testing.T, h Harness) {
	insertEvent(t, h, ev("e1", "s", ""))
	other := ev("e1", "s", "")
	other.Subject = "impostor"
	if _, _, err := h.Events.InsertEvent(ctx, other, ms); err == nil {
		t.Fatal("reusing an event ID must be an error, not a silent overwrite or skip")
	}
	if got, _ := h.Events.GetEvent(ctx, "e1"); got.Subject != "/var" {
		t.Fatalf("the original was modified: %+v", got)
	}
}

func testEventNotFound(t *testing.T, h Harness) {
	for _, id := range []string{"nope", "", "e1 ", "E1"} {
		if _, err := h.Events.GetEvent(ctx, id); !errors.Is(err, engine.ErrNotFound) {
			t.Errorf("GetEvent(%q) = %v, want ErrNotFound", id, err)
		}
	}
	insertEvent(t, h, ev("e1", "s", ""))
	if _, err := h.Events.GetEvent(ctx, "E1"); !errors.Is(err, engine.ErrNotFound) {
		t.Errorf("event IDs are case-sensitive: %v", err)
	}
}

func testEventNoAliasing(t *testing.T, h Harness) {
	in := ev("e1", "s", "")
	in.Data = map[string]any{"list": []any{"a"}, "m": map[string]any{"k": "v"}}
	insertEvent(t, h, in)
	in.Data["list"] = []any{"changed"} // the caller keeps working with its map afterwards
	in.Data["m"].(map[string]any)["k"] = "changed"

	got, _ := h.Events.GetEvent(ctx, "e1")
	if got.Data["list"].([]any)[0] != "a" || got.Data["m"].(map[string]any)["k"] != "v" {
		t.Fatalf("changing the caller's data after insert altered the stored event: %+v", got.Data)
	}
	got.Data["m"].(map[string]any)["k"] = "tampered"
	again, _ := h.Events.GetEvent(ctx, "e1")
	if again.Data["m"].(map[string]any)["k"] != "v" {
		t.Fatal("changing a returned event altered what the next read returns")
	}
}

// ---------------------------------------------------------------------------
// Deliveries

func testDeliveryRoundTrip(t *testing.T, h Harness) {
	insertEvent(t, h, ev("e1", "s", ""))
	d := nd("d1", "e1", "e1:rule:2")
	d.Title = "Disk /var — 95% 🚨\n'quoted' \"double\""
	d.Body = strings.Repeat("long body line\n", 8000) // ~120 KB
	d.Sink = "phone-sink_1"
	d.Rule = "disk alerts (prod)"
	createDelivery(t, h, d)

	got := getDelivery(t, h, "d1")
	if got.ID != "d1" || got.EventID != "e1" || got.Rule != d.Rule || got.ActionIndex != 2 || got.Sink != d.Sink ||
		got.Title != d.Title || got.Body != d.Body || got.IdempotencyKey != "e1:rule:2" || !got.CreatedAt.Equal(ms) {
		t.Fatalf("delivery changed in storage: %+v", got)
	}
	if got.Status != engine.DeliveryPending || got.Attempts != 0 || got.LastError != "" || !got.DeliveredAt.IsZero() {
		t.Fatalf("a new delivery must be pending with no history: %+v", got)
	}
	if !got.UpdatedAt.Equal(ms) {
		t.Fatalf("UpdatedAt = %v, want the creation time", got.UpdatedAt)
	}

	d2 := nd("d2", "e1", "k2")
	d2.Body = "" // body-only and title-only deliveries are legitimate
	createDelivery(t, h, d2)
	if got := getDelivery(t, h, "d2"); got.Body != "" || got.Title != "title" {
		t.Fatalf("%+v", got)
	}
}

func testDeliveryIdempotent(t *testing.T, h Harness) {
	insertEvent(t, h, ev("e1", "s", ""))
	createDelivery(t, h, nd("d1", "e1", "key"))
	record(t, h, attempt("d1", 1, engine.OutcomeRetry, "boom", ""))

	changed := nd("d2", "e1", "key")
	changed.Title, changed.Body, changed.Sink = "other title", "other body", "other-sink"
	for i := 0; i < 3; i++ {
		id, created, err := h.Deliveries.CreateDelivery(ctx, changed)
		if err != nil || created || id != "d1" {
			t.Fatalf("repeat %d: id=%q created=%v err=%v; want the original delivery and created=false", i, id, created, err)
		}
	}
	got := getDelivery(t, h, "d1")
	if got.Title != "title" || got.Sink != "phone" || got.Attempts != 1 || got.Status != engine.DeliveryPending {
		t.Fatalf("a repeated create must change nothing, not even history: %+v", got)
	}
	if _, err := h.Deliveries.GetDelivery(ctx, "d2"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("the duplicate must not exist: %v", err)
	}
	createDelivery(t, h, nd("d3", "e1", "different-key")) // a different key is a different delivery
}

func testDeliveryConcurrent(t *testing.T, h Harness) {
	insertEvent(t, h, ev("e1", "s", ""))
	const n = 24
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		created int
		ids     = map[string]bool{}
	)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, c, err := h.Deliveries.CreateDelivery(ctx, nd(fmt.Sprintf("d%d", i), "e1", "race"))
			if err != nil {
				t.Errorf("create: %v", err)
				return
			}
			mu.Lock()
			ids[id] = true
			if c {
				created++
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	if created != 1 || len(ids) != 1 {
		t.Fatalf("created=%d distinct ids=%d; exactly one create must win", created, len(ids))
	}
}

func testDeliveryNeedsEvent(t *testing.T, h Harness) {
	if _, _, err := h.Deliveries.CreateDelivery(ctx, nd("d1", "ghost", "k")); err == nil {
		t.Fatal("a delivery for an event that does not exist must be rejected")
	}
	if _, err := h.Deliveries.GetDelivery(ctx, "d1"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatal("the rejected delivery must not exist")
	}
	// ...and the rejected key is not burned
	insertEvent(t, h, ev("e1", "s", ""))
	createDelivery(t, h, nd("d1", "e1", "k"))
}

func testDeliveryNotFound(t *testing.T, h Harness) {
	for _, id := range []string{"nope", "", "D1"} {
		if _, err := h.Deliveries.GetDelivery(ctx, id); !errors.Is(err, engine.ErrNotFound) {
			t.Errorf("GetDelivery(%q) = %v, want ErrNotFound", id, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Attempts

func testAttemptStatus(t *testing.T, h Harness) {
	id := newDeliveryWithEvent(t, h)

	record(t, h, attempt(id, 1, engine.OutcomeRetry, "timeout", ""))
	if d := getDelivery(t, h, id); d.Status != engine.DeliveryPending || d.Attempts != 1 || d.LastError != "timeout" || !d.DeliveredAt.IsZero() {
		t.Fatalf("after a retryable failure: %+v", d)
	}
	record(t, h, attempt(id, 2, engine.OutcomeFailed, "HTTP 404", ""))
	if d := getDelivery(t, h, id); d.Status != engine.DeliveryFailed || d.Attempts != 2 || d.LastError != "HTTP 404" {
		t.Fatalf("after giving up: %+v", d)
	}

	// An operator requeues the dead-lettered job and it now succeeds.
	ok := attempt(id, 1, engine.OutcomeDelivered, "", "HTTP 200")
	record(t, h, ok)
	d := getDelivery(t, h, id)
	if d.Status != engine.DeliveryDelivered || d.Attempts != 3 || d.LastError != "" || !d.DeliveredAt.Equal(ok.FinishedAt) || !d.UpdatedAt.Equal(ok.FinishedAt) {
		t.Fatalf("after success: %+v (want delivered, 3 attempts, error cleared, delivered_at = %v)", d, ok.FinishedAt)
	}
}

// Delivery is at-least-once, so two workers can overlap on one delivery. If the
// slower one then fails, its attempt is logged and counted but must not undo
// the success.
func testDeliveredSticky(t *testing.T, h Harness) {
	id := newDeliveryWithEvent(t, h)
	success := attempt(id, 1, engine.OutcomeDelivered, "", "HTTP 200")
	record(t, h, success)

	for i, late := range []engine.Attempt{
		attempt(id, 2, engine.OutcomeRetry, "late failure", ""),
		attempt(id, 3, engine.OutcomeFailed, "later failure", ""),
		attempt(id, 4, engine.OutcomeDelivered, "", "delivered twice"),
	} {
		late.FinishedAt = success.FinishedAt.Add(time.Duration(i+1) * time.Hour)
		record(t, h, late)
		d := getDelivery(t, h, id)
		if d.Status != engine.DeliveryDelivered {
			t.Fatalf("after late %s attempt the status is %s; delivered must be sticky", late.Outcome, d.Status)
		}
		if d.LastError != "" {
			t.Fatalf("a late failure must not resurrect an error on a delivered message: %q", d.LastError)
		}
		if !d.DeliveredAt.Equal(success.FinishedAt) {
			t.Fatalf("delivered_at moved to %v; it must stay at the first success %v", d.DeliveredAt, success.FinishedAt)
		}
		if d.Attempts != i+2 {
			t.Fatalf("attempts = %d, want %d: late attempts are still counted", d.Attempts, i+2)
		}
	}
	if as := attemptsOf(t, h, id); len(as) != 4 {
		t.Fatalf("the log has %d entries, want all 4 including the late ones", len(as))
	}
}

func testAttemptLog(t *testing.T, h Harness) {
	id := newDeliveryWithEvent(t, h)
	insertEvent(t, h, ev("e2", "s", ""))
	createDelivery(t, h, nd("d2", "e2", "k2"))

	want := []engine.Attempt{
		attempt(id, 1, engine.OutcomeRetry, "first: 日本 \"q\"\nline2", ""),
		attempt(id, 2, engine.OutcomeRetry, "second", "some detail"),
		attempt(id, 3, engine.OutcomeDelivered, "", "HTTP 200 OK"),
	}
	for _, a := range want {
		record(t, h, a)
	}
	record(t, h, attempt("d2", 1, engine.OutcomeFailed, "other delivery", ""))

	got := attemptsOf(t, h, id)
	if len(got) != 3 {
		t.Fatalf("%d attempts, want 3 (another delivery's attempts must not leak in)", len(got))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.DeliveryID != w.DeliveryID || g.Number != w.Number || g.Outcome != w.Outcome || g.Error != w.Error || g.Detail != w.Detail ||
			!g.StartedAt.Equal(w.StartedAt) || !g.FinishedAt.Equal(w.FinishedAt) {
			t.Errorf("attempt %d changed in storage:\n got  %+v\n want %+v", i, g, w)
		}
	}
	if others := attemptsOf(t, h, "d2"); len(others) != 1 || others[0].Error != "other delivery" {
		t.Fatalf("d2's log: %+v", others)
	}
	if none := attemptsOf(t, h, "no-such-delivery"); len(none) != 0 {
		t.Fatalf("an unknown delivery has no attempts: %+v", none)
	}
}

func testAttemptRejections(t *testing.T, h Harness) {
	id := newDeliveryWithEvent(t, h)
	record(t, h, attempt(id, 1, engine.OutcomeRetry, "keep", ""))
	before := getDelivery(t, h, id)

	for name, a := range map[string]engine.Attempt{
		"unknown outcome": attempt(id, 2, "maybe", "x", ""),
		"empty outcome":   attempt(id, 2, "", "x", ""),
		"wrong case":      attempt(id, 2, "DELIVERED", "", ""),
	} {
		if err := h.Deliveries.RecordAttempt(ctx, a); err == nil {
			t.Errorf("%s must be rejected", name)
		}
	}
	if err := h.Deliveries.RecordAttempt(ctx, attempt("ghost", 1, engine.OutcomeDelivered, "", "")); !errors.Is(err, engine.ErrNotFound) {
		t.Errorf("an attempt for an unknown delivery: err = %v, want ErrNotFound", err)
	}

	if after := getDelivery(t, h, id); after != before {
		t.Fatalf("rejected attempts changed the delivery:\n before %+v\n after  %+v", before, after)
	}
	if as := attemptsOf(t, h, id); len(as) != 1 {
		t.Fatalf("rejected attempts left %d log entries, want only the original 1", len(as))
	}
	if as := attemptsOf(t, h, "ghost"); len(as) != 0 {
		t.Fatalf("an attempt for an unknown delivery left a log entry behind: %+v", as)
	}
}

func testAttemptHugeError(t *testing.T, h Harness) {
	id := newDeliveryWithEvent(t, h)
	huge := strings.Repeat("e", 1<<20)
	record(t, h, attempt(id, 1, engine.OutcomeRetry, huge, ""))
	d := getDelivery(t, h, id)
	if len(d.LastError) == 0 || !strings.HasPrefix(d.LastError, "eeee") {
		t.Fatalf("a huge error message must be accepted (stored whole or truncated), got %d bytes", len(d.LastError))
	}
}

// After any sequence of attempts, the delivery's counters and status are
// exactly what the log says they should be.
func testAttemptModel(t *testing.T, h Harness) {
	for seed := int64(1); seed <= 5; seed++ {
		eid, did := fmt.Sprintf("e%d", seed), fmt.Sprintf("d%d", seed)
		insertEvent(t, h, ev(eid, "s", ""))
		createDelivery(t, h, nd(did, eid, "k"+did))

		rng := rand.New(rand.NewSource(seed))
		outcomes := []engine.AttemptOutcome{engine.OutcomeRetry, engine.OutcomeFailed, engine.OutcomeDelivered}
		var (
			log         []engine.AttemptOutcome
			delivered   bool
			deliveredAt time.Time
			wantStatus  = engine.DeliveryPending
			wantLastErr string
		)
		for i := 1; i <= 40; i++ {
			o := outcomes[rng.Intn(3)]
			a := attempt(did, i, o, fmt.Sprintf("error %d", i), "")
			if o == engine.OutcomeDelivered {
				a.Error = ""
			}
			record(t, h, a)
			log = append(log, o)

			switch {
			case delivered: // sticky: nothing changes except the counter
			case o == engine.OutcomeDelivered:
				delivered, deliveredAt, wantStatus, wantLastErr = true, a.FinishedAt, engine.DeliveryDelivered, ""
			case o == engine.OutcomeFailed:
				wantStatus, wantLastErr = engine.DeliveryFailed, a.Error
			default:
				wantStatus, wantLastErr = engine.DeliveryPending, a.Error
			}

			d := getDelivery(t, h, did)
			if d.Status != wantStatus || d.LastError != wantLastErr || d.Attempts != len(log) {
				t.Fatalf("seed %d after attempt %d (%v): status=%s last_error=%q attempts=%d; want %s %q %d",
					seed, i, log, d.Status, d.LastError, d.Attempts, wantStatus, wantLastErr, len(log))
			}
			if delivered && !d.DeliveredAt.Equal(deliveredAt) {
				t.Fatalf("seed %d: delivered_at = %v, want the first success %v", seed, d.DeliveredAt, deliveredAt)
			}
			if !delivered && !d.DeliveredAt.IsZero() {
				t.Fatalf("seed %d: delivered_at set before any success", seed)
			}
			if as := attemptsOf(t, h, did); len(as) != d.Attempts {
				t.Fatalf("seed %d: attempts counter says %d but the log holds %d entries", seed, d.Attempts, len(as))
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Listing

func testListFilters(t *testing.T, h Harness) {
	insertEvent(t, h, ev("e1", "s", ""))
	insertEvent(t, h, ev("e2", "s", ""))
	spec := []struct{ id, event string }{{"d1", "e1"}, {"d2", "e1"}, {"d3", "e2"}, {"d4", "e2"}, {"d5", "e2"}}
	for _, s := range spec {
		createDelivery(t, h, nd(s.id, s.event, "k-"+s.id))
	}
	record(t, h, attempt("d2", 1, engine.OutcomeDelivered, "", ""))
	record(t, h, attempt("d4", 1, engine.OutcomeFailed, "x", ""))

	list := func(q engine.DeliveryQuery) string {
		t.Helper()
		ds, err := h.Deliveries.ListDeliveries(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, d := range ds {
			ids = append(ids, d.ID)
		}
		return strings.Join(ids, ",")
	}
	for name, tc := range map[string]struct {
		q    engine.DeliveryQuery
		want string
	}{
		"everything, ordered by id": {engine.DeliveryQuery{}, "d1,d2,d3,d4,d5"},
		"one event":                 {engine.DeliveryQuery{EventID: "e1"}, "d1,d2"},
		"another event":             {engine.DeliveryQuery{EventID: "e2"}, "d3,d4,d5"},
		"unknown event":             {engine.DeliveryQuery{EventID: "ghost"}, ""},
		"delivered":                 {engine.DeliveryQuery{Status: engine.DeliveryDelivered}, "d2"},
		"failed":                    {engine.DeliveryQuery{Status: engine.DeliveryFailed}, "d4"},
		"pending":                   {engine.DeliveryQuery{Status: engine.DeliveryPending}, "d1,d3,d5"},
		"event and status":          {engine.DeliveryQuery{EventID: "e2", Status: engine.DeliveryPending}, "d3,d5"},
		"event and status, no hits": {engine.DeliveryQuery{EventID: "e1", Status: engine.DeliveryFailed}, ""},
		"after a cursor":            {engine.DeliveryQuery{AfterID: "d2"}, "d3,d4,d5"},
		"cursor beyond the end":     {engine.DeliveryQuery{AfterID: "d9"}, ""},
		"cursor with a filter":      {engine.DeliveryQuery{AfterID: "d1", EventID: "e1"}, "d2"},
		"limit":                     {engine.DeliveryQuery{Limit: 2}, "d1,d2"},
		"limit and cursor":          {engine.DeliveryQuery{Limit: 2, AfterID: "d2"}, "d3,d4"},
	} {
		if got := list(tc.q); got != tc.want {
			t.Errorf("%s: got [%s], want [%s]", name, got, tc.want)
		}
	}
}

func testListPagination(t *testing.T, h Harness) {
	insertEvent(t, h, ev("e1", "s", ""))
	const total = 37
	for i := 0; i < total; i++ {
		createDelivery(t, h, nd(fmt.Sprintf("d%03d", i), "e1", fmt.Sprintf("k%d", i)))
	}
	for _, size := range []int{1, 2, 5, 10, 36, 37, 38, 200} {
		var got []string
		after := ""
		for pages := 0; pages < 100; pages++ {
			page, err := h.Deliveries.ListDeliveries(ctx, engine.DeliveryQuery{Limit: size, AfterID: after})
			if err != nil {
				t.Fatal(err)
			}
			if len(page) == 0 {
				break
			}
			if len(page) > size {
				t.Fatalf("page size %d returned %d rows", size, len(page))
			}
			for _, d := range page {
				got = append(got, d.ID)
			}
			after = page[len(page)-1].ID
		}
		if len(got) != total || !sort.StringsAreSorted(got) {
			t.Fatalf("page size %d: got %d ids (sorted=%v), want %d in order", size, len(got), sort.StringsAreSorted(got), total)
		}
		for i, id := range got {
			if id != fmt.Sprintf("d%03d", i) {
				t.Fatalf("page size %d: position %d holds %s: a delivery was skipped or repeated", size, i, id)
			}
		}
	}
}

func testListLimits(t *testing.T, h Harness) {
	insertEvent(t, h, ev("e1", "s", ""))
	for i := 0; i < 60; i++ {
		createDelivery(t, h, nd(fmt.Sprintf("d%03d", i), "e1", fmt.Sprintf("k%d", i)))
	}
	list := func(limit int) int {
		t.Helper()
		ds, err := h.Deliveries.ListDeliveries(ctx, engine.DeliveryQuery{Limit: limit})
		if err != nil {
			t.Fatalf("limit %d: %v", limit, err)
		}
		return len(ds)
	}
	if n := list(0); n != 50 {
		t.Errorf("no limit should mean the default page of 50, got %d", n)
	}
	if n := list(-5); n != 50 {
		t.Errorf("a negative limit should mean the default page of 50, got %d", n)
	}
	if n := list(1_000_000); n != 60 {
		t.Errorf("an enormous limit must be clamped, not rejected (got %d rows)", n)
	}
	if ds, err := h.Deliveries.ListDeliveries(ctx, engine.DeliveryQuery{EventID: "none"}); err != nil || len(ds) != 0 {
		t.Errorf("empty result: %v %v", ds, err)
	}
}

// ---------------------------------------------------------------------------
// Transactions

func testTransactions(t *testing.T, h Harness) {
	boom := errors.New("rollback")

	err := h.InTx(ctx, func(c context.Context) error {
		if _, created, err := h.Events.InsertEvent(c, ev("e1", "s", "key"), ms); err != nil || !created {
			t.Errorf("insert in tx: %v %v", created, err)
		}
		if _, created, err := h.Deliveries.CreateDelivery(c, nd("d1", "e1", "dk")); err != nil || !created {
			t.Errorf("create in tx: %v %v", created, err)
		}
		if err := h.Deliveries.RecordAttempt(c, attempt("d1", 1, engine.OutcomeRetry, "x", "")); err != nil {
			t.Errorf("record in tx: %v", err)
		}
		// visible to the transaction itself...
		if _, err := h.Events.GetEvent(c, "e1"); err != nil {
			t.Errorf("an event is not visible inside its own transaction: %v", err)
		}
		if d, err := h.Deliveries.GetDelivery(c, "d1"); err != nil || d.Attempts != 1 {
			t.Errorf("delivery inside its transaction: %+v %v", d, err)
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	// ...and gone afterwards, including idempotency keys.
	if _, err := h.Events.GetEvent(ctx, "e1"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("rolled-back event survives: %v", err)
	}
	if _, err := h.Deliveries.GetDelivery(ctx, "d1"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("rolled-back delivery survives: %v", err)
	}
	if as := attemptsOf(t, h, "d1"); len(as) != 0 {
		t.Fatalf("rolled-back attempts survive: %+v", as)
	}
	if _, created, err := h.Events.InsertEvent(ctx, ev("e2", "s", "key"), ms); err != nil || !created {
		t.Fatalf("the idempotency key leaked out of the rollback: created=%v err=%v", created, err)
	}

	// RecordAttempt joins an outer transaction: undone when the outer one is.
	insertEvent(t, h, ev("e3", "s", ""))
	createDelivery(t, h, nd("d3", "e3", "k3"))
	_ = h.InTx(ctx, func(c context.Context) error {
		_ = h.Deliveries.RecordAttempt(c, attempt("d3", 1, engine.OutcomeDelivered, "", ""))
		return boom
	})
	if d := getDelivery(t, h, "d3"); d.Status != engine.DeliveryPending || d.Attempts != 0 {
		t.Fatalf("RecordAttempt did not roll back with its transaction: %+v", d)
	}

	// Committing persists everything together.
	if err := h.InTx(ctx, func(c context.Context) error {
		if _, _, err := h.Events.InsertEvent(c, ev("e4", "s", ""), ms); err != nil {
			return err
		}
		_, _, err := h.Deliveries.CreateDelivery(c, nd("d4", "e4", "k4"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	getDelivery(t, h, "d4")
}

// ---------------------------------------------------------------------------

// equalJSON compares two JSON-like values structurally.
func equalJSON(a, b any) error {
	switch x := a.(type) {
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return fmt.Errorf("map mismatch: %v vs %v", a, b)
		}
		for k, v := range x {
			w, present := y[k]
			if !present {
				return fmt.Errorf("key %q missing", k)
			}
			if err := equalJSON(v, w); err != nil {
				return fmt.Errorf("%q: %w", k, err)
			}
		}
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return fmt.Errorf("list mismatch: %v vs %v", a, b)
		}
		for i := range x {
			if err := equalJSON(x[i], y[i]); err != nil {
				return fmt.Errorf("[%d]: %w", i, err)
			}
		}
	default:
		if a != b {
			return fmt.Errorf("%#v != %#v", a, b)
		}
	}
	return nil
}
