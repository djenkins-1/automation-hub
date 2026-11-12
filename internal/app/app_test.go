package app_test

// End-to-end specification of the hub, through its public surface only:
// build from a config, feed events in, observe the sinks and the database the
// way an operator would (a second handle on the same file).
//
//  - A matching event reaches its sink exactly once, rendered; others do not.
//  - Failures follow their classification: flaky sinks recover, permanent
//    failures dead-letter (and can be requeued), retries are bounded.
//  - Nothing is lost: events accepted while the hub was down, or whose
//    worker crashed, are processed later. Nothing is duplicated: producer
//    retries and concurrent producers deliver once each.
//  - One sink's failure never affects another sink's delivery.
//  - Misconfiguration is refused at startup; shutdown is prompt.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/djenkins-1/automation-hub/internal/app"
	"github.com/djenkins-1/automation-hub/internal/config"
	"github.com/djenkins-1/automation-hub/internal/engine"
	"github.com/djenkins-1/automation-hub/internal/queue"
	"github.com/djenkins-1/automation-hub/internal/sinks"
	"github.com/djenkins-1/automation-hub/internal/sinks/sinktest"
	"github.com/djenkins-1/automation-hub/internal/store"
	"github.com/djenkins-1/automation-hub/internal/store/sqlite"
	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

var (
	bg    = context.Background()
	quiet = slog.New(slog.NewTextHandler(io.Discard, nil))
)

func yamlConfig(t *testing.T, path string, maxAttempts int, extra string) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(fmt.Sprintf(`
storage: { path: %q }
queue: { workers: 3, lease: 2s, poll_every: 10ms, max_attempts: %d, retry_base: 5ms, retry_max: 10ms }
log: { level: error }
sinks:
  - { name: test, type: stdout }
  - { name: other, type: stdout }
rules:
  - name: disk
    match: { source: monitor, type: "disk.*", when: "data.percent > 90" }
    actions:
      - { sink: test, title: "Disk {{.Event.Subject}}", body: "{{.Event.Data.percent}}%% used" }
%s`, path, maxAttempts, extra)))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func registry(m map[string]sdk.Sink) app.Option { return app.WithSinks(sinks.NewRegistry(m)) }

// run starts the hub's worker and stops it (and closes the app) at test end.
func run(t *testing.T, a *app.App) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(bg)
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Run: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("Run did not return after cancellation")
			}
		})
	}
	t.Cleanup(func() { stop(); _ = a.Close() })
	return stop
}

type hub struct {
	t          *testing.T
	app        *app.App
	deliveries engine.DeliveryStore
	admin      queue.Admin
	db         *store.DB
}

// startHub builds, runs and observes a hub. The observer is a separate handle
// on the same database file, like an operator running a query.
func startHub(t *testing.T, maxAttempts int, sinkMap map[string]sdk.Sink) *hub {
	t.Helper()
	h := newHub(t, filepath.Join(t.TempDir(), "hub.db"), maxAttempts, sinkMap, "")
	run(t, h.app)
	return h
}

func newHub(t *testing.T, path string, maxAttempts int, sinkMap map[string]sdk.Sink, extra string) *hub {
	t.Helper()
	a, err := app.New(bg, yamlConfig(t, path, maxAttempts, extra), app.WithLogger(quiet), registry(sinkMap))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return &hub{t: t, app: a, deliveries: nil}
}

func observe(t *testing.T, path string) *hub {
	t.Helper()
	db, err := store.Open(bg, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &hub{t: t, deliveries: sqlite.NewDeliveries(db), admin: queue.NewSQLite(db, queue.Options{}), db: db}
}

func disk(percent float64) sdk.Event {
	return sdk.Event{Source: "monitor", Type: "disk.full", Subject: "/var", Data: map[string]any{"percent": percent}}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func (o *hub) deliveriesOf(eventID string) []engine.Delivery {
	o.t.Helper()
	ds, err := o.deliveries.ListDeliveries(bg, engine.DeliveryQuery{EventID: eventID})
	if err != nil {
		o.t.Fatal(err)
	}
	return ds
}

func (o *hub) waitStatus(eventID string, status engine.DeliveryStatus) engine.Delivery {
	o.t.Helper()
	var d engine.Delivery
	eventually(o.t, fmt.Sprintf("event %s to have one %s delivery", eventID, status), func() bool {
		ds := o.deliveriesOf(eventID)
		if len(ds) == 1 && ds[0].Status == status {
			d = ds[0]
			return true
		}
		return false
	})
	return d
}

func (o *hub) waitIdle() {
	o.t.Helper()
	eventually(o.t, "no job to be ready or leased", func() bool {
		var n int
		_ = o.db.Read.QueryRowContext(bg, `SELECT COUNT(*) FROM jobs WHERE state IN ('ready','leased')`).Scan(&n)
		return n == 0
	})
}

func accept(t *testing.T, a *app.App, e sdk.Event) engine.IngestResult {
	t.Helper()
	res, err := a.Ingest().Accept(bg, e)
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	return res
}

// start wires a hub with sinks and returns it plus an observer.
func start(t *testing.T, maxAttempts int, sinkMap map[string]sdk.Sink) (*app.App, *hub, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "hub.db")
	h := newHub(t, path, maxAttempts, sinkMap, "")
	run(t, h.app)
	return h.app, observe(t, path), path
}

// ---------------------------------------------------------------------------

func TestAMatchingEventReachesItsSinkExactlyOnceRendered(t *testing.T) {
	rec := &sinktest.Recorder{}
	a, o, _ := start(t, 4, map[string]sdk.Sink{"test": rec, "other": &sinktest.Recorder{}})

	res := accept(t, a, disk(95))
	d := o.waitStatus(res.EventID, engine.DeliveryDelivered)
	o.waitIdle()

	if d.Title != "Disk /var" || d.Body != "95% used" || d.Attempts != 1 {
		t.Fatalf("delivery = %+v", d)
	}
	got := rec.Notifications()
	if len(got) != 1 || got[0].Title != "Disk /var" || got[0].Body != "95% used" || got[0].Event.ID != res.EventID {
		t.Fatalf("the sink received %+v", got)
	}
}

func TestEventsThatMatchNothingAreAbsorbedQuietly(t *testing.T) {
	rec := &sinktest.Recorder{}
	a, o, _ := start(t, 4, map[string]sdk.Sink{"test": rec, "other": &sinktest.Recorder{}})

	wrongSource := disk(99)
	wrongSource.Source = "someone-else"
	var ids []string
	for _, e := range []sdk.Event{disk(50), wrongSource, {Source: "monitor", Type: "cpu.high", Data: map[string]any{"percent": 99.0}}, {Source: "monitor", Type: "disk.full"}} {
		ids = append(ids, accept(t, a, e).EventID) // the last has no data at all: the rule cannot even be evaluated
	}
	o.waitIdle()
	for _, id := range ids {
		if n := len(o.deliveriesOf(id)); n != 0 {
			t.Fatalf("event %s produced %d deliveries", id, n)
		}
	}
	if rec.Count() != 0 {
		t.Fatalf("the sink was called %d times", rec.Count())
	}
}

func TestAFlakySinkRecoversAndRetriesKeepTheirIdentity(t *testing.T) {
	rec := &sinktest.Recorder{Behavior: func(_ sdk.Notification, call int) error {
		if call <= 2 {
			return errors.New("temporarily down")
		}
		return nil
	}}
	a, o, _ := start(t, 6, map[string]sdk.Sink{"test": rec, "other": &sinktest.Recorder{}})
	res := accept(t, a, disk(95))
	d := o.waitStatus(res.EventID, engine.DeliveryDelivered)
	o.waitIdle()

	if d.Attempts != 3 || rec.Count() != 3 {
		t.Fatalf("attempts=%d sink calls=%d, want 3", d.Attempts, rec.Count())
	}
	as, _ := o.deliveries.ListAttempts(bg, d.ID)
	if len(as) != 3 || as[0].Outcome != engine.OutcomeRetry || as[2].Outcome != engine.OutcomeDelivered {
		t.Fatalf("attempt log = %+v", as)
	}
	keys := map[string]bool{}
	for _, n := range rec.Notifications() {
		keys[n.IdempotencyKey] = true
	}
	if len(keys) != 1 {
		t.Fatalf("the idempotency key changed between retries: %v", keys)
	}
}

func TestPermanentFailuresAreDeadLetteredOnceAndCanBeRequeued(t *testing.T) {
	rec := &sinktest.Recorder{Behavior: func(sdk.Notification, int) error { return sdk.Permanent(errors.New("HTTP 401")) }}
	a, o, _ := start(t, 6, map[string]sdk.Sink{"test": rec, "other": &sinktest.Recorder{}})
	res := accept(t, a, disk(95))
	d := o.waitStatus(res.EventID, engine.DeliveryFailed)
	o.waitIdle()
	if rec.Count() != 1 || d.Attempts != 1 || d.LastError != "HTTP 401" {
		t.Fatalf("a permanent error must not be retried: sink calls=%d delivery=%+v", rec.Count(), d)
	}
	dead, err := o.admin.ListDead(bg, 10, "")
	if err != nil || len(dead) != 1 || dead[0].Kind != engine.KindDeliver {
		t.Fatalf("dead letters = %+v %v", dead, err)
	}

	rec.Behavior = nil // credentials fixed; the operator requeues
	if err := o.admin.Requeue(bg, dead[0].ID); err != nil {
		t.Fatal(err)
	}
	d = o.waitStatus(res.EventID, engine.DeliveryDelivered)
	if d.Attempts != 2 || rec.Count() != 2 {
		t.Fatalf("after requeue: attempts=%d sink calls=%d", d.Attempts, rec.Count())
	}
}

func TestRetriesAreBounded(t *testing.T) {
	rec := &sinktest.Recorder{Behavior: func(sdk.Notification, int) error { return errors.New("down") }}
	a, o, _ := start(t, 3, map[string]sdk.Sink{"test": rec, "other": &sinktest.Recorder{}})
	res := accept(t, a, disk(95))
	d := o.waitStatus(res.EventID, engine.DeliveryFailed)
	o.waitIdle()
	time.Sleep(100 * time.Millisecond) // and it stays given up on
	if d.Attempts != 3 || rec.Count() != 3 {
		t.Fatalf("attempts=%d sink calls=%d, want exactly the configured 3", d.Attempts, rec.Count())
	}
	if dead, _ := o.admin.ListDead(bg, 10, ""); len(dead) != 1 {
		t.Fatalf("dead letters = %+v", dead)
	}
}

func TestProducerRetriesAndConcurrentProducersDeliverOnceEach(t *testing.T) {
	rec := &sinktest.Recorder{}
	a, o, _ := start(t, 4, map[string]sdk.Sink{"test": rec, "other": &sinktest.Recorder{}})

	dup := disk(95)
	dup.IdempotencyKey = "nightly"
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		fresh  int
		dupIDs = map[string]bool{}
	)
	for i := 0; i < 10; i++ { // a producer that retries aggressively, concurrently
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := a.Ingest().Accept(bg, dup)
			if err != nil {
				t.Errorf("accept: %v", err)
				return
			}
			mu.Lock()
			dupIDs[res.EventID] = true
			if !res.Duplicate {
				fresh++
			}
			mu.Unlock()
		}()
	}
	const n = 30
	ids := make(chan string, n)
	for i := 0; i < n; i++ { // and unrelated producers at the same time
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e := disk(float64(91 + i%9))
			e.Subject = fmt.Sprintf("/mnt/%d", i)
			ids <- accept(t, a, e).EventID
		}(i)
	}
	wg.Wait()
	close(ids)

	if fresh != 1 || len(dupIDs) != 1 {
		t.Fatalf("fresh=%d ids=%d: ten racing retries must collapse into one event", fresh, len(dupIDs))
	}
	for id := range dupIDs {
		o.waitStatus(id, engine.DeliveryDelivered)
	}
	for id := range ids {
		o.waitStatus(id, engine.DeliveryDelivered)
	}
	o.waitIdle()
	if rec.Count() != n+1 {
		t.Fatalf("the sink was called %d times, want %d (one per logical event, none lost, none doubled)", rec.Count(), n+1)
	}
}

func TestEventsAcceptedWhileTheHubWasStoppedAreProcessedOnRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hub.db")
	rec := &sinktest.Recorder{}
	sinkMap := map[string]sdk.Sink{"test": rec, "other": &sinktest.Recorder{}}

	down := newHub(t, path, 4, sinkMap, "") // built but never run: events are accepted, nothing processes them
	var ids []string
	for i := 0; i < 5; i++ {
		ids = append(ids, accept(t, down.app, disk(float64(92+i))).EventID)
	}
	if err := down.app.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if rec.Count() != 0 {
		t.Fatal("nothing should have been delivered while the hub was not running")
	}

	up := newHub(t, path, 4, sinkMap, "")
	run(t, up.app)
	o := observe(t, path)
	for _, id := range ids {
		o.waitStatus(id, engine.DeliveryDelivered)
	}
	o.waitIdle()
	if rec.Count() != 5 {
		t.Fatalf("sink calls = %d, want 5: accepted events must survive a restart", rec.Count())
	}
}

func TestWorkAbandonedByACrashedWorkerIsRecoveredOnStartup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hub.db")
	rec := &sinktest.Recorder{}
	sinkMap := map[string]sdk.Sink{"test": rec, "other": &sinktest.Recorder{}}

	h := newHub(t, path, 4, sinkMap, "")
	res := accept(t, h.app, disk(95))
	// A previous process leased the routing job and died, leaving a short lease behind.
	o := observe(t, path)
	q := queue.NewSQLite(o.db, queue.Options{})
	if jobs, err := q.Lease(bg, "crashed-process", []string{engine.KindRoute}, 1, 30*time.Millisecond); err != nil || len(jobs) != 1 {
		t.Fatalf("setup lease: %v %d", err, len(jobs))
	}
	time.Sleep(80 * time.Millisecond)

	run(t, h.app)
	o.waitStatus(res.EventID, engine.DeliveryDelivered)
	if rec.Count() != 1 {
		t.Fatalf("sink calls = %d", rec.Count())
	}
}

func TestOneSinksFailureNeverAffectsAnotherSinksDelivery(t *testing.T) {
	good := &sinktest.Recorder{}
	bad := &sinktest.Recorder{Behavior: func(sdk.Notification, int) error { return sdk.Permanent(errors.New("always broken")) }}
	path := filepath.Join(t.TempDir(), "hub.db")
	two := `  - name: both
    match: { type: "disk.*" }
    actions:
      - { sink: test, title: "to good" }
      - { sink: other, title: "to bad" }
`
	h := newHub(t, path, 4, map[string]sdk.Sink{"test": good, "other": bad}, two)
	run(t, h.app)
	o := observe(t, path)

	res := accept(t, h.app, disk(95)) // matches both the disk rule and "both": 3 deliveries
	eventually(t, "all three deliveries to settle", func() bool {
		ds := o.deliveriesOf(res.EventID)
		if len(ds) != 3 {
			return false
		}
		for _, d := range ds {
			if d.Status == engine.DeliveryPending {
				return false
			}
		}
		return true
	})
	status := map[string]engine.DeliveryStatus{}
	for _, d := range o.deliveriesOf(res.EventID) {
		status[d.Sink+"/"+d.Title] = d.Status
	}
	if status["test/to good"] != engine.DeliveryDelivered || status["test/Disk /var"] != engine.DeliveryDelivered || status["other/to bad"] != engine.DeliveryFailed {
		t.Fatalf("statuses = %v", status)
	}
}

func TestTheShippedExampleConfigRunsEndToEnd(t *testing.T) {
	t.Setenv("HUB_TOKEN", "example-test-token") // the example reads its token from the environment
	cfg, err := config.Load("../../hub.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "hub.db")
	cfg.Storage.Path = path
	cfg.Queue.PollEvery = 10 * time.Millisecond
	a, err := app.New(bg, cfg, app.WithLogger(quiet)) // the real stdout sink
	if err != nil {
		t.Fatal(err)
	}
	run(t, a)
	o := observe(t, path)

	// Both example rules fire for a full disk; only the catch-all fires for an
	// event the disk rule does not cover, or one without data.percent.
	type msg struct{ title, body string }
	for _, tc := range []struct {
		name string
		ev   sdk.Event
		want map[string]msg // by rule
	}{
		{"full disk", sdk.Event{Source: "monitor", Type: "disk.full", Subject: "/var", Data: map[string]any{"percent": 97.0}},
			map[string]msg{"disk-alerts": {"Disk /var", "97% used"}, "log-everything": {"monitor: disk.full", "/var"}}},
		{"other event", sdk.Event{Source: "cron", Type: "backup.failed"},
			map[string]msg{"log-everything": {"cron: backup.failed", ""}}},
		{"disk event without percent", sdk.Event{Source: "monitor", Type: "disk.full", Subject: "/home"},
			map[string]msg{"log-everything": {"monitor: disk.full", "/home"}}},
	} {
		id := accept(t, a, tc.ev).EventID
		o.waitIdle()
		got := map[string]msg{}
		for _, d := range o.deliveriesOf(id) {
			if d.Status != engine.DeliveryDelivered {
				t.Errorf("%s: delivery for rule %s is %s, want delivered", tc.name, d.Rule, d.Status)
			}
			got[d.Rule] = msg{d.Title, d.Body}
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: deliveries = %+v, want %+v", tc.name, got, tc.want)
		}
	}
}

func TestMisconfigurationIsRefusedAtStartup(t *testing.T) {
	dir := t.TempDir()
	good := yamlConfig(t, filepath.Join(dir, "ok.db"), 3, "")

	// a rule that delivers to a sink the registry does not have
	_, err := app.New(bg, good, app.WithLogger(quiet), registry(map[string]sdk.Sink{"unrelated": &sinktest.Recorder{}}))
	if err == nil {
		t.Error("a rule delivering to an undefined sink must be refused")
	}
	// an unknown sink type
	bad := yamlConfig(t, filepath.Join(dir, "bad.db"), 3, "")
	bad.Sinks = append(bad.Sinks, sinks.Config{Name: "x", Type: "carrier-pigeon"})
	if _, err := app.New(bg, bad, app.WithLogger(quiet)); err == nil {
		t.Error("an unknown sink type must be refused")
	}
	// a database that cannot be opened
	unwritable := yamlConfig(t, filepath.Join(dir, "no", "such", "dir", "x.db"), 3, "")
	if _, err := app.New(bg, unwritable, app.WithLogger(quiet), registry(map[string]sdk.Sink{"test": &sinktest.Recorder{}, "other": &sinktest.Recorder{}})); err == nil {
		t.Error("an unusable database path must be refused")
	}
}

func TestAnIdleHubStopsPromptlyAndIngestStillWorksAfterwards(t *testing.T) {
	rec := &sinktest.Recorder{}
	path := filepath.Join(t.TempDir(), "hub.db")
	h := newHub(t, path, 4, map[string]sdk.Sink{"test": rec, "other": &sinktest.Recorder{}}, "")
	stop := run(t, h.app)
	time.Sleep(50 * time.Millisecond)

	began := time.Now()
	stop()
	if d := time.Since(began); d > 2*time.Second {
		t.Fatalf("an idle hub took %v to stop", d)
	}
	// Accepting is just a durable write, so it keeps working; processing resumes on restart.
	res := accept(t, h.app, disk(95))
	time.Sleep(50 * time.Millisecond)
	if rec.Count() != 0 {
		t.Fatal("a stopped hub must not deliver")
	}
	if d := observe(t, path).deliveriesOf(res.EventID); len(d) != 0 {
		t.Fatalf("a stopped hub must not route: %+v", d)
	}
}
