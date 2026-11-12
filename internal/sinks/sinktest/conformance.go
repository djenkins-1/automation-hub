package sinktest

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

// Conformance checks the obligations every sdk.Sink accepts, whatever it talks
// to. Each sink package runs it against itself:
//
//	func TestStdoutConforms(t *testing.T) {
//		sinktest.Conformance(t, func(t *testing.T) sdk.Sink { return sinks.NewStdout(io.Discard) })
//	}
//
// The sink must be pointed at something harmless (a local test server, a
// buffer); the suite only checks behaviour a sink can show without a real
// destination: it must not panic on odd input, must stop when its context is
// cancelled, must tolerate concurrent use, and must leave the notification (and
// the event inside it) exactly as it found them.
func Conformance(t *testing.T, newSink func(t *testing.T) sdk.Sink) {
	t.Helper()

	t.Run("DoesNotPanicOnSparseNotifications", func(t *testing.T) {
		s := newSink(t)
		for name, n := range map[string]sdk.Notification{
			"zero value":       {},
			"title only":       {Title: "t"},
			"body only":        {Body: "b"},
			"no event data":    {Title: "t", Body: "b", Event: sdk.Event{ID: "e"}},
			"nil event data":   {Title: "t", Event: sdk.Event{Data: nil}},
			"huge text":        {Title: string(make([]byte, 1<<16)), Body: string(make([]byte, 1<<20))},
			"awkward text":     {Title: "日本語 🚨 \x00 \n \r\n \t", Body: "'\"\\%s${x}{{.y}}<b>&amp;"},
			"zero attempt":     {Title: "t", Attempt: 0},
			"negative attempt": {Title: "t", Attempt: -3},
		} {
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("%s: the sink panicked: %v", name, r)
					}
				}()
				_, _ = s.Deliver(context.Background(), n)
			}()
		}
	})

	t.Run("StopsWhenItsContextIsCancelled", func(t *testing.T) {
		s := newSink(t)
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		done := make(chan error, 1)
		go func() { _, err := s.Deliver(cancelled, sdk.Notification{Title: "t"}); done <- err }()
		select {
		case err := <-done:
			if err == nil {
				t.Error("a cancelled context must produce an error: reporting success would record a delivery that never happened")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the sink ignored a cancelled context and kept going")
		}
	})

	t.Run("IsSafeForConcurrentUse", func(t *testing.T) { // meaningful under -race
		s := newSink(t)
		var wg sync.WaitGroup
		var failures atomic.Int32
		for g := 0; g < 16; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for i := 0; i < 25; i++ {
					if _, err := s.Deliver(context.Background(), sdk.Notification{Title: "t", Body: "b", Attempt: i + 1}); err != nil {
						failures.Add(1)
					}
				}
			}(g)
		}
		wg.Wait()
		if failures.Load() != 0 {
			t.Errorf("%d of 400 concurrent deliveries failed", failures.Load())
		}
	})

	t.Run("LeavesTheNotificationUntouched", func(t *testing.T) {
		s := newSink(t)
		n := sdk.Notification{
			ID: "d1", IdempotencyKey: "k", Title: "t", Body: "b", Attempt: 2,
			Event: sdk.Event{ID: "e", Source: "s", Type: "t", Data: map[string]any{"list": []any{1.0, "x"}, "m": map[string]any{"k": "v"}}},
		}
		before, _ := json.Marshal(n)
		_, _ = s.Deliver(context.Background(), n)
		after, _ := json.Marshal(n)
		if string(before) != string(after) {
			t.Fatalf("the sink modified the notification it was given (events are shared between sinks):\n before %s\n after  %s", before, after)
		}
	})
}
