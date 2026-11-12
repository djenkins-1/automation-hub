package sinktest_test

// Specification of the Recorder test double: tests rely on it being honest.
//
//  - It remembers EVERY call, including the ones it was told to fail, in the
//    order they arrived, and counts them correctly under concurrency.
//  - Behavior sees 1-based call numbers and decides success or failure.
//  - What it hands back cannot be used to tamper with its record.

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/djenkins-1/automation-hub/internal/sinks/sinktest"
	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

var bg = context.Background()

func TestARecorderWithNoBehaviorAlwaysSucceeds(t *testing.T) {
	r := &sinktest.Recorder{}
	res, err := r.Deliver(bg, sdk.Notification{Title: "t"})
	if err != nil || res.Detail == "" {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

func TestEveryCallIsRecordedInOrderEvenWhenItFails(t *testing.T) {
	boom := errors.New("boom")
	r := &sinktest.Recorder{Behavior: func(n sdk.Notification, call int) error {
		if call%2 == 0 {
			return boom
		}
		return nil
	}}
	for i := 1; i <= 5; i++ {
		_, err := r.Deliver(bg, sdk.Notification{Title: string(rune('a' + i - 1)), Attempt: i})
		if (i%2 == 0) != (err != nil) {
			t.Fatalf("call %d: err = %v", i, err)
		}
		if err != nil && !errors.Is(err, boom) {
			t.Fatalf("call %d: the behavior's own error must come back unchanged, got %v", i, err)
		}
	}
	if r.Count() != 5 {
		t.Fatalf("Count = %d, want 5 (failed calls count)", r.Count())
	}
	for i, n := range r.Notifications() {
		if n.Title != string(rune('a'+i)) || n.Attempt != i+1 {
			t.Fatalf("record %d = %+v: calls must be stored in arrival order", i, n)
		}
	}
}

func TestBehaviorSeesCallNumbersStartingAtOne(t *testing.T) {
	var seen []int
	r := &sinktest.Recorder{Behavior: func(_ sdk.Notification, call int) error { seen = append(seen, call); return nil }}
	for i := 0; i < 3; i++ {
		_, _ = r.Deliver(bg, sdk.Notification{})
	}
	if len(seen) != 3 || seen[0] != 1 || seen[1] != 2 || seen[2] != 3 {
		t.Fatalf("call numbers = %v", seen)
	}
}

func TestBehaviorCanBeChangedBetweenCalls(t *testing.T) {
	r := &sinktest.Recorder{Behavior: func(sdk.Notification, int) error { return errors.New("down") }}
	if _, err := r.Deliver(bg, sdk.Notification{}); err == nil {
		t.Fatal("expected a failure")
	}
	r.Behavior = nil
	if _, err := r.Deliver(bg, sdk.Notification{}); err != nil {
		t.Fatalf("after clearing Behavior: %v", err)
	}
	if r.Count() != 2 {
		t.Fatalf("Count = %d", r.Count())
	}
}

func TestTheRecorderCountsCorrectlyUnderConcurrency(t *testing.T) {
	r := &sinktest.Recorder{}
	var wg sync.WaitGroup
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				_, _ = r.Deliver(bg, sdk.Notification{})
			}
		}()
	}
	wg.Wait()
	if r.Count() != 1000 || len(r.Notifications()) != 1000 {
		t.Fatalf("Count=%d Notifications=%d, want 1000", r.Count(), len(r.Notifications()))
	}
}

func TestTheReturnedSliceCannotTamperWithTheRecord(t *testing.T) {
	r := &sinktest.Recorder{}
	_, _ = r.Deliver(bg, sdk.Notification{Title: "original"})
	got := r.Notifications()
	got[0].Title = "tampered"
	_ = append(got, sdk.Notification{Title: "extra"})
	again := r.Notifications()
	if len(again) != 1 || again[0].Title != "original" {
		t.Fatalf("the record was changed through a returned slice: %+v", again)
	}
}

func TestAnEmptyRecorderReportsNothing(t *testing.T) {
	r := &sinktest.Recorder{}
	if r.Count() != 0 || len(r.Notifications()) != 0 {
		t.Fatal("a new recorder has recorded nothing")
	}
}
