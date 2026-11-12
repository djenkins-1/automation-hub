package sinktest

import (
	"context"
	"sync"

	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

// Recorder is a fake sdk.Sink for tests. It remembers every notification it
// is given, in arrival order, including the ones it was told to fail. It is
// safe for concurrent use.
//
// The zero value accepts everything. Set Behavior to decide, per call, whether
// a delivery succeeds; set it before the Recorder is shared with goroutines.
type Recorder struct {
	// Behavior is called with each notification and its 1-based call number.
	// A nil Behavior, or a nil return, means success; any error is returned
	// from Deliver unchanged.
	Behavior func(n sdk.Notification, call int) error

	mu   sync.Mutex
	sent []sdk.Notification
}

var _ sdk.Sink = (*Recorder)(nil)

// Deliver implements sdk.Sink.
func (r *Recorder) Deliver(_ context.Context, n sdk.Notification) (sdk.Result, error) {
	r.mu.Lock()
	r.sent = append(r.sent, n)
	call := len(r.sent)
	behavior := r.Behavior
	r.mu.Unlock()

	// Called outside the lock so a slow Behavior does not serialise callers.
	if behavior != nil {
		if err := behavior(n, call); err != nil {
			return sdk.Result{}, err
		}
	}
	return sdk.Result{Detail: "recorded"}, nil
}

// Count returns how many times Deliver has been called.
func (r *Recorder) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sent)
}

// Notifications returns a copy of every notification received, in order.
func (r *Recorder) Notifications() []sdk.Notification {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]sdk.Notification, len(r.sent))
	copy(out, r.sent)
	return out
}
