package sinks

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

// stdout prints each notification as one line. It is the reference sink: the
// simplest thing that implements sdk.Sink, and the quickest way to check that
// a rule does what you think before wiring up a real destination.
type stdout struct {
	mu sync.Mutex
	w  io.Writer
}

// NewStdout returns a sink that writes to w.
func NewStdout(w io.Writer) sdk.Sink { return &stdout{w: w} }

func newStdout(options map[string]any) (sdk.Sink, error) {
	var none struct{} // the stdout sink takes no options
	if err := DecodeOptions(options, &none); err != nil {
		return nil, err
	}
	return NewStdout(os.Stdout), nil
}

var oneLine = strings.NewReplacer("\r\n", " ⏎ ", "\n", " ⏎ ", "\r", " ⏎ ")

func (s *stdout) Deliver(ctx context.Context, n sdk.Notification) (sdk.Result, error) {
	line := fmt.Sprintf("notification: %s | %s  [source=%s type=%s event=%s delivery=%s attempt=%d]\n",
		oneLine.Replace(n.Title), oneLine.Replace(n.Body),
		n.Event.Source, n.Event.Type, n.Event.ID, n.ID, n.Attempt)

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := io.WriteString(s.w, line); err != nil {
		return sdk.Result{}, err // a failed write is worth retrying
	}
	return sdk.Result{Detail: "written to stdout"}, nil
}
