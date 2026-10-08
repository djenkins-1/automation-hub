package sinks_test

// Specification of the sinks package: building sinks from configuration and the
// reference stdout sink.
//
//  - Build constructs one sink per configured entry, reports EVERY problem at
//    once, and rejects anything ambiguous (duplicate or blank names, unknown
//    types, options the sink does not understand) instead of guessing.
//  - A Registry hands out the same sink instance every time and is safe for
//    concurrent reads; NewRegistry is isolated from the map it was given.
//  - DecodeOptions turns loose config values into a typed struct strictly: a
//    misspelt or mistyped option is an error naming the culprit.
//  - stdout writes exactly one line per notification, whatever the text
//    contains, even under concurrency, and it obeys the Sink contract.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/djenkins-1/automation-hub/internal/sinks"
	"github.com/djenkins-1/automation-hub/internal/sinks/sinktest"
	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

var bg = context.Background()

// ---------------------------------------------------------------------------
// stdout

func TestStdoutConformsToTheSinkContract(t *testing.T) {
	sinktest.Conformance(t, func(t *testing.T) sdk.Sink { return sinks.NewStdout(io.Discard) })
}

func TestStdoutWritesOneInformativeLinePerNotification(t *testing.T) {
	var buf bytes.Buffer
	s := sinks.NewStdout(&buf)
	res, err := s.Deliver(bg, sdk.Notification{
		ID: "d1", Title: "Disk /var", Body: "95% used", Attempt: 2,
		Event: sdk.Event{ID: "e1", Source: "monitor", Type: "disk.full"},
	})
	if err != nil || res.Detail == "" {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	out := buf.String()
	if !strings.HasSuffix(out, "\n") || strings.Count(out, "\n") != 1 {
		t.Fatalf("want exactly one newline-terminated line, got %q", out)
	}
	for _, want := range []string{"Disk /var", "95% used", "source=monitor", "type=disk.full", "event=e1", "delivery=d1", "attempt=2"} {
		if !strings.Contains(out, want) {
			t.Errorf("line %q is missing %q", out, want)
		}
	}
	// two notifications, two lines, in order
	_, _ = s.Deliver(bg, sdk.Notification{Title: "second"})
	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], "Disk /var") || !strings.Contains(lines[1], "second") {
		t.Fatalf("lines = %q", lines)
	}
}

// Titles and bodies come from event data that anyone with a webhook can influence.
// A newline must never let one notification forge or hide another in the log.
func TestStdoutCannotBeUsedToForgeLogLines(t *testing.T) {
	hostile := []string{"x\nnotification: FORGED | pwned", "x\r\nnotification: FORGED", "x\rnotification: FORGED", "a\n\n\nb", "\n", "tail\n"}
	for _, h := range hostile {
		for field := 0; field < 4; field++ {
			var buf bytes.Buffer
			n := sdk.Notification{Title: "t", Body: "b", ID: "d", Event: sdk.Event{ID: "e", Source: "s", Type: "t"}}
			switch field {
			case 0:
				n.Title = h
			case 1:
				n.Body = h
			case 2:
				n.Event.Source = h
			case 3:
				n.Event.Type = h
			}
			if _, err := sinks.NewStdout(&buf).Deliver(bg, n); err != nil {
				t.Fatal(err)
			}
			if got := strings.Count(buf.String(), "\n"); got != 1 {
				t.Fatalf("hostile text %q in field %d produced %d lines: %q", h, field, got, buf.String())
			}
			if strings.Contains(buf.String(), "\r") {
				t.Fatalf("a carriage return survived: %q", buf.String())
			}
		}
	}
}

func TestStdoutPreservesInternationalAndEmptyText(t *testing.T) {
	var buf bytes.Buffer
	s := sinks.NewStdout(&buf)
	_, _ = s.Deliver(bg, sdk.Notification{Title: "日本語 🚨 ünï", Body: ""})
	_, _ = s.Deliver(bg, sdk.Notification{})
	out := buf.String()
	if !strings.Contains(out, "日本語 🚨 ünï") || strings.Count(out, "\n") != 2 {
		t.Fatalf("output = %q", out)
	}
}

func TestStdoutLinesNeverInterleaveUnderConcurrency(t *testing.T) {
	var buf bytes.Buffer
	s := sinks.NewStdout(&buf)
	const goroutines, each = 40, 25
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				marker := fmt.Sprintf("<%d-%d>", g, i)
				_, _ = s.Deliver(bg, sdk.Notification{Title: marker, Body: marker, ID: marker})
			}
		}(g)
	}
	wg.Wait()

	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != goroutines*each {
		t.Fatalf("%d lines, want %d", len(lines), goroutines*each)
	}
	seen := map[string]bool{}
	for _, l := range lines {
		start, end := strings.Index(l, "<"), strings.Index(l, ">")
		if !strings.HasPrefix(l, "notification: ") || start < 0 || end < start || strings.Count(l, "<") != 3 {
			t.Fatalf("a line is corrupted or interleaved: %q", l)
		}
		marker := l[start : end+1]
		if strings.Count(l, marker) != 3 { // title, body and delivery id carry the same marker
			t.Fatalf("a line mixes output from two notifications: %q", l)
		}
		seen[marker] = true
	}
	if len(seen) != goroutines*each {
		t.Fatalf("%d distinct notifications reached the output, want %d", len(seen), goroutines*each)
	}
}

type failingWriter struct{ err error }

func (f failingWriter) Write([]byte) (int, error) { return 0, f.err }

func TestStdoutWriteFailuresAreReportedAsRetryable(t *testing.T) {
	boom := errors.New("disk full")
	res, err := sinks.NewStdout(failingWriter{boom}).Deliver(bg, sdk.Notification{Title: "t"})
	if !errors.Is(err, boom) || sdk.IsPermanent(err) {
		t.Fatalf("err = %v, want the write error, retryable (the writer may recover)", err)
	}
	if res.Detail != "" {
		t.Fatalf("a failed delivery must not claim a detail: %+v", res)
	}
}

func TestACancelledContextWritesNothing(t *testing.T) {
	var buf bytes.Buffer
	c, cancel := context.WithCancel(bg)
	cancel()
	if _, err := sinks.NewStdout(&buf).Deliver(c, sdk.Notification{Title: "t"}); err == nil || buf.Len() != 0 {
		t.Fatalf("err=%v output=%q: a cancelled delivery must fail without side effects", err, buf.String())
	}
}

// ---------------------------------------------------------------------------
// Build and Registry

func TestBuildCreatesOneSinkPerEntry(t *testing.T) {
	reg, err := sinks.Build([]sinks.Config{{Name: "b", Type: "stdout"}, {Name: "a", Type: "stdout", Options: map[string]any{}}})
	if err != nil {
		t.Fatal(err)
	}
	if got := reg.Names(); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("Names = %v, want sorted", got)
	}
	a, ok1 := reg.Get("a")
	b, ok2 := reg.Get("b")
	if !ok1 || !ok2 || a == nil || b == nil {
		t.Fatal("configured sinks must be retrievable")
	}
	if a == b {
		t.Fatal("two entries must be two separate sinks")
	}
	again, _ := reg.Get("a")
	if again != a {
		t.Fatal("Get must hand out the same instance each time (sinks may hold connections or state)")
	}
	if _, ok := reg.Get("nope"); ok {
		t.Fatal("unknown names are not found")
	}
	if _, ok := reg.Get(""); ok {
		t.Fatal("the empty name is not found")
	}
	if _, ok := reg.Get("A"); ok {
		t.Fatal("names are case-sensitive")
	}
}

func TestBuildingNothingIsFine(t *testing.T) {
	for _, cfgs := range [][]sinks.Config{nil, {}} {
		reg, err := sinks.Build(cfgs)
		if err != nil || reg == nil || len(reg.Names()) != 0 {
			t.Fatalf("empty config: %v %v", reg, err)
		}
	}
}

func TestBuildRejectsAmbiguousOrUnusableEntriesAndReportsAllOfThem(t *testing.T) {
	_, err := sinks.Build([]sinks.Config{
		{Name: "", Type: "stdout"},
		{Name: "   ", Type: "stdout"},
		{Name: "dup", Type: "stdout"},
		{Name: "dup", Type: "stdout"},
		{Name: "weird", Type: "carrier-pigeon"},
		{Name: "notype", Type: ""},
		{Name: "typo", Type: "stdout", Options: map[string]any{"colour": "red"}},
		{Name: "fine", Type: "stdout"},
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	msg := err.Error()
	for _, want := range []string{
		"sink #1", "sink #2", "name is required",
		`sink "dup": duplicate sink name`,
		`sink "weird": unknown type "carrier-pigeon"`, "stdout", // the message should list what IS available
		`sink "notype": unknown type ""`,
		`sink "typo"`, "colour",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("the error should mention %q:\n%s", want, msg)
		}
	}
}

func TestAFailedBuildReturnsNoRegistry(t *testing.T) {
	reg, err := sinks.Build([]sinks.Config{{Name: "ok", Type: "stdout"}, {Name: "bad", Type: "nope"}})
	if err == nil || reg != nil {
		t.Fatalf("reg=%v err=%v: the hub must not start with half its sinks", reg, err)
	}
}

func TestStdoutAcceptsNoOptions(t *testing.T) {
	for name, opts := range map[string]map[string]any{
		"one option": {"color": true}, "several": {"a": 1, "b": "two"}, "nested": {"x": map[string]any{"y": 1}},
	} {
		if _, err := sinks.Build([]sinks.Config{{Name: "s", Type: "stdout", Options: opts}}); err == nil {
			t.Errorf("%s: stdout takes no options, so any option is a mistake", name)
		}
	}
	if _, err := sinks.Build([]sinks.Config{{Name: "s", Type: "stdout", Options: nil}}); err != nil {
		t.Errorf("nil options: %v", err)
	}
}

func TestNewRegistryIsIsolatedFromTheMapItWasGiven(t *testing.T) {
	a, b := &sinktest.Recorder{}, &sinktest.Recorder{}
	src := map[string]sdk.Sink{"a": a}
	reg := sinks.NewRegistry(src)
	src["b"] = b
	delete(src, "a")
	if got, ok := reg.Get("a"); !ok || got != sdk.Sink(a) {
		t.Fatal("removing an entry from the caller's map must not affect the registry")
	}
	if _, ok := reg.Get("b"); ok {
		t.Fatal("adding to the caller's map must not affect the registry")
	}
	if got := reg.Names(); !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("Names = %v", got)
	}
	if empty := sinks.NewRegistry(nil); len(empty.Names()) != 0 {
		t.Fatal("a nil map gives an empty registry")
	}
}

func TestARegistryIsSafeForConcurrentReads(t *testing.T) {
	reg, _ := sinks.Build([]sinks.Config{{Name: "a", Type: "stdout"}, {Name: "b", Type: "stdout"}})
	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if _, ok := reg.Get("a"); !ok {
					t.Error("sink vanished")
				}
				if len(reg.Names()) != 2 {
					t.Error("names changed")
				}
			}
		}()
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// DecodeOptions

type sample struct {
	URL     string            `json:"url"`
	Retries int               `json:"retries"`
	Ratio   float64           `json:"ratio"`
	On      bool              `json:"on"`
	Headers map[string]string `json:"headers"`
	Tags    []string          `json:"tags"`
	Nested  struct {
		Depth int `json:"depth"`
	} `json:"nested"`
}

func TestDecodeOptionsFillsATypedStruct(t *testing.T) {
	var s sample
	err := sinks.DecodeOptions(map[string]any{
		"url": "https://x", "retries": uint64(3), "ratio": 1.5, "on": true,
		"headers": map[string]any{"A": "b"}, "tags": []any{"x", "y"}, "nested": map[string]any{"depth": int64(2)},
	}, &s)
	if err != nil {
		t.Fatal(err)
	}
	if s.URL != "https://x" || s.Retries != 3 || s.Ratio != 1.5 || !s.On || s.Headers["A"] != "b" ||
		!reflect.DeepEqual(s.Tags, []string{"x", "y"}) || s.Nested.Depth != 2 {
		t.Fatalf("decoded = %+v", s)
	}
}

func TestDecodeOptionsLeavesDefaultsAloneForMissingKeys(t *testing.T) {
	s := sample{URL: "default", Retries: 5}
	for _, opts := range []map[string]any{nil, {}, {"ratio": 2.0}} {
		s = sample{URL: "default", Retries: 5}
		if err := sinks.DecodeOptions(opts, &s); err != nil {
			t.Fatal(err)
		}
		if s.URL != "default" || s.Retries != 5 {
			t.Fatalf("options %v overwrote values they did not mention: %+v", opts, s)
		}
	}
}

func TestDecodeOptionsRejectsMistakes(t *testing.T) {
	for name, tc := range map[string]struct {
		opts map[string]any
		want string
	}{
		"misspelt key":           {map[string]any{"urll": "x"}, "urll"},
		"unknown key next to ok": {map[string]any{"url": "x", "timeout": "5s"}, "timeout"},
		"text for a number":      {map[string]any{"retries": "three"}, "retries"},
		"fraction for an int":    {map[string]any{"retries": 2.5}, "retries"},
		"number for text":        {map[string]any{"url": 5}, "url"},
		"text for a bool":        {map[string]any{"on": "yes"}, "on"},
		"list for a mapping":     {map[string]any{"headers": []any{"a"}}, "headers"},
		"unknown nested key":     {map[string]any{"nested": map[string]any{"deep": 1}}, "deep"},
		"wrong item type":        {map[string]any{"tags": []any{1, 2}}, "tags"},
	} {
		var s sample
		err := sinks.DecodeOptions(tc.opts, &s)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want it to mention %q", name, err, tc.want)
		}
	}
}

func TestDecodeOptionsDoesNotModifyItsInput(t *testing.T) {
	in := map[string]any{"url": "x", "headers": map[string]any{"A": "b"}, "tags": []any{"t"}}
	var s sample
	_ = sinks.DecodeOptions(in, &s)
	s.Headers["A"] = "changed"
	s.Tags[0] = "changed"
	if in["headers"].(map[string]any)["A"] != "b" || in["tags"].([]any)[0] != "t" {
		t.Fatal("decoding shared structure with the config, so editing the result edited the config")
	}
}
