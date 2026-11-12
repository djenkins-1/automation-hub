package sdk_test

// Specification under test (pkg/sdk is the contract plugin and API authors code against):
//
//  1. Event.Validate accepts every well-formed event and rejects every
//     malformed one, reporting ALL problems at once, always wrapping
//     ErrInvalidEvent, and never modifying the event.
//  2. Limits are exact: a value at the limit is valid, one past it is not.
//  3. The hub assigns ID and Time, so Validate must not care about them.
//  4. The JSON field names of Event are a public wire contract.
//  5. Error classification (Permanent / RetryAfter) survives wrapping, carries
//     the original message and cause, and nil in means nil out.

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

func valid() sdk.Event {
	return sdk.Event{Source: "monitor", Type: "disk.full", Subject: "/var", Data: map[string]any{"percent": 95}}
}

func wantInvalid(t *testing.T, e sdk.Event, mention ...string) {
	t.Helper()
	err := e.Validate()
	if err == nil {
		t.Fatalf("event %+v should be invalid", e)
	}
	if !errors.Is(err, sdk.ErrInvalidEvent) {
		t.Fatalf("error %q must wrap ErrInvalidEvent", err)
	}
	for _, m := range mention {
		if !strings.Contains(err.Error(), m) {
			t.Fatalf("error %q should mention %q", err, m)
		}
	}
}

func wantValid(t *testing.T, e sdk.Event) {
	t.Helper()
	if err := e.Validate(); err != nil {
		t.Fatalf("event %+v should be valid, got: %v", e, err)
	}
}

func TestRealisticEventsAreValid(t *testing.T) {
	for name, e := range map[string]sdk.Event{
		"monitoring alert": {Source: "monitor", Type: "disk.full", Subject: "/var", Data: map[string]any{"percent": 95.5}},
		"cron failure":     {Source: "cron", Type: "backup.failed", Subject: "nightly-photos", Data: map[string]any{"exit_code": 2, "stderr": "no space left\non device"}},
		"mail arrived":     {Source: "imap:me@example.com", Type: "mail:received", Subject: "Re: invoice — due Friday ✉", Data: map[string]any{"from": "bob@example.com"}},
		"minimal":          {Source: "s", Type: "t"},
		"with key":         {Source: "webhook", Type: "push", IdempotencyKey: "delivery-7f3a9c"},
		"unicode subject":  {Source: "s", Type: "t", Subject: "日本語のサブジェクト 🚨"},
		"empty data":       {Source: "s", Type: "t", Data: map[string]any{}},
		"nested data":      {Source: "s", Type: "t", Data: map[string]any{"a": map[string]any{"b": []any{1, "two", nil, true, map[string]any{"c": 3.5}}}}},
		"null value":       {Source: "s", Type: "t", Data: map[string]any{"maybe": nil}},
	} {
		t.Run(name, func(t *testing.T) { wantValid(t, e) })
	}
}

func TestRequiredFields(t *testing.T) {
	e := valid()
	e.Source = ""
	wantInvalid(t, e, "source is required")

	e = valid()
	e.Type = ""
	wantInvalid(t, e, "type is required")

	wantInvalid(t, sdk.Event{}, "source is required", "type is required")
}

// Source and type are what rule globs match on, so their alphabet is restricted
// to characters that are unambiguous in globs, logs and URLs.
func TestSourceAndTypeNaming(t *testing.T) {
	accepted := []string{
		"a", "A", "0", "monitor", "disk.full", "backup-job_1", "mail:received", "a/b", "x+y", "user@host",
		"9lives", "a.b.c.d.e", strings.Repeat("a", 128),
	}
	rejected := []string{
		"", " ", "my monitor", "tab\there", "new\nline", "trailing ", " leading",
		".hidden", "-dash", "_under", ":colon", "/slash", "@at", "+plus", // must start alphanumeric
		"disk.*", "disk.?", "a*b", // glob metacharacters would make rules ambiguous
		"mönitor", "日本", "emoji🚨", // ASCII only
		"quote\"", "back\\slash", "semi;colon", "hash#", "pct%", "amp&", "eq=", "lt<", "gt>", "pipe|", "paren(", "brace{", "bracket[", "bang!", "caret^", "tilde~", "dollar$",
		"nul\x00byte", "del\x7f",
	}
	for _, field := range []string{"source", "type"} {
		for _, name := range accepted {
			e := valid()
			set(&e, field, name)
			if err := e.Validate(); err != nil {
				t.Errorf("%s %q should be valid: %v", field, name, err)
			}
		}
		for _, name := range rejected {
			e := valid()
			set(&e, field, name)
			wantInvalidQuiet(t, e, fmt.Sprintf("%s %q", field, name), field)
		}
	}
}

func set(e *sdk.Event, field, v string) {
	if field == "source" {
		e.Source = v
	} else {
		e.Type = v
	}
}

func wantInvalidQuiet(t *testing.T, e sdk.Event, label, mention string) {
	t.Helper()
	err := e.Validate()
	if err == nil {
		t.Errorf("%s should be rejected", label)
		return
	}
	if !errors.Is(err, sdk.ErrInvalidEvent) || !strings.Contains(err.Error(), mention) {
		t.Errorf("%s: error %q should wrap ErrInvalidEvent and mention %q", label, err, mention)
	}
}

func TestLimitsAreExact(t *testing.T) {
	cases := []struct {
		field string
		limit int
		set   func(e *sdk.Event, s string)
	}{
		{"source", 128, func(e *sdk.Event, s string) { e.Source = s }},
		{"type", 128, func(e *sdk.Event, s string) { e.Type = s }},
		{"subject", 512, func(e *sdk.Event, s string) { e.Subject = s }},
		{"idempotency_key", 256, func(e *sdk.Event, s string) { e.IdempotencyKey = s }},
	}
	for _, c := range cases {
		t.Run(c.field, func(t *testing.T) {
			e := valid()
			c.set(&e, strings.Repeat("a", c.limit))
			wantValid(t, e)
			c.set(&e, strings.Repeat("a", c.limit+1))
			wantInvalid(t, e, c.field)
		})
	}
}

func TestDataSizeLimitIsExact(t *testing.T) {
	// {"k":"<n x's>"} encodes to n+8 bytes.
	atLimit := sdk.Event{Source: "s", Type: "t", Data: map[string]any{"k": strings.Repeat("x", sdk.MaxEventDataBytes-8)}}
	if b, _ := json.Marshal(atLimit.Data); len(b) != sdk.MaxEventDataBytes {
		t.Fatalf("test setup: encoded size is %d, want exactly %d", len(b), sdk.MaxEventDataBytes)
	}
	wantValid(t, atLimit)

	overLimit := sdk.Event{Source: "s", Type: "t", Data: map[string]any{"k": strings.Repeat("x", sdk.MaxEventDataBytes-7)}}
	wantInvalid(t, overLimit, "data is larger")
}

func TestSubjectMustBeValidUTF8(t *testing.T) {
	e := valid()
	e.Subject = "ok \xff\xfe broken"
	wantInvalid(t, e, "subject")
	e.Subject = "fine — ünïcödé"
	wantValid(t, e)
}

func TestIdempotencyKeyRejectsControlCharacters(t *testing.T) {
	for _, bad := range []string{"a\nb", "a\tb", "a\x00b", "a\x7fb", "a\rb"} {
		e := valid()
		e.IdempotencyKey = bad
		wantInvalid(t, e, "idempotency_key")
	}
	for _, good := range []string{"", "plain", "with spaces are fine", "ünï-🔑", "a:b/c?d=e&f"} {
		e := valid()
		e.IdempotencyKey = good
		wantValid(t, e)
	}
}

func TestDataMustBeSerialisableJSON(t *testing.T) {
	for name, data := range map[string]map[string]any{
		"NaN":        {"v": math.NaN()},
		"+Inf":       {"v": math.Inf(1)},
		"-Inf":       {"v": math.Inf(-1)},
		"channel":    {"v": make(chan int)},
		"function":   {"v": func() {}},
		"nested NaN": {"a": []any{map[string]any{"v": math.NaN()}}},
	} {
		t.Run(name, func(t *testing.T) {
			e := valid()
			e.Data = data
			wantInvalid(t, e, "data")
		})
	}
	for name, data := range map[string]map[string]any{
		"nil":   nil,
		"empty": {},
		"zero":  {"v": 0},
		"huge":  {"v": 1e300},
		"tiny":  {"v": 5e-324},
	} {
		t.Run("valid "+name, func(t *testing.T) {
			e := valid()
			e.Data = data
			wantValid(t, e)
		})
	}
}

func TestHubAssignedFieldsAreNotValidated(t *testing.T) {
	for _, tm := range []time.Time{{}, time.Unix(0, 0), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), time.Now().Add(100 * 365 * 24 * time.Hour)} {
		e := valid()
		e.Time = tm
		wantValid(t, e)
	}
	for _, id := range []string{"", "anything", strings.Repeat("x", 1000), "with space\n"} {
		e := valid()
		e.ID = id
		wantValid(t, e) // the hub overwrites it anyway
	}
}

func TestValidateReportsEveryProblemAtOnce(t *testing.T) {
	e := sdk.Event{
		Source:         "bad source",
		Type:           "",
		Subject:        strings.Repeat("s", 600),
		IdempotencyKey: "has\nnewline",
		Data:           map[string]any{"v": math.NaN()},
	}
	wantInvalid(t, e, "source", "type is required", "subject is longer", "idempotency_key", "data is not valid JSON")
}

func TestValidateDoesNotModifyTheEvent(t *testing.T) {
	e := sdk.Event{
		ID: "keep", Source: "s", Type: "t", Subject: "sub", Time: time.Unix(5, 0), IdempotencyKey: "k",
		Data: map[string]any{"list": []any{1, 2}, "m": map[string]any{"x": "y"}},
	}
	before, _ := json.Marshal(e)
	_ = e.Validate()
	_ = (sdk.Event{Source: "bad source"}).Validate()
	after, _ := json.Marshal(e)
	if string(before) != string(after) {
		t.Fatalf("Validate mutated the event:\n before %s\n after  %s", before, after)
	}
}

func TestValidationIsDeterministic(t *testing.T) {
	bad := sdk.Event{Source: "x y", Type: "a*b"}
	first := bad.Validate().Error()
	for i := 0; i < 50; i++ {
		if got := bad.Validate().Error(); got != first {
			t.Fatalf("error text changed between calls:\n%s\n%s", first, got)
		}
	}
}

// ---------------------------------------------------------------------------
// Wire format: API clients and stored data depend on these names.

func TestEventWireFormat(t *testing.T) {
	in := `{
		"id": "01H",
		"source": "monitor",
		"type": "disk.full",
		"subject": "/var",
		"time": "2026-01-02T03:04:05Z",
		"data": {"percent": 95.5, "tags": ["a"]},
		"idempotency_key": "k1"
	}`
	var e sdk.Event
	if err := json.Unmarshal([]byte(in), &e); err != nil {
		t.Fatal(err)
	}
	if e.ID != "01H" || e.Source != "monitor" || e.Type != "disk.full" || e.Subject != "/var" ||
		!e.Time.Equal(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)) || e.IdempotencyKey != "k1" ||
		e.Data["percent"] != 95.5 {
		t.Fatalf("decoded wrongly: %+v", e)
	}

	out, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	var keys map[string]any
	_ = json.Unmarshal(out, &keys)
	if got := sortedKeys(keys); !reflect.DeepEqual(got, []string{"data", "id", "idempotency_key", "source", "subject", "time", "type"}) {
		t.Fatalf("encoded keys = %v", got)
	}
}

func TestOptionalFieldsAreOmittedWhenEmpty(t *testing.T) {
	out, _ := json.Marshal(sdk.Event{ID: "1", Source: "s", Type: "t", Time: time.Unix(0, 0).UTC()})
	var keys map[string]any
	_ = json.Unmarshal(out, &keys)
	if got := sortedKeys(keys); !reflect.DeepEqual(got, []string{"id", "source", "time", "type"}) {
		t.Fatalf("a minimal event should encode only id/source/time/type, got %v", got)
	}
}

func TestEventJSONRoundTrip(t *testing.T) {
	in := sdk.Event{
		ID: "1", Source: "s", Type: "t", Subject: "日本語 \"quoted\" \n newline 🚨", IdempotencyKey: "k",
		Time: time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC),
		Data: map[string]any{"s": "a\\b\"c", "n": 1.5, "b": true, "z": nil, "l": []any{"x", 2.0}, "m": map[string]any{"k": "v"}},
	}
	raw, _ := json.Marshal(in)
	var out sdk.Event
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip changed the event:\n in  %+v\n out %+v", in, out)
	}
}

func sortedKeys(m map[string]any) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// ---------------------------------------------------------------------------
// Error classification

var errBase = errors.New("boom")

func TestNilStaysNil(t *testing.T) {
	if sdk.Permanent(nil) != nil {
		t.Error("Permanent(nil) must be nil, or `return sdk.Permanent(err)` would turn success into failure")
	}
	if sdk.RetryAfter(nil, time.Second) != nil {
		t.Error("RetryAfter(nil, d) must be nil")
	}
	if sdk.IsPermanent(nil) {
		t.Error("nil is not permanent")
	}
	if _, ok := sdk.RetryDelay(nil); ok {
		t.Error("nil has no retry delay")
	}
}

func TestPlainErrorsAreRetryable(t *testing.T) {
	for _, err := range []error{errBase, errors.New("permanent failure (just words)"), fmt.Errorf("wrapped: %w", errBase)} {
		if sdk.IsPermanent(err) {
			t.Errorf("%q must not count as permanent", err)
		}
		if _, ok := sdk.RetryDelay(err); ok {
			t.Errorf("%q must not carry a retry delay", err)
		}
	}
}

func TestPermanentKeepsMessageAndCause(t *testing.T) {
	err := sdk.Permanent(errBase)
	if !sdk.IsPermanent(err) {
		t.Fatal("not classified permanent")
	}
	if err.Error() != "boom" {
		t.Errorf("the message must be unchanged (it ends up in logs and the database), got %q", err.Error())
	}
	if !errors.Is(err, errBase) {
		t.Error("errors.Is must still find the cause")
	}
}

func TestClassificationSurvivesWrappingInEitherOrder(t *testing.T) {
	inner := fmt.Errorf("sending to ntfy: %w", sdk.Permanent(errBase))
	outer := sdk.Permanent(fmt.Errorf("sending to ntfy: %w", errBase))
	joined := errors.Join(errors.New("also failed"), sdk.Permanent(errBase))
	twice := sdk.Permanent(sdk.Permanent(errBase))
	for name, err := range map[string]error{"wrapped after": inner, "wrapped before": outer, "joined": joined, "doubled": twice} {
		if !sdk.IsPermanent(err) {
			t.Errorf("%s: classification lost", name)
		}
	}

	d := 30 * time.Second
	for name, err := range map[string]error{
		"direct":  sdk.RetryAfter(errBase, d),
		"wrapped": fmt.Errorf("ctx: %w", sdk.RetryAfter(errBase, d)),
	} {
		if got, ok := sdk.RetryDelay(err); !ok || got != d {
			t.Errorf("%s: RetryDelay = %v, %v", name, got, ok)
		}
	}
}

func TestPermanentAndRetryAfterCanBeCombined(t *testing.T) {
	err := sdk.Permanent(sdk.RetryAfter(errBase, time.Minute))
	if !sdk.IsPermanent(err) {
		t.Error("lost the permanent classification")
	}
	if d, ok := sdk.RetryDelay(err); !ok || d != time.Minute {
		t.Errorf("lost the retry delay: %v %v", d, ok)
	}
	if err.Error() != "boom" || !errors.Is(err, errBase) {
		t.Error("message or cause changed")
	}
}

func TestRetryDelayIsReportedExactlyAsGiven(t *testing.T) {
	for _, d := range []time.Duration{0, time.Nanosecond, time.Hour, 365 * 24 * time.Hour, -time.Second} {
		if got, ok := sdk.RetryDelay(sdk.RetryAfter(errBase, d)); !ok || got != d {
			t.Errorf("RetryDelay(RetryAfter(_, %v)) = %v, %v", d, got, ok)
		}
	}
}
