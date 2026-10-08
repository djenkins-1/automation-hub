package rules_test

// Specification under test: HOW a matched action becomes message text.
//
//  - Title and body are Go text/templates over .Event (the event: ID, Source,
//    Type, Subject, Time, Data, IdempotencyKey) and .Rule (the rule's name).
//  - Output is plain text: nothing is HTML-escaped (these go to phones, not browsers).
//  - A missing data key renders as nothing, never as "<no value>".
//  - Functions: upper, lower, trim, truncate, default, json (and the standard
//    ones such as printf). `truncate` counts characters and never exceeds its limit.
//  - Syntax errors are rejected when the rules are loaded.
//  - A template that fails at render time skips only that action, is
//    reported, and never affects other actions, other rules or later events.
//  - Rendering is stateless: output depends only on the event given.

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/djenkins-1/automation-hub/internal/rules"
	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

var tmplEvent = sdk.Event{
	ID: "evt-42", Source: "monitor", Type: "disk.full", Subject: "/var/log",
	Time: time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC), IdempotencyKey: "idem-1",
	Data: map[string]any{
		"percent": 95.5, "whole": 95.0, "zero": 0.0, "neg": -3.0, "host": "nas-01", "ok": true, "off": false,
		"tags": []any{"a", "b", "c"}, "disk": map[string]any{"mount": "/var"}, "none": nil, "empty": "",
		"bytes": 1500000000.0, "emptyList": []any{}, "emptyMap": map[string]any{},
	},
}

// renderOne renders title and body of a single-action rule for e.
func renderOne(t *testing.T, title, body string, e sdk.Event) (rules.Rendered, []error) {
	t.Helper()
	s := newSet(t, rules.Rule{Name: "rn", Actions: []rules.Action{{Sink: "s", Title: title, Body: body}}})
	out, errs := s.Match(e)
	if len(out) > 1 {
		t.Fatalf("one action produced %d notifications", len(out))
	}
	if len(out) == 0 {
		return rules.Rendered{}, errs
	}
	return out[0], errs
}

// title renders a title template and fails the test if rendering was not clean.
func title(t *testing.T, tmpl string) string {
	t.Helper()
	r, errs := renderOne(t, tmpl, "", tmplEvent)
	if len(errs) != 0 {
		t.Fatalf("template %q: unexpected errors %v", tmpl, errs)
	}
	return r.Title
}

func TestTemplateVariables(t *testing.T) {
	for tmpl, want := range map[string]string{
		"{{.Event.ID}}":                       "evt-42",
		"{{.Event.Source}}":                   "monitor",
		"{{.Event.Type}}":                     "disk.full",
		"{{.Event.Subject}}":                  "/var/log",
		"{{.Event.IdempotencyKey}}":           "idem-1",
		`{{.Event.Time.Format "2006-01-02"}}`: "2026-03-04",
		"{{.Event.Time.Year}}":                "2026",
		"{{.Rule}}":                           "rn",
		"{{.Event.Data.host}}":                "nas-01",
		"{{.Event.Data.disk.mount}}":          "/var",
		"{{index .Event.Data.tags 1}}":        "b",
		"{{len .Event.Data.tags}}":            "3",
		"literal text only":                   "literal text only",
		"{{.Event.Source}}/{{.Event.Type}}":   "monitor/disk.full",
		"[{{.Rule}}] {{.Event.Subject}}: {{.Event.Data.percent}}%": "[rn] /var/log: 95.5%",
	} {
		if got := title(t, tmpl); got != want {
			t.Errorf("%s\n got  %q\n want %q", tmpl, got, want)
		}
	}
}

func TestNumbersRenderAsPlainNumbers(t *testing.T) {
	for tmpl, want := range map[string]string{
		"{{.Event.Data.percent}}": "95.5",
		"{{.Event.Data.whole}}":   "95", // 95.0 must not print as "95.0"
		"{{.Event.Data.zero}}":    "0",
		"{{.Event.Data.neg}}":     "-3",
		"{{.Event.Data.ok}}":      "true",
		"{{.Event.Data.off}}":     "false",
	} {
		if got := title(t, tmpl); got != want {
			t.Errorf("%s = %q, want %q", tmpl, got, want)
		}
	}
}

// Very large round numbers are floats under the hood and print in exponent
// form by default; printf is the documented way to format them.
func TestPrintfFormatsNumbersExactly(t *testing.T) {
	for tmpl, want := range map[string]string{
		`{{printf "%.0f" .Event.Data.bytes}}`:   "1500000000",
		`{{printf "%.1f" .Event.Data.percent}}`: "95.5",
		`{{printf "%.0f" .Event.Data.percent}}`: "96",
		`{{printf "%05.1f" .Event.Data.whole}}`: "095.0",
		`{{printf "%s-%d" "x" 7}}`:              "x-7",
		`{{printf "%q" .Event.Subject}}`:        `"/var/log"`,
	} {
		if got := title(t, tmpl); got != want {
			t.Errorf("%s = %q, want %q", tmpl, got, want)
		}
	}
}

func TestOutputIsPlainTextNotHTML(t *testing.T) {
	e := tmplEvent
	e.Subject = `<b>"R&D" & 'co'</b> > 5`
	r, errs := renderOne(t, "{{.Event.Subject}}", "{{.Event.Subject}}", e)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	if r.Title != e.Subject || r.Body != e.Subject {
		t.Fatalf("text was altered (HTML-escaped?):\n title %q\n body  %q\n want  %q", r.Title, r.Body, e.Subject)
	}
}

func TestWhitespaceAndLinesArePreserved(t *testing.T) {
	r, errs := renderOne(t, "T", "line one\n\n  indented {{.Event.Source}}\r\n\ttabbed\nlast", tmplEvent)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	if want := "line one\n\n  indented monitor\r\n\ttabbed\nlast"; r.Body != want {
		t.Fatalf("body = %q, want %q", r.Body, want)
	}
}

func TestUnicodeSurvivesRendering(t *testing.T) {
	e := tmplEvent
	e.Subject = "日本語 🚨 ünï"
	r, _ := renderOne(t, "{{.Event.Subject}} | {{.Event.Subject | upper}} | {{.Event.Subject | truncate 4}}", "", e)
	if want := "日本語 🚨 ünï | 日本語 🚨 ÜNÏ | 日本語…"; r.Title != want {
		t.Fatalf("title = %q, want %q", r.Title, want)
	}
}

// ---------------------------------------------------------------------------
// Missing values

func TestMissingValuesRenderAsNothing(t *testing.T) {
	for _, tmpl := range []string{
		"{{.Event.Data.nope}}", "[{{.Event.Data.nope}}]", "{{.Event.Data.none}}", "{{.Event.Data.nope | upper}}",
	} {
		r, errs := renderOne(t, tmpl, tmpl, tmplEvent)
		if len(errs) != 0 {
			t.Errorf("%s: unexpected errors %v", tmpl, errs)
			continue
		}
		for field, got := range map[string]string{"title": r.Title, "body": r.Body} {
			if strings.Contains(got, "no value") || strings.Contains(got, "<nil>") {
				t.Errorf("%s in %s leaked a placeholder: %q", tmpl, field, got)
			}
		}
	}
	if got := title(t, "[{{.Event.Data.nope}}]"); got != "[]" {
		t.Errorf("got %q, want []", got)
	}
}

func TestEventWithNoDataRendersWithoutErrors(t *testing.T) {
	for _, data := range []map[string]any{nil, {}} {
		e := sdk.Event{ID: "e", Source: "s", Type: "t", Data: data}
		r, errs := renderOne(t, "a{{.Event.Data.x}}b", "{{.Event.Data.x | default \"d\"}}", e)
		if len(errs) != 0 || r.Title != "ab" || r.Body != "d" {
			t.Errorf("data=%v: title=%q body=%q errs=%v", data, r.Title, r.Body, errs)
		}
	}
}

func TestDefaultFunction(t *testing.T) {
	tests := []struct {
		tmpl, want string
	}{
		{`{{.Event.Data.nope | default "n/a"}}`, "n/a"},             // missing
		{`{{.Event.Data.none | default "n/a"}}`, "n/a"},             // null
		{`{{.Event.Data.empty | default "n/a"}}`, "n/a"},            // empty string
		{`{{.Event.Data.zero | default "n/a"}}`, "n/a"},             // zero value
		{`{{.Event.Data.off | default "n/a"}}`, "n/a"},              // false
		{`{{.Event.Data.emptyList | default "n/a"}}`, "n/a"},        // empty collection
		{`{{.Event.Data.emptyMap | default "n/a"}}`, "n/a"},         //
		{`{{.Event.Data.host | default "n/a"}}`, "nas-01"},          // real value wins
		{`{{.Event.Data.percent | default "n/a"}}`, "95.5"},         //
		{`{{.Event.Data.ok | default "n/a"}}`, "true"},              //
		{`{{.Event.Data.neg | default "n/a"}}`, "-3"},               // non-zero number
		{`{{.Event.Subject | default "n/a"}}`, "/var/log"},          //
		{`{{default "fallback" "0"}}`, "0"},                         // the STRING "0" is not empty
		{`{{default "fallback" "false"}}`, "false"},                 //
		{`{{.Event.Data.nope | default .Event.Source}}`, "monitor"}, // fallback may itself be dynamic
	}
	for _, tc := range tests {
		if got := title(t, tc.tmpl); got != tc.want {
			t.Errorf("%s = %q, want %q", tc.tmpl, got, tc.want)
		}
	}
}

func TestMissingNestedObjectsRenderAsNothingToo(t *testing.T) {
	for tmpl, want := range map[string]string{
		"[{{.Event.Data.gone.mount}}]":                  "[]",
		"[{{.Event.Data.gone.deeper.still}}]":           "[]",
		"[{{with .Event.Data.gone}}{{.mount}}{{end}}]":  "[]",
		"[{{with .Event.Data.disk}}{{.mount}}{{end}}]":  "[/var]",
		`[{{.Event.Data.gone.mount | default "none"}}]`: "[none]",
		"[{{.Event.Data.gone.mount | upper}}]":          "[]",
	} {
		if got := title(t, tmpl); got != want {
			t.Errorf("%s = %q, want %q", tmpl, got, want)
		}
	}
}

func TestStringFunctionsAcceptMissingAndNonStringValues(t *testing.T) {
	// An optional field piped through a function must not cost us the notification.
	for tmpl, want := range map[string]string{
		"[{{.Event.Data.nope | upper}}]":         "[]",
		"[{{.Event.Data.nope | lower}}]":         "[]",
		"[{{.Event.Data.nope | trim}}]":          "[]",
		"[{{.Event.Data.nope | truncate 5}}]":    "[]",
		"[{{.Event.Data.none | upper}}]":         "[]",
		"[{{.Event.Data.percent | truncate 3}}]": "[95…]",
		"[{{.Event.Data.ok | upper}}]":           "[TRUE]",
		"[{{.Event.Data.whole | truncate 10}}]":  "[95]",
	} {
		if got := title(t, tmpl); got != want {
			t.Errorf("%s = %q, want %q", tmpl, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// Functions

func TestStringFunctions(t *testing.T) {
	e := tmplEvent
	e.Subject = "  Mixed Case ünï \n"
	for tmpl, want := range map[string]string{
		"[{{.Event.Subject | trim}}]":           "[Mixed Case ünï]",
		"[{{.Event.Subject | trim | upper}}]":   "[MIXED CASE ÜNÏ]",
		"[{{.Event.Subject | trim | lower}}]":   "[mixed case ünï]",
		`[{{upper ""}}{{lower ""}}{{trim ""}}]`: "[]",
		`[{{trim "\t\r\n x \t"}}]`:              "[x]",
	} {
		r, errs := renderOne(t, tmpl, "", e)
		if len(errs) != 0 || r.Title != want {
			t.Errorf("%s = %q (errs %v), want %q", tmpl, r.Title, errs, want)
		}
	}
}

func TestTruncate(t *testing.T) {
	tests := []struct {
		n    int
		in   string
		want string
	}{
		{5, "short", "short"},   // exactly at the limit: untouched
		{10, "short", "short"},  // under the limit
		{4, "short", "sho…"},    // one over: n-1 characters plus an ellipsis
		{1, "short", "s"},       // too small for an ellipsis
		{0, "short", ""},        //
		{-1, "short", "short"},  // negative means "no limit"
		{3, "", ""},             //
		{3, "日本語日本語", "日本…"},    // counts characters, not bytes
		{2, "🚨🚨🚨", "🚨…"},        // astral-plane characters stay whole
		{6, "日本語日本語", "日本語日本語"}, //
	}
	for _, tc := range tests {
		got := title(t, fmt.Sprintf(`{{%q | truncate %d}}`, tc.in, tc.n))
		if got != tc.want {
			t.Errorf("truncate %d %q = %q, want %q", tc.n, tc.in, got, tc.want)
		}
	}
}

func TestTruncateInvariants(t *testing.T) {
	inputs := []string{"", "a", "abcdef", "日本語日本語日本語", "🚨🚨🚨🚨🚨", "mixed 日本 🚨 text"}
	for _, in := range inputs {
		for n := -2; n <= 20; n++ {
			got := title(t, fmt.Sprintf(`{{%q | truncate %d}}`, in, n))
			if !utf8.ValidString(got) {
				t.Errorf("truncate %d %q produced invalid UTF-8: %q", n, in, got)
			}
			runes := utf8.RuneCountInString(in)
			switch {
			case n < 0 || runes <= n:
				if got != in {
					t.Errorf("truncate %d %q must be unchanged, got %q", n, in, got)
				}
			default:
				if utf8.RuneCountInString(got) > n {
					t.Errorf("truncate %d %q = %q exceeds its limit", n, in, got)
				}
				if !strings.HasPrefix(in, strings.TrimSuffix(got, "…")) {
					t.Errorf("truncate %d %q = %q is not a prefix of the input", n, in, got)
				}
			}
		}
	}
}

func TestJSONFunction(t *testing.T) {
	r, errs := renderOne(t, "T", "{{.Event.Data.disk | json}}", tmplEvent)
	if len(errs) != 0 || r.Body != `{"mount":"/var"}` {
		t.Fatalf("body = %q, errs = %v", r.Body, errs)
	}

	// Whatever goes in comes back out equal.
	r, errs = renderOne(t, "T", "{{.Event.Data | json}}", tmplEvent)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	var back map[string]any
	if err := json.Unmarshal([]byte(r.Body), &back); err != nil {
		t.Fatalf("json function produced invalid JSON %q: %v", r.Body, err)
	}
	if !reflect.DeepEqual(back, tmplEvent.Data) {
		t.Fatalf("json round trip changed the data:\n got  %v\n want %v", back, tmplEvent.Data)
	}

	// Values that cannot be encoded skip the action instead of sending garbage.
	e := tmplEvent
	e.Data = map[string]any{"bad": math.NaN()}
	r, errs = renderOne(t, "T", "{{.Event.Data | json}}", e)
	if r != (rules.Rendered{}) || len(errs) != 1 {
		t.Fatalf("want the action skipped and reported, got %+v / %v", r, errs)
	}
}

func TestStandardTemplateFeaturesWork(t *testing.T) {
	for tmpl, want := range map[string]string{
		`{{if gt .Event.Data.percent 90.0}}CRITICAL{{else}}ok{{end}}`:        "CRITICAL",
		`{{if lt .Event.Data.percent 90.0}}CRITICAL{{else}}ok{{end}}`:        "ok",
		`{{if and .Event.Data.ok (not .Event.Data.off)}}yes{{end}}`:          "yes",
		`{{range .Event.Data.tags}}<{{.}}>{{end}}`:                           "<a><b><c>",
		`{{range $i, $t := .Event.Data.tags}}{{if $i}},{{end}}{{$t}}{{end}}`: "a,b,c",
		`{{with .Event.Data.host}}host={{.}}{{end}}`:                         "host=nas-01",
		`{{with .Event.Data.nope}}host={{.}}{{else}}no host{{end}}`:          "no host",
		`{{$h := .Event.Data.host}}{{$h}}-{{$h | upper}}`:                    "nas-01-NAS-01",
		`{{eq .Event.Type "disk.full"}}`:                                     "true",
		`{{/* a comment */}}visible`:                                         "visible",
	} {
		if got := title(t, tmpl); got != want {
			t.Errorf("%s = %q, want %q", tmpl, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// Failure handling

func TestSyntaxErrorsAreRejectedWhenRulesAreLoaded(t *testing.T) {
	for name, tmpl := range map[string]string{
		"unclosed action":    "{{.Event.Subject",
		"unknown function":   "{{nosuchfunc .Rule}}",
		"unterminated quote": `{{printf "abc}}`,
		"stray end":          "{{end}}",
		"unclosed if":        "{{if .Event.Data.ok}}yes",
		"bad pipeline":       "{{| upper}}",
		"unexpected token":   "{{.Event..Subject}}",
		"stray else":         "{{else}}",
	} {
		for _, field := range []string{"title", "body"} {
			a := rules.Action{Sink: "s"}
			if field == "title" {
				a.Title = tmpl
			} else {
				a.Body = tmpl
			}
			_, err := rules.NewSet([]rules.Rule{{Name: "bad", Actions: []rules.Action{{Sink: "s", Title: "fine"}, a}}})
			if err == nil {
				t.Errorf("%s in %s: should be rejected at load time", name, field)
				continue
			}
			if msg := err.Error(); !strings.Contains(msg, `rule "bad"`) || !strings.Contains(msg, "actions[1]."+field) {
				t.Errorf("%s in %s: error should locate the problem, got: %v", name, field, msg)
			}
		}
	}
}

func TestRenderFailuresSkipOnlyTheFailingAction(t *testing.T) {
	failing := map[string]string{
		"index out of range":      "{{index .Event.Data.tags 9}}",
		"function wrong arg type": `{{truncate "abc" 5}}`, // the limit must be a number
		"function missing arg":    "{{upper}}",
		"undefined template":      `{{template "nothing" .}}`,
		"field of a non-object":   "{{.Event.Data.host.mount}}",
		"method on wrong type":    "{{.Event.Data.host.Year}}",
	}
	for name, bad := range failing {
		for _, field := range []string{"title", "body"} {
			badAction := rules.Action{Sink: "bad"}
			if field == "title" {
				badAction.Title = bad
			} else {
				badAction.Title, badAction.Body = "x", bad
			}
			s := newSet(t,
				rules.Rule{Name: "first", Actions: []rules.Action{{Sink: "ok0", Title: "first-ok"}}},
				rules.Rule{Name: "mixed", Actions: []rules.Action{
					{Sink: "ok1", Title: "before"}, badAction, {Sink: "ok2", Title: "after"}}},
				rules.Rule{Name: "last", Actions: []rules.Action{{Sink: "ok3", Title: "last-ok"}}},
			)
			out, errs := s.Match(tmplEvent)

			var sinksFired []string
			for _, o := range out {
				sinksFired = append(sinksFired, o.Sink)
			}
			if want := []string{"ok0", "ok1", "ok2", "ok3"}; !reflect.DeepEqual(sinksFired, want) {
				t.Errorf("%s/%s: fired %v, want %v (only the failing action is lost)", name, field, sinksFired, want)
			}
			if len(errs) != 1 || !strings.Contains(errs[0].Error(), `rule "mixed"`) || !strings.Contains(errs[0].Error(), "actions[1]."+field) {
				t.Errorf("%s/%s: want one error locating rule and action, got %v", name, field, errs)
			}
			for _, o := range out {
				if o.Rule == "mixed" && o.Sink == "ok2" && o.ActionIndex != 2 {
					t.Errorf("%s/%s: a skipped action must not shift the others' indexes (got %d)", name, field, o.ActionIndex)
				}
			}
		}
	}
}

func TestARenderFailureForOneEventDoesNotAffectTheNext(t *testing.T) {
	s := newSet(t, rules.Rule{Name: "r", Actions: []rules.Action{{Sink: "s", Title: "{{index .Event.Data.tags 1}}"}}})
	short := sdk.Event{Source: "s", Type: "t", Data: map[string]any{"tags": []any{"only-one"}}}
	long := sdk.Event{Source: "s", Type: "t", Data: map[string]any{"tags": []any{"x", "second"}}}
	if out, errs := s.Match(short); len(out) != 0 || len(errs) != 1 {
		t.Fatalf("short: %v %v", out, errs)
	}
	if out, errs := s.Match(long); len(out) != 1 || len(errs) != 0 || out[0].Title != "second" {
		t.Fatalf("long: %v %v", out, errs)
	}
}

// ---------------------------------------------------------------------------
// Statelessness

func TestRenderingDependsOnlyOnTheEventGiven(t *testing.T) {
	s := newSet(t, rules.Rule{Name: "r", Match: rules.Match{When: "data.n >= 0"}, Actions: []rules.Action{
		{Sink: "s", Title: "n={{.Event.Data.n}} s={{.Event.Subject}}", Body: `{{.Event.Data.tag | default "none"}}`}}})

	want := func(i int) (string, string) {
		tag := "none"
		if i%2 == 0 {
			tag = fmt.Sprintf("tag-%d", i)
		}
		return fmt.Sprintf("n=%d s=subject-%d", i, i), tag
	}
	build := func(i int) sdk.Event {
		data := map[string]any{"n": float64(i)}
		if i%2 == 0 {
			data["tag"] = fmt.Sprintf("tag-%d", i)
		}
		return sdk.Event{Source: "s", Type: "t", Subject: fmt.Sprintf("subject-%d", i), Data: data}
	}

	// sequentially, then forwards and backwards (order must not matter)...
	for _, i := range []int{0, 1, 2, 3, 3, 2, 1, 0} {
		out, errs := s.Match(build(i))
		wt, wb := want(i)
		if len(errs) != 0 || len(out) != 1 || out[0].Title != wt || out[0].Body != wb {
			t.Fatalf("event %d: %+v %v, want title %q body %q", i, out, errs, wt, wb)
		}
	}

	// ...and from many goroutines at once, with no cross-contamination.
	var wg sync.WaitGroup
	for g := 0; g < 12; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for j := 0; j < 150; j++ {
				i := g*1000 + j
				out, errs := s.Match(build(i))
				wt, wb := want(i)
				if len(errs) != 0 || len(out) != 1 || out[0].Title != wt || out[0].Body != wb {
					t.Errorf("event %d: got %+v %v", i, out, errs)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
