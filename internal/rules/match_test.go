package rules_test

// Specification under test: WHICH events a rule matches.
//
//  - match.source / match.type are globs over the WHOLE value: `*` is any run
//    of characters (including none and including newlines), `?` is exactly
//    one character, everything else is literal and case-sensitive.
//    Regexp metacharacters have no special meaning.
//  - match.when is an expression over id, source, type, subject, time, data.
//  - A rule matches only if ALL its criteria hold; omitted criteria match
//    anything.
//  - Cheap glob criteria gate the expression: it is not evaluated for events
//    the globs already rejected (so it cannot produce noise for them).
//  - An expression that cannot be evaluated never matches and is reported,
//    but never aborts matching of other rules.
//  - Matching never modifies the event.

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/djenkins-1/automation-hub/internal/rules"
	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

func oneRule(m rules.Match) rules.Rule {
	return rules.Rule{Name: "r", Match: m, Actions: []rules.Action{{Sink: "s", Title: "t"}}}
}

func newSet(t *testing.T, rs ...rules.Rule) *rules.Set {
	t.Helper()
	s, err := rules.NewSet(rs)
	if err != nil {
		t.Fatalf("NewSet: %v", err)
	}
	return s
}

// fires reports whether a single rule with match m matches ev, failing the test
// if evaluation reported any problem.
func fires(t *testing.T, m rules.Match, ev sdk.Event) bool {
	t.Helper()
	out, errs := newSet(t, oneRule(m)).Match(ev)
	if len(errs) != 0 {
		t.Fatalf("unexpected evaluation errors: %v", errs)
	}
	return len(out) == 1
}

func ev(source, typ string) sdk.Event { return sdk.Event{ID: "e1", Source: source, Type: typ} }

// ---------------------------------------------------------------------------
// Globs

func TestGlobSemantics(t *testing.T) {
	tests := []struct {
		glob, input string
		want        bool
	}{
		// no glob: matches anything
		{"", "", true}, {"", "anything", true},
		// literal
		{"disk.full", "disk.full", true}, {"disk.full", "disk.full2", false}, {"disk.full", "xdisk.full", false},
		{"disk.full", "Disk.full", false}, // case-sensitive
		{"a.b", "aXb", false},             // a dot is a dot
		// star
		{"*", "", true}, {"*", "x", true}, {"*", "a.b/c:d", true}, {"**", "x", true}, {"**", "", true},
		{"disk.*", "disk.full", true}, {"disk.*", "disk.warn.root", true}, {"disk.*", "disk.", true},
		{"disk.*", "disk", false}, {"disk.*", "xdisk.full", false}, {"disk.*", "Disk.full", false},
		{"*.failed", "backup.failed", true}, {"*.failed", ".failed", true}, {"*.failed", "backup.failed.twice", false},
		{"*mid*", "mid", true}, {"*mid*", "amidb", true}, {"*mid*", "mi", false},
		{"a*b*c", "abc", true}, {"a*b*c", "aXbYc", true}, {"a*b*c", "abbcc", true}, {"a*b*c", "ab", false}, {"a*b*c", "acb", false},
		// question mark
		{"?", "x", true}, {"?", "", false}, {"?", "xy", false}, {"?", "é", true}, {"?", "日", true},
		{"backup.?", "backup.x", true}, {"backup.?", "backup.xy", false}, {"backup.?", "backup.", false},
		{"??", "ab", true}, {"??", "a", false},
		{"a?c*", "abcdef", true}, {"a?c*", "ac", false},
		// regexp metacharacters are literal
		{"a+b", "a+b", true}, {"a+b", "aab", false}, {"a+b", "ab", false},
		{"a|b", "a|b", true}, {"a|b", "a", false}, {"a|b", "b", false},
		{"[abc]", "[abc]", true}, {"[abc]", "a", false},
		{"(x)", "(x)", true}, {"(x)", "x", false},
		{"^start$", "^start$", true}, {"^start$", "start", false},
		{"a{2}", "a{2}", true}, {"a{2}", "aa", false},
		{`a\b`, `a\b`, true}, {`a\d`, "a1", false}, {`\.`, `\.`, true},
		{"(?i)abc", "(?i)abc", true}, {"(?i)abc", "ABC", false},
		{"$", "$", true}, {"^", "^", true},
		// "any characters" includes newlines (events from other code paths may carry them)
		{"*", "line1\nline2", true}, {"a*z", "a\nz", true}, {"?", "\n", true}, {"*.go", "x\ny.go", true},
	}
	for _, tc := range tests {
		for _, field := range []string{"source", "type"} {
			m := rules.Match{}
			e := ev("src", "typ")
			if field == "source" {
				m.Source, e.Source = tc.glob, tc.input
			} else {
				m.Type, e.Type = tc.glob, tc.input
			}
			if got := fires(t, m, e); got != tc.want {
				t.Errorf("%s glob %q against %q: matched=%v, want %v", field, tc.glob, tc.input, got, tc.want)
			}
		}
	}
}

// Properties that follow from the definition of the glob language, checked
// over a deliberately nasty set of strings instead of hand-picked examples.
var nastyStrings = []string{
	"", "a", "ab", "abc", "ba", "aa", "a.b", "a+b", "a|b", "a\\b", "[a]", "(a)", "^a$", "a{1}", "(?i)A", "A",
	"disk.full", "disk.", ".", "..", "-", "_", "/", ":", "@", "é", "日本", "line\nbreak", "tab\t", " ", "$1", "%s", "\\", "\\n",
}

func TestGlobWithoutWildcardsIsExactEquality(t *testing.T) {
	for _, g := range nastyStrings {
		if strings.ContainsAny(g, "*?") {
			continue
		}
		for _, in := range nastyStrings {
			if got, want := fires(t, rules.Match{Type: g}, ev("s", in)), g == in || g == ""; got != want {
				t.Errorf("glob %q vs %q: matched=%v, want %v", g, in, got, want)
			}
		}
	}
}

func TestStarPositionsMeanContainsPrefixSuffix(t *testing.T) {
	for _, s := range nastyStrings {
		if strings.ContainsAny(s, "*?") || s == "" {
			continue
		}
		for _, in := range nastyStrings {
			checks := []struct {
				glob string
				want bool
				name string
			}{
				{"*" + s + "*", strings.Contains(in, s), "contains"},
				{s + "*", strings.HasPrefix(in, s), "prefix"},
				{"*" + s, strings.HasSuffix(in, s), "suffix"},
			}
			for _, c := range checks {
				if got := fires(t, rules.Match{Type: c.glob}, ev("s", in)); got != c.want {
					t.Errorf("%s glob %q vs %q: matched=%v, want %v", c.name, c.glob, in, got, c.want)
				}
			}
		}
	}
}

func TestQuestionMarksCountCharactersNotBytes(t *testing.T) {
	for _, in := range []string{"a", "é", "日", "🚨"} {
		if !fires(t, rules.Match{Type: "?"}, ev("s", in)) {
			t.Errorf("? must match the single character %q", in)
		}
	}
	if fires(t, rules.Match{Type: "?"}, ev("s", "ab")) {
		t.Error("? must not match two characters")
	}
}

// ---------------------------------------------------------------------------
// Combining criteria

func TestAllCriteriaMustHold(t *testing.T) {
	m := rules.Match{Source: "monitor", Type: "disk.*", When: "data.percent > 90"}
	e := func(src, typ string, pct float64) sdk.Event {
		return sdk.Event{ID: "e", Source: src, Type: typ, Data: map[string]any{"percent": pct}}
	}
	for name, tc := range map[string]struct {
		ev   sdk.Event
		want bool
	}{
		"everything holds":     {e("monitor", "disk.full", 95), true},
		"source differs":       {e("other", "disk.full", 95), false},
		"type differs":         {e("monitor", "cpu.high", 95), false},
		"condition fails":      {e("monitor", "disk.full", 50), false},
		"only source holds":    {e("monitor", "cpu.high", 10), false},
		"only condition holds": {e("other", "cpu.high", 99), false},
	} {
		if got := fires(t, m, tc.ev); got != tc.want {
			t.Errorf("%s: matched=%v, want %v", name, got, tc.want)
		}
	}
}

func TestEmptyMatchMatchesEveryEvent(t *testing.T) {
	for _, e := range []sdk.Event{{}, ev("a", "b"), {Source: "x", Type: "y", Subject: "z", Data: map[string]any{"k": 1}}} {
		if !fires(t, rules.Match{}, e) {
			t.Errorf("an unconditional rule must match %+v", e)
		}
	}
}

func TestWhitespaceOnlyConditionIsNoCondition(t *testing.T) {
	if !fires(t, rules.Match{When: "   \n\t "}, ev("a", "b")) {
		t.Fatal("a blank `when` must behave like an omitted one")
	}
}

// ---------------------------------------------------------------------------
// Expressions

func TestWhenExpressions(t *testing.T) {
	base := sdk.Event{
		ID: "evt-1", Source: "monitor", Type: "disk.full", Subject: "/var/log",
		Time: time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC),
		Data: map[string]any{
			"percent": 95.5, "count": 3.0, "host": "nas-01", "ok": true, "flag": false,
			"tags":   []any{"urgent", "disk"},
			"values": []any{1.0, 2.0, 3.0},
			"disk":   map[string]any{"used": 90.0, "total": 100.0},
			"none":   nil,
		},
	}
	tests := []struct {
		expr string
		want bool
	}{
		// the documented variables
		{`id == "evt-1"`, true}, {`source == "monitor"`, true}, {`type == "disk.full"`, true},
		{`subject == "/var/log"`, true}, {`time.Year() == 2026`, true}, {`time.Year() == 2025`, false},
		{`data.host == "nas-01"`, true},
		// comparison boundaries
		{`data.percent > 95.5`, false}, {`data.percent >= 95.5`, true}, {`data.percent < 95.5`, false}, {`data.percent <= 95.5`, true},
		{`data.count == 3`, true}, {`data.count != 3`, false},
		// boolean logic and precedence
		{`data.ok && data.count > 1`, true}, {`data.ok && data.flag`, false}, {`data.flag || data.ok`, true},
		{`!data.flag`, true}, {`!data.ok`, false},
		{`true || false && false`, true}, {`(true || false) && false`, false},
		// strings
		{`subject startsWith "/var"`, true}, {`subject startsWith "/home"`, false},
		{`subject endsWith "log"`, true}, {`subject contains "ar/l"`, true}, {`subject contains "LOG"`, false},
		{`data.host matches "^nas-[0-9]+$"`, true}, {`data.host matches "^srv"`, false},
		// collections
		{`"urgent" in data.tags`, true}, {`"calm" in data.tags`, false},
		{`subject in ["/var/log", "/home"]`, true}, {`type in ["a", "b"]`, false},
		{`len(data.values) == 3`, true}, {`data.values[0] == 1`, true}, {`data.values[2] > data.values[1]`, true},
		{`all(data.values, # > 0)`, true}, {`any(data.values, # > 2)`, true}, {`any(data.values, # > 3)`, false},
		// nesting and arithmetic
		{`data.disk.used / data.disk.total >= 0.9`, true}, {`data.disk.used / data.disk.total > 0.95`, false},
		{`data.count * 2 + 1 == 7`, true}, {`int(data.count) % 2 == 1`, true},
		// conditional expression
		{`data.count > 1 ? true : false`, true}, {`data.count > 5 ? true : false`, false},
		// missing and null values can be tested explicitly
		{`data.missing == nil`, true}, {`data.none == nil`, true}, {`data.host != nil`, true},
		{`data.missing != nil && data.missing > 1`, false},
		{`data.missing?.deeper == nil`, true},
		{`(data.missing ?? 5) == 5`, true},
	}
	for _, tc := range tests {
		t.Run(tc.expr, func(t *testing.T) {
			if got := fires(t, rules.Match{When: tc.expr}, base); got != tc.want {
				t.Errorf("`%s` matched=%v, want %v", tc.expr, got, tc.want)
			}
		})
	}
}

func TestEventWithoutDataBehavesLikeEmptyData(t *testing.T) {
	for _, data := range []map[string]any{nil, {}} {
		e := sdk.Event{Source: "s", Type: "t", Data: data}
		if !fires(t, rules.Match{When: `data.anything == nil`}, e) {
			t.Errorf("data=%#v: a missing key must read as nil", data)
		}
		if fires(t, rules.Match{When: `data.anything != nil`}, e) {
			t.Errorf("data=%#v: a missing key must not count as present", data)
		}
	}
}

func TestExpressionsThatCannotBeEvaluatedNeverMatchAndAreReported(t *testing.T) {
	e := sdk.Event{Source: "s", Type: "t", Data: map[string]any{"name": "disk", "flag": "yes", "n": 5.0}}
	for name, when := range map[string]string{
		"missing key compared with a number": `data.missing > 90`,
		"string compared with a number":      `data.name > 5`,
		"member of a missing object":         `data.nothing.deeper == 1`,
		"index out of range":                 `data.list[3] == 1`,
		"arithmetic on a string":             `data.name * 2 > 1`,
		"truthy string where bool is needed": `data.flag`,
		"number where bool is needed":        `data.n`,
	} {
		t.Run(name, func(t *testing.T) {
			out, errs := newSet(t, oneRule(rules.Match{When: when})).Match(e)
			if len(out) != 0 {
				t.Errorf("`%s` must not match when it cannot be evaluated", when)
			}
			if len(errs) != 1 || !strings.Contains(errs[0].Error(), `rule "r"`) {
				t.Errorf("`%s`: want exactly one error naming the rule, got %v", when, errs)
			}
		})
	}
}

func TestMissingOrNullValueInBooleanPositionIsFalse(t *testing.T) {
	// `when: data.urgent` is a natural way to say "only urgent events", and an
	// event that does not carry the field is simply not urgent.
	for _, data := range []map[string]any{nil, {}, {"urgent": nil}, {"urgent": false}} {
		if fires(t, rules.Match{When: "data.urgent"}, sdk.Event{Source: "s", Type: "t", Data: data}) {
			t.Errorf("data %v must not match", data)
		}
	}
	if !fires(t, rules.Match{When: "data.urgent"}, sdk.Event{Source: "s", Type: "t", Data: map[string]any{"urgent": true}}) {
		t.Error("urgent=true must match")
	}
}

func TestAnEvaluationFailureNeverBlocksOtherRules(t *testing.T) {
	s := newSet(t,
		rules.Rule{Name: "broken", Match: rules.Match{When: "data.missing > 1"}, Actions: []rules.Action{{Sink: "s", Title: "x"}}},
		rules.Rule{Name: "before", Actions: []rules.Action{{Sink: "s", Title: "x"}}},
		rules.Rule{Name: "after", Match: rules.Match{Type: "t"}, Actions: []rules.Action{{Sink: "s", Title: "x"}}},
	)
	out, errs := s.Match(sdk.Event{Source: "s", Type: "t"})
	if names := ruleNames(out); !reflect.DeepEqual(names, []string{"before", "after"}) {
		t.Fatalf("fired %v, want the two healthy rules in order", names)
	}
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "broken") {
		t.Fatalf("errs = %v", errs)
	}
}

func TestFailureIsPerEventNotPerRule(t *testing.T) {
	s := newSet(t, oneRule(rules.Match{When: "data.percent > 90"}))
	if out, errs := s.Match(sdk.Event{Source: "s", Type: "t"}); len(out) != 0 || len(errs) != 1 {
		t.Fatalf("event without percent: out=%v errs=%v", out, errs)
	}
	// the very same rule keeps working for events that do have the field
	if out, errs := s.Match(sdk.Event{Source: "s", Type: "t", Data: map[string]any{"percent": 99.0}}); len(out) != 1 || len(errs) != 0 {
		t.Fatalf("a previous failure must not poison the rule: out=%v errs=%v", out, errs)
	}
}

func TestGlobsGateTheExpression(t *testing.T) {
	// The expression would fail on this event (no data), but the source glob
	// already rules the event out, so it must never run and never complain.
	s := newSet(t, oneRule(rules.Match{Source: "monitor", Type: "disk.*", When: "data.percent > 90"}))
	for _, e := range []sdk.Event{ev("someone-else", "disk.full"), ev("monitor", "cpu.high")} {
		out, errs := s.Match(e)
		if len(out) != 0 || len(errs) != 0 {
			t.Errorf("event %s/%s: out=%v errs=%v; want silently no match", e.Source, e.Type, out, errs)
		}
	}
}

func TestMatchingNeverModifiesTheEvent(t *testing.T) {
	e := sdk.Event{
		ID: "e", Source: "s", Type: "t", Subject: "sub", Time: time.Unix(10, 0),
		Data: map[string]any{"list": []any{1.0, 2.0}, "m": map[string]any{"k": "v"}, "n": 5.0},
	}
	snapshot := deepCopy(e)
	s := newSet(t,
		rules.Rule{Name: "a", Match: rules.Match{When: `len(data.list) > 1 && data.m.k == "v"`},
			Actions: []rules.Action{{Sink: "s", Title: "{{.Event.Data.n}} {{.Event.Data.list}} {{upper .Event.Subject}}", Body: `{{.Event.Data | json}}`}}},
		rules.Rule{Name: "b", Match: rules.Match{When: `data.absent > 1`}, Actions: []rules.Action{{Sink: "s", Title: "x"}}},
	)
	for i := 0; i < 3; i++ {
		_, _ = s.Match(e)
	}
	if !reflect.DeepEqual(e, snapshot) {
		t.Fatalf("Match modified its input:\n before %+v\n after  %+v", snapshot, e)
	}
}

func deepCopy(e sdk.Event) sdk.Event {
	c := e
	c.Data = copyMap(e.Data)
	return c
}

func copyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		switch t := v.(type) {
		case map[string]any:
			out[k] = copyMap(t)
		case []any:
			out[k] = append([]any(nil), t...)
		default:
			out[k] = v
		}
	}
	return out
}

func ruleNames(rs []rules.Rendered) []string {
	var names []string
	for _, r := range rs {
		names = append(names, r.Rule)
	}
	return names
}
