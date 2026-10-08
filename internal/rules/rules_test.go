package rules_test

// Specification under test: a rule SET as the operator experiences it.
//
//  - Loading (NewSet) validates everything up front and reports EVERY problem
//    at once, each locating the rule (and action) at fault. Nothing is half-loaded.
//  - Matching an event returns notifications in a stable, documented order:
//    rules in config order, then each rule's actions in order. Every
//    matching rule fires; there is no "first match wins".
//  - ActionIndex is the action's position in the configuration, whatever
//    happened to its siblings, because it is part of a delivery's identity.
//  - A loaded Set is immutable and deterministic.

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/djenkins-1/automation-hub/internal/rules"
	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

func act(sink, title string) rules.Action { return rules.Action{Sink: sink, Title: title} }

func TestAnEmptyRuleListIsValidAndMatchesNothing(t *testing.T) {
	for _, rs := range [][]rules.Rule{nil, {}} {
		s := newSet(t, rs...)
		out, errs := s.Match(ev("a", "b"))
		if len(out) != 0 || len(errs) != 0 {
			t.Fatalf("out=%v errs=%v", out, errs)
		}
		if len(s.Rules()) != 0 || len(s.SinkNames()) != 0 {
			t.Fatal("an empty set has no rules and no sinks")
		}
	}
}

func TestMinimalRulesAreValid(t *testing.T) {
	for name, a := range map[string]rules.Action{
		"title only": {Sink: "s", Title: "t"},
		"body only":  {Sink: "s", Body: "b"},
		"both":       {Sink: "s", Title: "t", Body: "b"},
	} {
		if _, err := rules.NewSet([]rules.Rule{{Name: "r", Actions: []rules.Action{a}}}); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestEachKindOfInvalidRuleIsRejectedAndLocated(t *testing.T) {
	okAction := []rules.Action{act("s", "t")}
	tests := []struct {
		name string
		rule rules.Rule
		want []string // fragments the error must contain
	}{
		{"no name", rules.Rule{Actions: okAction}, []string{"rule #1", "name is required"}},
		{"blank name", rules.Rule{Name: " \t", Actions: okAction}, []string{"rule #1", "name is required"}},
		{"no actions", rules.Rule{Name: "r"}, []string{`rule "r"`, "at least one action"}},
		{"empty actions", rules.Rule{Name: "r", Actions: []rules.Action{}}, []string{`rule "r"`, "at least one action"}},
		{"action without sink", rules.Rule{Name: "r", Actions: []rules.Action{{Title: "t"}}}, []string{`rule "r"`, "actions[0]", "sink is required"}},
		{"action with blank sink", rules.Rule{Name: "r", Actions: []rules.Action{{Sink: "  ", Title: "t"}}}, []string{"actions[0]", "sink is required"}},
		{"action without title or body", rules.Rule{Name: "r", Actions: []rules.Action{{Sink: "s"}}}, []string{"actions[0]", "title or body is required"}},
		{"second action broken", rules.Rule{Name: "r", Actions: []rules.Action{act("s", "t"), {Sink: "s"}}}, []string{"actions[1]", "title or body is required"}},
		{"expression syntax", rules.Rule{Name: "r", Match: rules.Match{When: "data.percent >"}, Actions: okAction}, []string{`rule "r"`, "match.when"}},
		{"expression with unknown variable", rules.Rule{Name: "r", Match: rules.Match{When: "dat.percent > 1"}, Actions: okAction}, []string{"match.when", "dat"}},
		{"expression that is not boolean", rules.Rule{Name: "r", Match: rules.Match{When: "1 + 1"}, Actions: okAction}, []string{"match.when"}},
		{"expression calling unknown function", rules.Rule{Name: "r", Match: rules.Match{When: "nosuch(data)"}, Actions: okAction}, []string{"match.when", "nosuch"}},
		{"expression with invalid regexp", rules.Rule{Name: "r", Match: rules.Match{When: `subject matches "("`}, Actions: okAction}, []string{"match.when"}},
		{"title syntax", rules.Rule{Name: "r", Actions: []rules.Action{{Sink: "s", Title: "{{"}}}, []string{"actions[0].title"}},
		{"body syntax", rules.Rule{Name: "r", Actions: []rules.Action{{Sink: "s", Title: "t", Body: "{{if}}"}}}, []string{"actions[0].body"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			set, err := rules.NewSet([]rules.Rule{tc.rule})
			if err == nil {
				t.Fatal("expected an error")
			}
			if set != nil {
				t.Error("a failed load must not return a half-built set")
			}
			for _, frag := range tc.want {
				if !strings.Contains(err.Error(), frag) {
					t.Errorf("error should contain %q:\n%v", frag, err)
				}
			}
		})
	}
}

func TestDuplicateNamesAreRejectedWhereverTheyAre(t *testing.T) {
	mk := func(names ...string) []rules.Rule {
		var rs []rules.Rule
		for _, n := range names {
			rs = append(rs, rules.Rule{Name: n, Actions: []rules.Action{act("s", "t")}})
		}
		return rs
	}
	for _, names := range [][]string{{"a", "a"}, {"a", "b", "a"}, {"x", "a", "b", "c", "x"}} {
		if _, err := rules.NewSet(mk(names...)); err == nil || !strings.Contains(err.Error(), "duplicate rule name") {
			t.Errorf("%v: err = %v", names, err)
		}
	}
	// Names are case-sensitive and compared exactly.
	if _, err := rules.NewSet(mk("Disk", "disk", "disk ", " disk")); err != nil {
		t.Errorf("names differing in case or whitespace are different names: %v", err)
	}
}

func TestEveryProblemInEveryRuleIsReportedTogether(t *testing.T) {
	good := rules.Rule{Name: "good", Actions: []rules.Action{act("s", "t")}}
	_, err := rules.NewSet([]rules.Rule{
		good,
		{Name: "no-actions"},
		{Name: "bad-when", Match: rules.Match{When: "data.x >"}, Actions: []rules.Action{act("s", "t")}},
		good,
		{Name: "bad-tmpl", Actions: []rules.Action{{Sink: "s", Title: "{{"}, {Sink: "", Title: "t"}, {Sink: "s"}}},
		{Name: "", Actions: []rules.Action{act("s", "t")}},
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	msg := err.Error()
	for _, want := range []string{
		`rule "no-actions"`, `rule "bad-when"`, `rule "good": duplicate`,
		`rule "bad-tmpl": actions[0].title`, `rule "bad-tmpl": actions[1]: sink is required`, `rule "bad-tmpl": actions[2]: title or body is required`,
		"rule #6", "name is required",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("missing %q in:\n%s", want, msg)
		}
	}
	if strings.Count(msg, "\n") < 6 {
		t.Errorf("expected one line per problem, got:\n%s", msg)
	}
}

func TestOneBrokenRuleNeverLoadsPartially(t *testing.T) {
	set, err := rules.NewSet([]rules.Rule{
		{Name: "fine", Actions: []rules.Action{act("s", "t")}},
		{Name: "broken"},
	})
	if err == nil || set != nil {
		t.Fatalf("set=%v err=%v: the hub must refuse to start with a rule it cannot honour", set, err)
	}
}

// ---------------------------------------------------------------------------
// Ordering and identity

func TestNotificationOrderIsRuleOrderThenActionOrder(t *testing.T) {
	s := newSet(t,
		rules.Rule{Name: "r1", Actions: []rules.Action{act("a", "r1a0"), act("b", "r1a1")}},
		rules.Rule{Name: "skip", Match: rules.Match{Type: "never"}, Actions: []rules.Action{act("x", "no")}},
		rules.Rule{Name: "r3", Actions: []rules.Action{act("c", "r3a0")}},
		rules.Rule{Name: "r4", Match: rules.Match{Source: "monitor"}, Actions: []rules.Action{act("d", "r4a0"), act("e", "r4a1"), act("f", "r4a2")}},
	)
	out, errs := s.Match(sdk.Event{Source: "monitor", Type: "disk.full"})
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	var got []string
	for _, o := range out {
		got = append(got, fmt.Sprintf("%s/%d/%s/%s", o.Rule, o.ActionIndex, o.Sink, o.Title))
	}
	want := []string{"r1/0/a/r1a0", "r1/1/b/r1a1", "r3/0/c/r3a0", "r4/0/d/r4a0", "r4/1/e/r4a1", "r4/2/f/r4a2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %v\nwant %v", got, want)
	}
}

func TestEveryMatchingRuleFiresNotJustTheFirst(t *testing.T) {
	var rs []rules.Rule
	for i := 0; i < 5; i++ {
		rs = append(rs, rules.Rule{Name: fmt.Sprintf("r%d", i), Match: rules.Match{Type: "disk.*"}, Actions: []rules.Action{act("s", "t")}})
	}
	out, _ := newSet(t, rs...).Match(ev("m", "disk.full"))
	if len(out) != 5 {
		t.Fatalf("%d of 5 matching rules fired", len(out))
	}
}

func TestActionIndexIsThePositionInTheConfiguration(t *testing.T) {
	// Index 1 renders badly and is skipped; its neighbours keep indexes 0 and 2.
	// Delivery identity (and so duplicate suppression) depends on this.
	s := newSet(t, rules.Rule{Name: "r", Actions: []rules.Action{
		act("a", "ok"), {Sink: "b", Title: "{{index .Event.Data.nothing 3}}"}, act("c", "ok"),
	}})
	out, errs := s.Match(sdk.Event{Source: "s", Type: "t", Data: map[string]any{"nothing": []any{}}})
	if len(errs) != 1 || len(out) != 2 || out[0].ActionIndex != 0 || out[1].ActionIndex != 2 {
		t.Fatalf("out=%+v errs=%v", out, errs)
	}
}

func TestRenderedCarriesEverythingTheEngineNeeds(t *testing.T) {
	s := newSet(t, rules.Rule{Name: "disk", Actions: []rules.Action{{Sink: "phone", Title: "T {{.Event.Subject}}", Body: "B {{.Rule}}"}}})
	out, _ := s.Match(sdk.Event{Source: "m", Type: "t", Subject: "/var"})
	want := rules.Rendered{Rule: "disk", ActionIndex: 0, Sink: "phone", Title: "T /var", Body: "B disk"}
	if len(out) != 1 || out[0] != want {
		t.Fatalf("got %+v, want %+v", out, want)
	}
}

func TestSinkNamesAreTrimmedConsistently(t *testing.T) {
	s := newSet(t, rules.Rule{Name: "r", Actions: []rules.Action{{Sink: "  phone\t", Title: "t"}}})
	out, _ := s.Match(ev("a", "b"))
	if len(out) != 1 || out[0].Sink != "phone" {
		t.Fatalf("rendered sink = %q", out[0].Sink)
	}
	if got := s.SinkNames(); !reflect.DeepEqual(got, []string{"phone"}) {
		t.Fatalf("SinkNames = %v", got)
	}
}

func TestSinkNamesAreSortedAndDistinct(t *testing.T) {
	s := newSet(t,
		rules.Rule{Name: "a", Actions: []rules.Action{act("zeta", "t"), act("alpha", "t"), act("zeta", "t")}},
		rules.Rule{Name: "b", Actions: []rules.Action{act("mid", "t"), act("alpha", "t")}},
	)
	if got := s.SinkNames(); !reflect.DeepEqual(got, []string{"alpha", "mid", "zeta"}) {
		t.Fatalf("SinkNames = %v", got)
	}
}

func TestRulesAreReturnedInConfigurationOrder(t *testing.T) {
	s := newSet(t,
		rules.Rule{Name: "c", Actions: []rules.Action{act("s", "t")}},
		rules.Rule{Name: "a", Actions: []rules.Action{act("s", "t")}},
		rules.Rule{Name: "b", Actions: []rules.Action{act("s", "t")}},
	)
	var names []string
	for _, r := range s.Rules() {
		names = append(names, r.Name)
	}
	if !reflect.DeepEqual(names, []string{"c", "a", "b"}) {
		t.Fatalf("Rules() = %v", names)
	}
}

// ---------------------------------------------------------------------------
// Immutability and determinism

func TestALoadedSetIsUnaffectedByLaterChangesToItsInputs(t *testing.T) {
	input := []rules.Rule{{
		Name: "r", Match: rules.Match{Type: "disk.*"},
		Actions: []rules.Action{{Sink: "phone", Title: "original {{.Event.Subject}}"}},
	}}
	s := newSet(t, input...)
	before, _ := s.Match(sdk.Event{Source: "m", Type: "disk.full", Subject: "x"})

	// The caller keeps editing its slices after loading...
	input[0].Name = "renamed"
	input[0].Match.Type = "cpu.*"
	input[0].Actions[0].Sink = "elsewhere"
	input[0].Actions[0].Title = "tampered"
	input[0] = rules.Rule{}
	// ...and even edits what Rules() handed back.
	for _, r := range s.Rules() {
		r.Name = "tampered"
		if len(r.Actions) > 0 {
			r.Actions[0].Title = "tampered"
		}
	}

	after, errs := s.Match(sdk.Event{Source: "m", Type: "disk.full", Subject: "x"})
	if len(errs) != 0 || !reflect.DeepEqual(before, after) {
		t.Fatalf("behaviour changed after the inputs were modified:\n before %+v\n after  %+v\n errs %v", before, after, errs)
	}
	if got, _ := s.Match(sdk.Event{Source: "m", Type: "cpu.high"}); len(got) != 0 {
		t.Fatal("the match criteria were changed through the caller's copy")
	}
}

func TestMatchingIsDeterministic(t *testing.T) {
	s := newSet(t,
		rules.Rule{Name: "a", Match: rules.Match{When: "data.n > 1"}, Actions: []rules.Action{{Sink: "x", Title: "{{.Event.Data.n}}"}, act("y", "t")}},
		rules.Rule{Name: "b", Actions: []rules.Action{act("z", "{{.Event.Subject}}")}},
	)
	e := sdk.Event{Source: "s", Type: "t", Subject: "sub", Data: map[string]any{"n": 5.0}}
	first, _ := s.Match(e)
	for i := 0; i < 100; i++ {
		if got, _ := s.Match(e); !reflect.DeepEqual(got, first) {
			t.Fatalf("run %d differs:\n%+v\n%+v", i, got, first)
		}
	}
}

func TestLargeRuleSetsBehaveLikeSmallOnes(t *testing.T) {
	const n = 2000
	var rs []rules.Rule
	for i := 0; i < n; i++ {
		rs = append(rs, rules.Rule{
			Name:    fmt.Sprintf("rule-%04d", i),
			Match:   rules.Match{Type: fmt.Sprintf("type-%d", i%50), When: fmt.Sprintf("data.n == %d", i%7)},
			Actions: []rules.Action{act("s", fmt.Sprintf("title-%d", i))},
		})
	}
	s := newSet(t, rs...)
	out, errs := s.Match(sdk.Event{Source: "s", Type: "type-3", Data: map[string]any{"n": 3.0}})
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	want := 0
	for i := 0; i < n; i++ {
		if i%50 == 3 && i%7 == 3 {
			want++
		}
	}
	if len(out) != want || want == 0 {
		t.Fatalf("%d notifications, want %d", len(out), want)
	}
	for i := 1; i < len(out); i++ {
		if out[i-1].Rule >= out[i].Rule {
			t.Fatalf("results out of rule order: %s then %s", out[i-1].Rule, out[i].Rule)
		}
	}
}
