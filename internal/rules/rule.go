// Package rules decides what to send when an event arrives. It is pure: no
// database, no queue, no network. Give it rules at startup, then ask it which
// notifications an event produces.
//
// A rule says "when an event looks like this, send these messages":
//
//	name: disk-alerts
//	match:
//	  source: monitor            # optional glob
//	  type: "disk.*"             # optional glob
//	  when: "data.percent > 90"  # optional expression
//	actions:
//	  - sink: phone
//	    title: "Disk {{.Event.Subject}}"
//	    body:  "{{.Event.Data.percent}}% used"
//
// Every rule whose match succeeds fires (there is no first-match-wins), and
// each of its actions becomes one notification.
package rules

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"text/template"

	"github.com/expr-lang/expr/vm"

	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

// Rule is the declarative form of a rule, as written in the config file.
type Rule struct {
	Name    string   `yaml:"name"`
	Match   Match    `yaml:"match"`
	Actions []Action `yaml:"actions"`
}

// Match selects events. All specified criteria must hold; omitted ones match
// anything, so an empty Match matches every event.
type Match struct {
	// Source and Type are globs: * matches any run of characters (including
	// none), ? matches exactly one, everything else is literal.
	Source string `yaml:"source"`
	Type   string `yaml:"type"`
	// When is an expression evaluated against the event, for example
	// `data.percent > 90 && subject startsWith "/var"`. Variables: id,
	// source, type, subject, time, data. See https://expr-lang.org/docs/language-definition.
	When string `yaml:"when"`
}

// Action is one notification to produce when the rule matches. Title and Body
// are Go text/template strings; at least one must be non-empty.
//
// Template data: .Event (the sdk.Event, so .Event.Subject, .Event.Data.key ...)
// and .Rule (the rule name). Functions: upper, lower, trim, truncate, default,
// json, plus the standard library ones such as printf.
type Action struct {
	Sink  string `yaml:"sink"`
	Title string `yaml:"title"`
	Body  string `yaml:"body"`
}

// Rendered is a matched action with its templates filled in: one message to
// send to one sink.
type Rendered struct {
	Rule        string
	ActionIndex int // position within the rule's actions; part of the delivery identity
	Sink        string
	Title       string
	Body        string
}

// Set is an immutable, compiled collection of rules. It is safe for
// concurrent use.
type Set struct {
	rules []*compiled
}

type compiled struct {
	rule    Rule
	source  *regexp.Regexp // nil = any
	typ     *regexp.Regexp // nil = any
	when    *vm.Program    // nil = always true
	actions []compiledAction
}

type compiledAction struct {
	sink  string
	title *template.Template // nil = empty
	body  *template.Template // nil = empty
}

// NewSet validates and compiles rules. Every problem in every rule is
// reported, so a config author can fix them all in one pass. Bad globs,
// expressions that do not compile (including references to unknown variables),
// template syntax errors, duplicate names and empty rules are all caught here,
// at startup, rather than when the first event arrives.
func NewSet(rs []Rule) (*Set, error) {
	var errs []error
	seen := make(map[string]bool, len(rs))
	set := &Set{}

	for i, r := range rs {
		label := fmt.Sprintf("rule #%d", i+1)
		if strings.TrimSpace(r.Name) != "" {
			label = fmt.Sprintf("rule %q", r.Name)
		}

		var ruleErrs []error
		if strings.TrimSpace(r.Name) == "" {
			ruleErrs = append(ruleErrs, errors.New("name is required"))
		} else if seen[r.Name] {
			ruleErrs = append(ruleErrs, errors.New("duplicate rule name"))
		}
		seen[r.Name] = true

		c, cerrs := compile(r)
		ruleErrs = append(ruleErrs, cerrs...)

		if len(ruleErrs) > 0 {
			for _, e := range ruleErrs {
				errs = append(errs, fmt.Errorf("%s: %w", label, e))
			}
			continue
		}
		set.rules = append(set.rules, c)
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return set, nil
}

func compile(r Rule) (*compiled, []error) {
	var errs []error
	c := &compiled{rule: r}

	var err error
	if c.source, err = compileGlob(r.Match.Source); err != nil {
		errs = append(errs, fmt.Errorf("match.source: %w", err))
	}
	if c.typ, err = compileGlob(r.Match.Type); err != nil {
		errs = append(errs, fmt.Errorf("match.type: %w", err))
	}
	if strings.TrimSpace(r.Match.When) != "" {
		if c.when, err = compileWhen(r.Match.When); err != nil {
			errs = append(errs, fmt.Errorf("match.when: %w", err))
		}
	}

	if len(r.Actions) == 0 {
		errs = append(errs, errors.New("at least one action is required"))
	}
	for i, a := range r.Actions {
		ca := compiledAction{sink: strings.TrimSpace(a.Sink)}
		if ca.sink == "" {
			errs = append(errs, fmt.Errorf("actions[%d]: sink is required", i))
		}
		if a.Title == "" && a.Body == "" {
			errs = append(errs, fmt.Errorf("actions[%d]: title or body is required", i))
		}
		if ca.title, err = parseTemplate(fmt.Sprintf("%s/actions[%d]/title", r.Name, i), a.Title); err != nil {
			errs = append(errs, fmt.Errorf("actions[%d].title: %w", i, err))
		}
		if ca.body, err = parseTemplate(fmt.Sprintf("%s/actions[%d]/body", r.Name, i), a.Body); err != nil {
			errs = append(errs, fmt.Errorf("actions[%d].body: %w", i, err))
		}
		c.actions = append(c.actions, ca)
	}
	return c, errs
}

// Rules returns the rules in the set, in order.
func (s *Set) Rules() []Rule {
	out := make([]Rule, len(s.rules))
	for i, c := range s.rules {
		out[i] = c.rule
	}
	return out
}

// SinkNames returns the distinct sink names the rules refer to, sorted. The
// caller checks them against the sinks that actually exist.
func (s *Set) SinkNames() []string {
	seen := map[string]bool{}
	for _, c := range s.rules {
		for _, a := range c.actions {
			seen[a.sink] = true
		}
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Match returns the notifications ev produces, in rule order then action
// order. It never fails as a whole: a rule whose condition cannot be evaluated
// (say, `data.percent > 90` on an event with no percent) is treated as not
// matching, and an action whose template fails to render is skipped; each
// problem is returned in errs for the caller to log.
func (s *Set) Match(ev sdk.Event) (out []Rendered, errs []error) {
	for _, c := range s.rules {
		ok, err := c.matches(ev)
		if err != nil {
			errs = append(errs, fmt.Errorf("rule %q: evaluating match.when: %w", c.rule.Name, err))
			continue
		}
		if !ok {
			continue
		}
		data := templateData{Event: ev, Rule: c.rule.Name}
		for i, a := range c.actions {
			title, err := render(a.title, data)
			if err != nil {
				errs = append(errs, fmt.Errorf("rule %q: actions[%d].title: %w", c.rule.Name, i, err))
				continue
			}
			body, err := render(a.body, data)
			if err != nil {
				errs = append(errs, fmt.Errorf("rule %q: actions[%d].body: %w", c.rule.Name, i, err))
				continue
			}
			out = append(out, Rendered{Rule: c.rule.Name, ActionIndex: i, Sink: a.sink, Title: title, Body: body})
		}
	}
	return out, errs
}
