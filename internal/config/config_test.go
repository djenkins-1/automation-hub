package config_test

// Specification under test: what an operator can rely on when writing hub.yaml.
//
//  - A missing/empty document is valid and yields documented defaults.
//  - Loading is strict: unknown keys and wrong types are errors (a typo must
//    never silently fall back to a default), durations need units.
//  - Values are validated; EVERY problem is reported at once, naming what is wrong.
//  - ${VAR} expansion applies to storage.path and to strings under sinks, and
//    nowhere else; unset variables are errors; $${VAR} is a literal.
//  - Rules and sinks are cross-checked: no duplicate sinks, no rule that
//    delivers to a sink that does not exist.
//  - The shipped example file is valid and documents every setting.

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/djenkins-1/automation-hub/internal/config"
)

const okRule = `rules: [{ name: r, actions: [{ sink: s, title: t }] }]`
const okSink = `sinks: [{ name: s, type: stdout }]`

func mustParse(t *testing.T, yaml string) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("Parse:\n%s\nerror: %v", yaml, err)
	}
	return cfg
}

func mustFail(t *testing.T, yaml string, mention ...string) error {
	t.Helper()
	cfg, err := config.Parse([]byte(yaml))
	if err == nil {
		t.Fatalf("expected an error for:\n%s", yaml)
	}
	if cfg != nil {
		t.Errorf("a failed parse must not return a config")
	}
	for _, m := range mention {
		if !strings.Contains(err.Error(), m) {
			t.Errorf("error should mention %q, got:\n%v", m, err)
		}
	}
	return err
}

// ---------------------------------------------------------------------------
// Defaults and overrides

func TestEmptyDocumentsYieldDocumentedDefaults(t *testing.T) {
	for name, doc := range map[string]string{
		"empty": "", "whitespace": "  \n\n\t\n", "comments only": "# nothing here\n# at all\n", "document marker": "---\n",
		"empty sections": "storage: {}\nqueue: {}\nlog: {}\nsinks: []\nrules: []\n",
	} {
		cfg := mustParse(t, doc)
		if cfg.Storage.Path != "hub.db" ||
			cfg.Queue.Workers != 4 || cfg.Queue.Lease != 30*time.Second || cfg.Queue.PollEvery != 500*time.Millisecond ||
			cfg.Queue.MaxAttempts != 6 || cfg.Queue.RetryBase != 5*time.Second || cfg.Queue.RetryMax != 15*time.Minute ||
			cfg.Log.Level != "info" || cfg.Log.Format != "text" ||
			len(cfg.Sinks) != 0 || len(cfg.Rules) != 0 {
			t.Errorf("%s: unexpected defaults: %+v", name, cfg)
		}
	}
}

func TestEachSettingCanBeOverriddenIndependently(t *testing.T) {
	check := func(doc string, ok func(c *config.Config) bool) {
		t.Helper()
		c := mustParse(t, doc)
		if !ok(c) {
			t.Errorf("override not applied for %q: %+v", doc, c)
		}
		// everything else keeps its default
		def := mustParse(t, "")
		if doc != "" && reflect.DeepEqual(c, def) {
			t.Errorf("%q changed nothing", doc)
		}
	}
	check("storage: { path: /data/x.db }", func(c *config.Config) bool { return c.Storage.Path == "/data/x.db" })
	check("queue: { workers: 9 }", func(c *config.Config) bool { return c.Queue.Workers == 9 && c.Queue.Lease == 30*time.Second })
	check("queue: { lease: 2m }", func(c *config.Config) bool { return c.Queue.Lease == 2*time.Minute && c.Queue.Workers == 4 })
	check("queue: { poll_every: 50ms }", func(c *config.Config) bool { return c.Queue.PollEvery == 50*time.Millisecond })
	check("queue: { max_attempts: 1 }", func(c *config.Config) bool { return c.Queue.MaxAttempts == 1 })
	check("queue: { retry_base: 1s }", func(c *config.Config) bool { return c.Queue.RetryBase == time.Second })
	check("queue: { retry_max: 1h }", func(c *config.Config) bool { return c.Queue.RetryMax == time.Hour })
	check("log: { level: debug }", func(c *config.Config) bool { return c.Log.Level == "debug" && c.Log.Format == "text" })
	check("log: { format: json }", func(c *config.Config) bool { return c.Log.Format == "json" && c.Log.Level == "info" })
}

func TestDurationsNeedUnitsAndAcceptCompoundForms(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"30s": 30 * time.Second, "1m30s": 90 * time.Second, "1.5s": 1500 * time.Millisecond, "250ms": 250 * time.Millisecond,
		"2h": 2 * time.Hour, "2h30m": 150 * time.Minute,
	} {
		if got := mustParse(t, "queue: { lease: "+in+" }").Queue.Lease; got != want {
			t.Errorf("lease %s = %v, want %v", in, got, want)
		}
	}
	for _, bad := range []string{"30", "0.5", `"30"`, "soon", "10 seconds", "-", "s", "1d"} {
		mustFail(t, "queue: { lease: "+bad+" }")
	}
}

func TestCRLFAndQuotedStyles(t *testing.T) {
	cfg := mustParse(t, "queue:\r\n  workers: 7\r\nlog:\r\n  level: \"warn\"\r\n")
	if cfg.Queue.Workers != 7 || cfg.Log.Level != "warn" {
		t.Fatalf("%+v", cfg)
	}
}

// ---------------------------------------------------------------------------
// Strictness

func TestUnknownKeysAreErrorsAtEveryLevel(t *testing.T) {
	for name, tc := range map[string]struct{ doc, key string }{
		"top level":      {"stoarge: { path: x }", "stoarge"},
		"storage":        {"storage: { paht: x }", "paht"},
		"queue":          {"queue: { leas: 30s }", "leas"},
		"queue workers":  {"queue: { worker: 3 }", "worker"},
		"log":            {"log: { lvl: info }", "lvl"},
		"rule":           {"rules: [{ name: r, mach: {} }]", "mach"},
		"match":          {"rules: [{ name: r, match: { sorce: x }, actions: [] }]", "sorce"},
		"action":         {"rules: [{ name: r, actions: [{ sink: s, titel: t }] }]", "titel"},
		"case sensitive": {"Queue: { workers: 3 }", "Queue"},
	} {
		t.Run(name, func(t *testing.T) { mustFail(t, tc.doc, tc.key) })
	}
}

func TestFractionalValuesForWholeNumberSettingsAreErrors(t *testing.T) {
	// Silently truncating these (2.5 -> 2, 0.5 -> 0 -> "use the default") would hide a typo.
	mustFail(t, "queue: { workers: 2.5 }", "queue.workers", "whole number")
	mustFail(t, "queue: { workers: 0.5 }", "queue.workers")
	mustFail(t, "queue: { max_attempts: 3.7 }", "queue.max_attempts", "whole number")
	mustFail(t, "queue: { workers: 1.5, max_attempts: 2.5 }", "queue.workers", "queue.max_attempts")
	mustParse(t, "queue: { workers: 3, max_attempts: 4 }")
}

func TestWrongTypesAreErrors(t *testing.T) {
	for name, doc := range map[string]string{
		"workers as text":         `queue: { workers: four }`,
		"max_attempts as list":    `queue: { max_attempts: [1] }`,
		"path as list":            `storage: { path: [a, b] }`,
		"path as mapping":         `storage: { path: { a: b } }`,
		"sinks as mapping":        `sinks: { name: s }`,
		"sink entry as string":    `sinks: ["stdout"]`,
		"sink entry as list":      `sinks: [[a, b]]`,
		"rules as mapping":        `rules: { name: r }`,
		"rule name as list":       `rules: [{ name: [a], actions: [] }]`,
		"actions as string":       `rules: [{ name: r, actions: "x" }]`,
		"not yaml at all":         "queue: [unclosed",
		"tab indentation":         "queue:\n\tworkers: 3\n",
		"duplicate top-level key": "queue: { workers: 1 }\nqueue: { workers: 2 }\n",
	} {
		t.Run(name, func(t *testing.T) { mustFail(t, doc) })
	}
}

// ---------------------------------------------------------------------------
// Validation

func TestNumericBoundaries(t *testing.T) {
	for _, ok := range []string{
		"queue: { workers: 1 }", "queue: { max_attempts: 1 }", "queue: { lease: 1ms }",
		"queue: { retry_base: 1s, retry_max: 1s }", // equal is fine
	} {
		mustParse(t, ok)
	}
	for doc, mention := range map[string]string{
		"queue: { workers: -1 }":                    "queue.workers",
		"queue: { max_attempts: -1 }":               "queue.max_attempts",
		"queue: { lease: -1s }":                     "queue.lease",
		"queue: { poll_every: -1ms }":               "queue.poll_every",
		"queue: { retry_base: -1s }":                "queue.retry_base",
		"queue: { retry_max: -1s }":                 "queue.retry_max",
		"queue: { retry_base: 10s, retry_max: 9s }": "retry_max",
		"queue: { retry_base: 20m }":                "retry_max", // base above the default ceiling
	} {
		mustFail(t, doc, mention)
	}
}

func TestLogSettingsAreExactLowercaseKeywords(t *testing.T) {
	for _, l := range []string{"debug", "info", "warn", "error"} {
		mustParse(t, "log: { level: "+l+" }")
	}
	for _, f := range []string{"text", "json"} {
		mustParse(t, "log: { format: "+f+" }")
	}
	for _, bad := range []string{"INFO", "Info", "warning", "trace", "loud", "off", `" info"`} {
		mustFail(t, "log: { level: "+bad+" }", "log.level")
	}
	for _, bad := range []string{"JSON", "xml", "logfmt", "yaml"} {
		mustFail(t, "log: { format: "+bad+" }", "log.format")
	}
}

func TestEveryIndependentProblemIsReportedTogether(t *testing.T) {
	err := mustFail(t, `
queue: { workers: -2, lease: -1s }
log: { level: loud, format: xml }
sinks:
  - { name: dup, type: stdout }
  - { name: dup, type: stdout }
  - { type: stdout }
rules:
  - name: r
    actions: [{ sink: ghost, title: t }]
`)
	for _, want := range []string{"queue.workers", "queue.lease", "log.level", "log.format", `sink "dup" is defined more than once`, "name is required"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %q in:\n%v", want, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Sinks

func TestSinkEntriesSplitIntoNameTypeAndOptions(t *testing.T) {
	cfg := mustParse(t, `
sinks:
  - name: console
    type: stdout
  - name: alt
    type: stdout
    url: "https://example.com/x"
    retries: 3
    ratio: 1.5
    enabled: true
    headers: { A: "1", B: two }
    list: [a, 2, { k: v }]
`)
	if len(cfg.Sinks) != 2 {
		t.Fatalf("%d sinks", len(cfg.Sinks))
	}
	if s := cfg.Sinks[0]; s.Name != "console" || s.Type != "stdout" || len(s.Options) != 0 {
		t.Errorf("first sink: %+v", s)
	}
	s := cfg.Sinks[1]
	if s.Name != "alt" || s.Type != "stdout" {
		t.Fatalf("second sink: %+v", s)
	}
	if _, leaked := s.Options["name"]; leaked {
		t.Error("name must not be passed on as an option")
	}
	if _, leaked := s.Options["type"]; leaked {
		t.Error("type must not be passed on as an option")
	}
	o := s.Options
	if o["url"] != "https://example.com/x" || o["enabled"] != true || o["ratio"] != 1.5 || num(o["retries"]) != "3" {
		t.Errorf("scalar options not preserved: %#v", o)
	}
	if h, _ := o["headers"].(map[string]any); h["A"] != "1" || h["B"] != "two" {
		t.Errorf("nested mapping not preserved: %#v", o["headers"])
	}
	if l, _ := o["list"].([]any); len(l) != 3 || l[0] != "a" || num(l[1]) != "2" || l[2].(map[string]any)["k"] != "v" {
		t.Errorf("list not preserved: %#v", o["list"])
	}
}

func num(v any) string { return fmt.Sprint(v) }

func TestSinksKeepTheirConfiguredOrder(t *testing.T) {
	cfg := mustParse(t, "sinks: [{ name: c, type: stdout }, { name: a, type: stdout }, { name: b, type: stdout }]")
	var got []string
	for _, s := range cfg.Sinks {
		got = append(got, s.Name)
	}
	if !reflect.DeepEqual(got, []string{"c", "a", "b"}) {
		t.Fatalf("order = %v", got)
	}
}

func TestSinkNameAndTypeRules(t *testing.T) {
	mustFail(t, "sinks: [{ type: stdout }]", "name is required")
	mustFail(t, "sinks: [{ name: x }]", "type is required")
	mustFail(t, `sinks: [{ name: "", type: stdout }]`, "name")
	mustFail(t, `sinks: [{ name: "   ", type: stdout }]`, "name")
	mustFail(t, `sinks: [{ name: x, type: "" }]`, "type")
	mustFail(t, "sinks: [{ name: 5, type: stdout }]", "name must be a non-empty string")
	mustFail(t, "sinks: [{ name: x, type: [stdout] }]", "type must be a non-empty string")
	mustFail(t, "sinks: [{ name: x, type: true }]", "type must be a non-empty string")
}

func TestDuplicateSinkNamesAreRejected(t *testing.T) {
	mustFail(t, "sinks: [{ name: a, type: stdout }, { name: a, type: stdout }]", `sink "a" is defined more than once`)
	mustFail(t, "sinks: [{ name: a, type: stdout }, { name: b, type: stdout }, { name: a, type: stdout }]", `"a"`)
	mustParse(t, "sinks: [{ name: a, type: stdout }, { name: A, type: stdout }]") // case-sensitive
}

func TestSinkNamesIgnoreSurroundingWhitespaceLikeRulesDo(t *testing.T) {
	// A rule's `sink: " console "` is trimmed, so a sink declared with stray
	// whitespace must be reachable under the trimmed name.
	cfg := mustParse(t, `
sinks: [{ name: " console ", type: stdout }]
rules: [{ name: r, actions: [{ sink: console, title: t }] }]
`)
	if cfg.Sinks[0].Name != "console" {
		t.Fatalf("name = %q", cfg.Sinks[0].Name)
	}
	mustFail(t, `sinks: [{ name: "a", type: stdout }, { name: " a ", type: stdout }]`, "defined more than once")
}

// ---------------------------------------------------------------------------
// Rules and their sinks

func TestRulesMustReferToDefinedSinks(t *testing.T) {
	mustParse(t, okSink+"\n"+okRule)
	mustFail(t, `rules: [{ name: r, actions: [{ sink: ghost, title: t }] }]`, `sink "ghost"`)
	mustFail(t, okSink+"\n"+`rules: [{ name: r, actions: [{ sink: s, title: t }, { sink: ghost, title: t }] }]`, `"ghost"`)
	mustFail(t, `sinks: [{ name: Phone, type: stdout }]
rules: [{ name: r, actions: [{ sink: phone, title: t }] }]`, `"phone"`) // case-sensitive
	// A sink that no rule uses is fine.
	mustParse(t, `sinks: [{ name: a, type: stdout }, { name: unused, type: stdout }]
rules: [{ name: r, actions: [{ sink: a, title: t }] }]`)
}

func TestInvalidRulesAreReportedWithTheirName(t *testing.T) {
	mustFail(t, okSink+`
rules: [{ name: broken, match: { when: "data.x >" }, actions: [{ sink: s, title: t }] }]`, `rule "broken"`, "match.when")
	mustFail(t, okSink+`
rules: [{ name: broken, actions: [{ sink: s, title: "{{" }] }]`, `rule "broken"`, "title")
	mustFail(t, okSink+`
rules: [{ name: a, actions: [{ sink: s, title: t }] }, { name: a, actions: [{ sink: s, title: t }] }]`, "duplicate rule name")
}

func TestRulesAreParsedIntoTheirFields(t *testing.T) {
	cfg := mustParse(t, okSink+`
rules:
  - name: disk
    match: { source: monitor, type: "disk.*", when: "data.percent > 90" }
    actions:
      - { sink: s, title: "T {{.Event.Subject}}", body: "B" }
      - { sink: s, body: "only body" }
`)
	r := cfg.Rules[0]
	if r.Name != "disk" || r.Match.Source != "monitor" || r.Match.Type != "disk.*" || r.Match.When != "data.percent > 90" ||
		len(r.Actions) != 2 || r.Actions[0].Title != "T {{.Event.Subject}}" || r.Actions[0].Body != "B" ||
		r.Actions[1].Title != "" || r.Actions[1].Body != "only body" {
		t.Fatalf("rule parsed wrongly: %+v", r)
	}
}

// ---------------------------------------------------------------------------
// Environment expansion

func TestEnvExpansionAppliesToStoragePathAndSinkStringsOnly(t *testing.T) {
	t.Setenv("HUBT_DIR", "/srv/hub")
	t.Setenv("HUBT_TOKEN", "s3cret")
	t.Setenv("HUBT_LEVEL", "debug")
	t.Setenv("HUBT_NAME", "from-env")

	cfg := mustParse(t, `
storage: { path: "${HUBT_DIR}/hub.db" }
sinks:
  - name: x
    type: stdout
    token: "Bearer ${HUBT_TOKEN}"
    both: "${HUBT_DIR}:${HUBT_TOKEN}"
    adjacent: "${HUBT_DIR}${HUBT_TOKEN}"
    nested: { list: ["a-${HUBT_TOKEN}", 5, { deep: "${HUBT_DIR}" }] }
    "${HUBT_NAME}": keys-are-not-expanded
rules:
  - name: ${HUBT_NAME}
    match: { source: "${HUBT_NAME}" }
    actions: [{ sink: x, title: "${HUBT_NAME} costs $5", body: "${HUBT_NAME}" }]
`)
	if cfg.Storage.Path != "/srv/hub/hub.db" {
		t.Errorf("storage.path = %q", cfg.Storage.Path)
	}
	o := cfg.Sinks[0].Options
	if o["token"] != "Bearer s3cret" || o["both"] != "/srv/hub:s3cret" || o["adjacent"] != "/srv/hubs3cret" {
		t.Errorf("options not expanded: %#v", o)
	}
	nested := o["nested"].(map[string]any)["list"].([]any)
	if nested[0] != "a-s3cret" || nested[2].(map[string]any)["deep"] != "/srv/hub" {
		t.Errorf("nested values not expanded: %#v", nested)
	}
	if _, ok := o["${HUBT_NAME}"]; !ok {
		t.Errorf("keys must be left alone: %#v", o)
	}
	// Rules are never expanded: a template or name may legitimately contain "${".
	r := cfg.Rules[0]
	if r.Name != "${HUBT_NAME}" || r.Match.Source != "${HUBT_NAME}" || r.Actions[0].Title != "${HUBT_NAME} costs $5" || r.Actions[0].Body != "${HUBT_NAME}" {
		t.Errorf("rules were expanded: %+v", r)
	}
	// Neither are the log settings.
	mustFail(t, `log: { level: "${HUBT_LEVEL}" }`, "log.level")
}

func TestOnlyWellFormedReferencesAreExpanded(t *testing.T) {
	t.Setenv("HUBT_X", "X")
	cfg := mustParse(t, `
sinks:
  - name: a
    type: stdout
    bare: "$HUBT_X"
    empty: "${}"
    digit: "${1BAD}"
    space: "${HUBT_X }"
    open: "${HUBT_X"
    dollar: "cost $5 or $$"
    good: "${HUBT_X}"
`)
	o := cfg.Sinks[0].Options
	for k, want := range map[string]string{
		"bare": "$HUBT_X", "empty": "${}", "digit": "${1BAD}", "space": "${HUBT_X }", "open": "${HUBT_X", "dollar": "cost $5 or $$", "good": "X",
	} {
		if o[k] != want {
			t.Errorf("%s = %q, want %q", k, o[k], want)
		}
	}
}

func TestDoubleDollarEscapesAReference(t *testing.T) {
	// No lookup happens for an escaped reference, so it needs no variable.
	t.Setenv("HUBT_SET", "real")
	cfg := mustParse(t, `sinks: [{ name: a, type: stdout, lit: "$${HUBT_NEVER_SET}", mixed: "$${HUBT_NEVER_SET}/${HUBT_SET}" }]`)
	o := cfg.Sinks[0].Options
	if o["lit"] != "${HUBT_NEVER_SET}" || o["mixed"] != "${HUBT_NEVER_SET}/real" {
		t.Fatalf("escape handling wrong: %#v", o)
	}
}

func TestExpansionIsSingleShot(t *testing.T) {
	t.Setenv("HUBT_OUTER", "value-with-${HUBT_INNER}-inside")
	t.Setenv("HUBT_INNER", "SHOULD-NOT-APPEAR")
	cfg := mustParse(t, `sinks: [{ name: a, type: stdout, v: "${HUBT_OUTER}" }]`)
	if got := cfg.Sinks[0].Options["v"]; got != "value-with-${HUBT_INNER}-inside" {
		t.Fatalf("expanded values must not be expanded again, got %q", got)
	}
}

func TestEnvValuesAreInsertedVerbatim(t *testing.T) {
	weird := "quote\" apostrophe' backslash\\ newline\n colon: hash # brace }{ [ ] 日本 🚨"
	t.Setenv("HUBT_WEIRD", weird)
	cfg := mustParse(t, `sinks: [{ name: a, type: stdout, v: "${HUBT_WEIRD}" }]`)
	if got := cfg.Sinks[0].Options["v"]; got != weird {
		t.Fatalf("value changed in transit:\n got  %q\n want %q", got, weird)
	}
}

func TestSetButEmptyVariablesAreFine(t *testing.T) {
	t.Setenv("HUBT_EMPTY", "")
	cfg := mustParse(t, `sinks: [{ name: a, type: stdout, v: "[${HUBT_EMPTY}]" }]`)
	if cfg.Sinks[0].Options["v"] != "[]" {
		t.Fatalf("v = %v", cfg.Sinks[0].Options["v"])
	}
}

func TestUnsetVariablesAreErrorsListedOnceEach(t *testing.T) {
	_ = os.Unsetenv("HUBT_MISSING_A")
	_ = os.Unsetenv("HUBT_MISSING_B")
	_ = os.Unsetenv("HUBT_MISSING_C")
	err := mustFail(t, `
storage: { path: "${HUBT_MISSING_C}/hub.db" }
sinks:
  - { name: a, type: stdout, x: "${HUBT_MISSING_B}", y: "${HUBT_MISSING_B}" }
  - { name: b, type: stdout, nested: { z: ["${HUBT_MISSING_A}"] } }
`)
	if !strings.Contains(err.Error(), "HUBT_MISSING_A, HUBT_MISSING_B, HUBT_MISSING_C") {
		t.Fatalf("want all three names, sorted and each once, in:\n%v", err)
	}
	if strings.Count(err.Error(), "HUBT_MISSING_B") != 1 {
		t.Fatalf("a name used twice should be listed once:\n%v", err)
	}
}

// ---------------------------------------------------------------------------
// Loading from files, and purity

func TestLoadReadsAFileAndNamesItInErrors(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "good.yaml")
	bad := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(good, []byte("queue: { workers: 3 }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bad, []byte("queue: { workers: -3 }\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.Load(good)
	if err != nil || cfg.Queue.Workers != 3 {
		t.Fatalf("good: %+v, %v", cfg, err)
	}
	if _, err := config.Load(bad); err == nil || !strings.Contains(err.Error(), bad) || !strings.Contains(err.Error(), "queue.workers") {
		t.Fatalf("a validation error should name the file and the problem, got: %v", err)
	}
	if _, err := config.Load(filepath.Join(dir, "missing.yaml")); !os.IsNotExist(rootCause(err)) {
		t.Fatalf("a missing file should surface as not-exist, got: %v", err)
	}
	if _, err := config.Load(dir); err == nil {
		t.Fatal("loading a directory must fail")
	}
}

func TestParseIsPureAndResultsAreIndependent(t *testing.T) {
	doc := []byte(okSink + "\n" + okRule + "\nqueue: { workers: 3 }\n")
	a, err := config.Parse(doc)
	if err != nil {
		t.Fatal(err)
	}
	b, err := config.Parse(doc)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatal("the same input must give equal configs")
	}
	a.Queue.Workers = 99
	a.Rules[0].Name = "changed"
	a.Sinks[0].Options["injected"] = true
	c, _ := config.Parse(doc)
	if !reflect.DeepEqual(b, c) || c.Queue.Workers != 3 {
		t.Fatal("mutating one parsed config must not affect later ones")
	}
}

func rootCause(err error) error {
	for {
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return err
		}
		err = u.Unwrap()
	}
}

// ---------------------------------------------------------------------------
// The example file

func TestExampleConfigIsValidAndComplete(t *testing.T) {
	t.Setenv("HUB_TOKEN", "example-test-token") // the example reads its token from the environment
	cfg, err := config.Load("../../hub.example.yaml")
	if err != nil {
		t.Fatalf("the shipped example must load: %v", err)
	}
	if len(cfg.Sinks) == 0 || len(cfg.Rules) == 0 {
		t.Fatal("the example should demonstrate at least one sink and one rule")
	}
	// Its stated values must be the real defaults, or users copying it get surprises.
	def := mustParse(t, "")
	if cfg.Queue != def.Queue || cfg.Log != def.Log {
		t.Errorf("the example's queue/log settings differ from the defaults it documents:\n example  %+v %+v\n defaults %+v %+v",
			cfg.Queue, cfg.Log, def.Queue, def.Log)
	}
}

// Adding a setting to the code without documenting it in the example fails here.
func TestExampleDocumentsEverySetting(t *testing.T) {
	raw, err := os.ReadFile("../../hub.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, section := range []any{config.StorageConfig{}, config.QueueConfig{}, config.LogConfig{}} {
		typ := reflect.TypeOf(section)
		for i := 0; i < typ.NumField(); i++ {
			key := strings.Split(typ.Field(i).Tag.Get("yaml"), ",")[0]
			if key == "" {
				t.Errorf("%s.%s has no yaml tag", typ.Name(), typ.Field(i).Name)
				continue
			}
			if !strings.Contains(text, key+":") {
				t.Errorf("setting %q (%s) is not documented in hub.example.yaml", key, typ.Name())
			}
		}
	}
	for _, key := range []string{"storage:", "queue:", "log:", "sinks:", "rules:", "match:", "actions:"} {
		if !strings.Contains(text, key) {
			t.Errorf("example is missing section %q", key)
		}
	}
}
