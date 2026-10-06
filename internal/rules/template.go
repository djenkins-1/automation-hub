package rules

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"text/template"

	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

// templateData is what title and body templates see as ".".
type templateData struct {
	Event sdk.Event
	Rule  string
}

// A deliberately small, side-effect-free function set. Notable: truncate and
// default are written so they read naturally at the end of a pipeline:
//
//	{{.Event.Subject | truncate 40}}
//	{{.Event.Data.host | default "unknown host"}}
var funcs = template.FuncMap{
	"upper": strings.ToUpper,
	"lower": strings.ToLower,
	"trim":  strings.TrimSpace,
	"truncate": func(n int, s string) string {
		r := []rune(s)
		if n < 0 || len(r) <= n {
			return s
		}
		if n <= 1 {
			return string(r[:n])
		}
		return string(r[:n-1]) + "…"
	},
	"default": func(fallback, v any) any {
		if v == nil || reflect.ValueOf(v).IsZero() {
			return fallback
		}
		return v
	},
	"json": func(v any) (string, error) {
		b, err := json.Marshal(v)
		return string(b), err
	},
}

// parseTemplate compiles text. Empty text yields a nil template, which
// renders as the empty string.
func parseTemplate(name, text string) (*template.Template, error) {
	if text == "" {
		return nil, nil
	}
	return template.New(name).Funcs(funcs).Parse(text)
}

// render executes t. Missing map keys (an event without the field a template
// mentions) render as nothing, not as "<no value>": a slightly imperfect
// notification is better than none.
func render(t *template.Template, data templateData) (string, error) {
	if t == nil {
		return "", nil
	}
	var b strings.Builder
	if err := t.Execute(&b, data); err != nil {
		return "", fmt.Errorf("template: %w", err)
	}
	return strings.ReplaceAll(b.String(), "<no value>", ""), nil
}
