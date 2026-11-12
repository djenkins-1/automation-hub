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
//
// The string functions take any value, so an optional field piped through
// them renders as nothing when missing instead of failing the action, and a
// number or boolean is formatted as it would be printed.
var funcs = template.FuncMap{
	"upper": func(v any) string { return strings.ToUpper(asText(v)) },
	"lower": func(v any) string { return strings.ToLower(asText(v)) },
	"trim":  func(v any) string { return strings.TrimSpace(asText(v)) },
	"truncate": func(n int, v any) string {
		s := asText(v)
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
		if empty(v) {
			return fallback
		}
		return v
	},
	"json": func(v any) (string, error) {
		b, err := json.Marshal(v)
		return string(b), err
	},
}

// asText is v as a template would print it, with nil (a missing or null field)
// as the empty string.
func asText(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	default:
		return fmt.Sprint(t)
	}
}

// empty reports whether default should replace v: nil, a zero value (0, "",
// false), or an empty list or object.
func empty(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Map, reflect.Array:
		return rv.Len() == 0
	default:
		return rv.IsZero()
	}
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
