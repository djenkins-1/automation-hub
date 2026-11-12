package rules

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"

	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

// exprEnv is the set of variables available to match.when. Using a struct (not
// a map) lets the expression compiler reject unknown names such as `dat.x` at
// startup.
type exprEnv struct {
	ID      string         `expr:"id"`
	Source  string         `expr:"source"`
	Type    string         `expr:"type"`
	Subject string         `expr:"subject"`
	Time    time.Time      `expr:"time"`
	Data    map[string]any `expr:"data"`
}

func compileWhen(src string) (*vm.Program, error) {
	return expr.Compile(src, expr.Env(exprEnv{}), expr.AsBool())
}

// compileGlob turns "disk.*" into an anchored regexp. An empty glob returns
// nil, meaning "match anything". The s flag lets * and ? match newlines too:
// they mean "any characters", whatever they are.
func compileGlob(glob string) (*regexp.Regexp, error) {
	if glob == "" {
		return nil, nil
	}
	var b strings.Builder
	b.WriteString("(?s)^")
	for _, r := range glob {
		switch r {
		case '*':
			b.WriteString(".*")
		case '?':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString("$")
	return regexp.Compile(b.String())
}

// matches reports whether ev satisfies the rule's match criteria. The cheap
// glob checks run first; the expression only runs for events that get past them.
func (c *compiled) matches(ev sdk.Event) (bool, error) {
	if c.source != nil && !c.source.MatchString(ev.Source) {
		return false, nil
	}
	if c.typ != nil && !c.typ.MatchString(ev.Type) {
		return false, nil
	}
	if c.when == nil {
		return true, nil
	}

	data := ev.Data
	if data == nil {
		data = map[string]any{}
	}
	out, err := expr.Run(c.when, exprEnv{
		ID: ev.ID, Source: ev.Source, Type: ev.Type, Subject: ev.Subject, Time: ev.Time, Data: data,
	})
	if err != nil {
		return false, err
	}
	matched, ok := out.(bool)
	if !ok { // cannot happen with expr.AsBool, but do not trust it silently
		return false, fmt.Errorf("expression returned %T, not a boolean", out)
	}
	return matched, nil
}
