// Package sinks builds the notification destinations named in the config.
package sinks

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

// Config is one entry of the config file's `sinks:` list. Name and Type are
// common to all sinks; Options holds every other key, which the sink's factory
// interprets.
type Config struct {
	Name    string
	Type    string
	Options map[string]any
}

// Factory builds a sink from its options. It should reject unknown or invalid
// options (see DecodeOptions), because the config is checked once, at startup.
type Factory func(options map[string]any) (sdk.Sink, error)

// builtin lists the sink types that ship with the hub. New integrations add
// an entry here (or, later, register through the plugin SDK).
func builtin() map[string]Factory {
	return map[string]Factory{
		"http":   newHTTP,
		"stdout": newStdout,
	}
}

// Registry looks up built sinks by name. It satisfies engine.SinkSet.
type Registry struct {
	sinks map[string]sdk.Sink
}

// NewRegistry wraps ready-made sinks. Tests use it to inject fakes.
func NewRegistry(sinks map[string]sdk.Sink) *Registry {
	m := make(map[string]sdk.Sink, len(sinks))
	for k, v := range sinks {
		m[k] = v
	}
	return &Registry{sinks: m}
}

// Build constructs every configured sink. All problems are reported together.
func Build(cfgs []Config) (*Registry, error) {
	factories := builtin()
	reg := &Registry{sinks: make(map[string]sdk.Sink, len(cfgs))}
	var errs []error

	for i, c := range cfgs {
		label := fmt.Sprintf("sink #%d", i+1)
		if strings.TrimSpace(c.Name) != "" { // a blank name is labelled by position
			label = fmt.Sprintf("sink %q", c.Name)
		}
		switch {
		case strings.TrimSpace(c.Name) == "":
			errs = append(errs, fmt.Errorf("%s: name is required", label))
			continue
		case reg.sinks[c.Name] != nil:
			errs = append(errs, fmt.Errorf("%s: duplicate sink name", label))
			continue
		}
		f, ok := factories[c.Type]
		if !ok {
			errs = append(errs, fmt.Errorf("%s: unknown type %q (available: %s)", label, c.Type, strings.Join(types(factories), ", ")))
			continue
		}
		s, err := f(c.Options)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", label, err))
			continue
		}
		reg.sinks[c.Name] = s
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return reg, nil
}

// Get returns the sink with the given name.
func (r *Registry) Get(name string) (sdk.Sink, bool) {
	s, ok := r.sinks[name]
	return s, ok
}

// Names returns the configured sink names, sorted.
func (r *Registry) Names() []string {
	names := make([]string, 0, len(r.sinks))
	for n := range r.sinks {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func types(f map[string]Factory) []string {
	out := make([]string, 0, len(f))
	for t := range f {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// DecodeOptions copies options into a typed struct (using its `json` tags) and
// fails on unknown keys, so a typo in the config is an error at startup, not a
// silently ignored setting.
func DecodeOptions(options map[string]any, into any) error {
	raw, err := json.Marshal(options)
	if err != nil {
		return fmt.Errorf("options: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		return fmt.Errorf("options: %w", err)
	}
	return nil
}
