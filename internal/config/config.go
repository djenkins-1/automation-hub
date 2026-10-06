// Package config loads and validates the hub's YAML configuration.
//
// Loading is strict: unknown keys are errors, so a typo like `leas: 30s` is
// reported at startup instead of silently falling back to a default. Every
// problem found is reported together.
package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/goccy/go-yaml"

	"github.com/djenkins-1/automation-hub/internal/rules"
	"github.com/djenkins-1/automation-hub/internal/sinks"
)

// Config is the validated configuration, with defaults applied.
type Config struct {
	Storage StorageConfig
	Queue   QueueConfig
	Log     LogConfig
	Sinks   []sinks.Config
	Rules   []rules.Rule
}

// StorageConfig locates the SQLite database file.
type StorageConfig struct {
	Path string `yaml:"path"`
}

// QueueConfig tunes the job queue and its workers.
type QueueConfig struct {
	Workers     int           `yaml:"workers"`      // jobs in flight at once
	Lease       time.Duration `yaml:"lease"`        // how long a worker holds a job before it counts as crashed
	PollEvery   time.Duration `yaml:"poll_every"`   // how often an idle worker looks for jobs
	MaxAttempts int           `yaml:"max_attempts"` // tries before a job is dead-lettered
	RetryBase   time.Duration `yaml:"retry_base"`   // first retry delay; doubles each attempt
	RetryMax    time.Duration `yaml:"retry_max"`    // retry delay ceiling
}

// LogConfig selects log level and format.
type LogConfig struct {
	Level  string `yaml:"level"`  // debug | info | warn | error
	Format string `yaml:"format"` // text | json
}

// file mirrors the YAML document. Sinks are decoded as loose maps because
// everything except name and type belongs to the sink type.
type file struct {
	Storage StorageConfig    `yaml:"storage"`
	Queue   QueueConfig      `yaml:"queue"`
	Log     LogConfig        `yaml:"log"`
	Sinks   []map[string]any `yaml:"sinks"`
	Rules   []rules.Rule     `yaml:"rules"`
}

// Load reads and parses the config file at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	cfg, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	return cfg, nil
}

// Parse parses, defaults and validates a YAML document.
//
// `${NAME}` is expanded from the environment in storage.path and in every
// string under sinks (for secrets such as tokens and webhook URLs). It is
// deliberately not expanded elsewhere, so `$` in rule templates and comments
// is left alone. An unset variable is an error; write `$${NAME}` for a literal.
func Parse(data []byte) (*Config, error) {
	var f file
	if err := yaml.UnmarshalWithOptions(data, &f, yaml.Strict()); err != nil {
		return nil, err
	}

	var errs []error
	cfg := &Config{Storage: f.Storage, Queue: f.Queue, Log: f.Log, Rules: f.Rules}
	cfg.applyDefaults()

	missing := map[string]bool{}
	cfg.Storage.Path = expandString(cfg.Storage.Path, missing)
	for i, raw := range f.Sinks {
		sc, err := sinkConfig(raw, missing)
		if err != nil {
			errs = append(errs, fmt.Errorf("sinks[%d]: %w", i, err))
			continue
		}
		cfg.Sinks = append(cfg.Sinks, sc)
	}
	if len(missing) > 0 {
		names := make([]string, 0, len(missing))
		for n := range missing {
			names = append(names, n)
		}
		sort.Strings(names)
		errs = append(errs, fmt.Errorf("unset environment variable(s) referenced: %s", strings.Join(names, ", ")))
	}

	errs = append(errs, cfg.validate()...)
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return cfg, nil
}

func (c *Config) applyDefaults() {
	if c.Storage.Path == "" {
		c.Storage.Path = "hub.db"
	}
	q := &c.Queue
	if q.Workers == 0 {
		q.Workers = 4
	}
	if q.Lease == 0 {
		q.Lease = 30 * time.Second
	}
	if q.PollEvery == 0 {
		q.PollEvery = 500 * time.Millisecond
	}
	if q.MaxAttempts == 0 {
		q.MaxAttempts = 6
	}
	if q.RetryBase == 0 {
		q.RetryBase = 5 * time.Second
	}
	if q.RetryMax == 0 {
		q.RetryMax = 15 * time.Minute
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.Format == "" {
		c.Log.Format = "text"
	}
}

func (c *Config) validate() []error {
	var errs []error
	bad := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	q := c.Queue
	if q.Workers < 1 {
		bad("queue.workers must be at least 1")
	}
	for name, d := range map[string]time.Duration{
		"queue.lease": q.Lease, "queue.poll_every": q.PollEvery,
		"queue.retry_base": q.RetryBase, "queue.retry_max": q.RetryMax,
	} {
		if d <= 0 {
			bad("%s must be positive", name)
		}
	}
	if q.MaxAttempts < 1 {
		bad("queue.max_attempts must be at least 1")
	}
	if q.RetryMax < q.RetryBase {
		bad("queue.retry_max must not be smaller than queue.retry_base")
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		bad("log.level must be debug, info, warn or error, not %q", c.Log.Level)
	}
	switch c.Log.Format {
	case "text", "json":
	default:
		bad("log.format must be text or json, not %q", c.Log.Format)
	}

	// Rules: structure first, then that every sink they name exists.
	set, err := rules.NewSet(c.Rules)
	if err != nil {
		errs = append(errs, err)
		return errs
	}
	known := map[string]bool{}
	for _, s := range c.Sinks {
		known[s.Name] = true
	}
	for _, name := range set.SinkNames() {
		if !known[name] {
			bad("rules refer to sink %q, which is not defined under sinks", name)
		}
	}
	return errs
}

// sinkConfig splits one `sinks:` entry into name, type and options.
func sinkConfig(raw map[string]any, missing map[string]bool) (sinks.Config, error) {
	str := func(key string) (string, error) {
		v, present := raw[key]
		if !present {
			return "", fmt.Errorf("%s is required", key)
		}
		s, ok := v.(string)
		if !ok || strings.TrimSpace(s) == "" {
			return "", fmt.Errorf("%s must be a non-empty string", key)
		}
		return s, nil
	}
	name, err := str("name")
	if err != nil {
		return sinks.Config{}, err
	}
	typ, err := str("type")
	if err != nil {
		return sinks.Config{}, err
	}
	opts := make(map[string]any, len(raw))
	for k, v := range raw {
		if k != "name" && k != "type" {
			opts[k] = expandValue(v, missing)
		}
	}
	return sinks.Config{Name: name, Type: typ, Options: opts}, nil
}

// ${NAME} with an optional extra leading $ as an escape.
var envRef = regexp.MustCompile(`(\$?)\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

func expandString(s string, missing map[string]bool) string {
	return envRef.ReplaceAllStringFunc(s, func(m string) string {
		sub := envRef.FindStringSubmatch(m)
		if sub[1] == "$" { // $${NAME} -> literal ${NAME}
			return m[1:]
		}
		v, ok := os.LookupEnv(sub[2])
		if !ok {
			missing[sub[2]] = true
		}
		return v
	})
}

func expandValue(v any, missing map[string]bool) any {
	switch t := v.(type) {
	case string:
		return expandString(t, missing)
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, vv := range t {
			out[k] = expandValue(vv, missing)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, vv := range t {
			out[i] = expandValue(vv, missing)
		}
		return out
	default:
		return v
	}
}
