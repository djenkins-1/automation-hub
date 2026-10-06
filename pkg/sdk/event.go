package sdk

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// Limits enforced by Event.Validate.
const (
	MaxEventDataBytes = 64 << 10 // size of the JSON-encoded Data
	maxNameLen        = 128
	maxSubjectLen     = 512
	maxKeyLen         = 256
)

// ErrInvalidEvent is wrapped by every Validate failure, so callers (the HTTP
// API in particular) can tell bad input from internal errors with errors.Is.
var ErrInvalidEvent = errors.New("invalid event")

// Source and type names are matched by rule globs, so keep them boring:
// "monitor", "disk.full", "backup:finished".
var namePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/+@-]*$`)

// Event is the envelope every source produces and every rule matches against.
type Event struct {
	// ID is assigned by the hub on ingest; any value supplied is ignored.
	ID string `json:"id"`
	// Source says who produced the event, e.g. "monitor" or "backup-job".
	Source string `json:"source"`
	// Type says what happened, e.g. "disk.full". Rules usually glob on it.
	Type string `json:"type"`
	// Subject is what the event is about, e.g. a hostname or mount point.
	Subject string `json:"subject,omitempty"`
	// Time is when the event happened. Defaults to the time of receipt.
	Time time.Time `json:"time"`
	// Data is an arbitrary JSON object for rule conditions and templates.
	Data map[string]any `json:"data,omitempty"`
	// IdempotencyKey is optional. A second event from the same Source with
	// the same key is treated as a duplicate and dropped, which makes it safe
	// for producers (cron jobs, webhooks) to retry.
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

// Validate checks the fields a producer is responsible for (not ID or Time,
// which the hub fills in). All problems are reported at once.
func (e Event) Validate() error {
	var problems []string

	name := func(field, v string) {
		switch {
		case v == "":
			problems = append(problems, field+" is required")
		case len(v) > maxNameLen:
			problems = append(problems, fmt.Sprintf("%s is longer than %d characters", field, maxNameLen))
		case !namePattern.MatchString(v):
			problems = append(problems, field+" must start with a letter or digit and contain only letters, digits and . _ : / + @ -")
		}
	}
	name("source", e.Source)
	name("type", e.Type)

	if len(e.Subject) > maxSubjectLen {
		problems = append(problems, fmt.Sprintf("subject is longer than %d bytes", maxSubjectLen))
	}
	if !utf8.ValidString(e.Subject) {
		problems = append(problems, "subject is not valid UTF-8")
	}
	if len(e.IdempotencyKey) > maxKeyLen {
		problems = append(problems, fmt.Sprintf("idempotency_key is longer than %d bytes", maxKeyLen))
	}
	if strings.IndexFunc(e.IdempotencyKey, unicode.IsControl) >= 0 {
		problems = append(problems, "idempotency_key must not contain control characters")
	}
	if e.Data != nil {
		if b, err := json.Marshal(e.Data); err != nil {
			problems = append(problems, fmt.Sprintf("data is not valid JSON: %v", err))
		} else if len(b) > MaxEventDataBytes {
			problems = append(problems, fmt.Sprintf("data is larger than %d bytes", MaxEventDataBytes))
		}
	}

	if len(problems) > 0 {
		return fmt.Errorf("%w: %s", ErrInvalidEvent, strings.Join(problems, "; "))
	}
	return nil
}
