package sdk

import (
	"errors"
	"time"
)

// Plugins (and the delivery engine) classify failures by wrapping errors with
// the helpers below. The queue worker inspects the classification to decide
// whether a job is retried, retried at a specific time, or dead-lettered.
//
//	return sdk.Permanent(fmt.Errorf("webhook returned 404"))
//	return sdk.RetryAfter(err, 30*time.Second) // e.g. from a Retry-After header
//	return err                                 // anything else: retried with backoff

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as not worth retrying. Permanent(nil) returns nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err, or anything it wraps, was marked Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

type retryAfterError struct {
	err   error
	after time.Duration
}

func (e *retryAfterError) Error() string { return e.err.Error() }
func (e *retryAfterError) Unwrap() error { return e.err }

// RetryAfter asks for the next attempt no sooner than d from now, overriding
// the default backoff. RetryAfter(nil, d) returns nil.
func RetryAfter(err error, d time.Duration) error {
	if err == nil {
		return nil
	}
	return &retryAfterError{err: err, after: d}
}

// RetryDelay returns the delay requested via RetryAfter, if any.
func RetryDelay(err error) (time.Duration, bool) {
	if r, ok := errors.AsType[*retryAfterError](err); ok {
		return r.after, true
	}
	return 0, false
}
