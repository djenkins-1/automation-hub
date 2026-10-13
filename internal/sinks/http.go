package sinks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

const (
	// maxRetryAfter caps a server-supplied Retry-After. The queue worker
	// trusts the delay it is given, so without a cap a bad header could park
	// a delivery for years.
	maxRetryAfter = time.Hour

	maxErrorBody   = 512      // bytes of a failure response kept for the attempt log
	maxDrainBody   = 64 << 10 // bytes read and discarded so the connection can be reused
	maxDetailRunes = 200      // length of the response excerpt in an error message
)

// httpOptions are the settings of a `type: http` sink.
type httpOptions struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}

// httpPayload is the JSON body POSTed for every notification.
type httpPayload struct {
	ID      string    `json:"id"`
	Title   string    `json:"title"`
	Body    string    `json:"body"`
	Attempt int       `json:"attempt"`
	Event   sdk.Event `json:"event"`
}

// httpSink POSTs each notification as JSON to a fixed URL. It is a generic
// webhook: the receiver gets the hub's own envelope, not a service-specific
// format such as Slack's or ntfy's.
type httpSink struct {
	url     string
	headers map[string]string
	client  *http.Client
}

var (
	headerName = regexp.MustCompile("^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")

	// Headers the sink sets itself; letting the config override them would
	// only produce confusing failures.
	reservedHeaders = map[string]bool{
		"Content-Type":    true,
		"Content-Length":  true,
		"Idempotency-Key": true,
		"Host":            true,
	}
)

func newHTTP(options map[string]any) (sdk.Sink, error) {
	var o httpOptions
	if err := DecodeOptions(options, &o); err != nil {
		return nil, err
	}

	// Parse errors are not wrapped: *url.Error prints the URL, and webhook
	// URLs commonly embed a secret.
	if strings.TrimSpace(o.URL) == "" {
		return nil, errors.New("url is required")
	}
	u, err := url.Parse(o.URL)
	if err != nil {
		return nil, errors.New("url is not a valid URL")
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errors.New("url must be an absolute http:// or https:// URL")
	}

	headers := make(map[string]string, len(o.Headers))
	for name, value := range o.Headers {
		if !headerName.MatchString(name) {
			return nil, fmt.Errorf("headers: invalid header name %q", name)
		}
		canon := http.CanonicalHeaderKey(name)
		if reservedHeaders[canon] {
			return nil, fmt.Errorf("headers: %s is set by the hub and cannot be configured", canon)
		}
		if _, dup := headers[canon]; dup {
			return nil, fmt.Errorf("headers: %s is listed more than once", canon)
		}
		// The value is deliberately not echoed; it is often a credential. A
		// bad value found here is a startup error, whereas at send time it
		// would fail every attempt.
		if strings.IndexFunc(value, func(r rune) bool { return (r < 0x20 && r != '\t') || r == 0x7f }) >= 0 {
			return nil, fmt.Errorf("headers: value of %s contains control characters", canon)
		}
		headers[canon] = value
	}

	return &httpSink{
		url:     o.URL,
		headers: headers,
		client: &http.Client{
			Transport: http.DefaultTransport.(*http.Transport).Clone(),
			// No Timeout: the dispatcher's context bounds each call. Redirects
			// are not followed: Go would turn a redirected POST into a GET,
			// which silently drops the notification.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, nil
}

func (s *httpSink) Deliver(ctx context.Context, n sdk.Notification) (sdk.Result, error) {
	body, err := json.Marshal(httpPayload{
		ID: n.ID, Title: n.Title, Body: n.Body, Attempt: n.Attempt, Event: n.Event,
	})
	if err != nil {
		return sdk.Result{}, sdk.Permanent(fmt.Errorf("http sink: encode payload: %w", err))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil { // the URL was validated at startup, so this should not happen
		return sdk.Result{}, sdk.Permanent(errors.New("http sink: could not build request"))
	}
	req.Header.Set("User-Agent", "automation-hub")
	for k, v := range s.headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")
	if n.IdempotencyKey != "" {
		req.Header.Set("Idempotency-Key", n.IdempotencyKey)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		// *url.Error carries the full URL, which may contain a secret; keep
		// only the underlying cause. %w preserves context.Canceled and
		// context.DeadlineExceeded for the worker.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return sdk.Result{}, fmt.Errorf("http sink: request failed: %w", err)
	}
	defer resp.Body.Close()

	excerpt, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainBody))

	code := resp.StatusCode
	switch {
	case code >= 200 && code < 300:
		return sdk.Result{Detail: fmt.Sprintf("HTTP %d", code)}, nil

	case code == http.StatusTooManyRequests, code == http.StatusServiceUnavailable:
		serr := statusError(code, excerpt)
		if d, ok := parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()); ok {
			return sdk.Result{}, sdk.RetryAfter(serr, d)
		}
		return sdk.Result{}, serr

	case code == http.StatusRequestTimeout, code >= 500:
		return sdk.Result{}, statusError(code, excerpt)

	default: // redirects (never followed) and every other 4xx: retrying cannot help
		return sdk.Result{}, sdk.Permanent(statusError(code, excerpt))
	}
}

// statusError describes an unsuccessful response, including a short, cleaned
// excerpt of its body because that is usually where the receiver says why.
func statusError(code int, excerpt []byte) error {
	msg := fmt.Sprintf("http sink: server returned HTTP %d", code)
	if code >= 300 && code < 400 {
		msg += " (redirects are not followed)"
	}
	if detail := tidy(excerpt); detail != "" {
		msg += ": " + detail
	}
	return errors.New(msg)
}

// tidy turns a response excerpt into one short printable line.
func tidy(b []byte) string {
	s := strings.ToValidUTF8(string(b), "")
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) && !unicode.IsSpace(r) {
			return -1
		}
		return r
	}, s)
	s = strings.Join(strings.Fields(s), " ")
	if r := []rune(s); len(r) > maxDetailRunes {
		s = string(r[:maxDetailRunes]) + "…"
	}
	return s
}

// parseRetryAfter reads a Retry-After header, either delay-seconds or an HTTP
// date. It reports false when the header is absent, malformed or already in
// the past, in which case the caller falls back to the normal backoff.
func parseRetryAfter(v string, now time.Time) (time.Duration, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	var d time.Duration
	if secs, err := strconv.Atoi(v); err == nil {
		switch {
		case secs <= 0:
			return 0, false
		case secs > int(maxRetryAfter/time.Second): // also avoids overflow in the multiplication
			d = maxRetryAfter
		default:
			d = time.Duration(secs) * time.Second
		}
	} else if t, err := http.ParseTime(v); err == nil {
		d = t.Sub(now)
	} else {
		return 0, false
	}
	if d <= 0 {
		return 0, false
	}
	return min(d, maxRetryAfter), true
}
