package sinks_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djenkins-1/automation-hub/internal/sinks"
	"github.com/djenkins-1/automation-hub/internal/sinks/sinktest"
	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

// Webhook URLs and header values are secrets. These markers are planted in
// them so tests can check they never surface in an error.
const (
	secretPath  = "/hooks/PATHSECRET"
	secretQuery = "?key=QUERYSECRET"
	secretValue = "HEADERSECRET"
)

func buildHTTP(options map[string]any) (sdk.Sink, error) {
	reg, err := sinks.Build([]sinks.Config{{Name: "hook", Type: "http", Options: options}})
	if err != nil {
		return nil, err
	}
	s, _ := reg.Get("hook")
	return s, nil
}

func mustHTTP(t *testing.T, url string, headers map[string]any) sdk.Sink {
	t.Helper()
	opts := map[string]any{"url": url}
	if headers != nil {
		opts["headers"] = headers
	}
	s, err := buildHTTP(opts)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// receiver starts a server that answers every request with h and returns the
// webhook URL for it, with the secret path and query planted.
func receiver(t *testing.T, h http.HandlerFunc) string {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL + secretPath + secretQuery
}

func noSecrets(t *testing.T, err error) {
	t.Helper()
	for _, s := range []string{"PATHSECRET", "QUERYSECRET", secretValue} {
		if strings.Contains(err.Error(), s) {
			t.Fatalf("error leaks a secret (%s): %v", s, err)
		}
	}
}

var note = sdk.Notification{
	ID: "del-1", IdempotencyKey: "evt-1:rule:0", Title: "Disk /var", Body: "95% used", Attempt: 2,
	Event: sdk.Event{ID: "evt-1", Source: "monitor", Type: "disk.full", Subject: "/var", Data: map[string]any{"percent": 95.0}},
}

func TestHTTPSinkConformsToTheSinkContract(t *testing.T) {
	sinktest.Conformance(t, func(t *testing.T) sdk.Sink {
		return mustHTTP(t, receiver(t, func(w http.ResponseWriter, r *http.Request) {}), nil)
	})
}

func TestBadHTTPSinkSettingsAreStartupErrorsThatKeepSecrets(t *testing.T) {
	for name, opts := range map[string]map[string]any{
		"no url":            {},
		"blank url":         {"url": "  "},
		"relative url":      {"url": secretPath + secretQuery},
		"no host":           {"url": "https://" + secretPath},
		"other scheme":      {"url": "ftp://example.com" + secretPath + secretQuery},
		"unparseable url":   {"url": "http://exa mple.com:port" + secretPath},
		"unknown option":    {"url": "https://example.com", "timeout": "5s"},
		"bad header name":   {"url": "https://example.com", "headers": map[string]any{"X Token": secretValue}},
		"reserved header":   {"url": "https://example.com", "headers": map[string]any{"content-type": "text/plain"}},
		"idempotency key":   {"url": "https://example.com", "headers": map[string]any{"Idempotency-Key": "x"}},
		"host header":       {"url": "https://example.com", "headers": map[string]any{"Host": "evil.example"}},
		"duplicate header":  {"url": "https://example.com", "headers": map[string]any{"X-Token": "a", "x-token": "b"}},
		"newline in value":  {"url": "https://example.com", "headers": map[string]any{"Authorization": "Bearer " + secretValue + "\r\nX-Evil: 1"}},
		"non-string header": {"url": "https://example.com", "headers": map[string]any{"X-Retries": 3}},
	} {
		_, err := buildHTTP(opts)
		if err == nil {
			t.Errorf("%s: expected a startup error", name)
			continue
		}
		noSecrets(t, err)
	}
}

func TestTheRequestCarriesTheEnvelopeAndHeaders(t *testing.T) {
	var got *http.Request
	var body []byte
	url := receiver(t, func(w http.ResponseWriter, r *http.Request) {
		got, body = r, must(io.ReadAll(r.Body))
	})
	s := mustHTTP(t, url, map[string]any{"Authorization": "Bearer " + secretValue, "x-extra": "yes"})
	res, err := s.Deliver(context.Background(), note)
	if err != nil || res.Detail != "HTTP 200" {
		t.Fatalf("res=%+v err=%v", res, err)
	}

	if got.Method != http.MethodPost || got.URL.String() != secretPath+secretQuery {
		t.Errorf("request = %s %s, want POST to the configured URL", got.Method, got.URL)
	}
	for h, want := range map[string]string{
		"Content-Type":    "application/json",
		"Idempotency-Key": note.IdempotencyKey,
		"Authorization":   "Bearer " + secretValue,
		"X-Extra":         "yes",
	} {
		if got.Header.Get(h) != want {
			t.Errorf("header %s = %q, want %q", h, got.Header.Get(h), want)
		}
	}
	var env struct {
		ID, Title, Body string
		Attempt         int
		Event           sdk.Event
	}
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("body is not JSON: %v\n%s", err, body)
	}
	if env.ID != note.ID || env.Title != note.Title || env.Body != note.Body || env.Attempt != note.Attempt ||
		env.Event.ID != "evt-1" || env.Event.Subject != "/var" || env.Event.Data["percent"] != 95.0 {
		t.Errorf("envelope = %+v", env)
	}

	// Without a key there is no header, rather than an empty one.
	n := note
	n.IdempotencyKey = ""
	if _, err := s.Deliver(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	if _, present := got.Header["Idempotency-Key"]; present {
		t.Error("an empty idempotency key must not be sent")
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func TestResponsesAreClassified(t *testing.T) {
	type want int
	const (
		ok want = iota
		retry
		permanent
	)
	for _, tc := range []struct {
		code int
		want want
	}{
		{200, ok}, {201, ok}, {202, ok}, {204, ok},
		{408, retry}, {429, retry}, {500, retry}, {502, retry}, {503, retry}, {504, retry},
		{400, permanent}, {401, permanent}, {403, permanent}, {404, permanent}, {409, permanent}, {410, permanent}, {422, permanent},
		{301, permanent}, {302, permanent}, {303, permanent}, {307, permanent}, {308, permanent},
	} {
		url := receiver(t, func(w http.ResponseWriter, r *http.Request) {
			if tc.code >= 300 && tc.code < 400 {
				w.Header().Set("Location", "/elsewhere")
			}
			w.WriteHeader(tc.code)
		})
		_, err := mustHTTP(t, url, nil).Deliver(context.Background(), note)
		var got want
		switch {
		case err == nil:
			got = ok
		case sdk.IsPermanent(err):
			got = permanent
		default:
			got = retry
		}
		if got != tc.want {
			t.Errorf("HTTP %d: classified %v (err %v), want %v (0 ok, 1 retry, 2 permanent)", tc.code, got, err, tc.want)
			continue
		}
		if err != nil {
			noSecrets(t, err)
			if !strings.Contains(err.Error(), "HTTP "+strconv.Itoa(tc.code)) {
				t.Errorf("HTTP %d: the error should name the status: %v", tc.code, err)
			}
		}
	}
}

func TestRedirectsAreNeverFollowed(t *testing.T) {
	var followed atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { followed.Add(1) }))
	t.Cleanup(target.Close)
	url := receiver(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	})
	_, err := mustHTTP(t, url, nil).Deliver(context.Background(), note)
	if !sdk.IsPermanent(err) || followed.Load() != 0 {
		t.Fatalf("err=%v, requests to the redirect target=%d: a redirect must fail permanently and not be followed", err, followed.Load())
	}
}

func TestRetryAfterIsHonouredAndCapped(t *testing.T) {
	future := time.Now().Add(90 * time.Second).UTC().Format(http.TimeFormat)
	past := time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)
	for _, tc := range []struct {
		code     int
		header   string
		min, max time.Duration // want a delay in [min, max]; both zero means no delay requested
	}{
		{429, "120", 120 * time.Second, 120 * time.Second},
		{503, "7", 7 * time.Second, 7 * time.Second},
		{429, future, 85 * time.Second, 91 * time.Second},
		{429, "999999999999", time.Hour, time.Hour}, // capped, and no overflow
		{429, "86400", time.Hour, time.Hour},
		{429, "0", 0, 0},
		{429, "-5", 0, 0},
		{429, "soon", 0, 0},
		{429, past, 0, 0},
		{429, "", 0, 0},
		{500, "30", 0, 0}, // only 429 and 503 carry a meaningful Retry-After
	} {
		url := receiver(t, func(w http.ResponseWriter, r *http.Request) {
			if tc.header != "" {
				w.Header().Set("Retry-After", tc.header)
			}
			w.WriteHeader(tc.code)
		})
		_, err := mustHTTP(t, url, nil).Deliver(context.Background(), note)
		if err == nil || sdk.IsPermanent(err) {
			t.Errorf("%d Retry-After %q: err = %v, want a retryable error", tc.code, tc.header, err)
			continue
		}
		d, has := sdk.RetryDelay(err)
		switch {
		case tc.max == 0 && has:
			t.Errorf("%d Retry-After %q: delay %v requested, want the normal backoff", tc.code, tc.header, d)
		case tc.max != 0 && (!has || d < tc.min || d > tc.max):
			t.Errorf("%d Retry-After %q: delay = %v (requested %v), want %v..%v", tc.code, tc.header, d, has, tc.min, tc.max)
		}
	}
}

func TestErrorsQuoteAShortCleanExcerptOfTheResponse(t *testing.T) {
	url := receiver(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, "field \x1b[31m'title'\x00 is\n\n required"+strings.Repeat(" padding", 500))
	})
	_, err := mustHTTP(t, url, nil).Deliver(context.Background(), note)
	if err == nil {
		t.Fatal("expected an error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "'title'") || !strings.Contains(msg, "is required") {
		t.Errorf("the receiver's reason should be quoted: %q", msg)
	}
	if strings.ContainsAny(msg, "\x1b\x00\n") {
		t.Errorf("control characters survived: %q", msg)
	}
	if len([]rune(msg)) > 300 {
		t.Errorf("the excerpt is not bounded: %d runes", len([]rune(msg)))
	}
}

func TestNetworkFailuresAreRetryableAndKeepTheURLSecret(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL + secretPath + secretQuery
	srv.Close() // nothing listens there now
	_, err := mustHTTP(t, url, nil).Deliver(context.Background(), note)
	if err == nil || sdk.IsPermanent(err) {
		t.Fatalf("err = %v, want a retryable error", err)
	}
	noSecrets(t, err)
}

func TestTheCallerDeadlineBoundsTheRequest(t *testing.T) {
	release := make(chan struct{})
	url := receiver(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body) // the server notices a client hang-up only once the body is read
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	t.Cleanup(func() { close(release) }) // registered after the server, so it runs before the server's Close
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	began := time.Now()
	_, err := mustHTTP(t, url, nil).Deliver(ctx, note)
	if !errors.Is(err, context.DeadlineExceeded) || sdk.IsPermanent(err) {
		t.Fatalf("err = %v, want a retryable context.DeadlineExceeded the worker can recognise", err)
	}
	if time.Since(began) > 5*time.Second {
		t.Fatal("the request outlived its deadline")
	}
	noSecrets(t, err)
}
