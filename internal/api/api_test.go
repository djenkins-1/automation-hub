package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djenkins-1/automation-hub/internal/api"
	"github.com/djenkins-1/automation-hub/internal/engine"
	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

const token = "s3cret-token-0123456789abcdef"

// fakeIngester records what reaches it and answers with result/err.
type fakeIngester struct {
	mu     sync.Mutex
	got    []sdk.Event
	result engine.IngestResult
	err    error
}

func (f *fakeIngester) Accept(_ context.Context, ev sdk.Event) (engine.IngestResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, ev)
	return f.result, f.err
}

func (f *fakeIngester) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.got)
}

type harness struct {
	ing *fakeIngester
	h   http.Handler
	log *bytes.Buffer
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	ing := &fakeIngester{result: engine.IngestResult{EventID: "evt-1"}}
	var logs bytes.Buffer
	h, err := api.New(ing, token, slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if err != nil {
		t.Fatal(err)
	}
	return &harness{ing: ing, h: h, log: &logs}
}

type reqOpt func(*http.Request)

func withAuth(v string) reqOpt { return func(r *http.Request) { r.Header.Set("Authorization", v) } }
func withType(v string) reqOpt { return func(r *http.Request) { r.Header.Set("Content-Type", v) } }
func noType() reqOpt           { return func(r *http.Request) { r.Header.Del("Content-Type") } }

// post sends body to POST /v1/events with a valid token and JSON content type
// unless opts say otherwise.
func (h *harness) post(body string, opts ...reqOpt) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "application/json")
	for _, o := range opts {
		o(r)
	}
	w := httptest.NewRecorder()
	h.h.ServeHTTP(w, r)
	return w
}

func errorOf(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body.Error == "" {
		t.Fatalf("want a JSON error body, got %q (%v)", w.Body.String(), err)
	}
	return body.Error
}

const valid = `{"source":"monitor","type":"disk.full"}`

func TestNewFailsClosed(t *testing.T) {
	if _, err := api.New(&fakeIngester{}, "", nil); err == nil {
		t.Fatal("an empty token must be refused, not treated as no authentication")
	}
	if _, err := api.New(nil, token, nil); err == nil {
		t.Fatal("a nil ingester must be refused")
	}
}

func TestOnlyTheExactBearerTokenIsAccepted(t *testing.T) {
	for name, auth := range map[string]string{
		"missing":            "",
		"no scheme":          token,
		"basic scheme":       "Basic " + token,
		"wrong token":        "Bearer nope",
		"empty token":        "Bearer ",
		"prefix of token":    "Bearer " + token[:len(token)-1],
		"token plus a char":  "Bearer " + token + "x",
		"different case":     "Bearer " + strings.ToUpper(token),
		"scheme without gap": "Bearer" + token,
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			// A bad Content-Type too: authentication must come first, so an
			// unauthenticated caller learns nothing about the endpoint.
			w := h.post(valid, withAuth(auth), withType("text/plain"))
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", w.Code)
			}
			if !strings.HasPrefix(w.Header().Get("WWW-Authenticate"), "Bearer") {
				t.Errorf("WWW-Authenticate = %q", w.Header().Get("WWW-Authenticate"))
			}
			errorOf(t, w)
			if h.ing.calls() != 0 {
				t.Fatal("an unauthenticated request reached the ingester")
			}
		})
	}
	for name, auth := range map[string]string{
		"canonical":        "Bearer " + token,
		"lowercase scheme": "bearer " + token,
		"padded":           "Bearer   " + token + "  ",
	} {
		if w := newHarness(t).post(valid, withAuth(auth)); w.Code != http.StatusAccepted {
			t.Errorf("%s: status = %d, want 202", name, w.Code)
		}
	}
}

func TestRejectedCredentialsAreNeverLogged(t *testing.T) {
	h := newHarness(t)
	guess := "Bearer guessed-credential-xyz"
	h.post(valid, withAuth(guess))
	if h.log.Len() == 0 {
		t.Fatal("a rejected request should be logged")
	}
	if strings.Contains(h.log.String(), "guessed-credential-xyz") || strings.Contains(h.log.String(), token) {
		t.Fatalf("the log contains a credential:\n%s", h.log.String())
	}
}

func TestContentTypeMustBeJSON(t *testing.T) {
	for name, opt := range map[string]reqOpt{
		"missing":    noType(),
		"plain text": withType("text/plain"),
		"form":       withType("application/x-www-form-urlencoded"),
		"json-ish":   withType("application/jsonp"),
		"malformed":  withType("application/json; charset"),
	} {
		h := newHarness(t)
		if w := h.post(valid, opt); w.Code != http.StatusUnsupportedMediaType || h.ing.calls() != 0 {
			t.Errorf("%s: status = %d, ingester calls = %d; want 415 and none", name, w.Code, h.ing.calls())
		}
	}
	for _, ct := range []string{"application/json; charset=utf-8", "Application/JSON"} {
		if w := newHarness(t).post(valid, withType(ct)); w.Code != http.StatusAccepted {
			t.Errorf("%q: status = %d, want 202", ct, w.Code)
		}
	}
}

func TestMalformedBodiesAreRejectedBeforeIngest(t *testing.T) {
	for name, body := range map[string]string{
		"empty":                    "",
		"whitespace only":          "  \n",
		"not JSON":                 "source=monitor",
		"truncated":                `{"source":"monitor"`,
		"array":                    `[` + valid + `]`,
		"two objects":              valid + valid,
		"trailing garbage":         valid + "x",
		"misspelt idempotency key": `{"source":"monitor","type":"disk.full","idempotencykey":"k"}`,
		"unknown field":            `{"source":"monitor","type":"disk.full","severity":"high"}`,
		"data not an object":       `{"source":"monitor","type":"disk.full","data":[1,2]}`,
		"time not a time":          `{"source":"monitor","type":"disk.full","time":"yesterday"}`,
	} {
		h := newHarness(t)
		w := h.post(body)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", name, w.Code)
			continue
		}
		errorOf(t, w)
		if h.ing.calls() != 0 {
			t.Errorf("%s: a malformed body reached the ingester", name)
		}
	}
	if w := newHarness(t).post(valid + "\n\t "); w.Code != http.StatusAccepted {
		t.Errorf("trailing whitespace is allowed, got %d", w.Code)
	}
}

func TestBodySizeLimit(t *testing.T) {
	const limit = 128 << 10
	// The envelope plus padding inside data, so the JSON stays valid and only
	// the size decides.
	body := func(size int) string {
		prefix, suffix := `{"source":"monitor","type":"disk.full","data":{"pad":"`, `"}}`
		return prefix + strings.Repeat("a", size-len(prefix)-len(suffix)) + suffix
	}
	h := newHarness(t)
	if w := h.post(body(limit + 1)); w.Code != http.StatusRequestEntityTooLarge || h.ing.calls() != 0 {
		t.Fatalf("one byte over: status = %d, ingester calls = %d; want 413 and none", w.Code, h.ing.calls())
	}
	if w := h.post(body(limit)); w.Code != http.StatusAccepted {
		t.Fatalf("exactly at the limit: status = %d, want 202 (the ingester judges the data size)", w.Code)
	}
}

func TestTheDecodedEventReachesTheIngester(t *testing.T) {
	h := newHarness(t)
	w := h.post(`{"source":"backup","type":"job.done","subject":"nas","time":"2026-01-02T03:04:05Z",
		"data":{"n":3,"tags":["a"]},"idempotency_key":"run-7"}`)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d: %s", w.Code, w.Body)
	}
	got := h.ing.got[0]
	want := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if got.Source != "backup" || got.Type != "job.done" || got.Subject != "nas" || !got.Time.Equal(want) ||
		got.IdempotencyKey != "run-7" || got.Data["n"] != 3.0 || fmt.Sprint(got.Data["tags"]) != "[a]" {
		t.Fatalf("ingester got %+v", got)
	}
}

func TestResponsesReflectWhatTheIngesterDid(t *testing.T) {
	type resp struct {
		ID        string `json:"id"`
		Duplicate bool   `json:"duplicate"`
	}
	for _, tc := range []struct {
		dup  bool
		code int
	}{{false, http.StatusAccepted}, {true, http.StatusOK}} {
		h := newHarness(t)
		h.ing.result = engine.IngestResult{EventID: "evt-9", Duplicate: tc.dup}
		w := h.post(valid)
		var r resp
		if err := json.Unmarshal(w.Body.Bytes(), &r); err != nil || w.Code != tc.code || r != (resp{"evt-9", tc.dup}) ||
			w.Header().Get("Content-Type") != "application/json" {
			t.Errorf("duplicate=%v: status %d, body %q; want %d with id evt-9", tc.dup, w.Code, w.Body, tc.code)
		}
	}
}

func TestIngestFailuresAreSeparatedIntoClientAndServerErrors(t *testing.T) {
	h := newHarness(t)
	h.ing.err = fmt.Errorf("%w: source is required", sdk.ErrInvalidEvent)
	w := h.post(valid)
	if w.Code != http.StatusBadRequest || !strings.Contains(errorOf(t, w), "source is required") {
		t.Fatalf("invalid event: status %d, body %q; want 400 that says why", w.Code, w.Body)
	}

	h = newHarness(t)
	h.ing.err = errors.New("open /srv/hub/private.db: disk I/O error")
	w = h.post(valid)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("internal failure: status = %d, want 500", w.Code)
	}
	if msg := errorOf(t, w); strings.Contains(msg, "private.db") || strings.Contains(msg, "disk I/O") {
		t.Fatalf("internal details leaked to the client: %q", msg)
	}
	if !strings.Contains(h.log.String(), "disk I/O error") {
		t.Fatal("the internal error must be logged for the operator")
	}
}

func TestOnlyPostToTheEventsPathIsServed(t *testing.T) {
	h := newHarness(t)
	for _, m := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		r := httptest.NewRequest(m, "/v1/events", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		h.h.ServeHTTP(w, r)
		if w.Code != http.StatusMethodNotAllowed || !strings.Contains(w.Header().Get("Allow"), http.MethodPost) {
			t.Errorf("%s: status = %d, Allow = %q; want 405 naming POST", m, w.Code, w.Header().Get("Allow"))
		}
	}
	for _, path := range []string{"/", "/v1/events/", "/v1/event", "/healthz"} {
		r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(valid))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.h.ServeHTTP(w, r)
		if w.Code != http.StatusNotFound {
			t.Errorf("POST %s: status = %d, want 404", path, w.Code)
		}
	}
	if h.ing.calls() != 0 {
		t.Fatal("a request to another route reached the ingester")
	}
}
