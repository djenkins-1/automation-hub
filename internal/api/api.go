// Package api is the hub's HTTP interface.
//
// It currently has one endpoint:
//
//	POST /v1/events
//	Authorization: Bearer <token>
//	Content-Type: application/json
//
//	{"source": "monitor", "type": "disk.full", "subject": "/var",
//	 "data": {"percent": 93}, "idempotency_key": "optional"}
//
// The body is an sdk.Event. The hub assigns the ID (any "id" sent is ignored)
// and defaults "time" to the moment of receipt.
//
// Responses are JSON:
//
//	202 {"id": "...", "duplicate": false}   accepted; will be routed at least once
//	200 {"id": "...", "duplicate": true}    idempotency key seen before; nothing new stored
//	400 {"error": "..."}                    malformed JSON or an invalid event
//	401 {"error": "..."}                    missing or wrong token
//	413 {"error": "..."}                    body larger than 128 KiB
//	415 {"error": "..."}                    Content-Type is not application/json
//	500 {"error": "internal error"}         the event was not stored; safe to retry
package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strings"

	"github.com/djenkins-1/automation-hub/internal/engine"
	"github.com/djenkins-1/automation-hub/pkg/sdk"
)

// maxBodyBytes bounds a request body. An event's data may be up to
// sdk.MaxEventDataBytes (64 KiB); the rest is envelope and headroom.
const maxBodyBytes = 128 << 10

// Ingester accepts events. *engine.Ingest satisfies it.
type Ingester interface {
	Accept(ctx context.Context, ev sdk.Event) (engine.IngestResult, error)
}

type server struct {
	ingest    Ingester
	tokenHash [sha256.Size]byte
	log       *slog.Logger
}

// New returns the API handler. Every request must carry the bearer token. An
// empty token is refused rather than treated as "no auth", so a configuration
// mistake cannot leave the API open.
func New(ingest Ingester, token string, log *slog.Logger) (http.Handler, error) {
	if ingest == nil {
		return nil, errors.New("api: ingester is required")
	}
	if token == "" {
		return nil, errors.New("api: token must not be empty")
	}
	if log == nil {
		log = slog.Default()
	}
	s := &server{
		ingest: ingest,
		// Only a hash is kept, and comparisons are made between hashes, so
		// the comparison time does not depend on how much of a guess matches
		// (nor on the token's length).
		tokenHash: sha256.Sum256([]byte(token)),
		log:       log.With("component", "api"),
	}
	mux := http.NewServeMux()
	// A method in the pattern makes the mux answer other methods with 405
	// and an Allow header.
	mux.HandleFunc("POST /v1/events", s.postEvent)
	return mux, nil
}

func (s *server) postEvent(w http.ResponseWriter, r *http.Request) {
	// Authenticate before looking at anything else, so an unauthenticated
	// caller learns nothing about what the endpoint accepts.
	if !s.authorized(r) {
		s.log.Warn("rejected request: missing or invalid token", "remote", r.RemoteAddr)
		w.Header().Set("WWW-Authenticate", `Bearer realm="hub"`)
		writeError(w, http.StatusUnauthorized, "missing or invalid bearer token")
		return
	}

	if mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mt != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	ev, err := decodeEvent(r.Body)
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge,
				fmt.Sprintf("request body is larger than %d bytes", maxBodyBytes))
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	res, err := s.ingest.Accept(r.Context(), ev)
	switch {
	case errors.Is(err, sdk.ErrInvalidEvent):
		writeError(w, http.StatusBadRequest, err.Error())
		return
	case err != nil:
		// Detail stays in the log; the client only needs to know it can retry.
		s.log.Error("accept event failed", "err", err, "source", ev.Source, "type", ev.Type)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	status := http.StatusAccepted
	if res.Duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, struct {
		ID        string `json:"id"`
		Duplicate bool   `json:"duplicate"`
	}{res.EventID, res.Duplicate})
}

// authorized reports whether the request carries the configured bearer token.
func (s *server) authorized(r *http.Request) bool {
	scheme, cred, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return false
	}
	sum := sha256.Sum256([]byte(strings.TrimSpace(cred)))
	return subtle.ConstantTimeCompare(sum[:], s.tokenHash[:]) == 1
}

// decodeEvent reads exactly one JSON object into an sdk.Event, rejecting
// unknown fields so that a misspelt "idempotency_key" is an error instead of
// silently disabling deduplication. A *http.MaxBytesError is returned as is.
func decodeEvent(body io.Reader) (sdk.Event, error) {
	var tooBig *http.MaxBytesError
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()

	var ev sdk.Event
	if err := dec.Decode(&ev); err != nil {
		switch {
		case errors.Is(err, io.EOF):
			return sdk.Event{}, errors.New("request body is empty")
		case errors.As(err, &tooBig):
			return sdk.Event{}, err
		default:
			return sdk.Event{}, fmt.Errorf("invalid JSON: %w", err)
		}
	}
	// Nothing but whitespace may follow the object.
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		if errors.As(err, &tooBig) {
			return sdk.Event{}, err
		}
		return sdk.Event{}, errors.New("invalid JSON: body must contain a single JSON object")
	}
	return ev, nil
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, struct {
		Error string `json:"error"`
	}{msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v) // a failed write means the client went away
}
