// Package rest holds the HTTP conventions shared by the telemetry handlers: problem codes, path and query
// parameters, and the mapping of unexpected errors.
package rest

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/veritrace-platform/telemetry-stream-service/internal/gs1"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/httpx"
)

// Problem codes of the telemetry service (rest-api.md §1.1). The platform codes are in httpx.
const (
	CodeTokenExpired         = "TOKEN_EXPIRED"
	CodeInvalidGS1Identifier = "INVALID_GS1_IDENTIFIER"
)

// InternalError answers 500 for an unexpected error and logs it. The response carries only the trace ID, which
// leads to the log record.
func InternalError(w http.ResponseWriter, r *http.Request, logger *slog.Logger, err error) {
	if errors.Is(err, context.Canceled) && r.Context().Err() != nil {
		// The client went away; nobody reads this response.
		httpx.WriteProblem(w, r, httpx.NewProblem(http.StatusServiceUnavailable, httpx.CodeServiceUnavailable,
			"request canceled"))
		return
	}
	logger.ErrorContext(r.Context(), "request failed", slog.String("path", r.URL.Path), slog.Any("error", err))
	httpx.WriteProblem(w, r, httpx.NewProblem(http.StatusInternalServerError, httpx.CodeInternalError,
		"internal error"))
}

// NotFound answers 404 for a resource that does not exist or that the caller may not see.
func NotFound(w http.ResponseWriter, r *http.Request, what string) {
	httpx.WriteProblem(w, r, httpx.NewProblem(http.StatusNotFound, httpx.CodeNotFound, what+" not found"))
}

// Forbidden answers 403 for a visible resource on which the action is not permitted.
func Forbidden(w http.ResponseWriter, r *http.Request, detail string) {
	httpx.WriteProblem(w, r, httpx.NewProblem(http.StatusForbidden, httpx.CodeForbidden, detail))
}

// SSCC reads the {sscc} path parameter. A malformed SSCC answers 422 INVALID_GS1_IDENTIFIER, and ok is false.
func SSCC(w http.ResponseWriter, r *http.Request) (sscc string, ok bool) {
	sscc = chi.URLParam(r, "sscc")
	reason := gs1.CheckSSCC(sscc)
	if reason == "" {
		return sscc, true
	}
	p := httpx.NewProblem(http.StatusUnprocessableEntity, CodeInvalidGS1Identifier, "sscc is not a valid SSCC")
	p.Errors = []httpx.FieldError{{Field: "sscc", Code: reason, Message: gs1.Message(reason)}}
	httpx.WriteProblem(w, r, p)
	return "", false
}

// Query collects the errors of query parameters, so that one 400 response lists them all.
type Query struct {
	values url.Values
	Errors []httpx.FieldError
}

// NewQuery reads the query parameters of r.
func NewQuery(r *http.Request) *Query {
	return &Query{values: r.URL.Query()}
}

func (q *Query) get(name string) string {
	return strings.TrimSpace(q.values.Get(name))
}

func (q *Query) fail(field, code, message string) {
	q.Errors = append(q.Errors, httpx.FieldError{Field: field, Code: code, Message: message})
}

// Time reads an RFC 3339 timestamp, or returns def when the parameter is absent.
func (q *Query) Time(name string, def time.Time) time.Time {
	raw := q.get(name)
	if raw == "" {
		return def
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		q.fail(name, httpx.FieldInvalidFormat, "must be an RFC 3339 timestamp, such as 2026-09-01T14:30:00Z")
		return def
	}
	return t.UTC()
}

// Enum reads one of the allowed values, or returns def when the parameter is absent.
func (q *Query) Enum(name, def string, allowed ...string) string {
	raw := q.get(name)
	if raw == "" {
		return def
	}
	for _, a := range allowed {
		if raw == a {
			return raw
		}
	}
	q.fail(name, httpx.FieldInvalidValue, "must be one of "+strings.Join(allowed, ", "))
	return def
}

// Fail adds an error that a handler finds while combining parameters.
func (q *Query) Fail(field, code, message string) {
	q.fail(field, code, message)
}

// Problem returns the 400 problem of the collected errors, or nil if there are none.
func (q *Query) Problem() *httpx.Problem {
	if len(q.Errors) == 0 {
		return nil
	}
	p := httpx.ValidationProblem(q.Errors)
	return &p
}
