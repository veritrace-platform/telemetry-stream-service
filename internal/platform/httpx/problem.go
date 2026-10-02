// Package httpx holds HTTP plumbing shared by every handler: RFC 9457 problem responses and middleware.
package httpx

import (
	"encoding/json"
	"net/http"

	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/tracecontext"
)

// Platform-level problem codes. Domain packages define their own codes; the catalog is in
// veritrace/docs/contracts/rest-api.md.
const (
	CodeValidationFailed     = "VALIDATION_FAILED"
	CodeUnauthenticated      = "UNAUTHENTICATED"
	CodeForbidden            = "FORBIDDEN"
	CodeNotFound             = "NOT_FOUND"
	CodeMethodNotAllowed     = "METHOD_NOT_ALLOWED"
	CodePayloadTooLarge      = "PAYLOAD_TOO_LARGE"
	CodeUnsupportedMediaType = "UNSUPPORTED_MEDIA_TYPE"
	CodeRateLimited          = "RATE_LIMITED"
	CodeInternalError        = "INTERNAL_ERROR"
	CodeServiceUnavailable   = "SERVICE_UNAVAILABLE"
)

// Field error codes shared by every endpoint. Domain packages add their own, such as the GS1 codes.
const (
	FieldRequired      = "REQUIRED"
	FieldInvalidFormat = "INVALID_FORMAT"
	FieldInvalidType   = "INVALID_TYPE"
	FieldInvalidValue  = "INVALID_VALUE"
	FieldTooShort      = "TOO_SHORT"
	FieldTooLong       = "TOO_LONG"
	FieldOutOfRange    = "OUT_OF_RANGE"
	FieldUnknown       = "UNKNOWN_FIELD"
)

// ProblemContentType is the media type of every error response.
const ProblemContentType = "application/problem+json"

// Problem is an RFC 9457 problem document extended with a stable code, trace ID, and field errors.
type Problem struct {
	Type     string       `json:"type"`
	Title    string       `json:"title"`
	Status   int          `json:"status"`
	Code     string       `json:"code"`
	Detail   string       `json:"detail,omitempty"`
	Instance string       `json:"instance,omitempty"`
	TraceID  string       `json:"trace_id,omitempty"`
	Errors   []FieldError `json:"errors,omitempty"`
	// Extensions are the extension members that the problem's code defines (RFC 9457 §3.2), such as how far a
	// position lies outside a geo-fence. They are written next to the members above and must not reuse
	// their names.
	Extensions map[string]any `json:"-"`
}

// MarshalJSON writes the extension members at the top level of the problem document.
func (p Problem) MarshalJSON() ([]byte, error) {
	type members Problem // the same fields without this method
	doc, err := json.Marshal(members(p))
	if err != nil || len(p.Extensions) == 0 {
		return doc, err
	}
	extensions, err := json.Marshal(p.Extensions)
	if err != nil {
		return nil, err
	}
	// Both are JSON objects; join them into one.
	return append(append(doc[:len(doc)-1], ','), extensions[1:]...), nil
}

// FieldError describes one invalid request field.
type FieldError struct {
	Field   string `json:"field"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// NewProblem builds a problem with the standard title for status.
func NewProblem(status int, code, detail string) Problem {
	return Problem{
		Type:   "about:blank",
		Title:  http.StatusText(status),
		Status: status,
		Code:   code,
		Detail: detail,
	}
}

// ValidationProblem builds the 400 VALIDATION_FAILED problem that lists every invalid field.
func ValidationProblem(errs []FieldError) Problem {
	p := NewProblem(http.StatusBadRequest, CodeValidationFailed, "request validation failed")
	p.Errors = errs
	return p
}

// WriteProblem writes p as the response, filling in the request path and trace ID.
func WriteProblem(w http.ResponseWriter, r *http.Request, p Problem) {
	if p.Instance == "" {
		p.Instance = r.URL.Path
	}
	if tc, ok := tracecontext.FromContext(r.Context()); ok {
		p.TraceID = tc.TraceID
	}
	w.Header().Set("Content-Type", ProblemContentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(p.Status)
	// The status line is already sent; an encoding failure can only be observed by the client.
	_ = json.NewEncoder(w).Encode(p)
}

// NotFound responds with 404 NOT_FOUND for unknown routes.
func NotFound(w http.ResponseWriter, r *http.Request) {
	WriteProblem(w, r, NewProblem(http.StatusNotFound, CodeNotFound, "resource not found"))
}

// MethodNotAllowed responds with 405 METHOD_NOT_ALLOWED for known routes with an unsupported method.
func MethodNotAllowed(w http.ResponseWriter, r *http.Request) {
	WriteProblem(w, r, NewProblem(http.StatusMethodNotAllowed, CodeMethodNotAllowed, "method not allowed"))
}
