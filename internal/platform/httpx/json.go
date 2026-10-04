package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"reflect"
	"strings"
)

// MaxJSONBodyBytes is the largest JSON request body the API accepts (rest-api.md §1).
const MaxJSONBodyBytes = 1 << 20

// Media types of JSON request bodies. PATCH bodies are JSON merge patches (RFC 7396).
const (
	JSONContentType       = "application/json"
	MergePatchContentType = "application/merge-patch+json"
)

// DecodeJSON reads the request body into dst, which must be a pointer. It returns a problem ready to write
// when the body is not one JSON value of the expected shape: a wrong media type (415), a body over
// MaxJSONBodyBytes (413), or an empty, malformed, or mistyped body or an unknown field (400).
func DecodeJSON(w http.ResponseWriter, r *http.Request, dst any) *Problem {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || (mediaType != JSONContentType && mediaType != MergePatchContentType) {
		p := NewProblem(http.StatusUnsupportedMediaType, CodeUnsupportedMediaType,
			"Content-Type must be application/json")
		return &p
	}

	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxJSONBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return decodeProblem(err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		p := NewProblem(http.StatusBadRequest, CodeValidationFailed, "request body must be a single JSON value")
		return &p
	}
	return nil
}

func decodeProblem(err error) *Problem {
	var (
		tooLarge  *http.MaxBytesError
		wrongType *json.UnmarshalTypeError
		p         Problem
	)
	switch {
	case errors.As(err, &tooLarge):
		p = NewProblem(http.StatusRequestEntityTooLarge, CodePayloadTooLarge,
			fmt.Sprintf("request body exceeds %d bytes", MaxJSONBodyBytes))
	case errors.Is(err, io.EOF):
		p = NewProblem(http.StatusBadRequest, CodeValidationFailed, "request body is required")
	case errors.As(err, &wrongType):
		p = ValidationProblem([]FieldError{{
			Field:   wrongType.Field,
			Code:    FieldInvalidType,
			Message: "must be " + jsonKind(wrongType.Type),
		}})
	case strings.HasPrefix(err.Error(), "json: unknown field "):
		// encoding/json reports unknown fields only through the message.
		field := strings.Trim(strings.TrimPrefix(err.Error(), "json: unknown field "), `"`)
		p = ValidationProblem([]FieldError{{Field: field, Code: FieldUnknown, Message: "is not a known field"}})
	default:
		p = NewProblem(http.StatusBadRequest, CodeValidationFailed, "request body is not valid JSON")
	}
	return &p
}

// jsonKinds names the JSON type that decodes into each Go kind; every other kind decodes from an object.
var jsonKinds = map[reflect.Kind]string{
	reflect.String: "a string", reflect.Bool: "a boolean", reflect.Slice: "an array", reflect.Array: "an array",
	reflect.Int: "a number", reflect.Int8: "a number", reflect.Int16: "a number", reflect.Int32: "a number",
	reflect.Int64: "a number", reflect.Uint: "a number", reflect.Uint8: "a number", reflect.Uint16: "a number",
	reflect.Uint32: "a number", reflect.Uint64: "a number", reflect.Float32: "a number", reflect.Float64: "a number",
}

// jsonKind names the JSON type that decodes into t.
func jsonKind(t reflect.Type) string {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if kind, ok := jsonKinds[t.Kind()]; ok {
		return kind
	}
	return "an object"
}

// WriteJSON writes v as the response body with the given status.
func WriteJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		// Response types are defined by this service, so this is a programming error such as a NaN value.
		WriteProblem(w, r, NewProblem(http.StatusInternalServerError, CodeInternalError, "internal error"))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	// The status line is already sent; a write failure can only be observed by the client.
	_, _ = w.Write(append(body, '\n'))
}
