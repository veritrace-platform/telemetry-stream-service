package httpx

import (
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"

	"github.com/google/uuid"
)

// Collection page sizes (rest-api.md §1).
const (
	DefaultPageLimit = 20
	MaxPageLimit     = 100
)

// Page is a validated request for one page of a collection.
type Page struct {
	Limit  int
	Cursor string // opaque; empty for the first page
}

// ParsePage reads the limit and cursor query parameters.
func ParsePage(r *http.Request) (Page, []FieldError) {
	q := r.URL.Query()
	page := Page{Limit: DefaultPageLimit, Cursor: q.Get("cursor")}
	var errs []FieldError
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		switch {
		case err != nil:
			errs = append(errs, FieldError{Field: "limit", Code: FieldInvalidFormat, Message: "must be an integer"})
		case n < 1 || n > MaxPageLimit:
			errs = append(errs, FieldError{Field: "limit", Code: FieldOutOfRange, Message: "must be between 1 and 100"})
		default:
			page.Limit = n
		}
	}
	return page, errs
}

// Collection is the body of every collection response.
type Collection[T any] struct {
	Items      []T     `json:"items"`
	NextCursor *string `json:"next_cursor"`
}

// NewCollection builds a page from up to limit+1 fetched items. The extra item only signals that another page
// exists; cursor encodes the position after the last item returned.
func NewCollection[T any](fetched []T, limit int, cursor func(T) string) Collection[T] {
	c := Collection[T]{Items: fetched}
	if len(fetched) > limit {
		c.Items = fetched[:limit]
		next := cursor(c.Items[limit-1])
		c.NextCursor = &next
	}
	if c.Items == nil {
		c.Items = []T{}
	}
	return c
}

// ErrInvalidCursor reports a cursor that no response of this API produced.
var ErrInvalidCursor = errors.New("invalid cursor")

// UUIDCursor encodes the key of the last item of a page. UUIDv7 keys are time-ordered, so a page continues
// from the key onwards.
func UUIDCursor(id uuid.UUID) string {
	return base64.RawURLEncoding.EncodeToString(id[:])
}

// ParseUUIDCursor decodes a cursor made by UUIDCursor.
func ParseUUIDCursor(cursor string) (uuid.UUID, error) {
	b, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil || len(b) != len(uuid.UUID{}) {
		return uuid.Nil, ErrInvalidCursor
	}
	return uuid.UUID(b), nil
}

// InvalidCursorError is the field error for a cursor ParseUUIDCursor rejects.
func InvalidCursorError() FieldError {
	return FieldError{Field: "cursor", Code: FieldInvalidFormat, Message: "is not a cursor returned by this API"}
}
