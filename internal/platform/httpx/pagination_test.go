package httpx_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/httpx"
)

func TestParsePage(t *testing.T) {
	tests := []struct {
		query      string
		wantLimit  int
		wantCursor string
		wantCode   string
	}{
		{query: "", wantLimit: httpx.DefaultPageLimit},
		{query: "limit=1&cursor=abc", wantLimit: 1, wantCursor: "abc"},
		{query: "limit=100", wantLimit: 100},
		{query: "limit=0", wantLimit: httpx.DefaultPageLimit, wantCode: httpx.FieldOutOfRange},
		{query: "limit=101", wantLimit: httpx.DefaultPageLimit, wantCode: httpx.FieldOutOfRange},
		{query: "limit=ten", wantLimit: httpx.DefaultPageLimit, wantCode: httpx.FieldInvalidFormat},
	}
	for _, tt := range tests {
		page, errs := httpx.ParsePage(httptest.NewRequest(http.MethodGet, "/items?"+tt.query, nil))
		if page.Limit != tt.wantLimit || page.Cursor != tt.wantCursor {
			t.Errorf("%q: page = %+v, want limit %d cursor %q", tt.query, page, tt.wantLimit, tt.wantCursor)
		}
		switch {
		case tt.wantCode == "" && len(errs) != 0:
			t.Errorf("%q: errors = %+v, want none", tt.query, errs)
		case tt.wantCode != "" && (len(errs) != 1 || errs[0].Field != "limit" || errs[0].Code != tt.wantCode):
			t.Errorf("%q: errors = %+v, want %s on limit", tt.query, errs, tt.wantCode)
		}
	}
}

func TestNewCollection(t *testing.T) {
	cursor := func(n int) string { return string(rune('a' + n)) }

	tests := []struct {
		name      string
		fetched   []int
		wantItems []int
		wantNext  string
	}{
		{name: "empty", fetched: nil, wantItems: []int{}},
		{name: "last page", fetched: []int{1, 2}, wantItems: []int{1, 2}},
		{name: "full last page", fetched: []int{1, 2, 3}, wantItems: []int{1, 2, 3}},
		{name: "more pages", fetched: []int{1, 2, 3, 4}, wantItems: []int{1, 2, 3}, wantNext: "d"},
	}
	for _, tt := range tests {
		c := httpx.NewCollection(tt.fetched, 3, cursor)
		if c.Items == nil || !slices.Equal(c.Items, tt.wantItems) {
			t.Errorf("%s: items = %#v, want %v", tt.name, c.Items, tt.wantItems)
		}
		switch {
		case tt.wantNext == "" && c.NextCursor != nil:
			t.Errorf("%s: next cursor = %q, want none", tt.name, *c.NextCursor)
		case tt.wantNext != "" && (c.NextCursor == nil || *c.NextCursor != tt.wantNext):
			t.Errorf("%s: next cursor = %v, want %q", tt.name, c.NextCursor, tt.wantNext)
		}
	}
}

func TestUUIDCursorRoundTrip(t *testing.T) {
	id := uuid.Must(uuid.NewV7())
	got, err := httpx.ParseUUIDCursor(httpx.UUIDCursor(id))
	if err != nil || got != id {
		t.Fatalf("ParseUUIDCursor(UUIDCursor(%s)) = %s, %v", id, got, err)
	}

	for _, bad := range []string{"", "not base64!", "AAAA"} {
		if _, err := httpx.ParseUUIDCursor(bad); !errors.Is(err, httpx.ErrInvalidCursor) {
			t.Errorf("ParseUUIDCursor(%q) error = %v, want ErrInvalidCursor", bad, err)
		}
	}
}
