package httpapi_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/veritrace-platform/telemetry-stream-service/internal/httpapi"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/httpx"
)

func TestMiddlewareAppliesWithoutRoutes(t *testing.T) {
	registry := prometheus.NewRegistry()
	router := httpapi.NewRouter(slog.New(slog.DiscardHandler), registry, httpapi.Mounts{})

	for _, path := range []string{"/", "/api/v1/shipments", "/.well-known/jwks.json"} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))

		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s: status = %d, want 404", path, rec.Code)
		}
		var p httpx.Problem
		if err := json.NewDecoder(rec.Body).Decode(&p); err != nil {
			t.Fatalf("%s: decode problem: %v", path, err)
		}
		if rec.Header().Get("Cache-Control") != "no-store" || rec.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Errorf("%s: Cache-Control = %q, X-Content-Type-Options = %q", path, rec.Header().Get("Cache-Control"),
				rec.Header().Get("X-Content-Type-Options"))
		}
		if p.TraceID == "" || p.TraceID != rec.Header().Get(httpx.TraceHeader) {
			t.Errorf("%s: trace_id = %q, X-Trace-Id = %q; want equal and non-empty", path, p.TraceID, rec.Header().Get(httpx.TraceHeader))
		}
	}

	if n := testutil.CollectAndCount(registry, "veritrace_telemetry_http_request_duration_seconds"); n == 0 {
		t.Error("no request metrics recorded")
	}
}

func TestMountedRoutesAreServedUnderTheirPrefix(t *testing.T) {
	noContent := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }
	router := httpapi.NewRouter(slog.New(slog.DiscardHandler), prometheus.NewRegistry(), httpapi.Mounts{
		API:       []httpapi.Routes{func(r chi.Router) { r.Get("/ping", noContent) }},
		WellKnown: []httpapi.Routes{func(r chi.Router) { r.Get("/jwks.json", noContent) }},
		WebSocket: []httpapi.Routes{func(r chi.Router) { r.Get("/notifications", noContent) }},
	})

	tests := []struct {
		method, path string
		want         int
	}{
		{http.MethodGet, "/api/v1/ping", http.StatusNoContent},
		{http.MethodPost, "/api/v1/ping", http.StatusMethodNotAllowed},
		{http.MethodGet, "/.well-known/jwks.json", http.StatusNoContent},
		{http.MethodGet, "/.well-known/other", http.StatusNotFound},
		{http.MethodGet, "/ws/v1/notifications", http.StatusNoContent},
		{http.MethodGet, "/ws/v1/other", http.StatusNotFound},
		{http.MethodGet, "/ping", http.StatusNotFound},
	}
	for _, tt := range tests {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(tt.method, tt.path, nil))
		if rec.Code != tt.want {
			t.Errorf("%s %s: status = %d, want %d", tt.method, tt.path, rec.Code, tt.want)
		}
		// Routes in mounted trees name their methods in the Allow header of a 405 too.
		if tt.want == http.StatusMethodNotAllowed && rec.Header().Get("Allow") != http.MethodGet {
			t.Errorf("%s %s: Allow = %q, want GET", tt.method, tt.path, rec.Header().Get("Allow"))
		}
	}
}
