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
	router := httpapi.NewRouter(slog.New(slog.DiscardHandler), registry)

	for _, path := range []string{"/", "/api/v1/shipments"} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))

		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s: status = %d, want 404", path, rec.Code)
		}
		var p httpx.Problem
		if err := json.NewDecoder(rec.Body).Decode(&p); err != nil {
			t.Fatalf("%s: decode problem: %v", path, err)
		}
		if p.TraceID == "" || p.TraceID != rec.Header().Get(httpx.TraceHeader) {
			t.Errorf("%s: trace_id = %q, X-Trace-Id = %q; want equal and non-empty", path, p.TraceID, rec.Header().Get(httpx.TraceHeader))
		}
	}

	if n := testutil.CollectAndCount(registry, "veritrace_telemetry_http_request_duration_seconds"); n == 0 {
		t.Error("no request metrics recorded")
	}
}

func TestRegisteredRoutesAreServedUnderVersionPrefix(t *testing.T) {
	router := httpapi.NewRouter(slog.New(slog.DiscardHandler), prometheus.NewRegistry(), func(r chi.Router) {
		r.Get("/ping", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	})

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/ping", nil))
	if rec.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", rec.Code)
	}

	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/ping", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST status = %d, want 405", rec.Code)
	}
}
