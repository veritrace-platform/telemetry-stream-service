package httpx_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/httpx"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/logging"
)

func newRouter(t *testing.T, logs *bytes.Buffer, registry *prometheus.Registry) chi.Router {
	t.Helper()
	logger := logging.New(logs, "test", "dev", slog.LevelInfo)

	r := chi.NewRouter()
	r.Use(httpx.Trace, httpx.AccessLog(logger), httpx.Metrics(registry, "test"), httpx.Recover(logger))
	r.NotFound(httpx.NotFound)
	r.MethodNotAllowed(httpx.MethodNotAllowed)
	r.Get("/items/{id}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	r.Patch("/items/{id}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	r.Get("/panic", func(http.ResponseWriter, *http.Request) { panic("boom") })
	return r
}

func decodeProblem(t *testing.T, rec *httptest.ResponseRecorder) httpx.Problem {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != httpx.ProblemContentType {
		t.Fatalf("Content-Type = %q, want %q", ct, httpx.ProblemContentType)
	}
	var p httpx.Problem
	if err := json.NewDecoder(rec.Body).Decode(&p); err != nil {
		t.Fatalf("decode problem: %v", err)
	}
	return p
}

func TestUnknownRouteReturnsProblem(t *testing.T) {
	var logs bytes.Buffer
	router := newRouter(t, &logs, prometheus.NewRegistry())

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/missing", nil))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	p := decodeProblem(t, rec)
	if p.Code != httpx.CodeNotFound || p.Status != http.StatusNotFound || p.Instance != "/missing" {
		t.Errorf("unexpected problem: %+v", p)
	}
	if p.TraceID == "" || p.TraceID != rec.Header().Get(httpx.TraceHeader) {
		t.Errorf("trace_id = %q, header = %q; want equal and non-empty", p.TraceID, rec.Header().Get(httpx.TraceHeader))
	}
}

func TestMethodNotAllowedReturnsProblem(t *testing.T) {
	var logs bytes.Buffer
	router := newRouter(t, &logs, prometheus.NewRegistry())

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/items/1", nil))

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
	if p := decodeProblem(t, rec); p.Code != httpx.CodeMethodNotAllowed {
		t.Errorf("code = %q, want %q", p.Code, httpx.CodeMethodNotAllowed)
	}
	if allow := rec.Header().Get("Allow"); allow != "GET, PATCH" {
		t.Errorf("Allow = %q, want the methods of the route", allow)
	}
}

func TestSecureHeaders(t *testing.T) {
	handler := httpx.SecureHeaders(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/keys" {
			w.Header().Set("Cache-Control", "public, max-age=300")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	for path, cacheControl := range map[string]string{"/items": "no-store", "/keys": "public, max-age=300"} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if got := rec.Header().Get("Cache-Control"); got != cacheControl {
			t.Errorf("%s: Cache-Control = %q, want %q", path, got, cacheControl)
		}
		if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
			t.Errorf("%s: X-Content-Type-Options = %q, want nosniff", path, got)
		}
	}
}

func TestTraceContinuesIncomingTrace(t *testing.T) {
	var logs bytes.Buffer
	router := newRouter(t, &logs, prometheus.NewRegistry())

	req := httptest.NewRequest(http.MethodGet, "/items/1", nil)
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if got := rec.Header().Get(httpx.TraceHeader); got != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("X-Trace-Id = %q, want incoming trace ID", got)
	}
}

func TestAccessLogUsesRoutePattern(t *testing.T) {
	var logs bytes.Buffer
	router := newRouter(t, &logs, prometheus.NewRegistry())

	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/items/42", nil))

	var record map[string]any
	if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
		t.Fatalf("access log is not JSON: %v", err)
	}
	if record["route"] != "/items/{id}" || record["status"] != float64(http.StatusNoContent) {
		t.Errorf("unexpected access log: %v", record)
	}
	if record["trace_id"] == nil {
		t.Error("access log lacks trace_id")
	}
}

func TestRecoverTurnsPanicIntoInternalError(t *testing.T) {
	var logs bytes.Buffer
	router := newRouter(t, &logs, prometheus.NewRegistry())

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/panic", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if p := decodeProblem(t, rec); p.Code != httpx.CodeInternalError || strings.Contains(p.Detail, "boom") {
		t.Errorf("problem leaks panic or has wrong code: %+v", p)
	}
	if !strings.Contains(logs.String(), "boom") {
		t.Error("panic value was not logged")
	}
}

func TestMetricsRecordsRequests(t *testing.T) {
	var logs bytes.Buffer
	registry := prometheus.NewRegistry()
	router := newRouter(t, &logs, registry)

	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/items/7", nil))

	if n := testutil.CollectAndCount(registry, "veritrace_test_http_request_duration_seconds"); n != 1 {
		t.Errorf("series count = %d, want 1", n)
	}
}

func TestProblemExtensionMembers(t *testing.T) {
	p := httpx.NewProblem(http.StatusUnprocessableEntity, "OUT_OF_AREA", "position outside the area")
	p.Extensions = map[string]any{"distance_meters": 412.5, "allowed_meters": 212}
	rec := httptest.NewRecorder()
	httpx.WriteProblem(rec, httptest.NewRequest(http.MethodPost, "/api/v1/things", nil), p)

	var got map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["code"] != "OUT_OF_AREA" || got["instance"] != "/api/v1/things" || got["distance_meters"] != 412.5 ||
		got["allowed_meters"] != 212.0 || len(got) != 8 {
		t.Errorf("problem = %v, want the standard members and both extensions", got)
	}

	b, err := json.Marshal(httpx.NewProblem(http.StatusNotFound, httpx.CodeNotFound, "missing"))
	if err != nil || string(b) != `{"type":"about:blank","title":"Not Found","status":404,"code":"NOT_FOUND","detail":"missing"}` {
		t.Errorf("without extensions: %s, %v", b, err)
	}
}
