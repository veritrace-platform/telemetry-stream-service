package admin_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/admin"
)

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestHealthz(t *testing.T) {
	h := admin.NewHandler(admin.NewReadiness(nil, time.Second), prometheus.NewRegistry())

	if rec := get(t, h, "/healthz"); rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}

func TestReadyz(t *testing.T) {
	failing := errors.New("connection refused")
	tests := []struct {
		name       string
		checks     map[string]admin.Check
		draining   bool
		wantStatus int
		wantState  string
	}{
		{
			name:       "all checks pass",
			checks:     map[string]admin.Check{"postgres": func(context.Context) error { return nil }},
			wantStatus: http.StatusOK,
			wantState:  "ready",
		},
		{
			name: "one check fails",
			checks: map[string]admin.Check{
				"postgres": func(context.Context) error { return nil },
				"kafka":    func(context.Context) error { return failing },
			},
			wantStatus: http.StatusServiceUnavailable,
			wantState:  "not_ready",
		},
		{
			name:       "draining",
			checks:     map[string]admin.Check{"postgres": func(context.Context) error { return nil }},
			draining:   true,
			wantStatus: http.StatusServiceUnavailable,
			wantState:  "draining",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			readiness := admin.NewReadiness(tt.checks, time.Second)
			if tt.draining {
				readiness.SetDraining()
			}
			rec := get(t, admin.NewHandler(readiness, prometheus.NewRegistry()), "/readyz")

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			var body struct {
				Status string `json:"status"`
			}
			if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if body.Status != tt.wantState {
				t.Errorf("status field = %q, want %q", body.Status, tt.wantState)
			}
		})
	}
}

func TestMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector())

	rec := get(t, admin.NewHandler(admin.NewReadiness(nil, time.Second), registry), "/metrics")

	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "go_goroutines") {
		t.Errorf("metrics endpoint: status %d, body lacks go_goroutines", rec.Code)
	}
}
