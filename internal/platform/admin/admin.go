// Package admin serves operational endpoints (health, readiness, metrics) on a port that is never
// exposed through the gateway.
package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Check reports whether one dependency is usable.
type Check func(ctx context.Context) error

// Readiness tracks whether the service should receive traffic.
type Readiness struct {
	checks   map[string]Check
	timeout  time.Duration
	draining atomic.Bool
}

// NewReadiness returns a readiness tracker that runs checks with the given per-request timeout.
func NewReadiness(checks map[string]Check, timeout time.Duration) *Readiness {
	return &Readiness{checks: checks, timeout: timeout}
}

// SetDraining makes readiness fail so that load balancers stop routing new requests during shutdown.
func (r *Readiness) SetDraining() {
	r.draining.Store(true)
}

// NewHandler returns the admin HTTP handler.
func NewHandler(readiness *Readiness, gatherer prometheus.Gatherer) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", readiness.serve)
	mux.Handle("GET /metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	return mux
}

type readinessReport struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

func (r *Readiness) serve(w http.ResponseWriter, req *http.Request) {
	if r.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, readinessReport{Status: "draining"})
		return
	}

	ctx, cancel := context.WithTimeout(req.Context(), r.timeout)
	defer cancel()

	names := make([]string, 0, len(r.checks))
	for name := range r.checks {
		names = append(names, name)
	}
	sort.Strings(names)

	report := readinessReport{Status: "ready", Checks: make(map[string]string, len(names))}
	status := http.StatusOK
	for _, name := range names {
		if err := r.checks[name](ctx); err != nil {
			report.Checks[name] = err.Error()
			report.Status = "not_ready"
			status = http.StatusServiceUnavailable
			continue
		}
		report.Checks[name] = "ok"
	}
	writeJSON(w, status, report)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
