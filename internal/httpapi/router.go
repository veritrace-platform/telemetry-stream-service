// Package httpapi assembles the public REST router: shared middleware, error handlers, and the
// /api/v1 route tree that domain packages register into.
package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/httpx"
)

// Routes registers a domain's handlers under /api/v1.
type Routes func(r chi.Router)

// NewRouter returns the API handler with middleware applied to every request, including unmatched ones.
func NewRouter(logger *slog.Logger, registerer prometheus.Registerer, routes ...Routes) http.Handler {
	r := chi.NewRouter()
	r.Use(
		httpx.Trace,
		httpx.AccessLog(logger),
		httpx.Metrics(registerer, "telemetry"),
		httpx.Recover(logger),
	)
	r.NotFound(httpx.NotFound)
	r.MethodNotAllowed(httpx.MethodNotAllowed)

	// Mounting the version root also initializes chi's middleware chain, which chi otherwise builds only
	// when the first route is added.
	r.Route("/api/v1", func(api chi.Router) {
		for _, register := range routes {
			register(api)
		}
	})
	return r
}
