// Package httpapi assembles the public REST router: shared middleware, error handlers, and the route trees
// that domain packages register into.
package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/httpx"
)

// Routes registers a domain's handlers on the router it receives.
type Routes func(r chi.Router)

// Mounts lists the route trees of a service.
type Mounts struct {
	// API trees are served under /api/v1.
	API []Routes
	// WellKnown trees are served under /.well-known, for example jwks.json.
	WellKnown []Routes
}

// NewRouter returns the API handler with middleware applied to every request, including unmatched ones.
func NewRouter(logger *slog.Logger, registerer prometheus.Registerer, mounts Mounts) http.Handler {
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
		for _, register := range mounts.API {
			register(api)
		}
	})
	if len(mounts.WellKnown) > 0 {
		r.Route("/.well-known", func(wellKnown chi.Router) {
			for _, register := range mounts.WellKnown {
				register(wellKnown)
			}
		})
	}
	return r
}
