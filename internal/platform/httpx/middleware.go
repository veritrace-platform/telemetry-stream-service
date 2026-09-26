package httpx

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/tracecontext"
)

// TraceHeader carries the trace ID back to clients.
const TraceHeader = "X-Trace-Id"

// Trace continues the caller's W3C trace (or starts one), stores it in the request context, and echoes
// the trace ID in the response.
func Trace(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tc, ok := tracecontext.Parse(r.Header.Get(tracecontext.HeaderName))
		if ok {
			tc = tc.Child()
		} else {
			tc = tracecontext.New()
		}
		w.Header().Set(TraceHeader, tc.TraceID)
		next.ServeHTTP(w, r.WithContext(tracecontext.NewContext(r.Context(), tc)))
	})
}

// Recover converts panics into 500 responses and logs them with a stack trace.
func Recover(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				rec := recover()
				if rec == nil {
					return
				}
				if rec == http.ErrAbortHandler { //nolint:errorlint // sentinel compared by identity, as net/http does
					panic(rec)
				}
				logger.ErrorContext(r.Context(), "panic while handling request",
					slog.Any("panic", rec),
					slog.String("stack", string(debug.Stack())),
				)
				WriteProblem(w, r, NewProblem(http.StatusInternalServerError, CodeInternalError, "internal error"))
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// AccessLog logs one record per request with the matched route pattern rather than the raw path.
func AccessLog(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)

			status := ww.Status()
			if status == 0 {
				status = http.StatusOK
			}
			level := slog.LevelInfo
			if status >= http.StatusInternalServerError {
				level = slog.LevelError
			}
			logger.LogAttrs(r.Context(), level, "http request",
				slog.String("method", r.Method),
				slog.String("route", routePattern(r)),
				slog.Int("status", status),
				slog.Int64("duration_ms", time.Since(start).Milliseconds()),
				slog.Int("bytes", ww.BytesWritten()),
			)
		})
	}
}

// Metrics records request count and latency by method, route pattern, and status.
func Metrics(registerer prometheus.Registerer, subsystem string) func(http.Handler) http.Handler {
	duration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "veritrace",
		Subsystem: subsystem,
		Name:      "http_request_duration_seconds",
		Help:      "HTTP request latency by method, route, and status.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "route", "status"})
	registerer.MustRegister(duration)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)

			status := ww.Status()
			if status == 0 {
				status = http.StatusOK
			}
			duration.WithLabelValues(r.Method, routePattern(r), strconv.Itoa(status)).
				Observe(time.Since(start).Seconds())
		})
	}
}

func routePattern(r *http.Request) string {
	if rctx := chi.RouteContext(r.Context()); rctx != nil {
		if pattern := rctx.RoutePattern(); pattern != "" {
			return pattern
		}
	}
	return "unmatched"
}
