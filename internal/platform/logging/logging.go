// Package logging builds the structured JSON logger shared by the whole service.
package logging

import (
	"context"
	"io"
	"log/slog"

	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/tracecontext"
)

// New returns a JSON logger that tags every record with the service identity, the trace ID, and any
// attributes attached to the context with WithAttrs.
func New(w io.Writer, service, version string, level slog.Leveler) *slog.Logger {
	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level, ReplaceAttr: utcTime})
	return slog.New(contextHandler{Handler: handler}).With(
		slog.String("service", service),
		slog.String("version", version),
	)
}

// utcTime renders record timestamps in UTC regardless of the host time zone.
func utcTime(groups []string, a slog.Attr) slog.Attr {
	if a.Key == slog.TimeKey && len(groups) == 0 && a.Value.Kind() == slog.KindTime {
		a.Value = slog.TimeValue(a.Value.Time().UTC())
	}
	return a
}

type attrsKey struct{}

// WithAttrs returns a copy of ctx whose log records also carry attrs (for example tenant_id).
func WithAttrs(ctx context.Context, attrs ...slog.Attr) context.Context {
	existing, _ := ctx.Value(attrsKey{}).([]slog.Attr)
	merged := make([]slog.Attr, 0, len(existing)+len(attrs))
	merged = append(merged, existing...)
	merged = append(merged, attrs...)
	return context.WithValue(ctx, attrsKey{}, merged)
}

type contextHandler struct {
	slog.Handler
}

func (h contextHandler) Handle(ctx context.Context, record slog.Record) error {
	if tc, ok := tracecontext.FromContext(ctx); ok {
		record.AddAttrs(slog.String("trace_id", tc.TraceID))
	}
	if attrs, ok := ctx.Value(attrsKey{}).([]slog.Attr); ok {
		record.AddAttrs(attrs...)
	}
	return h.Handler.Handle(ctx, record)
}

func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return contextHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{Handler: h.Handler.WithGroup(name)}
}
