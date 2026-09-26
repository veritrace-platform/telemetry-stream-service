package logging_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/logging"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/tracecontext"
)

func TestNewAddsServiceTraceAndContextAttributes(t *testing.T) {
	var buf bytes.Buffer
	logger := logging.New(&buf, "test-service", "1.2.3", slog.LevelInfo)

	tc := tracecontext.New()
	ctx := tracecontext.NewContext(context.Background(), tc)
	ctx = logging.WithAttrs(ctx, slog.String("tenant_id", "t-1"))
	ctx = logging.WithAttrs(ctx, slog.String("user_id", "u-1"))

	logger.With(slog.String("component", "http")).InfoContext(ctx, "handled")

	var record map[string]any
	if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
		t.Fatalf("log output is not JSON: %v (%s)", err, buf.String())
	}
	want := map[string]string{
		"msg":       "handled",
		"level":     "INFO",
		"service":   "test-service",
		"version":   "1.2.3",
		"component": "http",
		"trace_id":  tc.TraceID,
		"tenant_id": "t-1",
		"user_id":   "u-1",
	}
	for key, value := range want {
		if record[key] != value {
			t.Errorf("record[%q] = %v, want %q", key, record[key], value)
		}
	}
	if ts, _ := record["time"].(string); !strings.HasSuffix(ts, "Z") {
		t.Errorf("time = %q, want UTC (Z suffix)", ts)
	}
}

func TestNewRespectsLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := logging.New(&buf, "svc", "dev", slog.LevelWarn)

	logger.Info("dropped")

	if buf.Len() != 0 {
		t.Errorf("info record written at warn level: %s", buf.String())
	}
}
