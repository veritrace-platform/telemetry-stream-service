package tracecontext_test

import (
	"context"
	"testing"

	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/tracecontext"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name        string
		header      string
		wantOK      bool
		wantSampled bool
	}{
		{"sampled", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", true, true},
		{"not sampled", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00", true, false},
		{"surrounding whitespace", " 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01 ", true, true},
		{"empty", "", false, false},
		{"unknown version", "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", false, false},
		{"uppercase hex", "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01", false, false},
		{"short trace id", "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01", false, false},
		{"zero trace id", "00-00000000000000000000000000000000-00f067aa0ba902b7-01", false, false},
		{"zero span id", "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", false, false},
		{"missing part", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc, ok := tracecontext.Parse(tt.header)
			if ok != tt.wantOK {
				t.Fatalf("Parse() ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && tc.Sampled != tt.wantSampled {
				t.Errorf("Sampled = %v, want %v", tc.Sampled, tt.wantSampled)
			}
		})
	}
}

func TestNewRoundTrip(t *testing.T) {
	tc := tracecontext.New()

	parsed, ok := tracecontext.Parse(tc.Traceparent())
	if !ok {
		t.Fatalf("Parse(%q) failed", tc.Traceparent())
	}
	if parsed != tc {
		t.Errorf("round trip = %+v, want %+v", parsed, tc)
	}
}

func TestChildKeepsTraceID(t *testing.T) {
	parent := tracecontext.New()
	child := parent.Child()

	if child.TraceID != parent.TraceID {
		t.Errorf("child TraceID = %s, want %s", child.TraceID, parent.TraceID)
	}
	if child.SpanID == parent.SpanID {
		t.Error("child SpanID must differ from parent")
	}
}

func TestContext(t *testing.T) {
	if _, ok := tracecontext.FromContext(context.Background()); ok {
		t.Fatal("FromContext on empty context reported ok")
	}

	tc := tracecontext.New()
	got, ok := tracecontext.FromContext(tracecontext.NewContext(context.Background(), tc))
	if !ok || got != tc {
		t.Errorf("FromContext = %+v, %v; want %+v, true", got, ok, tc)
	}
}
