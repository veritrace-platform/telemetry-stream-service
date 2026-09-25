// Package tracecontext implements W3C Trace Context (traceparent) parsing, generation, and propagation.
package tracecontext

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"
)

// HeaderName is the W3C Trace Context request header.
const HeaderName = "traceparent"

// TraceContext identifies the current trace and span.
type TraceContext struct {
	TraceID string // 32 lowercase hex characters
	SpanID  string // 16 lowercase hex characters
	Sampled bool
}

// Parse decodes a version-00 traceparent header. It reports false for malformed or all-zero identifiers.
func Parse(header string) (TraceContext, bool) {
	parts := strings.Split(strings.TrimSpace(header), "-")
	if len(parts) != 4 || parts[0] != "00" {
		return TraceContext{}, false
	}
	traceID, spanID, flags := parts[1], parts[2], parts[3]
	if !isLowerHex(traceID, 32) || !isLowerHex(spanID, 16) || !isLowerHex(flags, 2) {
		return TraceContext{}, false
	}
	if isZero(traceID) || isZero(spanID) {
		return TraceContext{}, false
	}
	flagByte, err := hex.DecodeString(flags)
	if err != nil {
		return TraceContext{}, false
	}
	return TraceContext{TraceID: traceID, SpanID: spanID, Sampled: flagByte[0]&0x01 == 1}, true
}

// New starts a new sampled trace.
func New() TraceContext {
	return TraceContext{TraceID: randomHex(16), SpanID: randomHex(8), Sampled: true}
}

// Child returns a new span within the same trace.
func (tc TraceContext) Child() TraceContext {
	return TraceContext{TraceID: tc.TraceID, SpanID: randomHex(8), Sampled: tc.Sampled}
}

// Traceparent encodes the context as a traceparent header value.
func (tc TraceContext) Traceparent() string {
	flags := "00"
	if tc.Sampled {
		flags = "01"
	}
	return "00-" + tc.TraceID + "-" + tc.SpanID + "-" + flags
}

type contextKey struct{}

// NewContext returns a copy of ctx carrying tc.
func NewContext(ctx context.Context, tc TraceContext) context.Context {
	return context.WithValue(ctx, contextKey{}, tc)
}

// FromContext returns the trace context stored in ctx, if any.
func FromContext(ctx context.Context) (TraceContext, bool) {
	tc, ok := ctx.Value(contextKey{}).(TraceContext)
	return tc, ok
}

func randomHex(n int) string {
	b := make([]byte, n)
	// crypto/rand.Read never returns an error on supported platforms.
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func isLowerHex(s string, length int) bool {
	if len(s) != length {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func isZero(s string) bool {
	return strings.Trim(s, "0") == ""
}
