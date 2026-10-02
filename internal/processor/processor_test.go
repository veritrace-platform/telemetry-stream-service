package processor_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/veritrace-platform/telemetry-stream-service/internal/detector"
	"github.com/veritrace-platform/telemetry-stream-service/internal/processor"
	"github.com/veritrace-platform/telemetry-stream-service/internal/reading"
	"github.com/veritrace-platform/telemetry-stream-service/internal/stream"
)

var now = time.Date(2026, 9, 1, 14, 31, 0, 0, time.UTC)

// fakeStore refuses the readings of the devices in refuse, as a check constraint would, or fails every insert
// with err.
type fakeStore struct {
	refuse map[string]bool
	err    error
	calls  int
	stored []reading.Reading
}

func (s *fakeStore) Insert(_ context.Context, readings []reading.Reading) (int64, error) {
	s.calls++
	if s.err != nil {
		return 0, s.err
	}
	for _, r := range readings {
		if s.refuse[r.DeviceID] {
			return 0, fmt.Errorf("insert readings: %w", &pgconn.PgError{Code: "23514", Message: "check violation"})
		}
	}
	s.stored = append(s.stored, readings...)
	return int64(len(readings)), nil
}

// fakeDetector records the readings it evaluates, or fails with err.
type fakeDetector struct {
	err       error
	evaluated []detector.Input
}

func (d *fakeDetector) Evaluate(_ context.Context, inputs []detector.Input) error {
	if d.err != nil {
		return d.err
	}
	d.evaluated = append(d.evaluated, inputs...)
	return nil
}

type fakeProducer struct {
	records []*kgo.Record
}

func (p *fakeProducer) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	p.records = append(p.records, records...)
	results := make(kgo.ProduceResults, len(records))
	for i, r := range records {
		results[i] = kgo.ProduceResult{Record: r}
	}
	return results
}

func raw(t *testing.T, device string, offset time.Duration) *kgo.Record {
	t.Helper()
	r := reading.Reading{
		DeviceID: device, SSCC: "089300010000000018", RecordedAt: now.Add(offset), ReceivedAt: now.Add(offset),
		TemperatureCelsius: 5, Latitude: 10.87, Longitude: 106.8,
	}
	value, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: reading.RawTopic, Key: []byte(r.SSCC), Value: value,
		Headers: stream.JSONHeaders("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")}
}

func newProcessor(store processor.ReadingStore, producer processor.Producer) (*processor.Processor, *prometheus.Registry) {
	return newProcessorWith(store, &fakeDetector{}, producer)
}

func newProcessorWith(store processor.ReadingStore, d processor.Detector, producer processor.Producer,
) (*processor.Processor, *prometheus.Registry) {
	registry := prometheus.NewRegistry()
	return processor.New(store, d, producer, slog.New(slog.DiscardHandler), registry, func() time.Time { return now }),
		registry
}

type deadLetter struct {
	Error    string          `json:"error"`
	FailedAt string          `json:"failed_at"`
	Record   json.RawMessage `json:"record"`
}

func decodeDeadLetter(t *testing.T, r *kgo.Record) deadLetter {
	t.Helper()
	var d deadLetter
	if err := json.Unmarshal(r.Value, &d); err != nil {
		t.Fatalf("decode dead letter: %v", err)
	}
	return d
}

func TestHandleStoresABatchInOneInsert(t *testing.T) {
	store, producer := &fakeStore{}, &fakeProducer{}
	p, _ := newProcessor(store, producer)
	if err := p.Handle(t.Context(), []*kgo.Record{raw(t, "D1", 0), raw(t, "D1", 5*time.Second)}); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if store.calls != 1 || len(store.stored) != 2 || len(producer.records) != 0 {
		t.Errorf("calls = %d, stored = %d, dead letters = %d", store.calls, len(store.stored), len(producer.records))
	}
}

func TestHandleDeadLettersInvalidRecords(t *testing.T) {
	store, producer := &fakeStore{}, &fakeProducer{}
	p, registry := newProcessor(store, producer)
	invalid := raw(t, "D1", 0)
	invalid.Value = []byte(`{"device_id":"D1","sscc":"089300010000000019"}`)
	garbage := &kgo.Record{Topic: reading.RawTopic, Key: []byte("x"), Value: []byte("not json")}
	if err := p.Handle(t.Context(), []*kgo.Record{invalid, raw(t, "D1", 0), garbage}); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(store.stored) != 1 || len(producer.records) != 2 {
		t.Fatalf("stored = %d, dead letters = %d", len(store.stored), len(producer.records))
	}

	first := producer.records[0]
	if first.Topic != reading.DeadLetterTopic || string(first.Key) != "089300010000000018" ||
		stream.Header(first, stream.HeaderContentType) != stream.ContentTypeJSON {
		t.Errorf("dead letter = %+v", first)
	}
	// The dead letter continues the record's trace.
	if tp := stream.Header(first, stream.HeaderTraceparent); len(tp) != 55 || tp[3:35] != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("traceparent = %q", tp)
	}
	d := decodeDeadLetter(t, first)
	if d.FailedAt != "2026-09-01T14:31:00.000000Z" || string(d.Record) != string(invalid.Value) || d.Error == "" {
		t.Errorf("dead letter = %+v", d)
	}
	if got := string(decodeDeadLetter(t, producer.records[1]).Record); got != `"not json"` {
		t.Errorf("non-JSON record embedded as %s", got)
	}
	if got := deadLettered(t, registry, string(reading.ReasonMalformed)); got != 2 {
		t.Errorf("malformed dead letters = %v, want 2", got)
	}
}

func TestHandleRetriesRefusedReadingsAloneThenDeadLetters(t *testing.T) {
	store, producer := &fakeStore{refuse: map[string]bool{"BAD": true}}, &fakeProducer{}
	p, registry := newProcessor(store, producer)
	records := []*kgo.Record{raw(t, "D1", 0), raw(t, "BAD", 0), raw(t, "D2", 0)}
	if err := p.Handle(t.Context(), records); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	// One batch insert, then D1 once, BAD four times, and D2 once.
	if store.calls != 7 {
		t.Errorf("inserts = %d, want 7", store.calls)
	}
	if len(store.stored) != 2 || len(producer.records) != 1 || string(decodeDeadLetter(t, producer.records[0]).Record) !=
		string(records[1].Value) {
		t.Errorf("stored = %d, dead letters = %d", len(store.stored), len(producer.records))
	}
	if got := deadLettered(t, registry, "storage_refused"); got != 1 {
		t.Errorf("storage dead letters = %v, want 1", got)
	}
}

func TestHandleEvaluatesTheStoredReadings(t *testing.T) {
	store, d, producer := &fakeStore{refuse: map[string]bool{"BAD": true}}, &fakeDetector{}, &fakeProducer{}
	p, _ := newProcessorWith(store, d, producer)
	invalid := raw(t, "D3", 0)
	invalid.Value = []byte(`{}`)
	records := []*kgo.Record{raw(t, "D1", 0), raw(t, "BAD", 0), invalid, raw(t, "D2", 5*time.Second)}
	if err := p.Handle(t.Context(), records); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(d.evaluated) != 2 || d.evaluated[0].Reading.DeviceID != "D1" || d.evaluated[1].Reading.DeviceID != "D2" {
		t.Fatalf("evaluated = %+v, want the readings of D1 and D2", d.evaluated)
	}
	if d.evaluated[0].Traceparent != stream.Header(records[0], stream.HeaderTraceparent) {
		t.Errorf("traceparent = %q", d.evaluated[0].Traceparent)
	}
}

func TestHandleFailsTheBatchWhenDetectionFails(t *testing.T) {
	d, producer := &fakeDetector{err: errors.New("database unavailable")}, &fakeProducer{}
	p, _ := newProcessorWith(&fakeStore{}, d, producer)
	invalid := raw(t, "D1", 0)
	invalid.Value = []byte(`{}`)
	if err := p.Handle(t.Context(), []*kgo.Record{raw(t, "D1", 0), invalid}); err == nil {
		t.Fatal("Handle() succeeded")
	}
	if len(producer.records) != 0 {
		t.Errorf("dead letters = %d, want none until the batch succeeds", len(producer.records))
	}
}

func TestHandleFailsTheBatchWhenTheDatabaseIsUnavailable(t *testing.T) {
	store, producer := &fakeStore{err: errors.New("connection refused")}, &fakeProducer{}
	p, _ := newProcessor(store, producer)
	invalid := raw(t, "D1", 0)
	invalid.Value = []byte(`{}`)
	if err := p.Handle(t.Context(), []*kgo.Record{raw(t, "D1", 0), invalid}); err == nil {
		t.Fatal("Handle() succeeded")
	}
	// Nothing is dead-lettered, so a retry of the batch does not dead-letter the invalid record twice.
	if len(producer.records) != 0 {
		t.Errorf("dead letters = %d, want 0", len(producer.records))
	}
}

// deadLettered reads the dead-letter counter of a reason.
func deadLettered(t *testing.T, registry *prometheus.Registry, reason string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != "veritrace_telemetry_readings_dead_lettered_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "reason" && l.GetValue() == reason {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}
