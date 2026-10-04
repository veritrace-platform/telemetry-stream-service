// Package processor runs the processor role of ADR-0012 on Kafka topic iot.telemetry.raw: it stores each batch of
// readings, runs them through breach detection, and sets aside on the dead-letter topic the records that it
// cannot store.
package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/veritrace-platform/telemetry-stream-service/internal/detector"
	"github.com/veritrace-platform/telemetry-stream-service/internal/pgerror"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/tracecontext"
	"github.com/veritrace-platform/telemetry-stream-service/internal/reading"
	"github.com/veritrace-platform/telemetry-stream-service/internal/stream"
)

// Group is the consumer group of the processor.
const Group = "telemetry-stream-service.processor"

// retries is how many more times a reading that the database refuses is tried before it is dead-lettered.
const retries = 3

// failedAtLayout formats the time a record failed, as domain events format timestamps.
const failedAtLayout = "2006-01-02T15:04:05.000000Z"

// reasonStorage labels dead letters that the database refused.
const reasonStorage = "storage_refused"

// ReadingStore stores readings and skips those already stored.
type ReadingStore interface {
	Insert(ctx context.Context, readings []reading.Reading) (int64, error)
}

// Detector evaluates stored readings against the breach rules, as *detector.Detector does.
type Detector interface {
	Evaluate(ctx context.Context, inputs []detector.Input) error
}

// Producer writes records and waits for them to be acknowledged, as *kgo.Client does.
type Producer interface {
	ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults
}

// Processor handles batches of raw readings.
type Processor struct {
	readings     ReadingStore
	detector     Detector
	producer     Producer
	logger       *slog.Logger
	now          func() time.Time
	deadLettered *prometheus.CounterVec
}

// New returns a processor. Its metrics are registered with registerer.
func New(readings ReadingStore, detector Detector, producer Producer, logger *slog.Logger,
	registerer prometheus.Registerer, now func() time.Time,
) *Processor {
	deadLettered := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "veritrace_telemetry_readings_dead_lettered_total",
		Help: "Raw readings sent to the dead-letter topic, by reason.",
	}, []string{"reason"})
	registerer.MustRegister(deadLettered)
	return &Processor{
		readings: readings, detector: detector, producer: producer, logger: logger, now: now, deadLettered: deadLettered,
	}
}

// pending is a valid reading and the record it came from.
type pending struct {
	record  *kgo.Record
	reading reading.Reading
}

// Handle stores a batch and evaluates the stored readings. A record that is not a valid reading is dead-lettered
// at once; the same check would fail again. A reading that the database refuses is tried alone up to three more
// times and then dead-lettered. Any other failure, such as an unreachable database, fails the batch, which the
// consumer retries.
func (p *Processor) Handle(ctx context.Context, records []*kgo.Record) error {
	var (
		valid []pending
		dead  []*kgo.Record
	)
	for _, rec := range records {
		r, err := reading.Decode(rec.Value)
		if err != nil {
			dead = append(dead, p.deadLetter(ctx, rec, err))
			continue
		}
		valid = append(valid, pending{record: rec, reading: r})
	}

	stored, refused, err := p.store(ctx, valid)
	if err != nil {
		return err
	}
	dead = append(dead, refused...)

	inputs := make([]detector.Input, len(stored))
	for i, s := range stored {
		inputs[i] = detector.Input{Reading: s.reading, Traceparent: stream.Header(s.record, stream.HeaderTraceparent)}
	}
	if err := p.detector.Evaluate(ctx, inputs); err != nil {
		return fmt.Errorf("detect breaches: %w", err)
	}

	if len(dead) > 0 {
		if err := p.producer.ProduceSync(ctx, dead...).FirstErr(); err != nil {
			return fmt.Errorf("produce dead letters: %w", err)
		}
	}
	return nil
}

// store inserts the readings in one statement and returns those stored. If the database refuses the batch, each
// reading is inserted alone to find the ones it refuses, which are returned as dead letters.
func (p *Processor) store(ctx context.Context, batch []pending) ([]pending, []*kgo.Record, error) {
	readings := make([]reading.Reading, len(batch))
	for i, b := range batch {
		readings[i] = b.reading
	}
	n, err := p.readings.Insert(ctx, readings)
	if err == nil {
		p.logger.DebugContext(ctx, "stored readings", slog.Int("received", len(readings)), slog.Int64("new", n))
		return batch, nil, nil
	}
	if !pgerror.IsDataError(err) {
		return nil, nil, fmt.Errorf("store readings: %w", err)
	}

	var (
		stored []pending
		dead   []*kgo.Record
	)
	for _, b := range batch {
		var err error
		for range 1 + retries {
			if _, err = p.readings.Insert(ctx, []reading.Reading{b.reading}); err == nil || !pgerror.IsDataError(err) {
				break
			}
		}
		switch {
		case err == nil:
			stored = append(stored, b)
		case pgerror.IsDataError(err):
			dead = append(dead, p.deadLetter(ctx, b.record, err))
		default:
			return nil, nil, fmt.Errorf("store reading: %w", err)
		}
	}
	return stored, dead, nil
}

// deadLetter wraps a failed record as messaging.md §4 defines, counts it, and logs it.
func (p *Processor) deadLetter(ctx context.Context, rec *kgo.Record, cause error) *kgo.Record {
	tc, ok := tracecontext.Parse(stream.Header(rec, stream.HeaderTraceparent))
	if !ok {
		tc = tracecontext.New()
	}
	ctx = tracecontext.NewContext(ctx, tc)
	reason := reasonStorage
	var rejected *reading.RejectedError
	if errors.As(cause, &rejected) {
		reason = string(rejected.Reason)
	}
	p.deadLettered.WithLabelValues(reason).Inc()
	p.logger.WarnContext(ctx, "reading dead-lettered", slog.String("reason", reason),
		slog.Int("partition", int(rec.Partition)), slog.Int64("offset", rec.Offset), slog.Any("error", cause))

	// The original record is embedded as JSON when it is JSON, and as a string otherwise.
	original := json.RawMessage(rec.Value)
	if !json.Valid(rec.Value) {
		original, _ = json.Marshal(string(rec.Value))
	}
	value, _ := json.Marshal(struct {
		Error    string          `json:"error"`
		FailedAt string          `json:"failed_at"`
		Record   json.RawMessage `json:"record"`
	}{cause.Error(), p.now().UTC().Format(failedAtLayout), original})
	return &kgo.Record{
		Topic:   reading.DeadLetterTopic,
		Key:     rec.Key,
		Value:   value,
		Headers: stream.JSONHeaders(tc.Child().Traceparent()),
	}
}
