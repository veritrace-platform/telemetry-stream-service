// Package envelope is the domain event envelope (messaging.md §2.1) of the events that the telemetry service
// produces on telemetry.incidents and telemetry.forecasts, and the derived IDs that make those events repeatable.
//
// IDs are derived from what an event describes rather than drawn at random. A batch that is processed again after
// a failure therefore produces the same events, which consumers recognize by event_id.
package envelope

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/veritrace-platform/telemetry-stream-service/internal/stream"
)

// TopicForecasts is the Kafka topic of model estimates: excursion warnings and shelf-life alerts (messaging.md §7).
const TopicForecasts = "telemetry.forecasts"

// Producer names the telemetry service in the envelope.
const Producer = "telemetry-stream-service"

// Version is the event_version of every event the service produces.
const Version = 1

// TimeLayout formats times in event data, which are device times with milliseconds (messaging.md §5, §7).
const TimeLayout = "2006-01-02T15:04:05.000Z07:00"

// OccurredAtLayout formats the envelope's occurred_at with six fractional digits (messaging.md §2.1).
const OccurredAtLayout = "2006-01-02T15:04:05.000000Z07:00"

// Subject names the shipment of an event.
type Subject struct {
	ShipmentID uuid.UUID `json:"shipment_id"`
	SSCC       string    `json:"sscc"`
	Status     string    `json:"status"`
}

// Event is the envelope of an event of the telemetry service. Its events have no actor and no hash chain.
type Event struct {
	EventID      uuid.UUID       `json:"event_id"`
	EventType    string          `json:"event_type"`
	EventVersion int             `json:"event_version"`
	OccurredAt   string          `json:"occurred_at"`
	Producer     string          `json:"producer"`
	Subject      Subject         `json:"subject"`
	Actor        any             `json:"actor"`
	Data         json.RawMessage `json:"data"`
}

// New returns an event of eventType about subject that occurred at occurredAt, with data encoded as JSON.
func New(id uuid.UUID, eventType string, occurredAt time.Time, subject Subject, data any) (Event, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return Event{}, fmt.Errorf("encode %s data: %w", eventType, err)
	}
	return Event{
		EventID: id, EventType: eventType, EventVersion: Version, OccurredAt: occurredAt.UTC().Format(OccurredAtLayout),
		Producer: Producer, Subject: subject, Data: raw,
	}, nil
}

// Record returns the Kafka record of the event on topic, keyed by SSCC, in the trace of traceparent.
func (e Event) Record(topic, traceparent string) (*kgo.Record, error) {
	value, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", e.EventType, err)
	}
	return &kgo.Record{
		Topic:   topic,
		Key:     []byte(e.Subject.SSCC),
		Value:   value,
		Headers: append(stream.JSONHeaders(traceparent), kgo.RecordHeader{Key: stream.HeaderEventType, Value: []byte(e.EventType)}),
	}, nil
}

// Decode reads an envelope.
func Decode(value []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(value, &e); err != nil {
		return Event{}, fmt.Errorf("decode event: %w", err)
	}
	return e, nil
}

// DerivedV7 returns a UUIDv7 with the millisecond timestamp of t and, instead of random bits, bits of the SHA-256 of
// seed. IDs of things that happen later sort later.
func DerivedV7(t time.Time, seed string) uuid.UUID {
	sum := sha256.Sum256([]byte(seed))
	var id uuid.UUID
	var ms [8]byte
	binary.BigEndian.PutUint64(ms[:], uint64(t.UnixMilli())) //nolint:gosec // device times are after 1970
	copy(id[:6], ms[2:])
	copy(id[6:], sum[:10])
	id[6] = id[6]&0x0f | 0x70 // version 7
	id[8] = id[8]&0x3f | 0x80 // RFC 9562 variant
	return id
}

// DerivedV8 returns a UUIDv8 (RFC 9562, custom) made of bits of the SHA-256 of seed alone, for an event whose time
// may differ between two productions of it.
func DerivedV8(seed string) uuid.UUID {
	sum := sha256.Sum256([]byte(seed))
	var id uuid.UUID
	copy(id[:], sum[:16])
	id[6] = id[6]&0x0f | 0x80 // version 8
	id[8] = id[8]&0x3f | 0x80 // RFC 9562 variant
	return id
}
