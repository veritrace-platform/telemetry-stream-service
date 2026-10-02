package incident

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/veritrace-platform/telemetry-stream-service/internal/stream"
)

// Subject names the shipment of an event.
type Subject struct {
	ShipmentID uuid.UUID `json:"shipment_id"`
	SSCC       string    `json:"sscc"`
	Status     string    `json:"status"`
}

// Event is the domain event envelope (messaging.md §2.1) of telemetry.incidents. Incidents have no actor and no
// hash chain: data.incident_hash makes a confirmation tamper-evident instead.
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

// ConfirmedEvent returns cold_chain.breach_confirmed for the incident, which occurred at its confirmation. status
// is the shipment's status.
func ConfirmedEvent(i Incident, status string) (Event, error) {
	data, err := json.Marshal(i.Confirmation())
	if err != nil {
		return Event{}, fmt.Errorf("encode confirmation: %w", err)
	}
	return Event{
		EventID:      derivedV7(i.ConfirmedAt, TypeBreachConfirmed+"|"+i.ID.String()),
		EventType:    TypeBreachConfirmed,
		EventVersion: eventVersion,
		OccurredAt:   i.ConfirmedAt.UTC().Format(occurredAtLayout),
		Producer:     Producer,
		Subject:      Subject{ShipmentID: i.ShipmentID, SSCC: i.SSCC, Status: status},
		Data:         data,
	}, nil
}

// ResolvedEvent returns cold_chain.breach_resolved for the incident, which occurred when it ended. status is the
// shipment's status.
func ResolvedEvent(i Incident, status string) (Event, error) {
	if i.EndedAt == nil {
		return Event{}, errors.New("the incident has not ended")
	}
	data, err := json.Marshal(i.Resolution())
	if err != nil {
		return Event{}, fmt.Errorf("encode resolution: %w", err)
	}
	return Event{
		EventID:      derivedV7(*i.EndedAt, TypeBreachResolved+"|"+i.ID.String()),
		EventType:    TypeBreachResolved,
		EventVersion: eventVersion,
		OccurredAt:   i.EndedAt.UTC().Format(occurredAtLayout),
		Producer:     Producer,
		Subject:      Subject{ShipmentID: i.ShipmentID, SSCC: i.SSCC, Status: status},
		Data:         data,
	}, nil
}

// Record returns the Kafka record of the event, keyed by SSCC, in the trace of traceparent.
func (e Event) Record(traceparent string) (*kgo.Record, error) {
	value, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", e.EventType, err)
	}
	return &kgo.Record{
		Topic:   Topic,
		Key:     []byte(e.Subject.SSCC),
		Value:   value,
		Headers: append(stream.JSONHeaders(traceparent), kgo.RecordHeader{Key: stream.HeaderEventType, Value: []byte(e.EventType)}),
	}, nil
}

// DecodeEvent reads an envelope of telemetry.incidents.
func DecodeEvent(value []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(value, &e); err != nil {
		return Event{}, fmt.Errorf("decode incident event: %w", err)
	}
	if e.EventType != TypeBreachConfirmed && e.EventType != TypeBreachResolved {
		return Event{}, fmt.Errorf("unknown incident event type %q", e.EventType)
	}
	return e, nil
}
