package incident

import (
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/veritrace-platform/telemetry-stream-service/internal/envelope"
)

// Subject names the shipment of an event.
type Subject = envelope.Subject

// Event is the domain event envelope (messaging.md §2.1) of telemetry.incidents. Incidents have no actor and no
// hash chain: data.incident_hash makes a confirmation tamper-evident instead.
type Event struct {
	envelope.Event
}

// ConfirmedEvent returns cold_chain.breach_confirmed for the incident, which occurred at its confirmation. status
// is the shipment's status.
func ConfirmedEvent(i Incident, status string) (Event, error) {
	e, err := envelope.New(envelope.DerivedV7(i.ConfirmedAt, TypeBreachConfirmed+"|"+i.ID.String()), TypeBreachConfirmed,
		i.ConfirmedAt, Subject{ShipmentID: i.ShipmentID, SSCC: i.SSCC, Status: status}, i.Confirmation())
	return Event{e}, err
}

// ResolvedEvent returns cold_chain.breach_resolved for the incident, which occurred when it ended. status is the
// shipment's status.
func ResolvedEvent(i Incident, status string) (Event, error) {
	if i.EndedAt == nil {
		return Event{}, errors.New("the incident has not ended")
	}
	e, err := envelope.New(envelope.DerivedV7(*i.EndedAt, TypeBreachResolved+"|"+i.ID.String()), TypeBreachResolved,
		*i.EndedAt, Subject{ShipmentID: i.ShipmentID, SSCC: i.SSCC, Status: status}, i.Resolution())
	return Event{e}, err
}

// Record returns the Kafka record of the event, keyed by SSCC, in the trace of traceparent.
func (e Event) Record(traceparent string) (*kgo.Record, error) {
	return e.Event.Record(Topic, traceparent)
}

// DecodeEvent reads an envelope of telemetry.incidents.
func DecodeEvent(value []byte) (Event, error) {
	e, err := envelope.Decode(value)
	if err != nil {
		return Event{}, fmt.Errorf("decode incident event: %w", err)
	}
	if e.EventType != TypeBreachConfirmed && e.EventType != TypeBreachResolved {
		return Event{}, fmt.Errorf("unknown incident event type %q", e.EventType)
	}
	return Event{e}, nil
}
