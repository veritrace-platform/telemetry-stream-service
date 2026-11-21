// Package warning records excursion warnings (cold-chain-monitoring.md §7; ADR-0023): the warnings that the forecast
// issues, their outcomes, their events on Kafka topic telemetry.forecasts (messaging.md §7), and the forecast quality
// that the outcomes measure.
package warning

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/veritrace-platform/telemetry-stream-service/internal/envelope"
	"github.com/veritrace-platform/telemetry-stream-service/internal/forecast"
)

// Event types (messaging.md §7).
const (
	TypeIssued = "cold_chain.warning_issued"
	TypeClosed = "cold_chain.warning_closed"
)

// Outcome is how a warning ended.
type Outcome string

// Outcomes.
const (
	// Breached: a breach was confirmed while the warning was open.
	Breached Outcome = "BREACHED"
	// Cleared: the trend no longer approached the bound.
	Cleared Outcome = "CLEARED"
	// Inconclusive: the shipment stopped being monitored while the warning was open.
	Inconclusive Outcome = "INCONCLUSIVE"
)

// Warning is an excursion warning of one shipment.
type Warning struct {
	ID         uuid.UUID
	ShipmentID uuid.UUID
	SSCC       string
	// DeviceID and TemperatureCelsius are those of the reading that issued the warning.
	DeviceID              string
	IssuedAt              time.Time
	Bound                 forecast.Bound
	BoundCelsius          float64
	TemperatureCelsius    float64
	SlopeCelsiusPerMinute float64
	PredictedExcursionAt  time.Time
	// ClosedAt and Outcome are nil while the warning is open.
	ClosedAt *time.Time
	Outcome  *Outcome
	// IncidentID and LeadTimeSeconds are set for a BREACHED warning only.
	IncidentID      *uuid.UUID
	LeadTimeSeconds *int
}

// NewID returns the ID of the warning of an SSCC issued at issuedAt: a UUIDv7 with the issue time, so IDs sort by
// issue.
func NewID(sscc string, issuedAt time.Time) uuid.UUID {
	return envelope.DerivedV7(issuedAt, "warning|"+sscc+"|"+issuedAt.UTC().Format(envelope.TimeLayout))
}

// FromForecast returns the warning that the forecast issued for a shipment.
func FromForecast(w forecast.Warning, sscc string, shipmentID uuid.UUID) Warning {
	return Warning{
		ID: NewID(sscc, w.IssuedAt), ShipmentID: shipmentID, SSCC: sscc, DeviceID: w.DeviceID, IssuedAt: w.IssuedAt,
		Bound: w.Bound, BoundCelsius: w.BoundCelsius, TemperatureCelsius: w.TemperatureCelsius,
		SlopeCelsiusPerMinute: w.SlopeCelsiusPerMinute, PredictedExcursionAt: w.PredictedExcursionAt,
	}
}

// Forecast returns the warning as the forecaster continues it.
func (w Warning) Forecast() forecast.Warning {
	return forecast.Warning{
		IssuedAt: w.IssuedAt, DeviceID: w.DeviceID, TemperatureCelsius: w.TemperatureCelsius, Bound: w.Bound,
		BoundCelsius: w.BoundCelsius, SlopeCelsiusPerMinute: w.SlopeCelsiusPerMinute,
		PredictedExcursionAt: w.PredictedExcursionAt,
	}
}

// Issue is the data of cold_chain.warning_issued (messaging.md §7).
type Issue struct {
	WarningID             uuid.UUID      `json:"warning_id"`
	ShipmentID            uuid.UUID      `json:"shipment_id"`
	SSCC                  string         `json:"sscc"`
	DeviceID              string         `json:"device_id"`
	IssuedAt              string         `json:"issued_at"`
	Bound                 forecast.Bound `json:"bound"`
	BoundCelsius          float64        `json:"bound_celsius"`
	TemperatureCelsius    float64        `json:"temperature_celsius"`
	SlopeCelsiusPerMinute float64        `json:"slope_celsius_per_minute"`
	PredictedExcursionAt  string         `json:"predicted_excursion_at"`
}

// Closure is the data of cold_chain.warning_closed (messaging.md §7).
type Closure struct {
	WarningID       uuid.UUID  `json:"warning_id"`
	SSCC            string     `json:"sscc"`
	ClosedAt        string     `json:"closed_at"`
	Outcome         Outcome    `json:"outcome"`
	IncidentID      *uuid.UUID `json:"incident_id"`
	LeadTimeSeconds *int       `json:"lead_time_seconds"`
}

// IssuedEvent returns cold_chain.warning_issued for the warning, which occurred when it was issued. status is the
// shipment's status.
func IssuedEvent(w Warning, status string) (envelope.Event, error) {
	return envelope.New(envelope.DerivedV7(w.IssuedAt, TypeIssued+"|"+w.ID.String()), TypeIssued, w.IssuedAt,
		envelope.Subject{ShipmentID: w.ShipmentID, SSCC: w.SSCC, Status: status}, Issue{
			WarningID: w.ID, ShipmentID: w.ShipmentID, SSCC: w.SSCC, DeviceID: w.DeviceID,
			IssuedAt: w.IssuedAt.UTC().Format(envelope.TimeLayout), Bound: w.Bound, BoundCelsius: w.BoundCelsius,
			TemperatureCelsius: w.TemperatureCelsius, SlopeCelsiusPerMinute: w.SlopeCelsiusPerMinute,
			PredictedExcursionAt: w.PredictedExcursionAt.UTC().Format(envelope.TimeLayout),
		})
}

// ClosedEvent returns cold_chain.warning_closed for the warning, which occurred when it was closed. status is the
// shipment's status.
func ClosedEvent(w Warning, status string) (envelope.Event, error) {
	if w.ClosedAt == nil || w.Outcome == nil {
		return envelope.Event{}, errors.New("the warning is open")
	}
	return envelope.New(envelope.DerivedV7(*w.ClosedAt, TypeClosed+"|"+w.ID.String()), TypeClosed, *w.ClosedAt,
		envelope.Subject{ShipmentID: w.ShipmentID, SSCC: w.SSCC, Status: status}, Closure{
			WarningID: w.ID, SSCC: w.SSCC, ClosedAt: w.ClosedAt.UTC().Format(envelope.TimeLayout), Outcome: *w.Outcome,
			IncidentID: w.IncidentID, LeadTimeSeconds: w.LeadTimeSeconds,
		})
}

// Record returns the Kafka record of a warning event on telemetry.forecasts, in the trace of traceparent.
func Record(e envelope.Event, traceparent string) (*kgo.Record, error) {
	record, err := e.Record(envelope.TopicForecasts, traceparent)
	if err != nil {
		return nil, fmt.Errorf("record %s: %w", e.EventType, err)
	}
	return record, nil
}
