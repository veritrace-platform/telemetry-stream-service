// Package incident defines cold-chain incidents (cold-chain-monitoring.md §4–§5): the record of a breached
// episode, its tamper-evident hash (ADR-0006), its events on Kafka topic telemetry.incidents (messaging.md §5),
// and its storage.
//
// Incident and event IDs are derived from what they describe rather than drawn at random. A batch that is
// processed again after a failure therefore produces the same incident, with the same hash, and the same events,
// which consumers can recognize by event_id.
package incident

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/gowebpki/jcs"

	"github.com/veritrace-platform/telemetry-stream-service/internal/envelope"
)

// Kafka topic and event types (messaging.md §5).
const (
	Topic               = "telemetry.incidents"
	Producer            = envelope.Producer
	TypeBreachConfirmed = "cold_chain.breach_confirmed"
	TypeBreachResolved  = "cold_chain.breach_resolved"
)

// TimeLayout formats incident times, which are device times with milliseconds (messaging.md §5).
const TimeLayout = envelope.TimeLayout

// Incident is a breached episode of one shipment.
type Incident struct {
	ID         uuid.UUID
	ShipmentID uuid.UUID
	SSCC       string
	// DeviceID is the device whose reading confirmed the breach.
	DeviceID    string
	StartedAt   time.Time
	ConfirmedAt time.Time
	// EndedAt and DurationSeconds are nil while the episode lasts.
	EndedAt         *time.Time
	DurationSeconds *int
	// MinTempCelsius and MaxTempCelsius are the bounds in force.
	MinTempCelsius float64
	MaxTempCelsius float64
	// TriggerTemperatureCelsius is the temperature of the confirming reading.
	TriggerTemperatureCelsius float64
	// ExtremeTemperatureCelsius is the temperature furthest from the bounds so far.
	ExtremeTemperatureCelsius float64
	// Latitude and Longitude are the position of the confirming reading.
	Latitude     float64
	Longitude    float64
	IncidentHash string
}

// NewID returns the ID of the incident of the episode that started at startedAt. An SSCC has one episode per
// start, so the ID names the incident however often its breach is confirmed. The ID is a UUIDv7 with the start
// time, so IDs sort by start.
func NewID(sscc string, startedAt time.Time) uuid.UUID {
	return envelope.DerivedV7(startedAt, "incident|"+sscc+"|"+startedAt.UTC().Format(TimeLayout))
}

// Duration returns the whole seconds from start to end.
func Duration(startedAt, endedAt time.Time) int {
	return int(endedAt.Sub(startedAt) / time.Second)
}

// Confirmation is the data of cold_chain.breach_confirmed (messaging.md §5).
type Confirmation struct {
	IncidentID         uuid.UUID `json:"incident_id"`
	ShipmentID         uuid.UUID `json:"shipment_id"`
	SSCC               string    `json:"sscc"`
	DeviceID           string    `json:"device_id"`
	StartedAt          string    `json:"started_at"`
	ConfirmedAt        string    `json:"confirmed_at"`
	MinTempCelsius     float64   `json:"min_temp_celsius"`
	MaxTempCelsius     float64   `json:"max_temp_celsius"`
	TemperatureCelsius float64   `json:"temperature_celsius"`
	Latitude           float64   `json:"latitude"`
	Longitude          float64   `json:"longitude"`
	IncidentHash       string    `json:"incident_hash,omitempty"`
}

// Confirmation returns the incident's confirmation fields.
func (i Incident) Confirmation() Confirmation {
	return Confirmation{
		IncidentID: i.ID, ShipmentID: i.ShipmentID, SSCC: i.SSCC, DeviceID: i.DeviceID,
		StartedAt: i.StartedAt.UTC().Format(TimeLayout), ConfirmedAt: i.ConfirmedAt.UTC().Format(TimeLayout),
		MinTempCelsius: i.MinTempCelsius, MaxTempCelsius: i.MaxTempCelsius,
		TemperatureCelsius: i.TriggerTemperatureCelsius, Latitude: i.Latitude, Longitude: i.Longitude,
		IncidentHash: i.IncidentHash,
	}
}

// Canonical returns the RFC 8785 canonical JSON of the confirmation without incident_hash: the hash input.
func (c Confirmation) Canonical() ([]byte, error) {
	c.IncidentHash = ""
	raw, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("encode confirmation: %w", err)
	}
	canonical, err := jcs.Transform(raw)
	if err != nil {
		return nil, fmt.Errorf("canonicalize confirmation: %w", err)
	}
	return canonical, nil
}

// Hash returns incident_hash: the lowercase hex SHA-256 of the canonical confirmation (ADR-0006).
func (c Confirmation) Hash() (string, error) {
	canonical, err := c.Canonical()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// Resolution is the data of cold_chain.breach_resolved (messaging.md §5).
type Resolution struct {
	IncidentID                uuid.UUID `json:"incident_id"`
	SSCC                      string    `json:"sscc"`
	EndedAt                   string    `json:"ended_at"`
	DurationSeconds           int       `json:"duration_seconds"`
	ExtremeTemperatureCelsius float64   `json:"extreme_temperature_celsius"`
}

// Resolution returns the incident's resolution fields. The incident must have ended.
func (i Incident) Resolution() Resolution {
	r := Resolution{IncidentID: i.ID, SSCC: i.SSCC, ExtremeTemperatureCelsius: i.ExtremeTemperatureCelsius}
	if i.EndedAt != nil {
		r.EndedAt = i.EndedAt.UTC().Format(TimeLayout)
	}
	if i.DurationSeconds != nil {
		r.DurationSeconds = *i.DurationSeconds
	}
	return r
}
