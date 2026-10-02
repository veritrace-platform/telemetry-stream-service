// Package detector runs the breach rules of cold-chain-monitoring.md §4 on the readings of each monitored shipment
// and records what they find as incidents (ADR-0012).
package detector

import (
	"time"

	"github.com/google/uuid"

	"github.com/veritrace-platform/telemetry-stream-service/internal/incident"
	"github.com/veritrace-platform/telemetry-stream-service/internal/reading"
)

// Breach rules (cold-chain-monitoring.md §4).
const (
	// ConfirmAfter is how long an excursion lasts before it is a breach.
	ConfirmAfter = 30 * time.Second
	// MaxGap is the longest silence between readings within one episode.
	MaxGap = 15 * time.Second
)

// Bounds are a shipment's inclusive temperature range.
type Bounds struct {
	Min, Max float64
}

// outside returns how far t lies outside the bounds, or 0 if it lies within.
func (b Bounds) outside(t float64) float64 {
	switch {
	case t < b.Min:
		return b.Min - t
	case t > b.Max:
		return t - b.Max
	}
	return 0
}

type phase int

const (
	normal phase = iota
	excursion
	breached
)

// ChangeKind says what a reading did to an incident.
type ChangeKind int

// Changes to an incident.
const (
	// Confirmed: the breach was confirmed, and the incident begins.
	Confirmed ChangeKind = iota + 1
	// Extended: the incident has a new extreme temperature.
	Extended
	// Resolved: the episode ended, and so does the incident.
	Resolved
)

// Change is a change to an incident, with the incident as it is after the change. A confirmed incident has no
// hash yet.
type Change struct {
	Kind     ChangeKind
	Incident incident.Incident
}

// Episode is the episode state machine of one SSCC: NORMAL, EXCURSION, or BREACHED. Readings of every device on
// the SSCC move it, in device-time order.
type Episode struct {
	sscc       string
	shipmentID uuid.UUID
	bounds     Bounds

	phase phase
	// last is the time of the last reading evaluated; zero before the first.
	last time.Time
	// start is the time of the first out-of-bounds reading of the episode.
	start time.Time
	// extreme is the temperature furthest from the bounds in the episode.
	extreme float64
	// open is the incident of a breached episode.
	open *incident.Incident
}

// NewEpisode returns an episode in NORMAL state for a shipment's SSCC.
func NewEpisode(sscc string, shipmentID uuid.UUID, bounds Bounds) *Episode {
	return &Episode{sscc: sscc, shipmentID: shipmentID, bounds: bounds}
}

// Seed puts a new episode where the database left it: lastBefore is the time of the last reading before the
// readings to replay (zero if none), and latest is the SSCC's latest incident (nil if none). An open incident
// continues as a BREACHED episode. Readings up to the end of a closed incident, or up to the confirmation of an
// open one, belong to that incident and are not evaluated again.
func (e *Episode) Seed(lastBefore time.Time, latest *incident.Incident) {
	e.last = lastBefore
	if latest == nil {
		return
	}
	if latest.EndedAt != nil {
		if e.last.Before(*latest.EndedAt) {
			e.last = *latest.EndedAt
		}
		return
	}
	open := *latest
	e.phase, e.start, e.extreme, e.open = breached, open.StartedAt, open.ExtremeTemperatureCelsius, &open
	if e.last.Before(open.ConfirmedAt) {
		e.last = open.ConfirmedAt
	}
}

// Step evaluates a reading and returns the changes it makes, in order. A reading whose time is not after the last
// evaluated one is not evaluated, and Step reports false.
func (e *Episode) Step(r reading.Reading) ([]Change, bool) {
	t := r.RecordedAt
	if !e.last.IsZero() && !t.After(e.last) {
		return nil, false
	}
	var changes []Change
	// Missing data is not evidence of a sustained excursion: a gap ends the episode, and a breach ends at the last
	// reading received.
	if !e.last.IsZero() && t.Sub(e.last) > MaxGap {
		if e.phase == breached {
			changes = append(changes, e.resolve(e.last))
		}
		e.phase = normal
	}
	e.last = t

	out := e.bounds.outside(r.TemperatureCelsius)
	switch e.phase {
	case normal:
		if out > 0 {
			e.phase, e.start, e.extreme = excursion, t, r.TemperatureCelsius
		}
	case excursion:
		if out == 0 {
			e.phase = normal
			break
		}
		if out > e.bounds.outside(e.extreme) {
			e.extreme = r.TemperatureCelsius
		}
		if t.Sub(e.start) >= ConfirmAfter {
			changes = append(changes, e.confirm(r))
		}
	case breached:
		if out == 0 {
			changes = append(changes, e.resolve(t))
			e.phase = normal
			break
		}
		if out > e.bounds.outside(e.extreme) {
			e.extreme = r.TemperatureCelsius
			e.open.ExtremeTemperatureCelsius = e.extreme
			changes = append(changes, Change{Kind: Extended, Incident: *e.open})
		}
	}
	return changes, true
}

// confirm turns the excursion into a breach, confirmed by r.
func (e *Episode) confirm(r reading.Reading) Change {
	e.phase = breached
	e.open = &incident.Incident{
		ID: incident.NewID(e.sscc, e.start), ShipmentID: e.shipmentID, SSCC: e.sscc, DeviceID: r.DeviceID,
		StartedAt: e.start, ConfirmedAt: r.RecordedAt, MinTempCelsius: e.bounds.Min, MaxTempCelsius: e.bounds.Max,
		TriggerTemperatureCelsius: r.TemperatureCelsius, ExtremeTemperatureCelsius: e.extreme,
		Latitude: r.Latitude, Longitude: r.Longitude,
	}
	return Change{Kind: Confirmed, Incident: *e.open}
}

// resolve ends the breached episode's incident at end.
func (e *Episode) resolve(end time.Time) Change {
	resolved := *e.open
	duration := incident.Duration(resolved.StartedAt, end)
	resolved.EndedAt, resolved.DurationSeconds = &end, &duration
	e.open = nil
	return Change{Kind: Resolved, Incident: resolved}
}
