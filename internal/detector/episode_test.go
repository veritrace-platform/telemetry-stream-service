package detector_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/veritrace-platform/telemetry-stream-service/internal/detector"
	"github.com/veritrace-platform/telemetry-stream-service/internal/incident"
	"github.com/veritrace-platform/telemetry-stream-service/internal/reading"
)

const sscc = "089300010000000018"

var (
	t0         = time.Date(2026, 9, 1, 14, 30, 0, 0, time.UTC)
	shipmentID = uuid.MustParse("0192f7a4-7c3e-7d2a-9b1e-3f4a5b6c7da0")
	chilled    = detector.Bounds{Min: 2, Max: 8}
)

// at returns a reading taken seconds after t0.
func at(seconds float64, temperature float64) reading.Reading {
	return reading.Reading{
		DeviceID: "REEFER-0001", SSCC: sscc, RecordedAt: t0.Add(time.Duration(seconds * float64(time.Second))),
		TemperatureCelsius: temperature, Latitude: 10.8705, Longitude: 106.8035,
	}
}

// event is a change, abbreviated for comparison: kind, start, confirmation, end, and extreme, in seconds after t0.
type event struct {
	kind                      detector.ChangeKind
	started, confirmed, ended float64
	extreme                   float64
}

func (e event) String() string {
	return fmt.Sprintf("{kind %d, started %v, confirmed %v, ended %v, extreme %v}", e.kind, e.started, e.confirmed, e.ended,
		e.extreme)
}

func abbreviate(c detector.Change) event {
	secs := func(t time.Time) float64 { return t.Sub(t0).Seconds() }
	e := event{kind: c.Kind, started: secs(c.Incident.StartedAt), confirmed: secs(c.Incident.ConfirmedAt),
		extreme: c.Incident.ExtremeTemperatureCelsius, ended: -1}
	if c.Incident.EndedAt != nil {
		e.ended = secs(*c.Incident.EndedAt)
	}
	return e
}

// run steps an episode through the readings and returns every change.
func run(t *testing.T, ep *detector.Episode, readings ...reading.Reading) []event {
	t.Helper()
	var events []event
	for _, r := range readings {
		changes, _ := ep.Step(r)
		for _, c := range changes {
			events = append(events, abbreviate(c))
		}
	}
	return events
}

// series returns readings every 5 s from start (inclusive) to end (exclusive), at a constant temperature.
func series(start, end, temperature float64) []reading.Reading {
	var rs []reading.Reading
	for s := start; s < end; s += 5 {
		rs = append(rs, at(s, temperature))
	}
	return rs
}

func concat(parts ...[]reading.Reading) []reading.Reading {
	var all []reading.Reading
	for _, p := range parts {
		all = append(all, p...)
	}
	return all
}

func TestEpisode(t *testing.T) {
	tests := []struct {
		name     string
		readings []reading.Reading
		want     []event
	}{
		{
			name:     "readings in bounds",
			readings: concat(series(0, 60, 4.5), []reading.Reading{at(60, 2), at(65, 8)}),
		},
		{
			// The short-excursion scenario of the simulator: five readings span 20 s.
			name:     "an excursion shorter than 30 s",
			readings: concat(series(0, 25, 9.2), series(25, 60, 6)),
		},
		{
			name:     "a breach is confirmed 30 s after the excursion starts",
			readings: concat(series(0, 10, 4.5), series(10, 40, 9), []reading.Reading{at(40, 9.5)}),
			want:     []event{{kind: detector.Confirmed, started: 10, confirmed: 40, ended: -1, extreme: 9.5}},
		},
		{
			name: "the incident tracks the extreme and ends at the first reading in bounds",
			readings: []reading.Reading{
				at(0, 9), at(5, 9.5), at(10, 9), at(15, 9), at(20, 9), at(25, 9), at(30, 9.2), at(35, 11.5),
				at(40, 10), at(45, 12), at(50, 7.9),
			},
			want: []event{
				{kind: detector.Confirmed, started: 0, confirmed: 30, ended: -1, extreme: 9.5},
				{kind: detector.Extended, started: 0, confirmed: 30, ended: -1, extreme: 11.5},
				{kind: detector.Extended, started: 0, confirmed: 30, ended: -1, extreme: 12},
				{kind: detector.Resolved, started: 0, confirmed: 30, ended: 50, extreme: 12},
			},
		},
		{
			name:     "a breach below the bounds keeps the coldest temperature",
			readings: []reading.Reading{at(0, 1.5), at(10, 0.5), at(20, 1), at(30, 1.9), at(35, -0.5), at(40, 2)},
			want: []event{
				{kind: detector.Confirmed, started: 0, confirmed: 30, ended: -1, extreme: 0.5},
				{kind: detector.Extended, started: 0, confirmed: 30, ended: -1, extreme: -0.5},
				{kind: detector.Resolved, started: 0, confirmed: 30, ended: 40, extreme: -0.5},
			},
		},
		{
			name:     "the extreme is the temperature furthest from the bounds on either side",
			readings: []reading.Reading{at(0, 9), at(10, 0.5), at(20, 9.4), at(30, 9.2)},
			want:     []event{{kind: detector.Confirmed, started: 0, confirmed: 30, ended: -1, extreme: 0.5}},
		},
		{
			name:     "bounds are inclusive",
			readings: concat(series(0, 60, 8), series(60, 120, 2)),
		},
		{
			name:     "a gap of exactly 15 s continues the episode",
			readings: []reading.Reading{at(0, 9), at(15, 9), at(30, 9)},
			want:     []event{{kind: detector.Confirmed, started: 0, confirmed: 30, ended: -1, extreme: 9}},
		},
		{
			// The sensor-gap scenario of the simulator: two short excursions 25 s apart.
			name:     "a gap longer than 15 s splits an excursion into two episodes",
			readings: concat(series(0, 20, 9.5), series(40, 60, 9.5), series(60, 90, 4.5)),
		},
		{
			name:     "a new episode after a gap is confirmed from its own start",
			readings: concat(series(0, 20, 9.5), series(36, 70, 9.5)),
			want:     []event{{kind: detector.Confirmed, started: 36, confirmed: 66, ended: -1, extreme: 9.5}},
		},
		{
			name:     "a gap ends a breach at the last reading received",
			readings: concat(series(0, 40, 9), []reading.Reading{at(56, 9), at(61, 9)}),
			want: []event{
				{kind: detector.Confirmed, started: 0, confirmed: 30, ended: -1, extreme: 9},
				{kind: detector.Resolved, started: 0, confirmed: 30, ended: 35, extreme: 9},
			},
		},
		{
			name:     "a gap before an in-bounds reading also ends the breach at the last reading received",
			readings: concat(series(0, 40, 9), []reading.Reading{at(60, 5)}),
			want: []event{
				{kind: detector.Confirmed, started: 0, confirmed: 30, ended: -1, extreme: 9},
				{kind: detector.Resolved, started: 0, confirmed: 30, ended: 35, extreme: 9},
			},
		},
		{
			name: "readings that are not after the last evaluated one are not evaluated",
			readings: []reading.Reading{
				at(0, 9), at(10, 9), at(10, 5), at(5, 5), at(20, 9), at(30, 9), at(30, 9), at(25, 3),
			},
			want: []event{{kind: detector.Confirmed, started: 0, confirmed: 30, ended: -1, extreme: 9}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := run(t, detector.NewEpisode(sscc, shipmentID, chilled), tt.readings...)
			if fmt.Sprint(got) != fmt.Sprint(tt.want) {
				t.Errorf("changes = %v\nwant %v", got, tt.want)
			}
		})
	}
}

func TestEpisodeIncidentFields(t *testing.T) {
	ep := detector.NewEpisode(sscc, shipmentID, chilled)
	trigger := at(30, 9.3)
	trigger.DeviceID, trigger.Latitude, trigger.Longitude = "REEFER-0002", 10.9, 106.7
	for _, r := range []reading.Reading{at(0, 9.6), at(10, 9), at(20, 9)} {
		if changes, evaluated := ep.Step(r); !evaluated || changes != nil {
			t.Fatalf("Step() = %v, %v", changes, evaluated)
		}
	}
	changes, _ := ep.Step(trigger)
	if len(changes) != 1 {
		t.Fatalf("changes = %v", changes)
	}
	got := changes[0].Incident
	want := incident.Incident{
		ID: incident.NewID(sscc, t0), ShipmentID: shipmentID, SSCC: sscc, DeviceID: "REEFER-0002", StartedAt: t0,
		ConfirmedAt: t0.Add(30 * time.Second), MinTempCelsius: 2, MaxTempCelsius: 8, TriggerTemperatureCelsius: 9.3,
		ExtremeTemperatureCelsius: 9.6, Latitude: 10.9, Longitude: 106.7,
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("incident = %+v\nwant %+v", got, want)
	}
}

func TestEpisodeSeed(t *testing.T) {
	start := t0
	confirmed := t0.Add(30 * time.Second)
	open := &incident.Incident{
		ID: incident.NewID(sscc, start), ShipmentID: shipmentID, SSCC: sscc, DeviceID: "REEFER-0001", StartedAt: start,
		ConfirmedAt: confirmed, MinTempCelsius: 2, MaxTempCelsius: 8, TriggerTemperatureCelsius: 9,
		ExtremeTemperatureCelsius: 10, Latitude: 10.8705, Longitude: 106.8035,
	}

	t.Run("an open incident continues", func(t *testing.T) {
		ep := detector.NewEpisode(sscc, shipmentID, chilled)
		ep.Seed(t0.Add(25*time.Second), open)
		// Readings up to the confirmation belong to the incident already.
		got := run(t, ep, at(28, 13), at(30, 13), at(35, 11), at(40, 10.5), at(45, 5))
		want := []event{
			{kind: detector.Extended, started: 0, confirmed: 30, ended: -1, extreme: 11},
			{kind: detector.Resolved, started: 0, confirmed: 30, ended: 45, extreme: 11},
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("changes = %v\nwant %v", got, want)
		}
	})

	t.Run("an open incident whose readings stopped ends at the last reading", func(t *testing.T) {
		ep := detector.NewEpisode(sscc, shipmentID, chilled)
		ep.Seed(t0.Add(90*time.Second), open)
		got := run(t, ep, at(200, 9))
		want := []event{{kind: detector.Resolved, started: 0, confirmed: 30, ended: 90, extreme: 10}}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("changes = %v\nwant %v", got, want)
		}
	})

	t.Run("readings of a closed incident are not evaluated again", func(t *testing.T) {
		closed := *open
		end := t0.Add(80 * time.Second)
		closed.EndedAt = &end
		ep := detector.NewEpisode(sscc, shipmentID, chilled)
		ep.Seed(time.Time{}, &closed)
		// Replaying the excursion that the incident covers confirms nothing again; the readings after its end
		// start a new episode.
		if got := run(t, ep, series(20, 115, 9)...); got != nil {
			t.Errorf("replay changes = %v", got)
		}
		got := run(t, ep, at(115, 9))
		want := []event{{kind: detector.Confirmed, started: 85, confirmed: 115, ended: -1, extreme: 9}}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("changes = %v\nwant %v", got, want)
		}
	})
}
