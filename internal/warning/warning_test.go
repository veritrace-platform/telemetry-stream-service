package warning_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/veritrace-platform/telemetry-stream-service/internal/envelope"
	"github.com/veritrace-platform/telemetry-stream-service/internal/forecast"
	"github.com/veritrace-platform/telemetry-stream-service/internal/stream"
	"github.com/veritrace-platform/telemetry-stream-service/internal/warning"
)

const sscc = "089300010000000018"

func sample() warning.Warning {
	issued := time.Date(2026, 9, 1, 14, 26, 40, 0, time.UTC)
	return warning.FromForecast(forecast.Warning{
		IssuedAt: issued, DeviceID: "REEFER-0001", TemperatureCelsius: 6.4, Bound: forecast.Max, BoundCelsius: 8,
		SlopeCelsiusPerMinute: 0.62, PredictedExcursionAt: issued.Add(155 * time.Second),
	}, sscc, uuid.MustParse("0192f7a4-7c3e-7d2a-9b1e-3f4a5b6c7da0"))
}

func TestIDsAreDerived(t *testing.T) {
	w := sample()
	if w.ID != warning.NewID(sscc, w.IssuedAt.In(time.FixedZone("ICT", 7*3600))) || w.ID.Version() != 7 {
		t.Errorf("ID %s is not derived from the SSCC and issue time", w.ID)
	}
	if w.ID == warning.NewID(sscc, w.IssuedAt.Add(time.Millisecond)) {
		t.Error("two issue times give one ID")
	}
	if back := w.Forecast(); back.IssuedAt != w.IssuedAt || back.Bound != forecast.Max || back.PredictedExcursionAt != w.PredictedExcursionAt {
		t.Errorf("Forecast() = %+v", back)
	}
}

func TestIssuedEvent(t *testing.T) {
	w := sample()
	e, err := warning.IssuedEvent(w, "IN_TRANSIT")
	if err != nil {
		t.Fatalf("IssuedEvent() error = %v", err)
	}
	again, _ := warning.IssuedEvent(w, "IN_TRANSIT")
	if e.EventID != again.EventID || e.OccurredAt != "2026-09-01T14:26:40.000000Z" || e.Producer != envelope.Producer ||
		e.Actor != nil || e.Subject.SSCC != sscc {
		t.Errorf("event = %+v", e)
	}
	var data map[string]any
	if err := json.Unmarshal(e.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data["warning_id"] != w.ID.String() || data["bound"] != "MAX" || data["issued_at"] != "2026-09-01T14:26:40.000Z" ||
		data["predicted_excursion_at"] != "2026-09-01T14:29:15.000Z" || data["slope_celsius_per_minute"] != 0.62 || len(data) != 10 {
		t.Errorf("data = %v", data)
	}
	record, err := warning.Record(e, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	if err != nil || record.Topic != envelope.TopicForecasts || string(record.Key) != sscc ||
		stream.Header(record, stream.HeaderEventType) != warning.TypeIssued {
		t.Errorf("record = %+v, %v", record, err)
	}
}

func TestClosedEvent(t *testing.T) {
	w := sample()
	if _, err := warning.ClosedEvent(w, "IN_TRANSIT"); err == nil {
		t.Error("ClosedEvent() of an open warning succeeded")
	}
	closedAt, outcome, incidentID, lead := w.IssuedAt.Add(230*time.Second), warning.Breached, uuid.New(), 200
	w.ClosedAt, w.Outcome, w.IncidentID, w.LeadTimeSeconds = &closedAt, &outcome, &incidentID, &lead
	e, err := warning.ClosedEvent(w, "IN_TRANSIT")
	if err != nil {
		t.Fatalf("ClosedEvent() error = %v", err)
	}
	var data map[string]any
	if err := json.Unmarshal(e.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data["outcome"] != "BREACHED" || data["incident_id"] != incidentID.String() || data["lead_time_seconds"] != 200.0 ||
		data["closed_at"] != "2026-09-01T14:30:30.000Z" || len(data) != 6 || e.OccurredAt != "2026-09-01T14:30:30.000000Z" {
		t.Errorf("data = %v, event %+v", data, e)
	}
}
