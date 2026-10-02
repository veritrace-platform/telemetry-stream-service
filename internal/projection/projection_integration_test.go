//go:build integration

package projection_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/veritrace-platform/telemetry-stream-service/internal/dbtest"
	"github.com/veritrace-platform/telemetry-stream-service/internal/incident"
	"github.com/veritrace-platform/telemetry-stream-service/internal/projection"
	"github.com/veritrace-platform/telemetry-stream-service/internal/reading"
)

// vectorEvents returns the envelopes of testdata/shipment-events.json, a copy of
// veritrace/docs/contracts/test-vectors/shipment-events.json: a shipment that is created, gets a driver, is
// picked up, and is recalled.
func vectorEvents(t *testing.T) []map[string]any {
	t.Helper()
	raw, err := os.ReadFile("testdata/shipment-events.json")
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var vectors struct {
		Events []struct {
			Envelope map[string]any `json:"envelope"`
		} `json:"events"`
	}
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatalf("decode vectors: %v", err)
	}
	envelopes := make([]map[string]any, len(vectors.Events))
	for i, e := range vectors.Events {
		envelopes[i] = e.Envelope
	}
	return envelopes
}

// record wraps an envelope as the outbox relay publishes it.
func record(t *testing.T, envelope map[string]any) *kgo.Record {
	t.Helper()
	value, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("encode envelope: %v", err)
	}
	sscc, _ := envelope["subject"].(map[string]any)["sscc"].(string)
	return &kgo.Record{Topic: projection.Topic, Key: []byte(sscc), Value: value}
}

// rekey returns a copy of an envelope for another shipment.
func rekey(envelope map[string]any, shipmentID uuid.UUID, sscc string) map[string]any {
	clone := map[string]any{}
	raw, _ := json.Marshal(envelope)
	_ = json.Unmarshal(raw, &clone)
	clone["subject"].(map[string]any)["shipment_id"] = shipmentID.String()
	clone["subject"].(map[string]any)["sscc"] = sscc
	return clone
}

func TestProjector(t *testing.T) {
	db := dbtest.Start(t)
	producer := &recorder{}
	projector := projection.NewProjector(db.App, producer, slog.New(slog.DiscardHandler))
	store := projection.NewStore(db.App)
	events := vectorEvents(t)
	owner := uuid.MustParse("0192f7a4-7c3e-7d2a-9b1e-3f4a5b6c7d8e")
	carrier := uuid.MustParse("0192f7a4-7c3e-7d2a-9b1e-3f4a5b6c7d93")
	consignee := uuid.MustParse("0192f7a4-7c3e-7d2a-9b1e-3f4a5b6c7da3")
	driver := uuid.MustParse("0192f7a4-7c3e-7d2a-9b1e-3f4a5b6c7da2")

	handle := func(t *testing.T, envelopes ...map[string]any) {
		t.Helper()
		records := make([]*kgo.Record, len(envelopes))
		for i, e := range envelopes {
			records[i] = record(t, e)
		}
		if err := projector.Handle(t.Context(), records); err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
	}
	get := func(t *testing.T, sscc string) projection.Shipment {
		t.Helper()
		s, err := store.Get(t.Context(), sscc)
		if err != nil {
			t.Fatalf("Get(%s) error = %v", sscc, err)
		}
		return s
	}

	t.Run("follows the shipment's events", func(t *testing.T) {
		const sscc = "089300010000000018"
		handle(t, events[0])
		s := get(t, sscc)
		want := projection.Shipment{
			SSCC: sscc, ShipmentID: uuid.MustParse("0192f7a4-7c3e-7d2a-9b1e-3f4a5b6c7da0"), OwnerTenantID: owner,
			ParticipantTenantIDs: []uuid.UUID{owner, carrier, consignee}, Status: projection.StatusCreated,
			GTIN: "08930001000018", ProductName: "Fresh milk 1 L", LotNumber: "L2026-09-30A", MinTempCelsius: 2,
			MaxTempCelsius: 6, LastEventSequence: 1,
		}
		assertShipment(t, s, want)
		if !s.Monitored() {
			t.Error("a created shipment is not monitored")
		}

		handle(t, events[1], events[2])
		want.AssignedDriverID, want.Status, want.LastEventSequence = &driver, projection.StatusInTransit, 3
		assertShipment(t, get(t, sscc), want)

		handle(t, events[3])
		want.Status, want.LastEventSequence = projection.StatusRecalled, 4
		s = get(t, sscc)
		assertShipment(t, s, want)
		if s.Monitored() {
			t.Error("a recalled shipment is monitored")
		}

		// Redelivered events change nothing, not even the status.
		handle(t, events...)
		assertShipment(t, get(t, sscc), want)
	})

	t.Run("a new carrier joins and ends the driver assignment", func(t *testing.T) {
		const sscc = "089300010000000025"
		id := uuid.New()
		created := rekey(events[0], id, sscc)
		data := created["data"].(map[string]any)
		data["participants"] = []any{
			map[string]any{"tenant_id": owner.String(), "role": "OWNER"},
			map[string]any{"tenant_id": owner.String(), "role": "CARRIER"},
			map[string]any{"tenant_id": consignee.String(), "role": "CONSIGNEE"},
		}
		data["assigned_driver_id"] = driver.String()
		external := uuid.New()
		joined := rekey(events[1], id, sscc)
		joined["event_type"], joined["sequence"] = "shipment.participant_added", 2
		joined["data"] = map[string]any{"tenant_id": external.String(), "role": "CARRIER"}
		inspector := rekey(events[1], id, sscc)
		inspector["event_type"], inspector["sequence"] = "shipment.participant_added", 3
		inspector["data"] = map[string]any{"tenant_id": consignee.String(), "role": "INSPECTOR"}

		handle(t, created)
		if s := get(t, sscc); s.AssignedDriverID == nil || *s.AssignedDriverID != driver ||
			!slices.Equal(s.ParticipantTenantIDs, []uuid.UUID{owner, consignee}) {
			t.Fatalf("created = %+v", s)
		}
		handle(t, joined, inspector)
		s := get(t, sscc)
		if s.AssignedDriverID != nil || !slices.Equal(s.ParticipantTenantIDs, []uuid.UUID{owner, consignee, external}) ||
			s.LastEventSequence != 3 {
			t.Errorf("after the carrier and an inspector joined = %+v", s)
		}
	})

	t.Run("skips events it cannot apply and applies the rest", func(t *testing.T) {
		const sscc, refused = "089300010000000032", "089300010000000049"
		id := uuid.New()
		otherVersion := rekey(events[1], id, sscc)
		otherVersion["event_version"], otherVersion["sequence"] = 2, 2
		badDriver := rekey(events[1], id, sscc)
		badDriver["data"] = map[string]any{"driver_user_id": "nobody"}
		badSSCC := rekey(events[2], id, "089300010000000033")
		unknownStatus := rekey(events[2], id, sscc)
		unknownStatus["subject"].(map[string]any)["status"] = "LOST"
		// The database refuses bounds that core never produces.
		badBounds := rekey(events[0], uuid.New(), refused)
		badBounds["data"].(map[string]any)["min_temp_celsius"] = 9
		unknownShipment := rekey(events[2], uuid.New(), "089300010000000056")

		records := []*kgo.Record{
			record(t, rekey(events[0], id, sscc)),
			{Topic: projection.Topic, Key: []byte(sscc), Value: []byte(`{"event_type":`)},
			record(t, otherVersion), record(t, badDriver), record(t, badSSCC), record(t, unknownStatus),
			record(t, badBounds), record(t, unknownShipment),
			record(t, rekey(events[2], id, sscc)),
		}
		if err := projector.Handle(t.Context(), records); err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if s := get(t, sscc); s.Status != projection.StatusInTransit || s.LastEventSequence != 3 ||
			s.AssignedDriverID != nil {
			t.Errorf("projection = %+v, want only the created and pickup events applied", s)
		}
		if _, err := store.Get(t.Context(), refused); !errors.Is(err, projection.ErrNotFound) {
			t.Errorf("Get(refused) error = %v, want ErrNotFound", err)
		}
	})

	t.Run("an open incident is resolved when the shipment stops being monitored", func(t *testing.T) {
		const sscc = "089300010000000070"
		id := uuid.New()
		created := rekey(events[0], id, sscc)
		handle(t, created, rekey(events[1], id, sscc), rekey(events[2], id, sscc))

		start := time.Date(2026, 10, 2, 8, 30, 0, 0, time.UTC)
		var readings []reading.Reading
		for s := 0; s <= 60; s += 5 {
			readings = append(readings, reading.Reading{
				DeviceID: "REEFER-0001", SSCC: sscc, RecordedAt: start.Add(time.Duration(s) * time.Second),
				ReceivedAt: start.Add(time.Duration(s) * time.Second), TemperatureCelsius: 9, Latitude: 10.8, Longitude: 106.7,
			})
		}
		if _, err := reading.NewStore(db.App).Insert(t.Context(), readings); err != nil {
			t.Fatal(err)
		}
		open := incident.Incident{
			ID: incident.NewID(sscc, start), ShipmentID: id, SSCC: sscc, DeviceID: "REEFER-0001", StartedAt: start,
			ConfirmedAt: start.Add(30 * time.Second), MinTempCelsius: 2, MaxTempCelsius: 6, TriggerTemperatureCelsius: 9,
			ExtremeTemperatureCelsius: 9, Latitude: 10.8, Longitude: 106.7,
		}
		open.IncidentHash, _ = open.Confirmation().Hash()
		if _, err := incident.NewStore(db.Owner).Insert(t.Context(), open); err != nil {
			t.Fatal(err)
		}

		// Delivered 50 s after the excursion started; the readings after that are not the incident's.
		delivered := rekey(events[2], id, sscc)
		delivered["event_type"], delivered["sequence"] = "shipment.delivery_confirmed", 4
		delivered["occurred_at"] = start.Add(52 * time.Second).Format("2006-01-02T15:04:05.000000Z")
		delivered["subject"].(map[string]any)["status"] = projection.StatusDelivered
		handle(t, delivered)

		closed, _, err := incident.NewStore(db.Owner).Latest(t.Context(), sscc)
		if err != nil || closed.EndedAt == nil || !closed.EndedAt.Equal(start.Add(50*time.Second)) || *closed.DurationSeconds != 50 {
			t.Fatalf("incident = %+v, %v; want it ended at the last reading before delivery", closed, err)
		}
		if len(producer.records) != 1 {
			t.Fatalf("events = %d, want 1", len(producer.records))
		}
		e, err := incident.DecodeEvent(producer.records[0].Value)
		if err != nil || e.EventType != incident.TypeBreachResolved || e.Subject.Status != projection.StatusDelivered {
			t.Errorf("event = %+v, %v", e, err)
		}

		// A redelivered event, and later events of the finished shipment, change nothing.
		recalled := rekey(events[3], id, sscc)
		recalled["sequence"] = 5
		handle(t, delivered, recalled)
		if len(producer.records) != 1 {
			t.Errorf("events = %d, want still 1", len(producer.records))
		}
	})

	t.Run("an event without occurred_at is skipped", func(t *testing.T) {
		const sscc = "089300010000000087"
		created := rekey(events[0], uuid.New(), sscc)
		delete(created, "occurred_at")
		handle(t, created)
		if _, err := store.Get(t.Context(), sscc); !errors.Is(err, projection.ErrNotFound) {
			t.Errorf("Get() error = %v, want ErrNotFound", err)
		}
	})

	t.Run("the runtime role cannot delete shipments", func(t *testing.T) {
		if _, err := db.App.Exec(t.Context(), `DELETE FROM telemetry.shipment_projection`); err == nil {
			t.Error("the runtime role deleted from the projection")
		}
	})
}

// recorder records the records it is asked to produce.
type recorder struct {
	records []*kgo.Record
}

func (r *recorder) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	r.records = append(r.records, records...)
	results := make(kgo.ProduceResults, len(records))
	for i, rec := range records {
		results[i] = kgo.ProduceResult{Record: rec}
	}
	return results
}

func assertShipment(t *testing.T, got, want projection.Shipment) {
	t.Helper()
	driver := func(id *uuid.UUID) string {
		if id == nil {
			return "none"
		}
		return id.String()
	}
	if got.SSCC != want.SSCC || got.ShipmentID != want.ShipmentID || got.OwnerTenantID != want.OwnerTenantID ||
		!slices.Equal(got.ParticipantTenantIDs, want.ParticipantTenantIDs) ||
		driver(got.AssignedDriverID) != driver(want.AssignedDriverID) || got.Status != want.Status ||
		got.GTIN != want.GTIN || got.ProductName != want.ProductName || got.LotNumber != want.LotNumber ||
		got.MinTempCelsius != want.MinTempCelsius || got.MaxTempCelsius != want.MaxTempCelsius ||
		got.LastEventSequence != want.LastEventSequence || got.UpdatedAt.IsZero() {
		t.Errorf("shipment = %+v (driver %s), want %+v (driver %s)", got, driver(got.AssignedDriverID), want,
			driver(want.AssignedDriverID))
	}
}
