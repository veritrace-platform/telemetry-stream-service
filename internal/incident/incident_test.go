package incident_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/veritrace-platform/telemetry-stream-service/internal/incident"
	"github.com/veritrace-platform/telemetry-stream-service/internal/stream"
)

// vectors is testdata/incident-hash.json, a copy of veritrace/docs/contracts/test-vectors/incident-hash.json.
type vectors struct {
	Incidents []struct {
		Note         string                `json:"note"`
		Confirmation incident.Confirmation `json:"confirmation"`
		HashInput    string                `json:"hash_input"`
		IncidentHash string                `json:"incident_hash"`
	} `json:"incidents"`
}

func TestHashVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/incident-hash.json")
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var v vectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("decode vectors: %v", err)
	}
	if len(v.Incidents) == 0 {
		t.Fatal("no vectors")
	}
	for _, vec := range v.Incidents {
		canonical, err := vec.Confirmation.Canonical()
		if err != nil {
			t.Fatalf("%s: Canonical() error = %v", vec.Note, err)
		}
		if string(canonical) != vec.HashInput {
			t.Errorf("%s: Canonical() =\n%s\nwant\n%s", vec.Note, canonical, vec.HashInput)
		}
		hash, err := vec.Confirmation.Hash()
		if err != nil || hash != vec.IncidentHash {
			t.Errorf("%s: Hash() = %s, %v; want %s", vec.Note, hash, err, vec.IncidentHash)
		}
		// The hash never covers itself.
		vec.Confirmation.IncidentHash = vec.IncidentHash
		if again, _ := vec.Confirmation.Hash(); again != vec.IncidentHash {
			t.Errorf("%s: Hash() with incident_hash set = %s", vec.Note, again)
		}
	}
}

func sample() incident.Incident {
	start := time.Date(2026, 9, 1, 14, 30, 0, 0, time.UTC)
	return incident.Incident{
		ID: incident.NewID("089300010000000018", start), ShipmentID: uuid.MustParse("0192f7a4-7c3e-7d2a-9b1e-3f4a5b6c7da0"),
		SSCC: "089300010000000018", DeviceID: "REEFER-0001", StartedAt: start, ConfirmedAt: start.Add(30 * time.Second),
		MinTempCelsius: 2, MaxTempCelsius: 8, TriggerTemperatureCelsius: 9.4, ExtremeTemperatureCelsius: 9.4,
		Latitude: 10.8705, Longitude: 106.8035,
	}
}

func TestNewIDIsDerivedFromTheEpisode(t *testing.T) {
	start := time.Date(2026, 9, 1, 14, 30, 0, 123_000_000, time.UTC)
	id := incident.NewID("089300010000000018", start)
	if id != incident.NewID("089300010000000018", start.In(time.FixedZone("ICT", 7*3600))) {
		t.Error("NewID() depends on the time zone")
	}
	if id == incident.NewID("089345670000000017", start) || id == incident.NewID("089300010000000018", start.Add(time.Millisecond)) {
		t.Error("NewID() is the same for different episodes")
	}
	if id.Version() != 7 || id.Variant() != uuid.RFC4122 {
		t.Errorf("NewID() = %s, version %d, variant %s", id, id.Version(), id.Variant())
	}
	if ms, _ := id.Time().UnixTime(); ms != start.Unix() {
		t.Errorf("NewID() time = %d, want %d", ms, start.Unix())
	}
	// Later episodes have greater IDs, so IDs sort by start.
	if later := incident.NewID("089300010000000018", start.Add(time.Second)); strings.Compare(later.String(), id.String()) <= 0 {
		t.Errorf("NewID() of a later episode %s sorts before %s", later, id)
	}
}

func TestConfirmedEvent(t *testing.T) {
	i := sample()
	hash, err := i.Confirmation().Hash()
	if err != nil {
		t.Fatal(err)
	}
	i.IncidentHash = hash
	e, err := incident.ConfirmedEvent(i, "IN_TRANSIT")
	if err != nil {
		t.Fatalf("ConfirmedEvent() error = %v", err)
	}
	again, _ := incident.ConfirmedEvent(i, "IN_TRANSIT")
	if e.EventID != again.EventID || e.EventID.Version() != 7 {
		t.Errorf("event ID %s is not derived from the incident", e.EventID)
	}
	record, err := e.Record("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	if err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	if record.Topic != incident.Topic || string(record.Key) != i.SSCC ||
		stream.Header(record, stream.HeaderEventType) != incident.TypeBreachConfirmed ||
		stream.Header(record, stream.HeaderContentType) != stream.ContentTypeJSON {
		t.Errorf("record = %+v", record)
	}
	var envelope map[string]any
	if err := json.Unmarshal(record.Value, &envelope); err != nil {
		t.Fatal(err)
	}
	// messaging.md §2.1 and §5: no actor, no hash chain, and the producer.
	if envelope["actor"] != nil || envelope["producer"] != "telemetry-stream-service" || envelope["event_version"] != 1.0 ||
		envelope["occurred_at"] != "2026-09-01T14:30:30.000000Z" || envelope["sequence"] != nil {
		t.Errorf("envelope = %v", envelope)
	}
	data := envelope["data"].(map[string]any)
	if data["incident_hash"] != hash || data["started_at"] != "2026-09-01T14:30:00.000Z" ||
		data["temperature_celsius"] != 9.4 || len(data) != 12 {
		t.Errorf("data = %v", data)
	}
	if subject := envelope["subject"].(map[string]any); subject["status"] != "IN_TRANSIT" || subject["sscc"] != i.SSCC {
		t.Errorf("subject = %v", subject)
	}
	decoded, err := incident.DecodeEvent(record.Value)
	if err != nil || decoded.EventID != e.EventID {
		t.Errorf("DecodeEvent() = %+v, %v", decoded, err)
	}
}

func TestResolvedEvent(t *testing.T) {
	i := sample()
	if _, err := incident.ResolvedEvent(i, "IN_TRANSIT"); err == nil {
		t.Error("ResolvedEvent() of an open incident succeeded")
	}
	end := i.StartedAt.Add(370*time.Second + 400*time.Millisecond)
	duration := incident.Duration(i.StartedAt, end)
	i.EndedAt, i.DurationSeconds, i.ExtremeTemperatureCelsius = &end, &duration, 11.2
	e, err := incident.ResolvedEvent(i, "DELIVERED")
	if err != nil {
		t.Fatalf("ResolvedEvent() error = %v", err)
	}
	var data map[string]any
	if err := json.Unmarshal(e.Data, &data); err != nil {
		t.Fatal(err)
	}
	if duration != 370 || data["ended_at"] != "2026-09-01T14:36:10.400Z" || data["duration_seconds"] != 370.0 ||
		data["extreme_temperature_celsius"] != 11.2 || data["incident_id"] != i.ID.String() || len(data) != 5 {
		t.Errorf("data = %v", data)
	}
	if e.OccurredAt != "2026-09-01T14:36:10.400000Z" || e.Subject.Status != "DELIVERED" {
		t.Errorf("event = %+v", e)
	}
	if _, err := incident.DecodeEvent([]byte(`{"event_type":"shipment.created"}`)); err == nil {
		t.Error("DecodeEvent() accepted another event type")
	}
}
