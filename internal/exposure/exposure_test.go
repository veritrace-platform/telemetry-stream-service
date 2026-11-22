package exposure_test

import (
	"encoding/json"
	"math"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/veritrace-platform/telemetry-stream-service/internal/exposure"
)

// vectors is testdata/thermal-exposure.json, a copy of veritrace/docs/contracts/test-vectors/thermal-exposure.json.
type vectors struct {
	Exposure []struct {
		Note           string  `json:"note"`
		MinTempCelsius float64 `json:"min_temp_celsius"`
		MaxTempCelsius float64 `json:"max_temp_celsius"`
		ShelfLifeQ10   float64 `json:"shelf_life_q10"`
		Readings       []struct {
			RecordedAt         time.Time `json:"recorded_at"`
			DeviceID           string    `json:"device_id"`
			TemperatureCelsius float64   `json:"temperature_celsius"`
		} `json:"readings"`
		Expected map[string]float64 `json:"expected"`
	} `json:"exposure"`
	ShelfLife []struct {
		Note              string    `json:"note"`
		ShelfLifeQ10      float64   `json:"shelf_life_q10"`
		ExtraAgingSeconds float64   `json:"extra_aging_seconds"`
		ProductionDate    string    `json:"production_date"`
		ExpirationDate    string    `json:"expiration_date"`
		AssessedAt        time.Time `json:"assessed_at"`
		Expected          struct {
			EstimatedExpiry         time.Time `json:"estimated_expiry"`
			EstimatedExpirationDate string    `json:"estimated_expiration_date"`
			ShelfLifeLossHours      float64   `json:"shelf_life_loss_hours"`
			ShelfLifeLossPercent    float64   `json:"shelf_life_loss_percent"`
			RemainingShelfLifeHours float64   `json:"remaining_shelf_life_hours"`
			Status                  string    `json:"status"`
		} `json:"expected"`
	} `json:"shelf_life"`
}

func load(t *testing.T) vectors {
	t.Helper()
	raw, err := os.ReadFile("testdata/thermal-exposure.json")
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var v vectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("decode vectors: %v", err)
	}
	if len(v.Exposure) == 0 || len(v.ShelfLife) == 0 {
		t.Fatal("no vectors")
	}
	return v
}

func date(t *testing.T, s string) time.Time {
	t.Helper()
	d, err := time.Parse(time.DateOnly, s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func near(a, b float64) bool {
	return math.Abs(a-b) <= 1e-6
}

func TestShelfLifeVectors(t *testing.T) {
	for _, vec := range load(t).ShelfLife {
		extra := time.Duration(vec.ExtraAgingSeconds * float64(time.Second))
		got := exposure.Assess(extra, date(t, vec.ProductionDate), date(t, vec.ExpirationDate), vec.ShelfLifeQ10, vec.AssessedAt)
		want := vec.Expected
		if got.EstimatedExpiry.Sub(want.EstimatedExpiry).Abs() > time.Millisecond ||
			got.EstimatedExpirationDate.Format(time.DateOnly) != want.EstimatedExpirationDate ||
			!near(got.ExtraAging.Hours(), want.ShelfLifeLossHours) || !near(got.LossPercent, want.ShelfLifeLossPercent) ||
			!near(got.Remaining.Hours(), want.RemainingShelfLifeHours) || string(got.Status) != want.Status {
			t.Errorf("%s:\n got %+v\nwant %+v", vec.Note, got, want)
		}
	}
}

func TestMeanKineticTemperatureOfAConstant(t *testing.T) {
	// A constant temperature is its own MKT.
	for _, c := range []float64{-20, 4.5, 25} {
		kinetic := 60 * math.Exp(-exposure.ActivationEnergyOverR/(c+273.15))
		if got := exposure.MeanKineticTemperature(kinetic, 60); !near(got, c) {
			t.Errorf("MKT of a constant %v °C = %v", c, got)
		}
	}
}

func TestAlertEvent(t *testing.T) {
	production, expiration := date(t, "2026-09-01"), date(t, "2026-09-14")
	assessed := time.Date(2026, 9, 1, 17, 30, 0, 0, time.UTC)
	sl := exposure.Assess(11*time.Hour, production, expiration, 3, assessed)
	s := exposure.Shipment{SSCC: "089300010000000018", ShipmentID: uuid.New(), Status: "IN_TRANSIT"}
	e, err := exposure.AlertEvent(s, sl, assessed)
	if err != nil {
		t.Fatalf("AlertEvent() error = %v", err)
	}
	later, _ := exposure.AlertEvent(s, sl, assessed.Add(time.Hour))
	if e.EventID != later.EventID || e.EventID.Version() != 8 {
		t.Errorf("event IDs %s and %s are not derived from the SSCC and status alone", e.EventID, later.EventID)
	}
	var data map[string]any
	if err := json.Unmarshal(e.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data["status"] != "REDUCED" || data["estimated_expiration_date"] != "2026-09-14" ||
		data["label_expiration_date"] != "2026-09-14" || data["shelf_life_loss_hours"] != 11.0 ||
		data["shelf_life_loss_percent"] != 3.27 || data["assessed_at"] != "2026-09-01T17:30:00.000Z" || len(data) != 10 {
		t.Errorf("data = %v", data)
	}
	if e.EventType != exposure.TypeShelfLifeReduced || e.OccurredAt != "2026-09-01T17:30:00.000000Z" ||
		e.Subject.SSCC != s.SSCC || e.Producer != "telemetry-stream-service" {
		t.Errorf("event = %+v", e)
	}
}
