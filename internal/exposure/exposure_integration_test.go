//go:build integration

package exposure_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/veritrace-platform/telemetry-stream-service/internal/dbtest"
	"github.com/veritrace-platform/telemetry-stream-service/internal/exposure"
	"github.com/veritrace-platform/telemetry-stream-service/internal/gs1"
	"github.com/veritrace-platform/telemetry-stream-service/internal/projection"
)

// sscc returns the nth fixture SSCC of this test.
func sscc(n int) string {
	payload := fmt.Sprintf("08930001%09d", 600000+n)
	return payload + fmt.Sprint(gs1.CheckDigit(payload))
}

func insertReading(t *testing.T, db *dbtest.Database, code, device string, at time.Time, celsius float64) {
	t.Helper()
	if _, err := db.Owner.Exec(t.Context(), `
		INSERT INTO telemetry.sensor_readings (recorded_at, sscc, device_id, temperature_celsius, latitude, longitude, received_at)
		VALUES ($1, $2, $3, $4, 10.87, 106.8, $1)`, at, code, device, celsius); err != nil {
		t.Fatalf("insert reading: %v", err)
	}
}

func TestMeasureVectors(t *testing.T) {
	db := dbtest.Start(t)
	store := projection.NewStore(db.App)
	for i, vec := range load(t).Exposure {
		code := sscc(i)
		q10 := vec.ShelfLifeQ10
		db.Project(t, dbtest.Shipment{SSCC: code, MinTempCelsius: vec.MinTempCelsius, MaxTempCelsius: vec.MaxTempCelsius, ShelfLifeQ10: &q10})
		var last time.Time
		for _, r := range vec.Readings {
			insertReading(t, db, code, r.DeviceID, r.RecordedAt, r.TemperatureCelsius)
			last = r.RecordedAt
		}
		s, err := store.Get(t.Context(), code)
		if err != nil {
			t.Fatal(err)
		}
		got, err := exposure.Measure(t.Context(), db.App, s.Exposure(), last)
		if err != nil {
			t.Fatalf("%s: Measure() error = %v", vec.Note, err)
		}
		want := vec.Expected
		values := map[string]float64{
			"reading_count": float64(got.ReadingCount), "covered_seconds": got.CoveredSeconds,
			"uncovered_seconds": got.UncoveredSeconds, "mean_temperature_celsius": *got.MeanTemperatureCelsius,
			"mean_kinetic_temperature_celsius": *got.MeanKineticTemperatureCelsius,
			"lowest_temperature_celsius":       *got.LowestTemperatureCelsius,
			"highest_temperature_celsius":      *got.HighestTemperatureCelsius,
			"time_above_max_seconds":           got.TimeAboveMaxSeconds, "time_below_min_seconds": got.TimeBelowMinSeconds,
			"degree_minutes_above_max": got.DegreeMinutesAboveMax, "degree_minutes_below_min": got.DegreeMinutesBelowMin,
			"extra_aging_seconds": got.ExtraAging.Seconds(),
		}
		for key, w := range want {
			if g, ok := values[key]; !ok || !near(g, w) {
				t.Errorf("%s: %s = %v, want %v", vec.Note, key, g, w)
			}
		}
		if got.ShelfLife != nil {
			t.Errorf("%s: a shelf-life estimate without the lot's dates", vec.Note)
		}
		// A reading after the assessment time is not counted.
		before, err := exposure.Measure(t.Context(), db.App, s.Exposure(), vec.Readings[0].RecordedAt)
		if err != nil || before.ReadingCount != 1 || before.CoveredSeconds != 0 || before.MeanTemperatureCelsius != nil {
			t.Errorf("%s: as of the first reading = %+v, %v", vec.Note, before, err)
		}
	}

	t.Run("no readings", func(t *testing.T) {
		code := sscc(90)
		db.Project(t, dbtest.Shipment{SSCC: code})
		s, _ := store.Get(t.Context(), code)
		got, err := exposure.Measure(t.Context(), db.App, s.Exposure(), time.Now())
		if err != nil || got.ReadingCount != 0 || got.FirstReadingAt != nil || got.MeanKineticTemperatureCelsius != nil {
			t.Errorf("Measure() = %+v, %v", got, err)
		}
	})
}

func TestReassessRaisesTheStatusOnce(t *testing.T) {
	db := dbtest.Start(t)
	code := sscc(100)
	production, expiration, q10 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC), 3.0
	db.Project(t, dbtest.Shipment{SSCC: code, ProductionDate: &production, ExpirationDate: &expiration, ShelfLifeQ10: &q10})
	store := projection.NewStore(db.App)
	start := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	// Two hours at 18 °C, a reading every 15 s: 2 h × (3 − 1) = 4 h of extra aging, 5.6 % of three days.
	for i := range 2 * 60 * 4 {
		insertReading(t, db, code, "SIM-1", start.Add(time.Duration(i)*15*time.Second), 18)
	}
	end := start.Add(2 * time.Hour)

	reassess := func(asOf time.Time) string {
		t.Helper()
		s, err := store.Get(t.Context(), code)
		if err != nil {
			t.Fatal(err)
		}
		tx, err := db.App.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(t.Context()) }()
		e, err := exposure.Reassess(t.Context(), tx, s.Exposure(), asOf)
		if err != nil {
			t.Fatalf("Reassess() error = %v", err)
		}
		if err := tx.Commit(t.Context()); err != nil {
			t.Fatal(err)
		}
		if e == nil {
			return ""
		}
		return e.EventType + " " + string(e.Data)
	}

	if got := reassess(start.Add(10 * time.Minute)); got != "" {
		t.Errorf("after ten minutes: %s", got)
	}
	got := reassess(end)
	if got == "" || !strings.Contains(got, `"status":"REDUCED"`) {
		t.Fatalf("after two hours: %q, want REDUCED", got)
	}
	if again := reassess(end); again != "" {
		t.Errorf("REDUCED again: %s", again)
	}
	// Past the estimated expiry, the status becomes EXPIRED, once.
	if got := reassess(expiration.Add(23 * time.Hour)); !strings.Contains(got, `"status":"EXPIRED"`) {
		t.Errorf("past the expiry: %q, want EXPIRED", got)
	}
	if status := db.Count(t, `SELECT count(*) FROM telemetry.shipment_projection WHERE sscc = $1 AND shelf_life_status = 'EXPIRED'`, code); status != 1 {
		t.Errorf("shelf_life_status is not EXPIRED")
	}

	t.Run("without the lot's dates, nothing is assessed", func(t *testing.T) {
		other := sscc(101)
		db.Project(t, dbtest.Shipment{SSCC: other})
		insertReading(t, db, other, "SIM-1", start, 30)
		s, _ := store.Get(t.Context(), other)
		tx, _ := db.App.Begin(t.Context())
		defer func() { _ = tx.Rollback(t.Context()) }()
		if e, err := exposure.Reassess(t.Context(), tx, s.Exposure(), start.Add(time.Hour)); e != nil || err != nil {
			t.Errorf("Reassess() = %v, %v", e, err)
		}
	})
}
