//go:build integration

package reading_test

import (
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/veritrace-platform/telemetry-stream-service/internal/dbtest"
	"github.com/veritrace-platform/telemetry-stream-service/internal/reading"
)

func TestStore(t *testing.T) {
	db := dbtest.Start(t)
	store := reading.NewStore(db.App)
	start := time.Date(2026, 9, 1, 14, 30, 0, 0, time.UTC)
	at := func(sscc, device string, offset time.Duration, temperature float64) reading.Reading {
		return reading.Reading{
			DeviceID: device, SSCC: sscc, RecordedAt: start.Add(offset), ReceivedAt: start.Add(offset + time.Second),
			TemperatureCelsius: temperature, Latitude: 10.8705, Longitude: 106.8035,
		}
	}

	t.Run("inserts each reading once", func(t *testing.T) {
		const sscc = "089300010000000018"
		first := []reading.Reading{
			at(sscc, "D1", 0, 5.5),
			at(sscc, "D1", 5*time.Second, 6.25),
			at(sscc, "D1", 5*time.Second, 6.25), // delivered twice in the same batch
			at(sscc, "D2", 5*time.Second, 7),    // same time, other device
		}
		n, err := store.Insert(t.Context(), first)
		if err != nil || n != 3 {
			t.Fatalf("Insert() = %d, %v; want 3 new readings", n, err)
		}
		// A redelivered batch adds only what is new.
		n, err = store.Insert(t.Context(), append(first, at(sscc, "D1", 10*time.Second, 8)))
		if err != nil || n != 1 {
			t.Fatalf("Insert() again = %d, %v; want 1 new reading", n, err)
		}
		if got := db.Count(t, `SELECT count(*) FROM telemetry.sensor_readings WHERE sscc = $1`, sscc); got != 4 {
			t.Errorf("stored readings = %d, want 4", got)
		}
	})

	t.Run("stores every value", func(t *testing.T) {
		const sscc = "089345670000000017"
		humidity := 65.2
		r := at(sscc, "REEFER-0001", 0, -18.75)
		r.HumidityPercent = &humidity
		if _, err := store.Insert(t.Context(), []reading.Reading{r, at(sscc, "REEFER-0001", time.Minute, 2)}); err != nil {
			t.Fatalf("Insert() error = %v", err)
		}
		var (
			recordedAt, receivedAt time.Time
			deviceID               string
			temperature, lat, lng  float64
			storedHumidity         *float64
		)
		err := db.Owner.QueryRow(t.Context(), `
			SELECT recorded_at, received_at, device_id, temperature_celsius, humidity_percent, latitude, longitude
			FROM telemetry.sensor_readings WHERE sscc = $1 ORDER BY recorded_at LIMIT 1`, sscc,
		).Scan(&recordedAt, &receivedAt, &deviceID, &temperature, &storedHumidity, &lat, &lng)
		if err != nil {
			t.Fatalf("select: %v", err)
		}
		if !recordedAt.Equal(r.RecordedAt) || !receivedAt.Equal(r.ReceivedAt) || deviceID != r.DeviceID ||
			temperature != -18.75 || storedHumidity == nil || *storedHumidity != 65.2 || lat != 10.8705 || lng != 106.8035 {
			t.Errorf("stored %v %v %s %v %v %v %v", recordedAt, receivedAt, deviceID, temperature, storedHumidity, lat, lng)
		}
	})

	t.Run("a failing batch stores nothing", func(t *testing.T) {
		const sscc = "089300010000000025"
		bad := at(sscc, "D1", time.Second, 5)
		bad.DeviceID = "not valid" // the database checks the format too
		_, err := store.Insert(t.Context(), []reading.Reading{at(sscc, "D1", 0, 5), bad})
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.ConstraintName != "sensor_readings_device_id_check" {
			t.Fatalf("Insert() error = %v, want the device ID check", err)
		}
		if got := db.Count(t, `SELECT count(*) FROM telemetry.sensor_readings WHERE sscc = $1`, sscc); got != 0 {
			t.Errorf("stored readings = %d, want 0", got)
		}
		// The connection and its staging table stay usable.
		if n, err := store.Insert(t.Context(), []reading.Reading{at(sscc, "D1", 0, 5)}); err != nil || n != 1 {
			t.Errorf("Insert() after a failure = %d, %v", n, err)
		}
	})

	t.Run("feeds the 15-minute aggregate", func(t *testing.T) {
		const sscc = "089300010000000032"
		batch := []reading.Reading{
			at(sscc, "D1", 0, 4), at(sscc, "D1", 5*time.Minute, 6), at(sscc, "D1", 14*time.Minute, 11),
			at(sscc, "D1", 15*time.Minute, 3),
		}
		if _, err := store.Insert(t.Context(), batch); err != nil {
			t.Fatalf("Insert() error = %v", err)
		}
		// The runtime role reads the aggregate, which covers readings not yet materialized.
		rows, err := db.App.Query(t.Context(), `
			SELECT bucket, avg_temperature_celsius, min_temperature_celsius, max_temperature_celsius, reading_count
			FROM telemetry.sensor_readings_15m WHERE sscc = $1 ORDER BY bucket`, sscc)
		if err != nil {
			t.Fatalf("query aggregate: %v", err)
		}
		defer rows.Close()
		type bucket struct {
			start         time.Time
			avg, min, max float64
			count         int64
		}
		var got []bucket
		for rows.Next() {
			var b bucket
			if err := rows.Scan(&b.start, &b.avg, &b.min, &b.max, &b.count); err != nil {
				t.Fatalf("scan: %v", err)
			}
			got = append(got, b)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows: %v", err)
		}
		want := []bucket{{start, 7, 4, 11, 3}, {start.Add(15 * time.Minute), 3, 3, 3, 1}}
		if len(got) != len(want) {
			t.Fatalf("buckets = %+v, want %+v", got, want)
		}
		for i := range want {
			if !got[i].start.Equal(want[i].start) || got[i].avg != want[i].avg || got[i].min != want[i].min ||
				got[i].max != want[i].max || got[i].count != want[i].count {
				t.Errorf("bucket %d = %+v, want %+v", i, got[i], want[i])
			}
		}
	})

	t.Run("readings are append-only", func(t *testing.T) {
		for _, stmt := range []string{
			`UPDATE telemetry.sensor_readings SET temperature_celsius = 0`,
			`DELETE FROM telemetry.sensor_readings`,
			`TRUNCATE telemetry.sensor_readings`,
		} {
			_, err := db.App.Exec(t.Context(), stmt)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
				t.Errorf("%s as the runtime role: error = %v, want insufficient_privilege", stmt, err)
			}
		}
		// Views report that they cannot be written before privileges are checked, so ask for the grants.
		if n := db.Count(t, `SELECT count(*) FROM unnest(ARRAY['SELECT', 'INSERT', 'UPDATE', 'DELETE']) p
			WHERE has_table_privilege($1, 'telemetry.sensor_readings_15m', p) <> (p = 'SELECT')`, dbtest.AppRole); n != 0 {
			t.Errorf("the runtime role's privileges on the aggregate differ from SELECT only in %d cases", n)
		}
	})
}
