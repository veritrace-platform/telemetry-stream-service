//go:build integration

package migrations_test

import (
	"log/slog"
	"testing"

	"github.com/veritrace-platform/telemetry-stream-service/internal/dbtest"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/migrate"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/postgres/postgrestest"
	"github.com/veritrace-platform/telemetry-stream-service/migrations"
)

// TestRoundTrip rolls every migration back and applies them again, so that each Down section undoes its Up section.
func TestRoundTrip(t *testing.T) {
	database := postgrestest.Start(t).CreateDatabase(t, migrations.Database, dbtest.OwnerRole, dbtest.AppRole,
		migrations.RequiredExtensions...)
	runner, err := migrate.Open(database.OwnerURL, migrations.FS, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("open migrations: %v", err)
	}
	defer func() { _ = runner.Close() }()
	ctx := t.Context()
	if err := runner.Up(ctx); err != nil {
		t.Fatalf("up: %v", err)
	}
	for {
		version, err := runner.Version(ctx)
		if err != nil {
			t.Fatalf("version: %v", err)
		}
		if version == 0 {
			break
		}
		if err := runner.Down(ctx); err != nil {
			t.Fatalf("down from version %d: %v", version, err)
		}
	}
	if err := runner.Up(ctx); err != nil {
		t.Fatalf("up again: %v", err)
	}
}

// TestTimescaleObjects checks the hypertable, its compression, and the background jobs that maintain it.
func TestTimescaleObjects(t *testing.T) {
	db := dbtest.Start(t)
	checks := map[string]string{
		"hypertable with one-day chunks": `SELECT count(*) FROM timescaledb_information.dimensions
			WHERE hypertable_schema = 'telemetry' AND hypertable_name = 'sensor_readings'
			  AND column_name = 'recorded_at' AND time_interval = INTERVAL '1 day'`,
		"compression segmented by SSCC": `SELECT count(*) FROM timescaledb_information.compression_settings
			WHERE hypertable_schema = 'telemetry' AND hypertable_name = 'sensor_readings'
			  AND attname = 'sscc' AND segmentby_column_index = 1`,
		"compression after 7 days": `SELECT count(*) FROM timescaledb_information.jobs
			WHERE proc_name = 'policy_compression' AND hypertable_name = 'sensor_readings'
			  AND config->>'compress_after' = '7 days'`,
		"15-minute aggregate refreshed over two days": `SELECT count(*) FROM timescaledb_information.jobs
			WHERE proc_name = 'policy_refresh_continuous_aggregate' AND config->>'start_offset' = '2 days'
			  AND config->>'end_offset' = '00:15:00'`,
		"15-minute aggregate includes recent readings": `SELECT count(*) FROM timescaledb_information.continuous_aggregates
			WHERE view_schema = 'telemetry' AND view_name = 'sensor_readings_15m' AND NOT materialized_only`,
	}
	for name, query := range checks {
		if n := db.Count(t, query); n != 1 {
			t.Errorf("%s: %d matching objects, want 1", name, n)
		}
	}
}
