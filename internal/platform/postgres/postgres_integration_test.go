//go:build integration

package postgres_test

import (
	"testing"
	"time"

	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/postgres"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/postgres/postgrestest"
	"github.com/veritrace-platform/telemetry-stream-service/migrations"
)

func TestNewPoolScansTimestampsInUTC(t *testing.T) {
	database := postgrestest.Start(t).CreateDatabase(t, migrations.Database, migrations.Database+"_owner",
		migrations.Database+"_app")
	pool, err := postgres.NewPool(t.Context(), database.AppURL, "postgres-test")
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	defer pool.Close()

	var got time.Time
	if err := pool.QueryRow(t.Context(), `SELECT '2026-09-30 21:30:00+07'::timestamptz`).Scan(&got); err != nil {
		t.Fatalf("scan timestamptz: %v", err)
	}
	want := time.Date(2026, 9, 30, 14, 30, 0, 0, time.UTC)
	if got.Location() != time.UTC || !got.Equal(want) {
		t.Errorf("scanned %v (%v), want %v in UTC", got, got.Location(), want)
	}
}
