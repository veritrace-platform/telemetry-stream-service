package reading

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store writes readings to telemetry.sensor_readings.
//
// It uses pgx directly rather than generated queries: the staging table is temporary, so it is not part of the
// schema that sqlc reads.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore returns a store on the runtime role's pool.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

var columns = []string{
	"recorded_at", "sscc", "device_id", "temperature_celsius", "humidity_percent", "latitude", "longitude", "received_at",
}

// Insert stores the readings that are not stored yet and returns how many were new. The batch is copied into a
// staging table, from which one statement inserts it and skips duplicates (ADR-0012). Either every reading of the
// batch is stored or none is.
func (s *Store) Insert(ctx context.Context, readings []Reading) (int64, error) {
	if len(readings) == 0 {
		return 0, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// The staging table lives as long as the connection, and commits empty it.
	if _, err := tx.Exec(ctx,
		`CREATE TEMP TABLE IF NOT EXISTS reading_staging (LIKE telemetry.sensor_readings) ON COMMIT DELETE ROWS`,
	); err != nil {
		return 0, fmt.Errorf("create staging table: %w", err)
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"reading_staging"}, columns,
		pgx.CopyFromSlice(len(readings), func(i int) ([]any, error) {
			r := readings[i]
			return []any{r.RecordedAt, r.SSCC, r.DeviceID, r.TemperatureCelsius, r.HumidityPercent, r.Latitude,
				r.Longitude, r.ReceivedAt}, nil
		}),
	); err != nil {
		return 0, fmt.Errorf("copy readings: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO telemetry.sensor_readings (
			recorded_at, sscc, device_id, temperature_celsius, humidity_percent, latitude, longitude, received_at)
		SELECT recorded_at, sscc, device_id, temperature_celsius, humidity_percent, latitude, longitude, received_at
		FROM reading_staging
		ON CONFLICT DO NOTHING`)
	if err != nil {
		return 0, fmt.Errorf("insert readings: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return tag.RowsAffected(), nil
}
