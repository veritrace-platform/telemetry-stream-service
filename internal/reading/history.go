package reading

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/veritrace-platform/telemetry-stream-service/internal/reading/queries"
)

// Between returns the stored readings of an SSCC recorded in [from, before), oldest first.
func Between(ctx context.Context, db queries.DBTX, sscc string, from, before time.Time) ([]Reading, error) {
	rows, err := queries.New(db).ReadingsBetween(ctx, queries.ReadingsBetweenParams{Sscc: sscc, FromTime: from, Before: before})
	if err != nil {
		return nil, fmt.Errorf("read readings: %w", err)
	}
	readings := make([]Reading, len(rows))
	for i, r := range rows {
		readings[i] = Reading{
			DeviceID: r.DeviceID, SSCC: r.Sscc, RecordedAt: r.RecordedAt, ReceivedAt: r.ReceivedAt,
			TemperatureCelsius: r.TemperatureCelsius, HumidityPercent: r.HumidityPercent, Latitude: r.Latitude,
			Longitude: r.Longitude,
		}
	}
	return readings, nil
}

// LastBefore returns the time of the last stored reading of an SSCC recorded before before, if any.
func LastBefore(ctx context.Context, db queries.DBTX, sscc string, before time.Time) (time.Time, bool, error) {
	t, err := queries.New(db).LatestReadingBefore(ctx, queries.LatestReadingBeforeParams{Sscc: sscc, Before: before})
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("read last reading: %w", err)
	}
	return t, true, nil
}
