package warning

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/veritrace-platform/telemetry-stream-service/internal/forecast"
	"github.com/veritrace-platform/telemetry-stream-service/internal/warning/queries"
)

// Store reads and writes telemetry.excursion_warnings through a pool or a transaction.
type Store struct {
	q *queries.Queries
}

// NewStore returns a store on db.
func NewStore(db queries.DBTX) *Store {
	return &Store{q: queries.New(db)}
}

// Insert stores an issued warning. It reports false if the warning exists already, or if the SSCC has another open
// warning.
func (s *Store) Insert(ctx context.Context, w Warning) (bool, error) {
	n, err := s.q.InsertWarning(ctx, queries.InsertWarningParams{
		ID: w.ID, Sscc: w.SSCC, ShipmentID: w.ShipmentID, DeviceID: w.DeviceID, IssuedAt: w.IssuedAt,
		Bound: string(w.Bound), BoundCelsius: w.BoundCelsius, TemperatureCelsius: w.TemperatureCelsius,
		SlopeCelsiusPerMinute: w.SlopeCelsiusPerMinute, PredictedExcursionAt: w.PredictedExcursionAt,
	})
	if err != nil {
		return false, fmt.Errorf("insert warning: %w", err)
	}
	return n == 1, nil
}

// Open returns the open warning of the SSCC, if any.
func (s *Store) Open(ctx context.Context, sscc string) (Warning, bool, error) {
	return one(s.q.OpenWarning(ctx, sscc))
}

// Close closes an open warning as CLEARED or INCONCLUSIVE at closedAt and returns it. It reports false if the
// warning was closed already.
func (s *Store) Close(ctx context.Context, id uuid.UUID, outcome Outcome, closedAt time.Time) (Warning, bool, error) {
	o := string(outcome)
	return one(s.q.CloseWarning(ctx, queries.CloseWarningParams{ID: id, Outcome: &o, ClosedAt: &closedAt}))
}

// CloseBreached closes the open warning of the SSCC as BREACHED by an incident, at its confirmation, if the warning
// was issued before the incident's episode started, and returns it.
func (s *Store) CloseBreached(ctx context.Context, sscc string, incidentID uuid.UUID, startedAt, confirmedAt time.Time,
) (Warning, bool, error) {
	return one(s.q.CloseBreached(ctx, queries.CloseBreachedParams{
		Sscc: sscc, IncidentID: &incidentID, StartedAt: startedAt, ConfirmedAt: &confirmedAt,
	}))
}

// CloseAtShipmentEnd closes the open warning of the SSCC, if any, as INCONCLUSIVE at its last reading recorded until
// until, and returns it.
func (s *Store) CloseAtShipmentEnd(ctx context.Context, sscc string, until time.Time) (Warning, bool, error) {
	return one(s.q.CloseAtShipmentEnd(ctx, queries.CloseAtShipmentEndParams{Sscc: sscc, Until: until}))
}

func one(row queries.TelemetryExcursionWarning, err error) (Warning, bool, error) {
	if errors.Is(err, pgx.ErrNoRows) {
		return Warning{}, false, nil
	}
	if err != nil {
		return Warning{}, false, fmt.Errorf("write warning: %w", err)
	}
	return fromRow(row), true, nil
}

func fromRow(r queries.TelemetryExcursionWarning) Warning {
	w := Warning{
		ID: r.ID, ShipmentID: r.ShipmentID, SSCC: r.Sscc, DeviceID: r.DeviceID, IssuedAt: r.IssuedAt,
		Bound: forecast.Bound(r.Bound), BoundCelsius: r.BoundCelsius, TemperatureCelsius: r.TemperatureCelsius,
		SlopeCelsiusPerMinute: r.SlopeCelsiusPerMinute, PredictedExcursionAt: r.PredictedExcursionAt,
		ClosedAt: r.ClosedAt, IncidentID: r.IncidentID,
	}
	if r.Outcome != nil {
		o := Outcome(*r.Outcome)
		w.Outcome = &o
	}
	if r.LeadTimeSeconds != nil {
		lead := int(*r.LeadTimeSeconds)
		w.LeadTimeSeconds = &lead
	}
	return w
}
