package incident

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/veritrace-platform/telemetry-stream-service/internal/incident/queries"
)

// Store reads and writes telemetry.cold_chain_incidents through a pool or a transaction.
type Store struct {
	q *queries.Queries
}

// NewStore returns a store on db.
func NewStore(db queries.DBTX) *Store {
	return &Store{q: queries.New(db)}
}

// Insert stores a confirmed incident. It reports false if the incident of the episode exists already.
func (s *Store) Insert(ctx context.Context, i Incident) (bool, error) {
	n, err := s.q.InsertIncident(ctx, queries.InsertIncidentParams{
		ID: i.ID, Sscc: i.SSCC, ShipmentID: i.ShipmentID, DeviceID: i.DeviceID, StartedAt: i.StartedAt,
		ConfirmedAt: i.ConfirmedAt, MinTempCelsius: i.MinTempCelsius, MaxTempCelsius: i.MaxTempCelsius,
		TriggerTemperatureCelsius: i.TriggerTemperatureCelsius, ExtremeTemperatureCelsius: i.ExtremeTemperatureCelsius,
		Latitude: i.Latitude, Longitude: i.Longitude, IncidentHash: i.IncidentHash,
	})
	if err != nil {
		return false, fmt.Errorf("insert incident: %w", err)
	}
	return n == 1, nil
}

// Extend records a new extreme temperature of an open incident. It reports false if the incident has ended.
func (s *Store) Extend(ctx context.Context, id uuid.UUID, extreme float64) (bool, error) {
	n, err := s.q.ExtendIncident(ctx, queries.ExtendIncidentParams{ID: id, ExtremeTemperatureCelsius: extreme})
	if err != nil {
		return false, fmt.Errorf("extend incident: %w", err)
	}
	return n == 1, nil
}

// Resolve ends an open incident with its end, duration, and extreme temperature. It reports false if the
// incident has ended already.
func (s *Store) Resolve(ctx context.Context, i Incident) (bool, error) {
	if i.EndedAt == nil || i.DurationSeconds == nil {
		return false, errors.New("resolve incident: no end")
	}
	n, err := s.q.ResolveIncident(ctx, queries.ResolveIncidentParams{
		ID: i.ID, EndedAt: i.EndedAt, DurationSeconds: int32Ptr(*i.DurationSeconds),
		ExtremeTemperatureCelsius: i.ExtremeTemperatureCelsius,
	})
	if err != nil {
		return false, fmt.Errorf("resolve incident: %w", err)
	}
	return n == 1, nil
}

// Latest returns the incident of the SSCC that started last, if any.
func (s *Store) Latest(ctx context.Context, sscc string) (Incident, bool, error) {
	row, err := s.q.LatestIncident(ctx, sscc)
	if errors.Is(err, pgx.ErrNoRows) {
		return Incident{}, false, nil
	}
	if err != nil {
		return Incident{}, false, fmt.Errorf("read latest incident: %w", err)
	}
	return fromRow(row), true, nil
}

// ResolveAtShipmentEnd resolves the open incident of the SSCC, if any, at its last reading recorded until until.
func (s *Store) ResolveAtShipmentEnd(ctx context.Context, sscc string, until time.Time) (Incident, bool, error) {
	row, err := s.q.ResolveAtShipmentEnd(ctx, queries.ResolveAtShipmentEndParams{Sscc: sscc, Until: until})
	if errors.Is(err, pgx.ErrNoRows) {
		return Incident{}, false, nil
	}
	if err != nil {
		return Incident{}, false, fmt.Errorf("resolve incident at shipment end: %w", err)
	}
	return fromRow(row), true, nil
}

func fromRow(r queries.TelemetryColdChainIncident) Incident {
	return Incident{
		ID: r.ID, ShipmentID: r.ShipmentID, SSCC: r.Sscc, DeviceID: r.DeviceID, StartedAt: r.StartedAt,
		ConfirmedAt: r.ConfirmedAt, EndedAt: r.EndedAt, DurationSeconds: intPtr(r.DurationSeconds),
		MinTempCelsius: r.MinTempCelsius, MaxTempCelsius: r.MaxTempCelsius,
		TriggerTemperatureCelsius: r.TriggerTemperatureCelsius, ExtremeTemperatureCelsius: r.ExtremeTemperatureCelsius,
		Latitude: r.Latitude, Longitude: r.Longitude, IncidentHash: r.IncidentHash,
	}
}

func int32Ptr(v int) *int32 {
	n := int32(v) //nolint:gosec // durations of incidents fit in 68 years
	return &n
}

func intPtr(v *int32) *int {
	if v == nil {
		return nil
	}
	n := int(*v)
	return &n
}
