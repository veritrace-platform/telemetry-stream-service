package incident

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/veritrace-platform/telemetry-stream-service/internal/incident/queries"
)

// Listed is an incident with the shipment fields that lists show.
type Listed struct {
	Incident
	ProductName string
	GTIN        string
	LotNumber   string
}

// Page selects a page of incidents, newest first.
type Page struct {
	// After is the last incident of the previous page; nil starts at the newest.
	After *uuid.UUID
	Limit int
}

// ShipmentIncidents returns a page of the incidents of one SSCC.
func (s *Store) ShipmentIncidents(ctx context.Context, sscc string, page Page) ([]Listed, error) {
	rows, err := s.q.ShipmentIncidents(ctx, queries.ShipmentIncidentsParams{
		Sscc: sscc, After: page.After, PageLimit: int32(page.Limit), //nolint:gosec // limits are at most 101
	})
	if err != nil {
		return nil, fmt.Errorf("list incidents of %s: %w", sscc, err)
	}
	listed := make([]Listed, len(rows))
	for i, r := range rows {
		listed[i] = Listed{
			Incident: fromRow(r.TelemetryColdChainIncident), ProductName: r.ProductName, GTIN: r.Gtin, LotNumber: r.LotNumber,
		}
	}
	return listed, nil
}

// Scope is the shipments whose incidents a caller sees: those its tenant takes part in, and for a driver only those
// assigned to it.
type Scope struct {
	TenantID uuid.UUID
	DriverID *uuid.UUID
}

// States of incidents in lists.
const (
	StateOpen     = "open"
	StateResolved = "resolved"
)

// VisibleIncidents returns a page of the incidents in scope, optionally only open or resolved ones.
func (s *Store) VisibleIncidents(ctx context.Context, scope Scope, state string, page Page) ([]Listed, error) {
	var filter *string
	if state != "" {
		filter = &state
	}
	rows, err := s.q.VisibleIncidents(ctx, queries.VisibleIncidentsParams{
		TenantID: scope.TenantID, DriverID: scope.DriverID, State: filter, After: page.After,
		PageLimit: int32(page.Limit), //nolint:gosec // limits are at most 101
	})
	if err != nil {
		return nil, fmt.Errorf("list incidents: %w", err)
	}
	listed := make([]Listed, len(rows))
	for i, r := range rows {
		listed[i] = Listed{
			Incident: fromRow(r.TelemetryColdChainIncident), ProductName: r.ProductName, GTIN: r.Gtin, LotNumber: r.LotNumber,
		}
	}
	return listed, nil
}

// Summary counts the open incidents in scope and those confirmed since a time.
type Summary struct {
	Open   int
	Recent int
}

// Summarize counts the incidents in scope.
func (s *Store) Summarize(ctx context.Context, scope Scope, since time.Time) (Summary, error) {
	row, err := s.q.IncidentSummary(ctx, queries.IncidentSummaryParams{
		TenantID: scope.TenantID, DriverID: scope.DriverID, Since: since,
	})
	if err != nil {
		return Summary{}, fmt.Errorf("summarize incidents: %w", err)
	}
	return Summary{Open: int(row.OpenCount), Recent: int(row.RecentCount)}, nil
}
