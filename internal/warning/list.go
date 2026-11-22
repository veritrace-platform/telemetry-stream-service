package warning

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/veritrace-platform/telemetry-stream-service/internal/warning/queries"
)

// Listed is a warning with the shipment fields that lists show.
type Listed struct {
	Warning
	ProductName string
	GTIN        string
	LotNumber   string
}

// Page selects a page of warnings, newest first.
type Page struct {
	// After is the last warning of the previous page; nil starts at the newest.
	After *uuid.UUID
	Limit int
}

// ShipmentWarnings returns a page of the warnings of one SSCC.
func (s *Store) ShipmentWarnings(ctx context.Context, sscc string, page Page) ([]Listed, error) {
	rows, err := s.q.ShipmentWarnings(ctx, queries.ShipmentWarningsParams{
		Sscc: sscc, After: page.After, PageLimit: int32(page.Limit), //nolint:gosec // limits are at most 101
	})
	if err != nil {
		return nil, fmt.Errorf("list warnings of %s: %w", sscc, err)
	}
	listed := make([]Listed, len(rows))
	for i, r := range rows {
		listed[i] = Listed{Warning: fromRow(r.TelemetryExcursionWarning), ProductName: r.ProductName, GTIN: r.Gtin, LotNumber: r.LotNumber}
	}
	return listed, nil
}

// Scope is the shipments whose warnings a caller sees: those its tenant takes part in, and for a driver only those
// assigned to it.
type Scope struct {
	TenantID uuid.UUID
	DriverID *uuid.UUID
}

// States of warnings in lists.
const (
	StateOpen   = "open"
	StateClosed = "closed"
)

// VisibleWarnings returns a page of the warnings in scope, optionally only open or closed ones.
func (s *Store) VisibleWarnings(ctx context.Context, scope Scope, state string, page Page) ([]Listed, error) {
	var filter *string
	if state != "" {
		filter = &state
	}
	rows, err := s.q.VisibleWarnings(ctx, queries.VisibleWarningsParams{
		TenantID: scope.TenantID, DriverID: scope.DriverID, State: filter, After: page.After,
		PageLimit: int32(page.Limit), //nolint:gosec // limits are at most 101
	})
	if err != nil {
		return nil, fmt.Errorf("list warnings: %w", err)
	}
	listed := make([]Listed, len(rows))
	for i, r := range rows {
		listed[i] = Listed{Warning: fromRow(r.TelemetryExcursionWarning), ProductName: r.ProductName, GTIN: r.Gtin, LotNumber: r.LotNumber}
	}
	return listed, nil
}

// Performance is the forecast quality over a period (cold-chain-monitoring.md §7.3).
type Performance struct {
	// Warnings issued in the period, by outcome; Open have none yet.
	Issued, Open, Breached, Cleared, Inconclusive int
	// Incidents started in the period, and those that a warning was closed as BREACHED with.
	Incidents, WarnedIncidents int
	// Statistics of the BREACHED warnings; nil without any.
	MedianLeadTimeSeconds, MinLeadTimeSeconds, MeanLeadTimeSeconds *float64
	MeanExcursionTimeErrorSeconds                                  *float64
}

// Precision is BREACHED / (BREACHED + CLEARED), or nil without either.
func (p Performance) Precision() *float64 {
	return ratio(p.Breached, p.Breached+p.Cleared)
}

// Recall is the share of incidents that a warning preceded, or nil without incidents.
func (p Performance) Recall() *float64 {
	return ratio(p.WarnedIncidents, p.Incidents)
}

func ratio(part, whole int) *float64 {
	if whole == 0 {
		return nil
	}
	r := float64(part) / float64(whole)
	return &r
}

// Measure returns the forecast quality over [from, before) for the shipments in scope, or for one SSCC of them.
func (s *Store) Measure(ctx context.Context, scope Scope, sscc *string, from, before time.Time) (Performance, error) {
	row, err := s.q.WarningPerformance(ctx, queries.WarningPerformanceParams{
		TenantID: scope.TenantID, DriverID: scope.DriverID, Sscc: sscc, FromTime: from, Before: before,
	})
	if err != nil {
		return Performance{}, fmt.Errorf("measure warnings: %w", err)
	}
	p := Performance{
		Issued: int(row.Issued), Open: int(row.Open), Breached: int(row.Breached), Cleared: int(row.Cleared),
		Inconclusive: int(row.Inconclusive), Incidents: int(row.Incidents), WarnedIncidents: int(row.WarnedIncidents),
	}
	if p.Breached > 0 {
		p.MedianLeadTimeSeconds, p.MinLeadTimeSeconds = &row.MedianLeadTimeSeconds, &row.MinLeadTimeSeconds
		p.MeanLeadTimeSeconds, p.MeanExcursionTimeErrorSeconds = &row.MeanLeadTimeSeconds, &row.MeanExcursionTimeErrorSeconds
	}
	return p, nil
}
