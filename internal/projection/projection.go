// Package projection keeps the telemetry service's view of shipments, built from Kafka topic shipment.events
// (messaging.md §3; ADR-0008). The detector reads a shipment's status and temperature bounds from it, and the read
// API and the notification hub read which tenants and which driver may see the shipment's telemetry.
//
// Events of one shipment share its SSCC as key, so they arrive in order. Each event carries the shipment's
// sequence number; an event at or below the last applied sequence is a redelivery and changes nothing.
package projection

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/veritrace-platform/telemetry-stream-service/internal/incident"
	"github.com/veritrace-platform/telemetry-stream-service/internal/pgerror"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/logging"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/tracecontext"
	"github.com/veritrace-platform/telemetry-stream-service/internal/projection/queries"
	"github.com/veritrace-platform/telemetry-stream-service/internal/stream"
)

// Topic is the Kafka topic of shipment events.
const Topic = "shipment.events"

// Group is the consumer group of the projection.
const Group = "telemetry-stream-service.projection"

// Shipment statuses (shipment-lifecycle.md §3).
const (
	StatusCreated   = "CREATED"
	StatusInTransit = "IN_TRANSIT"
	StatusDelivered = "DELIVERED"
	StatusCancelled = "CANCELLED"
	StatusRecalled  = "RECALLED"
)

var statuses = map[string]bool{
	StatusCreated: true, StatusInTransit: true, StatusDelivered: true, StatusCancelled: true, StatusRecalled: true,
}

// Shipment is the projection of one shipment.
type Shipment struct {
	SSCC                 string
	ShipmentID           uuid.UUID
	OwnerTenantID        uuid.UUID
	ParticipantTenantIDs []uuid.UUID
	AssignedDriverID     *uuid.UUID
	Status               string
	GTIN                 string
	ProductName          string
	LotNumber            string
	MinTempCelsius       float64
	MaxTempCelsius       float64
	LastEventSequence    int
	UpdatedAt            time.Time
}

// Monitored reports whether the shipment's readings are evaluated against its bounds
// (cold-chain-monitoring.md §3).
func (s Shipment) Monitored() bool {
	return statusMonitored(s.Status)
}

func statusMonitored(status string) bool {
	return status == StatusCreated || status == StatusInTransit
}

// ErrNotFound reports an SSCC that the projection does not know.
var ErrNotFound = errors.New("shipment not projected")

// Store reads and writes telemetry.shipment_projection.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore returns a store on the runtime role's pool.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Get returns the projection of the shipment with the SSCC.
func (s *Store) Get(ctx context.Context, sscc string) (Shipment, error) {
	row, err := queries.New(s.pool).GetShipment(ctx, sscc)
	if errors.Is(err, pgx.ErrNoRows) {
		return Shipment{}, ErrNotFound
	}
	if err != nil {
		return Shipment{}, fmt.Errorf("get shipment: %w", err)
	}
	return fromRow(row), nil
}

func fromRow(row queries.TelemetryShipmentProjection) Shipment {
	return Shipment{
		SSCC: row.Sscc, ShipmentID: row.ShipmentID, OwnerTenantID: row.OwnerTenantID,
		ParticipantTenantIDs: row.ParticipantTenantIds, AssignedDriverID: row.AssignedDriverID, Status: row.Status,
		GTIN: row.Gtin, ProductName: row.ProductName, LotNumber: row.LotNumber, MinTempCelsius: row.MinTempCelsius,
		MaxTempCelsius: row.MaxTempCelsius, LastEventSequence: int(row.LastEventSequence), UpdatedAt: row.UpdatedAt,
	}
}

// LookUp returns the projections of the SSCCs that the projection knows, by SSCC.
func LookUp(ctx context.Context, db queries.DBTX, ssccs []string) (map[string]Shipment, error) {
	rows, err := queries.New(db).GetShipments(ctx, ssccs)
	if err != nil {
		return nil, fmt.Errorf("look up shipments: %w", err)
	}
	shipments := make(map[string]Shipment, len(rows))
	for _, row := range rows {
		shipments[row.Sscc] = fromRow(row)
	}
	return shipments, nil
}

// Producer writes records and waits for them to be acknowledged, as *kgo.Client does.
type Producer interface {
	ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults
}

// Projector applies batches of shipment events to the projection.
type Projector struct {
	pool     *pgxpool.Pool
	producer Producer
	logger   *slog.Logger
}

// NewProjector returns a projector that writes through the runtime role's pool and produces the incident events
// of shipments that stop being monitored.
func NewProjector(pool *pgxpool.Pool, producer Producer, logger *slog.Logger) *Projector {
	return &Projector{pool: pool, producer: producer, logger: logger}
}

// Handle applies a batch of shipment.events records in one transaction. An event that cannot be applied, because
// it is malformed or the database refuses it, is logged and skipped, so that it never blocks the shipments after
// it; any other error fails the batch, which the consumer retries.
//
// A shipment that is delivered, cancelled, or recalled is no longer monitored, so readings can no longer resolve
// an incident that is still open: it is resolved at the last reading recorded until the event occurred. Its
// cold_chain.breach_resolved event is produced before the transaction commits, as the detector does.
func (p *Projector) Handle(ctx context.Context, records []*kgo.Record) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var events []*kgo.Record
	for _, rec := range records {
		if err := p.applyRecord(ctx, tx, rec, &events); err != nil {
			return err
		}
	}
	if len(events) > 0 {
		if err := p.producer.ProduceSync(ctx, events...).FirstErr(); err != nil {
			return fmt.Errorf("produce incident events: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// applyRecord applies one record within a savepoint, so that a refused event leaves the rest of the batch intact.
func (p *Projector) applyRecord(ctx context.Context, tx pgx.Tx, rec *kgo.Record, events *[]*kgo.Record) error {
	if tc, ok := tracecontext.Parse(stream.Header(rec, stream.HeaderTraceparent)); ok {
		ctx = tracecontext.NewContext(ctx, tc)
	}
	ctx = logging.WithAttrs(ctx, slog.String("sscc", string(rec.Key)))
	e, err := decode(rec.Value)
	if errors.Is(err, errOtherVersion) {
		p.logger.DebugContext(ctx, "skipped an event of another version", slog.Any("error", err))
		return nil
	}
	if err != nil {
		p.logger.ErrorContext(ctx, "skipped a malformed shipment event", slog.Int("partition", int(rec.Partition)),
			slog.Int64("offset", rec.Offset), slog.Any("error", err))
		return nil
	}
	sp, err := tx.Begin(ctx)
	if err != nil {
		return fmt.Errorf("savepoint: %w", err)
	}
	result, err := e.apply(ctx, queries.New(sp))
	var resolved *kgo.Record
	if err == nil && result == applied && !statusMonitored(e.Subject.Status) {
		resolved, err = p.resolveOpenIncident(ctx, sp, e, stream.Header(rec, stream.HeaderTraceparent))
	}
	if pgerror.IsDataError(err) {
		_ = sp.Rollback(ctx)
		p.logger.ErrorContext(ctx, "skipped a shipment event that the database refuses",
			slog.String("event_id", e.EventID), slog.Any("error", err))
		return nil
	}
	if err != nil {
		return err
	}
	if err := sp.Commit(ctx); err != nil {
		return fmt.Errorf("release savepoint: %w", err)
	}
	if resolved != nil {
		*events = append(*events, resolved)
	}
	switch result {
	case applied:
	case duplicate:
		p.logger.DebugContext(ctx, "skipped a shipment event that was applied before",
			slog.String("event_id", e.EventID), slog.Int("sequence", int(e.Sequence)))
	case unknownShipment:
		// The shipment.created event comes first in the partition, so this means it was lost or refused.
		p.logger.WarnContext(ctx, "skipped an event of a shipment that the projection does not know",
			slog.String("event_id", e.EventID), slog.String("event_type", e.EventType))
	}
	return nil
}

// resolveOpenIncident resolves the shipment's open incident, if any, at its last reading recorded until the event
// occurred, and returns the record of its cold_chain.breach_resolved event.
func (p *Projector) resolveOpenIncident(ctx context.Context, db pgx.Tx, e event, traceparent string) (*kgo.Record, error) {
	closed, ok, err := incident.NewStore(db).ResolveAtShipmentEnd(ctx, e.Subject.SSCC, e.occurredAt)
	if err != nil || !ok {
		return nil, err
	}
	resolved, err := incident.ResolvedEvent(closed, e.Subject.Status)
	if err != nil {
		return nil, err
	}
	p.logger.InfoContext(ctx, "breach resolved at shipment end", slog.String("incident_id", closed.ID.String()),
		slog.String("status", e.Subject.Status))
	tc, ok := tracecontext.Parse(traceparent)
	if !ok {
		tc = tracecontext.New()
	}
	return resolved.Record(tc.Child().Traceparent())
}
