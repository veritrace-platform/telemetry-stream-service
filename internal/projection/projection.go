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

	"github.com/veritrace-platform/telemetry-stream-service/internal/envelope"
	"github.com/veritrace-platform/telemetry-stream-service/internal/exposure"
	"github.com/veritrace-platform/telemetry-stream-service/internal/incident"
	"github.com/veritrace-platform/telemetry-stream-service/internal/pgerror"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/logging"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/tracecontext"
	"github.com/veritrace-platform/telemetry-stream-service/internal/projection/queries"
	"github.com/veritrace-platform/telemetry-stream-service/internal/stream"
	"github.com/veritrace-platform/telemetry-stream-service/internal/warning"
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
	// ProductionDate, ExpirationDate, and ShelfLifeQ10 are the lot's, as shipment.created carried them; nil when an
	// older event lacked them.
	ProductionDate *time.Time
	ExpirationDate *time.Time
	ShelfLifeQ10   *float64
	// MonitoringEndedAt is when the shipment stopped being monitored; nil while it is.
	MonitoringEndedAt *time.Time
	// ShelfLifeStatus is the worst shelf-life status notified so far.
	ShelfLifeStatus   string
	LastEventSequence int
	UpdatedAt         time.Time
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
		MaxTempCelsius: row.MaxTempCelsius, ProductionDate: row.ProductionDate, ExpirationDate: row.ExpirationDate,
		ShelfLifeQ10: row.ShelfLifeQ10, MonitoringEndedAt: row.MonitoringEndedAt, ShelfLifeStatus: row.ShelfLifeStatus,
		LastEventSequence: int(row.LastEventSequence), UpdatedAt: row.UpdatedAt,
	}
}

// Exposure returns what the thermal exposure of the shipment needs.
func (s Shipment) Exposure() exposure.Shipment {
	return exposure.Shipment{
		SSCC: s.SSCC, ShipmentID: s.ShipmentID, Status: s.Status, MinTempCelsius: s.MinTempCelsius,
		MaxTempCelsius: s.MaxTempCelsius, ProductionDate: s.ProductionDate, ExpirationDate: s.ExpirationDate,
		ShelfLifeQ10: s.ShelfLifeQ10, MonitoringEndedAt: s.MonitoringEndedAt,
	}
}

// LockForDetection returns the projections of the SSCCs that the projection knows, by SSCC, and keeps their
// status from changing until the transaction of db ends. The detector evaluates readings in that transaction, so a
// shipment that stops being monitored waits for the incidents the transaction records, and the projector then
// resolves them (Projector.Handle); without the lock, an incident confirmed at that moment could stay open
// forever. A deadlock with a projector batch is possible and broken by PostgreSQL; the failed batch is retried.
func LockForDetection(ctx context.Context, db queries.DBTX, ssccs []string) (map[string]Shipment, error) {
	rows, err := queries.New(db).LockShipments(ctx, ssccs)
	if err != nil {
		return nil, fmt.Errorf("lock shipments: %w", err)
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
// an incident or close a warning that is still open: the incident is resolved, and the warning closed as
// INCONCLUSIVE, at the last reading recorded until the event occurred, and a resolved incident has its shelf-life
// cost assessed (cold-chain-monitoring.md §7.2, §8.3). Their events are produced before the transaction commits, as
// the detector does.
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
	var ended []*kgo.Record
	if err == nil && result == applied && !statusMonitored(e.Subject.Status) {
		ended, err = p.endMonitoring(ctx, sp, e, stream.Header(rec, stream.HeaderTraceparent))
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
	*events = append(*events, ended...)
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

// endMonitoring ends what readings can no longer end once a shipment stops being monitored: it resolves the open
// incident and closes the open warning, if any, at the last reading recorded until the event occurred, and assesses
// the shelf life that a resolved incident cost. It returns the records of their events.
func (p *Projector) endMonitoring(ctx context.Context, db pgx.Tx, e event, traceparent string) ([]*kgo.Record, error) {
	tc, ok := tracecontext.Parse(traceparent)
	if !ok {
		tc = tracecontext.New()
	}
	var records []*kgo.Record
	add := func(ev envelope.Event, topic string) error {
		record, err := ev.Record(topic, tc.Child().Traceparent())
		if err == nil {
			records = append(records, record)
		}
		return err
	}

	closed, resolved, err := incident.NewStore(db).ResolveAtShipmentEnd(ctx, e.Subject.SSCC, e.occurredAt)
	if err != nil {
		return nil, err
	}
	if resolved {
		event, err := incident.ResolvedEvent(closed, e.Subject.Status)
		if err != nil {
			return nil, err
		}
		if err := add(event.Event, incident.Topic); err != nil {
			return nil, err
		}
		p.logger.InfoContext(ctx, "breach resolved at shipment end", slog.String("incident_id", closed.ID.String()),
			slog.String("status", e.Subject.Status))
	}

	w, inconclusive, err := warning.NewStore(db).CloseAtShipmentEnd(ctx, e.Subject.SSCC, e.occurredAt)
	if err != nil {
		return nil, err
	}
	if inconclusive {
		event, err := warning.ClosedEvent(w, e.Subject.Status)
		if err != nil {
			return nil, err
		}
		if err := add(event, envelope.TopicForecasts); err != nil {
			return nil, err
		}
		p.logger.InfoContext(ctx, "excursion warning inconclusive at shipment end", slog.String("warning_id", w.ID.String()),
			slog.String("status", e.Subject.Status))
	}

	if resolved {
		row, err := queries.New(db).GetShipment(ctx, e.Subject.SSCC)
		if err != nil {
			return nil, fmt.Errorf("read shipment: %w", err)
		}
		alert, err := exposure.Reassess(ctx, db, fromRow(row).Exposure(), *closed.EndedAt)
		if err != nil {
			return nil, err
		}
		if alert != nil {
			if err := add(*alert, envelope.TopicForecasts); err != nil {
				return nil, err
			}
			p.logger.InfoContext(ctx, "shelf life reduced", slog.String("data", string(alert.Data)))
		}
	}
	return records, nil
}
