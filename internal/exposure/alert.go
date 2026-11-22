package exposure

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/veritrace-platform/telemetry-stream-service/internal/envelope"
	"github.com/veritrace-platform/telemetry-stream-service/internal/exposure/queries"
)

// TypeShelfLifeReduced is the event type of a shelf-life alert (messaging.md §7).
const TypeShelfLifeReduced = "cold_chain.shelf_life_reduced"

// AssessEvery is how often, in device time, the detector assesses shelf life while a breach lasts
// (cold-chain-monitoring.md §8.3).
const AssessEvery = 15 * time.Minute

// Alert is the data of cold_chain.shelf_life_reduced (messaging.md §7).
type Alert struct {
	ShipmentID              uuid.UUID `json:"shipment_id"`
	SSCC                    string    `json:"sscc"`
	Status                  Status    `json:"status"`
	AssessedAt              string    `json:"assessed_at"`
	ShelfLifeQ10            float64   `json:"shelf_life_q10"`
	LabelExpirationDate     string    `json:"label_expiration_date"`
	EstimatedExpirationDate string    `json:"estimated_expiration_date"`
	ShelfLifeLossHours      float64   `json:"shelf_life_loss_hours"`
	ShelfLifeLossPercent    float64   `json:"shelf_life_loss_percent"`
	RemainingShelfLifeHours float64   `json:"remaining_shelf_life_hours"`
}

// Reassess assesses the shelf life of a shipment as of asOf, in the transaction of tx. When the status is worse than
// the one notified so far, it records the new status and returns cold_chain.shelf_life_reduced; otherwise it returns
// nil. A shipment whose projection lacks the lot's dates is never assessed.
func Reassess(ctx context.Context, tx pgx.Tx, s Shipment, asOf time.Time) (*envelope.Event, error) {
	if s.ProductionDate == nil || s.ExpirationDate == nil || s.ShelfLifeQ10 == nil {
		return nil, nil
	}
	e, err := Measure(ctx, tx, s, asOf)
	if err != nil {
		return nil, err
	}
	sl := e.ShelfLife
	if sl == nil || sl.Status == Intact {
		return nil, nil
	}
	n, err := queries.New(tx).RaiseShelfLifeStatus(ctx, queries.RaiseShelfLifeStatusParams{Sscc: s.SSCC, Status: string(sl.Status)})
	if err != nil {
		return nil, fmt.Errorf("raise shelf-life status: %w", err)
	}
	if n == 0 {
		return nil, nil
	}
	event, err := AlertEvent(s, *sl, asOf)
	if err != nil {
		return nil, err
	}
	return &event, nil
}

// AlertEvent returns cold_chain.shelf_life_reduced for a shipment's shelf-life estimate, assessed at asOf. Its ID is
// derived from the SSCC and the status alone, so an alert produced again after a failure, even when assessed at a
// later reading, has the same ID.
func AlertEvent(s Shipment, sl ShelfLife, asOf time.Time) (envelope.Event, error) {
	id := envelope.DerivedV8(TypeShelfLifeReduced + "|" + s.SSCC + "|" + string(sl.Status))
	event, err := envelope.New(id, TypeShelfLifeReduced, asOf,
		envelope.Subject{ShipmentID: s.ShipmentID, SSCC: s.SSCC, Status: s.Status}, Alert{
			ShipmentID: s.ShipmentID, SSCC: s.SSCC, Status: sl.Status, AssessedAt: asOf.UTC().Format(envelope.TimeLayout),
			ShelfLifeQ10: sl.Q10, LabelExpirationDate: sl.LabelExpirationDate.Format(time.DateOnly),
			EstimatedExpirationDate: sl.EstimatedExpirationDate.Format(time.DateOnly),
			ShelfLifeLossHours:      Round2(sl.ExtraAging.Hours()), ShelfLifeLossPercent: Round2(sl.LossPercent),
			RemainingShelfLifeHours: Round2(sl.Remaining.Hours()),
		})
	if err != nil {
		return envelope.Event{}, fmt.Errorf("shelf-life alert of %s: %w", s.SSCC, err)
	}
	return event, nil
}

// Round2 rounds to two decimals, the precision of hours, percentages, and temperatures in the contracts.
func Round2(v float64) float64 {
	return math.Round(v*100) / 100
}
