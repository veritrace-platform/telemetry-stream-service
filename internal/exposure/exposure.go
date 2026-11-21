// Package exposure measures the thermal exposure of a shipment and estimates the shelf life it costs the goods
// (cold-chain-monitoring.md §8; ADR-0024): mean and mean kinetic temperature, time and degree-minutes out of bounds,
// and a Q10 model of extra aging above the maximum temperature. It also produces the shelf-life alerts on Kafka topic
// telemetry.forecasts (messaging.md §7).
package exposure

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"

	"github.com/veritrace-platform/telemetry-stream-service/internal/exposure/queries"
)

// Exposure rules (cold-chain-monitoring.md §8).
const (
	// MaxInterval is the longest time that one reading stands for: the gap limit of the breach rule.
	MaxInterval = 15 * time.Second
	// ActivationEnergyOverR is ΔH/R of mean kinetic temperature: 83 144 J/mol over 8.3144 J/(mol·K), in kelvin.
	ActivationEnergyOverR = 10000.0
	// ReducedLossPercent is the shelf-life loss from which the status is REDUCED.
	ReducedLossPercent = 1.0
	kelvin             = 273.15
)

// Shipment is what the exposure of a shipment needs from its projection.
type Shipment struct {
	SSCC           string
	ShipmentID     uuid.UUID
	Status         string
	MinTempCelsius float64
	MaxTempCelsius float64
	// ProductionDate, ExpirationDate, and ShelfLifeQ10 are nil when shipment.created lacked them.
	ProductionDate    *time.Time
	ExpirationDate    *time.Time
	ShelfLifeQ10      *float64
	MonitoringEndedAt *time.Time
}

// monitoringEnded tells whether the shipment had stopped being monitored by until. A shipment that stopped before
// the projection recorded when monitoring ends has only its status to tell.
func (s Shipment) monitoringEnded(until time.Time) bool {
	if s.MonitoringEndedAt != nil {
		return !s.MonitoringEndedAt.After(until)
	}
	return s.Status != "" && s.Status != "CREATED" && s.Status != "IN_TRANSIT"
}

// Exposure is a shipment's thermal exposure as of a time.
type Exposure struct {
	SSCC string
	// AssessedAt is the time the exposure runs to: the end of monitoring, or the time of the assessment.
	AssessedAt time.Time
	// MonitoringEnded tells whether the shipment had stopped being monitored by then.
	MonitoringEnded bool
	MinTempCelsius  float64
	MaxTempCelsius  float64
	ReadingCount    int
	// FirstReadingAt, LastReadingAt, and the temperatures are nil without readings.
	FirstReadingAt                *time.Time
	LastReadingAt                 *time.Time
	CoveredSeconds                float64
	UncoveredSeconds              float64
	MeanTemperatureCelsius        *float64
	MeanKineticTemperatureCelsius *float64
	LowestTemperatureCelsius      *float64
	HighestTemperatureCelsius     *float64
	TimeAboveMaxSeconds           float64
	TimeBelowMinSeconds           float64
	DegreeMinutesAboveMax         float64
	DegreeMinutesBelowMin         float64
	// ExtraAging is the time the goods aged beyond the label, by the shipment's Q10.
	ExtraAging time.Duration
	// ShelfLife is nil when the projection lacks the lot's dates.
	ShelfLife *ShelfLife
}

// Measure returns the exposure of a shipment over its readings recorded until until.
func Measure(ctx context.Context, db queries.DBTX, s Shipment, until time.Time) (Exposure, error) {
	q10 := 1.0 // no aging, when the projection has no Q10
	if s.ShelfLifeQ10 != nil {
		q10 = *s.ShelfLifeQ10
	}
	row, err := queries.New(db).ThermalExposure(ctx, queries.ThermalExposureParams{
		Sscc: s.SSCC, Until: until, MaxIntervalSeconds: MaxInterval.Seconds(), MinCelsius: s.MinTempCelsius,
		MaxCelsius: s.MaxTempCelsius, Q10: q10,
	})
	if err != nil {
		return Exposure{}, fmt.Errorf("measure thermal exposure of %s: %w", s.SSCC, err)
	}
	e := Exposure{
		SSCC: s.SSCC, AssessedAt: until, MonitoringEnded: s.monitoringEnded(until),
		MinTempCelsius: s.MinTempCelsius, MaxTempCelsius: s.MaxTempCelsius, ReadingCount: int(row.ReadingCount),
		CoveredSeconds: row.CoveredSeconds, UncoveredSeconds: row.UncoveredSeconds,
		TimeAboveMaxSeconds: row.AboveMaxSeconds, TimeBelowMinSeconds: row.BelowMinSeconds,
		DegreeMinutesAboveMax: row.AboveMaxCelsiusSeconds / 60, DegreeMinutesBelowMin: row.BelowMinCelsiusSeconds / 60,
		ExtraAging: time.Duration(row.ExtraAgingSeconds * float64(time.Second)),
	}
	if e.ReadingCount > 0 {
		first, last, lowest, highest := row.FirstReadingAt, row.LastReadingAt, row.LowestCelsius, row.HighestCelsius
		e.FirstReadingAt, e.LastReadingAt, e.LowestTemperatureCelsius, e.HighestTemperatureCelsius = &first, &last, &lowest, &highest
	}
	if row.CoveredSeconds > 0 {
		mean := row.CelsiusSeconds / row.CoveredSeconds
		mkt := MeanKineticTemperature(row.KineticSeconds, row.CoveredSeconds)
		e.MeanTemperatureCelsius, e.MeanKineticTemperatureCelsius = &mean, &mkt
	}
	if s.ProductionDate != nil && s.ExpirationDate != nil && s.ShelfLifeQ10 != nil {
		sl := Assess(e.ExtraAging, *s.ProductionDate, *s.ExpirationDate, *s.ShelfLifeQ10, until)
		e.ShelfLife = &sl
	}
	return e, nil
}

// MeanKineticTemperature returns the MKT in °C from Σ Δt·e^(−(ΔH/R)/T) and Σ Δt (USP <1079>).
func MeanKineticTemperature(kineticSeconds, coveredSeconds float64) float64 {
	return ActivationEnergyOverR/-math.Log(kineticSeconds/coveredSeconds) - kelvin
}

// Status is a shipment's shelf-life status.
type Status string

// Shelf-life statuses, from best to worst.
const (
	Intact  Status = "INTACT"
	Reduced Status = "REDUCED"
	Expired Status = "EXPIRED"
)

// ShelfLife is a shelf-life estimate (cold-chain-monitoring.md §8.2).
type ShelfLife struct {
	Q10                 float64
	ProductionDate      time.Time
	LabelExpirationDate time.Time
	ExtraAging          time.Duration
	// LossPercent is the extra aging as a percentage of the label shelf life.
	LossPercent float64
	// EstimatedExpiry is the label expiry moved earlier by the extra aging; EstimatedExpirationDate is the day of the
	// last moment before it.
	EstimatedExpiry         time.Time
	EstimatedExpirationDate time.Time
	// Remaining is the estimated expiry minus the assessment time; negative once it has passed.
	Remaining time.Duration
	Status    Status
}

// Assess estimates the shelf life left at asOf of goods produced and expiring on the given days, which aged by
// extraAging beyond their label. Days are whole days in UTC: the label shelf life runs from the start of the
// production date to the end of the expiration date.
func Assess(extraAging time.Duration, production, expiration time.Time, q10 float64, asOf time.Time) ShelfLife {
	start := day(production)
	labelExpiry := day(expiration).AddDate(0, 0, 1)
	estimated := labelExpiry.Add(-extraAging)
	sl := ShelfLife{
		Q10: q10, ProductionDate: start, LabelExpirationDate: day(expiration), ExtraAging: extraAging,
		LossPercent: 100 * extraAging.Seconds() / labelExpiry.Sub(start).Seconds(), EstimatedExpiry: estimated,
		EstimatedExpirationDate: day(estimated.Add(-time.Nanosecond)), Remaining: estimated.Sub(asOf), Status: Intact,
	}
	switch {
	case sl.Remaining <= 0:
		sl.Status = Expired
	case sl.LossPercent >= ReducedLossPercent:
		sl.Status = Reduced
	}
	return sl
}

// day returns the start of t's day in UTC.
func day(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}
