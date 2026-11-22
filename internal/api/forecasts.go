package api

import (
	"math"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/veritrace-platform/telemetry-stream-service/internal/access"
	"github.com/veritrace-platform/telemetry-stream-service/internal/auth"
	"github.com/veritrace-platform/telemetry-stream-service/internal/exposure"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/httpx"
	"github.com/veritrace-platform/telemetry-stream-service/internal/rest"
	"github.com/veritrace-platform/telemetry-stream-service/internal/warning"
)

// Windows of the forecast quality: the default and the longest.
const (
	performanceWindow    = 7 * 24 * time.Hour
	maxPerformanceWindow = 90 * 24 * time.Hour
)

// Warning is an excursion warning with its shipment's product and lot.
type Warning struct {
	ID                    uuid.UUID  `json:"id"`
	ShipmentID            uuid.UUID  `json:"shipment_id"`
	SSCC                  string     `json:"sscc"`
	DeviceID              string     `json:"device_id"`
	IssuedAt              time.Time  `json:"issued_at"`
	Bound                 string     `json:"bound"`
	BoundCelsius          float64    `json:"bound_celsius"`
	TemperatureCelsius    float64    `json:"temperature_celsius"`
	SlopeCelsiusPerMinute float64    `json:"slope_celsius_per_minute"`
	PredictedExcursionAt  time.Time  `json:"predicted_excursion_at"`
	ClosedAt              *time.Time `json:"closed_at"`
	Outcome               *string    `json:"outcome"`
	IncidentID            *uuid.UUID `json:"incident_id"`
	LeadTimeSeconds       *int       `json:"lead_time_seconds"`
	ProductName           string     `json:"product_name"`
	GTIN                  string     `json:"gtin"`
	LotNumber             string     `json:"lot_number"`
}

func warningOf(l warning.Listed) Warning {
	w := Warning{
		ID: l.ID, ShipmentID: l.ShipmentID, SSCC: l.SSCC, DeviceID: l.DeviceID, IssuedAt: l.IssuedAt,
		Bound: string(l.Bound), BoundCelsius: l.BoundCelsius, TemperatureCelsius: l.TemperatureCelsius,
		SlopeCelsiusPerMinute: l.SlopeCelsiusPerMinute, PredictedExcursionAt: l.PredictedExcursionAt,
		ClosedAt: l.ClosedAt, IncidentID: l.IncidentID, LeadTimeSeconds: l.LeadTimeSeconds, ProductName: l.ProductName,
		GTIN: l.GTIN, LotNumber: l.LotNumber,
	}
	if l.Outcome != nil {
		o := string(*l.Outcome)
		w.Outcome = &o
	}
	return w
}

// warningPage reads limit and cursor; on errors it answers 400 and reports false.
func warningPage(w http.ResponseWriter, r *http.Request, q *rest.Query) (warning.Page, bool) {
	p, ok := page(w, r, q)
	return warning.Page{After: p.After, Limit: p.Limit}, ok
}

// warningCollection answers a page of warnings, fetched with one more than the limit.
func warningCollection(w http.ResponseWriter, r *http.Request, listed []warning.Listed, limit int) {
	items := make([]Warning, len(listed))
	for i, l := range listed {
		items[i] = warningOf(l)
	}
	httpx.WriteJSON(w, r, http.StatusOK, httpx.NewCollection(items, limit, func(w Warning) string {
		return httpx.UUIDCursor(w.ID)
	}))
}

func (h *Handler) shipmentWarnings(w http.ResponseWriter, r *http.Request) {
	s, ok := h.shipment(w, r)
	if !ok {
		return
	}
	pg, ok := warningPage(w, r, rest.NewQuery(r))
	if !ok {
		return
	}
	listed, err := warning.NewStore(h.pool).ShipmentWarnings(r.Context(), s.SSCC, pg)
	if err != nil {
		rest.InternalError(w, r, h.logger, err)
		return
	}
	warningCollection(w, r, listed, pg.Limit-1)
}

func (h *Handler) warnings(w http.ResponseWriter, r *http.Request) {
	p, ok := auth.Caller(w, r)
	if !ok {
		return
	}
	q := rest.NewQuery(r)
	state := q.Enum("state", "", warning.StateOpen, warning.StateClosed)
	pg, ok := warningPage(w, r, q)
	if !ok {
		return
	}
	scope := warning.Scope{TenantID: p.TenantID, DriverID: access.AssignedTo(p)}
	listed, err := warning.NewStore(h.pool).VisibleWarnings(r.Context(), scope, state, pg)
	if err != nil {
		rest.InternalError(w, r, h.logger, err)
		return
	}
	warningCollection(w, r, listed, pg.Limit-1)
}

// Performance is the body of the forecast quality endpoint (cold-chain-monitoring.md §7.3).
type Performance struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
	// SSCC is the shipment measured, or nil for every shipment of the caller.
	SSCC      *string `json:"sscc"`
	Warnings  Counts  `json:"warnings"`
	Incidents struct {
		Count  int `json:"count"`
		Warned int `json:"warned"`
	} `json:"incidents"`
	Precision                     *float64   `json:"precision"`
	Recall                        *float64   `json:"recall"`
	LeadTimeSeconds               *LeadTimes `json:"lead_time_seconds"`
	MeanExcursionTimeErrorSeconds *float64   `json:"mean_excursion_time_error_seconds"`
}

// Counts are the warnings issued in a period, by outcome.
type Counts struct {
	Issued       int `json:"issued"`
	Open         int `json:"open"`
	Breached     int `json:"breached"`
	Cleared      int `json:"cleared"`
	Inconclusive int `json:"inconclusive"`
}

// LeadTimes are statistics of the lead times of BREACHED warnings.
type LeadTimes struct {
	Median float64 `json:"median"`
	Min    float64 `json:"min"`
	Mean   float64 `json:"mean"`
}

func (h *Handler) performance(w http.ResponseWriter, r *http.Request) {
	p, ok := auth.Caller(w, r)
	if !ok {
		return
	}
	q := rest.NewQuery(r)
	to := q.Time("to", h.now().UTC())
	from := q.Time("from", to.Add(-performanceWindow))
	if len(q.Errors) == 0 {
		switch {
		case !from.Before(to):
			q.Fail("from", httpx.FieldInvalidValue, "must be before to")
		case to.Sub(from) > maxPerformanceWindow:
			q.Fail("from", httpx.FieldOutOfRange, "the window is at most 90 days")
		}
	}
	code := q.String("sscc")
	if problem := q.Problem(); problem != nil {
		httpx.WriteProblem(w, r, *problem)
		return
	}
	var sscc *string
	if code != "" {
		checked, ok := rest.CheckSSCC(w, r, code)
		if !ok {
			return
		}
		if _, ok := h.visible(w, r, p, checked); !ok {
			return
		}
		sscc = &checked
	}

	scope := warning.Scope{TenantID: p.TenantID, DriverID: access.AssignedTo(p)}
	m, err := warning.NewStore(h.pool).Measure(r.Context(), scope, sscc, from, to)
	if err != nil {
		rest.InternalError(w, r, h.logger, err)
		return
	}
	body := Performance{
		From: from, To: to, SSCC: sscc, Precision: round(m.Precision(), 4), Recall: round(m.Recall(), 4),
		Warnings: Counts{
			Issued: m.Issued, Open: m.Open, Breached: m.Breached, Cleared: m.Cleared, Inconclusive: m.Inconclusive,
		},
		MeanExcursionTimeErrorSeconds: round(m.MeanExcursionTimeErrorSeconds, 1),
	}
	body.Incidents.Count, body.Incidents.Warned = m.Incidents, m.WarnedIncidents
	if m.MedianLeadTimeSeconds != nil {
		body.LeadTimeSeconds = &LeadTimes{
			Median: *m.MedianLeadTimeSeconds, Min: *m.MinLeadTimeSeconds, Mean: *round(m.MeanLeadTimeSeconds, 1),
		}
	}
	httpx.WriteJSON(w, r, http.StatusOK, body)
}

// round rounds a value, if any, to the given decimals.
func round(v *float64, decimals int) *float64 {
	if v == nil {
		return nil
	}
	scale := math.Pow(10, float64(decimals))
	r := math.Round(*v*scale) / scale
	return &r
}

// ThermalExposure is the body of the thermal exposure endpoint (cold-chain-monitoring.md §8).
type ThermalExposure struct {
	SSCC                          string     `json:"sscc"`
	AssessedAt                    time.Time  `json:"assessed_at"`
	MonitoringEnded               bool       `json:"monitoring_ended"`
	MinTempCelsius                float64    `json:"min_temp_celsius"`
	MaxTempCelsius                float64    `json:"max_temp_celsius"`
	ReadingCount                  int        `json:"reading_count"`
	FirstReadingAt                *time.Time `json:"first_reading_at"`
	LastReadingAt                 *time.Time `json:"last_reading_at"`
	CoveredSeconds                int        `json:"covered_seconds"`
	UncoveredSeconds              int        `json:"uncovered_seconds"`
	MeanTemperatureCelsius        *float64   `json:"mean_temperature_celsius"`
	MeanKineticTemperatureCelsius *float64   `json:"mean_kinetic_temperature_celsius"`
	LowestTemperatureCelsius      *float64   `json:"lowest_temperature_celsius"`
	HighestTemperatureCelsius     *float64   `json:"highest_temperature_celsius"`
	TimeAboveMaxSeconds           int        `json:"time_above_max_seconds"`
	TimeBelowMinSeconds           int        `json:"time_below_min_seconds"`
	DegreeMinutesAboveMax         float64    `json:"degree_minutes_above_max"`
	DegreeMinutesBelowMin         float64    `json:"degree_minutes_below_min"`
	ShelfLife                     *ShelfLife `json:"shelf_life"`
}

// ShelfLife is a shelf-life estimate.
type ShelfLife struct {
	ShelfLifeQ10            float64 `json:"shelf_life_q10"`
	ProductionDate          string  `json:"production_date"`
	LabelExpirationDate     string  `json:"label_expiration_date"`
	EstimatedExpirationDate string  `json:"estimated_expiration_date"`
	ShelfLifeLossHours      float64 `json:"shelf_life_loss_hours"`
	ShelfLifeLossPercent    float64 `json:"shelf_life_loss_percent"`
	RemainingShelfLifeHours float64 `json:"remaining_shelf_life_hours"`
	Status                  string  `json:"status"`
}

func (h *Handler) thermalExposure(w http.ResponseWriter, r *http.Request) {
	s, ok := h.shipment(w, r)
	if !ok {
		return
	}
	// The exposure of a shipment that is no longer monitored ends with its monitoring.
	until := h.now().UTC()
	if s.MonitoringEndedAt != nil && s.MonitoringEndedAt.Before(until) {
		until = s.MonitoringEndedAt.UTC()
	}
	e, err := exposure.Measure(r.Context(), h.pool, s.Exposure(), until)
	if err != nil {
		rest.InternalError(w, r, h.logger, err)
		return
	}
	body := ThermalExposure{
		SSCC: e.SSCC, AssessedAt: e.AssessedAt, MonitoringEnded: e.MonitoringEnded, MinTempCelsius: e.MinTempCelsius,
		MaxTempCelsius: e.MaxTempCelsius, ReadingCount: e.ReadingCount, FirstReadingAt: e.FirstReadingAt,
		LastReadingAt: e.LastReadingAt, CoveredSeconds: seconds(e.CoveredSeconds), UncoveredSeconds: seconds(e.UncoveredSeconds),
		MeanTemperatureCelsius: round(e.MeanTemperatureCelsius, 2), MeanKineticTemperatureCelsius: round(e.MeanKineticTemperatureCelsius, 2),
		LowestTemperatureCelsius: e.LowestTemperatureCelsius, HighestTemperatureCelsius: e.HighestTemperatureCelsius,
		TimeAboveMaxSeconds: seconds(e.TimeAboveMaxSeconds), TimeBelowMinSeconds: seconds(e.TimeBelowMinSeconds),
		DegreeMinutesAboveMax: exposure.Round2(e.DegreeMinutesAboveMax), DegreeMinutesBelowMin: exposure.Round2(e.DegreeMinutesBelowMin),
	}
	if sl := e.ShelfLife; sl != nil {
		body.ShelfLife = &ShelfLife{
			ShelfLifeQ10: sl.Q10, ProductionDate: sl.ProductionDate.Format(time.DateOnly),
			LabelExpirationDate:     sl.LabelExpirationDate.Format(time.DateOnly),
			EstimatedExpirationDate: sl.EstimatedExpirationDate.Format(time.DateOnly),
			ShelfLifeLossHours:      exposure.Round2(sl.ExtraAging.Hours()), ShelfLifeLossPercent: exposure.Round2(sl.LossPercent),
			RemainingShelfLifeHours: exposure.Round2(sl.Remaining.Hours()), Status: string(sl.Status),
		}
	}
	httpx.WriteJSON(w, r, http.StatusOK, body)
}

// seconds rounds a number of seconds to whole seconds.
func seconds(v float64) int {
	return int(math.Round(v))
}
