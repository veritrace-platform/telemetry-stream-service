// Package api serves the telemetry read API (rest-api.md §3.3): a shipment's readings at raw, 1-minute, or
// 15-minute resolution, and cold-chain incidents per shipment and across the caller's shipments. Every read is
// limited to the shipments that the caller may view (access.View).
package api

import (
	"errors"
	"log/slog"
	"math"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/veritrace-platform/telemetry-stream-service/internal/access"
	"github.com/veritrace-platform/telemetry-stream-service/internal/auth"
	"github.com/veritrace-platform/telemetry-stream-service/internal/incident"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/httpx"
	"github.com/veritrace-platform/telemetry-stream-service/internal/projection"
	"github.com/veritrace-platform/telemetry-stream-service/internal/reading/queries"
	"github.com/veritrace-platform/telemetry-stream-service/internal/rest"
)

// Resolutions of readings, with the window that a request covers by default and at most.
const (
	ResolutionRaw         = "raw"
	ResolutionMinute      = "1m"
	ResolutionQuarterHour = "15m"
)

var (
	defaultWindow = map[string]time.Duration{
		ResolutionRaw: time.Hour, ResolutionMinute: 24 * time.Hour, ResolutionQuarterHour: 7 * 24 * time.Hour,
	}
	maxWindow = map[string]time.Duration{
		ResolutionRaw: 6 * time.Hour, ResolutionMinute: 7 * 24 * time.Hour, ResolutionQuarterHour: 90 * 24 * time.Hour,
	}
	windowLimit = map[string]string{
		ResolutionRaw: "6 hours", ResolutionMinute: "7 days", ResolutionQuarterHour: "90 days",
	}
)

// recentWindow is the period of the summary's last_24h_count.
const recentWindow = 24 * time.Hour

// Handler serves the read API.
type Handler struct {
	pool         *pgxpool.Pool
	authenticate func(http.Handler) http.Handler
	logger       *slog.Logger
	now          func() time.Time
}

// NewHandler returns the read API on the runtime role's pool; authenticate requires a bearer token.
func NewHandler(pool *pgxpool.Pool, authenticate func(http.Handler) http.Handler, logger *slog.Logger,
	now func() time.Time,
) *Handler {
	return &Handler{pool: pool, authenticate: authenticate, logger: logger, now: now}
}

// Routes registers the endpoints under /api/v1.
func (h *Handler) Routes(r chi.Router) {
	r.Route("/telemetry", func(r chi.Router) {
		r.Use(h.authenticate)
		r.Get("/shipments/{sscc}/readings", h.readings)
		r.Get("/shipments/{sscc}/incidents", h.shipmentIncidents)
		r.Get("/incidents", h.incidents)
		r.Get("/incidents/summary", h.summary)
	})
}

// shipment returns the projection of the SSCC in the path if the caller may view it, and otherwise answers 404 or
// 403 and reports false.
func (h *Handler) shipment(w http.ResponseWriter, r *http.Request) (projection.Shipment, bool) {
	p, ok := auth.Caller(w, r)
	if !ok {
		return projection.Shipment{}, false
	}
	sscc, ok := rest.SSCC(w, r)
	if !ok {
		return projection.Shipment{}, false
	}
	s, err := projection.NewStore(h.pool).Get(r.Context(), sscc)
	if errors.Is(err, projection.ErrNotFound) {
		rest.NotFound(w, r, "shipment")
		return projection.Shipment{}, false
	}
	if err != nil {
		rest.InternalError(w, r, h.logger, err)
		return projection.Shipment{}, false
	}
	switch err := access.View(p, s); {
	case errors.Is(err, access.ErrNotFound):
		rest.NotFound(w, r, "shipment")
		return projection.Shipment{}, false
	case errors.Is(err, access.ErrForbidden):
		rest.Forbidden(w, r, err.Error())
		return projection.Shipment{}, false
	}
	return s, true
}

// ReadingSeries is the body of the readings endpoint. Items are Reading for raw resolution and Bucket otherwise,
// oldest first.
type ReadingSeries struct {
	SSCC       string    `json:"sscc"`
	Resolution string    `json:"resolution"`
	From       time.Time `json:"from"`
	To         time.Time `json:"to"`
	Items      any       `json:"items"`
}

// Reading is one stored reading.
type Reading struct {
	RecordedAt         time.Time `json:"recorded_at"`
	DeviceID           string    `json:"device_id"`
	TemperatureCelsius float64   `json:"temperature_celsius"`
	HumidityPercent    *float64  `json:"humidity_percent"`
	Latitude           float64   `json:"latitude"`
	Longitude          float64   `json:"longitude"`
}

// Bucket aggregates the readings of one interval.
type Bucket struct {
	Bucket                time.Time `json:"bucket"`
	AvgTemperatureCelsius float64   `json:"avg_temperature_celsius"`
	MinTemperatureCelsius float64   `json:"min_temperature_celsius"`
	MaxTemperatureCelsius float64   `json:"max_temperature_celsius"`
	ReadingCount          int64     `json:"reading_count"`
}

func (h *Handler) readings(w http.ResponseWriter, r *http.Request) {
	s, ok := h.shipment(w, r)
	if !ok {
		return
	}
	q := rest.NewQuery(r)
	resolution := q.Enum("resolution", ResolutionRaw, ResolutionRaw, ResolutionMinute, ResolutionQuarterHour)
	to := q.Time("to", h.now().UTC())
	from := q.Time("from", to.Add(-defaultWindow[resolution]))
	if len(q.Errors) == 0 {
		switch {
		case !from.Before(to):
			q.Fail("from", httpx.FieldInvalidValue, "must be before to")
		case to.Sub(from) > maxWindow[resolution]:
			q.Fail("from", httpx.FieldOutOfRange, "the window of "+resolution+" readings is at most "+windowLimit[resolution])
		}
	}
	if problem := q.Problem(); problem != nil {
		httpx.WriteProblem(w, r, *problem)
		return
	}

	series := ReadingSeries{SSCC: s.SSCC, Resolution: resolution, From: from, To: to}
	db := queries.New(h.pool)
	var err error
	switch resolution {
	case ResolutionRaw:
		var rows []queries.TelemetrySensorReading
		rows, err = db.ReadingsBetween(r.Context(), queries.ReadingsBetweenParams{Sscc: s.SSCC, FromTime: from, Before: to})
		items := make([]Reading, len(rows))
		for i, row := range rows {
			items[i] = Reading{
				RecordedAt: row.RecordedAt, DeviceID: row.DeviceID, TemperatureCelsius: row.TemperatureCelsius,
				HumidityPercent: row.HumidityPercent, Latitude: row.Latitude, Longitude: row.Longitude,
			}
		}
		series.Items = items
	case ResolutionMinute:
		var rows []queries.MinuteBucketsRow
		rows, err = db.MinuteBuckets(r.Context(), queries.MinuteBucketsParams{Sscc: s.SSCC, FromTime: from, Before: to})
		items := make([]Bucket, len(rows))
		for i, row := range rows {
			items[i] = bucket(row.Bucket, row.AvgTemperatureCelsius, row.MinTemperatureCelsius, row.MaxTemperatureCelsius,
				row.ReadingCount)
		}
		series.Items = items
	case ResolutionQuarterHour:
		var rows []queries.QuarterHourBucketsRow
		rows, err = db.QuarterHourBuckets(r.Context(), queries.QuarterHourBucketsParams{Sscc: s.SSCC, FromTime: from, Before: to})
		items := make([]Bucket, len(rows))
		for i, row := range rows {
			items[i] = bucket(row.Bucket, row.AvgTemperatureCelsius, row.MinTemperatureCelsius, row.MaxTemperatureCelsius,
				row.ReadingCount)
		}
		series.Items = items
	}
	if err != nil {
		rest.InternalError(w, r, h.logger, err)
		return
	}
	httpx.WriteJSON(w, r, http.StatusOK, series)
}

// bucket rounds the average to the two decimals of stored temperatures.
func bucket(start time.Time, avg, minimum, maximum float64, count int64) Bucket {
	return Bucket{
		Bucket: start, AvgTemperatureCelsius: math.Round(avg*100) / 100, MinTemperatureCelsius: minimum,
		MaxTemperatureCelsius: maximum, ReadingCount: count,
	}
}

// Incident is an incident with its shipment's product and lot.
type Incident struct {
	ID                        uuid.UUID  `json:"id"`
	ShipmentID                uuid.UUID  `json:"shipment_id"`
	SSCC                      string     `json:"sscc"`
	DeviceID                  string     `json:"device_id"`
	StartedAt                 time.Time  `json:"started_at"`
	ConfirmedAt               time.Time  `json:"confirmed_at"`
	EndedAt                   *time.Time `json:"ended_at"`
	DurationSeconds           *int       `json:"duration_seconds"`
	MinTempCelsius            float64    `json:"min_temp_celsius"`
	MaxTempCelsius            float64    `json:"max_temp_celsius"`
	TriggerTemperatureCelsius float64    `json:"trigger_temperature_celsius"`
	ExtremeTemperatureCelsius float64    `json:"extreme_temperature_celsius"`
	Latitude                  float64    `json:"latitude"`
	Longitude                 float64    `json:"longitude"`
	IncidentHash              string     `json:"incident_hash"`
	ProductName               string     `json:"product_name"`
	GTIN                      string     `json:"gtin"`
	LotNumber                 string     `json:"lot_number"`
}

func incidentOf(l incident.Listed) Incident {
	return Incident{
		ID: l.ID, ShipmentID: l.ShipmentID, SSCC: l.SSCC, DeviceID: l.DeviceID, StartedAt: l.StartedAt,
		ConfirmedAt: l.ConfirmedAt, EndedAt: l.EndedAt, DurationSeconds: l.DurationSeconds,
		MinTempCelsius: l.MinTempCelsius, MaxTempCelsius: l.MaxTempCelsius,
		TriggerTemperatureCelsius: l.TriggerTemperatureCelsius, ExtremeTemperatureCelsius: l.ExtremeTemperatureCelsius,
		Latitude: l.Latitude, Longitude: l.Longitude, IncidentHash: l.IncidentHash, ProductName: l.ProductName,
		GTIN: l.GTIN, LotNumber: l.LotNumber,
	}
}

// page reads limit and cursor; on errors it answers 400 and reports false.
func page(w http.ResponseWriter, r *http.Request, q *rest.Query) (incident.Page, bool) {
	p, errs := httpx.ParsePage(r)
	q.Errors = append(q.Errors, errs...)
	result := incident.Page{Limit: p.Limit + 1}
	if p.Cursor != "" {
		after, err := httpx.ParseUUIDCursor(p.Cursor)
		if err != nil {
			q.Errors = append(q.Errors, httpx.InvalidCursorError())
		}
		result.After = &after
	}
	if problem := q.Problem(); problem != nil {
		httpx.WriteProblem(w, r, *problem)
		return incident.Page{}, false
	}
	return result, true
}

// collection answers a page of incidents, fetched with one more than the limit.
func collection(w http.ResponseWriter, r *http.Request, listed []incident.Listed, limit int) {
	items := make([]Incident, len(listed))
	for i, l := range listed {
		items[i] = incidentOf(l)
	}
	httpx.WriteJSON(w, r, http.StatusOK, httpx.NewCollection(items, limit, func(i Incident) string {
		return httpx.UUIDCursor(i.ID)
	}))
}

func (h *Handler) shipmentIncidents(w http.ResponseWriter, r *http.Request) {
	s, ok := h.shipment(w, r)
	if !ok {
		return
	}
	pg, ok := page(w, r, rest.NewQuery(r))
	if !ok {
		return
	}
	listed, err := incident.NewStore(h.pool).ShipmentIncidents(r.Context(), s.SSCC, pg)
	if err != nil {
		rest.InternalError(w, r, h.logger, err)
		return
	}
	collection(w, r, listed, pg.Limit-1)
}

func (h *Handler) incidents(w http.ResponseWriter, r *http.Request) {
	p, ok := auth.Caller(w, r)
	if !ok {
		return
	}
	q := rest.NewQuery(r)
	state := q.Enum("state", "", incident.StateOpen, incident.StateResolved)
	pg, ok := page(w, r, q)
	if !ok {
		return
	}
	scope := incident.Scope{TenantID: p.TenantID, DriverID: access.AssignedTo(p)}
	listed, err := incident.NewStore(h.pool).VisibleIncidents(r.Context(), scope, state, pg)
	if err != nil {
		rest.InternalError(w, r, h.logger, err)
		return
	}
	collection(w, r, listed, pg.Limit-1)
}

// Summary is the body of the summary endpoint.
type Summary struct {
	OpenCount    int `json:"open_count"`
	Last24hCount int `json:"last_24h_count"`
}

func (h *Handler) summary(w http.ResponseWriter, r *http.Request) {
	p, ok := auth.Caller(w, r)
	if !ok {
		return
	}
	scope := incident.Scope{TenantID: p.TenantID, DriverID: access.AssignedTo(p)}
	s, err := incident.NewStore(h.pool).Summarize(r.Context(), scope, h.now().Add(-recentWindow))
	if err != nil {
		rest.InternalError(w, r, h.logger, err)
		return
	}
	httpx.WriteJSON(w, r, http.StatusOK, Summary{OpenCount: s.Open, Last24hCount: s.Recent})
}
