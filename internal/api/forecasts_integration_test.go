//go:build integration

package api_test

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/veritrace-platform/telemetry-stream-service/internal/api"
	"github.com/veritrace-platform/telemetry-stream-service/internal/auth"
	"github.com/veritrace-platform/telemetry-stream-service/internal/dbtest"
	"github.com/veritrace-platform/telemetry-stream-service/internal/forecast"
	"github.com/veritrace-platform/telemetry-stream-service/internal/incident"
	"github.com/veritrace-platform/telemetry-stream-service/internal/warning"
)

// warning stores a warning issued at issuedAt that predicts an excursion predictIn later; a closed outcome closes
// it, as BREACHED by breach or as CLEARED three minutes after its issue.
func (f *fixture) warning(t *testing.T, s dbtest.Shipment, issuedAt time.Time, predictIn time.Duration, breach *incident.Incident,
	cleared bool,
) warning.Warning {
	t.Helper()
	w := warning.Warning{
		ID: warning.NewID(s.SSCC, issuedAt), ShipmentID: s.ShipmentID, SSCC: s.SSCC, DeviceID: "REEFER-0001",
		IssuedAt: issuedAt, Bound: forecast.Max, BoundCelsius: 8, TemperatureCelsius: 6.4, SlopeCelsiusPerMinute: 0.62,
		PredictedExcursionAt: issuedAt.Add(predictIn),
	}
	store := warning.NewStore(f.db.Owner)
	if _, err := store.Insert(t.Context(), w); err != nil {
		t.Fatal(err)
	}
	switch {
	case breach != nil:
		if _, _, err := store.CloseBreached(t.Context(), s.SSCC, breach.ID, breach.StartedAt, breach.ConfirmedAt); err != nil {
			t.Fatal(err)
		}
	case cleared:
		if _, _, err := store.Close(t.Context(), w.ID, warning.Cleared, issuedAt.Add(3*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	return w
}

func TestWarnings(t *testing.T) {
	f := newFixture(t)
	owner, carrier := uuid.New(), uuid.New()
	driver := uuid.New()
	first := f.db.Project(t, dbtest.Shipment{SSCC: "089300010000000018", OwnerTenantID: owner, Participants: []uuid.UUID{carrier}, AssignedDriverID: &driver})
	second := f.db.Project(t, dbtest.Shipment{SSCC: "089300010000000025", OwnerTenantID: owner})
	other := f.db.Project(t, dbtest.Shipment{SSCC: "089300010000000032"})

	// first: a warning 4 minutes before a breach (predicted 60 s late), a cleared warning, and a breach that no
	// warning preceded. second: a warning 2 minutes before a breach (predicted 30 s late), and an open warning.
	breach1 := f.incident(t, first, f.now.Add(-3*time.Hour+4*time.Minute), 5*time.Minute)
	w1 := f.warning(t, first, f.now.Add(-3*time.Hour), 5*time.Minute, &breach1, false)
	w2 := f.warning(t, first, f.now.Add(-2*time.Hour), 5*time.Minute, nil, true)
	f.incident(t, first, f.now.Add(-30*time.Minute), 2*time.Minute)
	breach3 := f.incident(t, second, f.now.Add(-time.Hour+2*time.Minute), 5*time.Minute)
	w3 := f.warning(t, second, f.now.Add(-time.Hour), 150*time.Second, &breach3, false)
	w4 := f.warning(t, second, f.now.Add(-10*time.Minute), 5*time.Minute, nil, false)
	f.warning(t, other, f.now.Add(-5*time.Minute), 5*time.Minute, nil, false)

	type page struct {
		Items      []api.Warning
		NextCursor *string `json:"next_cursor"`
	}
	ids := func(p page) []uuid.UUID {
		var out []uuid.UUID
		for _, w := range p.Items {
			out = append(out, w.ID)
		}
		return out
	}
	admin := principal(owner, auth.RoleAdmin)

	t.Run("across the caller's shipments, newest first, filtered by state", func(t *testing.T) {
		var got page
		if status, code := f.get(t, admin, "/telemetry/warnings", nil, &got); status != http.StatusOK {
			t.Fatalf("status = %d %s", status, code)
		}
		if want := []uuid.UUID{w4.ID, w3.ID, w2.ID, w1.ID}; fmtIDs(ids(got)) != fmtIDs(want) {
			t.Fatalf("warnings = %v, want %v", ids(got), want)
		}
		breached, open := got.Items[1], got.Items[0]
		if breached.Outcome == nil || *breached.Outcome != "BREACHED" || breached.IncidentID == nil || *breached.IncidentID != breach3.ID ||
			breached.LeadTimeSeconds == nil || *breached.LeadTimeSeconds != 120 || breached.ClosedAt == nil ||
			!breached.ClosedAt.Equal(breach3.ConfirmedAt) || breached.Bound != "MAX" || breached.ProductName != "Fresh milk 1 L" {
			t.Errorf("breached warning = %+v", breached)
		}
		if open.Outcome != nil || open.ClosedAt != nil || open.LeadTimeSeconds != nil || open.SlopeCelsiusPerMinute != 0.62 {
			t.Errorf("open warning = %+v", open)
		}

		var openOnly, closed page
		f.get(t, admin, "/telemetry/warnings", url.Values{"state": {"open"}}, &openOnly)
		f.get(t, admin, "/telemetry/warnings", url.Values{"state": {"closed"}, "limit": {"2"}}, &closed)
		if fmtIDs(ids(openOnly)) != fmtIDs([]uuid.UUID{w4.ID}) || fmtIDs(ids(closed)) != fmtIDs([]uuid.UUID{w3.ID, w2.ID}) ||
			closed.NextCursor == nil {
			t.Errorf("open = %v, closed = %v (next %v)", ids(openOnly), ids(closed), closed.NextCursor)
		}
		if status, _ := f.get(t, admin, "/telemetry/warnings", url.Values{"state": {"resolved"}}, nil); status != http.StatusBadRequest {
			t.Errorf("state=resolved: status = %d", status)
		}
	})

	t.Run("of one shipment, and for a driver", func(t *testing.T) {
		var got page
		f.get(t, principal(carrier, auth.RoleWarehouseManager), "/telemetry/shipments/"+first.SSCC+"/warnings", nil, &got)
		if fmtIDs(ids(got)) != fmtIDs([]uuid.UUID{w2.ID, w1.ID}) {
			t.Errorf("shipment warnings = %v", ids(got))
		}
		if status, _ := f.get(t, principal(carrier, auth.RoleAdmin), "/telemetry/shipments/"+second.SSCC+"/warnings", nil, nil); status != http.StatusNotFound {
			t.Errorf("another tenant's shipment: status = %d", status)
		}
		f.get(t, &auth.Principal{UserID: driver, TenantID: carrier, Role: auth.RoleDriver}, "/telemetry/warnings", nil, &got)
		if fmtIDs(ids(got)) != fmtIDs([]uuid.UUID{w2.ID, w1.ID}) {
			t.Errorf("driver warnings = %v", ids(got))
		}
	})

	t.Run("forecast quality", func(t *testing.T) {
		var got api.Performance
		if status, code := f.get(t, admin, "/telemetry/warnings/performance", nil, &got); status != http.StatusOK {
			t.Fatalf("status = %d %s", status, code)
		}
		w := got.Warnings
		if w.Issued != 4 || w.Open != 1 || w.Breached != 2 || w.Cleared != 1 || w.Inconclusive != 0 ||
			got.Incidents.Count != 3 || got.Incidents.Warned != 2 || got.SSCC != nil || !got.To.Equal(f.now) ||
			got.To.Sub(got.From) != 7*24*time.Hour {
			t.Errorf("performance = %+v", got)
		}
		if got.Precision == nil || *got.Precision != 0.6667 || got.Recall == nil || *got.Recall != 0.6667 {
			t.Errorf("precision %v, recall %v; want 2/3 each", got.Precision, got.Recall)
		}
		if l := got.LeadTimeSeconds; l == nil || l.Median != 180 || l.Min != 120 || l.Mean != 180 {
			t.Errorf("lead times = %+v, want median 180, min 120, mean 180", l)
		}
		if e := got.MeanExcursionTimeErrorSeconds; e == nil || *e != 45 {
			t.Errorf("excursion time error = %v, want 45", e)
		}

		var one api.Performance
		f.get(t, admin, "/telemetry/warnings/performance", url.Values{"sscc": {second.SSCC}}, &one)
		if one.SSCC == nil || *one.SSCC != second.SSCC || one.Warnings.Issued != 2 || one.Incidents.Count != 1 ||
			*one.Precision != 1 || *one.Recall != 1 || one.LeadTimeSeconds.Median != 120 {
			t.Errorf("performance of one shipment = %+v", one)
		}

		// The last 90 minutes hold the second shipment's breached warning and the first shipment's unwarned breach.
		var recent api.Performance
		f.get(t, admin, "/telemetry/warnings/performance", url.Values{"from": {f.now.Add(-90 * time.Minute).Format(time.RFC3339)}}, &recent)
		if recent.Warnings.Issued != 2 || recent.Incidents.Count != 2 || recent.Incidents.Warned != 1 || *recent.Recall != 0.5 {
			t.Errorf("performance of the last 90 minutes = %+v", recent)
		}

		var none api.Performance
		f.get(t, principal(uuid.New(), auth.RoleAdmin), "/telemetry/warnings/performance", nil, &none)
		if none.Warnings.Issued != 0 || none.Precision != nil || none.Recall != nil || none.LeadTimeSeconds != nil ||
			none.MeanExcursionTimeErrorSeconds != nil {
			t.Errorf("performance without warnings = %+v", none)
		}
	})

	t.Run("invalid parameters", func(t *testing.T) {
		for name, tc := range map[string]struct {
			query  url.Values
			status int
			code   string
		}{
			"from after to":        {url.Values{"from": {f.now.Format(time.RFC3339)}, "to": {f.now.Add(-time.Hour).Format(time.RFC3339)}}, http.StatusBadRequest, "VALIDATION_FAILED/from:INVALID_VALUE"},
			"window over 90 days":  {url.Values{"from": {f.now.Add(-91 * 24 * time.Hour).Format(time.RFC3339)}}, http.StatusBadRequest, "VALIDATION_FAILED/from:OUT_OF_RANGE"},
			"malformed SSCC":       {url.Values{"sscc": {"089300010000000019"}}, http.StatusUnprocessableEntity, "INVALID_GS1_IDENTIFIER/sscc:CHECK_DIGIT"},
			"another tenant's":     {url.Values{"sscc": {other.SSCC}}, http.StatusNotFound, "NOT_FOUND"},
			"malformed timestamps": {url.Values{"to": {"yesterday"}}, http.StatusBadRequest, "VALIDATION_FAILED/to:INVALID_FORMAT"},
		} {
			if status, code := f.get(t, admin, "/telemetry/warnings/performance", tc.query, nil); status != tc.status || code != tc.code {
				t.Errorf("%s: %d %s, want %d %s", name, status, code, tc.status, tc.code)
			}
		}
	})
}

func TestThermalExposure(t *testing.T) {
	f := newFixture(t)
	owner := uuid.New()
	production, expiration, q10 := f.now.AddDate(0, 0, -1).Truncate(24*time.Hour), f.now.AddDate(0, 0, 6).Truncate(24*time.Hour), 3.0
	monitored := f.db.Project(t, dbtest.Shipment{
		SSCC: "089300010000000018", OwnerTenantID: owner, ProductionDate: &production, ExpirationDate: &expiration, ShelfLifeQ10: &q10,
	})
	legacy := f.db.Project(t, dbtest.Shipment{SSCC: "089300010000000025", OwnerTenantID: owner})
	start := f.now.Add(-10 * time.Minute)
	// One minute at 4.5 °C, then one minute at 12 °C: 60 s above 8 °C.
	var seconds []int
	var temperatures []float64
	for s := 0; s <= 120; s += 5 {
		seconds = append(seconds, s)
		temperatures = append(temperatures, map[bool]float64{true: 4.5, false: 12}[s < 60])
	}
	f.readings(t, monitored.SSCC, start, seconds, temperatures)
	f.readings(t, legacy.SSCC, start, seconds, temperatures)
	admin := principal(owner, auth.RoleAdmin)

	t.Run("while monitored, until now", func(t *testing.T) {
		var got api.ThermalExposure
		if status, code := f.get(t, admin, "/telemetry/shipments/"+monitored.SSCC+"/thermal-exposure", nil, &got); status != http.StatusOK {
			t.Fatalf("status = %d %s", status, code)
		}
		if !got.AssessedAt.Equal(f.now) || got.MonitoringEnded || got.ReadingCount != 25 || got.CoveredSeconds != 120 ||
			got.UncoveredSeconds != 0 || got.TimeAboveMaxSeconds != 60 || got.TimeBelowMinSeconds != 0 ||
			got.DegreeMinutesAboveMax != 4 || *got.MeanTemperatureCelsius != 8.25 || *got.HighestTemperatureCelsius != 12 ||
			*got.MeanKineticTemperatureCelsius <= 8.25 {
			t.Errorf("exposure = %+v", got)
		}
		sl := got.ShelfLife
		// 60 s × (3^0.4 − 1) of extra aging is negligible.
		if sl == nil || sl.Status != "INTACT" || sl.EstimatedExpirationDate != expiration.Format(time.DateOnly) ||
			sl.ShelfLifeQ10 != 3 || sl.ShelfLifeLossHours != 0.01 || sl.RemainingShelfLifeHours <= 0 {
			t.Errorf("shelf life = %+v", sl)
		}
	})

	t.Run("after monitoring ended, until its end", func(t *testing.T) {
		if _, err := f.db.Owner.Exec(t.Context(), `UPDATE telemetry.shipment_projection SET status = 'DELIVERED',
			monitoring_ended_at = $2 WHERE sscc = $1`, monitored.SSCC, start.Add(30*time.Second)); err != nil {
			t.Fatal(err)
		}
		var got api.ThermalExposure
		f.get(t, admin, "/telemetry/shipments/"+monitored.SSCC+"/thermal-exposure", nil, &got)
		if !got.AssessedAt.Equal(start.Add(30*time.Second)) || !got.MonitoringEnded || got.ReadingCount != 7 ||
			got.CoveredSeconds != 30 || got.TimeAboveMaxSeconds != 0 {
			t.Errorf("exposure after delivery = %+v", got)
		}
	})

	t.Run("without the lot's dates, no shelf life", func(t *testing.T) {
		var got api.ThermalExposure
		f.get(t, admin, "/telemetry/shipments/"+legacy.SSCC+"/thermal-exposure", nil, &got)
		if got.ShelfLife != nil || got.ReadingCount != 25 {
			t.Errorf("exposure = %+v", got)
		}
	})

	t.Run("who may read", func(t *testing.T) {
		if status, _ := f.get(t, principal(uuid.New(), auth.RoleAdmin), "/telemetry/shipments/"+monitored.SSCC+"/thermal-exposure", nil, nil); status != http.StatusNotFound {
			t.Errorf("another tenant: status = %d", status)
		}
		if status, _ := f.get(t, nil, "/telemetry/shipments/"+monitored.SSCC+"/thermal-exposure", nil, nil); status != http.StatusUnauthorized {
			t.Errorf("no token: status = %d", status)
		}
	})
}
