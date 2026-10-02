//go:build integration

package api_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/veritrace-platform/telemetry-stream-service/internal/api"
	"github.com/veritrace-platform/telemetry-stream-service/internal/auth"
	"github.com/veritrace-platform/telemetry-stream-service/internal/auth/authtest"
	"github.com/veritrace-platform/telemetry-stream-service/internal/dbtest"
	"github.com/veritrace-platform/telemetry-stream-service/internal/httpapi"
	"github.com/veritrace-platform/telemetry-stream-service/internal/incident"
	"github.com/veritrace-platform/telemetry-stream-service/internal/reading"
)

type fixture struct {
	db     *dbtest.Database
	issuer *authtest.Issuer
	server *httptest.Server
	now    time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	db := dbtest.Start(t)
	issuer := authtest.NewIssuer(t)
	now := time.Now().UTC().Truncate(time.Second)
	verifier := auth.NewVerifier(auth.NewJWKS(issuer.URL, http.DefaultClient, slog.New(slog.DiscardHandler), time.Now), time.Now)
	handler := api.NewHandler(db.App, verifier.Middleware, slog.New(slog.DiscardHandler), func() time.Time { return now })
	server := httptest.NewServer(httpapi.NewRouter(slog.New(slog.DiscardHandler), prometheus.NewRegistry(),
		httpapi.Mounts{API: []httpapi.Routes{handler.Routes}}))
	t.Cleanup(server.Close)
	return &fixture{db: db, issuer: issuer, server: server, now: now}
}

// get calls the API as p and decodes the body into out; it returns the status and the problem code, if any.
func (f *fixture) get(t *testing.T, p *auth.Principal, path string, query url.Values, out any) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, f.server.URL+"/api/v1"+path+"?"+query.Encode(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if p != nil {
		req.Header.Set("Authorization", "Bearer "+f.issuer.Token(t, *p))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		var problem struct {
			Code   string `json:"code"`
			Errors []struct {
				Field string `json:"field"`
				Code  string `json:"code"`
			} `json:"errors"`
		}
		_ = json.Unmarshal(body, &problem)
		code := problem.Code
		if len(problem.Errors) > 0 {
			code += "/" + problem.Errors[0].Field + ":" + problem.Errors[0].Code
		}
		return resp.StatusCode, code
	}
	if out != nil {
		if err := json.Unmarshal(body, out); err != nil {
			t.Fatalf("decode %s: %v", body, err)
		}
	}
	return resp.StatusCode, ""
}

func principal(tenant uuid.UUID, role auth.Role) *auth.Principal {
	return &auth.Principal{UserID: uuid.New(), TenantID: tenant, Role: role}
}

func (f *fixture) readings(t *testing.T, sscc string, start time.Time, seconds []int, temperatures []float64) {
	t.Helper()
	var rs []reading.Reading
	for i, s := range seconds {
		at := start.Add(time.Duration(s) * time.Second)
		rs = append(rs, reading.Reading{
			DeviceID: "REEFER-0001", SSCC: sscc, RecordedAt: at, ReceivedAt: at, TemperatureCelsius: temperatures[i],
			Latitude: 10.87, Longitude: 106.8,
		})
	}
	if _, err := reading.NewStore(f.db.App).Insert(t.Context(), rs); err != nil {
		t.Fatal(err)
	}
}

// incident stores an incident that started at start; a positive duration ends it.
func (f *fixture) incident(t *testing.T, s dbtest.Shipment, start time.Time, duration time.Duration) incident.Incident {
	t.Helper()
	i := incident.Incident{
		ID: incident.NewID(s.SSCC, start), ShipmentID: s.ShipmentID, SSCC: s.SSCC, DeviceID: "REEFER-0001",
		StartedAt: start, ConfirmedAt: start.Add(30 * time.Second), MinTempCelsius: 2, MaxTempCelsius: 8,
		TriggerTemperatureCelsius: 9, ExtremeTemperatureCelsius: 9.5, Latitude: 10.87, Longitude: 106.8,
	}
	i.IncidentHash, _ = i.Confirmation().Hash()
	store := incident.NewStore(f.db.Owner)
	if _, err := store.Insert(t.Context(), i); err != nil {
		t.Fatal(err)
	}
	if duration > 0 {
		end := start.Add(duration)
		seconds := incident.Duration(start, end)
		i.EndedAt, i.DurationSeconds = &end, &seconds
		if _, err := store.Resolve(t.Context(), i); err != nil {
			t.Fatal(err)
		}
	}
	return i
}

func TestReadings(t *testing.T) {
	f := newFixture(t)
	owner, carrier := uuid.New(), uuid.New()
	driver := uuid.New()
	s := f.db.Project(t, dbtest.Shipment{
		SSCC: "089300010000000018", OwnerTenantID: owner, Participants: []uuid.UUID{carrier}, AssignedDriverID: &driver,
	})
	start := f.now.Add(-2 * time.Hour).Truncate(time.Hour)
	f.readings(t, s.SSCC, start, []int{0, 30, 61, 900, 959}, []float64{4, 6, 9.25, 3, 5})
	window := url.Values{"from": {start.Format(time.RFC3339)}, "to": {start.Add(time.Hour).Format(time.RFC3339)}}

	t.Run("raw", func(t *testing.T) {
		var series struct {
			SSCC, Resolution string
			From, To         time.Time
			Items            []api.Reading
		}
		q := url.Values{"from": {start.Add(30 * time.Second).Format(time.RFC3339)}, "to": {start.Add(900 * time.Second).Format(time.RFC3339)}}
		if status, code := f.get(t, principal(owner, auth.RoleAdmin), "/telemetry/shipments/"+s.SSCC+"/readings", q, &series); status != http.StatusOK {
			t.Fatalf("status = %d %s", status, code)
		}
		// [from, to): the reading at to is left out.
		if series.SSCC != s.SSCC || series.Resolution != "raw" || len(series.Items) != 2 ||
			series.Items[0].TemperatureCelsius != 6 || series.Items[1].TemperatureCelsius != 9.25 ||
			series.Items[0].DeviceID != "REEFER-0001" || series.Items[0].HumidityPercent != nil {
			t.Errorf("series = %+v", series)
		}
	})

	t.Run("per minute and per 15 minutes", func(t *testing.T) {
		for resolution, want := range map[string][]api.Bucket{
			"1m": {
				{Bucket: start, AvgTemperatureCelsius: 5, MinTemperatureCelsius: 4, MaxTemperatureCelsius: 6, ReadingCount: 2},
				{Bucket: start.Add(time.Minute), AvgTemperatureCelsius: 9.25, MinTemperatureCelsius: 9.25, MaxTemperatureCelsius: 9.25, ReadingCount: 1},
				{Bucket: start.Add(15 * time.Minute), AvgTemperatureCelsius: 4, MinTemperatureCelsius: 3, MaxTemperatureCelsius: 5, ReadingCount: 2},
			},
			"15m": {
				{Bucket: start, AvgTemperatureCelsius: 6.42, MinTemperatureCelsius: 4, MaxTemperatureCelsius: 9.25, ReadingCount: 3},
				{Bucket: start.Add(15 * time.Minute), AvgTemperatureCelsius: 4, MinTemperatureCelsius: 3, MaxTemperatureCelsius: 5, ReadingCount: 2},
			},
		} {
			var series struct{ Items []api.Bucket }
			q := url.Values{"resolution": {resolution}, "from": window["from"], "to": window["to"]}
			if status, code := f.get(t, principal(carrier, auth.RoleWarehouseManager), "/telemetry/shipments/"+s.SSCC+"/readings", q, &series); status != http.StatusOK {
				t.Fatalf("%s: status = %d %s", resolution, status, code)
			}
			if len(series.Items) != len(want) {
				t.Fatalf("%s: buckets = %+v", resolution, series.Items)
			}
			for i := range want {
				if !series.Items[i].Bucket.Equal(want[i].Bucket) || series.Items[i].AvgTemperatureCelsius != want[i].AvgTemperatureCelsius ||
					series.Items[i].MinTemperatureCelsius != want[i].MinTemperatureCelsius ||
					series.Items[i].MaxTemperatureCelsius != want[i].MaxTemperatureCelsius ||
					series.Items[i].ReadingCount != want[i].ReadingCount {
					t.Errorf("%s: bucket %d = %+v, want %+v", resolution, i, series.Items[i], want[i])
				}
			}
		}
	})

	t.Run("who may read", func(t *testing.T) {
		path := "/telemetry/shipments/" + s.SSCC + "/readings"
		assigned := &auth.Principal{UserID: driver, TenantID: carrier, Role: auth.RoleDriver}
		tests := []struct {
			name   string
			caller *auth.Principal
			path   string
			status int
			code   string
		}{
			{"participant inspector", principal(carrier, auth.RoleInspector), path, http.StatusOK, ""},
			{"assigned driver", assigned, path, http.StatusOK, ""},
			{"another driver of the carrier", principal(carrier, auth.RoleDriver), path, http.StatusForbidden, "FORBIDDEN"},
			{"another tenant", principal(uuid.New(), auth.RoleAdmin), path, http.StatusNotFound, "NOT_FOUND"},
			{"an SSCC the projection does not know", principal(owner, auth.RoleAdmin), "/telemetry/shipments/089300010000000025/readings", http.StatusNotFound, "NOT_FOUND"},
			{"a malformed SSCC", principal(owner, auth.RoleAdmin), "/telemetry/shipments/089300010000000019/readings", http.StatusUnprocessableEntity, "INVALID_GS1_IDENTIFIER/sscc:CHECK_DIGIT"},
			{"no token", nil, path, http.StatusUnauthorized, "UNAUTHENTICATED"},
		}
		for _, tt := range tests {
			if status, code := f.get(t, tt.caller, tt.path, window, nil); status != tt.status || code != tt.code {
				t.Errorf("%s: %d %s, want %d %s", tt.name, status, code, tt.status, tt.code)
			}
		}
	})

	t.Run("invalid parameters", func(t *testing.T) {
		admin := principal(owner, auth.RoleAdmin)
		path := "/telemetry/shipments/" + s.SSCC + "/readings"
		for name, tt := range map[string]struct {
			query url.Values
			code  string
		}{
			"raw window over 6 hours": {url.Values{"from": {start.Add(-7 * time.Hour).Format(time.RFC3339)}, "to": window["to"]}, "VALIDATION_FAILED/from:OUT_OF_RANGE"},
			"from after to":           {url.Values{"from": window["to"], "to": window["from"]}, "VALIDATION_FAILED/from:INVALID_VALUE"},
			"unknown resolution":      {url.Values{"resolution": {"5m"}}, "VALIDATION_FAILED/resolution:INVALID_VALUE"},
			"malformed time":          {url.Values{"from": {"yesterday"}}, "VALIDATION_FAILED/from:INVALID_FORMAT"},
		} {
			if status, code := f.get(t, admin, path, tt.query, nil); status != http.StatusBadRequest || code != tt.code {
				t.Errorf("%s: %d %s, want 400 %s", name, status, code, tt.code)
			}
		}
		// The default window ends now and covers the last hour.
		var series struct{ From, To time.Time }
		if status, _ := f.get(t, admin, path, nil, &series); status != http.StatusOK || !series.To.Equal(f.now) ||
			!series.From.Equal(f.now.Add(-time.Hour)) {
			t.Errorf("default window = %+v", series)
		}
	})
}

func TestIncidents(t *testing.T) {
	f := newFixture(t)
	owner, carrier := uuid.New(), uuid.New()
	driver := uuid.New()
	first := f.db.Project(t, dbtest.Shipment{SSCC: "089300010000000018", OwnerTenantID: owner, Participants: []uuid.UUID{carrier}, AssignedDriverID: &driver})
	second := f.db.Project(t, dbtest.Shipment{SSCC: "089300010000000025", OwnerTenantID: owner})
	other := f.db.Project(t, dbtest.Shipment{SSCC: "089300010000000032"})
	start := f.now.Add(-48 * time.Hour)
	old := f.incident(t, first, start, 5*time.Minute)                    // resolved two days ago
	recent := f.incident(t, first, f.now.Add(-time.Hour), 2*time.Minute) // resolved in the last 24 hours
	open := f.incident(t, second, f.now.Add(-10*time.Minute), 0)         // ongoing
	f.incident(t, other, f.now.Add(-5*time.Minute), 0)                   // another tenant's

	type page struct {
		Items      []api.Incident
		NextCursor *string `json:"next_cursor"`
	}
	ids := func(p page) []uuid.UUID {
		var out []uuid.UUID
		for _, i := range p.Items {
			out = append(out, i.ID)
		}
		return out
	}
	admin := principal(owner, auth.RoleAdmin)

	t.Run("across the caller's shipments, newest first", func(t *testing.T) {
		var got page
		if status, code := f.get(t, admin, "/telemetry/incidents", nil, &got); status != http.StatusOK {
			t.Fatalf("status = %d %s", status, code)
		}
		if want := []uuid.UUID{open.ID, recent.ID, old.ID}; fmtIDs(ids(got)) != fmtIDs(want) || got.NextCursor != nil {
			t.Errorf("incidents = %v, want %v", ids(got), want)
		}
		item := got.Items[0]
		if item.SSCC != second.SSCC || item.EndedAt != nil || item.DurationSeconds != nil || item.ProductName != "Fresh milk 1 L" ||
			item.GTIN != "08930001000018" || item.LotNumber != "L2026-09-30A" || item.IncidentHash != open.IncidentHash ||
			item.TriggerTemperatureCelsius != 9 || item.ExtremeTemperatureCelsius != 9.5 {
			t.Errorf("open incident = %+v", item)
		}
		if old := got.Items[2]; old.EndedAt == nil || *old.DurationSeconds != 300 {
			t.Errorf("resolved incident = %+v", old)
		}
	})

	t.Run("filtered by state, in pages", func(t *testing.T) {
		var openOnly, resolved page
		f.get(t, admin, "/telemetry/incidents", url.Values{"state": {"open"}}, &openOnly)
		f.get(t, admin, "/telemetry/incidents", url.Values{"state": {"resolved"}, "limit": {"1"}}, &resolved)
		if fmtIDs(ids(openOnly)) != fmtIDs([]uuid.UUID{open.ID}) || fmtIDs(ids(resolved)) != fmtIDs([]uuid.UUID{recent.ID}) ||
			resolved.NextCursor == nil {
			t.Fatalf("open = %v, resolved = %v (next %v)", ids(openOnly), ids(resolved), resolved.NextCursor)
		}
		var next page
		f.get(t, admin, "/telemetry/incidents", url.Values{"state": {"resolved"}, "limit": {"1"}, "cursor": {*resolved.NextCursor}}, &next)
		if fmtIDs(ids(next)) != fmtIDs([]uuid.UUID{old.ID}) || next.NextCursor != nil {
			t.Errorf("second page = %v (next %v)", ids(next), next.NextCursor)
		}
		for name, q := range map[string]url.Values{
			"state":  {"state": {"closed"}},
			"cursor": {"cursor": {"not-a-cursor"}},
			"limit":  {"limit": {"500"}},
		} {
			if status, _ := f.get(t, admin, "/telemetry/incidents", q, nil); status != http.StatusBadRequest {
				t.Errorf("invalid %s: status = %d", name, status)
			}
		}
	})

	t.Run("a driver sees the shipments assigned to it", func(t *testing.T) {
		var got page
		assigned := &auth.Principal{UserID: driver, TenantID: carrier, Role: auth.RoleDriver}
		f.get(t, assigned, "/telemetry/incidents", nil, &got)
		if fmtIDs(ids(got)) != fmtIDs([]uuid.UUID{recent.ID, old.ID}) {
			t.Errorf("driver incidents = %v", ids(got))
		}
		f.get(t, principal(carrier, auth.RoleDriver), "/telemetry/incidents", nil, &got)
		if len(got.Items) != 0 {
			t.Errorf("an unassigned driver sees %v", ids(got))
		}
	})

	t.Run("of one shipment", func(t *testing.T) {
		var got page
		if status, code := f.get(t, principal(carrier, auth.RoleWarehouseManager), "/telemetry/shipments/"+first.SSCC+"/incidents", nil, &got); status != http.StatusOK {
			t.Fatalf("status = %d %s", status, code)
		}
		if fmtIDs(ids(got)) != fmtIDs([]uuid.UUID{recent.ID, old.ID}) {
			t.Errorf("shipment incidents = %v", ids(got))
		}
		if status, _ := f.get(t, principal(carrier, auth.RoleAdmin), "/telemetry/shipments/"+second.SSCC+"/incidents", nil, nil); status != http.StatusNotFound {
			t.Errorf("another tenant's shipment: status = %d", status)
		}
	})

	t.Run("summary", func(t *testing.T) {
		var got api.Summary
		if status, code := f.get(t, admin, "/telemetry/incidents/summary", nil, &got); status != http.StatusOK {
			t.Fatalf("status = %d %s", status, code)
		}
		// Open: the ongoing one. Last 24 hours: the ongoing one and the one resolved an hour ago.
		if got.OpenCount != 1 || got.Last24hCount != 2 {
			t.Errorf("summary = %+v", got)
		}
		f.get(t, principal(uuid.New(), auth.RoleAdmin), "/telemetry/incidents/summary", nil, &got)
		if got.OpenCount != 0 || got.Last24hCount != 0 {
			t.Errorf("summary of a tenant without shipments = %+v", got)
		}
	})
}

func fmtIDs(ids []uuid.UUID) string {
	b, _ := json.Marshal(ids)
	return string(b)
}
