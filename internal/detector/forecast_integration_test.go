//go:build integration

package detector_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/veritrace-platform/telemetry-stream-service/internal/dbtest"
	"github.com/veritrace-platform/telemetry-stream-service/internal/envelope"
	"github.com/veritrace-platform/telemetry-stream-service/internal/exposure"
	"github.com/veritrace-platform/telemetry-stream-service/internal/reading"
	"github.com/veritrace-platform/telemetry-stream-service/internal/stream/kafkatest"
	"github.com/veritrace-platform/telemetry-stream-service/internal/warning"
)

// segment is a stretch of readings every step from one temperature to another.
type segment struct {
	duration time.Duration
	from, to float64
	step     time.Duration
}

// journey returns the readings of an SSCC through segments, from start, one every 5 s unless a segment says
// otherwise.
func journey(sscc string, start time.Time, segments ...segment) []reading.Reading {
	var rs []reading.Reading
	at := start
	for _, s := range segments {
		step := s.step
		if step == 0 {
			step = 5 * time.Second
		}
		n := int(s.duration / step)
		for i := range n {
			c := s.from + (s.to-s.from)*float64(i)/float64(n)
			rs = append(rs, reading.Reading{
				DeviceID: "SIM-1", SSCC: sscc, RecordedAt: at, ReceivedAt: at.Add(300 * time.Millisecond),
				TemperatureCelsius: float64(int(c*100+0.5)) / 100, Latitude: 10.87, Longitude: 106.8,
			})
			at = at.Add(step)
		}
	}
	return rs
}

// processInBatches evaluates readings in batches of 50, as the processor would receive them.
func (h *harness) processInBatches(t *testing.T, readings []reading.Reading) {
	t.Helper()
	for i := 0; i < len(readings); i += 50 {
		end := min(i+50, len(readings))
		if err := h.process(t, readings[i:end]...); err != nil {
			t.Fatalf("Evaluate() error = %v", err)
		}
	}
}

// forecasts returns the events of telemetry.forecasts about an SSCC, in order.
func (h *harness) forecasts(t *testing.T, sscc string) []envelope.Event {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(h.brokers...))
	if err != nil {
		t.Fatal(err)
	}
	ends, err := kadm.NewClient(client).ListEndOffsets(t.Context(), envelope.TopicForecasts)
	client.Close()
	if err != nil {
		t.Fatalf("list end offsets: %v", err)
	}
	total := 0
	ends.Each(func(o kadm.ListedOffset) { total += int(o.Offset) })
	var events []envelope.Event
	for _, r := range kafkatest.Consume(t, h.brokers, envelope.TopicForecasts, total) {
		if string(r.Key) != sscc {
			continue
		}
		e, err := envelope.Decode(r.Value)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, e)
	}
	return events
}

func types(events []envelope.Event) []string {
	var ts []string
	for _, e := range events {
		ts = append(ts, e.EventType)
	}
	return ts
}

func TestForecast(t *testing.T) {
	h := newHarness(t)
	start := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Second)

	t.Run("a warming load is warned, then breached", func(t *testing.T) {
		const sscc = "089300010000000070"
		h.db.Project(t, dbtest.Shipment{SSCC: sscc})
		h.processInBatches(t, journey(sscc, start,
			segment{duration: 10 * time.Minute, from: 4.5, to: 4.5},
			segment{duration: 12 * time.Minute, from: 4.5, to: 10.5},
			segment{duration: 2 * time.Minute, from: 10.5, to: 10.5},
			segment{duration: time.Minute, from: 5, to: 5},
		))
		events := h.forecasts(t, sscc)
		if got := types(events); len(got) != 2 || got[0] != warning.TypeIssued || got[1] != warning.TypeClosed {
			t.Fatalf("events = %v, want issued then closed", got)
		}
		var issued warning.Issue
		var closed warning.Closure
		if err := json.Unmarshal(events[0].Data, &issued); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(events[1].Data, &closed); err != nil {
			t.Fatal(err)
		}
		if issued.Bound != "MAX" || issued.BoundCelsius != 8 || closed.Outcome != warning.Breached ||
			closed.IncidentID == nil || closed.LeadTimeSeconds == nil || *closed.LeadTimeSeconds < 120 {
			t.Errorf("issued %+v, closed %+v", issued, closed)
		}
		if n := h.db.Count(t, `SELECT count(*) FROM telemetry.excursion_warnings w JOIN telemetry.cold_chain_incidents i
			ON i.id = w.incident_id WHERE w.sscc = $1 AND w.outcome = 'BREACHED'`, sscc); n != 1 {
			t.Errorf("breached warnings linked to their incident = %d, want 1", n)
		}
	})

	t.Run("a door opening is warned and cleared", func(t *testing.T) {
		const sscc = "089300010000000087"
		h.db.Project(t, dbtest.Shipment{SSCC: sscc})
		h.processInBatches(t, journey(sscc, start,
			segment{duration: 10 * time.Minute, from: 4.5, to: 4.5},
			segment{duration: 3 * time.Minute, from: 4.5, to: 7.2},
			segment{duration: 3 * time.Minute, from: 7.2, to: 4.5},
			segment{duration: 5 * time.Minute, from: 4.5, to: 4.5},
		))
		events := h.forecasts(t, sscc)
		var closed warning.Closure
		if len(events) == 2 {
			_ = json.Unmarshal(events[1].Data, &closed)
		}
		if got := types(events); len(got) != 2 || got[0] != warning.TypeIssued || closed.Outcome != warning.Cleared ||
			closed.IncidentID != nil || closed.LeadTimeSeconds != nil {
			t.Errorf("events = %v, closure %+v; want a warning cleared", got, closed)
		}
		if n := h.db.Count(t, `SELECT count(*) FROM telemetry.cold_chain_incidents WHERE sscc = $1`, sscc); n != 0 {
			t.Errorf("incidents = %d, want 0", n)
		}
	})

	t.Run("an open warning continues after a restart", func(t *testing.T) {
		const sscc = "089300010000000094"
		h.db.Project(t, dbtest.Shipment{SSCC: sscc})
		readings := journey(sscc, start,
			segment{duration: 10 * time.Minute, from: 4.5, to: 4.5},
			segment{duration: 12 * time.Minute, from: 4.5, to: 10.5},
			segment{duration: time.Minute, from: 10.5, to: 10.5},
		)
		// Stop once the warning is issued, while the temperature is still in bounds.
		split := 0
		for i, r := range readings {
			if r.TemperatureCelsius > 7 {
				split = i
				break
			}
		}
		h.processInBatches(t, readings[:split])
		if got := types(h.forecasts(t, sscc)); len(got) != 1 || got[0] != warning.TypeIssued {
			t.Fatalf("events before the restart = %v", got)
		}
		h.detector.Reset()
		h.processInBatches(t, readings[split:])
		events := h.forecasts(t, sscc)
		var closed warning.Closure
		if len(events) == 2 {
			_ = json.Unmarshal(events[1].Data, &closed)
		}
		if got := types(events); len(got) != 2 || closed.Outcome != warning.Breached {
			t.Errorf("events = %v, closure %+v; want the same warning breached", got, closed)
		}
	})

	t.Run("a lasting breach reduces the shelf life once", func(t *testing.T) {
		const sscc = "089300010000000100"
		production := start.Truncate(24*time.Hour).AddDate(0, 0, -1)
		expiration := production.AddDate(0, 0, 1)
		q10 := 3.0
		h.db.Project(t, dbtest.Shipment{SSCC: sscc, ProductionDate: &production, ExpirationDate: &expiration, ShelfLifeQ10: &q10})
		// 40 minutes at 25 °C, a reading every 15 s: after 15 minutes the goods have lost 15 × (3^1.7 − 1) ≈ 82
		// minutes, 2.8 % of their two days.
		h.processInBatches(t, journey(sscc, start,
			segment{duration: 40 * time.Minute, from: 25, to: 25, step: 15 * time.Second},
			segment{duration: time.Minute, from: 5, to: 5, step: 15 * time.Second},
		))
		var alerts []exposure.Alert
		for _, e := range h.forecasts(t, sscc) {
			if e.EventType != exposure.TypeShelfLifeReduced {
				continue
			}
			var a exposure.Alert
			if err := json.Unmarshal(e.Data, &a); err != nil {
				t.Fatal(err)
			}
			alerts = append(alerts, a)
		}
		if len(alerts) != 1 || alerts[0].Status != exposure.Reduced || alerts[0].ShelfLifeLossPercent < 1 {
			t.Fatalf("alerts = %+v, want one REDUCED", alerts)
		}
		// Assessed during the breach, 15 minutes after its confirmation.
		assessed, _ := time.Parse(time.RFC3339, alerts[0].AssessedAt)
		if confirmed := start.Add(30 * time.Second); assessed.Sub(confirmed) != exposure.AssessEvery {
			t.Errorf("assessed at %s, %s after the confirmation", alerts[0].AssessedAt, assessed.Sub(confirmed))
		}
		if n := h.db.Count(t, `SELECT count(*) FROM telemetry.shipment_projection WHERE sscc = $1 AND shelf_life_status = 'REDUCED'`,
			sscc); n != 1 {
			t.Errorf("shelf_life_status is not REDUCED")
		}
	})

	t.Run("the runtime role cannot change what a warning forecast", func(t *testing.T) {
		for _, stmt := range []string{
			`UPDATE telemetry.excursion_warnings SET predicted_excursion_at = now()`,
			`UPDATE telemetry.excursion_warnings SET slope_celsius_per_minute = 1`,
			`DELETE FROM telemetry.excursion_warnings`,
		} {
			if _, err := h.db.App.Exec(t.Context(), stmt); !denied(err) {
				t.Errorf("%s: error = %v, want insufficient_privilege", stmt, err)
			}
		}
	})
}

func denied(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42501"
}
