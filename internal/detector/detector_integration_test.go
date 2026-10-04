//go:build integration

package detector_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/veritrace-platform/telemetry-stream-service/internal/dbtest"
	"github.com/veritrace-platform/telemetry-stream-service/internal/detector"
	"github.com/veritrace-platform/telemetry-stream-service/internal/incident"
	"github.com/veritrace-platform/telemetry-stream-service/internal/reading"
	"github.com/veritrace-platform/telemetry-stream-service/internal/stream"
	"github.com/veritrace-platform/telemetry-stream-service/internal/stream/kafkatest"
)

// flakyProducer fails while failing is set, before reaching Kafka.
type flakyProducer struct {
	client  *kgo.Client
	failing bool
}

func (p *flakyProducer) ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults {
	if p.failing {
		results := make(kgo.ProduceResults, len(records))
		for i, r := range records {
			results[i] = kgo.ProduceResult{Record: r, Err: errors.New("broker unavailable")}
		}
		return results
	}
	return p.client.ProduceSync(ctx, records...)
}

type harness struct {
	db       *dbtest.Database
	brokers  []string
	producer *flakyProducer
	detector *detector.Detector
	store    *reading.Store
	consumed int
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	db := dbtest.Start(t)
	brokers := kafkatest.Start(t, 6, incident.Topic)
	client, err := stream.NewProducer(brokers)
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}
	t.Cleanup(client.Close)
	producer := &flakyProducer{client: client}
	return &harness{
		db: db, brokers: brokers, producer: producer, store: reading.NewStore(db.App),
		detector: detector.New(db.App, producer, slog.New(slog.DiscardHandler), prometheus.NewRegistry(), time.Now),
	}
}

// process stores readings and evaluates them, as the processor does with a batch.
func (h *harness) process(t *testing.T, readings ...reading.Reading) error {
	t.Helper()
	if _, err := h.store.Insert(t.Context(), readings); err != nil {
		t.Fatalf("store readings: %v", err)
	}
	inputs := make([]detector.Input, len(readings))
	for i, r := range readings {
		inputs[i] = detector.Input{Reading: r, Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
	}
	return h.detector.Evaluate(t.Context(), inputs)
}

// event returns the next incident event produced.
func (h *harness) event(t *testing.T) incident.Event {
	t.Helper()
	records := kafkatest.Consume(t, h.brokers, incident.Topic, h.consumed+1)
	var events []incident.Event
	for _, r := range records[h.consumed:] {
		e, err := incident.DecodeEvent(r.Value)
		if err != nil {
			t.Fatalf("decode event: %v", err)
		}
		if stream.Header(r, stream.HeaderEventType) != e.EventType || string(r.Key) != e.Subject.SSCC {
			t.Errorf("record headers or key do not match %s", e.EventType)
		}
		events = append(events, e)
	}
	h.consumed++
	return events[0]
}

// noMoreEvents fails if the topic holds more than the events consumed so far.
func (h *harness) noMoreEvents(t *testing.T) {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(h.brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ends, err := kadm.NewClient(client).ListEndOffsets(t.Context(), incident.Topic)
	if err != nil {
		t.Fatalf("list end offsets: %v", err)
	}
	total := 0
	ends.Each(func(o kadm.ListedOffset) { total += int(o.Offset) })
	if total != h.consumed {
		t.Errorf("the topic holds %d events, want %d", total, h.consumed)
	}
}

// readingOf returns a reading of an SSCC taken seconds after start.
func readingOf(sscc string, start time.Time, seconds, temperature float64) reading.Reading {
	r := at(seconds, temperature)
	r.SSCC = sscc
	r.RecordedAt = start.Add(time.Duration(seconds * float64(time.Second)))
	r.ReceivedAt = r.RecordedAt.Add(300 * time.Millisecond)
	return r
}

// excursionOf returns readings of 9 °C, above chilled bounds, every 5 s from start until to seconds after it.
func excursionOf(sscc string, start time.Time, to float64) []reading.Reading {
	var rs []reading.Reading
	for s := 0.0; s < to; s += 5 {
		rs = append(rs, readingOf(sscc, start, s, 9))
	}
	return rs
}

func TestDetector(t *testing.T) {
	h := newHarness(t)
	start := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)

	t.Run("confirms, extends, and resolves an incident", func(t *testing.T) {
		const sscc = "089300010000000018"
		s := h.db.Project(t, dbtest.Shipment{SSCC: sscc})
		if err := h.process(t, excursionOf(sscc, start, 30)...); err != nil {
			t.Fatalf("Evaluate() error = %v", err)
		}
		if err := h.process(t, readingOf(sscc, start, 30, 9.4), readingOf(sscc, start, 35, 11.2)); err != nil {
			t.Fatalf("Evaluate() error = %v", err)
		}
		confirmed := h.event(t)
		id := incident.NewID(sscc, start)
		if confirmed.EventType != incident.TypeBreachConfirmed || confirmed.Subject.ShipmentID != s.ShipmentID ||
			confirmed.Subject.Status != "IN_TRANSIT" {
			t.Errorf("event = %+v", confirmed)
		}
		stored, found, err := incident.NewStore(h.db.Owner).Latest(t.Context(), sscc)
		if err != nil || !found {
			t.Fatalf("Latest() = %v, %v", found, err)
		}
		hash, _ := stored.Confirmation().Hash()
		if stored.ID != id || stored.EndedAt != nil || stored.TriggerTemperatureCelsius != 9.4 ||
			stored.ExtremeTemperatureCelsius != 11.2 || !stored.ConfirmedAt.Equal(start.Add(30*time.Second)) ||
			stored.IncidentHash != hash {
			t.Errorf("incident = %+v", stored)
		}

		if err := h.process(t, readingOf(sscc, start, 40, 10), readingOf(sscc, start, 45, 7.5)); err != nil {
			t.Fatalf("Evaluate() error = %v", err)
		}
		resolved := h.event(t)
		stored, _, _ = incident.NewStore(h.db.Owner).Latest(t.Context(), sscc)
		if resolved.EventType != incident.TypeBreachResolved || stored.EndedAt == nil ||
			!stored.EndedAt.Equal(start.Add(45*time.Second)) || *stored.DurationSeconds != 45 ||
			stored.ExtremeTemperatureCelsius != 11.2 {
			t.Errorf("event %+v, incident %+v", resolved, stored)
		}

		// Processing the same readings again, for example after a crash, changes nothing and produces nothing.
		h.detector.Reset()
		if err := h.process(t, append(excursionOf(sscc, start, 45), readingOf(sscc, start, 45, 7.5))...); err != nil {
			t.Fatalf("Evaluate() again error = %v", err)
		}
		h.noMoreEvents(t)
	})

	t.Run("a failed batch is detected when it is processed again", func(t *testing.T) {
		const sscc = "089300010000000025"
		h.db.Project(t, dbtest.Shipment{SSCC: sscc})
		if err := h.process(t, excursionOf(sscc, start, 30)...); err != nil {
			t.Fatalf("Evaluate() error = %v", err)
		}
		h.producer.failing = true
		if err := h.process(t, readingOf(sscc, start, 30, 9)); err == nil {
			t.Fatal("Evaluate() succeeded without a broker")
		}
		h.producer.failing = false
		if n := h.db.Count(t, `SELECT count(*) FROM telemetry.cold_chain_incidents WHERE sscc = $1`, sscc); n != 0 {
			t.Fatalf("incidents after the failure = %d, want 0", n)
		}
		if err := h.process(t, readingOf(sscc, start, 30, 9)); err != nil {
			t.Fatalf("Evaluate() again error = %v", err)
		}
		if e := h.event(t); e.EventType != incident.TypeBreachConfirmed ||
			e.Subject.SSCC != sscc {
			t.Errorf("event = %+v", e)
		}
	})

	t.Run("readings stored but never evaluated are caught up when the episode is rebuilt", func(t *testing.T) {
		const sscc = "089300010000000032"
		h.db.Project(t, dbtest.Shipment{SSCC: sscc})
		// A crash after storing a batch and before evaluating it.
		if _, err := h.store.Insert(t.Context(), excursionOf(sscc, start, 35)); err != nil {
			t.Fatal(err)
		}
		if err := h.process(t, readingOf(sscc, start, 35, 9)); err != nil {
			t.Fatalf("Evaluate() error = %v", err)
		}
		e := h.event(t)
		var data incident.Confirmation
		if err := json.Unmarshal(e.Data, &data); err != nil {
			t.Fatal(err)
		}
		// The confirmation is the reading that first completed 30 s, not the one that arrived after the crash.
		if data.ConfirmedAt != start.Add(30*time.Second).Format(incident.TimeLayout) {
			t.Errorf("confirmed_at = %s", data.ConfirmedAt)
		}
	})

	t.Run("an open incident older than the history continues after a restart", func(t *testing.T) {
		const sscc = "089300010000000049"
		h.db.Project(t, dbtest.Shipment{SSCC: sscc})
		if err := h.process(t, excursionOf(sscc, start, 120)...); err != nil {
			t.Fatalf("Evaluate() error = %v", err)
		}
		h.event(t)
		h.detector.Reset()
		if err := h.process(t, readingOf(sscc, start, 120, 9.5)); err != nil {
			t.Fatalf("Evaluate() error = %v", err)
		}
		h.noMoreEvents(t)
		if err := h.process(t, readingOf(sscc, start, 125, 5)); err != nil {
			t.Fatalf("Evaluate() error = %v", err)
		}
		resolved := h.event(t)
		var data incident.Resolution
		if err := json.Unmarshal(resolved.Data, &data); err != nil {
			t.Fatal(err)
		}
		if data.DurationSeconds != 125 || data.ExtremeTemperatureCelsius != 9.5 {
			t.Errorf("resolution = %+v", data)
		}
		if n := h.db.Count(t, `SELECT count(*) FROM telemetry.cold_chain_incidents WHERE sscc = $1`, sscc); n != 1 {
			t.Errorf("incidents = %d, want 1", n)
		}
	})

	t.Run("shipments that are unknown or not monitored are not evaluated", func(t *testing.T) {
		const delivered, unknown = "089300010000000056", "089300010000000063"
		h.db.Project(t, dbtest.Shipment{SSCC: delivered, Status: "DELIVERED"})
		if err := h.process(t, append(excursionOf(delivered, start, 60), excursionOf(unknown, start, 60)...)...); err != nil {
			t.Fatalf("Evaluate() error = %v", err)
		}
		h.noMoreEvents(t)
		if n := h.db.Count(t, `SELECT count(*) FROM telemetry.cold_chain_incidents WHERE sscc IN ($1, $2)`, delivered,
			unknown); n != 0 {
			t.Errorf("incidents = %d, want 0", n)
		}
	})

	t.Run("the runtime role cannot change what the hash covers", func(t *testing.T) {
		for _, stmt := range []string{
			`UPDATE telemetry.cold_chain_incidents SET trigger_temperature_celsius = 0`,
			`UPDATE telemetry.cold_chain_incidents SET confirmed_at = now()`,
			`DELETE FROM telemetry.cold_chain_incidents`,
		} {
			_, err := h.db.App.Exec(t.Context(), stmt)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
				t.Errorf("%s: error = %v, want insufficient_privilege", stmt, err)
			}
		}
	})
}
