//go:build integration

package hub_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/veritrace-platform/telemetry-stream-service/internal/auth"
	"github.com/veritrace-platform/telemetry-stream-service/internal/hub"
	"github.com/veritrace-platform/telemetry-stream-service/internal/incident"
	"github.com/veritrace-platform/telemetry-stream-service/internal/projection"
	"github.com/veritrace-platform/telemetry-stream-service/internal/reading"
	"github.com/veritrace-platform/telemetry-stream-service/internal/stream"
	"github.com/veritrace-platform/telemetry-stream-service/internal/stream/kafkatest"
)

func TestFanout(t *testing.T) {
	f := newFixture(t, 0)
	brokers := kafkatest.Start(t, 6, incident.Topic, projection.Topic, reading.RawTopic)
	producer, err := stream.NewProducer(brokers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	fanout, err := hub.NewFanout(brokers, f.hub, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewFanout() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { _ = fanout.Run(ctx) })
	t.Cleanup(func() {
		cancel()
		wg.Wait()
		fanout.Close()
	})

	conn := f.connect(t, auth.Principal{UserID: f.driver, TenantID: f.carrier, Role: auth.RoleDriver})
	send(t, conn, subscribe(sscc))
	expect(t, conn, hub.TypeSubscribed)

	// The fan-out starts at the end of each partition once it has joined its group, so each record is produced
	// until it arrives.
	produceUntil := func(t *testing.T, record func() *kgo.Record, typ string) received {
		t.Helper()
		messages := make(chan received, 16)
		go func() {
			for {
				m, status := nextQuietly(conn)
				if status != -1 {
					close(messages)
					return
				}
				messages <- m
				if m.Type == typ {
					close(messages)
					return
				}
			}
		}()
		deadline := time.After(30 * time.Second)
		tick := time.NewTicker(250 * time.Millisecond)
		defer tick.Stop()
		for {
			if err := producer.ProduceSync(t.Context(), record()).FirstErr(); err != nil {
				t.Fatalf("produce: %v", err)
			}
			select {
			case m, ok := <-messages:
				if !ok {
					t.Fatalf("the connection closed before %s arrived", typ)
				}
				if m.Type == typ {
					return m
				}
			case <-tick.C:
			case <-deadline:
				t.Fatalf("%s did not arrive", typ)
			}
		}
	}

	t.Run("live readings of a subscribed SSCC", func(t *testing.T) {
		r := reading.Reading{
			DeviceID: "REEFER-0001", SSCC: sscc, RecordedAt: time.Now().UTC().Truncate(time.Millisecond),
			ReceivedAt: time.Now().UTC(), TemperatureCelsius: 5.25, Latitude: 10.87, Longitude: 106.8,
		}
		value, _ := json.Marshal(r)
		m := produceUntil(t, func() *kgo.Record {
			return &kgo.Record{Topic: reading.RawTopic, Key: []byte(sscc), Value: value}
		}, hub.TypeReading)
		var data map[string]any
		_ = json.Unmarshal(m.Data, &data)
		if data["device_id"] != "REEFER-0001" || data["temperature_celsius"] != 5.25 || data["received_at"] != nil ||
			data["recorded_at"] != r.RecordedAt.Format(reading.TimeLayout) || len(data) != 7 {
			t.Errorf("reading = %v", data)
		}
	})

	t.Run("a breach, with the product", func(t *testing.T) {
		start := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
		i := incident.Incident{
			ID: incident.NewID(sscc, start), ShipmentID: f.shipment.ShipmentID, SSCC: sscc, DeviceID: "REEFER-0001",
			StartedAt: start, ConfirmedAt: start.Add(30 * time.Second), MinTempCelsius: 2, MaxTempCelsius: 8,
			TriggerTemperatureCelsius: 9.4, ExtremeTemperatureCelsius: 9.4, Latitude: 10.87, Longitude: 106.8,
		}
		i.IncidentHash, _ = i.Confirmation().Hash()
		e, _ := incident.ConfirmedEvent(i, projection.StatusInTransit)
		m := produceUntil(t, func() *kgo.Record {
			r, _ := e.Record("")
			return r
		}, incident.TypeBreachConfirmed)
		var data map[string]any
		_ = json.Unmarshal(m.Data, &data)
		if m.ID != e.EventID || data["incident_hash"] != i.IncidentHash || data["product_name"] != "Fresh milk 1 L" ||
			data["gtin"] != "08930001000018" || data["temperature_celsius"] != 9.4 {
			t.Errorf("message %s: %v", m.ID, data)
		}
	})

	t.Run("a recall", func(t *testing.T) {
		recallID := uuid.New()
		envelope, _ := json.Marshal(map[string]any{
			"event_id": uuid.NewString(), "event_type": "shipment.recalled", "event_version": 1,
			"occurred_at": "2026-10-02T08:30:00.000000Z", "producer": "core-business-service",
			"subject": map[string]any{"shipment_id": f.shipment.ShipmentID, "sscc": sscc, "status": "RECALLED"},
			"data": map[string]any{
				"recall_id": recallID, "lot_id": uuid.NewString(), "gtin": "08930001000018", "lot_number": "L2026-09-30A",
				"reason": "Supplier reported contamination", "previous_status": "IN_TRANSIT",
			},
		})
		m := produceUntil(t, func() *kgo.Record {
			return &kgo.Record{Topic: projection.Topic, Key: []byte(sscc), Value: envelope}
		}, hub.TypeShipmentRecalled)
		var data hub.Recall
		_ = json.Unmarshal(m.Data, &data)
		want := hub.Recall{
			ShipmentID: f.shipment.ShipmentID, SSCC: sscc, RecallID: recallID, GTIN: "08930001000018",
			LotNumber: "L2026-09-30A", ProductName: "Fresh milk 1 L", Reason: "Supplier reported contamination",
		}
		if data != want {
			t.Errorf("recall = %+v, want %+v", data, want)
		}
	})
}

// nextQuietly reads the next message, or returns the close status; it reports -2 for any other error.
func nextQuietly(conn *websocket.Conn) (received, websocket.StatusCode) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil {
		if status := websocket.CloseStatus(err); status != -1 {
			return received{}, status
		}
		return received{}, -2
	}
	var m received
	_ = json.Unmarshal(data, &m)
	return m, -1
}
