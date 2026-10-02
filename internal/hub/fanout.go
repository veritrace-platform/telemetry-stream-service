package hub

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/veritrace-platform/telemetry-stream-service/internal/incident"
	"github.com/veritrace-platform/telemetry-stream-service/internal/projection"
	"github.com/veritrace-platform/telemetry-stream-service/internal/reading"
)

// groupPrefix starts the consumer group ID of each instance's fan-out.
const groupPrefix = "telemetry-stream-service.hub."

// Fanout feeds the hub of this instance from Kafka (ADR-0012): cold-chain incidents, recalls from shipment.events,
// and readings for live subscriptions. Each instance consumes in a consumer group of its own, from the end of each
// partition and without committing offsets, so every instance sees every message and delivers it to its own
// connections. Kafka removes the group after the instance leaves.
type Fanout struct {
	client *kgo.Client
	hub    *Hub
	logger *slog.Logger
}

// NewFanout returns the fan-out of hub. It connects on first use.
func NewFanout(brokers []string, hub *Hub, logger *slog.Logger) (*Fanout, error) {
	suffix := make([]byte, 6)
	if _, err := rand.Read(suffix); err != nil {
		return nil, fmt.Errorf("draw instance ID: %w", err)
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupPrefix+hex.EncodeToString(suffix)),
		kgo.ConsumeTopics(incident.Topic, projection.Topic, reading.RawTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka consumer of the hub: %w", err)
	}
	return &Fanout{client: client, hub: hub, logger: logger}, nil
}

// Run delivers messages until ctx is cancelled.
func (f *Fanout) Run(ctx context.Context) error {
	for {
		fetches := f.client.PollFetches(ctx)
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		if fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			f.logger.WarnContext(ctx, "fetch failed", slog.String("topic", topic), slog.Int("partition", int(partition)),
				slog.Any("error", err))
		})
		fetches.EachRecord(func(r *kgo.Record) {
			if err := f.dispatch(ctx, r); err != nil {
				f.logger.WarnContext(ctx, "notification not delivered", slog.String("topic", r.Topic),
					slog.String("sscc", string(r.Key)), slog.Any("error", err))
			}
		})
	}
}

// Close leaves the group and closes the connections to the brokers.
func (f *Fanout) Close() {
	f.client.Close()
}

func (f *Fanout) dispatch(ctx context.Context, r *kgo.Record) error {
	switch r.Topic {
	case incident.Topic:
		return f.incident(ctx, r.Value)
	case projection.Topic:
		return f.recall(ctx, r.Value)
	case reading.RawTopic:
		// Readings are decoded only for the SSCCs that a connection follows.
		if !f.hub.Subscribed(string(r.Key)) {
			return nil
		}
		return f.reading(r.Value)
	}
	return nil
}

// incident delivers cold_chain.breach_confirmed, with the product of the shipment, and cold_chain.breach_resolved
// (messaging.md §6.1).
func (f *Fanout) incident(ctx context.Context, value []byte) error {
	e, err := incident.DecodeEvent(value)
	if err != nil {
		return err
	}
	s, err := f.shipment(ctx, e.Subject.SSCC)
	if err != nil {
		return err
	}
	var data map[string]any
	if err := json.Unmarshal(e.Data, &data); err != nil {
		return fmt.Errorf("decode %s data: %w", e.EventType, err)
	}
	if e.EventType == incident.TypeBreachConfirmed {
		data["product_name"], data["gtin"] = s.ProductName, s.GTIN
	}
	f.hub.Deliver(s, e.EventType, e.EventID, data)
	return nil
}

// recallEvent is the part of shipment.recalled that the notification carries.
type recallEvent struct {
	EventID   uuid.UUID `json:"event_id"`
	EventType string    `json:"event_type"`
	Subject   struct {
		ShipmentID uuid.UUID `json:"shipment_id"`
		SSCC       string    `json:"sscc"`
	} `json:"subject"`
	Data struct {
		RecallID  uuid.UUID `json:"recall_id"`
		GTIN      string    `json:"gtin"`
		LotNumber string    `json:"lot_number"`
		Reason    string    `json:"reason"`
	} `json:"data"`
}

// Recall is the data of a shipment.recalled notification (messaging.md §6.1).
type Recall struct {
	ShipmentID  uuid.UUID `json:"shipment_id"`
	SSCC        string    `json:"sscc"`
	RecallID    uuid.UUID `json:"recall_id"`
	GTIN        string    `json:"gtin"`
	LotNumber   string    `json:"lot_number"`
	ProductName string    `json:"product_name"`
	Reason      string    `json:"reason"`
}

// recall delivers shipment.recalled; other shipment events are not notified.
func (f *Fanout) recall(ctx context.Context, value []byte) error {
	var e recallEvent
	if err := json.Unmarshal(value, &e); err != nil {
		return fmt.Errorf("decode shipment event: %w", err)
	}
	if e.EventType != TypeShipmentRecalled {
		return nil
	}
	s, err := f.shipment(ctx, e.Subject.SSCC)
	if err != nil {
		return err
	}
	f.hub.Deliver(s, TypeShipmentRecalled, e.EventID, Recall{
		ShipmentID: e.Subject.ShipmentID, SSCC: e.Subject.SSCC, RecallID: e.Data.RecallID, GTIN: e.Data.GTIN,
		LotNumber: e.Data.LotNumber, ProductName: s.ProductName, Reason: e.Data.Reason,
	})
	return nil
}

// LiveReading is the data of a telemetry.reading notification: the raw record without received_at.
type LiveReading struct {
	DeviceID           string   `json:"device_id"`
	SSCC               string   `json:"sscc"`
	RecordedAt         string   `json:"recorded_at"`
	TemperatureCelsius float64  `json:"temperature_celsius"`
	HumidityPercent    *float64 `json:"humidity_percent"`
	Latitude           float64  `json:"latitude"`
	Longitude          float64  `json:"longitude"`
}

func (f *Fanout) reading(value []byte) error {
	r, err := reading.Decode(value)
	if err != nil {
		return err
	}
	f.hub.Publish(r.SSCC, LiveReading{
		DeviceID: r.DeviceID, SSCC: r.SSCC, RecordedAt: r.RecordedAt.UTC().Format(reading.TimeLayout),
		TemperatureCelsius: r.TemperatureCelsius, HumidityPercent: r.HumidityPercent, Latitude: r.Latitude,
		Longitude: r.Longitude,
	})
	return nil
}

// shipment returns the projection that routes a notification.
func (f *Fanout) shipment(ctx context.Context, sscc string) (projection.Shipment, error) {
	s, err := f.hub.shipments.Get(ctx, sscc)
	if errors.Is(err, projection.ErrNotFound) {
		return projection.Shipment{}, fmt.Errorf("no projection routes %s", sscc)
	}
	return s, err
}
