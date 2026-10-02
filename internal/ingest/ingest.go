// Package ingest runs the ingest role of ADR-0012: it receives device readings over MQTT through a shared
// subscription (messaging.md §1), validates them (cold-chain-monitoring.md §2), and produces the accepted ones to
// Kafka topic iot.telemetry.raw, keyed by SSCC.
//
// A message is acknowledged to the broker only after Kafka has acknowledged its reading, so a reading is never
// lost between the two: the broker keeps the session across restarts and redelivers what was not acknowledged.
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/logging"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/tracecontext"
	"github.com/veritrace-platform/telemetry-stream-service/internal/reading"
	"github.com/veritrace-platform/telemetry-stream-service/internal/stream"
)

// ShareGroup is the shared subscription group of the ingestion instances. The broker delivers each message to one
// member of the group.
const ShareGroup = "telemetry-ingest"

// Subscription is the shared subscription to every device's telemetry topic.
const Subscription = "$share/" + ShareGroup + "/" + reading.DeviceTopicFilter

// MQTT session settings.
const (
	keepAliveSeconds = 30
	// sessionExpirySeconds keeps the session, with its subscription and unacknowledged messages, while the
	// service restarts.
	sessionExpirySeconds = 3600
	// flushTimeout bounds how long shutdown waits for Kafka to acknowledge the readings in flight.
	flushTimeout = 10 * time.Second
	// ackInterval is how often acknowledgements are sent to the broker, in the order the messages arrived.
	ackInterval = 50 * time.Millisecond
	// disconnectTimeout bounds the MQTT disconnect at shutdown.
	disconnectTimeout = 5 * time.Second
)

// rejectedPublish labels readings that were valid but could not be produced to Kafka.
const rejectedPublish = "publish_failed"

// Config holds the MQTT settings of the ingest role.
type Config struct {
	// URL is the broker address, for example mqtt://localhost:1883.
	URL      string `env:"MQTT_URL"`
	Username string `env:"MQTT_USERNAME" envDefault:"telemetry-ingest"`
	Password string `env:"MQTT_PASSWORD"`
	// ClientID identifies the session at the broker. It must be stable across restarts and unique per instance;
	// empty means telemetry-ingest-<hostname>.
	ClientID string `env:"MQTT_CLIENT_ID"`
}

// Validate reports the settings that are missing or invalid.
func (c Config) Validate() error {
	var errs []error
	if u, err := url.Parse(c.URL); c.URL == "" || err != nil || u.Host == "" {
		errs = append(errs, errors.New("MQTT_URL must be a broker URL such as mqtt://localhost:1883"))
	}
	if c.Username == "" {
		errs = append(errs, errors.New("MQTT_USERNAME is required"))
	}
	if c.Password == "" {
		errs = append(errs, errors.New("MQTT_PASSWORD is required"))
	}
	return errors.Join(errs...)
}

func (c Config) clientID() string {
	if c.ClientID != "" {
		return c.ClientID
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "local"
	}
	return "telemetry-ingest-" + host
}

// Producer writes records asynchronously and waits for those in flight, as *kgo.Client does.
type Producer interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	Flush(ctx context.Context) error
}

// Bridge forwards readings from MQTT to Kafka.
type Bridge struct {
	cfg      Config
	producer Producer
	logger   *slog.Logger
	now      func() time.Time

	// subscribed is true while the bridge holds its subscription on a live connection.
	subscribed atomic.Bool
	// closing stops forwarding at shutdown; messages received after it stay unacknowledged and are redelivered.
	closing atomic.Bool

	ingested prometheus.Counter
	rejected *prometheus.CounterVec
}

// New returns a bridge. Its metrics are registered with registerer.
func New(cfg Config, producer Producer, logger *slog.Logger, registerer prometheus.Registerer,
	now func() time.Time,
) *Bridge {
	b := &Bridge{
		cfg: cfg, producer: producer, logger: logger, now: now,
		ingested: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "veritrace_telemetry_readings_ingested_total",
			Help: "Device readings accepted and produced to iot.telemetry.raw.",
		}),
		rejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "veritrace_telemetry_readings_rejected_total",
			Help: "Device readings rejected by validation, or not produced to Kafka, by reason.",
		}, []string{"reason"}),
	}
	registerer.MustRegister(b.ingested, b.rejected)
	return b
}

// Run connects to the broker and forwards readings until ctx is cancelled. It then stops forwarding, waits for
// Kafka to acknowledge the readings in flight so that their messages are acknowledged, and disconnects.
func (b *Bridge) Run(ctx context.Context) error {
	brokerURL, err := url.Parse(b.cfg.URL)
	if err != nil {
		return fmt.Errorf("parse MQTT_URL: %w", err)
	}
	// The connection outlives ctx until the readings in flight are acknowledged.
	connCtx, stopConn := context.WithCancel(context.WithoutCancel(ctx))
	defer stopConn()
	conn, err := autopaho.NewConnection(connCtx, autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{brokerURL},
		KeepAlive:                     keepAliveSeconds,
		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         sessionExpirySeconds,
		ConnectUsername:               b.cfg.Username,
		ConnectPassword:               []byte(b.cfg.Password),
		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			go b.subscribe(connCtx, cm)
		},
		OnConnectionDown: func() bool {
			b.subscribed.Store(false)
			b.logger.WarnContext(ctx, "MQTT connection lost; reconnecting")
			return true
		},
		OnConnectError: func(err error) {
			b.logger.WarnContext(ctx, "MQTT connection failed", slog.Any("error", err))
		},
		ClientConfig: paho.ClientConfig{
			ClientID:                   b.cfg.clientID(),
			EnableManualAcknowledgment: true,
			SendAcksInterval:           ackInterval,
			OnPublishReceived:          []func(paho.PublishReceived) (bool, error){b.receive},
			OnClientError: func(err error) {
				b.logger.WarnContext(ctx, "MQTT client error", slog.Any("error", err))
			},
		},
	})
	if err != nil {
		return fmt.Errorf("connect to MQTT broker: %w", err)
	}

	<-ctx.Done()
	b.closing.Store(true)
	b.subscribed.Store(false)
	flushCtx, cancelFlush := context.WithTimeout(context.WithoutCancel(ctx), flushTimeout)
	defer cancelFlush()
	if err := b.producer.Flush(flushCtx); err != nil {
		b.logger.WarnContext(ctx, "readings still in flight at shutdown will be redelivered", slog.Any("error", err))
	}
	// Let the acknowledgements of the flushed readings go out before disconnecting.
	time.Sleep(2 * ackInterval)
	disconnectCtx, cancelDisconnect := context.WithTimeout(context.WithoutCancel(ctx), disconnectTimeout)
	defer cancelDisconnect()
	if err := conn.Disconnect(disconnectCtx); err != nil {
		return fmt.Errorf("disconnect from MQTT broker: %w", err)
	}
	return nil
}

// subscribe subscribes to every device topic. A resumed session already holds the subscription, and subscribing
// again changes nothing.
func (b *Bridge) subscribe(ctx context.Context, cm *autopaho.ConnectionManager) {
	suback, err := cm.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: Subscription, QoS: 1}},
	})
	if err == nil && len(suback.Reasons) == 1 && suback.Reasons[0] >= 0x80 {
		err = fmt.Errorf("broker refused the subscription with reason code %#x", suback.Reasons[0])
	}
	if err != nil {
		b.logger.ErrorContext(ctx, "MQTT subscribe failed", slog.String("topic", Subscription), slog.Any("error", err))
		return
	}
	b.subscribed.Store(true)
	b.logger.InfoContext(ctx, "subscribed to device telemetry", slog.String("topic", Subscription))
}

// receive validates a message and produces its reading. It runs on the MQTT client's goroutine, one message at a
// time, in the order the broker sent them.
func (b *Bridge) receive(pr paho.PublishReceived) (bool, error) {
	if b.closing.Load() {
		return true, nil
	}
	msg := pr.Packet
	tc := tracecontext.New()
	ctx := tracecontext.NewContext(context.Background(), tc)

	r, err := reading.FromDevice(msg.Topic, msg.Payload, b.now())
	if err != nil {
		reason := reading.ReasonMalformed
		var rejected *reading.RejectedError
		if errors.As(err, &rejected) {
			reason = rejected.Reason
		}
		b.rejected.WithLabelValues(string(reason)).Inc()
		b.logger.WarnContext(ctx, "reading rejected", slog.String("topic", msg.Topic), slog.String("reason", string(reason)),
			slog.Any("error", err))
		b.ack(ctx, pr)
		return true, nil
	}

	value, err := json.Marshal(r)
	if err != nil {
		return true, fmt.Errorf("encode reading: %w", err)
	}
	record := &kgo.Record{
		Topic:   reading.RawTopic,
		Key:     []byte(r.SSCC),
		Value:   value,
		Headers: stream.JSONHeaders(tc.Traceparent()),
	}
	// Records are produced without a deadline: the producer retries until Kafka acknowledges them, and shutdown
	// bounds the wait with its flush.
	b.producer.Produce(context.Background(), record, func(_ *kgo.Record, err error) {
		if err != nil {
			if b.closing.Load() {
				return // left unacknowledged, so the broker redelivers it
			}
			b.rejected.WithLabelValues(rejectedPublish).Inc()
			b.logger.ErrorContext(logging.WithAttrs(ctx, slog.String("sscc", r.SSCC)), "reading not produced",
				slog.String("device_id", r.DeviceID), slog.Any("error", err))
		} else {
			b.ingested.Inc()
		}
		b.ack(ctx, pr)
	})
	return true, nil
}

// ack acknowledges a message. The client sends acknowledgements in the order the messages arrived.
func (b *Bridge) ack(ctx context.Context, pr paho.PublishReceived) {
	if err := pr.Client.Ack(pr.Packet); err != nil {
		b.logger.WarnContext(ctx, "MQTT acknowledgement failed; the broker will redeliver the message",
			slog.Any("error", err))
	}
}

// Ready reports whether the bridge holds its subscription on a live connection.
func (b *Bridge) Ready(context.Context) error {
	if !b.subscribed.Load() {
		return errors.New("not subscribed to the MQTT broker")
	}
	return nil
}
