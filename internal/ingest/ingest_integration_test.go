//go:build integration

package ingest_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/veritrace-platform/telemetry-stream-service/internal/ingest"
	"github.com/veritrace-platform/telemetry-stream-service/internal/ingest/mqtttest"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/tracecontext"
	"github.com/veritrace-platform/telemetry-stream-service/internal/reading"
	"github.com/veritrace-platform/telemetry-stream-service/internal/stream"
	"github.com/veritrace-platform/telemetry-stream-service/internal/stream/kafkatest"
)

const sscc = "089300010000000018"

// running is a bridge running in the background.
type running struct {
	bridge   *ingest.Bridge
	registry *prometheus.Registry
	stop     func()
}

func start(t *testing.T, cfg ingest.Config, brokers []string) *running {
	t.Helper()
	producer, err := stream.NewProducer(brokers)
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}
	registry := prometheus.NewRegistry()
	bridge := ingest.New(cfg, producer, slog.New(slog.DiscardHandler), registry, time.Now)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := bridge.Run(ctx); err != nil {
			t.Errorf("Run() error = %v", err)
		}
	})
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			wg.Wait()
			producer.Close()
		})
	}
	t.Cleanup(stop)
	eventually(t, "the bridge subscribes", func() bool { return bridge.Ready(t.Context()) == nil })
	return &running{bridge: bridge, registry: registry, stop: stop}
}

func eventually(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func payload(at time.Time, temperature float64) []byte {
	p, _ := json.Marshal(map[string]any{
		"sscc": sscc, "ts": at.UnixMilli(), "temperature_c": temperature, "humidity_pct": 61.5,
		"lat": 10.8705, "lng": 106.8035,
	})
	return p
}

func TestBridge(t *testing.T) {
	brokerURL := mqtttest.Start(t)
	brokers := kafkatest.Start(t, 6, reading.RawTopic)
	cfg := ingest.Config{
		URL: brokerURL, Username: mqtttest.IngestUser, Password: mqtttest.IngestPassword, ClientID: "ingest-test",
	}
	bridge := start(t, cfg, brokers)

	recordedAt := time.Now().Add(-time.Second).Truncate(time.Millisecond)
	topic := reading.DeviceTopic("REEFER-0001")
	mqtttest.Publish(t, brokerURL, mqtttest.SimulatorUser, mqtttest.SimulatorPassword, topic,
		payload(recordedAt, 9.4),
		[]byte(`{"sscc":"089300010000000019","ts":1788273000123,"temperature_c":9.4,"lat":1,"lng":1}`),
		[]byte(`not json`),
		payload(recordedAt.Add(5*time.Second), 9.6),
	)

	records := kafkatest.Consume(t, brokers, reading.RawTopic, 2)
	for i, want := range []float64{9.4, 9.6} {
		rec := records[i]
		r, err := reading.Decode(rec.Value)
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if string(rec.Key) != sscc || r.DeviceID != "REEFER-0001" || r.TemperatureCelsius != want ||
			!r.RecordedAt.Equal(recordedAt.Add(time.Duration(i)*5*time.Second)) || r.HumidityPercent == nil {
			t.Errorf("record %d = key %s, %+v", i, rec.Key, r)
		}
		if stream.Header(rec, stream.HeaderContentType) != stream.ContentTypeJSON {
			t.Errorf("record %d content type = %q", i, stream.Header(rec, stream.HeaderContentType))
		}
		if _, ok := tracecontext.Parse(stream.Header(rec, stream.HeaderTraceparent)); !ok {
			t.Errorf("record %d traceparent = %q", i, stream.Header(rec, stream.HeaderTraceparent))
		}
	}
	// Both readings share the SSCC, so they share a partition and keep their order.
	if records[0].Partition != records[1].Partition {
		t.Errorf("partitions = %d and %d", records[0].Partition, records[1].Partition)
	}

	eventually(t, "the metrics count every message", func() bool {
		return metric(t, bridge.registry, "veritrace_telemetry_readings_ingested_total", "") == 2 &&
			metric(t, bridge.registry, "veritrace_telemetry_readings_rejected_total", string(reading.ReasonSSCC)) == 1 &&
			metric(t, bridge.registry, "veritrace_telemetry_readings_rejected_total", string(reading.ReasonMalformed)) == 1
	})

	t.Run("readings published during a restart are delivered afterwards", func(t *testing.T) {
		bridge.stop()
		if bridge.bridge.Ready(t.Context()) == nil {
			t.Error("a stopped bridge is ready")
		}
		mqtttest.Publish(t, brokerURL, mqtttest.SimulatorUser, mqtttest.SimulatorPassword, topic,
			payload(recordedAt.Add(10*time.Second), 9.8))
		start(t, cfg, brokers)
		records := kafkatest.Consume(t, brokers, reading.RawTopic, 3)
		if r, err := reading.Decode(records[2].Value); err != nil || r.TemperatureCelsius != 9.8 {
			t.Errorf("record after restart = %+v, %v", r, err)
		}
	})
}

// metric reads a counter from a registry: the one with the reason label, or the unlabelled one when reason is "".
func metric(t *testing.T, registry *prometheus.Registry, name, reason string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if reason == "" || (len(m.GetLabel()) == 1 && m.GetLabel()[0].GetValue() == reason) {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}
