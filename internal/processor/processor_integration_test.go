//go:build integration

package processor_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/veritrace-platform/telemetry-stream-service/internal/dbtest"
	"github.com/veritrace-platform/telemetry-stream-service/internal/processor"
	"github.com/veritrace-platform/telemetry-stream-service/internal/reading"
	"github.com/veritrace-platform/telemetry-stream-service/internal/stream"
	"github.com/veritrace-platform/telemetry-stream-service/internal/stream/kafkatest"
)

func TestProcessorConsumesRawReadings(t *testing.T) {
	db := dbtest.Start(t)
	brokers := kafkatest.Start(t, 6, reading.RawTopic, reading.DeadLetterTopic)
	producer, err := stream.NewProducer(brokers)
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}
	t.Cleanup(producer.Close)

	valid := []*kgo.Record{raw(t, "D1", 0), raw(t, "D1", 5*time.Second), raw(t, "D2", 0)}
	invalid := raw(t, "D3", 0)
	invalid.Value = []byte(`{"device_id":"D3","sscc":"089300010000000018","temperature_celsius":120}`)
	// The first reading arrives twice, as at-least-once delivery allows.
	records := append(append([]*kgo.Record{}, valid...), invalid, raw(t, "D1", 0))
	if err := producer.ProduceSync(t.Context(), records...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	p := processor.New(reading.NewStore(db.App), producer, slog.New(slog.DiscardHandler), prometheus.NewRegistry(),
		time.Now)
	consumer, err := stream.NewConsumer(brokers, processor.Group, []string{reading.RawTopic}, p.Handle,
		slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { consumer.Run(ctx) })
	t.Cleanup(func() {
		cancel()
		wg.Wait()
		consumer.Close()
	})

	dead := kafkatest.Consume(t, brokers, reading.DeadLetterTopic, 1)
	if string(dead[0].Key) != "089300010000000018" {
		t.Errorf("dead letter key = %s", dead[0].Key)
	}
	deadline := time.Now().Add(20 * time.Second)
	for db.Count(t, `SELECT count(*) FROM telemetry.sensor_readings`) != len(valid) {
		if time.Now().After(deadline) {
			t.Fatalf("stored %d readings, want %d", db.Count(t, `SELECT count(*) FROM telemetry.sensor_readings`),
				len(valid))
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Offsets are committed after the batch is stored, so a restart does not process the records again.
	admin := kadm.NewClient(producer)
	for {
		lag, err := admin.Lag(t.Context(), processor.Group)
		if err != nil {
			t.Fatalf("lag: %v", err)
		}
		if lag[processor.Group].Lag.Total() == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("group lag = %d after processing", lag[processor.Group].Lag.Total())
		}
		time.Sleep(100 * time.Millisecond)
	}
}
