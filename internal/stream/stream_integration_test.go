//go:build integration

package stream_test

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/veritrace-platform/telemetry-stream-service/internal/stream"
	"github.com/veritrace-platform/telemetry-stream-service/internal/stream/kafkatest"
)

func TestConsumerRetriesAFailedBatchAndCommitsAfterSuccess(t *testing.T) {
	const topic, group = "test.events", "telemetry-stream-service.test"
	brokers := kafkatest.Start(t, 1, topic)
	producer, err := stream.NewProducer(brokers)
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}
	t.Cleanup(producer.Close)
	want := []string{"a", "b", "c"}
	for _, v := range want {
		r := &kgo.Record{Topic: topic, Key: []byte("k"), Value: []byte(v), Headers: stream.JSONHeaders("tp")}
		if err := producer.ProduceSync(t.Context(), r).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	var (
		mu        sync.Mutex
		failed    []string
		processed []string
	)
	handle := func(_ context.Context, records []*kgo.Record) error {
		mu.Lock()
		defer mu.Unlock()
		var values []string
		for _, r := range records {
			values = append(values, string(r.Value))
			if got := stream.Header(r, stream.HeaderContentType); got != stream.ContentTypeJSON {
				t.Errorf("content type = %q", got)
			}
		}
		if failed == nil {
			failed = values
			return errors.New("database unavailable")
		}
		processed = append(processed, values...)
		return nil
	}
	consumer, err := stream.NewConsumer(brokers, group, []string{topic}, handle, slog.New(slog.DiscardHandler))
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

	admin := kadm.NewClient(producer)
	deadline := time.Now().Add(30 * time.Second)
	for {
		// A group that has not joined yet has no lag either, so wait for the records first.
		mu.Lock()
		done := len(processed) == len(want)
		mu.Unlock()
		lags, err := admin.Lag(t.Context(), group)
		if done && err == nil && lags[group].Lag.Total() == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the group did not catch up: %v %v", lags, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	// The failed batch is handed over again, then every record is processed once.
	if len(failed) == 0 || !slices.Equal(processed[:len(failed)], failed) || !slices.Equal(processed, want) {
		t.Errorf("failed %v, then processed %v; want %v", failed, processed, want)
	}
}
