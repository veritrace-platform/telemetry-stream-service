// Package stream connects the service to Kafka (messaging.md §2) through a producer that is idempotent and waits
// for every in-sync replica.
package stream

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Record headers (messaging.md §2).
const (
	HeaderTraceparent = "traceparent"
	HeaderContentType = "content-type"
	HeaderEventType   = "event-type"
	// ContentTypeJSON is the content type of every record value.
	ContentTypeJSON = "application/json"
)

// Header returns the value of a record header, or "" when the record lacks it.
func Header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// JSONHeaders returns the headers of a JSON record that belongs to the trace of traceparent.
func JSONHeaders(traceparent string) []kgo.RecordHeader {
	return []kgo.RecordHeader{
		{Key: HeaderContentType, Value: []byte(ContentTypeJSON)},
		{Key: HeaderTraceparent, Value: []byte(traceparent)},
	}
}

// NewProducer returns a client that produces to the brokers. It connects on first use.
func NewProducer(brokers []string) (*kgo.Client, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}
	return client, nil
}
