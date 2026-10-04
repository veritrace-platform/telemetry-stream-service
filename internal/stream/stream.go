// Package stream connects the service to Kafka (messaging.md §2): a producer that is idempotent and waits for
// every in-sync replica, and consumer groups that commit offsets only after a batch has been processed, which
// gives at-least-once processing (ADR-0012).
package stream

import (
	"context"
	"fmt"
	"log/slog"
	"time"

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

// Handler processes one polled batch of records. It handles bad records itself, for example by sending them to a
// dead-letter topic; an error means the batch could not be processed, for example because the database is down,
// and the consumer retries the whole batch.
type Handler func(ctx context.Context, records []*kgo.Record) error

// Consumer settings.
const (
	// maxPollRecords bounds a batch.
	maxPollRecords = 500
	// fetchMaxWait bounds how long a fetch waits for records, which bounds detection latency (ADR-0012).
	fetchMaxWait = 200 * time.Millisecond
	// Retries of a failing batch back off from minBackoff to maxBackoff.
	minBackoff = 500 * time.Millisecond
	maxBackoff = 30 * time.Second
)

// Consumer feeds the batches of a consumer group to a handler. Offsets are committed only after the handler
// succeeds, and partitions are not reassigned while a batch is processed, so a batch is processed again only
// after a crash or a failed commit; handlers must therefore be idempotent.
type Consumer struct {
	client *kgo.Client
	group  string
	handle Handler
	logger *slog.Logger
}

// NewConsumer returns a consumer of the topics in the group. A group without committed offsets starts at the
// beginning of each partition. opts add client options, such as callbacks on revoked partitions.
func NewConsumer(brokers []string, group string, topics []string, handle Handler, logger *slog.Logger,
	opts ...kgo.Opt,
) (*Consumer, error) {
	client, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.FetchMaxWait(fetchMaxWait),
	}, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka consumer %s: %w", group, err)
	}
	return &Consumer{client: client, group: group, handle: handle, logger: logger.With(slog.String("group", group))}, nil
}

// Run processes batches until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) {
	// Each poll blocks rebalances, and leaving the group at Close waits for them to be allowed.
	defer c.client.AllowRebalance()
	for {
		fetches := c.client.PollRecords(ctx, maxPollRecords)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.logger.WarnContext(ctx, "fetch failed", slog.String("topic", topic), slog.Int("partition", int(partition)),
				slog.Any("error", err))
		})
		if records := fetches.Records(); len(records) > 0 {
			if !c.process(ctx, records) {
				return
			}
			if err := c.client.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
				// The batch will be processed again, which the handler tolerates.
				c.logger.WarnContext(ctx, "commit offsets failed", slog.Any("error", err))
			}
		}
		c.client.AllowRebalance()
	}
}

// process runs the handler until it succeeds, backing off between attempts. It reports false if ctx was
// cancelled first.
func (c *Consumer) process(ctx context.Context, records []*kgo.Record) bool {
	backoff := minBackoff
	for {
		err := c.handle(ctx, records)
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		c.logger.ErrorContext(ctx, "batch failed; retrying", slog.Int("records", len(records)),
			slog.Duration("backoff", backoff), slog.Any("error", err))
		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, maxBackoff)
	}
}

// Close leaves the group and closes the connections to the brokers.
func (c *Consumer) Close() {
	c.client.Close()
}
