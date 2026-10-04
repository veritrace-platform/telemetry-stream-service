// Package kafkatest starts a single-node Kafka broker for integration tests, from the image that the local
// environment runs, with automatic topic creation disabled as in every environment.
package kafkatest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// image matches the kafka service of platform-infrastructure/compose.yaml.
const image = "apache/kafka:4.3.1"

// startScript launches the broker once the test knows the host port to advertise.
const startScript = "/tmp/kafkatest-start.sh"

// Start launches Kafka, creates the topics with the given partition count, and returns the broker address. The
// broker is removed when the test finishes.
func Start(t *testing.T, partitions int32, topics ...string) []string {
	t.Helper()
	ctx := t.Context()
	ctr, err := testcontainers.Run(ctx, image,
		testcontainers.WithExposedPorts("9092/tcp"),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_LISTENERS":                                "PLAINTEXT://:9092,CONTROLLER://:9093",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "false",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
		}),
		// The advertised address must name the host port, which is known only after the container starts.
		testcontainers.WithEntrypoint("sh"),
		testcontainers.WithCmd("-c", "while [ ! -f "+startScript+" ]; do sleep 0.1; done; exec sh "+startScript),
		testcontainers.WithLifecycleHooks(testcontainers.ContainerLifecycleHooks{
			PostStarts: []testcontainers.ContainerHook{
				func(ctx context.Context, c testcontainers.Container) error {
					if err := wait.ForMappedPort("9092/tcp").WaitUntilReady(ctx, c); err != nil {
						return err
					}
					endpoint, err := c.PortEndpoint(ctx, "9092/tcp", "")
					if err != nil {
						return err
					}
					script := fmt.Sprintf("export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://%s\nexec /__cacert_entrypoint.sh /etc/kafka/docker/run\n", endpoint)
					if err := c.CopyToContainer(ctx, []byte(script), startScript, 0o755); err != nil {
						return err
					}
					return wait.ForLog("Kafka Server started").WaitUntilReady(ctx, c)
				},
			},
		}),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start Kafka: %v", err)
	}
	endpoint, err := ctr.PortEndpoint(ctx, "9092/tcp", "")
	if err != nil {
		t.Fatalf("Kafka endpoint: %v", err)
	}
	brokers := []string{endpoint}

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("Kafka client: %v", err)
	}
	defer client.Close()
	created, err := kadm.NewClient(client).CreateTopics(ctx, partitions, 1, nil, topics...)
	if err == nil {
		err = created.Error()
	}
	if err != nil {
		t.Fatalf("create topics: %v", err)
	}
	return brokers
}

// Consume reads count records from the start of a topic, in the order of each partition. It fails the test if
// they do not arrive within 30 seconds.
func Consume(t *testing.T, brokers []string, topic string, count int) []*kgo.Record {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatalf("Kafka consumer: %v", err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	var records []*kgo.Record
	for len(records) < count {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("consume %s: got %d of %d records", topic, len(records), count)
		}
		if err := fetches.Err(); err != nil {
			t.Fatalf("consume %s: %v", topic, err)
		}
		records = append(records, fetches.Records()...)
	}
	return records
}
