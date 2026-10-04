// Package mqtttest starts a Mosquitto broker for integration tests, from the image that the local environment runs,
// with the users and access control list of platform-infrastructure/mosquitto.
package mqtttest

import (
	"context"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// image matches the mosquitto service of platform-infrastructure/compose.yaml.
const image = "eclipse-mosquitto:2.0.22"

// Users of the broker.
const (
	IngestUser        = "telemetry-ingest"
	IngestPassword    = "ingest-test"
	SimulatorUser     = "fleet-simulator"
	SimulatorPassword = "simulator-test"
)

// config, acl, and start mirror platform-infrastructure/mosquitto/config.
const (
	config = `listener 1883
protocol mqtt
allow_anonymous false
password_file /mosquitto/data/passwd
acl_file /mosquitto/config/acl
log_dest stdout
`
	acl = `user telemetry-ingest
topic read veritrace/v1/devices/+/telemetry
topic read $SYS/#

user fleet-simulator
topic write veritrace/v1/devices/+/telemetry

pattern write veritrace/v1/devices/%u/telemetry
`
	start = `set -eu
passwd_file=/mosquitto/data/passwd
touch "$passwd_file"
chmod 0700 "$passwd_file"
mosquitto_passwd -b "$passwd_file" ` + IngestUser + ` ` + IngestPassword + `
mosquitto_passwd -b "$passwd_file" ` + SimulatorUser + ` ` + SimulatorPassword + `
chown mosquitto:mosquitto "$passwd_file"
exec /docker-entrypoint.sh /usr/sbin/mosquitto -c /mosquitto/config/mosquitto.conf
`
)

// Start launches the broker and returns its URL. The broker is removed when the test finishes.
func Start(t *testing.T) string {
	t.Helper()
	ctx := t.Context()
	file := func(content, path string) testcontainers.ContainerFile {
		return testcontainers.ContainerFile{Reader: strings.NewReader(content), ContainerFilePath: path, FileMode: 0o644}
	}
	ctr, err := testcontainers.Run(ctx, image,
		testcontainers.WithExposedPorts("1883/tcp"),
		testcontainers.WithFiles(
			file(config, "/mosquitto/config/mosquitto.conf"),
			file(acl, "/mosquitto/config/acl"),
			file(start, "/mosquitto/config/start.sh"),
		),
		testcontainers.WithEntrypoint("/bin/sh", "/mosquitto/config/start.sh"),
		testcontainers.WithWaitStrategy(wait.ForLog("running").WithStartupTimeout(30*time.Second)),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start Mosquitto: %v", err)
	}
	endpoint, err := ctr.PortEndpoint(ctx, "1883/tcp", "mqtt")
	if err != nil {
		t.Fatalf("Mosquitto endpoint: %v", err)
	}
	return endpoint
}

// Publish publishes payloads to a topic with QoS 1 as the given user, and waits for the broker to acknowledge each.
func Publish(t *testing.T, brokerURL, user, password, topic string, payloads ...[]byte) {
	t.Helper()
	u, err := url.Parse(brokerURL)
	if err != nil {
		t.Fatalf("parse broker URL: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", u.Host)
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	client := paho.NewClient(paho.ClientConfig{Conn: conn})
	connack, err := client.Connect(ctx, &paho.Connect{
		ClientID: "mqtttest-" + user, KeepAlive: 30, CleanStart: true,
		Username: user, UsernameFlag: true, Password: []byte(password), PasswordFlag: true,
	})
	if err != nil {
		t.Fatalf("connect as %s: %v (%+v)", user, err, connack)
	}
	defer func() { _ = client.Disconnect(&paho.Disconnect{ReasonCode: 0}) }()
	for _, payload := range payloads {
		resp, err := client.Publish(ctx, &paho.Publish{Topic: topic, QoS: 1, Payload: payload})
		if err != nil {
			t.Fatalf("publish to %s: %v", topic, err)
		}
		if resp.ReasonCode >= 0x80 {
			t.Fatalf("publish to %s refused with reason code %#x", topic, resp.ReasonCode)
		}
	}
}
