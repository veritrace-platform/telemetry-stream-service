# telemetry-stream-service

VeriTrace cold-chain telemetry. The service:

- ingests sensor readings over MQTT and forwards them to Kafka;
- persists readings in TimescaleDB;
- detects temperature breaches;
- maintains a shipment projection from `shipment.events`;
- delivers real-time notifications (breaches, recalls, live readings) over WebSocket.

Platform documentation, including the architecture, domain rules, messaging contracts, and ADRs, lives in
[`veritrace/docs`](https://github.com/veritrace-platform/veritrace/tree/main/docs).
The rules this service implements are in `docs/domain/cold-chain-monitoring.md` and
`docs/adr/0012-cold-chain-detection-engine.md`.

## Requirements

- Go 1.27 (`GOTOOLCHAIN=auto` downloads it automatically)
- Docker, for integration tests and the local environment
- The local environment from `platform-infrastructure` (`make up`)

## Getting started

```bash
cp .env.example .env
make migrate-up     # create or upgrade the schema (owner role)
make run            # API and WebSocket on :8090, admin on :8091, plus the ingest component
```

`make run` needs Kafka and Mosquitto from the local environment. The IoT fleet simulator in
`platform-infrastructure` publishes readings to try it with.

## Commands

The binary exposes these subcommands:

| Command | Purpose |
| --- | --- |
| `serve` | Run the API/WebSocket server, the admin server, and the components in `COMPONENTS` |
| `migrate up\|down\|status` | Manage the database schema |
| `healthcheck` | Probe the admin server (used by container health checks) |
| `version` | Print the build version |

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `APP_ENV` | `development` | `development` or `production` |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `HTTP_ADDR` | `:8090` | Public API and WebSocket listener |
| `ADMIN_ADDR` | `:8091` | `/healthz`, `/readyz`, `/metrics` listener (never exposed publicly) |
| `DATABASE_URL` | — | Runtime role connection (`veritrace_telemetry_app`) |
| `MIGRATIONS_DATABASE_URL` | — | Owner role connection (`veritrace_telemetry_owner`), used by `migrate` only |
| `SHUTDOWN_TIMEOUT` | `15s` | Graceful shutdown budget |
| `COMPONENTS` | `ingest` | Components this instance runs (see below) |
| `KAFKA_BROKERS` | — | Kafka bootstrap brokers, comma-separated |
| `MQTT_URL` | — | MQTT broker, for example `mqtt://localhost:1883`; required by `ingest` |
| `MQTT_USERNAME` | `telemetry-ingest` | MQTT user of the ingest component |
| `MQTT_PASSWORD` | — | MQTT password; required by `ingest` |
| `MQTT_CLIENT_ID` | `telemetry-ingest-<hostname>` | MQTT session identity; stable across restarts and unique per instance |

## Components

One binary runs the components of [ADR-0012](https://github.com/veritrace-platform/veritrace/blob/main/docs/adr/0012-cold-chain-detection-engine.md),
and `COMPONENTS` selects the ones an instance runs. The API runs in every instance.

| Component | Consumes | Produces | Work |
| --- | --- | --- | --- |
| `ingest` | MQTT `$share/telemetry-ingest/veritrace/v1/devices/+/telemetry` | `iot.telemetry.raw` | Validates device readings and forwards the accepted ones. A message is acknowledged to the broker after Kafka acknowledges its reading. |

`/readyz` reports PostgreSQL, Kafka, and, with `ingest`, the MQTT subscription.

## Project layout

```
cmd/telemetry-stream-service/   entry point
internal/app/                   configuration and wiring of the components
internal/httpapi/               REST router and /api/v1 route registration
internal/stream/                Kafka producer; kafkatest: broker for tests
internal/<domain>/              reading, ingest, ...
internal/platform/              config, logging, trace context, HTTP plumbing, admin, database, migrations
migrations/                     goose SQL migrations (embedded)
api/openapi.yaml                REST contract
```

## Development

```bash
make test               # unit tests
make test-integration   # unit + integration tests (Docker: TimescaleDB, Kafka, Mosquitto)
make lint               # golangci-lint
make openapi-lint       # validate api/openapi.yaml
make help               # all targets
```

## License

[MIT](LICENSE)
