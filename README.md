# telemetry-stream-service

VeriTrace cold-chain telemetry. The service:

- ingests sensor readings over MQTT and forwards them to Kafka;
- persists readings in TimescaleDB;
- detects temperature breaches and records them as hashed incidents;
- maintains a shipment projection from `shipment.events`;
- serves readings and incidents over REST;
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
make run            # API and WebSocket on :8090, admin on :8091, plus the ingest and processor components
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
| `COMPONENTS` | `ingest,processor` | Components this instance runs (see below) |
| `KAFKA_BROKERS` | — | Kafka bootstrap brokers, comma-separated |
| `MQTT_URL` | — | MQTT broker, for example `mqtt://localhost:1883`; required by `ingest` |
| `MQTT_USERNAME` | `telemetry-ingest` | MQTT user of the ingest component |
| `MQTT_PASSWORD` | — | MQTT password; required by `ingest` |
| `MQTT_CLIENT_ID` | `telemetry-ingest-<hostname>` | MQTT session identity; stable across restarts and unique per instance |
| `JWKS_URL` | — | core's `/.well-known/jwks.json`, whose keys verify access tokens |
| `WS_ALLOWED_ORIGINS` | — | Origins, besides the gateway's own, from which browsers may open the WebSocket; comma-separated |

## Components

Every instance serves the read API and the WebSocket hub. One binary runs two background components ([ADR-0012](https://github.com/veritrace-platform/veritrace/blob/main/docs/adr/0012-cold-chain-detection-engine.md)),
and `COMPONENTS` selects the ones an instance runs.

| Component | Consumes | Produces | Work |
| --- | --- | --- | --- |
| `ingest` | MQTT `$share/telemetry-ingest/veritrace/v1/devices/+/telemetry` | `iot.telemetry.raw` | Validates device readings and forwards the accepted ones. A message is acknowledged to the broker after Kafka acknowledges its reading. |
| `processor` | `iot.telemetry.raw` (group `telemetry-stream-service.processor`) | `iot.telemetry.dlq`, `telemetry.incidents` | Stores readings in the `sensor_readings` hypertable; duplicates are stored once. Records it cannot store go to the dead-letter topic. Runs the breach rules per SSCC and records incidents with their hashes. |
| `processor` | `shipment.events` (group `telemetry-stream-service.projection`) | `telemetry.incidents` | Keeps `shipment_projection`: status, temperature bounds, participant tenants, and assigned driver of each shipment. Resolves an open incident when its shipment is delivered, cancelled, or recalled. |
| every instance | `telemetry.incidents`, `shipment.events`, `iot.telemetry.raw` (a group per instance, from the end) | — | Feeds the WebSocket hub: breaches and recalls for the users who may view the shipment, live readings for subscribers. |

Consumers commit offsets only after a batch is stored, so a restart may process a batch again, which is
harmless: incidents and their events derive their IDs from the episode, so a repeated event is recognizable.
Episodes live in memory and are rebuilt from the database after a restart or a rebalance. `/readyz` reports
PostgreSQL, Kafka, the token keys, and, with `ingest`, the MQTT subscription.

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/telemetry/shipments/{sscc}/readings` | Readings in `[from, to)` at `raw`, `1m`, or `15m` resolution |
| `GET` | `/api/v1/telemetry/shipments/{sscc}/incidents` | Incidents of one shipment |
| `GET` | `/api/v1/telemetry/incidents` | Incidents of the caller's shipments (`state=open\|resolved`) |
| `GET` | `/api/v1/telemetry/incidents/summary` | `{open_count, last_24h_count}` |
| `GET` | `/ws/v1/notifications` | WebSocket: breaches, recalls, and live readings |

Access tokens are core's EdDSA tokens, verified with its JWKS. The contract is `api/openapi.yaml`.

## Project layout

```
cmd/telemetry-stream-service/   entry point
internal/app/                   configuration and wiring of the components
internal/httpapi/               REST router and /api/v1 route registration
internal/stream/                Kafka producer and consumer groups; kafkatest: broker for tests
internal/api/                   read API handlers
internal/hub/                   WebSocket hub and its Kafka fan-out
internal/<domain>/              reading, ingest, processor, detector, incident, projection, auth, access, ...;
                                SQL in <domain>/queries
internal/platform/              config, logging, trace context, HTTP plumbing, admin, database, migrations
migrations/                     goose SQL migrations (embedded)
api/openapi.yaml                REST contract
sqlc.yaml                       query code generation
```

## Development

```bash
make test               # unit tests
make test-integration   # unit + integration tests (Docker: TimescaleDB, Kafka, Mosquitto)
make generate           # regenerate query code after editing SQL (sqlc, in Docker)
make lint               # golangci-lint
make openapi-lint       # validate api/openapi.yaml
make help               # all targets
```

## License

[MIT](LICENSE)
