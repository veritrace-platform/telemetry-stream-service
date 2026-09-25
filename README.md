# telemetry-stream-service

VeriTrace cold-chain telemetry. The service:

- ingests sensor readings over MQTT and forwards them to Kafka;
- persists readings in TimescaleDB;
- detects temperature breaches;
- maintains a shipment projection from `shipment.events`;
- delivers real-time notifications (breaches, recalls, live readings) over WebSocket.

Platform documentation, including the architecture, domain rules, messaging contracts, and ADRs, lives in
[`platform-infrastructure/docs`](https://github.com/veritrace-platform/platform-infrastructure/tree/main/docs).
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
make run            # API and WebSocket on :8090, admin on :8091
```

## Commands

The binary exposes these subcommands:

| Command | Purpose |
| --- | --- |
| `serve` | Run the API/WebSocket server and the admin server |
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

Kafka, MQTT, and JWKS settings are added with the stories that use them (EP3).

## Project layout

```
cmd/telemetry-stream-service/   entry point and wiring
internal/httpapi/               REST router and /api/v1 route registration
internal/<domain>/              ingest, processor, detector, projection, hub (added per story)
internal/platform/              config, logging, trace context, HTTP plumbing, admin, database, migrations
migrations/                     goose SQL migrations (embedded)
api/openapi.yaml                REST contract
```

## Development

```bash
make test               # unit tests
make test-integration   # unit + integration tests (Docker)
make lint               # golangci-lint
make openapi-lint       # validate api/openapi.yaml
make help               # all targets
```

## License

[MIT](LICENSE)
