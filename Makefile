SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

# Local settings (DATABASE_URL, ...) come from .env when present.
-include .env
export

BINARY                := telemetry-stream-service
MODULE                := github.com/veritrace-platform/$(BINARY)
VERSION               ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS               := -s -w -X $(MODULE)/internal/platform/buildinfo.Version=$(VERSION)
GOLANGCI_LINT_VERSION := v2.14.0
REDOCLY_CLI_VERSION   := 2.54.2
SQLC_VERSION          := 1.31.1

.PHONY: help
help: ## List available targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_-]+:.*## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: run
run: ## Run the API on the host (reads .env)
	go run ./cmd/$(BINARY) serve

.PHONY: migrate-up
migrate-up: ## Apply pending migrations
	go run ./cmd/$(BINARY) migrate up

.PHONY: migrate-down
migrate-down: ## Roll back the latest migration
	go run ./cmd/$(BINARY) migrate down

.PHONY: migrate-status
migrate-status: ## Show migration status
	go run ./cmd/$(BINARY) migrate status

.PHONY: build
build: ## Build the binary into bin/
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/$(BINARY) ./cmd/$(BINARY)

.PHONY: test
test: ## Run unit tests
	go test -race ./...

.PHONY: test-integration
test-integration: ## Run unit and integration tests (requires Docker)
	go test -race -tags=integration ./...

.PHONY: cover
cover: ## Run all tests with a coverage report
	go test -race -tags=integration -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -n 1

.PHONY: lint
lint: ## Run golangci-lint
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run ./...

.PHONY: fmt
fmt: ## Format code
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) fmt ./...

.PHONY: vuln
vuln: ## Scan dependencies for known vulnerabilities
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

.PHONY: generate
generate: ## Generate Go code from the SQL queries (sqlc, in Docker)
	docker run --rm -v "$(CURDIR):/src" -w /src -u "$$(id -u):$$(id -g)" sqlc/sqlc:$(SQLC_VERSION) generate

.PHONY: generate-check
generate-check: ## Fail when the generated query code is out of date
	docker run --rm -v "$(CURDIR):/src" -w /src sqlc/sqlc:$(SQLC_VERSION) diff

.PHONY: openapi-lint
openapi-lint: ## Validate the OpenAPI document
	npx --yes @redocly/cli@$(REDOCLY_CLI_VERSION) lint api/openapi.yaml

.PHONY: docker-build
docker-build: ## Build the container image
	docker build --build-arg VERSION=$(VERSION) -t veritrace/$(BINARY):$(VERSION) .

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf bin coverage.out
