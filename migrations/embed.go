// Package migrations embeds the goose SQL migrations of the veritrace_telemetry database.
//
// Files are named NNNNN_description.sql and are never edited after they are merged.
// Design reference: platform-infrastructure/docs/architecture/data-model.md.
package migrations

import "embed"

// Database is the database these migrations target.
const Database = "veritrace_telemetry"

// Schema is the application schema created by the first migration.
const Schema = "telemetry"

// RequiredExtensions are installed by infrastructure bootstrap before migrations run.
var RequiredExtensions = []string{"timescaledb"}

// FS contains every migration file.
//
//go:embed *.sql
var FS embed.FS
