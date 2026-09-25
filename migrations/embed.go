// Package migrations embeds the goose SQL migrations of the veritrace_telemetry database.
//
// Files are named NNNNN_description.sql and are never edited after they are merged.
// Design reference: platform-infrastructure/docs/architecture/data-model.md.
package migrations

import "embed"

// FS contains every migration file.
//
//go:embed *.sql
var FS embed.FS
