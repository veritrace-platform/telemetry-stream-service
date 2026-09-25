//go:build integration

package migrate_test

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"

	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/migrate"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/postgres/postgrestest"
	"github.com/veritrace-platform/telemetry-stream-service/migrations"
)

func TestMigrationsApplyAndRollBack(t *testing.T) {
	ctx := context.Background()
	pg := postgrestest.Start(t)
	database := pg.CreateDatabase(t, "veritrace_telemetry", "veritrace_telemetry_owner", "veritrace_telemetry_app", "timescaledb")

	runner, err := migrate.Open(database.OwnerURL, migrations.FS, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = runner.Close() })

	if err := runner.Up(ctx); err != nil {
		t.Fatalf("Up() error = %v", err)
	}
	statuses, err := runner.Status(ctx)
	if err != nil {
		t.Fatalf("Status() error = %v", err)
	}
	version, err := runner.Version(ctx)
	if err != nil {
		t.Fatalf("Version() error = %v", err)
	}
	if want := statuses[len(statuses)-1].Source.Version; version != want {
		t.Fatalf("Version() = %d, want %d", version, want)
	}

	assertRuntimeRole(t, database.AppURL)

	// Every migration must roll back cleanly and re-apply.
	for range statuses {
		if err := runner.Down(ctx); err != nil {
			t.Fatalf("Down() error = %v", err)
		}
	}
	if err := runner.Up(ctx); err != nil {
		t.Fatalf("Up() after full rollback error = %v", err)
	}
}

// assertRuntimeRole checks the runtime role's privileges on the application schema.
func assertRuntimeRole(t *testing.T, appURL string) {
	t.Helper()
	db, err := sql.Open("pgx", appURL)
	if err != nil {
		t.Fatalf("open app connection: %v", err)
	}
	defer func() { _ = db.Close() }()

	var bypassRLS, canUse, canCreate bool
	err = db.QueryRow(`
		SELECT r.rolbypassrls,
		       has_schema_privilege(current_user, 'telemetry', 'USAGE'),
		       has_schema_privilege(current_user, 'telemetry', 'CREATE')
		FROM pg_roles r
		WHERE r.rolname = current_user`).Scan(&bypassRLS, &canUse, &canCreate)
	if err != nil {
		t.Fatalf("query role privileges: %v", err)
	}
	if bypassRLS {
		t.Error("runtime role must not bypass row-level security")
	}
	if !canUse {
		t.Error("runtime role lacks USAGE on schema telemetry")
	}
	if canCreate {
		t.Error("runtime role must not create objects in schema telemetry")
	}

	var timescaleVersion string
	if err := db.QueryRow(`SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'`).Scan(&timescaleVersion); err != nil {
		t.Errorf("timescaledb extension unavailable: %v", err)
	}
}
