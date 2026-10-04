// Package dbtest provides a migrated telemetry database for integration tests. Fixtures and assertions use the
// owner role; the code under test connects as the runtime role, with the grants that production has.
package dbtest

import (
	"log/slog"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/migrate"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/postgres"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/postgres/postgrestest"
	"github.com/veritrace-platform/telemetry-stream-service/migrations"
)

// Database roles, named as the infrastructure bootstrap names them.
const (
	OwnerRole = migrations.Database + "_owner"
	AppRole   = migrations.Database + "_app"
)

// Database is a migrated telemetry database.
type Database struct {
	// Owner connects as the schema owner. Use it for fixtures and assertions.
	Owner *pgxpool.Pool
	// App connects as the runtime role.
	App *pgxpool.Pool
}

// Start launches PostgreSQL with TimescaleDB, applies every migration, and connects as both roles. Everything is
// removed when the test finishes. Subtests share the database, so each test uses its own SSCCs.
func Start(t *testing.T) *Database {
	t.Helper()
	database := postgrestest.Start(t).CreateDatabase(t, migrations.Database, OwnerRole, AppRole,
		migrations.RequiredExtensions...)
	runner, err := migrate.Open(database.OwnerURL, migrations.FS, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("open migrations: %v", err)
	}
	defer func() { _ = runner.Close() }()
	if err := runner.Up(t.Context()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	return &Database{Owner: connect(t, database.OwnerURL), App: connect(t, database.AppURL)}
}

func connect(t *testing.T, url string) *pgxpool.Pool {
	t.Helper()
	pool, err := postgres.NewPool(t.Context(), url, "dbtest")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// Count returns the result of a count query run as the owner.
func (d *Database) Count(t testing.TB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := d.Owner.QueryRow(t.Context(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// Shipment is a projected shipment for fixtures. Zero fields take defaults: a new shipment ID, a new owner tenant
// that is the only participant, status IN_TRANSIT, and bounds of 2 to 8 °C.
type Shipment struct {
	SSCC             string
	ShipmentID       uuid.UUID
	OwnerTenantID    uuid.UUID
	Participants     []uuid.UUID
	AssignedDriverID *uuid.UUID
	Status           string
	MinTempCelsius   float64
	MaxTempCelsius   float64
}

// Project inserts a shipment into the projection as the owner and returns it with its defaults filled in.
func (d *Database) Project(t testing.TB, s Shipment) Shipment {
	t.Helper()
	if s.ShipmentID == uuid.Nil {
		s.ShipmentID = uuid.New()
	}
	if s.OwnerTenantID == uuid.Nil {
		s.OwnerTenantID = uuid.New()
	}
	if !slices.Contains(s.Participants, s.OwnerTenantID) {
		s.Participants = append([]uuid.UUID{s.OwnerTenantID}, s.Participants...)
	}
	if s.Status == "" {
		s.Status = "IN_TRANSIT"
	}
	if s.MinTempCelsius == 0 && s.MaxTempCelsius == 0 {
		s.MinTempCelsius, s.MaxTempCelsius = 2, 8
	}
	_, err := d.Owner.Exec(t.Context(), `
		INSERT INTO telemetry.shipment_projection (
			sscc, shipment_id, owner_tenant_id, participant_tenant_ids, assigned_driver_id, status, gtin, product_name,
			lot_number, min_temp_celsius, max_temp_celsius, last_event_sequence)
		VALUES ($1, $2, $3, $4, $5, $6, '08930001000018', 'Fresh milk 1 L', 'L2026-09-30A', $7, $8, 1)`,
		s.SSCC, s.ShipmentID, s.OwnerTenantID, s.Participants, s.AssignedDriverID, s.Status, s.MinTempCelsius,
		s.MaxTempCelsius)
	if err != nil {
		t.Fatalf("project shipment %s: %v", s.SSCC, err)
	}
	return s
}
