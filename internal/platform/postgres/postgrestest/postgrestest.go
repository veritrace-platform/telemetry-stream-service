// Package postgrestest starts disposable PostgreSQL instances for integration tests.
package postgrestest

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Image matches the image used by the local environment in platform-infrastructure.
const Image = "timescale/timescaledb:2.30.1-pg18"

// Database describes a service database provisioned like the infrastructure bootstrap does.
type Database struct {
	OwnerURL string // schema owner; runs migrations
	AppURL   string // runtime role; subject to row-level security
}

// Instance is a running PostgreSQL container.
type Instance struct {
	superuserURL string
}

// Start launches a container that is terminated when the test finishes.
func Start(t *testing.T) *Instance {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, Image,
		tcpostgres.WithDatabase("postgres"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		testcontainers.WithEnv(map[string]string{"TIMESCALEDB_TELEMETRY": "off", "NO_TS_TUNE": "true"}),
		tcpostgres.BasicWaitStrategies(),
	)
	testcontainers.CleanupContainer(t, container)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	return &Instance{superuserURL: dsn}
}

// CreateDatabase creates a database with an owner role, a runtime role, and the given extensions,
// mirroring platform-infrastructure/postgres/initdb/10-bootstrap.sh.
func (i *Instance) CreateDatabase(t *testing.T, name, ownerRole, appRole string, extensions ...string) Database {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", i.superuserURL)
	if err != nil {
		t.Fatalf("open superuser connection: %v", err)
	}
	defer func() { _ = db.Close() }()

	statements := []string{
		fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD 'owner'`, ownerRole),
		fmt.Sprintf(`CREATE ROLE %s LOGIN NOBYPASSRLS PASSWORD 'app'`, appRole),
		fmt.Sprintf(`CREATE DATABASE %s OWNER %s`, name, ownerRole),
		fmt.Sprintf(`REVOKE ALL ON DATABASE %s FROM PUBLIC`, name),
		fmt.Sprintf(`GRANT CONNECT ON DATABASE %s TO %s`, name, appRole),
	}
	for _, stmt := range statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("bootstrap %q: %v", stmt, err)
		}
	}

	if len(extensions) > 0 {
		target, err := sql.Open("pgx", i.url(t, "postgres", "postgres", name))
		if err != nil {
			t.Fatalf("open %s as superuser: %v", name, err)
		}
		defer func() { _ = target.Close() }()
		for _, ext := range extensions {
			if _, err := target.ExecContext(ctx, fmt.Sprintf(`CREATE EXTENSION IF NOT EXISTS %s`, ext)); err != nil {
				t.Fatalf("create extension %s: %v", ext, err)
			}
		}
	}

	return Database{
		OwnerURL: i.url(t, ownerRole, "owner", name),
		AppURL:   i.url(t, appRole, "app", name),
	}
}

func (i *Instance) url(t *testing.T, user, password, database string) string {
	t.Helper()
	u, err := url.Parse(i.superuserURL)
	if err != nil {
		t.Fatalf("parse superuser url: %v", err)
	}
	u.User = url.UserPassword(user, password)
	u.Path = "/" + database
	return u.String()
}
