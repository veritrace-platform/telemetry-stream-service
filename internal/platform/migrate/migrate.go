// Package migrate applies the embedded SQL migrations with goose.
package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

// VersionTable stores applied migration versions. It lives in public because the application schema is
// itself created by the first migration.
const VersionTable = "public.goose_db_version"

// Runner applies migrations from an embedded filesystem to one database.
type Runner struct {
	db       *sql.DB
	provider *goose.Provider
	logger   *slog.Logger
}

// Open connects to databaseURL (as the schema owner role) and prepares the migrations in fsys.
func Open(databaseURL string, fsys fs.FS, logger *slog.Logger) (*Runner, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// A session-level advisory lock prevents concurrent replicas from migrating at the same time.
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create migration lock: %w", err)
	}

	provider, err := goose.NewProvider(goose.DialectPostgres, db, fsys,
		goose.WithTableName(VersionTable),
		goose.WithSessionLocker(locker),
		goose.WithDisableGlobalRegistry(true),
	)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create migration provider: %w", err)
	}
	return &Runner{db: db, provider: provider, logger: logger}, nil
}

// Up applies every pending migration.
func (r *Runner) Up(ctx context.Context) error {
	results, err := r.provider.Up(ctx)
	for _, res := range results {
		r.logResult(ctx, res)
	}
	if err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	if len(results) == 0 {
		r.logger.InfoContext(ctx, "database schema is up to date")
	}
	return nil
}

// Down rolls back the most recently applied migration.
func (r *Runner) Down(ctx context.Context) error {
	res, err := r.provider.Down(ctx)
	if res != nil {
		r.logResult(ctx, res)
	}
	if errors.Is(err, goose.ErrNoNextVersion) {
		r.logger.InfoContext(ctx, "no migration to roll back")
		return nil
	}
	if err != nil {
		return fmt.Errorf("roll back migration: %w", err)
	}
	return nil
}

// Status returns the state of every known migration.
func (r *Runner) Status(ctx context.Context) ([]*goose.MigrationStatus, error) {
	statuses, err := r.provider.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("read migration status: %w", err)
	}
	return statuses, nil
}

// Version returns the highest applied migration version.
func (r *Runner) Version(ctx context.Context) (int64, error) {
	version, err := r.provider.GetDBVersion(ctx)
	if err != nil {
		return 0, fmt.Errorf("read schema version: %w", err)
	}
	return version, nil
}

// Close releases the database connection.
func (r *Runner) Close() error {
	return r.provider.Close()
}

func (r *Runner) logResult(ctx context.Context, res *goose.MigrationResult) {
	attrs := []slog.Attr{
		slog.Int64("migration_version", res.Source.Version),
		slog.String("direction", res.Direction),
		slog.Int64("duration_ms", res.Duration.Milliseconds()),
	}
	if res.Error != nil {
		r.logger.LogAttrs(ctx, slog.LevelError, "migration failed", append(attrs, slog.Any("error", res.Error))...)
		return
	}
	r.logger.LogAttrs(ctx, slog.LevelInfo, "migration applied", attrs...)
}
