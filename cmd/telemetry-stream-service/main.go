// Command telemetry-stream-service runs VeriTrace telemetry ingestion, processing, and real-time delivery.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/veritrace-platform/telemetry-stream-service/internal/app"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/admin"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/buildinfo"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/config"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/logging"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/migrate"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/postgres"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/server"
	"github.com/veritrace-platform/telemetry-stream-service/migrations"
)

const usage = `Usage: telemetry-stream-service <command>

Commands:
  serve                      Run the REST API, the admin server, and the components in COMPONENTS
  migrate up|down|status     Manage the database schema
  healthcheck                Probe the local admin server (container health checks)
  version                    Print the build version
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = fmt.Fprint(stderr, usage)
		return 2
	}

	cfg, err := config.Load()
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "configuration error: %v\n", err)
		return 1
	}
	logger := logging.New(stdout, buildinfo.ServiceName, buildinfo.Version, cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch args[0] {
	case "serve":
		err = serve(ctx, cfg, logger)
	case "migrate":
		err = runMigrate(ctx, cfg, logger, args[1:], stdout)
	case "healthcheck":
		err = healthcheck(ctx, cfg.AdminAddr)
	case "version":
		_, _ = fmt.Fprintln(stdout, buildinfo.Version)
	default:
		_, _ = fmt.Fprint(stderr, usage)
		return 2
	}

	if err != nil {
		logger.Error("command failed", slog.String("command", args[0]), slog.Any("error", err))
		return 1
	}
	return 0
}

func serve(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	if err := cfg.ValidateServe(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	appCfg, err := app.LoadConfig()
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	if err := appCfg.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL, buildinfo.ServiceName)
	if err != nil {
		return err
	}
	defer pool.Close()

	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	deps := app.Dependencies{Logger: logger, Registerer: registry, Pool: pool}
	api, err := app.NewAPI(appCfg, deps)
	if err != nil {
		return err
	}
	workers, err := app.NewWorkers(appCfg, deps, api.Hub)
	if err != nil {
		return err
	}
	defer workers.Close()

	checks := map[string]admin.Check{"postgres": pool.Ping}
	maps.Copy(checks, workers.Checks())
	maps.Copy(checks, api.Checks)
	readiness := admin.NewReadiness(checks, 2*time.Second)

	apiServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.Handler,
		ReadHeaderTimeout: 5 * time.Second,
		// No server-wide read or write timeout: WebSocket connections are long-lived and manage their own
		// deadlines; REST handlers are bounded per request.
		IdleTimeout: 120 * time.Second,
		ErrorLog:    slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
	// The server does not track hijacked WebSocket connections; the hub closes them with 1001 at shutdown.
	apiServer.RegisterOnShutdown(api.Hub.Shutdown)
	adminServer := &http.Server{
		Addr:              cfg.AdminAddr,
		Handler:           admin.NewHandler(readiness, registry),
		ReadHeaderTimeout: 5 * time.Second,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}

	// Offsets and MQTT acknowledgements follow persistence, so work interrupted at shutdown is redone at the next
	// start.
	workersCtx, stopWorkers := context.WithCancel(ctx)
	workersDone := make(chan struct{})
	go func() {
		defer close(workersDone)
		workers.Run(workersCtx)
	}()

	logger.InfoContext(ctx, "starting", slog.String("env", cfg.Env), slog.Any("components", appCfg.Components))
	runErr := server.Run(ctx, logger, cfg.ShutdownTimeout, readiness.SetDraining, apiServer, adminServer)
	stopWorkers()
	<-workersDone
	if runErr != nil {
		return runErr
	}
	logger.InfoContext(ctx, "stopped")
	return nil
}

func runMigrate(ctx context.Context, cfg config.Config, logger *slog.Logger, args []string, stdout io.Writer) error {
	if err := cfg.ValidateMigrate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	if len(args) != 1 {
		return errors.New("usage: migrate up|down|status")
	}

	runner, err := migrate.Open(cfg.MigrationsDatabaseURL, migrations.FS, logger)
	if err != nil {
		return err
	}
	defer func() { _ = runner.Close() }()

	switch args[0] {
	case "up":
		return runner.Up(ctx)
	case "down":
		return runner.Down(ctx)
	case "status":
		statuses, err := runner.Status(ctx)
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "VERSION\tSTATE\tAPPLIED AT\tFILE")
		for _, s := range statuses {
			applied := "-"
			if !s.AppliedAt.IsZero() {
				applied = s.AppliedAt.UTC().Format(time.RFC3339)
			}
			_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%s\n", s.Source.Version, s.State, applied, s.Source.Path)
		}
		return tw.Flush()
	default:
		return fmt.Errorf("unknown migrate command %q", args[0])
	}
}

// healthcheck lets distroless containers (which have no shell or curl) report liveness.
func healthcheck(ctx context.Context, adminAddr string) error {
	host, port, err := net.SplitHostPort(adminAddr)
	if err != nil {
		return fmt.Errorf("parse ADMIN_ADDR: %w", err)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+net.JoinHostPort(host, port)+"/healthz", nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("probe admin server: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("admin server returned %d", resp.StatusCode)
	}
	return nil
}
