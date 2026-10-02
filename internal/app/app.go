// Package app assembles the telemetry service's background components (ADR-0012), starting with the ingest
// component, which forwards device readings from MQTT to Kafka. COMPONENTS selects the ones an instance runs; the
// serve command starts them next to the API.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/veritrace-platform/telemetry-stream-service/internal/ingest"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/admin"
	"github.com/veritrace-platform/telemetry-stream-service/internal/stream"
)

// Component is a part of the service that an instance may run.
type Component string

// Components (ADR-0012).
const (
	// Ingest forwards device readings from MQTT to iot.telemetry.raw.
	Ingest Component = "ingest"
)

// Config holds the settings of the telemetry service beyond the shared platform configuration.
type Config struct {
	// Components lists the components this instance runs.
	Components []Component `env:"COMPONENTS" envSeparator:"," envDefault:"ingest"`
	// KafkaBrokers are the bootstrap brokers, comma-separated.
	KafkaBrokers []string `env:"KAFKA_BROKERS" envSeparator:","`
	// MQTT is read when the ingest component runs.
	MQTT ingest.Config
}

// LoadConfig reads the telemetry settings from the environment.
func LoadConfig() (Config, error) {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return Config{}, fmt.Errorf("parse environment: %w", err)
	}
	return cfg, nil
}

// Runs reports whether the instance runs a component.
func (c Config) Runs(component Component) bool {
	return slices.Contains(c.Components, component)
}

// Validate reports the settings that are missing or invalid.
func (c Config) Validate() error {
	var errs []error
	if len(c.Components) == 0 {
		errs = append(errs, errors.New("COMPONENTS must list ingest"))
	}
	for _, component := range c.Components {
		if component != Ingest {
			errs = append(errs, fmt.Errorf("COMPONENTS: unknown component %q", component))
		}
	}
	if len(c.KafkaBrokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS is required"))
	}
	if c.Runs(Ingest) {
		if err := c.MQTT.Validate(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Dependencies are the resources the components run on.
type Dependencies struct {
	Logger     *slog.Logger
	Registerer prometheus.Registerer
	// Pool connects as the runtime role.
	Pool *pgxpool.Pool
	// Now reads the clock; tests inject a fixed one.
	Now func() time.Time
}

// Workers are the components that an instance runs.
type Workers struct {
	logger *slog.Logger
	runs   []func(context.Context) error
	checks map[string]admin.Check
	closes []func()
}

// NewWorkers creates the components that cfg selects. They connect to their brokers when they run.
func NewWorkers(cfg Config, deps Dependencies) (*Workers, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	now := deps.Now
	if now == nil {
		now = time.Now
	}
	producer, err := stream.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		return nil, err
	}
	w := &Workers{
		logger: deps.Logger,
		checks: map[string]admin.Check{"kafka": producer.Ping},
		closes: []func(){producer.Close},
	}

	if cfg.Runs(Ingest) {
		bridge := ingest.New(cfg.MQTT, producer, deps.Logger.With(slog.String("component", string(Ingest))),
			deps.Registerer, now)
		w.runs = append(w.runs, bridge.Run)
		w.checks["mqtt"] = bridge.Ready
	}

	return w, nil
}

// Checks returns the readiness checks of the components' brokers.
func (w *Workers) Checks() map[string]admin.Check {
	return w.checks
}

// Run runs every component until ctx is cancelled and they have stopped.
func (w *Workers) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, run := range w.runs {
		wg.Go(func() {
			if err := run(ctx); err != nil {
				w.logger.ErrorContext(ctx, "component stopped", slog.Any("error", err))
			}
		})
	}
	wg.Wait()
}

// Close releases the components' broker connections.
func (w *Workers) Close() {
	for _, c := range w.closes {
		c()
	}
}
