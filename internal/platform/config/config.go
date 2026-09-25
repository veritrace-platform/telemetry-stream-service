// Package config loads service configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/caarlos0/env/v11"
)

// Config holds every setting the service reads at startup.
type Config struct {
	Env                   string        `env:"APP_ENV" envDefault:"development"`
	LogLevel              slog.Level    `env:"LOG_LEVEL" envDefault:"info"`
	HTTPAddr              string        `env:"HTTP_ADDR" envDefault:":8090"`
	AdminAddr             string        `env:"ADMIN_ADDR" envDefault:":8091"`
	DatabaseURL           string        `env:"DATABASE_URL"`
	MigrationsDatabaseURL string        `env:"MIGRATIONS_DATABASE_URL"`
	ShutdownTimeout       time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"15s"`
}

// Load parses the environment into a Config.
func Load() (Config, error) {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return Config{}, fmt.Errorf("parse environment: %w", err)
	}
	return cfg, nil
}

// ValidateServe reports the settings missing or invalid for the serve command.
func (c Config) ValidateServe() error {
	var errs []error
	if c.Env != "development" && c.Env != "production" {
		errs = append(errs, fmt.Errorf("APP_ENV must be development or production, got %q", c.Env))
	}
	if c.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required"))
	}
	if c.HTTPAddr == c.AdminAddr {
		errs = append(errs, errors.New("HTTP_ADDR and ADMIN_ADDR must differ"))
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, errors.New("SHUTDOWN_TIMEOUT must be positive"))
	}
	return errors.Join(errs...)
}

// ValidateMigrate reports the settings missing for the migrate command.
func (c Config) ValidateMigrate() error {
	if c.MigrationsDatabaseURL == "" {
		return errors.New("MIGRATIONS_DATABASE_URL is required")
	}
	return nil
}
