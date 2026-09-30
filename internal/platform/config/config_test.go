package config_test

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/config"
)

// isolateEnv unsets every variable Config reads until the test ends, so results do not depend on the
// caller's environment (make exports the local .env into test runs).
func isolateEnv(t *testing.T) {
	t.Helper()
	params, err := env.GetFieldParams(&config.Config{})
	if err != nil {
		t.Fatalf("GetFieldParams() error = %v", err)
	}
	for _, p := range params {
		t.Setenv(p.Key, "") // registers the restore of the original value
		if err := os.Unsetenv(p.Key); err != nil {
			t.Fatalf("unset %s: %v", p.Key, err)
		}
	}
}

func TestLoadDefaults(t *testing.T) {
	isolateEnv(t)
	t.Setenv("DATABASE_URL", "postgres://app@localhost/db")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Env != "development" || cfg.HTTPAddr != ":8090" || cfg.AdminAddr != ":8091" {
		t.Errorf("unexpected defaults: %+v", cfg)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %v, want info", cfg.LogLevel)
	}
	if cfg.ShutdownTimeout != 15*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 15s", cfg.ShutdownTimeout)
	}
	if err := cfg.ValidateServe(); err != nil {
		t.Errorf("ValidateServe() error = %v", err)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	isolateEnv(t)
	t.Setenv("LOG_LEVEL", "verbose")

	if _, err := config.Load(); err == nil {
		t.Fatal("Load() error = nil, want error for invalid LOG_LEVEL")
	}
}

func TestValidateServe(t *testing.T) {
	valid := config.Config{
		Env:             "production",
		HTTPAddr:        ":8090",
		AdminAddr:       ":8091",
		DatabaseURL:     "postgres://app@localhost/db",
		ShutdownTimeout: time.Second,
	}

	tests := []struct {
		name   string
		mutate func(*config.Config)
	}{
		{"unknown environment", func(c *config.Config) { c.Env = "staging" }},
		{"missing database url", func(c *config.Config) { c.DatabaseURL = "" }},
		{"same listen addresses", func(c *config.Config) { c.AdminAddr = c.HTTPAddr }},
		{"non-positive shutdown timeout", func(c *config.Config) { c.ShutdownTimeout = 0 }},
	}

	if err := valid.ValidateServe(); err != nil {
		t.Fatalf("valid config: ValidateServe() error = %v", err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid
			tt.mutate(&cfg)
			if err := cfg.ValidateServe(); err == nil {
				t.Error("ValidateServe() error = nil, want error")
			}
		})
	}
}

func TestValidateMigrate(t *testing.T) {
	if err := (config.Config{}).ValidateMigrate(); err == nil {
		t.Error("ValidateMigrate() error = nil, want error for missing URL")
	}
	if err := (config.Config{MigrationsDatabaseURL: "postgres://owner@localhost/db"}).ValidateMigrate(); err != nil {
		t.Errorf("ValidateMigrate() error = %v", err)
	}
}
