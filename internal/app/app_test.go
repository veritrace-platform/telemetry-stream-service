package app_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/veritrace-platform/telemetry-stream-service/internal/app"
)

func TestLoadConfigDefaults(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "localhost:9092,localhost:9093")
	t.Setenv("MQTT_URL", "mqtt://localhost:1883")
	t.Setenv("MQTT_PASSWORD", "ingest-dev")
	t.Setenv("JWKS_URL", "http://localhost:8080/.well-known/jwks.json")
	t.Setenv("WS_ALLOWED_ORIGINS", "https://app.example.com,http://localhost:3001")
	cfg, err := app.LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if !slices.Equal(cfg.Components, []app.Component{app.Ingest, app.Processor}) ||
		!slices.Equal(cfg.KafkaBrokers, []string{"localhost:9092", "localhost:9093"}) ||
		cfg.MQTT.Username != "telemetry-ingest" || len(cfg.WSAllowedOrigins) != 2 {
		t.Errorf("LoadConfig() = %+v", cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate() error = %v", err)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want []string // substrings of the error; none means valid
	}{
		{"processor only needs no MQTT", map[string]string{"COMPONENTS": "processor", "KAFKA_BROKERS": "kafka:19092"}, nil},
		{"ingest needs MQTT", map[string]string{"COMPONENTS": "ingest", "KAFKA_BROKERS": "kafka:19092"},
			[]string{"MQTT_URL", "MQTT_PASSWORD"}},
		{"everything needs Kafka", map[string]string{"COMPONENTS": "processor"}, []string{"KAFKA_BROKERS"}},
		{"unknown component", map[string]string{"COMPONENTS": "processor,detector", "KAFKA_BROKERS": "kafka:19092"},
			[]string{`unknown component "detector"`}},
		{"no component", map[string]string{"COMPONENTS": " ", "KAFKA_BROKERS": "kafka:19092"},
			[]string{"unknown component"}},
		{"broker URL without host", map[string]string{"COMPONENTS": "ingest", "KAFKA_BROKERS": "kafka:19092",
			"MQTT_URL": "localhost", "MQTT_PASSWORD": "x"}, []string{"MQTT_URL"}},
		{"no JWKS", map[string]string{"COMPONENTS": "processor", "KAFKA_BROKERS": "kafka:19092", "JWKS_URL": "-"},
			[]string{"JWKS_URL"}},
		{"JWKS that is not a URL", map[string]string{"COMPONENTS": "processor", "KAFKA_BROKERS": "kafka:19092",
			"JWKS_URL": "core:8080/jwks"}, []string{"JWKS_URL"}},
		{"origin with a path", map[string]string{"COMPONENTS": "processor", "KAFKA_BROKERS": "kafka:19092",
			"WS_ALLOWED_ORIGINS": "https://app.example.com/login"}, []string{"WS_ALLOWED_ORIGINS"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, name := range []string{"COMPONENTS", "KAFKA_BROKERS", "MQTT_URL", "MQTT_PASSWORD", "WS_ALLOWED_ORIGINS"} {
				t.Setenv(name, tt.env[name])
			}
			// Every case but the JWKS ones has a valid JWKS URL; "-" stands for none.
			switch jwks := tt.env["JWKS_URL"]; jwks {
			case "":
				t.Setenv("JWKS_URL", "http://core-business-service:8080/.well-known/jwks.json")
			case "-":
				t.Setenv("JWKS_URL", "")
			default:
				t.Setenv("JWKS_URL", jwks)
			}
			cfg, err := app.LoadConfig()
			if err != nil {
				t.Fatalf("LoadConfig() error = %v", err)
			}
			err = cfg.Validate()
			if len(tt.want) == 0 {
				if err != nil {
					t.Errorf("Validate() error = %v", err)
				}
				return
			}
			for _, want := range tt.want {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Errorf("Validate() error = %v, want it to mention %s", err, want)
				}
			}
		})
	}
}
