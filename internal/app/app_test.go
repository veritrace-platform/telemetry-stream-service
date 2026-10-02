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
	cfg, err := app.LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if !slices.Equal(cfg.Components, []app.Component{app.Ingest}) ||
		!slices.Equal(cfg.KafkaBrokers, []string{"localhost:9092", "localhost:9093"}) ||
		cfg.MQTT.Username != "telemetry-ingest" {
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
		{"ingest needs MQTT", map[string]string{"COMPONENTS": "ingest", "KAFKA_BROKERS": "kafka:19092"},
			[]string{"MQTT_URL", "MQTT_PASSWORD"}},
		{"everything needs Kafka", map[string]string{"COMPONENTS": "ingest", "MQTT_URL": "mqtt://localhost:1883",
			"MQTT_PASSWORD": "x"}, []string{"KAFKA_BROKERS"}},
		{"unknown component", map[string]string{"COMPONENTS": "ingest,detector", "KAFKA_BROKERS": "kafka:19092",
			"MQTT_URL": "mqtt://localhost:1883", "MQTT_PASSWORD": "x"},
			[]string{`unknown component "detector"`}},
		{"no component", map[string]string{"COMPONENTS": " ", "KAFKA_BROKERS": "kafka:19092"},
			[]string{"unknown component"}},
		{"broker URL without host", map[string]string{"COMPONENTS": "ingest", "KAFKA_BROKERS": "kafka:19092",
			"MQTT_URL": "localhost", "MQTT_PASSWORD": "x"}, []string{"MQTT_URL"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, name := range []string{"COMPONENTS", "KAFKA_BROKERS", "MQTT_URL", "MQTT_PASSWORD"} {
				t.Setenv(name, tt.env[name])
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
