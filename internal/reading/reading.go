// Package reading defines a sensor reading on its way from a device to the database: the compact payload that
// devices publish over MQTT (messaging.md §1), the normalized record on Kafka topic iot.telemetry.raw (§4), and
// the validation rules of cold-chain-monitoring.md §2.
package reading

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/veritrace-platform/telemetry-stream-service/internal/gs1"
)

// Kafka topics of readings.
const (
	// RawTopic carries every accepted reading, keyed by SSCC.
	RawTopic = "iot.telemetry.raw"
	// DeadLetterTopic receives the records that the processor cannot store.
	DeadLetterTopic = "iot.telemetry.dlq"
)

// Validation limits (cold-chain-monitoring.md §2).
const (
	MinTemperatureCelsius = -50
	MaxTemperatureCelsius = 80
	// MaxClockAhead is how far a device timestamp may be ahead of the time the reading was received.
	MaxClockAhead = 5 * time.Minute
	// MaxAge is how far a device timestamp may be behind the time the reading was received.
	MaxAge = 24 * time.Hour
)

// TimeLayout formats the timestamps of raw records: UTC with milliseconds, the precision of device clocks.
const TimeLayout = "2006-01-02T15:04:05.000Z07:00"

var deviceIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// Reading is one sensor reading. Values are rounded to the precision that the database stores, so that what
// the detector evaluates is what the database keeps.
type Reading struct {
	DeviceID   string
	SSCC       string
	RecordedAt time.Time // device time
	ReceivedAt time.Time // when the ingestion service received the reading
	// TemperatureCelsius has two decimals.
	TemperatureCelsius float64
	// HumidityPercent has two decimals; nil when the device has no humidity sensor.
	HumidityPercent *float64
	// Latitude and Longitude have six decimals.
	Latitude  float64
	Longitude float64
}

// Reason is why a reading is rejected. It labels rejection logs and metrics.
type Reason string

// Rejection reasons.
const (
	ReasonMalformed   Reason = "malformed"
	ReasonDeviceID    Reason = "invalid_device_id"
	ReasonSSCC        Reason = "invalid_sscc"
	ReasonTemperature Reason = "temperature_out_of_range"
	ReasonHumidity    Reason = "humidity_out_of_range"
	ReasonPosition    Reason = "position_out_of_range"
	ReasonTimestamp   Reason = "timestamp_out_of_window"
)

// RejectedError reports a reading that breaks a validation rule.
type RejectedError struct {
	Reason Reason
	Detail string
}

func (e *RejectedError) Error() string {
	return fmt.Sprintf("reading rejected (%s): %s", e.Reason, e.Detail)
}

func rejected(reason Reason, format string, args ...any) *RejectedError {
	return &RejectedError{Reason: reason, Detail: fmt.Sprintf(format, args...)}
}

// Validate checks the reading against cold-chain-monitoring.md §2.
func (r Reading) Validate() error {
	switch {
	case !deviceIDPattern.MatchString(r.DeviceID):
		return rejected(ReasonDeviceID, "device ID %q must be 1 to 64 letters, digits, '_', or '-'", r.DeviceID)
	case !gs1.ValidSSCC(r.SSCC):
		return rejected(ReasonSSCC, "%q is not a valid SSCC-18", r.SSCC)
	case r.TemperatureCelsius < MinTemperatureCelsius || r.TemperatureCelsius > MaxTemperatureCelsius:
		return rejected(ReasonTemperature, "temperature %v °C is outside %d…%d", r.TemperatureCelsius,
			MinTemperatureCelsius, MaxTemperatureCelsius)
	case r.HumidityPercent != nil && (*r.HumidityPercent < 0 || *r.HumidityPercent > 100):
		return rejected(ReasonHumidity, "humidity %v %% is outside 0…100", *r.HumidityPercent)
	case r.Latitude < -90 || r.Latitude > 90 || r.Longitude < -180 || r.Longitude > 180:
		return rejected(ReasonPosition, "position (%v, %v) is not a WGS-84 coordinate", r.Latitude, r.Longitude)
	case r.RecordedAt.After(r.ReceivedAt.Add(MaxClockAhead)):
		return rejected(ReasonTimestamp, "recorded %s, more than %s after it was received", r.RecordedAt.Format(TimeLayout),
			MaxClockAhead)
	case r.RecordedAt.Before(r.ReceivedAt.Add(-MaxAge)):
		return rejected(ReasonTimestamp, "recorded %s, more than %s before it was received", r.RecordedAt.Format(TimeLayout),
			MaxAge)
	}
	return nil
}

// round rounds the values to the scale of their database columns.
func (r Reading) round() Reading {
	r.TemperatureCelsius = roundTo(r.TemperatureCelsius, 2)
	if r.HumidityPercent != nil {
		h := roundTo(*r.HumidityPercent, 2)
		r.HumidityPercent = &h
	}
	r.Latitude = roundTo(r.Latitude, 6)
	r.Longitude = roundTo(r.Longitude, 6)
	return r
}

// roundTo rounds v to the given number of decimals, as its exact decimal value rounds.
func roundTo(v float64, decimals int) float64 {
	rounded, _ := strconv.ParseFloat(strconv.FormatFloat(v, 'f', decimals, 64), 64)
	return rounded
}

// Device topics: veritrace/v1/devices/{device_id}/telemetry.
const (
	topicPrefix = "veritrace/v1/devices/"
	topicSuffix = "/telemetry"
)

// DeviceTopicFilter matches the telemetry topic of every device.
const DeviceTopicFilter = topicPrefix + "+" + topicSuffix

// DeviceTopic returns the telemetry topic of a device.
func DeviceTopic(deviceID string) string {
	return topicPrefix + deviceID + topicSuffix
}

// devicePayload is the compact JSON that devices publish. Pointers tell a missing member from a zero value.
type devicePayload struct {
	SSCC        *string  `json:"sscc"`
	Timestamp   *int64   `json:"ts"` // Unix epoch milliseconds
	Temperature *float64 `json:"temperature_c"`
	Humidity    *float64 `json:"humidity_pct"`
	Latitude    *float64 `json:"lat"`
	Longitude   *float64 `json:"lng"`
}

// FromDevice reads the payload that a device published on topic and validates it. The device ID comes from the
// topic, never from the payload.
func FromDevice(topic string, payload []byte, receivedAt time.Time) (Reading, error) {
	deviceID, ok := strings.CutPrefix(topic, topicPrefix)
	if ok {
		deviceID, ok = strings.CutSuffix(deviceID, topicSuffix)
	}
	if !ok {
		return Reading{}, rejected(ReasonMalformed, "topic %q is not a device telemetry topic", topic)
	}
	var p devicePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return Reading{}, rejected(ReasonMalformed, "payload is not a reading: %v", err)
	}
	if p.SSCC == nil || p.Timestamp == nil || p.Temperature == nil || p.Latitude == nil || p.Longitude == nil {
		return Reading{}, rejected(ReasonMalformed, "payload lacks one of sscc, ts, temperature_c, lat, and lng")
	}
	r := Reading{
		DeviceID:           deviceID,
		SSCC:               *p.SSCC,
		RecordedAt:         time.UnixMilli(*p.Timestamp).UTC(),
		ReceivedAt:         receivedAt.UTC(),
		TemperatureCelsius: *p.Temperature,
		HumidityPercent:    p.Humidity,
		Latitude:           *p.Latitude,
		Longitude:          *p.Longitude,
	}.round()
	if err := r.Validate(); err != nil {
		return Reading{}, err
	}
	return r, nil
}

// record is the JSON of a raw record. Pointers tell a missing member from a zero value when decoding.
type record struct {
	DeviceID           *string  `json:"device_id"`
	SSCC               *string  `json:"sscc"`
	RecordedAt         *string  `json:"recorded_at"`
	ReceivedAt         *string  `json:"received_at"`
	TemperatureCelsius *float64 `json:"temperature_celsius"`
	HumidityPercent    *float64 `json:"humidity_percent"`
	Latitude           *float64 `json:"latitude"`
	Longitude          *float64 `json:"longitude"`
}

// MarshalJSON writes the reading as a raw record.
func (r Reading) MarshalJSON() ([]byte, error) {
	recordedAt, receivedAt := r.RecordedAt.UTC().Format(TimeLayout), r.ReceivedAt.UTC().Format(TimeLayout)
	return json.Marshal(record{
		DeviceID: &r.DeviceID, SSCC: &r.SSCC, RecordedAt: &recordedAt, ReceivedAt: &receivedAt,
		TemperatureCelsius: &r.TemperatureCelsius, HumidityPercent: r.HumidityPercent,
		Latitude: &r.Latitude, Longitude: &r.Longitude,
	})
}

// Decode reads a raw record and validates it, so the processor stores only what the rules accept even when a
// record did not come from the ingestion service.
func Decode(data []byte) (Reading, error) {
	var rec record
	if err := json.Unmarshal(data, &rec); err != nil {
		return Reading{}, rejected(ReasonMalformed, "record is not a reading: %v", err)
	}
	if rec.DeviceID == nil || rec.SSCC == nil || rec.RecordedAt == nil || rec.ReceivedAt == nil ||
		rec.TemperatureCelsius == nil || rec.Latitude == nil || rec.Longitude == nil {
		return Reading{}, rejected(ReasonMalformed,
			"record lacks one of device_id, sscc, recorded_at, received_at, temperature_celsius, latitude, and longitude")
	}
	recordedAt, err := time.Parse(time.RFC3339Nano, *rec.RecordedAt)
	if err != nil {
		return Reading{}, rejected(ReasonMalformed, "recorded_at: %v", err)
	}
	receivedAt, err := time.Parse(time.RFC3339Nano, *rec.ReceivedAt)
	if err != nil {
		return Reading{}, rejected(ReasonMalformed, "received_at: %v", err)
	}
	r := Reading{
		DeviceID:           *rec.DeviceID,
		SSCC:               *rec.SSCC,
		RecordedAt:         recordedAt.UTC(),
		ReceivedAt:         receivedAt.UTC(),
		TemperatureCelsius: *rec.TemperatureCelsius,
		HumidityPercent:    rec.HumidityPercent,
		Latitude:           *rec.Latitude,
		Longitude:          *rec.Longitude,
	}.round()
	if err := r.Validate(); err != nil {
		return Reading{}, err
	}
	return r, nil
}
