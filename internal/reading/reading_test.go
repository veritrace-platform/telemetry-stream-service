package reading_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/veritrace-platform/telemetry-stream-service/internal/reading"
)

const (
	sscc  = "089300010000000018"
	topic = "veritrace/v1/devices/REEFER-0001/telemetry"
)

var receivedAt = time.Date(2026, 9, 1, 14, 30, 0, 410_000_000, time.UTC)

func ptr[T any](v T) *T { return &v }

func TestFromDevice(t *testing.T) {
	payload := `{"sscc":"089300010000000018","ts":1788273000123,"temperature_c":9.4,"humidity_pct":65.2,` +
		`"lat":10.8705,"lng":106.8035,"firmware":"2.1"}`
	got, err := reading.FromDevice(topic, []byte(payload), receivedAt)
	if err != nil {
		t.Fatalf("FromDevice() error = %v", err)
	}
	want := reading.Reading{
		DeviceID: "REEFER-0001", SSCC: sscc,
		RecordedAt: time.Date(2026, 9, 1, 14, 30, 0, 123_000_000, time.UTC), ReceivedAt: receivedAt,
		TemperatureCelsius: 9.4, HumidityPercent: ptr(65.2), Latitude: 10.8705, Longitude: 106.8035,
	}
	assertReading(t, got, want)
}

func TestFromDeviceRoundsToStoredPrecision(t *testing.T) {
	payload := `{"sscc":"089300010000000018","ts":1788273000123,"temperature_c":8.004999,"humidity_pct":65.236,` +
		`"lat":10.87050049,"lng":-106.8035006}`
	got, err := reading.FromDevice(topic, []byte(payload), receivedAt)
	if err != nil {
		t.Fatalf("FromDevice() error = %v", err)
	}
	if got.TemperatureCelsius != 8 || *got.HumidityPercent != 65.24 || got.Latitude != 10.8705 ||
		got.Longitude != -106.803501 {
		t.Errorf("FromDevice() = %+v, want values rounded to the column scales", got)
	}
}

func TestFromDeviceWithoutHumidity(t *testing.T) {
	payload := `{"sscc":"089300010000000018","ts":1788273000123,"temperature_c":-18,"lat":0,"lng":0}`
	got, err := reading.FromDevice(topic, []byte(payload), receivedAt)
	if err != nil {
		t.Fatalf("FromDevice() error = %v", err)
	}
	if got.HumidityPercent != nil || got.TemperatureCelsius != -18 {
		t.Errorf("FromDevice() = %+v", got)
	}
}

func TestFromDeviceRejects(t *testing.T) {
	const ts = 1788273000123 // receivedAt minus 287 ms
	tests := []struct {
		name    string
		topic   string
		payload string
		want    reading.Reason
	}{
		{"not JSON", topic, `temperature=9.4`, reading.ReasonMalformed},
		{"array", topic, `[1,2]`, reading.ReasonMalformed},
		{"missing sscc", topic, `{"ts":1788273000123,"temperature_c":9.4,"lat":1,"lng":1}`, reading.ReasonMalformed},
		{"missing position", topic, `{"sscc":"089300010000000018","ts":1788273000123,"temperature_c":9.4,"lat":1}`,
			reading.ReasonMalformed},
		{"null temperature", topic, `{"sscc":"089300010000000018","ts":1788273000123,"temperature_c":null,"lat":1,"lng":1}`,
			reading.ReasonMalformed},
		{"fractional timestamp", topic, `{"sscc":"089300010000000018","ts":1788273000.5,"temperature_c":9.4,"lat":1,"lng":1}`,
			reading.ReasonMalformed},
		{"string temperature", topic, `{"sscc":"089300010000000018","ts":1788273000123,"temperature_c":"9.4","lat":1,"lng":1}`,
			reading.ReasonMalformed},
		{"other topic", "veritrace/v1/devices/REEFER-0001/status", valid(ts), reading.ReasonMalformed},
		{"nested device topic", "veritrace/v1/devices/a/b/telemetry", valid(ts), reading.ReasonDeviceID},
		{"empty device ID", "veritrace/v1/devices//telemetry", valid(ts), reading.ReasonDeviceID},
		{"long device ID", "veritrace/v1/devices/" + string(make([]byte, 65)) + "/telemetry", valid(ts),
			reading.ReasonDeviceID},
		{"check digit", topic, `{"sscc":"089300010000000019","ts":1788273000123,"temperature_c":9.4,"lat":1,"lng":1}`,
			reading.ReasonSSCC},
		{"GTIN instead of SSCC", topic, `{"sscc":"08930001000018","ts":1788273000123,"temperature_c":9.4,"lat":1,"lng":1}`,
			reading.ReasonSSCC},
		{"too cold", topic, `{"sscc":"089300010000000018","ts":1788273000123,"temperature_c":-50.01,"lat":1,"lng":1}`,
			reading.ReasonTemperature},
		{"too hot", topic, `{"sscc":"089300010000000018","ts":1788273000123,"temperature_c":80.01,"lat":1,"lng":1}`,
			reading.ReasonTemperature},
		{"humidity", topic,
			`{"sscc":"089300010000000018","ts":1788273000123,"temperature_c":9.4,"humidity_pct":100.5,"lat":1,"lng":1}`,
			reading.ReasonHumidity},
		{"negative humidity", topic,
			`{"sscc":"089300010000000018","ts":1788273000123,"temperature_c":9.4,"humidity_pct":-1,"lat":1,"lng":1}`,
			reading.ReasonHumidity},
		{"latitude", topic, `{"sscc":"089300010000000018","ts":1788273000123,"temperature_c":9.4,"lat":90.5,"lng":1}`,
			reading.ReasonPosition},
		{"longitude", topic, `{"sscc":"089300010000000018","ts":1788273000123,"temperature_c":9.4,"lat":1,"lng":-181}`,
			reading.ReasonPosition},
		{"future", topic, valid(receivedAt.Add(5*time.Minute + time.Millisecond).UnixMilli()), reading.ReasonTimestamp},
		{"stale", topic, valid(receivedAt.Add(-24*time.Hour - time.Millisecond).UnixMilli()), reading.ReasonTimestamp},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := reading.FromDevice(tt.topic, []byte(tt.payload), receivedAt)
			var rejected *reading.RejectedError
			if !errors.As(err, &rejected) || rejected.Reason != tt.want {
				t.Errorf("FromDevice() error = %v, want reason %s", err, tt.want)
			}
		})
	}
}

func TestFromDeviceAcceptsLimits(t *testing.T) {
	for _, payload := range []string{
		valid(receivedAt.Add(5 * time.Minute).UnixMilli()),
		valid(receivedAt.Add(-24 * time.Hour).UnixMilli()),
		`{"sscc":"089300010000000018","ts":1788273000123,"temperature_c":-50,"humidity_pct":0,"lat":-90,"lng":-180}`,
		`{"sscc":"089300010000000018","ts":1788273000123,"temperature_c":80,"humidity_pct":100,"lat":90,"lng":180}`,
	} {
		if _, err := reading.FromDevice(topic, []byte(payload), receivedAt); err != nil {
			t.Errorf("FromDevice(%s) error = %v", payload, err)
		}
	}
}

func valid(ts int64) string {
	r, _ := json.Marshal(map[string]any{"sscc": sscc, "ts": ts, "temperature_c": 5.5, "lat": 10.87, "lng": 106.8})
	return string(r)
}

func TestRawRecordRoundTrip(t *testing.T) {
	r, err := reading.FromDevice(topic, []byte(valid(1788273000123)), receivedAt)
	if err != nil {
		t.Fatalf("FromDevice() error = %v", err)
	}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	const want = `{"device_id":"REEFER-0001","sscc":"089300010000000018","recorded_at":"2026-09-01T14:30:00.123Z",` +
		`"received_at":"2026-09-01T14:30:00.410Z","temperature_celsius":5.5,"humidity_percent":null,` +
		`"latitude":10.87,"longitude":106.8}`
	if string(data) != want {
		t.Errorf("Marshal() =\n%s\nwant\n%s", data, want)
	}
	decoded, err := reading.Decode(data)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	assertReading(t, decoded, r)
}

// TestDecodeContractExample decodes the iot.telemetry.raw example of messaging.md §4.
func TestDecodeContractExample(t *testing.T) {
	got, err := reading.Decode([]byte(`{
		"device_id": "REEFER-0001",
		"sscc": "089300010000000018",
		"recorded_at": "2026-09-01T14:30:00.123Z",
		"received_at": "2026-09-01T14:30:00.410Z",
		"temperature_celsius": 9.4,
		"humidity_percent": 65.2,
		"latitude": 10.8705,
		"longitude": 106.8035
	}`))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	assertReading(t, got, reading.Reading{
		DeviceID: "REEFER-0001", SSCC: sscc,
		RecordedAt: time.Date(2026, 9, 1, 14, 30, 0, 123_000_000, time.UTC), ReceivedAt: receivedAt,
		TemperatureCelsius: 9.4, HumidityPercent: ptr(65.2), Latitude: 10.8705, Longitude: 106.8035,
	})
}

func TestDecodeRejects(t *testing.T) {
	tests := []struct {
		name string
		data string
		want reading.Reason
	}{
		{"not JSON", `{"device_id":`, reading.ReasonMalformed},
		{"missing received_at", `{"device_id":"D1","sscc":"089300010000000018","recorded_at":"2026-09-01T14:30:00Z",` +
			`"temperature_celsius":5,"latitude":1,"longitude":1}`, reading.ReasonMalformed},
		{"bad timestamp", `{"device_id":"D1","sscc":"089300010000000018","recorded_at":"yesterday",` +
			`"received_at":"2026-09-01T14:30:00Z","temperature_celsius":5,"latitude":1,"longitude":1}`, reading.ReasonMalformed},
		{"device ID", `{"device_id":"D 1","sscc":"089300010000000018","recorded_at":"2026-09-01T14:30:00Z",` +
			`"received_at":"2026-09-01T14:30:00Z","temperature_celsius":5,"latitude":1,"longitude":1}`, reading.ReasonDeviceID},
		{"stale", `{"device_id":"D1","sscc":"089300010000000018","recorded_at":"2026-08-30T14:30:00Z",` +
			`"received_at":"2026-09-01T14:30:00Z","temperature_celsius":5,"latitude":1,"longitude":1}`, reading.ReasonTimestamp},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := reading.Decode([]byte(tt.data))
			var rejected *reading.RejectedError
			if !errors.As(err, &rejected) || rejected.Reason != tt.want {
				t.Errorf("Decode() error = %v, want reason %s", err, tt.want)
			}
		})
	}
}

func TestDeviceTopic(t *testing.T) {
	if got := reading.DeviceTopic("REEFER-0001"); got != topic {
		t.Errorf("DeviceTopic() = %s", got)
	}
	if reading.DeviceTopicFilter != "veritrace/v1/devices/+/telemetry" {
		t.Errorf("DeviceTopicFilter = %s", reading.DeviceTopicFilter)
	}
}

func assertReading(t *testing.T, got, want reading.Reading) {
	t.Helper()
	humidity := func(h *float64) any {
		if h == nil {
			return nil
		}
		return *h
	}
	if got.DeviceID != want.DeviceID || got.SSCC != want.SSCC || !got.RecordedAt.Equal(want.RecordedAt) ||
		!got.ReceivedAt.Equal(want.ReceivedAt) || got.TemperatureCelsius != want.TemperatureCelsius ||
		humidity(got.HumidityPercent) != humidity(want.HumidityPercent) || got.Latitude != want.Latitude ||
		got.Longitude != want.Longitude {
		t.Errorf("reading = %+v (humidity %v), want %+v (humidity %v)", got, humidity(got.HumidityPercent), want,
			humidity(want.HumidityPercent))
	}
}
