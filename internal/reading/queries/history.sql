-- name: ReadingsBetween :many
-- Readings of an SSCC in [from, before), oldest first; devices break ties in a fixed order.
SELECT recorded_at, sscc, device_id, temperature_celsius, humidity_percent, latitude, longitude, received_at
FROM telemetry.sensor_readings
WHERE sscc = @sscc
  AND recorded_at >= @from_time
  AND recorded_at < @before
ORDER BY recorded_at, device_id;

-- name: LatestReadingBefore :one
SELECT recorded_at
FROM telemetry.sensor_readings
WHERE sscc = @sscc
  AND recorded_at < @before
ORDER BY recorded_at DESC
LIMIT 1;
