-- name: MinuteBuckets :many
-- Average, minimum, and maximum temperature per minute in [from, before), oldest first.
SELECT time_bucket(INTERVAL '1 minute', recorded_at)::timestamptz AS bucket,
       avg(temperature_celsius)::numeric AS avg_temperature_celsius,
       min(temperature_celsius)::numeric AS min_temperature_celsius,
       max(temperature_celsius)::numeric AS max_temperature_celsius,
       count(*) AS reading_count
FROM telemetry.sensor_readings
WHERE sscc = @sscc
  AND recorded_at >= @from_time
  AND recorded_at < @before
GROUP BY bucket
ORDER BY bucket;

-- name: QuarterHourBuckets :many
-- The 15-minute aggregate in [from, before), oldest first.
SELECT bucket::timestamptz AS bucket,
       avg_temperature_celsius::numeric AS avg_temperature_celsius,
       min_temperature_celsius::numeric AS min_temperature_celsius,
       max_temperature_celsius::numeric AS max_temperature_celsius,
       reading_count::bigint AS reading_count
FROM telemetry.sensor_readings_15m
WHERE sscc = @sscc
  AND bucket >= @from_time
  AND bucket < @before
ORDER BY bucket;
