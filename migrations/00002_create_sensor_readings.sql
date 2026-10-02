-- Sensor readings (data-model.md §4; cold-chain-monitoring.md §2). A TimescaleDB hypertable with one-day chunks,
-- compressed after seven days, and a 15-minute continuous aggregate for charts.

-- +goose Up
CREATE TABLE telemetry.sensor_readings (
    recorded_at timestamptz NOT NULL,
    sscc char(18) NOT NULL,
    device_id text NOT NULL,
    temperature_celsius numeric(5, 2) NOT NULL,
    humidity_percent numeric(5, 2),
    latitude numeric(9, 6) NOT NULL,
    longitude numeric(9, 6) NOT NULL,
    received_at timestamptz NOT NULL,
    -- A reading delivered twice is stored once.
    CONSTRAINT sensor_readings_sscc_device_id_recorded_at_key UNIQUE (sscc, device_id, recorded_at),
    CONSTRAINT sensor_readings_sscc_check CHECK (sscc ~ '^[0-9]{18}$'),
    CONSTRAINT sensor_readings_device_id_check CHECK (device_id ~ '^[A-Za-z0-9_-]{1,64}$'),
    CONSTRAINT sensor_readings_temperature_celsius_check CHECK (temperature_celsius BETWEEN -50 AND 80),
    CONSTRAINT sensor_readings_humidity_percent_check CHECK (humidity_percent BETWEEN 0 AND 100),
    CONSTRAINT sensor_readings_latitude_check CHECK (latitude BETWEEN -90 AND 90),
    CONSTRAINT sensor_readings_longitude_check CHECK (longitude BETWEEN -180 AND 180)
);

-- Every read selects one SSCC over a time range, which the index below serves, so the default time index is
-- left out.
SELECT create_hypertable('telemetry.sensor_readings', by_range('recorded_at', INTERVAL '1 day'),
    create_default_indexes => false);
CREATE INDEX sensor_readings_sscc_recorded_at_idx ON telemetry.sensor_readings (sscc, recorded_at DESC);

-- Readings arrive at most 24 hours late, so chunks older than seven days no longer change.
ALTER TABLE telemetry.sensor_readings SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'sscc',
    timescaledb.compress_orderby = 'recorded_at DESC, device_id'
);
SELECT add_compression_policy('telemetry.sensor_readings', INTERVAL '7 days');

-- Readings are facts: the service adds them and never changes them.
REVOKE UPDATE, DELETE ON telemetry.sensor_readings FROM veritrace_telemetry_app;

-- The processor copies each batch into a temporary staging table before it inserts the new readings, so the
-- runtime role may create temporary tables.
-- +goose StatementBegin
DO $$
BEGIN
    EXECUTE format('GRANT TEMPORARY ON DATABASE %I TO veritrace_telemetry_app', current_database());
END
$$;
-- +goose StatementEnd

-- Average, minimum, and maximum temperature per SSCC per 15 minutes. The policy materializes buckets up to two
-- days back, which covers late readings; newer buckets are aggregated from the readings at query time.
CREATE MATERIALIZED VIEW telemetry.sensor_readings_15m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    sscc,
    time_bucket(INTERVAL '15 minutes', recorded_at) AS bucket,
    avg(temperature_celsius) AS avg_temperature_celsius,
    min(temperature_celsius) AS min_temperature_celsius,
    max(temperature_celsius) AS max_temperature_celsius,
    count(*) AS reading_count
FROM telemetry.sensor_readings
GROUP BY sscc, bucket
WITH NO DATA;
SELECT add_continuous_aggregate_policy('telemetry.sensor_readings_15m',
    start_offset => INTERVAL '2 days',
    end_offset => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '5 minutes');
REVOKE INSERT, UPDATE, DELETE ON telemetry.sensor_readings_15m FROM veritrace_telemetry_app;

-- +goose Down
DROP MATERIALIZED VIEW telemetry.sensor_readings_15m;
-- +goose StatementBegin
DO $$
BEGIN
    EXECUTE format('REVOKE TEMPORARY ON DATABASE %I FROM veritrace_telemetry_app', current_database());
END
$$;
-- +goose StatementEnd
DROP TABLE telemetry.sensor_readings;
