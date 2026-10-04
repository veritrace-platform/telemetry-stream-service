-- Cold-chain incidents (data-model.md §4; cold-chain-monitoring.md §4–§5). One row per breached episode: it is
-- inserted when the breach is confirmed, tracks the extreme temperature while the episode lasts, and is resolved
-- when the episode ends. incident_hash covers the confirmation fields, which never change.

-- +goose Up
CREATE TABLE telemetry.cold_chain_incidents (
    id uuid PRIMARY KEY,
    sscc char(18) NOT NULL,
    shipment_id uuid NOT NULL,
    device_id text NOT NULL,
    started_at timestamptz NOT NULL,
    confirmed_at timestamptz NOT NULL,
    ended_at timestamptz,
    min_temp_celsius numeric(5, 2) NOT NULL,
    max_temp_celsius numeric(5, 2) NOT NULL,
    trigger_temperature_celsius numeric(5, 2) NOT NULL,
    extreme_temperature_celsius numeric(5, 2) NOT NULL,
    latitude numeric(9, 6) NOT NULL,
    longitude numeric(9, 6) NOT NULL,
    duration_seconds integer,
    incident_hash char(64) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    -- An episode starts at one reading, so reprocessing a reading never creates a second incident.
    CONSTRAINT cold_chain_incidents_sscc_started_at_key UNIQUE (sscc, started_at),
    CONSTRAINT cold_chain_incidents_incident_hash_key UNIQUE (incident_hash),
    CONSTRAINT cold_chain_incidents_sscc_check CHECK (sscc ~ '^[0-9]{18}$'),
    CONSTRAINT cold_chain_incidents_device_id_check CHECK (device_id ~ '^[A-Za-z0-9_-]{1,64}$'),
    CONSTRAINT cold_chain_incidents_incident_hash_check CHECK (incident_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT cold_chain_incidents_temperature_range_check CHECK (min_temp_celsius < max_temp_celsius),
    -- A breach is confirmed by a reading at least 30 seconds after the episode started.
    CONSTRAINT cold_chain_incidents_confirmed_at_check CHECK (confirmed_at >= started_at + INTERVAL '30 seconds'),
    CONSTRAINT cold_chain_incidents_ended_at_check CHECK (ended_at >= confirmed_at),
    CONSTRAINT cold_chain_incidents_duration_seconds_check
        CHECK ((ended_at IS NULL) = (duration_seconds IS NULL) AND duration_seconds >= 0),
    CONSTRAINT cold_chain_incidents_latitude_check CHECK (latitude BETWEEN -90 AND 90),
    CONSTRAINT cold_chain_incidents_longitude_check CHECK (longitude BETWEEN -180 AND 180)
);

-- An SSCC has at most one open incident: its episodes follow one another.
CREATE UNIQUE INDEX cold_chain_incidents_open_sscc_key ON telemetry.cold_chain_incidents (sscc) WHERE ended_at IS NULL;

-- Incidents are listed newest first, per shipment or across the shipments a tenant takes part in.
CREATE INDEX cold_chain_incidents_sscc_id_idx ON telemetry.cold_chain_incidents (sscc, id DESC);

-- The runtime role inserts incidents and may change only what an ongoing episode changes. The confirmation
-- fields, which incident_hash covers, stay as they were inserted.
REVOKE UPDATE, DELETE ON telemetry.cold_chain_incidents FROM veritrace_telemetry_app;
GRANT UPDATE (ended_at, duration_seconds, extreme_temperature_celsius, updated_at)
    ON telemetry.cold_chain_incidents TO veritrace_telemetry_app;

-- +goose Down
DROP TABLE telemetry.cold_chain_incidents;
