-- Excursion warnings (data-model.md §4; cold-chain-monitoring.md §7). One row per warning: it is inserted when a
-- trend points at an excursion and closed with an outcome, which makes every warning a labelled forecast.

-- +goose Up
CREATE TABLE telemetry.excursion_warnings (
    id uuid PRIMARY KEY,
    sscc char(18) NOT NULL,
    shipment_id uuid NOT NULL,
    device_id text NOT NULL,
    issued_at timestamptz NOT NULL,
    bound text NOT NULL,
    bound_celsius numeric(5, 2) NOT NULL,
    temperature_celsius numeric(5, 2) NOT NULL,
    slope_celsius_per_minute numeric(7, 3) NOT NULL,
    predicted_excursion_at timestamptz NOT NULL,
    closed_at timestamptz,
    outcome text,
    incident_id uuid,
    lead_time_seconds integer,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    -- A warning is issued at one reading, so reprocessing a reading never issues a second warning.
    CONSTRAINT excursion_warnings_sscc_issued_at_key UNIQUE (sscc, issued_at),
    CONSTRAINT excursion_warnings_sscc_check CHECK (sscc ~ '^[0-9]{18}$'),
    CONSTRAINT excursion_warnings_device_id_check CHECK (device_id ~ '^[A-Za-z0-9_-]{1,64}$'),
    CONSTRAINT excursion_warnings_bound_check CHECK (bound IN ('MAX', 'MIN')),
    -- A rising temperature approaches the maximum, a falling one the minimum.
    CONSTRAINT excursion_warnings_slope_check
        CHECK ((bound = 'MAX' AND slope_celsius_per_minute > 0) OR (bound = 'MIN' AND slope_celsius_per_minute < 0)),
    CONSTRAINT excursion_warnings_predicted_excursion_at_check CHECK (predicted_excursion_at >= issued_at),
    CONSTRAINT excursion_warnings_outcome_check CHECK (outcome IN ('BREACHED', 'CLEARED', 'INCONCLUSIVE')),
    CONSTRAINT excursion_warnings_closed_check CHECK ((closed_at IS NULL) = (outcome IS NULL) AND closed_at >= issued_at),
    -- Only a breach links a warning to an incident and gives it a lead time.
    CONSTRAINT excursion_warnings_incident_check
        CHECK ((outcome IS NOT DISTINCT FROM 'BREACHED') = (incident_id IS NOT NULL)
               AND (outcome IS NOT DISTINCT FROM 'BREACHED') = (lead_time_seconds IS NOT NULL)
               AND lead_time_seconds >= 0)
);

-- An SSCC has at most one open warning.
CREATE UNIQUE INDEX excursion_warnings_open_sscc_key ON telemetry.excursion_warnings (sscc) WHERE closed_at IS NULL;

-- Warnings are listed newest first, per shipment or across the shipments a tenant takes part in.
CREATE INDEX excursion_warnings_sscc_id_idx ON telemetry.excursion_warnings (sscc, id DESC);

-- The performance of the forecast matches breached warnings with their incidents.
CREATE INDEX excursion_warnings_incident_id_idx ON telemetry.excursion_warnings (incident_id)
    WHERE incident_id IS NOT NULL;

-- The runtime role inserts warnings and may change only what closing one changes.
REVOKE UPDATE, DELETE ON telemetry.excursion_warnings FROM veritrace_telemetry_app;
GRANT UPDATE (closed_at, outcome, incident_id, lead_time_seconds, updated_at)
    ON telemetry.excursion_warnings TO veritrace_telemetry_app;

-- +goose Down
DROP TABLE telemetry.excursion_warnings;
