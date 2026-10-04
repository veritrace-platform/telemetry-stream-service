-- Shipment projection (data-model.md §4; ADR-0008). Built from the shipment.events topic: it tells the detector
-- which readings to evaluate and against which bounds, and tells the read API and the notification hub which
-- tenants and driver may see a shipment's telemetry.

-- +goose Up
CREATE TABLE telemetry.shipment_projection (
    sscc char(18) PRIMARY KEY,
    shipment_id uuid NOT NULL,
    owner_tenant_id uuid NOT NULL,
    participant_tenant_ids uuid[] NOT NULL,
    assigned_driver_id uuid,
    status text NOT NULL,
    gtin text NOT NULL,
    product_name text NOT NULL,
    lot_number text NOT NULL,
    min_temp_celsius numeric(5, 2) NOT NULL,
    max_temp_celsius numeric(5, 2) NOT NULL,
    -- The sequence of the last event applied; events at or below it are duplicates.
    last_event_sequence integer NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT shipment_projection_shipment_id_key UNIQUE (shipment_id),
    CONSTRAINT shipment_projection_status_check
        CHECK (status IN ('CREATED', 'IN_TRANSIT', 'DELIVERED', 'CANCELLED', 'RECALLED')),
    CONSTRAINT shipment_projection_participant_tenant_ids_check
        CHECK (owner_tenant_id = ANY (participant_tenant_ids)),
    CONSTRAINT shipment_projection_temperature_range_check CHECK (min_temp_celsius < max_temp_celsius),
    CONSTRAINT shipment_projection_last_event_sequence_check CHECK (last_event_sequence >= 1)
);

CREATE INDEX shipment_projection_participant_tenant_ids_idx
    ON telemetry.shipment_projection USING gin (participant_tenant_ids);

-- Shipments are never deleted.
REVOKE DELETE ON telemetry.shipment_projection FROM veritrace_telemetry_app;

-- +goose Down
DROP TABLE telemetry.shipment_projection;
