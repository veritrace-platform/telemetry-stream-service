-- Shelf-life inputs of the shipment projection (data-model.md §4; cold-chain-monitoring.md §8). shipment.created
-- carries the lot's dates and Q10; events produced before they were added lack them, so the columns are nullable.
-- monitoring_ended_at bounds the readings of a shipment's thermal exposure, and shelf_life_status remembers the
-- worst shelf-life status notified, so that each is notified once.

-- +goose Up
ALTER TABLE telemetry.shipment_projection
    ADD COLUMN production_date date,
    ADD COLUMN expiration_date date,
    ADD COLUMN shelf_life_q10 numeric(4, 2),
    ADD COLUMN monitoring_ended_at timestamptz,
    ADD COLUMN shelf_life_status text NOT NULL DEFAULT 'INTACT',
    ADD CONSTRAINT shipment_projection_expiration_date_check CHECK (expiration_date >= production_date),
    ADD CONSTRAINT shipment_projection_shelf_life_q10_check CHECK (shelf_life_q10 BETWEEN 1 AND 10),
    ADD CONSTRAINT shipment_projection_shelf_life_status_check
        CHECK (shelf_life_status IN ('INTACT', 'REDUCED', 'EXPIRED'));

-- +goose Down
ALTER TABLE telemetry.shipment_projection
    DROP COLUMN shelf_life_status,
    DROP COLUMN monitoring_ended_at,
    DROP COLUMN shelf_life_q10,
    DROP COLUMN expiration_date,
    DROP COLUMN production_date;
