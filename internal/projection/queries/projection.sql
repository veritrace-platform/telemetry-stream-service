-- name: CreateShipment :execrows
-- A shipment delivered twice is created once.
INSERT INTO telemetry.shipment_projection (
    sscc, shipment_id, owner_tenant_id, participant_tenant_ids, assigned_driver_id, status, gtin, product_name,
    lot_number, min_temp_celsius, max_temp_celsius, production_date, expiration_date, shelf_life_q10,
    last_event_sequence)
VALUES (
    @sscc, @shipment_id, @owner_tenant_id, @participant_tenant_ids::uuid[], sqlc.narg(assigned_driver_id), @status,
    @gtin, @product_name, @lot_number, @min_temp_celsius, @max_temp_celsius, sqlc.narg(production_date)::date,
    sqlc.narg(expiration_date)::date, sqlc.narg(shelf_life_q10)::numeric, @sequence)
ON CONFLICT (sscc) DO NOTHING;

-- name: Advance :execrows
-- Applies an event that follows the last applied one: the new status, a tenant that joined, and a driver
-- assignment that started or ended. The first event that takes the shipment out of CREATED and IN_TRANSIT ends its
-- monitoring. Events at or below the last applied sequence change nothing.
UPDATE telemetry.shipment_projection
SET status = @status,
    monitoring_ended_at = CASE
        WHEN monitoring_ended_at IS NULL AND @status NOT IN ('CREATED', 'IN_TRANSIT') THEN @occurred_at::timestamptz
        ELSE monitoring_ended_at
    END,
    last_event_sequence = @sequence,
    participant_tenant_ids = CASE
        WHEN sqlc.narg(joined_tenant_id)::uuid IS NULL
            OR sqlc.narg(joined_tenant_id)::uuid = ANY (participant_tenant_ids) THEN participant_tenant_ids
        ELSE array_append(participant_tenant_ids, sqlc.narg(joined_tenant_id)::uuid)
    END,
    assigned_driver_id = CASE
        WHEN @change_driver::boolean THEN sqlc.narg(assigned_driver_id)::uuid
        ELSE assigned_driver_id
    END,
    updated_at = now()
WHERE sscc = @sscc
  AND last_event_sequence < @sequence;

-- name: LastEventSequence :one
SELECT last_event_sequence
FROM telemetry.shipment_projection
WHERE sscc = @sscc;

-- name: GetShipment :one
SELECT sscc, shipment_id, owner_tenant_id, participant_tenant_ids, assigned_driver_id, status, gtin, product_name,
       lot_number, min_temp_celsius, max_temp_celsius, last_event_sequence, updated_at, production_date,
       expiration_date, shelf_life_q10, monitoring_ended_at, shelf_life_status
FROM telemetry.shipment_projection
WHERE sscc = @sscc;

-- name: LockShipments :many
-- Reads the projections of a detector batch and keeps their status from changing until the transaction ends.
-- The rows are locked in SSCC order.
SELECT sscc, shipment_id, owner_tenant_id, participant_tenant_ids, assigned_driver_id, status, gtin, product_name,
       lot_number, min_temp_celsius, max_temp_celsius, last_event_sequence, updated_at, production_date,
       expiration_date, shelf_life_q10, monitoring_ended_at, shelf_life_status
FROM telemetry.shipment_projection
WHERE sscc = ANY (CAST(sqlc.arg(ssccs)::text[] AS bpchar[]))
ORDER BY sscc
FOR SHARE;
