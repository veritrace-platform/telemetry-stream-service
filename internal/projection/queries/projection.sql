-- name: CreateShipment :execrows
-- A shipment delivered twice is created once.
INSERT INTO telemetry.shipment_projection (
    sscc, shipment_id, owner_tenant_id, participant_tenant_ids, assigned_driver_id, status, gtin, product_name,
    lot_number, min_temp_celsius, max_temp_celsius, last_event_sequence)
VALUES (
    @sscc, @shipment_id, @owner_tenant_id, @participant_tenant_ids::uuid[], sqlc.narg(assigned_driver_id), @status,
    @gtin, @product_name, @lot_number, @min_temp_celsius, @max_temp_celsius, @sequence)
ON CONFLICT (sscc) DO NOTHING;

-- name: Advance :execrows
-- Applies an event that follows the last applied one: the new status, a tenant that joined, and a driver
-- assignment that started or ended. Events at or below the last applied sequence change nothing.
UPDATE telemetry.shipment_projection
SET status = @status,
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
       lot_number, min_temp_celsius, max_temp_celsius, last_event_sequence, updated_at
FROM telemetry.shipment_projection
WHERE sscc = @sscc;

-- name: GetShipments :many
SELECT sscc, shipment_id, owner_tenant_id, participant_tenant_ids, assigned_driver_id, status, gtin, product_name,
       lot_number, min_temp_celsius, max_temp_celsius, last_event_sequence, updated_at
FROM telemetry.shipment_projection
WHERE sscc = ANY (CAST(sqlc.arg(ssccs)::text[] AS bpchar[]));
