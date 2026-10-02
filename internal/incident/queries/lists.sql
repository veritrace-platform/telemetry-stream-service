-- name: ShipmentIncidents :many
-- A page of one shipment's incidents, newest first. Incident IDs are time-ordered by start.
SELECT sqlc.embed(i), p.product_name, p.gtin, p.lot_number
FROM telemetry.cold_chain_incidents i
JOIN telemetry.shipment_projection p ON p.sscc = i.sscc
WHERE i.sscc = @sscc
  AND (sqlc.narg(after)::uuid IS NULL OR i.id < sqlc.narg(after)::uuid)
ORDER BY i.id DESC
LIMIT @page_limit;

-- name: VisibleIncidents :many
-- A page of the incidents of the shipments that a tenant takes part in, newest first. A driver sees only the
-- shipments assigned to it. state is 'open', 'resolved', or NULL for both.
SELECT sqlc.embed(i), p.product_name, p.gtin, p.lot_number
FROM telemetry.cold_chain_incidents i
JOIN telemetry.shipment_projection p ON p.sscc = i.sscc
WHERE p.participant_tenant_ids @> ARRAY[@tenant_id::uuid]
  AND (sqlc.narg(driver_id)::uuid IS NULL OR p.assigned_driver_id = sqlc.narg(driver_id)::uuid)
  AND (sqlc.narg(state)::text IS NULL
       OR (sqlc.narg(state)::text = 'open') = (i.ended_at IS NULL))
  AND (sqlc.narg(after)::uuid IS NULL OR i.id < sqlc.narg(after)::uuid)
ORDER BY i.id DESC
LIMIT @page_limit;

-- name: IncidentSummary :one
-- Open incidents, and incidents confirmed since a time, of the shipments that a tenant takes part in.
SELECT count(*) FILTER (WHERE i.ended_at IS NULL) AS open_count,
       count(*) FILTER (WHERE i.confirmed_at >= @since::timestamptz) AS recent_count
FROM telemetry.cold_chain_incidents i
JOIN telemetry.shipment_projection p ON p.sscc = i.sscc
WHERE p.participant_tenant_ids @> ARRAY[@tenant_id::uuid]
  AND (sqlc.narg(driver_id)::uuid IS NULL OR p.assigned_driver_id = sqlc.narg(driver_id)::uuid)
  AND (i.ended_at IS NULL OR i.confirmed_at >= @since::timestamptz);
