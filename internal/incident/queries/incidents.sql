-- name: InsertIncident :execrows
-- A breach confirmed again, when a batch is processed again, is inserted once.
INSERT INTO telemetry.cold_chain_incidents (
    id, sscc, shipment_id, device_id, started_at, confirmed_at, min_temp_celsius, max_temp_celsius,
    trigger_temperature_celsius, extreme_temperature_celsius, latitude, longitude, incident_hash)
VALUES (
    @id, @sscc, @shipment_id, @device_id, @started_at, @confirmed_at, @min_temp_celsius, @max_temp_celsius,
    @trigger_temperature_celsius, @extreme_temperature_celsius, @latitude, @longitude, @incident_hash)
ON CONFLICT (sscc, started_at) DO NOTHING;

-- name: ExtendIncident :execrows
UPDATE telemetry.cold_chain_incidents
SET extreme_temperature_celsius = @extreme_temperature_celsius,
    updated_at = now()
WHERE id = @id
  AND ended_at IS NULL;

-- name: ResolveIncident :execrows
UPDATE telemetry.cold_chain_incidents
SET ended_at = @ended_at,
    duration_seconds = @duration_seconds,
    extreme_temperature_celsius = @extreme_temperature_celsius,
    updated_at = now()
WHERE id = @id
  AND ended_at IS NULL;

-- name: LatestIncident :one
SELECT *
FROM telemetry.cold_chain_incidents
WHERE sscc = @sscc
ORDER BY started_at DESC
LIMIT 1;

-- name: ResolveAtShipmentEnd :one
-- Resolves the open incident of a shipment that is no longer monitored, at its last reading recorded until the
-- shipment left monitoring (never before the confirmation).
UPDATE telemetry.cold_chain_incidents i
SET ended_at = e.ended_at,
    duration_seconds = floor(extract(epoch FROM e.ended_at - i.started_at))::integer,
    updated_at = now()
FROM (
    SELECT o.id,
           GREATEST(o.confirmed_at, COALESCE((
               SELECT max(r.recorded_at)
               FROM telemetry.sensor_readings r
               WHERE r.sscc = o.sscc
                 AND r.recorded_at >= o.started_at
                 AND r.recorded_at <= @until::timestamptz), o.confirmed_at)) AS ended_at
    FROM telemetry.cold_chain_incidents o
    WHERE o.sscc = @sscc
      AND o.ended_at IS NULL
) e
WHERE i.id = e.id
  AND i.ended_at IS NULL
RETURNING i.*;
