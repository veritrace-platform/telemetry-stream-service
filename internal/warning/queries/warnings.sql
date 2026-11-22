-- name: InsertWarning :execrows
-- A warning issued again, when a batch is processed again, is inserted once. A second open warning of the SSCC is
-- refused by the partial unique index and changes nothing either.
INSERT INTO telemetry.excursion_warnings (
    id, sscc, shipment_id, device_id, issued_at, bound, bound_celsius, temperature_celsius, slope_celsius_per_minute,
    predicted_excursion_at)
VALUES (
    @id, @sscc, @shipment_id, @device_id, @issued_at, @bound, @bound_celsius, @temperature_celsius,
    @slope_celsius_per_minute, @predicted_excursion_at)
ON CONFLICT DO NOTHING;

-- name: OpenWarning :one
SELECT *
FROM telemetry.excursion_warnings
WHERE sscc = @sscc
  AND closed_at IS NULL;

-- name: CloseWarning :one
-- Closes an open warning as CLEARED or INCONCLUSIVE.
UPDATE telemetry.excursion_warnings
SET closed_at = @closed_at,
    outcome = @outcome,
    updated_at = now()
WHERE id = @id
  AND closed_at IS NULL
RETURNING *;

-- name: CloseBreached :one
-- Closes the open warning of an SSCC as BREACHED by the incident whose episode started at started_at, if the warning
-- was issued before that episode. Its lead time counts whole seconds from the warning to the start of the episode.
UPDATE telemetry.excursion_warnings
SET closed_at = @confirmed_at,
    outcome = 'BREACHED',
    incident_id = @incident_id,
    lead_time_seconds = floor(date_part('epoch', sqlc.arg(started_at)::timestamptz - issued_at))::integer,
    updated_at = now()
WHERE sscc = @sscc
  AND closed_at IS NULL
  AND issued_at <= sqlc.arg(started_at)::timestamptz
RETURNING *;

-- name: CloseAtShipmentEnd :one
-- Closes the open warning of a shipment that is no longer monitored as INCONCLUSIVE, at its last reading recorded
-- until the shipment left monitoring (never before the warning was issued).
UPDATE telemetry.excursion_warnings w
SET closed_at = GREATEST(w.issued_at, COALESCE((
        SELECT max(r.recorded_at)
        FROM telemetry.sensor_readings r
        WHERE r.sscc = w.sscc
          AND r.recorded_at >= w.issued_at
          AND r.recorded_at <= @until::timestamptz), w.issued_at)),
    outcome = 'INCONCLUSIVE',
    updated_at = now()
WHERE w.sscc = @sscc
  AND w.closed_at IS NULL
RETURNING *;

-- name: ShipmentWarnings :many
-- A page of one shipment's warnings, newest first. Warning IDs are time-ordered by issue.
SELECT sqlc.embed(w), p.product_name, p.gtin, p.lot_number
FROM telemetry.excursion_warnings w
JOIN telemetry.shipment_projection p ON p.sscc = w.sscc
WHERE w.sscc = @sscc
  AND (sqlc.narg(after)::uuid IS NULL OR w.id < sqlc.narg(after)::uuid)
ORDER BY w.id DESC
LIMIT @page_limit;

-- name: VisibleWarnings :many
-- A page of the warnings of the shipments that a tenant takes part in, newest first. A driver sees only the
-- shipments assigned to it. state is 'open', 'closed', or NULL for both.
SELECT sqlc.embed(w), p.product_name, p.gtin, p.lot_number
FROM telemetry.excursion_warnings w
JOIN telemetry.shipment_projection p ON p.sscc = w.sscc
WHERE p.participant_tenant_ids @> ARRAY[@tenant_id::uuid]
  AND (sqlc.narg(driver_id)::uuid IS NULL OR p.assigned_driver_id = sqlc.narg(driver_id)::uuid)
  AND (sqlc.narg(state)::text IS NULL
       OR (sqlc.narg(state)::text = 'open') = (w.closed_at IS NULL))
  AND (sqlc.narg(after)::uuid IS NULL OR w.id < sqlc.narg(after)::uuid)
ORDER BY w.id DESC
LIMIT @page_limit;

-- name: WarningPerformance :one
-- The forecast quality over [from, before) for the shipments in scope (cold-chain-monitoring.md, section 7.3): warnings by
-- outcome, counted by issue; incidents, counted by start, and those that a warning was closed as BREACHED with; and
-- the lead times and excursion time errors of the BREACHED warnings.
WITH visible AS (
    SELECT p.sscc
    FROM telemetry.shipment_projection p
    WHERE p.participant_tenant_ids @> ARRAY[@tenant_id::uuid]
      AND (sqlc.narg(driver_id)::uuid IS NULL OR p.assigned_driver_id = sqlc.narg(driver_id)::uuid)
      AND (sqlc.narg(sscc)::text IS NULL OR p.sscc = sqlc.narg(sscc)::bpchar)
), warnings AS (
    SELECT w.outcome, w.lead_time_seconds,
           abs(extract(epoch FROM i.started_at - w.predicted_excursion_at)) AS excursion_time_error_seconds
    FROM telemetry.excursion_warnings w
    JOIN visible v ON v.sscc = w.sscc
    LEFT JOIN telemetry.cold_chain_incidents i ON i.id = w.incident_id
    WHERE w.issued_at >= @from_time::timestamptz
      AND w.issued_at < @before::timestamptz
), incidents AS (
    SELECT EXISTS (SELECT 1 FROM telemetry.excursion_warnings w WHERE w.incident_id = i.id) AS warned
    FROM telemetry.cold_chain_incidents i
    JOIN visible v ON v.sscc = i.sscc
    WHERE i.started_at >= @from_time::timestamptz
      AND i.started_at < @before::timestamptz
)
SELECT
    (SELECT count(*) FROM warnings)::bigint AS issued,
    (SELECT count(*) FROM warnings WHERE outcome IS NULL)::bigint AS open,
    (SELECT count(*) FROM warnings WHERE outcome = 'BREACHED')::bigint AS breached,
    (SELECT count(*) FROM warnings WHERE outcome = 'CLEARED')::bigint AS cleared,
    (SELECT count(*) FROM warnings WHERE outcome = 'INCONCLUSIVE')::bigint AS inconclusive,
    (SELECT count(*) FROM incidents)::bigint AS incidents,
    (SELECT count(*) FROM incidents WHERE warned)::bigint AS warned_incidents,
    -- The statistics of BREACHED warnings are 0 when there are none; the caller tells by the breached count.
    coalesce((SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY lead_time_seconds) FROM warnings
              WHERE outcome = 'BREACHED'), 0)::double precision AS median_lead_time_seconds,
    coalesce((SELECT min(lead_time_seconds) FROM warnings WHERE outcome = 'BREACHED'), 0)::double precision
        AS min_lead_time_seconds,
    coalesce((SELECT avg(lead_time_seconds) FROM warnings WHERE outcome = 'BREACHED'), 0)::double precision
        AS mean_lead_time_seconds,
    coalesce((SELECT avg(excursion_time_error_seconds) FROM warnings WHERE outcome = 'BREACHED'), 0)::double precision
        AS mean_excursion_time_error_seconds;
