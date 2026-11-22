-- name: ThermalExposure :one
-- The sums behind a shipment's thermal exposure over its readings recorded until a time (cold-chain-monitoring.md,
-- section 8). Readings of all devices merge in device-time order; each stands for the time until the next one, at
-- most max_interval_seconds, and the rest of the gap is uncovered. The times of the first and last reading are the
-- epoch when there are none.
WITH ordered AS (
    SELECT recorded_at,
           temperature_celsius::double precision AS t,
           lead(recorded_at) OVER (ORDER BY recorded_at, device_id) AS next_at
    FROM telemetry.sensor_readings
    WHERE sscc = sqlc.arg(sscc)
      AND recorded_at <= sqlc.arg(until)::timestamptz
), intervals AS (
    SELECT recorded_at,
           t,
           coalesce(date_part('epoch', next_at - recorded_at), 0) AS gap,
           least(coalesce(date_part('epoch', next_at - recorded_at), 0),
                 sqlc.arg(max_interval_seconds)::double precision) AS dt
    FROM ordered
)
SELECT count(*)::bigint AS reading_count,
       coalesce(min(recorded_at), 'epoch')::timestamptz AS first_reading_at,
       coalesce(max(recorded_at), 'epoch')::timestamptz AS last_reading_at,
       coalesce(min(t), 0)::double precision AS lowest_celsius,
       coalesce(max(t), 0)::double precision AS highest_celsius,
       coalesce(sum(dt), 0)::double precision AS covered_seconds,
       coalesce(sum(gap - dt), 0)::double precision AS uncovered_seconds,
       coalesce(sum(dt * t), 0)::double precision AS celsius_seconds,
       -- e^(-(dH/R)/T) with dH/R = 10 000 K and T in kelvin (mean kinetic temperature).
       coalesce(sum(dt * exp(-10000.0 / (t + 273.15))), 0)::double precision AS kinetic_seconds,
       coalesce(sum(dt) FILTER (WHERE t > sqlc.arg(max_celsius)::double precision), 0)::double precision
           AS above_max_seconds,
       coalesce(sum(dt) FILTER (WHERE t < sqlc.arg(min_celsius)::double precision), 0)::double precision
           AS below_min_seconds,
       coalesce(sum(dt * (t - sqlc.arg(max_celsius)::double precision))
                FILTER (WHERE t > sqlc.arg(max_celsius)::double precision), 0)::double precision
           AS above_max_celsius_seconds,
       coalesce(sum(dt * (sqlc.arg(min_celsius)::double precision - t))
                FILTER (WHERE t < sqlc.arg(min_celsius)::double precision), 0)::double precision
           AS below_min_celsius_seconds,
       -- Q10^((T - T_max) / 10) - 1: how much faster than the label the goods age above the maximum.
       coalesce(sum(dt * (power(sqlc.arg(q10)::double precision, (t - sqlc.arg(max_celsius)::double precision) / 10) - 1))
                FILTER (WHERE t > sqlc.arg(max_celsius)::double precision), 0)::double precision
           AS extra_aging_seconds
FROM intervals;

-- name: RaiseShelfLifeStatus :execrows
-- Records a shelf-life status worse than the one notified so far (INTACT < REDUCED < EXPIRED). It changes nothing,
-- and reports no row, when the status is not worse.
UPDATE telemetry.shipment_projection
SET shelf_life_status = @status,
    updated_at = now()
WHERE sscc = @sscc
  AND array_position(ARRAY['INTACT', 'REDUCED', 'EXPIRED'], shelf_life_status)
      < array_position(ARRAY['INTACT', 'REDUCED', 'EXPIRED'], @status::text);
