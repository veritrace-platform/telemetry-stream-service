-- Application schema, runtime grants, and default privileges for the telemetry database.
-- Roles veritrace_telemetry_owner (runs this) and veritrace_telemetry_app, and the timescaledb
-- extension, are created by infrastructure bootstrap.

-- +goose Up
CREATE SCHEMA telemetry;
REVOKE ALL ON SCHEMA telemetry FROM PUBLIC;
GRANT USAGE ON SCHEMA telemetry TO veritrace_telemetry_app;

ALTER DEFAULT PRIVILEGES IN SCHEMA telemetry
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO veritrace_telemetry_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA telemetry
    GRANT USAGE, SELECT ON SEQUENCES TO veritrace_telemetry_app;

-- Functions are not executable by default; each one is granted explicitly.
ALTER DEFAULT PRIVILEGES
    REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;

-- +goose Down
ALTER DEFAULT PRIVILEGES
    GRANT EXECUTE ON FUNCTIONS TO PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA telemetry
    REVOKE USAGE, SELECT ON SEQUENCES FROM veritrace_telemetry_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA telemetry
    REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM veritrace_telemetry_app;
DROP SCHEMA telemetry;
