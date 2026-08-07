-- V012__dealer_portal.sql
--
-- Dealer-entered inventory moves into the inventory Postgres database.
--
-- Until now the dealer portal (backend/dealer/routes.py) kept its rows in a sidecar
-- SQLite file, ``dealer_portal.db``, sitting next to the repository. That was fine when
-- everything was SQLite, but production inventory moved to Postgres and the web process
-- runs on a platform whose filesystem is ephemeral: a redeploy discards the file, and a
-- second worker or a one-off script sees whatever copy happens to be on its own disk.
-- These are rows a dealer typed in by hand -- the one class of data we cannot re-scan
-- our way back to -- so they belong in the database that is backed up and carries the
-- migration chain, not in a file nothing else can see.
--
-- Shape notes, so nobody "fixes" them later:
--
--   * ``user_id`` is an app ``users.id``, but there is deliberately NO foreign key:
--     app users live in the separate users store (backend/db/users_sqlite.py), not in
--     this database, so there is no parent table here to reference. Deletion is handled
--     in application code (delete_vehicles_for_user on account deletion).
--   * ``created_at`` / ``updated_at`` are TEXT holding ISO-8601, not TIMESTAMPTZ.
--     backend/db/dealer_portal_db.py writes isoformat() strings and returns rows
--     unconverted on both backends; TEXT keeps the SQLite and Postgres read shapes
--     identical (the same trade incomplete_listings made). ISO-8601 sorts
--     lexicographically, so ORDER BY updated_at DESC still means newest first.
--   * ``gallery_json`` is a TEXT JSON array of upload paths, not JSONB, for the same
--     parity reason -- the module is the only reader and it json.loads() the value.
--   * ``UNIQUE (user_id, vin)`` is load-bearing: the add-VIN route relies on the
--     constraint violation (surfaced as sqlite3.IntegrityError on both backends) to
--     reject a VIN the dealer already listed.
--
-- The module re-asserts this DDL idempotently at startup (init_dealer_portal_db), so a
-- fresh dev box works before running the chain; this file is the schema of record.
-- Existing sidecar data is copied by backend/scripts/migrate_dealer_portal_sqlite_to_postgres.py.

CREATE TABLE IF NOT EXISTS dealer_vehicles (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    vin TEXT NOT NULL,
    title TEXT,
    year INTEGER,
    make TEXT,
    model TEXT,
    trim TEXT,
    price DOUBLE PRECISION,
    mileage INTEGER,
    transmission TEXT,
    drivetrain TEXT,
    fuel_type TEXT,
    exterior_color TEXT,
    interior_color TEXT,
    cylinders INTEGER,
    body_style TEXT,
    engine_description TEXT,
    stock_number TEXT,
    notes TEXT,
    gallery_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (user_id, vin)
);

-- The portal's only read patterns: "my inventory" (by user) and duplicate-VIN checks.
CREATE INDEX IF NOT EXISTS idx_dealer_vehicles_user ON dealer_vehicles (user_id);
CREATE INDEX IF NOT EXISTS idx_dealer_vehicles_vin ON dealer_vehicles (vin);
