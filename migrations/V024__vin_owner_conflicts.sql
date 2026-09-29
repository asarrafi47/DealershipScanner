-- V024__vin_owner_conflicts.sql
--
-- VIN ownership guard (2026-09-29 Railway fleet incident, docs/RAILWAY_SCANNING.md).
-- `cars` is one row per VIN and the scanner upsert is ON CONFLICT(vin), so the
-- last store to write a VIN owned it; the first full fleet run reassigned 6,961
-- VINs to the wrong dealer. backend/scanner/database.py upsert_vehicles now
-- refuses to move a VIN whose stored row is active and scraped within
-- SCANNER_VIN_OWNER_GUARD_HOURS (default 48) under a different dealer_id, skips
-- that vehicle's write, and records the refusal here.
--
-- One row per (vin, owner, claimant); seen_at (ISO-8601 UTC text written by the
-- scanner) is the latest sighting, so the table stays bounded across nightly
-- runs. Pairs that recur are either a group feed replayed by a sibling store or
-- the same store under two entity ids (hughwhitehonda-com / hughwhitehonda-net),
-- which the guard cannot tell apart; this table is where to find them:
--
--   SELECT owner_dealer_id, claimant_dealer_id, COUNT(*), MAX(seen_at)
--     FROM vin_owner_conflicts GROUP BY 1, 2 ORDER BY 3 DESC;
--
-- The scanner also creates this table lazily (CREATE TABLE IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS vin_owner_conflicts (
    vin                 TEXT NOT NULL,
    owner_dealer_id     TEXT NOT NULL,
    claimant_dealer_id  TEXT NOT NULL,
    seen_at             TEXT NOT NULL,
    PRIMARY KEY (vin, owner_dealer_id, claimant_dealer_id)
);

CREATE INDEX IF NOT EXISTS idx_vin_owner_conflicts_pair
    ON vin_owner_conflicts (claimant_dealer_id, owner_dealer_id);
