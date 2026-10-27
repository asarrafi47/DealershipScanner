-- V035__vdp_fetch_attempts.sql
--
-- N18 P12A.5 (W05b, IL-12): per-VIN attempt ledger for the HTTP-first VDP prefetch. A detail
-- page that could not be fetched (blocked, 404, error) records one attempt here; a car with
-- SCANNER_VDP_ATTEMPT_MAX (default 3) failed attempts is skipped until a back-off that doubles
-- with every further failure has passed, so a blocked or dead URL is no longer re-tried on
-- every run while the wall clock goes to cars that were never tried. A fetched page deletes
-- the row. One row per (dealer, VIN).
--
--   SELECT dealer_id, count(*), sum(attempts) FROM vdp_fetch_attempts GROUP BY 1 ORDER BY 3 DESC;
--
-- Runtime copy (lazy, for a database behind the chain): backend/db/vdp_attempts.py.
-- (The plan assigned no number; the next free number on the integration head is V035.)

CREATE TABLE IF NOT EXISTS public.vdp_fetch_attempts (
    dealer_id text NOT NULL,
    vin text NOT NULL,
    attempts integer NOT NULL DEFAULT 1,
    last_status integer,
    last_outcome text,
    first_attempt_at text NOT NULL,
    last_attempt_at text NOT NULL,
    PRIMARY KEY (dealer_id, vin)
);
