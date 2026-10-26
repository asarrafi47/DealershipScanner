-- V033__listing_miss_strikes.sql
--
-- N9 P5B.4 (D-RC2): two-strike retirement. The first run that proves a car is gone
-- (plan_retirement named it) records a strike here and retires nothing; a later run at
-- least SCANNER_STRIKE_GAP_HOURS (default 20) after the first miss that names the car
-- again retires it (with a listing_retirements row) and deletes the strike. A run that
-- sees the car again clears its strike. One row per car.
--
--   SELECT dealer_id, count(*) FROM listing_miss_strikes GROUP BY 1 ORDER BY 2 DESC;
--
-- Runtime copy (lazy, for a database behind the chain): backend/db/retirement_log.py.
-- (The plan assigned V029; the next free number on the integration head is V033.)

CREATE TABLE IF NOT EXISTS public.listing_miss_strikes (
    car_id bigint PRIMARY KEY,
    dealer_id text NOT NULL,
    vin text,
    strikes integer NOT NULL DEFAULT 1,
    first_missed_at text NOT NULL,
    last_missed_at text NOT NULL,
    source text
);

CREATE INDEX IF NOT EXISTS idx_listing_miss_strikes_dealer
    ON public.listing_miss_strikes (dealer_id);
