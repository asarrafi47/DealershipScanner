-- V007__car_photo_attribution.sql
--
-- Which rooftop a car's own photographs say it is sitting at.
--
-- Group feeds serve the whole group to every rooftop's host, so a car scanned from
-- hileyvwhuntsville.com may in fact be at the group's Audi or Mazda store. Counting
-- makes does not settle it -- a VW dealer legitimately takes BMW trade-ins -- but the
-- photographs frequently do: the dealer's own signage is in the frame, and the
-- "Vehicle Highlights" slide carries the selling store's domain outright. Reading
-- 337 cars filed under Hiley VW turned up audihuntsville.com on cars whose backdrop
-- is the Audi building.
--
-- Rows here are evidence plus a decision, kept separate from `cars` for two reasons:
--
--   1. `original_dealer_id` makes every move reversible. The scanner rewrites
--      cars.dealer_id on each pass, so without a record of what it was, a wrong
--      reattribution is unrecoverable.
--   2. A domain that resolves to no known dealer is still worth keeping. Audi
--      Huntsville is not in the 592-row dealerships registry, so those cars cannot be
--      moved anywhere -- but the evidence says they do not belong where they are, and
--      that is a prompt to add the rooftop rather than something to discard.
--
-- `resolved_dealer_id` NULL therefore means "evidence found, no destination known",
-- which is deliberately different from having no evidence at all (no row).

CREATE TABLE IF NOT EXISTS car_photo_attribution (
    car_id               INTEGER PRIMARY KEY,
    vin                  TEXT,

    -- Where the scanner filed it, captured before any move.
    original_dealer_id   TEXT NOT NULL,

    -- The dealer domain read out of the car's own photographs.
    photo_domain         TEXT NOT NULL,
    -- How many of this car's images carried that domain. One is weak; several is not.
    evidence_images      INTEGER NOT NULL DEFAULT 1,
    evidence_image_url   TEXT,

    -- The known rooftop that domain maps to, or NULL when we do not scan it.
    resolved_dealer_id   TEXT,

    -- Set once the move has actually been written to cars.dealer_id.
    applied_at           TIMESTAMPTZ,
    detected_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- "Show me everything still awaiting a decision" and "everything pointing at a rooftop
-- we do not have" are both common; index the columns those filter on.
CREATE INDEX IF NOT EXISTS idx_car_photo_attr_resolved
    ON car_photo_attribution (resolved_dealer_id);

CREATE INDEX IF NOT EXISTS idx_car_photo_attr_pending
    ON car_photo_attribution (original_dealer_id)
    WHERE applied_at IS NULL;
