-- V011__car_attribution.sql
--
-- Whether we actually know which rooftop a car sits at.
--
-- The problem this records, stated concretely. `bmwofmurrieta-com` holds 1,921 active cars
-- across 37 makes. Of the 168 whose photographs name a dealer, **22 say "BMW of Murrieta"**.
-- The rest name other Hendrick Automotive Group rooftops -- Hendrick Porsche, Rick Hendrick
-- Chevrolet Duluth, Hendrick Lexus Northlake, Stevenson-Hendrick, Mall of Georgia Mazda --
-- in Georgia, North Carolina and Tennessee. The scanner pointed at bmwofmurrieta.com and was
-- served HendrickCars.com's national inventory, then filed all of it under one California
-- store. `mbontario-com`, `bmwofmonrovia-net` and others do the same thing.
--
-- No car is listed twice: a cross-dealer duplicate-VIN check over the whole fleet returns
-- ZERO. Each car was assigned to exactly one rooftop, and for most of these it is the wrong
-- one. So there is no cheap dedup signal; the resolving evidence is what the photographs say.
--
-- Why a separate table rather than a column on `cars`:
--
--   * `cars` is the scanner's write surface and is rewritten on every scan. An attribution
--     judgement derived from photographs would be clobbered nightly.
--   * The judgement has provenance -- which rooftop, from what evidence, when. That does not
--     belong in a single column, and squeezing it into one is how the trim "adds" ended up
--     unattributable.
--   * Read-time joins keep the catalogue/observation split this project already follows:
--     facts we derived never get written back into the scanned record.
--
-- `status` values:
--   confirmed   -- a photograph names the dealer the car is filed under. Attribution holds.
--   conflicting -- a photograph names a DIFFERENT rooftop. The filing is wrong.
--   unverified  -- no photographic evidence, and the car sits at a dealer known to be fed a
--                  group-wide feed. We do not know where it is, and the site must not claim to.
--
-- Deliberately NOT a re-attribution. Moving a car to `observed_rooftop` requires that rooftop
-- to be registered, geocoded, and confirmed -- doing it from a plate frame alone would repeat
-- the Sold-To regression, where an invoice rooftop overwrote a correct plate-frame reading.
-- This table records what we know; moving cars is a separate, reversible step.

CREATE TABLE IF NOT EXISTS car_attribution (
    car_id           BIGINT PRIMARY KEY,

    status           TEXT NOT NULL CHECK (status IN ('confirmed', 'conflicting', 'unverified')),

    -- The rooftop the photographs actually name, verbatim, when there is one.
    observed_rooftop TEXT,

    -- What the dealer was filed as at the time of the judgement, so a later re-scan that
    -- changes dealer_id does not silently make this row look consistent.
    filed_dealer_id  TEXT,

    -- Free text: which signal (plate frame, signage, watermark, Sold-To box), plus VIN-decode
    -- corroboration where the make itself contradicts the franchise.
    evidence         TEXT,

    decided_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_car_attribution_status
    ON car_attribution (status);

-- "Which dealers are we misreporting, and how badly" is the query this exists to answer.
CREATE INDEX IF NOT EXISTS idx_car_attribution_filed
    ON car_attribution (filed_dealer_id, status);


-- Dealer-level: is this host serving its own rooftop, or the whole group?
--
-- A car with no photograph is only suspect if its dealer is known to be fed a group feed.
-- Without this, "unverified" would smear across the entire fleet and mean nothing.
CREATE TABLE IF NOT EXISTS dealer_feed_scope (
    dealer_id        TEXT PRIMARY KEY,

    -- rooftop -- the feed serves this store only.
    -- group   -- the feed serves several rooftops; attribution here is unreliable.
    scope            TEXT NOT NULL CHECK (scope IN ('rooftop', 'group')),

    -- Evidence counts behind the call.
    photos_naming_self   INTEGER NOT NULL DEFAULT 0,
    photos_naming_other  INTEGER NOT NULL DEFAULT 0,
    distinct_rooftops    INTEGER NOT NULL DEFAULT 0,

    decided_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
