-- V006__car_image_text.sql
--
-- Cache what OCR read out of each car's gallery photos.
--
-- Dealer galleries carry two kinds of image that are not photographs of the car: a
-- scan of the Monroney window sticker, and a rendered "Vehicle Highlights" slide
-- listing packages and equipment. backend/vision/image_text.py reads them with Apple's
-- Vision framework. That pass costs roughly a second per image and 114,901 of 122,663
-- active cars carry a gallery, so the result has to be durable -- re-OCRing the fleet
-- because a process restarted is not acceptable.
--
-- This table is that cache, and it is also what makes the pass resumable: a row at the
-- current `version` is skipped, a row at an older version is eligible for re-extraction
-- when classification or parsing improves.
--
-- Deliberately NOT written into `cars`. Everything here is a *candidate* fact from a
-- low-trust source (OCR of a third-party image), and this project has already been
-- burned by promoting unprovenanced candidates straight into the vehicle record -- the
-- trim "adds" incident produced 3,185 overlay files of which only 13 carried provenance.
-- Adoption is a separate, gated step; `summary` keeps the source image URL and the OCR
-- confidence behind every candidate so that gate has evidence to weigh.
--
-- `images_rejected_json` is retained rather than discarded so a fleet run can be
-- audited. A silently dropped image looks identical to an image that contained nothing,
-- and the difference matters when the reason was "this sticker belongs to another VIN".

CREATE TABLE IF NOT EXISTS car_image_text (
    car_id                INTEGER PRIMARY KEY,
    vin                   TEXT,
    version               INTEGER NOT NULL DEFAULT 0,
    extracted_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Full candidate record: equipment[], packages[], priced_options[],
    -- sticker_msrp, sticker_image_urls[], dealer_domains[].
    summary               JSONB,

    images_seen           INTEGER NOT NULL DEFAULT 0,
    images_read           INTEGER NOT NULL DEFAULT 0,
    images_rejected_json  JSONB,

    -- Denormalised so the common "which cars did we actually learn something from?"
    -- query does not have to open the JSON.
    has_sticker           BOOLEAN NOT NULL DEFAULT FALSE,
    sticker_msrp          DOUBLE PRECISION,
    equipment_count       INTEGER NOT NULL DEFAULT 0
);

-- Drives the resumable pass: "give me cars not yet done at this version".
CREATE INDEX IF NOT EXISTS idx_car_image_text_version
    ON car_image_text (version);

-- Attribution work reads dealer domains found in photos; keep the lookup cheap.
CREATE INDEX IF NOT EXISTS idx_car_image_text_sticker
    ON car_image_text (has_sticker)
    WHERE has_sticker;
