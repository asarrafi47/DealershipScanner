-- V010__trim_msrp_bands.sql
--
-- What a given trim has actually been observed to sticker at.
--
-- Built from window stickers we have READ -- parsed PDFs and, since the vision backfill,
-- photographed Monroneys whose totals were verified against the car's VIN.
--
-- This is a PLAUSIBILITY BAND, not a price. It must never be used to fill in a missing
-- MSRP, and the numbers say why: across 59 observed 2026 BMW X5 xDrive40i stickers the
-- totals run $76,375 to $93,125. That $16,750 spread is not measurement error, it is
-- options -- a Monroney total is base + options + destination, so two cars of the same
-- trim legitimately differ by that much. Quoting the median as "this car's MSRP" would be
-- wrong by five figures on a shopper-facing page, which is the same class of invented
-- value that produced the fabricated trim "adds" and the AI-sourced spec numbers.
--
-- What it is good for is the opposite question: is a number we already hold believable?
-- An MSRP far outside everything a trim has ever stickered at is a misread, a currency
-- mix-up, or another car's document. That check needs a floor and a ceiling, not a point
-- estimate, and it is the one thing a distribution can honestly provide.
--
-- Rebuilt in full by backend/scripts/build_trim_msrp_bands.py; safe to truncate and
-- regenerate, since every row is derived.

CREATE TABLE IF NOT EXISTS trim_msrp_bands (
    year            INTEGER NOT NULL,
    make            TEXT    NOT NULL,
    model           TEXT    NOT NULL,
    trim            TEXT    NOT NULL DEFAULT '',

    observations    INTEGER NOT NULL,
    msrp_min        INTEGER NOT NULL,
    msrp_p25        INTEGER,
    msrp_median     INTEGER NOT NULL,
    msrp_p75        INTEGER,
    msrp_max        INTEGER NOT NULL,

    -- Which readers contributed, e.g. {"sticker_pdf": 12, "sticker_photo": 47}.
    sources         JSONB,
    built_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (year, make, model, trim)
);

CREATE INDEX IF NOT EXISTS idx_trim_msrp_bands_lookup
    ON trim_msrp_bands (make, model, year);
