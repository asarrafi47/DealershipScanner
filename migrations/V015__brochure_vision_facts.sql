-- V015__brochure_vision_facts.sql
--
-- One row per fact a vision-reading agent saw on one page of one brochure PDF,
-- carrying the same discipline as option_rejections and the package registry:
-- provenance travels with the row, and nothing here is trusted until reviewed.
--
-- Why a new table instead of catalog_trims / catalog_packages: those tables
-- have no citation columns at all (no source document, no page, no quoted
-- text) -- writing brochure-vision output into them would recreate the exact
-- defect this project already found and is trying to fix: 3,172 of 3,185
-- trim-adds overlay files carry no provenance. Every row here must be able to
-- say which PDF, which page, and what it actually read before it can be
-- trusted anywhere else.
--
-- review_verdict is NULL until a second, independent agent re-checks the
-- fact against the same page image. Nothing downstream may read a row whose
-- review_verdict is not 'confirmed' -- unreviewed and rejected rows are kept,
-- never deleted, so a bad extractor run is visible and auditable rather than
-- silently vanished.

CREATE TABLE IF NOT EXISTS brochure_vision_facts (
    id                  BIGSERIAL PRIMARY KEY,

    year                INTEGER NOT NULL,
    make                TEXT NOT NULL,
    model               TEXT NOT NULL,
    trim                TEXT,

    -- 'spec' (engine/mpg/etc), 'option', 'package', 'package_feature', 'trim_add'
    fact_type           TEXT NOT NULL,
    name                TEXT NOT NULL,
    value_text          TEXT,
    price               NUMERIC,

    -- provenance: exactly what page said this, in the reader's own words
    source_pdf_sha256   TEXT NOT NULL,
    source_pdf_path     TEXT NOT NULL,
    page_number         INTEGER NOT NULL,
    quoted_text         TEXT,

    extracted_by        TEXT NOT NULL,
    extracted_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- 'confirmed' | 'rejected' | NULL (not yet reviewed)
    review_verdict       TEXT,
    review_notes          TEXT,
    reviewed_by            TEXT,
    reviewed_at             TIMESTAMPTZ,

    UNIQUE (source_pdf_sha256, page_number, fact_type, name)
);

CREATE INDEX IF NOT EXISTS idx_brochure_vision_facts_ymmt
    ON brochure_vision_facts (year, make, model, trim);

CREATE INDEX IF NOT EXISTS idx_brochure_vision_facts_review
    ON brochure_vision_facts (review_verdict);

CREATE INDEX IF NOT EXISTS idx_brochure_vision_facts_source
    ON brochure_vision_facts (source_pdf_sha256);
