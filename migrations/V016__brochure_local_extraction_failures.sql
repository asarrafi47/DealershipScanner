-- V016__brochure_local_extraction_failures.sql
--
-- Every brochure/page the local-LLM extraction pipeline could not read, and why.
-- Purpose: this is the hand-off queue to the Sonnet cloud workflow
-- (backend/scripts/brochure_vision_batch.js pattern) -- a brochure that failed
-- locally gets picked up there instead of silently having no coverage.
--
-- Same discipline as option_rejections: append-only, never deleted, so a
-- transient failure (server hiccup) and a structural one (corrupt PDF) are
-- both visible and a re-run can tell them apart by rerun_count/last reason.

CREATE TABLE IF NOT EXISTS brochure_local_extraction_failures (
    id                  BIGSERIAL PRIMARY KEY,

    source_pdf_sha256   TEXT NOT NULL,
    source_pdf_path     TEXT NOT NULL,
    page_number         INTEGER,        -- NULL = whole-document failure (e.g. rasterize)

    stage               TEXT NOT NULL,  -- 'rasterize' | 'extract_request' | 'extract_parse'
    reason              TEXT NOT NULL,

    attempts            INTEGER NOT NULL DEFAULT 1,
    first_failed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_failed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    handed_to_cloud_at  TIMESTAMPTZ,    -- set once a Sonnet workflow has picked this up

    UNIQUE (source_pdf_sha256, page_number, stage)
);

CREATE INDEX IF NOT EXISTS idx_brochure_local_failures_pending
    ON brochure_local_extraction_failures (handed_to_cloud_at)
    WHERE handed_to_cloud_at IS NULL;
