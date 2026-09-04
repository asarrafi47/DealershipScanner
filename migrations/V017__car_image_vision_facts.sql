-- V017__car_image_vision_facts.sql
--
-- Citation-first facts read off a car's OWN gallery photos by a vision model --
-- the local-LLM counterpart to brochure_vision_facts (V015), same discipline,
-- scoped to car photography instead of brochure PDFs.
--
-- Candidates come from car_image_text (V006): a cheap deterministic OCR pass
-- already classifies each gallery image and records sticker_image_urls in its
-- JSONB summary. This table exists so an expensive vision-model READ of exactly
-- those flagged images can run locally, unattended, without the cloud
-- Workflow's rate limits -- mirroring why local_brochure_vision_extract.py
-- exists instead of running everything through cloud agents.
--
-- Deliberately NOT written into car_image_text (the table generated_spec_sheet
-- reads for "this car's OWN photo says X"): that table is a single row per car,
-- last-writer-wins, and its only vision-model writer today is the reviewed
-- cloud Sonnet agent pass (car_image_text.version = 100). A local, unreviewed
-- pass has no business overwriting that claim. Instead this table holds
-- per-image candidate facts with review_verdict IS NULL until an independent
-- pass promotes them to 'confirmed' -- only then does
-- feed_package_registry_from_car_image_vision.py fold them into the shared
-- package_values registry as a 'sticker_photo' observation, trim-level, never
-- claimed as a fact about the specific VIN it was read from.

CREATE TABLE IF NOT EXISTS car_image_vision_facts (
    id                  BIGSERIAL PRIMARY KEY,
    car_id              INTEGER NOT NULL,
    vin                 TEXT,
    year                INTEGER,
    make                TEXT,
    model               TEXT,
    trim                TEXT,

    fact_type           TEXT NOT NULL,   -- spec|option|package|package_feature|trim_add
    name                TEXT NOT NULL,
    value_text          TEXT,
    price               NUMERIC,

    image_url           TEXT NOT NULL,
    quoted_text          TEXT,

    extracted_by        TEXT NOT NULL,
    extracted_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    review_verdict       TEXT,           -- NULL | 'confirmed' | 'rejected'
    review_notes          TEXT,
    reviewed_by           TEXT,
    reviewed_at            TIMESTAMPTZ,

    UNIQUE (car_id, image_url, fact_type, name)
);

CREATE INDEX IF NOT EXISTS idx_car_image_vision_facts_car
    ON car_image_vision_facts (car_id);

CREATE INDEX IF NOT EXISTS idx_car_image_vision_facts_trim
    ON car_image_vision_facts (year, make, model, trim);

CREATE INDEX IF NOT EXISTS idx_car_image_vision_facts_confirmed
    ON car_image_vision_facts (review_verdict)
    WHERE review_verdict = 'confirmed';

-- Failure-handoff queue, same shape and purpose as
-- brochure_local_extraction_failures (V016): what the local lane could not
-- read, so a future cloud Sonnet pass can be pointed at exactly those images.
CREATE TABLE IF NOT EXISTS car_image_vision_extraction_failures (
    id                  BIGSERIAL PRIMARY KEY,
    car_id              INTEGER NOT NULL,
    image_url           TEXT NOT NULL,
    stage               TEXT NOT NULL,   -- fetch|extract_request|extract_parse
    reason              TEXT,
    attempts            INTEGER NOT NULL DEFAULT 1,
    first_failed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_failed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    handed_to_cloud_at  TIMESTAMPTZ,

    UNIQUE (car_id, image_url, stage)
);
