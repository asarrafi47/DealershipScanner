-- V014__option_rejections.sql
--
-- Every option price we refused, and why.
--
-- Three separate gates drop option prices, and until now two of them destroyed the
-- evidence:
--
--   * the price-book feed refuses ~1,372 of 2,404 captured prices for lack of cross-car
--     corroboration. Recoverable, because the raw reading survives in car_image_text.
--   * ``scrub-skew`` REWRITES car_image_text.summary in place. The prior value is gone.
--     It has twice been shown to delete correct data -- "M Sport Package" $2,550 and
--     "Rear Climate Control Console" $900 were both plainly printed on the photographs and
--     both discarded as corpus outliers.
--   * record-time guards (mandatory fee, never-priced, arithmetic) log a warning to stdout
--     and drop the line. Nothing persists.
--
-- That asymmetry is backwards. A price we ADMIT is visible and checkable; a price we
-- REFUSE vanishes, so a guard tuned too tight is invisible by construction -- which is
-- exactly how the two false strips above went unnoticed until an auditor diffed an agent's
-- submitted JSON against the database by hand.
--
-- So: write the refusals down. This table is append-only, never read on a serving path,
-- and exists to answer three questions:
--
--   1. Is a guard over-firing?  (group by reason, look at what it is killing)
--   2. What would we gain by loosening one?  (the refused rows ARE the upside)
--   3. Can a price be re-admitted later?  Corroboration grows with the corpus: a price
--      refused today for having no second sighting may earn one next week, and this table
--      is what makes that re-admission possible without re-reading the photograph.

CREATE TABLE IF NOT EXISTS option_rejections (
    id            BIGSERIAL PRIMARY KEY,
    car_id        BIGINT,
    vin           TEXT,
    year          INTEGER,
    make          TEXT,
    model         TEXT,
    trim          TEXT,

    option_name   TEXT NOT NULL,
    price         NUMERIC,

    -- Which gate refused it: 'uncorroborated', 'corpus_outlier', 'usually_included',
    -- 'mandatory_fee', 'never_priced', 'arithmetic'.
    reason        TEXT NOT NULL,
    detail        TEXT,

    rejected_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_option_rejections_reason
    ON option_rejections (reason, rejected_at DESC);

-- "Has this price earned corroboration since we refused it?" is the re-admission query.
CREATE INDEX IF NOT EXISTS idx_option_rejections_lookup
    ON option_rejections (make, model, year, option_name);
