-- V009__rooftop_refusals.sql
--
-- Keep the rooftop gate's refusals instead of logging them into the void.
--
-- backend/parsers/__init__.py already does the hard part: when a group feed serves several
-- rooftops and none of them is the store being scanned, it refuses the rows rather than
-- mis-attributing them. That is the right call and it fires roughly 1,106 times per scan.
--
-- But the decision leaves no trace. The reason is written onto an in-memory dict
-- (`row["_rooftop_reject"]`), the rows are dropped, and the only record is a WARNING line
-- in a log that nobody reads and that drowns every other warning in the run.
--
-- This is the same failure the vision-agent work kept running into, in a different
-- subsystem: the reader observes correctly, and the structure around it discards the
-- answer. There the fix was a typed column; here it is this table.
--
-- What it buys, beyond quieter logs:
--
--   * The refusals ARE the group-feed census. A dealer whose scans repeatedly refuse
--     hundreds of rows naming other rooftops is, by definition, being served someone
--     else's inventory -- which is the open misattribution investigation, answered as a
--     by-product of scanning rather than by reading photographs.
--   * `rooftops_seen` names the stores the feed actually claimed, so a rooftop we do not
--     have registered (Audi Huntsville, BMW of Silver Spring) shows up as a gap.
--   * A refusal count that suddenly drops to zero means the gate stopped firing -- either
--     the feed was fixed, or the gate broke. Without history, neither is visible.
--
-- Deliberately append-only and cheap: one row per (dealer, reason, scan), never read on
-- the serving path, so a failed insert must never interrupt a scan.

CREATE TABLE IF NOT EXISTS rooftop_refusals (
    id             BIGSERIAL PRIMARY KEY,
    dealer_id      TEXT NOT NULL,

    -- Matches the values the gate already assigns to row["_rooftop_reject"], e.g.
    -- 'single_rooftop_is_not_this_store', 'group_feed_no_matching_rooftop'.
    reason         TEXT NOT NULL,

    rows_refused   INTEGER NOT NULL DEFAULT 0,

    -- The rooftop names the feed carried. This is the part worth keeping: it says WHOSE
    -- inventory arrived, which is exactly what the misattribution work needs.
    rooftops_seen  JSONB,
    rooftop_count  INTEGER,

    scanned_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- "Which dealers are being served other people's inventory, worst first" is the query
-- this exists to answer.
CREATE INDEX IF NOT EXISTS idx_rooftop_refusals_dealer
    ON rooftop_refusals (dealer_id, scanned_at DESC);

CREATE INDEX IF NOT EXISTS idx_rooftop_refusals_seen
    ON rooftop_refusals USING GIN (rooftops_seen);
