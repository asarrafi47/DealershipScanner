-- V004__comment_flag_dedupe.sql
--
-- One reporter counts at most once per comment.
--
-- V002 gave car_comments/dealer_comments a bare `flag_count` that
-- backend/db/comments_db.py incremented unconditionally, and
-- backend/routes/community_api.py exposes the increment at
-- POST /api/{cars,dealerships}/<id>/comments/<comment_id>/flag with no login
-- requirement. Because comments_db.FLAG_HIDE_THRESHOLD auto-sets is_hidden at 3
-- flags, and list_comments filters `is_hidden = FALSE`, one visitor could send
-- the same request three times and remove any comment on the site from every
-- reader. The per-IP limiter in front of it caps 20 requests a minute, which is
-- six times more than a takeover needs.
--
-- The sibling surface already solved this: backend/reviews/store.py records each
-- reporter in `review_reports` with UNIQUE (review_id, ip_hash) and treats a
-- repeat report as a no-op. This table is that arrangement for comments, keyed
-- on scope as well because car and dealer comment ids are independent sequences.
--
-- ip_hash is the salted digest from backend/reviews/guards.hash_ip, never a raw
-- address. A NULL-hash caller (no resolvable IP) is not recorded here and does
-- not bump the count -- the alternative would let an unidentifiable client back
-- into the unlimited path this migration exists to close.
--
-- Additive and idempotent.

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_min_messages = warning;

CREATE TABLE IF NOT EXISTS comment_flags (
    scope TEXT NOT NULL,
    comment_id BIGINT NOT NULL,
    ip_hash TEXT NOT NULL,
    created_at TEXT,
    UNIQUE (scope, comment_id, ip_hash)
);

CREATE INDEX IF NOT EXISTS idx_comment_flags_comment ON comment_flags (scope, comment_id);
