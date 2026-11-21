-- V028__rooftop_evidence_tier.sql
--
-- N8.6a (CF-4, WI-6): persist the rooftop attribution tier and a short evidence
-- record per car. backend/attribution/rooftop.py computes both for every row
-- of a group feed and discarded them, so the VIN ownership guard
-- (backend/scanner/upsert/guard.py, and the sql.py ON CONFLICT backstop) could
-- only compare dealer ids and scrape age, never how well each store's claim to
-- the VIN was evidenced. N8.6b reads these columns.
--
--   rooftop_tier      'strong' | 'weak' | 'none'. strong = the store's own feed
--                     stamped the car (rooftop name or site-id match); weak =
--                     matched only on a locality or partial-name tier; none = no
--                     per-car evidence (single-store feeds, unstamped group
--                     feeds). Every row that exists today is 'none': the tier
--                     was never stored, so legacy rows carry no evidence.
--   rooftop_evidence  short JSON text: the tier name that decided it, the
--                     stamped rooftop, and the feed source. NULL when none.
--
-- Additive and metadata-only on PostgreSQL 11+: a constant DEFAULT does not
-- rewrite `cars`. The runner's lock_timeout bounds the ACCESS EXCLUSIVE wait.
-- Runtime copy for SQLite: backend/db/repositories/schema_repo.py.

ALTER TABLE public.cars ADD COLUMN IF NOT EXISTS rooftop_tier text NOT NULL DEFAULT 'none';
ALTER TABLE public.cars ADD COLUMN IF NOT EXISTS rooftop_evidence text;
