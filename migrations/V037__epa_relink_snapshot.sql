-- V037__epa_relink_snapshot.sql
--
-- N23 P10B.6 (the plan registry assigns no number to N23; V036 was N22's, so the next free
-- number is V037).
--
-- Undo ledger of backend/scripts/reresolve_legacy_catalog_links.py. A run re-resolves the
-- cars linked to legacy catalog rows with those rows excluded, then archives the legacy rows
-- no car links to. Everything it changes is recorded here, per run, so --undo <run_id> can
-- put each car back on its old link and each archived row back under its original id:
--
--   * epa_relink_snapshot: one row per car whose link the run changed (old and new link with
--     confidence and method; old_* restore the car exactly, method is the new match method).
--   * epa_relink_archived: one row per catalog row the run moved to epa_master_archive
--     (superseded_by is its content twin, or NULL; reason is the
--     epa_master_archive.archive_reason it was archived under).
--
-- undone_at is stamped by --undo; a stamped row is never restored twice.
--
--   SELECT run_id, count(*), count(new_id) FROM epa_relink_snapshot GROUP BY 1;
--
-- Idempotent. New tables only (no lock on cars); no schema mirror, as for V035.

SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS public.epa_relink_snapshot (
    run_id text NOT NULL,
    car_id bigint NOT NULL,
    old_id bigint,
    new_id bigint,
    method text,
    old_confidence double precision,
    old_method text,
    new_confidence double precision,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    undone_at timestamp with time zone,
    PRIMARY KEY (run_id, car_id)
);

CREATE TABLE IF NOT EXISTS public.epa_relink_archived (
    run_id text NOT NULL,
    epa_master_id bigint NOT NULL,
    superseded_by bigint,
    reason text,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    undone_at timestamp with time zone,
    PRIMARY KEY (run_id, epa_master_id)
);
