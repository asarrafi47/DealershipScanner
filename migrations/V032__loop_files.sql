-- V032__loop_files.sql
--
-- Scan loop file store (docs/scan_loop/README.md, section "Where the data lives").
--
-- loop_files: a small virtual file tree in the Railway database. The scanner publishes dealer logs, run bundles and
--   the recipe mirror into it; cloud sessions read it and submit requests and proposals through the token-guarded
--   /api/loop endpoints. path is the repo-style relative path (workspace/dealer_logs/<id>/summary.md, runs/<id>/...,
--   requests/rq-....json). The application enforces path rules, per-file and total size caps and retention
--   (backend/scanner/loop/store.py); the check below only keeps the table sane if something bypasses it.
--
-- Additive only: one new table, nothing reads it until LOOP_STORE=db is set on the scanner and the web service.
-- Re-running this file is harmless (IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS public.loop_files (
    path        TEXT        PRIMARY KEY,
    body        BYTEA       NOT NULL,
    sha256      TEXT        NOT NULL,
    size        INTEGER     NOT NULL,
    writer      TEXT        NOT NULL DEFAULT 'railway',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT loop_files_path_check CHECK (length(path) BETWEEN 1 AND 240),
    CONSTRAINT loop_files_size_check CHECK (size >= 0 AND size <= 4194304)
);
