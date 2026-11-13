-- V030__chat_usage.sql
--
-- Chat limits (docs/auth_sms/CHAT_LIMITS.md section 4, card C1).
--
-- chat_usage_buckets: one row per (period, bucket, subject). 'hour' rows start at the clock
--   hour; 'day' rows start at midnight in the reset time zone (setting chat.reset_tz,
--   default America/New_York). subject_key is a user id, a keyed hash (phone, device, ip),
--   'uid:car' for a listing, or 'all' for the whole site. No message text and no model
--   reply is ever stored. user ids have no foreign key (users still live in SQLite).
-- app_settings: small editable key/value settings (chat numbers now, other features later).
--
-- Additive only: two new tables, nothing reads them until the chat gate is wired (card C3),
-- and the gate falls back to environment and code defaults while app_settings is empty or
-- missing. Re-running this file is harmless (IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS public.chat_usage_buckets (
    period        TEXT        NOT NULL,
    bucket_start  TIMESTAMPTZ NOT NULL,
    subject_kind  TEXT        NOT NULL,
    subject_key   TEXT        NOT NULL,
    messages      INTEGER     NOT NULL DEFAULT 0,
    tokens_in     BIGINT      NOT NULL DEFAULT 0,
    tokens_out    BIGINT      NOT NULL DEFAULT 0,
    cost_micro    BIGINT      NOT NULL DEFAULT 0,
    denied        INTEGER     NOT NULL DEFAULT 0,
    PRIMARY KEY (period, bucket_start, subject_kind, subject_key),
    CONSTRAINT chat_usage_buckets_period_check CHECK (period IN ('hour', 'day')),
    CONSTRAINT chat_usage_buckets_kind_check
        CHECK (subject_kind IN ('user', 'phone', 'device', 'ip', 'listing', 'global'))
);

-- the nightly cleanup deletes by age; the admin totals read one bucket at a time
CREATE INDEX IF NOT EXISTS chat_usage_buckets_bucket_start_idx
    ON public.chat_usage_buckets (bucket_start);

CREATE TABLE IF NOT EXISTS public.app_settings (
    key         TEXT        PRIMARY KEY,
    value       TEXT        NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by  BIGINT
);
