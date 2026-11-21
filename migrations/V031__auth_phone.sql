-- V031__auth_phone.sql
--
-- Phone sign-in tables (docs/auth_sms/DESIGN.md section 4.2, card S5 of docs/auth_sms/TASKS.md).
--
-- auth_phone_identities: one verified number per account and one account per number. The number
--   is stored in clear on purpose (DESIGN.md section 16); user_id is NULL while state = 'reserving'
--   (account creation order, card S6) and after a dormant number is detached.
-- auth_phone_challenges: one row per code request. No code is ever stored (the provider holds it);
--   phone_e164 is cleared when the challenge ends.
-- auth_phone_sends: one row per text and per refused send; every limit, pool and the spend cap
--   are counted from these rows (never from vendor callbacks). Hashes only, no number.
-- auth_phone_wrong_tries: the wrong-code counter a new code does NOT reset.
-- auth_phone_lookup_cache: line-type results by phone hash, kept 30 days (also for refused numbers).
-- auth_session_epochs: "sessions issued before valid_after are void" per user (card S10).
--
-- user ids have no foreign key while users live in SQLite (same rule as V018 and V021).
-- Additive only: six new tables, nothing reads them while AUTH_PHONE_LOGIN_ENABLED is off.
-- Re-running this file is harmless (IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS public.auth_phone_identities (
    id                    BIGSERIAL   PRIMARY KEY,
    user_id               BIGINT,
    phone_e164            TEXT        NOT NULL,
    phone_hash            TEXT        NOT NULL,
    country               TEXT        NOT NULL,
    line_type             TEXT,
    verified_at           TIMESTAMPTZ NOT NULL,
    last_login_at         TIMESTAMPTZ,
    state                 TEXT        NOT NULL DEFAULT 'active',
    dormant_user_id       BIGINT,
    detached_at           TIMESTAMPTZ,
    consent_at            TIMESTAMPTZ NOT NULL,
    consent_text_version  TEXT        NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT auth_phone_e164_shape CHECK (phone_e164 ~ '^\+[1-9][0-9]{6,14}$'),
    CONSTRAINT auth_phone_identities_state_check CHECK (state IN ('reserving', 'active')),
    CONSTRAINT auth_phone_identities_active_has_user CHECK (state <> 'active' OR user_id IS NOT NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS auth_phone_identities_phone_key
    ON public.auth_phone_identities (phone_e164);
CREATE UNIQUE INDEX IF NOT EXISTS auth_phone_identities_user_key
    ON public.auth_phone_identities (user_id);

CREATE TABLE IF NOT EXISTS public.auth_phone_challenges (
    id             BIGSERIAL   PRIMARY KEY,
    public_id      TEXT        NOT NULL UNIQUE,
    phone_hash     TEXT        NOT NULL,
    phone_e164     TEXT,
    purpose        TEXT        NOT NULL,
    user_id        BIGINT,
    session_nonce  TEXT        NOT NULL,
    provider       TEXT        NOT NULL,
    provider_ref   TEXT,
    attempts       INTEGER     NOT NULL DEFAULT 0,
    sends          INTEGER     NOT NULL DEFAULT 1,
    status         TEXT        NOT NULL DEFAULT 'open',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at     TIMESTAMPTZ NOT NULL,
    last_send_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at    TIMESTAMPTZ,
    CONSTRAINT auth_phone_challenges_e164_shape CHECK (phone_e164 IS NULL OR phone_e164 ~ '^\+[1-9][0-9]{6,14}$'),
    CONSTRAINT auth_phone_challenges_purpose_check
        CHECK (purpose IN ('login', 'link', 'claim', 'step_up', 'change', 'seller_verify')),
    CONSTRAINT auth_phone_challenges_status_check
        CHECK (status IN ('open', 'approved', 'failed', 'expired', 'cancelled')),
    CONSTRAINT auth_phone_challenges_ended_has_no_number CHECK (status = 'open' OR phone_e164 IS NULL)
);

-- retention deletes by end time; the open-challenge sweep reads by expiry
CREATE INDEX IF NOT EXISTS auth_phone_challenges_finished_at_idx ON public.auth_phone_challenges (finished_at);
CREATE INDEX IF NOT EXISTS auth_phone_challenges_open_expires_idx
    ON public.auth_phone_challenges (expires_at) WHERE status = 'open';

CREATE TABLE IF NOT EXISTS public.auth_phone_sends (
    id                 BIGSERIAL    PRIMARY KEY,
    at                 TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    phone_hash         TEXT         NOT NULL,
    ip_hash            TEXT         NOT NULL,
    device_hash        TEXT,
    country            TEXT         NOT NULL,
    est_cost_cents     NUMERIC(8,3) NOT NULL DEFAULT 0,
    lookup_cost_cents  NUMERIC(8,3) NOT NULL DEFAULT 0,
    purpose            TEXT         NOT NULL,
    pool               TEXT         NOT NULL,
    ip_prefix_hash     TEXT         NOT NULL,
    has_device_cookie  BOOLEAN      NOT NULL,
    channel            TEXT         NOT NULL DEFAULT 'sms',
    outcome            TEXT         NOT NULL,
    CONSTRAINT auth_phone_sends_purpose_check
        CHECK (purpose IN ('login', 'link', 'claim', 'step_up', 'change', 'seller_verify')),
    CONSTRAINT auth_phone_sends_pool_check CHECK (pool IN ('known', 'new', 'resend', 'seller')),
    CONSTRAINT auth_phone_sends_channel_check CHECK (channel IN ('sms', 'call')),
    CONSTRAINT auth_phone_sends_outcome_check CHECK (outcome IN (
        'pending', 'sent', 'unavailable', 'provider_error', 'blocked_turnstile', 'blocked_voip',
        'blocked_country', 'blocked_limit', 'blocked_cap')),
    CONSTRAINT auth_phone_sends_costs_not_negative CHECK (est_cost_cents >= 0 AND lookup_cost_cents >= 0)
);

CREATE INDEX IF NOT EXISTS auth_phone_sends_at_idx ON public.auth_phone_sends (at);
CREATE INDEX IF NOT EXISTS auth_phone_sends_phone_at_idx ON public.auth_phone_sends (phone_hash, at);
CREATE INDEX IF NOT EXISTS auth_phone_sends_ip_at_idx ON public.auth_phone_sends (ip_hash, at);
CREATE INDEX IF NOT EXISTS auth_phone_sends_device_at_idx ON public.auth_phone_sends (device_hash, at);
CREATE INDEX IF NOT EXISTS auth_phone_sends_ip_prefix_at_idx ON public.auth_phone_sends (ip_prefix_hash, at);
CREATE INDEX IF NOT EXISTS auth_phone_sends_pool_at_idx ON public.auth_phone_sends (pool, at);

CREATE TABLE IF NOT EXISTS public.auth_phone_wrong_tries (
    phone_hash    TEXT        PRIMARY KEY,
    tries         INTEGER     NOT NULL DEFAULT 0,
    window_start  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_until  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS public.auth_phone_lookup_cache (
    phone_hash  TEXT        PRIMARY KEY,
    ok          BOOLEAN     NOT NULL,
    line_type   TEXT,
    country     TEXT,
    reason      TEXT        NOT NULL,
    checked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS auth_phone_lookup_cache_expires_idx ON public.auth_phone_lookup_cache (expires_at);

CREATE TABLE IF NOT EXISTS public.auth_session_epochs (
    user_id      BIGINT      PRIMARY KEY,
    valid_after  TIMESTAMPTZ NOT NULL,
    reason       TEXT        NOT NULL,
    set_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
