-- V013__users.sql
--
-- The users domain, translated from users.db (SQLCipher SQLite) to Postgres.
--
-- Why this exists. App users live in a SQLCipher-encrypted SQLite file on the web host
-- (backend/db/users_sqlite.py). That file is the last piece of state pinning the deploy to a
-- particular machine: Railway containers get a fresh filesystem on every deploy, so users.db
-- either rides a mounted volume that only one container can hold, or gets lost. Everything
-- else already moved to inventory Postgres; this migration gives the users domain a home
-- there so a Railway deploy needs no host-bound database file at all.
--
-- What this is NOT. Creating these tables changes nothing at runtime. Every module that
-- serves logins (backend/db/users_db/*, backend/db/user_history_db.py,
-- backend/db/search_analytics_db.py) still reads users.db through get_users_conn(). The
-- cutover -- porting those modules to a dual-mode connection and running the one-shot copier
-- (backend/scripts/migrate_users_sqlite_to_postgres.py) -- is a separate, later step. Until
-- then these tables sit empty and harmless.
--
-- IMPORTANT -- SEC-088 must be re-answered before cutover. SQLCipher encrypts users.db at
-- rest: password hashes, emails, Stripe customer ids, and above all plaintext TOTP seeds
-- (users.totp_secret). Postgres on a managed host does not replicate that property by
-- itself -- pg_dump output, logical replicas, and anyone with the DSN see cleartext. Before
-- any row is copied, decide and record: managed-disk encryption + DSN hygiene is enough, or
-- totp_secret (and mfa_phone) get application-level encryption, or the users domain gets its
-- own database/role. Do not let the cutover silently downgrade the at-rest story.
--
-- Faithfulness notes (the sqlite source of truth is backend/db/users_db/schema.py plus the
-- bootstrap DDL in user_history_db.py and search_analytics_db.py):
--
--   * INTEGER PRIMARY KEY AUTOINCREMENT -> BIGINT GENERATED ALWAYS AS IDENTITY. The copier
--     preserves existing ids with OVERRIDING SYSTEM VALUE and then resets the sequences.
--   * SQLite 0/1 flag columns (totp_enabled, is_premium, is_active) stay SMALLINT, not
--     BOOLEAN: every call site writes literal 0/1 and reads through bool(). BOOLEAN would
--     reject those writes ("column is of type boolean but expression is of type integer")
--     and force a wider port than the cutover needs.
--   * TEXT datetime('now') columns become TIMESTAMPTZ. sqlite's datetime('now') is naive
--     UTC text; the copier converts it explicitly. Read sites that treat these as strings
--     (admin.py checks email_verified_at with .strip()) must adapt at cutover.
--   * password_reset_expires_at stays BIGINT: it stores epoch seconds and is compared
--     against int(time.time()) in auth.py, never parsed as a datetime.
--   * orgs.stripe_current_period_end stays TEXT: it is a pass-through of whatever ISO
--     string Stripe sent, written and read as an opaque string.
--   * The case-insensitive unique indexes (idx_users_email_ci / idx_users_username_ci)
--     carry over as unique expression indexes on lower(). schema.py tolerates legacy dupes
--     by warning; here the index is unconditional, so case-duplicate accounts in the old
--     file must be merged BEFORE the copy or the copier fails loudly (which is correct).
--   * org_invites' foreign keys exist in the sqlite DDL but were never enforced (PRAGMA
--     foreign_keys defaults off). Here they are real, with ON DELETE CASCADE chosen to
--     match what the code already does by hand (delete_user_by_email deletes a user's
--     invites first; empty orgs are removed after their last user).
--   * users.org_id and the user_id/car_id columns on the history/search tables get NO
--     foreign keys, on purpose: sqlite never enforced them, existing data may dangle, and
--     user deletion intentionally leaves history rows behind.

-- ---------------------------------------------------------------------------
-- Organizations (dealer groups buying seats). Created before users only for
-- readability; nothing in users references it by constraint.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS orgs (
    id                          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name                        TEXT NOT NULL UNIQUE,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    stripe_customer_id          TEXT,
    stripe_subscription_id      TEXT,
    stripe_subscription_status  TEXT,
    -- ISO string pass-through from Stripe webhooks; read back verbatim for the UI.
    stripe_current_period_end   TEXT
);

-- ---------------------------------------------------------------------------
-- App users. Column set mirrors schema.py's CREATE TABLE + its ALTER ladder,
-- flattened: a fresh Postgres database has no legacy rows to migrate around.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS users (
    id                              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    username                        TEXT NOT NULL UNIQUE,
    email                           TEXT NOT NULL UNIQUE,
    password                        TEXT NOT NULL,   -- bcrypt/argon hash, never plaintext
    role                            TEXT,            -- normalized to 'dealer_staff' at read time when NULL
    dealer_id                       TEXT,
    dealership_registry_id          BIGINT,
    org_id                          BIGINT,          -- no FK: sqlite never enforced it, legacy rows may dangle
    totp_secret                     TEXT,            -- SEC-088: plaintext TOTP seed -- see header
    totp_enabled                    SMALLINT NOT NULL DEFAULT 0,
    mfa_method                      TEXT,
    mfa_phone                       TEXT,            -- legacy (SMS MFA removed)
    is_premium                      SMALLINT NOT NULL DEFAULT 0,
    premium_stripe_customer_id      TEXT,
    premium_stripe_session_id       TEXT,
    premium_stripe_subscription_id  TEXT,
    google_sub                      TEXT,
    apple_sub                       TEXT,
    subscription_plan_id            TEXT,
    email_verified_at               TIMESTAMPTZ,     -- sqlite TEXT datetime('now'); NULL = unverified
    email_verify_token_hash         TEXT,
    password_reset_token_hash       TEXT,
    password_reset_expires_at       BIGINT,          -- epoch seconds, compared to time.time()
    is_active                       SMALLINT NOT NULL DEFAULT 1
);

-- One account per OAuth subject; empty string means "not linked" in legacy rows,
-- so the partial predicate mirrors the sqlite index exactly.
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_google_sub
    ON users (google_sub) WHERE google_sub IS NOT NULL AND google_sub != '';

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_apple_sub
    ON users (apple_sub) WHERE apple_sub IS NOT NULL AND apple_sub != '';

-- Login matches case-insensitively while the column UNIQUEs are case-sensitive;
-- without these, Foo@x.com and foo@x.com are two accounts that both answer to
-- the same login. Case-duplicate legacy rows must be merged before the copy.
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email_ci
    ON users (lower(email));

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_ci
    ON users (lower(username));

-- ---------------------------------------------------------------------------
-- Org invites. The only table whose sqlite DDL declared foreign keys; enforced
-- here for real (CASCADE matches the manual cleanup the code already performs).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS org_invites (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id           BIGINT NOT NULL REFERENCES orgs (id) ON DELETE CASCADE,
    token_hash       TEXT NOT NULL UNIQUE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    used_at          TIMESTAMPTZ,
    used_by_user_id  BIGINT REFERENCES users (id) ON DELETE CASCADE
);

-- ---------------------------------------------------------------------------
-- Per-user engagement history (backend/db/user_history_db.py). Lives in the
-- same file as users today, so it moves with them. user_id/car_id carry no
-- FKs: deleting a user keeps their (anonymous-by-then) history rows, exactly
-- as the sqlite behavior is now, and car_id points into the cars table which
-- is rewritten wholesale by scans.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS car_view_history (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id    BIGINT NOT NULL,
    car_id     BIGINT NOT NULL,
    viewed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, car_id)   -- upsert target: ON CONFLICT (user_id, car_id)
);

CREATE INDEX IF NOT EXISTS idx_cvh_user_time
    ON car_view_history (user_id, viewed_at DESC);

CREATE TABLE IF NOT EXISTS car_compare_history (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id      BIGINT NOT NULL,
    car_id       BIGINT NOT NULL,
    compared_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, car_id)
);

CREATE INDEX IF NOT EXISTS idx_cch_user_time
    ON car_compare_history (user_id, compared_at DESC);

CREATE TABLE IF NOT EXISTS compare_sessions (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id      BIGINT NOT NULL,
    car_ids      TEXT NOT NULL,   -- comma-joined car ids, written by record_compare_session
    compared_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_compare_sessions_user_time
    ON compare_sessions (user_id, compared_at DESC);

-- ---------------------------------------------------------------------------
-- Search analytics (backend/db/search_analytics_db.py). Also stored in
-- users.db today. filters_json stays TEXT to keep the copy byte-faithful;
-- the cutover port must swap sqlite json_extract() for filters_json::jsonb
-- lookups (a read-time cast, no reload needed -- JSONB conversion can be a
-- later migration if the dashboard queries want an index).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS search_events (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    occurred_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    user_id       BIGINT,
    session_key   TEXT,
    source        TEXT NOT NULL,
    query_text    TEXT,
    filters_json  TEXT NOT NULL DEFAULT '{}',
    result_count  INTEGER NOT NULL DEFAULT 0,
    geo_zip       TEXT,
    geo_state     TEXT
);

CREATE INDEX IF NOT EXISTS idx_search_events_time
    ON search_events (occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_search_events_user
    ON search_events (user_id, occurred_at DESC);
