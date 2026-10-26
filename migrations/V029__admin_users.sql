-- V029__admin_users.sql
--
-- N37.9 (D-PG9): /dev operator accounts move from the SQLCipher dev_users.db to
-- Postgres. Shape follows backend/db/admin_users_db.py; the dead mfa_method /
-- mfa_phone columns are not carried. Case-insensitive uniqueness matches the
-- login lookup (lower(username) OR lower(email)). The table lands in public;
-- the app_users schema move rides with N37.10. Nothing reads this table until
-- DEV_USERS_DB_BACKEND=pg (default sqlite), so the migration is inert on deploy.

CREATE TABLE IF NOT EXISTS public.admin_users (
    id           BIGSERIAL PRIMARY KEY,
    username     TEXT NOT NULL,
    email        TEXT NOT NULL,
    password     TEXT NOT NULL,
    totp_enabled INTEGER NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS admin_users_username_lower_key ON public.admin_users (lower(username));
CREATE UNIQUE INDEX IF NOT EXISTS admin_users_email_lower_key ON public.admin_users (lower(email));
