-- V035__users_auth_epoch.sql
--
-- P16.1 (N30): session revocation. session["ae"] is compared with this counter on
-- every request; it is bumped only by a password change/reset and the bootstrap
-- force-sync, never by the hash rehash. Runtime copy (SQLite):
-- backend/db/users_db/schema.py::init_users_db. Provisional number, renumber at merge.

ALTER TABLE public.users ADD COLUMN IF NOT EXISTS auth_epoch integer NOT NULL DEFAULT 0;
