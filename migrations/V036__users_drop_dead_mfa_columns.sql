-- V036__users_drop_dead_mfa_columns.sql
--
-- P16.2m (N30), D-SEC2 (a): TOTP and SMS MFA were removed from the app; the columns were
-- never read or written by any live path. SEC-088 (V013 header: plaintext TOTP seed on a
-- host without encryption at rest) is answered by deleting the storage, not encrypting it.
-- users.totp_enabled stays (always 0, still in INSERTs and the admin listing).
--
-- Non-raising on purpose: the prod users row count was not known when this was drafted. Run
-- backend/scripts/preflight_drop_dead_mfa.py against prod first; it reports how many rows
-- hold a non-empty seed or phone. Dropping them is the point, but the owner may want the
-- optional scrub/backup (P16.3) first. Provisional number, renumber at merge.

ALTER TABLE public.users DROP COLUMN IF EXISTS totp_secret;
ALTER TABLE public.users DROP COLUMN IF EXISTS mfa_phone;
ALTER TABLE public.users DROP COLUMN IF EXISTS mfa_method;
