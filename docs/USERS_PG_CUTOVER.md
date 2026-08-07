# users.db → Postgres cutover plan

Status: **preparation only** — V013__users.sql and the copier script exist; nothing at runtime
has changed. App users are still served from users.db via `backend.db.users_sqlite.get_users_conn()`.

## What is in scope

Seven tables live in users.db and move together (all reached through `get_users_conn()`):

| Table | Created by |
|---|---|
| users, orgs, org_invites | `backend/db/users_db/schema.py` (`init_users_db`) |
| car_view_history, car_compare_history, compare_sessions | `backend/db/user_history_db.py` (`_bootstrap`, runs at import) |
| search_events | `backend/db/search_analytics_db.py` (`_bootstrap`, runs at import) |

Out of scope: `dev_users.db` (`dev_users_sqlite` / `admin_users_db` — operator accounts, separate
file, separate decision), `saved_cars` (already in inventory Postgres), `incomplete_listings`
(already dual-mode).

## Where sqlite3 is touched directly

Every function below opens the users DB; the port must route each through the new dual-mode
connection. sqlite-isms that will not survive verbatim are noted.

- **`users_db/_common.py`** — `get_conn()` (the single funnel; the cutover switch lives here);
  `_users_select_columns()` (PRAGMA table_info — PRAGMA is a no-op under the pg compat layer,
  needs an `information_schema` shim; also `lru_cache`d, so backend switches in tests must clear it);
  `_schedule_password_rehash()` (background thread, catches `sqlite3.OperationalError` "locked" —
  meaningless on PG, harmless).
- **`users_db/schema.py`** — `init_users_db` (CREATE TABLE + ALTER ladder + `INSERT OR IGNORE`;
  on PG the DDL is V013's job, so init should become a near no-op that only runs the seeding/
  promotion logic), `_env_admin_set_clause` (PRAGMA), `_apply_env_admin_privileges`,
  `sync_env_admin_user_row`. **This file is being edited concurrently — see ordering below.**
- **`users_db/accounts.py`** — all org/user CRUD. sqlite-isms: `cursor.lastrowid` in
  `create_org`, `save_user`, `save_oauth_user`, `save_apple_oauth_user` (PG needs
  `... RETURNING id`); `conn.row_factory = sqlite3.Row` in `get_org`; PRAGMA column probing
  throughout; "database is locked" retry loops (dead code on PG).
- **`users_db/auth.py`** — login/token/TOTP paths. sqlite-isms: `datetime('now')` in
  `mark_user_email_verified` (→ `NOW()`); PRAGMA probing; `?` placeholders everywhere.
- **`users_db/billing.py`**, **`users_db/admin.py`** — PRAGMA probing + `?` placeholders only.
- **`user_history_db.py`** — upserts already use `ON CONFLICT (user_id, car_id) DO UPDATE`
  (valid PG); `datetime('now')` inside VALUES needs rewriting.
- **`search_analytics_db.py`** — `json_extract(filters_json, '$.make')` →
  `filters_json::jsonb ->> 'make'`; string-vs-timestamp comparisons (`occurred_at >= ?` with a
  `%Y-%m-%d %H:%M:%S` string) work on TIMESTAMPTZ only if the session/param is UTC-aware.
- Scripts/tests: `backend/scripts/app_users_status.py`, `backend/scripts/reset_app_user_password.py`,
  `backend/db/admin_users_db.py` (reads the legacy path for a copy check), ~30 test files that
  set `USERS_DB_PATH` and will need a PG-or-sqlite fixture story.

## The dual-mode port (pattern: `backend/db/incomplete_listings_db.py`)

`incomplete_listings_db.get_conn()` returns the inventory-PG compat connection when
`inventory_pg.is_inventory_postgres()`, else sidecar sqlite — call sites unchanged because the
compat wrapper (`inventory_db.get_conn` → `adapt_sql_for_postgres_execute`) rewrites `?`→`%s`,
`IFNULL`, `datetime('now')`, `INSERT OR IGNORE`, and skips PRAGMA.

Proposed shape, gated on a **dedicated env flag** (`USERS_DB_BACKEND=postgres` or similar),
NOT on `is_inventory_postgres()` alone — inventory is already Postgres in prod, and keying on
that would flip the users backend the moment the code deploys, before the copy has run:

1. `_common.get_conn()` returns the compat-wrapped PG connection when the flag is set.
2. Replace every `PRAGMA table_info(users)` probe with a `users_table_columns()` helper in
   `_common` that uses PRAGMA on sqlite and `information_schema.columns` on PG (reuse
   `inventory_pg.pg_table_columns`). On PG it can be a constant — V013 has all columns.
3. `lastrowid` call sites: append `RETURNING id` on PG (the compat layer does not emulate
   `lastrowid`).
4. Flag columns stay int 0/1 (V013 uses SMALLINT precisely so these writes port unchanged).
5. Read-side timestamp adaptation: `email_verified_at`, `orgs.created_at`, `used_at`,
   `viewed_at`/`compared_at`, `occurred_at` come back as `datetime`, not `str`. Known breakage:
   `admin.py list_users_for_admin` does `(item.get("email_verified_at") or "").strip()` and
   `auth.get_user_email_verification_state` returns the raw value — normalize to
   ISO string (or truthiness) at the fetch boundary.
6. `search_analytics_db.usage_summary`: branch the two `json_extract` queries on backend.
7. Rollback story: the flag flips back to sqlite; the file is untouched by the copy.

## Encryption decision (SEC-088) — must be answered before cutover

SQLCipher encrypts the whole file at rest. Inventory Postgres does not give the equivalent:
anyone with `INVENTORY_DATABASE_URL` (which every scanner worker and script has) can read
`users.totp_secret` (plaintext TOTP seeds — the crown jewel here), `password` hashes, emails,
Stripe ids. Options, in rough order of preference:

1. **Application-level encryption of `totp_secret` (and `mfa_phone`)** reusing
   `USERS_DB_ENCRYPTION_KEY` as the column key (Fernet or pgcrypto). Keeps one secret, narrows
   the exposure to the only truly dangerous column. Requires a small encrypt/decrypt shim in
   `auth.get_user_totp`/`set_user_totp` and a re-encrypt pass in the copier.
2. Separate Postgres database (or same cluster, separate DB + role) for users, with a
   users-only DSN not distributed to scanner workers.
3. Accept managed-disk encryption (Railway PG encrypts volumes) + DSN hygiene, and record that
   SEC-088's answer changed. Weakest; at minimum rotate anything exposed if chosen.

Whichever is chosen must be written into `V013`'s successor or the port PR — the migration
header explicitly refuses to let this default silently.

## Ordering vs the concurrent schema.py work

1. **Land the other session's `users_db/schema.py` changes first**, then re-diff V013 against
   the final `init_users_db` before anyone runs `migrate --apply` past V012. V013 mirrors
   schema.py as of 2026-08-07; an unapplied migration file may still be edited freely (the
   checksum is only recorded at apply time). If V013 has already been applied somewhere by
   then, new columns go in a V014.
2. V012 (dealer_portal, other task) and V013 are independent; the runner applies them in
   numeric order and neither references the other's tables.
3. Apply V013 (`migrate --apply`) — inert, tables sit empty.
4. Pre-copy check: merge any case-duplicate emails/usernames in users.db (schema.py only
   *warns* about them; V013's `lower()` unique indexes will make the copier fail loudly).
5. Land the dual-mode port with the flag OFF. No behavior change.
6. Maintenance window: run the copier `--apply`, flip the flag, verify login/TOTP/billing
   reads, keep users.db as the rollback artifact.
7. Only after soak: remove the sqlite path and re-answer what happens to
   `USERS_DB_ENCRYPTION_KEY` (per the encryption decision above).

## Open questions

- `users.created_at` does not exist in sqlite (admin.py opportunistically reads it if present).
  V013 deliberately omits it for faithfulness — add in a follow-up migration if wanted; a
  `DEFAULT NOW()` backfill would fabricate signup dates for migrated rows.
- `orgs.stripe_current_period_end` kept as TEXT (opaque Stripe ISO pass-through) — convert to
  TIMESTAMPTZ later if anything ever needs to compare it.
- `org_invites` FKs are enforced with CASCADE on PG where sqlite enforced nothing — audit any
  future direct `DELETE FROM users/orgs` paths for surprise cascades.
- `filters_json` stays TEXT; move to JSONB (with a GIN index) if the analytics dashboard grows.
- Test suite strategy: ~30 test files monkeypatch `USERS_DB_PATH`; decide whether tests run the
  sqlite mode forever or gain a PG fixture (cf. `INVENTORY_SQLITE_TESTS`).
