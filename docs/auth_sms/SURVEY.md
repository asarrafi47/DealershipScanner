# Users and sign-in today: a survey of the code

Survey of `dev` at VERSION 1.5.24 (2026-11-13). Documentation only; no behavior was changed. Paths are relative to the repo root; `B/` means `backend/`. Line numbers were read from the tree on that date. Environment variable NAMES appear below; no values were read or printed.

Read this if you are the owner deciding about phone-number login, or a cloud session taking a card from `TASKS.md`. Companion files: `PAY_AND_STRIPE_AUDIT.md`, `DESIGN.md`, `TASKS.md`.

## 0. The short version (read this first)

- **There is no second factor anywhere today.** Public sign-in is password, Google or Apple. The old app 2FA (authenticator app, email code, SMS code, phone QR) was removed, and the `/dev` operator login is password-only too. `finalize_app_session` always sets `session["mfa_ok"] = True` (`B/auth/session.py:110`). The task brief assumed admins have "password plus authenticator"; the code does not have that. See section 3.6 and `DESIGN.md` section 12.
- **App users are not in Postgres yet.** They sit in an encrypted SQLite file (`users.db`) on the web volume. A Postgres `users` table exists (`migrations/V013__users.sql`) but is empty and unused. `USERS_DB_BACKEND` appears only in docs (`docs/USERS_PG_CUTOVER.md:57`), not in code.
- **Operators ("/dev") are a different account system**, `admin_users`: encrypted SQLite by default, Postgres (`public.admin_users`, V029) when `DEV_USERS_DB_BACKEND=pg`.
- **Login identity is `lower(username) OR lower(email)`.** Email is required, unique and used for password reset, verification mail, Google/Apple linking and the Stripe checkout form.
- **Sessions are signed browser cookies** (14 days, sliding). There is no server-side session list, so no "sign out everywhere".
- **Rate limits and lockouts live in memory**, per web worker, and reset on restart. Production runs one worker.
- **Billing is off in production**, so today paid features are login gates only (see `PAY_AND_STRIPE_AUDIT.md`).
- `docs/ALL_POSTGRES_PLAN_2026_10.md`, named in the brief, does not exist on dev. The closest documents are `docs/USERS_PG_CUTOVER.md`, `docs/NEXT_PHASES_PLAN_2026_10.md` (migration registry at lines 504-522, N37 at 2914-2930) and `docs/REMEDIATION_PLAN_2026_10.md` (Phase 16, lines 3585-3700).

## 1. Where users and per-user data live

### 1.1 Stores and the variables that choose them

| Store | Engine | Chosen by (names only) | Code |
|---|---|---|---|
| `users.db`: `users`, `orgs`, `org_invites`, `car_view_history`, `car_compare_history`, `compare_sessions`, `search_events` | SQLCipher SQLite file on the web volume (plain SQLite only in dev) | `USERS_DB_PATH`, `USERS_DB_ENCRYPTION_KEY` (32+ chars, required in production), `ALLOW_UNENCRYPTED_USER_DB` (dev/test), `USERS_DB_CONNECT_TIMEOUT_S`, `USERS_DB_BUSY_TIMEOUT_MS` | `B/db/users_sqlite.py:34-116`; `B/utils/credential_db_encryption.py:12-64` |
| `dev_users.db`: `admin_users` | SQLCipher SQLite | `DEV_USERS_DB_PATH`, `DEV_USERS_DB_ENCRYPTION_KEY` | `B/db/dev_users_sqlite.py:32-99`; `B/db/admin_users_db.py:189-255` |
| `public.admin_users` | Postgres | `DEV_USERS_DB_BACKEND=pg` (default sqlite) | `B/db/admin_users_db.py:25-36`; `migrations/V029__admin_users.sql:10-20` |
| Per-user shopper data (section 1.4) | Inventory Postgres | `INVENTORY_DATABASE_URL` or `DATABASE_URL` | `B/db/inventory_pg.py:76-109` |
| `dealer_vehicles` (dealer portal) | Inventory Postgres (sidecar SQLite `dealer_portal.db` only without Postgres) | `DEALER_PORTAL_DB_PATH` (sidecar only) | `B/db/dealer_portal_db.py:36-150` |
| Postgres copy of the users domain | Postgres, empty, not wired | none | `migrations/V013__users.sql`; `B/scripts/migrate_users_sqlite_to_postgres.py`; `docs/USERS_PG_CUTOVER.md:3-5` |

Boot order: `B/main.py:96-99` runs `init_users_db()`, `init_admin_db()`, `init_inventory_db()`, `init_dealer_portal_db()`.

### 1.2 `users` table (SQLite, the live one)

Base DDL `B/db/users_db/schema.py:118-127`, extra columns added by an ALTER ladder `:130-176`, indexes `:179-208`.

| Column | Type | Meaning |
|---|---|---|
| `id` | INTEGER PK AUTOINCREMENT | The session key `user_id`; every other table joins on it |
| `username` | TEXT UNIQUE NOT NULL | plus unique index on `lower(username)` |
| `email` | TEXT UNIQUE NOT NULL | plus unique index on `lower(email)` |
| `password` | TEXT NOT NULL | bcrypt hash; Google/Apple users get a random hash (`B/db/users_db/accounts.py:269,334`) |
| `role` | TEXT | see section 2; NULL is read as `dealer_staff` |
| `dealer_id`, `dealership_registry_id`, `org_id` | TEXT, INT, INT | dealer or dealer-group scope |
| `totp_secret`, `totp_enabled` | TEXT, INT | dead (authenticator removed); `totp_secret` is a plaintext seed column |
| `mfa_method`, `mfa_phone` | TEXT | dead (SMS MFA removed), see 3.7 |
| `is_premium`, `subscription_plan_id`, `premium_stripe_customer_id`, `premium_stripe_session_id`, `premium_stripe_subscription_id` | INT / TEXT | pay state, written by `B/db/users_db/billing.py` |
| `google_sub`, `apple_sub` | TEXT | provider ids; partial unique indexes where non-empty (`schema.py:179-186`) |
| `email_verified_at`, `email_verify_token_hash`, `email_verify_expires_at` | TEXT, TEXT, INT | email check |
| `password_reset_token_hash`, `password_reset_expires_at` | TEXT, INT | reset |
| `is_active` | INT NOT NULL DEFAULT 1 | suspend flag (`B/db/users_db/admin.py:490-513`) |

There is no `created_at` on users (`docs/USERS_PG_CUTOVER.md` open questions).

Other tables in `users.db`: `orgs` (name, `stripe_*` columns; `schema.py:210-222`); `org_invites` (declared at `:223-236`, **no code creates or uses invites**); `car_view_history`, `car_compare_history`, `compare_sessions` (`B/db/user_history_db.py:11-60`); `search_events` (nullable `user_id` plus an anonymous `session_key`; `B/db/search_analytics_db.py:25-45`).

Postgres twin `migrations/V013__users.sql:75-161` repeats these but **lacks `email_verify_expires_at`**, which SQLite has and `B/db/users_db/auth.py:450-454,522-526` uses. That is a latent cutover bug to fix before any copy.

### 1.3 `admin_users` (operators)

SQLite `B/db/admin_users_db.py:196-206`: id, username, email (both UNIQUE NOT NULL), password, `totp_secret`, `totp_enabled`, plus dead `mfa_method`/`mfa_phone` added at `:214-215`. Postgres V029 has the same shape without the dead MFA columns and with unique indexes on `lower(username)` and `lower(email)`. Writers: boot seed from `ADMIN_USERNAME`, `ADMIN_EMAIL`, `ADMIN_PASSWORD` (`:224-255`, Postgres `:51-70`), `/dev/register` when allowed, break-glass seeding at login (Postgres only, `:83-101`).

### 1.4 Shopper data in inventory Postgres

None of these has a foreign key to `users`, on purpose (the users were in another database): `migrations/V002:41`, `V018:12-15`, `V021:10-13`, `V022:18-21`.

| Table | User column | Defined | Written by |
|---|---|---|---|
| `saved_cars` | `user_id` NOT NULL, `UNIQUE(user_id, car_id)` | `V001__baseline.sql:1308-1314,1858-1862` | `B/db/repositories/saved_cars_repo.py:5-35`, route `POST /api/cars/<id>/save` (`B/routes/listings_api.py:812`) |
| `saved_searches` | `user_id` | `V018__saved_searches.sql:16-25` | `B/db/repositories/saved_searches_repo.py:17-90` |
| `user_hidden_dealers` | `user_id`, `UNIQUE(user_id, dealer_id)` | `V021__user_hidden_dealers.sql:14-24` | `B/db/repositories/hidden_dealers_repo.py:30-130` |
| `user_search_history` | `user_id` | `V022__user_search_history.sql:22-32` | `B/db/repositories/search_history_repo.py:99-250` |
| `car_comments`, `dealer_comments` | `user_id` NOT NULL, `ip_hash`, soft delete | `V002__community.sql:53-156` | `B/db/comments_db.py:569-636`, `B/routes/community_api.py` |
| `comment_attachments` | `user_id` | `V005__comment_attachments.sql:42-70` | `B/db/comments_db.py` |
| `dealer_reviews` | `user_id`, `display_name`, `UNIQUE(dealer_id, user_id)` | `V001:572-584,1602-1606` | `B/routes/dealer_reviews.py:53` (display name copied from the username at post time) |
| `dealer_vehicles` | `user_id`, `UNIQUE(user_id, vin)` | `V001:674-676`, `V012__dealer_portal.sql:37-63` | `B/db/dealer_portal_db.py:218-390`, `B/dealer/routes.py` |

Uploaded dealer photos sit on disk under `UPLOAD_ROOT/<user_id>/<vehicle_id>/` (`B/dealer/routes.py:182-186,450-475`).

Deleting a user (`delete_user_by_email`, `B/db/users_db/accounts.py:447-518`) removes the row, the dealer vehicles and invites. It leaves saved cars, comments, reviews, history and search rows behind.

### 1.5 Who writes `users`

| Path | Writer |
|---|---|
| Public sign-up | `B/auth/app_registration.py:312-363` then `save_user` (`accounts.py:390`) |
| Dealer sign-up | `B/dealer/routes.py:114-161` (creates an org; role `dealership_owner`) |
| Google / Apple first login | `save_oauth_user` (`accounts.py:260`), `save_apple_oauth_user` (`:325`) |
| Admin hub create/edit/reset/suspend/delete | `B/db/users_db/admin.py:237-513`, `B/dealer/admin/users_hub.py:57-200` |
| Profile and password self-service | `B/routes/account.py:99-165`; `B/db/users_db/auth.py:140-304` |
| Stripe webhooks and success pages | `grant_user_premium` / `revoke_user_premium` (`B/db/users_db/billing.py:4-68`); orgs via `update_org_stripe_subscription` (`accounts.py:46-85`) |
| Env-admin promotion | `B/db/users_db/schema.py:23-112` |
| Scripts | `scripts/bootstrap_site_admin.py`, `B/scripts/migrate_users_sqlite_to_postgres.py` |

The select-column list is cached (`B/db/users_db/_common.py:326-348`); a schema change needs `cache_clear()`.

## 2. Roles

Role names: `admin`, `dealership_owner`, `dealership_admin`, `dealership_member`, `general_user` (`B/utils/roles.py:9-23`). The word "shopper" in the brief is `general_user`; "dealer" is the three `dealership_*` roles. `normalize_role` (`:26-30`) turns anything else, including the old `dealer_staff`, into `general_user`. `dealer_staff` is still the stored default for a NULL role (`schema.py:287-292`, `accounts.py:123-124`) and is still assignable in the admin hub (`B/db/users_db/admin.py:167-174`). Result: the session says `general_user` while `/admin` scope checks read the raw stored value (`B/dealer/admin/routes.py:72-82`).

How a role is decided:

| Source | Rule | Where |
|---|---|---|
| Public sign-up, Google, Apple | always `general_user` | `app_registration.py:350`, `google_oauth.py:159`, `apple_oauth.py:213` |
| Dealer sign-up | always `dealership_owner` plus a new org | `B/dealer/routes.py:114-161` |
| Admin hub | any of the assignable roles | `B/db/users_db/admin.py:237-275,323-443` |
| Site admin by env list | `APP_ADMIN_EMAILS` (must contain `@`) and `APP_ADMIN_USERNAMES` | `B/utils/roles.py:33-47,64-66` |
| Promotion | at init outside production, and after every login or profile save through `sync_env_admin_user_row` | `schema.py:23-112,281-285`; `B/auth/pages.py:106,283`; `B/dealer/routes.py:99,102`; `google_oauth.py:193`; `B/routes/account.py:155` |
| Effect | role set to `admin`, `totp_enabled=0`, `totp_secret=NULL`, `mfa_method=NULL` | `schema.py:11-20` |
| Seeding a missing admin | only outside production and only with `ALLOW_DEFAULT_APP_USER` | `schema.py:44-82,237-274` |
| Protection | env-admin email or username cannot be registered by others, edited onto, demoted or deleted | `roles.py:69-76`; `auth.py:190-197`; `admin.py:224-235,372-374,474` |

There is no organisation-level permission logic: `dealership_admin` and `dealership_member` are labels only. The Stripe org gate applies to anyone with `org_id > 0` (`B/auth/session.py:40-50`, `B/web/security.py:120-143`).

## 3. Sign-in methods

### 3.1 Password
- Hash: bcrypt, cost 13 by default (`BCRYPT_ROUNDS`, clamped 12-15; tests use `BCRYPT_TEST_LOW_COST`). Inputs over 72 bytes are SHA-256 pre-hashed. `B/db/password_hash.py:13-48`. Old hashes are re-hashed in a background thread after a good login (`B/db/users_db/_common.py:382-407`; called at `auth.py:361-362,674-675`). Plain-text legacy passwords verify only outside production with `ALLOW_LEGACY_PLAINTEXT_PASSWORDS` (`password_hash.py:86-106`).
- Routes: `POST /login` (`B/auth/pages.py:81-114`), `POST /api/auth/login` JSON `{login, password}` (`:267-289`), `POST /dealer/login` (`B/dealer/routes.py:74-111`).
- Lookup: `lower(username)=lower(?) OR lower(email)=lower(?)` (`auth.py:97-100,328-349,658-662`). A suspended user gets the same failure as a wrong password (`:359-360`).
- Policy: length `MIN_PASSWORD_LENGTH` (`B/config.py:45`, minimum 8), maximum 128; new usernames may not contain `@` (`registration_validation.py:5-53`).

### 3.2 Google
Server-side code flow at `/auth/google` and `/auth/google/callback` (`B/auth/google_oauth.py:33,210-280`). Variables: `GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET`, `GOOGLE_OAUTH_REDIRECT_URI`, `GOOGLE_OAUTH_SHOW_BUTTON`, `GOOGLE_OAUTH_OFFER_PREMIUM_ON_LOGIN`. State token compared in constant time; no PKCE; uses the userinfo endpoint. Only `email_verified is True` is accepted (`:266-269`). Rate limit key `google_oauth:{ip}` (`:236-238`). Identity key `users.google_sub`.

### 3.3 Apple
`/auth/apple` and `/auth/apple/callback` (`B/auth/apple_oauth.py:248-312`). Client secret is an ES256 JWT built from `APPLE_OAUTH_CLIENT_ID`, `APPLE_OAUTH_TEAM_ID`, `APPLE_OAUTH_KEY_ID`, `APPLE_OAUTH_PRIVATE_KEY` (+ `APPLE_OAUTH_REDIRECT_URI`, `APPLE_OAUTH_SHOW_BUTTON`). The id token is checked against Apple's keys (`:147-170`). **If Apple sends no email, a made-up `<sub12>@privaterelay.appleid.com` is stored as the account email** (`:206-209`). Identity key `users.apple_sub`.

### 3.4 Email verification
Off unless `EMAIL_VERIFICATION_ENABLED` (`B/auth/email_verification.py:28-34`). Token `secrets.token_urlsafe(32)`, stored as a peppered SHA-256 (`EMAIL_VERIFY_PEPPER`, else the secret key), 48 hours (`:37-115`). Sent at sign-up (`app_registration.py:359-361`) and by `POST /resend-verification`, 5 per hour per IP (`pages.py:157-188`); `GET /verify-email` (`:149-154`). **Never enforced as a gate**: it only decides OAuth auto-linking and some admin counts. Changing the email clears `email_verified_at` (`auth.py:220-226`).

### 3.5 Password reset
Off unless `PASSWORD_RESET_ENABLED` (`B/auth/password_reset.py:164-170`). `/forgot-password`, `/reset-password` (`pages.py:191-234`). One hour; peppered hash (`PASSWORD_RESET_PEPPER`, else `EMAIL_VERIFY_PEPPER`, else the secret key). Skips accounts with a Google or Apple id (`:222-223`). Needs an account email containing `@` (`:226-227`). Limits 10 per hour per IP and 3 per hour per account (`pages.py:201`; `password_reset.py:230-236`). **A reset does not end other sessions.**

### 3.6 Authenticator-app TOTP (removed)
No login path calls it. Left behind: `get_user_totp`/`set_user_totp` (`auth.py:27-86`), `get_admin_totp`/`set_admin_totp` (`admin_users_db.py:331-400`), the `mfa_label` in the admin list (`admin.py:151-155`), and redirect-only stubs `/mfa/choose`, `/mfa/setup`, `/mfa/verify`, `/mfa/qr*` (`B/auth/pages.py:348-394`) and `/dev/mfa/*` (`B/dev/routes.py:696-703`). The V013 Postgres table copies the plaintext `totp_secret` column; the plan is to drop it (`docs/REMEDIATION_PLAN_2026_10.md` D-SEC2, P16.2; registry V031 "drop dead MFA columns").

### 3.7 Legacy SMS MFA: where, when, why, what is left
- **When and why.** The only written record is `docs/SECURITY_MASTER_TODO.md`. On 2026-04-25 SEC-014 made authenticator 2FA mandatory (`:1085`), then added a `/mfa/choose` email or SMS code step (`:1087`), then Resend for email and Twilio Verify for SMS (`:1090`). The same day SEC-062 "Removed all SMS 2FA (Verify + messaging); deleted `mfa_twilio_verify.py` and `twilio` dependency" (`:1091`; entry `:372-380`: "no SMS MFA ... `mfa_phone` column may remain unused"). Later entries retire phone-QR 2FA (SEC-063, `:362-369`), the `/dev` 2FA (`:1096-1097`), and finally all app 2FA (SEC-014 "App 2FA removed (password-only auth)", `:322-330`; `docs/FULL_AUDIT_MAY2026.md:98`). **No document states the reason.** Do not assume one; ask the owner (`DESIGN.md` question 1).
- **Git history.** This clone holds only 71 commits on dev (oldest 2026-10-08), so the removal commits are not in it. `git log -S` for `send_sms`, `mfa_phone`, `twilio`, `mfa_method` finds only bulk merges (`9d6d5002`, `5e39d9ce`, `0cc7090e`, `35442c89`, `5ec5ac60`). `CHANGELOG.md:1161` names commit `9215d85` ("removed dead MFA modules"); that object is not in this clone.
- **Left in code:** the columns `mfa_method` and `mfa_phone` (`schema.py:137-138`, `admin_users_db.py:214-215`, V013 `:87-88`); `set_user_mfa_phone` (`auth.py:626-646`, exported `B/db/users_db/__init__.py:80,141`; **no caller**); phone handling in `_common.py:38,65-66` and `auth.py:379,398-399`; `B/utils/mfa_delivery.py` (its email sender is now used for verify and reset mail) and `B/utils/mfa_action_log.py`.
- **Not left:** no `twilio` import or requirement, no `send_sms`, no SMS templates. Stale doc names (`backend/auth/mfa.py`, `mfa_twilio_verify.py`, `mfa_qr_store.py`) do not exist on disk.
- **Tests:** `backend/tests/test_mfa_totp.py` (3, now "password-only plus redirects"), `test_mfa_qr.py` (1), `test_mfa_action_log.py` (1).

## 4. Sessions and CSRF

- **Secret:** `SECRET_KEY` (legacy `FLASK_SECRET_KEY`); production refuses to start without it (`B/config.py:69-75`, `B/main.py:57-66`).
- **Cookie:** HttpOnly, SameSite=Lax, Secure in production or with `SESSION_COOKIE_SECURE=1` (`B/main.py:79-81`; `B/utils/runtime_env.py:26-36`). Default Flask name. Lifetime 14 days, refreshed on each request (`main.py:82-83`; `session.permanent` at `B/auth/session.py:81`). The cookie is the whole session (signed, client side).
- **Contents** (`B/auth/session.py:67-121`): `user_id`, `username`, `user_email`, `user_role`, `user_dealer_id`, `user_dealership_registry_id`, `org_id`, `user_is_premium`, `subscription_plan_id`, `entitlements`, `mfa_ok`, `org_subscription_status`, `_csrf_token`. `/dev` adds `admin_user_id`, `admin_username` in the same cookie (`B/dev/routes.py:186-196`).
- **Healing:** each request re-reads the user row to correct role, premium flag and plan (`B/billing/access.py:185-215`). A suspended user's cookie still passes plain routes; only gates that re-fetch the profile (`/admin`, `/api/admin/*`, `/inventory`, billing) reject it.
- **Fixation:** every login does `session.clear()` first (`pages.py:108,285`; `B/dealer/routes.py:104-105`; `google_oauth.py:194`). **Logout** is `session.clear()` on `POST /logout` and `POST /api/auth/logout` (`pages.py:237-239,341-343`). There is no revocation; a planned `users.auth_epoch` (registry V030, P16.1) is not on dev.
- **CSRF:** home-made double-submit token (`B/utils/csrf.py:9-42`): session key `_csrf_token`, form field `csrf_token`, header `X-CSRF-Token`, constant-time compare; token endpoint `GET /api/auth/csrf` (`pages.py:253-254`). One `before_request` hook with explicit lists of form routes, header routes and DELETE routes (`B/web/security.py:49-117`, registered `:214`); `test_csrf_coverage.py` fails on any new mutating route that is not classified. Exempt by omission: the two Stripe webhooks (`billing.stripe_webhook`, `billing.premium_webhook`). `/dev` has its own hook (`B/dev/routes.py:246-282`). Two GET routes start Stripe checkouts (`B/billing/routes.py:182,317`).

## 5. Rate limits, lockout, guessing which emails exist, email

- **Limiter:** `B/utils/ip_rate_limit.py`, a sliding window in a Python dict. Per worker, lost on restart, no Redis. Production sets `GUNICORN_WORKERS=1` (`scripts/docker-entrypoint-web.sh:54-65`; warning at `B/utils/production_security.py:104`). Client IP from `B/utils/client_ip.py` (`TRUSTED_PROXY_HOPS`, `TRUST_PROXY_HEADERS`).
- **Limits:** login per IP `RATE_LIMIT_LOGIN_PER_MIN` default 30 (`pages.py:35-51,94,269`; `config.py:56`); per account 10 failures per 15 minutes, reserved before the bcrypt check and refunded on success (`pages.py:30-78`, SEC-A-010); sign-up `RATE_LIMIT_REGISTER_PER_MIN` 10 (`pages.py:120,295`); dealer auth `RATE_LIMIT_DEALER_AUTH_PER_MIN` 20 (`B/dealer/routes.py:70-120`); forgot password 10 per hour per IP; resend verification 5 per hour; OAuth callbacks at the login limit; `/dev/login` `RATE_LIMIT_DEV_LOGIN_PER_MIN` 20, `/dev/register` 5 (`B/dev/routes.py:159-160,603,649`); chat limits per user or IP (`B/routes/ai_chat_bp.py:165-170`).
- **Not guessing which accounts exist:** login says one thing for unknown user, wrong password or suspended, and burns a dummy bcrypt check for unknown users (`pages.py:113,281`; `auth.py:14-24,352-354`). Forgot-password always says "If an account exists..." (`pages.py:209-211`). **Leaks:** sign-up says "already registered" (`app_registration.py:337-343`; 409 `duplicate_user` at `pages.py:314-315`); profile edit does too (`auth.py:204,219`); `/dealer/login` shows no error text (`B/dealer/routes.py:111`); `/dev/login` returns early for unknown users, so timing differs (`admin_users_db.py:298-328`).
- **Email sending:** `send_transactional_email` in `B/utils/mfa_delivery.py:56-180`, plain text only, bodies inline (`email_verification.py:98-104`, `password_reset.py:242-248`). Provider chosen by `MFA_EMAIL_PROVIDER` (auto, resend, smtp): Resend when `RESEND_API_KEY` is set (`RESEND_FROM`, else `SMTP_FROM`); otherwise SMTP with `SMTP_HOST`, `SMTP_PORT`, `SMTP_USERNAME`, `SMTP_PASSWORD`, `SMTP_FROM`. `MFA_DELIVERY_MODE` smtp, log or test (log/test write the message to the log). Production with nothing set raises. Links use `PUBLIC_BASE_URL`. Failures are caught: reset still says success. Failure logs include the destination address (`B/utils/mfa_action_log.py`, `MFA_ACTION_LOG_PATH`). The app sends no receipts; Stripe does. Resend variables are missing from `.env.example:128-135`.

## 6. Linking Google and Apple to existing accounts (`B/auth/oauth_link.py`)

1. A known provider id signs in directly.
2. Otherwise match by exact `lower(email)` (not username) via `find_users_by_email` (`auth.py:108-137`). More than one match: refuse (`oauth_link.py:192-194`). Different provider id already on the account: refuse (`:197-199`).
3. If the account's email is **not verified**, nothing is linked. The link waits 600 seconds in the session and completes only after a successful password login to that account (`:157,209-216,237-250,258-289`; `pages.py:111,288`). This blocks pre-registration takeover.
4. If verified: link the id, mark verified (`:203-206`).
5. No account: create a `general_user` with a random password and a username from the email.
6. The provider must say the email is verified. Env-admin identities cannot be created this way (`google_oauth.py:156-157`). A username equal to another account's email is blocked (`accounts.py:242-257`).
7. There is no way to unlink. Tests: `test_google_oauth.py` (18), `test_apple_oauth.py` (10).

## 7. Admin sites (three)

1. **`/dev` operator console** (`B/main.py:175`; `B/dev/routes.py`): `admin_users` accounts, form fields `login`/`password` (`:596-632`). **Password only.** Gate `_dev_require_admin` (`:245-282`): optional `DEV_IP_ALLOWLIST`, then CSRF, then session. An app user with role `admin` is also let in outside production, or in production with `ALLOW_APP_ADMIN_DEV_PASS_THROUGH` (`:175-183`; `B/utils/production_security.py:15-23`). `/dev/register` only with `ALLOW_DEV_PUBLIC_REGISTER` (`admin_users_db.py:138-144`). The operator cookie check does not re-read the database.
2. **`/dev/manifest`**: a shared secret (`DEV_CONSOLE`, `DEV_CONSOLE_SECRET`), not an account (`B/dev/console.py:95-270`).
3. **`/admin/*`** (`B/dealer/admin/routes.py:48-378`): normal app session, database re-checked with the `is_active` flag (`:56-69`). Entry for admins and for users with a dealer id; user, dealer, review, scanner, data-quality and attribution hubs are admin-only.

## 8. How routes are gated

There is **no `@login_required` decorator.** Gates are small helpers and hooks.

| Gate | Defined | Result | Re-reads DB |
|---|---|---|---|
| inline `session.get("user_id")` then 401 JSON | repeated in `B/routes/listings_api.py` (lines 803, 814, 856, 868, 886, 946, 955, 981, 1004, 1019, 1030) | 401 | no |
| `_require_user()` | `B/routes/community_api.py:103-111` | 401 | no |
| inline redirect to `/login` | `B/routes/account.py:101-103,170-172,203-205`; `home_dashboard.py:73-74,351-352`; `dealer_reviews.py:54-58` | 302 | no |
| `_require_app_login()` | `B/billing/routes.py:39-46` | redirect | no |
| `_require_login()` | `B/dealer/routes.py:164-179` | redirect | yes |
| `_require_store_profile`, `_require_access` | `B/dealer/admin/routes.py:55-98` | redirect | yes |
| `_require_site_admin()` (six copies) | `users_hub.py:37`, `dealers_hub.py:16`, `reviews_hub.py:16`, `scanner_ops_hub.py:13`, `data_quality_hub.py:35`, `attribution_hub.py:29` | redirect | yes |
| `_require_site_admin_api()` | `B/dealer/admin/operator_api.py:18-32` | 403 | yes |
| `_current_admin_ok()` | `B/routes/admin_dealer_api.py:25-42` | 403, fails closed | yes |
| `_dev_require_admin` | `B/dev/routes.py:245-282` | redirect or 401 | no |
| `require_dev_access` | `B/dev/console.py:95-117` | secret | no |
| `_billing_gate_paid_routes` | `B/web/security.py:120-143` | redirect to `/billing/required` (orgs only; endpoints `app_home`, `dashboard`, `dealer_portal.*`, `store_admin.*`) | partly |
| `paid_access.check_feature(...)` | `B/billing/access.py` | 401/403 by plan | yes |

Routes by gate:

| Gate | Routes |
|---|---|
| Login, 401 JSON | `GET /api/saved-cars` (`listings_api.py:801`); `POST /api/cars/<id>/save` (`:812`); saved searches `GET/POST /api/saved-searches`, `DELETE /api/saved-searches/<id>` (`:851,863,882`, also plan feature); hidden dealers `GET/POST /api/profile/hidden-dealers`, `DELETE .../<dealer_id>` (`:944,953,979`); `GET/DELETE /api/profile/search-history`, `DELETE .../<id>` (`:1002,1017,1028`) |
| `_require_user` | car and dealer comments: `POST/DELETE /api/cars/<id>/comments[/<cid>]`, `POST/DELETE /api/dealerships/<id>/comments[/<cid>]` (`community_api.py:222-300,365-395`) |
| Login, redirect | `/home` (`home_dashboard.py:72`), `/dashboard` (`:350`), `/account/profile`, `/account/password` (`account.py:99`), `/account/billing` (`:168`), `/account/billing/portal` (`:202`), `POST /dealership/<key>/reviews` (`dealer_reviews.py:53`; the `next` it sends is ignored by `login_page`), `POST /resend-verification` |
| `_require_app_login` | `/billing/required`, `/checkout`, `/success`, `/premium/checkout`, `/premium/success`, `/plan/checkout`, `/plan/success` (`B/billing/routes.py:59,73,97,182,198,317,344`). `/billing/plans` and both webhooks are public |
| `_require_login` (dealer) | `/inventory` and its sub-routes, `GET /dealer-uploads/<uid>/<vid>/<file>` (`B/dealer/routes.py:196-450`) |
| Store admin hook | `/admin/`, `/admin/site`, `/admin/inventory...`, `/admin/scans`, `POST /admin/dealers/<id>/rescan` (`B/dealer/admin/routes.py:112-378`) |
| Site admin | `/admin/users`, `/admin/users/new`, `/admin/users/<id>/edit`, `/admin/dealers`, `/admin/reviews...`, `/admin/scanner-ops`, `/admin/data-quality`, `/admin/attribution`; APIs `/api/admin/operator/*`, `/api/admin/dealer-*` |
| `/dev` gate | every `/dev/*` route (about 40) except login, register, logout |

Routes with no hard gate that still touch per-user data (they degrade for anonymous visitors): search and listings record history and read hidden dealers when logged in (`listings_api.py:535,732,746`; `B/listings/routes.py:103-264`); `GET /car/<id>`, `GET /api/cars/<id>` write view history (`B/routes/car_detail/viewer.py:10-22`); `/compare` writes compare history (`cars_pages.py:137-141`); the dealership page reads saved state (`dealership_page.py:854-906`); dealer locator login is optional unless `DEALER_LOCATOR_REQUIRE_LOGIN` (`dealers_recalls.py:146-170`); the three chat endpoints are open with per-user or per-IP daily limits; comment and review flagging is anonymous by design.

## 9. What breaks if the login identifier becomes a phone number

**Does not break:** joins. Every per-user table joins on `users.id`, which does not change. Stripe webhooks key on `metadata.user_id` / `org_id`, not email (`B/billing/routes.py:143-161,246-262`). A blank email is tolerated by checkout (`B/billing/stripe_billing.py:89,128`; `checkout_service.py:65`); Stripe then asks the buyer.

**Breaks or needs work:**

| Item | Where | What happens |
|---|---|---|
| `email`, `username`, `password` are NOT NULL and unique | `schema.py:118-127`; V013 `:78-80` | A phone-only account has no email. Needs a relaxed column or a placeholder. SQLite cannot drop NOT NULL without rebuilding the table, and `init_users_db` only ALTERs. Empty emails would collide on `idx_users_email_ci`. |
| Need a new unique phone column | none today; `users.mfa_phone` is unnormalized, not unique, dead | add a new column and index; do not reuse `mfa_phone` |
| Delete by email | `delete_user_by_email` requires `@` (`accounts.py:447-518`); `delete_user_by_id` fails when email is empty (`admin.py:481-487`) | a phone-only account cannot be deleted in the admin hub |
| Password reset and email verification | `password_reset.py:226-227`; `email_verification.py` | silently skip accounts without an email; the phone flow needs its own code path |
| Validators | `registration_validation.py:5-53`; `account.py:120-124`; `password_reset.py:270-276` | email must contain `@`; some callers fall back to `"user@local"` |
| Google/Apple linking | `oauth_link.py` | wholly email-based; a phone-only user would never match; Apple's fake relay email (`apple_oauth.py:206-209`) is another collision source |
| Env admin lists | `B/utils/roles.py:33-47` | `APP_ADMIN_EMAILS` needs `@`; a phone admin would use `APP_ADMIN_USERNAMES` |
| Lockout key | `pages.py:54-60` | unknown accounts key on the raw lowercased input; phone numbers must be normalized first or one number gets several buckets |
| Session | `session["user_email"]` is used for checkout; `username` is shown in the header and copied into review names (`dealer_reviews.py:33-43`) | a phone-only user needs a generated display name; never show the number |
| Forms and JSON | `register.html:37-47`, `dealer_register.html:32-37`, `account_profile.html:64-69` (`type="email" required`); admin `user_form.html:25-27`; `/api/auth/me` and login JSON return `email` (`pages.py:242-250`) | change |
| iOS app | cookie session plus CSRF, no tokens (`ios/SarrafiCars/SarrafiCars/Networking/APIClient.swift:60-118`); fields `login`/`password`, register `username`,`email`,`password`,`plan`; labels in `Features/Auth/AuthHeaderBar.swift:25,83,120-125`; contract `ios/docs/API_CONTRACT.md`, `B/mobile/contract.py:27-47`, `test_mobile_api_contract.py` | client and contract change |
| Stats | `B/platform_stats.py:98-101` counts verified emails | cosmetic |
| Copier | `B/scripts/migrate_users_sqlite_to_postgres.py` lists columns | add the new column to both V013 successor and copier |
| Admin accounts | `admin_users.email` NOT NULL | untouched if operators keep email and password (recommended) |

## 10. Tests that guard this area

About 164 tests in 28 files (`test_google_oauth.py` 18, `test_store_admin.py` 13, `test_admin_users.py` 10, `test_apple_oauth.py` 10, `test_password_hash.py` 10, `test_billing_gate.py` 9, `test_username_email_collision.py` 9, `test_password_reset.py` 8, `test_admin_users_pg.py` 7, `test_login_account_throttle.py` 7, `test_login_session_fixes.py` 7, `test_mobile_api_contract.py` 7, `test_csrf_coverage.py` 6, `test_email_verification.py` 6, others smaller). 56 of 440 files in `backend/tests/` touch users in some way. Shared fixtures are in the root `conftest.py` (`app_factory`, `BCRYPT_TEST_LOW_COST`). `backend/tests/test_no_sqlite.py` freezes SQLite use (`sqlite_allowlist.txt` can only shrink), which matters for any new table.

## 11. Open items this survey could not settle
- Why app-wide 2FA was removed (not written down).
- Callers of `is_dealer_portal_role` were not listed.
- No caller of `set_user_mfa_phone` was found in `backend/`; `scripts/` was not grepped separately.
- Whether the iOS app uses native Apple/Google sign-in (only an embedded web view is evidenced, `App/AppState.swift:50-52`).
