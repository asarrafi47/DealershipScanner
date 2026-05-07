# Security Master Todo

**Single source of truth** for security hardening in the Sarrafi Collection project.  
**Policy:** Security-related work is **not complete** until this document reflects reality: statuses, validation evidence, and the [Changelog](#changelog) are updated in the **same change** as the code or config.

---

## How to use (humans & AI)

1. **Before** implementing or reviewing security work: read this file; pick or add the relevant item IDs.
2. **During** work: keep IDs in commit messages / PR descriptions (e.g. `SEC-012`).
3. **After** work: update **Status**, **Last verified** (date), **Validation** notes if the procedure changed, and append **Changelog**.
4. **New** risks or follow-ups: add a row under the right phase; do not rely on chat-only tracking.

**Status values:** `Done` | `In progress` | `Blocked` | `Not started`

---

## Summary

| Phase | Theme                        | Open items |
|-------|------------------------------|------------|
| 1     | Secrets & environment        | 0          |
| 2     | Identity, sessions, CSRF     | 0          |
| 3     | Input validation & stability | 0          |
| 4     | Frontend XSS & CSP           | 0          |
| 5     | APIs, abuse, LLM surfaces    | 0          |
| 6     | Dev / operator tooling       | 0          |

---

## Authorization model (SEC-013)

| Surface | Login required? | Protection |
|---------|-----------------|------------|
| `/search`, `/listings`, `/car/<id>` | **No** — public inventory | Normal browsing |
| `/dashboard`, `/` → `/login` | No (dashboard is placeholder) | Nav shows **My inventory** when `session['user_id']` is set |
| `/inventory`, `POST /inventory/*` | **Yes** — `session['user_id']` (app `users` table) | CSRF on POST; per-IP rate limit on VIN add; rows scoped to `user_id` in `dealer_portal.db` |
| `/admin/*` (store dashboard, inventory, scans) | **Yes** — same app session; **admin** role or **dealer_staff** with `dealer_id` / `dealership_registry_id` on `users` row | CSRF on POST; inventory rows scoped to dealer match on `cars`; internal notes never exposed on public `/car` JSON; re-scan subprocess **off** unless `ALLOW_STORE_ADMIN_RESCAN=1` and caller is **admin** |
| `/dealer-uploads/<user_id>/<vehicle_id>/<file>` | **Yes** — path `user_id` must match session | JPEG/PNG/WebP only at upload; filenames are server-generated; `send_from_directory` under upload root |
| `/logout` | N/A | `POST` only; CSRF form; clears Flask session; `GET /logout` → **405** (avoids logout CSRF) (see **SEC-060**). |
| `/mfa/choose` | **Yes** — `mfa_pending_user_id` in session (post-password) | CSRF on **POST**; user selects **email**, **TOTP** (if enabled), or **phone QR** (when `REDIS_URL` or in-memory in non-prod; see **SEC-063**); **POST** sends the email OTP when applicable |
| `/mfa/qr-wait`, `/mfa/qr-approve-png`, `POST /mfa/qr/complete` | **Yes** — same `mfa_pending_user_id` + `mfa_qr_attempt_id` (desktop) | `POST /mfa/qr/complete` is CSRF-protected; desktop Socket.IO `mfa_qr_subscribe` requires matching session + attempt; PNG only for the session that created the attempt |
| `GET/POST /mfa/qr-confirm/<token>` | **No** (browser on phone) | **Unguessable** `token` in URL + **per-view** `ap_nonce` in POST (no session CSRF); per-IP rate limit; state in **Redis** (or in-memory in dev; production needs `REDIS_URL`) with short TTL |
| `/socket.io/*` (Flask-SocketIO) | Same-origin to app | Production defaults: explicit origins from `SOCKETIO_CORS_ORIGINS`, else `PUBLIC_BASE_URL` / `MFA_QR_BASE_URL`, else empty allowlist (see SEC-063); dev unset → `*`. `SOCKETIO_CORS_ORIGINS=*` in prod logs a warning. `mfa_qr_subscribe` is session-bound before `join_room` |
| `/mfa/qr` (TOTP setup image) | **Yes** during pending TOTP setup | `otpauth://` QR as PNG (**Segno**); no public data beyond what the user’s session already holds |
| `/login`, `/register` | N/A | CSRF on POST; bcrypt passwords; per-IP rate limits on POST; **general** accounts (no org) use these; **dealership** sign-up with org uses `/dealer/register` + `/dealer/login` |
| `/dev/login`, `/dev/register` | N/A | CSRF on POST; bcrypt; per-IP rate limits; `/dev/register` gated in production; optional `DEV_IP_ALLOWLIST` (comma IPs / CIDRs) restricts all `/dev/*` when set |
| `/api/search/smart` | No | CSRF header (`X-CSRF-Token`) + per-IP rate limit (see SEC-043) |
| `POST /api/session/listings-geo` | No | CSRF header; stores last listings ZIP + radius in the Flask session (for dashboard recommendations) |
| `/api/car/<id>/chat` | No | CSRF header + per-IP rate limit + max message/body size |
| `POST /api/cars/<id>/save` | **Yes** — `session['user_id']` | CSRF header (`X-CSRF-Token`, same token as meta `csrf-token`) |
| `/dev/*` (dashboard, APIs) | **Yes** — `admin_users` in `dev_users.db` (password at `/dev/login` only; **no** 2FA on `/dev` currently) | CSRF on forms + `X-CSRF-Token` on API; POST `/dev/logout`; includes `POST /dev/api/cars/<id>/spec-backfill` (optional Google CSE env for search tier) and `POST /dev/api/cars/<id>/kbb-refresh` (optional `KBB_API_KEY` for IDWS) |
| `/dev/manifest`, `/api/dev/*` | `DEV_CONSOLE` + optional `DEV_CONSOLE_SECRET` | CSRF on mutations; safe `next` under `/dev/manifest` |

**Intent:** Inventory and smart search stay **public** for this product; `/dev` and manifest console stay **operator-only**. Tighten with app-level login or API keys if you expose the app to untrusted networks.

---

## Environment variables (quick reference)

| Variable | Required when | Purpose |
|----------|----------------|---------|
| `FLASK_ENV=production` | Production | Enables strict checks below |
| `SECRET_KEY` or `FLASK_SECRET_KEY` | `FLASK_ENV=production` | Flask session signing |
| `ADMIN_PASSWORD` | `FLASK_ENV=production` | Bootstrap/update `/dev` admin hash |
| `SESSION_COOKIE_SECURE=0` | Local HTTPS testing | Allow session cookie without HTTPS |
| `MIN_PASSWORD_LENGTH` | Optional | Registration (default 8) |
| `USERS_DB_PATH` | Optional | Default `users.db` (app `users` table) |
| `USERS_DB_CONNECT_TIMEOUT_S` | Optional | Default `30` (SQLite `connect` wait; reduces `database is locked` under dev reload / concurrency) |
| `USERS_DB_BUSY_TIMEOUT_MS` | Optional | Default `30000` (SQLite `busy_timeout` per query) |
| `RATE_LIMIT_SMART_SEARCH_PER_MIN` | Optional | Default 90 |
| `RATE_LIMIT_CAR_CHAT_PER_MIN` | Optional | Default 40 |
| `TRUST_PROXY_HEADERS` | Optional (`1` / `true`) | When set, rate limits use first `X-Forwarded-For` hop (use only behind a trusted proxy) |
| `RATE_LIMIT_LOGIN_PER_MIN` | Optional | Default 30 (`/login` POST) |
| `RATE_LIMIT_REGISTER_PER_MIN` | Optional | Default 10 (`/register` POST) |
| `RATE_LIMIT_MFA_VERIFY_PER_MIN` | Optional | Default 20 (`POST /mfa/verify` per IP) |
| `RATE_LIMIT_MFA_TOTP_ENROLL_PER_MIN` | Optional | Default 10 (`POST` TOTP confirm on `/mfa/setup` per IP) |
| `MFA_DELIVERY_MODE` | Optional | `smtp` (default for transport fallback label), `log` (log code; dev), or `test` (pytest) — see `backend/utils/mfa_delivery.py` |
| `MFA_EMAIL_PROVIDER` | Optional | `auto` (default: Resend if `RESEND_API_KEY` set, else SMTP), `resend`, or `smtp` |
| `RESEND_API_KEY`, `RESEND_FROM` | Resend (recommended) | Transactional email API; `RESEND_FROM` is a verified sender (else `SMTP_FROM`) |
| `SMTP_HOST`, `SMTP_PORT`, `SMTP_USERNAME`, `SMTP_PASSWORD`, `SMTP_FROM` | If not using Resend, or as `RESEND_FROM` fallback | Legacy SMTP path for email OTP; non-prod may **fall back to log** if nothing is configured |
| `MFA_DEV_UI_CODE` | Optional (non-production) | When delivery is `log`/`test`, may surface the last code on `/mfa/verify` to ease local testing |
| `MFA_ACTION_LOG_PATH` | Optional | Append-only JSONL of MFA **events** (no OTPs); use a **writable** path (e.g. `logs/mfa_actions.jsonl` from the project directory, or `$HOME/...`); not a literal `/path/to/...` |
| `REDIS_URL` | **Production** phone QR 2FA | e.g. `redis://:password@host:6379/0` — **required** for **SEC-063** QR sign-in in production; dev may omit (in-process store) |
| `MFA_QR_INMEMORY` | Optional | When `1`/`true`, use in-process attempt store in **production** (intended for tests; not for multi-server) |
| `MFA_QR_ATTEMPT_TTL_SECONDS` / `MFA_QR_APPROVED_TTL_SECONDS` | Optional | Defaults 120; Redis key TTLs for scan + desktop finalize window |
| `PUBLIC_BASE_URL` or `MFA_QR_BASE_URL` | **Recommended** behind reverse proxy | Base URL used **inside the QR** for `https://…/mfa/qr-confirm/…` so phones hit the public hostname |
| `SOCKETIO_CORS_ORIGINS` | Optional | Comma-separated origins or `*`. **Dev:** unset → `*`. **Production:** unset → `PUBLIC_BASE_URL` + `MFA_QR_BASE_URL` (deduped), else `[]` (tight; set explicitly if QR Socket.IO breaks). `*` in prod logs a warning. |
| `DEV_IP_ALLOWLIST` | Optional hardening | Comma-separated client IPs or CIDRs (`10.0.0.0/8`). When set, `/dev/*` allows only those addresses (uses `client_ip`; enable `TRUST_PROXY_HEADERS` behind a trusted proxy). Complements VPN/firewall controls. |
| `DEALER_PORTAL_DB_PATH` | Optional | Default `dealer_portal.db` (dealer-managed inventory) |
| `DEALER_UPLOAD_ROOT` | Optional | Absolute or cwd-relative root for dealer photo files (default `uploads/dealer`) |
| `DEALER_UPLOAD_MAX_BYTES` | Optional | Per-file cap (default 8 MiB) |
| `RATE_LIMIT_DEALER_VIN_PER_MIN` | Optional | Default 20 (`POST /inventory/add-vin` per IP) |
| `RATE_LIMIT_DEV_LOGIN_PER_MIN` | Optional | Default 20 (`/dev/login` POST) |
| `RATE_LIMIT_DEV_REGISTER_PER_MIN` | Optional | Default 5 (`/dev/register` POST) |
| `DEV_USERS_DB_PATH` | Optional | Default `dev_users.db` (operator accounts for `/dev`) |
| `DEV_USERS_DB_ENCRYPTION_KEY` | Optional | SQLCipher on `dev_users.db` (requires `sqlcipher3`) |
| `ALLOW_DEV_PUBLIC_REGISTER` | Production self-serve `/dev/register` | Must be truthy in production to allow new dev accounts |
| `DEV_DISABLE_PUBLIC_REGISTER` | Non-production | When truthy, closes `/dev/register` locally |
| `CHAT_MAX_MESSAGE_CHARS` | Optional | Default 4000 |
| `CHAT_MAX_BODY_BYTES` | Optional | Default 65536 |
| `MAX_REQUEST_BODY_BYTES` | Optional | Max Werkzeug request size (default ≥ 9 MiB; covers JSON APIs + dealer multipart) |
| `CSP_ENFORCE` | Optional | `1`/`0`: force `Content-Security-Policy` on vs off. If unset, **on** in production, **off** in non-production (see SEC-032). |
| `CSP_REPORT_ONLY` | Optional (`1` / `true`) | `Content-Security-Policy-Report-Only` when `CSP_ENFORCE` is off (see SEC-032) |
| `RATE_LIMIT_SQLITE_PATH` | Optional | Shared SQLite file for per-IP rate limit state across multiple workers (see SEC-040) |
| `GOOGLE_CSE_API_KEY` | Optional spec search tier | Google Programmable Search JSON API key (never commit; used by `scripts/backfill_vehicle_specs.py` / `POST /dev/api/cars/<id>/spec-backfill` only when enabled) |
| `GOOGLE_CSE_ID` | With `GOOGLE_CSE_API_KEY` | Programmable Search Engine cx identifier |
| `SPEC_SEARCH_EXTRA_ALLOWED_HOSTS` | Optional | Comma-separated extra hostnames allowed for follow-up HTTP GET after CSE (default: `fueleconomy.gov`, `epa.gov` only) |
| `SPEC_BACKFILL_USE_MASTER_CATALOG` | Optional | Set `0` to skip pgvector MasterCatalog tier during spec backfill |
| `KBB_API_KEY` | Optional KBB IDWS / valuation | Cox/KBB-issued key; never commit; used only server-side by `backend/kbb_idws.py`, `POST /dev/api/cars/<id>/kbb-refresh`, optional post-scan / scripts |
| `KBB_IDWS_BASE_URL` | With `KBB_API_KEY` | Override default `https://api.kbb.com/idws` if your tenant uses a different host |
| `KBB_DEFAULT_ZIP` | KBB refresh when rows lack ZIP | Five-digit ZIP for IDWS mileage/region pricing |
| `SCANNER_POST_KBB` | Optional scanner | When `1`, run KBB refresh for VINs touched in the scan (same as `--post-kbb`) |
| `APP_ADMIN_EMAILS` | Optional | Comma-separated emails → app `users.role=admin`, Stripe billing bypass, **skip app MFA** on login/register |
| `APP_ADMIN_USERNAMES` | Optional | Comma-separated usernames (case-insensitive match) → same as `APP_ADMIN_EMAILS`; **set only in `.env`**, never commit |
| `ALLOW_STORE_ADMIN_RESCAN` | Optional | When `1`, allows **admin** users to POST re-scan from `/admin/scans` (spawns `scanner.py` subprocess; long-running) |
| `STORE_ADMIN_MIN_PHOTOS` | Optional | Merchandising rule threshold for “low photo count” (default 3) |
| `STORE_ADMIN_STALE_PRICE_DAYS` | Optional | Days without list-price change for stale heuristic (default 45) |

---

## Phase 1 — Secrets & environment

### SEC-001 — Production `SECRET_KEY`

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py` |
| **Outcome** | `FLASK_ENV=production` raises at import if `SECRET_KEY` / `FLASK_SECRET_KEY` unset. |
| **Validation** | `FLASK_ENV=production python -c "import backend.main"` → `RuntimeError`; with `SECRET_KEY` set → import succeeds. |
| **Last verified** | 2026-04-18 |

### SEC-002 — No default `admin` / `password` app user

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/db/users_db.py` |
| **Outcome** | Seeded `admin` / `password` row is **not** inserted unless `ALLOW_DEFAULT_APP_USER` is truthy (dev-only), and any legacy seeded `admin@admin.com` + `password` row is removed at init when the flag is off. |
| **Validation** | App DB init: default row absent unless explicitly opted-in (and never in production). With an existing legacy `admin/password` row present, init removes it when `ALLOW_DEFAULT_APP_USER` is unset. |
| **Last verified** | 2026-04-25 |

### SEC-003 — Admin `/dev` bootstrap password

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/db/admin_users_db.py` |
| **Outcome** | Production requires `ADMIN_PASSWORD` at startup; dev keeps `changeme` when unset. |
| **Validation** | `FLASK_ENV=production` without `ADMIN_PASSWORD` → `RuntimeError` from `init_admin_db()`. |
| **Last verified** | 2026-04-18 |

---

## Phase 2 — Identity, sessions, CSRF

### SEC-010 — Session cookie hardening

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py`, `backend/utils/runtime_env.py` |
| **Outcome** | `HttpOnly` + `SameSite=Lax`; `Secure` when production or `SESSION_COOKIE_SECURE=1`, overridable with `SESSION_COOKIE_SECURE=0` for local HTTP. |
| **Validation** | Inspect `app.config["SESSION_COOKIE_*"]` for prod vs dev; browser shows flags on session cookie after login. |
| **Last verified** | 2026-04-18 |

### SEC-011 — CSRF for mutating routes

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/utils/csrf.py`, `backend/main.py` (incl. `POST /api/session/listings-geo`), `backend/dev/routes.py`, `backend/dev/console.py`, templates (incl. `car.html`), `frontend/static/{dev.js,listings.js,car_chat.js,dev_console.js,main.js}` |
| **Outcome** | Form POSTs use hidden `csrf_token` (incl. `POST /logout`); JSON / DELETE use `X-CSRF-Token` (same session token). `POST /api/cars/<id>/save` and `POST /api/session/listings-geo` use the same header check in `_csrf_mutating_requests`; `validate_csrf_header` accepts ignored `*args`/`**kwargs` so legacy `validate_csrf_header(request)` cannot raise `TypeError`. |
| **Validation** | Replay POST without token → 403; with token from same session → success; `python -m pytest backend/tests/test_app_security_basics.py -q` (logout 403/405). |
| **Last verified** | 2026-05-06 |

### SEC-060 — App `/logout` POST + CSRF

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py` (`logout_page`, `before_request` CSRF for `logout_page`); `frontend/templates/_app_logout.html` + nav includes; MFA (`mfa_choose`, `mfa_verify`, `mfa_setup`, `mfa_qr_wait`), billing (`billing_required`, `billing_success`), `inventory/base_inventory.html` — all use `POST` logout (no `GET` links). |
| **Outcome** | `POST /logout` only, with `csrf_token` in form. `GET /logout` returns **405**. Prevents cross-site “logout” image tricks. |
| **Validation** | `GET /logout` → 405; `POST` without valid CSRF → 403. `python -m pytest tests/test_app_security_basics.py`. |
| **Last verified** | 2026-04-28 |

### SEC-012 — Registration safety

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py`, `backend/utils/registration_validation.py`, `frontend/templates/register.html` |
| **Outcome** | Min password length (8+); duplicate username/email → friendly message; CSRF on POST. |
| **Validation** | Duplicate register → 200 with error text, no 500; password `short` → validation error. |
| **Last verified** | 2026-04-18 |

### SEC-013 — Route authorization model documented + enforced

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | This document — **Authorization model** table; `backend/main.py` + `backend/dealer_portal.py` for `/inventory` and `/dealer-uploads` enforcement. |
| **Outcome** | Explicit matrix; code matches “public listings + locked-down `/dev`” + dealer **My inventory** when logged in; general vs dealer auth entry points (`/login` vs `/dealer/login`). |
| **Validation** | Review table vs `backend/main.py`, `backend/dev_routes.py`, and `backend/dealer_portal.py` route list. |
| **Last verified** | 2026-04-25 |

### SEC-014 — Mandatory MFA (TOTP) for all accounts

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py`, `backend/dev_routes.py`, `backend/db/users_db.py`, `backend/db/admin_users_db.py`, templates under `frontend/templates/` |
| **Outcome** | All accounts except **`role=admin` (app admin)** require a second factor after password **login** on the **app**; admins skip MFA. **Email** is sent from **`/mfa/choose`** (or setup) via **Resend** (preferred) or SMTP; session-stored OTP for email. **SMS 2FA is not supported.** **TOTP** is optional for non-admins. The **`/dev` operator** surface uses **password only** (no 2FA on `/dev` routes). |
| **Validation** | `python -m pytest tests/test_mfa_totp.py tests/test_billing_gate.py tests/test_mfa_action_log.py -q`; manual: set `RESEND_API_KEY` + `RESEND_FROM` and/or Twilio Verify envs. |
| **Last verified** | 2026-04-25 |

### SEC-063 — Phone QR sign-in (Segno, Redis, Flask-SocketIO)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/mfa_qr.py`, `backend/utils/mfa_qr_store.py`, `backend/utils/qr_segno.py`, `backend/main.py` (MFA choose/verify, `/mfa/qr` TOTP PNG, Socket.IO via `_socketio_cors_allowed_origins`), `run.py`, `frontend/templates/mfa_choose.html`, `frontend/templates/mfa_qr_*.html`, `requirements.txt` |
| **Outcome** | Optional second factor: user picks **phone QR** on `/mfa/choose`; **Redis** (or in-process in non-prod) stores a short-lived `mfa_qr:attempt:*` with `user_id` + `mfa_intent`; **Segno** renders a PNG of `PUBLIC_BASE_URL`/`MFA_QR_BASE_URL` + `/mfa/qr-confirm/<token>`; phone approves with **per-view `ap_nonce`**; **Flask-SocketIO** `mfa_qr_approved` notifies the browser; `POST /mfa/qr/complete` (CSRF) + `mfa_qr_consume_approved` finalizes the app session. TOTP-enrollment QRs also use **Segno** (no `qrcode` dep). **Production** Socket.IO CORS does not default to `*` (see `SOCKETIO_CORS_ORIGINS` / `PUBLIC_BASE_URL`). |
| **Validation** | `python -m pytest tests/test_mfa_qr.py -q`; set `REDIS_URL` in production; run `python run.py` and walk scan + approve; if Socket.IO fails in prod after upgrade, set explicit `SOCKETIO_CORS_ORIGINS`. |
| **Last verified** | 2026-05-06 |

### SEC-062 — Resend (email) for MFA; SMS removed

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/utils/mfa_delivery.py`, `backend/main.py`, `backend/dev_routes.py`, `requirements.txt` |
| **Outcome** | **Resend** (or SMTP) for email codes only; **no SMS** MFA (Twilio / Verify / Programmable SMS code paths removed; `mfa_phone` column may remain unused in DB). |
| **Validation** | `python -m pytest -q` |
| **Last verified** | 2026-04-25 |

### SEC-061 — MFA action audit log (2FA pipeline)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/utils/mfa_action_log.py`, `backend/utils/mfa_delivery.py`, `backend/main.py`, `backend/dev_routes.py` |
| **Outcome** | Operator can tail JSONL at `MFA_ACTION_LOG_PATH` and/or process logger `mfa_action` (INFO) to see `login.mfa_start`, `mfa_choose.*`, `delivery.email` (e.g. `resend_sent`, `smtp_sent`), and `mfa_verify.*` — **never** the OTP. |
| **Validation** | `python -m pytest tests/test_mfa_action_log.py tests/test_mfa_totp.py -q` |
| **Last verified** | 2026-04-25 |

### SEC-055 — App user session + dealer `/inventory` (separate DB, uploads)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py` (login/register session, `/logout`, CSRF branch), `backend/db/users_db.py` (`get_user_by_login`, `save_user` returns id), `backend/dealer_portal.py`, `backend/db/dealer_portal_db.py`, `backend/utils/dealer_vin_prefill.py`, `frontend/templates/dealer_inventory.html`, nav in `dashboard.html` / `listings.html` / `car.html` |
| **Outcome** | Successful `/login` and `/register` set `session['user_id']` + `session['username']`; `POST /logout` clears session (see SEC-060). Dealer inventory lives in `dealer_portal.db` (path `DEALER_PORTAL_DB_PATH`). Mutating dealer routes validate CSRF form token. VIN add is rate-limited per IP (`RATE_LIMIT_DEALER_VIN_PER_MIN`, default 20/min). Photo uploads: MIME allow-list, max bytes (`DEALER_UPLOAD_MAX_BYTES`), bounded file count; gallery URLs stored as JSON on the vehicle row; files on disk under `DEALER_UPLOAD_ROOT` (default `uploads/dealer/`). |
| **Validation** | `python -m pytest tests/test_dealer_portal.py`; replay `POST /inventory/add-vin` without `csrf_token` → 403; logged-out `GET /inventory` → redirect to `/login`. |
| **Last verified** | 2026-04-21 |

### SEC-059 — Stripe org billing (subscription gate, webhook verification, admin bypass)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py` (register/login gates), `backend/db/users_db.py` (org + role schema), `backend/billing/stripe_billing.py`, `backend/billing/routes.py`, templates (`register.html`, billing screens) |
| **Outcome** | One Stripe subscription per org (dealership). New registrations either create an org (owner) or join via invite; non-admin users require an active org subscription to access paid surfaces. Stripe webhook is signature-verified and is the only source of truth for subscription activation. Admin users bypass Stripe and gates via env-driven bootstrap (no hard-coded accounts). Billing templates expose only org display name and navigation links — no Stripe secrets. |
| **Validation** | `python -m pytest backend/tests/test_billing_gate.py -q` (gates + `test_stripe_webhook_rejects_invalid_signature` + disabled webhook 404). Manual spot-check: `rg -n 'STRIPE_SECRET|STRIPE_WEBHOOK|sk_live|sk_test' frontend/templates` → no secret literals in templates. |
| **Last verified** | 2026-05-06 |

### SEC-065 — Env-only app admin usernames (`APP_ADMIN_USERNAMES`)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/utils/roles.py` (`admin_usernames`, `username_is_admin`, `account_has_env_admin_privilege`), `backend/db/users_db.py` (`_apply_env_admin_privileges`, `sync_env_admin_user_row`), `backend/main.py`, `backend/dealer/routes.py`, `backend/tests/test_billing_gate.py` |
| **Outcome** | Operators set comma-separated usernames in `.env` only (`APP_ADMIN_USERNAMES`); matching `users` rows get `role=admin`, TOTP cleared, MFA skipped on login — same as `APP_ADMIN_EMAILS`. Startup migration + per-login sync cover accounts created before env updates. |
| **Validation** | `python -m pytest backend/tests/test_billing_gate.py` |
| **Last verified** | 2026-04-29 |

### SEC-064 — Runtime SQLite not tracked in git

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `.gitignore` (root app DB paths, local Chroma under `backend/data/chroma/`, pipeline outputs under `backend/data/oem/`); `git rm --cached` for previously tracked `users.db`, `inventory.db`, `dev_users.db`, `incomplete_listings.db`, `dealer_portal.db`, `backend/database.db` |
| **Outcome** | Local/ops database files and generated embedding stores stay out of version control; reference data under `data/vehicle_reference/` and similar **remain** explicitly tracked. |
| **Validation** | `git ls-files '*.db'` at repo root — no runtime app DBs; `git check-ignore -v backend/data/chroma/` confirms ignore when present locally; `git ls-files | grep -E '^csv_out/|backend/dictionary/csv_out/|csv_out_cleaned_main/|csv_out_rejected_main/'` → empty after excluding listing/options trees. |
| **Last verified** | 2026-04-29 |

---

## Phase 3 — Input validation & stability

### SEC-020 — Safe numeric query params (`/search`)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/hybrid_inventory_search.py` |
| **Outcome** | Non-numeric `max_price`, `max_mileage`, `radius` → `None` filters, no exception. |
| **Validation** | `/search?max_price=abc&max_mileage=xx` → 200, no traceback in logs. |
| **Last verified** | 2026-04-18 |

### SEC-061 — Max request / JSON size

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py` (`app.config["MAX_CONTENT_LENGTH"]`, `MAX_REQUEST_BODY_BYTES`); `api_search_smart` content-length check vs `CHAT_MAX_BODY_BYTES` |
| **Outcome** | Global Werkzeug cap (default max(9 MiB, 2× `CHAT_MAX_BODY_BYTES`)) to reduce oversized POST DoS. `/api/search/smart` returns 413 for bodies over `CHAT_MAX_BODY_BYTES` (aligned with chat). |
| **Validation** | `python -m pytest tests/test_app_security_basics.py::test_smart_search_payload_too_large`. |
| **Last verified** | 2026-04-22 |

### SEC-021 — Admin `next` redirect hardening

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/dev_routes.py` `_safe_dev_next_url` |
| **Outcome** | Normalized path must be `/dev` or under `/dev/`; rejects `//`, `\`, and `..` escapes. |
| **Validation** | `next=/dev/../../login` → dashboard; `next=/dev/api/status` → allowed. |
| **Last verified** | 2026-04-18 |

---

## Phase 4 — Frontend XSS & CSP

### SEC-030 — Listings grid HTML safety

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `frontend/static/main.js` |
| **Outcome** | HTML-escape text; http(s)-only image URLs in CSS. |
| **Validation** | Malicious `title` in JSON renders as text. |
| **Last verified** | 2026-04-18 |

### SEC-031 — Audit remaining `innerHTML` sinks

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `frontend/static/dev.js` (incomplete grid + status panel), `dev_console.js` (escaped attrs), `car.html` highlights (existing `esc`) |
| **Outcome** | Dev incomplete cards: `escHtml` on tags and fields; http(s) CSS URLs; numeric `data-car-id`; status paths escaped. |
| **Validation** | Grep `innerHTML` — each site reviewed; logs use `escHtml` per line. |
| **Last verified** | 2026-04-18 |

### SEC-032 — Content-Security-Policy (enforce + report-only)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py` (`CSP_ENFORCE`, `CSP_REPORT_ONLY`, per-request nonce, `g.csp_nonce`), all app templates with `<script>` (listings, car, dashboard, auth, `dev*`, `dev_manifest`, `_app_logout.html` partial) |
| **Outcome** | Enforced `Content-Security-Policy` in production by default; per-request `script-src` **nonce** on all script elements (incl. `type="application/json"` blobs). `style-src` includes `unsafe-inline` for existing `style=""` until migrated to classes. `CSP_ENFORCE=0` / `1` overrides env default; if enforcement is off, `CSP_REPORT_ONLY=1` still sends report-only policy. |
| **Validation** | `CSP_ENFORCE=1` → `GET /login` includes `Content-Security-Policy` with `nonce-`; public pages and `/dev` load. `python -m pytest tests/test_app_security_basics.py`. |
| **Last verified** | 2026-04-22 |

---

## Phase 5 — APIs, abuse, LLM

### SEC-040 — `/api/search/smart` abuse controls

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py`, `backend/utils/ip_rate_limit.py` |
| **Outcome** | Per-IP sliding window (default 90/min, env-tunable). Optional `RATE_LIMIT_SQLITE_PATH` shares counters across gunicorn/uwsgi workers on the same host (SQLite WAL + busy timeout). For multi-node, use a reverse proxy or Redis. |
| **Validation** | Burst > limit → HTTP 429 JSON `rate_limited`; with `RATE_LIMIT_SQLITE_PATH` set, `python -m pytest tests/test_app_security_basics.py::test_ip_rate_limit_sqlite_shared`. |
| **Last verified** | 2026-04-22 |

### SEC-041 — `/api/car/<id>/chat` controls

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py`, `frontend/templates/car.html` (chat disclosure copy) |
| **Outcome** | Per IP+car rate limit (default 40/min); max message length; max JSON body bytes. User messages remain **untrusted** (prompt injection); model output is advisory only. Car page states AI answers may be wrong and must not be used for VIN verification, pricing, financing, or compliance. |
| **Validation** | Oversized body → 413; long message → `message_too_long`; flood → 429; car template includes non-authoritative AI disclaimer adjacent to chat. |
| **Last verified** | 2026-05-06 |

### SEC-042 — LLM key & data exfiltration review

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/ai_agent.py`, `llm/` |
| **Outcome** | API keys read from environment only (e.g. `OPENAI_API_KEY`); no hardcoded secrets in repo from review. |
| **Validation** | `rg` for key-like literals in `backend/ai_agent.py` / `llm/` — none committed. |
| **Last verified** | 2026-04-18 |

### SEC-058 — Ollama LLaVA (listing gallery + interior)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/vision/ollama_llava.py` |
| **Outcome** | Vision calls go to operator-configured `OLLAMA_HOST` (default loopback). Listing image URLs are fetched with browser-like headers and the vehicle’s VDP URL as `Referer` when `_detail_url` is available (avoids many dealer CDN 403s on unauthenticated fetches). Gallery filter uses URL heuristics before LLM (F&I / VPP / plan-overview path substrings) and **lot-score demotion** for F&I-like URL tokens. **Thin bar** dimensions auto-drop as `marketing_strip` and override a bad `keep`. **No** auto pixel-triage of portrait “flyers” (that mis-tagged real 3:4/4:5 lot photos). `warranty_flyer_page` and technical rows are excluded from empty-set fallback. Prompts and per-image user triage text stress **landscape** and text-heavy F&I / VPP slides. No secrets in prompts. |
| **Validation** | `python -m pytest tests/test_ollama_llava.py`. |
| **Last verified** | 2026-04-22 |

### SEC-043 — Trusted client IP (rate limits, `X-Forwarded-For`)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/utils/client_ip.py`, `backend/main.py`, `backend/dev_routes.py` |
| **Outcome** | `X-Forwarded-For` is honored only when `TRUST_PROXY_HEADERS` is truthy; otherwise `request.remote_addr` is used for smart search, car chat, `/login`, `/register`, and `/dev` auth rate limits. |
| **Validation** | `python -m pytest tests/test_client_ip.py`. |
| **Last verified** | 2026-04-18 |

### SEC-056 — KBB IDWS API key & outbound valuation

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/kbb_idws.py`, `backend/dev_routes.py` (`POST /dev/api/cars/<id>/kbb-refresh`), `backend/scanner_post_pipeline.py`, `scanner.py`, `scripts/fetch_kbb_for_inventory.py` |
| **Outcome** | `KBB_API_KEY` (and related `KBB_*` tuning vars) are read from the environment only; outbound calls use HTTPS to the configured IDWS base URL; the key is not logged or returned on public routes. Dev refresh requires an existing `/dev` admin session plus CSRF header (same as other `/dev/api/*` POST routes). |
| **Validation** | `rg "KBB_API_KEY" -g'*.py'` in repo shows no hardcoded secret literals; `python -m pytest tests/test_kbb_idws.py`. |
| **Last verified** | 2026-04-20 |

---

## Phase 6 — Dev / operator tooling

### SEC-050 — Vector reindex errors visible

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/dev_routes.py` |
| **Outcome** | `logger.exception` on pgvector reindex failure (no silent `pass`). |
| **Validation** | Force failure in `reindex_all` → stack trace in server logs. |
| **Last verified** | 2026-04-18 |

### SEC-051 — `api/audit-last-scrape` doc vs behavior

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/dev_routes.py` docstring |
| **Outcome** | Doc states admin session is required (matches `before_request`). |
| **Validation** | Anonymous `GET /dev/api/audit-last-scrape` → 401 JSON or redirect to login. |
| **Last verified** | 2026-04-18 |

### SEC-052 — Dev operators in `dev_users.db` (split from `users.db`)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/db/dev_users_sqlite.py`, `backend/db/admin_users_db.py`, `backend/main.py` (`init_admin_db` order unchanged) |
| **Outcome** | Table `admin_users` for `/dev` lives only in `dev_users.db` (path from `DEV_USERS_DB_PATH`). On first init, rows may be copied from a legacy plain-SQLite `users.db` `admin_users` table if the new DB is empty. Encrypted legacy DBs are skipped (operators re-bootstrap). |
| **Validation** | `FLASK_ENV=production` with `SECRET_KEY` + `ADMIN_PASSWORD` imports `backend.main` without error; dashboard status shows resolved `dev_users_db_path`. |
| **Last verified** | 2026-04-18 |

### SEC-053 — `/dev/register`, auth rate limits, POST `/dev/logout`

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/dev_routes.py` (`DEV_IP_ALLOWLIST`, `_dev_client_ip_allowed`), `backend/db/admin_users_db.py` (`save_dev_admin_user`, `dev_public_registration_allowed`), `backend/utils/registration_validation.py`, `frontend/templates/{admin_login,dev_register,dev}.html`, `frontend/static/style.css` |
| **Outcome** | Shared field validation for dev register; production self-register only if `ALLOW_DEV_PUBLIC_REGISTER`; non-production closed if `DEV_DISABLE_PUBLIC_REGISTER`; sliding-window limits on `/dev/login` and `/dev/register`; logout is POST with CSRF. Optional **`DEV_IP_ALLOWLIST`** (IPs / CIDRs, comma-separated) returns **403** for non-matching clients on all `/dev/*` (API JSON includes `reason: dev_ip_allowlist`). Operators should still use VPN, firewall, or separate admin ingress for `/dev` — this env is defense in depth. |
| **Validation** | Register with duplicate username → friendly error; login flood → 429; `GET /dev/logout` → 405; `python -m pytest backend/tests/test_dev_ip_allowlist.py -q`. |
| **Last verified** | 2026-05-06 |

### SEC-054 — Dev manifest login `next` URL allowlist

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/dev_console.py` |
| **Outcome** | After secret login, `next` must normalize to `/dev/manifest` or under `/dev/manifest/` (same pattern as SEC-021 for `/dev`). |
| **Validation** | `next=//evil.com` redirects to manifest home. |
| **Last verified** | 2026-04-18 |

### SEC-057 — Store admin (`/admin`) + optional scanner subprocess

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/dealer_admin/*`, `backend/main.py`, `backend/db/users_db.py`, `backend/db/inventory_db.py` (`scan_runs`), `backend/database.py` (upsert timestamps), `scanner.py` (scan log), `frontend/templates/admin/*`, `tests/test_store_admin.py` |
| **Outcome** | Dealer-scoped read of scraped inventory; CSRF on mutations; `internal_notes` / `marked_for_review` excluded from public `serialize_car_for_api`. Re-scan from UI requires explicit `ALLOW_STORE_ADMIN_RESCAN=1` and **admin** role; subprocess uses repo-root `scanner.py` + `sys.executable` (no `shell=True`). |
| **Validation** | `python -m pytest tests/test_store_admin.py`; anonymous `GET /admin/` → 302 to `/login`. |
| **Last verified** | 2026-04-20 |

### SEC-063 — Dealership discovery pipeline (Overpass / DDG HTTP, new CLI)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/discovery/` (`candidate.py`, `pipeline.py`, `merge.py`, `normalize.py`, `osm.py`, `web.py`, `cli.py`, `zcta_gazetteer.py`, `dmv/`), `scripts/discover_dealerships.py`, `backend/db/dealerships_db.py` (`upsert_discovery_row`), `docs/discovery/DMV_SOURCES.md` |
| **Outcome** | Outbound HTTP only in `backend/discovery/osm.py` (Overpass API, timeout default 90 s, configurable) and `backend/discovery/web.py` (DuckDuckGo Instant Answer JSON, timeout ≤ 15 s). Both use `requests` with bounded timeouts; no `shell=True`. ZIP/radius validated before use; Overpass POST body is built server-side from numeric bbox only. OSM/DDG responses parsed defensively. DMV tier reads **local CSV** paths from env / `data/discovery/dmv/<STATE>/` (operator-supplied files). ZCTA centroid tier reads **local** pipe-delimited gazetteer (`backend/ZIPs/*.txt` or `DISCOVERY_ZCTA_GAZETTEER` / `--zcta-gazetteer`); no outbound HTTP for that file. CLI defaults to seed-ZIP-only rows after enrichment unless `--allow-adjacent-zips`. CLI (`scripts/discover_dealerships.py`) validates `--zip` (5-digit), `--radius` (0–500 mi), optional `--dmv-state`; `--persist` uses parameterized SQL via `upsert_discovery_row`. No new Flask routes; operator-only CLI. |
| **Validation** | `python -m pytest tests/test_dealership_discovery.py` (mocked HTTP). Invalid ZIP raises before network; Overpass failure returns empty OSM list without crashing; DDG skips aggregator URLs. |
| **Last verified** | 2026-05-02 |

### SEC-066 — Scanner failure HAR traces (`workspace/debug/`)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/scanner/cli.py` (`_capture_scanner_failure_har`, `SCANNER_FAILURE_HAR`), `.gitignore` (`workspace/debug/`) |
| **Outcome** | When a **known inventory provider** (`dealer_dot_com`, `dealer_on`) yields **zero vehicles**, the scanner may write a Playwright **HAR** under `workspace/debug/fail_<dealer_id>_<epoch>.har` for operator diagnosis. HAR files can contain **cookies, Authorization headers, and URL query tokens** — treat as **sensitive**, do not commit, and restrict filesystem permissions in shared environments. Disabled with `SCANNER_FAILURE_HAR=0`. |
| **Validation** | `.gitignore` contains `workspace/debug/`; `python -m pytest backend/tests/test_scanner_intercept_filter.py -q` passes; manual optional: run scanner against a dealer forced to 0 rows with `SCANNER_FAILURE_HAR=1` and confirm a `.har` appears under `workspace/debug/` when play succeeds. |
| **Last verified** | 2026-05-02 |

### SEC-064 — Post-scan listing gap fill (operator inventory DB URLs + DDG Instant Answer)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/listing_gap_fill.py`, `backend/scanner_post_pipeline.py` (`run_listing_gap_fill_stage`), `scanner.py` (`--post-listing-gap-fill` / `SCANNER_POST_LISTING_GAP_FILL`), `backend/utils/vdp_spec_parse.py` (`parse_condition_from_listing_html`) |
| **Outcome** | After a scan, optionally backfill incomplete listing rows for VINs touched in that run: tier 1 reuses existing EPA/vPIC structured backfill; tier 2 fetches each row’s stored HTTPS `source_url` via `requests` then optional **sync Playwright** Chromium when the HTML shell looks thin; **condition** is parsed only from page HTML/JSON-LD (never from DDG). Tier 3 uses **DuckDuckGo Instant Answer** (`api.duckduckgo.com`, bounded timeout) only for residual mechanical fields (transmission/drivetrain/fuel_type). Provenance merged into `cars.spec_source_json`. No new Flask routes; operator CLI / batch only. Max rows per run capped via `SCANNER_POST_LISTING_GAP_FILL_MAX` (default 400). DDG can be disabled with `LISTING_GAP_FILL_ALLOW_DDG=0`. |
| **Validation** | `python -m pytest tests/test_listing_gap_fill.py`; manual: `SCANNER_POST_LISTING_GAP_FILL=1` or `--post-listing-gap-fill` after a scan with `SCANNER_POST_REPAIR=1` recommended so repair runs first. |
| **Last verified** | 2026-04-29 |

---

## Changelog

| Date (UTC) | Change |
|------------|--------|
| 2026-04-18 | Initial master list. SEC-030 marked **Done** (listings `renderCarGrid` escaping + http(s) images). |
| 2026-04-18 | Added governance: Cursor rules (`.cursor/rules/`), project skill (`.cursor/skills/security-master-todo/`). |
| 2026-04-18 | **Security pass:** SEC-001–003, 010–013, 020–021, 031, 040–042, 050–051 **Done**. Implemented: prod secrets, gated default users, session cookies, CSRF, registration validation, safe search numerics, safe `/dev` redirects, rate limits + chat limits, vector reindex logging, dev XSS hardening, env table. Removed stray invalid `backend/dashboard.py`. |
| 2026-04-18 | **CSP prep (SEC-032):** listings JSON boot scripts; `car_page.js` for car gallery + history; car styles → `style.css`; report-only CSP via `CSP_REPORT_ONLY=1` in `backend/main.py`. |
| 2026-04-18 | **SEC-043, 052–054:** trusted-proxy gated client IP; `dev_users.db` for `/dev` operators + legacy migration; `/dev/register` + rate limits + POST logout; manifest `next` allowlist. App `/login` and `/register` POST rate limits; `registration_validation` shared helper. |
| 2026-04-18 | Documented optional env vars for vehicle spec backfill (`GOOGLE_CSE_*`, `SPEC_SEARCH_EXTRA_ALLOWED_HOSTS`, `SPEC_BACKFILL_USE_MASTER_CATALOG`). `POST /dev/api/cars/<id>/spec-backfill` is admin+CSRF like other `/dev/api/*` JSON routes. |
| 2026-04-20 | **SEC-056:** KBB IDWS — env-only `KBB_API_KEY`, HTTPS client, dev `kbb-refresh` route, optional `SCANNER_POST_KBB` / `--post-kbb`; env table + **SEC-013** surface list updated. |
| 2026-04-20 | **SEC-057:** Store admin at `/admin` (session + dealer scope + CSRF); `scan_runs` table; optional `ALLOW_STORE_ADMIN_RESCAN` gated scanner subprocess; **SEC-013** + env quick reference updated. |
| 2026-04-21 | **SEC-055:** App login/register now establish a signed Flask session; `/logout` clears it. Dealer **My inventory** at `/inventory` uses separate `dealer_portal.db`, CSRF on dealer POSTs, per-IP VIN-add rate limit, and validated image uploads + per-user file access under `/dealer-uploads/...`. **SEC-013** authorization table updated. |
| 2026-04-22 | **SEC-058:** Documented Ollama LLaVA gallery/interior path; stricter listing-image prompt, KBB/guide URL pre-drop, dealer-lot sort; validation via `tests/test_ollama_llava.py`. Gallery keep requires explicit model `keep: true` plus an allowed category. |
| 2026-04-22 | **SEC-058:** Category synonyms (`vehicle`, `lot_photo`, …) map to canonical keep classes; `SCANNER_GALLERY_VISION_FALLBACK_ON_EMPTY` (default on) restores lot-ordered URLs when the model drops every loadable image, excluding fetch/blank technical failures. |
| 2026-04-22 | **SEC-058:** F&I / VPP / warranty URL pre-drop; `marketing_strip` (dimension-based banner) drops or overrides mislabeled vision keeps; that category and technical failures are excluded from fallback. |
| 2026-04-22 | **SEC-058:** Gallery image HTTP fetch uses browser-like User-Agent and optional VDP `Referer` (from `page_referer` / `_detail_url`) so dealer CDNs do not 403 “hotlinked” lot photos; additional `warranty_flyer_page` triage for portrait F&I plan infographics. |
| 2026-04-22 | **SEC-058:** Removed automatic **portrait** “warranty flyer” pixel triage (it mis-classified real 3:4/4:5 lot photos and missed landscape F&I art); only thin **strip** bar shapes are auto-dropped. F&I / VPP URL substrings are extended for pre-vision drop and for **lot-score demotion**; listing prompt + user triage line stress landscape VPP and text-heavy slides. |
| 2026-04-22 | **SEC-002 / SEC-032 / SEC-040 / SEC-060/061/062 (logout, bodies, dbs):** Tightened default `admin` handling (see **SEC-002** for opt-in); **SEC-032** enforced CSP in prod (per-request nonces) + `CSP_ENFORCE` override; **SEC-040** optional `RATE_LIMIT_SQLITE_PATH`; app `POST` logout+CSRF, `MAX_REQUEST_BODY_BYTES` + smart-search 413, `.gitignore` runtime DBs + `git rm --cached`. Validation: `python -m pytest` including `tests/test_app_security_basics.py`. |
| 2026-04-25 | **SEC-002:** Removed legacy seeded `admin/password` app user at init unless explicitly opted-in via `ALLOW_DEFAULT_APP_USER` (dev only). |
| 2026-04-25 | **SEC-059:** Added org-level Stripe billing gate (post-register/login), admin bypass via `APP_ADMIN_EMAILS`, and signature-verified Stripe webhook endpoint. Validated: invalid webhook signature rejected (HTTP 400), billing gate redirects non-admins to `/billing/required`, and no Stripe secrets referenced in frontend templates. |
| 2026-04-25 | **SEC-014:** Mandatory TOTP 2FA for app + dev accounts (setup + verify flows, DB fields, and route gating); added pytest coverage. |
| 2026-04-25 | **SEC-014 (follow-up):** TOTP enrollment now calls `set_user_totp` / `set_admin_totp` after QR + code confirm; stable setup secret in session until confirm or regenerate; login uses TOTP path when enabled; per-IP rate limits on verify and TOTP enroll; `tests/test_mfa_totp.py` covers enroll + relogin. |
| 2026-04-25 | **SEC-014 (follow-up):** App MFA adds **`/mfa/choose`**: email/SMS OTPs are **sent** when the user POSTs the channel; SMS needs E.164 on the form or stored `mfa_phone`. Env table: `MFA_DELIVERY_MODE`, `SMTP_*`, `TWILIO_*`, `MFA_DEV_UI_CODE`. Tests updated: `test_billing_gate.py` + `test_mfa_totp.py`. |
| 2026-04-25 | **SEC-061:** MFA JSONL + `mfa_action` logger for 2FA debugging; delivery layer logs `smtp_sent` / fallbacks / Twilio errors. Env: `MFA_ACTION_LOG_PATH` (writable path, e.g. `logs/mfa_actions.jsonl`). |
| 2026-04-25 | **SEC-061 (follow-up):** If `MFA_ACTION_LOG_PATH` is invalid, warn **once** and keep INFO logging without repeated file errors. |
| 2026-04-25 | **SEC-062 (email):** Resend for email (`RESEND_API_KEY` / `MFA_EMAIL_PROVIDER`); Twilio Verify for SMS (`TWILIO_VERIFY_SERVICE_SID`, `MFA_SMS_MODE`); session+Programmable SMS preserved for `log`/`test` and `MFA_SMS_MODE=session`. |
| 2026-04-25 | **SEC-062 (follow-up):** Removed all **SMS 2FA** (Verify + messaging); deleted `mfa_twilio_verify.py` and `twilio` dependency. |
| 2026-04-25 | **SEC-014 (follow-up):** App `role=admin` users **skip 2FA** on login and on new registration when the email is in the admin list; `tests/test_billing_gate.py` updated. |
| 2026-04-25 | **SEC-013 / auth entry points:** General `/register` no longer requires an organization; `ROLE_GENERAL` for non-admin. Dealership flows use `/dealer/register` and `/dealer/login` (with org for non-admin); `session['mfa_intent']` routes post-2FA to `/listings` (general) or dealer inventory. Stripe gate applies only when `org_id` is set. Added `delete_user_by_email` + `dealer_vehicles` cleanup. |
| 2026-04-25 | **SQLite (users.db):** Longer default connect/busy timeout; `save_user` hashes password before `connect` and retries on `SQLITE_BUSY`; `check_user` no longer runs bcrypt while the first connection is open. Env: `USERS_DB_CONNECT_TIMEOUT_S`, `USERS_DB_BUSY_TIMEOUT_MS`. |
| 2026-04-25 | **SEC-063:** Phone **QR 2FA** (Segno PNG, Redis-bounded attempt IDs, Socket.IO to notify the desktop, CSRF on finalize); `REDIS_URL` for production; **SEC-013** + env table updated. |
| 2026-04-25 | **`/dev` 2FA removed:** `/dev` operator login is password-only; `/dev/mfa/*` and dev MFA templates **removed**; **SEC-013** and env rate-limit rows updated. |
| 2026-04-25 | **Follow-up:** `GET/POST /dev/mfa/verify|setup` and `GET /dev/mfa/qr` redirect to `/dev/` or `/dev/login?next=/dev/` so old tabs/bookmarks do not 500. |
| 2026-04-28 | **SEC-060:** Replaced remaining **GET** `/logout` links (MFA, billing, dealer inventory nav) with `POST` via `_app_logout.html`; optional button label for QR wait **Cancel**. `tests/test_mfa_totp.py` uses `POST /logout` + CSRF. Validation: `python -m pytest tests/` |
| 2026-04-29 | **SEC-063:** Tiered dealership discovery (DMV CSV pilot `NC`, Overpass `shop=car`/`amenity=car_dealer`, DDG instant JSON URL gap-fill); merge/dedupe + CLI `scripts/discover_dealerships.py`. Validation: `python -m pytest tests/test_dealership_discovery.py`. |
| 2026-05-02 | **SEC-063 (follow-up):** ZCTA gazetteer internal-point centroids (`backend/ZIPs/` or `DISCOVERY_ZCTA_GAZETTEER`); CLI `--allow-adjacent-zips` / `--zcta-gazetteer`; seed-ZIP filter after enrichment. Validation: `python -m pytest backend/tests/test_dealership_discovery.py`. |
| 2026-04-29 | **SEC-064:** Post-scan listing gap fill (`listing_gap_fill.py`): EPA/vPIC → listing-page fetch (`requests` + optional Playwright) → DDG Instant Answer for mechanical fields only; condition from listing HTML; `scanner.py --post-listing-gap-fill`. Validation: `python -m pytest tests/test_listing_gap_fill.py`. |
| 2026-04-29 | **SEC-065:** Env-only operator bootstrap: `APP_ADMIN_USERNAMES` (comma-separated) mirrors `APP_ADMIN_EMAILS` for `users.role=admin`, MFA disabled (`totp` cleared), applied at `init_users_db` and on login sync — **no credentials in source**. Validation: `python -m pytest backend/tests/test_billing_gate.py`. |
| 2026-04-29 | **SEC-065 (follow-up):** Login POST trims identifiers/password; `python -m backend.scripts.app_users_status` lists `users.db` rows (no secrets); gated `ALLOW_LOCAL_PASSWORD_RESET=1` + non-prod password reset via `python -m backend.scripts.reset_app_user_password`. |
| 2026-04-29 | **SEC-064 (pre-commit audit):** `.gitignore` — `backend/data/chroma/`, `backend/data/oem/`, scraper scratch JSON, `backend/data/review_queue.jsonl`; stop ignoring `backend/package.json` / `package-lock.json` (reproducible npm); restored accidental working-tree wipes (`csv_out/`); reconciled index with `backend/dealer/`, `backend/auth/mfa.py`, scanner package + root `scanner.py` shim; no `.env` or live API keys staged. Validation: `git ls-files .env` empty; staged diff grep for `sk_live_`, `AKIA`, `ghp_` → none; `python -m pytest backend/tests/test_app_security_basics.py backend/tests/test_scanner_intercept_filter.py backend/tests/test_listing_gap_fill.py backend/tests/test_mfa_totp.py -q`. |
| 2026-04-29 | **SEC-064 (follow-up):** Listing/options scrapes (`csv_out*` at repo root and under `backend/dictionary/`) — `git rm --cached` (~7k paths), `.gitignore` patterns, broader FUSE ignore; `README.md` documents fresh-machine setup and regeneration (`import_epa_to_dictionary.py`, `car_data_scraper.py`, `reindex_vectors.py`). EPA `*_EPA.csv` dictionary files remain tracked where present. |
| 2026-04-29 | **SEC-064 (follow-up):** Also excluded `csv_out_cleaned_main/` + `csv_out_rejected_main/` (~2.3k paths) and repo-root `data/chroma/` from tracking; same regeneration story as other `csv_out*` trees. |
| 2026-05-02 | **Hygiene (docs):** Removed obsolete `backend/routes/` shims and corrected **SEC-065** scope row to `backend/dealer/routes.py` (dealer portal blueprint). |
| 2026-05-02 | **SEC-066:** Documented + gated scanner failure **HAR** capture under `workspace/debug/` (sensitive network artifacts); `.gitignore` excludes that directory. Validation: `python -m pytest backend/tests/test_scanner_intercept_filter.py -q`. |
| 2026-05-06 | **SEC-011 / SEC-013:** Car save `POST /api/cars/<id>/save` — CSRF via `_csrf_mutating_requests` + `validate_csrf_header(*args, **kwargs)` (ignores legacy `request` positional); import-time check in `main.py` fails fast if an old `csrf` module is loaded; `api_toggle_save` uses `session[\"user_id\"]` (avoids odd `.get` edge cases); `car.html` uses `X-CSRF-Token`. Validation: `python -m pytest backend/tests/test_app_security_basics.py -q`. |
| 2026-05-06 | **SEC-011 / SEC-013:** `POST /api/session/listings-geo` (CSRF header) persists last listings ZIP + radius for dashboard “Recommended for You” geo filtering; `GET /search` and `/listings` also refresh session when query params include a valid ZIP + radius. Validation: `python -m pytest backend/tests/test_app_security_basics.py -q`. |
| 2026-05-06 | **SEC-059:** Stripe billing marked **Done** — webhook signature failure → HTTP 400 JSON (`invalid_signature`); billing disabled → webhook **404**; gate tests in `backend/tests/test_billing_gate.py`; templates reviewed for Stripe secret leakage. **SEC-063 / SEC-053:** Production Socket.IO CORS no longer defaults to `*` (`_socketio_cors_allowed_origins` in `backend/main.py`). **`DEV_IP_ALLOWLIST`** optional IP/CIDR gate for `/dev` (`backend/dev/routes.py`). **SEC-041:** Car chat AI non-authoritative disclosure on `car.html`. **Cluster ops:** Registry threat-model note in `deploy/car-scanner/cronjob.yaml`. Validation: `python -m pytest backend/tests/test_billing_gate.py backend/tests/test_dev_ip_allowlist.py backend/tests/test_app_security_basics.py -q`. |

**Done items** stay in their phase table with **Status: Done** and **Last verified** — do not duplicate into a second list.

---

## Future: non-security engineering backlog

Security work **must** stay in this file until all Phase 1–6 items are `Done` or explicitly `Won't do` with rationale in Changelog.  
CSP: enforced in production (nonces; `CSP_ENFORCE=0` to disable). Use `CSP_REPORT_ONLY=1` when enforcement is off to collect additional violations. Optional follow-up: remove `style-src` `unsafe-inline` by moving inline `style=""` to CSS. For general features, consider `docs/ENGINEERING_MASTER_TODO.md` separately.

