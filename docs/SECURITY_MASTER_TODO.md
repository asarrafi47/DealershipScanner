# Security Master Todo

**Single source of truth** for security hardening in DealershipScanner.  
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
| 4     | Frontend XSS & CSP           | 0 (CSP: report-only opt-in; enforce + dev pages TBD) |
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
| `/logout` | N/A | Clears Flask session (`GET` or `POST`) |
| `/mfa/choose` | **Yes** — `mfa_pending_user_id` in session (post-password) | CSRF on **POST**; user selects **email**, **TOTP** (if enabled), or **phone QR** (when `REDIS_URL` or in-memory in non-prod; see **SEC-063**); **POST** sends the email OTP when applicable |
| `/mfa/qr-wait`, `/mfa/qr-approve-png`, `POST /mfa/qr/complete` | **Yes** — same `mfa_pending_user_id` + `mfa_qr_attempt_id` (desktop) | `POST /mfa/qr/complete` is CSRF-protected; desktop Socket.IO `mfa_qr_subscribe` requires matching session + attempt; PNG only for the session that created the attempt |
| `GET/POST /mfa/qr-confirm/<token>` | **No** (browser on phone) | **Unguessable** `token` in URL + **per-view** `ap_nonce` in POST (no session CSRF); per-IP rate limit; state in **Redis** (or in-memory in dev; production needs `REDIS_URL`) with short TTL |
| `/socket.io/*` (Flask-SocketIO) | Same-origin to app | CORS from `SOCKETIO_CORS_ORIGINS` (default `*` in dev; restrict in prod); `mfa_qr_subscribe` is session-bound before `join_room` |
| `/mfa/qr` (TOTP setup image) | **Yes** during pending TOTP setup | `otpauth://` QR as PNG (**Segno**); no public data beyond what the user’s session already holds |
| `/login`, `/register` | N/A | CSRF on POST; bcrypt passwords; per-IP rate limits on POST; **general** accounts (no org) use these; **dealership** sign-up with org uses `/dealer/register` + `/dealer/login` |
| `/dev/login`, `/dev/register` | N/A | CSRF on POST; bcrypt; per-IP rate limits; `/dev/register` gated in production |
| `/api/search/smart` | No | CSRF header (`X-CSRF-Token`) + per-IP rate limit (see SEC-043) |
| `/api/car/<id>/chat` | No | CSRF header + per-IP rate limit + max message/body size |
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
| `ALLOW_DEFAULT_APP_USER=1` | Optional in production | Allow seeded `admin`/`password` app user (discouraged) |
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
| `SOCKETIO_CORS_ORIGINS` | Optional | Comma list or `*`; default `*` (same origin in typical deployments) |
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
| `CSP_REPORT_ONLY` | Optional (`1` / `true`) | Adds `Content-Security-Policy-Report-Only` (see SEC-032) |
| `GOOGLE_CSE_API_KEY` | Optional spec search tier | Google Programmable Search JSON API key (never commit; used by `scripts/backfill_vehicle_specs.py` / `POST /dev/api/cars/<id>/spec-backfill` only when enabled) |
| `GOOGLE_CSE_ID` | With `GOOGLE_CSE_API_KEY` | Programmable Search Engine cx identifier |
| `SPEC_SEARCH_EXTRA_ALLOWED_HOSTS` | Optional | Comma-separated extra hostnames allowed for follow-up HTTP GET after CSE (default: `fueleconomy.gov`, `epa.gov` only) |
| `SPEC_BACKFILL_USE_MASTER_CATALOG` | Optional | Set `0` to skip pgvector MasterCatalog tier during spec backfill |
| `KBB_API_KEY` | Optional KBB IDWS / valuation | Cox/KBB-issued key; never commit; used only server-side by `backend/kbb_idws.py`, `POST /dev/api/cars/<id>/kbb-refresh`, optional post-scan / scripts |
| `KBB_IDWS_BASE_URL` | With `KBB_API_KEY` | Override default `https://api.kbb.com/idws` if your tenant uses a different host |
| `KBB_DEFAULT_ZIP` | KBB refresh when rows lack ZIP | Five-digit ZIP for IDWS mileage/region pricing |
| `SCANNER_POST_KBB` | Optional scanner | When `1`, run KBB refresh for VINs touched in the scan (same as `--post-kbb`) |
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

### SEC-002 — Remove or dev-gate default app user

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
| **Scope** | `backend/utils/csrf.py`, `backend/main.py`, `backend/dev_routes.py`, `backend/dev_console.py`, templates, `frontend/static/{dev.js,listings.js,car_chat.js,dev_console.js}` |
| **Outcome** | Form POSTs use hidden `csrf_token`; JSON / DELETE use `X-CSRF-Token` (same session token). |
| **Validation** | Replay POST without token → 403; with token from same session → success (see manual / test client). |
| **Last verified** | 2026-04-18 |

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
| **Scope** | `backend/mfa_qr.py`, `backend/utils/mfa_qr_store.py`, `backend/utils/qr_segno.py`, `backend/main.py` (MFA choose/verify, `/mfa/qr` TOTP PNG, Socket.IO init), `run.py`, `frontend/templates/mfa_choose.html`, `frontend/templates/mfa_qr_*.html`, `requirements.txt` |
| **Outcome** | Optional second factor: user picks **phone QR** on `/mfa/choose`; **Redis** (or in-process in non-prod) stores a short-lived `mfa_qr:attempt:*` with `user_id` + `mfa_intent`; **Segno** renders a PNG of `PUBLIC_BASE_URL`/`MFA_QR_BASE_URL` + `/mfa/qr-confirm/<token>`; phone approves with **per-view `ap_nonce`**; **Flask-SocketIO** `mfa_qr_approved` notifies the browser; `POST /mfa/qr/complete` (CSRF) + `mfa_qr_consume_approved` finalizes the app session. TOTP-enrollment QRs also use **Segno** (no `qrcode` dep). |
| **Validation** | `python -m pytest tests/test_mfa_qr.py -q`; set `REDIS_URL` in production; run `python run.py` and walk scan + approve. |
| **Last verified** | 2026-04-25 |

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
| **Outcome** | Successful `/login` and `/register` set `session['user_id']` + `session['username']`; `/logout` clears session. Dealer inventory lives in `dealer_portal.db` (path `DEALER_PORTAL_DB_PATH`). Mutating dealer routes validate CSRF form token. VIN add is rate-limited per IP (`RATE_LIMIT_DEALER_VIN_PER_MIN`, default 20/min). Photo uploads: MIME allow-list, max bytes (`DEALER_UPLOAD_MAX_BYTES`), bounded file count; gallery URLs stored as JSON on the vehicle row; files on disk under `DEALER_UPLOAD_ROOT` (default `uploads/dealer/`). |
| **Validation** | `python -m pytest tests/test_dealer_portal.py`; replay `POST /inventory/add-vin` without `csrf_token` → 403; logged-out `GET /inventory` → redirect to `/login`. |
| **Last verified** | 2026-04-21 |

### SEC-060 — Stripe org billing (subscription gate, webhook verification, admin bypass)

| Field | Content |
|-------|---------|
| **Status** | In progress |
| **Scope** | `backend/main.py` (register/login gates), `backend/db/users_db.py` (org + role schema), `backend/billing/stripe_billing.py`, `backend/billing/routes.py`, templates (`register.html`, billing screens) |
| **Outcome** | One Stripe subscription per org (dealership). New registrations either create an org (owner) or join via invite; non-admin users require an active org subscription to access paid surfaces. Stripe webhook is signature-verified and is the only source of truth for subscription activation. Admin users bypass Stripe and gates via env-driven bootstrap (no hard-coded accounts). |
| **Validation** | (1) With billing enabled, register non-admin → redirected to Stripe Checkout; no access to paid routes until webhook marks subscription active. (2) Replay webhook with invalid signature → 400/403 and no state change. (3) With `APP_ADMIN_EMAILS` containing a user email, that user registers/logs in without Stripe redirect and can access gated routes. (4) Confirm no Stripe secrets are logged or rendered to templates. |
| **Last verified** | 2026-04-25 |

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

### SEC-032 — Content-Security-Policy (optional hardening)

| Field | Content |
|-------|---------|
| **Status** | In progress |
| **Scope** | `backend/main.py`, `frontend/templates/{listings,car}.html`, `frontend/static/{main,car_page}.js`, `frontend/static/style.css` |
| **Outcome** | **Done for this pass:** (1) Listings boot data → `application/json` script tags + `main.js` parse. (2) Car gallery + history → `car_page.js`. (3) Car inline `<style>` + small inline layout styles → `style.css`; `javascript:` back link → `<button>` + JS. **Opt-in:** `CSP_REPORT_ONLY=1` sends a report-only policy (`script-src 'self' https://esm.sh`, Motion; Google Fonts on `style-src`/`font-src`/`connect-src`; dealer images `http:`/`https:`). **Still TBD:** strict enforcement / nonces for `application/json` `<script>` blobs if browsers report them; `/dev` and auth templates not yet audited for CSP. |
| **Validation** | Listings + car pages load and behave (gallery, thumbs, history list, smart search). With `CSP_REPORT_ONLY=1`, response includes `Content-Security-Policy-Report-Only`; watch browser console / reporting endpoint for remaining violations. |
| **Last verified** | 2026-04-18 |

---

## Phase 5 — APIs, abuse, LLM

### SEC-040 — `/api/search/smart` abuse controls

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py`, `backend/utils/ip_rate_limit.py` |
| **Outcome** | Per-IP sliding window (default 90/min, env-tunable). In-process only — use a reverse proxy or Redis for multi-worker deployments. |
| **Validation** | Burst > limit → HTTP 429 JSON `rate_limited`. |
| **Last verified** | 2026-04-18 |

### SEC-041 — `/api/car/<id>/chat` controls

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py` |
| **Outcome** | Per IP+car rate limit (default 40/min); max message length; max JSON body bytes. User messages remain **untrusted** (prompt injection); model output is advisory only. |
| **Validation** | Oversized body → 413; long message → `message_too_long`; flood → 429. |
| **Last verified** | 2026-04-18 |

### SEC-042 — LLM key & data exfiltration review

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/ai_agent.py`, `llm/` |
| **Outcome** | API keys read from environment only (e.g. `OPENAI_API_KEY`); no hardcoded secrets in repo from review. |
| **Validation** | `rg` for key-like literals in `backend/ai_agent.py` / `llm/` — none committed. |
| **Last verified** | 2026-04-18 |

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
| **Scope** | `backend/dev_routes.py`, `backend/db/admin_users_db.py` (`save_dev_admin_user`, `dev_public_registration_allowed`), `backend/utils/registration_validation.py`, `frontend/templates/{admin_login,dev_register,dev}.html`, `frontend/static/style.css` |
| **Outcome** | Shared field validation for dev register; production self-register only if `ALLOW_DEV_PUBLIC_REGISTER`; non-production closed if `DEV_DISABLE_PUBLIC_REGISTER`; sliding-window limits on `/dev/login` and `/dev/register`; logout is POST with CSRF. |
| **Validation** | Register with duplicate username → friendly error; login flood → 429; `GET /dev/logout` → 405. |
| **Last verified** | 2026-04-18 |

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
| 2026-04-25 | **SEC-002:** Removed legacy seeded `admin/password` app user at init unless explicitly opted-in via `ALLOW_DEFAULT_APP_USER` (dev only). |
| 2026-04-25 | **SEC-060:** Added org-level Stripe billing gate (post-register/login), admin bypass via `APP_ADMIN_EMAILS`, and signature-verified Stripe webhook endpoint. Validated: invalid webhook signature rejected (HTTP 400), billing gate redirects non-admins to `/billing/required`, and no Stripe secrets referenced in frontend templates. |
| 2026-04-25 | **SEC-014:** Mandatory TOTP 2FA for app + dev accounts (setup + verify flows, DB fields, and route gating); added pytest coverage. |
| 2026-04-25 | **SEC-014 (follow-up):** TOTP enrollment now calls `set_user_totp` / `set_admin_totp` after QR + code confirm; stable setup secret in session until confirm or regenerate; login uses TOTP path when enabled; per-IP rate limits on verify and TOTP enroll; `tests/test_mfa_totp.py` covers enroll + relogin. |
| 2026-04-25 | **SEC-014 (follow-up):** App MFA adds **`/mfa/choose`**: email/SMS OTPs are **sent** when the user POSTs the channel; SMS needs E.164 on the form or stored `mfa_phone`. Env table: `MFA_DELIVERY_MODE`, `SMTP_*`, `TWILIO_*`, `MFA_DEV_UI_CODE`. Tests updated: `test_billing_gate.py` + `test_mfa_totp.py`. |
| 2026-04-25 | **SEC-061:** MFA JSONL + `mfa_action` logger for 2FA debugging; delivery layer logs `smtp_sent` / fallbacks / Twilio errors. Env: `MFA_ACTION_LOG_PATH` (writable path, e.g. `logs/mfa_actions.jsonl`). |
| 2026-04-25 | **SEC-061 (follow-up):** If `MFA_ACTION_LOG_PATH` is invalid, warn **once** and keep INFO logging without repeated file errors. |
| 2026-04-25 | **SEC-062:** Resend for email (`RESEND_API_KEY` / `MFA_EMAIL_PROVIDER`); Twilio Verify for SMS (`TWILIO_VERIFY_SERVICE_SID`, `MFA_SMS_MODE`); session+Programmable SMS preserved for `log`/`test` and `MFA_SMS_MODE=session`. |
| 2026-04-25 | **SEC-062 (follow-up):** Removed all **SMS 2FA** (Verify + messaging); deleted `mfa_twilio_verify.py` and `twilio` dependency. |
| 2026-04-25 | **SEC-014 (follow-up):** App `role=admin` users **skip 2FA** on login and on new registration when the email is in the admin list; `tests/test_billing_gate.py` updated. |
| 2026-04-25 | **SEC-013 / auth entry points:** General `/register` no longer requires an organization; `ROLE_GENERAL` for non-admin. Dealership flows use `/dealer/register` and `/dealer/login` (with org for non-admin); `session['mfa_intent']` routes post-2FA to `/listings` (general) or dealer inventory. Stripe gate applies only when `org_id` is set. Added `delete_user_by_email` + `dealer_vehicles` cleanup. |
| 2026-04-25 | **SQLite (users.db):** Longer default connect/busy timeout; `save_user` hashes password before `connect` and retries on `SQLITE_BUSY`; `check_user` no longer runs bcrypt while the first connection is open. Env: `USERS_DB_CONNECT_TIMEOUT_S`, `USERS_DB_BUSY_TIMEOUT_MS`. |
| 2026-04-25 | **SEC-063:** Phone **QR 2FA** (Segno PNG, Redis-bounded attempt IDs, Socket.IO to notify the desktop, CSRF on finalize); `REDIS_URL` for production; **SEC-013** + env table updated. |
| 2026-04-25 | **`/dev` 2FA removed:** `/dev` operator login is password-only; `/dev/mfa/*` and dev MFA templates **removed**; **SEC-013** and env rate-limit rows updated. |
| 2026-04-25 | **Follow-up:** `GET/POST /dev/mfa/verify|setup` and `GET /dev/mfa/qr` redirect to `/dev/` or `/dev/login?next=/dev/` so old tabs/bookmarks do not 500. |

**Done items** stay in their phase table with **Status: Done** and **Last verified** — do not duplicate into a second list.

---

## Future: non-security engineering backlog

Security work **must** stay in this file until all Phase 1–6 items are `Done` or explicitly `Won't do` with rationale in Changelog.  
**SEC-032** CSP: public listings/car paths refactored for script/style hygiene; enable `CSP_REPORT_ONLY=1` to collect violations before enforcement. Remaining: dev/admin templates, optional nonces on JSON `<script>` blobs, then `Content-Security-Policy` (enforce). For general features, consider `docs/ENGINEERING_MASTER_TODO.md` separately.
