# Security Master Todo

**Single source of truth** for security hardening in the Sarrafi Collection project.  
**Policy:** Security-related work is **not complete** until this document reflects reality: statuses, validation evidence, and the [Changelog](#changelog) are updated in the **same change** as the code or config.

---

## How to use (humans & AI)

1. **Before** implementing or reviewing security work: read this file; pick or add the relevant item IDs.
2. **During** work: keep IDs in commit messages / PR descriptions (e.g. `SEC-012`).
3. **After** work: update **Status**, **Last verified** (date), **Validation** notes if the procedure changed, and append **Changelog**.
4. **New** risks or follow-ups: add a row under the right phase; do not rely on chat-only tracking.

**Validation runbook (same PR as code):** When you change **auth / sessions**, **CSRF**, **CSP**, **LLM or chat APIs** (e.g. `POST /api/car/<id>/chat`, `POST /api/search/smart`), **rate limits**, **subprocess** (e.g. `scanner.py`, operator CLIs), or **templates/JS** that affect those surfaces, re-run the **Validation** column for every touched **SEC-xxx** row, set **Last verified** to the run date, and add a one-line **Changelog** entry. The document is the contract; chat-only “done” is not enough.

**Status values:** `Done` | `In progress` | `Blocked` | `Not started`. Closed rows may also read `Won't do` or `Retired`.

**Status tracks the security property in the code on the release branch.** Going live (a deploy, a prod variable, an owner step) is tracked separately, in the row's **Go-live** field where it has one, so a Done row can still be waiting for its release.

---

## Summary

**Open items** counts the rows in that phase whose Status is `In progress`, `Blocked` or `Not started`; **Open IDs** names them. Update this table in the same change as any row status. `backend/tests/test_security_master_todo.py` fails when the table and the rows disagree.

| Phase | Theme                        | Open items | Open IDs |
|-------|------------------------------|------------|----------|
| 1     | Secrets & environment        | 1          | SEC-110 (Blocked) |
| 2     | Identity, sessions, CSRF     | 0          | none |
| 3     | Input validation & stability | 0          | none |
| 4     | Frontend XSS & CSP           | 0          | none |
| 5     | APIs, abuse, LLM surfaces    | 1          | SEC-113 (Not started) |
| 6     | Dev / operator tooling       | 3          | SEC-096 (In progress), SEC-097 (In progress), SEC-112 (Not started) |

SEC-096 (email verification) and SEC-097 (password reset) sit in the Phase 6 section for history, although they are identity work. Moving rows between phases is left to the re-scoping pass (remediation P18B.5).

---

## Authorization model (SEC-013)

| Surface | Login required? | Protection |
|---------|-----------------|------------|
| `/search`, `/listings`, `/car/<id>` | **No** — public inventory | Normal browsing |
| `/compare` | **Yes** — `session['user_id']` | Spec comparison table; compare chat gated like car chat when billing on |
| `/dashboard`, `/` → `/login` | No (dashboard is placeholder) | Nav shows **My inventory** when `session['user_id']` is set |
| `/inventory`, `POST /inventory/*` | **Yes** — `session['user_id']` (app `users` table) | CSRF on POST; per-IP rate limit on VIN add; rows scoped to `user_id` in `dealer_portal.db` |
| `/admin/*` (store dashboard, inventory, scans, data-quality, scanner-ops) | **Yes** — same app session; **admin** role or **dealer_staff** with `dealer_id` / `dealership_registry_id` on `users` row; `/admin/data-quality` and `/admin/scanner-ops` require **site admin** (`role=admin`) | CSRF on POST; inventory rows scoped to dealer match on `cars`; internal notes never exposed on public `/car` JSON; re-scan subprocess **off** unless `ALLOW_STORE_ADMIN_RESCAN=1` and caller is **admin** |
| `/api/admin/operator/*` | **Yes** — app session + `role=admin` | CSRF `X-CSRF-Token` on mutations; mirrors legacy `/dev/api` incomplete listings + scanner/registry tools (smart-import URLs validated per **SEC-068**); car delete removes `cars` + incomplete index row |
| `/dealer-uploads/<user_id>/<vehicle_id>/<file>` | **Yes** — path `user_id` must match session | JPEG/PNG/WebP only at upload; filenames are server-generated; `send_from_directory` under upload root |
| `/logout` | N/A | `POST` only; CSRF form; clears Flask session; `GET /logout` → **405** (avoids logout CSRF) (see **SEC-060**). |
| `/mfa/*` (legacy URLs) | N/A — **2FA removed** | `GET`/`POST` on `/mfa/choose`, `/mfa/setup`, `/mfa/verify`, `/mfa/qr`, `/mfa/qr-wait`, `/mfa/qr-confirm/<token>`, `POST /mfa/qr/complete` → **302** to `/login` (logged out) or `/listings` (logged in). `/dev/mfa/*` → `/dev/` or `/dev/login` (see dev routes). |
| `/login`, `/register` | N/A | CSRF on POST; bcrypt passwords; per-IP rate limits on POST; **general** accounts (no org) use these; **dealership** sign-up with org uses `/dealer/register` + `/dealer/login` |
| `/auth/google`, `/auth/google/callback`, `/auth/apple`, `/auth/apple/callback` | N/A | Optional Google / Apple OAuth when the provider env is set (Google: `GOOGLE_OAUTH_CLIENT_ID` + `GOOGLE_OAUTH_CLIENT_SECRET`); OAuth `state` validation; per-IP rate limit on callback; provider email must be verified; auto-links an existing account only when that account's email is verified, otherwise the link waits for a password login (see **SEC-075**, **SEC-104**) |
| `/dev/login`, `/dev/register` | N/A | CSRF on POST; bcrypt; per-IP rate limits; `/dev/register` gated in production; optional `DEV_IP_ALLOWLIST` (comma IPs / CIDRs) restricts all `/dev/*` when set |
| `/api/search/smart` | No | CSRF header (`X-CSRF-Token`) + per-IP rate limit (see SEC-043); response `search_meta` trimmed to mode/counts only (**SEC-087**) |
| `POST /api/session/listings-geo` | No | CSRF header; stores last listings ZIP + radius in the Flask session (for dashboard recommendations) |
| `/car/<id>/window-sticker-preview.png`, `GET /api/cars/<id>/window-sticker-preview` | Same as car chat (SEC-072/073) | `_require_premium_feature`; PNG from stored Monroney PDF |
| `/api/car/<id>/chat` | **Yes** when `BILLING_STRIPE_ENABLED=1` (premium / org subscription / app admin); open when billing off | CSRF header + **layered** rate limits (see **SEC-041**) + max message/body size; **403** `premium_required` when billing on and unpaid. Web research (HTTP fetch only since **SEC-111**; no browser in the web image) runs only for signed-in users in production; prod web has `CAR_CHAT_WEB_RESEARCH=0` since 2026-10-08. |
| `/api/compare/chat` | Same as car chat | CSRF header + same rate limits as **SEC-041**; up to 4 `car_ids`; **403** when billing on and unpaid. |
| `/api/nearby-dealers` | Same as car chat (dealership picker) | ZIP + radius; **403** `premium_required` when billing on and unpaid. |
| `/billing/premium/success` | **Yes** | Grants `is_premium` only after Stripe Checkout Session `payment_status=paid` + metadata match (see **SEC-066**). |
| `POST /api/cars/<id>/save` | **Yes** — `session['user_id']` | CSRF header (`X-CSRF-Token`, same token as meta `csrf-token`) |
| `GET /api/auth/csrf`, `GET /api/auth/me` | No (me → 401 if anonymous) | Session cookie; CSRF token issued on `GET /api/auth/csrf` |
| `GET /api/saved-cars` | **Yes** — `session['user_id']` | Saved inventory for native clients |
| `POST /api/auth/login`, `POST /api/auth/register`, `POST /api/auth/logout` | N/A / session | CSRF header (`X-CSRF-Token`); login/register per-IP rate limits same as HTML `/login` and `/register` (**SEC-077**, **SEC-086**) |
| `GET /api/cars/<id>` | No | Public aggregate JSON (same data as `/car/<id>` template context; market intel when paid) |
| `/dev/*` (dashboard, APIs) | **Yes** — `dev_users.db` via `/dev/login` in production (**SEC-083**); app `role=admin` pass-through only if `ALLOW_APP_ADMIN_DEV_PASS_THROUGH=1` (unset on prod web since 2026-10-08; boot warns if it returns, **SEC-109**) | CSRF on forms + `X-CSRF-Token` on API; POST `/dev/logout`; scanner jobs (`POST /dev/api/test-scanner`, smart-import) require `http`/`https` public URLs (**SEC-068**); includes `POST /dev/api/cars/<id>/spec-backfill` (optional Google CSE env for search tier) |
| `/dev/manifest`, `/api/dev/*` | `DEV_CONSOLE=1` + **`DEV_CONSOLE_SECRET` required in production** (**SEC-081**) | CSRF on mutations; safe `next` under `/dev/manifest` |
| `GET /api/dealer-locator` | Public by default (registry + Google); optional `DEALER_LOCATOR_REQUIRE_LOGIN=1` gates Google tier in production (**SEC-085**) | Per-IP rate limit (default 30/min) |

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
| `USERS_DB_ENCRYPTION_KEY` | **`FLASK_ENV=production`** | SQLCipher passphrase for `users.db` (≥32 chars; requires `sqlcipher3`) |
| `ALLOW_UNENCRYPTED_USER_DB` | Dev/test only | When `1`, allows plain SQLite for credential DBs; **forbidden in production** |
| `BCRYPT_ROUNDS` | Optional | bcrypt work factor for new password hashes (default **13**, range 12–15) |
| `USERS_DB_CONNECT_TIMEOUT_S` | Optional | Default `30` (SQLite `connect` wait; reduces `database is locked` under dev reload / concurrency) |
| `USERS_DB_BUSY_TIMEOUT_MS` | Optional | Default `30000` (SQLite `busy_timeout` per query) |
| `RATE_LIMIT_SMART_SEARCH_PER_MIN` | Optional | Default 90 |
| `RATE_LIMIT_CAR_CHAT_PER_MIN` | Optional | Per **IP + car** window; default **24**/min (sliding) |
| `RATE_LIMIT_CAR_CHAT_PER_IP_PER_MIN` | Optional | Per-IP cap across all cars; default **48**/min |
| `RATE_LIMIT_CAR_CHAT_GLOBAL_PER_MIN` | Optional | Default **0** (disabled). When positive, shared cap for all `POST /api/car/*/chat` (e.g. **200** in prod; use with `RATE_LIMIT_SQLITE_PATH` for multi-worker) |
| `CAR_CHAT_WEB_RESEARCH` | Optional | Car-chat live web research. Since **SEC-111** the research itself is HTTP-only for every setting (DuckDuckGo HTML search plus direct page fetches; no browser). `auto` (default when unset): dev allows it; **production** allows it only for signed-in users. `0`/`1` = force off/on for all. Prod web: `0` since 2026-10-08 (D-SEC1 stopgap). The code still names this "Playwright" (`web_research_playwright_allowed`); the rename is a later unit. |
| `CAR_CHAT_WEB_RESEARCH_PUBLIC` | Optional (dev only) | When `1` and `CAR_CHAT_WEB_RESEARCH` is `auto`, anonymous users may trigger web research in **non-production**. Ignored in production (`car_chat_policy.py`), although production boot still logs a warning when it is set (SEC-081). |
| `WEB_RESEARCH_ALLOWED_HOSTS` | Optional | Comma-separated hostnames; when set, Brave result links must match (exact or `*.domain`). Always applies blocklist + private-IP/localhost guard. Unset = blocklist + private guard only. |
| `TRUST_PROXY_HEADERS` | Required behind a proxy (`1` / `true`) | When set, rate limits read the client from `X-Forwarded-For`: `TRUSTED_PROXY_HOPS` entries from the right, or, with `TRUSTED_PROXY_CIDRS`, the first entry from the right outside those ranges (bounded by the hop count). Unset behind a proxy collapses every per-IP limit into one shared bucket. |
| `TRUSTED_PROXY_HOPS` | Optional (default `1`) | Number of trusted proxies in front of the app (Railway edge = 1; Cloudflare → Railway = 2, prod web). Only read when `TRUST_PROXY_HEADERS` is truthy. With `TRUSTED_PROXY_CIDRS` set it bounds the walk, so prod keeps `2`. `deploy/railway/sync-vault-to-railway.sh` refuses to run without an explicit value (**SEC-110**). |
| `TRUSTED_PROXY_CIDRS` | Recommended in prod (`cloudflare`) | Comma-separated CIDRs / IPs and/or the token `cloudflare` (Cloudflare's published ranges, dated constant `CLOUDFLARE_PROXY_CIDRS_AS_OF` in `backend/utils/client_ip.py`, refresh at least quarterly). When set, a request sent straight to the origin can no longer choose its own client IP (**SEC-105**). Not yet set on prod web: N3.13 step 5, after the N3 deploy. |
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
| `DEV_IP_ALLOWLIST` | Optional hardening | Comma-separated client IPs or CIDRs (`10.0.0.0/8`). When set, `/dev/*` allows only those addresses (uses `client_ip`; enable `TRUST_PROXY_HEADERS` behind a trusted proxy). Complements VPN/firewall controls. Spoofable through a direct-to-origin request until `TRUSTED_PROXY_CIDRS` is set (**SEC-105**), and defence in depth only until an origin lock exists (**SEC-113**); prod leaves it unset until then (D-SEC5). |
| `DEALER_PORTAL_DB_PATH` | Optional | Default `dealer_portal.db` (dealer-managed inventory) |
| `DEALER_UPLOAD_ROOT` | Optional | Absolute or cwd-relative root for dealer photo files (default `uploads/dealer`) |
| `DEALER_UPLOAD_MAX_BYTES` | Optional | Per-file cap (default 8 MiB) |
| `RATE_LIMIT_DEALER_VIN_PER_MIN` | Optional | Default 20 (`POST /inventory/add-vin` per IP) |
| `RATE_LIMIT_DEV_LOGIN_PER_MIN` | Optional | Default 20 (`/dev/login` POST) |
| `RATE_LIMIT_DEV_REGISTER_PER_MIN` | Optional | Default 5 (`/dev/register` POST) |
| `DEV_USERS_DB_PATH` | Optional | Default `dev_users.db` (operator accounts for `/dev`) |
| `DEV_USERS_DB_ENCRYPTION_KEY` | **`FLASK_ENV=production`** | See `USERS_DB_ENCRYPTION_KEY` (SEC-088); same requirement for operator DB |
| `ALLOW_DEV_PUBLIC_REGISTER` | Production self-serve `/dev/register` | Must be truthy in production to allow new dev accounts |
| `DEV_DISABLE_PUBLIC_REGISTER` | Non-production | When truthy, closes `/dev/register` locally |
| `CHAT_MAX_MESSAGE_CHARS` | Optional | Default 4000 |
| `CHAT_MAX_BODY_BYTES` | Optional | Default 65536 |
| `MAX_REQUEST_BODY_BYTES` | Optional | Max Werkzeug request size (default ≥ 9 MiB; covers JSON APIs + dealer multipart) |
| `CSP_ENFORCE` | Optional | `1`/`0`: force `Content-Security-Policy` on vs off. If unset, **on** in production, **off** in non-production (see SEC-032). |
| `CSP_REPORT_ONLY` | Optional (`1` / `true`) | `Content-Security-Policy-Report-Only` when `CSP_ENFORCE` is off (see SEC-032) |
| `RATE_LIMIT_SQLITE_PATH` | Optional | Shared SQLite file for per-IP rate limit state across multiple workers (see SEC-040) |
| `GOOGLE_CSE_API_KEY` | Optional spec search tier | Google Programmable Search JSON API key (never commit; used by `POST /dev/api/cars/<id>/spec-backfill` only when enabled) |
| `GOOGLE_CSE_ID` | With `GOOGLE_CSE_API_KEY` | Programmable Search Engine cx identifier |
| `GOOGLE_OAUTH_CLIENT_ID` | Optional Google sign-in | OAuth 2.0 web client id (never commit) |
| `GOOGLE_OAUTH_CLIENT_SECRET` | With `GOOGLE_OAUTH_CLIENT_ID` | OAuth client secret (never commit) |
| `GOOGLE_OAUTH_REDIRECT_URI` | Optional | Defaults to `/auth/google/callback` on current host; must match Google Cloud console |
| `GOOGLE_OAUTH_SHOW_BUTTON` | Optional | `1` shows “Sign in with Google” on `/login` when OAuth env is configured (default hidden) |
| `GOOGLE_OAUTH_OFFER_PREMIUM_ON_LOGIN` | Optional | `1` sends **returning** Google sign-ins (non-premium) to `/premium` after login; new Google accounts always get the offer |
| `SPEC_SEARCH_EXTRA_ALLOWED_HOSTS` | Optional | Comma-separated extra hostnames allowed for follow-up HTTP GET after CSE (default: `fueleconomy.gov`, `epa.gov` only) |
| `SPEC_BACKFILL_USE_MASTER_CATALOG` | Optional | Set `0` to skip pgvector MasterCatalog tier during spec backfill |
| `APP_ADMIN_EMAILS` | Optional | Comma-separated emails → app `users.role=admin`, Stripe billing bypass |
| `APP_ADMIN_USERNAMES` | Optional | Comma-separated usernames (case-insensitive match) → same as `APP_ADMIN_EMAILS`; **set only in `.env`**, never commit |
| `ALLOW_STORE_ADMIN_RESCAN` | Optional | When `1`, allows **admin** users to POST re-scan from `/admin/scans` (spawns `scanner.py` subprocess; long-running) |
| `STORE_ADMIN_MIN_PHOTOS` | Optional | Merchandising rule threshold for “low photo count” (default 3) |
| `STORE_ADMIN_STALE_PRICE_DAYS` | Optional | Days without list-price change for stale heuristic (default 45) |
| `DB_ADMIN_TOKEN` | **Required** for `python -m backend.scripts.db_admin` | Min 16 chars; Bearer / `X-DB-Admin-Token` on API (local `127.0.0.1` only) |
| `ALLOW_APP_ADMIN_DEV_PASS_THROUGH` | Optional | When `1` in **production**, app `role=admin` may use `/dev` without `/dev/login` (default **off**). Deleted from prod web on 2026-10-08; production boot logs a warning if it is set (**SEC-083**, **SEC-109**). The local Docker stack (`deploy/docker-compose.yml`, `deploy/load-vault-env.sh`) defaults it to `1` with `FLASK_ENV=development`, where it has no effect on the production check. |
| `RATE_LIMIT_DEALER_LOCATOR_PER_MIN` | Optional | Default 30 (`GET /api/dealer-locator` per IP) |

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

### SEC-076 — Transitive dependency security floors (`pip-audit`)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `requirements.txt`, `requirements-scanner.txt`, `scripts/security_check.sh` |
| **Outcome** | Minimum versions pin known CVE fixes for `urllib3`, `idna`, `pygments` and the other transitive floors. **2026-10-18 (remediation P15B.1, D-DEP1 (a)):** the orphan dependencies are gone (`crawl4ai`, which had no importers and pinned `lxml~=5.3`, taking its transitive `nltk` with it; `pyotp`, `segno`, `ollama`; and the floors for `langchain-core` / `langchain-openai` / `langchain-text-splitters`, `langsmith`, `starlette` and `tornado`, which nothing requires any more), the patchright browser-install instructions are removed, `lxml` is `>=6.1.0` in both requirements files (fixes PYSEC-2026-87), and `scripts/security_check.sh` runs `pip-audit` in requirements mode over both files with **no** `--ignore-vuln` (the lxml and orphan-`chromadb` ignores are removed). |
| **Go-live** | Ships with the N3 release. The `Dockerfile.web` / `Dockerfile.discovery` stage builds wait for the owner's Docker OK (N3.13 smoke). The MBP and mini dev venvs still hold lxml 5.4.0 and crawl4ai until they are rebuilt (owner). |
| **Validation** | `pip-audit --desc on -r requirements.txt -r requirements-scanner.txt` → 0 findings, no ignores (or `bash scripts/security_check.sh`); `python -m pytest backend/tests/test_requirements_hygiene.py -q`. Before P15B.1 the same audit found 9 vulnerabilities in 3 packages (crawl4ai x6, lxml 5.4.0 x2, nltk x1). |
| **Last verified** | 2026-10-18 (pip-audit run by the P15B.1 build on the requirements files merged into `phase/N3`; the dev venv here has no pip-audit) |

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

### SEC-081 — Production configuration guards

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/utils/production_security.py`, `backend/main.py` (`assert_production_security_config`), `backend/dev/console.py` |
| **Outcome** | `FLASK_ENV=production` raises at import if `DEV_CONSOLE=1` without `DEV_CONSOLE_SECRET`. Manifest console routes return **404** when console enabled in prod but secret unset (defense in depth). Logs warnings for `BILLING_STRIPE_ENABLED=0`, unset `RATE_LIMIT_SQLITE_PATH`, and `CAR_CHAT_WEB_RESEARCH_PUBLIC=1`. |
| **Validation** | `python -m pytest backend/tests/test_production_security.py -q`. |
| **Last verified** | 2026-05-26 |

### SEC-109 — Production config warnings; per-account password reset cooldown (remediation P3.6)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/utils/production_security.py` (`_warn_email_and_link_config`, pass-through guard in `assert_production_security_config`), `backend/auth/password_reset.py` (`request_password_reset`), `backend/tests/test_production_security.py`, `backend/tests/test_password_reset.py` |
| **Outcome** | In production, boot logs a warning (no refusal) when: (1) `PASSWORD_RESET_ENABLED`, `EMAIL_VERIFICATION_ENABLED` or `BILLING_STRIPE_ENABLED` is on while `PUBLIC_BASE_URL` is unset (emailed links fall back to `MFA_QR_BASE_URL` or become relative; Stripe return URLs come from the request host); (2) an email flow is on with neither `RESEND_API_KEY` nor `SMTP_HOST` set (send errors are caught and only logged); (3) `ALLOW_APP_ADMIN_DEV_PASS_THROUGH` is truthy (a guard only: prod deleted it on 2026-10-08, see SEC-083). `request_password_reset` sends at most 3 reset emails per account per hour (`forgot_pw_user:<uid>`), on top of the per-IP limit. The check runs before a token is issued, so a throttled request neither rotates the token (the link already emailed stays valid) nor changes the response (no account enumeration). Known gaps (review): other no-send setups stay silent (`MFA_DELIVERY_MODE=log` in prod, `RESEND_API_KEY` with no sender address); the attacker and the victim share the per-account bucket; without `RATE_LIMIT_SQLITE_PATH` the counter is per process and resets on redeploy. |
| **Go-live** | Ships with the N3 release. Owner: confirm `PUBLIC_BASE_URL` is set on prod web (the new warning shows it on the next boot). |
| **Validation** | `python -m pytest backend/tests/test_production_security.py backend/tests/test_password_reset.py -q` (each warning fires exactly when its condition holds and stays silent outside production; a 4th reset request within the hour sends no email and returns the same page). |
| **Last verified** | 2026-10-18 |

### SEC-110 — Vault sync refuses an unset `TRUSTED_PROXY_HOPS` (remediation P3.7)

| Field | Content |
|-------|---------|
| **Status** | Blocked |
| **Scope** | `deploy/railway/sync-vault-to-railway.sh`, `deploy/railway/README.md`, `backend/tests/test_sync_vault_guard.py` (new). On branch `unit/P3.7` (b8364d6d6); not merged into `phase/N3` yet. |
| **Outcome** | The script set `TRUSTED_PROXY_HOPS="${TRUSTED_PROXY_HOPS:-1}"`, so a run without the variable would silently reset prod web (Cloudflare, then Railway's edge: 2 since 1.4.4) to 1, and every per-IP rate limit would key on a Cloudflare address. A guard right after argument parsing, before the CLI check and every `railway` call, exits 1 unless `TRUSTED_PROXY_HOPS` is a positive integer and `RAILWAY_SERVICE` names a service; every call now passes `--service`, so nothing lands on the checkout's linked service. The README lists the current services (web, scanner-nightly, Postgres; scanner-worker and scanner-scheduler deleted 2026-10-08) and the hops rule for the Cloudflare-fronted domain (2). Follow-up nit: the guard accepts any positive integer (3, 22) even for web. |
| **Blocked on** | Remediation P3.7 acceptance: the owner confirms the live web value before merge (`railway variable list -s web --kv \| grep '^TRUSTED_PROXY_HOPS='`, expect `TRUSTED_PROXY_HOPS=2`); agents do not run `railway`. On merge: set Status Done, update the Summary (Phase 1 to 0), and add the README sentence that once `TRUSTED_PROXY_CIDRS=cloudflare` is set the hop count only bounds the walk, so it stays 2. |
| **Validation** | `bash -n deploy/railway/sync-vault-to-railway.sh`; `python -m pytest backend/tests/test_sync_vault_guard.py -q` (unset, empty or malformed hops or service refuse with zero `railway` calls, even with no CLI on PATH; the explicit hops value is the one set; every call names the service). |
| **Last verified** | 2026-10-18 (run in the `unit/P3.7` worktree) |

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
| **Outcome** | Form POSTs use hidden `csrf_token` (incl. `POST /logout`); JSON / DELETE use `X-CSRF-Token` (same session token). `POST /api/cars/<id>/save`, `POST /api/session/listings-geo`, `POST /api/auth/login`, and `POST /api/auth/logout` use the same header check in `_csrf_mutating_requests`; `validate_csrf_header` accepts ignored `*args`/`**kwargs` so legacy `validate_csrf_header(request)` cannot raise `TypeError`. |
| **Validation** | Replay POST without token → 403; with token from same session → success; bad login CSRF → redirect `session_expired` (not authenticated); `python -m pytest backend/tests/test_app_security_basics.py backend/tests/test_mobile_auth_api.py -q`. |
| **Last verified** | 2026-05-25 |

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
| **Validation** | Review table vs `backend/main.py`, `backend/dev/routes.py`, and `backend/dealer_portal.py` route list; `python -m pytest backend/tests/test_compare_and_stats.py backend/tests/test_compare_specs.py backend/tests/test_mobile_auth_api.py backend/tests/test_car_detail_api.py -q`. |
| **Last verified** | 2026-05-25 |

### SEC-077 — Mobile JSON auth API (`/api/auth/*`)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py` (`api_auth_csrf`, `api_auth_me`, `api_auth_login`, `api_auth_logout`); `_csrf_mutating_requests` |
| **Outcome** | Native clients obtain CSRF via `GET /api/auth/csrf`, then `POST /api/auth/login` with JSON credentials and `X-CSRF-Token`. Same bcrypt check and per-IP login rate limit as HTML `/login`. `GET /api/auth/me` returns user profile or 401. `POST /api/auth/logout` clears Flask session with CSRF header. No separate API keys; session cookie is the credential. |
| **Validation** | `python -m pytest backend/tests/test_mobile_auth_api.py -q`; login without CSRF → 403; invalid credentials → 401; successful login → `GET /api/auth/me` 200. |
| **Last verified** | 2026-05-25 |

### SEC-086 — Mobile registration (`POST /api/auth/register`)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/auth/app_registration.py`, `backend/main.py` (`register_page`, `api_auth_register`), `ios/.../APIClient.swift`, `AppState.swift`, `AuthHeaderBar.swift`, `SessionCookieBridge.swift`, `backend/mobile/contract.py`, `ios/docs/API_CONTRACT.md` |
| **Outcome** | iOS native sign-up calls `POST /api/auth/register`, which writes the same `users.db` row as HTML `/register` via shared `register_general_app_user`. Session established with `_finalize_app_session`; saved cars and profile work on web after login with same credentials. `SessionCookieBridge.syncFromWebView()` supports legacy WKWebView auth if needed. |
| **Validation** | `python -m pytest backend/tests/test_mobile_auth_register.py backend/tests/test_registration_email.py backend/tests/test_mobile_auth_api.py -q`. |
| **Last verified** | 2026-05-26 |

### SEC-078 — iOS native client hardening (WebView + cookies + ATS)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `ios/SarrafiCars/SarrafiCars/Web/WebView.swift`, `Networking/APIClient.swift`, `Networking/PinnedTrustEvaluator.swift`, `Networking/SessionCookieBridge.swift`, `App/AppConfig.swift`, `Resources/Info.plist`, `backend/utils/safe_listing_url.py`, `ios/docs/SECURITY_AUDIT.md`, `ios/docs/TLS_PINNING.md` |
| **Outcome** | WKWebView allowlist (actions + responses) + TLS cert pinning (Release, sarraficars.com). Session cookie sync/clear between URLSession and WKWebView. ATS narrowed to localhost HTTP exceptions. Logout clears CSRF + cookies. Listing images: `resolveSafeImageURL` (iOS) + `normalize_listing_image_url` (grid JSON). Debug `SARRAFI_API_BASE_URL` must pass host allowlist. |
| **Validation** | `pytest backend/tests/test_mobile_auth_api.py backend/tests/test_safe_listing_url.py -q`; manual checklist in `ios/docs/SECURITY_AUDIT.md`; pin rotation documented in `ios/docs/TLS_PINNING.md`. |
| **Last verified** | 2026-05-26 |

### SEC-079 — Listing image URL sanitization (API grid JSON)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/utils/safe_listing_url.py`, `backend/utils/car_serialize.py` (`serialize_car_for_listings_grid`) |
| **Outcome** | Grid `image_url` / `gallery` reject `javascript:`/`data:`/private-IP hosts; production allows HTTPS only (plus `/car-images/`). |
| **Validation** | `pytest backend/tests/test_safe_listing_url.py -q` |
| **Last verified** | 2026-05-26 |

### SEC-014 — App 2FA removed (password-only auth)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py`, `backend/dealer/routes.py`, `backend/dealer/admin/routes.py`, `backend/dev/routes.py`, `backend/db/users_db.py`, legacy `backend/auth/mfa.py` (unused by app routes) |
| **Outcome** | **App** and **dealer** sign-in: password → `_finalize_app_session` sets `session['mfa_ok']=True` and clears stale MFA keys; `_post_login_redirect()` handles billing. Legacy `/mfa/*` URLs redirect (no OTP/TOTP/QR enrollment). **`/dev`** remains password-only. DB columns (`totp_*`, `mfa_method`) may remain; env admin sync still clears TOTP for `APP_ADMIN_*`. |
| **Validation** | `python -m pytest backend/tests/test_mfa_totp.py backend/tests/test_mfa_qr.py backend/tests/test_billing_gate.py backend/tests/test_mfa_action_log.py -q` |
| **Last verified** | 2026-05-22 |

### SEC-082 — No plaintext password verification in production

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/db/password_hash.py` (`verify_or_legacy`), `backend/db/users_db.py`, `backend/db/admin_users_db.py` |
| **Outcome** | Production accepts only bcrypt (`$2*`) hashes at login; legacy plaintext works in non-production only. New hashes use **bcrypt cost 13** (env `BCRYPT_ROUNDS`, 12–15). Successful login upgrades legacy plaintext and weaker bcrypt costs to the current work factor. |
| **Validation** | `python -m pytest backend/tests/test_password_hash.py::test_plaintext_password_rejected_in_production backend/tests/test_password_hash.py::test_check_user_upgrades_weak_bcrypt_on_login backend/tests/test_incomplete_listings_index.py::test_rehash_does_not_overwrite_newer_password -q`. |
| **Last verified** | 2026-06-02 |

### SEC-089 — Public listings fail closed when incomplete index unavailable

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/db/incomplete_listings_db.py`, `backend/db/inventory_db.py` (`_incomplete_index_snapshot_for_listings`, `_filter_public_listings_cars`, listings grid cache token) |
| **Outcome** | Production listings hide incomplete rows via the sidecar index; if the index cannot be read, filters fall back to per-row completeness checks instead of treating all cars as complete. Incomplete-index cache invalidates when `incomplete_listings.db` mtime changes. |
| **Validation** | `python -m pytest backend/tests/test_incomplete_listings_index.py -q`. |
| **Last verified** | 2026-06-02 |

### SEC-088 — Credential encryption at rest (SQLCipher) + bcrypt hardening

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/db/password_hash.py`, `backend/db/users_sqlite.py`, `backend/db/dev_users_sqlite.py`, `backend/utils/credential_db_encryption.py`, `backend/utils/production_security.py`, `backend/db/users_db.py`, `backend/db/admin_users_db.py`, `backend/main.py` (assert before DB init) |
| **Outcome** | **In production:** `USERS_DB_ENCRYPTION_KEY` and `DEV_USERS_DB_ENCRYPTION_KEY` (each ≥32 chars) required; `sqlcipher3` required; `ALLOW_UNENCRYPTED_USER_DB=1` forbidden. Dev/test may set `ALLOW_UNENCRYPTED_USER_DB=1` for plain SQLite. Password hashes use bcrypt cost 13 by default; login rehashes weak/legacy stored hashes. Sessions remain signed with `SECRET_KEY` (HttpOnly / Secure cookies per SEC-010). No 2FA. |
| **Validation** | `python -m pytest backend/tests/test_password_hash.py backend/tests/test_production_security.py -q`; production import in `scripts/security_check.sh` with encryption keys. |
| **Last verified** | 2026-05-30 |

### SEC-063 — Phone QR sign-in (retired with app 2FA)

| Field | Content |
|-------|---------|
| **Status** | Won't do (superseded by SEC-014) |
| **Scope** | `backend/auth/mfa.py`, `backend/utils/mfa_qr_store.py`, legacy templates `frontend/templates/mfa_qr_*.html` |
| **Outcome** | QR MFA **not** registered on the app; legacy `/mfa/qr-*` URLs redirect per SEC-014. Utilities remain in tree for reference only. Socket.IO CORS helper in `backend/main.py` still applies to `run.py` if other events are added later. |
| **Validation** | `python -m pytest backend/tests/test_mfa_qr.py -q` (legacy redirect smoke). |
| **Last verified** | 2026-05-22 |

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
| **Status** | Done (dormant for app auth) |
| **Scope** | `backend/utils/mfa_action_log.py`, `backend/utils/mfa_delivery.py` |
| **Outcome** | `MFA_ACTION_LOG_PATH` / logger `mfa_action` utilities remain; **app register/login no longer emit** `*.mfa_start` events after SEC-014. |
| **Validation** | `python -m pytest backend/tests/test_mfa_action_log.py -q` |
| **Last verified** | 2026-05-22 |

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

### SEC-104 — OAuth links only verified emails; pending link after a password login (remediation P3.1)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/auth/oauth_link.py` (new), `backend/auth/google_oauth.py`, `backend/auth/apple_oauth.py`, `backend/auth/pages.py` (`login_page`, `api_auth_login`), `backend/auth/app_registration.py`, `backend/dealer/routes.py` (dealer registration), `backend/db/users_db/{auth,accounts,admin,__init__}.py` (`find_users_by_email`, `update_user_profile`), `backend/utils/registration_validation.py` (`username_format_error`), `frontend/templates/login.html`; tests `test_google_oauth.py`, `test_apple_oauth.py`, `test_username_email_collision.py` (new) |
| **Outcome** | Closes the OAuth pre-registration takeover: someone registers a password account with another person's email (email verification is optional), the owner of that email later signs in with Google or Apple, and lands in the squatter's account. Also closes the variant where a username equals the victim's email. (1) Linking looks the account up by `lower(email)=lower(?)` (`find_users_by_email`), never `get_user_by_login`, which also matches usernames; more than one case-variant match refuses to pick. (2) Google requires `email_verified is True` (a missing claim is now rejected, not only `False`); Apple requires a true `email_verified` claim before the email-match path. (3) An existing account auto-links only when its `email_verified_at` is set. Otherwise the link waits in `session['pending_oauth_link']` (provider, sub, email, 10-minute expiry) and completes only after a password login as that same user in the same browser (`/login` or `POST /api/auth/login`); that links the sub, marks the email verified and clears the key. Accounts created through OAuth are verified at creation and keep auto-linking. (4) Web, mobile and dealer registration and profile update reject a new username that contains `@` or equals an existing email; changing the email clears `email_verified_at` and any pending verify token. **Deviation from the plan:** the plan held the link only when the account had no provider sub, a usable password and no `email_verified_at`. The code holds it whenever the matched email is unverified: an OAuth-created account's random bcrypt hash makes "usable password" unreadable, and a sub from the other provider does not vouch for a later email change. **No data repair:** accounts auto-linked by email, or that changed email and kept the verified flag, before this release cannot be found from the data and stay as they are. An unverified password account must log in with its password once before Google or Apple sign-in works (D-SEC4 (a)). Follow-ups (low; none allows a takeover): `google_oauth.py` / `apple_oauth.py` catch `sqlite3.IntegrityError` but not sqlcipher3's, so two concurrent first callbacks return 500 (fails closed); Apple's synthetic `{sub[:12]}@privaterelay.appleid.com` email can collide (the second user now gets "Could not create your account"); a new email equal to a legacy username containing `@` is still accepted (denial of service only). |
| **Go-live** | Ships with the N3 release. Then the owner may set `EMAIL_VERIFICATION_ENABLED=1` (remediation Section 8, Phase 16). |
| **Validation** | `TESTS_BLOCK_NETWORK=1 python -m pytest backend/tests/test_google_oauth.py backend/tests/test_apple_oauth.py backend/tests/test_username_email_collision.py -q` (for both providers: the pre-registration takeover is blocked, a username equal to the victim's email does not match, a missing `email_verified` is rejected, and the link completes after a password login). |
| **Last verified** | 2026-10-18 |

### SEC-107 — CSRF on operator DELETE routes; url_map coverage test; esm.sh out of CSP (remediation P3.4)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/web/security.py` (`_csrf_mutating_requests`, `_CSRF_HEADER_PREFIXES`, both CSP builders), `backend/tests/test_csrf_coverage.py` (new), `backend/tests/test_csp_policy.py` (new) |
| **Outcome** | The CSRF hook checked the `api_admin_operator_` prefix on POST only, so a cross-site page could make a signed-in site admin's browser call `api_admin_operator_delete_incomplete_car` or `api_admin_operator_api_delete_dealer`. DELETE on that prefix now requires `X-CSRF-Token` too (dev.js `devFetch` already sends it). `test_csrf_coverage.py` walks `app.url_map` and fails on any POST/PUT/PATCH/DELETE rule that the hook does not cover and that is not classified as: the `dev` / `dev_console` blueprint hooks, an in-view `validate_csrf_header` call (community_api, users_hub, dealers_hub), a signature-verified webhook (`billing.stripe_webhook`, `billing.premium_webhook`), or a redirect-only MFA stub (including `dev.dev_mfa_gone`). Today: 78 hook, 6 in-view, 2 exempt, 10 MFA stubs, 0 unclassified. `https://esm.sh` is removed from `script-src` and `connect-src` in both the enforced and the report-only CSP; nothing loaded from it. Follow-ups (review): the operator prefix is checked on POST and DELETE only, so a future PUT/PATCH operator route relies on the coverage test to be noticed; the in-view classification is a source-text check, not proof that the check runs before the first write. |
| **Go-live** | Ships with the N3 release; N3.13 step 4 checks the live CSP header for esm.sh. |
| **Validation** | `python -m pytest backend/tests/test_csrf_coverage.py backend/tests/test_csp_policy.py backend/tests/test_admin_operator_api.py backend/tests/test_app_security_basics.py -q` (DELETE `/api/admin/operator/incomplete-cars/1` without the token returns 403; the coverage test fails when a dummy unclassified POST is registered); `grep -n esm.sh backend/web/security.py` prints nothing. |
| **Last verified** | 2026-10-18 |

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
| **Scope** | `frontend/static/dev.js`, `dev_console.js`, `car.html`, `frontend/static/compare.js`, `frontend/templates/listings.html` (nearby-dealer filter) |
| **Outcome** | Dev incomplete cards: `escHtml` on tags and fields; http(s) CSS URLs; numeric `data-car-id`. Compare tray uses `safeCssBackgroundUrl` + `escapeHtml`. Listings dealer picker builds labels with `escHtml` + numeric registry ids only. |
| **Validation** | Grep `innerHTML` — each site reviewed; `python -m pytest backend/tests/test_app_security_basics.py -q`. |
| **Last verified** | 2026-05-30 |

### SEC-066 — Consumer premium Checkout verification

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/billing/stripe_billing.py` (`verify_premium_checkout_session`), `backend/billing/routes.py` (`premium_success`, `premium_webhook`), `frontend/templates/premium_success.html` |
| **Outcome** | Consumer Premium uses Stripe Checkout **subscription** mode (`STRIPE_PREMIUM_PRICE_ID`). `/billing/premium/success` grants `is_premium` only when session `status=complete`, `payment_status` paid (or trial `no_payment_required`), `mode=subscription`, and `metadata.user_id` / `premium=1` match. Premium webhook handles `checkout.session.completed`, `customer.subscription.updated`, and `customer.subscription.deleted` (revoke on cancel). |
| **Validation** | `python -m pytest backend/tests/test_premium_checkout.py backend/tests/test_stripe_premium_verify.py -q`; manual: visit success URL without `session_id` → no DB premium flag. |
| **Last verified** | 2026-05-25 |

### SEC-067 — Premium feature gates when billing enabled

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py` (`_require_premium_feature`, `api_car_chat`, `api_nearby_dealers`); `frontend/templates/listings.html` (403 hides dealer filter) |
| **Outcome** | When `BILLING_STRIPE_ENABLED=1`, car chat and dealership picker API require premium, active org subscription, or app admin. When billing disabled, features remain open (early access). |
| **Validation** | `python -m pytest backend/tests/test_premium_checkout.py backend/tests/test_billing_gate.py -q`; unpaid session → `POST /api/car/1/chat` → 403 `premium_required`. |
| **Last verified** | 2026-05-22 |

### SEC-071 — Email case normalization on registration

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/db/users_db.py` (`save_user`, `user_exists_by_email`), `backend/main.py` (`register`), `backend/utils/registration_validation.py` |
| **Outcome** | Store and compare emails case-insensitively (normalize to lowercase on insert/update). Prevent duplicate accounts differing only by case (`Test@x.com` vs `test@x.com`). |
| **Validation** | `python -m pytest backend/tests/test_registration_email.py -q`; register `A@example.com`, attempt `a@example.com` → duplicate error. |
| **Last verified** | 2026-05-22 |

### SEC-072 — Window sticker preview aligns with premium gate

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py` (`_serve_car_window_sticker_preview`, `car_detail` preview URL context) |
| **Outcome** | Window sticker PNG preview (`/car/<id>/window-sticker-preview.png`, legacy API path) uses `_require_premium_feature`: paid access when billing enabled; **login required in production when billing off** (same as SEC-073). OEM URLs in template context follow premium UI gates. |
| **Validation** | `python -m pytest backend/tests/test_audit_fixes.py::test_sticker_preview_requires_premium_when_billing_on backend/tests/test_audit_fixes.py::test_sticker_preview_requires_login_in_production_when_billing_off -q`. |
| **Last verified** | 2026-05-30 |

### SEC-073 — Login required for premium APIs when billing off in production

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py` (`_require_premium_feature`, `api_car_chat`, packages ensure routes, `_serve_car_window_sticker_preview`) |
| **Outcome** | In production, car chat, packages APIs, and window sticker PNG preview require an authenticated session even when `BILLING_STRIPE_ENABLED=0`; billing-off early access remains dev-only. |
| **Validation** | `python -m pytest backend/tests/test_audit_fixes.py::test_car_chat_requires_login_in_production_when_billing_off backend/tests/test_audit_fixes.py::test_sticker_preview_requires_login_in_production_when_billing_off -q`. |
| **Last verified** | 2026-05-30 |

### SEC-074 — Block admin self-registration via env lists

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py` (`register_page`), `backend/dealer/routes.py` (`dealer_register`), `backend/utils/roles.py` (`registration_blocked_by_env_admin`) |
| **Outcome** | Public `/register` and `/dealer/register` reject emails/usernames in `APP_ADMIN_EMAILS` / `APP_ADMIN_USERNAMES`. Admin promotion happens on login via `sync_env_admin_user_row` only. |
| **Validation** | `python -m pytest backend/tests/test_registration_email.py backend/tests/test_billing_gate.py -q`. |
| **Last verified** | 2026-05-22 |

### SEC-075 — Google OAuth sign-in (optional)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/auth/google_oauth.py`, `backend/db/users_db.py` (`google_sub`), `backend/main.py`, `frontend/templates/login.html` |
| **Outcome** | Server-side OAuth code flow when `GOOGLE_OAUTH_CLIENT_ID` + `GOOGLE_OAUTH_CLIENT_SECRET` set; `state` stored in session; callback rate-limited like login; verified Google email auto-links to existing app user by email (**changed 2026-10-18 by SEC-104:** only when that account's email is verified, otherwise after a password login); new users get `ROLE_GENERAL` + random password hash; UI hidden unless `GOOGLE_OAUTH_SHOW_BUTTON=1`. New Google sign-ups (non-premium) redirect to `/premium` with welcome banner; Stripe checkout uses existing `user_id` session (same account as Google). Optional `GOOGLE_OAUTH_OFFER_PREMIUM_ON_LOGIN=1` for returning users; `/auth/google?intent=premium` preserves upgrade intent through OAuth. |
| **Validation** | `python -m pytest backend/tests/test_google_oauth.py -q`; unconfigured `/auth/google` → redirect `/login`; callback with bad `state` → login error. |
| **Last verified** | 2026-10-18 |

### SEC-032 — Content-Security-Policy (enforce + report-only)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py` (`CSP_ENFORCE`, `CSP_REPORT_ONLY`, per-request nonce, `g.csp_nonce`), all app templates with `<script>` (listings, car, dashboard, auth, `dev*`, `dev_manifest`, `_app_logout.html` partial) |
| **Outcome** | Enforced `Content-Security-Policy` in production by default; per-request `script-src` **nonce** on all script elements (incl. `type="application/json"` blobs). `style-src` includes `unsafe-inline` for existing `style=""` until migrated to classes. `CSP_ENFORCE=0` / `1` overrides env default; if enforcement is off, `CSP_REPORT_ONLY=1` still sends report-only policy. 2026-10-18: the unused `https://esm.sh` origin is gone from both policies (**SEC-107**). |
| **Validation** | `CSP_ENFORCE=1` → `GET /login` includes `Content-Security-Policy` with `nonce-`; public pages and `/dev` load. `python -m pytest backend/tests/test_app_security_basics.py backend/tests/test_csp_policy.py -q`. |
| **Last verified** | 2026-10-18 |

### SEC-084 — HTTP security response headers

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py` (`_csp_headers` after_request) |
| **Outcome** | All responses: `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy: strict-origin-when-cross-origin`. When secure session cookies are enabled: `Strict-Transport-Security` (1 year, includeSubDomains). CSP behavior unchanged (SEC-032). |
| **Validation** | `python -m pytest backend/tests/test_production_security.py::test_security_headers_on_login -q`. |
| **Last verified** | 2026-05-26 |

### SEC-108 — `css_url` filter for style background-image URLs (remediation P3.5)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/web/templating.py` (`_jinja_css_url`, Jinja filter `css_url`), `frontend/templates/{home,landing,compare}.html`, `frontend/templates/inventory/{dashboard,listings}.html`, `frontend/static/compare.js`, `frontend/static/dev.js`, `backend/tests/test_css_url_filter.py` (new), `backend/tests/fixtures/app_surface.json` |
| **Outcome** | Autoescape turns `'` into `&#39;`, but the HTML parser decodes it back before the CSS parser reads a `style` attribute, so a dealer image URL containing `');` injected CSS on the home, landing, compare and inventory pages. The `css_url` filter accepts only http(s) URLs that `normalize_listing_image_url` passes (https only in production; no private hosts, credentials or script schemes) and same-origin `/static/`, `/car-images/` and `/dealer-uploads/` paths without dot segments. It percent-encodes `' " ( ) \ < >`, whitespace and control characters, and falls back to `/static/placeholder.svg`. All 10 template sites use it; compare.js and dev.js got entity-safe equivalents. Ordinary CDN URLs pass byte-identical; 402 of 214,675 active local image URLs (parentheses or spaces) change encoding and still serve the same image. Follow-ups (review): `frontend/static/listings/card.js` `cssSingleQuotedUrl` keeps weak escaping (no production caller); `listings_repo.py` `_css_url_safe` still sends those 402 landing photos to the placeholder; the template scan checks only `url(` inside `style` attributes. |
| **Go-live** | Ships with the N3 release. |
| **Validation** | `python -m pytest backend/tests/test_css_url_filter.py backend/tests/test_no_inline_style_elements.py backend/tests/test_app_surface_golden.py -q` (the injection payload is neutralized, CDN URLs come back byte-identical, and no template has `url(` in a `style` attribute without `css_url`). |
| **Last verified** | 2026-10-18 |

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
| **Scope** | `backend/main.py`, `backend/utils/car_chat_policy.py`, `backend/intelligence/ai/agent.py`, `backend/utils/web_researcher.py`, `frontend/templates/car.html`, `frontend/templates/compare.html`, `frontend/static/car_page.js`, `frontend/static/compare_chat.js` (chat disclosure copy) |
| **Outcome** | **Layered rate limits:** optional deployment-wide key (`RATE_LIMIT_CAR_CHAT_GLOBAL_PER_MIN`, default off); per-IP (`RATE_LIMIT_CAR_CHAT_PER_IP_PER_MIN`, default 48/min); per IP+car (`RATE_LIMIT_CAR_CHAT_PER_MIN`, default 24/min); compare chat uses per-IP compare key. Max message length and JSON body bytes unchanged. **Web research** (`CAR_CHAT_WEB_RESEARCH`): production + `auto` allows live research only for **signed-in** users (`CAR_CHAT_WEB_RESEARCH_PUBLIC` is ignored in production); since 2026-10-18 the research is HTTP-only and the web image has no browser (**SEC-111**); prod web has `CAR_CHAT_WEB_RESEARCH=0` since 2026-10-08; pgvector model-knowledge cache may still be read when keywords match. **Logging:** car chat no longer `print`s full user text; use DEBUG for operator detail. **Web research URLs:** `WEB_RESEARCH_ALLOWED_HOSTS` optional allowlist; blocklist + private/loopback host guard in `web_researcher.href_is_acceptable_result`. User messages remain **untrusted**; model output is advisory only. Car page states AI may be wrong; signed-out users see a line about signing in for full web research when the server allows it. Compare page includes the same advisory copy for side-by-side AI. |
| **Validation** | `python -m pytest backend/tests/test_app_security_basics.py backend/tests/test_car_chat_policy.py backend/tests/test_compare_specs.py -q` (413/429 paths, global chat limit, policy + URL guard). Set `BILLING_STRIPE_ENABLED=0` in tests before `import backend.main` so `.env` does not re-enable billing. Oversized body → 413; long message → `message_too_long`; flood → 429. |
| **Last verified** | 2026-10-18 |

### SEC-042 — LLM key & data exfiltration review

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/ai_agent.py`, `llm/` |
| **Outcome** | API keys read from environment only (e.g. `OPENAI_API_KEY`); no hardcoded secrets in repo from review. |
| **Validation** | `rg` for key-like literals in `backend/ai_agent.py` / `llm/` — none committed. |
| **Last verified** | 2026-04-18 |

### SEC-085 — Dealer locator Google Places login gate (production)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py` (`api_dealer_locator`) |
| **Outcome** | `GET /api/dealer-locator` is public by default (registry + Google when key set). Set `DEALER_LOCATOR_REQUIRE_LOGIN=1` in production to return **403** `login_required` for anonymous Google-tier requests. Per-IP rate limit unchanged (default 30/min). |
| **Validation** | `python -m pytest backend/tests/test_production_security.py::test_dealer_locator_google_requires_login_in_production -q`. |
| **Last verified** | 2026-06-12 |

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
| **Outcome** | `X-Forwarded-For` is honored only when `TRUST_PROXY_HEADERS` is truthy; otherwise `request.remote_addr` is used for smart search, car chat, `/login`, `/register`, and `/dev` auth rate limits. Hardened 2026-07-08: the client IP is taken `TRUSTED_PROXY_HOPS` entries from the **right** (not the first hop), so a client-supplied `X-Forwarded-For` can never become the rate-limit key. Entries are validated as IPs and ports stripped; a chain shorter than the hop count falls back to `remote_addr`. 2026-10-18: optional `TRUSTED_PROXY_CIDRS` mode closes the direct-to-origin spoof that hops=2 leaves open (**SEC-105**). |
| **Validation** | `python -m pytest backend/tests/test_client_ip.py`. |
| **Last verified** | 2026-10-18 |

### SEC-056 — KBB IDWS API key & outbound valuation

| Field | Content |
|-------|---------|
| **Status** | Retired |
| **Scope** | Former: `backend/enrichment/kbb_idws.py`, `POST /dev/api/cars/<id>/kbb-refresh`, post-scan `--post-kbb`, `scripts/fetch_kbb_for_inventory.py` |
| **Outcome** | KBB IDWS integration removed (no licensed API). Pricing insights use in-database similar-listing averages (`market_intel`) only. Legacy `kbb_*` DB columns may remain but are not populated or exposed in UI/API. |
| **Validation** | `rg -i "kbb_idws|KBB_API_KEY|kbb-refresh|Kelley Blue" backend frontend/templates docs/SECURITY_MASTER_TODO.md` shows no active product integration (gallery URL junk filters for `kbb.com` / `kbb-widget` remain). |
| **Last verified** | 2026-05-22 |

### SEC-105 — `client_ip` trusts X-Forwarded-For only from known proxy CIDRs (remediation P3.2)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/utils/client_ip.py` (`TRUSTED_PROXY_CIDRS`, `CLOUDFLARE_PROXY_CIDRS`, `CLOUDFLARE_PROXY_CIDRS_AS_OF`, `client_ip`), `backend/tests/test_client_ip.py` |
| **Outcome** | With `TRUSTED_PROXY_HOPS=2` (prod: Cloudflare, then Railway's edge), a request sent straight to the origin had only the edge's entry to the right of its own forged X-Forwarded-For entry, so `client_ip` returned the forged value: any per-IP rate limit could be dodged, and `DEV_IP_ALLOWLIST` would accept a spoofed address. New optional `TRUSTED_PROXY_CIDRS`: CIDRs or IPs, and/or the token `cloudflare`, which expands to Cloudflare's published ranges (dated 2026-10-18, with a refresh note). When it is set, X-Forwarded-For is walked from the right and the client is the first entry outside the ranges, but never more than `TRUSTED_PROXY_HOPS` entries from the right. So `client, 172.67.1.1` resolves to the client, the direct-origin spoof `spoof, 203.0.113.9` resolves to `203.0.113.9`, and a cross-zone Cloudflare Worker cannot forge: the hop bound stops at the Worker's Cloudflare address (review round 1 finding, fixed). With the CIDRs unset the hop rule is unchanged (30,000-input differential check). The module logs once when hops >= 2 and no CIDRs are set, and warns on a very broad CIDR. **Residual risk:** a sender with a Cloudflare egress address that reaches Railway's edge directly can still forge, even after the `*.up.railway.app` domain is removed (the edge routes by Host); see SEC-113. Follow-ups: no warning when the CIDRs are set with hops = 1 (the CIDR list then has no effect); `.env.example`, the production boot warning and conftest `app_env_defaults` do not mention `TRUSTED_PROXY_CIDRS` yet. |
| **Go-live** | Ships with the N3 release. Then N3.13, owner only, after `web-source-clear`: check that live hops is 2, set `TRUSTED_PROXY_CIDRS=cloudflare` on web (step 5), remove the `*.up.railway.app` domain (step 6, D-SEC6 (c)), and only then consider `DEV_IP_ALLOWLIST` (step 8, D-SEC5). |
| **Validation** | `python -m pytest backend/tests/test_client_ip.py backend/tests/test_dev_ip_allowlist.py -q`, run twice: as is, and with `TRUSTED_PROXY_CIDRS=cloudflare TRUSTED_PROXY_HOPS=2` exported. |
| **Last verified** | 2026-10-18 |

### SEC-111 — Web research is HTTP-only; Chromium leaves the web image (remediation P3.8, D-SEC1 (a))

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/utils/web_researcher.py` (`WebResearcher(allow_browser=False)`, `BrowserModeDisabled`), `backend/scripts/analyze_trim_adds_with_web.py`, `Dockerfile.web`, `backend/tests/test_web_researcher.py`, `backend/tests/test_car_chat_policy.py` |
| **Outcome** | Car-chat web research used to launch headless Chromium inside the web process, as root and with `--no-sandbox` (see the SEC-095 correction), for any signed-in user. `WebResearcher` now defaults to `allow_browser=False` (keyword-only). Brave search and the Playwright page fallback are both gated, every browser step raises `BrowserModeDisabled` before any Playwright import, and research runs on the HTTP path only (DuckDuckGo HTML search and direct page fetches through `fetch_page_text_http`, behind the SEC-106 host guard). The car-chat and compare-chat callers in `backend/intelligence/ai/agent.py` keep the default. Only the operator script `analyze_trim_adds_with_web.py`, run on a dev host, passes `allow_browser=True`, and an AST test pins that. `Dockerfile.web` no longer runs `playwright install chromium` / `install-deps` and drops the 17 Chromium-only apt libraries; a static test fails if a browser install comes back. The playwright pip package stays until remediation P15B.2 splits the requirements (inert without a browser). Scanner browser paths still need `SCANNER_ALLOW_BROWSER`, which web does not set. |
| **Go-live** | Ships with the N3 release. The N3.13 Docker smoke (owner OK) must show no `ms-playwright` directory, a clean `scripts/scanner_import_sweep.py --profile web`, and the image-size drop against 1.5.6; the owner confirms `SCANNER_ALLOW_BROWSER` is absent from the web variables. Prod also has `CAR_CHAT_WEB_RESEARCH=0` (D-SEC1 stopgap, 2026-10-08). Follow-ups: DNS rebinding in the HTTP fetcher (SEC-112); the code still says "Playwright" for this policy (`car_chat_policy.web_research_playwright_allowed`, the production warning text, comments in `agent.py`, `allow_playwright` in `routes/cars_pages.py`), to be renamed to `web_research_allowed` by a later unit. |
| **Validation** | `TESTS_BLOCK_NETWORK=1 python -m pytest backend/tests/test_web_researcher.py backend/tests/test_car_chat_policy.py -q` (with the Playwright imports patched to raise, `WebResearcher()` touches neither Brave nor the fallback; `allow_browser=True` reaches the mocked fallback; `Dockerfile.web` installs no browser). |
| **Last verified** | 2026-10-18 |

### SEC-113 — Origin lock: only Cloudflare may reach the web origin

| Field | Content |
|-------|---------|
| **Status** | Not started |
| **Scope** | Web ingress (Railway edge for `sarraficars.com`) and the app's request hook. No unit owns it yet: a D-SEC6 follow-up and a candidate for remediation Phase 16. |
| **Outcome** | Planned. Even with `TRUSTED_PROXY_CIDRS=cloudflare` (SEC-105) and the `*.up.railway.app` domain removed, a sender that egresses from a Cloudflare address (for example a Worker using TCP sockets) can reach Railway's edge directly with `Host: sarraficars.com`. The edge routes by Host, so removing the domain does not stop it (remediation plan Appendix A.13), and the sender's own Cloudflare address is the one hop the walk skips, so it can still choose its client IP. Until an origin lock exists, per-IP rate limits are best effort against such a sender, and `DEV_IP_ALLOWLIST` is defence in depth, not a gate. Candidate fix: Cloudflare adds a secret request header (for example a Transform Rule) and the app refuses any request without it before routing; check whether Railway can do authenticated origin pulls instead. |
| **Validation** | When built: a request that reaches the origin without the secret header is refused (403) before any view runs; a request with it passes; `backend/tests/test_client_ip.py` is unchanged. |
| **Last verified** | 2026-10-18 (gap confirmed by the P3.2 review; no code yet) |

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

### SEC-068 — Dev scanner URL validation (SSRF)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/utils/outbound_url.py`, `backend/dev/routes.py` (`POST /dev/api/test-scanner`, `POST /dev/api/smart-import`, `POST /dev/api/smart-import-bulk`), `backend/utils/web_researcher.py` (shared host guard) |
| **Outcome** | Dev scanner subprocess endpoints accept only `http`/`https` URLs with a public hostname (blocks loopback, RFC-private IPs, `.local`, embedded credentials, non-HTTP schemes). Bulk import rejects the whole request when any URL fails validation. |
| **Validation** | `python -m pytest backend/tests/test_outbound_url.py -q`; manual: `POST /dev/api/test-scanner` with `http://127.0.0.1/` → 400 `url_host_blocked` (authenticated dev session + CSRF). |
| **Last verified** | 2026-05-22 |

### SEC-069 — Window sticker fetch allowlist

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/scanner/window_sticker.py` (`_fetch_url_as_sticker`) |
| **Outcome** | Outbound `requests.get` runs only when the URL is in `get_window_sticker_candidate_urls(vin)` for that VIN (OEM template URLs only). |
| **Validation** | `python -m pytest backend/tests/test_window_sticker_urls.py::test_fetch_blocks_url_not_in_oem_candidates -q`. |
| **Last verified** | 2026-05-22 |

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
| **Outcome** | When a **known inventory provider** (`dealer_dot_com`, `dealer_on`) yields **zero vehicles**, the scanner may write a Playwright **HAR** under `workspace/debug/fail_<dealer_id>_<epoch>.har` for operator diagnosis when **`SCANNER_FAILURE_HAR=1`** (default **off** since SEC-087). HAR files can contain **cookies, Authorization headers, and URL query tokens** — treat as **sensitive**, do not commit, and restrict filesystem permissions in shared environments. |
| **Validation** | `.gitignore` contains `workspace/debug/` and repo-root `debug/` (except `debug/README.md`); `python -m pytest backend/tests/test_scanner_intercept_filter.py -q` passes; manual optional: run scanner against a dealer forced to 0 rows with `SCANNER_FAILURE_HAR=1` and confirm a `.har` appears under `workspace/debug/` when play succeeds. |
| **Last verified** | 2026-05-30 |

### SEC-087 — Debug audit remediation (May 2026)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `.gitignore`, `debug/README.md`, `backend/utils/hybrid_search.py` (`public_search_meta`), `backend/main.py` (`api_search_smart`), `backend/utils/car_serialize.py` (`redact_sensitive_car_row`), `backend/dev/routes.py` (`api_car_debug`, `api_audit_last_scrape`), `backend/scanner/cli.py` (`SCANNER_FAILURE_HAR` default), `deploy/always-on/systemd/sarraficars-web.service`, `deploy/always-on/launchd/com.sarraficars.web.plist` |
| **Outcome** | Repo-root `debug/` artifacts untracked (scanner PNGs, e2e screenshots). Public `POST /api/search/smart` returns trimmed `search_meta` (mode + counts only). `/dev/api/car-debug` and `/dev/api/audit-last-scrape` redact `internal_notes`, `price_provenance_json`, and related operator columns even for dev admins. Scanner HAR capture opt-in (`SCANNER_FAILURE_HAR=1`). Supervised always-on units use **gunicorn** (not Werkzeug `run.py`). |
| **Validation** | `python -m pytest backend/tests/test_debug_audit.py backend/tests/test_production_security.py -q`; `git ls-files 'debug/*'` → only `debug/README.md`; `git check-ignore -v debug/fail_foo.png` confirms ignore. |
| **Last verified** | 2026-05-30 |

### SEC-064 — Post-scan listing gap fill (operator inventory DB URLs + DDG Instant Answer)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/listing_gap_fill.py`, `backend/scanner_post_pipeline.py` (`run_listing_gap_fill_stage`), `scanner.py` (`--post-listing-gap-fill` / `SCANNER_POST_LISTING_GAP_FILL`), `backend/utils/vdp_spec_parse.py` (`parse_condition_from_listing_html`) |
| **Outcome** | After a scan, optionally backfill incomplete listing rows for VINs touched in that run: tier 1 reuses existing EPA/vPIC structured backfill; tier 2 fetches each row’s stored HTTPS `source_url` via `requests` then optional **sync Playwright** Chromium when the HTML shell looks thin; **condition** is parsed only from page HTML/JSON-LD (never from DDG). Tier 3 uses **DuckDuckGo Instant Answer** (`api.duckduckgo.com`, bounded timeout) only for residual mechanical fields (transmission/drivetrain/fuel_type). Provenance merged into `cars.spec_source_json`. No new Flask routes; operator CLI / batch only. Max rows per run capped via `SCANNER_POST_LISTING_GAP_FILL_MAX` (default 400). DDG can be disabled with `LISTING_GAP_FILL_ALLOW_DDG=0`. |
| **Validation** | `python -m pytest tests/test_listing_gap_fill.py`; manual: `SCANNER_POST_LISTING_GAP_FILL=1` or `--post-listing-gap-fill` after a scan with `SCANNER_POST_REPAIR=1` recommended so repair runs first. |
| **Last verified** | 2026-04-29 |

### SEC-080 — Local `model_specs` db_admin tool

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/scripts/db_admin.py`, `backend/tests/test_db_admin_security.py` |
| **Outcome** | Operator CLI at `http://127.0.0.1:5001/model-specs-admin` (not store `/admin`). Requires `DB_ADMIN_TOKEN` (≥16 chars) on all `/model-specs-admin/api/*` routes; UPDATE allowlist columns only (`transmission`, `drivetrain`, `cylinders`); UI escapes row text; `DB_ADMIN_DEBUG` forbidden in production. |
| **Validation** | `python -m pytest backend/tests/test_db_admin_security.py -q`; API without token → **401**; invalid `field` on PUT → **400**. |
| **Last verified** | 2026-05-26 |

### SEC-083 — `/dev` access: separate operator login in production

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/dev/routes.py` (`_admin_session_ok`), `backend/utils/production_security.py` (`app_admin_dev_pass_through_allowed`; boot warning since SEC-109) |
| **Outcome** | In production, app `role=admin` (from `APP_ADMIN_*` env) does **not** satisfy `/dev` `before_request` unless `ALLOW_APP_ADMIN_DEV_PASS_THROUGH=1`. Operators use `/dev/login` (`dev_users.db`) by default. Prod web ran with the pass-through set to `1` (P0A.1 audit, 2026-10-07); the owner deleted the variable on 2026-10-08 (`docs/SCANNING_OPS_LOG.md`, D-SEC5 applied in effect), so prod matches this row and has no exception. Since 2026-10-18 production boot logs a warning if the variable comes back (SEC-109). `DEV_IP_ALLOWLIST` stays unset until `TRUSTED_PROXY_CIDRS` is live (SEC-105), because until then the allowlist can be spoofed. A web rollback to a deployment older than `d9aaa75e` (for example `c9872e66`) would bring the variable back (`docs/RELEASING.md`, Rollback step 2). |
| **Validation** | `python -m pytest "backend/tests/test_production_security.py::test_app_admin_session_does_not_access_dev_api_in_production" "backend/tests/test_production_security.py::test_app_admin_dev_pass_through_off_in_production" "backend/tests/test_production_security.py::test_pass_through_guard_warns_exactly_when_enabled" -q`. Owner, read-only: the names-only web variable snapshot (RELEASING.md step 8) has no `ALLOW_APP_ADMIN_DEV_PASS_THROUGH`. |
| **Last verified** | 2026-10-18 (tests; the prod variable state comes from the 2026-10-08 ops-log block, not a new read: agents do not read prod) |

### SEC-089 — DNS-aware dev scanner URL guard

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/utils/outbound_url.py` (`destination_host_blocked_after_dns`, `validate_dev_scanner_url`) |
| **Outcome** | Dev scanner subprocess URLs reject hostnames that resolve to loopback, RFC-private, link-local, or metadata-style addresses (not only literal IP literals). |
| **Validation** | `python -m pytest backend/tests/test_outbound_url.py backend/tests/test_outbound_url_dns.py -q`. |
| **Last verified** | 2026-06-02 |

### SEC-090 — Attribute-safe escaping (trim ladder + dealer links)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `frontend/static/car_trim_ladder.js`, `frontend/static/find_dealers.js`, `frontend/static/dev.js`, `backend/listings/dealer_locator.py`, `backend/utils/safe_listing_url.py` (`normalize_safe_http_url`) |
| **Outcome** | Trim ladder `escapeHtml` escapes `"` for attribute contexts; dealer locator API returns http(s)-only `website_url`; client UIs refuse `javascript:` hrefs. |
| **Validation** | Grep `data-trim-name` + `escapeHtml` in `car_trim_ladder.js`; `pytest backend/tests/test_safe_listing_url.py backend/tests/test_dealer_locator.py -q`. |
| **Last verified** | 2026-06-02 |

### SEC-091 — Enrichment vision outbound URL guard

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/enrichment/service.py` (`_all_gallery_urls_ordered`, `_fetch_image_b64_optimized`), `backend/utils/safe_listing_url.py` |
| **Outcome** | Vision image fetches run only through `normalize_listing_image_url` (blocks private/loopback hosts in production, script URLs, embedded credentials). |
| **Validation** | `python -m pytest backend/tests/test_safe_listing_url.py -q`; code review `_fetch_image_b64_optimized`. |
| **Last verified** | 2026-06-02 |

### SEC-092 — Dev manifest console secret required (all environments)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/dev/console.py`, `backend/utils/production_security.py` (SEC-081 prod import guard unchanged) |
| **Outcome** | When `DEV_CONSOLE=1`, `DEV_CONSOLE_SECRET` is required before any manifest route is reachable (non-production included). Login uses `secrets.compare_digest`. |
| **Validation** | `DEV_CONSOLE=1` without secret → `GET /dev/manifest` returns disabled page or 404 API; with secret → login gate. |
| **Last verified** | 2026-06-02 |

### SEC-093 — Session lifetime + password max length

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py` (`PERMANENT_SESSION_LIFETIME`, `SESSION_REFRESH_EACH_REQUEST`), `backend/utils/registration_validation.py` |
| **Outcome** | Flask sessions expire after 14 days with refresh on each request; registration passwords capped at 128 characters. |
| **Validation** | `python -m pytest backend/tests/test_registration_password_limits.py -q`. |
| **Last verified** | 2026-06-02 |

### SEC-094 — LLM client error sanitization + untrusted listing blocks

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/intelligence/ai/agent.py` |
| **Outcome** | Car/compare chat returns generic `chat_unavailable` on provider failures (details logged server-side). Listing notes, packages JSON, and web research wrapped in UNTRUSTED delimiters with model instructions not to follow embedded instructions. |
| **Validation** | `python -m pytest backend/tests/test_car_chat_policy.py backend/tests/test_compare_specs.py -q`. |
| **Last verified** | 2026-06-02 |

### SEC-095 — Smart search parse rate limit + CSP map tiles + Playwright launch flags

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/main.py` (`GET /api/search/smart/parse`, CSP `connect-src`, `object-src 'none'`), `backend/utils/web_researcher.py` (`_playwright_launch_args`, `PLAYWRIGHT_NO_SANDBOX`), `frontend/templates/car.html`, `frontend/templates/listings.html`, `frontend/static/listings.js`, `backend/auth/google_oauth.py` (constant-time `state`) |
| **Outcome** | GET parse endpoint shares POST smart-search per-IP limit; CSP allows OpenStreetMap tiles; price history uses `\| tojson`; guest banner dismiss is nonce-safe; OAuth `state` uses `compare_digest`. **Correction (2026-10-18):** this row used to say Playwright runs Chromium without `--no-sandbox` unless `PLAYWRIGHT_NO_SANDBOX=1`. That was never true. Playwright adds `--no-sandbox` to every Chromium launch unless `chromium_sandbox=True` is passed (Playwright 1.60.0 driver: `if (options.chromiumSandbox !== true) chromeArguments.push("--no-sandbox")`), and `_LAUNCH_ARGS` passes `--disable-setuid-sandbox` as well. `PLAYWRIGHT_NO_SANDBOX` therefore changes nothing, and Chromium in the web image ran as root with no sandbox. The chosen remedy (D-SEC1 (a)) takes the browser out of the web process entirely (SEC-111). Browser mode remains only in the operator script `analyze_trim_adds_with_web.py` on a dev host, still unsandboxed; passing `chromium_sandbox=True` there is optional later hardening. |
| **Validation** | `python -m pytest backend/tests/test_smart_search.py backend/tests/test_google_oauth.py backend/tests/test_app_security_basics.py backend/tests/test_web_researcher.py -q`. |
| **Last verified** | 2026-10-18 |

### SEC-096 — Email verification tokens (B1 scaffold)

| Field | Content |
|-------|---------|
| **Status** | In progress |
| **Scope** | `backend/auth/email_verification.py`, `backend/db/users_db.py` (`email_verified_at`, `email_verify_token_hash`), `backend/main.py` (`/verify-email`, `POST /resend-verification`), `backend/auth/app_registration.py`, Google/Apple OAuth create/link |
| **Outcome** | When `EMAIL_VERIFICATION_ENABLED=1`, password registration stores **hashed** verify tokens (SHA-256 + pepper); plain token only in email link; Resend/SMTP/log delivery via `send_transactional_email`; OAuth users marked verified on create/link; per-IP rate limit on resend (5/hour). Feature **off** by default until operator enables env. |
| **Validation** | `python -m pytest backend/tests/test_email_verification.py -q`; manual: register with `EMAIL_VERIFICATION_ENABLED=1` + `MFA_DELIVERY_MODE=log` → link in logs; `/verify-email?token=…` sets `email_verified_at`. |
| **Last verified** | 2026-06-11 |

### SEC-097 — Password reset tokens (B2 scaffold)

| Field | Content |
|-------|---------|
| **Status** | In progress |
| **Scope** | `backend/auth/password_reset.py`, `backend/db/users_db.py` (`password_reset_token_hash`, `password_reset_expires_at`), `backend/main.py` (`/forgot-password`, `/reset-password`), `frontend/templates/login.html` |
| **Outcome** | When `PASSWORD_RESET_ENABLED=1`, forgot-password stores **hashed** reset tokens with 1-hour expiry; plain token only in email link; generic success message (no email enumeration); OAuth-only accounts skipped; per-IP rate limit on forgot (10/hour); since 2026-10-18 also at most 3 reset emails per account per hour (**SEC-109**). Feature **off** by default (prod enables it only after remediation P16.1 ships). |
| **Validation** | `python -m pytest backend/tests/test_password_reset.py -q`; manual: `PASSWORD_RESET_ENABLED=1` + `MFA_DELIVERY_MODE=log` → link in logs; reset updates password. |
| **Last verified** | 2026-10-18 |

### SEC-098 — Site-admin user CRUD (D1)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/dealer/admin/users_hub.py`, `backend/db/users_db.py` (`admin_create_user`, `admin_update_user`, `admin_reset_user_password`, `delete_user_by_id`, `is_active`), `frontend/templates/admin/users.html`, `frontend/templates/admin/user_form.html`, `frontend/templates/_icons.html` |
| **Outcome** | Site admin (`role=admin`) can list/search users at `/admin/users`; create at `/admin/users/new`; edit role/scope at `/admin/users/<id>/edit`; reset password; suspend/reactivate/delete via CSRF POST. Env-configured admins cannot be deleted or demoted; admins cannot suspend/delete themselves. Suspended users fail password login. Admin UI uses vector icons only (no emoji action buttons). |
| **Validation** | `python -m pytest backend/tests/test_admin_users.py -q`; manual: login as site admin, create user, change role, reset password, suspend → login denied. |
| **Last verified** | 2026-06-11 |

### SEC-101 — Site-admin platform dashboard + search analytics

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/dealer/admin/platform_stats.py`, `backend/db/search_analytics_db.py`, `backend/dealer/admin/routes.py` (`/admin/site`), `backend/listings/routes.py`, `backend/main.py` (`api_search_smart`), `frontend/templates/admin/site_hub.html`, `frontend/templates/_nav_sidebar.html` |
| **Outcome** | Site admin dashboard shows platform health, inventory/dealership/user scale (all three dealer counts), and aggregated search/usage metrics with audience toggle (all / logged-in / anonymous). Search events stored without blocking requests; dashboard shows aggregates only (top queries, makes, states, sources). Site-admin nav slimmed to Dashboard / Dealer jobs / Users; merchandising demoted to debug card. |
| **Validation** | `python -m pytest backend/tests/test_platform_dashboard.py -q`; manual: login as site admin → `/admin/site`, run a listings search, refresh dashboard. |
| **Last verified** | 2026-06-14 |

### SEC-099 — Stripe Customer Portal (C3 scaffold)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/billing/stripe_billing.py` (`create_customer_portal_session`), `backend/routes/account.py` (`account_billing_page`, `account_billing_portal`; moved out of `backend/main.py`), `frontend/templates/account_billing.html` |
| **Outcome** | Security property (Done): signed-in users view plan and features at `/account/billing`. The portal redirect requires a login, does nothing unless `BILLING_STRIPE_ENABLED=1`, and uses only the session user's own `premium_stripe_customer_id` read from `users.db`: no customer id comes from the request, so no user can open another user's portal. Failures are logged server-side and fall back to `/account/billing`. The return URL uses `PUBLIC_BASE_URL`, or the request host when that is unset (SEC-109 warns about this in production). |
| **Go-live** | Not live, and not a security item: billing is off on prod web (`BILLING_STRIPE_ENABLED` is not among the web variables in the 2026-10-07 P0A.1 inventory, and it defaults to off). Before billing goes live the owner configures the Stripe Customer Portal in the Stripe dashboard (remediation plan Section 8, Phase 16 owner actions). Also listed under "Future" at the end of this file. |
| **Validation** | `python -m pytest backend/tests/test_billing_portal.py -q`; manual (when billing goes live): grant a test user `premium_stripe_customer_id`, open the portal link with Stripe keys configured. |
| **Last verified** | 2026-10-18 |

### SEC-102 — Postgres-only inventory (no SQLite fallback)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/db/inventory_pg.py`, `backend/db/inventory_compat.py`, `backend/db/inventory_db.py`, `backend/main.py`, `backend/utils/production_security.py`, `deploy/up.sh`, `deploy/docker-compose.yml`, `frontend/templates/admin/site_hub.html` |
| **Outcome** | Runtime requires `INVENTORY_DATABASE_URL` (postgresql://). SQLite inventory fallback disabled except `INVENTORY_SQLITE_TESTS=1` in pytest. `./deploy/up.sh` always starts Postgres + web + workers; dashboard shows **Postgres required** instead of SQLite when misconfigured. |
| **Validation** | `python -m pytest backend/tests/test_inventory_postgres_required.py backend/tests/test_store_admin.py backend/tests/test_platform_dashboard.py -q`; app without `INVENTORY_DATABASE_URL` raises at startup. |
| **Last verified** | 2026-06-14 |

### SEC-103 — Site-admin bootstrap must not overwrite UI password changes

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `scripts/bootstrap_site_admin.py`, `scripts/docker-entrypoint-web.sh`, `backend/db/admin_users_db.py`, `backend/utils/bootstrap_policy.py`, `deploy/up.sh` |
| **Outcome** | Startup bootstrap promotes env admins but **does not** reset existing passwords from `ADMIN_PASSWORD` in `users.db` (site admin) or `dev_users.db` (`/dev` admin). Password sync runs only for **new** admin rows or when `BOOTSTRAP_FORCE_ADMIN_PASSWORD=1`. Admin UI password resets persist across restarts and `import backend.main` reloads. Host `deploy/up.sh` no longer runs duplicate bootstrap (web entrypoint owns it). |
| **Validation** | `python -m pytest backend/tests/test_bootstrap_site_admin.py backend/tests/test_init_admin_db.py -q` |
| **Last verified** | 2026-06-14 |

### SEC-106 — SSRF guards block every non-global address (remediation P3.3)

| Field | Content |
|-------|---------|
| **Status** | Done |
| **Scope** | `backend/utils/outbound_url.py` (`ip_is_non_global`, `unwrap_embedded_ipv4`, `destination_host_blocked`, `destination_host_blocked_after_dns`), `backend/utils/safe_listing_url.py` (`_host_is_blocked`), `backend/tests/test_outbound_url.py`, `backend/tests/test_outbound_url_dns.py`, `backend/tests/test_safe_listing_url.py` |
| **Outcome** | The guards (SEC-068, SEC-089) used `is_private`-style flags, so CGNAT and Tailscale addresses (100.64.0.0/10, including 100.100.100.200), benchmarking space (198.18.0.0/15), IETF protocol space (192.0.0.0/24) and IPv4-mapped forms such as `::ffff:100.64.0.1` got through. Both outbound checks now unwrap IPv4-mapped and 6to4 addresses and block when `not is_global or is_multicast or is_reserved` (reserved is kept so `::/8` and NAT64 `64:ff9b::/96` stay refused). Legacy IPv4 spellings (`127.1`, `2130706433`, `0x7f.0.0.1`, `0177.0.0.1`) are parsed as addresses, and an after-DNS answer with no IP or an unparseable IP fails closed. `safe_listing_url._host_is_blocked` applies the same rule (and now refuses `*.local` / `*.localhost`) while keeping its non-production localhost allowance. Its docstring states the threat model: listing image URLs are fetched by the visitor's browser, so that check is client-side hardening, not SSRF protection. Local data impact: 0 of 320,984 image URLs and 0 of 550 dealer websites use an IP literal or a local host. Follow-ups: resolve-then-connect rebinding and two unguarded server-side callers (SEC-112); latent, not exploitable today: the after-DNS check resolves the UTS #46 form of a name while CPython resolves IDNA 2003; `test_idna_folding_matches_uts46` follows the unpinned `idna` release. |
| **Go-live** | Ships with the N3 release. |
| **Validation** | `TESTS_BLOCK_NETWORK=1 python -m pytest backend/tests/test_outbound_url.py backend/tests/test_outbound_url_dns.py backend/tests/test_safe_listing_url.py -q` (blocked: 100.64.0.1, 100.127.255.254, 100.100.100.200, ::ffff:100.64.0.1, 198.18.0.1, 192.0.0.8, and a name that a patched `getaddrinfo` resolves to 100.x; allowed: 8.8.8.8 and 2606:4700:4700::1111). The same tests pass on Python 3.12 (CI). |
| **Last verified** | 2026-10-18 |

### SEC-112 — Pin outbound fetches to the validated address (DNS rebinding); guard the remaining server-side fetchers

| Field | Content |
|-------|---------|
| **Status** | Not started |
| **Scope** | `backend/utils/web_researcher.py` (`fetch_page_text_http`, `_SafeRedirectHandler`), callers of `backend/utils/outbound_url.py` `validate_dev_scanner_url`, `backend/enrichment/service.py` (`_fetch_image_b64_optimized`), `backend/dealer/admin/onboard_api.py` (onboard URL check). No unit owns it yet: extend remediation P16.4's resolver helper, or add a unit. |
| **Outcome** | Planned. (1) Resolve-then-connect: `destination_host_blocked_after_dns` resolves the host, then urllib resolves it again when it connects. The P3.8 review's probe got 8.8.8.8 on the first lookup and connected to 169.254.169.254:80 on the second. Since SEC-111 this HTTP path is the only car-chat research fetcher. The dev scanner URL check (SEC-089) validates a URL that a scanner subprocess resolves again later. (2) `enrichment/service.py` `_fetch_image_b64_optimized` fetches server-side with only the literal-host check (`normalize_listing_image_url`, no DNS) and follows redirects without checking them (a 302 to 169.254.169.254 or a 100.x address is followed). (3) `dealer/admin/onboard_api.py` checks only the literal host (`normalize_safe_http_url`). Fix: one shared helper that resolves once, checks every answer with `ip_is_non_global`, connects to the checked address (an HTTP(S) connection that keeps the Host header and SNI), and re-checks every redirect hop; use it in all of these callers and add rebinding tests. Fold in the IDNA 2003 vs UTS #46 mismatch noted in SEC-106. |
| **Validation** | When built: with a fake resolver that answers a public IP first and 169.254.169.254 second, no connection is made to the second address; each caller refuses a redirect to a non-global host. |
| **Last verified** | 2026-10-18 (gap confirmed by the P3.3 and P3.8 reviews; no code yet) |

---

## Changelog

| Date (UTC) | Change |
|------------|--------|
| 2026-10-18 | **Doc step for security wave 1 (remediation P3.9):** the Summary table counted 0 open items in every phase while SEC-096, SEC-097 and SEC-099 were In progress; it now counts and names the open rows, and `backend/tests/test_security_master_todo.py` fails when the table and the rows disagree. **SEC-095** corrected: Playwright adds `--no-sandbox` unless `chromium_sandbox=True`, so `PLAYWRIGHT_NO_SANDBOX` never sandboxed anything. **SEC-099** security property Done (own customer id only); billing go-live tracked separately. **SEC-083** records that prod deleted `ALLOW_APP_ADMIN_DEV_PASS_THROUGH` on 2026-10-08, so there is no prod exception. **SEC-076** Done (remediation P15B.1: crawl4ai and the other orphans removed, `lxml>=6.1.0`, pip-audit with no ignores). Env table: `TRUSTED_PROXY_CIDRS` added; `CAR_CHAT_WEB_RESEARCH*` rows corrected (HTTP-only research; `_PUBLIC` is ignored in production). Validation: `python -m pytest backend/tests/test_security_master_todo.py -q`. |
| 2026-10-18 | **SEC-104 (P3.1):** OAuth (Google, Apple) links by strict `lower(email)` only, requires a verified provider email, and auto-links an existing account only when its email is verified; otherwise the link waits for a password login in the same browser. Usernames may not contain `@` or equal another account's email. Validation: `pytest backend/tests/test_google_oauth.py backend/tests/test_apple_oauth.py backend/tests/test_username_email_collision.py -q`. |
| 2026-10-18 | **SEC-105 (P3.2):** optional `TRUSTED_PROXY_CIDRS` (`cloudflare` token) makes `client_ip` skip only trusted proxy entries, bounded by `TRUSTED_PROXY_HOPS`, closing the direct-to-origin X-Forwarded-For spoof (also used against `DEV_IP_ALLOWLIST`). Live after N3.13 step 5. Validation: `pytest backend/tests/test_client_ip.py backend/tests/test_dev_ip_allowlist.py -q`. |
| 2026-10-18 | **SEC-106 (P3.3):** SSRF guards unwrap IPv4-mapped / 6to4 addresses and block every non-global address (CGNAT/Tailscale, 198.18/15, 192.0.0/24 included); listing-image host check gets the same rule and a stated threat model. Validation: `pytest backend/tests/test_outbound_url.py backend/tests/test_outbound_url_dns.py backend/tests/test_safe_listing_url.py -q`. |
| 2026-10-18 | **SEC-107 (P3.4):** CSRF header required on operator DELETE routes (`api_admin_operator_` prefix); url_map coverage test classifies every mutating route; `https://esm.sh` removed from both CSPs. Validation: `pytest backend/tests/test_csrf_coverage.py backend/tests/test_csp_policy.py backend/tests/test_admin_operator_api.py -q`. |
| 2026-10-18 | **SEC-108 (P3.5):** `css_url` Jinja filter for every `style="background-image:url(...)"` (http(s) or same-origin paths only, breakout characters percent-encoded, placeholder fallback), closing CSS injection from dealer image URLs. Validation: `pytest backend/tests/test_css_url_filter.py -q`. |
| 2026-10-18 | **SEC-109 (P3.6):** production boot warnings for email/billing flows without `PUBLIC_BASE_URL` or a mail transport, and for `ALLOW_APP_ADMIN_DEV_PASS_THROUGH`; password reset limited to 3 emails per account per hour. Validation: `pytest backend/tests/test_production_security.py backend/tests/test_password_reset.py -q`. |
| 2026-10-18 | **SEC-110 (P3.7), Blocked:** `sync-vault-to-railway.sh` refuses an unset or malformed `TRUSTED_PROXY_HOPS` (and an unset `RAILWAY_SERVICE`) before any `railway` call; README service list and hops rule. Code on `unit/P3.7`; merge waits for the owner to confirm the live hops value. Validation: `bash -n deploy/railway/sync-vault-to-railway.sh`; `pytest backend/tests/test_sync_vault_guard.py -q`. |
| 2026-10-18 | **SEC-111 (P3.8, D-SEC1 (a)):** `WebResearcher` is HTTP-only unless `allow_browser=True` (operator script only); `Dockerfile.web` installs no Chromium and none of its libraries. Docker smoke pending (N3.13). Validation: `pytest backend/tests/test_web_researcher.py backend/tests/test_car_chat_policy.py -q`. |
| 2026-10-18 | **SEC-112, SEC-113 (new, Not started):** follow-ups from the wave-1 reviews. SEC-112: pin outbound fetches to the validated address (DNS rebinding in the research fetcher and dev scanner check) and guard `enrichment/service.py` and `dealer/admin/onboard_api.py`. SEC-113: an origin lock, because a Cloudflare-egress sender that reaches Railway's edge directly can still choose its client IP. Validation: none yet (no code). |
| 2026-06-14 | **SEC-103:** Startup bootstrap no longer overwrites existing site-admin or `/dev` admin passwords; vault sync only on new admin or `BOOTSTRAP_FORCE_ADMIN_PASSWORD=1`; removed duplicate host bootstrap from `deploy/up.sh`. Validation: `pytest backend/tests/test_bootstrap_site_admin.py backend/tests/test_init_admin_db.py -q`. |
| 2026-06-14 | **SEC-102:** Postgres-only inventory — fail fast without `INVENTORY_DATABASE_URL`; Docker stack always includes Postgres; pytest uses `INVENTORY_SQLITE_TESTS=1`. Validation: `pytest backend/tests/test_inventory_postgres_required.py -q`. |
| 2026-06-11 | **SEC-099:** C3 Stripe Customer Portal scaffold — `/account/billing` + portal redirect using session user's `premium_stripe_customer_id` only. Validation: `python -m pytest backend/tests/test_billing_portal.py -q`. |
| 2026-06-11 | **SEC-098:** D1 `/admin/users` full CRUD — create/edit/role/scope/password-reset/delete + list suspend/activate; env-admin guards; vector icon actions in `users.html` / `user_form.html`. Validation: `python -m pytest backend/tests/test_admin_users.py -q`. |
| 2026-06-11 | **SEC-097:** B2 password reset scaffold — hashed tokens + expiry in `users.db`, `/forgot-password` + `/reset-password`, anti-enumeration response, OAuth accounts skipped; gated by `PASSWORD_RESET_ENABLED` (default off). Validation: `python -m pytest backend/tests/test_password_reset.py -q`. |
| 2026-06-11 | **SEC-096:** B1 email verification scaffold — hashed verify tokens in `users.db`, `/verify-email` + `POST /resend-verification`, Resend/log delivery, OAuth users auto-verified; gated by `EMAIL_VERIFICATION_ENABLED` (default off). Validation: `python -m pytest backend/tests/test_email_verification.py -q`. |
| 2026-06-12 | **SEC-085:** Dealer locator Google tier public by default in production; optional `DEALER_LOCATOR_REQUIRE_LOGIN=1` restores login gate. `find_dealers.js` retries with `include_google=0` on 403. Validation: `pytest backend/tests/test_production_security.py::test_dealer_locator_google_requires_login_in_production -q`. |
| 2026-06-02 | **SEC-089–095 (June 2026 deep audit remediation):** DNS-aware dev scanner SSRF; attribute-safe trim ladder + dealer link sanitization; enrichment vision URL guard; dev console secret required in all envs + constant-time login; 14-day session lifetime + password max 128; LLM generic errors + untrusted listing delimiters; smart-search GET rate limit; CSP OSM tiles + `object-src 'none'`; Playwright sandbox default; OAuth state timing-safe; `totp_secret` omitted from default user queries. Validation: `bash scripts/security_check.sh`. |
| 2026-05-30 | **SEC-082 / SEC-088:** bcrypt cost 13 default + login rehash for weak/legacy hashes; production requires SQLCipher keys + `sqlcipher3` for `users.db` and `dev_users.db`; `ALLOW_UNENCRYPTED_USER_DB` dev-only. Validation: `bash scripts/security_check.sh`. |
| 2026-05-30 | **SEC-072 / SEC-073 / SEC-031 / SEC-076 (security audit follow-up):** Window sticker PNG preview uses `_require_premium_feature` (login in prod when billing off); compare tray `safeCssBackgroundUrl`; `pip-audit` ignores orphan `chromadb` CVE-2026-45829; redacted demo password from `docs/FULL_AUDIT_MAY2026.md`. Validation: `bash scripts/security_check.sh`. |
| 2026-05-30 | **SEC-087 / SEC-066:** Debug audit remediation — untrack repo-root `debug/`; trim public smart-search `search_meta`; redact sensitive columns on `/dev/api/car-debug` and `/dev/api/audit-last-scrape`; `SCANNER_FAILURE_HAR` default off; always-on systemd/launchd use gunicorn. Validation: `pytest backend/tests/test_debug_audit.py -q`. |
| 2026-05-26 | **SEC-086:** `POST /api/auth/register` + shared `register_general_app_user`; iOS native `RegisterSheet`; cookie bridge `syncFromWebView`. Validation: `pytest backend/tests/test_mobile_auth_register.py -q`. |
| 2026-05-26 | **SEC-080–085 (May 2026 audit remediation):** Hardened `db_admin` (token auth, route rename, SQL column allowlist); production config guards (`DEV_CONSOLE` + secret); app-admin `/dev` pass-through off by default in prod; plaintext login rejected in prod; security headers (HSTS when secure cookies); dealer locator Google requires login in prod. Validation: `bash scripts/security_check.sh`. |
| 2026-05-26 | **SEC-078 / SEC-079:** iOS TLS pinning (Release), WebView response policy, image URL allowlist client + server; `pytest backend/tests/test_safe_listing_url.py backend/tests/test_mobile_auth_api.py -q`. |
| 2026-05-26 | **SEC-078 (partial):** iOS WebView allowlist all navigations; `SessionCookieBridge` sync/clear; logout clears CSRF + cookies; ATS narrowed to localhost exceptions. |
| 2026-05-26 | **SEC-078:** iOS security audit recorded in `ios/docs/SECURITY_AUDIT.md` (WebView redirect policy, URLSession/WK cookie split, ATS local networking). |
| 2026-05-25 | **SEC-013:** `GET /api/saved-cars` login-required JSON for native Saved tab. Validation: `pytest backend/tests/test_saved_cars_api.py -q`. |
| 2026-05-25 | **SEC-077:** Mobile JSON auth (`GET /api/auth/csrf`, `GET /api/auth/me`, `POST /api/auth/login`, `POST /api/auth/logout`) with CSRF header on mutations and login rate limit. **SEC-011 / SEC-013:** `_csrf_mutating_requests` includes `api_auth_login` / `api_auth_logout`; authorization table + `GET /api/cars/<id>` public detail JSON. Validation: `pytest backend/tests/test_mobile_auth_api.py backend/tests/test_car_detail_api.py -q`. |
| 2026-05-25 | **SEC-076:** Pinned transitive dependency floors in `requirements.txt`; `scripts/security_check.sh` for pytest + `pip-audit`; `db_admin.py` binds `127.0.0.1` and disables Flask debug unless `DB_ADMIN_DEBUG=1`. **SEC-013:** `/compare` login-required documented; `test_compare_specs` uses session. **lxml** CVE blocked on `crawl4ai` `lxml~=5.3` until upstream update. Validation: `bash scripts/security_check.sh`. |
| 2026-05-25 | **SEC-066:** Consumer Premium checkout switched from one-time payment to recurring subscription; premium webhook grants/revokes on subscription lifecycle. Validation: `pytest backend/tests/test_premium_checkout.py backend/tests/test_stripe_premium_verify.py -q`. |
| 2026-05-23 | **SEC-075 (follow-up):** After Google sign-in, new users redirect to `/premium` with upgrade/skip banner; checkout still ties via `user_id`. `GOOGLE_OAUTH_OFFER_PREMIUM_ON_LOGIN`, `/auth/google?intent=premium`. Validation: `pytest backend/tests/test_google_oauth.py -q`. |
| 2026-05-22 | **Compare page + chat:** `/compare` full spec table with diff highlighting; `POST /api/compare/chat` (premium, CSRF, same rate limits as car chat). **SEC-041** scope updated. Validation: `pytest backend/tests/test_compare_specs.py backend/tests/test_car_chat_policy.py -q`. |
| 2026-05-22 | **SEC-071–074 (audit fixes):** Email lowercase on register + duplicate check; sticker preview/OEM URLs require paid access when billing on; production login required for chat/packages APIs when billing off; block admin self-registration via env lists; per-user chat daily cap; `CAR_CHAT_WEB_RESEARCH_PUBLIC` dev-only; dealer website URL sanitization on car detail. Validation: `pytest backend/tests/test_registration_email.py backend/tests/test_audit_fixes.py backend/tests/test_billing_gate.py backend/tests/test_car_chat_policy.py -q`. |
| 2026-05-22 | **Registration E2E + full audit:** Verified `/register` → `/dashboard` for free accounts (headless + DB). Report: `docs/FULL_AUDIT_MAY2026.md`. New follow-ups: **SEC-071** (email case), **SEC-072** (sticker preview gate), **SEC-073** (login for APIs when billing off in prod). |
| 2026-05-22 | **SEC-068, SEC-069:** Dev scanner URLs validated (`outbound_url.py`; blocks private/loopback hosts); window sticker fetch limited to OEM candidate URLs per VIN. **SEC-041:** `test_car_chat_global_rate_limit` sets `BILLING_STRIPE_ENABLED=0` before app import. Validation: `pytest backend/tests/test_outbound_url.py backend/tests/test_window_sticker_urls.py backend/tests/test_car_chat_policy.py -q`. |
| 2026-05-22 | **SEC-011, SEC-031, SEC-066, SEC-067:** Login CSRF failure returns redirect (propagated from `before_request`); listings dealer filter escaped; premium success requires verified Stripe Checkout Session; car chat + `/api/nearby-dealers` gated when `BILLING_STRIPE_ENABLED=1`. Tests: `test_premium_checkout.py`, `test_app_security_basics` login CSRF. |
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
| 2026-05-06 | **SEC-041 (hardening):** Layered chat rate limits + optional global budget; production disables Playwright web research for anonymous sessions (`CAR_CHAT_WEB_RESEARCH` / `CAR_CHAT_WEB_RESEARCH_PUBLIC`); structured logging in `agent.py` / `web_researcher.py` (no full user message at INFO); optional `WEB_RESEARCH_ALLOWED_HOSTS` + SSRF-style host blocks on research URLs; validation runbook note under **How to use**. Validation: `python -m pytest backend/tests/test_app_security_basics.py backend/tests/test_car_chat_policy.py -q`. |
| 2026-05-22 | **SEC-014:** App/dealer **2FA removed** — password-only auth; `_finalize_app_session` sets `mfa_ok`; legacy `/mfa/*` redirect; dealer register/login use `_post_login_redirect` for billing; store admin no longer checks `mfa_ok`. **SEC-063:** QR MFA retired (redirect tests). **SEC-061:** register no longer writes `*.mfa_start` to MFA action log. **SEC-011:** `POST /api/cars/<id>/save` returns **404** for unknown car id. **SEC-041 / premium UX:** `car.html` hides chat UI when billing on and unpaid; `car_chat.js` surfaces `premium_required`. Validation: `python -m pytest backend/tests/test_mfa_totp.py backend/tests/test_mfa_qr.py backend/tests/test_mfa_action_log.py backend/tests/test_billing_gate.py -q`. |
| 2026-05-22 | **SEC-056 (retired):** Removed KBB IDWS client, dev `kbb-refresh`, post-scan `--post-kbb`, and UI/marketing copy; pricing insights use similar-listing `market_intel` only. **SEC-013** dev surface list + env table updated. Validation: `rg -i "kbb_idws|KBB_API_KEY|kbb-refresh|Kelley Blue" backend frontend/templates` (gallery junk filters excluded). |
| 2026-06-02 | **SEC-041:** Removed premium negotiation script generator (`negotiation_script` branch, Ollama `run_negotiation_script`, VDP UI). Car chat endpoint unchanged for normal `message` requests. Validation: `python -m pytest backend/tests/test_app_security_basics.py backend/tests/test_car_chat_policy.py backend/tests/test_compare_specs.py -q`. |
| 2026-06-02 | **SEC-082 / SEC-089:** Login rehash updates only when stored hash unchanged (`password = ?` guard); public listings fail closed when incomplete index errors; sidecar index mtime in listings cache token; atomic incomplete rebuild + dev stats join. Validation: `python -m pytest backend/tests/test_incomplete_listings_index.py backend/tests/test_password_hash.py -q`. |
| 2026-06-25 | **Site admin operator merge:** `/admin/data-quality` (incomplete listings grid + dealer issue stats) and `/admin/scanner-ops` (smart import, registry maintenance) for app `role=admin`; JSON at `/api/admin/operator/*` with CSRF header on mutations. Legacy `/dev` unchanged. Validation: `python -m pytest backend/tests/test_admin_operator_api.py -q`. |
| 2026-09-21 | **SEC-063 retired:** Flask-SocketIO removed (`backend/main.py`, `run.py`, `requirements.txt`, `Config.socketio_cors_origins_raw`). It had zero handlers, zero emits and no frontend consumer; `/socket.io/*` no longer exists, `SOCKETIO_CORS_ORIGINS` is ignored. |

**Done items** stay in their phase table with **Status: Done** and **Last verified** — do not duplicate into a second list.

---

## Future: non-security engineering backlog

Security work **must** stay in this file until all Phase 1–6 items are `Done` or explicitly `Won't do` with rationale in Changelog.  
Go-live items that are not open security work (status is tracked on the row): **SEC-099** Stripe Customer Portal: the owner configures the portal in the Stripe dashboard before billing goes live.  
CSP: enforced in production (nonces; `CSP_ENFORCE=0` to disable). Use `CSP_REPORT_ONLY=1` when enforcement is off to collect additional violations. Optional follow-up: remove `style-src` `unsafe-inline` by moving inline `style=""` to CSS. For general features, consider `docs/ENGINEERING_MASTER_TODO.md` separately.

