# Sarrafi Cars — Platform Master Plan

**Status:** Approved direction — implementation in progress  
**Last updated:** 2026-06-14  
**Scope:** Local Docker → Kubernetes production; consumer site; operator scraping; user management; feature-based billing.

---

## 1. Vision

Sarrafi Cars is a **consumer-facing** vehicle discovery product backed by an **operator-controlled inventory pipeline**.

| Audience | Experience |
|----------|------------|
| **Consumers** | Browse, search, compare, and research cars; self-signup; subscribe to feature packages |
| **Site admins** | Discover dealerships, onboard + scrape inventory, set refresh schedules |
| **Dealer orgs** | Portal billing track (separate from consumer plans) |

Revenue comes from **tiered feature packages** with **discounts and trials**, not a single binary “Premium” flag.

**Development strategy:** Build and validate **locally in Docker** (Postgres + web + scanner workers in one compose group), then deploy **web to Railway** and **scanner fleet to Kubernetes**.

---

## 2. Locked decisions

| # | Decision | Choice |
|---|----------|--------|
| 1 | Dev environment | Docker Compose locally first |
| 2 | Inventory database | **Postgres** in its own container (not SQLite in prod) |
| 3 | App auth DB | **SQLite** (`users.db`) for now — migrate later if needed |
| 4 | Scraper architecture | **Separate worker containers**; job queue; scale horizontally |
| 5 | Parallelism target | 100+ simultaneous dealer scrapes via worker replicas |
| 6 | Per-dealer catalog | Deterministic scrape paths; VIN fingerprint deltas; minimal LLM/token use |
| 7 | Admin dealer UX | **`/admin/dealers` hub** (map embed + job queue + intervals) |
| 8 | Billing model | **Bundled tiers first**; à la carte later if data supports it |
| 9 | Discounts | Stripe promotion codes + env-mapped coupons + plan trials |
| 10 | OAuth providers | **Google** (live) + **Apple** (scaffold) on login/register |
| 11 | Free tier | Anonymous browse listings; login for save/compare; paid for AI/stickers |
| 12 | Production split | Railway = web + Postgres + scanner workers + scheduler |

---

## 3. Architecture overview

```mermaid
flowchart TB
  subgraph consumers [Consumers]
    WEB[Web + iOS]
  end

  subgraph identity [Identity and billing]
    USERS[(users.db SQLite)]
    PG[(Postgres)]
    STRIPE[Stripe]
  end

  subgraph operators [Operators]
    ADMIN["/admin"]
    WORKERS[scanner-worker x N]
    SCHED[scanner-scheduler]
  end

  WEB --> USERS
  WEB --> PG
  WEB --> STRIPE
  ADMIN --> PG
  ADMIN -->|enqueue| JOBS[(dealer_jobs)]
  SCHED --> JOBS
  WORKERS --> JOBS
  WORKERS --> PG
  WORKERS --> CAT[dealer_scan_registry]
```

### Local Docker Compose (target stack)

| Service | Image | Responsibility |
|---------|-------|----------------|
| `postgres` | `postgres:16-alpine` | Inventory, dealerships, jobs, catalog, entitlements |
| `web` | `Dockerfile.web` | Flask — consumer UI, admin UI, billing, job enqueue |
| `scanner-worker` | `Dockerfile.scanner` | Claim one `dealer_jobs` row → scrape → upsert Postgres |
| `scanner-scheduler` | `Dockerfile.scanner` | Enqueue refresh jobs when `dealer_scan_registry.next_scan_at` due |

**Shared volume `/data`:** `users.db`, `dev_users.db`, VDP images, PDFs. Inventory **only** in Postgres.

```bash
# Target local usage
docker compose -f deploy/docker-compose.yml up -d postgres web scanner-scheduler
docker compose -f deploy/docker-compose.yml up -d --scale scanner-worker=10
```

### Production mapping

| Component | Local Docker | Production |
|-----------|--------------|------------|
| Web | `web` container | Railway `web` service |
| Postgres | `postgres` container | Railway Postgres |
| Scanner workers | `--scale scanner-worker=N` | Railway `scanner-worker` (scale replicas) |
| Scheduler | `scanner-scheduler` | Railway `scanner-scheduler` |
| Secrets | kmac vault (`host.docker.internal`) | kmac-vault Railway + env mirror |

**Current production (Railway):** Web healthy; inventory still SQLite on `/data` until Railway Postgres + `INVENTORY_DATABASE_URL` deploy. Postgres job queue and scanner workers not in prod. Dealer-locator geocoding may fail (`location_not_found`) without `GOOGLE_MAPS_API_KEY`.

---

## 4. Implementation status

### Done ✅

| Area | What |
|------|------|
| Consumer auth | Password register/login, profile edit, Google OAuth, CSRF, bcrypt |
| Apple OAuth scaffold | `backend/auth/apple_oauth.py`, `_oauth_signin.html`, `users.apple_sub` |
| Billing scaffold | `catalog.py`, `discounts.py`, `checkout_service.py`, `entitlements.py` |
| Plan routes | `/billing/plans`, `/billing/plan/checkout`, `/billing/plan/success` |
| Plan DB column | `users.subscription_plan_id` |
| Legacy billing | Single Premium + org Stripe tracks still work |
| Find dealers | Public map + `/api/dealer-locator` |
| Dev scrape ops | Smart Import, manifest editor, scan-dealer |
| Dealer profile cache | `dealer_scan_profile` (winning recovery strategy) |
| Postgres path | `INVENTORY_DATABASE_URL`, parallel upsert, migration script |
| Postgres-only runtime | SEC-102 — no SQLite inventory fallback (pytest escape hatch only) |
| Railway deploy | Web container, vault bootstrap, admin bootstrap |
| Docker full stack | `./deploy/up.sh` — postgres + web + workers + scheduler |
| Feature gates (C1) | `_require_feature()` on gated APIs; `session["entitlements"]` |
| Premium plan UI (C2) | `/premium` multi-plan cards → `billing.plan_checkout` |
| Per-feature upsell (C4) | 403 responses include `upgrade_plan_*` hints |
| `/admin/dealers` scaffold (A4) | Job queue UI, onboard form, catalog list |
| Email verification scaffold (B1) | `EMAIL_VERIFICATION_ENABLED` (default off) |
| Password reset scaffold (B2) | `PASSWORD_RESET_ENABLED` (default off) |
| Stripe Customer Portal scaffold (C3) | `/account/billing` + portal redirect |
| Site-admin users CRUD (D1) | `/admin/users` — create/edit/role/scope/reset/suspend/delete (SEC-098) |
| Platform dashboard (SEC-101) | `/admin/site` — health, scale, search analytics |

### In progress / next 🔨

| Priority | Phase | Deliverable |
|----------|-------|-------------|
| P0 | Infra | Local Docker stack running + SQLite→Postgres migration |
| P0 | Infra | Railway Postgres plugin + `INVENTORY_DATABASE_URL` on web |
| P1 | B5 | Apple JWKS signature verification |
| P1 | B3 | Account delete + GDPR soft-delete |
| P2 | D2 | Manual entitlement grants UI |
| P2 | D3 | Org invites flow |
| P2 | C5 | À la carte add-on checkout |
| P2 | C6 | AI chat usage metering per tier |
| P2 | A5 | K8s worker Deployment + scheduler CronJob |

### Not started ⏳

- Enable B1/B2 in production (`EMAIL_VERIFICATION_ENABLED`, `PASSWORD_RESET_ENABLED`)
- B4 Terms acceptance on register
- `/account/features` à la carte toggles
- Upload local inventory to Railway after Postgres migration
- Fix dealer-locator geocoding on Railway (vault `GOOGLE_MAPS_API_KEY`)

---

## 5. Pillar A — Scraper platform (dealer onboarding agent)

### 5.1 Current state

| Piece | Status |
|-------|--------|
| `/find-dealers` + `/api/dealer-locator` | ✅ Public |
| Smart Import (`POST /dev/api/smart-import`) | ✅ Dev-only |
| `dealer_scan_profile` | ✅ Strategy cache |
| Dealer.com bulk API (Irvine BMW) | ✅ Deterministic list fetch |
| K8s cron (whole manifest, 4h) | ✅ Exists; replace with job queue |
| Locator → scrape bridge | ✅ Site admin **Request scrape** on Find dealers (Google-only) |
| Per-dealer schedule | ❌ Global cron only |
| Postgres inventory (local) | ✅ Required (`INVENTORY_DATABASE_URL`); SEC-102 |
| Postgres inventory (Railway) | ❌ Not deployed yet |

### 5.2 Target workflow

```mermaid
sequenceDiagram
  participant Admin
  participant Web
  participant Queue as dealer_jobs
  participant Worker
  participant Cat as dealer_scan_registry
  participant PG as Postgres

  Admin->>Web: Find dealer on map → Add & scan
  Web->>Queue: INSERT job (onboard)
  Worker->>Queue: CLAIM job
  Worker->>Worker: Smart Import + scrape
  Worker->>Cat: Write provider, API endpoint, VIN fingerprint
  Worker->>PG: Upsert cars
  Worker->>Queue: status=done
  Note over Worker,PG: Refresh jobs repeat using catalog (delta VINs only)
```

### 5.3 `dealer_scan_registry` table

| Column | Purpose |
|--------|---------|
| `dealer_id` | Manifest slug (PK) |
| `registry_id` | FK → `dealerships.id` |
| `provider` | `dealer_dot_com`, `dealer_on`, `dealer_inspire`, … |
| `inventory_mode` | `api_bulk`, `algolia`, `intercept`, `html` |
| `inventory_endpoint` | Cached API URL or index name |
| `site_config_json` | Provider IDs, VDP template, Algolia params |
| `last_vin_fingerprint` | SHA of sorted VIN list |
| `scan_interval_hours` | Admin-set (default 24) |
| `next_scan_at` | Scheduler input |
| `onboarded_at`, `last_scan_at` | Audit |

Extends existing `dealer_scan_profile` (recovery strategy cache).

### 5.4 `dealer_jobs` table

| Column | Purpose |
|--------|---------|
| `id` | PK |
| `dealer_id` | Target |
| `job_type` | `onboard`, `refresh`, `rescan` |
| `status` | `queued` → `claimed` → `running` → `done` / `failed` |
| `payload_json` | URL, name, address, lat/lon from locator |
| `worker_id`, timestamps, `error`, `result_json` | Ops |

Workers claim with `SELECT … FOR UPDATE SKIP LOCKED`.

### 5.5 Scan phases (deterministic, low tokens)

1. **List:** Use `dealer_scan_registry` path — API/intercept, no LLM
2. **Delta:** Diff VIN fingerprint → VDP fetch only for new/changed
3. **Reconcile:** Mark missing VINs `listing_active = 0`
4. **Enrich (optional):** Vision/gap-fill for changed rows only

### 5.6 Phases

| Phase | Deliverable | Status |
|-------|-------------|--------|
| A1 | Postgres container + `up.sh --full` | ✅ Verified 2026-06-13 |
| A2 | `dealer_jobs` + `dealer_scan_registry` + worker/scheduler | ✅ Scaffold running |
| A3 | `scanner-scheduler`; per-dealer `scan_interval_hours` | ✅ Scaffold (`record_dealer_scan_registry`) |
| A4 | `/admin/dealers` — discover, queue, status, interval config | ✅ Scaffold |
| C4 | Per-feature upsell on 403 (`upgrade_plan_*`) | ✅ |
| A5 | K8s worker Deployment + scheduler CronJob | ⏳ |

---

## 6. Pillar B — Consumer identity and self-signup

### 6.1 Current state

| Capability | Location | Status |
|------------|----------|--------|
| Password register | `/register`, `POST /api/auth/register` | ✅ |
| Google OAuth | `/auth/google` (`GOOGLE_OAUTH_*`) | ✅ |
| Apple Sign In | `/auth/apple` (`APPLE_OAUTH_*`) | 🔨 Scaffold |
| OAuth UI | `_oauth_signin.html` on login/register | ✅ |
| Login / session | `/login`, 14-day cookie, CSRF | ✅ |
| Profile | `/account/profile` | ✅ |
| Mobile API | `/api/auth/me`, register, login | ✅ |
| iOS parity | `is_premium`, `has_paid_access` | ✅ |

### 6.2 OAuth configuration

| Provider | Env vars |
|----------|----------|
| **Google** | `GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET`, `GOOGLE_OAUTH_SHOW_BUTTON=1` |
| **Apple** | `APPLE_OAUTH_CLIENT_ID`, `APPLE_OAUTH_TEAM_ID`, `APPLE_OAUTH_KEY_ID`, `APPLE_OAUTH_PRIVATE_KEY`, `APPLE_OAUTH_SHOW_BUTTON=1` |

Dependency: `PyJWT[crypto]>=2.8` (in `requirements.txt`).

### 6.3 Gaps

| Gap | Priority |
|-----|----------|
| Apple `id_token` JWKS signature verification | P1 |
| Email verification | P1 |
| Password reset | P1 |
| Account delete (GDPR) | P2 |
| `is_active` suspension | P2 |
| Terms acceptance on register | P2 |
| Sign out all devices | P3 |

### 6.4 Planned `users.db` columns

```
email_verified_at, email_verify_token_hash,
password_reset_token_hash, password_reset_expires_at,
is_active, terms_accepted_at, marketing_opt_in, session_version
```

(Already added: `apple_sub`, `subscription_plan_id`, `google_sub`)

### 6.5 Phases

| Phase | Deliverable | Status |
|-------|-------------|--------|
| B0 | Apple + Google OAuth buttons, `apple_sub` column | ✅ Scaffold |
| B1 | Email verification (Resend) | ✅ Scaffold (`EMAIL_VERIFICATION_ENABLED`) |
| B2 | Password reset | ✅ Scaffold (`PASSWORD_RESET_ENABLED`) |
| B3 | Account delete + `is_active` | ⏳ |
| B4 | Terms acceptance | ⏳ |
| B5 | Apple JWKS hardening | ⏳ |

---

## 7. Pillar C — Feature-based billing (packages + discounts)

### 7.1 Subscription packages

| Plan ID | Name | Price | Trial | Features |
|---------|------|-------|-------|----------|
| `free` | Free | $0 | — | Browse, search, save cars (logged in) |
| `research` | Research | $4.99/mo | 7 days | `market_intel`, `vehicle_history`, `nearby_dealers` |
| `assistant` | Assistant | $9.99/mo | 7 days | Research + `ai_car_chat`, `ai_compare_chat` |
| `complete` | Complete | $14.99/mo | 14 days | All features incl. `window_sticker`, `packages_ensure` |
| `dealer_org` | Dealer portal | Custom | — | Org Stripe track (existing) |

**Code:** `backend/billing/catalog.py`

### 7.2 Feature → API mapping

| Feature ID | Gated surface |
|------------|---------------|
| `ai_car_chat` | `POST /api/car/<id>/chat` |
| `ai_compare_chat` | `POST /api/compare/chat` |
| `window_sticker` | Window sticker PDF + PNG preview |
| `vehicle_history` | Vehicle history intelligence |
| `market_intel` | Market stats + trim ladder |
| `nearby_dealers` | Dealership picker API |
| `packages_ensure` | Window sticker fetch + packages |

**Today:** All gates use binary `_session_has_paid_access()`.  
**Target:** `_require_feature("ai_car_chat")` via `backend/billing/entitlements.py`.

### 7.3 Discount mechanisms

| Mechanism | How |
|-----------|-----|
| Stripe Promotion Codes | User enters code at Checkout (`allow_promotion_codes=True`) |
| Pre-applied coupons | `?promo=LAUNCH20` → `BILLING_PROMO_LAUNCH20` env → Stripe Coupon |
| Plan trials | `trial_period_days` from catalog |
| Admin grants | Manual entitlement override (Phase D2) |

**Code:** `backend/billing/discounts.py`

### 7.4 Stripe env vars

| Variable | Purpose |
|----------|---------|
| `BILLING_STRIPE_ENABLED` | Master switch |
| `STRIPE_SECRET_KEY` | API key |
| `STRIPE_PRICE_RESEARCH` | Research plan price |
| `STRIPE_PRICE_ASSISTANT` | Assistant plan price |
| `STRIPE_PRICE_COMPLETE` | Complete plan price |
| `STRIPE_PREMIUM_PRICE_ID` | Legacy fallback for `complete` |
| `STRIPE_PREMIUM_WEBHOOK_SECRET` | Consumer webhook |
| `STRIPE_WEBHOOK_SECRET` | Org webhook |
| `BILLING_PROMO_<CODE>` | Promo code → Coupon id |
| `PUBLIC_BASE_URL` | Checkout redirect URLs |

### 7.5 Billing routes

| Route | Purpose | Status |
|-------|---------|--------|
| `GET /billing/plans` | JSON plan catalog | ✅ |
| `GET /billing/plan/checkout?plan=&promo=` | Multi-plan Checkout | ✅ |
| `GET /billing/plan/success` | Verify + grant plan | ✅ |
| `GET /billing/premium/checkout` | Legacy single Premium | ✅ |
| `POST /billing/premium/webhook` | Grant/revoke + `plan_id` | ✅ |
| Stripe Customer Portal | Self-serve cancel/update card | ⏳ |

### 7.6 Entitlement resolution (target)

1. Site `admin` → all features
2. `user_entitlement_overrides` (manual grants)
3. `subscription_plan_id` → plan → features (from catalog)
4. Legacy `is_premium = 1` → `complete` plan
5. Active org subscription → all consumer features (unchanged)

### 7.7 Phases

| Phase | Deliverable | Status |
|-------|-------------|--------|
| C0 | Catalog, discounts, checkout_service, entitlements scaffold | ✅ |
| C1 | Wire `_require_feature()` on all gated APIs; `session["entitlements"]` | ✅ |
| C2 | `/premium` pricing page with plan cards | ✅ |
| C3 | Stripe Customer Portal in `/account/billing` | ✅ Scaffold |
| C4 | Per-feature upsell UI (403 → “Upgrade to Research”) | ✅ |
| C5 | À la carte add-on checkout | ⏳ |
| C6 | AI chat usage metering per tier | ⏳ |

---

## 8. Pillar D — User and org administration

### 8.1 Current state

- Site admin (`role=admin`) bypasses billing — no user CRUD UI
- Store admin `/admin` = inventory KPIs (requires manual `dealer_id` assignment)
- Dev admin `/dev` = scanner tools (separate `dev_users.db`)
- `org_invites` table exists — no routes

### 8.2 Target `/admin` console

| Section | Actions |
|---------|---------|
| **Users** (`/admin/users`) | List, search, suspend, change role, grant entitlements, assign dealer scope |
| **Orgs** (`/admin/orgs`) | List orgs, Stripe status, create/revoke invites, add members |
| **Dealers** (`/admin/dealers`) | Map discover, Add & scan, job queue, catalog edit, scan interval, inventory health |

### 8.3 Consumer account pages

| Page | Purpose | Status |
|------|---------|--------|
| `/account/profile` | Username, email, password | ✅ |
| `/account/billing` | Plan, features, upgrade, Stripe Portal | ✅ Scaffold |
| `/account/features` | À la carte toggles | ⏳ |

### 8.4 Phases

| Phase | Deliverable | Status |
|-------|-------------|--------|
| D1 | `/admin/users` list + suspend + role | ✅ Scaffold (suspend) |
| D2 | Manual entitlement grants | ⏳ |
| D3 | Org invites flow | ⏳ |
| D4 | `/account/billing` + Customer Portal | ✅ Scaffold |
| D5 | `/admin/dealers` (with A4) | ⏳ |

---

## 9. Pillar E — Security and compliance

Track in `docs/SECURITY_MASTER_TODO.md`:

| Area | Notes |
|------|-------|
| Admin user CRUD | SEC-093+ — CSRF, audit log, no self-demotion |
| Apple OAuth | SEC-094+ — JWKS verify, state param, rate limits |
| Entitlement grants | Webhook-verified Stripe only; admin manual grants |
| Email/reset tokens | Hashed, expiring, rate-limited |
| Scanner workers | No public HTTP; vault credentials |
| Job queue | Admin-only enqueue |
| Feature gates | `feature_required` + `feature_id` in 403 JSON |
| Account delete | Soft-delete + PII anonymization |

Existing: SEC-059 (org billing), SEC-066 (premium checkout verify), SEC-067 (premium API gates), SEC-075 (Google OAuth), SEC-085 (dealer-locator login gate).

---

## 10. Master implementation roadmap

### Sprint 1 — Docker foundation (start here)

| # | Task | Pillar | Files |
|---|------|--------|-------|
| 1 | Add `postgres` service to `deploy/docker-compose.yml` | A1 | `docker-compose.yml`, `up.sh` |
| 2 | Set `INVENTORY_DATABASE_URL` on web + scanner services | A1 | `docker-compose.yml` |
| 3 | Init Postgres schema on startup | A1 | `inventory_pg.py` |
| 4 | Optional: migrate local SQLite → Postgres | A1 | `migrate_inventory_sqlite_to_postgres.py` |
| 5 | Add `scanner-worker` + `scanner-scheduler` services | A2 | `docker-compose.yml`, entrypoint scripts |

### Sprint 2 — Job queue + billing wire-up

| # | Task | Pillar |
|---|------|--------|
| 6 | Create `dealer_jobs` + `dealer_scan_registry` tables | A2 |
| 7 | Worker claim loop (reuse Smart Import) | A2 |
| 8 | Wire `_require_feature()` on gated APIs | C1 |
| 9 | Redesign `/premium` with plan cards | C2 |

### Sprint 3 — Admin + identity

| # | Task | Pillar |
|---|------|--------|
| 10 | `/admin/dealers` UI | A4, D5 |
| 11 | Scheduler service | A3 |
| 12 | Email verification | B1 |
| 13 | Password reset | B2 |
| 14 | `/admin/users` list | D1 |

### Sprint 4 — Production

| # | Task | Pillar |
|---|------|--------|
| 15 | Railway Postgres plugin | Infra |
| 16 | Fix dealer-locator geocoding on Railway | Infra |
| 17 | K8s worker Deployment | A5 |
| 18 | Stripe Customer Portal | C3 |
| 19 | Apple JWKS hardening | B5 |
| 20 | Upload local inventory to Railway | Infra |

---

## 11. Local development reference

```bash
# Full stack (Postgres required)
./deploy/up.sh
WEB_PORT=8000 ./deploy/up.sh

# Host gunicorn against Docker Postgres on :5432
./scripts/run-web-local.sh

# Scale workers
docker compose -f deploy/docker-compose.yml up -d --scale scanner-worker=10

# Migrate inventory SQLite → Postgres (one-time)
INVENTORY_DB_PATH=data/runtime/inventory.db \
INVENTORY_DATABASE_URL=postgresql://dealership:dealership@127.0.0.1:5432/dealership \
PYTHONPATH=. .venv/bin/python backend/scripts/migrate_inventory_sqlite_to_postgres.py

# Billing (local test)
BILLING_STRIPE_ENABLED=1 \
STRIPE_SECRET_KEY=sk_test_... \
STRIPE_PRICE_ASSISTANT=price_... \
./deploy/up.sh

# Promo checkout
open "http://localhost:18000/billing/plan/checkout?plan=assistant&promo=LAUNCH20"
```

---

## 12. Production status (Railway)

| Item | State |
|------|-------|
| URL | https://web-production-26b11.up.railway.app |
| Web health | ✅ 200 |
| Dealer-locator API | ⚠️ Needs `GOOGLE_MAPS_API_KEY` in vault for geocoding |
| Inventory | Empty on Railway until Postgres migration |
| Postgres | **Required locally (SEC-102); not yet on Railway** |
| Scanner workers | Not deployed (local Docker only) |
| Local changes | Large uncommitted diff; not pushed to Railway |
| Site admin | `asarrafi` bootstrapped via entrypoint |

---

## 13. Success metrics

| Metric | Target |
|--------|--------|
| Dealer onboard time | < 15 min from “Add & scan” to listings visible |
| Refresh latency | 95% of scheduled refreshes start within 5 min of `next_scan_at` |
| Scanner efficiency | > 70% refreshes use cached catalog (no rediscovery) |
| Signup completion | > 80% verify email within 24h |
| Billing conversion | register → plan page → checkout → active |
| Feature attach rate | Track `feature_required` 403s by `feature_id` |
| OAuth adoption | % signups via Google vs Apple vs password |

---

## 14. Code index (billing + auth scaffold)

```
backend/billing/catalog.py          # Plan definitions + feature bundles
backend/billing/discounts.py        # Promo codes + coupon mapping
backend/billing/checkout_service.py # Multi-plan Stripe Checkout
backend/billing/entitlements.py     # Per-feature gate scaffold
backend/billing/stripe_billing.py   # Legacy org + premium Stripe
backend/billing/routes.py           # All billing routes
backend/auth/google_oauth.py        # Google Sign In
backend/auth/apple_oauth.py         # Apple Sign In scaffold
frontend/templates/_oauth_signin.html
backend/tests/test_billing_catalog.py
backend/tests/test_apple_oauth.py
```

---

## 15. Related docs

| Doc | Purpose |
|-----|---------|
| `docs/INVENTORY_POSTGRES.md` | Postgres migration guide |
| `docs/SECURITY_MASTER_TODO.md` | Security contract |
| `deploy/AUTONOMOUS_DEV.md` | Loop commands + vault key map for unattended dev |
| `deploy/railway/README.md` | Railway deploy + vault |
| `deploy/k8s/cronjob-scanner-sharded.yaml` | K8s scanner reference |

---

## Changelog

| Date | Change |
|------|--------|
| 2026-06-14 | SEC-102 Postgres-only inventory; platform dashboard (SEC-101); admin users CRUD (SEC-098); master plan status refresh |
| 2026-06-13 | Initial master plan |
| 2026-06-13 | Locked: local Docker + Postgres container + K8s prod |
| 2026-06-13 | Added billing scaffold (packages, discounts, plan checkout) |
| 2026-06-13 | Added Apple OAuth scaffold; updated roadmap with implementation status |
| 2026-06-13 | Added production status, sprint-based roadmap, code index |

---

*Next step: P0 — bring up local Postgres stack, migrate SQLite inventory, then Railway Postgres plugin.*
