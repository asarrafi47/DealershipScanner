# Sarrafi Cars

**Version 0.2.0** · Consumer vehicle discovery backed by a live dealership inventory pipeline.

| | |
|---|---|
| **Production** | [web-production-26b11.up.railway.app](https://web-production-26b11.up.railway.app) |
| **Repository** | [github.com/asarrafi47/DealershipScanner](https://github.com/asarrafi47/DealershipScanner) |
| **Health** | `GET /health` → `{"status":"ok","version":"0.2.0"}` |

---

## For business leaders

### What this is

Sarrafi Cars helps people **find, compare, and research vehicles** using inventory pulled directly from dealership websites—not stale third-party feeds. Behind the public site, operators run a **controlled scraping and refresh system** so listings stay current and trustworthy.

Think of it as two products in one:

| Audience | What they get |
|----------|----------------|
| **Car shoppers** | Search, maps, vehicle detail pages, saved cars, and paid features (AI research, stickers, comparisons) |
| **Your team** | Tools to discover dealers, onboard them, schedule refreshes, and see job results in plain language |

### Why it matters

- **Fresh inventory** — Each dealer can be scraped on a schedule; shoppers see when data was last updated and how many cars are in stock.
- **One platform** — Consumer website, admin operations, billing, and mobile app share the same data and accounts.
- **Revenue-ready** — Tiered packages and Stripe integration are built in; free browsing, paid upgrades for premium tools.
- **Operator visibility** — The admin dealer hub shows map context, job status, car counts, and clear failure reasons instead of opaque error codes.

### What is live today (v0.2.0)

- Public site and **Find dealers** (ZIP search, map + list, distance, listing counts, last-synced timestamps).
- **Admin dealer hub** at `/admin/dealers` — discover, onboard, queue scrapes, set refresh intervals.
- **Postgres inventory** — cars, dealer catalog, job queue, and search indexes in production.
- **Full scrape fleet on Railway** — web app, database, background workers, and scheduler (no separate “scraping only runs locally” split).
- **Secrets via kmac vault** — API keys and encryption material are not baked into the repo or checked into config files.
- **Native iOS app** (SwiftUI) — documented separately; shares mobile API contracts with the web backend.

### How we make money (direction)

Bundled **feature tiers** first (browse free; login for save/compare; paid for AI and advanced tools). Stripe handles subscriptions, trials, and promotion codes. Dealer-portal billing is a separate track for B2B accounts.

### What is still in motion

- Scaling worker replicas and `/data` volumes on Railway for heavy scrape loads.
- Clearing or requeuing legacy failed jobs from early production runs.
- Redis for multi-replica web sessions and QR MFA in production.
- Continued platform coverage for dealer website templates (some sites still return zero cars until parser rules are extended).

---

## For engineers and operators

### Architecture

```mermaid
flowchart TB
  subgraph consumers [Consumers]
    WEB[Flask web + static UI]
    IOS[iOS Sarrafi Cars]
  end

  subgraph identity [Identity and billing]
    USERS[(users.db SQLite on /data)]
    PG[(Postgres inventory)]
    VAULT[kmac vault]
    STRIPE[Stripe]
  end

  subgraph operators [Operators]
    ADMIN["/admin/dealers hub"]
    SCHED[scanner-scheduler]
    WORKERS[scanner-worker x N]
  end

  WEB --> USERS
  WEB --> PG
  WEB --> VAULT
  WEB --> STRIPE
  IOS --> WEB
  ADMIN --> PG
  ADMIN -->|enqueue| JOBS[(dealer_jobs)]
  SCHED --> JOBS
  WORKERS --> JOBS
  WORKERS --> PG
  WORKERS --> VAULT
  SCHED --> VAULT
```

| Layer | Technology | Notes |
|-------|------------|--------|
| Web | Python 3.12, Flask, Gunicorn | Consumer UI, admin, REST + mobile APIs |
| Inventory | PostgreSQL 16 | `cars`, `dealer_catalog`, `dealer_jobs`, pgvector embeddings |
| App auth | SQLite (SQLCipher-capable) | `users.db` on persistent `/data` volume |
| Scraper | Python `scanner.py`, Node `scanner.js`, Playwright, Puppeteer | Per-dealer jobs from `dealer_jobs` queue |
| Search | SQL + pgvector hybrid | See `docs/LISTINGS_PGVECTOR_SEARCH.md` |
| Secrets | kmac vault (`Dealer:*` keys) | `KMAC_VAULT_AUTO=1`; no secrets in git |
| Production | Railway | Project `dealership-scanner`: web, Postgres, scanner-worker, scanner-scheduler |
| Local | Docker Compose | `./deploy/up.sh` — Postgres + web + workers + scheduler |

### Repository layout (high level)

```
backend/           Flask app, listings, scanner job queue, billing, admin
frontend/          Templates + static JS/CSS
scripts/           Entrypoints, scanner_worker_loop.py, scheduler, migrations
deploy/            docker-compose, Railway scripts, up.sh
ios/               Native SwiftUI client
docs/              Security master todo, platform plan, Postgres inventory
scanner.py         CLI inventory scrape (cron / manual)
```

### Local development (recommended)

**Prerequisites:** Docker, kmac vault reachable (local or `host.docker.internal:9999`), Railway CLI only for production ops.

```bash
git clone https://github.com/asarrafi47/DealershipScanner.git
cd DealershipScanner

# Full stack: Postgres + web + 2 scanner workers + scheduler
./deploy/up.sh

# App URL (default)
open http://localhost:18000
```

Secrets load from vault via `deploy/load-vault-env.sh` — **do not commit API keys or use a shared `.env` in production.** For bare-metal Python dev without Docker:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cd backend/scanner && npm ci

# Export secrets from vault into your shell, then:
export PYTHONPATH=.
export INVENTORY_DATABASE_URL=postgresql://dealership:dealership@localhost:5432/dealership
flask --app backend.main run
```

### Production (Railway)

| Service | Role | Image / entry |
|---------|------|----------------|
| `web` | HTTP, admin, APIs | `Dockerfile.web` → Gunicorn |
| `Postgres` | Inventory + job queue | Railway template |
| `scanner-worker` | Claims `dealer_jobs`, runs scrapes | Same image; `RAILWAY_SERVICE_NAME=scanner-worker` → worker loop |
| `scanner-scheduler` | Enqueues due refreshes from `dealer_catalog` | Same image; `RAILWAY_SERVICE_NAME=scanner-scheduler` → scheduler loop |

Root `railway.toml` points all Git-connected services at `Dockerfile.web`. **`docker-entrypoint-web.sh` dispatches** by `RAILWAY_SERVICE_NAME` so scanner services do not run Gunicorn. Worker and scheduler expose `GET /health` via a lightweight sidecar HTTP server for Railway healthchecks.

**Wire vault + Postgres to every app service:**

```bash
railway login && railway link -p dealership-scanner -s web
./deploy/railway/setup-full-stack.sh
./deploy/railway/sync-vault-to-railway.sh --all-services
```

**Migrate local Postgres inventory to Railway:**

```bash
./deploy/railway/migrate-local-postgres-to-railway.sh
```

Deep deploy runbook: [`deploy/railway/README.md`](deploy/railway/README.md).

### Scanner operations

| Component | Command / process |
|-----------|-------------------|
| Job queue | `dealer_jobs` in Postgres — `backend/scanner/job_queue.py` |
| Worker | `scripts/scanner_worker_loop.py` — poll, claim, `scanner.py` or `scanner.js` |
| Scheduler | `scripts/scanner_scheduler_loop.py` — `dealer_catalog.next_scan_at` |
| Admin UI | `/admin/dealers` — enqueue onboard/refresh, view cars + human-readable results |
| CLI | `python scanner.py --scan-only`, `python discovery.py`, `python post_scan.py` |

Scale workers locally: `WORKER_REPLICAS=4 ./deploy/up.sh`. On Railway, increase `numReplicas` in `deploy/railway/railway.scanner-worker.toml` or dashboard; attach a **Volume at `/data`** per worker for browser profiles and VDP image cache.

### Data and search

| Store | Contents |
|-------|----------|
| Postgres | Live inventory, dealer catalog, geopoints, scan profiles, job history |
| `users.db` | Accounts, entitlements, MFA, dealer-portal users |
| pgvector | Listing/dealer embeddings — rebuild with `backend/scripts/reindex_vectors.py` |
| EPA / specs | `backend/dictionary/*_EPA.csv`, `epa_master` table, trim ladders |

Legacy SQLite inventory paths remain as env overrides for migration tooling only; **production requires `INVENTORY_DATABASE_URL`.**

### Security

All security work is tracked in [`docs/SECURITY_MASTER_TODO.md`](docs/SECURITY_MASTER_TODO.md) (SEC-xxx items). Production expectations:

- Parameterized SQL only; no `shell=True` in subprocess spawns.
- CSRF on mutating admin/API routes; rate limits on locator and auth endpoints.
- Vault-backed secrets; SQLCipher keys for user DBs when not in local dev mode.
- `FLASK_ENV=production` enforces secure cookies and hardened headers (`backend/production_security.py`).

### Testing

```bash
PYTHONPATH=. pytest backend/tests -q
```

Notable suites: dealer locator, production security headers, trim ladder audit, Postgres inventory compat (`backend/tests/conftest.py`).

### Key API surfaces

| Endpoint | Purpose |
|----------|---------|
| `GET /health` | Version + liveness |
| `GET /api/dealer-locator?zip=&radius_miles=` | Nearby dealers + `listing_count`, `last_synced_at` |
| `GET /find-dealers` | Consumer map/list UI |
| `GET /admin/dealers` | Operator hub (auth required) |
| Mobile contract | `backend/mobile/contract.py` — routes mirrored for iOS |

### Rebuilding large local artifacts (not in git)

| Artifact | Rebuild |
|----------|---------|
| EPA CSVs | `backend/scripts/import_epa_to_dictionary.py` |
| Options / NHTSA CSVs | `backend/scripts/car_data_scraper.py` |
| Trim ladders | `python -m backend.scripts.build_trim_ladders` |
| EPA master index | `python -m backend.scripts.build_epa_master` |
| Vector index | `python backend/scripts/reindex_vectors.py` |
| SQLite → Postgres (legacy) | `backend/scripts/migrate_inventory_sqlite_to_postgres.py` |

### Documentation index

| Doc | Audience |
|-----|----------|
| [`docs/PLATFORM_MASTER_PLAN.md`](docs/PLATFORM_MASTER_PLAN.md) | Product + engineering roadmap |
| [`docs/SECURITY_MASTER_TODO.md`](docs/SECURITY_MASTER_TODO.md) | Security contract + validation |
| [`docs/INVENTORY_POSTGRES.md`](docs/INVENTORY_POSTGRES.md) | Postgres inventory model |
| [`docs/LISTINGS_PGVECTOR_SEARCH.md`](docs/LISTINGS_PGVECTOR_SEARCH.md) | Hybrid search |
| [`deploy/railway/README.md`](deploy/railway/README.md) | Railway full-stack deploy |
| [`ios/README.md`](ios/README.md) | iOS app + TestFlight |
| [`docs/SCANNER_NODE.md`](docs/SCANNER_NODE.md) | Node/Puppeteer scanner |
| [`docs/IOS_APP.md`](docs/IOS_APP.md) | App Store signing |

### Version history (recent)

| Version | Highlights |
|---------|------------|
| **0.2.0** | Postgres-required inventory; `/admin/dealers` hub; Find dealers map/list fix; listing counts + last synced; Railway scrape fleet; job result clarity |
| Prior | SQLite inventory, local-only workers, initial Stripe + OAuth scaffolding |

---

**Sarrafi Collection** — built for operators who care about data freshness and shoppers who deserve accurate inventory.
