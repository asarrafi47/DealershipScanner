# DealershipScanner — Master TODO (single source of truth)

_Last updated: 2026-07-10 (v0.2.0). See [`RESUME_HERE.md`](RESUME_HERE.md) + [`CHANGELOG.md`](CHANGELOG.md)._


Status key: `[ ]` todo · `[~]` in progress · `[x]` done · `[-]` cancelled/wontfix

---

## Phase A — Configuration & app structure

| ID | Task | Status | Owner | Notes |
|----|------|--------|-------|-------|
| A1 | Migrate remaining `os.getenv` call sites to the `Config` class in `backend/config.py` | [ ] | team-backend | `Config` landed in the 2026-07 rework (Phase R). Grep `os.getenv`/`os.environ.get` outside `backend/config.py` and migrate incrementally; keep default values identical. |
| A2 | Extract remaining routes from `backend/main.py` into blueprints | [ ] | team-backend | Blueprint split started in Phase R. Behavior preservation is the contract: no route URL, endpoint name, or import path may change. |

---

## Phase B — Data layer

| ID | Task | Status | Owner | Notes |
|----|------|--------|-------|-------|
| B1 | Finish SQLite → Postgres inventory consolidation | [ ] | team-infra | See [docs/INVENTORY_POSTGRES.md](docs/INVENTORY_POSTGRES.md). Migration script: `backend/scripts/migrate_inventory_sqlite_to_postgres.py` (`--dry-run` first). Remaining SQLite DBs: `users.db`, `dealer_portal.db`, `incomplete_listings.db`, … |
| B2 | Adopt versioned SQL migrations (Flyway-style `V<N>__desc.sql`) for the Postgres side | [ ] | team-infra | Replace ad-hoc `init_inventory_db()` / `init_job_queue_schema()` schema pushes (see `deploy/up.sh`) with an ordered, recorded migration chain. |
| B3 | Extend `epa_extended_specs` coverage + make it available on SQLite dev | [ ] | team-data | VDP spec sheet now renders hp/torque/0-60/fuel tank/tow/curb weight uniformly (— when unknown, `frontend/templates/car.html` 2026-07-11). Values come from `epa_extended_specs`, which exists only in Postgres (`backend/db/inventory_pg.py:375`, imported 2026-07-06) and matches at model level — local SQLite has no table at all (`lookup_epa_extended_specs` silently returns `{}`). Import/mirror the table for dev and measure per-field fill rates. |
| B4 | Top-speed spec: needs a data source | [ ] | team-data | Requested VDP row; no `top_speed` field exists anywhere in the pipeline (not in `epa_extended_specs`, dictionary CSVs, or scrapes). Candidate sources: OEM brochures (`backend/scripts/analyze_brochure_with_llm.py`) or a reference import. Add column + template row once a source exists. |

---

## Phase C — Scanner stack

| ID | Task | Status | Owner | Notes |
|----|------|--------|-------|-------|
| C1 | Retire legacy Node scanner (`backend/scanner/scanner.js`) in favor of the Python stack | [ ] | team-scrape | See [docs/SCANNER_NODE.md](docs/SCANNER_NODE.md). Confirm no cron/deploy path still shells out to Node (`backend/scanner/` `npm ci` step in README becomes removable). |
| C2 | Integrate `ScraperChain` (`backend/scanner/chain.py`) at more call sites | [ ] | team-scrape | Framework landed in Phase R with initial adopters; migrate remaining per-platform scraper dispatch to the chain one site at a time, tests green after each. |

---

## Phase R — 2026-07 structural rework (closed)

| ID | Task | Status |
|----|------|--------|
| R1 | Blueprint split of `backend/main.py` (URLs/endpoints preserved) | [x] |
| R2 | `Config` class in `backend/config.py` | [x] |
| R3 | `/api/health` + `/api/ready` endpoints | [x] |
| R4 | `backend/db/repositories` split behind a facade | [x] |
| R5 | `ScraperChain` framework in `backend/scanner/chain.py` | [x] |
| R6 | CI workflow + `integration`/`regression` marker scaffolding (no tests tagged yet, so CI runs the full suite); docs kit (this file, `CHANGELOG.md`, `RESUME_HERE.md`) | [x] |

See [`CHANGELOG.md`](CHANGELOG.md) `[Unreleased]` for details. Baseline before rework: 1156 passed, 9 skipped.

---
