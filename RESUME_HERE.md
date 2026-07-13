# DealershipScanner — Resume Here

**Version 0.2.0** · _Updated 2026-07-10_
**Master handoff:** [`master-todo.md`](master-todo.md) · [`CHANGELOG.md`](CHANGELOG.md)
**Branch:** `feature/admin-scanner-ops-hub` (**2026-07 structural rework landed in working tree, uncommitted — see Current state**)

## Restart

```bash
cd /Users/asarrafi/Projects/DealershipScanner
source .venv/bin/activate
python run.py                          # dev web app on http://localhost:5001 (override with PORT=…)
curl -sf http://localhost:5001/api/ready   # failure = app not up or a dependency (DB) is unavailable
```

**`run.py` is dev-only** (Werkzeug + local LLM autostart; opt out with `LOCAL_LLM_AUTOSTART=0`). `PUBLIC=1 python run.py` binds `0.0.0.0` for the Cloudflare tunnel (`./start.sh`). `.env` in the repo root is loaded and must never be committed.

## Docker stack (Postgres inventory)

```bash
./deploy/up.sh                         # postgres + web + 2 scanner-workers + scheduler; web on :18000
WEB_PORT=8000 ./deploy/up.sh           # custom port
curl -sf http://127.0.0.1:18000/health    # failure => docker logs dealership-scanner-web
```

## Scanner pipeline

```bash
python scanner.py --scan-only          # inventory only (fast cron path)
python post_scan.py --help             # repair/enrichment for recent VINs
python discovery.py --help             # dealer discovery
```

## URLs

| What | Where |
|------|-------|
| Dev app | http://localhost:5001 |
| Dev API health | http://localhost:5001/api/health |
| Dev API ready | http://localhost:5001/api/ready |
| Docker web | http://127.0.0.1:18000 (default `WEB_PORT` in `deploy/up.sh`) |
| Docker health | http://127.0.0.1:18000/health |
| Postgres (docker) | localhost:5432 · db `dealership` · user `dealership` |
| **Prod app** | https://sarraficars.com (Cloudflare tunnel, `./start.sh`) |

**Secrets:** loaded from `.env` / kmac vault (`deploy/load-vault-env.sh`); never stored in docs.

## Current state (2026-07-10)

| Program | Status |
|---------|--------|
| 2026-07 structural rework (blueprints, `backend/config.py`, `/api/health` + `/api/ready`, repositories facade, `ScraperChain`, CI tiers, docs kit) | **Done** — in working tree on `feature/admin-scanner-ops-hub`, uncommitted |
| Follow-ups (config migration, Node scanner retirement, Postgres consolidation, SQL migrations, chain adoption, route extraction) | **Todo** — tracked in [`master-todo.md`](master-todo.md) Phases A–C |
| Test baseline | **1156 passed, 9 skipped** (~4 min full suite) |

## Tests / CI

```bash
cd /Users/asarrafi/Projects/DealershipScanner
source .venv/bin/activate
python -m pytest backend/tests -q -p no:cacheprovider
```

## What to do next

Pick the top open row in [`master-todo.md`](master-todo.md) (A1 `os.getenv` → `Config` migration is the lowest-risk starting point). Flip its status token when you start, and log the change under `[Unreleased]` in [`CHANGELOG.md`](CHANGELOG.md).

## ⚠️ Standing constraints

**Behavior preservation is the contract:** no route URL, endpoint name, function signature, or import path may break — existing tests enforce this. **Never commit `.env`, API keys, or `*.db` files.** Production requires `FLASK_ENV=production` + `SECRET_KEY`/`ADMIN_PASSWORD` (see `docs/SECURITY_MASTER_TODO.md`); `run.py` must never serve production traffic (gunicorn/docker only).
