# Inventory Postgres + parallel scanner writes

## Goal

Use PostgreSQL as the inventory source of truth so multiple scanner workers can upsert
concurrently. SQLite remains supported for local dev (single writer).

## Phase A — Postgres as source of truth

1. Set **`INVENTORY_DATABASE_URL`** (or **`DATABASE_URL`**) to a `postgresql://…` DSN.
2. Initialize schema: `PYTHONPATH=. python -c "from backend.db.inventory_db import init_inventory_db; init_inventory_db()"`
3. Optional one-time migration from SQLite:

```bash
export INVENTORY_DB_PATH=/data/inventory.db
export INVENTORY_DATABASE_URL=postgresql://user:pass@host:5432/dealership
PYTHONPATH=. python backend/scripts/migrate_inventory_sqlite_to_postgres.py
```

4. Point the Flask app and scanner at the same DSN (no `INVENTORY_DB_PATH` needed in prod).

Tables created on Postgres include `cars`, `scan_runs`, `dealer_scan_profile`, `epa_master`,
`model_specs`, `incomplete_listings`, etc. (`backend/db/inventory_pg.py`).

## Phase B — Parallel scanner upserts

When `is_inventory_postgres()` is true:

- **`SCANNER_PARALLEL_UPSERT`** defaults to **on** (set `0` to force serialized upserts).
- The global asyncio `write_lock` around `upsert_vehicles` is **not** used.
- Different dealers can commit inventory in parallel; Postgres row locks handle VIN conflicts.
- **`dealer_scan_profile`** caches recovery strategy per dealer on Postgres too.

Env tuning for Postgres:

| Variable | SQLite default | Postgres suggestion |
|---|---|---|
| `SCANNER_MAX_DEALER_CONCURRENCY` | 3 | 5–8 |
| `SCANNER_PARALLEL_UPSERT` | off (implicit) | on (default) |
| `SCANNER_SHARD_COUNT` | 1 | N indexed job completions |

## Kubernetes (multiple scanner pods)

With Postgres, replace single-pod `concurrencyPolicy: Forbid` with an **Indexed Job**:

- `SCANNER_SHARD_COUNT=N`, each pod gets `JOB_COMPLETION_INDEX` 0..N-1
- All pods share `INVENTORY_DATABASE_URL` from secrets (not a SQLite file on PVC)
- Keep PVC only for PDFs / VDP images unless using object storage

Example shard env (see `deploy/k8s/cronjob-scanner.yaml` comments).

## Local dev

Keep using `inventory.db` — no Postgres required. Parallel upsert stays off automatically.
