# N37 Postgres-only cleanup - RESUME

Branch phase/N37 (head 850c530a0), merged into integ/draft at 518ebd4ce, no conflicts. Draft only.

| Unit | Head | Status |
|---|---|---|
| N37.0, N37.1 | a64c7d405 | DONE (pg_test_up.sh part of N37.1) |
| N37.11, N37.12 | 6c2e4597b | DONE |
| N37.15 | ce9f0b5b6 | DONE |
| N37.13 | b8b8cab90 | DONE |
| N37.9 | 850c530a0 | DONE (V035 provisional) |
| N37.2-N37.8, N37.10, N37.14, N37.16-N37.25 | - | NOT STARTED (dependencies, owner/data ops, later waves) |

## Open after merge
- test_no_sqlite.py::test_no_new_sqlite_beyond_allowlist fails on integ/draft: allowlist does not cover N8 files (routes/dealership_page.py, scanner/upsert/ownership.py, scripts/apply_recipe_repairs.py, tests/test_apply_recipe_repairs.py, tests/test_prefetch_n8_7.py, tests/test_shared_host_identity.py, maybe more). Regenerate with `python backend/tests/test_no_sqlite.py --write` after review (shrink-only intent), or move those to PG.
- V035 collides with phase/N30 (users auth_epoch); renumber one at merge. V036 in N30 is also provisional.
- Docs still mention removed env vars RATE_LIMIT_SQLITE_PATH and DEV_JOB_STORE_SQLITE_PATH.
- Root conftest still sets SCAN_LAB_INVENTORY_DB_PATH (stale, harmless).

## Deferred checks
- pg_harness clone-median measurement and cache-reset sweep (N37.1).
- Create and migrate cars_lab; D-PG7 rescan into it (not run).
- Admin cutover and dev_users.db copier (N37.19-N37.21).

## Owner asks
- Confirm GUNICORN_WORKERS=1 on Railway before deploy (entrypoint still defaults to 3).
- Break-glass policy: should the env admin also override a changed password during a lockout?
- Optional cleanup migration dropping bmw_*_embeddings tables.
