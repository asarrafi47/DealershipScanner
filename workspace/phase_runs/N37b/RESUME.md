# N37b Postgres-only cleanup, batch 2 - RESUME

Branch phase/N37b (head ec3d1981b, from integ/draft), merged into integ/draft at 67082578c. Only conflict: backend/tests/sqlite_allowlist.txt (kept integ lines, dropped the three test_dealerships_db_dedupe_city.py entries). Draft only.

| Unit | Head | Status |
|---|---|---|
| N37.4 | 97b22ad2a | PARTIAL: pg.py module and tests done; read-site migration deferred |
| N37.8 | ec3d1981b | PARTIAL: five stores done (comments, reviews, specials, lease matches, dealerships) |

## Open after merge
- test_no_sqlite.py allowlist failure (see N9 RESUME; same test).
- dealerships_db.ensure_dealerships_table is a no-op shim until manifest.py's unit removes the call.
- A database without the V020 dealerships columns now fails instead of being altered at runtime.
- Comments/reviews/specials callers still use the compat connection with ? placeholders (N37.6 codemod).
- pip-installed psycopg_pool into the shared .venv; images need the new requirements lines (added).

## Deferred checks
- N37.4 call-site migration (fetchone()[0] to scalar()/one(), 78 files), inventory_pg/inventory_compat/base_repo.
- N37.8 remainder: manifest.py, generations (N13), recipe_store/recipes (N13/N9 own), scan_lab, users_db, upsert path.
- Not started: N37.2, N37.3, N37.5-N37.7, N37.10, N37.14, N37.16-N37.25.

## Owner asks
- None new. Carry-over from N37: GUNICORN_WORKERS=1 on Railway; break-glass policy; optional bmw_*_embeddings drop.
