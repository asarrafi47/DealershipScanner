# N37: All-Postgres (no SQLite anywhere)

Status: DRAFT plan, 2026-10-26. Read-only audit of branch `integ/draft`; no code changed.
Owner directive (2026-10-26): "we should not have a single sqlite database. all postgres."

Counting note: the audits used different methods (AST vs line grep), so some counts differ by a few percent. Ranges are shown where they do. The final codemod and the CI guard (section 6) are the source of truth.

---

## 1. Directive and why

Meaning: no SQLite file, no `sqlite3`/`sqlcipher3` import, no SQLite-only SQL and no SQLite test tier may remain. Every store, including tests and tooling, is Postgres.

Why, with evidence:

1. The SQLite test tier hides Postgres-invalid SQL. The 2026-10-26 deep-dive audit (`workspace/audits/bug_deep_dive_2026_10_26.md`) found:
   - **B-002**: `backend/enrichment/service.py:619` uses `CAST(engine_l AS REAL)`. On Postgres this raises 22P02 on non-numeric text. SQLite casts silently.
   - **B-003**: `backend/enrichment/trim_ladder/inventory.py:69` uses a bare `trim` with `GROUP BY`. Postgres rejects it. SQLite accepts it.
   - Related: B-034 (`vector/pgvector_service.py:353`) and B-056 (`service.py:1084`). B-030/B-031 are lab and test scripts that write to prod Postgres.
   - Every one of these passed a green SQLite suite, because `backend/tests/conftest.py:82-92` blanks `INVENTORY_DATABASE_URL` and sets `INVENTORY_SQLITE_TESTS=1` for all tests.
2. Postgres is already the system of record. Prod inventory refuses SQLite (`backend/db/inventory_pg.py:105-110`). Local `cars` has 82 tables, 3,239 MB, 357,149 cars. SQLite survives as code and test debt, plus two real prod stores (`users.db`, `dev_users.db`).
3. A regex adapter (`inventory_pg.py:142-363`, about 222 lines) rewrites SQLite SQL on every Postgres `execute`. It is partial: `INSERT OR REPLACE` maps for only 3 tables; `datetime('now','-7 days')` and `json_extract` are not handled and fail at run time.
4. Dual-mode code doubles the surface: 111 branch sites in 51 non-test files, 9 parallel `_DDL_SQLITE` constants.

---

## 2. Current state

### 2.1 Every SQLite store

| # | Store (file) | Module | Tables | Prod status | PG equivalent |
|---|---|---|---|---|---|
| 1 | `users.db` (SQLCipher) | `backend/db/users_sqlite.py:14-116`, `users_db/*`, `user_history_db.py`, `search_analytics_db.py` | users, orgs, org_invites, car_view_history, car_compare_history, compare_sessions, search_events (7) | REAL SQLite on Railway `web-volume` `/data`. Cutover never run. PG tables are empty | `migrations/V013__users.sql`. No runtime code uses it. Copier `backend/scripts/migrate_users_sqlite_to_postgres.py` exists |
| 2 | `dev_users.db` (SQLCipher) | `backend/db/dev_users_sqlite.py`, `admin_users_db.py` | admin_users (1 table, 1 row locally) | REAL SQLite on `/data` | NONE. New migration needed |
| 3 | `dealer_portal.db` | `backend/db/dealer_portal_db.py:36,55-70` | dealer_vehicles | Dead file in prod (code routes to PG when the DSN is set). Prod file contents unverified | V012 (and V001) |
| 4 | `incomplete_listings.db` | `backend/db/incomplete_listings_db.py:28-56` | incomplete_listings, incomplete_listings_meta | PG in prod (local PG: 20,330 rows) | V001 + V027 |
| 5 | `inventory.db` | `backend/db/repositories/base_repo.py:20-66`, `inventory_compat.py`, `schema_repo.py`, `scanner/database.py` | cars and the other inventory tables | PG in prod. SQLite only in pytest (`INVENTORY_SQLITE_TESTS`) and an import-time fallback probe | V001..V027 |
| 6 | comments store | `backend/db/comments_db.py` | car_comments, dealer_comments, dealer_ratings, comment_flags, comment_attachments | PG | V002, V004, V005 |
| 7 | reviews store | `backend/reviews/store.py:44` | dealer_reviews, review_reports | PG | V001, V019, V026 |
| 8 | specials and lease matches | `backend/scanner/specials/store.py:49`, `lease_matches_store.py:34` | dealer_specials, lease_offer_matches | PG (local 71 / 40 rows) | V001, V019 |
| 9 | dealerships and manifest | `backend/db/dealerships_db.py`, `scanner/manifest.py` | dealerships | PG (local 630 rows) | V001, V003, V008, V020 |
| 10 | generations and recipes | `catalog/generations.py:34`, `scanner/recipe_store.py:125,167-176`, `scanner/recipes.py:347`, `scripts/reconcile_recipe_store.py:598` | model_generations, dealer_recipes | PG (recipes local 687 rows) | V001, V019 |
| 11 | rate limiter `rate_limits.db` | `backend/utils/ip_rate_limit.py` | _rate_hits | Dormant: prod runs `GUNICORN_WORKERS=1`. Entrypoint creates it when workers != 1 (`scripts/docker-entrypoint-web.sh:55`) | NONE |
| 12 | dev job store | `backend/dev/routes.py:100-295` | dev_job_store | Opt-in, unset | NONE |
| 13 | scan lab `workspace/inventory_92694.db` (20,226,048 B) | `backend/dev/scan_lab.py:35-95` | cars (4,386), nhtsa_vpic_cache (1,647), dealer_scan_profile (3) | Lab only. The subprocess launch (`scan_lab.py:466`) sets only `INVENTORY_DB_PATH`, so a lab scan may write to main PG (to verify) | NONE. Needs schema or DB |
| 14 | `dictionary_catalog.db` (4,546,560 B, 14,598 rows) | `backend/enrichment/dictionary_catalog.py:240-414` | dictionary_entries | Prod never had it (`.dockerignore` excludes `*.db`); prod uses the glob fallback | NONE |
| 15 | tracked `dictionary_catalog.sqlite3` | none | none | 0 bytes, zero references (P18A.1 already plans deletion) | none |
| 16 | `bmw_intake.db` | `backend/vector/bmw_store.py`, `pgvector_service.py:374-500` | 3 bmw_* tables | Dead: writer deleted 2026-10-01, no file on disk | none |
| 17 | `bmw_reference.db` | `backend/oem/vehicle_reference/` (19 files, 1,758 lines) | 10 ref_* tables | CLI-only, no consumers, no tests, no file | none (D-DC7 (b) already deletes) |
| 18 | direct readers | `scraping/sources.py:12`, `scripts/image_downloader.py:292,581`, `app_users_status.py:48`, `reset_app_user_password.py:90`, `scripts/create_demo_free_user.py:37`, `scripts/cleanroom/db.py` | reads of cars/users | n/a. The 3 user scripts bypass SQLCipher and cannot read prod | cars (V001), users (V013) |
| 19 | one-shot copiers | `migrate_{users,dealer_portal,inventory}_sqlite_to_postgres.py` | n/a | Only legitimate sqlite readers after cutover | delete after soak |

### 2.2 sqlite-ism counts (non-test unless noted)

| Item | Count |
|---|---|
| Non-test `.py` files importing sqlite3/sqlcipher3 | 56 to 68 (by method); 4 use sqlcipher3 |
| Test files importing sqlite3 | 63 to 74; about 162 `sqlite3.connect` calls in 71 files |
| Files mentioning sqlite (non-test / test) | 124 to 142 / 98 to 108 of 390 to 429 |
| Test files DB-coupled (broad) | 175 to 190 files, 1,969 to 2,354 tests (about 45 to 53%) |
| Hermetic SQLite files per test session | 6 (`conftest.py:41-67`) |
| `sqlite3.connect()` non-test | 20 to 24 sites |
| Dual-mode branch sites (`is_inventory_postgres`) | 111 in 51 files |
| `?` placeholders | prod about 1,196 to 1,232 (112 files); tests 911 to 952 (68 files) |
| PRAGMA | 87 to 102 prod (55 `table_info`); 19 to 39 test |
| `row_factory` / `sqlite3.Row` | 59 to 86 / 66 to 69 prod |
| `row[<int>]` positional reads | 263 in 78 files |
| AUTOINCREMENT | 38 lines in 17 files prod |
| `datetime(` / `DEFAULT (datetime('now'))` | 29 / 16 |
| INSERT OR IGNORE / OR REPLACE | 5 to 7 / 5 to 11 prod (adapter maps only 3 tables for REPLACE) |
| `lastrowid` | 18 to 19 in 7 files |
| IFNULL / INSTR / GROUP_CONCAT / json_extract / sqlite_master / executescript | 40 / 4 / 2 / 5 / 7 / 3 |
| Runtime DDL | 112 CREATE TABLE (37 files), 85 CREATE INDEX, 62 ALTER ADD COLUMN |
| Adapter code to delete | `inventory_pg.py` 142-363 plus legacy DDL 421-900, `inventory_compat.py` (129), `runtime_ddl.py` (132) |
| Env vars to retire | INVENTORY_DB_PATH (89 refs), USERS_DB_PATH (67), RATE_LIMIT_SQLITE_PATH (34), DEV_USERS_DB_PATH (30), INVENTORY_SQLITE_TESTS (26), DEALER_PORTAL_DB_PATH (22), CATALOG_DB_PATH (20), DICTIONARY_CATALOG_DB_PATH (14), INCOMPLETE_LISTINGS_DB_PATH (8) and others |
| SQLite files on disk (main checkout) | 15 to 19 files, about 26 MB (20.2 MB is the lab file). Largest user file `backend/users.db` 434,176 B (8 users, 3,118 view rows, 99 search_events) |
| Tracked SQLite artifacts | 1 empty `.sqlite3`; fixture `search_golden_sqlite.json.gz` (106,806 B) |
| Non-Python residue | `requirements.txt:43` sqlcipher3; `.gitignore` (24 lines); `.env.example:42-53`; docker-compose (4 vars + `/data` mount); k8s/car-scanner (6 files, D-IF9 deletes); Railway sync script (`sync-vault-to-railway.sh:130-151`); README; 24 docs files (286 mentions) |

Prod facts: Postgres 18.6; `web-volume` 1,062 MB holds `users.db`, `dev_users.db` and the dead `dealer_portal.db`. Prod `users` table has 0 rows. Prod file row counts are unread (needs `railway ssh`).

Plan conflicts: D-TC4 ("Targeted" PG tier), P14A.6 ("SQLite golden byte-unchanged"), P14B.4 (SQLite-vs-PG parity), P14C (SQL corpus), and P13A.6, P16.3, P16.6 (R-22), which assume users.db stays on `/data`.

---

## 3. Target architecture

### 3.1 One data-access layer
- `backend/db/pg.py` (evolve `backend/db/connect.py`, which already owns `inventory_dsn()`): one `psycopg_pool.ConnectionPool` per process; `dict_row`; `configure=` sets `statement_timeout`, `lock_timeout`, `timezone=UTC`.
- API: `tx()`, `fetch_one/fetch_all/scalar`, `execute_returning_id`, `execute_many`, `readonly()`. `in_list(col, values)` using `= ANY(%s)` retires `_placeholders()`.
- Native `%s` placeholders. Codemod the about 1,200 prod and about 900 test `?` sites using `sql_lexical_runs` one last time, then delete the lexer. Every literal `%` in LIKE gets doubled (58 prod lines).
- Project-owned exception types replace `sqlite3.IntegrityError` (3 route catches in `dealer/routes.py`) and the 74 sqlite3 exception handlers.
- Native rewrites: `RETURNING id` (18 lastrowid sites), `COALESCE`, `strpos`, `string_agg`, `filters_json::jsonb ->> 'make'`, `to_regclass`, `information_schema` or constants instead of PRAGMA, `ON CONFLICT` with explicit targets per table.

### 3.2 Which Postgres holds what
- One cluster, one migration chain, one `schema_migrations` (D-DB5 (b)).
- Database `railway` (prod) / `cars` (local): schema `public` = inventory, reference catalog, dictionary catalog, and operational tables. New schema `app_users` (users, orgs, org_invites, histories, search_events, admin_users) owned by a web-only role. The scanner role gets no grant (D-SEC3 (a)). `migrate.py` must set `search_path` or qualify names. V013 is already applied on prod in `public`, so moving it is a new migration (checksum rule forbids editing V013).
- Scan lab: schema `scan_lab` in a scratch database (`cars_lab`), selected by a lab DSN passed to the scanner subprocess as `INVENTORY_DATABASE_URL`.
- Scratch and test: guarded `pytest_*` databases (D-TC9). Never `cars`, `cars_cleanroom`, non-local hosts or port 15432.

### 3.3 Reference catalog in Postgres
- `dictionary_catalog_entries` table (21 columns, key `catalog_key`; indexes on `(make_norm, model_norm)` and `(year)`) replacing `dictionary_catalog.db`. The loader is a transactional TRUNCATE+COPY inside `build_dictionary_manifest`, run after `migrate --apply` in deploys. Lookup uses the shared pool.
- `find_epa_csv` year-distance ranking (`dictionary_catalog.py:407-413`) must be reproduced exactly. Replay-test against current prod glob output first, because PF-1 shows 128 YMMs resolve differently with and without the catalog.
- Alternative (needs owner choice, D-PG6): serve from `manifest.json` in memory, or collapse onto `epa_master` (70,496 rows) and `dictionary_options`.
- Delete the empty tracked `.sqlite3`.

### 3.4 Tests get Postgres
- N7 `pg_harness` becomes the default autouse inventory fixture; the `pg` marker disappears. Session template built by `migrate --apply` to V027 (with a version assert); per-test `CREATE DATABASE ... TEMPLATE ... STRATEGY WAL_LOG`; `DROP DATABASE ... WITH (FORCE)`; janitor for leaked `pytest_%` older than 6 h; `reset_pg_process_caches()` for about 15 module-level caches; `REQUIRE_PG=1` turns skips into failures.
- Measured clone cost (local PG 17.10, 6 cores, load about 10, so pessimistic; 82-table, 11 MB template, in-process): create median 77 ms (p95 126), drop median 23, full cycle median 106 ms (mean 129). FILE_COPY cycle median 92 ms but forces checkpoints on the cluster holding `cars`, so avoid it. 4 workers: 63 ms per cycle effective. TRUNCATE of 82 tables: 64 ms median, only 1.7x cheaper than a clone.
- Runtime: baseline 337.8 s pytest time (4,648 tests, 56 chunks). With 53% DB-touching tests: +261 to 318 s, so about 10 to 11 min serial. At the stated 6,700 tests: about 14.4 to 15.8 min serial; about 5 min with xdist -n 4 (assumed 3.3x scaling, unmeasured). Fits the 30-min CI timeout. Read-only tests share a module-scoped clone (about 16 to 20 s total).
- CI: `services: postgres` on the `pytest` job itself, image `pgvector/pgvector:0.8.6-pg18` (D-TC3), `pg_isready` health check, `TEST_PG_ADMIN_DSN` and `REQUIRE_PG=1`; `ALTER SYSTEM SET fsync=off` in a step. Drop `not pg` from `-m` (`ci.yml:96-99`); replace `pytest-integration` (3 tests, all skipped) with a migration-chain job; update `backend/tests/test_ci_workflow.py` and the main ruleset (owner action).
- Local dev, one command: `scripts/pg_test_up.sh` starts an ephemeral Homebrew pg18 cluster (`initdb --no-sync` in `$TMPDIR`, unix socket only, random port, `fsync=off`, pgvector) and tears it down on exit; pytest's session hook calls it when `TEST_PG_ADMIN_DSN` is unset. This isolates tests from the 3.2 GB `cars` server. Errors say "start Postgres with scripts/pg_test_up.sh" and never skip silently. Prerequisite to verify: `vector.control` under `/opt/homebrew/opt/postgresql@18/share/postgresql@18/extension/` (not found in the audit). Docker stays opt-in.
- Goldens: upsert and search goldens re-recorded once on PG with a documented normalisation. The expected-JSON edit needs owner approval (D-PG3). `test_car_detail_context_golden` gets a seeded corpus.

### 3.5 What gets deleted
- Code: `users_sqlite.py`, `dev_users_sqlite.py`, `_sqlite_connect_raw`, the import-time inventory.db probe, `INVENTORY_SQLITE_TESTS`, `inventory_pg.py:142-363` and `421-900`, `inventory_compat.py`, `runtime_ddl.py`, 9 `_DDL_SQLITE` constants, SQLCipher, `sqlite3.Row` factories, `ip_rate_limit` SQLite branch, dev-job SQLite option, `bmw_store.py` and the `vehicle_reference` package, `test_inventory_pg_adapter.py`, `test_adapt_sql_cache.py`, the 3 copier scripts (archive tag first).
- Config: 14+ env vars (section 2.2), `sqlcipher3` and `libsqlcipher`, `web-volume` mount, `ALLOW_UNENCRYPTED_USER_DB`, k8s/car-scanner manifests (D-IF9), `.env.example` SQLite block.
- Files: 15 to 19 local `.db` files (back up `backend/users.db` first), `search_golden_sqlite.json.gz`, the empty `.sqlite3`.

---

## 4. Work breakdown

Kind: code / test / migration / data-op / owner-op. "Draft" = can be drafted without owner action (branch work). Size: S under 1 day, M 1 to 3 days, L over 3 days of agent time.

### Wave 0: foundation (inside or right after N7)
| Unit | What | Files | Kind | Depends | Acceptance | Size | Draft |
|---|---|---|---|---|---|---|---|
| N37.0 | Freeze allowlist and rule: record today's SQLite-importing files as an allowlist that can only shrink; no new test may import sqlite3 | new `backend/tests/test_no_sqlite.py` | test | none | guard passes today, fails on a new import | S | yes |
| N37.1 | Measure and fix harness design: per-test clone vs shared clone, xdist, cache-reset sweep; pg18 dev cluster script | `backend/tests/pg_harness*`, `scripts/pg_test_up.sh` | code/test | N5 (chain to V027) | clone median under 0.5 s, `pg_test_up.sh` works from a cold shell, refuses `cars` | M | yes |
| N37.2 | Make the harness the default tier: autouse `pg_db`, `sqlite_inventory` shim backed by a clone (same `.add_cars` API), fixtures `pg_users`, `pg_inventory`, subprocess DSN pass-through | `conftest.py`, `backend/tests/conftest.py` | test | N37.1 | whole suite runs with the shim; SQLite fixtures unused | L | yes. Merge only after N8.6 lands, to avoid conftest/ci.yml conflicts |
| N37.3 | CI: Postgres service on `pytest`, drop `not pg`, retire `pytest-integration`, optional xdist | `.github/workflows/ci.yml`, `requirements-test.txt`, `test_ci_workflow.py` | code | N37.1 | CI green with service; P95 job time under 15 min | M | yes; ruleset update is owner-op |

### Wave 1: new data layer and PG-native conventions
| Unit | What | Files | Kind | Depends | Acceptance | Size | Draft |
|---|---|---|---|---|---|---|---|
| N37.4 | `backend/db/pg.py` pool and helpers; `scalar()/one()/all()`; migrate `fetchone()[0]` reads | `backend/db/pg.py`, 78 files with positional reads | code | N37.1 | no tuple-vs-dict breakage in golden tests | L | yes |
| N37.5 | Native-SQL rewrites per construct: INSERT OR → ON CONFLICT with explicit targets (geocode_dealers, db_admin, enrichment/service.py:254 first), datetime, IFNULL/INSTR/GROUP_CONCAT, json_extract, sqlite_master, LIKE audit (35 unwrapped LIKEs), B-002/B-003/B-034/B-056 fixes with failing PG tests first | listed in section 2.2 | code | N37.4 | per-table unique-key test from `information_schema` passes | M | yes |
| N37.6 | `?` → `%s` codemod using the lexer, plus a test that parses every SQL literal and fails on any remaining `?`; f-string SQL (287) by hand; `_placeholders()` → `ANY(%s)` | about 112 prod + 68 test files | code | N37.4 | guard test green | L | yes. Do after N8/N9/N10 merge to limit conflicts |

### Wave 2: stores (each unit deletes its SQLite branch and converts its tests in the same PR)
| Unit | What | Files | Kind | Depends | Acceptance | Size | Draft |
|---|---|---|---|---|---|---|---|
| N37.7 | `dealer_portal_db` (delete SQLite, project-owned exception, 3 route catches), `incomplete_listings_db` (delete sidecar, cache token from meta table only) | those modules, `dealer/routes.py`, `dealer/admin/*` | code/test | N9 released (file ownership), N37.2 | stores import no sqlite3; routes still reject duplicates | M | yes |
| N37.8 | comments, reviews, specials, lease matches, dealerships/manifest, generations, recipe_store/recipes: drop `_DDL_SQLITE`, `ensure_*` runtime DDL, row factories | modules in section 2.1 rows 6 to 10 | code/test | N37.4, N37.2 | AST guard: batch files import no sqlite3 | M | yes |
| N37.9 | New migration `admin_users` (V035 or later, see D-PG9 numbering) and port `admin_users_db.py`, `dev/routes.py` IntegrityError catch; break-glass env-seeded admin | `migrations/`, `admin_users_db.py`, `dev/routes.py` | migration/code | N37.4 | operator login works on a clone; env admin works with empty table | M | yes |
| N37.10 | Users port (flag `USERS_DB_BACKEND`, default off): `users_db/*`, `user_history_db.py`, `search_analytics_db.py`; RETURNING for 4 lastrowid sites; timestamp normalisation (`admin.py`, `auth.py`); constants instead of PRAGMA probes; remove import-time DDL; re-diff V013 against the current ALTER ladder; schema move to `app_users` migration; fold P16.2 (drop dead totp_secret/mfa_phone) | `backend/db/users_db/*` (2,369 lines, 51 conn calls), 23 importers, 32 test files | code/migration/test | N3 released, N37.2, D-PG4 | 24 users test files on PG; 3 user scripts use the compat connection | L | yes (flag off; no prod effect) |
| N37.11 | rate limiter: in-process only (default) or UNLOGGED table; delete `RATE_LIMIT_SQLITE_PATH` from entrypoint, k8s, 13 test refs | `utils/ip_rate_limit.py`, `scripts/docker-entrypoint-web.sh` | code | D-PG5 | no sqlite in module | S | yes |
| N37.12 | dev job store: delete the SQLite option (or `dev_job_store` table) | `backend/dev/routes.py` | code | D-PG5 | none | S | yes |
| N37.13 | scan lab: schema or DB, lab DSN passed to subprocess, fix the `scan_lab.py:466` isolation gap, copy 4,386 cars + 1,647 vpic rows or rescan | `dev/scan_lab.py`, `scan_lab_routes.py`, `scripts/scan_lab_report.py`, `scan_irvinebmw_test.sh` | code/data-op | D-PG7 | a lab scan cannot write prod `cars` (test with prod-looking DSN in env) | M | yes |
| N37.14 | dictionary catalog to Postgres table plus loader plus replay test; delete `.db`, `.sqlite3`, 6 test hash hooks | `enrichment/dictionary_catalog.py`, `dictionary_paths.py`, `build_dictionary_manifest.py` | migration/code/test | D-PG6, N20 (owner of prod dictionary lookups) | identical results to current glob output for all 128 PF-1 YMMs | M | yes |
| N37.15 | Delete dead stores: bmw_store, pgvector BMW reindex, `vehicle_reference` (D-DC7 (b)) | `backend/vector/`, `backend/oem/` | code | D-PG8, audit-deletions grep | no consumers | S | yes |
| N37.16 | Scripts and scraping: `scraping/sources.py`, `scraping/cli.py` `--dsn`, `image_downloader.py`, `backfill_specs`, `db_admin`, `enrich_from_dictionary`, `vision/analyze_images`, cleanroom `db.py` (collapse dialect; keep `test_cleanroom_*` on PG first) | list in section 2.1 row 18 | code | N37.4 | no `INVENTORY_DB_PATH` handling | M | yes |

### Wave 3: tests
| Unit | What | Kind | Depends | Acceptance | Size | Draft |
|---|---|---|---|---|---|---|
| N37.17 | Conversion batches B1 to B9 (heuristic, 190 files): B1 inventory read/grid/search/routes 76 files/845 tests; B2 upsert/write 23/203; B3 reconcile/retire/guard/cleanroom 21/415; B4 recipe stores 11/222; B5 users/auth/app 25/202; B6 portal/comments/nearby/lab 7/112; B7 dictionary/EPA 6/93; B8 adapter/DDL tests 12/231 (mostly delete); B9 goldens 9/33. B5 to B7 land with their store units | test | N37.2 | per batch: collected/passed/skipped/xfailed counts do not drop except an explicit list; SQLite fixture deleted in the same PR | L (split into sub-units) | mostly yes. Budget 15 to 25% of converted tests needing a real SQL or fixture fix |
| N37.18 | Goldens: re-record upsert and search goldens on PG with normalisation doc; seeded car-detail corpus | test | N37.17 B1/B2, D-PG3 | divergences each reviewed | M | draft yes; re-record needs owner OK |

### Wave 4: data operations (owner-gated)
| Unit | What | Kind | Depends | Acceptance | Size | Draft |
|---|---|---|---|---|---|---|
| N37.19 | Prod census (read-only `railway ssh`): row counts, case-duplicate emails/usernames, TOTP/mfa_phone non-empty counts, `dealer_portal.db` rows, upload directories on `/data` | owner-op | none | numbers recorded in the plan | S | no |
| N37.20 | Apply new migrations to prod manually before deploy (D-DB6 (a)); dry run on a restored dump | data-op | N37.9, N37.10 | `schema_migrations` current | S | runbook yes, execution owner |
| N37.21 | Users + dev_users cutover per section 5 | data-op | N12 plus 3 clean cron nights, N37.10, N37.19, D-PG4 | counts and checksums match; login and TOTP smoke pass | M | runbook yes, execution owner |
| N37.22 | Dictionary catalog load in prod | data-op | N37.14 | replay test passed | S | no |

### Wave 5: delete and lock
| Unit | What | Kind | Depends | Acceptance | Size | Draft |
|---|---|---|---|---|---|---|
| N37.23 | Delete adapter (`inventory_pg.py` 142-363, 421-900), `inventory_compat.py`, `runtime_ddl.py`, `base_repo` probe, `INVENTORY_SQLITE_TESTS`, sqlcipher and env vars, `.gitignore` entries (keep `*.db` catch-all), docker-compose, `.env.example`, Railway variables, k8s, README and docs | code | all of Wave 2 and 3, N28 strict 7 days, users soak 14 days | allowlist at zero | L | yes (hold merge until soak ends) |
| N37.24 | Detach Railway `web-volume`; archive then delete copiers; re-scope P16.6 | owner-op | N37.21 soak, off-volume archive of `users.db`/`dev_users.db` | volume gone, backups exist | S | no |
| N37.25 | Docs: rewrite `docs/INVENTORY_POSTGRES.md`, `docs/USERS_PG_CUTOVER.md`, README section 3 (Postgres setup), new `docs/DEV_SETUP.md`, update NEXT_PHASES_PLAN wording | code | N37.23 | no doc tells anyone to use SQLite | S | yes |

### Replaces, absorbs, changes
- **D-TC4 (Targeted)** replaced by whole-suite on PG. **N31** is absorbed into N37.17/N37.18; its 30-day "pytest-pg green" wait is dropped. P14B.4 (SQLite-vs-PG parity) and P14C (SQLite SQL corpus) become moot; P14A.6's "SQLite golden byte-unchanged" is reversed.
- **N7 (D-PLAN1)** kept right after N5 and grows from M to L (N37.1 to N37.3). D-PLAN1's premise "SQLite stays the default tier" is replaced.
- **N27**: P13A.2 (loud adapter) becomes "delete the SQLite-translation half of the adapter". P13A.1 (portable ON CONFLICT) and P13A.6 (no import-time init) stay and become mandatory (N37.5, N37.10).
- **N28**: P13B.1 to P13B.3 delete SQLite branches and `_DDL_SQLITE` instead of keeping them for tests; N37.8 and N37.23 do the deletion in the same units.
- **N30**: P16.1 and P16.2 are written once as PG migrations (no SQLite ALTER ladder). P16.3 and P16.6 are re-scoped: no `users.db` or `dev_users.db` WAL chown on `/data`.
- **N32 (shims)** and **N33/P17B.10** shrink slightly (D-DC7 (b) deletes `vehicle_reference`; N37.15 covers it).
- **Users cutover**: moves ahead of N30 (data cutover after N12 plus 3 cron nights). Replaces the "deferred" status in `docs/USERS_PG_CUTOVER.md`.
- **Critical path**: N1 → N5 → N9 → N8.6 → N8 release → N10 wave 3 → N11 → N12 is unchanged (+0 days) if the default-tier switch (N37.2) lands after N8.6. If the owner forces the switch before N8.6: +3 to 4 days (fleet #1 from about day 29 to 31 to about 33 to 35). Not recommended. One new rule from now on: every new or changed test in N8/N9/N10 is PG-native; none new on SQLite.
- New work estimate after N12: about 7 to 9 agent phases (about 35 to 45 agents), parallel with N13 to N20.

Ownership collisions to avoid: `conftest.py` and `ci.yml` are N7-owned; `incomplete_listings_db.py`/`data_quality_repo.py` are N6/N9; `users_db/*`/OAuth/web are N3; `scanner/database.py`/migrations/`migrate.py` are N5/N9/N8.6. Migration numbers V031 to V034 are already allocated; `admin_users` and any other new tables take V035 or later (or the next free number).

---

## 5. Data migration and cutover runbook outline

Data in scope: `users.db` (7 tables; local copy 8 users, 3,118 view rows; prod count unread), `dev_users.db` (1 table; 1 row locally), `dealer_portal.db` (verify; likely empty), dictionary catalog (14,598 rows, reload from source), scan lab (4,386 cars, optional).

1. **Census (N37.19)**: read-only `railway ssh` counts per table; case-duplicate emails/usernames (V013 has `lower()` unique indexes); non-empty TOTP/mfa_phone counts; presence of `/data` upload dirs. Resolve duplicates in the SQLite file copy before copying, never on the live file.
2. **Backups**: copy the encrypted files off the volume to a location outside the repo and the Railway volume; `pg_dump` prod before and after. Keep the SQLite files untouched for 14 days after cutover (the rollback artifact).
3. **Schema**: apply the new migrations manually before the deploy (D-DB6 (a)): verified dump, dry run on a restored copy, apply. Create the `app_users` schema and web-only role before the first row is copied (D-SEC3 (a)). Never let a no-deploy migration ride a web deploy by `MIGRATE_ON_BOOT`.
4. **Rehearsal**: run the copier `--apply` into a scratch DB (`cars_cr_*` naming) from a recent copy; compare counts and per-table checksums for all 8 tables; `setval` the 5 identity sequences; check the 13 PG tables carrying bare `user_id` (car_comments, saved_cars, user_search_history, and others) still join to copied ids.
5. **Window**: deploy the code with `USERS_DB_BACKEND` off, then freeze writes (read-only mode, signed-cookie sessions survive). Re-run the copier delta (higher id or updated_at), compare `max(id)` and counts, flip the flag on web only. Verify login, TOTP (if kept), registration, billing read, org invites, view history, search events, operator login at `/dev`. Expected downtime: seconds to a few minutes at this volume.
6. **Canary and soak**: 1 hour read-mostly canary, then 14 days. Rollback = unset the flag and `railway redeploy` (one release). If rollback occurs after the first PG-only write, run a reverse copy for rows written after the flip. dev_users cutover is a separate step with an env-seeded break-glass admin so operator lockout is impossible.
7. **Cleanup (N37.23, N37.24)**: after the soak and a second off-volume archive, delete the Railway variables (`USERS_DB_PATH`, `DEV_USERS_DB_PATH`, `DEALER_PORTAL_DB_PATH`, encryption keys), detach `web-volume`, delete the SQLite code.
8. **Dealer portal**: copy only if the prod file holds rows missing from PG `dealer_vehicles`; the copier is dry-run by default and idempotent.
9. **Uploads**: comment and dealer upload bytes live on disk (`COMMENT_UPLOAD_DIR`, `DEALER_UPLOAD_ROOT`), not clearly on `/data`. Decide bytea vs object storage before the volume is detached (D-PG10). If uploads are on the container writable layer, every deploy has been deleting them.

Never delete `users.db`, `USERS_DB_ENCRYPTION_KEY` or `DEV_USERS_DB_ENCRYPTION_KEY` before the soak and the second backup.

---

## 6. CI guard

`backend/tests/test_no_sqlite.py`, in the default tier, plus a lint-job grep step. It scans git-tracked `*.py`, `*.sql`, `*.sh`, `*.yml`, `Dockerfile*`, `requirements*.txt`, in both non-test and test code. Python files are tokenized or AST-parsed so comments and docstrings are ignored, except SQL string literals.

Fails on:
- `import sqlite3`, `from sqlite3`, `sqlite3.`, `sqlcipher`, `aiosqlite`
- path literals ending `.db`, `.sqlite`, `.sqlite3`, and tracked files with those suffixes (`git ls-files`)
- SQL-literal constructs: `INSERT OR (IGNORE|REPLACE)`, `PRAGMA `, `AUTOINCREMENT`, `datetime('now'`, `json_extract(`, `sqlite_master`, `IFNULL(`, `INSTR(`, `GROUP_CONCAT(`, bare `?` placeholders, `executescript`, `lastrowid`, `row_factory = sqlite3.Row`
- env names `INVENTORY_SQLITE_TESTS`, `*_DB_PATH` for retired stores, `RATE_LIMIT_SQLITE_PATH`

Allowlist rules:
- A file `backend/tests/sqlite_allowlist.txt` lists `path  construct` entries. Seeded by N37.0 with today's offenders (56 to 68 non-test and 63 to 74 test files for `sqlite3`, plus per-construct counts).
- A second test asserts the allowlist can only shrink: every entry must still match an actual hit, so fixing a file without removing its entry fails the build.
- Each deletion unit (N37.7 to N37.16, N37.23) removes its entries. Target size at N37.23: zero. Docs and `CHANGELOG.md` are exempt (history). The guard file itself and the test fixtures that assert detection are the only code exceptions, written as constructed strings.
- A temporary entry for the 3 copier scripts is allowed only until N37.24, and they are archived at tag `archive/sqlite-migrators` before deletion.

---

## 7. Risks and mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Users cutover outage or lost registrations | Highest. Logins, billing, TOTP | Flag off until rehearsed copy matches; write freeze plus delta; rollback via flag; owner-gated like N12; separate release from migrations |
| TOTP seeds and mfa_phone readable by every holder of the inventory DSN (PG has no file-level encryption; SQLCipher goes away) | Confidentiality downgrade (SEC-088) | Drop dead columns first (P16.2); `app_users` schema with web-only role (D-SEC3 (a)); if columns are live, column-encrypt before copy |
| Operator lockout (dev_users) | Loss of /dev access | Env-seeded break-glass admin; separate cutover step |
| Test suite slowness | CI over 30-min timeout, agent 180 s tool kill | Measured 106 ms per clone; shared clones for read-only tests; xdist with one DB per worker; keep the 56-chunk pattern; P95 job gate before merge |
| Coverage loss when SQLite tests are deleted before PG equivalents | Silent regressions | Replace-not-delete per batch; count collected/passed/skipped/xfailed before and after (baseline 4,577 passed); 3 hot-path parity runs (upsert, search, grid) before deleting each SQLite fixture |
| Golden re-record hides real divergences | Wrong data accepted | Review every diff; owner approval (D-PG3); keep xfail(strict) with finding ids for unresolved ones |
| Row-factory and positional-read breakage (263 `row[int]`, 59 to 86 factories) | Silent behavior change | `scalar/one/all` helpers first; migrate `fetchone()[0]` before flipping the default factory |
| Wrong ON CONFLICT targets silently turn replace into skip | Data staleness | Hand-written targets per table; unique-key test from `information_schema`; B-002/B-003 style PG failing tests first |
| `?` codemod mistakes (bare `%` in LIKE, f-string SQL, `?` in strings) | Runtime SQL errors | Lexer-based codemod; guard that parses every SQL literal; manual pass for 287 f-strings |
| Text timestamps vs timestamptz (49 text columns, `cars` 1,221 MB) | ALTER rewrites the table; type changes str → datetime | Out of scope for N37 core; keep text and fix comparisons (N8/N9) unless D-PG11 says otherwise; dual-read helper if converted |
| Case-insensitive LIKE (35 unwrapped sites) | Search regressions | Per-site decision: `lower()` both sides or ILIKE; test with mixed-case rows |
| Merge contention on `conftest.py`, `ci.yml`, `incomplete_listings_db.py`, `users_db/*` | Blocks critical path | Land harness extras after N8.6; follow the ownership table; batch deletions after owner units release |
| Harness hits the wrong database (D-TC9) | Data loss in `cars` (3,239 MB) or rehearsal DBs | Guard refuses `cars`, `cars_cleanroom`, non-local hosts, port 15432; dedicated ephemeral cluster for tests; test that the guard refuses |
| Scan lab writes prod `cars` today (`scan_lab.py:466`) | Possible live pollution | N37.13 first priority within wave 2; verify now with a read-only look at the env passed to the subprocess |
| Local PG 17.10 vs prod/CI 18.x | Version skew | Dev cluster on pg18 via `pg_test_up.sh`; CI pg18 |
| Dictionary catalog changes prod EPA resolution (PF-1, 128 YMMs) | Wrong specs | Replay test against current glob output before the first prod load |
| Uploads lost when volume detached | Missing user images | Decide uploads storage before N37.24 (D-PG10) |
| V013 stale vs the current `init_users_db` ALTER ladder | Copy fails or drops columns | Re-diff before applying; schema-from-migrations test |
| FK cascades enforced on PG, unenforced on SQLite | Surprise deletes | Review `org_invites` cascades; tests for user delete |
| Deleting a local `.db` that holds the only copy (`backend/users.db` 8 accounts; `backend/inventory.db` 26 recipe rows) | Loss of dev data | Back up via the copier `--dsn` path, compare against PG, then delete |

---

## 8. New owner decisions (recommended defaults)

| ID | Question | Recommended default |
|---|---|---|
| D-PG1 | Scope of "all postgres": (a) every runtime, test and generated store; (b) runtime and tests only; (c) prod only | (a) |
| D-PG2 | Reverse D-TC4 "Targeted" to whole suite on PG, drop N31's 30-day wait, N7 becomes the default tier (L) | Yes. Keep D-PLAN1 (N7 right after N5) |
| D-PG3 | One-time re-record of SQLite goldens (search, upsert) on PG, against the "never edit expected JSON" rule | Yes, with every diff reviewed and logged; otherwise pin each divergence xfail(strict) |
| D-PG4 | SEC-088 at-rest answer and placement of users/operators: separate DB, `app_users` schema with web-only role (D-SEC3 (a)), or column encryption | Keep D-SEC3 (a); drop dead `totp_secret`/`mfa_phone` first (P16.2); no app-level encryption unless those columns are live. Same cluster, dev_users in the same schema |
| D-PG5 | Rate limiter and dev job store without SQLite | Rate limiter in-process only (prod is `GUNICORN_WORKERS=1`); UNLOGGED table only if workers rise above 1. Dev job store: delete the SQLite option |
| D-PG6 | Dictionary catalog form: Postgres table with path indirection, in-memory from `manifest.json`, or fold onto `epa_master`/`dictionary_options` | Postgres table now (N37.14), revisit folding later; replay test required |
| D-PG7 | Scan lab: separate schema/database vs drop; reload the 4,386 cars | Scratch database `cars_lab`; rescan instead of copying |
| D-PG8 | Dead stores (`bmw_store`, BMW reindex, `vehicle_reference`) | Delete (consistent with D-DC7 (b)) after the consumer grep |
| D-PG9 | Migration numbering for `admin_users` and others (V031 to V034 already allocated) | V035 or later, assigned by the N1.9 registry owner now |
| D-PG10 | Uploads storage when `web-volume` is detached (bytea vs object storage) | Decide after the N37.19 census; default object storage |
| D-PG11 | Timestamp typing: convert 49 text timestamp columns to timestamptz, or keep text | Keep text for N37; fix only the guard comparisons in N8/N9 |
| D-PG12 | Test DB host: shared `cars` server with D-TC9 guard vs dedicated ephemeral cluster | Dedicated ephemeral pg18 cluster by default (`scripts/pg_test_up.sh`); shared server only as explicit override |
| D-PG13 | pytest-xdist (D-TC8) | Approve if measured serial time exceeds 12 min; one DB per worker |
| D-PG14 | `?` placeholder style: keep a PG-only shim vs rewrite everything to `%s` | Rewrite to `%s` (N37.6); the shim and lexer then get deleted so the CI guard can ban `?` |
| D-PG15 | Rollback artifact retention and write-freeze acceptance | Keep SQLite files 14 days off-volume; accept a short write-freeze |
| D-PG16 | Re-scope P13A.6, P16.3, P16.6 (R-22) | Yes, as in section 4 |
| D-PG17 | Local PG major | Upgrade the dev test cluster to 18 (the running Homebrew server is 17.10); verify pgvector for postgresql@18 |

Open facts still unread: prod row counts in `/data/*.db`; whether prod `/data` holds uploads; whether V026/V027 are applied on prod and on the retired mini; the cost of the seeded template; the real cost of the cleanroom test conversion.
