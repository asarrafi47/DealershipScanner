# Changelog

## [Unreleased]

## [1.5.14] - 2026-11-03

### N29 (Dockerfiles and requirements)
- Requirements split per image: requirements-web.txt (web image, no Playwright), requirements-browser.txt (Playwright, discovery/dev), requirements.txt now includes both; every Dockerfile copies requirements*.txt (P15B.2).
- scripts/scanner_import_sweep.py gains --profile scanner|web|discovery (P15B.2).
- Embedding model id and Hugging Face revision now live in backend/vector/embedding_model.py; all three SentenceTransformer call sites load through it with revision= (P15B.3).
- A production LISTING_EMBEDDING_MODEL override without LISTING_EMBEDDING_REVISION is refused (P15B.3).
- DEFAULT_MODEL_REVISION is pinned to `1110a243fdf4706b3f48f1d95db1a4f5529b4d41` (P15B.3o). Its weights, tokenizer and config are byte-identical to the 2025-03-06 head, and encoding the same sentences with both gave a max absolute difference of 0.0, so the stored embeddings stay valid.
- Dockerfile.web bakes the embedding model into /app/models (HF_HOME) and sets HF_HUB_OFFLINE=1 and TRANSFORMERS_OFFLINE=1. The bake skips the TensorFlow, Rust, Flax and ONNX/OpenVINO copies of the weights: 183 MB instead of 977 MB for the default model.
- Dockerfile.web installs CPU-only torch (pinned ARG TORCH_VERSION=2.12.1, pytorch cpu index) before requirements-web.txt (P15B.4). The web image no longer carries the CUDA wheels.
- Dockerfile.discovery installs requirements-scanner.txt + requirements-browser.txt instead of the full superset; scanner_import_sweep --profile discovery allows the web/ML packages to be absent (P15B.5).
- New backend/tests/test_dockerfiles.py static checks for the web and discovery images (P15B.4, P15B.5).
- CI: the pytest job now runs pip-audit on the installed environment and on requirements-scanner.txt (P15B.7). Its first run found setuptools 78.1.0 on the runner: requirements-web.txt gains a `setuptools>=78.1.1` floor (PYSEC-2025-49, a PackageIndex path traversal). Two later setuptools advisories, PYSEC-2026-3447 (macOS sdist MANIFEST.in exclusion) and GHSA-xmj3-9gf7-h52w (Windows-only legacy wheel install), need setuptools 83.0.0 and 84.1.0, which torch's `setuptools<82` requirement rules out, so the environment audit ignores those two ids and nothing else. `AUDIT_IGNORES` in test_requirements_hygiene.py pins the list; drop the ids once torch lifts its cap.

## [1.5.13] - 2026-11-03

### N7 (first merge)
- Test infrastructure: Postgres test tier (pg marker, backend/tests/pg_harness.py). Each pg test gets a throwaway database cloned (STRATEGY WAL_LOG) from a template built by the migration chain; TEST_PG_ADMIN_DSN is the only input and the harness refuses cars, cars_cleanroom, Railway hosts, non-local hosts and port 15432 (P14A.1).
- pytest.ini registers the pg marker and enables --strict-markers (P14A.1).
- conftest: pg_template (session) and pg_db (function) fixtures; the no-DSN skip is counted and REQUIRE_PG=1 turns it into a failure (P14A.1).
- CI: the `pytest-pg` job replaces `pytest-integration`. It runs `-m pg` against a pgvector 0.8.1 / Postgres 18 service container with REQUIRE_PG=1, fails if nothing is collected, and uploads reports/pg.xml. The `integration` marker stays but no CI job runs it. The required-check context on main changes to `pytest-pg` (P14A.3).
- Tests: pg_harness seed helpers add_dealerships, add_dealer_geopoints, add_dealer_catalog, add_jobs and add_recipes (P14A.2).
- Tests: the live-Postgres schema parity, migrate lock and record-only tests and the seeded data-quality regression now run on the pg harness; new pg tests prove migrate --apply reaches the chain head, reruns are idempotent, edited applied files exit 1 with CHECKSUM DRIFT, and V001's search_path reset does not break later files (P14A.4).
- Tests: Postgres tier executes the adapted UPSERT_CARS_SQL and one statement per adapter rule on a migration-built database (P14A.5).
- Tests: upsert_vehicles golden replays on Postgres with zero divergences from the SQLite golden, plus test_upsert_pg.py write-path cases (P14A.6).
- Tests: Postgres concurrency tests for InventoryWriteCoordinator (real deadlock and lock_timeout retries, VIN-owner backstop single owner) in test_inventory_write_pg.py (P14A.7).
- Tests: the pg harness's module-level flag sweep reviews the dealer_locator Google cache globals, and resets the vPIC `_cached_hit` and `family_trim_count` caches between pg tests.
- Release note: CI's required-check context on main moves from `pytest-integration` to `pytest-pg`. If a GitHub ruleset requires the old name, update it before opening PRs to main.

## [1.5.12] - 2026-11-02

### Audit fixes 2026-10-26

#### Bugs
- Billing: Stripe success URLs now require an active or trialing subscription, and the legacy premium route rejects non-Complete plan sessions (B-032). A subscription.deleted event for an old subscription no longer revokes current premium (B-041).
- Fixed: the VIN-in-URL fallback for listing stickers requires a whole-token, VIN-shaped match, so hashed sticker URLs are no longer dropped (B-066).
- Fixed: inventory trim ladder query is valid on Postgres (MIN(trim) under GROUP BY) (B-003).
- Fixed: pgvector dealer reindex took state from latitude and crashed after TRUNCATE; it now reads the state column (B-034).
- Fixed: grid card resolve includes the pending flag in aux_key, so pending cars are no longer rebuilt on every request (B-029).
- Fixed: discovery upsert fuzzy dedupe no longer merges rooftops that differ by city or osm_id (B-028).
- Fixed: vPIC per-VIN lookups no longer cache misses or DB errors (B-033).
- Fixed: 'Automatic with manual shift mode' and CVT with manual mode are no longer classed Manual; manual gearboxes that mention 'auto' (rev match, blip) stay Manual (B-025).
- Fixed: 'V6 Hybrid Electric' engine text parses as 6 cylinders rather than 0; a range-extender blob such as '1.5L I3' now parses as 3 (B-018).
- Fixed: hex colour values such as '#ffffff' are rejected as exterior_color (B-068).
- Fixed: forced-induction fallback no longer labels NA Ford/VW/Genesis/Lincoln/Hyundai/Kia/Honda Fit/Lexus UX engines turbo, and 'si'/'n' trim tokens are word-bounded (B-019, B-020, B-021).
- Fixed: static model-spec fallbacks no longer hard-code a drivetrain for mixed-layout nameplates, so VIN-decoded values are not overridden by a guess (B-014).
- Fixed: interior colour hint from descriptions requires the colour inside the candidate phrase (B-022); the interior colour lexicon uses word boundaries, so Titanium is not tan and Preferred Suede is not red (B-023).
- Fixed: BMW i3/i4/i7 in search queries are no longer read as inline-N cylinder counts (B-024).
- Fixed: trim spec extraction matches trim names as whole words, so SE no longer matches SEL and LT no longer matches LTZ (B-026); the Honda/Acura brochure trim filter no longer drops Touring (B-027).
- Fixed: a Dealer.com listing with no odometer reading is no longer labelled New (B-017).
- Fixed: oversized equipment-vision packages JSON is trimmed by shortening its lists instead of being cut mid-token (B-010).
- Fixed: scraping adjudicate_crawl and org_validation use package imports (B-007).
- Fixed: discovery merge keeps Google place/rating/status/oem/phone fields and the non-dealer verdict (B-009).
- Fixed: deleting a dealer user retires their mirrored public cars rows (B-008).
- Fixed: scan lab refuses to start without SCAN_LAB_DATABASE_URL and its scanner subprocess runs with PROJECT_DOTENV_DISABLE=1 (B-030); scan_irvinebmw_test.sh refuses to run without SCAN_TEST_DATABASE_URL (B-031).
- Fixed: a failed window-sticker image no longer hides the parsed options and packages block (B-038).
- Fixed: the inventory poll and a ?page=N deep link keep the current results page instead of resetting to page 1 (B-039).
- Fixed: the depreciation forecast card no longer appears, with an invented $20,000 price, on call-for-price or payment-listed cars (B-040).
- Fixed: Rivian, Lucid, Polestar, McLaren, Aston Martin, Rolls-Royce, Scion, Saturn, Pontiac and similar marques are no longer dropped from the global Make facet.
- Fixed: html_cards no longer lets a defaulted card condition of Used block the hydration object's New/Used condition or isNew flag.
- Fixed: the generated spec sheet no longer prints a sibling trim's horsepower or torque when the car has no catalog link and no exact trim row.
- Fixed: cached lease-offer matches refresh after 6 hours and immediately when a matched car is no longer active (B-035).

#### Security
- Profile edits can no longer move an account onto an APP_ADMIN_EMAILS / APP_ADMIN_USERNAMES identity (SEC-A-001).
- Stripe checkout success URLs fail closed unless the subscription is active or trialing (SEC-A-003); a renewal needs premium=1 subscription metadata (SEC-A-040, partial: a late 'active' event after 'deleted' can still re-grant).
- Resubmitting a review no longer republishes one an admin removed, and an edit clears its old report-dedup rows (SEC-A-007).
- Review add-on fee description capped at 300 characters, checked for URLs and profanity, and NaN/inf fee amounts rejected (SEC-A-038).
- Dealer-supplied listing sticker fetch anchors the iPacket URL pattern, trusts only exact iPacket hosts, refuses non-global destinations on every hop, follows redirects manually (max 3), and caps the body at 15MB (SEC-A-002, SEC-A-018; PDF render-size clamp not done).
- Per-IP limit on /api/listings cache-miss builds and /api/coords-to-zip (SEC-A-005).
- Dealer locator caches Google Places results by rounded centre and caps anonymous callers to a single Places tile (SEC-A-008, partial).
- Login locks an account for 15 minutes after 10 failed attempts (counted across IPs, shared across username/email, slot reserved before the password check) on /login, /api/auth/login and /dealer/login; IPv6 clients are rate-limited per /64 (SEC-A-010, partial). Known tradeoff: an attacker can lock a known account for 15 minutes; the per-process counter needs RATE_LIMIT_SQLITE_PATH or a single worker in prod.

## [1.5.11] - 2026-11-02

### Efficiency quick wins (N6b)
- Tests: bcrypt work factor drops to 4 under pytest only (BCRYPT_TEST_LOW_COST=1 set in root conftest.py); production floor 12 and default 13 unchanged; test_paid_access_parity 34s -> 4s.
- Perf: memoize adapt_sql_for_postgres_execute with lru_cache(maxsize=512); repeated upsert statements no longer re-run the SQL adapter per row.
- Facet options: background rebuilds are throttled to one per FACET_REBUILD_MIN_INTERVAL_S (default 300, 0 disables). The first build and clear_inventory_listings_cache() still rebuild immediately.
- Paint-family facets: color inference is memoized per distinct color string within a rebuild.
- Performance: the dealership registry backfill now runs in a background thread at process startup instead of inside the first /api/listings/geo-coords request (~210 s stall removed).
- pg_connect now bounds connection setup with connect_timeout (env PG_CONNECT_TIMEOUT, default 10 s); a connect_timeout already present in the DSN is left alone.
- Corrected stale worker-count and memory comments in docker-entrypoint-web.sh and gunicorn.conf.py (prod runs GUNICORN_WORKERS=1, ~3.3 GB warm).
- Fleet runner: the batch scan timeout (--scan-timeout) now counts active time only, so a Mac sleep no longer kills batches and leaves dealers with a false error verdict (pipeline/active_time.py, run_http_only_scan polls a Popen).
- Logs: scan batch lines print wall and active time; dealer_run_summary and the scan log carry wall_seconds beside seconds (active).
- Ops: docs/NETWORK_SCAN_PROCESS.md says to launch fleets under caffeinate -dims; scripts/cleanroom/preflight.sh warns on macOS when it is not running.
- Web image now bakes the pgeocode US postal cache at build time (PGEOCODE_DATA_DIR=/opt/pgeocode), removing the 5.6 MB GeoNames download on first import of geo.

### N6.1 Index rebuild
- Incomplete-listings index is rebuilt once per fleet scan, after every shard's vPIC heal and reconcile and before market stats and the grid cards, so the persisted cards match the final index.
- Index rebuild runs in chunks of at most 20k cars into a staging table, swaps into `incomplete_listings` in one transaction (fixes the DELETE+INSERT race), logs per-chunk seconds, and takes a Postgres advisory lock so concurrent rebuilds skip.
- `incomplete_listings_meta` now stores `rules_version` and `last_rebuilt_at`; bumping `INDEX_RULES_VERSION` forces a rebuild.

### N6.2 Re-sync after late writers
- Incomplete-listings index is re-synced for cars changed after the per-car sync: the structured spec backfill, the model_specs correction, the catalog link and the pipeline vPIC heal.

### N6.3 Admin counts
- Admin incomplete-listing count, per-dealer issue stats and the dev incomplete-cars list count active listings only. `get_cars_by_ids` gains an optional `active_only` flag.

### N6.4 New-arrival window
- Added: new-arrival pending class (D-N13). A car whose only gap is price or photos and whose latest scrape is within 3 days of its first sighting is classed pending, not incomplete. It stays visible and shows a plain "Details coming" line on the grid card and the car page, with no box.
- Changed: the `incomplete_listings` index carries `first_seen_at`, `last_scraped_at`, `scans_survived` and `pending` (migration V027). `INDEX_RULES_VERSION` is bumped to `2026-10-n6.4`, so the next fleet post step rebuilds the index.
- Added: `backend/scripts/incomplete_report.py --window N` prints inside-window and outside-window incomplete rates separately.
- Fixed: the grid card read path (`grid_cards_repo._resolve`) computes `_aux_key` with the pending flag, like the writer, so a pending car's stored card is no longer rebuilt on every read (audit B-029, moved here from the audit-fix series so this release stands alone).

### N6.5 Junk VDP values
- Fixed: dealer.com VDP parser no longer stores junk as values; `_clean()` unwraps `{_type,en_US}` objects and treats `[[...]]`, `[token]`, null and (for colours) option codes like X9/21 as missing.
- Fixed: the cars upsert drops a stored junk trim/colour to NULL when a rescan has no real value, and a junk incoming value never overwrites a real one (bracket detection is Postgres-safe).
- Fixed: the completeness invariant no longer counts junk trim/colour values as filled.
- Added: `backend/scripts/heal_vdp_junk_values.py` (dry run by default) to NULL existing junk trims/colours with quarantine and provenance.

### N6.6 Scan-lab verifier
- vPIC plug-in hybrids bucket as `phev` (and EPA plug-in rows too), so correct PHEV listings no longer raise `vpic_fuel`.
- MINI vPIC models of just "2 Door"/"4 Door" no longer raise `vpic_model`.
- A vPIC decode with ErrorCode other than 0 is no longer used to flag discrepancies; ErrorCode is read from the vPIC cache.

### N6.7 Prices
- Prices under $1,000 or over $1,000,000 are refused at write time (NULL, D-N14); the withheld value is noted in the info log and on the in-memory row (`price_withheld_note`).
- Completeness counts a junk or read-time-withheld positive price as a missing price instead of trusting the stored column.
- Added: `backend/scripts/repair_junk_prices.py`: dry run by default, lists junk active prices (below floor, above cap, same price on 5+ models at one dealer); `--apply` needs a backup file and `--expect-db`. The repeated-value rule is repair-only (not wired at write or read time).

### N6.8 Placeholder images
- Added: `backend/utils/placeholder_images.py`: one shared placeholder-image test (tokens `photo_unavailable`, `unavailable_stockphoto`, `photo-not-available`, `coming_soon`, `comingsoon`, `default.jpg`, `/static/placeholder.svg`, plus same-image-on-many-cars per dealer) and a per-dealer photo-coverage alarm (D-N15).
- Parser URL filter and listing completeness use it: a placeholder-only car counts as having no photo.

### N6.9 Identifier gate and mileage coverage
- Rows whose VIN is not 17 characters are refused at parse (dealer_eprocess, chapman, overfuel, dealermasters, team_velocity, jazel, motive_ridemotive) and again at upsert (order numbers, Koons 8-character codes); 17-character IDs with I/O/Q are kept (D-N17b, N21).
- Model year 0 or NULL is recovered from the VIN or the row is refused at upsert; upsert stats report `identifier_gate_refused` by reason.
- Pipeline coverage: used-row mileage (`used_mileage`, `used_zero_mi`, `used_zero_mi_my2026`) joins the capture coverage and the assess gate; MY2026+ used cars at 0 mi are reported as a data question, not counted as incomplete (D-N16).

### P8A.2 Fact precedence (pure module)
- Added `backend/vehicle_facts/precedence.py`: a pure write-time arbiter with per-(field, source) ranks, a stale-provenance rule, the vPIC 4x2 constraint (never AWD or an end), the D-PR4 catalog tiebreak with a trim veto, the D-PR6 cannot_assess flag and the D-PR7 vPIC cylinder guard. Not wired into any writer yet (N15).

### P8A.6 Read-time drivetrain
- The displayed drivetrain follows NHTSA vPIC whenever vPIC states an end (FWD/RWD/AWD/4WD) that disagrees with the trim decoder, EPA or dealer value. 4x2/2WD never overrides and is never mapped to an end. Merge-specs golden re-recorded (drivetrain-only diff).

## [1.5.10] - 2026-11-02

### Added
- `migrations/V026__chain_completeness.sql` (P4.3; D-DB7 (a), D-PR8 (a), D-N30). After it, a
  fresh `migrate --apply` builds every object that prod and local `cars` hold, except the
  D-DB1 backup tables. It adds:
  - the V019-only objects: `car_move_log` (with its sequence, pkey and index),
    `cars_trim_quarantine` and `package_values_msrp_quarantine`;
  - `review_reports` and `haiku_spec_cache`, exactly as the runtime DDL creates them;
  - the 4 untracked log tables `image_summary_restore_log`, `make_canon_log`,
    `model_canon_log` and `package_value_purge_log`;
  - `uq_option_rejections_standing` as NULLS NOT DISTINCT.

  Every step reads the catalog first and is additive. It writes no row and never touches
  `cars`. On a V025 database (prod and local `cars`) it only creates `review_reports` and
  `haiku_spec_cache` and rebuilds the uq index, which locks `option_rejections` alone for
  milliseconds. A duplicate group RAISEs and rolls the file back. The spec's pgvector
  `*_embeddings` part is dropped, because no such table exists. **Not applied yet:** local
  `cars` and prod record 25 until P4.8 (owner steps). `cars_cleanroom` never gets it (D-N1).
- `migrate.py` options (P4.2):
  - `--target N` applies pending files up to VN only. Later files stay pending and are
    listed, and `database is up to date` is never printed while files are held back.
  - `--record-only N[,M]` records versions without running them.
  - `--lock-timeout` (default `5s`) and `--lock-wait` (default `10min`); see Changed.
- `backend/scripts/schema_drift_report.py` (P4.1). It builds `migrations/` into a
  throwaway `pytest_chain_<hex>` database on the local server, then snapshots that
  database and a live target with the catalog queries of `test_schema_from_migrations`.
  It lists live-only objects, chain-only objects and definition mismatches.
  - Each difference is explained by the `KNOWN_DRIFT` registry (schema ledger and owner
    decisions) or by a migration the target has not applied. Anything else is UNEXPLAINED
    and the exit code is 1.
  - The target session must report `default_transaction_read_only` and
    `transaction_read_only` on, and runs only SELECT, SHOW and `SET statement_timeout`.
  - The scratch builder refuses `cars`, `cars_cleanroom`, non-local hosts, port 15432, an
    existing database name and the target's own name. It drops the scratch DB in `finally`.
  - P4.3 added `Known.while_pending` (a difference explained only while V026 is pending)
    and a `column_types` snapshot key (typmod, identity, generated, collation).
  - Run on 2026-10-18: 0 unexplained differences on `cars` and on `cars_cleanroom`, and 0
    rows written to either. The owner pack for a read-only prod run is prepared but has
    not been run.
- `backend/scripts/capture_module_ddl_golden.py` and
  `backend/tests/fixtures/module_ddl_schema_golden.json` (P4.3). The script runs each
  module's `ensure_*` helper and the script DDL on empty scratch databases and records
  what they create. `--check` captures again and compares.
  - `test_schema_from_migrations.py` checks statically that the files a fresh build runs
    cover both goldens, and checks V026's guarantees.
  - Opt-in live tests (`SCHEMA_PARITY_PG_DSN`) check V026 on a V025 build: the dry run
    lists only V026, the exact objects are created, a second run is a no-op, a duplicate
    group is refused and the index is rebuilt.
- `backend/db/runtime_ddl.py` (P4.6): `pg_legacy_ddl_needed()`, `pg_missing_relations()`
  (a `to_regclass` probe that takes no lock) and `run_pg_legacy_ddl()`. Runtime DDL now
  runs only when the database is behind the migration chain (see Changed).
- New tests: `backend/tests/test_schema_drift_report.py` (P4.1),
  `test_inventory_pg_adapter.py` (P4.7), `test_schema_strict_mode.py` (P4.5) and
  `test_runtime_ddl_gating.py` (P4.6). `test_migrate.py` (P4.2) gains fake-connection and
  opt-in live tests (`TEST_PG_ADMIN_DSN`, guarded `pytest_*` databases) of the V019 rule,
  `--target`, `--record-only` and the locks.

### Changed
- `backend/scripts/migrate.py` (P4.2):
  - V019 is recorded without being run in two cases only: V001 runs earlier in the same
    invocation (a fresh build), or all 48 objects V019 shares with V001 already exist.
    Otherwise migrate refuses before writing anything and names the missing objects. The
    check runs again under the lock before the row is written.
  - Its first log line is the target host, port and database.
  - Each file runs under `SET LOCAL lock_timeout` (5 s by default), on the
    `MIGRATE_ON_BOOT` path too.
  - Every write transaction takes `pg_advisory_xact_lock` and re-reads `schema_migrations`
    under it, so concurrent runs, such as two web boots, run one at a time. A second
    runner waits up to `--lock-wait`, then skips what the first one applied, or exits 1.
  - `schema_version.describe_gap` picks its hint by whether `public.cars` exists: an empty
    database gets `migrate --apply`, a populated legacy one gets `--baseline 1`.
  - `migrations/README.md` gains a V019 section, the D-DB6 protocol, the rule "a new table
    is a new migration, never runtime DDL", what `MIGRATE_ON_BOOT` implies, and the new
    options and locks.
- `backend/scripts/migrate.py` never writes to `cars_cleanroom`, the frozen clean-room
  baseline (P4.3b, D-N1). There, `--apply` (with `--baseline` or `--record-only` too) logs
  `REFUSE --apply` and exits 2 before any lock or write, and every write transaction
  re-checks `current_database()` before it takes the advisory lock. A dry run still
  reports. `cars_cr_*` scratch copies migrate as usual. New fake-connection tests and one
  live test in `test_migrate.py`.
- `migrations/V026__chain_completeness.sql` no longer sets its own
  `SET LOCAL lock_timeout = '5s'`, which overrode `migrate --lock-timeout` (P4.3b). The
  runner's per-file 5 s still applies, and the flag now governs V026 as well: on a scratch
  V025 build with `option_rejections` locked for 8 s, `--lock-timeout 1s` failed after 1.3 s
  and `--lock-timeout 30s` applied V026 after 8.0 s. The checksum is now
  `sha256:3a037d828db71a4b97e928e22cdb3c0746aff568a1803a0f1392157cc505e6f6`. Only throwaway
  databases had recorded the old one; local `cars`, `cars_cleanroom` and prod are at V025.
- `docs/RELEASING.md` "Migrations" now describes the N5 runner (P4.3b): the prod command is
  `--apply --target N`, the first log line names the host, port and database, the
  `--lock-timeout` and `--lock-wait` flags, and the `cars_cleanroom` refusal.
- Hot-path runtime DDL is gated on the migration chain (P4.6): `reviews.store`,
  `scanner.recipe_store` and `scanner.job_queue`.
  - On a Postgres database at the `migrations/` version they issue no DDL at all. A
    dealership page request used to run CREATE TABLE plus two CREATE INDEX IF NOT EXISTS
    statements, which take a SHARE lock even when the index exists.
  - On a database behind the chain, only the missing objects are created, after
    `SET LOCAL lock_timeout = '3s'`.
  - `recipe_store` never runs ALTER on Postgres. A missing `scan_hints` column logs a
    WARNING that names migrate.
  - The SQLite paths are unchanged. `job_queue` is Postgres-only, and it has no SQLite
    caller.
- The repo chain now ends at V026, so `expected_schema_version()` returns 26 (P4.3). Until
  P4.8 applies V026 to local `cars` and prod (both at 25):
  - the default `INVENTORY_SCHEMA_CHECK=warn` only logs the gap;
  - `strict` makes processes from this release refuse those databases.
- The strict-mode fallback warnings say "re-assert failed" instead of "skipped (table
  busy)", because a busy table is not always the cause (P4.5).

### Fixed
- `INVENTORY_SCHEMA_CHECK=strict` did not propagate (P4.5).
  `schema_repo.init_inventory_db` and the scanner's `database._ensure_schema` caught
  `SchemaNotMigratedError` in a broad `except Exception` and logged it as a busy
  re-assert. As a result, the web failed late in `init_dealer_portal_db` and the scanner
  kept writing on every upsert. Strict mode now refuses at init and before any upsert
  write. Warn mode and the busy-table fallback are unchanged.
- `schema_version.py` and `inventory_pg.py` no longer say prod is at V018. The P0A.2
  census (2026-10-08) found prod and local `cars` both at V001-V025 (P4.5).
- `inventory_pg`'s legacy DDL ran a session-level `SET lock_timeout = '3s'`, so the
  scanner's raw connection kept a 3 s lock_timeout on every later upsert. It is now
  `SET LOCAL`, which was verified live (P4.6).
- The SQLite-to-Postgres placeholder rewrite (`qmarks_to_percent_s`) understood only `'`
  literals, so one apostrophe in an SQL comment flipped every later `?` (P4.7).
  - `UPSERT_CARS_SQL` adapted correctly only because the 4 apostrophes in its comments
    happened to balance. With an odd number, psycopg rejected the output.
  - The rewrite now reads `'` literals, `"` identifiers, `--` comments and nested `/* */`
    comments the way Postgres does, and doubles a `%` anywhere outside code.
  - The output is byte-identical for all 1,334 DML literals in `backend/` and `scripts/`.
    It is about 6x faster on `UPSERT_CARS_SQL` (0.28 ms to 0.05 ms).
  - The INSERT OR IGNORE fallback drops a trailing `;` and trailing comments before it
    appends ON CONFLICT DO NOTHING.
- A fresh `migrate --apply` could not get past V019, which re-creates 18 tables without IF
  NOT EXISTS. It now records V019 under the rule above (P4.2).
- The live parity test `test_migrations_build_a_superset_of_the_runtime_ddl` was broken: its
  keyword conninfo fell back to the default database. It now builds with
  `migrate.main(['--apply'])`, URL DSNs and `pytest_*` names (P4.3).
- The comment on `MIGRATE_LOCK_KEY` in `migrate.py` gave the advisory lock key as
  7883954311434658097. The key is 7883946363647714609 (`0x6d69677261746531`), so a lock
  held by hand or a `pg_locks` lookup with the old number never matched (P4.3b, comment
  only; a test pins it).

### Security
- migrate's new target log line prints only the host, port and database. It never prints
  the DSN's user, password or options (`test_target_is_logged_without_credentials`, P4.2).

## [1.5.9] - 2026-11-02

### Added
- `deploy/railway/service_settings.json` and `deploy/railway/service_settings.py` (N4.1,
  D-N24): web's and scanner-nightly's Railway settings now live on the service instance,
  because Railway stops reading `railway.toml` / `railway.json` on 2026-12-01. The JSON
  holds the expected values. Web: `Dockerfile.web`, healthcheck `/health` within 300 s,
  restart `ON_FAILURE` up to 5 times, 1 replica. scanner-nightly: `Dockerfile.scanner`, no
  healthcheck, restart `NEVER`, 1 replica, 16 vCPU / 16 GB. Cron and the config-file path
  are unset on both. The stdlib helper reads the live settings with one read-only
  `railway api` query and compares them field by field. It refuses to send anything but a
  plain query. It prints the owner-pack query and the `serviceInstanceUpdate` command, and
  never runs a mutation. `docs/RELEASING.md` gains "Service settings (N4.1, D-N24)": the
  mechanism, the expected values, the check, the owner packs and the cron path for N12.11.
  The web pack runs only after `web-source-clear`. The scanner-nightly pack runs only
  under option (a) or inside N12.2.
- `backend/tests/test_railway_service_settings.py` (N4.1) checks that the settings equal
  the deleted files, the audited service ids, the read-only query guard, the pack commands
  and the check's exit codes. `backend/tests/test_deploy_guard.py` gains stubbed-railway
  tests of the settings check.
- `backend/tests/test_deploy_references.py` (P15B.6) checks that:
  - the deleted worker/scheduler files stay deleted;
  - their names appear only in `docs/` and CHANGELOG.md;
  - the N4 exit-gate grep matches only the allowlisted k8s manifest;
  - compose runs only `postgres` + `web`, on a pinned pgvector image at prod's major
    version 18;
  - every Dockerfile named by a Railway config or by `service_settings.json` exists. The
    test fails if it found nothing to check.
- `backend/tests/test_branch_naming_docs.py` (N4.3) checks that RELEASING.md, plan
  Section 7.0 and the Section 10.1 runner template agree on `phase/<NID>` /
  `unit/<unit id>`. It also checks that the `feature/http-only-scans` retirement stays
  owner-gated with fixed-text gate greps, and that every in-page link in RELEASING.md
  resolves.
- `docs/ops_log/N4.4.md` (N4.4) records the mini as excluded under D-N2 (b). It lists the
  re-planned units: D-DB5 (b), D-IF10 = MBP, D-IF1 host = MBP, P6A.7 with two hosts,
  P15A.6 on the MBP, and the Phase 7 rollout without the mini. It also holds read-only MBP
  evidence that the mini does not scan, and the revival steps for D-N2 (a).
  `docs/remediation/OWNER_DECISIONS_LOG.md` gains one row for this record. It is not a new
  owner answer.

### Changed
- `deploy/railway/deploy_web.sh`, `deploy_scanner_nightly.sh` and `_guarded_deploy.sh`
  (N4.1) check the live service settings before every deploy and dry run. They compare
  them with `service_settings.json` at the deployed ref. On any difference, or an
  unreadable answer, they refuse with exit 1 and nothing staged.
  - `ALLOW_UNRELEASED_DEPLOY=1` never overrides the check.
  - `--no-settings-check` is accepted for dry runs only.
  - A release that carries a root `railway.toml` or `railway.json` is refused.
  - `deploy_scanner_nightly.sh` no longer copies a `railway.json` into the stage.
  - RELEASING.md steps 12, 13 and 15 describe the new path.
- `deploy/docker-compose.yml` (P15B.6): the stack is now `postgres` + `web`.
  - Postgres moves from `postgres:16-alpine` to `pgvector/pgvector:0.8.6-pg18` (prod runs
    PG 18.6 with vector 0.8.6).
  - Its data volume, `postgres18_data`, mounts at `/var/lib/postgresql` (the PG18 image
    layout). The old PG16 `postgres_data` volume is never handed to PG18.
  - `deploy/up.sh` builds and starts web only.
  - `scripts/docker-entrypoint-scanner-worker.sh` is renamed to
    `docker-entrypoint-jobs-worker.sh`, and `Dockerfile.discovery` points at the new name.
    `Dockerfile.scanner` no longer chmods the worker/scheduler entrypoints.
- `docs/RELEASING.md` "Branching model" (N4.3, LE-5):
  - Phase branches are `phase/<id>`. Unit branches are `unit/<unit id>`, in
    `.claude/worktrees/<unit id>`.
  - A `.claude/worktrees/phase-<id>` integration worktree is described, with its create
    command. It must be removed before `git switch phase/<id>`.
  - A phase branch is rebased only when another release moved `main`. After a rebase,
    unit branches are checked with `git cherry` before `-D`.
  - New section "Retiring `feature/http-only-scans`": an owner-only step behind four
    gates. The gates are: ksarrafi told, the first phase release on main, the mini check
    or a logged waiver, and no checkout on the branch.
  - The release checklist no longer offers that branch.
  - `docs/NEXT_PHASES_PLAN_2026_10.md` Sections 7.0 and 10.3 now match. Release step 4
    rebases only when needed. A stale worktree is checked against `base_sha`, because unit
    branches have no upstream.
- `docs/RELEASING.md`, `RESUME_HERE.md` and `deploy/AUTONOMOUS_DEV.md` no longer describe
  the worker/scheduler stack (P15B.6). `docs/NEXT_PHASES_PLAN_2026_10.md` carries the N4
  unit status.

### Removed
- The root `railway.toml` and `deploy/railway/railway.scanner-nightly.json` (N4.1). Their
  values moved to `deploy/railway/service_settings.json`.
- `Dockerfile.scanner-worker`, `Dockerfile.scanner-scheduler`,
  `deploy/railway/railway.scanner-{worker,scheduler}.toml` and the compose
  `scanner-worker` / `scanner-scheduler` services, with the `scanner_node_modules` volume
  and `SCANNER_BROWSER_PROFILE_DIR` (P15B.6). Their Railway services were deleted on
  2026-10-08 (D-REL1 (a)).

### Fixed
- A fresh local compose stack failed migration V001 (`CREATE EXTENSION vector`), because
  `postgres:16-alpine` has no pgvector. The compose Postgres now uses the pgvector image
  (P15B.6, D-TC3).

## [1.5.8] - 2026-10-23

### Added
- `TRUSTED_PROXY_CIDRS` (P3.2, `backend/utils/client_ip.py`): a comma-separated list of
  CIDRs or bare IPs, and/or the token `cloudflare`. The token expands to Cloudflare's
  published ranges as of 2026-10-18: 22 networks, 15 IPv4 and 7 IPv6.
  - When it is set, `client_ip()` walks X-Forwarded-For from the right and skips entries
    inside the CIDRs. It never walks more than `TRUSTED_PROXY_HOPS` entries, and every
    skipped hop must lie inside the CIDRs. Prod keeps `TRUSTED_PROXY_HOPS=2` next to it.
  - A value with no valid entry fails closed: nothing past the socket peer is trusted.
    Invalid tokens are dropped with one warning. A very broad range (IPv4 shorter than /8,
    IPv6 shorter than /16) is kept but logged once.
  - Unset, the hop rule is unchanged: a 30,000-input differential found 0 differences. A
    one-time log line fires when `TRUSTED_PROXY_HOPS` is 2 or more and no CIDRs are set.
  - `TRUST_PROXY_HEADERS` stays the master switch. Prod sets the variable in N3.13 Part C
    (owner, after `web-source-clear`).
- `css_url` Jinja filter (P3.5, `backend/web/templating.py`) for
  `style="background-image:url(...)"`.
  - It accepts http(s) URLs that `normalize_listing_image_url` passes, and same-origin
    `/static/`, `/car-images/` and `/dealer-uploads/` paths without dot segments.
  - It percent-encodes `' " ( ) \ < >`, Unicode whitespace and control characters. Other
    characters, `%` included, are left alone, so ordinary CDN URLs come back byte-identical.
  - Anything rejected falls back to `/static/placeholder.svg`.
  - Applied at 10 template sites: `home.html` (4), `landing.html`, `compare.html`,
    `inventory/dashboard.html` and `inventory/listings.html` (3).
- `backend/auth/oauth_link.py` (P3.1): the pending OAuth link. When Google or Apple
  matches an account whose email is unverified, the session holds
  `pending_oauth_link` (provider, sub, email, user id, 10-minute expiry) and `/login`
  shows a plain notice line. A password login as that same account, on the web form or
  `api_auth_login`, links the sub, marks the email verified and clears the key. Another
  account, an expired entry or a changed email links nothing.
- "Inventory last checked <date>" line on the dealer page and the car page (N3.11, D-N19).
  - `<date>` is the newest `cars.scraped_at` over the dealer's rows, printed in UTC in
    the first-seen line's shape ("Sep 28, 2026"). The car page shows its dealer's date,
    not its own row's.
  - The line is hidden when that date is under 48 hours old against the current UTC
    time, and when no date is on file. A failed read hides the line and logs a warning;
    it never fails the page.
  - One query per page view: `dealers_repo.dealer_last_scraped_at` (re-exported from
    `inventory_db`), `SELECT MAX(scraped_at)` over the key and its lower-case form,
    served by `idx_cars_dealer_listing` on both backends. The rule is in the new
    `backend/utils/inventory_last_checked.py`.
  - Context key `inventory_last_checked` on both pages. On the car page it is the last
    key of `assemble_view_context`, so `GET /api/cars/<id>` carries it too.
  - Markup: one `<p>` with an existing muted-text class (`dealer-inventory__unconfirmed`
    under the dealer page's inventory counts, `car-detail-location-note` under the car
    page's dealer name). No new CSS, no container, no inline style.
- New tests:
  - `test_csp_policy.py` (P3.4): exact script and connect sources, the headers on a real
    response, and no frontend file loading a cross-origin script;
  - `test_csrf_coverage.py` (P3.4): walks `app.url_map`, and every POST, PUT, PATCH and
    DELETE rule must reach a CSRF validator or be classified. Today: 78 hook, 6 in-view,
    2 exempt (signed Stripe webhooks), 10 redirect-only MFA stubs, 0 unclassified;
  - `test_css_url_filter.py` (P3.5): injection payloads, byte-identical CDN URLs, the
    template scan, and node-backed checks of the two JS helpers;
  - `test_username_email_collision.py` (P3.1);
  - `test_admin_onboard_paused.py` (N3.10, N3.10b): every paused endpoint answers 409 and
    records zero `dealer_jobs` INSERTs through the real `enqueue_job`; no paused control
    is drawn;
  - `test_security_master_todo.py` (P3.9): the Summary table must match the row
    statuses, and every row needs its required fields;
  - `test_last_checked_line.py` (N3.11, 25 tests): the 48 h rule, the repo read and its
    SQLite query plan, both states on both pages, the markup (no container element), and
    a Postgres check on a throwaway `pytest_*` database gated on `LAST_CHECKED_PG_DSN`.
- Existing tests extended: `test_client_ip`, `test_outbound_url`,
  `test_outbound_url_dns`, `test_safe_listing_url`, `test_google_oauth`,
  `test_apple_oauth`, `test_password_reset`, `test_production_security`,
  `test_web_researcher`, `test_car_chat_policy` and `test_requirements_hygiene`.
  `test_dealer_onboard_api`'s two route tests now expect 409 (N3.10b).
- `docs/ops_log/N3.10.md` (N3.10): the local `dealer_jobs` count, read-only (0 rows on
  `cars`). Prod was not read. N3.10b adds its notes there.

### Changed
- `WebResearcher` is HTTP-only by default (P3.8, D-SEC1 (a)). `allow_browser=False`
  gates the Brave search fallback and the per-page Chromium fallback.
  - An HTTP-only instance never imports Playwright. A direct call to a browser step, or
    to `_make_context`, raises `BrowserModeDisabled` before any import.
  - Car chat and compare chat (`agent.py`) keep the default, so both are now HTTP-only.
  - The operator script `backend/scripts/analyze_trim_adds_with_web.py` passes
    `allow_browser=True`. An AST test fails if any other caller does.
  - The `playwright` pip package stays in `requirements.txt` until P15B.2. With no browser
    download it is inert.
- The admin dealer hub's onboard POST (`/admin/dealers`) now answers 409 with
  "Onboarding is paused; use dealer-discovery (see P15A.3)" and enqueues nothing (N3.10).
  No worker has consumed `dealer_jobs` since the worker and scheduler were deleted on
  2026-10-08. The template shows that line as plain text in place of the "Add & scan"
  form. Existing jobs are left alone.
- The other web paths that queued `dealer_jobs` are paused too (N3.10b, D-IF3 (a)). Find
  dealers "Request scrape" (`POST /api/admin/dealer-onboard`) and the hub's Restart and
  Smart restart (`POST /api/admin/dealer-jobs/<id>/retry` and `.../smart-retry`) answer a
  site admin with 409 `{"ok": false, "error": "onboarding_paused", "message": "Onboarding
  is paused; use dealer-discovery (see P15A.3)"}`. The check runs before rate limiting,
  URL validation and diagnosis, and nothing is enqueued. Non-admins still get 403.
  - Find dealers draws no "Request scrape" button and shows site admins the paused line
    once in its header. The hub draws no Restart buttons.
  - The line and a `QUEUE_ONBOARDING_PAUSED` constant (not an env knob) live in the new
    `backend/dealer/admin/onboarding_paused.py`. The queue functions and the handler
    bodies stay for P15A.3.
- `scripts/security_check.sh` (P15B.1) runs
  `pip-audit -r requirements.txt -r requirements-scanner.txt` with no `--ignore-vuln`. It
  accepts `PIP_AUDIT=<path>` and also runs `test_requirements_hygiene.py`.
  `scripts/scanner_import_sweep.py` `ALLOWED_MISSING` drops every name nothing imports.
- Profile and admin edits that change an account's email now clear `email_verified_at`
  and any pending verify token (P3.1). A username may no longer contain `@` or equal
  another account's email. Web, API and dealer registration and admin create refuse `@`
  usernames.
- `docs/SECURITY_MASTER_TODO.md` (P3.9):
  - new rows SEC-104 to SEC-111 for P3.1 to P3.8. SEC-110 (P3.7) is Blocked on the
    owner's live `TRUSTED_PROXY_HOPS` confirmation;
  - new Not started rows from the wave-1 reviews: SEC-112 (DNS rebinding and a pinned
    resolver; the enrichment image fetch and the onboard URL check) and SEC-113 (origin
    lock);
  - SEC-076 is Done (P15B.1). SEC-083 drops the stale prod-exception note. SEC-099's
    security property is Done, and billing go-live is tracked separately;
  - the environment table gains `TRUSTED_PROXY_CIDRS`.
- `docs/NEXT_PHASES_PLAN_2026_10.md` carries the N3 unit status.

### Removed
- Orphan dependencies from `requirements.txt` (P15B.1, D-DEP1 (a)): `crawl4ai`, `pyotp`,
  `segno`, `ollama`, `langchain-core`, `langchain-openai`, `langchain-text-splitters`
  and `langsmith`, plus the `starlette` and `tornado` floors. None has a Python importer.
  Ollama is reached over HTTP with `requests`. The `urllib3`, `idna` and `pygments`
  floors stay, because something still requires each of them.
- Chromium from the web image (P3.8): `Dockerfile.web` no longer runs
  `playwright install chromium && playwright install-deps chromium`, and drops the 17
  Chromium-only apt libraries. A static test fails if a browser install comes back.
- The Patchright Chromium install from `backend/scripts/install_scraper_browsers.sh`
  (P15B.1). Its only consumer was crawl4ai.
- The "Add & scan" form on the admin dealer hub (N3.10).

### Fixed
- `docs/SECURITY_MASTER_TODO.md` Summary counted 0 open items in every phase while
  SEC-096, SEC-097 and SEC-099 were In progress. It now counts and names the open rows,
  and states the counting rule. SEC-095 no longer claims that Playwright sandboxes
  Chromium by default: Playwright adds `--no-sandbox` unless `chromium_sandbox=True`
  (P3.9).
- `safe_listing_url` returns None for a malformed bracketed host instead of raising
  `ValueError` (P3.3).
- `test_requirements_hygiene` failed on P3.5's direct `jinja2` and `markupsafe` imports.
  Both are allowed as Flask transitives, like `werkzeug` (N3 integration).

### Security
- `deploy/railway/sync-vault-to-railway.sh` refuses to run without explicit `TRUSTED_PROXY_HOPS` (a positive integer; prod web uses 2) and `RAILWAY_SERVICE` values. It checks both before any railway call, so the old silent hops default of 1 is gone (P3.7, SEC-110; merged after the owner confirmed the live value 2 on 2026-10-23). `deploy/railway/README.md` lists the live services (web, scanner-nightly, Postgres) and the hops rule in section 7. New `backend/tests/test_sync_vault_guard.py`.
- OAuth pre-registration takeover closed (P3.1, security-4, D-SEC4 (a)).
  - Google and Apple find an existing account by a strict lower-case email lookup, never
    by username. When more than one case variant matches, they refuse to pick one.
  - The provider's email must be verified: Google `email_verified` true, Apple
    `email_verified` true or `"true"`.
  - An email-matched account is linked automatically only when its own email is verified.
    Otherwise the link waits for a password login (see Added).
  - The retry after an IntegrityError looks up by sub only. The old email/username
    fallback logged the caller in without any check.
- Client IP spoofing through the Railway origin (P3.2). With `TRUSTED_PROXY_CIDRS` set, a
  request sent straight to the origin can no longer choose its own client IP. A Worker in
  another Cloudflare zone cannot forge one through Cloudflare either. `DEV_IP_ALLOWLIST`
  goes through `client_ip()`, so the allowlist spoof is refused once the CIDRs are live.
- SSRF guards block every non-global address (P3.3, `backend/utils/outbound_url.py`).
  - One rule, `ip_is_non_global()`, serves both the literal check and the after-DNS check.
    IPv4-mapped and 6to4 addresses are judged by the IPv4 they carry.
  - Newly blocked: CGNAT/Tailscale `100.64.0.0/10`, which the old `is_private` rule let
    through, and the deprecated site-local `fec0::/10`. `192.0.0.0/24` and `3fff::/20`
    sit in an explicit table, so the verdict no longer depends on the Python patch level.
    The private-use names `.internal` and `.home.arpa` join `localhost` and `.local`, and
    `safe_listing_url` now refuses `.local` and `.localhost` names too.
  - Hosts are judged as clients decode them. Legacy IPv4 spellings (`127.1`,
    `2130706433`, `0x7f.0.0.1`) are parsed as addresses. Percent-encoded and Unicode
    spellings are decoded once and folded (NFKC plus UTS #46). A name that still holds a
    forbidden code point is refused.
  - After DNS, an answer with no usable address fails closed.
  - The same rule backs `safe_listing_url`. Local `cars` (read-only): 0 stored hosts
    affected.
- `https://esm.sh` is gone from `script-src` and `connect-src` in the enforced and the
  report-only CSP (P3.4). Nothing in `frontend/` loads from it.
- The operator DELETE routes (`api_admin_operator_delete_incomplete_car`,
  `api_admin_operator_api_delete_dealer`) now require `X-CSRF-Token` for logged-in
  sessions, through the `api_admin_operator_` prefix rule (P3.4).
- CSS injection through `style` `url()` closed (P3.5, security-7). Autoescape turns `'`
  into `&#39;`, but the HTML parser decodes it back before CSS reads the attribute. So
  before this release, a dealer image URL containing `');` injected CSS into every card
  that showed it.
  - Templates now go through `css_url`.
  - The JS helpers `compare.js` `safeCssBackgroundUrl` and `dev.js` `cssSingleQuotedUrl`
    use the same encoding rule and then escape `&`.
  - 402 of 214,675 active local image URLs (containing `(`, `)` or a space) are now
    percent-encoded. The encoded forms still serve the image.
- Password reset now allows at most 3 emails per account per hour (P3.6). A throttled
  request sends no mail, leaves the link already emailed valid, and shows the same page
  as an unknown account. The per-IP limit alone did not stop mail-bombing one address
  from rotating IPs.
- Production boot warnings (P3.6) fire in three cases:
  - an email or billing flow is on while `PUBLIC_BASE_URL` is unset;
  - an email flow is on with neither `RESEND_API_KEY` nor `SMTP_HOST` set;
  - `ALLOW_APP_ADMIN_DEV_PASS_THROUGH` comes back. This is a guard only: prod deleted the
    variable on 2026-10-08.
- The web process no longer launches headless Chromium (P3.8). Before this release it ran
  as root with `--no-sandbox`, for any signed-in user's car-chat question. P0B.3 measured
  0 of 30 answers depending on it.
- `lxml>=6.1.0` in `requirements-scanner.txt` (PYSEC-2026-87; P15B.1). With the orphans
  gone, `pip-audit -r requirements.txt` with no ignores reports 0 vulnerabilities across
  99 dependencies (it found 9 in 3 packages before).

## [1.5.7] - 2026-10-22

### Added
- `backend/scripts/repair_new_null_mileage_index.py` (N2.2, IL-02): a targeted repair of
  the incomplete-listings index for the F12 class, so the owner can fix prod's index
  without a full rebuild. It takes the `mileage` code off the index rows of active,
  non-used cars with a NULL or empty odometer. Rows are selected through the N2.1 rule
  itself (`_mileage_blank`, imported, not copied), in keyset batches of 1,000. Every other
  stored code is kept. A row left with no codes is deleted, so its car shows on the grid
  again. `--dry-run` is the default: it opens a read-only session and prints
  `repair_rows`, plus `census_il02_shape`, which counts the same rows the way the N1.6
  census does. `--apply` needs a new `--backup-file`, which gets each batch's original
  rows as CSV, fsynced before the batch is written. On Postgres it also needs
  `--expect-db` equal to `current_database()`. Writes are compare-and-set on the text that
  was read, so a row another writer changed in the meantime is left alone and counted.
  Results on a `cars_cleanroom` copy: the dry run found 21,152 rows, equal to the class
  count. The apply deleted 18,965 rows and updated 2,187, leaving 0 to repair. The
  slowest batch took 0.06 s. `backend/tests/test_repair_new_null_mileage_index.py`
  (SQLite) covers selection through the imported rule, batching, the read-only dry run,
  every refusal (no backup file, an existing one, a missing directory, and the target
  check), the apply, a byte-exact restore and the concurrent-change case.

### Changed
- `frontend/templates/car.html` (owner decision D-N12): a New, non-CPO car with no
  odometer figure now shows no mileage line in the headline, "At a glance" and the spec
  sheet. Used and CPO cars still show "Mileage not listed", and a current New car at 0
  still shows "0 mi". N2.4 (below) carries the rule to the other surfaces.
- D-N12 on every surface (N2.4): `backend/utils/mileage_display.py` gains
  `mileage_line_hidden`, true for a car with no odometer value (NULL or empty) that is
  not used or certified, unless its model year is older than last year.
  `serialize_car_for_api` and `serialize_car_for_listings_grid` send it as
  `mileage_line_hidden`. The grid card (`frontend/static/listings/card.js`), the car page,
  the home and landing cards (`frontend/templates/home.html`, `landing.html`) print no
  mileage for such a car, and the compare cell (`backend/utils/compare_specs.py`) prints
  the empty-cell dash. `is_used_or_certified` moved from `listing_completeness.py` into
  `mileage_display.py`, so the index and the page share one predicate (the index passes
  the raw condition, the page the displayed one). Change from N2.1's car page: a "new"
  car older than last model year at 0 or NULL keeps "Mileage not listed", because the
  New label is the doubtful part (on `cars_cleanroom` 59 at NULL and 543 at 0; on `cars`
  46 at 0). New tests: `backend/tests/test_mileage_line_d_n12.py` and
  `backend/tests/js/listings_card_mileage.test.js`.
  `backend/tests/fixtures/serialize_car_for_api_golden.json.gz` is re-recorded for the
  new key: of 8,000 outputs, none differs beyond it.
- `backend/tests/test_mileage_zero_f12.py`: new cases for New/NULL, CPO/NULL, Used/0,
  Used/NULL and blank-condition/NULL, at the rule, listing-code and car-page levels. The
  rule change forced two expectations to change: one assertion in
  `backend/tests/test_cleanroom_acceptance.py` (a NULL odometer on a New car is no longer
  coded) and one line in `backend/tests/fixtures/upsert_vehicles_golden_expected.json`
  (VIN `...00013` drops `mileage`).
- `docs/NEXT_PHASES_PLAN_2026_10.md`: N2 unit status and measured numbers.
- `docs/NEXT_PHASES_PLAN_2026_10.md`: N1.11 batches 2 and 3 done (resumed 2026-10-22 after the 10-19 pause). 58 more missing rooftops are profiled and staged for N8.9 (133/133 profiles valid), including 14 outside the 22 roster states under the owner's 2026-10-18 D-N4 answer; nothing was written to `cars` or `cars_cleanroom`. New unit N8.14 owns shared-host dealer identity (the 22 AutoSavvy stores on one host, Ted Russell Ford Parkside), and N8.9 must not apply a `shared_host` roster entry before it.
- `docs/NEXT_PHASES_PLAN_2026_10.md`: N1.11 marked DONE. All 75 queued missing rooftops are profiled (63 DONE, 2 REPAIR_EXISTING, 10 BLOCKED_SCANNER) and staged for N8.9; nothing was written to `cars`.

### Fixed
- `backend/utils/listing_completeness.py` `_mileage_blank` (N2.1, IL-01, owner decision
  D-N12): a NULL or empty odometer now counts as missing only on a used or certified car
  (a used/certified token in the raw condition, or `is_cpo`). New, blank-condition and
  other non-used cars (`ctp`, `demo`) with no odometer value no longer get the `mileage`
  code. Used cars at 0 or NULL keep it. Production hides cars that the index marks
  incomplete, so the old rule kept New cars with no odometer value off the grid. The
  stale code sits on 21,152 active cars in the 10-13 clean-room scan and on 28,983 in the
  10-04 prod dump; it is the only code on 18,965 and 27,423 of them. On `cars_cleanroom`
  the `mileage` code drops from 22,947 cars to 1,795 (1,374 used at 0 mi plus 421 used
  with NULL), and the incomplete count drops from 34,023 to 14,345. `cars` stays at 11,719,
  because it has no active cars with a NULL odometer. The rule reads the raw condition:
  118 blank-condition `cars_cleanroom` rows that the page displays as Used now count as
  complete for mileage. None of these are in the prod class.

## [1.5.6] - 2026-10-18

### Fixed
- `backend/tests/test_cleanroom_shapes_2026_10.py`: the `inv` fixture points the
  incomplete-listings sidecar index at a per-test file and clears the cached snapshot
  before and after. The session-wide index kept the car ids its scans flagged, so
  `test_listings_perf::test_warm_make_model_search_is_one_round_trip` hid all 60 of its
  seeded cars when it ran later in the same session (red `pytest` on the 1.5.5 release PR).
- `docs/RELEASING.md` step 1: the migration max-version command greps `/V[0-9]+__`, so
  `test_releasing_doc` no longer reads `migrations/V` as a repo path.

## [1.5.5] - 2026-10-18

### Added
- `backend/scripts/cleanroom_acceptance.py` with the `backend/scripts/cleanroom/` package
  (N1.3): a read-only acceptance tool that every later phase proves "the counts drop"
  against. It refuses a writable session (`default_transaction_read_only` off; SQLite opens
  `mode=ro` + `query_only`) and computes VIN-owner conflicts and the CF-7 class-(b) checks,
  the triage accounting identity, in-run retirements by writer, dict-repr trims and
  template-token colors, `vpic_fuel` false positives, AWD/4WD stored where vPIC decodes 2WD,
  page-cap walks, VDP wanted vs fetched, verdicts, junk prices, VIN quality, placeholder
  images and the chunked incomplete-listings recompute (ported from the 10-15 scratch
  scripts). `--expect` compares against the hand-built
  `backend/tests/fixtures/cleanroom_baseline_expected.json` (67 values; EXPECT OK on
  `cars_cleanroom`, 11,719 incomplete on `cars`); `--signals` writes the 458-row fleet
  signal file and flags at most 46 dealers for N11.10.
- `scripts/cleanroom/{build_db.sh,run.env.template,preflight.sh,isolation_snapshot.sh,make_manifest.sh,check_run_root.sh}`
  (N1.3): parameterized clean-room re-runs into `cars_cr_*` scratch DBs with run roots at
  `workspace/cleanroom_runs/<db>`. Preflight refuses `cars` and the frozen `cars_cleanroom`
  as write targets, non-`/tmp` hosts, port 15432, paths outside the run root and a HEAD
  other than `--sha`; `check_run_root.sh` refuses the repo root, `workspace/` and the frozen
  `workspace/cleanroom_2026_10`. `build_db.sh` refuses an existing DB and stamps the HEAD.
  `backend/tests/test_cleanroom_acceptance.py` covers the metrics on SQLite, the read-only
  guard and every refusal.
- `backend/tests/test_cleanroom_shapes_2026_10.py` + `backend/tests/fixtures/cleanroom_shapes/`
  (N1.4): 8 `xfail(strict=True)` tests cut from the 2026-10-13/14 clean room: the four
  lifecycle-retry retirements (Terry 0 of 16 re-seen, Coastline 8/11, Napleton 16/17, South
  County Lexus 14/15), the known=0 lifecycle reconcile, guard scan-order dependence
  (Chapman), the unstamped Jazel group feed (5 Star Ford, 124 sister cars) and the triage
  identity replay (`rows_written` = active). Each reason names the unit that must flip it
  (N9 P5A/P5B, N8.3, N8.6, N9.12).
- `docs/remediation/CLEANROOM_RCA_2026_10.md` (N1.1): the clean-room root-cause record with
  every figure re-derived (SQL appendix, run-log lines, acceptance-tool keys). Corrected
  premise: 21 retirements, all by the scanner's `inventory_reconcile` in lifecycle retries
  (10 correct, 11 wrong); the "~8.8k" was 9,610 VIN-owner guard refusals minus 834 rows of
  two pause-killed dealers. Conflicts 9,638 rows / 7,393 VINs / 92 pairs, classified per
  group from the N1.2 dossiers; six root causes with file:line; pause artifacts; the D-N4
  missing-rooftop list (60 confirmed, 15 likely); a fix map to N-units.
- `docs/remediation/DECISION_BRIEF_2026_10.md` (N1.8): the 105 open owner decisions grouped by
  the phase they block, each with options, recommendation, evidence and needed-by date,
  answered as "accept all except ...". D-N6's recommendation changes from the plan.

### Changed
- Owner decisions 2026-10-18: the owner accepted every recommendation in
  `docs/remediation/DECISION_BRIEF_2026_10.md` (D-N6 in the brief's changed form: keep
  mbbeverlyhills-com on CarsCommerce `listings/6074385`, drop fletcherjones-com as a store).
  `docs/remediation/OWNER_DECISIONS_LOG.md` gains one row per decision (105) and
  `docs/NEXT_PHASES_PLAN_2026_10.md` Section 5 marks each one ANSWERED.
- Plan and repo upkeep (N1.9): `docs/REMEDIATION_PLAN_2026_10.md` statuses for 0A-2B, the
  10-08/10-16 answers, P0B.1 numbers, the extended D-DB1 list, P1B.5 dropped, P4.4 no-op,
  the C.1 migration renumbering (V029 = attribution evidence), the OPS-11 long-run hygiene
  checklist and Appendix E (differences from the next-phases plan, line-reference drift);
  `docs/RELEASING.md` gains the always-`-s` Railway rule, a step-1 comparison against prod's
  applied max migration, the PG18 client path, the 10-16 web-source and `SCAN_FLEET=0`
  status and the 1.5.4 release-log row; `docs/db_layer/SCHEMA_LEDGER_2026_10.md` records
  `cars_cleanroom` as the frozen V025 baseline and the `cars_cr_*` scratch DBs;
  `docs/INCOMPLETE_LISTINGS_FINDINGS_2026_10.md` gains a finding-to-N-unit map;
  `docs/NEXT_PHASES_PLAN_2026_10.md` carries the N1 unit status.

## [1.5.4] - 2026-10-09

### Added
- `requirements-test.txt` pins the test-only dependencies (`pytest==9.1.1`,
  `pytest-timeout==2.4.0`, `pyyaml==6.0.3`); none of them may land in the deploy
  `requirements.txt`, and `test_requirements_hygiene.py` accepts them for imports under
  `backend/tests/` only. `backend/tests/test_ci_workflow.py` pins ci.yml's triggers,
  permissions, job ids, `needs: lint`, timeouts, install order, ruff version and offline
  selection (P2A.2).
- `TESTS_BLOCK_NETWORK=1` (CI sets it) makes `backend/tests/conftest.py` refuse Python socket
  connects and curl_cffi requests to any non-loopback destination (127.0.0.0/8, ::1 and unix
  sockets stay allowed) with the error a firewalled host gives; refused attempts are counted
  per test, listed in a "NETWORK-BLOCKED CONNECTS" terminal summary and recorded as
  `network_blocked` junit properties. Unset, nothing is patched. DNS, UDP sendto, libpq,
  subprocesses and browsers are not guarded (P2A.3).
- `scripts/release_guard.py --base <ref> [--head <ref>]` (stdlib only) judges whether a
  commit is releasable: VERSION at the head must be semver-greater than at the base
  (1.5.10 > 1.5.9) and CHANGELOG.md at the head must have a filled `## [<VERSION>]`
  section; a head already contained in the base passes; an all-zero base checks only the
  section. Exit 0 releasable, 1 refused, 2 unresolvable ref. CI gains a `release-guard` job
  (`needs: lint`) that runs only for pushes to main and PRs to main (D-REL3), comparing a
  PR's merge commit with origin/main and a push to main with `github.event.before`.
  `backend/tests/test_release_tooling.py` covers bump_version.sh, the pre-push hook, the
  guard and the CI job's base selection against local git repos (P2A.4).
- `docs/monolith_audit_2026_10_01/tests.md` gains the CI failure ledger for shakedown-1 (run
  37850535029): the 3 failures and the 5 network-touching tests, each with its class (a)-(f),
  cause, planned fix and owner, plus product finding PF-1: without the gitignored
  `dictionary_catalog.db` (which prod has never had, P0A.3) the EPA file fallback resolves
  128 YMMs to another model's file (2026 Audi A5 -> 2025 A4 mild hybrid), owned by Phase 9
  (P2A.6).
- `backend/tests/test_release_tooling.py` pins owner decision D-REL3 (a): the pre-push hook
  has no branch-name exemption, so `phase/*` and `ci/*` branches need a new VERSION like any
  other branch (a shakedown re-uses a bump only by pushing to a new `ci/shakedown-<n>`), and
  a `git push origin <branch>:main` is judged by the release guard (VERSION must rise,
  CHANGELOG section filled), not by the branch rule. The hook and CLAUDE.md are unchanged
  (P2B.1).
- `docs/RELEASING.md` documents the release flow (D-REL2 (a) trunk, D-REL3 (a) every push
  bumps): branching model, bump rule, a 15-step release checklist with a Verify command per
  step, the D-REL10 hotfix override, web and scanner-nightly rollback, the Railway source
  policy, the migration pointer and a release log seeded with the pre-flow deployments. No
  web variable may change while web's `serviceInstance.source.repo` is set; the read-only
  `web_source_check` prints `web-source-clear` or `WEB-SOURCE-SET <repo>` and fails closed
  (still set on 2026-10-09; the owner is disconnecting it). No doc checks out an old tag to
  run its deploy script (1.4.2 to 1.5.3 carry the pre-P2B.2 `deploy_scanner_nightly.sh`,
  which deploys on `--dry-run`): mid-fleet trouble means `railway down -s scanner-nightly`
  and a PATCH release from main. `deploy/railway/README.md` drops "connect the GitHub repo
  and deploy from main" for `deploy_web.sh`, `docs/RAILWAY_SCANNING.md` records the
  2026-10-08 deletion of `scanner-worker`/`scanner-scheduler`, the `SCAN_FLEET=1` hazard on
  variable changes and that a Railway rollback restores the target's variables, and
  RESUME_HERE.md's version and branch lines point at `VERSION` and the trunk model.
  `backend/tests/test_releasing_doc.py` pins the docs to the tooling (P2B.7).

### Changed
- CI (`.github/workflows/ci.yml`) runs on every branch push, on PRs to main and on manual
  dispatch, with `permissions: contents: read`, one cancellable run per branch or PR (never
  cancelled on main) and timeouts (lint 5, pytest 30, pytest-integration 15 min). The pytest
  job installs the CPU-only torch wheel first (D-TC5), then `requirements.txt` and
  `requirements-test.txt`, sets up node 20 so the JS unit tests run, selects
  `not integration and not slow and not pg` under `TESTS_BLOCK_NETWORK=1` with
  `--timeout=300`, and uploads the junit report. Job ids `lint`, `pytest` and
  `pytest-integration` are unchanged (P2A.2).
- The pre-push hook runs the release guard for pushes to `refs/heads/main`, with the sha the
  remote reported as base (never a possibly stale local origin/main), and fails closed when
  python3 or the script is missing or the remote main is not fetched. Pushes to every other
  ref keep the existing rule (VERSION must differ; bump-only, tag and deletion pushes
  skipped) (P2A.4).
- Railway deploys come only from a tagged release on main (D-REL10). The new
  `deploy/railway/deploy_web.sh` and the rewritten `deploy_scanner_nightly.sh` share
  `deploy/railway/_guarded_deploy.sh`, which refuses (exit 1, nothing staged, railway never
  called) unless the tracked tree is clean, HEAD is main on origin (read live with
  `git ls-remote`) and HEAD carries the annotated tag `v<VERSION>` with the same tag object
  on origin. The stage is `git archive` of the tag plus the P0A.3 must-ship allowlist (empty)
  plus `BUILD_COMMIT` (full SHA) and `BUILD_TAG`, uploaded with
  `railway up <stage> --path-as-root --service <service> --detach`; `--dry-run` prints the
  plan and file count without calling railway, and `--keep-stage DIR` keeps the stage for a
  local Docker build. `ALLOW_UNRELEASED_DEPLOY=1` (that exact value) overrides with a loud
  warning and ships the committed tree of HEAD with `BUILD_TAG=unreleased`. The scanner
  script still swaps in `railway.scanner-nightly.json` and refuses `SERVICE=web`. `/health`
  and `/api/health` now return `{status, version, commit}`, where `commit` is the stage's
  `BUILD_COMMIT` or "unknown"; the healthcheck path is unchanged. Covered by the new
  `backend/tests/test_deploy_guard.py` and an extended `test_health_ready.py` (P2B.2).

### Fixed
- `ruff check .` is green again: the hidden-dealers `env` fixture body is now the helper
  `make_account_env`, and `test_csrf_delete_routes.py` imports the helper instead of
  re-exporting the fixture (F811) (P2A.1).
- The offline suite passes in a clean checkout (CI) without the gitignored dictionary index,
  with no new skips. The two manifest write-guard tests in `test_dictionary_catalog.py`
  accept an absent `index/manifest.json` (the write must still be refused, and the file must
  still be absent afterwards). `test_merge_verified_specs_golden_hermetic` builds its own
  dictionary catalog DB from the tracked tree in `tmp_path` instead of reading the machine's
  index, so the 2026 Audi A5 no longer falls to the no-catalog fallback (PF-1, not fixed
  here). The `TestFetchListingHtmlIntegration` tests in `test_scraper_chain.py` stub the
  curl_cffi transport, so `TESTS_BLOCK_NETWORK=1` records no refused connects for them (was
  15 in 5 tests) (P2A.7).

## [1.5.3] - 2026-10-08

### Added
- `backend/scripts/reconcile_recipe_store.py` reconciles a recipe cache dir (`--cache-dir`) or a
  second store (`--source-dsn` / `--target-dsn`) with dealer_recipes per recipe instead of by
  whole set: a recipe on both sides comes from the side with the newer last_ok_at (stale follows
  the DB unless the cache has a strictly newer success; a tie with a DB-only stale flag resolves
  to live), a cache-only recipe is added only when its write stamp is newer than the DB set's
  last write (a `saved_at=0` row counts at its set's newest last_ok_at, never `updated_at`), and
  DB-only recipes are kept. Dry run by default (report.tsv, keys.tsv and summary.json under
  `workspace/backups/recipes_reconcile_<stamp>/`); `--apply` needs `--backup-dir`, tars the
  cache and exports every row it will write first, writes through a guarded
  `UPDATE ... WHERE dealer_id=? AND updated_at=?`, skips a row changed since the read, rewrites
  the differing cache files from the DB and writes the `_reconciled` marker; `--restore` undoes
  an apply. `import_recipes_to_db.py` keeps its CLI as a wrapper around it: it no longer pushes
  a newer-stamped file over a DB set wholesale (a July file over a September set saved with
  `saved_at=0`), and it refuses a cache tagged for another store (P1C.2).
- `python -m backend.scripts.backfill_recipe_saved_at` stamps dealer_recipes entries saved with
  `saved_at` 0 or null (sets synthesized or cascaded before P1B.3; 201 local rows) with the
  row's newest last_ok_at, else its updated_at, and recomputes max_saved_at, so an older cache
  file no longer outranks those sets. Dry run by default (counts and up to 20 ids); `--apply`
  needs `--backup-dir`, writes a mode-0600 JSON export of the touched rows first, then one
  guarded UPDATE per row that skips a row changed since the read; scan_hints and other columns
  are never touched. A dealer whose cache file has a newer success for a key is skipped and left
  to the reconcile (scottclarkhonda-com locally), and `--apply` is refused when the cache dir is
  missing, empty or tagged for another store. `--restore` puts rows back from the export (P1C.3).

### Fixed
- Listing retirement checks each condition bucket (new / used / unknown) in the full scan, the
  delta scan and the pipeline: a run that re-sees only the new cars no longer retires the used
  ones, blank-condition rows retire only when both buckets qualify, and `--no-reconcile` now
  also stops the scanner subprocesses from retiring (P1A.1).
- The catalog linker no longer clears car links when the epa_master read fails: it writes
  nothing and `link_cars_to_catalog.py` exits 2; score ties go to the lowest catalog id
  (6 of 214,546 local links flip) (P1A.3).
- InventoryEnricher no longer sends `ALTER TABLE cars` on Postgres (an ACCESS EXCLUSIVE lock
  request that then failed with DuplicateColumn); haiku_spec_cache is created only when absent,
  under a 3 s lock timeout (P1A.4).
- Recipe and VDP recipe cache files are written atomically (temp file in the same dir, fsync,
  `os.replace`): a failed write leaves the previous file byte-identical and no temp file. The
  cache dir is anchored at the repo root instead of the cwd (`/app/workspace/recipes` on
  Railway), `RECIPES_CACHE_DIR` overrides it (VDP recipes follow in `<dir>/vdp`), and a failed
  dealer_recipes write-through logs a WARNING once per process and error class instead of
  DEBUG (P1B.2).
- `save_recipes` stamps every row's `saved_at` once per call, so stale flags, un-stales,
  coverage updates and synthesized or cascaded sets reach the other hosts instead of being
  reverted by an older cache; the file-to-DB push-up in `load_recipes` does not stamp and logs
  a WARNING naming the dealer and the DB copy it overwrote (P1B.3).
- Recipe-store I/O no longer blocks the event loop: the replay success write-back, the feed
  provider hint, the discovery capture recipe counts and promote, and the four delta-scan hint
  notes run in `asyncio.to_thread` (P1B.4).
- `synthesize_recipes --dry-run` writes nothing: `gate_recipes(record=False)` returns the
  verdict without writing discovery.md or recipe_status, and a run that keeps a healthy recipe
  without `--force` no longer overwrites its recipe_status (P1B.8).
- `platform_candidates.py` no longer calls `load_recipes` (which rewrote the cache and pushed
  rows up): it reads the recipe file and the dealer_recipes row read-only, and a failed lookup
  is `unknown` (counted, never clustered). The report prints a live / none / stale / rejected /
  unknown census with the roots and cwd used, dates each verdict, drops members scanned after
  their probe (autosavvy-com: 403 probe 09-26, 3,958 rows 09-28), and no longer prints
  "nearest known template: none" when a template already detects members (P1B.7).
- The docs describe the recipe cache/store sync as it works: docs/data_architecture_plan.md
  gains "Recipe cache and store sync" (`saved_at` is the last-write stamp, the sync rule,
  `updated_at` is not a freshness stamp, the push-up WARNING, `RECIPES_CACHE_DIR`, the
  `_store.json` fingerprint, the 201 local / 235 prod `max_saved_at=0` rows);
  docs/CLOUD_RESTRUCTURE_PLAN.md:295 no longer says `load_recipes` "prefers the newer copy";
  docs/RAILWAY_SCANNING.md says the volume cache keeps the 2026-09-29 stale flags until Phase 0
  recipe code is deployed (P6B.1), and all three say `import_recipes_to_db.py` now merges per
  recipe and reads the store tag (P1C.6).
- Recipe replay honors `SCANNER_HTTP_PROXY` (through `scanner_proxies()`, as the other scanner
  fetchers do); VDP prefetch treats a 200 anti-bot interstitial as a 403 (the host gets the WAF
  cool-down instead of the page being parsed as a VDP); the SSRF guard
  `destination_host_blocked_after_dns` blocks hostnames that do not resolve (1344cbd09).
- The test suite stays off the network: an autouse stub makes vPIC unreachable (opt out with
  `@pytest.mark.real_vpic_client`; ~20 upsert tests used to reach vpic.nhtsa.dot.gov), a
  `fake_dns` fixture serves the SSRF host guards, and the web-research tests run on canned pages
  (39d7241db).

### Changed
- `build_epa_master_pg.py` refuses every write mode, `--rebuild` included, with exit 2 before
  any DB connection until the id-preserving builder lands (P10B.2); `--dry-run` reads
  backend/dictionary/epa recursively and refuses a source with fewer than 10,000 CSVs.
  `import_epa_master.py` refuses a full replace while any car is linked or epa_extended_specs
  has rows (P1A.2).
- Destructive enrichment scripts disarmed: `backend/scripts/backfill_forced_induction_pg.py`
  is deleted (it re-guessed forced_induction from listing text for every NULL row, 125,231
  active cars locally); `enrich_from_dictionary.py --all` refuses unless
  `ALLOW_DICTIONARY_OVERWRITE=1` is exported; `heal_cylinders_from_vpic.py` runs the
  forced-induction Phase B only with `--phase-b` (P1A.5).
- requirements.txt declares `anthropic>=0.116,<1` (the only Claude transport; nothing installed
  it, so prod car chat likely failed), `pdfplumber>=0.11` and `brotli>=1.1`. anthropic is capped
  below 1 because 1.x rejects the `temperature` argument car chat sends. New
  `test_requirements_hygiene.py` fails on any third-party import that is not declared (P1A.6).
- Each recipe cache dir carries `_store.json`, a sha256 of the host, port and database name of
  the store it mirrors (no credentials). A cache tagged for another store is read-only for the
  process: no file-to-DB push-ups, the store's own copy is served, saves go to the store only,
  and a failed store read holds that dealer's saves. `python -m backend.scanner.recipes
  --status` shows the tag and verdict, and `--reseed` moves the dealer files into
  `_reseed_backups/<stamp>/` and tags the cache for the current store; docs/RAILWAY_SCANNING.md
  explains it (P1B.6).
- Tests can no longer write the real workspace/: conftest.py pins `DEALER_LOGS_ROOT` to a
  session tmp dir, gives every test its own `RECIPES_DIR` / `VDP_RECIPES_DIR`, and fails the
  session when a name or mtime under workspace/dealer_logs or workspace/recipes changed
  (warning only while a scanner or pipeline is live; `WORKSPACE_GUARD=0` turns it off) (P1B.1).
- Recipe validation is one module: `validate_recipe` and its replay walkers move byte-identical
  from backend/scanner/synth/validate.py (now a re-export shim) into
  backend/scanner/recipe_validation.py. No behavior change (787546e21).
- backend/scanner/net/client.py is the one HTTP layer for the scanner fetchers (the chain
  fetchers, synth http, recipe replay, VDP prefetch, VDP recipes): lazy curl_cffi / requests
  import, proxies, send, impersonation rotation and challenge detection. No behavior change;
  test_scanner_http_characterization.py pins each fetcher's requests and returns (5986b498b).
- backend/scripts/fetch_oem_brochures.py split (2,507 -> 348 lines): the library code moves
  byte-identical into backend/enrichment/brochure_acquisition/ (paths, gaps, sources, download,
  corpus, quarantine, audits, reachability, html_specs, plan); the flags, defaults and mode
  dispatch order are unchanged, pinned by test_fetch_oem_brochures_surface.py (d096c2221).
- docs/monolith_audit_2026_10_01/: audit follow-up progress and the OWNER_DECISIONS.md list
  (77cddcb56).

## [1.5.2] - 2026-10-05

### Changed
- backend/scanner/recipe_synth.py split into backend/scanner/synth/ (http, common, registry,
  validate, one module per platform template); recipe_synth.py stays as a re-exporting facade.
- backend/scripts/dealer_pipeline.py split into backend/scanner/pipeline/ (db, roster, recipes,
  runner, vpic, dealer_logs, reconcile, assess, lifecycle, triage, run); the script keeps its CLI.
  Pure moves; surface tests pin exported names, registry order, CLI options and subprocess argv.

### Fixed
- test_run_discovery_overpass_timeout_returns_empty_osm no longer hangs (~100 s of unstubbed
  Overpass backoff); carscommerce synth tests no longer try the live API.

## [1.5.1] - 2026-10-05

### Changed
- backend/main.py split into owning modules (1,402 -> 192 lines; app assembly only):
  auth/session.py, auth/pages.py, routes/account.py, web/static.py, web/security.py,
  web/templating.py, enrichment/recalls_lookup.py. No behavior change; routes, endpoint
  names, hook order, filters and error handlers pinned by test_app_surface_golden.py.
- main_module() indirection removed; route modules import helpers from their owners.

## [1.5.0] - 2026-10-04

### Fixed
- Live bugs B1-B12 from the 2026-10-01 monolith audit: admin inline styles blocked by CSP,
  delta-scan scoping, raw vPIC "4x2" drivetrain, frozen market_price cache on Postgres,
  tests writing to real local DBs, iOS listings without zip, dev tools calling scanner.js.
- Migrations: V019 baselined; V020-V025 now apply (local).

### Changed
- One source of truth per duplicated rule (db/connect, billing/access, attribution/,
  vehicle_facts/, llm/client, schema from migrations).
- God functions split into named steps behind golden tests (no behavior change).
- Hermetic test suite, single app factory, JS unit tests.

### Removed
- ~20,000 lines of proven-dead code.

## [1.4.4] - 2026-09-30

### Fixed
- **Login stays on for 14 days.** The web session is now permanent (`PERMANENT_SESSION_LIFETIME`); it used to end when the browser closed.
- **Behind Cloudflare and Railway's edge** (`TRUST_PROXY_HEADERS=1`) the app is wrapped in `ProxyFix` for `X-Forwarded-Proto`, so redirects and absolute URLs keep `https://`. `client_ip` logs the `X-Forwarded-For` entry count once per process to check `TRUSTED_PROXY_HOPS`.
- `/register` strips the password like `/login`, `/account` and the API; accounts registered with outer spaces before this still log in.
- A relative `USERS_DB_PATH` / `DEV_USERS_DB_PATH` resolves against the repo root, not the working directory (a script run elsewhere silently opened a stray `users.db`). `app_users_status.py` and `reset_app_user_password.py` use the same path.

## [1.4.3] - 2026-09-29

### Fixed
- A datacenter scan host (`SCANNER_EGRESS_TAG`, `railway` on scanner-nightly) no longer marks shared recipes stale or rejected for a 401/403; it records `blocked:<tag>:...` and home scanners keep the recipe. `backend/scripts/unstale_host_blocked_recipes.py` re-opens the recipes the 2026-09-29 Railway runs staled.

## [1.4.2] - 2026-09-29

### Added
- **Railway scanning:** `scanner-nightly` service runs the sharded fleet from a slim image (11.8 GB to 1.09 GB, `requirements-scanner.txt`, `Dockerfile.scanner`, `backend/scripts/fleet_scan.py`, `deploy/railway/deploy_scanner_nightly.sh`). See `docs/RAILWAY_SCANNING.md`.
- **VIN ownership guard:** an upsert never moves a VIN that is fresh and active at another dealer (`SCANNER_VIN_OWNER_GUARD_HOURS`, default 48); refused moves land in `vin_owner_conflicts` (V024).

### Fixed
- Fleet roster keeps only dealers with active inventory; recipe-only dealers need `deploy/railway/revived_dealers.txt`. A recipe-only roster reassigned 6,961 VINs in prod on 2026-09-29 (restored).
- Upsert retries lock timeouts (55P03, and 57014 only when it is a lock timeout).
- Pipeline assess and retry loops use an autocommit connection and reconnect if it drops (prod `idle_in_transaction_session_timeout` killed three shards).
- Assess provider-hint query matches the `dealer_recipes` columns.

## [1.4.1] - 2026-09-28

### Changed
- **Listings no longer ship the whole fleet.** The page defaults to the shopper's ZIP + radius and shows no cars until a search starts; `/api/listings/cars` returns the cars within that area (radius snapped to 10/25/50/100/250 mi) from stored cards (`listings_grid_cards`, V023, refreshed by `build_listings_grid_cards` and nightly step 7). Largest metro: 0.99 s cold, 4 ms warm; web memory ~0.5 GB instead of a 9.5 GB peak. Filtering within the area stays client-side and instant. Mobile contract: session-area fallback, versioned `zip_required` response.
- **Nightly refresh:** consistent 7-step numbering; step 7 rebuilds grid cards.

### Fixed
- Upsert writes in sorted VIN order and retries Postgres deadlocks (Chapman Ford, two shards).
- Timing fingerprint no longer widens the window for dealers whose host throttled the pass (110 of 160 recommendations).
- Card store: freshness covers deal scores and market bands, bounded inline rebuilds, striped build locks, no mass rebuild on an attribution read failure, dealer-URL fallback includes cars with an empty dealer_id, per-dealer card cache with token ETags.

## [1.4.0] - 2026-09-28

### Added
- **Profile:** hidden dealerships (excluded from search, the grid and recommendations; toggle on the dealership page) and recent searches with run-again, save-as-saved-search, remove and clear. Migrations V021, V022.
- **Scanner, browser-free:** every scan replays HTTP recipes; a headless browser runs only inside `discovery_probe --browser-capture` (`backend/scanner/browser_gate.py`). Separate `Dockerfile.discovery` carries Chromium; the scanner images do not.
- **Recipe lifecycle:** validation against the site's own count at save time (`backend/scanner/recipe_validation.py`); stale/rejected recipes route to re-synth, then browser capture, then a retry batch, once per dealer per day; platform clustering flags dealers that share an unknown platform (`_learning/platform_candidates.md`).
- **Rooftop attribution:** a scored matcher (`backend/scanner/rooftop_match.py`, `SCANNER_ROOFTOP_SCORER`) with a 15-case regression corpus; subset-name tiers never un-list; "… Service / Parts" rooftops fold into their store; the scanner reconcile never retires a condition the run returned nothing for.
- **Per-dealer timing fingerprint:** scan duration, pages fetched vs needed and a recommended detail-page window stored in the dealer's scan hints and read back by the next scan.
- **Fleet sharding:** `SCANNER_LOCK_PATH` lets several pipeline shards run on one host (458 dealers in ~1.5 h on one laptop).
- **Version discipline:** `scripts/bump_version.sh` and a tracked pre-push hook that refuses a push without a VERSION change.
- **Docs:** data-completeness findings, competitor feature gaps, efficiency (backend, front end) and visual reviews for 2026-09-28.

### Changed
- **Data completeness:** CarsCommerce feed descriptions, dealer.com MSRP and engine from the feed and the VDP state, Team Velocity galleries (comma-joined URLs, widget logos, placeholders), engine/body derived from the vPIC cache, Hybrid→Plug-In Hybrid when vPIC says PHEV, placeholder drivetrain/fuel/condition vocabularies canonicalised, mileage 0 stored as unknown on used rows, Chapman MSRP; the incomplete tally counts only fixable gaps.
- **Static assets:** cached a year with immutable `?v=` stamps, precompressed `.gz`/`.br` siblings (`scripts/build_static_compressed.py`), self-hosted Inter, hero image preload, Leaflet loaded only when the Dealership tab opens.
- **Nightly refresh:** recomputes market price bands (step 6).

### Fixed
- **Wrong retirements:** assess/reconcile rated group-feed stores on the raw feed count and retired real cars; weak rooftop identification and "… Service" rooftops disowned real inventory; one-condition replays retired the other half of the lot. All restored.
- **vPIC write-path override** raised `KeyError 'cylinders'` since 09-25 and silently skipped, letting feed values overwrite healed drivetrain/fuel/cylinders.
- **Free-text listings search** embedded the whole fleet in the HTML (36.8 s, 237 MB) when pgvector is unconfigured; results are bounded now.
- **Car page:** price history was double JSON-encoded (always empty); market bands were stale since July; days-on-market misreported days since our first scan.
- **Tests** ran against the `.env` Postgres when no fixture set a DB; `idx_cars_active_zip` restored; pipeline waits for the database instead of burning the roster when a tunnel drops.

### Security
- CSRF header token now required on the account DELETE routes (saved searches, hidden dealers, search history).

## [1.3.2] - 2026-07-19

No CHANGELOG section was written at the time; this entry is backfilled from the release commit
(ba79193da): Redesign landing + register pages; fix page-load stalls.

## [0.2.0] - 2026-06-14

### Added
- **Delta scans:** scan only changed inventory; heal-pass parallelization; recipe capture fixes; fresh-pg schema (`e612399`).
- **Auto-heal:** quality-driven per-car re-acquisition after every dealer scan; floor only fields gap-fill can re-acquire (`0ba0fb3`, `5221259`).
- **VDP prefetch:** reuse known fields and cheap HTTP before browser visits (`e62aa9b`); capture 360-spin assets (Impel/SpinCar/WebRotate) during enrichment (`7f972e0`).
- **Listings/admin:** CPO + forced-induction filters; dealer hub and onboarding polish (`1250591`).

### Changed
- **Scanner:** collapsed flat/subpackage module duplication into alias shims (`6ca2bda`); UI responsive fixes, dashboard speedup, dev LLM lifecycle (`e6da3d7`).
- **Repo hygiene:** untracked `.venv` (`7f8b31c`); removed dead MFA modules (`9215d85`).

### Fixed
- **Recovery:** three stacked bugs left junk price-less inventory in place after failed scans — recovery now cleans them up (`c8eaece`).
- **SQLite schema:** `zip_code` column was missing from the cars migration list, breaking fresh-DB migrations (`0eb471e`).
- **Tests:** suite was non-hermetic and hiding real bugs; made hermetic and fixed the bugs it exposed (`d044234`).

### Security
- **X-Forwarded-For spoofing:** client IP now read from the right end of the header chain, not the left (`d482b3e`).
- **Sessions:** revalidate paid session claims; trusted client IP handling; suspended-login guard (`a4c8042`).
