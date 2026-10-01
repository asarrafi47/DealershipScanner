# Monolith audit — data/scripts area (323 files)
Read-only audit, 2026-10-01. Sizes from scratchpad/metrics.csv (lines / longest fn).
Sorted list: scratchpad/monolith/ds_sorted.txt

## Progress log (findings appended as reviewed)

### Step 1 — reference index (done)
Method: for each script/entry-point file, `git grep -l -w -F <stem>` across tracked files, bucketed
py / ops (sh, Dockerfile, railway/toml/json, plist, deploy/) / tests / docs. Raw: scratchpad/monolith/ds_refs.txt.
Last-commit date + commit count for zero-code/ops/test-reference scripts: scratchpad/monolith/ds_dead_dates.txt
(93 files have py=0 ops=0 test=0; many are deliberate CLIs like bump_version.sh / reset_app_user_password.py,
triaged below). Many c=1 2026-05-03 files are from the initial import commit and never touched since.

### F1 [P1] backend/db/inventory_pg.py::init_postgres_inventory — 477 lines (L287-764), file 764, fan_in 56
Responsibilities (all one straight-line function, one cursor, one commit at L760):
- L305-312 lock_timeout guard; L313-397 `cars` CREATE + pg_add_columns (L375) + idx_cars_dealer_listing (L398)
- L406-448 epa_master (+ add_columns L425, 2 idx); L450-480 epa_extended_specs; L482 model_specs
- L498-560 saved_cars, saved_searches, user_hidden_dealers, user_search_history (user-domain tables in the inventory init)
- L561-620 scan_runs (+provider col), dealer_scan_profile, nhtsa_vpic_cache, dealer_geopoints
- L625-680 dealerships (+ long pg_add_columns list L647, 3 idx)
- L682-700 incomplete_listings(+_meta); L708-758 package_observations / package_values
Evidence it is redundant: all 17 tables it creates are also created in migrations/ (V001 baseline .. V024; table-set diff
empty, see scratchpad/monolith/{pg,mig}_tables.txt). scripts/docker-entrypoint-web.sh:22 runs
`python3 -m backend.scripts.migrate --apply` on boot; migrations/README.md itself says this function builds a schema
"close to production, not identical". Comments inside mirror specific V0xx files by hand ("Mirrors migrations/V021...").
Why it hurts: three schema sources (migrations/, this function, schema_repo.init_inventory_db SQLite copy) that must be
hand-synced; every new column needs 2-3 edits; it ran CREATE INDEX on boot and caused the 2026-07-30 lock pile-up
(docstring L291-301); the lock_timeout+swallow in schema_repo.py:269-281 means drift fails silently.
Split/delete: (1) replace body with a migration-chain check: query schema_migrations max version vs highest V file, log/raise
if behind (5-10 lines). (2) Move the remaining pg_add_columns lists into a new V025 migration if any column is missing
from V001-V024 (needs a column-level diff against a fresh-migrated DB — not done here, read-only). (3) Keep the SQL-adapt
helpers L114-262 (qmarks_to_percent_s, adapt_insert_or_*), which are the real library surface (fan_in 56 is mostly those +
pg_connect). Scanner path backend/scanner/database.py:240-247 also calls it — needs the same replacement.
Blast radius: boot of web (backend/main.py:352 via schema_repo.init_inventory_db), scanner/cli.py:337, post_scan/job.py:256,
scanner/database.py:245, scripts/backfill_dealership_registry.py. Local dev without migrate would need `migrate --apply`.

### F2 [P1] backend/db/repositories/schema_repo.py::init_inventory_db — 188 lines (L262-455), file 473
L262-290 PG branch (calls F1, seed_cars, incomplete index bootstrap); L291-455 a full SQLite schema copy (cars, epa_master,
nhtsa_vpic_cache, saved_cars, scan_runs, car_move_log...) guarded by inventory_sqlite_tests_allowed(). Plus helpers
ensure_cars_table_columns L15, ensure_cars_listings_indexes L107, ensure_scan_runs_table L142, ensure_car_attribution_tables L191,
all typed sqlite3.Cursor. This is the SQLite-for-tests dual path (memory: "tests SQLite-only").
Why it hurts: a 4th copy of the schema that only tests exercise, so tests validate a schema prod does not have.
Split: move SQLite DDL to tests/fixtures (or generate SQLite schema from migrations with a tiny translator), leave
init_inventory_db as ~25-line PG-only bootstrap. Blast radius: whole SQLite test suite — P1 but must be paired with a
test-DB strategy decision (docs/CLOUD_RESTRUCTURE_PLAN.md D-items).

### F3 [P2] Runtime DDL scattered across 10 more backend/db modules (same disease as F1)
Count of CREATE TABLE/ADD COLUMN/CREATE INDEX statements per file: users_db/schema.py 27 (init_users_db 188 lines),
comments_db.py 11, dictionary_schema.py 7 (ensure_dictionary_tables 113 lines), user_history_db.py 6 (_bootstrap),
admin_users_db.py 5, dealerships_db.py 5, dealer_portal_db.py 4, incomplete_listings_db.py 3, search_analytics_db.py 3,
grid_cards_repo.py 1. Users/portal/comments are covered by V002/V004/V005/V012/V013. Fix: one "schema is migrated" assert
at boot; delete per-module bootstraps as each is confirmed covered.

### F4 [P2] backend/db/repositories/search_repo.py::search_cars — 415 lines (L175-590), file 696, fan_in 4
Signature: ~35 keyword params (L175-202). Body:
- L232-305 base WHERE: include_incomplete/flagged, candidate_ids, registry ids, vin; nested add_multi/add_multi_ci (L306-320)
- L322-336 country->makes; L337-380 vehicle_or (OR-of-ANDs); L381-406 packages any/all/single + trim_contains_list
- L407-450 year/price/mileage/cpo/inventory_condition; L452-472 nested color-family helpers + displacement
- L473-486 nested _post_sql_filters (Python-side filtering after SQL); L487-580 zip/radius geo path with
  two-phase rank-then-hydrate in chunks (L555 "Phase 2"); L590 sort by price.
Why it hurts: SQL builder, Python post-filter, geo radius and hydration paging in one closure-heavy function; any new
facet touches the 35-arg signature and both nearby_dealers.py:1072/1079 call sites (which pass **kwargs dicts, so typos
are silent). Comment L206 notes it runs on both SQLite and PG via ``?`` rewriting (dual path).
Split: `SearchFilters` dataclass (params) -> `build_where(filters) -> (sql, params)` (L232-472, pure, unit-testable
without DB) -> `post_filter(rows, filters)` (L452-486) -> `geo_ranked_hydrate(...)` (L487-580). search_cars becomes ~30 lines.
Blast radius: listings/nearby_dealers.py + 10 tests that call search_cars(**kw); keep the kw signature as a shim.

### F5 [P2] backend/db/repositories/listings_repo.py::_build_filter_options_uncached — 257 lines (L734-991), fan_in 11
Four independent DB reads + aggregation in one function under a single `with db_conn()`:
L740 nested distinct(col) per facet column; ~L768 color/interior bucket query; L790-830 packages JSON parse + dedupe;
L833-845 DISTINCT make/model/trim/... (26.6k rows); L901-950 make/model/trim normalized-variant ladders; L943-990 countries +
facet dict assembly. File also mixes grid serialization, grid cache/rebuild threads (L237-570), landing featured cars, geo maps.
Split: one function per facet family (`_facet_scalars`, `_facet_colors`, `_facet_packages`, `_facet_ymm_ladder`,
`_facet_countries`) each taking a cursor; and move grid cache/rebuild (L237-570) to its own module (grid_cards_repo.py
already exists at 1008 lines — that one is also a monolith: _resolve 151 lines).
Blast radius: get_filter_options consumers (listings page, API); behavior is pure aggregation so split is low-risk with
a golden-output test.

### F6 [P2] backend/db/comments_db.py — 1055 lines, 33 defs, longest flag_comment 59 (fine per-function; module is the monolith)
Five concerns: L88-350 DDL generation for both SQLite and PG (_comment_indexes, _ddl_comments, ensure_comment_*/flags/
attachments/dealer_ratings tables — ~260 lines, duplicates V002/V004/V005); L352-700 comment CRUD + sanitize/HTML +
rate-limit count + flagging; L701-873 attachments; L874-908 subject existence checks (cars/dealerships); L909-1055 dealer
Google-rating storage + "dealerships needing rating" queue (used by cron/sync_dealer_google_ratings.py and
scripts/import_dealer_google_ratings.py — not a comments concern at all).
Split: comments_db.py (CRUD+flags), comment_attachments_db.py, dealer_ratings_db.py (L909-1055); drop DDL once migrations
are authoritative (F1/F3). Blast radius: fan_in 4 — community routes, ratings cron, import script. P2 (low risk, mechanical).
(F6 correction: dealer-rating functions are consumed by backend/enrichment/dealer_ratings.py, not the cron/import scripts directly.)

### F7 [P1] backend/scripts/fetch_oem_brochures.py — 2525 lines; main() 425 (L2097-2522); a library disguised as a script
Seven subsystems in one file:
- L211-380 gap list from inventory DB (own `_inventory_dsn()` L211 + raw rows L228 — a private DB helper) + gap artifact
- L381-601 ArchiveResolver + resolve_source (source tiering: OEM vs archive)
- L623-884 download_and_extract — 262-line function (HTTP fetch, PDF save, text extraction, catalog keying, logging)
- L885-1029 page/corpus text helpers, fetch log, content index, audit_corpus_text
- L1030-1294 quarantine_unidentified (173 lines) + quarantine_derived — destructive corpus maintenance
- L1295-1553 archive_verify_overlap / archive_audit / audit_corpus — reports
- L1554-1609 reachability probe; L1610-2096 HTML spec pages: fetch_html_specs (227 lines, L1728-1954),
  rebuild_html_spec_overlays, reverify_html_specs (writes trim-adds overlays into TRIM_ADDS_BY_YEAR_DIR)
- main L2097-2522: ~22 argparse flags (L2099-2230) then a 9-way mode dispatch (L2232-2260: --quarantine-*, --audit-corpus,
  --rebuild-html-overlays, --reverify-html-specs, --archive-*, --probe-reachability) and then ~260 lines of the default
  "plan + download" mode inline (L2317-2520: gap selection, duplicate detection, archive resolution, tier summary, download loop).
Used as a library: backend/tests/test_brochure_sources.py imports build_gap_list and the module (L50, L1693-1749);
backend/enrichment/brochure_extract.py:642-692 documents that its page_texts/corpus_page_texts semantics must match this file,
and brochure_sources/{quality,quarantine,tiers,naming}.py reference its CLI modes as the writers of their state.
Why it hurts: enrichment correctness depends on helpers living in a CLI; the destructive quarantine modes share a parser with
the downloader; 262- and 227-line workers are untestable without network.
Split: backend/enrichment/brochure_sources/{gaps.py (L211-380), resolve.py (L381-601), download.py (L623-884),
corpus.py (L885-1029 + audits L1295-1553), quarantine_ops.py (L1030-1294), html_specs.py (L1610-2096)}; keep the script as
argparse + subcommands (`fetch_oem_brochures plan|download|quarantine|audit|html-specs|probe`), each mode a <40-line function.
Also extract the default-mode body of main (L2317-2520) as `plan_and_download(args)`.
Blast radius: tests/test_brochure_sources.py imports; brochure_extract.py text-semantics contract; operators' CLI flags
(keep flag aliases). No ops/cron refs (ds_refs: py=1 ops=0).

### F8 [P1] Duplicate DB connection helpers — 25 private `_dsn()/_inventory_dsn()` + ~8 `_connect()` copies in scripts
Raw list: scratchpad/monolith/ds_conn_helpers.txt (98 hits incl. psycopg/sqlite3.connect calls across 58 files).
29 files in this area hand-parse `.env` with `re.search(r"^INVENTORY_DATABASE_URL=(.+)$", ...)`; 25 `_dsn` helpers do this and
none import backend.db.inventory_pg. Representative verbatim copies: apply_attribution_moves.py:65, image_batch.py:175-191,
attribution_batch.py:47-59, recheck_msrp.py:47-59, report_rooftop_refusals.py:59-73, rooftop_refusals_census.py:50-62,
fetch_oem_brochures.py:211, trim_coverage_report.py:169, discovery_source_coverage.py:162, set_dealership_active.py:32, ...
Canonical helpers already exist: backend/db/inventory_pg.py::inventory_postgres_dsn (L71) / pg_connect (L108), and
repositories/base_repo.py::get_conn/db_conn (L76/L87). Plus 4 more module-local get_conn()s: dealer_portal_db.py:55,
incomplete_listings_db.py:42, users_db/_common.py:20, base_repo.py:76; vector/pgvector_service.py:89 _connect.
Why it hurts: (a) divergent semantics — the script copies fall back to reading `.env` directly, inventory_pg does not, and
the copies differ in autocommit (image_batch sets autocommit=True; pg_connect False) and connect_timeout; (b) the `.env`
fallback is exactly the Mac-mini trap in memory (mini .env points at its own local Postgres, so a script run without the
tunnel env silently writes to the wrong DB); (c) 25 places to change for any DSN/pooling/SSL change.
Fix: add `backend/db/connect.py::inventory_dsn(require=True)` + `connect(autocommit=..., timeout=15)` (or extend
inventory_pg.pg_connect with kwargs), delete every script copy (mechanical, ~10 lines each), decide once whether `.env`
fallback is allowed (recommend: load via python-dotenv at the CLI entry only, never regex). Blast radius: 25 scripts,
no library consumers. P1 because of the wrong-DB write risk, though the edit is low-risk.

### F9 [P1] Scripts used as libraries (by other scripts, tests, and by hand-mirrored constants in runtime code)
Import counts of `backend.scripts.X` across tracked .py (incl. tests): verify_trim_citations 15, dealer_pipeline 15,
image_batch 10, fetch_oem_brochures 4, data_quality_invariants 4, classify_attribution 4, build_trim_spec_sheets 4,
scan_lab_report 3, backfill_dealership_addresses 3, scanner_liveness 3, ... (28 scripts imported somewhere).
Script->script imports of PRIVATE names (underscore) — the worst kind:
- image_batch._download/_select_images <- attribution_batch.py:70, recheck_msrp.py:78;
  image_batch._is_mandatory_fee/_is_never_priced/reconciles_exactly <- feed_package_registry_from_vision.py:265,413
- classify_attribution._tokens/_MARQUES/_PAREN/_names_same_store <- apply_attribution_moves.py:115-197, attribution_batch.py:171
- verify_trim_citations._find_pdf/_sha256 <- build_trim_spec_sheets.py:170; read_page/PageReading <- verify_brochure_vision_facts.py:106;
  verify_overlay <- trim_coverage_report.py:1630
- dealer_pipeline._rows/get_conn/dealers_from_db <- unstale_host_blocked_recipes.py:33,78; discovery_probe.py:454
- data_quality_invariants.* <- heal_matcher_trims.py:75
Runtime code importing a script: backend/utils/local_llm.py:156 `from backend.scripts.scanner_liveness import scanner_pids`.
Runtime code hand-MIRRORING script constants (silent drift risk): backend/enrichment/generated_spec_sheet.py:802 "Mirrors
image_batch.AGENT_VISION_VERSION (= 100)"; backend/dealer/admin/data_quality_hub.py:28 copies
data_quality_invariants._FAIL_STATUSES/compare() "by hand"; backend/utils/spec_field_normalize.py:18 notes the canonical trim
splitter lived in data_quality_invariants.py; cars_repo.py:429-458 documents gates enforced only in image_batch.cmd_record.
Fix: promote the shared pieces into library modules — backend/enrichment/vision/{images.py (_download,_select_images),
sticker_gates.py (_is_mandatory_fee,_is_never_priced,reconciles_exactly, AGENT_VISION_VERSION)};
backend/enrichment/attribution/names.py (classify_attribution tokens/marques); backend/enrichment/brochure_sources/citations.py
(verify_trim_citations read_page/verify_overlay/_find_pdf); backend/scanner/pipeline_db.py (dealer_pipeline get_conn/_rows/
dealers_from_db); backend/data_quality/invariants.py (statuses + compare). Then runtime modules import the constant instead
of mirroring it. Blast radius: ~12 scripts + ~30 test imports (tests can import from new homes; keep re-exports one release).

### F10 [P1] backend/scripts/image_batch.py — 1345 lines; cmd_record 285 (L781-1066); cmd_claim 232 (L378-609)
Role: the claim/record seam between agent vision batches and car_image_text (writes shopper-facing MSRP/options).
Sections: L42-170 constants + CDN upsize regexes (_upsize_url L121); L175-257 private DSN/connect/fetch/download/
select_images (imported by 2 other scripts, F9); L258-377 MSRP reconciliation math (_options_gap, _reconciles,
reconciles_exactly); L378-609 cmd_claim (reserve + ordering by expected value + parallel download + manifest);
L610-780 value cleaners/gates (_clean_number, equipment cleaners, _is_never_priced, _is_mandatory_fee, _is_skewed_option);
L781-1066 cmd_record: load results (L782-788) -> reservation check (L793-818) -> per-item loop L818-1020 applying ~6 write
gates inline (VIN-confirmed, read-not-summed total L853-870, USD-only L863-870, reconciliation L894, option cleaning,
color clamp) -> INSERT ... ON CONFLICT into car_image_text (L1022-1050) -> immediate corpus-wide skew scrub (L1053-1066);
L1087-1209 cmd_scrub_skew; L1210-1290 requeue/pool/status; L1291 main.
Why it hurts: the MSRP write-gates (whose comments cite real incidents: car 888561 dropped destination, car 592273 CAD
sticker) live inline in a 285-line loop body; runtime readers (cars_repo.py:458, generated_spec_sheet.py:788-836) rely on
these gates having been applied but can't import them; AGENT_VISION_VERSION mirrored by hand (F9). Memory rule "audit the
write path" applies — the gates should be one pure function with tests.
Split: backend/enrichment/vision/sticker_gates.py: `evaluate_sticker_reading(item, car) -> (row|None, refusal_reason)`
holding L818-1020 gates + L258-377 reconciliation + L610-780 cleaners (pure, no DB); images.py for L121-257;
image_batch.py keeps claim/record/scrub as thin DB loops (~400 lines). Tests already exist (test_image_batch_reconcile.py,
test_image_batch_upsize.py) and would move to the pure module.
Blast radius: attribution_batch.py, recheck_msrp.py, feed_package_registry_from_vision.py (private imports), 4 test files,
agent-batch workflow (manual). Medium risk; high value.

### F11 [P1] backend/scripts/dealer_pipeline.py — 1394 lines; main 213 (L1153-1366); production orchestrator living in scripts/
It is the fleet's production entry point: Dockerfile.scanner:10 default cmd -> scripts/railway_scan_fleet.sh -> sharded
dealer_pipeline; also .claude/workflows/dealer-discovery.js. Imported by backend/scanner/recipe_validation.py,
scan_timing.py, discovery_probe.py:454, unstale_host_blocked_recipes.py:33,78 and 12 test files (15 import sites).
Responsibilities: L83-158 process/DB plumbing (chromium count, wait_for_db, _assess_conn autocommit fix, _rows);
L159-197 dealer roster (manifest + DB); L198-292 ensure_recipe; L293-393 lock wait + discovery capture subprocess + HTTP-only
scan subprocess; L394-418 vPIC; L419-607 per-dealer markdown logs (discovery.md / scan_runs.md / scan_instructions.md /
timing); L474-520 reconcile_dealer (retires listings: writes UPDATEs to cars — the riskiest code, buried mid-file between
log writers); L608-765 assess + verify_accuracy; L766-1017 scan hints, route_verdict, lifecycle (re-discovery) and retry
batches; L1018-1103 lifecycle pass; L1104-1152 + L1368 triage/needs-discovery/slow-dealer outputs; L1153-1366 main:
numbered steps (roster L1176-1202, recipes L1203-1240, scan L1242-1297, assess, lifecycle L1322-1350, platform clustering L1351).
Why it hurts: a scanner-core library (reconcile/assess/verdict routing, which memory ties to "1,595 unexplained retirements"
and the "guard refused 12,903 moves") lives in a CLI file; tests import it as a library; changes to log formatting and to
retirement logic share one file and one review.
Split into backend/scanner/pipeline/: roster.py (L159-197), recipes.py (L198-292), runner.py (L293-393, 991-1017),
assess.py (L608-765 + reconcile_dealer L474-520 — own module, own tests), lifecycle.py (L766-1103), dealer_logs.py
(L419-607 — the CLAUDE.md-mandated logs writer), triage.py (L1104-1152, L1368+). backend/scripts/dealer_pipeline.py keeps
argparse + main calling `pipeline.run(args)`; keep re-exports (dealers_from_db, get_conn, _rows) for one release.
Blast radius: HIGH — Railway fleet entrypoint + 12 tests. Do it as a pure move (no logic change) with the test suite as net.

### F12 [P3] backend/scripts/data_quality_invariants.py — 1394 lines; run_rendered_tier 192 (L936-1134)
Structure is mostly sound: L137-585 is a declarative registry (17 SqlInvariant entries = data, not logic); L586-640
DB helpers (_inventory_url L586 — another .env-regex DSN copy, and unlike the others it ignores DATABASE_URL: divergence,
see F8); L642-791 stored tier; L792-1134 rendered tier (one 192-line loop scoring each representative car); L1135-1290
baseline compare/print/write; main L1291. Run nightly by deploy/nightly_data_quality_invariants.sh; read by
backend/dealer/admin/data_quality_hub.py which copies _FAIL_STATUSES/compare() by hand (F9).
Split: move InvariantResult/statuses/compare/load_baseline (L137-164, L1135-1190) into backend/data_quality/invariants.py
so data_quality_hub imports instead of mirroring; split run_rendered_tier per check (ladder, provenance, fuel bucket).
Blast radius: nightly job + admin hub + 2 tests + heal_matcher_trims.py:75. P3 — works, the hub coupling is the real item.

### F13 [P2] Package-registry feeders: 4 scripts, 3 near-identical
backend/scripts/feed_package_registry_from_vision.py (551; main 265, L282-547), feed_package_registry_from_brochure_vision.py
(130), feed_package_registry_from_car_image_vision.py (126 — docstring: "Mirrors feed_package_registry_from_brochure_vision.py
exactly, pointed at car_image_vision_facts (V017) instead of brochure_vision_facts (V015)"), backfill_package_registry.py (112).
Each has its own _dsn (F8). feed_..._from_vision main L282-547 mixes: env/DSN export hack (L296-302, because the script
regex-parses .env while backend.db reads os.environ), registry source-authority precondition (L311), ledger precheck,
cohort SQL with 4 policy filters (L340-372), corroboration (L140-234), self-reconciling cars (L235-281), per-car write loop
(L392-500), rejection ledger (L504-526). It also reaches into package_registry._SOURCE_AUTHORITY (private) and
image_batch._is_mandatory_fee/_is_never_priced (private, F9). Called by nothing (cars_repo/package_registry/
window_sticker_service only mention it in comments).
Split: one `feed_package_registry.py --source {photo_text,brochure_vision,car_image_vision,cars_packages}` with a
per-source `rows(cur)` adapter; move the cohort policy filters + corroboration into backend/enrichment/package_registry.py
(where the authority map already lives). Removes ~250 duplicate lines. Blast radius: manual ops only (no cron/tests).
(F13 evidence: `diff` of lines 30-130 of the brochure_vision vs car_image_vision feeders shows only 10 differing lines.)

### F14 [P1] SQLite vs Postgres dual paths in backend/db (half-finished migrations)
- Users domain: runtime is SQLite/SQLCipher only — users_db/_common.py:20 get_conn -> users_sqlite.get_users_conn (L85-117,
  sqlite3/sqlcipher, PRAGMA WAL); users_db/schema.py init_users_db (188 lines, 27 DDL stmts, sqlite3.Cursor-typed). Meanwhile
  migrations/V013__users.sql creates the same tables in Postgres and backend/scripts/migrate_users_sqlite_to_postgres.py
  (commit c737c0147 "Prepare ... (no runtime cutover)") copies them. docs/CLOUD_RESTRUCTURE_PLAN.md:242 "users table in
  Postgres has 0 rows today". So the users schema exists twice and the PG copy is unused.
- Same shape for dev users (dev_users_sqlite.py, admin_users_db.py 5 DDL), dealer portal (dealer_portal_db.py get_conn L55 +
  4 DDL vs V012 + migrate_dealer_portal_sqlite_to_postgres.py), search_analytics_db.py, user_history_db.py (_bootstrap).
- Inventory: schema_repo.init_inventory_db SQLite branch (F2), inventory_compat.py (130) + inventory_pg.adapt_* SQL rewriting
  (qmarks_to_percent_s, adapt_insert_or_ignore/replace L114-262) let every repo write SQLite dialect and have it translated
  at runtime — search_repo.py:206 says so explicitly. base_repo._default_inventory_db_path (L103) still resolves a SQLite file.
Why it hurts: every repository function carries two dialects; tests run the SQLite one (memory: tests SQLite-only), prod runs
the translated one; DDL duplicated per domain; the users cutover is blocked on this.
Fix: execute the plan already written (docs/CLOUD_RESTRUCTURE_PLAN.md §4.2 / docs/USERS_PG_CUTOVER.md): USERS_DB_BACKEND switch,
then delete users_sqlite/dev_users_sqlite/DDL; for inventory, stand up a throwaway Postgres for tests and remove the SQLite
branch + adapt_* rewriting. Blast radius: whole web app auth + test harness — P1 strategic, not a quick edit.

### F15 [P3] Browser E2E scripts — 3 overlapping, unreferenced by tests/CI
scripts/e2e_user_audit.py (540; run_audit 272 L148-420 — one linear Playwright script for guest/free/premium flows, writes
docs/E2E_USER_AUDIT_REPORT.md), backend/scripts/e2e_smoke_browser.py (359), backend/scripts/e2e_deep_browser.py (226; main 205).
Refs: only docs (docs/FULL_AUDIT_MAY2026.md:115); no tests/ops. Last touched 2026-06-09. e2e_user_audit._start_server (L74-90)
spawns run.py with USERS_DB_PATH (SQLite users) and _grant_premium imports users_db internals — will break at the PG users cutover.
Recommendation: either make them a pytest-playwright suite (split run_audit into per-flow tests: guest L148-230, free,
premium) or delete; they are not run by anything. Evidence of staleness: c=2-3 commits, all <= 2026-06-09.

### F16 [P2] Root-level legacy entry points
- scanner.py (52), post_scan.py (28), discovery.py (34): thin shims (chdir to repo root, sys.path insert, dotenv, call
  backend.scanner.cli / backend.scanner.post_scan.job / backend.discovery.cli). KEEP scanner.py — live callers:
  deploy/nightly_http_refresh.sh:155, deploy/NIGHTLY_REFRESH.md:13, deploy/car-scanner/cronjob.yaml:74, scripts/scan_irvinebmw_test.sh:27,
  Dockerfile.scanner-worker:3. post_scan.py: only deploy/k8s/cronjob-post-scan.yaml (k8s stack, see below). discovery.py: name
  collides with the backend.discovery package in greps; real callers are docs/Dockerfile.discovery — verify before removal.
  They don't shadow anything at import time (no bare `import scanner|discovery` anywhere; `git grep` of bare imports = 0).
- run.py (66): Werkzeug dev server; used by scripts/e2e_user_audit.py:84 and run-web-local.sh. Calls
  backend/utils/project_env.ensure_backend_on_sys_path() — the ONLY caller in the repo — which inserts backend/ on sys.path so
  `backend/scanner` is also importable as top-level `scanner` (and `scraping`, `discovery`, ...). No code uses bare imports
  (0 hits), so it only creates a double-import hazard. Delete the call + helper.
- run_dealership_pipeline.py (206): DMV->OSM->DDG discovery then scanner; zero refs anywhere (py/ops/tests/docs = 0), single
  commit 2026-05-03. Superseded by backend/scripts/dealer_pipeline.py + discovery_probe. DEAD.
- run_city_discovery.py (232; main 179): only referenced by run_nationwide_discovery.py's docstring; last commit 2026-05-06. DEAD candidate.
- run_nationwide_discovery.py (227; main 157): referenced by deploy/k8s/cronjob-discovery.yaml only.
- search_dealer_addresses.py (97): writes dealers_with_addresses.json, which nothing reads (only self-reference); 2026-05-06. DEAD.
- deploy/k8s/ (10 manifests, last commit 2026-08-06) is not my area, but note: prod is Railway (memory), so the k8s cronjobs that
  are the sole callers of post_scan.py and run_nationwide_discovery.py are themselves probably dead — confirm with owner.
Fix: move surviving shims into one `python -m backend` dispatcher or keep scanner.py only; delete the 3 DEAD files.

### F17 [P3] backend/db/repositories/grid_cards_repo.py — 1008 lines; _resolve 151 (L652-810), fan_in 7
Concerns: L91-147 cache revision computed by hashing the SOURCE TEXT of listed functions/dirs (_function_source parses .py
files at import time to find `def name(` blocks — fragile: formatting-only edits invalidate every card; a renamed function
silently hashes to b""); L148-220 table ensure (runtime DDL, F3) + scope token; L221-370 attribution/market-band generation
keys; L372-530 row fetch/serialize/store; L531-604 background refresh thread + queue; L605-810 scope resolution (_resolve:
partitions fresh/aged/stale, inline rebuild vs background); L811-970 geo scopes (cards_near, cards_for_dealer); L971 offline builder.
Split: cache_keys.py (L91-370), grid_cards_store.py (L372-604), grid_cards_query.py (L605-970). Replace source hashing with an
explicit GRID_CARD_REV bump (already exists, L106) + a test that fails when serializer output changes.
Blast radius: listings page hot path; P3 (works, perf-sensitive, recently tuned).

### F18 [P1] SQLite-era inventory scripts — broken against the Postgres-only inventory
These open `sqlite3.connect(os.environ.get("INVENTORY_DB_PATH", "inventory.db"))` directly (no compat layer), so against
prod/mini they either fail or silently read/write a stray local inventory.db (schema_repo already refuses SQLite inventory
outside tests: "SQLite inventory init is disabled; set INVENTORY_DATABASE_URL to Postgres"):
  backend/scripts/backfill_transmission.py (414, :33, 2026-05-03), populate_model_specs.py (193, :125, 2026-05-03),
  backfill_interior_vision.py (158, 2026-05-22), backfill_inventory_engines.py (107), backfill_listing_packages.py (100, doc L6-8),
  backfill_mpg_from_source_url.py (81), backfill_window_stickers.py (140, doc L9-11), clean_gallery_urls_in_db.py (60, :21-25),
  scripts/audit_inventory_coverage.py (140, ":2 Coverage audit for inventory.db") — all last touched <= 2026-06-09, all with
  zero ops/test refs; and backend/scripts/image_downloader.py (832; read_cars L290-292, _update_db_with_local_images L573-581,
  plus Playwright stealth at L746-751 — violates the HTTP-only/no-browser rule) which runtime code still documents as the
  writer of /car-images/ (backend/routes/site_misc.py:10, utils/field_clean.py:659).
Also scripts/scanner_worker_loop.py::_sync_dealer_sqlite_to_postgres (L78-~140): "scanner.js writes SQLite" — scanner.js no
longer exists in the repo; docs/CLOUD_RESTRUCTURE_PLAN.md:164 and :367 already mark it "Delete ... verified safe".
Fix: delete the 9 SQLite backfills (or port the 1-2 still wanted onto base_repo.db_conn); delete the sync function; decide
whether local /car-images/ serving is still a feature (if not, delete image_downloader.py + the site_misc route).
Legit SQLite users (keep): migrate_*_sqlite_to_postgres.py (source side), app_users_status.py, reset_app_user_password.py,
scripts/create_demo_free_user.py (users.db is still SQLite at runtime, F14).

### F19 [P2] Options-dictionary CSV builder cluster — 13 scripts (~3,700 lines), mostly frozen since May-June 2026
The runtime still reads backend/dictionary/*_Complete_Options.csv (enrich_from_dictionary.py, used by
backend/scanner/post_scan/pipeline.py), but the builders that produced the CSVs are untouched one-shots, superseded by the
brochure-vision / trim-ladder pipelines (brochure_sources/, trim_ladder/):
  import_brochures_to_dictionary.py 950 (2026-06-09, 0 refs; downloads from auto-brochures.com — a third-party archive the
  newer tiering in fetch_oem_brochures explicitly ranks below OEM), car_data_scraper.py 944 (2026-05-06, Playwright —
  violates the no-browser rule; only refs are docstrings), clean_vehicle_csvs.py 260 (05-03, 0), prune_car_options_csv_passenger.py
  153 (05-03, 0), build_dictionary_options.py 153 (07-07, 0), augment_dictionary_engines.py 80 (0), fill_options_from_research.py
  474 (0), backfill_options_year_gaps.py 214 (0), dictionary_rebuild.py 104 + migrate_dictionary_layout.py 174 +
  normalize_complete_options_schema.py 41 + build_dictionary_manifest.py 42 (a self-contained rebuild chain, doc-only refs),
  import_epa_to_dictionary.py 219 (KEEP — imported by brochure_extract.py, trim_ladder/citations.py, 1 test).
Recommendation: keep dictionary_rebuild chain only if someone still regenerates CSVs (ask owner); otherwise move the whole
cluster to an archive/ dir or delete. Evidence: ds_refs.txt rows (py/ops/test = 0) + last-commit dates above.

### F20 [P2] Two parallel "fill dealerships.street_address/zip_code from the dealer's own site" implementations
- backend/discovery/address_enrich.py (437; 2026-09-26): _walk L125, _addresses_from_jsonld L136, _addresses_from_microdata
  L169, _fetch_via_browser L207, _fetch L237, resolve_address L257, main L339. Zero importers (git grep: only itself + a
  docs/CLOUD_RESTRUCTURE_PLAN.md mention).
- backend/scripts/backfill_dealership_addresses.py (887; 2026-09-27): _walk L213, jsonld_addresses L245, text_addresses L259,
  BrowserFetcher L418 (headless Chromium), fetch_http L376, OSM candidates L505, verify/choose with ZIP-centroid agreement
  L280-356, shared-storefront drop L659, run L736-857 (122 lines). Has 2 tests (test_dealership_address_backfill.py,
  test_rooftop_address_provenance.py) and V003 provenance columns.
Same JSON-LD tree walk written twice, same target columns, both carry a browser fallback (the HTTP-only rule says browsers
are discovery-only). Related: geocode_dealers.py (646, coordinates), coordinate_enrich.py (ZIP from coords), and root
search_dealer_addresses.py (dead, F16) — 4-5 address/geo fillers.
Fix: delete address_enrich.py (untested, unimported) or fold its microdata extractor into the tested script; move the pure
extractors (L181-356 of the script) into backend/discovery/address_extract.py so geocode_dealers/discovery can reuse.
Blast radius: none for the deletion (no importers); 2 tests for the move.

### F21 [P2] backend/scraping/ — dealer-group "copyright/org-name" crawler (~2,500 lines) reachable only from its own CLI
Importer census (scratchpad/monolith/ds_pkg_refs.txt; cols = non-test py importers, test importers, ops refs):
library half used elsewhere — models.py (7), text_utils.py (8), paths.py (6), constants.py (6), org_validation.py (5),
canonical_groups.py (3) — via backend/intelligence/pipeline/*, backend/oem/*, backend/dev/dealer_url_infer.py.
Crawler half used ONLY inside the package: cli.py (431; main 223 L~60-430), crawler.py (536; process_site_playwright 183 —
Playwright), site_profile.py (596; build_site_profile 97), inference.py (457; run_inference_on_blobs 114), html_extract.py
(251), fetch_requests.py (170), entity_specificity.py (267), adjudicate_crawl.py (50), sources.py (62 — opens SQLite per
docs/CLOUD_RESTRUCTURE_PLAN.md:267), redirects.py (118), fixture_tests.py (137), __main__.py; entry points are
`python -m backend.scraping` and backend/scripts/dealer_group_copyright.py (28 lines, 0 refs). 0 tests, 0 ops refs.
Latent bug: backend/scraping/cli.py:285 `from scraping.fixture_tests import main` — a bare import that only resolves when
backend/ is on sys.path (only run.py does that, F16), so `--fixture-test` raises ModuleNotFoundError under `python -m
backend.scraping`; docstrings say `python -m SCRAPING.fixture_tests` (fixture_tests.py:4, __init__.py:22).
Also backend/discovery/franchise_filter.py (74, 0 importers) duplicates overture_discovery.is_franchised_dealer (L92) + its
OEM_BRANDS list.
Fix: split package into backend/scraping/core (models, org_validation, canonical_groups, text_utils, paths, constants —
keep) and decide on the crawler half: delete (browser crawler, superseded by discovery_probe + HTTP-only discovery) or keep
with a test. Delete franchise_filter.py. Blast radius of deletion: dealer_group_copyright.py only.

### F22 [P2] backend/db/dealerships_db.py::upsert_discovery_row — 185 lines (L143-328); file 667, fan_in 25
Module mixes: L34-104 ensure_dealerships_table (sqlite3.Cursor-typed runtime DDL, F3); L118 geocode_city_state; L143-328
upsert: dedupe-key / osm_id / existing-row lookup (L162-205), a ~55-line UPDATE (L205-260) and two INSERT copies — one
`if is_inventory_postgres()` RETURNING id (L293-310), one SQLite lastrowid (L314-328) (dual path, F14); L330-440 radius search,
get-by-id, Google-rating queue + save; L440-517 sticker-provider/iPacket counters (scanner concerns); L518-700 insert/list/
delete/geocode-missing/deduplicate admin ops.
Google ratings are stored in TWO places by TWO code paths: dealerships.google_rating* columns via
dealerships_db.list_dealers_needing_google_rating/save_dealer_google_rating (cron/sync_dealer_google_ratings.py:29-105,
enrichment/dealer_ratings.py:645) AND the dealer_ratings table via comments_db.list_dealerships_needing_rating/
upsert_dealer_rating (enrichment/dealer_ratings.py:681-756). V002 header (L12) explains the second table exists on purpose,
but the two writers/queues remain separate.
Split: dealerships_db -> dealership_registry.py (upsert/insert/dedupe; one dialect), dealer_scan_counters.py (L440-517),
and a single dealer_ratings_db.py that owns both rating writes (merge with comments_db L909-1055, F6).
Blast radius: 25 importers — do via re-exports.

### F23 [P3] Long-but-linear pipeline functions (note only; split when next touched)
- backend/discovery/pipeline.py::run_discovery 200 (L96-295): tiers DMV (L135) -> Google Places -> OSM (unconditional, L147-175)
  -> merge -> non-dealer/closed filter (L176-215) -> seed-ZIP filter (L231) -> DDG URL fill (L241). Split per tier fn.
- backend/dictionary/enrich_from_dictionary.py::enrich_car 154 (L405-558): EPA candidate pick (L405-445) + CSV fill loop
  (L508). Live (post_scan pipeline).
- backend/vector/pgvector_service.py::_reindex_listings 81 (file 715, fan_in 9); own _connect L89 (F8).
- backend/cron/sync_gas_prices.py::fallback_payload 82 — static fallback table as code; move to JSON.
- backend/db/repositories/cars_repo.py::car_sticker_msrp_values 121 (file 739, fan_in 27) — reads image_batch-gated rows (F9/F10).
- backend/db/users_db/* (auth 570, accounts 511, admin 479, schema 303): already split from a 2000-line module (users_db/__init__.py
  docstring); remaining issue is SQLite-only (F14).

### F24 [P1] backend/scripts/reset_db.py — `DELETE FROM cars` with only a --yes flag; no prod guard, no refs
62 lines; main L~25-60: counts cars, then `cur.execute("DELETE FROM cars")` + commit when --yes. Uses
backend.db.inventory_db.get_conn -> whatever INVENTORY_DATABASE_URL/.env points at; no is_production_env() check, no
scanner_liveness guard (backend/scripts/scanner_liveness.py exists exactly for this), no backup. Zero references anywhere
(ds_refs: py/ops/test/doc = 0); last touched 2026-07-07. With the Mac-mini tunnel env (memory: INVENTORY_DATABASE_URL=
postgresql://localhost:15432/cars) this wipes the shared fleet DB (~70k cars). The "fresh scan" workflow it served
predates the recipe/reconcile pipeline. DELETE the file (no capability lost — `psql -c 'TRUNCATE'` remains for a real reset).

### Scripts triage, part 1 (from scratchpad/monolith/ds_scripts_triage.txt — date | lines | refs | purpose)
One-shot schema ops already applied and now superseded by migrations/ (dead): rename_catalog_tables.py (115, 06-26),
drop_dead_zip_code_column.py (113, 07-06), drop_dead_kbb_columns.py (120, 07-06), migrate_placeholder_nulls.py (125, 07-07,
"One-off cleanup"), remove_dummy_vin_cars.py (36, 05-03, targets inventory.db), migrate_recipe_aliases.py (181, 08-20),
migrate_inventory_sqlite_to_postgres.py (167, 06-14, "One-time copy"; doc refs only — keep until SQLite fully retired).
SQLite/inventory.db-era backfills (dead, see F18 for the sqlite3.connect list): backfill_engine_l, backfill_specs_from_
structured_sources, backfill_interior_color_buckets, backfill_vehicle_specs ("on SQLite cars"), apply_model_specs,
backfill_transmission, normalize_transmission_inventory, fill_incomplete_listings, parse_listing_descriptions,
backfill_interior_vision, backfill_interior_from_vision (35-line wrapper for backend.vision.analyze_images), image_analyzer
(406, "Standalone image analyzer"), backfill_inventory_engines, backfill_window_stickers, backfill_listing_packages,
backfill_mpg_from_source_url, clean_gallery_urls_in_db, verify_decode_trim (70, "Quick sanity checks" — belongs in tests),
trace_car_vin (49, local dev).
Browser (Playwright) scripts violating HTTP-only rule, 0 refs: refetch_descriptions.py (235, "Uses Playwright"),
car_data_scraper.py (F19), image_downloader.py (F18).
Manifest-era (dealers.json) one-shots, 0 refs: clean_existing_manifest (166, "One-off scrub"), fingerprint_providers (457),
discovery_merge_to_manifest (68).
Trim/dictionary research one-shots, 0 refs: analyze_trim_adds_with_web (371), backfill_missing_trim_adds (65),
apply_trim_ladder_diffs (76), fix_bmw_trims (140), apply_ai_spec_fill (130), apply_engine_spec_fill (93) + F19 cluster.

### Scripts triage, part 2
- Two epa_master importers: import_epa_master.py (278; downloads vehicles.csv; docstring still says "into SQLite table",
  uses inventory_db.get_conn via compat — works on PG) and build_epa_master_pg.py (188; from in-repo DICTIONARY/*_EPA.csv,
  pg_connect) — plus import_epa_to_dictionary.py writes the *_EPA.csv the second one reads. Three EPA ingestion paths.
  [P3] pick one source of truth (vehicles.csv -> epa_master) and delete build_epa_master_pg or document the split.
- Attribution family (08-20): classify_attribution (311), apply_attribution_moves (400), attribution_batch (235),
  resolve_photo_attribution (346), attribute_feed_rooftops (341), add_dealership (154): private cross-imports (F9) and 3
  of them own _dsn copies (F8). Consolidate shared name logic into backend/enrichment/attribution/.
- Local-LLM vision lanes (09-04): local_brochure_vision_extract (498; main 99), local_car_image_vision_extract (347; "sibling ...
  same architecture"), verify_brochure_vision_facts (328; main 149), run_image_text_extraction (364; main 126),
  local_brochure_text_cache (121). Same skeleton twice (brochure vs car photo) — [P3] share a lane runner. Each has own _dsn.
- Rooftop refusals: report_rooftop_refusals (384) + rooftop_refusals_census (297) — two readers of the same ledger, both with
  _dsn+_connect copies, both run by deploy/nightly_rooftop_refusals.sh. [P3] merge into one report with subcommands.
- scan_lab_report.py (863; tally_dealer 87): imported by dealer_pipeline (scan-lab section of summary) + 2 tests — another
  script-as-library (F9); move tally into backend/scanner/pipeline/.
- discovery_probe.py (474; _synth_report 92): live (dealer-discovery workflow, Dockerfile, 2 tests) — fine as CLI; imports
  dealer_pipeline.dealers_from_db (F9).
- synthesize_recipes.py (384; _process_dealer 111), fleet_scan.py (407; main 109, ops: railway), platform_candidates.py (315),
  build_listings_grid_cards.py (49), fingerprint_timing (107), unstale_host_blocked_recipes (102): live, recent, fine
  (fleet_scan main is 109 lines of shard orchestration — acceptable).
- One-shot heals that already ran (keep 1 release, then archive; no refs except tests): heal_stock_code_contamination (494,
  1 test), heal_matcher_trims (438, 1 test), heal_ev_cylinders (199), heal_cylinders_from_vpic (170), heal_from_vpic (59, live
  policy per CLAUDE.md — KEEP), backfill_inactive_from_last_scan (113, "One-time backfill for the reconcile bug 2026-07-19"),
  quarantine_package_value_msrps (324, 0 refs), recheck_msrp (297, "562" one-off re-read), recover_incomplete_listings (269,
  doc ref only), repair_vin_make_model (86, 0 refs).
- Discovery-era analysis one-shots, 0 refs, 08-06 single-commit: discovery_source_coverage (824; run 107), audit_unscannable_dealers
  (339), discover_platforms (190), build_synth_manifest (162), cascade_recipes (129), classify_dealers (129), audit_scan_coverage
  (113, 07-21), audit_recipe_coverage (114, 09-26), report_dealer_scores (95), report_inventory_signals (99), national_scan (209,
  imported by 2? -> check fleet_scan), backfill_dealerships_from_roster (61), set_dealership_active (111 — operator tool, KEEP).
- Operator tools, fine: migrate.py (330; boot path), dump_baseline_schema, scanner_liveness, db_admin (381), app_users_status,
  reset_app_user_password (guarded by ALLOW_LOCAL_PASSWORD_RESET), import_recipes_to_db, compute_market_stats (ops: nightly),
  rebuild_listings_index (ops), repair_inventory_fields (ops), harvest_html_jsonld/harvest_carscommerce/heal_from_recipes (ops:
  nightly_http_refresh.sh), link_cars_to_catalog, backfill_vpic_cache, backfill_mpg_from_epa, backfill_extended_specs,
  build_trim_* / merge_trim_ladders / validate_trim_overlays / promote_trim_candidates / build_brochure_* / extract_brochure_text /
  process_brochure_queue / reingest_brochures / delete_brochure_pdfs (disabled by default) / verify_trim_citations (ops=27 —
  referenced from trim overlay JSON) / build_trim_msrp_bands / backfill_package_registry / recover_team_velocity_images (thin
  wrapper) / import_dealer_google_ratings / geocode_dealers / merge_ep_batch (stdin filter used by scanner) / discover_dealerships
  (37-line wrapper) / reindex_vectors (44; docstring says "from SQLite" — stale text) / backfill_dealership_registry /
  fix_extended_spec_outliers (report+NULL, 0 refs, keep as tool) / fetch_keffer_interior_colors (332, single-dealer one-off, DEAD) /
  refetch_colors (612, 0 refs, 07-20; SRP re-fetch layers — superseded by heal_from_recipes, DEAD candidate).

### F25 [P2] scripts/ and deploy/ shell + root-level ops
Broken (reference files that no longer exist) — DELETE:
- scripts/scan_92694_mac_mini.sh (9) and scripts/continue_92694_mac_mini.sh (10): `exec python3 scanner_mac_mini.py ...` —
  scanner_mac_mini.py does not exist in the repo (ls: No such file). 0 refs. 2026-06-08.
- scripts/scanner_worker_loop.py::_sync_dealer_sqlite_to_postgres (F18): reads output of scanner.js, which no longer exists.
Unsafe:
- scripts/start-always-on.sh (38; 2026-05-30, 0 refs): `PUBLIC=1 ... python3 run.py &` behind the Cloudflare tunnel — the
  Werkzeug dev server on the public site, which start.sh's own header forbids ("must run under gunicorn — never the Werkzeug
  dev server in run.py"). DELETE (start.sh START_WEB=1 covers it with gunicorn).
Stale one-offs, 0 refs: scripts/scan_irvinebmw_test.sh (27, single-dealer test), scripts/discover_metro.py (206, Google Maps
city tiling — Places API disabled per memory), scripts/audit_inventory_coverage.py (inventory.db, F18), scripts/e2e_user_audit.py
(F15), scripts/create_demo_free_user.py (61, users.db demo user — keep only if demos still happen),
deploy/car-scanner/build-push.sh (30, pushes scanner image to the Mac-mini registry — k8s/mini era, 05-06),
deploy/railway/push-dealer-secrets-to-vault.py (162, 06-11, one-shot secret push; 0 refs — keep as runbook tool or move to docs).
Live/fine: docker-entrypoint-web.sh (runs migrate on boot), docker-entrypoint-scanner-{worker,scheduler}.sh, railway_scan_fleet.sh
(Dockerfile.scanner default), scanner_import_sweep.py (Dockerfile.scanner), scanner_scheduler_loop.py, bootstrap_site_admin.py,
build_static_compressed.py (Dockerfile.web), bump_version.sh (CLAUDE.md rule), security_check.sh (doc refs; pre-push style
check), run-web-local.sh, seed_dealer_catalog.py (09-26 one-shot for empty dealer_catalog, memory), refresh_lease_matches.py
(07-19, 0 refs but documented page cache warmer — low value), scan_dealer_specials.py (wrapper over backend.scanner.specials),
deploy/{build,up,restart,load-vault-env}.sh (local Docker), nightly_{http_refresh,data_quality_invariants,rooftop_refusals}.sh
(launchd plists), railway/{sync-vault-to-railway.sh (railway.toml), deploy_scanner_nightly.sh}, gunicorn.conf.py, start.sh.
Note deploy/nightly_http_refresh.sh still runs `scanner.py --delta` locally (L155) while memory says the nightly plist is
disabled and Railway is the target — confirm which nightly is canonical.

### F26 [P3] Small helper duplication across scripts (host/haversine/dealer slug)
- Host normalizers: 52 `def *host*(` definitions repo-wide (non-test); in this area: db/dealer_geo.py:9 normalize_dealer_host
  (canonical), discovery/address_enrich.py:115, discovery/non_dealer_filter.py:151, discovery/web.py:59, scripts/
  audit_scan_coverage.py:38, discovery_source_coverage.py:122, report_rooftop_refusals.py:110, resolve_photo_attribution.py:156,
  plus apply_attribution_moves._dealer_slug (L78) and build_synth_manifest._slug_to_host (L48). The "www." strip / registrable
  domain rules differ subtly — host keys are how dealers are joined (memory: geocode hits verified by host equality).
- Haversine: db/geo.py:49 (canonical), enrichment/dealer_ratings.py:306, scripts/discovery_source_coverage.py:111,
  scripts/discover_metro.py:64 (km).
Fix: backend/utils/hosts.py (normalize_host, registrable_domain, dealer_id_from_host) + reuse db/geo.haversine.

### F27 [P3] Duplicate CLI entry points for the same module
- Dealership discovery CLI (backend.discovery.cli::main, 166 lines) has THREE wrappers: root discovery.py (34),
  backend/scripts/discover_dealerships.py (38, emits a DeprecationWarning per its `import warnings`), and
  `python -m backend.discovery`; plus run_dealership_pipeline.py / run_city_discovery.py / run_nationwide_discovery.py
  (root) re-implement tiered orchestration around it (F16).
- pgvector reindex: backend/vector/__main__.py (24) and backend/scripts/reindex_vectors.py (45, docstring "from SQLite" — stale;
  calls the same pgvector_service.reindex_all / reindex_inventory_only). Keep one.
- backend/scraping: __main__.py + cli.py + scripts/dealer_group_copyright.py (F21).
- backend/db/inventory_db.py (137): facade re-exporting repositories, fan_in 261 — acceptable shim, but it hides which
  repo a caller uses; new code should import repositories directly (P3).
- Cross-database coupling note for F14: saved_cars / saved_searches / user_hidden_dealers / user_search_history live in the
  inventory Postgres (created by inventory_pg L498-560) keyed by user_id, while users live in SQLite users.db — the
  user_id is an unenforced cross-DB reference until the users cutover.

### F28 [P3] backend/scripts/probe_oem_discovery.py — 1092 lines, 0 refs, single commit 2026-08-06 (15767b2e2)
One-shot OEM reconnaissance (docstring: "Reconnaissance: how does each OEM publish trim/spec information today"); its output
workspace/oem_discovery_report.json was last written 2026-08-02. visible_text/spec_signal/index_signal (L99-420) are the
reusable part — fetch_oem_brochures.fetch_html_specs (F7) does the production HTML-spec fetching and does not import them;
targets_for (L697-981, ~280 lines) is a hard-coded OEM URL table. Recommendation: archive/delete; if HTML spec signal scoring
is wanted, move L99-420 into backend/enrichment/brochure_sources/html_signal.py and have fetch_html_specs use it.

### F29 [P3] backend/cron/ is not cron
None of backend/cron/*.py (sync_gas_prices 588, sync_dealer_google_ratings 139, scan_run_counter 79, sync_epa_ev_ranges 33)
is referenced by any plist/Dockerfile/railway/cron config (ops refs = 0). They're called in-process from
backend/routes/fuel_api.py, backend/scanner/orchestrator.py, post_scan/job.py and post_scan/pipeline.py, or by hand
(sync_epa_ev_ranges: 0 importers, run as `python backend/cron/sync_epa_ev_ranges.py`). Rename to backend/sync/ or similar to
stop implying a schedule exists; sync_gas_prices.fallback_payload (82 lines) is a data literal -> JSON file.

---------------------------------------------------------------------------------------------------------------------------

## APPENDIX: every other file -> finding(s) (231 files; DEAD = listed in dead-script candidates)
- .agents/skills/compress/scripts/__init__.py	F21,F23
- .agents/skills/compress/scripts/__main__.py	F21,F27
- .agents/skills/compress/scripts/cli.py	F1,F16,F21,F27
- .agents/skills/compress/scripts/validate.py	F2
- .claude/workflows/dealer-discovery.js	F11
- backend/__init__.py	F21,F23
- backend/cron/__init__.py	F21,F23
- backend/cron/scan_run_counter.py	F29
- backend/cron/sync_dealer_google_ratings.py	F6,F22,F29
- backend/cron/sync_epa_ev_ranges.py	F29
- backend/cron/sync_gas_prices.py	F23,F29
- backend/db/__init__.py	F21,F23
- backend/db/admin_users_db.py	F3,F14
- backend/db/comments_db.py	F3,F6,F22
- backend/db/dealer_geo.py	F26
- backend/db/dealer_portal_db.py	F3,F8,F14
- backend/db/dealerships_db.py	F3,F22
- backend/db/dev_users_sqlite.py	F14
- backend/db/dictionary_schema.py	F3
- backend/db/geo.py	F4,F5,F17,F20,F26
- backend/db/incomplete_listings_db.py	F3,F8
- backend/db/inventory_compat.py	F14
- backend/db/inventory_db.py	F24,F27
- backend/db/inventory_pg.py	F1,F8,F14,F27
- backend/db/repositories/__init__.py	F21,F23
- backend/db/repositories/base_repo.py	F8,F14,F18
- backend/db/repositories/cars_repo.py	F9,F10,F13,F23
- backend/db/repositories/grid_cards_repo.py	F3,F5,F17
- backend/db/repositories/listings_repo.py	F5
- backend/db/repositories/schema_repo.py	F1,F2,F14,F18
- backend/db/repositories/search_repo.py	F4,F14
- backend/db/search_analytics_db.py	F3,F14
- backend/db/user_history_db.py	F3,F14
- backend/db/users_db/__init__.py	F21,F23
- backend/db/users_db/_common.py	F8,F14
- backend/db/users_db/accounts.py	F23
- backend/db/users_db/admin.py	F9,F12,F22,F23
- backend/db/users_db/auth.py	F14,F23
- backend/db/users_db/schema.py	F1,F2,F3,F14,F23 DEAD
- backend/db/users_sqlite.py	F14
- backend/dictionary/__init__.py	F21,F23
- backend/dictionary/enrich_from_dictionary.py	F19,F23
- backend/discovery/__init__.py	F21,F23
- backend/discovery/address_enrich.py	F20,F26 DEAD
- backend/discovery/candidate.py	F16,F23
- backend/discovery/cli.py	F1,F16,F21,F27
- backend/discovery/coordinate_enrich.py	F20
- backend/discovery/dmv/__init__.py	F21,F23
- backend/discovery/dmv/registry.py	F4,F12,F13,F25 DEAD
- backend/discovery/dmv/schema.py	F1,F2,F3,F14,F23 DEAD
- backend/discovery/dmv/states/__init__.py	F21,F23
- backend/discovery/franchise_filter.py	F21 DEAD
- backend/discovery/merge.py	F22,F23
- backend/discovery/non_dealer_filter.py	F26
- backend/discovery/overture_discovery.py	F21 DEAD
- backend/discovery/pipeline.py	F11,F19,F21,F23,F24,F29
- backend/discovery/web.py	F1,F14,F16,F25,F26
- backend/mobile/__init__.py	F21,F23
- backend/mobile/contract.py	F7
- backend/reviews/__init__.py	F21,F23
- backend/reviews/store.py	F17
- backend/schemas/__init__.py	F21,F23
- backend/scraping/__init__.py	F21,F23
- backend/scraping/__main__.py	F21,F27
- backend/scraping/adjudicate_crawl.py	F21
- backend/scraping/canonical_groups.py	F21
- backend/scraping/cli.py	F1,F16,F21,F27
- backend/scraping/constants.py	F9,F10,F21
- backend/scraping/crawler.py	F21 DEAD
- backend/scraping/entity_specificity.py	F21
- backend/scraping/fetch_requests.py	F21
- backend/scraping/fixture_tests.py	F21
- backend/scraping/html_extract.py	F21
- backend/scraping/inference.py	F21
- backend/scraping/models.py	F21
- backend/scraping/org_validation.py	F21
- backend/scraping/paths.py	F14,F21,F22
- backend/scraping/redirects.py	F21
- backend/scraping/site_profile.py	F21
- backend/scraping/sources.py	F1,F21
- backend/scraping/text_utils.py	F21
- backend/scripts/__init__.py	F21,F23
- backend/scripts/analyze_trim_adds_with_web.py	 DEAD
- backend/scripts/app_users_status.py	F18
- backend/scripts/apply_ai_spec_fill.py	 DEAD
- backend/scripts/apply_attribution_moves.py	F8,F9,F26
- backend/scripts/apply_engine_spec_fill.py	 DEAD
- backend/scripts/apply_model_specs.py	 DEAD
- backend/scripts/apply_trim_ladder_diffs.py	 DEAD
- backend/scripts/attribution_batch.py	F8,F9,F10
- backend/scripts/audit_scan_coverage.py	F26 DEAD
- backend/scripts/audit_unscannable_dealers.py	 DEAD
- backend/scripts/augment_dictionary_engines.py	F19 DEAD
- backend/scripts/backfill_dealership_addresses.py	F9,F20
- backend/scripts/backfill_dealership_registry.py	F1
- backend/scripts/backfill_dealerships_from_roster.py	 DEAD
- backend/scripts/backfill_engine_l.py	 DEAD
- backend/scripts/backfill_forced_induction_pg.py	 DEAD
- backend/scripts/backfill_inactive_from_last_scan.py	 DEAD
- backend/scripts/backfill_interior_color_buckets.py	 DEAD
- backend/scripts/backfill_interior_from_vision.py	 DEAD
- backend/scripts/backfill_interior_vision.py	F18 DEAD
- backend/scripts/backfill_inventory_engines.py	F18 DEAD
- backend/scripts/backfill_listing_packages.py	F18 DEAD
- backend/scripts/backfill_missing_trim_adds.py	 DEAD
- backend/scripts/backfill_mpg_from_source_url.py	F18 DEAD
- backend/scripts/backfill_options_year_gaps.py	F19 DEAD
- backend/scripts/backfill_package_registry.py	F13
- backend/scripts/backfill_specs_from_structured_sources.py	 DEAD
- backend/scripts/backfill_transmission.py	F18 DEAD
- backend/scripts/backfill_vehicle_specs.py	 DEAD
- backend/scripts/backfill_window_stickers.py	F18 DEAD
- backend/scripts/build_dictionary_manifest.py	F19
- backend/scripts/build_dictionary_options.py	F19 DEAD
- backend/scripts/build_synth_manifest.py	F26 DEAD
- backend/scripts/build_trim_spec_sheets.py	F9
- backend/scripts/car_data_scraper.py	F19 DEAD
- backend/scripts/cascade_recipes.py	 DEAD
- backend/scripts/classify_attribution.py	F9
- backend/scripts/classify_dealers.py	 DEAD
- backend/scripts/clean_existing_manifest.py	 DEAD
- backend/scripts/clean_gallery_urls_in_db.py	F18 DEAD
- backend/scripts/clean_vehicle_csvs.py	F19 DEAD
- backend/scripts/data_quality_invariants.py	F9,F12,F25
- backend/scripts/dealer_group_copyright.py	F21,F27 DEAD
- backend/scripts/dealer_pipeline.py	F9,F11,F16
- backend/scripts/dictionary_rebuild.py	F19 DEAD
- backend/scripts/discover_dealerships.py	F27
- backend/scripts/discover_platforms.py	 DEAD
- backend/scripts/discovery_merge_to_manifest.py	 DEAD
- backend/scripts/discovery_probe.py	F9,F11,F16,F21
- backend/scripts/discovery_source_coverage.py	F8,F26 DEAD
- backend/scripts/drop_dead_kbb_columns.py	 DEAD
- backend/scripts/drop_dead_zip_code_column.py	 DEAD
- backend/scripts/e2e_deep_browser.py	F15 DEAD
- backend/scripts/e2e_smoke_browser.py	F15 DEAD
- backend/scripts/feed_package_registry_from_brochure_vision.py	F13
- backend/scripts/feed_package_registry_from_car_image_vision.py	F13
- backend/scripts/feed_package_registry_from_vision.py	F9,F10,F13
- backend/scripts/fetch_keffer_interior_colors.py	 DEAD
- backend/scripts/fetch_oem_brochures.py	F7,F8,F9,F19,F28
- backend/scripts/fill_incomplete_listings.py	 DEAD
- backend/scripts/fill_options_from_research.py	F19 DEAD
- backend/scripts/fingerprint_providers.py	 DEAD
- backend/scripts/fix_bmw_trims.py	 DEAD
- backend/scripts/geocode_dealers.py	F20
- backend/scripts/heal_matcher_trims.py	F9,F12
- backend/scripts/image_analyzer.py	 DEAD
- backend/scripts/image_batch.py	F8,F9,F10,F13,F23
- backend/scripts/image_downloader.py	F18
- backend/scripts/import_brochures_to_dictionary.py	F19 DEAD
- backend/scripts/import_dealer_google_ratings.py	F6
- backend/scripts/import_epa_to_dictionary.py	F19
- backend/scripts/migrate.py	F1,F25
- backend/scripts/migrate_dealer_portal_sqlite_to_postgres.py	F14
- backend/scripts/migrate_dictionary_layout.py	F19
- backend/scripts/migrate_placeholder_nulls.py	 DEAD
- backend/scripts/migrate_recipe_aliases.py	 DEAD
- backend/scripts/migrate_users_sqlite_to_postgres.py	F14
- backend/scripts/normalize_complete_options_schema.py	F19
- backend/scripts/normalize_transmission_inventory.py	 DEAD
- backend/scripts/parse_listing_descriptions.py	 DEAD
- backend/scripts/populate_model_specs.py	F18 DEAD
- backend/scripts/probe_oem_discovery.py	F28 DEAD
- backend/scripts/prune_car_options_csv_passenger.py	F19 DEAD
- backend/scripts/recheck_msrp.py	F8,F9,F10
- backend/scripts/refetch_colors.py	 DEAD
- backend/scripts/refetch_descriptions.py	 DEAD
- backend/scripts/reindex_vectors.py	F27 DEAD
- backend/scripts/remove_dummy_vin_cars.py	 DEAD
- backend/scripts/rename_catalog_tables.py	 DEAD
- backend/scripts/report_dealer_scores.py	 DEAD
- backend/scripts/report_inventory_signals.py	 DEAD
- backend/scripts/report_rooftop_refusals.py	F8,F26
- backend/scripts/reset_app_user_password.py	F18
- backend/scripts/reset_db.py	F24 DEAD
- backend/scripts/resolve_photo_attribution.py	F26
- backend/scripts/rooftop_refusals_census.py	F8
- backend/scripts/scan_lab_report.py	F9
- backend/scripts/scanner_liveness.py	F9,F24
- backend/scripts/set_dealership_active.py	F8
- backend/scripts/trace_car_vin.py	 DEAD
- backend/scripts/trim_coverage_report.py	F8,F9
- backend/scripts/unstale_host_blocked_recipes.py	F9,F11
- backend/scripts/verify_brochure_vision_facts.py	F9
- backend/scripts/verify_decode_trim.py	 DEAD
- backend/scripts/verify_trim_citations.py	F9
- backend/vector/__init__.py	F21,F23
- backend/vector/__main__.py	F21,F27
- backend/vector/pgvector_service.py	F8,F23,F27
- deploy/build.sh	F25 DEAD
- deploy/car-scanner/build-push.sh	F25 DEAD
- deploy/load-vault-env.sh	F25
- deploy/nightly_data_quality_invariants.sh	F12
- deploy/nightly_http_refresh.sh	F16,F25
- deploy/railway/deploy_scanner_nightly.sh	F25
- deploy/railway/push-dealer-secrets-to-vault.py	F25 DEAD
- deploy/railway/sync-vault-to-railway.sh	F25
- deploy/restart.sh	F25
- deploy/up.sh	F1,F14,F25
- discovery.py	F11,F16,F20,F21,F23,F26,F27 DEAD
- gunicorn.conf.py	F25
- post_scan.py	F1,F16,F19,F23,F29 DEAD
- run.py	F8,F11,F14,F15,F16,F20,F21,F25,F29
- run_city_discovery.py	F16,F27 DEAD
- run_dealership_pipeline.py	F16,F27 DEAD
- run_nationwide_discovery.py	F16,F27 DEAD
- scanner.py	F1,F9,F11,F16,F18,F19,F22,F25,F29 DEAD
- scripts/audit_inventory_coverage.py	F18,F25 DEAD
- scripts/bootstrap_site_admin.py	F25
- scripts/build_static_compressed.py	F25
- scripts/bump_version.sh	F25
- scripts/continue_92694_mac_mini.sh	F25 DEAD
- scripts/create_demo_free_user.py	F18,F25
- scripts/discover_metro.py	F25,F26 DEAD
- scripts/docker-entrypoint-web.sh	F1,F25
- scripts/e2e_user_audit.py	F15,F16,F25 DEAD
- scripts/railway_scan_fleet.sh	F11,F25
- scripts/refresh_lease_matches.py	F25
- scripts/run-web-local.sh	F16,F25
- scripts/scan_92694_mac_mini.sh	F25 DEAD
- scripts/scan_dealer_specials.py	F25
- scripts/scan_irvinebmw_test.sh	F16,F25
- scripts/scanner_import_sweep.py	F25
- scripts/scanner_scheduler_loop.py	F25
- scripts/scanner_worker_loop.py	F18,F25 DEAD
- scripts/security_check.sh	F25
- scripts/seed_dealer_catalog.py	F25
- scripts/start-always-on.sh	F25 DEAD
- search_dealer_addresses.py	F16,F20 DEAD
- start.sh	F25 DEAD

---------------------------------------------------------------------------------------------------------------------------

## RANKED TABLE

| # | Pri | Target | Size | Problem | Fix (short) | Blast radius |
|---|-----|--------|------|---------|-------------|--------------|
| F1 | P1 | db/inventory_pg.py::init_postgres_inventory | 477 L (L287-764) | 2nd copy of schema, all 17 tables also in migrations/; caused 07-30 boot lock pile-up | replace with "migrations at head?" check; move any missing cols to V025 | web boot, scanner cli/database, post_scan |
| F2 | P1 | db/repositories/schema_repo.py::init_inventory_db | 188 L | 3rd (SQLite) schema copy used only by tests | move SQLite DDL to test fixtures or test on PG | whole test suite |
| F8 | P1 | 25 script `_dsn()` + ~8 `_connect()` copies | ~10 L each | divergent DSN/.env/autocommit semantics; mini .env wrong-DB risk | one backend/db/connect.py; delete copies | 25 scripts, no libs |
| F9 | P1 | scripts used as libraries (image_batch, classify_attribution, verify_trim_citations, dealer_pipeline, data_quality_invariants) | — | private `_name` cross-imports; runtime mirrors constants by hand | promote to backend/enrichment/*, backend/scanner/pipeline | ~12 scripts, ~30 test imports |
| F10 | P1 | scripts/image_batch.py::cmd_record | 285 L (L781-1066) | MSRP write-gates inline in loop; readers can't import them | pure sticker_gates.evaluate(); images.py | 3 scripts, 4 tests |
| F11 | P1 | scripts/dealer_pipeline.py | 1394 L, main 213 | prod fleet entry + reconcile/assess core in a CLI | backend/scanner/pipeline/{roster,recipes,runner,assess,lifecycle,dealer_logs,triage} | Railway fleet, 12 tests |
| F7 | P1 | scripts/fetch_oem_brochures.py | 2525 L, main 425 | 7 subsystems; enrichment depends on its helpers | brochure_sources/{gaps,resolve,download,corpus,quarantine_ops,html_specs} + subcommands | 1 test file, CLI flags |
| F14 | P1 | SQLite vs PG dual paths (users, dev users, portal, inventory compat) | — | users schema in SQLite + unused PG copy (V013, 0 rows) | execute CLOUD_RESTRUCTURE_PLAN §4.2 | auth, tests |
| F18 | P1 | 9 inventory.db backfills + image_downloader + worker sqlite sync | ~2,300 L | broken vs PG-only inventory; browser use | delete / port | none (0 refs) |
| F24 | P1 | scripts/reset_db.py | 62 L | `DELETE FROM cars`, no prod/scanner guard, 0 refs | delete | none |
| F4 | P2 | db/repositories/search_repo.py::search_cars | 415 L | 35-arg SQL builder+post-filter+geo+paging | SearchFilters + build_where + post_filter + geo_hydrate | nearby_dealers, 10 tests |
| F5 | P2 | db/repositories/listings_repo.py::_build_filter_options_uncached | 257 L | 4 facet queries + ladders in one fn; module also owns grid cache | per-facet fns; move grid cache out | listings page |
| F6 | P2 | db/comments_db.py | 1055 L | DDL + comments + attachments + dealer ratings | split 3 modules; drop DDL | community routes, dealer_ratings |
| F3 | P2 | runtime DDL in 10 more db modules | 27+11+7+... stmts | schema duplicated per domain | one migrated-at-head assert | boot |
| F13 | P2 | 4 package-registry feeders | 551+130+126+112 | 2 differ by 10 lines; private imports | one feeder, --source adapters | manual ops |
| F16 | P2 | root entry points | — | 3 dead (run_dealership_pipeline, run_city_discovery, search_dealer_addresses); sys.path hack | delete; keep scanner.py | none |
| F19 | P2 | dictionary CSV builder cluster | 13 files ~3,700 L | frozen May-June, Playwright, 0 refs | archive/delete except import_epa_to_dictionary | none |
| F20 | P2 | discovery/address_enrich.py vs scripts/backfill_dealership_addresses.py | 437 + 887 | same job twice, both with browser | delete address_enrich; extract extractors | 2 tests |
| F21 | P2 | backend/scraping crawler half | ~2,500 L | reachable only from own CLI; bare-import bug cli.py:285 | split core/crawler; delete crawler | dealer_group_copyright.py |
| F22 | P2 | db/dealerships_db.py::upsert_discovery_row | 185 L | dual-dialect INSERT; ratings stored by 2 paths | registry/counters/ratings modules | 25 importers |
| F25 | P2 | scripts/ + deploy/ shell | — | 2 scripts call missing scanner_mac_mini.py; start-always-on runs Werkzeug publicly | delete | none |
| F12 | P3 | scripts/data_quality_invariants.py | 1394 L | hub mirrors statuses by hand | backend/data_quality/invariants.py | nightly + hub |
| F15 | P3 | 3 browser E2E scripts | 540+359+226 | unreferenced, SQLite-users bound | pytest-playwright or delete | none |
| F17 | P3 | db/repositories/grid_cards_repo.py | 1008 L | cache rev hashes function source text | explicit rev + golden test; split 3 | listings hot path |
| F23 | P3 | long linear pipelines (run_discovery 200, enrich_car 154, ...) | — | length only | split when touched | — |
| F26 | P3 | host/haversine helper copies | 52 host fns | subtly different host keys | backend/utils/hosts.py | many |
| F27 | P3 | duplicate CLI wrappers (discovery x3, vector reindex x2) | — | drift | keep one each | none |
| F28 | P3 | scripts/probe_oem_discovery.py | 1092 L | one-shot recon, 0 refs | archive | none |
| F29 | P3 | backend/cron/ | 4 files | not scheduled anywhere | rename; data to JSON | 4 importers |

## P1 / P2 DETAILS
See sections F1-F27 above (each has file::function, line ranges, responsibilities, why, concrete split, blast radius).
Suggested order: F24 (delete, 1 file) -> F18/F25/F16 deletions (no refs) -> F8 (mechanical, removes wrong-DB risk) ->
F9+F10 (promote gates/helpers; tests exist) -> F11 (pure move behind re-exports) -> F7 -> F1/F2/F3 together with the
test-DB decision -> F14 (users cutover, already planned).

## DEAD-SCRIPT CANDIDATES (evidence: `git grep -w <stem>` across all tracked files = no py/ops/test refs; date = last commit)
Delete now (broken or dangerous):
- backend/scripts/reset_db.py — 0 refs, 07-07; DELETE FROM cars without prod guard (F24)
- scripts/scan_92694_mac_mini.sh, scripts/continue_92694_mac_mini.sh — 0 refs, 06-08; exec missing scanner_mac_mini.py
- scripts/start-always-on.sh — 0 code/ops refs (1 doc), 05-30; public Werkzeug
- scripts/scanner_worker_loop.py::_sync_dealer_sqlite_to_postgres (function) — reads scanner.js output; scanner.js gone;
  CLOUD_RESTRUCTURE_PLAN.md:164,367 "Delete ... verified safe"
- SQLite inventory.db scripts (sqlite3.connect(INVENTORY_DB_PATH)): backfill_transmission (05-03), populate_model_specs (05-03;
  1 py ref is a comment), backfill_interior_vision (05-22), backfill_inventory_engines, backfill_listing_packages,
  backfill_mpg_from_source_url, backfill_window_stickers, clean_gallery_urls_in_db (all 06-09), scripts/audit_inventory_coverage.py (06-09)
- root: run_dealership_pipeline.py (0 refs at all, 05-03), search_dealer_addresses.py (0 refs; output file read by nothing, 05-06),
  run_city_discovery.py (only a docstring mention, 05-06)
Archive (one-shots already applied / superseded):
- schema one-shots now covered by migrations/: rename_catalog_tables (06-26), drop_dead_zip_code_column (07-06),
  drop_dead_kbb_columns (07-06), migrate_placeholder_nulls (07-07), remove_dummy_vin_cars (05-03), migrate_recipe_aliases (08-20),
  backfill_inactive_from_last_scan (07-19, "One-time backfill"), backfill_forced_induction_pg (07-07), backfill_engine_l (05-03),
  backfill_specs_from_structured_sources (05-03), backfill_interior_color_buckets (05-03), backfill_vehicle_specs (05-03, "SQLite cars"),
  apply_model_specs (05-03), normalize_transmission_inventory (05-06), fill_incomplete_listings (05-06), parse_listing_descriptions (05-22),
  backfill_interior_from_vision (05-22 wrapper), image_analyzer (05-22, 406 L), verify_decode_trim (05-03), trace_car_vin (05-03)
- manifest/dealers.json era: clean_existing_manifest (05-06 "One-off scrub"), fingerprint_providers (05-06, 457 L),
  discovery_merge_to_manifest (05-03), scripts/discover_metro.py (06-09; Google Places disabled per memory)
- browser one-shots: refetch_descriptions (07-07, Playwright), car_data_scraper (05-06, Playwright), fetch_keffer_interior_colors
  (07-07, single dealer), refetch_colors (07-20, 612 L)
- dictionary CSV cluster (F19): import_brochures_to_dictionary, clean_vehicle_csvs, prune_car_options_csv_passenger,
  build_dictionary_options, augment_dictionary_engines, fill_options_from_research, backfill_options_year_gaps (+ the
  dictionary_rebuild chain if no one regenerates CSVs)
- trim research one-shots: analyze_trim_adds_with_web (06-09), backfill_missing_trim_adds, apply_trim_ladder_diffs, fix_bmw_trims (07-07),
  apply_ai_spec_fill, apply_engine_spec_fill (07-13)
- discovery analysis 08-06 single-commit: probe_oem_discovery (1092), discovery_source_coverage (824), audit_unscannable_dealers,
  discover_platforms, build_synth_manifest, cascade_recipes, classify_dealers (07-17), audit_scan_coverage (07-21),
  report_dealer_scores, report_inventory_signals (07-18), backfill_dealerships_from_roster (07-18)
- e2e: scripts/e2e_user_audit.py, backend/scripts/e2e_smoke_browser.py, e2e_deep_browser.py (06-09, doc refs only)
- modules: backend/discovery/address_enrich.py (0 importers), backend/discovery/franchise_filter.py (0 importers; duplicate of
  overture_discovery.is_franchised_dealer), backend/scraping crawler half + scripts/dealer_group_copyright.py (F21),
  backend/scripts/reindex_vectors.py (dup of python -m backend.vector)
- deploy: deploy/car-scanner/build-push.sh (05-06, Mac-mini registry), deploy/railway/push-dealer-secrets-to-vault.py (06-11, one-shot)
Confirm-with-owner (live-looking but only k8s refs): post_scan.py, run_nationwide_discovery.py (sole callers in deploy/k8s/*.yaml,
last touched 08-06; prod is Railway).

## CHECKED, FINE (92 files — no structural finding; some carry a one-line note in the 'Scripts triage' sections)
Notes: .agents/skills/compress/* = vendored agent tooling (caveman compress), not app code, 0 refs — fine/out of scope;
backend/db/repositories/{attribution,data_quality,dealers,hidden_dealers,saved_cars,saved_searches,search_history}_repo.py = thin,
<=262 lines, longest fn <=100; backend/schemas/* = small dataclasses used by intelligence pipeline; backend/discovery/* tiers
(osm, google_places, web_city, normalize, zcta_gazetteer, manifest_merge, dmv/*) = live, used by discovery cli/pipeline with tests;
backend/vector/{catalog_service,ingest_master_specs,listings_semantic}.py live (pgvector_service importers); tools/ocr/macocr.swift used
by run_image_text_extraction + backend/vision/image_text.py; install_scraper_browsers.sh referenced by requirements.txt:32.

- .agents/skills/compress/scripts/benchmark.py
- .agents/skills/compress/scripts/compress.py
- .agents/skills/compress/scripts/detect.py
- backend/db/password_hash.py
- backend/db/repositories/attribution_repo.py
- backend/db/repositories/data_quality_repo.py
- backend/db/repositories/dealers_repo.py
- backend/db/repositories/hidden_dealers_repo.py
- backend/db/repositories/saved_cars_repo.py
- backend/db/repositories/saved_searches_repo.py
- backend/db/repositories/search_history_repo.py
- backend/db/users_db/billing.py
- backend/dictionary/color_extract.py
- backend/dictionary/epa_engine.py
- backend/discovery/dmv/states/generic_csv.py
- backend/discovery/dmv/states/nc.py
- backend/discovery/google_place_rating.py
- backend/discovery/google_places.py
- backend/discovery/manifest_merge.py
- backend/discovery/normalize.py
- backend/discovery/osm.py
- backend/discovery/web_city.py
- backend/discovery/zcta_gazetteer.py
- backend/reviews/guards.py
- backend/schemas/adjudication_result.py
- backend/schemas/dealership.py
- backend/schemas/evidence_package.py
- backend/schemas/run_summary.py
- backend/scraping/interrupt.py
- backend/scripts/add_dealership.py
- backend/scripts/analyze_brochure_with_llm.py
- backend/scripts/attribute_feed_rooftops.py
- backend/scripts/audit_recipe_coverage.py
- backend/scripts/audit_trim_ladders.py
- backend/scripts/backfill_extended_specs.py
- backend/scripts/backfill_mpg_from_epa.py
- backend/scripts/backfill_specs.py
- backend/scripts/backfill_vpic_cache.py
- backend/scripts/build_brochure_text_slim.py
- backend/scripts/build_brochure_trim_candidates.py
- backend/scripts/build_epa_master_pg.py
- backend/scripts/build_listings_grid_cards.py
- backend/scripts/build_trim_ladders.py
- backend/scripts/build_trim_ladders_from_brochure.py
- backend/scripts/build_trim_ladders_from_epa.py
- backend/scripts/build_trim_msrp_bands.py
- backend/scripts/compute_market_stats.py
- backend/scripts/db_admin.py
- backend/scripts/delete_brochure_pdfs.py
- backend/scripts/dump_baseline_schema.py
- backend/scripts/extract_brochure_text.py
- backend/scripts/fingerprint_timing.py
- backend/scripts/fix_extended_spec_outliers.py
- backend/scripts/fleet_scan.py
- backend/scripts/harvest_carscommerce.py
- backend/scripts/harvest_html_jsonld.py
- backend/scripts/heal_cylinders_from_vpic.py
- backend/scripts/heal_ev_cylinders.py
- backend/scripts/heal_from_recipes.py
- backend/scripts/heal_from_vpic.py
- backend/scripts/heal_stock_code_contamination.py
- backend/scripts/import_epa_master.py
- backend/scripts/import_recipes_to_db.py
- backend/scripts/install_scraper_browsers.sh
- backend/scripts/link_cars_to_catalog.py
- backend/scripts/local_brochure_text_cache.py
- backend/scripts/local_brochure_vision_extract.py
- backend/scripts/local_car_image_vision_extract.py
- backend/scripts/merge_ep_batch.py
- backend/scripts/merge_trim_ladders.py
- backend/scripts/migrate_inventory_sqlite_to_postgres.py
- backend/scripts/national_scan.py
- backend/scripts/platform_candidates.py
- backend/scripts/process_brochure_queue.py
- backend/scripts/promote_trim_candidates.py
- backend/scripts/quarantine_package_value_msrps.py
- backend/scripts/rebuild_listings_index.py
- backend/scripts/recover_incomplete_listings.py
- backend/scripts/recover_team_velocity_images.py
- backend/scripts/reingest_brochures.py
- backend/scripts/repair_inventory_fields.py
- backend/scripts/repair_vin_make_model.py
- backend/scripts/run_image_text_extraction.py
- backend/scripts/synthesize_recipes.py
- backend/scripts/validate_trim_overlays.py
- backend/vector/catalog_service.py
- backend/vector/ingest_master_specs.py
- backend/vector/listings_semantic.py
- deploy/nightly_rooftop_refusals.sh
- scripts/docker-entrypoint-scanner-scheduler.sh
- scripts/docker-entrypoint-scanner-worker.sh
- tools/ocr/macocr.swift
