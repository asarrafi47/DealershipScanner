# Test-suite structure audit (read-only) — 2026-10-01

Scope: 306 files (conftest.py + 305 under backend/tests). Branch feature/http-only-scans.

## Running notes (appended as work progresses)

### F1 conftest layering cancels the prod-DB guard (P1)
- /conftest.py:116 `_isolate_listings_env` does `monkeypatch.setenv("INVENTORY_DATABASE_URL", "")`
  with a comment that delenv is wrong (dotenv refills a missing key, keeps an empty one).
- backend/tests/conftest.py:57-62 `_inventory_sqlite_tests_mode` (autouse, dates from 2026-06-14 / 08-06)
  then runs `monkeypatch.delenv("INVENTORY_DATABASE_URL")` and `delenv("DATABASE_URL")`.
  Autouse fixtures run outer-conftest first, so the backend one runs LAST and wins: the var is
  DELETED, not blank.
- backend/main.py:5 calls `load_project_dotenv()` (override=False) at import; every
  `importlib.reload(backend.main)` therefore re-fills INVENTORY_DATABASE_URL / DATABASE_URL from
  .env (.env has 1 of these keys). Any reload-main test that does not set the var itself talks to
  whatever .env points at (prod via tunnel on the mini).
- Fix: delete `_inventory_sqlite_tests_mode` (root fixture already covers it) or change it to
  setenv("") for both keys; add one assertion fixture that fails if os.environ has a non-empty
  postgres URL at test call time.
- Confirmed consequence chain: backend/db/inventory_pg.py:71 `inventory_postgres_dsn()` reads os.environ
  at CALL time; backend/main.py:5 load_project_dotenv(); main.py:350-356 run init_users_db /
  init_admin_db / init_inventory_db / init_dealer_portal_db / init_job_queue_schema AT IMPORT.
  backend/db/repositories/schema_repo.py:262 init_inventory_db -> on Postgres runs
  init_postgres_inventory (DDL) + seed_cars(). .env on this MBP: INVENTORY_DATABASE_URL=postgresql:///cars
  (local socket DB). So every importlib.reload(main) in a test that does not itself set
  INVENTORY_DATABASE_URL="" re-asserts schema + seeds against the developer's real `cars` DB.
- Exposure (python scan of 47 files that import backend.main):
  reload-main files that do NOT set INVENTORY_DATABASE_URL: 20 of 22 (only test_admin_operator_api,
  test_admin_users set it; test_production_security sets none). List: test_apple_oauth, test_audit_fixes,
  test_billing_gate, test_debug_audit, test_dev_ip_allowlist, test_google_oauth, test_init_admin_db,
  test_login_session_fixes, test_mfa_action_log, test_mfa_qr, test_mfa_totp, test_mobile_api_contract,
  test_mobile_auth_api, test_mobile_auth_register, test_nearby_dealers_api, test_premium_checkout,
  test_production_security, test_registration_email, test_saved_cars_api, test_ux_premium_gates.
- Same mechanism, users side: .env sets DEV_USERS_DB_PATH, APP_ADMIN_USERNAMES, SECRET_KEY,
  GOOGLE_OAUTH_*. Reload-main files that never set DEV_USERS_DB_PATH: test_dev_ip_allowlist.py,
  test_mfa_action_log.py, test_saved_cars_api.py -> reload uses the developer's real dev-users DB path
  (backend/db/dev_users_sqlite.py, admin_users_db.py). Many _fresh_app's `monkeypatch.delenv(...)` of
  ADMIN_*/USERS_DB_ENCRYPTION_KEY are no-ops for any key present in .env (dotenv refills on reload).
  APP_ADMIN_USERNAMES from .env leaks into every reload test (only test_billing_gate sets it) -> results
  differ between a dev box and CI.
- Priority P1. Fix = one conftest fixture `fresh_main(monkeypatch, tmp_path, **env)` that setenv("")s
  every key in .env's key set (read names from dotenv_values) before reload, then sets tmp paths;
  or make load_project_dotenv a no-op when PYTEST_CURRENT_TEST / INVENTORY_SQLITE_TESTS is set.
- CORRECTION: exposure is 22 of 22 reload-main files. test_admin_users.py:_fresh_app and
  test_admin_operator_api.py "set" the URL with monkeypatch.delenv("INVENTORY_DATABASE_URL") right
  before importlib.reload(main) -> refilled from .env. test_ux_premium_gates.py:_fresh_app even carries
  the comment "setenv (never delenv): delenv + module reload re-populates from .env" but never blanks
  the inventory URL.
- WORSE, at COLLECTION time: 9 modules import backend.main at module top level
  (test_car_detail_api:5, test_comment_attachments:33, test_car_nhtsa_recalls_api:5,
  test_dealer_onboard_api:9, test_dealership_page:15, test_community_api_routes:20, test_fuel_lookup:10,
  test_listings_perf:11, test_not_found_page:5). Collection runs before any fixture; root conftest only
  sets ALLOW_UNENCRYPTED_USER_DB; pytest.ini has no env section. So first collection of any of these
  runs main.py:5 dotenv + main.py:350-356 init_* with .env values: USERS_DB_PATH/DEV_USERS_DB_PATH (real
  user DBs) and INVENTORY_DATABASE_URL=postgresql:///cars (schema re-assert + seed_cars on the real
  local DB). Even `pytest --collect-only` does it. (Inferred from code; deliberately NOT executed.)
  backend/config.py:30 also calls load_project_dotenv() at import.
- Fix (P1): in /conftest.py at module import (before collection) blank every key from
  dotenv_values(".env") that names a DB/secret (INVENTORY_DATABASE_URL, DATABASE_URL, *_DB_PATH,
  SECRET_KEY, APP_ADMIN_USERNAMES, GOOGLE_OAUTH_*) with os.environ[k]=""; plus move the 9 top-level
  `from backend.main import app` into a shared `app` fixture.

### F2 the 30 xfails (P3, mostly fine)
- All 30 come from backend/tests/test_rooftop_corpus.py:72-82 `_ideal_params()`: 15 corpus scenarios
  carrying `expected_ideal` (fixtures/rooftop_corpus/*.json) x 2 MODES ("legacy","scorer") = 30, each
  `xfail(strict=True)`. Logs: "337 passed, 30 xfailed" (fullsuite_final2.log, fullsuite_release.log).
- Reasons are documented known-open gate rules: identity-feed == page oem_code (mallofgamazda,
  stevensonhendrickhonda, group1toyotanorthaustin, tuttoncdjr), Location facet names the store
  (grandhyundaicanton, group1fordofsouthaustin, group1toyotanorthaustin), own street block
  (hendrickhonda, hondahuntersville), roster city in feed name (shottenkirkchrysler), group car
  (lexusofknoxville), merged identity (hendrickbuickgmccary, greenwaycdjrofrome).
- Strict xfail = a fix flips to XPASS-fail, which is the right design. Weaknesses:
  (a) count is doubled by MODES: once the legacy ladder is retired, half are noise;
  (b) one note is stale in substance: mallofgamazda "alias deleted from the live hint store; store-scoped
      recipe now bypasses the gate" - the real-world case is resolved outside the gate, so the xfail
      tracks a gate rule nobody plans to add. Fix: drop `expected_ideal` where production bypasses the
      gate, or tag notes with an issue id. P3.

### F3 the skips (P2)
- Skip sites: test_brochure_extract.py (43,52,138,163,288,315,359 + importorskip fitz 75,381),
  test_dictionary_catalog.py (100,111,140,169,181), test_trim_ladder.py (3187,3330,3538,3563,3566,3697 +
  importorskip pdfplumber/fitz), test_verify_trim_citations.py (183-308), test_data_quality_invariants.py:441
  (DQ_INVARIANTS_LIVE_DB), test_listings_indexes.py:19,44 (skip when Postgres), test_scraper_chain.py:313,330
  (bs4), test_dealership_discovery.py:92 (duckdb), test_brochure_sources.py:1554 (pypdf).
- "3 skipped" run (log with "1 failed, 1018 passed, 3 skipped"): 2x 2016 Grand Cherokee brochure PDF
  (backend/data/brochures/ is gitignored, .gitignore:95; the PDF IS present on this MBP now, so that run was
  from a checkout without the corpus) + 1x DQ live-DB test. The 9-skip variant adds RAV4/Passat PDFs and
  the catalog db.
- Asset-gated accounting in backend/tests/conftest.py:66-150 is good (counts, prints, REQUIRE_LOCAL_ASSETS=1
  escalates). Gaps:
  (a) .github/workflows/ci.yml never sets REQUIRE_LOCAL_ASSETS, and CI has no corpus -> these ~10 golden
      tests run nowhere automatically; only a dev who remembers the env var runs them.
  (b) test_data_quality_invariants.py:441 live-Postgres test: DQ_INVARIANTS_LIVE_DB is never set in CI or
      any script -> the only Postgres-backed invariant test has, as far as the repo shows, never run.
  (c) CI job `pytest-integration` (ci.yml:56-74) runs `-m integration` and tolerates exit 5; ZERO test files
      use mark.integration or mark.regression (grep 0/303) and conftest INTEGRATION_MODULES/REGRESSION_MODULES
      are empty -> a CI job that always collects nothing.
  Fix: either delete the integration job or tag the live-DB/PDF tests `integration` and run that job on a
  runner with a Postgres service + corpus (or a cached artifact). P2.

### F4 giant test files mixing units (P2)
- test_trim_ladder.py: 4460 lines, 200 tests. Imports trim_ladder_knowledge (40 import lines),
  trim_ladder (31), enrichment (20), brochure_extract (18), dictionary_paths (8),
  scripts.verify_trim_citations (6), backend.main (3: Flask route tests at :909-:963 via app.test_client),
  trim_spec_extractor, dictionary_catalog, inventory_db. Self-declared sections: rung match (1-1775),
  "what this trim adds" ranking (1775), bullet well-formedness (2248), encyclopedia contamination (2422),
  provenance gate (2663), every-store gate (3002), epa_csv revoked (3114), EPA file identity (3266),
  build-time verifier re-opening PDFs (3476), rung-name gate (3786), rung order provenance (4268).
  The source is ALREADY split: backend/enrichment/trim_ladder/ (19 modules: adds_filter, attribution,
  bullets, citations, claims, csv_ladders, document_order, epa, evidence, plausibility, selection,
  sticker_diffs...) and trim_ladder_knowledge/ (10 modules). The test file did not follow.
  Order/global-state hazard: autouse `_legacy_rung_gate_off` (:42-46) sets TRIM_RUNGS_REQUIRE_PROVENANCE=0
  for every test unless it requests `rung_gate_on` -> the first 3786 lines test a non-default config,
  which a reader cannot see per test.
  Fix: backend/tests/trim_ladder/ package with conftest.py holding the gate fixture; one file per
  section above (~11 files, 200-600 lines each); move the 2 Flask route tests to the car-detail route tests;
  move the verifier tests (3476-3785) next to test_verify_trim_citations.py. P2.
- test_brochure_sources.py: 2386 lines, 154 tests; covers brochure_sources + html_spec_sources +
  scripts.fetch_oem_brochures + dictionary_catalog/paths + brochure_extract; name clusters (quality_gate x7,
  archive, pdf_filename, page_scoped, official_host, identity_accepts, content_index) = natural split lines.
  P3 (one main unit, but split html_spec_sources + fetch_oem_brochures script tests out).
- test_brochure_extract.py: 1082 lines, 37 tests; real target is mostly brochure_trim_candidates (10)
  + brochure_extract (6) + trim_spec_extractor + trim_ladder; carries 7 asset-gated skipifs (see F3). A
  separate test_brochure_trim_candidates.py already exists -> move those 10 there. P3.
- test_recipe_synth.py: 1039 lines, 66 tests, single unit (backend.scanner.recipes). Large but coherent. Fine/P3.

### F5 tests coupled to template/JS/CSS source text (P2)
- 12 test files open frontend/templates or frontend/static source and grep it (not rendered output).
  Worst: test_visual_review_mobile_history_recalls.py (11 source reads; asserts exact CSS
  `body:has(.app-sidebar) {sel}` :26, literal `height: min(56vw, 340px);` :34, Jinja expression text
  `lookup_error not in ('invalid_vin', 'missing_vin')` :60), test_condition_mileage_location.py:66-77
  (exact JS lines like `if (cardMileageNotListed(c)) return Infinity;`), test_payment_listed_prices.py:120-126
  (`c.payment_listed === true`, Jinja `car.price > 0 and not car.payment_listed`),
  test_compare_features_transmission.py:119 (`<span class="compare-diff-tag">differs</span>`),
  test_horsepower_hybrid_guard.py:91-92, test_msrp_trust.py:662-663, test_first_seen_line.py:50-61.
- Rendered-HTML class-name asserts: test_search_history.py:495 asserts the full class attribute
  `class="secondary-button account-profile__history-btn account-profile__history-save"`;
  test_home_dashboard_routes.py:112-114 `reco-track`/`saved-section`/`find-cars-card--banner`, `hub-page`,
  `dash-hub-stats` (all still present today, but HEAD d81189f8d "Remove every card, tile, pill and boxed
  panel" is exactly the kind of change that breaks these); test_hidden_dealers.py:348,353 `data-hidden`
  (behavioural hook, acceptable).
- Negative source asserts (`"market-velocity-card" not in html`, test_first_seen_line.py:56) pass forever
  after any rename, so they guard nothing.
- Why it hurts: a pure refactor (rename a JS function, reformat a CSS rule, reword a Jinja condition)
  fails tests while real behaviour regressions (wrong value rendered) pass. Fix: render the template
  through the Flask client with a seeded car and assert on visible text / data-testid attributes;
  for JS logic, test via the server-side serializer field it reads (payment_listed, mileage_not_listed)
  instead of grepping source; keep test_csp_self_hosted_fonts.py (a policy scan over all files is a
  legitimate source check). P2.

### F6 tests that read/write the shared dev databases instead of a tmp DB (P1/P2)
- Root conftest `_isolate_listings_env` points inventory_db.DB_PATH at `_default_inventory_db_path()`
  = backend/inventory.db (gitignored, exists, mtime 2026-09-30 19:31 = last suite run). That file is
  shared, mutable, carried across runs and machines -> order/machine-dependent results.
- Python scan: 23 files use inventory_db/get_conn/backend.main with NO tmp DB isolation (no DB_PATH,
  sqlite_inventory, INVENTORY_DB_PATH, :memory:): test_ai_chat_bp, test_app_security_basics,
  test_car_chat_policy, test_car_detail_api, test_car_nhtsa_recalls_api, test_charger_daytona_specs,
  test_compare_and_stats, test_dealer_locator, test_dealership_address_backfill, test_dealership_page,
  test_dev_operator_premium, test_dummy_vin_cleanup, test_filter_options, test_fleet_roster_rule,
  test_fuel_lookup, test_generated_spec_sheet, test_health_ready, test_local_llm_assistant,
  test_manifest_roster, test_not_found_page, test_rooftop_address_provenance, test_trim_ladder_audit,
  test_window_sticker_scan. Several monkeypatch get_conn/db_conn with fakes (dealership_address_backfill,
  fleet_roster_rule, rooftop_address_provenance) = fine; the rest read real rows.
- Worst cases:
  * test_charger_daytona_specs.py:11-25 - looks up car id 3608 in whatever DB is live; `if not car: return`
    and `if not path ... : return` -> passes vacuously on CI/clean clones; when the row exists it calls
    `_analyze_local_sticker_pdf(3608, ...)` which WRITES to that DB. P1 (vacuous + writes real data).
  * test_filter_options.py:13-30 asserts "no case-variant trims" over get_filter_options() of the shared DB:
    vacuous on an empty DB, data-dependent on a populated one. P2.
  * test_compare_and_stats.py:28 `record_compare_session(999001, ...)` with no USERS_DB_PATH override ->
    backend/db/users_sqlite.py:34 resolves os.environ USERS_DB_PATH, which collection-time dotenv (F1)
    set to the developer's real users.db -> inserts a fake uid row into the real users DB. P1 via F1.
- Silent early `return` instead of pytest.skip (bypasses the ASSET-GATED SKIPS accounting and
  REQUIRE_LOCAL_ASSETS): test_charger_daytona_specs.py:13,20; test_brochure_promote.py:29,40;
  test_brochure_trim_candidates.py:19 (the latter files are tracked under backend/dictionary/derived/
  brochure_text, so they do run today; the return just hides it the day they move). Fix: pytest.skip with
  a reason from _ASSET_GATE_SKIP_REASONS, or seed via sqlite_inventory. P2.
- Fix overall: make root `_isolate_listings_env` point DB_PATH at a per-session tmp copy
  (tmp_path_factory) instead of backend/inventory.db; require `sqlite_inventory` for any test needing rows.

### F7 copy-pasted app factories / fixtures that belong in conftest (P1 cluster A, P2 others)
Cluster counts (python AST/regex scan over 303 test files):
- A. `def _fresh_app(monkeypatch, tmp_path, ...)` in 21 files + 3 more inline reload sites without the
  helper name (test_apple_oauth:21, test_debug_audit:54, test_init_admin_db:77, test_mfa_qr:22,
  test_production_security:51,62,83,118) -> 22 files / 25 `importlib.reload(main)` calls. Bodies drift:
  some set DEV_USERS_DB_PATH, 3 do not; some delenv USERS_DB_ENCRYPTION_KEY (no-op vs .env), some
  setenv(""); some set MFA_DELIVERY_MODE/RATE_LIMIT_SQLITE_PATH/ALLOW_DEFAULT_APP_USER, others don't;
  test_hidden_dealers/test_home_dashboard_routes/test_listings_search_bounded/test_search_history use the
  same name but DON'T reload (import app once, re-init DBs) -> same name, two semantics.
  USERS_DB_PATH set in 37 files (75 hits); `app.test_client()` in 53 files (174 hits).
  Cost: each reload re-executes main.py top to bottom (blueprint registration, 5 init_* DB passes,
  vault secret load at main.py:9) -> slowest per-test setup in the suite, and module identity changes
  (objects imported from backend.main before a reload are stale afterwards -> order dependence).
  Fix: conftest fixtures `app_factory(**env)` (single env baseline incl. setenv("") for every .env key,
  tmp users/dev-users/inventory/rate-limit paths) and `client`; prefer a create_app() factory in
  backend/main.py over reload. P1 (because of F1), otherwise P2.
- B. Module-level `from backend.main import app` in 9 files (list in F1) + local `client` fixture in
  6 files (ai_chat_bp, ai_narrate_bp, apple_oauth, home_dashboard_routes, listings_scoped_grid,
  local_llm_assistant). P2.
- C. Local `sqlite_inventory` fixtures SHADOW the conftest fixture of the same name with a different
  contract in 5 files: test_comments_db.py:35, test_community_api_routes.py, test_dealer_reviews.py:23,
  test_dealer_specials_store.py, test_lease_matcher.py. The local version returns the inventory_db
  module, only sets DB_PATH, and skips init_inventory_db + `_clear_inventory_derived_caches` that the
  conftest version (backend/tests/conftest.py:558) exists to do. Fix: delete the 5 copies, use the
  conftest fixture (it already exposes .add_cars). P2.
- D. Hand-rolled fake DB connection/cursor classes (`execute/cursor/fetchall/fetchone/close/__enter__/
  commit` helpers): 15 files define a `_Fake*Conn/_Conn/_Cursor` class; `execute` defined 24x, `cursor` 16x,
  `fetchall` 11x, `close` 18x across data_quality_invariants, dealer_locator, dealer_portal,
  fleet_roster_rule, generated_spec_sheet (x3), health_ready, job_queue, dealership_address_backfill,
  rooftop_address_provenance, ai_model_specs_merge, heal_stock_code_contamination, image_batch_reconcile...
  Each fakes SQL by string-matching, so the SQL itself is never executed. Fix: a shared
  `RecordingConn(rows_by_sql_prefix)` helper in backend/tests/_fakes.py, or real sqlite_inventory. P3.
- E. CSRF/login helpers: `_csrf_token` handling in 18 files; `_login/_csrf/_post_form/_client_with_session/
  _login_admin` defined in 8 files. Fix: `login(client, user)` + `csrf_post(client, url, data)` in conftest. P2.
- F. Row/payload builders `_row` (11 files), `_listing` (6), `_car` (5), `_payload` (5), `_vin` (5),
  `_rows` (5): domain-specific, acceptable per file; a `make_car(**over)` factory would cover _car/_row. P3.
- No shadowed duplicate test names within any file (AST check), only 2 assertion-free tests and both are
  intentional "must not raise" (test_job_queue.py:75, test_trim_invariant.py:288).

### F8 SQLite-only suite, Postgres-only production (P2)
- Every test runs with INVENTORY_SQLITE_TESTS=1 and a blank/deleted URL (conftest). Production refuses
  SQLite (test_inventory_postgres_required.py pins that). Only Postgres-touching tests: 17 files mention
  psycopg/pg_connect/is_inventory_postgres, all mocked (fake DSNs like postgresql://u:p@localhost/db,
  *.invalid hosts in test_health_ready.py:78-163) except test_data_quality_invariants.py:441 which needs
  DQ_INVARIANTS_LIVE_DB=1 and is never enabled (F3).
- Production code with Postgres-only branches: 41 backend modules call is_inventory_postgres() (79 call
  sites); 16 modules carry PG-dialect SQL (ON CONFLICT / SKIP LOCKED / jsonb / ILIKE / ::casts), e.g.
  db/inventory_pg.py (9), scanner/database.py (6), scanner/job_queue.py, scanner/inventory_write.py,
  db/user_history_db.py, db/comments_db.py. None of those branches executes under pytest.
- Past prod bugs this would have caught (from project memory): ON CONFLICT column ambiguity (`cars.col`),
  psycopg bare `%` in LIKE literals losing upserts, ON CONFLICT DO NOTHING dropping rows, 6x pool bug.
- test_listings_indexes.py:19,44 `skipif(is_inventory_postgres())` = SQLite index-name/EXPLAIN tests;
  they pin SQLite internals production never uses.
- 14 files hand-write their own `CREATE TABLE cars (...)` (23 files have some CREATE TABLE):
  test_assess_stamped_rows, test_comment_attachments, test_comments_db, test_community_api_routes,
  test_dealer_profile, test_incomplete_listings_index, test_inventory_db_path, test_lease_matcher,
  test_pipeline_reconcile_20260926:16, test_reconcile_condition_guard, test_rooftop_refusals_report,
  test_scanner_inventory_reconcile:44, test_upsert_vin_owner_guard:143, test_vpic_heal_derived_fields_f06.
  6-8 column schemas drift from the real one (no constraints, no indexes) so upsert/conflict logic is
  tested against a table production does not have. Fix: build from init_inventory_db (sqlite_inventory).
- Fix: add a CI job with a `postgres:16` service, run the migration chain, and mark a Postgres tier
  (`@pytest.mark.pg`, fixture `pg_inventory` creating a throwaway schema) for scanner/database.py,
  inventory_write.py, job_queue.py, comments_db, user_history_db upserts; reuse the empty
  `pytest-integration` job (F3). P2 (P1 if upsert regressions recur).

### F9 real-time sleeps + wall-clock assertions (P2)
- test_car_page_perf.py: real `time.sleep` at :264 (1.5), :287 (1.5), :393 (4), :428 (0.3), :524 (1.2),
  :540 (server-provided retry_after), :594 (1.5), :631 (1.0) = >=11.5 s of pure sleep in 22 tests, plus
  wall-clock asserts :276 (<2.0s), :299 (<0.2s), :400 (<2.0s), :532 (<1.0s).
- test_listings_perf.py:63 `elapsed < 0.2` warm search, :76 `elapsed < 1.0` cold grid build -> against
  the shared backend/inventory.db (F6) and machine speed; flaky on a throttled/battery laptop or CI.
- Smaller sleeps: test_incomplete_listings_index.py:92,350; test_listings_scoped_grid.py:668;
  test_password_hash.py:73.
- Fix: inject a clock / make the slow stub wait on a threading.Event the test releases; assert on
  "returned before the event was set" instead of seconds; tag remaining real-time tests `@pytest.mark.slow`
  and keep them out of the default run. P2.

### F10 naming: 23 files named after a date or finding id (P3)
- *_20260924/25/26.py (10 files) and *_fNN.py (13 files: f01,f02,f03,f05,f06,f09,f10,f12,f13,f14,f15...).
  The name records when/why a bug was found, not the unit under test, so related tests scatter
  (e.g. rooftop: test_rooftop_match, test_rooftop_same_source_20260926, test_store_scoped_recipe_gate_20260925,
  test_rooftop_weak_name_and_department, test_rooftop_corpus, test_rooftop_refusals_report,
  test_rooftop_address_provenance). Fix: rename to unit (test_recipes_store_scope.py etc.), keep the
  finding id in the docstring. P3.

---------------------------------------------------------------------------------------------------
# FINAL SUMMARY

Suite: 303 test files + 2 conftests, 58,046 lines, 3,188 test functions (AST count). 8 files > 800 lines.
Nothing was edited or run except `pytest --collect-only -q test_rooftop_corpus.py` (115 collected,
30 `corpus_ideal` params = the 30 xfails, confirmed).

## Ranked table

| # | Pri | Finding | Files | Key evidence |
|---|-----|---------|-------|--------------|
| 1 | P1 | .env leaks into tests: conftest layering deletes (not blanks) INVENTORY_DATABASE_URL; reload(main)/collection re-run dotenv + init_* DB passes against the dev's real Postgres `cars` + users.db | conftest.py:116, backend/tests/conftest.py:57-62, main.py:5,350-356, 22 reload files, 9 top-level-import files | delenv after setenv(""); load_dotenv(override=False); init_inventory_db -> DDL + seed_cars |
| 2 | P1 | 22 files / 25 reloads of backend.main via 21 drifting copies of `_fresh_app` | see F7-A | env baselines differ per copy; reload = full app boot per test |
| 3 | P1 | Vacuous + writing test on live data | test_charger_daytona_specs.py:11-25 | car 3608 lookup, bare `return`, `_analyze_local_sticker_pdf` writes |
| 4 | P1 | Tests write fake rows into the real users DB | test_compare_and_stats.py:28 (and any users_db call without USERS_DB_PATH) | collection-time dotenv sets USERS_DB_PATH; users_sqlite.py:34 |
| 5 | P2 | Shared mutable backend/inventory.db as default DB; 23 files unisolated | F6 list | mtime 09-30 19:31; test_filter_options data-dependent |
| 6 | P2 | SQLite-only suite vs Postgres-only prod; 14 hand-written `cars` schemas | F8 | 41 modules / 79 is_inventory_postgres() branches never run |
| 7 | P2 | CI integration job collects 0 tests; live-DB + golden-PDF tests run nowhere | ci.yml:56-74, test_data_quality_invariants.py:441 | 0/303 files use mark.integration; REQUIRE_LOCAL_ASSETS never set |
| 8 | P2 | Tests grep template/JS/CSS source text and CSS class names | 12 source-reading files + 3 rendered class asserts | test_visual_review_mobile_history_recalls.py:26,34,60; test_search_history.py:495 |
| 9 | P2 | Giant mixed-unit test_trim_ladder.py (4460 lines, 11 sections, file-wide autouse gate-off) | test_trim_ladder.py | source already split into 29 modules |
| 10 | P2 | 5 local `sqlite_inventory` fixtures shadow the conftest one with a weaker contract | comments_db, community_api_routes, dealer_reviews, dealer_specials_store, lease_matcher | skip init + cache clear |
| 11 | P2 | Real sleeps (>=11.5s) and wall-clock asserts | test_car_page_perf.py, test_listings_perf.py | :393 sleep(4); elapsed < 0.2 |
| 12 | P2 | Silent `return` instead of skip bypasses asset-gate accounting | charger_daytona_specs, brochure_promote:29,40, brochure_trim_candidates:19 | |
| 13 | P2 | CSRF/login helpers duplicated | 18 files _csrf_token, 8 define _login* | |
| 14 | P3 | test_brochure_sources (2386), test_brochure_extract (1082) mixed units | | split along name clusters |
| 15 | P3 | 30 strict xfails doubled by legacy/scorer modes; one stale note (mallofgamazda) | test_rooftop_corpus.py:72-82 | 15 scenarios x 2 |
| 16 | P3 | 15 hand-rolled fake conn/cursor classes | F7-D | SQL never executed |
| 17 | P3 | 23 files named by date / finding id | F10 | |

## P1 details
1. Env leak (F1). Mechanism: root `_isolate_listings_env` setenv("") -> backend `_inventory_sqlite_tests_mode`
   delenv (runs later, wins) -> `importlib.reload(backend.main)` -> main.py:5 load_project_dotenv refills
   INVENTORY_DATABASE_URL=postgresql:///cars, DEV_USERS_DB_PATH, APP_ADMIN_USERNAMES, SECRET_KEY,
   GOOGLE_OAUTH_* -> main.py:352 init_inventory_db -> schema_repo.py:262 Postgres DDL + seed_cars().
   Separately, 9 modules import backend.main at top level so COLLECTION boots the app with raw .env
   values (no fixture has run yet). Inferred from code, not executed.
   Fix (two lines of defence): (a) in /conftest.py at import time, `os.environ[k] = ""` for
   INVENTORY_DATABASE_URL, DATABASE_URL, PGVECTOR_URL, USERS_DB_PATH, DEV_USERS_DB_PATH, INVENTORY_DB_PATH,
   DEALER_PORTAL_DB_PATH, INCOMPLETE_LISTINGS_DB_PATH (then point paths at a session tmp dir);
   (b) delete backend/tests/conftest.py:57-62 or switch it to setenv(""); (c) make load_project_dotenv
   return early when INVENTORY_SQLITE_TESTS=1; (d) a session guard asserting no postgres DSN in
   os.environ at each test's call phase.
2. `_fresh_app` x21 (F7-A): replace with conftest `app_factory` fixture; long-term, a `create_app()`
   in backend/main.py so tests stop reloading the module.
3. test_charger_daytona_specs.py: rewrite on sqlite_inventory with a seeded Charger row + a fixture
   sticker PDF, or delete.
4. test_compare_and_stats.py:28: set USERS_DB_PATH to tmp (falls out of fix 1a).

## P2 details
- F6 shared inventory.db: per-session tmp copy for DB_PATH; data-needing tests use sqlite_inventory.
- F8 Postgres tier: CI service container + `pg` marker for upsert/conflict/queue SQL.
- F3 CI: repurpose the empty integration job for pg + asset tests or delete it; set REQUIRE_LOCAL_ASSETS=1
  on the mini.
- F5 template coupling: render + assert visible text/data-testid; drop negative source greps.
- F4 split test_trim_ladder.py into backend/tests/trim_ladder/ (11 files + conftest with the gate fixture).
- F7-C delete 5 shadowing sqlite_inventory fixtures; F7-E shared login/csrf helpers.
- F9 inject clocks / events instead of sleeps; `slow` marker.
- F6 silent returns -> pytest.skip with an asset-gate reason.

## Duplicated-fixture clusters (counts)
| Cluster | Count | Where it should live |
|---|---|---|
| `_fresh_app` + reload(main) | 21 helper defs; 22 files / 25 reload calls | conftest `app_factory` |
| USERS_DB_PATH env setup | 37 files / 75 hits | app_factory baseline |
| `app.test_client()` | 53 files / 174 hits | conftest `client` |
| top-level `from backend.main import app` | 9 files | conftest `app` |
| local `client` fixture | 6 files | conftest |
| local `sqlite_inventory` (shadowing conftest) | 5 files | delete, use conftest |
| CSRF token handling / login helpers | 18 files / 8 files | conftest `login`, `csrf_post` |
| fake DB conn/cursor classes | 15 files (`execute` 24 defs, `close` 18, `cursor` 16, `fetchall` 11) | backend/tests/_fakes.py |
| hand-written `CREATE TABLE cars` | 14 files (23 with any CREATE TABLE) | sqlite_inventory |
| `_row`/`_listing`/`_car`/`_payload`/`_vin` builders | 11/6/5/5/5 files | optional `make_car` factory |
| INVENTORY_DATABASE_URL delenv/setenv per file | 11 delenv sites + many setenv | conftest (and delenv is wrong, see F1) |

## Checked, fine
- Imports / dead tests: every `backend.*` and bare (`scanner.`, `enrichment.`...) import and every
  patch/setattr target in all 303 files resolves to an existing module; every literal path to
  frontend/, scripts/, backend/ files exists. No dead tests found.
- Shim modules: flat backend/scanner/*.py shims replace themselves in sys.modules
  (e.g. listing_gap_fill.py:11), so monkeypatching via the flat path hits the real module.
- No duplicate (shadowed) test names in any file/class (AST). Only 2 assertion-free tests, both
  intentional "must not raise".
- Network: all 6 files touching requests/urlopen/httpx patch them (test_vision_refusal_accounting,
  test_vehicle_history_nhtsa_recalls, test_dealership_discovery, test_outbound_url_dns via getaddrinfo,
  test_listing_sticker_ipacket / test_window_sticker_urls assert no call). test_health_ready uses
  *.invalid hosts. No real network found.
- Subprocess tests: test_bootstrap_site_admin.py:64 uses a tmp USERS_DB_PATH; test_enrich_dictionary_
  import_side_effects.py:51 runs with a minimal env on purpose (it asserts import has no env side effects).
- Dictionary safety: backend/tests/conftest.py session guard blocking writes to the real dictionary +
  byte-hash check, `scratch_dictionary_root`, `sqlite_inventory` with an AST self-check of cached readers
  - well designed.
- Asset-gated skip accounting + REQUIRE_LOCAL_ASSETS escalation (conftest.py:66-150) - good; only gaps
  are the silent returns and that nothing sets the variable.
- 30 xfails: strict, documented, auto-promote on fix (test_rooftop_corpus.py). 3 skips: 2 brochure PDF
  (gitignored corpus, present on this MBP) + 1 live-DB DQ test.
- test_recipe_synth.py (1039 lines): single unit, coherent - size alone is not a problem.
- Postgres-required guard tests (test_inventory_postgres_required.py, test_inventory_write.py) use fake
  DSNs and never connect.

# CI FAILURE LEDGER (P2A.6, 2026-10-08)

Remediation plan unit P2A.6 (cluster unit tests-ci-3a). This section classifies every failure,
every error and every network-touching test from the first CI shakedown. P2A.7 fixes the
test-only classes from it; class (e) findings are recorded here and never fixed in P2A.7.

## Source

- GitHub Actions run 37850535029, branch `ci/shakedown-1`, push event, head `836cf1060`,
  2026-10-08T21:59Z. CI is the source of truth; local runs only reproduce.
- Jobs: `lint` success; `pytest` failure (job 113562137045, 7m53s); `pytest-integration`
  success; `release-guard` skipped, as designed (it runs only for main and PRs to main, D-REL3).
- pytest job: Python 3.12.15 on ubuntu-latest, `TESTS_BLOCK_NETWORK=1`, selection
  `not integration and not slow and not pg`, junit artifact `junit-offline/offline.xml`.
- Result: **3 failed, 0 errors**, 5037 passed, 34 skipped, 30 xfailed, 5 deselected, 377 s.
  The junit header says `tests=5124 failures=3 errors=0 skipped=64`; its 64 is the 34 skips
  plus the 30 strict xfails (F2).
- Terminal summaries: `NETWORK-BLOCKED CONNECTS: 15 in 5 test(s)`; `ASSET-GATED SKIPS: 9`.
  The junit carries one `network_blocked` property per refused connect (15).

Classes: (a) needs a gitignored local asset or the dev DB copy but is not an asset-gated skip;
(b) Python 3.12 vs local 3.14; (c) macOS-only assumption or case-sensitive path; (d) missing
tool (node, pdftotext); (e) real product bug: filed here, not fixed; (f) network access (from
the P2A.3 counter).

## Ledger

Paths are under `backend/tests/`. "Reproduced" means the same result in a fresh worktree of
`feature/http-only-scans` at `12c56bf49` (which, like the runner, lacks every gitignored file),
with local Python 3.14.7. The same tests pass in the main checkout, which has the index files.

| # | Test | Class | CI symptom | Cause | Planned fix | Owner |
|---|---|---|---|---|---|---|
| 1 | `test_dictionary_catalog.py::test_writing_the_real_manifest_is_blocked` | (a) | `FileNotFoundError` on `backend/dictionary/index/manifest.json` at :122. Reproduced. | The manifest is gitignored (`.gitignore:109`) and absent from a clean checkout. The test's `skipif` checks only `DICTIONARY_ROOT.is_dir()`, and that directory is tracked. The test reads the file for its before/after byte check. | Keep the coverage; no skip needed. Take the baseline as the bytes, or `None` when the file is absent. Then assert that all three writes raise `RealDictionaryWriteBlocked` naming the real path, and that the file is unchanged (still absent) afterwards. The write guard blocks by path prefix, so it refuses the write whether or not the file exists. A throwaway probe confirmed this passes in a clean worktree, and the probe was deleted. Fallback if the reviewer prefers a skip: `pytest.skip("manifest not built")`, since "not built" is in `_ASSET_GATE_SKIP_REASONS`. | P2A.7, class (a) |
| 2 | `test_dictionary_catalog.py::test_the_2026_07_31_incident_shape_is_now_stopped` | (a) | Same `FileNotFoundError`, at :154. Reproduced. | Same as #1. | Same as #1. `rebuild_catalog([], enrich_derived=False)` still raises at the manifest write (`open(mode='w') would write the real dictionary tree at .../index/manifest.json`), and the probe confirmed it. | P2A.7, class (a) |
| 3 | `test_merge_verified_specs_golden.py::test_merge_verified_specs_golden_hermetic` | (a), plus product finding PF-1 (e) | 2 golden mismatches, both car `live:1239391` (a 2026 Audi A5 Premium Plus 2.0 TFSI quattro), one per `include_extended_specs` value. `epa_engine_description` is `'2.0L I4 (SIDI)'` in the golden and `'Hybrid 2.0L I4 (SIDI; Mild Hybrid)'` in CI; `epa_city08`/`epa_highway08` are 22/32 vs 26/36; `fuel_economy_display` is `22 City / 32 Hwy` vs `26 City / 36 Hwy`. Reproduced exactly. | The golden was recorded with `backend/dictionary/index/dictionary_catalog.db` present, and that file is gitignored (`.gitignore:110`). Without it, `find_epa_csv('Audi', 'A5', 2026)` resolves to `epa/Audi/2025_Audi_A4_EPA.csv` instead of `epa/Audi/2026_Audi_A5_EPA.csv`. PF-1 below has the mechanism. The other 2,033 of the 2,034 golden keys (2,000 live cars plus 34 hand-built rows) match in both environments. | Keep the coverage. Do not skip the whole golden, and do not re-record it without the catalog, which would pin PF-1 as expected output. Pin the catalog-dependent keys as `_CATALOG_DEPENDENT = frozenset({"live:1239391"})`, the set CI measured, and assert it is a subset of the golden's keys. The hermetic test compares every other key, everywhere. A new test compares only the pinned keys and calls `pytest.skip("catalog db not built")` when `dictionary_catalog.CATALOG_DB_PATH` is absent, so the gap is counted under ASSET-GATED SKIPS. Expected effect: the gated-skip count goes from 9 to 10 on CI. | P2A.7, class (a). PF-1 belongs to Phase 9 |
| 4 | `test_scraper_chain.py::TestFetchListingHtmlIntegration::test_requests_result_accepted_when_sufficient` | (f) | Passed. 3 refused connects, `d.example:443 (curl_cffi)`. Reproduced (15 in 5 tests, locally too). | `gap_fill._fetch_listing_html_via_chain` puts `ImpersonatingFetcher()` first (`backend/scanner/post_scan/gap_fill.py:153`). It makes one curl_cffi request per profile in `IMPERSONATE_PROFILES = ("chrome", "chrome124", "safari17_0")` (`backend/scanner/net/client.py:41`). The tests stub only `_requests_fetch_html` and `_playwright_fetch_html`. They pass because the refused stage raises `FetchError` and the chain falls through to the stubs. Without the guard, each test makes a real DNS query for `d.example` (a reserved TLD, so NXDOMAIN on a sane resolver) plus up to 3 connects with a 25 s timeout. A wildcard-DNS or captive-portal resolver would turn that into real traffic or a long hang. | Add a class-level autouse fixture that stubs the curl_cffi transport: monkeypatch `backend.scanner.net.client.import_curl_cffi` to return a fake module whose request entry point (reached through `client.send`, from `rotate_impersonation` at `client.py:194`) raises a connection error and records each call. The test still means "the impersonate stage fails, so the requests stage decides". Assert the fake saw 3 calls, which pins the profile rotation instead of leaking it. Accept: `TESTS_BLOCK_NETWORK=1` on `test_scraper_chain.py` prints "no refused connects". | P2A.7, class (f) |
| 5 | `...::test_thin_requests_result_falls_through_to_playwright` | (f) | Passed; 3 refused, same host. | Same as #4. | Same fixture as #4. | P2A.7, class (f) |
| 6 | `...::test_thin_requests_result_stays_http_in_scans` | (f) | Passed; 3 refused, same host. | Same as #4. | Same fixture as #4. | P2A.7, class (f) |
| 7 | `...::test_total_failure_returns_none` | (f) | Passed; 3 refused, same host. | Same as #4. | Same fixture as #4. | P2A.7, class (f) |
| 8 | `...::test_works_inside_running_event_loop` | (f) | Passed; 3 refused, same host. | Same as #4. | Same fixture as #4. | P2A.7, class (f) |

The other three tests in that class (`test_non_http_url_returns_none_without_fetching`,
`test_env_flag_forces_legacy_path`, `test_chain_bug_falls_back_to_legacy`) never reach the
chain's impersonate stage and record no connects.

Classes (b), (c) and (d): **none in this run.** No failure depends on the Python version or
the OS. node 20 is installed by the job, and `test_js_unit.py::test_js_unit_suite_passes` ran
and passed. No test skipped for a missing tool.

### P2A.7 file lists (one worktree per class)

| Class | Files | Tests |
|---|---|---|
| (a) | `backend/tests/test_dictionary_catalog.py`, `backend/tests/test_merge_verified_specs_golden.py` | #1-#3 |
| (f) | `backend/tests/test_scraper_chain.py` | #4-#8 |
| (b), (c), (d) | none | none |

P2A.7 exit check: the changed tests pass both with and without the gitignored index files. A
fresh worktree simulates "without", and the main checkout provides "with". `TESTS_BLOCK_NETWORK=1`
on `test_scraper_chain.py` must show 0 refused connects.

### P2A.7 as merged (d8be6049a; Phase 2A exit gate, 2026-10-08)

P2A.7 ran in parallel with this ledger, and two of its fixes differ from the plans above:

- #3: P2A.7 did not pin `_CATALOG_DEPENDENT` or add a gated-skip test. The hermetic golden now
  builds `dictionary_catalog.db` from the tracked dictionary tree into `tmp_path`, so every
  golden key, including `live:1239391`, is compared in every checkout. No skip was added, so
  the asset-gated count stays at 9, not 10. The golden was not re-recorded. The golden no longer
  runs the no-catalog fallback that prod takes; PF-1 below still owns that path.
- #4-#8: the fixture is as planned. Only `test_requests_result_accepted_when_sufficient` checks
  the stub's calls (one per profile, in `ImpersonatingFetcher.PROFILES` order). The other four
  tests send their attempts through the same stub.
- Measured in clean worktrees (no gitignored index), with `TESTS_BLOCK_NETWORK=1` and the CI
  selection on the three files. At `12c56bf49` (pre-phase): 3 failed, 15 refused connects in
  5 tests. At `e3f5f0aa6` (Phase 2A tip): 51 passed, 0 refused connects, and 1 asset-gated skip
  (`test_real_catalog_db_is_readable_but_not_writable`, already skipped before P2A.7). With the
  index present (main checkout), all three files pass too.

## Product findings (class (e): filed, not fixed)

### PF-1: without the dictionary catalog DB, the EPA file resolver picks another model's file (Audi S/A5 models, Audi e-tron variants, Toyota Supra)

- **Owner:** Phase 9: P9.2 (single resolver) and P9.6 (in-image catalog). The decisions are
  D-HY1 and D-HY9. The P0A.3 ops-log block and this unit's brief name it "Phase 9 / D-DC5", but
  Section 7's D-DC5 is browser retirement, so D-HY9 (should prod serve catalog-resolved
  dictionary content) is the matching decision. Do not fix it in Phase 2A.
- **Prod exposure:** prod has never had `backend/dictionary/index/dictionary_catalog.db`.
  P0A.3 (SCANNING_OPS_LOG.md, P0A.3 block) confirmed it absent from the web image, excluded by
  `.railwayignore:20 *.db`, with `DICTIONARY_CATALOG_DB_PATH` unset. The same block found that
  prod ships all 12,126 `epa/` files. So prod resolves EPA files the way CI does.
- **Mechanism:**
  1. `dictionary_catalog._find_epa_csv_uncached` tries the catalog SQL candidates first. With
     no DB, it drops to `_legacy_glob_find`.
  2. That fallback searches under the trim-ladder family label, not the model:
     `model_label = epa_model_search_name(make, model)` (`dictionary_catalog.py:448`).
     `_resolve_trim_model_key('Audi', 'A5')` is `('audi', 'a4')`, so the label is `'A4'`.
     `backend/vehicle_facts/epa_model.py` already says "epa_model_search_name is a trim-ladder
     FAMILY label (S5 -> 'A4')".
  3. Its glob is non-recursive (`root.glob(pat)`, `:463`), so it never sees the sharded
     `epa/<Make>/` files. The fuzzy scan then matches the family label's file by model name
     and year distance.
  4. `epa_csv_is_for_model` accepts that file, because its `wanted` set includes the family
     label (`:566`).
  5. `knowledge_engine._lookup_epa_from_dictionary_csv` then matches rows by trim substring
     only. For the 2026 A5 that is `quattro`, so it returns the 2025 A4 mild-hybrid row.
  6. That row's 26/36 mpg replaces the dealer's own 22/32, because `fuel_economy.py` ranks
     the EPA figure above the dealer mpg columns.
- **Size, measured locally and read-only** (2026-10-08): `find_epa_csv(make, model, year)`
  was run for each of the 12,126 sharded EPA files' own (year, make, model), with the catalog
  (a scratch copy of the MBP's DB) and without it.
  - With the catalog, 12,115 resolve to their own file. Without it, 11,987 do.
  - 130 differ, and 129 of those resolve to a **different model's** file without the catalog:

    | Requested | Resolves to | YMMs |
    |---|---|---|
    | Audi S4 | A4 | 27 |
    | Audi S6 | A6 | 20 |
    | Audi A5 | A4 | 19 |
    | Audi S5 | A4 | 19 |
    | Audi SQ5 | Q5 | 13 |
    | Toyota Supra | GR Supra | 13 |
    | Audi S3 | A3 | 11 |
    | Audi SQ8 e-tron | Q8 e-tron | 2 |
    | Audi SQ6 e-tron | Q6 e-tron | 2 |
    | Audi A6 e-tron | A6 | 1 |
    | Audi S6 e-tron | A6 | 1 |

  - The 130th is a J.K. Motors spelling variant.
  - Recent model years are affected: 2024 and 2025 A5/S3/S4/S5/S6/SQ5, and 2026
    A5/S3/S5/SQ5. 2026 A5 and S5 resolve to the **2025 A4** file.
  - The worst cases cross powertrains: 2027 A6 e-tron and S6 e-tron (battery-electric)
    resolve to the gas 2026 A6 file.
  - The sweep ran on macOS. CI (Linux) reproduced the A5 case identically. Active-inventory
    and prod-page counts were not measured.
- **Request-path reach:**
  - `merge_verified_specs` reaches the dictionary CSV only when the car has no accepted
    `epa_master_id` link and `epa_master` has no per-trim row (`knowledge_engine.py:709`). Car
    1239391 itself carries a link (19023), so on prod's Postgres its by-id row should win.
  - The trim ladder calls `find_epa_csv` directly (`trim_ladder/selection.py:254`,
    `trim_ladder/citations.py:132`), as does `trim_spec_extractor.py:881`.
- **Rules at stake:** the EPA catalog never outranks the dealer's own engine text or the VIN
  decode, and vPIC outranks the dealer feed for electrification. A gas A6 row served for an
  A6 e-tron, or a mild-hybrid A4 row served for a non-hybrid A5, breaks both.
- **Bearing on the plan:** P9.2 states "The EPA fuzzy fallback is unchanged; it already
  reaches parity, 217/217". That count shows a file resolves in both setups, not that it is
  the same file. P9.2's Accept should add an EPA file-identity parity check (catalog vs no
  catalog) that covers these 129 YMMs. A likely direction is to search the raw model before
  the family label and to glob the sharded tree, but Phase 9 decides.

## Observed in the same run, not failures (for P14C.3's skip budget)

| Skip | Count | Note | Owner |
|---|---|---|---|
| Asset-gated (brochure PDFs, catalog db) | 9 | The P14C.3 baseline. P2A.7 #3 added no skip (see "P2A.7 as merged"), so it stays 9. | P14C.3 |
| `test_car_detail_context_golden.py[shard_0..5]`: "needs PYTHONHASHSEED=0" | 6 | Not asset-gated, so not counted, and it runs nowhere in CI: a silent coverage gap. Options: set `PYTHONHASHSEED=0` for that test in CI, or make the trim-ladder order deterministic. | P14C.3 decides; not a P2A.7 item |
| `test_search_golden.py::*_postgres`: "SEARCH_GOLDEN_PG=1 not set" | 17 | Postgres tier (F8). | Phase 14A/14B |
| `test_charger_daytona_specs.py`: "car 3608 absent" | 1 | Already ranked P1 #3 above (vacuous and writing). | existing finding |
| `test_llm_call_site_parity.py::test_regenerate_goldens` | 1 | Intentional regen switch. | none |

Reproduce: `gh run view 37850535029 --log-failed` and `gh run download 37850535029` (junit).
Local: in a fresh worktree, run `TESTS_BLOCK_NETWORK=1 .venv/bin/python -m pytest
backend/tests/test_dictionary_catalog.py backend/tests/test_merge_verified_specs_golden.py
backend/tests/test_scraper_chain.py -q -p no:cacheprovider`. At `12c56bf49` (before P2A.7) expect
3 failed and 15 refused connects in 5 tests; from `e3f5f0aa6` on, 0 failed and 0 refused.
