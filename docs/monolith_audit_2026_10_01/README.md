# Monolith audit, 2026-10-01

Read-only review of every tracked source file (1,198 files, 306k lines; 893 non-test
files, 250k lines). A metrics pass measured size, longest function, imports and fan-in
for every file; six reviewers then read their area file by file. Area reports, each with
a ranked table, P1/P2 details, proposed splits and a "checked, fine" list:

| Area | Files | Report |
|---|---|---|
| Scanner (`backend/scanner`) | 106 | [scanner.md](scanner.md) |
| Enrichment, OEM, parsers, vision, AI | 186 | [enrich.md](enrich.md) |
| Web backend (`main.py`, routes, dev, auth, billing, utils) | 134 | [web.md](web.md) |
| Frontend JS/CSS/templates, iOS | 141 | [frontend.md](frontend.md) |
| DB, scripts, discovery, deploy, root entry points | 323 | [datascripts.md](datascripts.md) |
| Tests | 306 | [tests.md](tests.md) |

Priorities: **P1** = causing bugs or blocking work, **P2** = worth doing soon, **P3** = cosmetic.
Findings marked *unverified* came from reading code and need a data or runtime check first.

## 1. Live bugs found on the way (fix before any refactor)

| # | Bug | Where | Status |
|---|---|---|---|
| B1 | Prod CSP `style-src-elem 'self'` blocks every inline `<style>`: admin dealers, site hub, users, user form, reviews, inventory base, scan-lab listings render partly unstyled in prod | `backend/main.py:759-775`, admin templates | header confirmed live; move blocks to static CSS |
| B2 | Web, iOS API and dealer logins treated passwords differently | `main.py`, `dealer/routes.py` | **fixed** e2bf56c8e (`submitted_password_attempts`) |
| B3 | `scanner --delta` misses the learned-street address and the `_feed_scoped` exemption (Tutton 5-of-346 fix), so it can un-list a store's own cars | `scanner/delta_scan.py:173,186,218` | used only by the disabled laptop nightly; two-line port |
| B4 | vPIC "4x2" written raw into the car's drivetrain by one of 5 drivetrain normalizers | `enrichment/nhtsa_vpic.py:71-85,286-289` | *unverified*: count rows with drivetrain `4x2` |
| B5 | Market price averages cached on the SQLite file mtime, unbounded; on Postgres they may never refresh | `utils/market_price.py` | *unverified* in prod |
| B6 | Tests may write to the developer's real DBs: `delenv` undoes the root conftest's blanking, `importlib.reload(main)` re-reads `.env` | `conftest.py:116`, `backend/tests/conftest.py:57-62`, 22 files | *unverified*; a planted marker row test will prove it |
| B7 | iOS fetches `/api/listings/cars` without ZIP/radius (400 `zip_required` since 09-28) and swallows the session-ZIP save error | `ios/.../APIClient.swift`, `ListingsSession` | pass zip+radius explicitly |
| B8 | Dev/operator smart import, test-scanner, bulk import call `scanner.js` / `backend/scanner.py`, which no longer exist | `dev/routes.py`, `dealer/admin/operator_api.py`, `dev.js` | delete or move onto job_queue |
| B9 | 3 of 13 HTML-escape copies skip `"`; `dev.js:812` puts one inside `value=""` | `frontend/static/dev.js` | one shared escaper |
| B10 | Paid-feature gates disagree (4 of them): any plan sets `user_is_premium`, pages trust it, APIs check plan features; admin role trusted from the cookie for 14 days | `billing/routes.py:347`, `main.py`, `ai_chat_bp.py` | matters once billing is on |
| B11 | `reset_db.py` runs `DELETE FROM cars` behind `--yes` only, no prod guard, no references | `backend/scripts/reset_db.py` | delete |
| B12 | Broken entry points: `backend/scraping/cli.py:285` bare import; `scan_92694_mac_mini.sh` runs a missing script; `start-always-on.sh` serves Werkzeug publicly | scripts | delete / fix |

## 2. One rule, many copies (the main source of drift)

| Rule | Copies | Report |
|---|---|---|
| "Is this car this store's?" (rooftop attribution) | 7 (parser gate, full-scan pass, delta pass, sister-store filter, card stamp, disown, VIN-owner guard) + 2 address lookups | scanner.md, enrich.md |
| Paid-feature access | 4 gates | web.md W2/W22 |
| Drivetrain normalizing | 5, three different answers for "4x2" | enrich.md |
| BEV / electrification detection | 9+ | enrich.md |
| Listing model → EPA model map | 4 | enrich.md |
| Spec column writers | 4 writers + `inventory_repair`, each with its own priority, plus the read-side merge | enrich.md |
| Anthropic API call paths | 6+ (different retry/refusal/truncation handling, hard-coded model IDs) | enrich.md |
| HTTP fetchers in the scanner | 6 (feed replay ignores `SCANNER_HTTP_PROXY`) | scanner.md |
| DB connection helpers | 25 `_dsn()` + ~8 `_connect()` + 29 regex `.env` parsers | datascripts.md F8 |
| Schema DDL | 3 definitions (migrations, `init_postgres_inventory` 477 lines, SQLite `schema_repo`) + runtime DDL in 10 db modules | datascripts.md F1-F3 |
| JS HTML escape / CSRF reader / currency format | 13 / 6 / 7 | frontend.md |
| Test app factory (`_fresh_app` + reload main) | 21 / 25 | tests.md |
| Post-scan stage sequence | 2 (~70% identical) | scanner.md |

## 3. God functions and god files

| Function / file | Size | Report |
|---|---|---|
| `scanner/phases/dealer_run.py::run_dealer` | 664 lines, ~12 phases, browser leftovers | scanner.md |
| `utils/car_serialize/serialize.py::serialize_car_for_api` | 577 | web.md |
| `scanner/database.py::upsert_vehicles` | 554; post-write steps `except: pass`; per-row writes | scanner.md |
| `enrichment/knowledge_engine_specs.py::merge_verified_specs` | 489; per-field if-chains; 15 importers | enrich.md |
| `enrichment/trim_ladder/build.py::_build_ladder_result` | 486 (low risk, one caller) | enrich.md |
| `db/inventory_pg.py::init_postgres_inventory` | 477 runtime DDL duplicating migrations | datascripts.md |
| `db/repositories/search_repo.py::search_cars` | 415, ~35 parameters | datascripts.md |
| `routes/cars_pages.py::_build_car_detail_view_context` | 322 | web.md |
| `backend/main.py` | 1,471 lines, 104 imports, ~120 `main_module().X` lookups, 4 lazy `from backend.main import` | web.md W1 |
| `parsers/__init__.py` | ~960 lines of rooftop logic in a package `__init__`, fan-in 149 | enrich.md |
| `scanner/recipes.py` | 1,165 lines, fan-in 82 | scanner.md |
| `scanner/recipe_synth.py` | 2,455 lines: 17 platform plugins + HTTP + a second validation stack | scanner.md |
| `enrichment/brochure_extract.py` | 2,403 lines, six jobs, fan-in 52 | enrich.md |
| `frontend/static/main.js` | 3,360 lines in one closure, ~40 `window.__DS_*` globals | frontend.md |
| `frontend/static/car_page.js` | 2,727 lines (TCO ~960 lines, finance 351-line function, gallery 346) | frontend.md |
| `scripts/dealer_pipeline.py` | 1,394 lines; the Railway fleet entry point, `reconcile_dealer` among log writers | datascripts.md F11 |
| `scripts/fetch_oem_brochures.py` | 2,525 lines, seven subsystems | datascripts.md F7 |

## 4. Dead code

- `backend/oem/**` ~6,000 lines, no importers except two helpers; `bmw.py` imports a package that does not exist (enrich.md).
- ~80 scripts with no references, listed with `git grep` counts and last-commit dates (datascripts.md).
- ~190 unused CSS classes (old `lp-*` landing, pre-sidebar topbar, `docked-*`), `car_chat.js`, `static/style.css`, `ios/.../Legacy/` (frontend.md).
- Scanner `vdp/` queue, claude_extract, packages, specs, price_hints; `bmw_enhancer` flags; `phases/nav.py` diverged copies (scanner.md).
- `mfa_otp`, `qr_segno`, `totp`, `vehicle_naming`, `agent.verify_car_data` (web.md, enrich.md).

## 5. Test gaps

- No JavaScript tests at all; a node `vm` harness over the pure helper files is the cheap first step.
- 41 modules have Postgres-only branches that never run in tests (prod is Postgres); 14 test files hand-write a 6-8 column `cars` table.
- The CI `pytest-integration` job collects zero tests (no file carries the marker).
- `test_charger_daytona_specs.py` passes vacuously and can write to the live DB.
- 12 tests assert template/CSS source text.

## 6. Suggested order

1. **Bugs in section 1** (B1, B3, B6 proof, B7, B11, B12, then B4/B5 after a data check).
2. **Delete dead code** (oem, dead scripts, dead CSS, Legacy) — shrinks every later step.
3. **Test safety:** one app factory and DB isolation in conftest (B6), a JS test harness.
4. **Single sources of truth:** `backend/db/connect.py`; one access layer for paid features and admin; one attribution package for the rooftop decision; one drivetrain/BEV normalizer; one Anthropic client; schema from migrations only.
5. **Split god functions behind unchanged signatures,** with golden tests written first: `run_dealer` (also closes B3), `upsert_vehicles`, `merge_verified_specs`, `serialize_car_for_api`, `main.py` (move state out first), `main.js` / `car_page.js` (pure functions first).

Every CSS move needs a screenshot diff at 390 and 1440 px: load order is the cascade
(`_head_css.html`).

## 7. Progress (2026-10-01)

| Phase | Commit | Result |
|---|---|---|
| 1 Live bugs B1-B12 | dc86266ec, e2bf56c8e | fixed (B10 via phase 4); tests had written 167 rows to the real users.db (removed) |
| 2 Dead code | d17abb0d8 | ~20,000 lines deleted (oem, 55 scripts, dead modules, ~190 CSS classes, iOS Legacy) |
| 3 Test safety | b1578241d | hermetic DBs, one app_factory, JS unit tests (node:test via pytest), trim_ladder split |
| 4 Single sources of truth | 75671a38f | backend/db/connect.py, billing/access.py, attribution/, vehicle_facts/, llm/client.py, schema from migrations (V025) |
| 5 God functions | 14dc85936 | run_dealer, upsert_vehicles, merge_verified_specs, serialize_car_for_api, car detail context, search_cars, main.js, car_page.js split behind goldens |
| 6 main.py split | b3e829f0d, 0f439058d, 5bf71ff0a, d8618fced | main.py 1,402 -> 192 lines (app assembly only): auth/session.py, web/{static,security,templating}.py, auth/pages.py, routes/account.py, enrichment/recalls_lookup.py; main_module() retired; app-surface golden test |

Still open from this audit: `init_job_queue_schema()` and init_*_db() still run on web import; `recipe_synth.py`
platform plugins into a package; `dealer_pipeline.py` and `fetch_oem_brochures.py` splits;
`brochure_extract.py`; `comments_db.py`; the six scanner HTTP fetchers; the four spec-column
writers; 139 scanner env knobs; inline template scripts and CSS files whose contents belong to
other pages; a base layout for public templates. Operational: run
`python -m backend.scripts.migrate --baseline 19 --apply` on local and prod (V019 fails on every
boot; V020+ never apply) — needs owner approval.
