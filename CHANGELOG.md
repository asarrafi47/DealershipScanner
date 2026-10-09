# Changelog

## [Unreleased]

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
