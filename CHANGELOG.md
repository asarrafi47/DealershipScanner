# Changelog

## [Unreleased]

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

## [0.2.0] — 2026-07-08

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
