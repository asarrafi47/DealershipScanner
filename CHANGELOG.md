# Changelog

## [Unreleased]

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
