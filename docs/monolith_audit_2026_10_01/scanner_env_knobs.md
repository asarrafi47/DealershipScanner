# Scanner environment knobs

Generated from `backend/scanner/env_knobs.py` (`python -m backend.scanner.env_knobs --write-doc`).
Do not edit by hand: `test_scanner_env_knob_registry.py` fails when this file is stale.

Categories: live (tunes the current HTTP-only path), operator (kill switch or run-shape control),
browser_era (only browser code reads it, or nothing does; P17C.7 retires these), test_only, internal
(platform-set or secret). Readers are files under `backend/scanner/`; Set in is best effort (deploy
files, scripts and scanner code that assign the name).

## live (65)

| Name | Type | Default | Readers | Set in | What it does |
|---|---|---|---|---|---|
| `DEALERS_FROM_ACTIVE_INVENTORY` | bool | unset | `cli.py`, `manifest.py` | not set in tracked files | Roster = dealers with active inventory rows instead of the manifest. |
| `DEALERS_FROM_DB` | bool | unset | `cli.py`, `manifest.py` | not set in tracked files | Roster read from the dealers table instead of dealers.json. |
| `DEALER_LOGS_ROOT` | path | `workspace/dealer_logs` | `pipeline/constants.py`, `recipe_validation.py` | not set in tracked files | Where per-dealer logs are written. |
| `GROUP_POOLS_PATH` | path | `upsert/group_pools default` | `upsert/ownership.py` | not set in tracked files | Group-feed pool definitions used by ownership attribution. |
| `INTERIOR_VISION_FALLBACK_HERO` | bool | unset | `post_scan/pipeline.py` | not set in tracked files | Interior vision may fall back to the hero image. |
| `INTERIOR_VISION_FALLBACK_THROUGH_WINDOWS` | bool | `1` | `post_scan/pipeline.py` | not set in tracked files | Interior vision may use through-window exterior shots. |
| `INTERIOR_VISION_MAX_URL_TRIES` | int | `3` | `post_scan/pipeline.py` | not set in tracked files | Image URLs tried per car in interior vision. |
| `INTERIOR_VISION_NO_EXTERIOR_FALLBACK` | bool | unset | `post_scan/pipeline.py` | not set in tracked files | Disable the exterior fallback in interior vision. |
| `INVENTORY_PAGE_SIZE` | int | `500` | `scrapers/dealer_com_bulk_fetch.py` | not set in tracked files | Page size for the dealer.com bulk fetch. |
| `LISTING_GAP_FILL_ALLOW_DDG` | bool | `1` | `post_scan/gap_fill.py` | not set in tracked files | Gap fill may search DuckDuckGo for missing listing pages. |
| `LISTING_GAP_FILL_DDG_MAX` | int | `60` | `post_scan/gap_fill.py` | not set in tracked files | Max DuckDuckGo lookups per gap-fill run (0-2000). |
| `LISTING_GAP_FILL_DDG_TIMEOUT` | float | `15` | `post_scan/gap_fill.py` | not set in tracked files | Seconds per DuckDuckGo lookup. |
| `PIPELINE_RETIRE_ALARM_GAP_AWARE` | bool | unset | `pipeline/reconcile.py` | not set in tracked files | Pipeline retire alarm discounts rows covered by a gap. |
| `ROOFTOP_LEDGER` | bool | `1` | `rooftop_ledger.py` | not set in tracked files | Write the rooftop attribution ledger. |
| `SCANNER_ALGOLIA_AUTO_SCOPE` | bool | `1` | `scrapers/algolia_scope.py` | not set in tracked files | Auto-scope Algolia queries to the dealer. |
| `SCANNER_ALGOLIA_RECOVERY_FLOOR` | int | `80` | `inventory_recovery.py` | not set in tracked files | Minimum Algolia rows before a recovery result is accepted. |
| `SCANNER_ALLOW_PRIVATE_HOSTS` | bool | unset | `net/ssrf.py` | not set in tracked files | Switch the SSRF guard off (labs against local hosts). |
| `SCANNER_AUTO_HEAL` | bool | `1` | `post_scan/auto_heal.py` | not set in tracked files | Run the post-scan auto heal. |
| `SCANNER_DEFAULT_INTERVAL_HOURS` | int | `24` | `job_queue.py` | not set in tracked files | Default rescan interval for queued jobs. |
| `SCANNER_GALLERY_RECOVERY_MIN` | int | `14` | `vdp/html_recovery.py` | not set in tracked files | Gallery size below which HTML recovery runs. |
| `SCANNER_GALLERY_VISION_FILTER` | bool | unset | `post_scan/pipeline.py` | not set in tracked files | Vision-filter gallery images after the scan. |
| `SCANNER_HEAL_WORKERS` | int | `4` | `post_scan/gap_fill.py` | not set in tracked files | Gap-fill worker threads (1-12). |
| `SCANNER_IDLE_IN_TXN_TIMEOUT_MS` | int | `60000` | `database.py` | not set in tracked files | Postgres idle_in_transaction_session_timeout for scanner connections. |
| `SCANNER_INVENTORY_RECOVERY` | bool | `1` | `inventory_recovery.py` | not set in tracked files | Run inventory recovery strategies when a scan comes back thin. |
| `SCANNER_JOB_TIMEOUT_SEC` | int | `3600` | `job_queue.py` | not set in tracked files | Job-queue job timeout. |
| `SCANNER_LOCK_PATH` | path | `workspace/scanner.lock` | `scan_lock.py` | not set in tracked files | Scanner lock file; one process per lock file. |
| `SCANNER_MONRONEY_VISION` | bool | `1` | `post_scan/pipeline.py` | not set in tracked files | Read Monroney stickers with vision after the scan. |
| `SCANNER_NO_RETIRE` | bool | unset | `inventory_reconcile.py` | not set in tracked files | Stop button: no retirement by scanner, delta scan or rooftop disown. |
| `SCANNER_POST_GALLERY_RECOVERY` | bool | `1` | `post_scan/pipeline.py` | not set in tracked files | Post-scan gallery recovery stage. |
| `SCANNER_POST_LISTING_GAP_FILL_MAX` | int | `400` | `post_scan/gap_fill.py` | not set in tracked files | Max cars gap-filled per run. |
| `SCANNER_POST_SCAN_LOOKBACK_HOURS` | int | `6` | `post_scan/job.py` | not set in tracked files | Post-scan job lookback window. |
| `SCANNER_POST_WINDOW_STICKER_MAX` | int | `0` | `post_scan/pipeline.py` | not set in tracked files | Max stickers fetched per run (0 = no cap). |
| `SCANNER_RECIPE_FETCH` | bool | `1` | `recipes.py` | not set in tracked files | Recipe fetch path; 0 turns it off. |
| `SCANNER_RECIPE_MIN_FIELD_COVERAGE` | float | `0.5` | `recipes.py` | not set in tracked files | Minimum field coverage for a recipe to pass. |
| `SCANNER_RECIPE_MIN_PRICE_COVERAGE` | float | `0.5` | `recipes.py` | not set in tracked files | Minimum price coverage for a recipe to pass. |
| `SCANNER_RECIPE_MIN_VEHICLES` | int | `10` | `recipes.py` | not set in tracked files | Minimum vehicles for a recipe to pass. |
| `SCANNER_RECONCILE_BUCKET_SHARE` | float | `0.6` | `inventory_reconcile.py` | not set in tracked files | Largest share of prior valid-VIN rows one run may retire. |
| `SCANNER_RECONCILE_MIN_COVERAGE` | float | unset | `inventory_reconcile.py` | not set in tracked files | Deprecated and ignored, except 0 (bucket guard off). |
| `SCANNER_RECONCILE_MIN_ROWS` | int | `20` | `inventory_reconcile.py` | not set in tracked files | Rows a scan needs before it may retire anything. |
| `SCANNER_RECOVERY_HINT_FILTER` | bool | `1` | `inventory_recovery.py` | not set in tracked files | Recovery filters strategies by platform hints. |
| `SCANNER_RECOVERY_KNOWN_LOT_MIN` | int | `20` | `inventory_recovery.py` | not set in tracked files | Known-lot size where the known-lot ratio applies. |
| `SCANNER_RECOVERY_KNOWN_LOT_RATIO` | float | `0.5` | `inventory_recovery.py` | not set in tracked files | Share of a known lot recovery must reach. |
| `SCANNER_RECOVERY_MIN_ROWS` | int | `8` | `inventory_recovery.py` | not set in tracked files | Rows a recovery result needs to be accepted. |
| `SCANNER_RECOVERY_STRATEGY_TIMEOUT_SEC` | int | `90` | `inventory_recovery.py` | not set in tracked files | Seconds per recovery strategy. |
| `SCANNER_REPLAY_MAX_PAGES` | int | `hard cap` | `recipes.py` | not set in tracked files | Recipe replay page cap. |
| `SCANNER_SISTER_STORE_FILTER` | bool | `1` | `dealer/location.py` | not set in tracked files | Drop rows that belong to a sister store. |
| `SCANNER_SISTER_STORE_MIN_KEEP` | int | `8` | `dealer/location.py`, `phases/dealer_run_steps/enrich.py` | not set in tracked files | Rows the sister-store filter always keeps. |
| `SCANNER_SISTER_STORE_SAFE` | bool | `1` | `dealer/location.py`, `phases/dealer_run_steps/enrich.py` | not set in tracked files | Sister-store filter safe mode. |
| `SCANNER_SKIP_DEALER_SUBSTRINGS` | csv | unset | `manifest.py` | not set in tracked files | Dealers whose id or name contains one of these are skipped. |
| `SCANNER_STALE_JOB_SEC` | int | `JOB_TIMEOUT+900` | `job_queue.py` | not set in tracked files | Age at which a running job is reaped. |
| `SCANNER_SYNTH_FETCH_DELAY` | float | `0` | `synth/http.py` | not set in tracked files | Seconds between recipe-synthesis fetches. |
| `SCANNER_TRACE_VIN` | str | unset | `upsert/sql.py` | not set in tracked files | Log every upsert decision for this VIN. |
| `SCANNER_UNREACHABLE_BACKOFF_DAYS` | int | `7` | `job_queue.py` | not set in tracked files | Back-off for unreachable dealers in the job queue. |
| `SCANNER_VDP_DB_MERGE` | bool | `1` | `vdp/prefetch.py` | not set in tracked files | VDP prefetch carries forward DB values. |
| `SCANNER_VDP_HTML_RECOVERY` | bool | `1` | `vdp/html_recovery.py` | not set in tracked files | VDP HTML gallery recovery. |
| `SCANNER_VDP_HTTP_FIRST` | bool | `1` | `vdp/prefetch.py` | not set in tracked files | VDP pages fetched over HTTP before any browser. |
| `SCANNER_VDP_HTTP_FIRST_GALLERY_MIN` | int | `12` | `vdp/prefetch.py` | not set in tracked files | Gallery size that satisfies the HTTP-first pass. |
| `SCANNER_VDP_HTTP_FIRST_MAX` | int | `800` | `vdp/prefetch.py` | not set in tracked files | Max VDP pages per dealer in the HTTP-first pass. |
| `SCANNER_VDP_HTTP_FIRST_MAX_SEC` | float | `300` | `vdp/prefetch.py` | not set in tracked files | Wall-clock cap of the HTTP-first pass. |
| `SCANNER_VDP_HTTP_FIRST_SLOW_HOST_MAX` | int | `60` | `vdp/prefetch.py` | not set in tracked files | Pages per slow host in the HTTP-first pass. |
| `SCANNER_VDP_RECIPE_CAPTURE` | bool | `1` | `vdp/vdp_recipes.py` | not set in tracked files | Capture VDP recipes. |
| `SCANNER_VDP_RECIPE_MAX_SEC` | float | `300` | `vdp/vdp_recipes.py` | not set in tracked files | Wall-clock cap of VDP recipe replay. |
| `SCANNER_VDP_RECIPE_REPLAY` | bool | `1` | `vdp/vdp_recipes.py` | not set in tracked files | Replay VDP recipes. |
| `SCANNER_VIN_OWNER_GUARD_HOURS` | int | `48` | `database.py` | not set in tracked files | Window of the VIN owner-conflict guard. |
| `WINDOW_STICKER_GM_EXPERIMENTAL` | bool | unset | `post_scan/window_sticker.py` | not set in tracked files | Experimental GM window sticker path. |

## operator (37)

| Name | Type | Default | Readers | Set in | What it does |
|---|---|---|---|---|---|
| `DEALERS_FROM_SCANNABLE` | bool | unset | `manifest.py` | `deploy/nightly_http_refresh.sh` | Roster = the scannable-roster view (see SCANNABLE_ROSTER_RULE). |
| `DEALERS_MANIFEST_PATH` | path | `ROOT/dealers.json` | `constants.py`, `manifest.py` | `backend/scanner/cli.py`, `deploy/k8s/configmap.yaml`, `deploy/k8s/cronjob-discovery.yaml`, `scripts/scan_irvinebmw_test.sh` | Manifest file; the CLI --manifest flag sets it. |
| `RECIPES_DB_DISABLED` | bool | unset | `recipe_store.py` | `backend/scripts/platform_candidates.py` | Stop reading and writing recipes in the database. |
| `SCANNABLE_ROSTER_RULE` | bool | `1` | `manifest.py` | `backend/scripts/fleet_scan.py` | Apply the scannable-roster rule to rosters. |
| `SCANNER_ALLOW_BROWSER` | bool | unset | `browser_gate.py` | `Dockerfile.discovery`, `backend/scanner/discovery_capture.py`, `deploy/home_lane/run_home_lane.sh` | Allow browser (Playwright) paths; discovery only. |
| `SCANNER_DEALER_COM_BULK_FETCH` | bool | `1` | `scrapers/dealer_com_bulk_fetch.py` | `scripts/scan_irvinebmw_test.sh` | Use the dealer.com bulk inventory fetch. |
| `SCANNER_DEALER_TIMEOUT_SEC` | int | `10800` | `orchestrator.py` | `backend/scanner/cli.py` | Per-dealer wall-clock limit; CLI --dealer-timeout sets it. |
| `SCANNER_DELTA_CONCURRENCY` | int | `4` | `delta_scan.py` | `deploy/nightly_http_refresh.sh` | Delta-scan dealer concurrency (1-12). |
| `SCANNER_DELTA_DEALER_TIMEOUT` | int | `300` | `delta_scan.py` | `deploy/nightly_http_refresh.sh` | Delta-scan per-dealer timeout in seconds. |
| `SCANNER_EGRESS_TAG` | str | unset | `manifest.py`, `recipes.py` | `deploy/home_lane/run_home_lane.sh`, `scripts/railway_scan_fleet.sh` | Names the egress lane (for example railway); filters roster and recipe cache. |
| `SCANNER_FAST_MODE` | bool | unset | `scan_efficiency.py` | `deploy/k8s/cronjob-scanner-sharded.yaml`, `deploy/k8s/cronjob-scanner.yaml` | Apply the fast-mode env defaults. |
| `SCANNER_GALLERY_VISION_INLINE` | bool | unset | `scan_efficiency.py` | `backend/scanner/scan_efficiency.py` | Run gallery vision inline during the scan. |
| `SCANNER_GALLERY_VISION_POST` | bool | unset | `scan_efficiency.py` | `backend/scanner/cli.py`, `backend/scanner/scan_efficiency.py` | Run gallery vision after the scan; the CLI defaults it to 1. |
| `SCANNER_HTTP_ONLY` | bool | unset | `browser_gate.py` | `backend/scripts/dealer_pipeline.py` | Refuse every browser path. |
| `SCANNER_HTTP_PROXY` | str | unset | `http_fetch.py`, `net/ssrf.py` | `deploy/home_lane/run_home_lane.sh` | HTTP proxy for scanner fetches; also relaxes the SSRF guard. |
| `SCANNER_LISTING_FETCH_CHAIN` | bool | `1` | `post_scan/gap_fill.py` | `deploy/nightly_http_refresh.sh` | Gap fill uses the listing fetch chain. |
| `SCANNER_MAX_DEALER_CONCURRENCY` | int | unset | `inventory_write.py` | `backend/scanner/cli.py`, `deploy/k8s/cronjob-scanner-sharded.yaml` | Dealers scanned concurrently; CLI --dealer-concurrency sets it. |
| `SCANNER_MAX_VDP_CONCURRENCY` | int | unset | `scan_efficiency.py` | `backend/scanner/cli.py`, `scripts/scan_irvinebmw_test.sh` | VDP fetch concurrency; CLI --vdp-concurrency sets it. |
| `SCANNER_PARALLEL_UPSERT` | bool | `on for Postgres` | `inventory_write.py` | `deploy/k8s/cronjob-scanner-sharded.yaml` | Parallel inventory upserts; 0 or 1 overrides the backend default. |
| `SCANNER_POST_DEALER_RATINGS` | bool | `1` | `post_scan/pipeline.py` | `backend/scanner/scan_efficiency.py` | Post-scan dealer ratings stage. |
| `SCANNER_POST_DICT_ENRICH` | bool | `1` | `post_scan/pipeline.py` | `backend/scanner/scan_efficiency.py` | Post-scan dictionary enrichment stage. |
| `SCANNER_POST_ENRICH` | bool | `0` | `post_scan/pipeline.py` | `backend/scanner/scan_efficiency.py` | Post-scan LLM enrichment stage. |
| `SCANNER_POST_ENRICH_VISION` | bool | `0` | `post_scan/pipeline.py` | `backend/scanner/scan_efficiency.py` | Vision in the post-scan enrichment stage. |
| `SCANNER_POST_GAS_PRICES` | bool | `1` | `post_scan/pipeline.py` | `backend/scanner/scan_efficiency.py` | Post-scan gas price refresh stage. |
| `SCANNER_POST_INTERIOR_VISION` | bool | `1` | `post_scan/pipeline.py` | `backend/scanner/scan_efficiency.py` | Post-scan interior vision stage. |
| `SCANNER_POST_LISTING_DESCRIPTION` | bool | `1` | `post_scan/pipeline.py` | `backend/scanner/scan_efficiency.py` | Post-scan listing description stage. |
| `SCANNER_POST_LISTING_GAP_FILL` | bool | `1` | `delta_scan.py`, `post_scan/pipeline.py` | `backend/scanner/scan_efficiency.py` | Post-scan listing gap fill (also in the delta scan). |
| `SCANNER_POST_REPAIR` | bool | `1` | `post_scan/pipeline.py` | `backend/scanner/scan_efficiency.py` | Post-scan repair stage. |
| `SCANNER_POST_VPIC` | bool | `1` | `scan_efficiency.py` | `backend/scanner/scan_efficiency.py` | Post-scan NHTSA vPIC decode of new VINs. |
| `SCANNER_POST_WINDOW_STICKER` | bool | `1` | `post_scan/pipeline.py` | `backend/scanner/scan_efficiency.py` | Post-scan window sticker stage. |
| `SCANNER_RECONCILE` | str | `1` | `inventory_reconcile.py`, `pipeline/run.py` | `backend/scanner/pipeline/run.py` | 1 retires, 0 disables, dry records the plan and writes nothing. |
| `SCANNER_SCAN_ONLY` | bool | unset | `scan_efficiency.py` | `backend/scanner/cli.py`, `backend/scanner/scan_efficiency.py`, `deploy/k8s/cronjob-scanner-sharded.yaml`, `deploy/k8s/cronjob-scanner.yaml`, `scripts/scan_irvinebmw_test.sh` | Disable post-scan stages (the CLI --scan-only sets it). |
| `SCANNER_SHARD_COUNT` | int | unset | `manifest.py` | `deploy/car-scanner/cronjob.yaml`, `deploy/k8s/cronjob-scanner-sharded.yaml` | Number of disjoint scan shards. |
| `SCANNER_SHARD_INDEX` | int | unset | `manifest.py` | `deploy/car-scanner/cronjob.yaml` | This process's shard index (JOB_COMPLETION_INDEX fallback). |
| `SCANNER_VDP_COMPLETENESS_PASS` | bool | unset | `scan_efficiency.py` | `scripts/scan_irvinebmw_test.sh` | VDP completeness pass mode. |
| `SCANNER_WORKER_JOB_TYPES` | csv | unset | `job_queue.py` | `Dockerfile.discovery` | Job types this worker takes. |
| `SCAN_LANE` | str | unset | `manifest.py` | `backend/scripts/fleet_scan.py`, `deploy/home_lane/run_home_lane.sh` | Roster lane filter. |

## browser_era (26)

| Name | Type | Default | Readers | Set in | What it does |
|---|---|---|---|---|---|
| `SCANNER_GOTO_403_EXTRA_ATTEMPTS` | int | `2` | `phases/nav.py` | not set in tracked files | Extra browser goto attempts after a 403. |
| `SCANNER_GOTO_MAX_ATTEMPTS` | int | `3` | `phases/nav.py` | not set in tracked files | Browser goto attempts. |
| `SCANNER_HYDRATION_TIMEOUT_MS` | int | `30000` | `phases/nav.py` | not set in tracked files | Browser page hydration wait. |
| `SCANNER_INFINITE_SCROLL_MAX_ROUNDS` | int | `18` | `phases/nav.py` | not set in tracked files | Browser infinite-scroll rounds. |
| `SCANNER_INTERCEPT_COVERAGE_RATIO` | float | `0.92` | `scan_efficiency.py` | not set in tracked files | Browser intercept coverage ratio that ends the wait (0.5-1.0). |
| `SCANNER_INTERCEPT_MIN_ROWS` | int | `8` | `scan_efficiency.py` | not set in tracked files | Browser intercept rows needed to end the wait. |
| `SCANNER_INTERCEPT_URL_ALLOW` | csv | unset | `scrapers/scanner_intercept_filter.py` | not set in tracked files | Extra URL substrings the browser intercept allows. |
| `SCANNER_INTERCEPT_URL_DENY` | csv | unset | `scrapers/scanner_intercept_filter.py` | not set in tracked files | Extra URL substrings the browser intercept denies. |
| `SCANNER_INVENTORY_IDLE_SEC_FOUND` | int | `2` | `scan_efficiency.py` | not set in tracked files | Browser idle seconds once inventory was found. |
| `SCANNER_INVENTORY_IDLE_SEC_MISSING` | int | `6` | `scan_efficiency.py` | not set in tracked files | Browser idle seconds while inventory is missing. |
| `SCANNER_INVENTORY_JSON_TAIL_MS` | int | `2500` | `scan_efficiency.py` | not set in tracked files | Browser wait for trailing inventory JSON. |
| `SCANNER_INVENTORY_PATHS` | str | `core` | `scan_efficiency.py` | not set in tracked files | Inventory paths opened per dealer; extended = legacy nine-path sweep. |
| `SCANNER_INVENTORY_WAIT_MS` | int | `18000` | `phases/nav.py` | not set in tracked files | Browser wait for inventory responses. |
| `SCANNER_NETWORK_LEDGER` | bool | `1` | `network_observer.py` | not set in tracked files | Record the network ledger in the observer. |
| `SCANNER_NETWORK_LEDGER_AUTH` | bool | `0` | `network_observer.py` | not set in tracked files | Keep auth headers in the network ledger. |
| `SCANNER_PAGINATION_DEBUG` | bool | unset | `phases/nav.py` | not set in tracked files | Log browser pagination decisions. |
| `SCANNER_PAGINATION_RESPONSE_WAIT_MS` | int | `8000` | `phases/nav.py` | not set in tracked files | Browser wait for a pagination response. |
| `SCANNER_PROFILER_TIMEOUT_S` | int | `22` | `phases/site_profile.py` | not set in tracked files | Site profiler timeout. |
| `SCANNER_SCORE_INTERCEPT` | bool | `1` | `network_observer.py` | not set in tracked files | Score intercepted responses in the observer. |
| `SCANNER_SNIFF_MAX_BYTES` | int | `3000000` | `network_observer.py` | not set in tracked files | Largest response body the observer sniffs. |
| `SCANNER_SNIFF_NONJSON` | bool | `1` | `network_observer.py` | not set in tracked files | Observer also sniffs non-JSON bodies. |
| `SCANNER_VDP_DOWNLOAD_IMAGES` | bool | `0` | none | `backend/scanner/scan_efficiency.py`, `deploy/docker-compose.yml`, `deploy/k8s/configmap.yaml`, `deploy/railway/sync-vault-to-railway.sh` | Set by scan-only defaults; no reader left in the scanner. |
| `SCANNER_VDP_EP_MAX` | int | `0` | none | `backend/scanner/scan_efficiency.py`, `scripts/scan_irvinebmw_test.sh` | Set by fast-mode defaults; no reader left in the scanner. |
| `SCANNER_VDP_GALLERY_CAROUSEL_ONLY` | bool | `1` | `scan_efficiency.py` | not set in tracked files | Browser VDP gallery from the carousel only. |
| `SCANNER_VDP_GALLERY_MAX_URLS` | int | unset | `scan_efficiency.py` | not set in tracked files | Cap on browser VDP gallery URLs. |
| `SCANNER_VDP_PRICE_MAX` | int | `80` | none | `backend/scanner/scan_efficiency.py`, `scripts/scan_irvinebmw_test.sh` | Set by fast-mode defaults; no reader left in the scanner. |

## internal (8)

| Name | Type | Default | Readers | Set in | What it does |
|---|---|---|---|---|---|
| `ANTHROPIC_API_KEY` | str | unset | `job_diagnosis.py`, `post_scan/pipeline.py` | `deploy/docker-compose.yml`, `deploy/k8s/secret.yaml`, `deploy/railway/sync-vault-to-railway.sh` | Anthropic key; post-scan vision/enrichment stages and job diagnosis skip when empty. |
| `HTTPS_PROXY` | str | unset | `http_fetch.py` | not set in tracked files | Standard proxy variable; read after SCANNER_HTTP_PROXY. |
| `HTTP_PROXY` | str | unset | `http_fetch.py` | not set in tracked files | Standard proxy variable; read after HTTPS_PROXY. |
| `JOB_COMPLETION_INDEX` | int | unset | `manifest.py` | not set in tracked files | Kubernetes Indexed Job index; fallback for SCANNER_SHARD_INDEX. |
| `PGDATABASE` | str | unset | `recipes.py` | `scripts/pg_test_up.sh` | Fallback database name when a recipe DB URL omits it. |
| `PGHOST` | str | unset | `recipes.py` | not set in tracked files | Fallback host when a recipe DB URL omits it. |
| `PGPORT` | int | `5432` | `recipes.py` | `scripts/pg_test_up.sh` | Fallback port when a recipe DB URL omits it. |
| `SCANNER_WORKER_ID` | str | `worker-<hex>` | `job_queue.py` | `scripts/docker-entrypoint-jobs-worker.sh` | Job worker id. |
