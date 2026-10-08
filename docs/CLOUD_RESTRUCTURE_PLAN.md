# Cloud Restructure Plan

_Prepared 2026-09-09 against branch `feature/data-architecture-scan-reliability` (HEAD `da38f2dfe` + 116 uncommitted files). Every file:line below was read in this session; re-verify line numbers after the uncommitted work lands._

## Goal

Three deployables from one repo, one Postgres, no local filesystem as a store, no scraper code running inside the web process.

| Deployable | Image | Process | Today |
|---|---|---|---|
| **web** | `Dockerfile.web` | gunicorn `backend.main:app` | Railway, 1 replica, ~6GB RSS/worker, spawns scanners via `subprocess.Popen`, ships Chromium |
| **scanner-worker** | `Dockerfile.scanner` | `scripts/scanner_worker_loop.py` claims `dealer_jobs` | exists for docker-compose only; shells out to Node for onboarding |
| **scheduler + cron** | `Dockerfile.scanner` | `scripts/scanner_scheduler_loop.py` + CronJobs | launchd plists on one Mac mini, hardcoded `/Users/asarrafi`, nightly refresh disabled since 2026-08-05 |

Cross-cutting: Redis for rate limit and small caches, S3-compatible blob store for stickers/uploads/screenshots, migration chain as the only schema authority, all env reads through `backend/config.py`.

## Phase order and why

| Phase | Theme | Risk | Unblocks |
|---|---|---|---|
| 0 | Delete dead weight (Node scanner, SocketIO) | none | smaller images, honest dependency list |
| 1 | Split web vs scanner images and requirements | low | Chromium out of web, fast cold start |
| 2 | Move every background job to `dealer_jobs` | medium | web becomes stateless and horizontally scalable |
| 3 | Nightly pipeline to CronJobs | medium | scanning resumes on schedule, off the Mac mini |
| 4 | State out of process (rate limit, SQLite stores, caches, files) | high | multi-replica web, Railway volumes gone |
| 5 | Config, migrations, boot, auth blueprint | low | reproducible schema, one env surface |
| 6 | Monolith splits (opportunistic) | low | maintainability |

---

## Phase 0: delete dead weight

### 0.1 Retire the Node scanner (master-todo C1)

**Status 2026-09-09: implemented on branch `tmp/phase0` in a scratch worktree, not yet merged.** The replacement is `backend/scanner/onboard.py` (`onboard_dealer(url, …)`): discovery via `infer_dealer_from_url` + `crawl4ai_discovery` subprocess + `classify_dealer`, manifest upsert (or a temp manifest for trial scans), `scanner.py --dealer-id --scan-only` streamed with cancel/timeout, registry insert + car linking on success. Both `backend/dev/routes.py` job runners and `scripts/scanner_worker_loop.py` onboard jobs call it. `orchestrator.py` now prints `SCAN_VEHICLE_COUNT:N`; `dealers.smart_import_scrape_succeeded` recognises the Python scanner's summary lines. Tests: `backend/tests/test_scanner_onboard.py` (9) + 1 new case in `test_dev_dealers_manifest.py`.

Evidence: `backend/scanner/node_modules/` is empty; no Python imports Node code; two live paths still shell out to `node`.

| File | Change |
|---|---|
| `backend/scanner/scanner.js`, `backend/scanner/vdp_framework.js`, `backend/scanner/scanner_intercept.js`, `backend/scanner/package.json`, `backend/scanner/package-lock.json`, `backend/scanner/node_modules/` | Delete |
| `backend/artifacts/scanner_backups/scanner.js.bak`, `scanner.js.backup`, `backend/artifacts/scanner_backups/README.md`, `backend/artifacts/README.md:6,9` | Delete backups; drop the two README lines |
| `backend/scanner/config/scanner_intercept_policy.json:16` and `backend/config/scanner_intercept_policy.json:16` | Remove the "consumed by scanner_intercept.js" note (both copies must stay in sync, see memory note on intercept policy) |
| `scripts/scanner_worker_loop.py:39-58` (`_run_dealer_scan`) | Delete the `node scanner.js --url ... --smart-import` branch (:44-51). `onboard` jobs run `scanner.py --dealer-id <id> --scan-only` after Phase 2.2 gives `scanner.py` a `--url` onboarding path |
| `scripts/scanner_worker_loop.py:29-32` | Delete `PUPPETEER_EXECUTABLE_PATH` discovery (Puppeteer goes with Node) |
| `backend/dev/routes.py:440-537` (`_node_version_string`, `_probe_node_binary`, `_node_binary_cache`, `clear_node_binary_cache`, `_resolve_node_binary`, `_node_for_scanner`) | Delete. `GET /dev/api/status` (:975-980) and `_dev_status` (:572,:586) stop reporting a Node binary |
| `backend/dev/routes.py:604-648` (`_run_scanner_job`), `:650-848` (`_run_smart_import_job`), `:1092-1121` (`api_test_scanner`), `:1124-1155` (`api_smart_import`), `:1185-1256` (bulk queue), `:1290-1318` (skip-item) | Replaced in Phase 2.2, not merely deleted. Routes and JSON shapes stay |
| `backend/dev/routes.py:1069` | Rewrite the "Run node scanner.js to refresh debug/last_scrape_samples.json" instruction |
| `frontend/templates/dev.html:86`, `frontend/templates/admin/scanner_ops.html:63` | Replace `node scanner.js --url … --smart-import` copy with the Phase 2.2 job UI text |
| `Dockerfile.scanner:4-7` (NodeSource + nodejs), `:29-33` (`npm ci`, puppeteer smoke check) | Delete |
| `deploy/docker-compose.yml:88-92` (`scanner_node_modules` mount), `:129-131` (volume) | Delete |
| `scripts/docker-entrypoint-scanner-worker.sh:12-16` | Delete the `npm ci` bind-mount workaround |
| `README.md:101-108` ("8. Optional: Node"), `docs/SCANNER_NODE.md` | Delete section and file |
| `docs/DATA_QUALITY_ROLLOUT.md:15,29,61,77`, `master-todo.md:34` (C1) | Update references; mark C1 done |
| Docstring-only mentions (`backend/oem/scraper/crawl4ai_*` deleted 2026-10-01, `backend/dev/dealers.py:29,74,249`, `backend/db/repositories/dealers_repo.py:116`, `backend/intelligence/dealer_score.py:426`, `backend/scanner/vdp/config.py:26`, `backend/scanner/scrape_confidence.py:14`, `backend/scanner/job_diagnosis.py:67,76`, `run_nationwide_discovery.py:8`) | Reword "parity with scanner.js" comments; no logic change |

### 0.2 Remove Flask-SocketIO

**Status 2026-09-21: done in the working tree** (main.py, config.py, production_security.py, run.py, requirements.txt, SECURITY_MASTER_TODO rows).

Evidence: instantiated at `backend/main.py:1325`, zero `@socketio.on`, zero `emit`, zero frontend references. With 3 gunicorn workers and no message queue it could never work anyway.

| File | Change |
|---|---|
| `backend/main.py:129-156` (`_socketio_cors_allowed_origins`), `:1322-1333` (import, instantiate, `app.extensions["socketio"]`) | Delete |
| `backend/config.py:88-89` (`socketio_cors_origins_raw`) | Delete |
| `backend/utils/production_security.py:73-75` | Delete the `SOCKETIO_CORS_ORIGINS=*` warning |
| `run.py:8` (`from backend.main import app, socketio`), `:60-69` (`socketio.run(...)`) | Import only `app`; run `app.run(...)` |
| `requirements.txt:23` (`flask-socketio`) | Delete |
| `docs/SECURITY_MASTER_TODO.md:47,109,339,968,984` | Remove Socket.IO items |

### 0.3 Remove the OS file-manager shell-out

`backend/dealer/admin/incomplete_listings_api.py:92-105` (`_reveal_path_in_os_fs`) runs `open` / `explorer` / `xdg-open` inside the request (20s timeout) and is called at `:161`. Delete the function and the call. `export_incomplete_issue` keeps writing the export (to blob storage after 4.4, local `workspace/incomplete_exports/` until then, `:145-151`) and keeps the `export_path` response key because `frontend/static/dev.js:~984` reads `data.export_path`; add `export_text` alongside so the UI can offer a download without a server path. Routes `backend/dev/routes.py:1000-1007` and `backend/dealer/admin/operator_api.py:44-52` unchanged.

### 0.4 What the Node scanner did that Python must keep doing

Verified against `backend/scanner/scanner.js` and its Python consumer `backend/dev/routes.py:740-830`:

| Node capability | Python replacement (exists / to build) |
|---|---|
| `--smart-import` dealer metadata: name from JSON-LD `AutomotiveBusiness`/`AutoDealer`, `og:site_name`, `og:title`, address city/state (`scanner.js:2521-2620 discoverDealerMetadata`) | **Exists.** `scanner.js:2360 runCrawl4aiDiscoverySubprocess` already shells out to `backend/oem/scraper/crawl4ai_discovery.py` (`:167-203` parses the same JSON-LD/OG fields). Also `backend/dev/dealer_url_infer.py:321` (`og:site_name`) and `backend/discovery/address_enrich.py:28` (JSON-LD address; deleted 2026-10-01, unimported — `backend/scripts/backfill_dealership_addresses.py` is the live address filler). The new `scanner.py --url` path calls `crawl4ai_discovery` directly |
| `DISCOVERY:` / `SMART_IMPORT_RESULT:` / `SMART_IMPORT_ERROR:` stdout protocol (`dev/routes.py:712-736`) | Replace with `set_progress(job_id, {...})` from 2.1; worker writes the same dict the UI reads today |
| Post-scan persistence: `upsert_dealer_manifest_row`, `insert_dealership`, `link_cars_to_dealership_registry` (`dev/routes.py:782-798`, `backend/dev/dealers.py:61,247,264`) | **Exists.** Moves from the web thread into the worker's onboard handler unchanged |
| `--profile default / resilient / bare` retry ladder (`dev/routes.py:292,681`) | **Exists** as the recovery chain: `backend/scanner/dealer/profile.py:92 prioritize_recovery_chain`, `:104 manifest_recovery_strategies`, plus per-dealer `scan_hints` in `dealer_recipes` |
| `--headed` debugging | Drop. Headed Chromium cannot run in a pod; `SCAN_LAB_ENABLED` dev tooling covers local debugging |
| `debug/last_scrape_samples.json` (`scanner.js:1368-1390`, read only for its path and `generated_at` at `dev/routes.py:295,1067-1068`) | Drop the file; `GET /dev/api/status` reports the latest `scan_runs.finished_at` instead |
| Puppeteer stealth + intercept policy (`backend/scanner/config/scanner_intercept_policy.json`) | **Exists.** `backend/scanner/scrapers/scanner_intercept_filter.py` consumes the same JSON; `playwright-stealth` in `orchestrator.py` / `chain.py` |

Frontend contract to preserve for `GET /dev/api/scanner-job/<id>` and the import-queue endpoints (`frontend/static/dev.js:328,436,454,475`): `job_id`, `discovery`, `items`, `smart_error`, `log`, `insert_id`, `insert_error`, `cars_linked`, `exit_code`, `done`, `status`. The 2.2 mapping from `dealer_jobs` must emit exactly these keys.

---

## Phase 1: split deployables

### 1.1 Requirements files

Replace the single `requirements.txt` (`Dockerfile.web:17-18`, `Dockerfile.scanner:22-23` both install it) with three files.

| File | Contents (from import survey) |
|---|---|
| `requirements-base.txt` (new) | `python-dotenv`, `pydantic`, `requests`, `pillow`, `pgeocode`, `thefuzz`, `psycopg[binary]`, `pgvector`, `sentence-transformers`, `pypdf`, `urllib3`, `idna` |
| `requirements-web.txt` (new) | `-r requirements-base.txt`, `flask`, `gunicorn[gthread]`, `bcrypt`, `resend`, `pyotp`, `segno`, `redis`, `stripe`, `PyJWT[crypto]`, `pymupdf`, `sqlcipher3` (until Phase 4.2 finishes) |
| `requirements-scanner.txt` (new) | `-r requirements-base.txt`, `playwright`, `playwright-stealth`, `patchright` (currently undeclared; arrives only via `crawl4ai`), `fake-useragent`, `aiohttp`, `beautifulsoup4`, `duckdb`, `crawl4ai`, `curl_cffi`, and the crawl4ai transitive floors `langchain-core`, `langchain-openai`, `langchain-text-splitters`, `langsmith`, `pygments`, `starlette`, `tornado` |
| `requirements.txt` | Becomes `-r requirements-web.txt` + `-r requirements-scanner.txt` for local dev and CI |
| `requirements.txt:25` (`ollama`) | Drop. No `import ollama` exists under `backend/`; `backend/utils/local_llm.py:414` uses `shutil.which("ollama")` and `backend/vision/vlm_ollama.py` speaks HTTP |
| `.github/workflows/ci.yml:39-40` (pip cache key), `:43-45`, `:63-65` | Cache key on all three files; install `requirements.txt` |

### 1.2 Playwright out of the web image

Only one web feature reaches Playwright: car/compare chat web research (`backend/routes/cars_pages.py:1216-1217`, `:1298-1299`, `backend/routes/ai_chat_bp.py:222-223` → `backend/intelligence/ai/agent.py:1028,1345` → `backend/utils/web_researcher.py`).

| File | Change |
|---|---|
| `backend/utils/web_researcher.py:534-572` (`_brave_search_links`), `:574-` (`_fetch_with_playwright`), `:57-68` (`_playwright_launch_args`), `:360-` (`_make_context`) | Delete. Safe: `_collect_search_candidates` (`:499-531`) tries `duckduckgo_html_result_links` first and only falls back to Brave-via-Playwright; page fetch tries `fetch_page_text_http` (`:660`) first. Loss is recall on JS-only pages, not the feature |
| `backend/utils/car_chat_policy.py:10` (`web_research_playwright_allowed`) | Rename to `web_research_allowed`, keep the old name as an alias (imported by `backend/tests/test_car_chat_policy.py`); same env gates (`CAR_CHAT_WEB_RESEARCH`, `CAR_CHAT_WEB_RESEARCH_PUBLIC`). Importers: `backend/main.py:108`, `backend/routes/cars_pages.py:34`, `backend/routes/ai_chat_bp.py:26` |
| `backend/main.py:108` | Remove the three unused names (`car_chat_rate_limits`, `car_chat_user_daily_limit`, `web_research_playwright_allowed` are never referenced in `main.py`) |
| `backend/enrichment/spec_backfill.py:230-241` (`tier_b_vdp`) | No change. It lazy-imports `backend/scanner/vdp/spec_fetch.py:22-24`, which already warns and skips when Playwright is absent |
| `Dockerfile.web:4-12` (Chromium apt libs), `:21-22` (`playwright install`, `patchright install`) | Delete. Install `requirements-web.txt` at `:17-18` |
| `Dockerfile.scanner:10-17`, `:26-27` | Keep Chromium; install `requirements-scanner.txt` at `:22-23`; change `CMD ["python","scanner.py"]` (`:42`) to `ENTRYPOINT ["/app/scripts/docker-entrypoint-scanner-worker.sh"]` so a bare run is a worker; compose and CronJobs override |
| `.dockerignore` | Add `backend/dictionary/derived/brochure_text*/`, `workspace/`, `debug/`, `preview3/`, `preview4/`, `*.sql` dump, `*.db*` (the 5.5GB `backend/data` is already mostly excluded at `:26-34,54`) |

### 1.3 CI builds both images

`.github/workflows/ci.yml`: add a `docker-build` job after `lint` that runs `docker build -f Dockerfile.web .` and `docker build -f Dockerfile.scanner .` (no push). Catches a missing dependency before Railway does.

---

## Phase 2: every background job through `dealer_jobs`

### 2.1 Extend `backend/scanner/job_queue.py`

| Location | Change |
|---|---|
| `:93`, `:279`, `:530`, `:656` (`job_type` allow-lists `{onboard, refresh, rescan}`) | Single module constant `JOB_TYPES = {"onboard","refresh","rescan","enrich","vector_reindex","spec_backfill"}`; all four sites use it |
| `:47-59` (`dealer_jobs` DDL) | New migration `V021__dealer_jobs_cancel.sql`: `ADD COLUMN cancel_requested BOOLEAN NOT NULL DEFAULT FALSE`, `ADD COLUMN log_tail TEXT`, `ADD COLUMN progress_json TEXT`. Leave `TEXT` timestamps alone this round (`:342` lexical compare works on ISO strings) |
| new `request_cancel(job_id)`, `is_cancel_requested(job_id)`, `append_log(job_id, text, cap=4000)`, `set_progress(job_id, dict)` | Worker polls `is_cancel_requested` between dealers and terminates the child |
| `:116` (`claim_next_job`) | Add optional `job_types=` filter so a worker can be pinned (e.g. an enrich-only worker) |
| `:475` (`_has_active_job`) | Reuse for `vector_reindex` dedupe (today `backend/dev/routes.py:304-313` launches N concurrent full reindexes) |

### 2.2 Replace each in-process spawn

| Site | Today | Change |
|---|---|---|
| `backend/dealer/admin/routes.py:365-373,391` (`admin_dealer_rescan`, `POST /admin/dealers/<id>/rescan`) | `Popen(scanner.py --dealer-id)`, stdout to DEVNULL, no state | `enqueue_job(dealer_id, "rescan")`; response carries `job_id`; keep the `ALLOW_STORE_ADMIN_RESCAN` gate (`:384`). Delete `PROJECT_ROOT` (`:52`) if unused after |
| `backend/dev/console.py:45-46,219-233,369-374` (`POST /api/dev/scan-dealer`) | `subprocess.run` of `backend/scanner.py`, a path that does not exist, so silent no-op after 202 | `enqueue_job(dealer_id, "rescan")`; delete `_SCANNER_SCRIPT`, `_run_scanner_subprocess`, the thread |
| `backend/dev/routes.py:1092-1121` (`api_test_scanner`) + `:604-648` | Node `scanner.js --url` thread | `enqueue_job(dealer_id=<slug from url>, "onboard", payload={"url": url, "dry_run": True})` |
| `backend/dev/routes.py:1124-1155` (`api_smart_import`) + `:650-848` | Node `--smart-import` across 3 profiles, cancel via `watch_for_cancel` (`:668-680`) | `enqueue_job(..., "onboard", payload={"url": url})`. Manifest upsert (`:800-822`) moves into the worker-side onboard handler |
| `backend/dev/routes.py:1185-1256` (bulk queue), `:1259` (`GET import-queue/<id>`), `:1290-1318` (skip-item) | One thread walks the queue and runs jobs inline | Bulk = N `enqueue_job` calls sharing `payload["batch_id"]`; `GET` lists `list_recent_jobs(...)` filtered by batch; skip-item = `request_cancel(job_id)` |
| `backend/dev/routes.py:1158-1183` (`GET /dev/api/scanner-job/<id>`) | Reads `_dev_store` | Reads `get_job(job_id)` (`job_queue.py:445`) and maps `status/log_tail/progress_json` to the existing JSON keys `done`, `exit_code`, `log`, `discovery` |
| `backend/dev/routes.py:81,85,96-133,144-207` (`scanner_jobs`, `import_queues`, `dev_job_store` SQLite, `_dev_store_*`) | Module dict or SQLite job store | Delete. `DEV_JOB_STORE_SQLITE_PATH`, `DEV_MAX_SCANNER_JOBS`, `DEV_MAX_IMPORT_QUEUES` env vars retired |
| `backend/dev/routes.py:304-313` (`_vector_reindex_background`, `_spawn_vector_reindex_background`) | Daemon thread, undeduped | `enqueue_job(dealer_id="_global", "vector_reindex")` guarded by `_has_active_job` |
| `backend/dev/routes.py:1333-1346,1393-1434` (`_spawn_inventory_enrich`, `POST /dev/api/dev/enrich_all`) | Daemon thread, "check server logs" | `enqueue_job(dealer_id="_global", "enrich", payload={"limit","vision_only","max_workers"})`; 202 returns `job_id` |
| `backend/dev/scan_lab.py:449-531` + `backend/dev/scan_lab_routes.py:125-155` | `Popen(scanner_mac_mini.py)` against a private SQLite `workspace/inventory_92694.db` | Out of scope for cloud. Gate the whole blueprint on `SCAN_LAB_ENABLED=1` (default off) so it never registers in the web image. `backend/scanner/mac_mini_lite.py` stays dev-only |
| `backend/routes/cars_pages.py:291-355` (`_run_packages_ensure_with_budget`, `POST /api/cars/<id>/packages/ensure`) | Per-car thread with a 6s join budget, 4 module dicts | Keep for now (request-scoped, bounded). Note: state is per-process, so under 3 workers the `_packages_ensure_attempted_at` cooldown triples. Move to `enqueue_job("spec_backfill")` when Phase 4.9 lands Redis |
| `backend/main.py:250-302` (`_prewarm_listings_inventory_cache`) | Daemon thread at import time. Under `--preload` (`scripts/docker-entrypoint-web.sh:79`) it starts in the gunicorn master; threads do not survive `fork`, so workers may inherit a half-built cache | Move to a gunicorn `post_fork` hook in a new `gunicorn.conf.py`, or delete once Phase 4.8 externalizes the grid |
| `backend/dealer/admin/operator_api.py:63-70` (`_delegate_dev_operator_routes`) | Proxies `/api/admin/operator/*` to the dev handlers | No change; inherits the new behavior |

### 2.3 Worker side

| File | Change |
|---|---|
| `scripts/scanner_worker_loop.py:39-58` | Dispatch table by `job_type`: `refresh`/`rescan` → `scanner.py --dealer-id X --scan-only`; `onboard` → `scanner.py --url <url> --dealer-id <slug>` (new flag, below); `enrich` → `python -m backend.scripts.<enricher>` (whatever `InventoryEnricher().run_all` at `backend/dev/routes.py:1339-1346` wraps); `vector_reindex` → `backend.vector.pgvector_service.reindex_all` in-process |
| `scripts/scanner_worker_loop.py:83-140` (`_sync_dealer_sqlite_to_postgres`) | Delete. **Done 2026-10-01** (monolith audit phase 2). Inventory is Postgres-only (SEC-102); this reads a SQLite `cars` table that no longer exists in prod |
| `scripts/scanner_worker_loop.py:163-179` | Wrap the loop body in `try/except Exception: log.exception(); sleep` like the scheduler (`scripts/scanner_scheduler_loop.py:31-38`). Today one exception kills the worker |
| `scripts/scanner_worker_loop.py:62-69` | Stream child stdout to `append_log` instead of `capture_output=True` so the UI can tail; poll `is_cancel_requested` and `proc.terminate()` |
| `scripts/scanner_worker_loop.py` after `finish_job` (`:178`) | Call `record_scan_outcomes` (`backend/db/repositories/dealers_repo.py:15`) so `scan_runs` is written by queue-driven scans. Today only `backend/scanner/orchestrator.py:95` writes it and `backend/scanner/delta_scan.py` never does, which is why `scan_runs` stopped at 2026-07-21 while cars kept updating |
| `backend/scanner/cli.py` | Add `--url` (onboard a dealer from its inventory URL: `platform_registry` detect → `dealer_site_url`/`dealer_profile` → registry row → scan). Today the flags are `--dealer-id`, `--dealer-ids-file`, `--delta`, `--manifest`, `--scan-only`, `--shard-*`, and there is no URL entry point; that is what kept Node alive |
| `backend/db/repositories/schema_repo.py:138-141` (`ensure_scan_runs_table` returns early on Postgres) | Fine as-is because `backend/db/inventory_pg.py:525` creates `scan_runs` in the Postgres DDL; add a comment pointing there |

---

## Phase 3: nightly pipeline to CronJobs

### 3.1 One orchestrator script replaces three shell scripts

New `backend/scripts/nightly_pipeline.py` with subcommands mirroring the steps in `deploy/nightly_http_refresh.sh:149-181`, each with its own `--timeout`:

1. `delta` → `scanner.py --delta` with `DEALERS_FROM_SCANNABLE=1 SCANNER_DELTA_DEALER_TIMEOUT=900 SCANNER_DELTA_CONCURRENCY=8` (`:149-151`)
2. `harvest-carscommerce` → `backend/scripts/harvest_carscommerce.py` (`:165-166`)
3. `heal-from-recipes` → `backend/scripts/heal_from_recipes.py` (`:169-170`)
4. `harvest-html-jsonld` → `backend/scripts/harvest_html_jsonld.py` (`:176-177`)
5. `rebuild-listings-index` → `backend/scripts/rebuild_listings_index.py` (`:180-181`)
6. `data-quality` → `backend.scripts.data_quality_invariants --json` (`deploy/nightly_data_quality_invariants.sh:59`), report row written to a new `data_quality_runs` table instead of `workspace/data_quality/invariants_<date>.json`
7. `rooftop-refusals` → `rooftop_refusals_census --json` + `report_rooftop_refusals` (`deploy/nightly_rooftop_refusals.sh:70-78`), output to a table instead of `workspace/rooftop_refusals/`

| File | Change |
|---|---|
| `deploy/nightly_http_refresh.sh:45` (`REPO_ROOT=/Users/asarrafi/...`), `:128,:186` (`psql postgresql://localhost/cars`), `:60-61` (log/lock under `workspace/scanlogs/`) | Delete the script after the CronJobs are live. Until then, `REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"` and `psql "$INVENTORY_DATABASE_URL"` |
| `deploy/nightly_data_quality_invariants.sh:30`, `deploy/nightly_rooftop_refusals.sh:39` | Same `REPO_ROOT` fix, then delete |
| `deploy/com.sarraficars.nightly-*.plist` (3 files), `deploy/NIGHTLY_REFRESH.md:33,35,43,82,86` | Delete plists; rewrite the doc for CronJobs |
| `backend/scanner/scan_lock.py:18` (`workspace/scanner.lock` PID file) | Replace with `pg_advisory_lock` (`SELECT pg_try_advisory_lock(hashtext('scanner:'||scope))`) so two pods cannot both run `--delta` |

The 25h step-1 runtime on 2026-08-03 means step 1 needs `SCANNER_DELTA_CONCURRENCY` sharding across N pods (the `cronjob-scanner-sharded.yaml` Indexed Job pattern) rather than one process.

### 3.2 Kubernetes manifests (`deploy/k8s/`)

| File | Change |
|---|---|
| `cronjob-scanner.yaml:30` | `["python","scanner.py","--delta"]`; env from 3.1 step 1 |
| `cronjob-scanner-sharded.yaml:31-44` | Uncomment in `kustomization.yaml:16`; this is the delta shard runner |
| `cronjob-post-scan.yaml`, `cronjob-enrichment.yaml`, `cronjob-discovery.yaml` | Keep; point at `nightly_pipeline.py` subcommands where they overlap |
| new `cronjob-nightly-harvest.yaml`, `cronjob-nightly-heal.yaml`, `cronjob-nightly-index.yaml`, `cronjob-data-quality.yaml`, `cronjob-rooftop-refusals.yaml` | One CronJob per step, `activeDeadlineSeconds` per step, `concurrencyPolicy: Forbid` |
| new `deployment-scanner-worker.yaml`, `deployment-scanner-scheduler.yaml` | Worker `replicas: 2`, scheduler `replicas: 1`, both `Dockerfile.scanner` with the compose entrypoints (`deploy/docker-compose.yml:102,:123`) |
| new `job-migrate.yaml` | `python -m backend.scripts.migrate --apply`, run by `kubectl apply` before the web rollout (see 5.2) |
| `cronjob-scanner.yaml:28`, `cronjob-post-scan.yaml:25`, `cronjob-enrichment.yaml:25` (`REGISTRY/dealership-scanner:latest`) vs `deployment-web.yaml:27`, `cronjob-discovery.yaml:29` (`docker.io/library/…`) | One image ref via a kustomize `images:` block |
| `configmap.yaml:21` (`DATABASE_URL` with `$(POSTGRES_USER)` substitution, which ConfigMaps do not expand; references a `postgres-service` that has no manifest) | Move `INVENTORY_DATABASE_URL` to the Secret; drop `INVENTORY_DB_PATH`, `DEALER_PORTAL_DB_PATH`, `USERS_DB_PATH` (`:8-30`) once Phase 4 lands |
| `secret.yaml:15-24` | Replace placeholder secrets with `VAULT_TOKEN` only (the kmac-vault pattern already used on Railway, `deploy/railway/README.md:18`) |
| `pv-data.yaml`, `pvc-data.yaml`, every `/data` mount | Delete after Phase 4.10 (blob storage) and 4.2 (no SQLite) |
| `ollama.yaml` | Add to `kustomization.yaml:7-19` or delete; today it is orphaned |
| `deployment-web.yaml:9-12` (`replicas: 1`, `Recreate`) | `replicas: 2`, `RollingUpdate` after Phase 4 |

### 3.3 Railway

`railway.toml` builds only `Dockerfile.web`; no scanner runs on Railway and nothing moves inventory rows from the Mac mini to Railway Postgres (`deploy/railway/README.md:97-127`).

| File | Change |
|---|---|
| new `deploy/railway/scanner-worker.railway.toml` (or a second service in the dashboard pointed at `Dockerfile.scanner`) | Worker service, `startCommand` = worker entrypoint, same `INVENTORY_DATABASE_URL` + `VAULT_TOKEN` |
| new cron service(s) using Railway's cron schedule on the scanner image | One per `nightly_pipeline.py` subcommand |
| `deploy/railway/sync-vault-to-railway.sh:62-70,118,134` | Normalize to one CLI form (`railway variables --set`); drop `USERS_DB_PATH`, `DEV_USERS_DB_PATH`, `DEALER_PORTAL_DB_PATH` (`:85-89`) after Phase 4.2 |
| `deploy/railway/README.md:97-127` | Document: migrations via `job-migrate` equivalent (`railway run python -m backend.scripts.migrate --apply`), scanner service, and that the Railway Postgres is the only inventory DB (retire the Mac mini `postgresql://localhost/cars`) |

Decision needed (D1): Railway for everything, or Railway web + K8s scanners as `docs/PLATFORM_MASTER_PLAN.md:40` proposes. The plan above works for either; the K8s manifests already exist.

---

## Phase 4: state out of the process

### 4.1 Rate limiter to Redis

`backend/utils/ip_rate_limit.py` has two backends: memory (`:17`, `:91-107`) and SQLite via `RATE_LIMIT_SQLITE_PATH` (`:27-29`, `:81-88`, `:119`). 13 import sites call only `allow_request` / `clear_rate_limit_state`.

| File | Change |
|---|---|
| `backend/utils/ip_rate_limit.py` | Add a third backend: Redis sorted-set sliding window (`ZADD`/`ZREMRANGEBYSCORE`/`ZCARD`), selected when `Config.redis_url()` (`backend/config.py:100`) is set. Fallback order Redis → SQLite → memory |
| `scripts/docker-entrypoint-web.sh:59-63` | Delete the `RATE_LIMIT_SQLITE_PATH=/app/data/rate_limits.db` default |
| `backend/routes/health.py:34-37` | `/api/ready` already probes Redis lazily; report `rate_limit_backend` |

### 4.2 Users, dev users, dealer portal, incomplete listings to Postgres

`docs/USERS_PG_CUTOVER.md` is the spec; nothing at runtime has changed (`:3-4`). `users` table in Postgres has 0 rows today.

**Prerequisite 4.2.0: a Postgres test harness.** The suite runs on SQLite by default: `backend/tests/conftest.py:58-62` deletes `INVENTORY_DATABASE_URL` and sets `INVENTORY_SQLITE_TESTS=1` for every test, `conftest.py:105-106` does the same for the session init, 44 test files monkeypatch `USERS_DB_PATH` / `DEALER_PORTAL_DB_PATH` / `INCOMPLETE_LISTINGS_DB_PATH` / `INVENTORY_DB_PATH`, and zero tests carry `pytest.mark.integration`. Deleting any SQLite branch before this exists deletes the test coverage for that store. Build first:

| File | Change |
|---|---|
| `.github/workflows/ci.yml` `pytest-integration` job (`:56-74`) | Add `services: postgres:16` with `INVENTORY_DATABASE_URL` pointing at it |
| `backend/tests/conftest.py` | New `pg_inventory` fixture: creates a scratch schema, runs `backend.scripts.migrate --apply`, yields the DSN, drops the schema. Tests that exercise dual-mode stores get parametrized `("sqlite", "postgres")` |
| `pytest.ini` | Register the `integration` marker (already scaffolded per `CHANGELOG.md`) and tag the parametrized Postgres cases |

Only after that do the "delete the SQLite branch" rows below apply. Until then every dual-mode store keeps both branches and the SQLite branch stays test-only.

| File | Change (from the cutover doc, verified against source) |
|---|---|
| `backend/db/users_db/_common.py:20-21` (`get_conn` → always `get_users_conn()`) | Branch on new `USERS_DB_BACKEND=postgres` (explicitly not `is_inventory_postgres()`, `USERS_PG_CUTOVER.md:50-74`) → `inventory_compat` connection |
| `backend/db/users_db/_common.py:24-46` (`PRAGMA table_info(users)` under `lru_cache`) | `users_table_columns()` helper using `inventory_pg.pg_table_columns` on Postgres |
| `backend/db/users_db/_common.py:100-103` ("database is locked" retry) | Dead on Postgres; guard by backend |
| `backend/db/users_db/accounts.py`, `auth.py` (`create_org`, `save_user`, `save_oauth_user`, `save_apple_oauth_user` use `cursor.lastrowid`) | `RETURNING id` |
| `backend/db/users_db/admin.py` (`list_users_for_admin` does `(email_verified_at or "").strip()`), `auth.py` (`get_user_email_verification_state`) | Timestamp read-side adaptation (`USERS_PG_CUTOVER.md:69-72`) |
| `backend/db/search_analytics_db.py:20-53` (`json_extract`, `datetime('now')`, DDL at import), `backend/db/user_history_db.py:11-58` (DDL at import) | Branch queries; move DDL out of import time (V013 already creates these tables) |
| `backend/db/admin_users_db.py:29-68` (legacy copy from `users.db`), `backend/db/dev_users_sqlite.py` | New `V022__admin_users.sql` + Postgres branch; keep SQLCipher path only for local dev |
| `backend/db/dealer_portal_db.py:55-67,103` | Already dual-mode. After soak, delete the SQLite branch and `DEALER_PORTAL_DB_PATH` |
| `backend/db/incomplete_listings_db.py:42-54` | Same |
| `backend/db/repositories/base_repo.py:23-41` (import-time SQLite probe of `backend/inventory.db`) | Skip the probe unless `INVENTORY_SQLITE_TESTS` is set (`backend/db/inventory_compat.py:124-129` is the only legitimate SQLite consumer). Keep `_sqlite_connect_raw` (`:63-73`): the whole test suite goes through it |
| `backend/scripts/migrate_users_sqlite_to_postgres.py` (`--apply`), `migrate_dealer_portal_sqlite_to_postgres.py` | Run once in the maintenance window; keep `users.db` as rollback |
| `backend/scraping/sources.py:11-27` (`load_roots_from_db` opens SQLite) | Use `inventory_db.db_conn()`; single caller `backend/scraping/cli.py:28-31` |
| `backend/enrichment/dictionary_catalog.py:293,378` (`dictionary_catalog.db`) | Keep as a **read-only build artifact** baked into both images (`DICTIONARY_CATALOG_DB_PATH`, `backend/enrichment/dictionary_paths.py:29-33`); add a `RUN python -m backend.enrichment.dictionary_catalog --rebuild` step to both Dockerfiles. Not user state, so not a cloud blocker |
| `backend/main.py:233-236` (`init_users_db`, `init_admin_db`, `init_dealer_portal_db`) | Become no-ops on Postgres once the chain owns those tables (5.2) |

Blocking decision (D2, from `USERS_PG_CUTOVER.md:76-93`): SEC-088 for plaintext `users.totp_secret` in a shared-DSN Postgres. Recommend app-level encryption of `totp_secret` and `mfa_phone` with `USERS_DB_ENCRYPTION_KEY`, which the vault already supplies (`backend/utils/kmac_vault.py:25-34`).

### 4.3 Caches

| Cache | Location | Change |
|---|---|---|
| Full listings JSON, pre-gzipped, ~109MB | `backend/routes/listings_api.py:139-152,186-208` (`_cars_json_cache`) | Server-side pagination on `/api/listings/cars` (the entrypoint comment at `scripts/docker-entrypoint-web.sh:37-46` already says this). Frontend consumer is `frontend/static/listings_boot.js` |
| Listings grid memo family, rebuilt on a `grid-cars-refresh` daemon thread | `backend/db/repositories/listings_repo.py:366-400,~687` | Precompute into a `listings_grid` table by `backend/scripts/rebuild_listings_index.py` (nightly step 5) plus an incremental refresh job after each dealer scan; web reads pages from the table. This is the ~6GB/worker item |
| `_nearby_cache`, `_dealer_geo_index_cache`, `_featured_cars_cache` | `listings_repo.py:135-138,162-166,693-694` | Keep (small, TTL-bounded); note they are per-worker |
| `_reco_cache` | `backend/main.py:1247-1248`, invalidated at `backend/routes/home_dashboard.py:102-105` | Redis hash keyed by user, TTL 120s |
| `_live_gas_prices_cache` reading `backend/dictionary/derived/live_gas_prices.json` | `backend/main.py:1251-1255` | `backend/cron/sync_gas_prices.py` writes a `gas_prices` table; web reads it |
| `_epa_makes_cache` (process-lifetime, no invalidation) | `backend/routes/dealership_page.py:145-170` | Fine; static reference data |
| `_cache` LRU of LLM narrations | `backend/routes/ai_narrate_bp.py:42-63` | Redis with the same content-hash key, or leave per-worker (cost is only duplicate LLM calls) |
| `knowledge_engine` `lru_cache`s | `backend/enrichment/knowledge_engine.py:20,489,518,577,747,876,1673` | Keep; `epa_master` is static |

### 4.4 Filesystem stores to blob storage

New module `backend/storage/blob.py` with `put(key, bytes, content_type)`, `get(key)`, `exists(key)`, `url(key)`, backends `local` (dev, default) and `s3` (`BLOB_S3_BUCKET`, `BLOB_S3_ENDPOINT`, works for R2/S3/MinIO). Uses `boto3` (add to `requirements-base.txt`).

| Store | Location | Change |
|---|---|---|
| Window stickers (`car_window_stickers/<key>/<vin>/window_sticker.*`, preview PNG) | `backend/enrichment/window_sticker_service.py:22-23,93-110,164-167,182,225,776-793,1137-1140` | All writes through `blob.put`; reads through `blob.get`; serving route returns a signed URL or streams |
| Comment images | `backend/utils/comment_images.py:97-159` | `upload_dir()` → blob keys; keep `_STORAGE_KEY_RE` validation |
| Dealer vehicle uploads | `backend/dealer/routes.py:169,269,285` | Same |
| Scan recipes on disk (`workspace/recipes/*.json`, `_aliases.json`, `_store.json`) | `backend/scanner/recipes.py` (`RECIPES_DIR`, `_recipe_path`, the `_aliases.json` reads, `load_recipes`, `save_recipes`, the `_store.json` tag), `backend/scanner/recipe_synth.py` (its `RECIPES_DIR` now resolves to `recipes.RECIPES_DIR`) | Already DB-backed: `load_recipes` reads `recipe_store.db_load_recipes` and `save_recipes` writes through to `db_save_recipes`. **Corrected 2026-10-08 (P1C.6):** this row used to say `load_recipes` "prefers the newer copy". It compares the file's max `saved_at` with the row's `max_saved_at`: a newer row is adopted into the file, a newer file is pushed up into the row (logged as a WARNING), and on equal stamps the file wins and nothing is written. Before Phase 0 (VERSION 1.5.2 and older) `saved_at` was only set when a set was created, so stale flags and un-stales never propagated and pipeline-synthesized or cascaded sets (saved with `saved_at=0`) lost to any older file. Since P1B.3 `save_recipes` stamps every write, and since P1B.6 a cache tagged for another store never pushes up. The rules are in `docs/data_architecture_plan.md` ("Recipe cache and store sync"). Import any file-only recipes once with `backend/scripts/import_recipes_to_db.py`, run only against the store the cache mirrors (since P1C.2 it wraps `reconcile_recipe_store.py --cache-dir`: it merges per recipe, refuses a cache tagged for another store and still merges an untagged one; see `docs/RAILWAY_SCANNING.md`), then delete the file branch. `backend/tests/test_recipes.py` monkeypatches `RECIPES_DIR`, so keep a `RECIPE_BACKEND=file` test mode or rewrite that test against the DB fixture from 4.2.0. 15 importers of `recipes.py` are unaffected (same function names) |
| Unclassified platform log | `backend/scanner/platform_registry.py:75,525,540,550` | Table `unclassified_platforms(host PK, first_seen, last_seen, sample_url)`. Update its one reader, `backend/scripts/classify_dealers.py` |
| Failure screenshots, HAR captures, network ledgers | `backend/scanner/phases/dealer_run.py:963-964` (`fail_<dealer>.png`), `backend/scanner/phases/nav.py:293-294` (`fail_<dealer>_<ts>.har`), `backend/scanner/network_observer.py:743-745` (`netledger_*.json`), `backend/scanner/orchestrator.py:332-337`; constants at `backend/scanner/constants.py:11-12`, re-exported by `backend/scanner/cli.py:496` and the root `scanner.py` shim | Keep the `DEBUG_DIR` / `WORKSPACE_DEBUG_DIR` names (`backend/tests/test_network_observer.py` uses `DEBUG_DIR`) but make them a `blob` prefix `debug/<dealer_id>/` when `BLOB_BACKEND=s3`, local dir otherwise |
| Scan JSONL log (`workspace/scanlogs/scan_<ts>.jsonl`) | `backend/scanner/scan_log.py` (writer), `backend/scanner/orchestrator.py:280`, **read by** `backend/scanner/cli.py:130-137` (`--retry-failed-from` re-queues dealers whose `dealer_summary` row failed) and `workspace/analyze_scan.py` | Functional, not just a log. Write `scan_start` / `dealer_summary` rows to a new `scan_events` table (`V023`) in addition to stdout; `--retry-failed-from` accepts a scan run id and reads the table. Keep the file writer behind `SCAN_LOG_FILE=1` for local analysis |
| Locally downloaded car images | `backend/routes/site_misc.py:32,45` (`_CAR_IMAGES_DIR` served via `send_from_directory`), written when `SCANNER_VDP_DOWNLOAD_IMAGES=1` (`deploy/railway/sync-vault-to-railway.sh:91` sets it to 0 in prod) | Blob prefix `car_images/`; serving route streams from blob or redirects to a signed URL |
| Serving routes that assume local files | `backend/routes/cars_pages.py:393,811` (sticker PDF / preview), `backend/dealer/routes.py:456` (vehicle uploads), `backend/routes/community_api.py:419` (comment images), `backend/routes/site_misc.py:32,45` | Each switches from `send_from_directory` / `send_file` to `blob.get` streaming (or a signed-URL redirect for public assets) |
| Brochure PDFs and text index | `backend/enrichment/brochure_sources/storage.py:160-166,270,284-285,308-312,344-347`, `backend/dictionary/derived/brochure_content_index.json` (355 absolute `/Users/asarrafi` paths) | PDFs to blob; regenerate the index with repo-relative paths; `_tier_by_sha` resolves via `blob.exists` |
| Manifests (`dealers.json`, `workspace/manifest_*.json`) | `backend/scanner/constants.py:10`, `backend/scanner/mac_mini_lite.py:21` | `DEALERS_FROM_ACTIVE_INVENTORY=1` is already the nightly roster source (memory: nightly delta roster staleness fix). Make it the default; manifests become a dev-only override |

---

## Phase 5: config, migrations, boot, auth blueprint

### 5.1 Config (master-todo A1)

414 direct env reads outside `backend/config.py`; `backend/scanner` alone has 166. Do it per file, highest count first, defaults unchanged:

`backend/scanner/post_scan/pipeline.py` (21), `backend/scanner/vdp/config.py` (20), `backend/scanner/scan_efficiency.py` (19), `backend/utils/mfa_delivery.py` (17), `backend/utils/kmac_vault.py` (15, leave: it runs before Config), `backend/scanner/phases/nav.py` (14), `backend/scanner/cli.py` (11), `backend/scanner/phases/dealer_run.py` (10), `backend/dev/routes.py` (10), `backend/auth/apple_oauth.py` (10).

Add to `backend/config.py`: `users_db_backend()`, `blob_backend()`, `scan_lab_enabled()`, `job_queue_enabled()`, `listings_prewarm()`; keep the eager/lazy split at `:43-59` / `:66-101`.

### 5.2 Migrations as the only schema authority (master-todo B2)

| File | Change |
|---|---|
| `scripts/docker-entrypoint-web.sh:21-24` | Make `migrate --apply` failure fatal (`exit 1`) instead of WARN; remove it entirely once `job-migrate.yaml` / `railway run` owns it, so 3 preloaded masters across replicas never race |
| `backend/scripts/migrate.py` | No behavior change. Use `--baseline 18` on the local DB, where V014–V018 tables were created by `init_postgres_inventory` (`backend/db/inventory_pg.py:287-696`) and are unrecorded (only 13 rows in `schema_migrations`) |
| `migrations/V019__missing_reference_tables.sql` | pg_dump-style, unguarded `CREATE FUNCTION` / `CREATE TABLE public.x`; fails on any DB that already has them. Split into `V019a` guarded (`CREATE OR REPLACE FUNCTION`, `CREATE TABLE IF NOT EXISTS`) before it is ever applied, since applied files are immutable (`migrate.py:77-86,133-137`) |
| `backend/db/inventory_pg.py:287-696` (`init_postgres_inventory`, 15 `CREATE TABLE IF NOT EXISTS` + 4 `pg_add_columns`) | Behind `WEB_BOOT_DDL=1` (default 0 in prod); the chain is authoritative. Keeps the `lock_timeout` guard at `:308`. **Prerequisite:** prove the chain is complete first. Apply V001–V020 to scratch DB A, run `init_postgres_inventory` on scratch DB B, diff `information_schema.columns` and `pg_indexes`; every column or index only B has becomes `V021__chain_catchup.sql` before the flag flips. `backend/scripts/dump_baseline_schema.py:6` already notes the two "drifted apart". The scanner calls the same function (`backend/scanner/database.py:127-134`) and gets the same flag |
| `backend/main.py:226-240` (`assert_*`, `init_*_db`, `init_job_queue_schema` at import) | Move into a `create_app()` factory in `backend/app_factory.py`; `backend/main.py` keeps `app = create_app()` so `backend.main:app` still works. Boot DDL calls wrapped by the same `WEB_BOOT_DDL` flag |
| `deploy/up.sh:49-51` (schema init after health) | Replace with `docker compose run --rm web python -m backend.scripts.migrate --apply` before `up -d web` |
| `backend/scripts/dump_baseline_schema.py:6` | Regenerate after the chain is trusted so the two stop drifting |

### 5.3 Auth routes out of `backend/main.py` (master-todo A2)

Move `backend/main.py:740-1195` (18 routes) plus helpers `_account_profile_context` (`:899-920`), `_auth_user_payload` (`:1058-1068`), `_app_mfa_gone` (`:1163-1185`), `_finalize_app_session` (`:1196-1241`) to `backend/auth/routes.py` as `auth_bp` with no prefix. Endpoint names and URLs unchanged; register in the factory. `main.py` keeps the `after_request`/`before_request`/`context_processor` hooks (`:374,:404,:481,:486,:618,:690`) and the template filters. Expected size after 0.2, 2.2, 5.2, 5.3: roughly 500 lines.

---

## Phase 6: monolith splits (opportunistic, when touched)

| File | Lines | Seam |
|---|---|---|
| `backend/scanner/post_scan/window_sticker.py` | 2,466 | fetch (`:299-310` pdftotext) / parse / persist |
| `backend/enrichment/brochure_extract.py` | 2,402 | already has `brochure_sources/` (17 modules); move extraction stages there |
| `backend/enrichment/brochure_trim_candidates.py` | 2,303 | candidates / scoring / output |
| `backend/enrichment/knowledge_engine.py` + `knowledge_engine_specs.py` | 3,325 | lookups by store (epa, extended, engine, aggregate) mirroring the 7 `lru_cache` groups |
| `backend/oem/scraper/oem/bmw_locator_discovery.py` | 1,890 | DELETED 2026-10-01 (unreachable; see docs/monolith_audit_2026_10_01/enrich.md #12) |
| `backend/scanner/recipe_synth.py` | 1,527 | synth / verify / persist (persist collapses into `recipe_store`) |
| `backend/dev/routes.py` | 1,482 → much smaller after Phase 2.2 removes ~450 lines of job plumbing | status / dealers / imports / enrich blueprints |
| `frontend/static/main.js` | 3,280 | continue the `listings_boot`/`market_intel`/`geo` extraction |
| `frontend/static/car_page.js` | 2,799 | gallery / packages / spin already have sibling files (`car_packages.js`, `car_spin.js`; the old `car_chat.js` was dead and deleted 2026-10-01); move the rest |
| `frontend/templates/car.html` | 1,555 | Jinja includes per tab |

---

## Preservation audit (2026-09-09)

Every deletion in this plan was checked for consumers in `backend/`, `scripts/`, `frontend/`, `workspace/*.py`, and `backend/tests/`. Items that changed as a result:

| Plan item | What the first draft would have broken | Fix in this revision |
|---|---|---|
| 0.1 Node scanner | Dealer metadata discovery, `--profile` retry ladder, the `dev.js` job JSON contract | 0.4 maps each Node capability to its existing Python module; contract keys listed |
| 0.3 export reveal | `dev.js` reads `export_path` | Key kept; `export_text` added |
| 1.2 web researcher | Nothing: DDG HTML search runs first | Documented why it is safe |
| 4.2 SQLite branches | Whole test suite runs SQLite; 44 files monkeypatch DB paths | New 4.2.0 Postgres test harness gates every SQLite deletion |
| 4.4 scan JSONL log | `scanner.py --retry-failed-from` reads it | `scan_events` table; file writer kept behind a flag |
| 4.4 `DEBUG_DIR` constants | HAR + netledger writers, `cli.py:496` re-export, `test_network_observer.py` | Names kept; semantics become a blob prefix |
| 4.4 unclassified log | `backend/scripts/classify_dealers.py` reads it | Reader listed |
| 4.4 local car images | `site_misc.py` serving route was not in the plan | Added, with all five `send_from_directory` / `send_file` sites |
| 5.2 boot DDL flag | Chain may lack columns that `init_postgres_inventory` adds | Scratch-DB diff and `V021__chain_catchup.sql` before the flag |

Verified safe with no consumer: SocketIO (no handlers, no emits, no tests), `ollama` package (no SDK import), `_sync_dealer_sqlite_to_postgres` (scanner already writes Postgres via `backend/scanner/database.py:120-124`), the three unused imports at `backend/main.py:108` (tests import from `car_chat_policy` directly), `scan_lock` (single caller `orchestrator.py:212-226`).

Tests that name things this plan renames or moves, to update in the same commit: `backend/tests/test_car_chat_policy.py` (`web_research_playwright_allowed`, `car_chat_user_daily_limit`), `test_network_observer.py` (`DEBUG_DIR`), `test_recipes.py` (`RECIPES_DIR`), `test_fuel_lookup.py` (`_live_gas_prices_cache`), `test_store_admin.py` (`record_scan_outcomes`, unchanged signature).

Not found: the `stack_up` fixture that `.github/workflows/ci.yml:51-55` describes does not exist in `backend/tests/conftest.py`, and no test is marked `integration`. The integration CI job currently runs nothing.

## Decisions the plan needs from you

- **D1** Hosting target: all-Railway (web + worker + cron services) or Railway web + K8s scanners (`docs/PLATFORM_MASTER_PLAN.md:40`).
- **D2** SEC-088: how `totp_secret` is protected once users live in the shared Postgres (recommend app-level encryption with the existing vault key).
- **D3** Listings grid: precomputed `listings_grid` table (recommended) vs Redis-cached serialized blob. Table survives restarts and is queryable; Redis is faster to ship.
- **D4** Blob backend: Cloudflare R2 (already behind Cloudflare, no egress fees) vs S3.
- **D5** Scan Lab (`backend/dev/scan_lab.py`, `scanner_mac_mini.py`): keep as Mac-only dev tooling behind a flag, or port to the job queue later.

## Verification per phase

- Phase 0/1: `docker build -f Dockerfile.web .` has no Chromium layer; `pip check` in each image; `pytest backend/tests -q` green (3,018 collected today); `/api/health` 200.
- Phase 2: `POST /admin/dealers/<id>/rescan` creates a `dealer_jobs` row, a worker claims it, `scan_runs` gains a row, `GET /admin/scans` shows it; `kill -9` a worker mid-job leaves the row `running` and the next claim does not double-run it.
- Phase 3: CronJob run of `delta` finishes inside its deadline; `cars.scraped_at` advances for every scannable dealer; nightly `data_quality_runs` row present.
- Phase 4: `GUNICORN_WORKERS=3` and two web replicas share one rate-limit bucket; RSS per worker under 1.5GB after D3; no path under the repo is written at runtime (`strace`/`fs_usage` or a read-only root filesystem in the pod spec).
- Phase 5: `migrate --apply` on a fresh Postgres from V001 to head yields a schema equal to `dump_baseline_schema.py` output; `WEB_BOOT_DDL=0` boots clean.
