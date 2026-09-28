# HTTP-only scans, browser for discovery only — plan

Decision (2026-09-26, user): from here on every scan is HTTP-only. Headless browsing stays
only as a **discovery** tool: learning how a new/unknown site presents its inventory
(endpoint, auth, pagination) so a recipe can be written. Nothing in the scan path may open
Chromium.

Companion: `docs/NETWORK_SCAN_PROCESS.md` (the per-dealer loop), `workspace/dealer_logs/_learning/platform_playbook.md`.

## Where we are (measured 2026-09-26)

- 459 active dealers; 454 have live HTTP recipes; fleet scans have run with `SCANNER_HTTP_ONLY=1` since 09-23.
- 3 dealers have no HTTP path yet (mcpeeks-com, quantumautosales-com, honestcardeal-com) — exactly the case discovery-with-a-browser is for.
- **But "HTTP-only" is not browser-free today.** Audit of `backend/scanner/**` (2026-09-26) found seven places a browser still runs or can run under `SCANNER_HTTP_ONLY=1`:

| # | where | what happens today |
|---|---|---|
| 1 | `backend/scanner/orchestrator.py:305` | Chromium is launched for every run, unconditionally; the scan holds a headless Chromium process for its whole life |
| 2 | `backend/scanner/phases/dealer_run.py:256-257` | `browser.new_context()` + `new_page()` per dealer (an about:blank page) |
| 3 | `dealer_run.py:655-669` → `inventory_recovery.py` | `recover_inventory(page=…)` runs with no HTTP-only guard; six of eight strategies read/navigate the page (`dealer_on_cosmos`, `dealer_inspire_algolia`, `dealer_venom_typesense`, `pixel_motion_html`, `dealer_eprocess_json`, `html_next_data`) |
| 4 | `dealer_run.py:828-836` → `vdp/dispatch.py` | `enrich_vehicles_vdp(page, …)` is always called; only the five `SCANNER_VDP_*_MAX=0` caps the pipeline sets keep the worker pages from opening |
| 5 | `dealer_run.py:979-988` → `post_scan/auto_heal.py` → `post_scan/gap_fill.py:80` | `SCANNER_AUTO_HEAL` (default on) launches **sync Playwright** per healed VIN batch; `SCANNER_LISTING_FETCH_CHAIN=0` still falls through to the same launcher (`_fetch_listing_html_legacy`) |
| 6 | `dealer_run.py:1090-1093` | failure HAR (off by default) and a `page.screenshot` on the zero-vehicle path |
| 7 | `orchestrator.py:346` | new page + goto + screenshot in the per-dealer exception handler |

Plus hard import coupling: `orchestrator.py:19` imports `dealer/bmw_enhancer.py`, whose module top imports `playwright.async_api`; five of its Page functions have no callers.

`network_observer.py` is a `page.on("response")` consumer only: an HTTP-only scan can replay recipes but never learn one. Recipe acquisition = `recipe_synth` (HTTP templates) or a browser capture. That is the discovery job.

## Target architecture

```
discovery (browser allowed)            scan (HTTP only, always)
──────────────────────────             ──────────────────────────
discovery_probe --browser-capture      dealer_pipeline / scanner.py
  ├ NetworkObserver on SRP load          ├ recipes.try_fetch_via_recipes (union)
  ├ site profiler                        ├ store scoping (carscommerce), section stripping
  ├ auth/key capture (Typesense, Algolia)├ HTTP recovery: shopperexpress API, DEP results API,
  └ promote_from_ledger → recipe         │   JSON-LD / __NEXT_DATA__ parse of fetched HTML
                                         ├ vdp/prefetch (curl_cffi) + vdp_recipes for detail pages
                                         ├ post-scan: vPIC decode+heal, reconcile, verify, logs
                                         └ assert: zero Chromium processes during the run
```

Rule of thumb: a browser may run only inside `discovery_probe`, only for a dealer the pipeline flagged (`no_recipe`, `validated_zero`, `no_rows`, stale-401/403), and its only output is a recipe (+ discovery.md). It never writes cars.

## Phase 0 — make HTTP-only actually browser-free (no deletions)

Small, reversible, test-backed. Goal: `pgrep -f 'ms-playwright|headless_shell'` = 0 for the whole scan.

1. **Default flip.** `scanner.py`/`backend/scanner/cli.py`: `SCANNER_HTTP_ONLY` defaults to `1`. New `SCANNER_ALLOW_BROWSER=1` is the only way to launch Chromium, and only `discovery_probe --browser-capture` sets it. Add `--http-only/--allow-browser` CLI flags mirroring the env.
2. **No launch.** `orchestrator.py`: when the browser is not allowed, run `run_dealers_with_browser(None)` — no `chromium.launch`, no `Stealth`, no failure-screenshot page (`:346`). Make the `bmw_enhancer` import lazy (only the dict-munging function is used).
3. **No context.** `dealer_run.run_dealer(browser=None)`: skip `new_context`/`new_page` (`:256-257`); `page=None` everywhere downstream; skip the zero-vehicle screenshot (`:1093`) and HAR (`:1090`).
4. **Recovery in HTTP mode.** `inventory_recovery.recover_inventory`: when `ctx.page is None`, run only the HTTP-capable strategies (`shopperexpress_api`, `dealer_eprocess_json` strategy 0 = `_fetch_results_api`, `jsonld_listing_html`, `html_next_data` on HTML fetched with curl_cffi instead of `page.evaluate`) and skip the rest with one log line naming them. `should_run_platform_recovery` unchanged.
5. **VDP.** `dealer_run:828`: do not call `enrich_vehicles_vdp` when `page is None` (the HTTP-first `prefetch_before_vdp` + `apply_vdp_recipes` already run at `:781`). Remove the pipeline's reliance on the five zero caps.
6. **Auto-heal / gap fill.** `gap_fill.fetch_listing_html`: when the browser is not allowed, use the HTTP fetchers only (curl_cffi/requests chain) and never the `PlaywrightFetcher` or `_playwright_fetch_html` legacy fallback. `DEFAULT_FETCHERS` / `default_chain` end in Playwright today — build the chain from an allow-list. Same for `delta_scan._complete_prices_from_vdp` and `run_listing_gap_fill_for_vins`.
7. **Guard.** A single helper `backend/scanner/browser_gate.py::browser_allowed()`; every remaining `chromium.launch` / `sync_playwright()` / `new_context` call site asserts it (raise `BrowserForbidden` with the caller's name). Log one line per run: `browser: forbidden` / `browser: allowed (discovery)`.
8. **Pipeline.** `dealer_pipeline.run_http_only_scan` drops `HTTP_ONLY_ENV`'s cap juggling; sets only `SCANNER_HTTP_ONLY=1` (now the default) and `DEALERS_FROM_SCANNABLE=1`. Records a `chromium_processes_seen` counter in the run summary (snapshot `pgrep` before/after each batch, as `deploy/nightly_http_refresh.sh` already does) and fails the batch if it rose.
9. **Tests.** `test_scan_only_mode.py` gains: orchestrator with browser forbidden never imports playwright launch; `run_dealer(browser=None)` completes a recipe replay; recovery skips page strategies; gap_fill chain has no Playwright fetcher; `BrowserForbidden` raised on every call site under the gate. Existing browser-path tests keep passing (they stub pages).

Exit criterion: one full fleet cycle (both machines) with `chromium_processes_seen = 0` on every batch and row counts within 5% of the 09-26 baseline per dealer.

## Phase 1 — discovery owns the browser

1. `discovery_probe --browser-capture <dealer>`: opens the SRP(s) under `SCANNER_ALLOW_BROWSER=1`, attaches `NetworkObserver`, scrolls/paginates once, and writes the captured endpoints to the ledger → `promote_from_ledger` → recipe candidates → `validate_recipe` → save. Also runs the site profiler and records the platform fingerprint. Output: `discovery.md` (what the site does), `scan_instructions.md`, recipe rows. **No car rows are written.**
2. `dealer_pipeline` calls it automatically for verdicts `no_recipe` / `validated_zero` / `no_rows` and for recipes marked stale by 401/403 (auth rotated), then re-validates over HTTP and rescans. Bounded: one capture per dealer per day, logged in `_learning/errors_index.md`.
3. `dealer-discovery` workflow (`.claude/workflows/dealer-discovery.js`) investigator step gets the same switch; the builder still writes recipes/templates only.
4. IP hygiene: a discovery capture may hit Cloudflare on a hot IP; run it from the other machine when the probe reports 403 (already in the playbook).
5. Remaining browser-only scrapers become **templates or discovery inputs**, not scan strategies: `dealer_on_cosmos` (HTTP template exists), `pixel_motion` (needs an HTTP template: capture once, replay), `dealer_com_bulk_fetch` (dealer.com has the getInventory recipe), `dealer_inspire_algolia` / `dealer_venom_typesense` (config discovery moves to `recipe_synth` HTML extraction, which already reads Algolia/Typesense keys from HTML; browser capture only when HTML lacks them).

## Phase 2 — delete the scan-time browser code (after Phase 0 exit criterion)

Rule from memory: grep every consumer (code, scripts, frontend, tests) before each deletion; rebuild better, never lose capability. Candidate list with the audit's consumer notes:

| delete | consumers to move/rewrite first |
|---|---|
| `vdp/dispatch.py` worker pages, `vdp/visit.py`, `vdp/gallery.py`, `vdp/spin_capture.py`, `vdp/browser_js.py` | `dealer_run:828`; tests `test_vdp_dispatch_probe`, `test_vdp_gallery_hang_fix`, `test_scanner_vdp_gallery_js`, `test_vdp_spin_capture`, `test_vdp_helpers`; keep `vdp/prefetch.py`, `vdp/vdp_recipes.py`, `vdp/html_recovery.py`, `vdp/queue.py` |
| `phases/inventory_scrape.py` browser SRP scrape, `phases/nav.py`, `inventory_card_location.py` | move the SRP-load + NetworkObserver part into `discovery_probe --browser-capture`; `test_network_observer` follows it |
| `phases/site_profile.py` (browser DOM probes) | discovery only |
| `phases/url_discovery.py` (DuckDuckGo via browser) | replace with an HTTP search or the registry; called from two `not _http_only()` branches |
| `scrapers/dealer_on.py`, `scrapers/pixel_motion.py` (browser fetch), `scrapers/dealer_com_bulk_fetch.py` | recovery strategy map `inventory_recovery.py:704-713`; `test_dealer_on_parser`, `test_pixel_motion*`, `test_dealer_com_bulk_fetch`; keep their pure parsers |
| browser halves of `scrapers/autowall.py`, `shopperexpress.py`, `dealer_eprocess.py`, `dealer_inspire.py`, `dealer_venom.py` | keep HTTP halves; `inventory_scrape.py:337-381` fallbacks go with the file |
| `post_scan/gap_fill._playwright_fetch_html`, `chain.PlaywrightFetcher`, `vdp/spec_fetch.py`, `vdp_spec_extract.py` | `test_scraper_chain.py` fetcher-order tests; `deploy/nightly_http_refresh.sh` workaround (`SCANNER_LISTING_FETCH_CHAIN=0`) becomes unnecessary |
| `dealer/bmw_enhancer.py` Page functions (5, no callers) | keep `enhance_scraping_for_bmw_dealerships` |
| `--capture-only` mode, `mac_mini_lite.py` preset, `SCANNER_FAILURE_HAR`, warmup phase, DealerOn renderer-wedge handling | `cli.py:243-252`, `scan_efficiency.py`; memory notes on the wedge |
| `Dockerfile.scanner` / `Dockerfile.scanner-worker`: `playwright install chromium`, `patchright`, Node + Puppeteer Chromium | build the scanner image from the `Dockerfile.scanner-scheduler` shape (python-slim, no browser); a separate `Dockerfile.discovery` keeps Chromium for `discovery_probe --browser-capture`; `job_diagnosis.py` Chromium checks move with it; `Dockerfile.web:34` browser install reviewed separately |
| `requirements.txt` `playwright`, `playwright-stealth` | scanner image no; discovery image yes |

Also retire the `SCANNER_VDP_*` cap knobs that only shaped browser visits (`vdp/config.py`, ~43 vars) and the `requires_browser` recipe hint's skip in `delta_scan.py:156` (a dealer without an HTTP recipe goes to discovery, not to a browser).

## Phase 3 — keep it honest

- Metrics per fleet run (already in the triage tables, add two): `chromium_processes_seen` (must be 0), `recipe_coverage` (dealers with live recipe / active dealers), rows vs 30-day baseline, one-condition dealers, reconcile audit ratio.
- Recipe lifecycle (built 2026-09-28, `dealer_pipeline.run_lifecycle_pass`): 401/403 → `mark_stale` + `scan_hints.recipe_status = stale:<status>:<iso>` (a stale recipe is still tried once per scan, after the live ones; an answer un-stales it and clears the status to `ok`) → after the main batches, `route_verdict` sends `no_recipe` / `no_rows` / `validated_zero` / auth `error` / `recipe_status stale:|rejected:` dealers through step 1 force re-synth + validator gate, step 2 `run_discovery_capture` (own process) + `validate_live_recipes`, step 3 one HTTP-only retry batch + assess → triage row `lifecycle` (none | resynth_ok | capture_ok | failed:<reason>) and `retried: <old> → <new>`; once per dealer per UTC day (`scan_hints.lifecycle_last_attempt`), `--no-lifecycle` to skip; dealers still failing land in `<out>/needs_discovery.txt` (the dealer-discovery workflow's input). Every step is a dated block in `discovery.md` and a failure class in `_learning/errors_index.md`.
- Recipe validation at save time (`backend/scanner/recipe_validation.py`, 2026-09-28): `validate_recipe_set` replays page 1 and 2 of every recipe through the scan's own request function, reads the site's own count per platform (`SITE_TOTAL_EXTRACTORS`: carscommerce `total_vehicle_count`, dealer.com `pageInfo.totalCount`, typesense `found`, cosmos `TotalCount`, Algolia `nbHits`, autoWALL `<title>`, DEP `data-vehicle_count`, single-shot list lengths; `None` when a platform exposes nothing), counts VINs per condition, and returns `ok | reject | uncertain` with flags `one_condition` (site census or pinned-condition recipes say both sides exist; `workspace/pipeline/one_condition_ok.txt` turns it into a note), `section_scoped`, `short_page` (200 page 2 adds nothing while page 1 is under 95% of the count; a reject only under half the lot), `auth_needed` (401/403 before any VIN), `zero_rows`. `gate_recipes` applies it on every save path — `dealer_pipeline.ensure_recipe`, `synthesize_recipes.py`, `promote_from_ledger(validate=True)` from the discovery capture — writing the report to `discovery.md`, a reject line to `_learning/errors_index.md`, and `scan_hints.recipe_status = ok | rejected:<code> | uncertain:<code>`. A reject is not saved; `validate_recipe` (per-candidate VIN count) is unchanged for its other callers (`recipe_cascade`).
- New-platform playbook: probe → synth template if the site is HTTP-describable (14 templates today) → else browser capture → recipe → template later if a second dealer appears.

## Decisions needed

- **D1** Keep `Dockerfile.web`'s Chromium? (only needed if the web app runs any browser feature; audit separately.)
- **D2** Discovery browser on which machine? Proposal: the mini (clean IP, always on); MBP only when the mini is off.
- **D3** Delete `mac_mini_lite.py` / capture-only mode outright, or keep capture-only as the discovery capture's internal mode? Proposal: delete; discovery gets its own entry point.
- **D4** Phase 2 timing: after one clean fleet cycle under Phase 0 (≈1 day) or after a week?

## Order of work

1. Phase 0 items 1-7 in one branch (`feature/http-only-scans`), item 8-9 tests, run the 51-dealer rerun set + 20 random dealers on both machines, compare rows to baseline, confirm zero Chromium.
2. Phase 1 item 1-2 (discovery capture), prove it on mcpeeks-com / quantumautosales-com (the two dealers no HTTP template covers today).
3. Phase 2 deletions, one table row per commit, consumer audit noted in each commit message.
4. Dockerfile split last; test locally (docker build + run against local Postgres) before any Railway deploy (memory rule).

## Progress log

- **2026-09-26 Phase 0 done** (commit 07aca8b60): gate `backend/scanner/browser_gate.py`; orchestrator/dealer_run/recovery/gap_fill/chain/spec_fetch gated; pipeline counts Chromium per batch. Verification: 6 dealers on the MBP (full lots, priced, 0 leaks), 20 on the mini queued.
- **2026-09-26 Phase 1 done**: `discovery_probe --browser-capture` (backend/scanner/discovery_capture.py). Proof on mcpeeks-com: capture ran, PixelMotion pages by HTML fragments the observer ignores — extend NetworkObserver to ledger non-JSON XHR bodies with VINs (open).
- **2026-09-26 Phase 2 in progress** (commit 74c8e2566 + this one): deleted bmw_enhancer Page helpers, spec_fetch browser (→HTTP), --capture-only, mac_mini_lite, scanner_mac_mini.py, scan_single_vin.py, url_discovery.py, vdp/{dispatch,visit,gallery,spin_capture,browser_js}.py and their tests, 19 browser-only VDP knobs; dealer_run lost warmup/profiler/SRP scrape/VDP pool/screenshot (1,140 → 839 lines); recover_incomplete_listings reads detail pages over HTTP. Kept for discovery: inventory_scrape, nav, site_profile, network_observer, inventory_card_location, the browser halves of the scrapers.
- **Next**: Dockerfiles (scanner images without Chromium, Dockerfile.discovery with it), then the browser scrapers once discovery has an HTTP replacement per platform (pixel_motion HTML-fragment pager first).
- **2026-09-26 Phase 2, images and worker**: `Dockerfile.scanner` / `Dockerfile.scanner-worker` are python-slim with no Chromium and no Node; new `Dockerfile.discovery` (Playwright Chromium, `SCANNER_ALLOW_BROWSER=1`, `SCANNER_WORKER_JOB_TYPES=onboard`). Worker: onboard jobs run `discovery_probe --browser-capture` (was Puppeteer `scanner.js --smart-import`); `job_queue.claim_next_job` honours `SCANNER_WORKER_JOB_TYPES`. Deleted `scanner.js`, `scanner_intercept.js`, `vdp_framework.js`, `package*.json`, the npm/PW_CHROME blocks of the worker entrypoint. `dealer_pipeline` runs the discovery capture (own process, once per dealer per day) for dealers no template describes (`--no-discover` to skip). **Not yet built**: run `docker build -f Dockerfile.scanner .` and `-f Dockerfile.discovery .` locally against local Postgres before any Railway deploy (memory rule); the OEM intake's crawl4ai/patchright stays on its own tooling.
- **Decisions taken (D1-D4)**: web image Chromium untouched (out of scope); discovery browser runs on the mini (clean IP), MBP fallback; capture-only and mac-mini-lite deleted; Phase 2 proceeded right after the 6-dealer MBP check passed (20-dealer mini check running).
- **2026-09-26 html_cards + HTML-fragment ledger** (commit b20224d24): `backend/parsers/html_cards.py` (data-vin cards + RSC hydration objects) and the `html_cards` template cover Quantum Auto Sales (209/209 rows, 53 s, HTTP-only). NetworkObserver ledgers non-JSON XHR bodies with >=5 VINs and, since the mcpeeks-com capture showed PixelMotion's `VlpAjaxEndpoint.php` returns `{"html": "<cards>"}` (classifier: near_miss, vin_items=0), also JSON envelopes whose largest string is VIN-bearing markup; `promote_from_ledger` writes those as `html_cards` recipes (`?page=N` walk). honestcardeal-com is a Vite SPA on Supabase: discovery capture path, no template. Used-only lots are listed in `workspace/pipeline/one_condition_ok.txt` so the one-condition check is a note, not a failure.
- **2026-09-26 one-condition census** (commits 3c181a70e, 6e0de4daf, 6b2d64257, 7747a26dc): the nine `only one condition` verdicts from the mini reruns were scoping bugs, not used-only lots. Fixed: DEP `lc=<store id>` scoping + one recipe per condition bucket (Lexus of Knoxville/Chattanooga); dealer.com `listing.config.id` stripped at replay (Camelback, still open: its widget has no VIN); rooftop gate keeps unnamed/unstamped rows filed under the matched store's feed source (Honda of Huntersville 141 → 389); carscommerce fully-unstamped accounts up to 1,500 cars are one store (Claremont 78 → 1,045, Group 1 Ford South Austin). PixelMotion `{"html": cards}` envelopes ledger as HTML fragments; Supabase allowed in the intercept policy (honestcardeal captured: `public-inventory?limit=1000`, 97 cars). Verification rescans queued on both machines; results go to the dealer summaries and `_learning/errors_index.md`.
- **2026-09-26 afternoon** (commits 7007dd48e, 101b5ca09): reverted the dealer.com `listing.config.id` strip (dealer.com omits VINs without it); same-source gate narrowed to store-own accounts and strong tiers; page JSON-LD streets persist to scan hints and reach the replay gate; Group 1 Ford single-store via the store-name Location facet; PixelMotion vehicle objects parsed from the JSON envelope; `generic_json` parser for flat feeds. Mini phase-0 check: 14 ok / 2 thin / 1 inaccurate / 3 no_rows (one recipe 404, one Hendrick group feed, one caused by the strip). Knoxville's 1,417 foreign rows await a human-run UPDATE (SQL in its summary).
- **2026-09-26 evening** (commit d74be6366): all one-condition reruns verified — Honda of Huntersville 389, Group 1 Ford South Austin 735, Claremont 1,039, McPeek's 311, Trinity/honestcardeal 97, Lexus Chattanooga 330, Lexus Knoxville 338 scoped. Full suite 3,178 passed. Open: Knoxville foreign-row UPDATE (human), Camelback VIN-less widget, Hendrick Honda group feed (registry street), Orange County Kia rebranded to Sutherlin Kia (DealerOn cosmos, rename decision).
- **2026-09-27**: Knoxville foreign rows retired (1,417, user-authorized). Fleet cycle started on the mini (458 dealers, two shards, tunnel DB env; MBP on battery so not sharded). Registry streets backfilled 184 → 401/621 via the site JSON-LD backfill with TLS-impersonated retry (commit e56894a4e).
- **2026-09-28 morning**: the 09-27 mini cycle produced 8 dealers (shard0's alphabetical head, 01:01–01:25Z, all `ok`, reconcile audit clean) and then the SSH reverse tunnel to the MBP Postgres dropped at ~02:18Z; every later batch exited 1 on connect, both shards printed DONE with nothing scanned. Fix: `wait_for_db()` polls before each batch and stops the run cleanly (commit b3af6733c). Remaining 450 relaunched on the MBP against its local Postgres (`workspace/pipeline/fleet_2026_09_28_mbp`, batch 8, no tunnel). Mini unreachable from this network; MBP joined the Tailscale tailnet, mini join pending (see memory). Structural fix worth deciding: move the inventory Postgres to the mini so the MBP connects out and no tunnel exists to drop.
- **2026-09-28 lifecycle** (branch feature/discovery-lifecycle): Phase 3 recipe lifecycle automated end to end — see the Phase 3 bullet. `DEALER_LOGS_ROOT` redirects the per-dealer logs for the pipeline, the probe and the validator (tests write to a tmp root). Tests: `backend/tests/test_recipe_lifecycle.py` (stale marking / clearing with a stubbed replay, `route_verdict` per verdict, the once-per-day guard, a retry batch built only from validated dealers, `--no-lifecycle` through `main()`).
- **2026-09-28 mid-morning**: batch 8 finished its first 8 dealers in 347 s (rc=0, no leak); MBP had 16 cores / 48 GB / Postgres idle, so the run was stopped between batches and relaunched at **batch 16** (`workspace/pipeline/fleet_2026_09_28_mbp16`, 442 dealers). Pipeline runs vPIC + assess + reconcile once after all batches, so the 16 dealers scanned by the two aborted runs (8 on the mini 09-27, 8 in the batch-8 run) have rows but no assess/logs: run `dealer_pipeline --skip-scan --since 2026-09-28T00:00:00Z --dealers <those 16> --out workspace/pipeline/fleet_2026_09_28_assess16` after the fleet. Images built and verified locally (memory rule): `dealership-scanner:http-only-20260928` (11.8 GB — requirements.txt still pulls torch/playwright/crawl4ai into it; slimming is a follow-up) replays a recipe against host Postgres with `browser_allowed()=False`, no `ms-playwright` dir, 0 Chromium processes; `dealership-discovery:20260928` (13.4 GB) has `SCANNER_ALLOW_BROWSER=1`, `SCANNER_WORKER_JOB_TYPES=onboard`, Chromium 153 launches. Not pushed to Railway.
