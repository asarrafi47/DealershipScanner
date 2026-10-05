# Monolith audit — backend/scanner (106 files)

Read-only audit, 2026-10-01. Metrics from scratchpad/metrics.csv.

## Step 1 — flat alias shims (13 files)

All 13 flat modules (window_sticker, vdp_specs_extract, vdp_spec_extract, vdp_packages_extract,
vdp_html_recovery, post_pipeline, listing_gap_fill, dealer_sticker_provider, dealer_site_url,
dealer_profile, dealer_location, claude_vdp_extract, bmw_enhancer) are 12-line `sys.modules[__name__] = _impl`
swaps. No code duplication left (good — the 2026-07-07 copy drift is gone).

FINDING S-1 (P3): shims still imported from INSIDE the scanner package, so the "deprecated" path is
load-bearing for the core itself:
- cli.py:58, orchestrator.py:28, phases/dealer_run.py:29 -> backend.scanner.post_pipeline
- orchestrator.py:19 -> backend.scanner.bmw_enhancer
- phases/dealer_run.py:14 -> dealer_site_url; dealer_run.py:517, phases/inventory_scrape.py:177,
  scrapers/dealer_inspire.py:357 -> dealer_location
- inventory_recovery.py:23 -> dealer_profile
Outside scanner: ~35 importers (17 in backend/tests, plus enrichment(4), scripts(2), routes, parsers,
utils, vision, run.py). Tests monkeypatching via the alias path still work only because the module
object is the same.
Split: codemod all importers to canonical paths (sed), then delete the 13 shims. Blast radius: import
lines only. Priority P3 (cosmetic, no bugs), but cheap.

Also checked: __init__.py (docstring only; stale — still says "Playwright" and lists bmw_enhancer
as top-level), dealer/__init__.py, post_scan/__init__.py (docstring lists), paths.py,
vdp/__init__.py (re-exports vdp.core incl. 9 private names — a smell: private helpers are public API
via `from backend.scanner.vdp import _max_vdp_concurrency`; fan_in 34).

## Step 2 — phases/dealer_run.py + delta_scan.py

### F-1 phases/dealer_run.py::run_dealer — 664 lines (186-850), 852-line file. P1
Responsibilities inside one async function:
- 186-235  URL normalize, result-dict schema (22 keys declared inline), early-out
- 240-262  browser context/UA setup — DEAD under HTTP-only (page always None after ff76d4e58
           "delete the scan-time browser stack"); `inv_paths`, `_site_profile`, `path_htmls=[]`,
           `merged_card_locations={}`, `vdp_stats={}` are vestigial and still threaded through
           (merged_card_locations branch ~471, vdp_stats.get("gallery_phase_bins") ~545 never fire)
- 264-327  recipe replay + coverage verdict + provider_hint override (re-loads recipes a 2nd time)
- 328-372  roster place lookup, two near-identical parse closures (_vehicles_for_body 344-359,
           _parse_inventory_raw 386-396 — same parse()+setdefault body, copy-paste)
- 373-414  feed-sufficiency + inventory recovery chain
- 415-470  rooftop attribution union gate (+ _feed_scoped exemption)
- 485-540  VIN dedupe, sister-store/location filter (imports via deprecated shim dealer_location)
- 540-560  VDP prefetch (HTTP-first)
- 595-650  gallery normalize, inline gallery vision, Monroney vision, registry id lookup,
           source_url stamping, VIN facts (vPIC)
- 655-690  upsert + VIN-owner-conflict accounting + scan_log
- 690-780  coverage report, auto-heal, rooftop disown, reconcile
- 790-850  summary/except/finally
Why it hurts: 16 commits since Aug, nearly every fleet fix lands here (f787bddf2 VIN ownership guard,
28173f712 rooftop gate, d74be6366 learned street, 07aca8b60 attribution/reconcile). Comments in the
body are bug post-mortems (Jordan Ford union 2026-09-22, Tutton CDJR 5/346 on 2026-09-26,
audifletcherjones 135/1005) — each fix was applied to this function only and NOT to delta_scan (see
F-2). Only 1 test file exercises run_dealer; phases cannot be unit-tested.
Split (new package backend/scanner/pipeline_steps/ or keep phases/):
- phases/feed.py: `fetch_feed(dealer, url) -> FeedCapture` (recipe replay, coverage verdict, provider hint)
- phases/attribution.py: `parse_and_attribute(capture, roster_place) -> (kept, refused)` — the ONE
  place doing parse + per-page gate + union re-gate + _feed_scoped exemption; used by delta_scan too
- phases/recovery.py wraps feed sufficiency + recover_inventory
- phases/enrich.py: dedupe, location filter, prefetch, gallery/Monroney vision, vin facts
- phases/persist.py: upsert + conflicts + scan_log; phases/after_write.py: coverage, auto-heal,
  disown, reconcile (shared with delta_scan)
- a `DealerRunResult` dataclass instead of the 22-key dict
- delete the browser-context/page vestiges.
Blast radius: callers orchestrator.py:321, cli.py re-export (__all__), scan_runs persistence
(dealers_repo reads result keys), 1 test file. Moderate.

### F-2 delta_scan.py::delta_scan_dealer — 250 lines (123-373). P1 (live drift bug)
Re-implements run_dealer's recipe replay -> parse -> per-page gate -> union gate -> disown -> upsert ->
reconcile pipeline (lines 127-345). The module comment (line 26) claims "shared so the two cannot
drift", but they already have:
- delta uses `rooftop_disown.roster_place(url)` (line 173); full scan uses
  `dealer_place.roster_place_with_hints(url, dealer_id)` (dealer_run ~334) — the page-learned
  street from d74be6366 ("learned street reaches both parse paths") does NOT reach the delta path.
- delta re-gates ALL rows in the union pass (186); full scan exempts `_feed_scoped` rows from
  store-scoped CarsCommerce recipes (dealer_run 446-458, the Tutton CDJR 5-of-346 fix). On delta the
  refused rows feed split_refusals -> _disown_foreign_rooftop_vins (218), i.e. the delta path can
  un-list a store's own cars that the full scan keeps.
- delta has its own VDP price completion (_complete_prices_from_vdp 59-104) separate from vdp/prefetch.
Live: `scanner --delta` (cli.py:377-385).
Split: after F-1, delta_scan_dealer = fetch_feed(union) + parse_and_attribute + persist +
after_write with `mode="delta"` flags. Shrinks to ~60 lines.
Blast radius: cli.py only + tests/test_delta_scan.py, test_dealer_attribution.py (patch `delta._roster_place`).

## Step 3 — database.py

### F-3 database.py::upsert_vehicles — 554 lines (390-944), file 1192 lines, fan_in 25. P1
Responsibilities:
- 390-420  dedupe by VIN, drop_unattributable_vehicles, VIN-sorted ordering (deadlock fix cc867dbc0)
- 420-460  prefetch existing spec_source_json + VIN-owner guard setup
- 460-560  per-row DOMAIN RULES: EP merge, clean_car_row_dict, mild-hybrid fuel relabel, EV cylinder
           override, transmission_type derive, engine_l, title synthesis, price/mileage coercion
           (F12 NULL-vs-0)
- 560-620  JSON serialization: gallery, spin_frames, interior_pano, highlights, image_url,
           data-quality score, interior colour buckets, availability/lot-location provenance patches,
           spec_source merge, packages, forced induction, price history
- 620-750  one ~190-line INSERT ... ON CONFLICT DO UPDATE literal (per-column keep-if-nonempty rules)
- 750-760  owner-guard rowcount accounting, batch commit, SCANNER_TRACE_VIN debug
- 855-885  conflict recording
- 885-944  post-write side passes: incomplete_listings_db + spec_structured_backfill, model_specs
           corrections, catalog linker (link_cars_by_vins) — each `try/except Exception: pass`.
Also in the same file: _ensure_schema (DDL from the write module), idle-in-txn guard,
apply_model_specs_corrections (1034-1175, 142 lines of enrichment rules), _resolve_canonical_make /
_infer_drivetrain_from_trim / _is_electric_make_model (imported by backend/enrichment/
model_specs_dictionary.py:279 — enrichment depends on the scanner DB module, inverted layering).
Why it hurts: 9 commits since July, each a data-integrity fix wedged into the loop (VIN ownership
guard f787bddf2, deadlock cc867dbc0, odometer F12 20a342de5, fuel-label self-seal 614b7ee98, spec
provenance 67bb52695). Memory notes: per-row upsert is the known throughput bottleneck ("fix per-row
upsert after fleet"); row normalization cannot be tested without a DB; silent `except: pass` around
post-write passes hides failures. It is also THE write path for dealer portal sync
(backend/dealer/portal_sync.py:169), so any scanner-only rule silently applies there too.
Split:
- backend/scanner/write/normalize.py: `normalize_row(raw) -> CarRow` (pure; all 460-620 rules; unit-testable)
- backend/scanner/write/sql.py: the INSERT/ON CONFLICT statement + column list as data (enables
  executemany / COPY batching = the throughput fix)
- backend/scanner/write/guard.py: VIN owner guard + record_vin_owner_conflicts
- backend/scanner/write/after_write.py: incomplete-listings, model_specs corrections, catalog link
  (log failures, count them in stats)
- move apply_model_specs_corrections + make/drivetrain helpers to backend/enrichment/model_specs_*
- _ensure_schema -> backend/db (migrations), not the scanner.
Blast radius: fan_in 25 (inventory_write.py, orchestrator, post_scan/job, vdp/core, parsers docs,
portal_sync, enrichment, 2 scripts, tests). Public signature `upsert_vehicles(vehicles, stats)` can stay.

## Step 4 — phases/inventory_scrape.py, discovery_capture.py

### F-4 phases/inventory_scrape.py::scrape_inventory_path — 503 lines (48-551). P2
Now browser-only (require_browser at 111) and its ONLY caller is discovery_capture.py:125 (scan path
no longer uses it since ff76d4e58). Responsibilities: request/response interception via
NetworkObserver (101-135), nav + cookie banner + hydration wait (150-170), location-filter attempt
(176-200), idle loop, Dealer.com bulk POST fetch + template merge (210-255), lazy scroll, then an
inline platform if-chain on page HTML: Pixel Motion (275-330), autoWALL (330-360), ShopperExpress
(360-395) each with their own early-exit scraping, then the pagination loop (395-520) with
next-button clicking + VIN-growth stop, final HTML capture and returns a 5-tuple.
Why it hurts: platform-specific scrapers wired by `if _is_X_html(peek_html)` inside a 500-line
function; returns a positional 5-tuple; dead weight for the scan path but still edited (discovery is
where new platforms get learned, so new platforms will grow this if-chain).
Split: discovery/srp_capture.py (nav+observer+pagination only) + a `PLATFORM_HTML_SCRAPERS` registry
[(detector, scraper)] in scrapers/__init__ (pixel_motion, autowall, shopperexpress, dealer_com_bulk)
iterated once; return a `PathCapture` dataclass. Also move phases/inventory_scrape.py, phases/nav.py,
phases/site_profile.py under a `discovery/` subpackage so `phases/` = HTTP scan only.
Blast radius: 1 caller (discovery_capture) + tests of discovery. Low.

### F-5 discovery_capture.py::capture_endpoints — 126 lines (55-180). P3
Writes `os.environ["SCANNER_ALLOW_BROWSER"]="1"` (line 68) process-wide — hidden global state that
permanently opens the browser gate for anything else in the process. Mitigated today because
dealer_pipeline.py runs it in a subprocess with the var popped (scripts/dealer_pipeline.py:312-337),
but tests (test_recipe_lifecycle, test_recipe_validation) and any in-process caller inherit it.
Fix: pass an explicit allow flag / context manager in browser_gate (`with browser_gate.allowed():`)
that restores the env. Otherwise fine-sized.

## Step 5 — recipe_synth.py

### F-6 recipe_synth.py — 2455 lines, 95 defs, fan_in 13. P2
The registry already exists (PlatformTemplate list at 2060-2094 — good, no if/elif dispatch). The
problem is that all 17 platform plugins + shared infra live in one file:
- 65-268    HTTP layer: browser headers, pacing, Cloudflare challenge detection, curl_cffi
            impersonated fetch with retries (fetch_dealer_html). A 3rd/4th copy of impersonated-fetch
            logic (curl_cffi also used directly in chain.py, recipes.py, vdp/prefetch.py, vdp/vdp_recipes.py).
- 279-346   dealer.com; 347-862 CarsCommerce incl. _carscommerce_store_filter (591-862, 250 lines:
            facet census, rooftop signatures, store verify, single-store size thresholds) — 4 fix
            commits in Sept (07aca8b60, 7747a26dc, 101b5ca09, d74be6366), each new magic constant
            commented with the dealer that broke it (Knight Claremont 461, etc.)
- 863-958 DealerOn cosmos; 959-1117 Typesense; 1118-1191 Team Velocity; 1192-1461 Dealer eProcess
  (own fetch/escalation/pagination); 1462-1529 Motive (hardcoded Algolia app id + API key at 1462-1463,
  presumably a public search key but it is a credential literal in source); 1530-2059 Overfuel,
  Nabthat, Chapman, Jazel, WP vehicles, autoWALL, oneAudi, html_cards, Dealermasters
- 2097-2227 fingerprint/synthesize entry points + HTML harvest detection
- 2228-2455 validate_recipe + 4 per-platform validators (_validate_json_feed/_dep/_html_walk/_cosmos)
            — a SECOND validation stack beside recipe_validation.py (952 lines, validate_recipe_set /
            _check_recipe with its own per-platform total extractors). Two definitions of "does this
            recipe work" (VIN count here, coverage/condition/total checks there).
Why it hurts: every new platform or store-scoping fix edits this file; merge conflicts with
parallel discovery work; per-platform tests have to import the 2.4k-line module.
Split: package backend/scanner/synth/
- synth/http.py (headers, pace, challenge, fetch_dealer_html) — and make chain.py/recipes.py use it
- synth/platforms/<name>.py, one per template (detect + synth + constants), carscommerce gets
  carscommerce.py + carscommerce_scope.py (store filter)
- synth/registry.py (PlatformTemplate list, fingerprint_platform, synthesize_recipe[s])
- move validate_recipe + _validate_* into recipe_validation.py (one validation module), keep a
  re-export for the 13 importers.
Blast radius: fan_in 13 (recipes.py, discovery scripts, dealer_pipeline, tests). Keep
`backend.scanner.recipe_synth` as a facade re-exporting public names; tests patching private
`_fetch_impersonated`/`_pace` need path updates.

## Step 6 — recipes.py, recipe_store.py, recipe_cascade.py

### F-7 recipes.py — 1165 lines, fan_in 82 (highest real hub in the area). P1 for try_fetch_via_recipes, P2 for the file
Five unrelated jobs:
- 52-130   EndpointRecipe model + pagination inference
- 132-292  JSON-file persistence + alias slugs + DB sync (load/save/mark_stale; recipe_store.py is the
           Postgres half of the same store)
- 296-366  egress tag / blocked-status / stale-status hint writes (lifecycle policy)
- 368-492  promote_from_ledger (discovery -> recipe)
- 516-822  request mutation for pagination (_mutate_for_page 578-653, _url_for_page) + HTTP replay
           (_replay_impersonated / _replay_request — another curl_cffi copy; see F-6)
- 823-934  coverage metrics + "replaces browser" policy + last_known_vin_count (DB)
- 935-1165 try_fetch_via_recipes (230 lines): place lookup, recipe load, live+stale ordering,
           per-recipe page loop with 401/403 lifecycle, _feed_scoped stamping, parse()+rooftop gate,
           dealer.com page-size learning, union accumulation, status write.
Why it hurts: 11 commits since mid-Aug. The rooftop gate (parse with rejected_out) is invoked in
~6 independent call sites (recipes.py x3, dealer_run x2, delta_scan, recipe_validation,
recipe_synth via parse_kept x10, scripts) and the 1000-1040 comment documents a real bug from one
site forgetting `rejected_out` (terrylabontechevy -> 3,931 Hendrick VINs; bmwofmurrieta 1,921 cars).
The `_feed_scoped` marker is smuggled INTO the parsed payload dict (line 1079) so that dealer_run's
later re-parse can see it — a cross-module side channel caused by parse happening in two places.
Also inside the same scan, try_fetch_via_recipes uses roster_place_with_hints while delta_scan's
second pass uses roster_place (F-2): two place dicts for one dealer in one run.
Split:
- recipes/model.py (EndpointRecipe, infer_pagination), recipes/store.py (JSON + merge recipe_store.py),
  recipes/lifecycle.py (stale/egress/blocked), recipes/replay.py (mutate/url/replay, HTTP via shared
  client), recipes/coverage.py (coverage + replaces-browser policy), recipes/promote.py.
- try_fetch_via_recipes returns raw pages + per-recipe scope flags (no parse); parsing + gate done
  once by the shared `parse_and_attribute` step proposed in F-1. That removes the _feed_scoped
  payload mutation and the double gate.
- Make `parse()`'s rejected_out mandatory (or split parse_all / parse_kept) — in backend/parsers.
Blast radius: fan_in 82 — keep `backend.scanner.recipes` as a facade re-exporting public names;
tests patch `_replay_request`, `load_recipes` on this module path.

recipe_store.py (328, fan_in 23): Postgres recipe/scan-hints store with `_ensure_table` DDL at
runtime (83-115) — fine-sized; only issue is it's half of the recipe store (merge with
recipes/store.py) and does CREATE TABLE on the scan path. P3.
recipe_cascade.py (252): cross-dealer donor recipe adaptation — single job, checked, fine.

## Step 7 — post_scan/window_sticker.py

### F-8 post_scan/window_sticker.py — 2467 lines, 101 defs, ~35 importers across web/billing/vision. P2
Not a scan step — a domain library that lives in scanner/post_scan. Seven jobs:
- 64-251    OEM sticker URL building + HTTP fetch per OEM family (Stellantis/Ford/Lincoln/GM) — per
            memory "Leave OEM sticker fetch alone": do not change behaviour, only move.
- 252-319   PDF text extraction (pypdf, module-global _PYPDF_MISSING_LOGGED flag)
- 320-1106  option/package/MSRP parsing from sticker text (parse_sticker_option_items 557-675, 117 lines)
- 708-1080  DISPLAY grouping for the web UI (group_sticker_options_for_display 819-921,
            organize_sticker_option_sections, sticker_options_for_display) — presentation logic
            consumed by backend/routes/cars_pages.py and utils/car_serialize
- 1107-1702 engine/turbo/eTorque/EV/transmission rules (car_has_turbo_signal,
            upgrade_engine_display_for_turbo, known_oem_engine_from_car) — consumed by
            utils/car_serialize/engine.py and msrp_trust.py; overlaps the engine-facts precedence
            rules (vPIC > sticker > dealer text > catalog) that live in enrichment
- 1703-2286 dealer listing sticker / iPacket URL discovery + fetch
- 2287-2467 UI eligibility predicates (show_window_sticker_panel, show_window_sticker_ui,
            oem_hide_photo_analysis)
Why it hurts: web routes, billing (billing/catalog.py), the AI agent and vision all import a
"scanner" module, so the web image depends on the scanner package; private helpers are imported
cross-package (`_extract_pdf_text`, `_parse_msrp_from_sticker_text` from utils/scripts). Low churn
(1 commit since Aug) so it is not causing bugs now — layering problem, not a bug factory.
Split: new package backend/stickers/: fetch_oem.py (64-251, untouched), pdf_text.py, parse_options.py,
parse_engine.py, listing_urls.py (iPacket), display.py (grouping + show_* predicates). Keep
backend/scanner/post_scan/window_sticker.py as a re-export facade; then repoint web/billing to
backend.stickers.display and promote the 2 private helpers to public names.
Blast radius: ~35 importers (all via the flat shim backend.scanner.window_sticker). Facade makes it zero-risk.

## Step 8 — post_scan/ (pipeline, job, gap_fill, auto_heal, coverage_report) + orchestrator tail

### F-9 post_scan/pipeline.py — 1174 lines, 41 defs. P2
A grab-bag of every post-scan stage plus its env flag:
- 26-87     9 `post_*_env_enabled()` flags (+ gallery_vision/monroney flags at 270, 492)
- 88-138    gas prices sync + Google ratings backfill (nothing to do with scanned cars)
- 139-214   window sticker stage; 215-269 gap fill + dictionary enrich stages
- 270-491   gallery vision filter + gallery recovery + gallery vision stages
- 514-603   storage repair + enrichment orchestration + car id lookup
- 604-1009  interior vision: URL selection heuristics + a direct Claude vision call
            (_analyze_interior_with_claude 819-928) — LLM client code inside the scanner
- 1010-1042 listing description parse
- 1043-1174 run_post_scan: sequential `if flag and vins:` chain (repair, description, sticker,
            interior, gallery recovery, gallery vision, enrich)
Split: post_scan/stages/<stage>.py each exposing `Stage(name, enabled(), run(vins))`; a
STAGES list in post_scan/registry.py drives run_post_scan; interior vision (604-1009) -> backend/vision/
interior.py next to the other vision code; gas prices + google ratings -> a separate nightly
"housekeeping" job, not post-scan.
Blast radius: fan_in 1 direct, but ~8 importers via flat shim post_pipeline (cli, orchestrator,
dealer_run, enrichment, tests). Facade keeps it safe.

### F-10 orchestrator.py::_run_post_scan_tail (108-215) duplicates post_scan/job.py::run_post_scan_job
(113-238). P2 (copy-paste)
Same stage sequence written twice: run_post_scan -> vPIC -> listing gap fill -> dictionary enrich ->
gas prices -> Google ratings; a line diff of the two bodies shows ~70% identical text. A new
post-scan stage (vPIC was the latest) must be added in both, and orderings already differ in
detail (job.py imports post_vpic_env_enabled inline, orchestrator at module level).
Fix: one `run_post_scan_tail(vins, flags)` in post_scan/job.py (or the stage registry of F-9);
orchestrator calls it.

post_scan/gap_fill.py (605; run_listing_gap_fill_for_vins 191 lines, 414-605): three fetch paths
(ScraperChain, legacy requests, gated Playwright fallback 66-107) + DuckDuckGo spec search
(215-339) + window-sticker enrich + provenance merge. P3: the legacy path + SCANNER_LISTING_FETCH_CHAIN
toggle is a dead-ish second implementation; DDG search belongs in enrichment. The browser fallback
is now gated (comment 70-72: 1,656 Chromium launches in one "HTTP-only" nightly before the gate).
post_scan/auto_heal.py (98), post_scan/coverage_report.py (98): single-purpose — checked, fine.

## Step 9 — cli.py, orchestrator.py, dealer/bmw_enhancer.py

### F-11 cli.py::run_cli_entry — 339 lines (146-485). P2
- 147-328  ~28 add_argument calls inline
- 330-372  side effects through os.environ: DEALERS_MANIFEST_PATH (335), SCANNER_ALLOW_BROWSER (350/352),
           SCANNER_SCAN_ONLY (357), SCANNER_MAX_DEALER_CONCURRENCY / _VDP_ / _DEALER_TIMEOUT (362-366),
           apply_fast_mode_env_defaults / apply_scan_only_env_defaults — the CLI configures the run by
           mutating process env that 43 scanner modules read (139 distinct env knob names in
           backend/scanner). Hidden global config; no single object says what a run is configured to do.
- 374-386  --delta short-circuit into a different pipeline (delta_scan)
- 387-440  dealer selection: ids, provider, limit, shard filter
- 440-460  ~15 `do_X = not scan_only and not args.no_X and X_env_enabled()` flag merges
- 462-484  asyncio.run(main(...15 kwargs...))
Split: cli/args.py (parser), cli/config.py `ScanConfig.from_args_env(args, environ)` -> frozen
dataclass passed to orchestrator/run_dealer (and still exported to env for subprocess compat
during migration), cli/selection.py (manifest filters/shards), cli/main.py thin dispatch
(full | delta). `__all__` re-exports run_dealer + private vision helpers — drop.
Blast radius: fan_in 2 (scanner.py root entry, tests). Low.

### F-12 orchestrator.py::_main_impl — 248 lines (238-486). P2
- 256-300  manifest filtering (OEM filter, fast/scan-only flags), BMW enhancer, scan log path
- 302-400  nested closures run_dealers_with_browser -> one_dealer -> bounded: browser launch,
           per-dealer timeout, failure screenshot via browser.new_page (dead under HTTP-only),
           scan_runs row write, shutdown flag check, progress log
- 408-424  Playwright/stealth import ladder — vestigial (scans are HTTP-only; browser only if
           someone passes --allow-browser, and then run_dealer still does no browser scraping)
- 426-486  outcome aggregation, coverage aggregation, post-scan tail (duplicated, see F-10)
Hidden global state: module-level `_scanner_shutdown_requested` (55-62) flipped by a SIGTERM
handler; shutdown_skipped_dealer_ids list captured by closure.
Split: orchestrator/runner.py `run_fleet(dealers, cfg) -> list[DealerRunResult]` (semaphore +
timeout + shutdown token object instead of module global), orchestrator/summary.py; delete the
browser/stealth branch and failure screenshots; post-scan via the shared tail.
Blast radius: fan_in 1 (cli) + tests patching orchestrator.run_dealer.

### F-13 dealer/bmw_enhancer.py (32) + flat shim. P3 — dead feature
Sets optimize_for/extended_timeout/max_wait_time/dynamic_processing on BMW dealer dicts; the only
consumers are a log line (orchestrator.py:318) and vdp/config.py:35 (reads optimize_for off a
site_profile, not the dealer). extended_timeout/max_wait_time/dynamic_processing have zero readers.
Delete module + shim + orchestrator call.

## Step 10 — inventory_recovery.py + scrapers/

### F-14 inventory_recovery.py::recover_inventory (624-804, 180 lines) + browser-era scrapers. P2
recover_inventory builds a dict registry of 8 strategy closures (661-718 — registry exists, good),
but 4 of 8 strategies (dealer_inspire_algolia, dealer_venom_typesense, pixel_motion_html,
dealer_on_cosmos) need a Playwright page and are filtered out by HTTP_SAFE_STRATEGIES (95, 727-734)
on every scan, since run_dealer always passes page=None. So the scan path carries ~2,000 lines
of Playwright scrapers that only run in discovery (if at all):
- scrapers/dealer_inspire.py (631; scrape_dealer_inspire_from_page 148 lines, browser Algolia
  fetch 191-278 + requests-based Algolia 134-190 — two transports)
- scrapers/dealer_venom.py (376), scrapers/pixel_motion.py (338), scrapers/dealer_on.py (445;
  _scrape_srp_all_pages 136 lines)
- scrapers/dealer_eprocess.py (991, 18 defs): FOUR vehicle mappers (_map_vehicle 112,
  _map_vehicle_resrc 217, _map_vehicle_jsonld 518 [128 lines], _map_vehicle_results 705) and four
  fetch paths (browser JSON 178, resrc 331, facts fallback 392, JSON-LD SRP 648, results API 794).
  The recipe path parses the same platform with backend/parsers/dealer_eprocess.py (301 lines,
  JSON-LD) and recipe_synth._synth_dealer_eprocess — same platform mapped 2-3 ways, so a field fix
  (e.g. MSRP, mileage NULL F12) has to be found in each. Same for DealerOn cosmos:
  scrapers/dealer_on._map_vehicle_card (196-254) vs parsers/dealer_on._map_vehicle (319).
Also: inventory_recovery imports dealer_profile via the deprecated shim (23).
Split: move page-based strategies + their scrapers to discovery/scrapers/ (browser-only package,
never imported by the scan image); keep inventory_recovery with HTTP-safe strategies only; make
scrapers/dealer_eprocess.py and scrapers/dealer_on.py call backend/parsers/<platform>.parse for
row mapping (one mapper per platform). Delete scrapers' private mappers after a parity test.
Blast radius: inventory_recovery fan_in 10 (dealer_run, delta? no — dealer_run + tests);
scrapers each 1-5 importers. P2 because the mapper duplication is the bug surface; the dead
weight alone would be P3.

Scrapers checked, fine (single job, small): scrapers/__init__.py, algolia_scope.py (208, pure
filter inference), inventory_vin_merge.py (134), next_data_inventory.py (45),
scanner_intercept_filter.py (366, 18 small pure helpers), dealer_com_bulk_fetch.py (255; only
used by discovery's inventory_scrape — moves with F-4), autowall.py (545; HTTP parse helpers used
by parsers + recipe_synth, plus scrape_autowall_via_playwright 143 lines that is discovery-only —
P3 split the Playwright half out), shopperexpress.py (549; HTTP API fetch + VDP schema mapping,
one platform, OK).

## Step 11 — vdp/ (12 files)

### F-15 vdp/ package — browser-era leftovers behind a re-export facade. P3 (dead code, misleading docs)
- vdp/core.py (110, 0 defs): pure re-export module whose 50-line docstring describes the deleted
  browser VDP pool and modules that no longer exist (vdp.gallery, vdp.browser_js, vdp.visit,
  vdp.dispatch, vdp.spin_capture, enrich_vehicles_vdp). vdp/__init__.py (fan_in 34) re-exports
  core incl. 9 private names; only _max_vdp_concurrency (orchestrator) and 4 test-only helpers
  are used externally.
- Zero production consumers (tests only or none): vdp/queue.py (138; VDP visit queue scoring for
  the deleted pool), vdp/claude_extract.py (368; Claude inline VDP extraction, fan_in 0 — another
  Anthropic client copy), vdp/packages.py (217; tests only), vdp/specs.py (57; tests only),
  vdp/price_hints.py (95; only re-exported), vdp/config.py gallery knobs (_vdp_gallery_min_https,
  _vdp_gallery_priority_enabled, _nav_timeout_ms unused).
- Live: vdp/prefetch.py, vdp/vdp_recipes.py, vdp/extract.py (via vdp_recipes), vdp/html_recovery.py
  (post_scan/pipeline gallery recovery), vdp/spec_fetch.py (enrichment/spec_backfill, reaches into
  prefetch's private _fetch_html).
Fix: delete queue/claude_extract/packages/specs/price_hints + dead config knobs and their tests
(after the "audit deletions" grep the user requires), shrink core/__init__ to the live surface,
rewrite the docstring. Make prefetch._fetch_html public (or move to the shared HTTP client).

### F-16 vdp/prefetch.py — 776 lines, 30 defs; http_prefetch_missing_fields 132 lines (560-693). P3
Cohesive feature (per-car detail-page pass) but mixes layers: env flags + per-dealer timing hints
(68-175), DB carry-forward SQL (_load_prior_rows_by_vin 184, merge_known_fields_from_db 210-280),
curl_cffi fetch with error/status bookkeeping (324-380), field application rules (381-514), gallery
dedupe across cars (515-559), async orchestration (560-776). It is the third "fetch a listing page
and pull fields" implementation beside post_scan/gap_fill.fetch_listing_html and
vdp/html_recovery.recover_from_detail_page.
Split (when next touched): prefetch/carry_forward.py (DB), prefetch/fetch.py -> shared HTTP client,
prefetch/apply.py (pure field merge, unit-testable). Blast radius: fan_in 5.

vdp/vdp_recipes.py (407): per-dealer VDP JSON recipe capture/replay; single job — checked, fine.
vdp/extract.py (565): capture analysis helpers, used by vdp_recipes — fine (many _private names
re-exported via core; cosmetic).
vdp/html_recovery.py (321): gallery/description from detail HTML — fine.

## Step 12 — phases/nav.py, phases/site_profile.py, network_observer.py, manifest.py

### F-17 phases/nav.py — 767 lines, 37 defs: Playwright helpers + a drifted copy of manifest code. P2
- 35-430, 527-767: browser navigation (goto retries, hydration, infinite scroll, failure HAR,
  warmup settle, location filter try_apply_location_filter 568-766 = 198 lines) — discovery-only.
  dealer_run only imports get_rotating_ua / is_playwright_shutdown_error / safe_close_context
  (dead browser vestiges, see F-1).
- 449-526: `_load_dealers_from_db`, `_default_skip_dealer_substrings`, `filter_skip_dealers` —
  COPY-PASTE of manifest.py:40 / 387, already diverged (diff of filter_skip_dealers: manifest has
  the `SCANNER_SKIP_DEALER_SUBSTRINGS=none|-` escape hatch, nav does not; _load_dealers_from_db
  bodies differ by ~90 diff lines). No production caller imports the nav copies (cli uses manifest),
  so they are dead drifted duplicates waiting to be imported by mistake.
Fix: delete nav.py 449-526; move nav.py whole into discovery/ (with inventory_scrape, site_profile);
get_rotating_ua -> http_fetch.py.

phases/site_profile.py (615; _run_profiler 159 lines 367-527): browser site profiler, discovery-only
(discovery_capture, discovery_probe). Single job; moves with F-4. Checked, fine otherwise.
phases/upsert.py (18) and phases/__init__.py (2): fine.

network_observer.py (877, 50 defs, 6 classes): payload scoring/classification (pure, 115-343),
endpoint fingerprint + auth header redaction (352-434), ledger persistence (435-499), Playwright
response observer (500-877). Cohesive-ish; P3 split pure scoring into observer/scoring.py so the
HTTP-only path (vdp_recipes uses it) doesn't import the Playwright observer. Checked otherwise fine.

manifest.py (504; _load_scannable_dealers 83 lines): roster loading, filters, sharding — single job,
checked, fine (owner of the functions nav.py duplicates).

## Step 13 — HTTP clients (chain.py, http_fetch.py, recipes/recipe_synth/prefetch fetchers), job_queue, recipe_validation

### F-18 Six independent HTTP fetch implementations with inconsistent features. P2 (P1 if a proxy is
in use on Railway)
http_fetch.py (111) is only proxy plumbing. Actual fetchers, each with its own headers/retries:
- chain.py ImpersonatingFetcher/RequestsFetcher/PlaywrightFetcher (190-378) — curl_cffi + proxy
- recipe_synth._fetch_impersonated/fetch_dealer_html (162-267) — curl_cffi + proxy + challenge detect + pacing
- recipes._replay_impersonated/_replay_request (712-822) — curl_cffi then requests fallback; NO
  SCANNER_HTTP_PROXY wiring (proxy grep count 0) — this is the main scan feed path
- vdp/prefetch._fetch_html (336-380) — curl_cffi + proxy, no challenge detection
- vdp/vdp_recipes._fetch_json (248-287) — curl_cffi + proxy
- scrapers/dealer_inspire, dealer_venom, post_scan/gap_fill — bare `requests` (no impersonation,
  no proxy)
Why it hurts: the 09-29 Railway run was dominated by 403s (memory: "Railway 403s staled shared
recipes"); fixes (egress tag, blocked-status) went into recipes.py only. Anti-bot behaviour,
proxy routing and challenge detection depend on which of 6 call sites a request goes through; the
feed replay — the path that matters most on a datacenter IP — is the one that ignores
SCANNER_HTTP_PROXY (unverified whether curl_cffi picks up HTTPS_PROXY env on its own; check before
relying on a proxy).
Fix: backend/scanner/net/client.py — one `fetch(url, *, method, body, headers, kind="html|json")`
with impersonation profiles, proxy, pacing, challenge detection, egress tag, status classification;
all six call it. chain.py's Fetcher classes become thin adapters.
Blast radius: internal to each module; tests patch the private fetchers (`_fetch_impersonated`,
`_replay_request`, `_fetch_html`) — keep those names as wrappers during migration.
Progress 2026-10-05 (behavior-preserving step): backend/scanner/net/client.py holds the shared
primitives (lazy curl_cffi/requests import, scanner_proxies, send, rotate_impersonation with
accept/pacing/error hooks, looks_like_challenge, status sets). chain Impersonating/Requests
fetchers, synth/http._fetch_impersonated, recipes._replay_impersonated/_replay_request,
vdp/prefetch._fetch_html and vdp/vdp_recipes._fetch_json call it with options reproducing their
old behavior, pinned by backend/tests/test_scanner_http_characterization.py. NOT yet unified (owner
decisions): replay still sends no proxies= (curl_cffi 0.16 never reads proxy env in Python —
`trust_env` is stored but unused; with proxies unset, libcurl's own env lookup may still apply
https_proxy/HTTPS_PROXY, but SCANNER_HTTP_PROXY is never seen); prefetch still has no challenge
detection; timeouts/headers/retries still differ per site. Untouched: scrapers/dealer_inspire,
dealer_venom, post_scan/gap_fill bare requests; pipeline/recipes.py and synth/platforms/wp_vehicles.py
own curl_cffi calls; PlaywrightFetcher.

### job_queue.py — 810 lines, 25 defs, fan_in 15. P3
Postgres job queue (DDL ensure_job_tables 25-71, enqueue/claim/finish/reap 83-280) + refresh
scheduler + dealer catalog bookkeeping (281-523) + admin retry/diagnosis/listing (524-810,
smart_retry_failed_job uses job_diagnosis). Consumers are mostly the WEB admin
(dealer/admin/onboard_api, dealers_hub, platform_stats, routes/admin_dealer_api, main.py), so the
web imports the scanner package for it. Split: backend/jobs/queue.py (mechanics), backend/jobs/
scheduler.py, backend/jobs/admin.py; DDL to migrations. Low churn; not causing bugs (scheduler
off per memory).
job_diagnosis.py (314; _rule_diagnosis 96 lines rule table): fine.

### recipe_validation.py — 952 lines, fan_in 5. P3 (see F-6)
Per-platform site-total extractors (166-291, a small registry — fine), condition census, the
RecipeValidationReport model, validate_recipe_set (667-858, 192 lines: replay each recipe, check
totals/conditions/coverage, build report), discovery log writer + status + gate. The function is
long but linear. Main issue is the parallel validate_recipe stack in recipe_synth (F-6); merge
them here. Otherwise fine.

## Step 14 — rooftop / store attribution spread (dealer/location.py, rooftop_match, rooftop_disown,
rooftop_ledger, dealer_place, inventory_card_location, inventory_reconcile)

### F-19 "Is this car this store's?" is decided in 7 places across 7 modules. P1 (attribution is the
#1 fix area: 12 commits since Sept 1 across these files + backend/parsers/__init__.py)
1. per-page rooftop gate inside backend/parsers.parse (rejected_out), scored by rooftop_match.py
   (463; score_rows 143 lines, SCANNER_ROOFTOP_SCORER default on) and logged by rooftop_ledger.py
2. union re-gate in run_dealer (415-470) with the _feed_scoped exemption
3. union re-gate in delta_scan (184-190) WITHOUT the exemption (F-2)
4. sister-store filter dealer/location.py::filter_sister_store_vehicles (340-437,
   SCANNER_SISTER_STORE_FILTER default on), run in run_dealer AFTER the rooftop gate with a
   separate heuristic (DealerSiteProfile from the manifest dict: name tokens, host stem, state) —
   a second, older attribution model that never consults the roster place / learned street
5. inventory_card_location.py stamps SRP card locations (browser-era; merged_card_locations is
   always {} under HTTP-only, so this is dead on the scan path)
6. rooftop_disown.py un-lists VINs the feed hands to a sibling (EVIDENCE_BACKED_REJECTS); it ALSO
   owns `roster_place` (44-85), while dealer_place.py owns `roster_place_with_hints` that wraps it —
   two place functions, callers pick inconsistently (F-2)
7. upsert_vehicles VIN-owner guard (database.py, f787bddf2) refuses moves at write time
   (guard refused 12,903 moves in the 09-29 Railway run per memory — "unread")
Why it hurts: every fleet run finds a new attribution bug and the fix lands in whichever of these
the symptom surfaced in (Tutton CDJR, Honda of Huntersville, Hendrick, terrylabonte, bmwofmurrieta,
audifletcherjones are all cited in comments). No single test can assert "store X keeps exactly its
cars" through the whole chain.
Split: backend/scanner/attribution/ package:
- place.py: ONE `store_place(dealer_url, dealer_id)` (roster + learned street) — delete the other
- gate.py: `attribute(rows, store, scopes) -> Verdicts` = per-page + union + feed_scoped rule,
  rooftop_match scoring, ledger write; called by run_dealer, delta_scan, recipe replay, validation
- sister.py: either fold dealer/location's heuristic in as one more scored signal or delete it once
  the gate covers its cases (measure first: count rows it removes after the gate on a fleet sample)
- disown.py (rooftop_disown minus roster_place); reconcile stays in inventory_reconcile.py
Blast radius: parsers/__init__.py (rooftop gate call sites), dealer_run, delta_scan, recipes,
recipe_validation, recipe_synth (parse_kept), tests (test_dealer_attribution, test_rooftop_*).

inventory_reconcile.py (276; reconcile_dealer_inventory_after_scan 148 lines 128-275): one job
(soft-unlist missing VINs with coverage floors). Long but linear — checked, P3.
dealer_place.py (176): place/name/oem-code from HTML + hints — fine apart from #6 above.

## Step 15 — scan_efficiency, specials/, utils/, JSON-LD helpers

### F-20 JSON-LD <script> extraction re-implemented in 10 modules. P3
inventory_recovery, dealer_place, html_jsonld_harvest, post_scan/gap_fill, utils/vdp_extras_parse,
utils/vdp_spec_parse (x2), vdp/html_recovery (x2), scrapers/shopperexpress, scrapers/dealer_eprocess,
parsers/dealer_eprocess each carry their own ld+json regex / Vehicle-node walk. Fix: one
backend/scanner/html/jsonld.py (`iter_jsonld(html)`, `vehicle_nodes(html)`) — html_jsonld_harvest is
the natural home. Blast radius: internal helpers only.

### scan_efficiency.py — 328 lines, 19 defs, fan_in 19. P3
An env-knob grab-bag (fast mode, scan-only defaults that WRITE env: apply_fast_mode_env_defaults
51-74, apply_scan_only_env_defaults 81-103 — more hidden global config, see F-11) plus feed-
sufficiency logic. Dead knobs from the deleted VDP pool: effective_vdp_ep_max (174),
effective_vdp_price_max (203), effective_vdp_spec_gap_max (216) have zero callers;
vdp_gallery_carousel_only tests-only; inventory_json_wait_ms / inventory_idle_loop_sec /
inventory_paths_for_dealer are discovery-only. Fix: delete dead knobs; move intercept_feed_is_sufficient
into inventory_recovery; fold the rest into the ScanConfig of F-11.

Checked, fine (cohesive):
- specials/ (__init__ 42, extract 343, fetch 118, scan 78, store 166, lease_matches_store 171):
  self-contained dealer-specials feature; web consumers (routes/dealership_page, intelligence/
  lease_matcher, reviews/store). Only nit: runtime CREATE TABLE in store/lease_matches_store and it
  is not a scanner concern (could be backend/specials/). P3.
- utils/vdp_spec_parse.py (501, fan_in 12), utils/vdp_extras_parse.py (341), utils/gallery_url_filter.py
  (157), utils/vdp_price_merge.py (132), utils/vdp_gallery_urls.py (41), utils/__init__.py (2): pure
  HTML/price parsing helpers; fine apart from F-20.

## Step 16 — remaining mid-size modules (checked)
- platform_registry.py (730; classify_dealer 92): DNS-CNAME + HTML platform learning registry,
  used by scripts only. Distinct from recipe_synth.fingerprint_platform (HTML markers) and
  platform_fingerprint (discovery clustering) — three classifiers, but documented as complementary.
  P3: classify_dealer could call recipe_synth.fingerprint_platform instead of its own marker table.
- platform_fingerprint.py (535): pure feature extraction + clustering. Fine.
- html_jsonld_harvest.py (336), carscommerce_harvest.py (250): browser-free harvesters, single job. Fine.
- scan_timing.py (242), scan_log.py (152), scan_lock.py (115), scrape_confidence.py (72),
  inventory_write.py (113), rooftop_ledger.py (112), dealer/sticker_provider.py (219),
  dealer/site_url.py (62), dealer/profile.py (133), constants.py (56; NEXT_SELECTORS etc. are
  browser-era, used by inventory_scrape only), browser_gate.py (61): single-purpose. Fine.
- inventory_card_location.py (151): browser-era SRP card location; dead on the scan path (F-19 #5).

---------------------------------------------------------------------------------------------------
# FINAL SUMMARY

## Ranked table
| # | Pri | Target | Size | Core problem | Fix sketch |
|---|-----|--------|------|--------------|------------|
| 1 | P1 | F-2 delta_scan.py::delta_scan_dealer | 250 ln | copy of run_dealer pipeline that has ALREADY drifted: no learned-street place, no _feed_scoped exemption -> can disown a store's own cars that the full scan keeps | rebuild on shared fetch/attribute/persist steps |
| 2 | P1 | F-19 attribution spread (parsers gate, rooftop_match, dealer/location sister filter, rooftop_disown, dealer_place, card locations, upsert owner guard) | 7 places | every fleet run's attribution fix lands in one of 7 places; two place functions; side-channel _feed_scoped flag | backend/scanner/attribution/ package, one place fn, one gate entry |
| 3 | P1 | F-1 phases/dealer_run.py::run_dealer | 664 ln | 12 phases in one async fn; dead browser vestiges; duplicated parse closures; 16 commits | split into phases/feed, attribution, recovery, enrich, persist, after_write + DealerRunResult |
| 4 | P1 | F-3 database.py::upsert_vehicles | 554 ln, fan_in 25 | row-normalization rules + JSON serialization + 190-ln SQL + post-write enrichment in one loop; per-row writes are the known throughput limit | write/normalize (pure), write/sql (batched), write/guard, write/after_write |
| 5 | P1 | F-7 recipes.py::try_fetch_via_recipes + file | 230 ln / 1165 ln, fan_in 82 | model+store+lifecycle+replay+coverage+fetch in one hub; parse/gate done here AND in dealer_run (forgotten rejected_out = Hendrick 3,931-VIN bug) | recipes/ subpackage behind facade; replay returns raw pages, gate once |
| 6 | P2 | F-18 six HTTP fetch implementations | ~600 ln total | proxy/impersonation/challenge handling differs per call site; recipe replay ignores SCANNER_HTTP_PROXY | one net/client.py |
| 7 | P2 | F-6 recipe_synth.py | 2455 ln | 17 platform plugins + HTTP layer + a 2nd validation stack in one file (registry already exists) | synth/platforms/<name>.py, synth/http.py, validation -> recipe_validation |
| 8 | P2 | F-10 orchestrator._run_post_scan_tail vs post_scan/job.run_post_scan_job | 108+126 ln | ~70% copy-paste stage sequence | one shared tail |
| 9 | P2 | F-9 post_scan/pipeline.py | 1174 ln | 10 stages + flags + Claude interior-vision client + gas prices/ratings | stage registry; vision -> backend/vision; housekeeping job |
| 10 | P2 | F-14 inventory_recovery + browser scrapers (dealer_eprocess 991 w/ 4 mappers, dealer_on, inspire, venom, pixel_motion) | ~2.8k ln | half the strategies can never run under HTTP-only; 2-3 mappers per platform vs backend/parsers | move page strategies to discovery/; scrapers map via parsers |
| 11 | P2 | F-8 post_scan/window_sticker.py | 2467 ln, ~35 importers | fetch+PDF+parse+engine rules+UI display in "scanner"; web/billing import scanner | backend/stickers/ behind facade (fetch untouched) |
| 12 | P2 | F-11 cli.py::run_cli_entry | 339 ln | config by mutating os.environ (139 env knobs across 43 files) | ScanConfig dataclass |
| 13 | P2 | F-12 orchestrator.py::_main_impl | 248 ln | nested closures, module-global shutdown flag, vestigial browser/stealth branch | run_fleet + shutdown token |
| 14 | P2 | F-17 phases/nav.py | 767 ln | drifted dead copies of manifest.filter_skip_dealers/_load_dealers_from_db + browser helpers | delete copies; move to discovery/ |
| 15 | P2 | F-4 phases/inventory_scrape.py::scrape_inventory_path | 503 ln | discovery-only; inline platform if-chain (pixel_motion/autowall/shopperexpress) | discovery/srp_capture + PLATFORM_HTML_SCRAPERS registry |
| 16 | P3 | F-15 vdp/ dead modules (queue, claude_extract, packages, specs, price_hints, core docstring) | ~900 ln | no production callers; stale docs | delete after consumer audit |
| 17 | P3 | F-16 vdp/prefetch.py | 776 ln | DB carry-forward + HTTP + merge rules mixed | carry_forward / fetch / apply |
| 18 | P3 | F-20 JSON-LD extraction x10 | — | duplicated helper | html/jsonld.py |
| 19 | P3 | scan_efficiency.py | 328 ln | env grab-bag, 3 dead VDP knobs, env-writing defaults | fold into ScanConfig |
| 20 | P3 | job_queue.py | 810 ln, fan_in 15 | queue+scheduler+admin service used by web | backend/jobs/ |
| 21 | P3 | S-1 shims (13 flat aliases) | 12 ln each | still imported from inside scanner (cli, orchestrator, dealer_run, inventory_recovery...) | codemod + delete |
| 22 | P3 | F-13 dealer/bmw_enhancer.py | 32 ln | flags with no readers | delete |
| 23 | P3 | F-5 discovery_capture env mutation | 1 line | sets SCANNER_ALLOW_BROWSER process-wide | context manager |
| 24 | P3 | recipe_validation.py, recipe_store.py, gap_fill.py, network_observer.py, inventory_reconcile.py, platform_registry.py | — | see sections | — |

## P1 details (short form; full text in the sections above)
- F-2 delta_scan: `roster_place(url)` at delta_scan.py:173 vs `roster_place_with_hints(url, dealer_id)`
  in dealer_run/recipes; union re-gate at delta_scan.py:186 has no `_feed_scoped` exemption (dealer_run
  446-458). Refused rows flow into `_disown_foreign_rooftop_vins` (delta_scan.py:218). Live via
  `scanner --delta` (cli.py:377). Fastest fix even before refactor: port those two lines.
- F-19 attribution: build backend/scanner/attribution/{place,gate,sister,disown}.py; one entry
  point called from dealer_run, delta_scan, recipe replay and validation. Measure what
  dealer/location's sister filter still removes after the gate before deciding to keep it.
- F-1 run_dealer: split per section list in Step 2; delete page/context/path_htmls/
  merged_card_locations/vdp_stats vestiges; shared steps make F-2 fall out for free.
- F-3 upsert_vehicles: pure normalize_row first (testable, no DB), then batched SQL (the
  documented post-fleet throughput item), then post-write passes with logged failures.
- F-7 recipes: facade + subpackage; stop parsing inside replay; make parse()'s rejected_out
  mandatory in backend/parsers so the "forgot rejected_out" class of bug cannot recur.

## P2 details
See F-18, F-6, F-10, F-9, F-14, F-8, F-11, F-12, F-17, F-4 above. Suggested order after P1: F-10
(small, removes a duplication), F-17 deletes, F-18 shared HTTP client (helps the Railway 403 work),
then F-6/F-14 (synth/scrapers split, parser unification), then F-9/F-8/F-11/F-12.

## Checked, fine (one line each)
- __init__.py — docstring only (stale wording: "Playwright", lists bmw_enhancer)
- paths.py, constants.py, browser_gate.py, http_fetch.py (proxy plumbing; see F-18 for fetchers)
- 13 flat shims (bmw_enhancer, claude_vdp_extract, dealer_location, dealer_profile, dealer_site_url,
  dealer_sticker_provider, listing_gap_fill, post_pipeline, vdp_html_recovery, vdp_packages_extract,
  vdp_spec_extract, vdp_specs_extract, window_sticker) — no duplication; S-1 only
- dealer/__init__.py, dealer/profile.py, dealer/site_url.py, dealer/sticker_provider.py
- dealer/bmw_enhancer.py — F-13 (dead) ; dealer/location.py — F-19 #4
- dealer_place.py (F-19 #6 only), rooftop_disown.py (F-19), rooftop_ledger.py, rooftop_match.py
  (F-19 #1; score_rows 143 ln long but one job)
- carscommerce_harvest.py, html_jsonld_harvest.py, platform_fingerprint.py, platform_registry.py
- discovery_capture.py (F-5 only), recipe_cascade.py, recipe_store.py (P3 DDL), recipe_validation.py (F-6 merge target)
- inventory_card_location.py (dead on scan path), inventory_reconcile.py, inventory_write.py,
  inventory_recovery.py (F-14)
- job_diagnosis.py, job_queue.py (P3), manifest.py, network_observer.py (P3), chain.py (F-18)
- scan_lock.py, scan_log.py, scan_timing.py, scrape_confidence.py, scan_efficiency.py (P3)
- phases/__init__.py, phases/upsert.py, phases/site_profile.py (moves with F-4)
- post_scan/__init__.py, post_scan/auto_heal.py, post_scan/coverage_report.py, post_scan/gap_fill.py (P3), post_scan/job.py (F-10)
- scrapers/__init__.py, algolia_scope.py, autowall.py (P3 split playwright half), dealer_com_bulk_fetch.py,
  inventory_vin_merge.py, next_data_inventory.py, scanner_intercept_filter.py, shopperexpress.py;
  dealer_eprocess.py / dealer_inspire.py / dealer_on.py / dealer_venom.py / pixel_motion.py -> F-14
- specials/__init__.py, extract.py, fetch.py, lease_matches_store.py, scan.py, store.py
- utils/__init__.py, gallery_url_filter.py, vdp_extras_parse.py, vdp_gallery_urls.py, vdp_price_merge.py, vdp_spec_parse.py
- vdp/__init__.py, vdp/core.py, vdp/config.py, vdp/queue.py, vdp/claude_extract.py, vdp/packages.py,
  vdp/specs.py, vdp/price_hints.py -> F-15 (dead/stale); vdp/extract.py, vdp/html_recovery.py,
  vdp/spec_fetch.py, vdp/vdp_recipes.py fine; vdp/prefetch.py -> F-16
