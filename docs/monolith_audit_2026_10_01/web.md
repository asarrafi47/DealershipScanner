# Web area monolith audit (134 files) — 2026-10-01
Read-only. Working notes appended incrementally; ranked table + P1/P2 details + checked-fine list at end.

## Working notes

### W1. backend/main.py (1472 lines, 56 defs, 104 imports, 12 pkgs, fan_in 71) — P1
Responsibilities (line ranges):
- 1-135 env/vault load + 104 imports (intelligence.ai.agent, scanner.job_queue, enrichment, db.*, billing, auth, dealer, dev).
- 137-150 rate-limit/chat constants copied from Config (module globals tests monkeypatch).
- 154-178 `_session_belongs_to_paid_org`, `_post_login_redirect` (auth flow, used by google/apple/dealer via lazy `from backend.main import`).
- 181-340 Flask app construction, secret key, ProxyFix, cookies, static cache-busting + precompressed static serving (`compute_static_cache_ver`, `_static_view`, `_static_cache_headers`).
- 342-392 import-time side effects: production security asserts, `assert_inventory_backend_configured`, `init_job_queue_schema()` (scanner job-queue DDL runs on every web import), EPA prewarm thread.
- 394-463 blueprint registration + 10 `routes.X.register(app)` + backward-compat re-exports of private helpers from routes modules (cars_pages/fuel_api/home_dashboard/listings_api).
- 465-493 gzip after_request; 495-540 context processor (does a `get_user_profile` DB read on EVERY template render for logged-in non-admins).
- 542-748 billing/paid gates: `_billing_enabled`, `_org_subscription_active`, `_require_paid_org_session`, `_dev_operator_grants_premium` (lazy import of backend.dev.routes), `_session_has_paid_access`, `_viewer_sees_premium_features`, `_require_feature`, `_feature_denied_json`, `_require_premium_feature`, `_billing_gate_paid_routes` before_request.
- 568-643 CSRF before_request with two hard-coded endpoint allow-lists (~35 bare endpoint names; new routes silently unprotected unless added).
- 750-833 CSP header build, jinja filters, 404.
- 845-1206 HTML auth pages (login/register/verify/resend/forgot/reset/logout) + account profile/password/billing/portal + profile helpers.
- 1209-1345 JSON auth API (/api/auth/*), MFA "gone" stubs.
- 1347-1400 `_finalize_app_session` (session population; imported lazily by 3 other packages).
- 1400-1408 leftover module state owned for extracted modules: `_reco_cache`, `_RECO_CACHE_TTL_S`, `_LIVE_GAS_PRICES_PATH`, `_live_gas_prices_cache(_mtime)` — mutated by routes/home_dashboard.py and routes/fuel_api.py via `main.<name>`.
- 1410-1471 `_nhtsa_recalls_lookup_payload` (domain logic, NHTSA fetch + VIN validation).
Why it hurts: it is the app factory, the auth blueprint, the billing policy, the CSRF/CSP policy and a shared-state bag at once. 10 route modules reach back via `main_module().<name>` (~120 attribute lookups: main._require_feature x11, get_car_by_id x10, _feature_denied_json x10, ...), 4 packages do `from backend.main import _finalize_app_session/_post_login_redirect` inside functions, dev/scan_lab_routes imports `_build_car_detail_view_context` from main. Importing anything that touches main pulls the whole app incl. scanner.job_queue DDL and the AI agent. The "monkeypatch backend.main.X" test convention (routes/_shared.py docstring) is what freezes this shape.
Split:
- `backend/web/app_factory.py`: `create_app()` (181-340 config, static, ProxyFix, blueprint registration, prewarm hook). Move `init_job_queue_schema()` out of web import into scanner startup or an explicit `ensure_schema` CLI.
- `backend/web/security.py`: CSRF hook + CSP + nonce; replace endpoint lists with a `@csrf_form` / `@csrf_header` decorator or a view attribute (default-deny for POST/DELETE).
- `backend/billing/access.py`: ONE access policy (see W2) — `can_use(feature)`, `denied_json(feature, err)`, `viewer_features()`, `paid_org_gate`.
- `backend/auth/session.py`: `finalize_app_session`, `post_login_redirect`, `session_belongs_to_paid_org` (kills the 4 lazy `from backend.main import`).
- `backend/auth/pages.py` (blueprint `auth`): login/register/verify/forgot/reset/logout + `/api/auth/*` (845-1345). Endpoint renames need `url_for` updates in templates — or keep bare names via `app.add_url_rule` in register().
- `backend/routes/account.py`: account profile/password/billing (1008-1206).
- `backend/enrichment/recalls_lookup.py`: `_nhtsa_recalls_lookup_payload`.
- Move `_reco_cache` into routes/home_dashboard.py and gas-price cache into routes/fuel_api.py (they are the only writers).
- Tests: replace `monkeypatch backend.main.X` with patching the owning module; this is the real cost (grep: dozens of tests).
Blast radius: high (71 importers; templates use bare endpoint names; tests monkeypatch main). Do it in steps: state moves first (no API change), then gates, then auth blueprint.

### W2. Paid-feature gates — four disagreeing policies — P1 (correctness bug, not just structure)
- `main._session_has_paid_access` (655): admin OR dev operator OR session `user_is_premium` OR org status active. Reads cookie flags WITHOUT DB revalidation. Feeds template `has_paid_access` (context processor 524, account 1216) and `_viewer_sees_premium_features` (666), which cars_pages uses for trim ladder/window-sticker/packages UI (cars_pages 423, 471-478, 565, 595).
- `main._require_feature(fid)` (675): dev operator bypass; login required in prod; then `billing.entitlements.require_feature` -> plan feature sets with DB revalidation; maps `feature_required`->`premium_required`. Used by APIs: cars_pages 367/784/853/1039/1130/1240, listings_api 411/685/698/717, dealers_recalls 23, main 1028.
- `main._require_premium_feature` (705): plan-agnostic "any paid" check — appears unused outside main now (verify; dead-code candidate).
- `billing.entitlements.require_feature` called directly by routes/ai_chat_bp.py:153 — no dev-operator bypass, own login rule, returns `feature_required` (not `premium_required`), so the client upgrade hint (`_feature_denied_json`) never fires there.
Concrete bug: billing/routes.py:210/347 set `session["user_is_premium"]=True` on any checkout; a Research-plan user (RESEARCH_FEATURES = market_intel, vehicle_history, nearby_dealers, saved_searches) therefore gets `has_paid_access`/`_viewer_sees_premium_features` True -> car page renders window-sticker/packages/chat UI, while the APIs (`_require_feature(FEATURE_WINDOW_STICKER / PACKAGES_ENSURE / AI_CAR_CHAT)`) return 403. Also UI path never revalidates, so a canceled sub keeps seeing paid UI for the cookie lifetime while APIs refuse.
Split/fix: `backend/billing/access.py` with `features_for_viewer(session) -> frozenset` (single source: entitlements_from_session + dev-operator + billing-off/login rules), `can_use(fid)`, `deny(fid)`. Template context gets `viewer_features` (set) and templates test `'window_sticker' in viewer_features` instead of a boolean. Delete `_session_has_paid_access`/`_viewer_sees_premium_features`/`_require_premium_feature`; ai_chat_bp uses `can_use`. Blast radius: car.html (5), listings.html (2), premium.html (3), nav partials, compare/home/dashboard templates, ~20 tests (test_trim_ladder, test_ux_premium_gates, test_window_sticker_ui_visibility, test_dev_operator_premium...).
- W2 addendum: `_require_premium_feature` has zero production callers (only backend/tests/test_dev_operator_premium.py:20) -> delete with the test.

### W3. Duplicated `_client_ip` wrappers — P3
Five identical 1-line wrappers around `backend.utils.client_ip.client_ip(request)`: routes/_shared.py:35, routes/ai_chat_bp.py:65, routes/ai_narrate_bp.py:36, auth/apple_oauth.py:75, auth/google_oauth.py:73 (main.py re-exports _shared's). Logic is NOT duplicated (all delegate), so harm is low. Real wart: routes/dealer_reviews.py:20 `_client_ip_hash` falls back to `request.remote_addr` on exception, which behind Railway's proxy hashes the edge IP (all reviewers collide). Fix: add `client_ip()` (no-arg, uses flask.request) in utils/client_ip.py, delete wrappers, drop the broad except. Blast radius: tiny (tests may monkeypatch `_client_ip` on these modules — grep first).

### W4. backend/dev/routes.py (1483 lines, 58 defs) + dev/console.py (395) + dev/scan_lab*.py + dev/dealers.py — P1 (largely DEAD/BROKEN paths, delete before splitting)
dev/routes.py responsibilities:
- 73-292 a private job store (in-memory dict + optional SQLite `DEV_JOB_STORE_SQLITE_PATH`): `_dev_job_store_conn/_dev_store_get/put/patch/_dev_queue_patch_item`, eviction.
- 293-425 admin auth for /dev: `_admin_session_ok` (also imported by main._dev_operator_grants_premium), `_finalize_dev_session`, IP allow-list `_dev_client_ip_allowed`, `_dev_require_admin` before_request, vector reindex spawner.
- 425-600 Node binary discovery (`_probe_node_binary`, `_resolve_node_binary`, cache), `_dev_status`.
- 603-850 subprocess job runners: `_run_scanner_job` (603-640), `_run_smart_import_job` (652-849, 197 lines: profile retry loop x3, cancel watcher thread, prefixed-JSON stdout protocol, DealerCreate validation, dealers.json upsert, registry insert, car linking, vector reindex).
- 851-960 dev login/register/logout/MFA-gone pages (a SECOND login system next to main.login_page).
- 961-1460 ~30 JSON endpoints: status, dealers, incomplete cars, audit-last-scrape, insert/delete dealer, test-scanner, smart-import(+bulk queue), geocode, dedupe, spec-backfill, enrich_all, car-debug.
VERIFIED BROKEN/DEAD (2026-10-01, on disk):
- `_run_scanner_job` (613) and `_run_smart_import_job` (694) launch `node backend/scanner/scanner.js` — that file does not exist (scanner is Python, HTTP-only). /dev/api/test-scanner, /dev/api/smart-import, /dev/api/smart-import-bulk can only fail. The whole Node-probe block (425-600) only serves them; audit-last-scrape (1017-1073) tells operators to "Run node scanner.js".
- dev/console.py:47-48 `_PROJECT_ROOT = Path(__file__).parent.parent` = `backend/`, `_SCANNER_SCRIPT = backend/scanner.py` — does not exist (scanner.py is at repo root). /api/dev/scan-dealer logs "scanner.py not found" and does nothing.
- dev/dealers.py:9-10 `ROOT = Path(__file__).resolve().parent.parent` = `backend/` so `DEALERS_PATH = backend/dealers.json` (4 KB, last touched Jun 14) while the scanner reads repo-root `dealers.json` (3 MB, backend/scanner/constants.py:10). Docstring says "project-root dealers.json". Every manifest write from the dev console / smart import lands in a file the scanner never reads.
Why it hurts: 1.5k lines of operator tooling where the main flows are dead, two separate dev auth systems (dev_bp `_admin_session_ok` + IP allow-list vs console.py `require_dev_access` with DEV_CONSOLE secret), three ways to spawn a scan (dev/routes, dev/console, dev/scan_lab._run_manifest_scan_job) plus a fourth in routes/admin_dealer_api (job_queue). main.py imports dev.routes for the premium bypass.
Split / action:
1. Delete: Node probe + `_run_scanner_job` + `_run_smart_import_job` + test-scanner/smart-import/bulk/import-queue endpoints (603-850, 1092-1320, 425-600) — or rewire smart import onto `backend/scanner/job_queue` (the path admin_dealer_api already uses). Confirm with owner first (feedback: audit deletions before editing — grep templates `frontend/templates/dev*.html` + static JS consumers).
2. Fix dev/dealers.py ROOT to `parents[2]` (or import `backend.scanner.constants.MANIFEST_PATH`), fix console `_SCANNER_SCRIPT` — or delete console scan-dealer in favour of job_queue.
3. What remains splits into `backend/dev/job_store.py` (73-292), `backend/dev/auth.py` (293-425, 851-960; exported `admin_session_ok` for billing.access), `backend/dev/api_inventory.py` (incomplete cars, car-debug, spec-backfill, enrich_all, geocode, dedupe).
4. One dev auth: fold console.py's secret gate into dev/auth.py.
Blast radius: dev-only surface; tests under backend/tests/test_dev_* and test_smart_import*; main._dev_operator_grants_premium import path.
dev/scan_lab.py (566) / scan_lab_routes.py (178): coherent; scan_lab_routes defines all 14 routes inside one 160-line `register_scan_lab_routes` closure and does `from backend.main import _build_car_detail_view_context` (line 72) — import from routes.cars_pages instead. P3.
dev/dealer_url_infer.py (478): cohesive single purpose (homepage -> manifest fields); fine. dev/dealers.py: cohesive except the ROOT bug above.

### W5. backend/routes/cars_pages.py (1346 lines, 30 defs; 12 main_module() lookups) — P1
Responsibilities:
- 44-178 window-sticker preview URL + constants; 179-360 a background-thread job manager for "packages ensure" (`_packages_ensure_inflight` dict of Threads 108, lock 120, budget/cooldown/TTL/poll env readers, `_run_packages_ensure_with_budget` spawning daemon threads 347) — process-local in-flight state inside a web route module (with GUNICORN_WORKERS=1 it works; any scale-out gives per-worker duplicates).
- 361-405 sticker preview PNG serving; 406-435 compare page.
- 436-758 `_build_car_detail_view_context` (322 lines): favorites/views (437-450), knowledge_engine context (450), window-sticker eligibility via `backend.scanner.window_sticker` (web importing scanner) + enrichment.window_sticker_service (451-500), serialize (499), incomplete-field codes (504), registry dealer info (512-525), attribution overlay (526-536), MSRP/color sticker overlays (538-558), dealer map (559-562), market price + deal score gated by `_session_has_paid_access` (565-580; API twin in listings_api:411 gates on FEATURE_MARKET_INTEL instead — another gate disagreement), trim ladder + premium strip (582-603), rarity (605-615), geo/hero/rating (618-625), generated build sheet (626-655), unified options list (660-685), then a ~70-line context dict. Shared by HTML `car_detail`, JSON `api_car_detail`, and dev scan_lab.
- 760-865 car detail / JSON / window-sticker / recalls / vehicle-history endpoints.
- 866-1127 packages-ensure payload assembly + `api_car_packages_ensure` (120-line endpoint with a documented retry contract).
- 1128-1318 `api_car_chat` and `api_compare_chat`: each re-implements the same gate -> global/IP/pair/daily rate-limit ladder -> body-size check -> agent call; ai_chat_bp.api_ai_chat (routes/ai_chat_bp.py:147-305, 158 lines) is a third copy with a different gate (W2).
Why it hurts: the car-page context builder is a 15-step pipeline with ~20 function-local imports, so its dependencies are invisible and any one overlay failure is handled ad hoc (some try/except, some not). Changing what the JSON API returns means editing an HTML-context builder. Threads-in-route-module make packages-ensure untestable without timing.
Split:
- `backend/car_page/context.py`: `build_car_detail_context(car_id, car_raw, viewer)` as an ordered list of small step functions (`_sticker_state`, `_dealer_block`, `_overlays`, `_market_block`, `_trim_ladder_block`, `_options_block`), each taking/returning a dict; viewer = `billing.access.viewer_features()` (fixes the market gate drift).
- `backend/car_page/packages_ensure.py`: in-flight registry, budget/cooldown, `_run_packages_ensure_with_budget`, payload/finalize/panel snapshot (179-360, 866-1007). Route stays a 20-line adapter.
- `backend/routes/chat_common.py`: `chat_preflight(feature_id, rate_key_prefix)` returning (ok, response) shared by api_car_chat, api_compare_chat, ai_chat_bp.
- Window-sticker eligibility helpers should come from enrichment, not `backend.scanner.window_sticker` (web should not import the scanner package).
Blast radius: medium — car.html/compare.html context keys must be preserved (snapshot test the dict keys before/after); tests monkeypatch `backend.main.get_car_by_id`/`prepare_car_detail_context` (resolved via main_module).

### W6. backend/routes/listings_api.py (928 lines, 41 defs; 15 main_module() uses) — P2
Responsibilities: 41-80 /search redirect, /listings, /premium page (billing pricing page living in listings!); 82-200 session geo, filter options, geo coords, radius clamp/snap, zip normalize; 200-340 a per-process "cars scope" cache with build locks (`_cars_scope_*`, `_build_cars_scope_entry`, `_cars_json_response`); 340-470 /api/listings/cars, market-stats (gate FEATURE_MARKET_INTEL), zip<->coords; 473-628 smart search (parse + POST + history recording); 630-735 saved cars + saved searches; 737-876 hidden dealers (incl. `_known_dealer_name` importing private `dealership_page._find_dealership_by_dealer_id`) + search history; 877-928 register.
Why it hurts: 6 unrelated feature areas; every user-data endpoint resolves its DB function through `main.<name>` (save_car, hide_dealer, list_saved_searches...) purely so tests can monkeypatch backend.main.
Split: `routes/listings_api.py` (grid + scope cache + market stats + geo), `routes/smart_search_api.py` (473-628), `routes/user_lists_api.py` (saved cars, saved searches, hidden dealers, search history; 630-876), `/premium` -> billing/routes.py. Move `_cars_scope_*` cache into `backend/listings/cars_scope_cache.py`. Import DB funcs directly; tests patch the new module.
Blast radius: medium-low; endpoint names unchanged if registered with same bare names.

### W7. backend/routes/dealership_page.py (861 lines, 26 defs) — P2
Responsibilities: 53-130 dealer key resolution (`_clean_dealer_key`, `_host_to_dealer_id`, `_fetch_dealership_by_id`, `_find_dealership_by_dealer_id`, `_resolve_dealer`, `_dealer_id_for_key`) — reused privately by routes/dealer_reviews.py and routes/listings_api.py; 131-195 per-dealer grid-card TTL cache; 196-430 facets: `_known_catalog_makes`, `_dealer_facet_make_valid`, `_dealer_facets` (174 lines, builds facet lists in Python from cards by importing 5 PRIVATE helpers of db/repositories/listings_repo and search_repo); 431-528 inventory + filter zip; 529-616 lease offer parsing/matching with SQL on view (`_attach_lease_matches`, specials); 617-650 reviews block; 652-843 views (`dealership_research_page` 120 lines).
Why it hurts: dealer resolution is a shared service hidden as private functions in a page module; facets re-derive the /listings facet logic against private repo internals (drift risk: listings_repo._build_filter_options_uncached is the SQL twin).
Split: `backend/listings/dealer_resolve.py` (53-130, 469-504; public `resolve_dealer`), `backend/listings/facets.py` (public `canonical_facet_label`, `facet_make_valid`... promoted out of listings_repo; `dealer_facets(cards)` + `known_catalog_makes`), `backend/dealer/specials.py` (529-616 lease matching). Page module keeps views + cache.
Blast radius: low-medium (dealer_reviews, listings_api, tests of dealership page/facets).

### W8. backend/listings/nearby_dealers.py (1199 lines, 28 defs) — P2
Responsibilities: 14-42 env kill switches; 43-342 read-side enrichment (listing counts by registry id / dealer id, last-scraped / catalog-scan timestamps, `attach_listing_counts`, `attach_last_synced_at`); 343-516 rooftop inventory/geopoint/registry readers; 517-736 OFFLINE write pass: mis-stamped registry detection + `UPDATE cars` (603) + `repair_mis_stamped_registry_ids` (only caller: backend/scripts/backfill_dealership_registry.py); 737-891 OFFLINE registry minting `_register_rooftops` (`INSERT INTO dealerships` 858/865, `UPDATE dealerships` 834) + name matching; 892-1081 request path radius search (`_dealers_with_inventory_near`, `_registry_ids_for_dealer_ids`, `_cars_matching_search_in_radius`); 1082-1199 `resolve_nearby_dealers_for_listings` (117 lines, request entry).
Why it hurts: the request-path picker and a registry-mutating repair job share one module and private helpers; docstrings repeatedly have to assert "offline only / nothing on the request path calls this", i.e. the separation is enforced by comments. A future edit can re-wire a write into a GET.
Split: `backend/listings/nearby_dealers.py` keeps 892-1199 + read helpers it needs; `backend/listings/dealer_activity.py` (counts + last-synced attachers 43-342, also used by dealer locator); `backend/dealer/registry_repair.py` (517-891: stamp repair + rooftop registration, env switches) imported only by the backfill script. In-memory stamp correction stays in the picker but imports `_mis_stamped_pairs` from registry_repair as a pure function.
Blast radius: low (7 importers; script import path changes).

### W9. backend/utils/query_parser.py (1290 lines, 47 defs; parse_natural_query 113) — P3
Cohesive single domain (NL search -> filter dict), but: 98-224 runs `SELECT DISTINCT make, model FROM cars` / package names directly via `inventory_db.get_conn` (lru_cache keyed by DB path) — a DB-reading module under utils; 225-1176 ~25 independent extractors (body style, colors, price, mileage, condition, cylinders, fuel, features, packages, engine liters, Mercedes line, trim, multi-vehicle segments); 1177-1290 orchestrator.
Why it hurts: size, not coupling (14 imports, fan_in 9). Hard to find an extractor; vocabulary loader hidden in utils.
Split (package `backend/search/nlq/`): `vocabulary.py` (98-224 DB loaders + caches), `extract_vehicle.py` (make/model/trim/Mercedes/segments 538-1160), `extract_attributes.py` (body/color/drivetrain/fuel/cylinders/engine/condition), `extract_money.py` (price/mileage/years), `extract_equipment.py` (features/packages 656-790), `__init__.py: parse_natural_query`. Keep `backend/utils/query_parser.py` as a re-export shim.
Blast radius: low (pure functions; tests import private extractors — keep names in shim).

### W10. backend/utils/hybrid_search.py (828 lines; filters_dict_to_search_cars_kwargs 142) — P3
Mixes: filter-dict -> SQL kwargs translation (136-279), Flask request -> kwargs (280-374), hybrid/semantic search with vector rerank calling `inventory_db.search_cars` (375-803), result sorting. `_normalize_listings_vin_query` is a private helper imported by main._nhtsa_recalls_lookup_payload.
Split: move with query_parser into `backend/search/` (`filters.py` 136-374, `hybrid.py` 375-828); publicize VIN normalizer in `backend/utils/vin.py`. Low blast radius (fan_in 17, mostly the kwargs translator).

### W11. backend/utils is a grab-bag (75 files in area incl. car_serialize/) — P2 (structural)
`backend/utils/__init__.py` has fan_in 674. Mixed in one namespace:
- true leaf utilities (no backend deps): roles, csrf, client_ip, totp, mfa_otp, qr_segno, runtime_env, project_env, bootstrap_policy, safe_listing_url, outbound_url, oem_links, registration_validation, ip_rate_limit, credential_db_encryption, model_aliases, vehicle_naming, first_seen, in_transit, mileage_display, dealer_rating_display, spec_provenance, interior_color_buckets.
- vehicle-data normalization/plausibility domain (should be `backend/vehicle_data/`): field_clean (fan_in 90), forced_induction, fuel_type_normalize, fuel_label_plausibility, transmission_normalize, engine_consistency, spec_field_normalize, price_plausibility, msrp_trust, listing_completeness, listing_description_extract/persist, analytics_ep, gallery_merge, inventory_repair, incomplete_recovery, vpic_specs, oem_option_catalog, vdp_* shims, rarity_score, market_price, compare_specs.
- DB-reading/service modules that import backend.db/enrichment/intelligence: car_serialize/* (9 upward imports in serialize.py), market_price (7), msrp_trust (4), query_parser, hybrid_search, rarity_score, listing_completeness, dealer_vin_prefill, search_history_format.
- network/LLM clients (should be `backend/llm/` or `backend/integrations/`): llm_client, local_llm (13 subprocess/HTTP hits; spawns a local server), web_researcher (Playwright/web), kmac_vault (secret fetch), mfa_delivery (email/SMS send), vehicle_narrator, car_chat_policy.
Why it hurts: `utils` importing `backend.db`/`backend.enrichment` creates upward edges, so anything importing a "utility" can pull the DB layer; cycle risk with enrichment importing utils back.
Split: `backend/vehicle_data/` (normalizers + plausibility), `backend/presentation/car_serialize/` (move package as-is), `backend/search/` (W9/W10), `backend/llm/` (llm_client, local_llm, web_researcher, vehicle_narrator, car_chat_policy), `backend/security/` (csrf, totp, mfa_*, kmac_vault, credential_db_encryption, production_security, outbound_url, ip_rate_limit, client_ip). Leave re-export shims at old paths (project already uses alias shims, e.g. vdp_*.py 4-line files). Rule after split: `backend/utils` imports nothing from backend.* except other utils.
Blast radius: high in count but mechanical with shims; do it last.

### W12. backend/utils/car_serialize/serialize.py::serialize_car_for_api (577 lines, 42-618; file 1056) — P1
Phases (approx): 42-80 clean row + `merge_verified_specs` (DB/enrichment call per car unless caller passes verified_specs) + BMW display + engine display; 80-175 URL normalization, 360 spin, VDP source URL; 176-195 price plausibility withholding; 196-216 forced induction; 217-328 transmission resolution (feed vs persisted bucket vs vPIC TransmissionStyle, `transmission_feed`); 329-445 extended specs: hp gate + vPIC fill-behind with `horsepower_source/_note`, suppression of curb weight / battery_kwh / tow, tank + EV range via intelligence resolvers, model generation, EV-only gating; 447-520 catalog options/packages + trim decode (enrichment calls, gated by include_extended_display); 520-610 mileage, paint buckets, price history, state, fuel requirement, TCO (intelligence.tco_fuel_estimates), MSRP trust, payment-shaped price; 612-618 deal score (intelligence.deal_score_cache).
~60% of the function body is policy commentary justifying each suppression — valuable, but it means data-precedence POLICY (vPIC > feed, withheld figures) lives inside a JSON serializer in `backend/utils`, with 9 lazy upward imports (enrichment.knowledge_engine x2, knowledge_engine_specs, catalog_lookup, intelligence.* x4). Per-car enrichment queries are opt-out flags (`include_verified`, `include_extended_display`), so a caller that forgets them does N reference queries per list.
Why it hurts: every consumer of the car shape (car page, API, compare, chat agents? — no: comment at 341 says the AI agent reads merge output directly and never calls this serializer, so precedence rules already DIVERGE between page and chat), any policy change touches this function; impossible to unit test one rule.
Split (`backend/presentation/car_serialize/`, or keep package and add modules):
- `spec_policy.py`: `resolve_horsepower(c, vs, vpic)`, `suppressed_extended_fields`, `resolve_tank_and_range`, EV gating (329-445) — pure, also called by the AI chat agent so chat and page agree.
- `transmission_policy.py`: 217-328.
- `price_policy.py`: 176-195 + msrp/payment-shape + deal score (520-618).
- `catalog_block.py`: 447-520 enrichment lookups behind an explicit `EnrichmentContext` passed in (no lazy imports; list callers pass none).
- `serialize.py`: thin `serialize_car_for_api` composing the above in order (target <100 lines); grid serializer (889-1013) and price-history helpers (621-740) to `grid.py` / `price_history.py`.
Blast radius: high importance (car_serialize/__init__ fan_in 88) but signature can stay identical; add golden-output tests on ~50 real rows before moving.

### W13. backend/utils/forced_induction.py::_classify_by_make_model (328 lines, 106-434) — P3
A 24-branch `if m == "<make>"` chain encoding per-make/era rules (BMW, Mercedes, Audi, Ford, ...) with regexes and year/cylinder/displacement thresholds; the rest of the file (text/trim/display helpers 11-105, `classify_forced_induction` tiers 436-524) is fine.
Why it hurts: domain knowledge as control flow — untestable per rule, no provenance, and it ignores the per-VIN fact the project says wins: NHTSA vPIC `Turbo` is already decoded in backend/enrichment/nhtsa_vpic.py:247 but `classify_forced_induction` never consults it (CLAUDE.md: vPIC outranks feed; catalog never outranks VIN decode). This is a heuristic used for display (serialize.py:196) on 11 importers.
Split: `backend/vehicle_data/forced_induction_rules.py` — a declarative table `[(make, model_regex, trim_regex, min_year, max_year, cyl, displ_range, result)]` evaluated by a 20-line loop; add a tier 0 in `classify_forced_induction` that uses the cached vPIC decode when present. Per-make unit tests become table rows. Blast radius: low.

### W14. backend/utils/analytics_ep.py::merge_analytics_ep_into_vehicle (287 lines, 489-776; file 797) — P2 (misplaced, scanner code in web/utils)
Only importers: backend/scanner/database.py, backend/scanner/vdp/vdp_recipes.py, backend/scripts/merge_ep_batch.py — it is a SCANNER ingest step (DDC analytics `ep` payload overlay), not a utility. One function fills ~20 fields in sequence (VIN 517, stock 524, year 529, make 539, model/BMW parse 545-570, trim 562, transmission 571, drivetrain 589, interior/exterior color 601-622, fuel type 623-670, body 674, certified/CPO 693-710, condition 711, engine/cylinders 734, BMW trace diagnostics 746-765), each with its own "is the existing value worse" rule.
Split: move to `backend/scanner/ingest/analytics_ep/` — `normalize.py` (flatten/aliases/nested 286-414, the `_norm_*` helpers 45-150), `bmw.py` (212-285 + trace), `merge.py` with a field table `FIELDS = [("transmission", _norm_transmission, _prefer_if_missing), ...]` driving a loop, special cases (VIN, condition/CPO, colors) as named functions. Shim at old path.
Blast radius: scanner-only; no web consumer.

### W15. backend/utils/field_clean.py (1013 lines, 27 defs, fan_in 90 — highest in area) — P2
Five concerns: 1-253 placeholder/emptiness predicates (`is_effectively_empty` etc., used everywhere); 255-470 storage coercion + filter preset vocab (drivetrain/fuel/body, `*_for_filter`, `sort_*_presets`); 472-750 model-specific repairs (Jeep Wrangler body, bZ Woodland split, stock-code guard, condition canonicalization) + `clean_car_row_dict` (750-854, the row cleaner every serializer calls); 855-935 display + `build_compact_listing_document` (vector-index text); 936-1013 MPG display, `compute_data_quality_score`, `clean_url_for_db`.
Why it hurts: fan_in 90 means any import of `is_effectively_empty` drags model-specific repair logic and its constants; edits to the cleaner risk the whole app.
Split: `backend/vehicle_data/placeholders.py` (1-253; the 90-importer hot path, keep tiny), `vehicle_data/coerce.py` (255-470), `vehicle_data/row_clean.py` (472-854), `search/document.py` (build_compact_listing_document), `vehicle_data/quality.py` (quality score). field_clean.py stays as re-export shim. Blast radius: mechanical; zero behaviour change.

### W16. backend/utils/market_price.py (608 lines) — P2 (latent memory/staleness bug, not size)
Cohesive module, but `get_cohort_index` (356-394) caches a full `SELECT ... FROM cars WHERE listing_active` scan (all active listings, ~120-214k rows) per key `(sqlite_file_mtime, zip, radius)` in an UNBOUNDED module dict `_cohort_cache` (22, 391). `_inventory_db_mtime` (25-35) stats the SQLite `DB_PATH`; on Postgres (prod) that file mtime never moves (or is 0.0), so (a) market averages never refresh for the life of the worker, (b) every distinct ZIP/radius pair adds another full-fleet scan + index that is never evicted. Verify on prod before acting (needs a read of how DB_PATH resolves under INVENTORY_DATABASE_URL), but the shape matches the 2026-09-16 web-memory findings.
Fix/split: key on a real data token (e.g. the grid_cards_repo / listings_repo invalidation token or `max(scraped_at)` checked every N min), LRU-bound the dict (e.g. 32 entries), and compute cohorts once fleet-wide then filter by geo per request. Move to `backend/listings/market_price.py` (it reads the DB; not a utility). Blast radius: 9 importers, signature unchanged.

### Checked notes (utils, medium)
- listing_description_extract.py (729): cohesive (deterministic tier + optional LLM tier `_llm_extract` 437-562). Misplaced in utils (calls an LLM); fits `backend/enrichment/`. P3.
- car_serialize/engine.py (717): already split out of the old monolith; fine except upward lazy imports of `backend.scanner.window_sticker` (573, 673, 696, 709) — web presentation depending on the scanner package; move `known_oem_engine_from_car` & friends to enrichment. P3.
- web_researcher.py (676): chat research helper incl. Playwright launch (57) — a headless browser inside the web process, gated by `car_chat_policy.web_research_playwright_allowed`. Cohesive; belongs in `backend/llm/` or intelligence. P3.

### W17. Login logic copied across 4 password endpoints (+2 OAuth, +dev console secret) — P1 (has a live divergence)
Password login implementations: main.login_page (845-875), main.api_auth_login (1236-1254, iOS/JSON), dealer/routes.py::dealer_login (73-101), dev/routes.py::admin_login (851-889); plus register twins main.register_page / main.api_auth_register / dealer_register / dev admin_register. Each re-does: rate-limit key (different keys: `login:`, `dealer_app_login:`, dev's own RPM), strip, authenticate, `sync_env_admin_user_row`, `session.clear()`, `_finalize_app_session`, redirect.
Verified divergence: main.login_page retries `authenticate_app_user` with the UNSTRIPPED password ("/register kept outer spaces before 2026-09-30; those hashes need the raw text", 865-867). api_auth_login and dealer_login do not, so an account registered with a leading/trailing space logs in on the website but fails in the iOS app (/api/auth/login) and the dealer portal. dealer_login also uses a different primitive (`check_user` + `get_user_by_login` twice) than `authenticate_app_user`.
Split: `backend/auth/login_service.py` — `attempt_password_login(login, raw_password, *, rate_key) -> LoginResult` (rate limit, both password forms, admin sync, session finalize) and `register_account(...)`; the four views become form/JSON adapters. Move `_finalize_app_session` / `_post_login_redirect` here too (kills `from backend.main import` in auth/google_oauth.py:189, auth/apple_oauth.py:218, dealer/routes.py:95,143).
Blast radius: medium (auth tests; templates unchanged).

### W18. backend/dealer/admin/* (14 files, ~1.9k lines) — P2
- `_require_site_admin` is copy-pasted in 6 hub modules (attribution_hub:29, data_quality_hub:35, dealers_hub:15, reviews_hub:16, scanner_ops_hub:13, users_hub:37; 3 distinct bodies by hash) + `_require_site_admin_api` in operator_api.py:18; `_fetch_scalar` duplicated in inventory_queries.py:11 and platform_stats.py:15. Hubs register by side-effect import in `__init__.py` (and main.py:53-54 imports two of them AGAIN).
- operator_api.py:64-93 `_delegate_dev_operator_routes` proxies 10 handlers from backend.dev.routes under `/api/admin/operator/*`, including smart-import / smart-import-bulk / scanner-job / import-queue — the Node-scanner.js paths that cannot work (W4). So the dead code is exposed twice (frontend/templates/admin/scanner_ops.html calls it).
- routes.py (393): store-admin blueprint, access guard, context processor, site hub, inventory list/detail/notes/review/export — cohesive.
Split: `backend/dealer/admin/guards.py` with `site_admin_required` (decorator, html + api variants) and `store_profile_required`; `backend/db/sql_helpers.fetch_scalar`. Register hubs explicitly in a `register_admin(app)` function instead of import side effects. Remove dead proxies with W4.
Blast radius: low (admin-only, tests test_admin_*).
Checked fine inside: merchandising.py (pure KPI rules), inventory_queries.py, platform_stats.py, incomplete_listings_api.py (note `_reveal_path_in_os_fs` 92 opens Finder/Explorer from a web handler — dev-only convenience, should be gated to non-production), onboard_api.py, attribution_hub, data_quality_hub, dealers_hub, reviews_hub, scanner_ops_hub, users_hub (253, fine).
- W2 addendum (verified): billing/routes.py::plan_checkout_success (333-359) sets `session["user_is_premium"]=True` for EVERY plan incl. `research`, and `grant_user_premium(uid, plan_id=...)` persists it, so `_finalize_app_session` (main 1370) restores `user_is_premium=True` on every later login. That is the exact path by which a Research subscriber's `has_paid_access` is True while `_require_feature(window_sticker|packages_ensure|ai_*)` refuses. Fix in the access layer (W2), and stop writing `user_is_premium` for non-complete plans (or retire the flag in favour of plan id).

### W19. backend/billing/* (6 files, ~1.1k lines) — P2
- Two parallel purchase flows: legacy single-price "premium" (`premium_checkout` 181, `premium_success` 197, `premium_webhook` 229; stripe_billing.create_premium_checkout_session / stripe_premium_price_id / stripe_premium_webhook_secret) and per-plan (`plan_checkout` 306, `plan_checkout_success` 333; checkout_service.py) plus the org subscription (`billing_checkout` 72, `stripe_webhook` 114). Three checkout paths, two webhooks, one boolean `user_is_premium` shared by two of them.
- `billing_enabled()` (stripe_billing:12) wraps `Config.billing_stripe_enabled()` which main wraps again as `_billing_enabled()`.
- entitlements.py is the best-shaped piece (DB-revalidated, cached) but is bypassed by the UI gate (W2).
Split: `billing/consumer_plans.py` (plan checkout + success + one webhook handling both legacy price and plan prices), `billing/org_subscription.py` (org checkout/webhook), `billing/access.py` (W2). Retire the legacy premium price once no live subscriptions use it (check Stripe first). Blast radius: medium (Stripe webhook URLs are configured externally — keep the paths).
Checked fine: catalog.py, discounts.py, checkout_service.py, stripe_billing.py (thin Stripe wrappers).

### W20. backend/auth/* — P3
- google_oauth.py (275) vs apple_oauth.py (297): `_pick_username` identical, `_complete_login` differs only in session key prefix and the premium-upsell rule (google uses `_should_offer_premium_after_google_login`, apple always upsells new users — likely unintended drift), `_resolve_or_create_user` ~70% shared, `_client_ip`/`_login_rpm`/`_oauth_login_error` copies, both `from backend.main import _finalize_app_session, _post_login_redirect`.
- email_verification.py vs password_reset.py: `_public_base_url` identical, `_verify_pepper`/`_reset_pepper` and `hash_*_token` same pattern.
Split: `auth/oauth_common.py` (pick_username, resolve_or_create_user(provider, sub, email), complete_login(provider)), `auth/tokens.py` (pepper + hash + public_base_url). Blast radius: low.
Checked fine: auth/__init__.py, app_registration.py.

### W21. backend/listings/routes.py::listings_page (144 lines, 153-296) — P3
Not a blueprint: a view function imported by main.py and wrapped by routes/listings_api.py::listings; it imports `clamp_listings_radius` back FROM routes/listings_api (201) — a two-module cycle (listings_api -> listings.routes -> listings_api, both lazy). Does: param parse, geo fallback, hidden-dealer exclusion, server-side smart search, analytics event, history, filter options, geo centre, render.
Fix: move `clamp/snap_listings_radius` + `_normalize_zip` into `backend/listings/geo_session.py` (where geo rules already live) and merge listings_page into the new `routes/listings_api.py` grid module (W6) or rename this file `listings/page.py`. Blast radius: low.
Checked fine (listings): __init__.py, dealer_google_rating.py (12), dealer_locator.py (246; find_nearby_dealers 111 lines but linear), dealer_map.py (206), dealer_registry_match.py (157), geo_session.py (69).

### W22. Admin-check sprawl (8+ variants, two trust models) — P2
DB-revalidated: dealer/admin/routes.py::_session_profile (55-70, used by the 6 hub `_require_site_admin` copies) and routes/admin_dealer_api.py::_current_admin_ok (21-39) both say "a demoted admin's cookie must lose access immediately". Session-trusting: `is_admin_role(session.get("user_role"))` in main.py x4 (paid access 657, paid-org gate 564, billing gate 739, context `is_admin` 500), billing/entitlements.py:120 (admin => all features), billing/routes.py x3, dealers_recalls.py, auth oauth x2, dev/routes.py. So a demoted admin loses /admin immediately but keeps all paid features and the billing-gate bypass for the 14-day cookie. Same idea as W2: one `backend/auth/identity.py::current_user()` (per-request `g` cached DB profile) and `is_site_admin()`; every gate reads it. Blast radius: medium (adds one DB read per request unless cached on `g`; context processor already does one — reuse it).

### W23. Remaining route modules — P3 notes
- routes/home_dashboard.py (363): `_recommendations_for_user_uncached` 123 lines; reco cache dict lives in main.py and is reached via `main_module()._reco_cache` (102-137) — move the cache here. Fine otherwise.
- routes/fuel_api.py (337): gas-price file cache state also lives in main.py (`_live_gas_prices_cache`, `_LIVE_GAS_PRICES_PATH`) and is mutated via `main.` — move here. Otherwise cohesive.
- routes/ai_chat_bp.py (305): `api_ai_chat` 158 lines; third copy of chat preflight (W5) and the gate that bypasses `_require_feature` (W2).
- routes/ai_narrate_bp.py (115), routes/dealer_reviews.py (184; remote_addr fallback W3), routes/dealers_recalls.py (170), routes/admin_dealer_api.py (236): fine apart from items cited.
- routes/site_misc.py `/health` (26) and routes/health.py `/api/health` + `/api/ready`: two health surfaces; keep one (check Railway healthcheck path first).
- routes/_shared.py (37): the `main_module()` indirection that preserves `monkeypatch backend.main.X` in tests. It is the mechanism that keeps main.py a hub; retire it as W1/W6 move state and tests patch owning modules.

### W24. Dead / orphaned modules in the web area (zero production importers, verified by grep 2026-10-01) — P2 (cheap wins)
- backend/utils/mfa_otp.py (50) — 0 refs anywhere (MFA routes are "gone" stubs in main.py 1314-1345 and dev/routes 951-958).
- backend/utils/qr_segno.py (20) — 0 refs (QR MFA removed).
- backend/utils/totp.py (40) — 0 imports of `backend.utils.totp` (the "totp" hits are DB column names in users_db).
- backend/utils/mfa_delivery.py::send_email_code (183-312, 129 lines) — 0 callers; only `send_transactional_email` (56) is used (password_reset, email_verification). mfa_action_log.py (104) is reached only from send_email_code -> dead with it (one test asserts it is NOT written).
- backend/utils/vehicle_naming.py (119) — 0 refs, 0 tests; docstring describes a make/model canonical-spelling corpus that nothing calls (either wire it into facets/price book as intended or delete).
- backend/utils/oem_links.py (41) — test-only.
- main.py `_require_premium_feature` (W2), MFA "gone" stubs (keep only if old links still hit them; check logs).
- dev Node-scanner paths (W4) and their `/api/admin/operator/*` proxies (W18).
Action: delete after the owner confirms (feedback rule: audit deletions — grep templates/static/scripts first; done above for .py/.html). Rename mfa_delivery.py -> backend/auth/email_delivery.py once send_email_code is gone.

### W25. backend/config.py (99) + env reads — P3
Config centralizes 12 env reads; the web area has 179 `os.environ/os.getenv` reads, 167 outside Config (mfa_delivery 17, kmac_vault 16, dev/routes 10, apple_oauth 10, production_security 8, local_llm 7, car_chat_policy 7, google_oauth 7, ...). Config class attributes are evaluated at import time (hence main.py copying them into module globals and tests reloading main). `Config.public_base_url()` exists but email_verification/password_reset each re-wrap it in an identical `_public_base_url`.
Fix: grow Config per area as modules are touched (auth, billing, chat policy), make values functions or a frozen settings object built in `create_app()`. Not worth a dedicated sprint.
- utils/incomplete_recovery.py (471): only importer is backend/scripts/recover_incomplete_listings.py (network fetch of VDPs) — scanner/script code in utils; move with W14 to `backend/scanner/` or `backend/scripts/lib`. P3.

---

## Ranked table

| Rank | ID | Target | Size | Problem in one line | Pri |
|---|---|---|---|---|---|
| 1 | W2 | main paid gates vs billing.entitlements vs ai_chat_bp | 4 gates | UI gate (`user_is_premium`, unrevalidated) != API gate (plan features); Research subscriber sees paid UI the API refuses (verified path: billing/routes.py:347 + main:1370) | P1 |
| 2 | W17 | login in main (x2), dealer/routes, dev/routes | 4 copies | API + dealer login lack the raw-password retry -> spaced-password users fail on iOS/dealer portal | P1 |
| 3 | W4 | backend/dev/routes.py (+console, dealers.py) | 1483 | smart-import/test-scanner run nonexistent `backend/scanner/scanner.js`; console runs nonexistent `backend/scanner.py`; manifest writes go to `backend/dealers.json` not root | P1 |
| 4 | W1 | backend/main.py | 1472, 104 imports | app factory + auth + billing policy + CSRF/CSP + shared state + scanner DDL at import; 4 lazy `from backend.main import`, ~120 `main_module().X` | P1 |
| 5 | W12 | car_serialize/serialize.py::serialize_car_for_api | 577-line fn | data-precedence policy inside a serializer, 9 lazy upward imports; chat agent bypasses it so page and chat diverge | P1 |
| 6 | W5 | routes/cars_pages.py | 1346; 322-line fn | 15-step car context builder + thread job manager + 2 chat endpoints in one module; market gate differs from API | P1 |
| 7 | W22 | admin checks (8+ variants) | — | /admin re-reads role from DB; paid/billing gates trust cookie role for 14 days | P2 |
| 8 | W16 | utils/market_price.py | 608 | unbounded cohort cache keyed on SQLite mtime: likely never refreshes on Postgres + grows per zip/radius | P2 |
| 9 | W24 | dead modules | ~500 lines | mfa_otp, qr_segno, totp, send_email_code+mfa_action_log, vehicle_naming, oem_links, `_require_premium_feature` | P2 |
| 10 | W19 | backend/billing | ~1.1k | 3 checkout flows, 2 webhooks, one shared boolean | P2 |
| 11 | W6 | routes/listings_api.py | 928 | 6 feature areas, /premium page inside listings | P2 |
| 12 | W7 | routes/dealership_page.py | 861 | shared dealer resolution hidden as privates; facets built from listings_repo private helpers | P2 |
| 13 | W8 | listings/nearby_dealers.py | 1199 | request-path picker + offline registry-mutating repair in one module | P2 |
| 14 | W15 | utils/field_clean.py | 1013, fan_in 90 | placeholders + coercion + model repairs + quality score in one hot import | P2 |
| 15 | W18 | dealer/admin/* | 14 files | 6 copied `_require_site_admin`, side-effect registration, proxies to dead dev routes | P2 |
| 16 | W11 | backend/utils (grab-bag) | 75 files | leaf utils mixed with DB/LLM/network/service modules; upward imports | P2 |
| 17 | W14 | utils/analytics_ep.py | 287-line fn | scanner ingest code living in web utils | P2 |
| 18 | W9 | utils/query_parser.py | 1290 | big-but-cohesive NLQ parser with DB vocabulary loader | P3 |
| 19 | W10 | utils/hybrid_search.py | 828 | filters + Flask parsing + hybrid search in utils | P3 |
| 20 | W13 | utils/forced_induction.py::_classify_by_make_model | 328-line fn | rules as if-chain; ignores vPIC `Turbo` | P3 |
| 21 | W20 | auth oauth/token twins | ~840 | google/apple and verify/reset near-copies (upsell rule drift) | P3 |
| 22 | W21 | listings/routes.py::listings_page | 144 | not a blueprint; cycle with listings_api | P3 |
| 23 | W23 | home_dashboard, fuel_api, ai_chat_bp, health twins | — | state lives in main; duplicate health endpoints | P3 |
| 24 | W3 | `_client_ip` wrappers | 5 copies | thin wrappers; dealer_reviews remote_addr fallback | P3 |
| 25 | W25 | config.py / env reads | 179 reads | Config holds 12 of 179 | P3 |

## P1 details (order to execute)
1. **W2 + W22, one access layer.** New `backend/billing/access.py` and `backend/auth/identity.py`. `current_user()` returns the DB profile cached on `g` (the context processor already does this read). `viewer_features()` returns `entitlements_from_session`, plus the dev operator, plus the billing-off/prod-login rules. `can_use(fid)` and `deny_json(fid, err)` sit on top. Templates get `viewer_features`. Delete `_session_has_paid_access`, `_viewer_sees_premium_features`, `_require_premium_feature` and the direct `require_feature` call in ai_chat_bp. Stop setting `user_is_premium` for non-complete plans. Regression tests: a Research session must not render sticker/packages/chat UI, and a demoted admin must lose paid features on the next request. Blast radius: car.html, listings.html, premium.html, nav partials and about 20 tests.
2. **W17, login service.** `backend/auth/login_service.py` takes in the four password logins and three register flows, plus `finalize_app_session` and `post_login_redirect`. That removes every `from backend.main import`. Fixes the spaced-password divergence.
3. **W4, dev cleanup.** Owner decides between deleting the Node-scanner paths and rewiring them onto `scanner.job_queue`. Either way, fix `dev/dealers.py` ROOT, which is a live bug for anything still writing the manifest, and fix console `_SCANNER_SCRIPT`. Remove the `/api/admin/operator/*` proxies to the dead handlers, then split what remains into `dev/job_store.py`, `dev/auth.py` and `dev/api_inventory.py`.
4. **W1, main.py.** Move state first (`_reco_cache` to home_dashboard, gas cache to fuel_api). Then lift out the gates (step 1) and auth (step 2), then `web/security.py` (CSRF decorator, default-deny) and `web/app_factory.py`. Take `init_job_queue_schema()` out of web import. Retire `routes/_shared.main_module()` once tests patch the owning modules.
5. **W12 + W5, car page.** Add golden tests on real rows first. Extract `spec_policy` / `transmission_policy` / `price_policy` from `serialize_car_for_api` and have the AI chat agent call `spec_policy` too. Split `_build_car_detail_view_context` into step functions under `backend/car_page/`. Move packages-ensure threading to `car_page/packages_ensure.py` and add `routes/chat_common.py` for the three chat preflights.

## P2 details
See W6, W7, W8, W11, W14, W15, W16, W18, W19, W22 and W24 above; each has a split and blast radius. Cheapest first: W24 (deletes), W18 guards decorator, W16 cache bound/token, W15 shim split, then W6/W7/W8 module splits, then W11 package moves with shims.

## Checked, fine (77 files; no action beyond notes above)
Reviewed by structure (def map, longest function, imports). Some were sampled rather than read line by line; those are marked (s).
- backend/auth/__init__.py (2)
- backend/auth/app_registration.py (74)
- backend/billing/catalog.py (228)
- backend/billing/checkout_service.py (102)
- backend/billing/discounts.py (92)
- backend/dealer/__init__.py (5)
- backend/dealer/admin/merchandising.py (220)
- backend/dealer/admin/onboard_api.py (56)
- backend/dealer/admin/routes.py (394)
- backend/dealer/portal_sync.py (209)
- backend/dev/__init__.py (1)
- backend/dev/dealer_url_infer.py (478)
- backend/dev/scan_lab.py (566)
- backend/listings/__init__.py (2)
- backend/listings/dealer_google_rating.py (12)
- backend/listings/dealer_locator.py (247)
- backend/listings/dealer_map.py (207)
- backend/listings/dealer_registry_match.py (158)
- backend/listings/geo_session.py (70)
- backend/routes/__init__.py (1)
- backend/routes/community_api.py (515)
- backend/routes/dealers_recalls.py (171)
- backend/utils/__init__.py (2)
- backend/utils/bootstrap_policy.py (16)
- backend/utils/car_chat_policy.py (96)
- backend/utils/car_serialize/__init__.py (179)
- backend/utils/car_serialize/_common.py (146)
- backend/utils/car_serialize/attribution.py (67)
- backend/utils/car_serialize/bmw.py (228)
- backend/utils/car_serialize/color_overlay.py (66)
- backend/utils/car_serialize/condition.py (232)
- backend/utils/car_serialize/location_tco.py (199)
- backend/utils/car_serialize/msrp_overlay.py (140)
- backend/utils/client_ip.py (87)
- backend/utils/comment_images.py (318)
- backend/utils/compare_specs.py (298)
- backend/utils/credential_db_encryption.py (66)
- backend/utils/csrf.py (43)
- backend/utils/dealer_rating_display.py (57)
- backend/utils/dealer_vin_prefill.py (82)
- backend/utils/engine_consistency.py (150)
- backend/utils/first_seen.py (69)
- backend/utils/fuel_label_plausibility.py (393)
- backend/utils/fuel_type_normalize.py (368)
- backend/utils/gallery_merge.py (148)
- backend/utils/in_transit.py (106)
- backend/utils/interior_color_buckets.py (184)
- backend/utils/inventory_repair.py (137)
- backend/utils/ip_rate_limit.py (145)
- backend/utils/kmac_vault.py (214)
- backend/utils/listing_completeness.py (303)
- backend/utils/listing_description_persist.py (216)
- backend/utils/listings_sort.py (107)
- backend/utils/llm_client.py (204)
- backend/utils/local_llm.py (461)
- backend/utils/mileage_display.py (53)
- backend/utils/model_aliases.py (155)
- backend/utils/msrp_trust.py (408)
- backend/utils/oem_option_catalog.py (164)
- backend/utils/outbound_url.py (93)
- backend/utils/price_plausibility.py (172)
- backend/utils/production_security.py (104)
- backend/utils/project_env.py (49)
- backend/utils/rarity_score.py (279)
- backend/utils/registration_validation.py (41)
- backend/utils/roles.py (91)
- backend/utils/runtime_env.py (37)
- backend/utils/safe_listing_url.py (77)
- backend/utils/search_history_format.py (213)
- backend/utils/spec_field_normalize.py (450)
- backend/utils/spec_provenance.py (36)
- backend/utils/transmission_normalize.py (136)
- backend/utils/vdp_gallery_urls.py (4)
- backend/utils/vdp_price_merge.py (4)
- backend/utils/vdp_spec_parse.py (4)
- backend/utils/vehicle_narrator.py (268)
- backend/utils/vpic_specs.py (255)
Notes on items in this list: car_serialize/condition.py::_fill_condition_from_signals (141 lines) and msrp_trust.py::resolve_display_msrp (103) are long but linear single-policy functions (s). community_api.py (515) is cohesive comments/ratings API (s). local_llm.py, llm_client.py, kmac_vault.py, vehicle_narrator.py, car_chat_policy.py are fine in themselves but belong in `backend/llm/` / `backend/security/` per W11. listing_completeness.py, rarity_score.py, compare_specs.py, search_history_format.py import backend.db/enrichment (W11 upward-edge list) (s). vdp_*.py are 4-line alias shims (intentional).
