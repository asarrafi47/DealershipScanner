# Scanner Changes & Additions

## Scan Summary: Chattanooga 25mi (2026-06-15)

| Metric | Value |
|--------|-------|
| Dealers in 25mi radius (filtered from 50mi manifest) | 18 |
| Skipped (DNS broken / offline / bot-blocked) | 4 |
| Scanned | 14 |
| Recovered after psycopg3 + column-ambiguity bug fixes (rescan 3) | 4 |
| Zero-row dealers (platform not yet supported) | 7 |
| — autoWALL (Long Automotive Group CMS) | 3 |
| — ShopperExpress (WordPress + Serti DMS) | 2 |
| — Site offline (Audi Chattanooga) | 1 |
| — Bot-protected / Vercel checkpoint (Porsche) | 1 |
| **Net vehicles upserted** | **951 across 4 dealers** |

### Per-dealer final results (rescan 3 — all column-ambiguity bugs fixed)

| Dealer | provider | inventory_rows | deduped | upserted | vdp_visited | scan_secs |
|--------|----------|---------------|---------|---------|-------------|-----------|
| BMW of Chattanooga | dealer_eprocess | 460 | 355 | **355** | 120 | 2921 |
| Volvo Cars Chattanooga | unknown (Roadster) | 378 | 289 | **289** | 123 | 2239 |
| Integrity Buick GMC | dealer_on | 256 | 220 | **220** | 121 | 1098 |
| HIXSON CHEVROLET | dealer_inspire | ~87 | 87 | **87** | ~87 | ~3600 |

---

## 1. New Phase: SiteProfiler

**File:** `backend/scanner/phases/site_profile.py`

A lightweight pre-scan site profiler now runs **after warmup and before parallel inventory scraping**. It opens its own page within the shared browser context (the warmup page is left untouched), visits the dealer base URL and optionally one inventory path, then returns a `SiteProfile` used to choose scraping strategy.

### Budget constraints
- Max 2 page navigations per dealer
- ~15–25 s wall time
- No VDP enrichment, no Claude API calls, no deep pagination, no long scrolling
- Hard timeout via `SCANNER_PROFILER_TIMEOUT_S` env var (default: `22`)
- Disabled entirely with `SCANNER_SITE_PROFILER=0`; current behaviour is fully preserved on failure or when disabled

### `SiteProfile` dataclass

```python
@dataclass
class SiteProfile:
    dealer_url: str
    detected_provider: str          # see provider table below
    inventory_paths_found: list[str]
    likely_inventory_urls: list[str]
    script_signatures: list[str]
    api_endpoint_candidates: list[str]
    pagination_type: str            # api | next_button | load_more | infinite_scroll | ssr | unknown
    vehicle_dom_selectors_found: list[str]
    vin_signals_found: bool
    total_count_signal: int | None
    location_filter_signal: bool
    confidence_score: float         # 0.0–1.0
    notes: list[str]
    profiled_at: float
```

### `profile_dealer_site(context, base_url, known_paths) -> SiteProfile`

Async entry point. Always returns a `SiteProfile` — callers must tolerate partial/empty results. Phases inside:

1. Navigate base URL (`domcontentloaded`, 12 s timeout)
2. Parse HTML: extract script sources, inventory nav links, total count, location filter hint
3. DOM probes: vehicle card selectors, pagination controls, location filter
4. Optional inventory path visit when base page is ambiguous (low confidence or no DOM hits)
5. Resolve API intercepts (JSON responses captured during navigation)
6. Merge provider signals from HTML, script URLs, and API URLs; compute confidence score

### `choose_inventory_paths(profile, dealer) -> list[str]`

Selects the optimal inventory path list given the profile. Falls back to `inventory_paths_for_dealer(dealer)` when:
- profile is `None` (profiler skipped or errored)
- confidence < 0.5
- no useful path signals found

High-confidence overrides (conf ≥ 0.7):

| Provider | Paths returned |
|----------|---------------|
| `pixel_motion` | `_PIXEL_PATHS` from pixel_motion scraper |
| `dealer_inspire` | `/new-vehicles/`, `/used-vehicles/`, `/certified-pre-owned/` |
| `autowall` | `/gs-vehicle/shopFromHome`, `/inventory/`, `/new-inventory/index.htm` |
| `shopperexpress` | `/inventory/`, `/new-vehicles/`, `/used-vehicles/`, `/certified-pre-owned/` |

### Provider detection tables

**HTML signatures** (`_HTML_SIGS` — matched against raw page source):

| Provider | Key substrings |
|----------|---------------|
| pixel_motion | `vlpm3vehiclerow`, `pm-motors-plugin`, `pixelmotiondemo` |
| dealer_on | `vhcliaa`, `prsnbaa.dealeron` |
| dealer_inspire | `dealerinspire_inventory_vars`, `mvnalgoliaconfig`, `diinventoryconfig` |
| dealer_eprocess | `dealereprocess`, `vehicle-facts.json` |
| fox_dealer | `foxdealer.com`, `foxdealer-` |
| dealer_fire | `dealerfire.com`, `df-inventory` |
| sincro | `sincrodigital.com`, `sincro.net` |
| dealer_dot_com | `dealer.com/widget`, `cdk.com`, `ws-inv-data`, `getinventoryandfacets` |
| **autowall** _(new)_ | `autowall-vehicle-list`, `powered_by_autowall`, `/gs-vehicle/` |
| **shopperexpress** _(new)_ | `/themes/shopperexpress/`, `themes/shopperexpress` |

**Script URL signatures** (`_SCRIPT_SIGS` — matched against `<script src="…">`):

| Provider | Key substrings |
|----------|---------------|
| _(all above providers)_ | _(see source)_ |
| **autowall** _(new)_ | `autowall-vehicle-list`, `autowall-vehicle-external`, `dkbcpcob6xxyt.cloudfront`, `d3dn269ayoh5p6.cloudfront` |
| **roadster** _(new)_ | `cdn.roadster.com`, `cdn1.roadster.com`, `roadster_frame_embed`, `roadster_dealer_analytics` |

**API intercept signatures** (`_API_SIGS` — matched against JSON response URLs):

| Provider | Key substrings |
|----------|---------------|
| dealer_on | `vhcliaa`, `prsnbaa.dealeron` |
| dealer_inspire | `algolianet.com` |
| dealer_eprocess | `vehicle-facts.json`, `canonicallexicon.json` |
| dealer_dot_com | `getinventory`, `getinventoryandfacets`, `ws-inv-data` |
| **autowall** _(new)_ | `cai-media-management.com/resize` |

**Vehicle DOM selectors** — added for new platforms:

```python
".se-vehicle-card",       # ShopperExpress
".vehicle-listing",       # ShopperExpress
"[class*='gs-vehicle']",  # autoWALL
".autowall-vehicle",      # autoWALL
"[class*='autowall']",    # autoWALL
```

**Inventory href fragments** — added `/gs-vehicle` for autoWALL discovery from nav links.

---

## 2. `run_dealer()` Integration

**File:** `backend/scanner/phases/dealer_run.py`

### New pipeline order

```
warmup()
  → profile_dealer_site()          # NEW — ~15–25 s, skippable
  → choose_inventory_paths()       # NEW — replaces inventory_paths_for_dealer()
  → scan_paths_parallel()
  → recover_inventory()
  → VDP enrichment
  → upsert
```

### Specific changes

- `profile_dealer_site(context, url, [])` called after warmup completes, before `asyncio.gather`
- Result stored as `_site_profile: SiteProfile | None`
- `choose_inventory_paths(_site_profile, dealer)` replaces the direct call to `inventory_paths_for_dealer(dealer)`
- `site_profile=_site_profile` keyword arg passed to every `scrape_inventory_path()` call
- Profile summary written to `result["site_profile"]`:
  ```json
  {
    "provider": "dealer_inspire",
    "pagination": "api",
    "confidence": 0.93,
    "paths_found": ["/new-vehicles/", "/used-vehicles/"],
    "api_eps": 2,
    "notes": []
  }
  ```
- Phase timing written to `result["phase_secs"]["site_profile"]`
- `SCANNER_SITE_PROFILER=0` disables the profiler block entirely; existing behaviour is unchanged

---

## 3. `scrape_inventory_path()` Profile Hint Enforcement

**File:** `backend/scanner/phases/inventory_scrape.py`

Two profile-derived flags were already computed from `site_profile` but were not used. They are now enforced:

### `_skip_lazy_scroll` (when `pagination_type == "api"`)

When the profiler identifies API-based pagination (e.g. Algolia, Dealer.com SPA), both the pre-pagination and post-pagination `infinite_scroll_lazy_batches` calls are skipped:

```python
# Before
if not await any_next_control_visible(page):
    await infinite_scroll_lazy_batches(...)

# After
if not _skip_lazy_scroll and not await any_next_control_visible(page):
    await infinite_scroll_lazy_batches(...)
```

**Savings:** 5–15 s per inventory path on API-backed SPA sites.

### `_is_pixel_motion_early` (when `detected_provider == "pixel_motion"`)

When the profiler already confirmed PixelMotion, the HTML content check is short-circuited:

```python
# Before
if _is_pixel_motion_html(peek_html):

# After
if _is_pixel_motion_early or _is_pixel_motion_html(peek_html):
```

This skips a redundant `page.content()` parse and goes directly to SSR pagination.

---

## 4. Bug Fix: psycopg3 `LIKE` Placeholder Escaping

**File:** `backend/db/inventory_pg.py` — `qmarks_to_percent_s()`

### Problem

`qmarks_to_percent_s()` converts SQLite `?` placeholders to psycopg3 `%s`, but did not escape bare `%` characters inside single-quoted SQL string literals. The upsert SQL contains:

```sql
WHERE LOWER(TRIM(dealer_url)) LIKE 'http%'
```

psycopg3 interpreted `%'` as an invalid format-string placeholder, raising:

```
ProgrammingError: only '%s', '%b', '%t' are allowed as placeholders, got '%''
```

### Impact

**1,185 vehicles lost** across 4 Chattanooga dealers — rows were scraped and parsed correctly, but every upsert call failed at the Postgres layer. The bug affects any dealer scan using the Postgres backend whenever the upsert SQL reaches the `LIKE 'http%'` branch.

### Fix

Added `%` → `%%` escaping inside string-literal segments of the existing quote-aware character scanner loop:

```python
# Inside single-quoted string literal
if c == "%":
    out.append("%%")  # escape so psycopg3 doesn't treat as placeholder
    i += 1
    continue
```

### Affected dealers (all recovered after rescan)

| Dealer | Rows lost | Recovered |
|--------|-----------|-----------|
| BMW of Chattanooga | 464 | yes |
| Volvo Cars Chattanooga | 378 | yes |
| Integrity Buick GMC | 256 | yes |
| HIXSON CHEVROLET | 87 | yes |

---

## 4b. Bug Fix: PostgreSQL ON CONFLICT Column Ambiguity

**File:** `backend/scanner/database.py` — `ON CONFLICT(vin) DO UPDATE SET` block

### Problem

PostgreSQL's `ON CONFLICT DO UPDATE SET` clause prohibits bare column name references on the right-hand side of `COALESCE` expressions when a column of the same name exists in both the target table and the `EXCLUDED` pseudo-table. This caused `AmbiguousColumn` errors:

```
column reference "fuel_type" is ambiguous
LINE 34:    fuel_type=COALESCE(EXCLUDED.fuel_type, fuel_type)
```

`trim` was patched first (found because `trim` shadows the SQL built-in function). A second rescan revealed `fuel_type`, which Postgres also finds ambiguous in this context despite not being a standard built-in.

**Root cause:** ALL bare column references on the fallback side of COALESCE in an `ON CONFLICT DO UPDATE SET` block must be qualified with the target table alias (`cars.`).

### Fix

Qualified every bare column reference with `cars.` in the DO UPDATE SET block:

```sql
-- Before (any of these patterns would cause ambiguity):
fuel_type=COALESCE(excluded.fuel_type, fuel_type)

-- After (all occurrences fixed):
fuel_type=COALESCE(excluded.fuel_type, cars.fuel_type)
```

Complete list of columns fixed (in addition to `trim` fixed earlier):
`fuel_type`, `cylinders`, `transmission`, `transmission_type`, `drivetrain`, `exterior_color`, `interior_color`, `stock_number`, `dealership_registry_id`, `source_url`, `body_style`, `engine_description`, `engine_l`, `condition`, `description`, `mpg_city`, `mpg_highway`, `is_cpo`

### Impact

This blocked ALL upserts for any dealer on Postgres. Fixed in rescan 3; confirmed by Integrity Buick GMC (220 upserted) and Volvo Cars Chattanooga (289 upserted).

---

## 5. Manifest Skip Flag

**Files:** `backend/scanner/manifest.py`, `backend/scanner/cli.py`

### `filter_manifest_skip_flag(dealers) -> list[dict]`

New filter that drops manifest entries with `"skip": true`. This lets you mark unreachable dealers in-place with a documented reason rather than deleting them:

```json
{
  "name": "Porsche of Chattanooga",
  "url": "https://porscheofchattanooga.com",
  "dealer_id": "porscheofchattanooga-com",
  "skip": true,
  "skip_reason": "bot_protected_vercel"
}
```

The filter is applied in the CLI pipeline immediately after `filter_skip_dealers()`:

```python
to_run = filter_manifest_skip_flag(filter_skip_dealers(load_manifest()))
```

### Chattanooga dealers marked skip

| dealer_id | skip_reason |
|-----------|-------------|
| hixsoncdjr-com | `dns_not_resolved` |
| chattanooga-ford-com | `dns_not_resolved` |
| audichattanooga-com | `site_offline` |
| porscheofchattanooga-com | `bot_protected_vercel` |

---

## 6. New Providers Discovered (Chattanooga Scan)

### autoWALL — Long Automotive Group CMS

Used by at least 3 Chattanooga dealers (all Long Automotive Group stores):

| Dealer | dealer_id |
|--------|-----------|
| Chattanooga Volvo | volvocarschattanooga-com |
| Mercedes-Benz at Long | mercedesbenzatlong-com |
| Genesis at Long | genesisatlongofchattanooga-com |

**Platform characteristics:**
- React SPA frontend
- Inventory loaded via AJAX from the dealer's own domain (`autowall-vehicle-list.js`)
- CloudFront-hosted scripts: `dkbcpcob6xxyt.cloudfront.net`, `d3dn269ayoh5p6.cloudfront.net`
- Vehicle media served from `assets.cai-media-management.com/resize/…`
- Common GTM tag across Long Group stores: `GTM-KMQZ7S3K`
- Inventory URL pattern: `/gs-vehicle/shopFromHome`

**Current support:** Detection only. JSON intercept does not fire from outer page — the AJAX endpoint is internal and not yet mapped. Scraper not yet written.

### ShopperExpress — WordPress + Serti DMS

Used by 2 Chattanooga dealers:

| Dealer | dealer_id |
|--------|-----------|
| Acura of Chattanooga | acuraofchattanooga-com |
| Kia of Chattanooga | kiaofchattanooga-com |

**Platform characteristics:**
- WordPress with `/wp-content/themes/shopperexpress/` theme
- Serti DMS backend (inventory management)
- Standard WP plugin stack: jQuery, Wistia video, Google Maps
- Likely SSR inventory pages at `/inventory/`, `/new-vehicles/`, `/used-vehicles/`

**Current support:** Detection only. HTML fallback scraper may work if vehicle cards include VIN data attributes — untested.

### Roadster — iframe embed (sub-component)

Detected on Volvo Cars Chattanooga (`chattanoogavolvotn-com`, a different dealer from Chattanooga Volvo).

**Platform characteristics:**
- Roadster storefront embedded as an iframe via `cdn1.roadster.com/roadster_frame_embed?dpid=<id>`
- Outer page JSON intercepts do not capture inventory — iframe has its own origin
- Direct Roadster API may be accessible at `https://buy.roadster.com/<dpid>/vehicles`

**Current support:** Not supported. Requires iframe-aware page navigation or direct Roadster API calls using the `dpid` extracted from the embed script URL.

---

## 7. Manifest Corrections (`workspace/manifest_chattanooga_25mi.json`)

Provider fields corrected based on site profiler detections and manual investigation:

| dealer_id | old provider | corrected provider | source |
|-----------|-------------|-------------------|--------|
| bmwofchattanooga-com | `dealer_dot_com` | `dealer_eprocess` | profiler (conf=0.45) |
| hixsonchevrolet-com | `unknown` | `dealer_inspire` | profiler (conf=0.93) |
| integrityofchattanooga-com | `unknown` | `dealer_on` | profiler (conf=0.65) |
| infinitichattanooga-com | `dealer_inspire` | `dealer_inspire` | confirmed ✓ |
| landroverchattanooga-com | `unknown` | `dealer_inspire` | profiler (conf=0.78) |
| volvocarschattanooga-com | `unknown` | `autowall` | manual investigation |
| mercedesbenzatlong-com | `unknown` | `autowall` | manual investigation |
| genesisatlongofchattanooga-com | `unknown` | `autowall` | manual investigation |
| acuraofchattanooga-com | `unknown` | `shopperexpress` | manual investigation |
| kiaofchattanooga-com | `unknown` | `shopperexpress` | manual investigation |

URLs also cleaned: removed Facebook UTM query strings from Cannon Chevrolet, Cannon CDJR, BMW of Chattanooga, Nissan East, Volvo Cars Chattanooga, and Chattanooga Volvo URLs that caused redirect noise during warmup.

---

## Known Limitations / Future Work

| Item | Priority | Notes |
|------|----------|-------|
| **`SCANNER_VDP_EP_MAX=0` doesn't suppress price VDP** | Medium | Setting `SCANNER_VDP_EP_MAX=0` disables EP enrichment but `SCANNER_VDP_PRICE_MAX` defaults to 400 (or lot size), still triggering hundreds of VDP visits. Fix applied in `dealer_run.py`: when `SCANNER_VDP_EP_MAX=0` is explicitly set and `SCANNER_VDP_PRICE_MAX` is unset, price VDP now also defaults to 0. To disable ALL VDP in one flag, use `SCANNER_VDP_EP_MAX=0` going forward. |
| **autoWALL scraper** not written | High | Inventory loaded via internal AJAX; endpoint URL pattern not yet reverse-engineered. Start by intercepting XHR on `/gs-vehicle/shopFromHome` to find the JSON source. |
| **ShopperExpress HTML fallback** untested | Medium | Existing `recovery_inventory` HTML fallback may extract VINs if vehicle cards use `data-vin` attributes. Run one dealer in debug mode to verify. |
| **Roadster iframe scraping** | Medium | Navigate the iframe src directly (Playwright supports `frame.goto()`), or call `https://buy.roadster.com/<dpid>/vehicles` API endpoint using `dpid` extracted from the embed script URL. |
| **Porsche of Chattanooga** bot-blocked | Low | Vercel Security Checkpoint blocks headless Chromium. Requires `playwright-extra` with stealth plugin, or residential proxy rotation. |
| **Audi Chattanooga** offline | Low | Mark `skip: true` in manifest; re-check site status before next scan cycle. |
| ShopperExpress inventory paths may differ | Low | Try `/inventory/` first; if 0 rows, also try `/wp-json/wp/v2/` to check if inventory is exposed as a custom post type. |
| Roadster `dpid` extraction in profiler | Low | Add a regex to `_run_profiler` that extracts the `dpid` param from `roadster_frame_embed` script URL and stores it in `SiteProfile.notes`. |
| autoWALL CAI media gallery extraction | Low | `cai-media-management.com/resize/<W>x<H>/common-vehicle-media/<UUID>.jpg` URLs follow a predictable pattern — gallery could be reconstructed from vehicle UUIDs. |

---

## Session 2026-06-20: CF bypass + platform gap fixes

### Problem context

After the Chattanooga 25mi scan, 7 dealers in the test manifest had zero or broken inventory:

| Dealer | Platform | Issue |
|--------|----------|-------|
| Capital Toyota | dealer_eprocess | Used + CPO SRP fully CF-Turnstile blocked |
| BMW of Chattanooga | dealer_eprocess | Needed verification |
| Kia of Cleveland | dealer_eprocess | Needed verification |
| Ford of Dalton | dealer_on | SRP path mismatch (`/searchnew.aspx`) |
| Long Hyundai | dealer_dot_com | Provider field was "unknown" in manifest |
| Land Rover Chattanooga | dealer_inspire | Algolia account rate-limited |
| Infiniti of Chattanooga | dealer_inspire | Same Algolia account rate-limited |

---

### 8. DealerEProcess Strategy 0 — Universal `/resrc/inventory/results/` API

**File:** `backend/scanner/scrapers/dealer_eprocess.py`

**Root cause discovered:** Cloudflare Turnstile on Capital Toyota covers all `/used-inventory/*` and `/certified-inventory/*` paths. The browser gets a challenge page instead of real HTML, so all existing browser-based strategies (vehicle-facts.json, /resrc/inventory/, JSON-LD) return empty. However, the `/resrc/*` API paths are NOT Turnstile-gated.

**Discovery process:** Reverse-engineered `cdn.dealereprocess.org/cdn/js/search/filter_search.min.js` (10 KB minified) to find `Authorization: "Basic " + btoa("DEP:20tD3QEscbHFY")` — a hardcoded credential used by all DealerEProcess dealers universally.

**Endpoint:**
```
GET {base_url}/resrc/inventory/results/?flag_new=0   → used + CPO
GET {base_url}/resrc/inventory/results/?flag_new=1   → new
Authorization: Basic REVQOjIwdEQzUUVzY2JIRlk=
```

**Response shape:**
```json
{
  "total": 134,
  "details": {
    "122068898": {
      "title": "2004 BMW 5 Series 525i",
      "url": "/auto/used-...-tn/122068898/",
      "detail": {
        "condition": "used", "certified": "no",
        "year": 2004, "make": "BMW", "model": "5 Series", "trim": "525i",
        "body": "Sedan", "exterior_color_manufacturer": "Grey",
        "exterior_color": "Gray", "interior_color_manufacturer": null,
        "transmission": "6-Speed Manual Sequential", "drivetrain": "RWD",
        "price": 5580, "stock": "848734UT",
        "vin": "WBANA53504B848734", "odometer": 212058
      }
    }
  }
}
```

**Implementation:** Added to `scrape_dealer_eprocess_from_page` as **Strategy 0** — tried before any browser-based strategy. If it returns vehicles, all Playwright strategies are skipped entirely.

```
Strategy 0  _fetch_results_api()        pure HTTP, asyncio.to_thread
Strategy 1–3  _fetch_facts_with_fallback()  browser, vehicle-facts.json
Strategy 4  _fetch_resrc_inventory()    browser, /resrc/inventory/
Strategy 5  _fetch_jsonld_from_srp()    browser, JSON-LD in SRP HTML
```

**New functions added:**

| Function | Purpose |
|----------|---------|
| `_map_vehicle_results(vehicle_id, obj, ...)` | Maps `detail` sub-object to canonical vehicle dict. Prefers `exterior_color_manufacturer` over generic `exterior_color`. |
| `_fetch_results_api(base_url, ...)` | `async` — fires two `urllib.request` GETs (flag_new=0, flag_new=1) via `asyncio.to_thread`. Returns combined deduped vehicle list. |

**New constants added:**
```python
_RESULTS_AUTH = "Basic " + _base64.b64encode(b"DEP:20tD3QEscbHFY").decode()
```

**Confirmed results (Capital Toyota, live test):**
- 194 new vehicles
- 134 used vehicles
- 328 total, 0 Playwright pages opened

**Coverage:** Universal — same credential works for all DealerEProcess dealers (BMW of Chattanooga, Kia of Cleveland, Capital Toyota confirmed).

---

### 9. DealerOn ASP.NET SRP Path Support

**File:** `backend/scanner/scrapers/dealer_on.py`

**Problem:** Ford of Dalton is a DealerOn dealer using an older ASP.NET site. Its SRP paths are `/searchnew.aspx` and `/searchused.aspx` — the DealerOn scraper only tried `/new-inventory/index.htm`, `/used-inventory/index.htm`, `/certified-inventory/index.htm`, which all 404.

**Detection signal:** The `window.performanceHub` config embedded in the page explicitly prefetches `["searchnew.aspx", "searchused.aspx"]`.

**Fix:** Added the `.aspx` variants to `_SRP_PATHS`:

```python
_SRP_PATHS = [
    "/new-inventory/index.htm",
    "/used-inventory/index.htm",
    "/certified-inventory/index.htm",
    "/searchnew.aspx",        # ASP.NET DealerOn sites (e.g. Ford of Dalton)
    "/searchused.aspx",
    "/searchcertified.aspx",
]
```

The intercept mechanism (`"cosmos/srp/vehicles" in rurl`) is path-agnostic — it captures the XHR fired by the DealerOn JS regardless of which SRP page was navigated to. No other changes required.

**Ford of Dalton details:** `dealerId: 16355`, `itemCount: 179` (confirmed from inline page script).

---

### 10. Manifest Provider Corrections

**File:** `workspace/manifest_issue_fixes_test.json`

| Dealer | Before | After | Reason |
|--------|--------|-------|--------|
| Ford of Dalton | `unknown` | `dealer_on` | DealerOn confirmed via `vhcliaa` + `prsnbaa.dealeron.com` fingerprints |
| Long Hyundai | `unknown` | `dealer_dot_com` | DDC bulk API confirmed (487 vehicles accessible) |

Correct provider fields ensure hint-based recovery chain selection fires immediately without waiting for HTML scan to detect the platform.

---

### Outstanding: Land Rover + Infiniti Chattanooga

Both are DealerInspire/Algolia. Algolia App ID `10APRXOTJR` (shared by both stores) was temporarily rate-limited due to excessive queries during investigation. No code changes needed — the existing `scrape_dealer_inspire_from_page` will work once the ban lifts (typically 24–48 hours).

Likely index names (to verify once unblocked):
- Land Rover: `landroverchattanooga-sbm0326_production_inventory` (confirmed)
- Infiniti: `infinitichattanooga-sbm0326_production_inventory` (inferred from suffix pattern)

---

### Updated Known Limitations / Future Work

| Item | Priority | Notes |
|------|----------|-------|
| **Land Rover + Infiniti Chattanooga** Algolia rate limit | High | Will self-resolve; no code change needed. Retry after 2026-06-22. |
| **DealerEProcess results API image_url** always empty | Medium | `/resrc/inventory/results/` does not return images. VDP enrichment fills gallery. For dealers where VDP is skipped, images will be missing. A follow-up fetch to `/resrc/vehicleresults/loadAjaxResults/{hash}/` can get rendered HTML with image URLs, but requires a server-issued session hash. |
| **DealerOn `.aspx` certification path** unconfirmed | Low | `/searchcertified.aspx` path added speculatively — may 404 on Ford of Dalton; harmless since the scraper continues on 404. |

---

### 11. Recovery Chain Provider Pinning

**File:** `backend/scanner/inventory_recovery.py` — `recovery_strategy_names()`

**Problem:** Ford of Dalton (DealerOn) had false `algolia` + `dealer_inspire` hints added by analytics/tracking intercepts during the main scan phase. `dealer_inspire_algolia` ran first, returned ≤16 vehicles, the early-exit condition (`intercept_feed_is_sufficient`) fired, and `dealer_on_cosmos` was never reached. Result: 16 vehicles instead of 158.

**Fix 1 — Force recovery for `dealer_on`:**  
Added to `should_run_platform_recovery`:
```python
if ctx.provider == "dealer_on":
    return True  # cosmos API never captured by standard intercept path
```

**Fix 2 — Pin canonical strategy to front:**  
Added `_PROVIDER_FIRST_STRATEGY` map and `provider` parameter to `recovery_strategy_names`. When `provider` is set and no cached winner exists, the provider's canonical strategy is treated as the cached winner (moved to front of chain):

```python
_PROVIDER_FIRST_STRATEGY = {
    "dealer_on": "dealer_on_cosmos",
    "shopperexpress": "shopperexpress_api",
    "dealer_inspire": "dealer_inspire_algolia",
    "dealer_eprocess": "dealer_eprocess_json",
    "dealer_venom": "dealer_venom_typesense",
    "pixel_motion": "pixel_motion_html",
}
```

**Result:** Recovery chain for Ford changes from `[dealer_inspire_algolia, dealer_on_cosmos, html_next_data]` to `[dealer_on_cosmos, dealer_inspire_algolia, html_next_data]`. Cosmos fires first, returns 158 vehicles, loop exits with `winner=dealer_on_cosmos`.

**Confirmed scan result — Ford of Dalton (2026-06-20):**

| Metric | Value |
|--------|-------|
| inventory_rows | 158 |
| deduped_rows | 117 |
| upserted | 117 |
| vdps_visited | 117 |
| gallery_bins (5p) | 117 |
| seconds | 802 |
| winning_strategy | `dealer_on_cosmos` |

---

## 12. VDP Performance Restructure — decouple gallery harvest from spec extraction

### Where the time goes

Per-dealer scan is a **sequential pipeline**; only the inventory-path scrapers and the
cross-dealer fan-out run in parallel:

```
warmup → site_profile → inventory scrape (parallel paths) → recovery → VDP enrichment → upsert
```

Phase timing from the Ford of Dalton scan above (802 s total):

| Phase | Seconds | Share |
|-------|---------|-------|
| site_profile | 5.0 | 0.6% |
| inventory | 98.4 | 12% |
| **vdp** | **668.1** | **83%** |
| upsert | 3.4 | 0.4% |

**VDP enrichment is the whole game.** It visits one detail page per vehicle (117 here) at
`SCANNER_MAX_VDP_CONCURRENCY` workers (default **4**). At 117 visits / 4 workers ≈ 30 serial
slots → ~22 s wall per visit. Each visit does: nav → settle → `PAGE_EXTRACT_JS` → **carousel
gallery interaction loop** → EP/spec/price merge.

### Root inefficiency

`_vdp_gallery_interaction_loop` (`vdp/core.py`) ran **unconditionally on every vehicle** — up
to `SCANNER_VDP_GALLERY_MAX_SEC` (default 150 s) wall-clock per page, advancing the carousel
round by round. But on every modern platform the **listing feed already returns a full
gallery**:

- DealerOn cosmos → `_build_gallery` (Ford: all 117 vehicles had 5+ feed images)
- Dealer.com bulk, eProcess `/resrc/inventory/results/`, DealerInspire Algolia → galleries inline

And `merge_vdp_gallery_into_vehicle` only **replaces** a gallery when the existing one has
fewer than `SCANNER_GALLERY_MERGE_REPLACE_IF_BELOW` (default **3**) images; otherwise it just
**extends**. So for a vehicle that already has ≥3 feed images, the entire carousel harvest is
spent looking for a few *marginal* extra photos — the dominant cost for near-zero gain.

### Change — additive, env-gated, default-preserving

Two new knobs. **Both default to current behaviour** (no change unless explicitly enabled).

**A. Gallery-loop skip gate** — `vdp/core.py`

New env `SCANNER_VDP_GALLERY_SKIP_IF_FEED_GE` (default `0` = disabled). When set to N > 0 and
the vehicle's feed gallery already has ≥ N HTTPS images, the per-VDP carousel interaction loop
is skipped. The visit still does nav + `PAGE_EXTRACT_JS` + EP/spec/price extraction (the cheap
part); only the expensive carousel walk is dropped. The feed gallery is preserved unchanged
(merge "extend" with nothing new = keep existing).

```python
# _vdp_visit_one, before the carousel loop:
_feed_gallery_count = count_https_gallery_urls([*v.get("gallery", []), v.get("image_url")])
_skip_gallery_loop = _gallery_skip_ge > 0 and _feed_gallery_count >= _gallery_skip_ge
...
if _skip_gallery_loop:
    extra_loop_gallery = []          # skip carousel harvest, keep feed gallery
else:
    extra_loop_gallery = await _vdp_gallery_interaction_loop(...)   # unchanged path
```

This **decouples gallery harvest from spec/EP extraction** at the per-vehicle level — the core
structural change. No existing function is removed; the loop, the merge, the HTML recovery
fallback all still run exactly as before when the gate is off or the feed gallery is thin.

**B. Adaptive VDP concurrency** — `scan_efficiency.py`

`SCANNER_MAX_VDP_CONCURRENCY` now accepts `auto` → `min(8, max(4, cpu_count - 2))`. Default
(unset) is still **4**. Once the carousel loop is skipped, visits become nav+extract-bound
(no per-page gallery memory pressure), so more workers convert directly to throughput.

### Recommended fast-scan profile

For platforms with feed-side galleries (DealerOn, Dealer.com, eProcess results API, Algolia):

```bash
SCANNER_VDP_GALLERY_SKIP_IF_FEED_GE=8   # skip carousel when feed already has 8+ images
SCANNER_MAX_VDP_CONCURRENCY=auto        # scale workers to host cores
```

Expected: VDP visits collapse from ~22 s (nav + full carousel) to ~2–4 s (nav + extract),
attacking the 83% phase directly. Spec/EP/price coverage is unchanged (those extractors are
untouched); only marginal extra carousel photos beyond the feed are foregone.

### Validation

Ford of Dalton, same dealer, back-to-back. Config:
`SCANNER_VDP_GALLERY_SKIP_IF_FEED_GE=8 SCANNER_MAX_VDP_CONCURRENCY=auto` (→ 8 workers).

| Metric | Baseline | Optimized | Δ |
|--------|----------|-----------|---|
| VDP phase (s) | 668.1 | **95.8** | **7.0× faster** |
| Total scan (s) | 802 | **226** | **3.5× faster** |
| VDP workers | 4 | 8 (`auto`) | — |
| Vehicles upserted | 117 | 119 | +2 (inventory drift) |
| Carousel harvests skipped | 0 | **106 / 119** | — |
| gallery_bins 5+ images | 117 | **119** | **quality preserved** |

**Every vehicle still landed in the 5+ image bin** — feed galleries are full, so skipping the
marginal carousel walk cost zero gallery coverage. Spec/EP/price extraction ran on all 119 as
before. The 7× VDP speedup came entirely from not walking carousels that had nothing material
to add, plus doubled worker parallelism on the now-lightweight visits.

### Not yet implemented — recommended next structural steps

| Step | Benefit | Risk | Notes |
|------|---------|------|-------|
| **Pipelined / incremental upsert** | Durability + overlap | Med | Today upsert runs once after *all* VDP visits — inventory sits in memory for the full VDP phase (668 s) before any DB write. Writing in batches as VDP workers drain would make data durable earlier and overlap DB I/O with VDP work. Touches `InventoryWriteCoordinator` + `inventory_reconcile` (which diffs scraped-set vs DB), so sequencing must be preserved. |
| **Gallery-only "thin" fast path** | Skip nav for full rows | Med | When a vehicle needs neither EP/spec gap nor price nor description (already complete from feed), skip the VDP visit entirely rather than visiting to harvest a marginal gallery. Requires a per-vehicle "fully complete" predicate composed from the existing `_vehicle_needs_*` gates. |
| **Shared gallery cap short-circuit** | Bounded loop | Low | When `SCANNER_VDP_GALLERY_SKIP_IF_FEED_GE` is off but the feed gallery is moderate, cap the carousel loop at fewer idle rounds instead of the full wall-clock budget. |
