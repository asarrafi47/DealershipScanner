# Frontend + iOS monolith audit (read-only) — 2026-10-01
Area: 142 files from scratchpad/area_frontend.txt. Findings appended as reviewed.

## F1. frontend/static/main.js — 3360 lines, ONE closure (P1)
Whole file is a single `document.addEventListener("DOMContentLoaded", () => {...})` (L1–L3360) with 28 closure-level `let/var` and ~120 inner functions. Despite the name it is *only* the listings/dealership grid (guard at L38: returns unless `#ds-listings-car-rows` + `CAR_ROWS`).
Responsibilities by range:
- L50–92 compact sticky header on scroll
- L93–152 condition/mileage/paint predicates (pure)
- L153–478 facet cascade (checked/compatibleRows/runCascade/cascadeParam/updateCylinders/updateCount)
- L479–661 lazy facet hydration (fetch + label HTML)
- L662–1180 ZIP/search-started state machine, start prompt, zip callout/banner, debounce schedulers (L693–1180)
- L1181–1472 per-page/sort/pagination + saved-searches widget IIFE (L1208)
- L1473–1594 renderCardHtml (string-concat card markup, ~120 lines)
- L1595–1856 page render, sort comparators, renderCarGrid
- L1857–2102 geo-hint, dealer-registry filter, syncUrl, facet filter state
- L2103–2433 render scheduling + smart-filter matcher (carMatchesSmartFilters ~110 lines) + `window.__DS_*` exports
- L2434–2729 radius filter, /api/listings/cars fetch w/ ETag + partial refetch, boot
- L2730–2976 undo stack, active filter chips, filters sheet
- L2977–3079 save buttons; L3080–3350 ZIP persistence + geolocation boot; L3351 compare sync
Why it hurts: nothing is testable (pure predicates and sort/filter live inside the closure, unreachable from a test runner); it talks to listings.js / listings_boot.js / market_intel.js / geo.js / templates through **~40 distinct `window.__DS_*` globals** (e.g. __DS_runFilterRender used in main.js x7, listings.js, listings.html) plus SC.* back-bridges (L44-45) — an implicit, untyped bus with load-order dependency (L25–36 hard-fail check). Previous extraction (listings_boot/geo/market_intel) stopped half way.
Split (ES-module-free, keep classic scripts, attach to SC):
- `listings_filters_pure.js` — L93–152, carMatchesFacetFilters, carMatchesSmartFilters, sortListingsCars, listingDepriorityCompare (pure, node-testable)
- `listings_facets.js` — cascade + lazy facets (L153–661)
- `listings_zip_state.js` — ZIP/search-started/geo prompts (L662–1180, L3080–3350)
- `listings_card.js` — renderCardHtml + cssSingleQuotedUrl (the card is also needed by dealership page; one renderer)
- `listings_data.js` — fetch/ETag/partial refetch/applyPayload (L2434–2729)
- `listings_chips.js` — undo, chips, filters sheet (L2730–2976)
- main.js left as orchestrator; replace `window.__DS_*` with one `SC.listings = {...}` object documented in one place.
Risk: high — load-order & hoisting games (L40 note), shared mutable closure state (_listingsAllCars, render gen counters). Do pure-function extraction first (zero risk), add node tests, then data layer.

## F2. frontend/static/car_page.js — 2727 lines, one IIFE (P1)
One `(function(){...})()` (L5–end), ~219 functions. Boot split into initCarPageCritical (L2697) / initCarPageDeferred (L2706).
Responsibilities:
- L8–82 back link + share toast
- L83–429 initCarGallery (~345 lines in ONE function: lightbox, thumbs, swipe, keyboard)
- L430–565 history highlights (own local `esc` at L489)
- L566–991 finance calculator + details animation (~425 lines)
- **L992–1954 TCO (total cost of ownership) — ~960 lines, 45 functions**: pure math (computeTco*, buildTco*, resolveTco*), formatting (3 separate currency formatters L1017/L1087/L1205), canvas chart (setupTcoCostCanvas L1261, renderTcoCostCurve L1307–1474 ~170 lines), input binding, async initTcoIntelligence (L1825, fetches fuel prices)
- L1955–2181 depreciation chart (math already moved to CP.* in car_page_helpers.js; canvas drawing L1993–2158 stays)
- L2182–2267 EV battery; L2268–2418 vehicle history actions (premium gate); L2419–2525 negotiation radar (own formatMoney L2432)
- L2526–2554 build sheet print; L2555–2598 lazy Leaflet loader; L2599–2676 VDP tabs
Why it hurts: TCO is a self-contained feature (a third of the file) whose pure math is untested yet drives a money figure on the page; two canvas chart engines (TCO + depreciation) duplicate setup/HiDPI scaling (setupTcoCostCanvas vs setupDepreciationCanvas — same job); 4 local currency formatters.
Split:
- `car_tco.js` (L992–1954) with pure part → `car_tco_math.js` on CP (computeTco*, buildTcoFuelPriceScenarios, buildTcoCumulativeOperationalCosts) — node-testable
- `car_finance.js` (L566–991)
- `car_gallery.js` (L83–429)
- `cp_canvas.js` — shared HiDPI canvas setup + axis/curve helper used by TCO and depreciation
- `car_premium.js` — history actions + negotiation radar + EV battery (L2182–2525)
- car_page.js keeps back/share/tabs/boot (~400 lines)
Risk: medium — mostly self-contained init functions; watch shared closure consts at file top and the CSP note (no inline scripts, order in car.html).

## F3. Duplicated JS helpers (P2)
- HTML escaping implemented **9 times**: SC.escapeHtml (sc-helpers.js:14), CP.escHtml (car_page_helpers.js:205), car_page.js:489 `esc` (inside closure, identical to CP.escHtml), car_packages.js:201, car_trim_ladder.js:8 (only one that escapes `'`), compare.js:230 (`text || ""` → turns 0 into ""), dev.js:107, dev_scan_lab.js:209, car_dealer_map.js:68, find_dealers.js:228 (+escapeAttr :234), dev_console.js:48 escapeAttr, plus inline in templates/listings.html:541 and admin/dealers.html:319.
  Three of them (car_packages.js:201, dev.js:107, listings.html:541) use the textContent→innerHTML trick which does NOT escape `"`; dev.js:812 puts that result inside `value="..."` (dev-only, low-impact, but a latent attribute-break).
- csrfToken() reimplemented 5x: account_profile.js:17, find_dealers.js:31, main.js:1219, dealership_page.js:75, ds_comments.js:52, ai_chatbot.js:120 (`csrf`).
- Currency formatters: SC.fmtUSD, dev.js:684 fmtUSD, car_page.js formatTcoCurrency/formatTcoFillUpCurrency/formatTcoCostCurrencyShort/formatMoney, CP.formatDepreciationCurrency(+Short).
Split: create `ds_core.js` (load first on every page via _head or base layout): `DS.esc`, `DS.escAttr`, `DS.csrf`, `DS.fmtUSD`, `DS.fmtUSDShort`; make SC/CP alias to it. Delete local copies.
Risk: low (pure functions) — only behavior delta is compare.js `||` vs `??` and the `'` escaping; both improvements.

## F4. Inline <script> blocks in templates — the counts overstate it (P2)
Metric "defs" counts every `<script>` tag. Real inline JS:
- listings.html: 18 tags = 9 JSON data islands (L20–28, L510) + 7 `src=` tags + **one 312-line inline IIFE L511–823**: the nearby-dealer picker (open/close panel, loadDealers fetch, premium hint, checkbox wiring, own escHtml L541, exports `__DS_reloadNearbyDealers`, `__DS_scheduleReloadNearbyDealers`, `__DS_syncActiveDealerIdsFromDom`). → move verbatim to `static/listings_dealer_picker.js`; the one Jinja value it needs (paid access) is already a JSON island (L510). Risk low; P2.
- car.html: 15 tags = 6 JSON islands + 7 src + **one 65-line inline save-button IIFE L1504–1569** that re-implements POST `/api/cars/<id>/save` already in main.js wireResultSaveButtons (L2977–3079, fetch at L3037). → one `static/save_car.js` used by grid and VDP. Contradicts car_page.js header comment "no inline script for CSP" (it works only via nonce). P2.
- dealership.html: 20 tags = 9 JSON islands + 9 src + 3 inline: L14–33 (19 lines, early prefetch of dealer cars — must stay early/inline, fine), L640–643 (3), L646–674 (28-line Leaflet map init with `setTimeout` polling for `L`) → move map init into dealership_page.js. P3.
- Script load order is hand-maintained in 3 templates (listings.html L501–509, dealership.html L629–645, car.html L1461–1502) with hard order dependencies (sc-helpers → listings_boot → market_intel → geo → main). → a `_scripts_listings.html` partial included by both listings.html and dealership.html removes the duplicated order list. P2, risk low.

## F5. CSS — global bundle, misplaced sections, dead rules (P1 for 06-dev split + dead sweep; P2 rest)
Method: scratchpad/monolith/deadcss.py — every `.class` in each CSS file checked as a token against templates/**, static/*.js, backend/**/*.py, ios/**. Then dynamic-prefix families checked by hand (e.g. `deal-badge--${x}` in sc-helpers.js is LIVE).
Load model: `_head_css.html` emits 00…14 as 15 `<link>`s on **every** page (35 templates include it); "ORDER IS THE CASCADE" — so admin/dev rules ship to shoppers and every file can override every other. 15-dealership.css only on dealership.html (good).
Totals: 1,771 distinct classes across CSS; **236 have no literal token anywhere** (~13%). After removing confirmed-dynamic families (deal-badge--*, result-market*, car-market-vs--*, car-chat-bubble--*, ai-chat-msg--*, dr-offer__kind--*, dr-qual__conf--*, dealer-inv-flash--*, admin-flash-*, listing-package-*/car-window-sticker-* partly), roughly **~190 are truly dead**. Per file (raw dead / total): 08-marketing 33/47, 01-nav 32/110, 02-listings 29/139, 06-dev 28/202, 03-car-page 25/266, 10-pages 20/127, 09-components 19/141, 15-dealership 15/94, 07-dealer-admin 13/137, 11-overrides 10/95.
Worst dead families (confirmed, no producer anywhere):
- `lp-*` (old landing: lp-hero, lp-hero-search-*, lp-trust-*, lp-stats, lp-bottom-*, lp-footer*) — landing.html now uses `lps-*`. 08-marketing.css ~33 of 47 classes dead + 10-pages.css lp-hero-search* (12) + 11-overrides.css lp-container/lp-hero. 08-marketing.css is ~70% removable.
- `nav-app-link*`, `nav-app-user*`, `nav-site-link*`, `topbar--app`, `topbar--admin`, `nav-links--admin`, `admin-nav-*` — the pre-sidebar topbar (01-nav L48–116 "Legacy signed-in app topbar", 07 L719 "Legacy horizontal admin topbar", and 11-overrides L1–311 responsive rules for it).
- `docked-*` (02-listings L322–475 DOCKED MODE + accordion; listings.html L478 says the dock markup was deleted) — and main.js L436 still queries `#docked-total` (dead JS too).
- `listings-hero-band*`, `listings-search-card--hero/--unified`, `pill-search-btn`, `filter-grid` (02-listings).
- `admin-credential-hint*` (01-nav), `dev-form*`/`dev-two-col` (06-dev), `dr-chip*` (15-dealership, no producer), `info-card`, `listing-*` (01-nav: listing-content/image/meta/price — pre-grid card).
Cross-file overrides: 98 selectors are defined in >1 file. Top pairs: 01↔11 (21 selectors: all topbar variants), 02↔05 (11), 09↔11 (10), 02↔11, 10↔11 (9 each). Worst: `h1` in 00/01/05/11; `body.listings-page .listings-layout` in 02/05/09/11; `.car-vdp-tabs` in 03/05/09/11; `.results-grid`, `.db-layout`, `.db-hero__actions` in 3 files each. `!important` is low (27 total; 02-listings 7, 03-car-page 6) — fine.
Misplaced sections:
- **06-dev.css (1637)**: only L1–622 + L1517–1637 are dev/data-quality. **L623–1516 (~890 lines) are public car-page rules**: Claude car chat (L623–727), back control (L728), gallery thumb strip (L739), history highlights (L790), packages + window sticker (L806–1065), generated build sheet (L1066–1261), Options-tab source ladder (L1262–1389), listing-derived packages (L1390–1507), spec provenance (L1508). → move to `03b-car-options.css` (packages/sticker/build sheet, ~700 lines) and `03c-car-chat.css`; 06-dev.css shrinks to ~740 and could then be loaded only by dev/admin templates.
- **01-nav.css (1227)**: nav is L1–406; **L407–1227 is guest banner, auth forms (.auth-card/.auth-form/.field-group/.password-toggle), plan options, admin credential, dealer pages, account profile (L1137–)**. → `01b-auth.css` (merge with 13-landing-auth.css which is already the auth file) and `01c-account.css`.
- **07-dealer-admin.css**: L241–711 is the **unified app+admin left sidebar** used on every signed-in page, sandwiched between dealer-portal (L1–240) and store admin (L712–). → `01a-sidebar.css` next to nav.
- **11-overrides.css (522)**: L1–311 a "responsive system" that re-targets selectors owned by 01/02/04/05/09/10; L320 VDP back nav; L407 "final vertical alignment (overrides cascade conflicts)". It exists to win cascade fights. → fold each @media block into the owning file, then delete.
- 03-car-page.css (2830) has its own internal legacy aliases (L1727 "Legacy alias", L2597 "Legacy hero button styles"), plus NHTSA recall page (L2418–2530) that isn't the car page → `10-pages.css`. Also lightbox (L1856–2018) → `03-car-gallery.css`.
- 02-listings.css (1818): pills top-mode (L116–321), docked mode (dead, L322–475), main layout (L577–1441), results/cards (L1459–). Split `02a-filters.css`, `02b-results.css` after deleting dock.
- `frontend/static/style.css` (35 lines): self-declared "NO LONGER LOADED BY ANY TEMPLATE"; only a test docstring references it → delete (P3).
Risk: CSS deletes are visually risky because cascade order matters; mitigate with screenshot diff of listings/car/dealership/admin/landing at 390 + 1440 (scratchpad already has such PNG tooling). Do dead-family deletes first (no cascade effect if truly unreferenced), then moves preserving relative order.

## F6. Other JS files (reviewed via function map + longest-function scan, scratchpad/monolith/fnlen.py)
- **car_page.js initCarFinanceCalculator L566–916 = 351 lines in one function**, initCarGallery L83–428 = 346 lines (both covered by F2 split; within car_finance.js separate `financeMath` (payment/amortization) from DOM binding). P1 via F2.
- **frontend/static/car_chat.js (97 lines) — DEAD**: loaded by no template (grep of templates/ and backend/ for the filename = 0), and car.html has no `#car-chat-section`. Backend route api_car_chat still exists (main.py:624). Its 06-dev.css rules (L623–727) are only partly live because compare_chat.js/compare.html reuse `car-chat-bubble--*`. → delete car_chat.js, rename shared chat CSS to `.ds-chat-*` in a `chat.css` used by compare. P2 (dead code; also yet another csrf reader `readCsrfToken`).
- car_spin.js (382): initSpinPlayer L99–338 = 240-line function (frame loading, drag/momentum, mode switch, pannellum) — split into `spinFrames`, `spinInput` inner modules. P3 (self-contained, single page).
- car_packages.js (755): `finish` L540–645 (106-line nested callback), renderStickerInBox L344; string-concat sticker HTML. Reasonable single-feature file. P3: extract sticker renderer `car_sticker_render.js`.
- dev.js (1015): dev dashboard + scanner ops + data quality (loaded by dev.html, admin/scanner_ops.html, admin/data_quality.html); longest fn 67 lines. It's 3 pages in one file → split per page (`dev_dealers.js`, `dev_scanner_jobs.js`, `dev_incomplete.js`). P3 (internal tool).
- Saved searches implemented twice: main.js initSavedSearchesWidget L1208–1395 (188 lines, GET/POST/DELETE /api/saved-searches) and account_profile.js (L116–, L168, L248 same endpoints). → `saved_searches.js` client shared by both. P2.
- find_dealers.js (625; runSearch 74 lines), compare.js (445), listings.js (416; smart-search parse chips), ds_comments.js (473, a small Widget component — the cleanest file), dev_scan_lab.js (361), account_profile.js (331), car_trim_ladder.js (318), nav_perf.js (278), dev_console.js (272), ai_chatbot.js (251), geo.js (226), sc-helpers.js (238), car_page_helpers.js (229), market_intel.js (167), dealership_page.js (146), compare_chat.js (118), sidebar.js (85), listings_boot.js (75), car_dealer_map.js (74), map_tiles.js (60), password_toggle.js (29): single-feature, functions ≤ ~80 lines → fine aside from the duplicated helpers in F3.
- Vendor: leaflet.js/css, pannellum.js/css — third-party, out of scope (fine).
- No JS test harness at all: no package.json / node tests in frontend. The pure modules (sc-helpers.js, car_page_helpers.js, geo.js filterCarsInRadius, market_intel.js) are already IIFE-on-namespace and could be loaded in node with a tiny `vm` shim — cheapest first step before any split (P1 enabler).

## F7. Templates
### F7a. Inline <style> blocks probably blocked by CSP in production (P1 — correctness, verify first)
backend/main.py:759–775 enforced CSP (on by default when is_production_env(), _csp_enforce_wanted L750) sets `style-src-elem 'self'` with **no nonce and no 'unsafe-inline'**. `style-src 'unsafe-inline'` only covers `style=""` attributes once style-src-elem is present. So these `<style>` elements should be refused by the browser in prod:
admin/dealers.html L5–164 (159 lines), admin/site_hub.html L4–290 (286), admin/users.html L5–124 (119), admin/user_form.html L5–104 (99), admin/reviews.html L4, dev_scan_lab_listings.html L79, and even inventory/base_inventory.html L11 (`<style nonce=…>` — the nonce isn't in style-src-elem either).
Unverified in a browser (read-only audit, CSP_ENFORCE may be overridden on Railway) — check devtools console on /admin/dealers in prod. Fix either way = move each block to a static file: `css/admin-dealers.css`, `css/admin-site-hub.css`, `css/admin-users.css` loaded from admin/base_admin.html (which also gets them off the public bundle). Risk low.
### F7b. admin/dealers.html (755) — 456-line inline script L296–752 (P2)
Dealer-jobs console: esc/fmtTs (another escape copy), job table render, side drawer, job detail, diagnosis fetch, retry, polling (`/api/admin/dealer-jobs*`). → `static/admin_dealer_jobs.js`; data the script needs via a JSON island. Risk low (nonced script moves to 'self').
### F7c. car.html (1573) — P2
Layout: macros L32–152 (dealer_reputation + 6 window-sticker macros), hero L156–536 (~380), overview panel L537–627, **specs panel L628–1068 (~440)**, options panel L1069 + build sheet L1090–1307 (~220), history L1308–1350, dealership L1351–1410, comments L1411–, scripts L1461–1569; 195 `{% if %}`.
Duplicated rendering: window-sticker sections are rendered by Jinja macros (render_sticker_option_sections etc., L56–137) AND by car_packages.js string builders (renderStickerOptionSectionsHtml L311, renderStickerPackagesAndOptionsHtml L290, renderPackageEntryHtml L258) for the lazy-ensure path — two renderers for the same markup that will drift (CSS classes listing-package-*/car-window-sticker-* already partially dead per F5).
Split: `car/_hero.html`, `car/_panel_specs.html`, `car/_panel_options.html` (+ `_build_sheet.html`), `car/_panel_history.html`, `car/_panel_dealership.html`, `car/_panel_comments.html`, `car/_sticker_macros.html`. For the sticker, have the lazy path fetch server-rendered HTML (render the macro partial from the ensure endpoint) and delete the JS renderers. Risk: low for includes; medium for the renderer unification.
### F7d. listings.html (827) / dealership.html (679) — P2 (see F4)
Both carry the same 9 `ds-listings-*` JSON islands and the same ordered script list; dealership.html re-declares the listings grid markup. → `_listings_data_islands.html` + `_listings_scripts.html` partials.
### F7e. No public base layout (P3)
34 templates are full `<!DOCTYPE>` documents; only admin (12) and inventory (2) use `{% extends %}`. Every public page hand-includes _head_css/_head_meta/_head_perf/_nav_*/_footer_version. → `base_public.html` with `{% block head_extra %}{% block content %}{% block scripts %}`. Low risk, mechanical, cuts drift (e.g. which pages load ai_chatbot/password_toggle).

## F8. iOS (ios/SarrafiCars, 29 Swift files)
### F8a. Legacy/ is dead — delete (P2, risk ~0)
ios/SarrafiCars/SarrafiCars/Legacy/{AccountTabView 101, CarDetailView 202, InventoryTabView 159, SavedTabView 66} = 528 lines. Not in project.pbxproj (0 matches each; pbxproj has 25 `.swift in Sources` files = the 29 minus these 4), and no Swift file outside Legacy/ references them (only Legacy/README.md and each other). ARCHITECTURE.md:39 already says "not in Xcode target". They also still call old APIClient shapes, so they rot silently. Delete the folder (git history keeps them).
### F8b. iOS listings fetch relies on the server-remembered ZIP (P2 — correctness risk)
APIClient.fetchListings() (Networking/APIClient.swift:115) GETs `/api/listings/cars` with **no zip/radius**. Since the 2026-09-28 owner decision the endpoint (backend/routes/listings_api.py:340) answers 400 `zip_required` unless a zip param or a session-remembered zip exists. ListingsSession.refresh (Features/Home/ListingsSession.swift:296–312) first calls `try? persistListingsGeo(...)` — failure swallowed — then fetchListings(); ListingsFacetCatalog.loadIfNeeded (L160–181) calls fetchListings() before any search (silently caught). If the cookie bridge or persist fails, the grid errors. Fix: `fetchListings(zip:radius:)` passing the params explicitly. Not monolith-related but found while mapping.
### F8c. Features/Dealers/DealersView.swift (596) — god file (P3)
L9–80 three Decodable models (DealerRow, DealerLocatorCenter, DealerLocatorResponse); L81–148 a CLLocationManager wrapper (LocationManager); L149–596 the view: 9 @State, `body` L171–340 (~170 lines), `dealerRow` L341–516 (~175 lines), search/networking L517–596 calling APIClient directly.
Split: models → `Networking/Models.swift` (or `DealerModels.swift`); `Core/Location/LocationManager.swift`; `Features/Dealers/DealerRowView.swift`; `DealersViewModel` (ObservableObject: zip/radius/dealers/status/search) like ListingsSession. Risk low.
### F8d. Features/Home/ListingsInlineFilters.swift (628) — P3
L4–24 FilterOptionDedupe; L25–359 the filter bar (body L175–267, applyFilters L288–359); L360–628 five reusable controls (FilterMenuLabel, MultiSelectFilterMenu L420, **IntMultiSelectFilterMenu L458 — copy of the String one**, FilterChecklistSheet L499, **IntFilterChecklistSheet L551 — copy**, CompactValuePicker L606). → move controls to `Core/Components/FilterMenus.swift`, make one generic `MultiSelectFilterMenu<Value: Hashable & CustomStringConvertible>` and one generic checklist sheet (removes ~100 lines).
### F8e. ListingsSession.swift (374) — P3
Holds filter model (L16), a client-side filter engine (ListingsFilterEngine L48–143, mirrors main.js carMatchesFacetFilters — third implementation of the filter rules after main.js and backend), facet catalog (L144–255) and the session VM (L256–). Split into `ListingsFilters.swift` (model+engine, unit-testable) / `ListingsFacetCatalog.swift` / `ListingsSession.swift`. Long-term: let the server filter (web already sends zip/radius).
### iOS checked, fine
App/{AppConfig 146, AppState 123, MainTabView 55, SarrafiCarsApp 11}; Core/Components/{EmptyStateView, ListingCardView 96, ListingsGridView, RemoteImage}; Core/Theme/AppTheme 155; Features/Auth/AuthHeaderBar 173; Dealers/DealersMapView 290 (map + DealerMapsLauncher enum, coherent); Home/{HomeView 242 (3 small views), ListingsFilterSheet 180, ListingsGeo 105}; Premium/WebTabView 20; Saved/SavedCarsView 178; Networking/{APIClient 284 (one method per endpoint; fine), Models 279, PinnedTrustEvaluator 95, SessionCookieBridge 77}; Web/EmbeddedWebView 41.

## F5 addendum — file names no longer match contents (P2)
- **12-viewers.css (813)**: only L1–141 are the 360/pano viewers; **L142–813 (~670 lines) are the new landing page (`lps-*`)**. → `08-landing.css` (replacing the ~70%-dead 08-marketing.css), keep 12-viewers at ~140.
- **09-components.css (1267)**: grab-bag — car-page dealership card (L1–52), listings dealer filter/ZIP banner (L53–455), SRP toolbar/chips/mobile sheet (L456–949), VDP sticky CTA (L950–1061), pagination/compare/landing search (L1062–). → distribute into 02-listings (filters/toolbar/pagination) and 03-car-page (dealer card, sticky CTA); 09 left for genuinely shared primitives.
- **04-dashboard.css (884)**: garage/hub/account/billing sections + "Save button (car detail page)" L874 → that block to 03-car-page.
- **05-car-tools.css (1130)**: coherent (VDP finance/EV/TCO/depreciation + recommendations L904) — maps 1:1 onto the proposed car_finance.js/car_tco.js split; just rename to `03d-car-tools.css`. Fine.
- 10-pages.css (1043): landing hero search L1–123 (dead lp-hero-search*), compare L124–404, find-dealers L405–. OK after deleting L1–123.
- 13-landing-auth.css (388, 0 dead), 14-vdp-uniform.css (190), 15-dealership.css (182; dr-chip* dead), 00-base.css (154), ai_chatbot.css (204), dev_console.css (328, 0 dead): fine.

---
# Ranked table
| # | Pri | File / block | Size | Problem | Split / action | Risk |
|---|-----|--------------|------|---------|----------------|------|
| 1 | P1 | backend CSP vs admin `<style>` blocks (admin/dealers, site_hub, users, user_form, reviews, dev_scan_lab_listings, inventory/base_inventory) | ~700 CSS lines | `style-src-elem 'self'` likely blocks them in prod (verify) | move to css/admin-*.css loaded by base_admin.html | low |
| 2 | P1 | frontend/static/main.js | 3360, 1 closure, ~120 fns, 28 closure vars | listings page god-closure; ~40 `window.__DS_*` globals as bus; untestable | listings_filters_pure / _facets / _zip_state / _card / _data / _chips; one `SC.listings` API | high (do pure fns first) |
| 3 | P1 | frontend/static/car_page.js | 2727, 1 IIFE | TCO ~960 lines, finance fn 351 lines, gallery fn 346 lines, 2 canvas engines | car_tco(+_math), car_finance, car_gallery, cp_canvas, car_premium | medium |
| 4 | P1 | 06-dev.css L623–1516 | ~890 | public car-page rules (chat, sticker, build sheet, options) in the dev file | 03b-car-options.css, chat.css; then load 06 on dev/admin only | medium (cascade order) |
| 5 | P1 | dead CSS families | ~190 classes (~13%) | lp-*, nav-app-link*/legacy topbar, docked-*, listings-hero-band*, admin-credential-hint*, dev-form*, dr-chip* | delete after screenshot diff | low-med |
| 6 | P1(enabler) | no JS tests | — | no package.json, no node tests | node `vm` harness over sc-helpers/car_page_helpers/geo/market_intel | none |
| 7 | P2 | duplicated helpers | 13 escapers, 6 csrf readers, 7 currency fmts | drift; 3 escapers don't escape `"` | ds_core.js (DS.esc/escAttr/csrf/fmtUSD) | low |
| 8 | P2 | 01-nav.css L407–1227 | ~820 | auth/account/plan rules in nav file | 01b-auth (merge 13), 01c-account | med |
| 9 | P2 | 07-dealer-admin.css L241–711 | ~470 | shared app sidebar inside dealer-admin file | 01a-sidebar.css | med |
| 10 | P2 | 11-overrides.css | 522 | exists to win cascade fights; 98 selectors defined in >1 file (01↔11: 21) | fold into owners, delete | med |
| 11 | P2 | 12-viewers.css L142–813, 09-components.css, 04 L874 | ~670 + 1267 | file names don't match content | 08-landing.css; redistribute 09 | med |
| 12 | P2 | admin/dealers.html inline script L296–752 | 456 | page app inline | static/admin_dealer_jobs.js | low |
| 13 | P2 | car.html | 1573 | 6 tab panels in one file; sticker rendered by Jinja macros AND car_packages.js | car/_panel_*.html partials; server-render sticker for lazy path | low/med |
| 14 | P2 | listings.html inline IIFE L511–823; car.html save IIFE L1504–1569 | 312 + 65 | inline apps; save-car duplicated with main.js L2977 | listings_dealer_picker.js; save_car.js | low |
| 15 | P2 | listings.html + dealership.html script lists / JSON islands | — | hand-synced load order | _listings_scripts.html, _listings_data_islands.html | low |
| 16 | P2 | car_chat.js | 97 | dead (no template loads it) | delete; rename shared chat CSS | low |
| 17 | P2 | saved searches main.js L1208–1395 vs account_profile.js | 188 + ~130 | two clients for /api/saved-searches | saved_searches.js | low |
| 18 | P2 | ios Legacy/ | 528 | dead, not in target | delete | ~0 |
| 19 | P2 | iOS fetchListings() without zip | — | relies on session-remembered ZIP after 09-28 zip_required contract | pass zip/radius explicitly | low |
| 20 | P3 | ios DealersView.swift | 596 | models + location mgr + 170-line body + 175-line row | DealerModels, LocationManager, DealerRowView, DealersViewModel | low |
| 21 | P3 | ios ListingsInlineFilters.swift | 628 | Int* copies of String menus/sheets | generic FilterMenus.swift | low |
| 22 | P3 | ios ListingsSession.swift | 374 | model+engine+catalog+VM | 3 files | low |
| 23 | P3 | 02-listings.css / 03-car-page.css | 1818 / 2830 | size; internal "legacy alias" blocks; NHTSA page in car CSS | 02a-filters/02b-results; 03-car-gallery; NHTSA → 10-pages | med |
| 24 | P3 | dev.js | 1015 | 3 admin/dev pages in one file | per-page files | low |
| 25 | P3 | car_spin.js initSpinPlayer | 240-line fn | — | inner modules | low |
| 26 | P3 | car_packages.js | 755 | sticker string renderers (see #13) | car_sticker_render.js or delete via #13 | low |
| 27 | P3 | dealership.html L646–674 map init | 28 | inline Leaflet polling | dealership_page.js | low |
| 28 | P3 | public templates w/o base layout | 34 docs | boilerplate drift | base_public.html | low |
| 29 | P3 | static/style.css | 35 | self-declared unused | delete | ~0 |

# P1 details (order of work)
1. **Verify CSP/admin styles** (F7a): open /admin/dealers in prod devtools; if blocked, move the 7 `<style>` blocks to static files. Highest value per hour.
2. **JS test harness** (F6 last bullet): `node --test` + `vm.runInContext` loading sc-helpers.js, car_page_helpers.js, geo.js, market_intel.js against a stub `window`. Needs only node; no bundler. Makes #3/#4 safe.
3. **main.js** (F1): extract pure predicates/sorters/smart-filter matcher first (L93–152, carMatchesFacetFilters L2078, carMatchesSmartFilters L2188, sortListingsCars L1687) into listings_filters_pure.js on `SC`; test them; then the card renderer (shared with dealership page), then the data layer (L2434–2729). Replace window.__DS_* with SC.listings.* incrementally (grep shows every consumer: main.js, listings.js, listings.html, dealership.html, geo.js, market_intel.js).
4. **car_page.js** (F2): TCO first (largest, self-contained L992–1954) → car_tco_math.js (tested) + car_tco.js; then finance (L566–991), gallery (L83–429); shared cp_canvas.js for HiDPI setup used by TCO and depreciation.
5. **CSS dead sweep + 06-dev relocation** (F5): delete confirmed-dead families (lp-*, legacy topbar/nav-app-*, docked-* + main.js L436 `#docked-total`, listings-hero-band*, admin-credential-hint*, dev-form*, dr-chip*), then move 06-dev L623–1516 to car-page files keeping their position in the cascade (insert the new link right after 06). Gate each step with the 390/1440 screenshot diff of listings, car, dealership, compare, landing, admin.

# P2 details
- ds_core.js (F3): 13 escape copies → DS.esc + DS.escAttr; 6 csrf readers → DS.csrf; currency formatters → DS.fmtUSD/DS.fmtUSDShort. car_packages.js:201, dev.js:107, listings.html:541 use the textContent trick that leaves `"` unescaped (dev.js:812 puts it in value="").
- CSS ownership moves (F5): auth/account out of 01-nav, sidebar out of 07, landing out of 12, 09 redistributed, 11-overrides folded and deleted. Keep cascade order: each moved block's new file must load at the same relative position, or bump specificity deliberately.
- Template extractions (F4/F7b–d): listings dealer picker, car save button (dedupe with main.js), admin dealer-jobs console, car.html panel partials, shared listings script/data-island partials.
- Dead code: car_chat.js, ios Legacy/ (528 lines), style.css.
- iOS fetchListings zip contract (F8b).

# Checked, fine (no action beyond F3 helper dedupe)
JS: account_profile.js (331; overlaps saved-searches #17), ai_chatbot.js (251), car_dealer_map.js (74), car_page_helpers.js (229, pure, good model), car_trim_ladder.js (318), compare.js (445), compare_chat.js (118), dealership_page.js (146), dev_console.js (272), dev_scan_lab.js (361), ds_comments.js (473, cleanest — small Widget), find_dealers.js (625, longest fn 74), geo.js (226), listings.js (416), listings_boot.js (75), map_tiles.js (60), market_intel.js (167), nav_perf.js (278), password_toggle.js (29), sc-helpers.js (238), sidebar.js (85). Vendor (out of scope): leaflet.js/.css, pannellum.js/.css.
CSS: 00-base, 05-car-tools (rename only), 13-landing-auth, 14-vdp-uniform, 15-dealership (minus dr-chip*), ai_chatbot.css, dev_console.css.
Templates (partials): _ai_chatbot, _app_logout, _footer_version, _head_css (good comment on cascade order), _head_meta, _head_perf, _icons, _nav_marketing, _nav_public, _nav_search, _nav_search_sidebar, _nav_sidebar (131), _nav_sidebar_guest, _oauth_signin, _password_field.
Templates (pages): account_billing, account_profile (173), admin/attribution, admin/base_admin, admin/dashboard, admin/data_quality (251), admin/inventory, admin/inventory_detail, admin/scanner_ops (159), admin/scans, admin/site_hub (564 — fine once its 286-line <style> moves, #1), admin/users & admin/user_form & admin/reviews (only the <style> issue, #1), admin_login, billing_required, billing_success, compare (112), dashboard (122), dealer_login, dealer_register, dev (288), dev_disabled, dev_login, dev_manifest, dev_register, dev_scan_lab (152), dev_scan_lab_db, dev_scan_lab_listings (<style> L79, #1), find_dealers (143), forgot_password, home (274; 52-line reco carousel inline script — could move to a file, trivial), inventory/base_inventory (<style nonce> #1), inventory/dashboard, inventory/listings, landing (165; 31-line inline script), login, nhtsa_recalls, not_found, premium, premium_success, register, reset_password, verify_email.
iOS: see F8 "checked, fine".
Coverage: all 141 files in area_frontend.txt are accounted for above (main/car_page/CSS/templates/iOS findings + these lists).
