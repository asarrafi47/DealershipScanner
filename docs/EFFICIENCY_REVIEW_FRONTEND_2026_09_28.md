# Front-end efficiency review — Sarrafi Cars (2026-09-28)

Scope: the Flask app at http://localhost:5001 (branch `feature/http-only-scans`), read-only.
Method: `curl` of each page with and without `Accept-Encoding: gzip`, static parse of the
returned HTML (script/link/img/preload/inline JSON), `curl -sI` on assets, `du`/`gzip -5` on
`frontend/static`, and a read of `main.py`, `gunicorn.conf.py`, the templates and the
listings scripts. All numbers are bytes unless stated. Times are loopback `time_total` and
therefore server time only; they carry no network cost.

Car page used: `/car/1470314` (newest live listing with a price). Dealership page used:
`/dealership/hendrickhonda-com` (that car's dealer).

## Decision 2026-09-28 (owner) — radius-scoped listings, no cars before a search

Owner: "whatever the user's zip code/radius is. dont present cars until they actually begin
a search. but if we do this, the results must appear quickly." This supersedes item 1's
"ship the whole fleet" design (F1).

What changed (branch `feature/http-only-scans`):

- `GET /api/listings/cars?zip=&radius=` is radius-scoped. No ZIP is a 400
  `{"ok":false,"error":"zip_required"}`; radius is clamped to 5..250 mi (default 50).
  Registry coordinates (bounding box in SQL, haversine per dealer location), plus the
  existing `dealer_url` coordinate fallback; cars with no coordinates at all are counted in
  `missing_coords`. Hidden dealerships are excluded in SQL. Gzipped body cached per
  (ZIP, radius, hidden set) in an LRU bounded to 64 entries / 96 MB; the ETag is a digest of
  the body.
- Cards are served from a persisted store, `listings_grid_cards`
  (`backend/db/repositories/grid_cards_repo.py`, `migrations/V023`): the grid serializer
  costs ~0.5-0.9 ms per car, so 47k cars would be 25-40 s per request. Freshness per car is
  Postgres `xmin` + attribution/incomplete/serializer revision, falling back to a content
  digest; few changes are rebuilt inline, many are served stale and refreshed in a
  background thread. `python -m backend.scripts.build_listings_grid_cards` fills the store
  offline (91 s for 214,678 cars locally) — run it after deploys that change the card
  serializer and after full scans.
- Nothing on a request path builds the whole-fleet grid: the `/listings` bootstrap grid,
  the dealership page (now a dealer-scoped query), `get_filter_options(include_all_cars=)`
  and the grid prewarm are gone. Filter facets were already SQL `DISTINCT`s; their cache is
  now keyed by the write fingerprint.
- `/listings` renders the search bar, ZIP + radius (prefilled from the session, radius 50)
  and filters, and no cars. A search begins on a typed query, a ZIP/radius change or any
  filter; deep links with filters/ZIP count as begun. Then one fetch of the shopper's area,
  skeleton cards meanwhile, and all facet filtering client-side on that subset. No ZIP ->
  a ZIP prompt with "Use my location" (geolocation is no longer requested on page load).
  Smart search (`POST /api/search/smart`) is scoped to ZIP + radius too (400
  `zip_required` without one).

Measured on a local instance (Postgres, 214,678 active cars, loopback, card store built):

| Request | Cold (LRU empty) | Warm (LRU hit) | Cars | Gzipped body |
|---|---|---|---|---|
| `/api/listings/cars?zip=92694&radius=50` | 0.99 s (first request of the process; 0.72 s once process-warm) | 4 ms | 47,551 | 6.50 MB |
| `/api/listings/cars?zip=37405&radius=50` | 111 ms | 2.5 ms | — | 764 KB |
| `/api/listings/cars?zip=60601&radius=25` | 62 ms | 2.8 ms | — | 300 KB |
| 304 revalidation (92694/50) | — | 2.7 ms | — | 0 |

- `/listings` document: 1,460,089 B / 165,900 B gz before -> 1,403,871 B / 159,487 B gz
  after (bootstrap grid and the head inventory fetch removed; the 920 KB cascade blob,
  item 3(a), remains). Warm render 25 ms.
- Process RSS after 13 different ZIP scopes plus `/listings`, a dealership page and
  filter-options: ~0.65 GB (the web process peaked at ~9.5 GB holding the fleet).
- Before: every `/listings` visit transferred 32 MB gz (214,678 cars). After: nothing until a
  search, then 0.3-1.5 MB gz for most metros and 6.5 MB for the densest (92694/50).

Follow-ups: the card payload is still the full grid card (~1.25 KB raw per car; `gallery`,
`image_url` and `deal_score` dominate) — trimming it would cut the 92694/50 body well below
6.5 MB; the scanner/nightly should run the card builder after scans (backend/scanner was
out of scope); the first `/listings` of a process still pays the cold facet build (~1-2 s).

## 1. Per-page measurements

### 1.1 Document and asset counts

| Page | HTML raw | HTML gz | Server time (gz, warm) | JS req | JS bytes (wire) | CSS req | CSS bytes (wire) | `<img>` in HTML | Inline `<script>` (non-JSON) | Third-party hosts referenced |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `/` | 15,294 | 4,088 | 170-210 ms | 1 | 10,739 | 16 | 330,662 | 0 | 1 (1.2 KB) | fonts.googleapis.com, fonts.gstatic.com (+ homenetiol / honda image URLs in markup) |
| `/listings` | 1,460,089 | 165,900 | 40-70 ms | 9 | 220,915 | 16 | 330,662 | 0 (48 cards rendered by JS) | 2 (3.4 KB + 12.8 KB) | fonts x2 + 15 dealer image CDNs (from the bootstrap grid) |
| `/listings?make=Toyota` | 1,460,108 | 165,923 | 40-70 ms | 9 | 220,915 | 16 | 330,662 | 0 | 2 | same as `/listings` |
| `/car/1470314` | 62,791 | 11,726 | 41-97 ms warm; 1.1-3.6 s in one earlier 3-sample run | 9 | 324,025 | 17 | 345,468 | 1 (hero) | 2 (79 B + 3.0 KB) | content.homenetiol.com, automobiles.honda.com, www.carfax.com, www.google.com, www.hendrickhonda.com, fonts x2 |
| `/dealership/hendrickhonda-com` | 105,816 | 15,793 | 160-360 ms | 11 | 362,213 | 17 | 345,468 | 0 | 3 (+ 14,160 B `<style>` block) | tile.openstreetmap.org, maps.apple.com, waze.com, www.google.com, www.hendrickhonda.com, fonts x2 |
| `/find-dealers` | 14,324 | 3,201 | 1-4 ms | 5 | 189,903 | 17 | 345,468 | 0 | 1 (71 B) | fonts x2 |
| `/compare` | 8,880 | 2,283 | 1-6 ms | 3 | 29,535 | 16 | 330,662 | 0 | 0 | fonts x2 |

Notes.
- "CSS bytes (wire)" is what the browser downloads: the 15 partials in `_head_css.html`
  total 329,746 raw and are served uncompressed (see 1.3), plus the 916-byte Google Fonts CSS.
  Pages with a map add `vendor/leaflet/leaflet.css` (14,806).
- Every page carries the same 15 stylesheet links regardless of what it renders.
- `/listings` and `/listings?make=Toyota` differ by 19 bytes (the `og:url` meta and CSP
  nonces). The 48-car bootstrap grid is identical and unfiltered (8 makes); the only
  server-side effect of `?make=Toyota` is `checked` on one checkbox. The grid is filtered on
  the client, and until the full inventory arrives (1.2, item 5) a filter runs against those
  48 cards only.
- The car page's 1.1-3.6 s samples were a single run of three consecutive fetches; the
  next five samples were 41-97 ms, and a second car (`/car/1462873`) measured 41-97 ms.
  That is server-side variance (not front-end) and worth a look in the VDP route, but this
  review does not assert a cause.

### 1.2 Largest inline JSON blob per page

| Page | Blob id | Raw | gz | Holds |
|---|---|---:|---:|---|
| `/listings` (and `?make=Toyota`) | `ds-listings-car-rows` | 920,339 | 127,255 | Dictionary-encoded cascade table: 23,157 rows x 8 columns (`make, model, trim, fuel, cyl, drive, body_style, induction`); vocabularies 72 makes / 2,004 models / 5,439 trims / 465 body styles / 43 drives. Used by `listings_boot.js` to build `CAR_ROWS` for Make -> Model -> Trim cascading facets. |
| `/listings` | `ds-listings-bootstrap-grid` | 54,239 | 4,807 | 48 cars x 34 keys, the first-paint grid. |
| `/listings` | `ds-listings-package-rows` | 5,239 | 1,394 | Package facet rows. |
| `/car/<id>` | `car-gallery-json` | 3,698 | - | 38 gallery image URLs. |
| `/dealership/<key>` | `ds-listings-bootstrap-grid` | 23,252 | - | This dealer's inventory cards (server-rendered subset). |
| `/dealership/<key>` | `ds-listings-car-rows` | 4,941 | - | Dealer-scoped cascade table. |
| `/`, `/find-dealers`, `/compare` | none | - | - | - |

Composition of the 1,460 KB listings document: 920 KB cascade blob + 54 KB bootstrap grid +
5 KB packages + 479 KB markup (30.6 KB gz). Of the 479 KB of markup, 363 KB is 1,419
`<label class="filter-option">` facet checkboxes: transmission 805, body_style 460,
drivetrain 45, make 38, fuel_type 14, colours 26, cylinders 10, country 7. The transmission
and body_style facets are un-normalised dealer strings and make up 1,265 of the 1,409
checkboxes. The 12.8 KB inline dealer-picker script is a third block of uncacheable payload.

Document order matters for `defer`: the cascade blob starts at byte 6,775 and the
`main.js` tag sits at byte 1,446,605. The preload scanner discovers the script URLs early,
but `DOMContentLoaded` (and therefore `listings_boot.js`'s `JSON.parse` of the 920 KB blob
and every `defer` script) waits for the parser to chew through the whole 1.46 MB.

### 1.3 Asset delivery headers (`curl -sI`)

| Asset | Content-Length | Cache-Control | ETag | Last-Modified | Content-Encoding |
|---|---:|---|---|---|---|
| `/static/main.js?v=...` | 147,190 | `no-cache` | yes (weak-ish: mtime-size-crc) | yes | none (147,190 on the wire with `Accept-Encoding: gzip`) |
| `/static/css/02-listings.css?v=...` | 41,007 | `no-cache` | yes | yes | none |
| `/static/vendor/leaflet/leaflet.js?v=...` | 147,552 | `no-cache` | yes | yes | none |

- Conditional requests work: `If-None-Match` -> `304`. But `Cache-Control: no-cache` is
  Flask's default when `SEND_FILE_MAX_AGE_DEFAULT` is unset (nothing in the repo sets it), so
  every navigation revalidates every asset: 15 CSS + 9 JS + fonts = about 26 conditional
  round trips on `/listings` even with a warm cache, despite every URL already carrying a
  `?v=<mtime>` cache-buster.
- The `?v=` value is `int(mtime(frontend/static/style.css))` (`main.py:398`). `style.css`
  is a dead file ("NO LONGER LOADED BY ANY TEMPLATE", last touched 2026-08-03), so editing
  any partial or script does not change the query string. Harmless today because of
  `no-cache`; it becomes a stale-asset bug the moment a long `max-age` is set.
  `car_page.js`, `car_page_helpers.js` and `car_packages.js` are emitted with no `?v=` at
  all (`car.html:1478-1485`).
- HTML and JSON: gzipped in-process by `_gzip_large_json` (`backend/main.py:360-387`,
  level 5, every response >= 2 KB, `Vary: Accept-Encoding`). No proxy compression exists:
  `railway.toml` deploys `Dockerfile.web`, whose entrypoint runs gunicorn directly
  (`-w 3 --threads 4 --preload`), and Railway's edge does not compress on the app's behalf.
  `gunicorn.conf.py` only handles the listings-cache prewarm. Static files therefore go
  over the wire uncompressed in production too.
- Page HTML: `/listings` sends no `Cache-Control` (session-varying, `Vary: Cookie`);
  `/car/<id>` sends `private, max-age=180`, which is what makes `nav_perf.js` prefetches
  useful.
- `/api/listings/cars`: `public, max-age=0, s-maxage=60, stale-while-revalidate=30`,
  weak ETag, `304` on `If-None-Match`; `/api/listings/geo-coords`: `private, max-age=300`.

### 1.4 Images

| Page | `<img>` count | `loading=lazy` | `srcset` | `width`+`height` | Notes |
|---|---:|---:|---:|---:|---|
| `/listings` | 0 in HTML; 48 rendered by `main.js` | all (template at `main.js:1429`: `loading="lazy" decoding="async"`) | 0 | 0 | Fixed CSS height (`.result-image { height: 200px }` / `clamp(180px,22vw,220px)`) prevents layout shift; images come from 15 different dealer CDN hosts (no preconnect possible). Inline `onerror` swaps to `placeholder.svg`. |
| `/car/<id>` | 1 (hero) | no | no | no | `object-fit: contain` inside an absolutely positioned box, so no CLS, but the LCP image has no `fetchpriority="high"`, no `<link rel=preload as=image>`, no `srcset`; 37 more gallery URLs are built by `car_page.js`. |
| others | 0 | - | - | - | - |

### 1.5 Preloads and hints

- `_head_perf.html` (all pages): `preconnect` to fonts.googleapis.com and fonts.gstatic.com;
  render-blocking Google Fonts stylesheet (Inter 400/500/600/700, `display=swap`); `nav_perf.js` deferred.
- `/listings`: `<link rel="preload" as="fetch">` for `/api/listings/geo-coords` and
  `as="script"` for `compare.js` (already a `defer` script in the body, so the preload buys nothing).
- No preload for the car page hero image, no `modulepreload`, no preload of `main.js`.

### 1.6 Listings page: what the browser requests

On load, in order:

1. Document (166 KB gz, 1.46 MB decoded).
2. 15 CSS (330 KB, uncompressed) + Google Fonts CSS (cross-origin, render-blocking) + Inter woff2 files.
3. `/api/listings/geo-coords` (39 KB gz) up to three times from three code paths: the
   `<link rel=preload as=fetch>`, the inline head fetch (`listings.html:64`, which does not
   publish a promise), and `main.js:startListingsAssetPrefetch` when `SC.listingsDealerCoordsReady()`
   is false. The later ones are served from the private 300 s cache, so the cost is
   request overhead, not bytes.
4. 9 JS files (221 KB uncompressed; 51 KB if gzipped): `nav_perf, sidebar, sc-helpers,
   listings_boot, market_intel, geo, main (147 KB), compare, listings`.
5. After `DOMContentLoaded` (`requestIdleCallback`, 1.5 s timeout, `priority: "low"`):
   `/api/listings/cars` = **32,298,381 bytes gzipped, 291,849,433 bytes decoded, 214,678
   cars x 35 keys** (about 440 raw bytes per car). Field share of raw bytes: `image_url`
   24.1%, `dealer_url` 7.5%, `gallery` 7.1%, `title` 6.8%, `dealer_name` 5.2%, `dealer_id`
   4.8%, `engine_description` 4.1%, colours and colour families 10.9%. CPython `json.load`
   of it takes 0.81 s on this machine; a browser `JSON.parse` of 292 MB is on the order of
   1-3 s of main-thread time and 0.5-1 GB of heap. The template comment describing this
   fetch (`listings.html:21-29`) still says "~8 MB gzipped, ~65 MB decoded" and the
   dealership template says "the whole 71k-row fleet"; the payload has grown roughly 4x
   since those notes were written. The `If-None-Match` path returns `304`, but a fresh
   tab or an evicted cache (32 MB is a prime eviction candidate) pays the full download.
6. 24-48 card images from dealer CDNs (lazy).
7. `nav_perf.js`: an IntersectionObserver warms up to 8 visible `/car/` links plus hover
   intent, budget 12 documents per page view, 3 in flight, `priority: "low"`. Each is
   ~12 KB gz on the wire but a full VDP render on the server (41-97 ms warm; the 1-3 s
   outliers above would hit here too).
8. Premium sessions only: `/api/listings/market-stats` (146 KB gz; 1.3 s cold, 33 ms warm).
9. Opening the Model / Trim / Package accordion: `/api/listings/filter-options` once (83 KB gz).

On each filter change: no inventory request. `renderResults()` filters `window.ALL_CARS`
(or the 48 bootstrap cards while the 32 MB fetch is still in flight) on the client and
`history.replaceState`s the URL. Side requests: debounced `POST /api/session/listings-geo`
when zip/radius changes; debounced `/api/listings/market-stats?zip_code&radius` for premium
users; `/api/search/smart` and `/api/search/smart/parse` (listings.js) while typing in
smart search; `/api/coords-to-zip` once if geolocation is granted; `/api/cars/<id>/save`
on heart clicks. `data-listings-poll-ms="0"`, so no polling.

Total on a cold `/listings` visit: roughly 30 requests before the grid is interactive,
~0.72 MB of document+CSS+JS on the wire (of which ~0.44 MB is avoidable by compressing
static), then 32 MB of inventory and 24-48 images.

## 2. Static asset table

`frontend/static` is 1.3 MB total (vendor 244 KB, brand 80 KB).

| File | Raw | gz -5 | Loaded on |
|---|---:|---:|---|
| `vendor/leaflet/leaflet.js` | 147,552 | 43,205 | car (when dealer has a pin), dealership (when geo), find-dealers |
| `main.js` | 147,190 | 32,851 | listings, dealership |
| `car_page.js` | 110,813 | 22,452 | car |
| `css/03-car-page.css` | 59,848 | - | every page |
| `vendor/pannellum/pannellum.js` | 56,249 | - | car, only with an interior pano |
| `dev.js` | 41,820 | - | dev console only |
| `css/02-listings.css` | 41,007 | - | every page |
| `css/06-dev.css` | 32,218 | - | every page |

CSS cascade (all 15 partials on every page): 329,746 raw -> 58,689 gz. Listings JS set:
220,915 raw -> 50,940 gz. Car page JS set: 324,025 raw (leaflet 147 KB + car_page 111 KB
+ ds_comments 19 KB + compare 16 KB + helpers 11 KB + nav_perf 11 KB + others).

Per-partial use on `/listings` (root class selectors referenced by the rendered HTML or its
JS): `06-dev` 0 of 187, `12-viewers` 0 of 63, `13-landing-auth` 0 of 28, `07-dealer-admin`
1 of 107, `04-dashboard` 1 of 82, `03-car-page` 1 of 211. About 150 KB of the 330 KB
(roughly 45%) is dead weight on the listings, home, compare and find-dealers pages.

Duplication inside the cascade: 2,145 distinct selectors, 106 defined in more than one
partial, 240 defined more than once in total (`.results-grid` in 3 files,
`body.listings-page .listings-layout` in 4, `.car-vdp-tabs` in 4, `.find-dealers-map` 6
times across `10-pages` and `11-overrides`, `h1` in 4). `11-overrides.css` (17 KB) exists
to re-override earlier partials; 28 `!important`s across the set.

Script inventory on the listings page: `main.js` is a single 147 KB file (15 `fetch(`
calls) but not a bundle; the page loads nine separate files. `listings_boot.js` (3.6 KB)
reads the 11 JSON blobs into `window.*` globals and decodes the cascade dictionary.
`market_intel.js` (6.8 KB) and `geo.js` (9.4 KB) are extractions from `main.js` that share
the `window.SC` namespace from `sc-helpers.js` (9 KB). `dealership_page.js` (6.5 KB)
monkey-patches `window.fetch` to redirect `/api/listings/filter-options` to the
dealer-scoped endpoint and wires the hide toggle. `account_profile.js` (14.8 KB) handles
the profile page actions and issues one GET on load (section 4).

## 3. Findings

F1. The full active inventory is shipped to every listings visitor: 32.3 MB gzipped /
    292 MB decoded / 214,678 rows, fetched at idle on every `/listings` load so that
    filtering, facet counts and radius search can run on the client. It is the largest
    payload on the site by two orders of magnitude, it is 4x the size the code comments
    assume, and until it lands any filter silently searches only the 48 bootstrap cards.
    `image_url`, `dealer_url`, `gallery`, `title`, `dealer_name` and colour families are
    about 55% of the raw bytes and are all derivable or joinable from smaller tables.

F2. Static assets are neither cached nor compressed. `Cache-Control: no-cache` (Flask
    default) on 26 assets per page forces a revalidation round trip for each on every
    navigation, and nothing gzips them (not Flask, not gunicorn, not a proxy). A cold
    listings visit downloads 551 KB of CSS+JS that would be 110 KB gzipped. The `?v=`
    cache-buster is keyed off the mtime of the dead `style.css`, so it never changes when
    real assets change, and three car-page scripts have no `?v=` at all.

F3. The listings document is 1.46 MB (166 KB gz). 77% of the gzipped document is the
    920 KB cascade blob, which is regenerated and re-sent with every listings and
    dealership document instead of being a cacheable, ETag'd resource. A further 363 KB
    of markup is 1,419 facet checkboxes, 1,265 of them un-normalised transmission and
    body-style strings. `DOMContentLoaded`, and so every `defer` script, waits on all of it.

F4. Every page loads all 15 CSS partials (330 KB raw, 59 KB gz). On public pages roughly
    45% is unused (dev console, dealer admin, dashboard, viewers, landing/auth, and the
    60 KB car-page sheet on non-car pages). 106 selectors are defined in more than one
    partial and `11-overrides.css` re-overrides earlier files.

F5. Fonts come from Google: one render-blocking cross-origin stylesheet plus woff2 files
    from a second origin on every page, mitigated only by `preconnect` and `display=swap`.

F6. Car page: the LCP hero image has no `fetchpriority`/preload/`srcset`; Leaflet
    (147 KB JS + 15 KB CSS + tile requests) is loaded and initialised at page load for a
    map that lives in the hidden Dealership tab. The remaining car-page JS is conditional
    (pannellum, spin, trim ladder, packages) which is good.

F7. Dealership page: a 14,160-byte inline `<style>` block is re-sent uncacheable with
    every dealership document; the page loads the entire listings JS stack (main.js etc.)
    for a single dealer's inventory.

F8. `/api/listings/geo-coords` is requested from three code paths on the listings page
    (preload, inline head fetch, `main.js`); only the first costs bytes but each costs a
    request and the inline fetch does not publish a promise the others can chain on.

F9. Server-rendered `/listings?make=Toyota` ships the same unfiltered 48-card grid as
    `/listings`; the filter is applied client-side after script execution.

F10. `nav_perf.js` can trigger up to 12 extra full VDP renders per listings view (8 from
     viewport warming). Cheap on the client (12 KB each) but each is a server render.

F11. HTML/JSON gzip runs per request at level 5 in the request thread with no cache of the
     compressed body; for the 1.46 MB listings document that is on the order of 20-30 ms
     of worker CPU per hit. Acceptable now; worth caching by inventory ETag once F3 lands.

F12. Profile page (`/account/profile`): the server already renders hidden dealers, the 20
     most recent searches and saved searches (`_account_profile_context`, three
     repository calls). `account_profile.js:102` then re-fetches
     `GET /api/profile/hidden-dealers` on load and re-renders the list it already has.
     History and saved searches are not refetched. `password_toggle.js` is a synchronous
     script (no `defer`). Requests on load: document + 15 CSS + fonts + `nav_perf.js`
     + `password_toggle.js` + `account_profile.js` + 1 redundant XHR.

F13. No render-blocking `<script src>` anywhere in the reviewed pages: all are `defer`
     (except the tiny synchronous `password_toggle.js` on auth/profile pages at the end of
     `<body>`). Inline `style=""` attributes are limited to 16 `display:none` toggles on
     listings and 12 on the dealership page; the car page has none. Leaflet is not loaded
     on pages without a map.

## 4. Ranked recommendations

| # | Recommendation | Expected gain | Effort | When |
|---|---|---|---|---|
| 1 | Stop shipping the whole fleet (F1). Target: server-side filtering with a paged `/api/listings/cars?<filters>` (or a compact columnar, dictionary-encoded subset with `image_url`/`dealer_url`/`gallery`/`title`/`dealer_name`/colour families removed and joined client-side from a ~200-row dealer table). Interim: drop `gallery`, `dealer_url`, `dealer_name`, `title`, `*_color_families` from the payload now. | Full fix: -32 MB per visit, -1-3 s main-thread parse, -0.5-1 GB heap, correct filter results from first interaction. Interim: about -25-35% raw / -15-25% gz (roughly -5-8 MB gz). | L (interim S) | Next (interim ship-now) |
| 2 | Cache and compress static (F2): set `SEND_FILE_MAX_AGE_DEFAULT` to one year and add `immutable` for `?v=`-stamped paths; serve precompressed `.gz`/`.br` for `/static` (Flask-Compress, WhiteNoise, or a small `after_request` that reads a sibling `.gz` built at image build). Prerequisites: derive `static_cache_ver` from a content hash or the max mtime across `frontend/static` instead of the dead `style.css`, and add `?v=` to `car_page.js`, `car_page_helpers.js`, `car_packages.js`. | Cold visit: -440 KB (551 KB -> 110 KB) on listings, -530 KB on car. Warm navigation: -26 conditional round trips (about 4-5 RTTs over 6 h1 connections, 200-400 ms at 40-80 ms RTT). | S | Ship now |
| 3 | Listings document diet (F3): (a) move the 920 KB cascade blob to an ETag'd, `max-age`+`immutable`-per-inventory-version `/api/listings/cascade` fetched via `<link rel=preload as=fetch>`; (b) normalise `transmission` and `body_style` to canonical values so the facets are ~30 options instead of 1,265; (c) move the 12.8 KB dealer-picker inline script into `listings.js`. | (a) document 166 KB -> ~40 KB gz, cascade cached across listings/dealership navigations, `DOMContentLoaded` earlier by the parse of 920 KB inline JSON (100-200 ms on mid-range phones); (b) -330 KB raw / -25 KB gz and a usable facet; (c) -12.8 KB per view, cacheable. | (a) M, (b) M (data), (c) S | (c) ship now; (a)(b) next |
| 4 | CSS bundles per audience (F4): public bundle = 00, 01, 02, 08, 09, 10, 11, 14; car page adds 03 and 05; dev/dealer-admin/dashboard/viewers/landing-auth only on their pages. Concatenate each bundle into one file at build (or keep links if HTTP/2 at the edge). Then dedupe the 106 multiply-defined selectors and fold `11-overrides.css` into its owners. | -117-177 KB raw (-20-30 KB gz) per public page, -5-8 requests, less style-recalc work; ~-60 KB raw on car page. | M | Next |
| 5 | Self-host Inter (F5): four latin woff2 files under `/static/fonts`, `@font-face` with `font-display: swap`, drop the two Google origins and preconnects; optionally `<link rel=preload as=font>` for the 400/600 weights. | Removes one render-blocking third-party CSS request and DNS/TLS to two origins on cold loads (100-300 ms FCP on mobile); ~80 KB of fonts now cacheable under the same policy as recommendation 2. | S | Ship now |
| 6 | Car page critical path (F6): `fetchpriority="high"` + `<link rel="preload" as="image">` for `gallery_images[0]`, `width`/`height` or `aspect-ratio` on the hero, `loading="lazy" decoding="async"` on thumbnails; load Leaflet + `leaflet.css` + `car_dealer_map.js` when the Dealership tab is first opened (IntersectionObserver or tab click) instead of at load. | LCP about one RTT earlier; -162 KB JS/CSS and all tile requests off the initial load; fewer 3rd-party hosts contacted on first paint. | S/M | Ship now |
| 7 | Dealership page (F7): move the 14.2 KB `<style>` block into `10-pages.css`; consider a dealer-scoped light script instead of the full listings stack for pages with fewer than a few hundred cars. | -14 KB raw / -2.5 KB gz per dealership view, cacheable; up to -200 KB JS on dealership pages if the stack is trimmed. | S (style), L (stack) | Style ship now; stack later |
| 8 | Profile page (F12): delete the on-load `GET /api/profile/hidden-dealers` (refetch only after a mutation, or update the DOM from the DELETE response as the code already does); add `defer` to `password_toggle.js`. | -1 request and one repository query per profile view; no functional change. | S | Ship now |
| 9 | Geo-coords single path (F8): have the inline head fetch publish `window.__DS_listingsGeoPrefetchPromise` (or rely solely on the `<link rel=preload>` + `main.js`) so exactly one request is issued. | -1 to -2 requests per listings view. | S | Ship now |
| 10 | Server-filter the bootstrap grid (F9): build the 48 bootstrap cards from the URL filters so the first paint is correct and no client re-render is needed. | Correct first paint on filtered URLs; avoids a full grid re-render after script execution. | M | Next |
| 11 | `nav_perf.js` viewport warming (F10): keep hover/touch intent, lower viewport warming from 8 to 3-4 cards or disable it while `/api/listings/cars` is in flight. | Up to -8 VDP renders per listings view on the server (each 41-97 ms warm). | S | Next |
| 12 | Cache the gzipped listings document body per inventory ETag once the cascade blob is external (F11). | -20-30 ms worker CPU per listings hit. | S | Later |

### Quick wins that can go in one small PR (ship now)
Items 2 (with its `?v=` prerequisites), 5, 6, 7-style, 8, 9 and 3(c). Together they remove
roughly 450-500 KB and 25-30 requests per cold listings visit, take one third-party CSS off
the render-blocking path, and cost no behaviour change.

### The one structural change that matters most
Item 1. Nothing else on the page is within two orders of magnitude of the 32 MB / 292 MB
inventory transfer, and the client-side filtering design that requires it is also the
reason facet counts, radius search and the very first filter interaction are wrong until it
arrives.

## Appendix: measurement artifacts

- Fetched HTML and parser output: `/private/tmp/claude-501/-Users-asarrafi-Projects-DealershipScanner/ae42936d-237a-4ba0-a6b7-802801370a1a/scratchpad/pages/` and `measure.json` (session scratchpad, not committed).
- Files read: `backend/main.py` (180-200, 355-400, 690-730, 903-1010), `gunicorn.conf.py`,
  `scripts/docker-entrypoint-web.sh`, `railway.toml`, `frontend/templates/{listings,car,dealership,account_profile,_head_css,_head_perf,_head_meta}.html`,
  `frontend/static/{main,listings_boot,listings,geo,market_intel,nav_perf,dealership_page,account_profile,car_dealer_map}.js`,
  `frontend/static/css/*.css`.
