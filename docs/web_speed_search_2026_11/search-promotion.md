# Search promotion: the search line ahead of the filters

Date: 2026-11-10 (third pass; the 2026-11-09 draft was re-measured from scratch and replaced).
Live site: https://sarraficars.com, release 1.5.8. `GET /api/health` returned
`{"commit":"0fd91872bedb07a30ecdd7e19773db5fdc53b1a7","status":"ok","version":"1.5.8"}` (C1), which is `main` and tag `v1.5.8`
(0fd91872b). The staged tip is faea2b985 (1.5.16). `git diff --stat v1.5.8 faea2b985 -- frontend/` lists 6 files
(`car_packages.js`, `listings/card.js`, `main.js`, `car.html`, two dev templates) and none is search UI, so every proposal below
applies to both. Line numbers are v1.5.8; in `main.js` the lines after 1278 are 13 to 14 higher at the tip
(`__DS_applySmartFilters` L1832, `setScalarSelect` L1881, filters toggle L2373 and L2384).

How it was done. Live-site screenshots and element measurements with Playwright/Chromium (phone 390x844 at DPR 2, desktop 1440x900).
Static prototypes built from the live `/listings` HTML with the site's 15 real stylesheets. Counts and result cards in the prototypes
are what the live site returned today. Read-only probes of the parser and the logging tables against the local database. Every number
below comes from a command in section 9 (tags C1 to C12). Live-site budget: **72 anonymous GET requests**, no POST, no login, no form,
at least 1.05 s apart; the ledger is `search_promotion_raw/pass3/out/live_requests.jsonl` (section 9.2). Nothing in the repository was
changed except this file and the folder `search_promotion_raw/pass3/` next to it.

The sibling reports: `search-quality.md` scores the search bar itself; `phone-spacing.md` PS-01 and PS-02 found the same phone
defect from the spacing side. This report is about where the bar sits and what surrounds it.

Screenshot folder (all paths below are inside it):
`scratchpad/web_shots/`
Prototype pages: `scratchpad/search_proto/`
(see 4.7 for how to open them).

---

## 1. Summary for the owner

**Where search is today**

- **On a phone the listings page has no search box.** The smart search field sits inside the Filters sheet, which is hidden until you
  tap "Filters" (a button that does not say "search"). A second, different search ("Find cars", 131x33 px, 13 px type) sits in the
  off-canvas menu. The first phone screen shows two ZIP fields, the Filters button, "Save this search" and a sort menu (C3).
- **On a 1440 px desktop search is one control among about 30**: a 320x49 px box in the filter column, next to 4 drop-downs, 13
  accordion sections (1,496 checkboxes) and 17 buttons, plus a second 131x33 px search in the navy sidebar. The first screen shows 4 ZIP
  fields (C3).
- **When nothing matches, the page is blank.** For "red Tesla Model Y under $30k" it shows "0 vehicles near 92694", a row of chips and
  a map link, and nothing else (live, section 2.3).

**What promoting search would amplify**

- **The examples the site shows return nothing.** Live, ZIP 92694 within 25 mi: 3 of the 4 example queries the site shows or contains
  return 0 cars (top-bar placeholder, landing placeholder, landing sample). The fourth, "white BMW X5 under $70k", returns 20 (C5).
- **Every keystroke is a full search.** Typing a 22-character query sends 22 parse requests and 22 searches (44 requests) at 90, 150,
  300 and 500 ms per key. The limiter allows 90 per minute per IP across both endpoints, so a third typed query in a minute hits it.
  A sandbox change cuts 44 to 6 (section 5, P0-1; C11).
- **The first keystroke starts a 17.1 MB download** (1.63 MB on the wire) of every car within the radius (C6).
- **Search results are cut off silently** at 200 (page load) or 500 (typed search); nothing on the page says so (3.4).

**Three options.** All three keep every filter control (the filter panel has the same 4 selects, 1,496 checkboxes, 13 accordion
sections and 17 buttons as today, C10). They share one base: the bare search line from the landing page, first on the page, beside the
menu button on a phone.

- **A, Search line first.** The line, "Near / Within", a "Try" list of five real queries with counts, and a "On the lot" list of the
  most listed models. Rotating real examples in the empty field, the `/` key. On desktop the line spans the top of the page above both
  columns ("A-wide"); at 1024 px the in-column version's field is 208 px wide, the wide one 553 px.
- **B, Read it back.** A, plus the typing demo you liked (a query types itself and each word lights its filter row) and, after a search,
  one editable sentence instead of two rows of chips.
- **C, Remembers and recovers.** A, plus a suggestion list in place on focus (recent, searched near you, jump to a filter) and "drop one
  part" rows with real counts when nothing matches: "red Tesla Model Y under $30k" has 0 cars, but 15 in any color.

**Measured on the prototypes** (390 and 1440 px, C10): no boxes, pills, shadows or backgrounds in any new region (0 hits on 26
prototype page-and-width runs); boxed controls on the first screen go from 12 to 3 on desktop and 7 to 0 on a phone; the first car after a search
moves from y=479 to y=336 on a phone; search fields on the first phone screen go from 0 to 1; tap targets under 44 px on the first phone
screen go from 16 to 0.

**Recommendation.** Ship three small fixes first (P0: stop searching on every key, make sessions countable, show only verified
examples), then A with the wide layout, then B's sentence and demo, then C's recovery once two weeks of clean logging show the real
zero-result rate. Metrics need P0-2's events first; about 300 to 400 sessions per arm detect a 10-point change (section 6).

**Redis** helps this feature in four places (verified examples and counts per area, a parse cache, relaxation counts, and a shared rate
limiter that comes back if workers exceed one). All four work in-process at today's single worker; Redis makes them survive deploys and
be shared (section 7).

---

## 2. How search and filters are presented today (live, 2026-11-10 20:24 EST)

### 2.1 Measured layout (C3)

| Measurement | Phone 390x844 | Desktop 1440x900 |
|---|---|---|
| Search inputs on the first screen | **0**. `#smart-search-input` is inside the Filters sheet (`09-components.css:883-922` hides the column at 960 px and below until `body.listings-filters-open`); the menu's "Find cars" input is off-canvas at x=-253 | **2**: sidebar "Model or VIN" 131x33 px, 13 px type at (20,84); smart search "Make, model, keyword, or VIN" 320x49 px, 15 px type at (284,140) |
| How a shopper reaches search | tap "Filters" (button at (12,312), 115x40): the field is the top item of the sheet, at (20,214), 350x49 px. Or tap the menu: "Find cars" (a plain GET form: full page load, no instant results) | already visible |
| Controls in the filter column | 4 selects, 2 text inputs, 13 accordion sections (1,496 checkboxes), 17 buttons | same |
| ZIP fields visible on the first screen | 2 (banner at y=149, start prompt at y=482) | 4 (sidebar, filter column, banner at y=71, start prompt at y=255) |
| Boxed controls on the first screen (rounded plus border or fill, outside sidebar and cards) | 7 | 12 |
| Start prompt | "Start a search to see cars. Enter your ZIP code to search inventory near you." With a ZIP the copy says "Search above or pick a filter" (`main.js:630`) although nothing is above on a phone | same |
| After "white bmw x5 under 70k" with ZIP 92694 / 25 mi | first car at **y=479** (479 / 844 = 57% of the screen is chrome); the query text appears only as the last chip; "20 vehicles" | first car at y=241; the query appears twice (parse line under the input, chip row in the results column) |
| Input type size | 15 px (sheet), 13 px (menu): below the 16 px at which iOS Safari zooms the page on focus (platform rule; not tested on a device here) | 15 px and 13 px |
| Landing page search | bare line, input 207x44 px at (60,450), 17 px; placeholder cut to "White BMW X5 with M S" | bare line, input 621x26 px at (380,394), 22 px |
| Car page search | only the menu's mini search (off-canvas at x=-253) | sidebar mini search, 131x33 px, 13 px |
| Placeholders across the site | four different strings: "Model or VIN", "Make, model, keyword, or VIN", "Accord with sunroof under $30k · M Sport · VIN…", "White BMW X5 with M Sport under $70k…" | |

Two checks that the prototypes are fair copies of the live pages. The 19 static files I compared (all 15 stylesheets, `listings.js`,
`main.js`, `sidebar.js`, `listings/chips.js`) are byte-identical live and in the repository (19/19, C2). A local render of the saved live
HTML with the repository's static files is pixel-identical to the live capture at both widths: SHA-256 17065c07a131 (1440) and
d95b1153d403 (390) for listings, 7955d740f996 (1440) and fca1642cc0fb (390) for the car page.

### 2.2 What the screen shows after a query

- Desktop: the line under the input reads "BMW X5 · Under $70,000 · Exterior: white" (`listings.js:95-175`, `fillParseChips`), and the
  chip row says "BMW × White × 92694 × 25 mi radius × **Under $75k** × white bmw x5 under 70k ×". The Price select snaps $70,000 up to
  the next preset, $75k (`main.js:1868-1874`, `setScalarSelect`), so one screen carries two prices.
- Phone: only the chip row is visible; the typed words are in the closed sheet.
- Rows the query set are marked with a cyan outline (`02-listings.css:82-95`, #00bde3), a color outside the site's navy and champagne.

### 2.3 What the screen shows when nothing matches (C4, C11)

For "red tesla model y under 30k" within 25 mi of 92694 (live: 0 cars), both paths end in the same state: the count line "0 vehicles near
92694", a row of chips (7 live: "Tesla × Model Y × Red × 92694 × 25 mi radius × Under $35k × red tesla model y under 30k ×", where the $30k became $35k; 6 in the replay),
a "View dealers on map near 92694" link, and a blank page. The `#empty-state` card ("No vehicles matched your filters"),
`#listings-zero-hint` and `#listings-geo-hint` (the message "No Tesla within 25 mi of 92694. Try a larger radius or different makes.",
`main.js:1431-1433`) were all hidden when measured, and `#listings-geo-hint` lives in the filter column (`listings.html:79`), out of
sight on a phone.
- URL path (what the landing page and the sidebar search produce): measured live.
- Typed path: replayed offline, because it ends in a POST; the saved live HTML, the repository's static files, the saved live area
  payload and canned search answers (the JSON the server code returns for a query with no matches, `listings_api.py:545-620`) were
  used, so the page code is the live code.

### 2.4 Screenshots of today

Phone 390:
```
scratchpad/web_shots/sp-today-listings-390-viewport.png        first screen
scratchpad/web_shots/sp-today-listings-390-full.png            full page
scratchpad/web_shots/sp-today-listings-390-menu.png            menu open: "Find cars"
scratchpad/web_shots/sp-today-listings-390-filters-drawer.png  Filters sheet: where the smart search lives
scratchpad/web_shots/sp-today-listings-390-drawer-query.png    query typed, no ZIP
scratchpad/web_shots/sp-today-listings-390-drawer-results.png  query typed, ZIP set
scratchpad/web_shots/sp-today-listings-390-results.png         results after the query
scratchpad/web_shots/sp-today-zero-390.png                     nothing matches (URL path)
scratchpad/web_shots/sp-today-zero-typed-390.png               nothing matches (typed path, replay)
scratchpad/web_shots/sp-today-zero-typed-390-sheet.png         same, Filters sheet open
scratchpad/web_shots/sp-today-landing-390-viewport.png         landing
scratchpad/web_shots/sp-today-landing-390-full.png
scratchpad/web_shots/sp-today-car-390-viewport.png             car page
scratchpad/web_shots/sp-today-local-390.png                    local render of the live HTML (parity check)
```
Desktop 1440:
```
scratchpad/web_shots/sp-today-listings-1440-viewport.png       first screen
scratchpad/web_shots/sp-today-listings-1440-full.png
scratchpad/web_shots/sp-today-listings-1440-query-nozip.png    query typed, no ZIP
scratchpad/web_shots/sp-today-listings-1440-area-loaded.png    ZIP set, area loaded
scratchpad/web_shots/sp-today-listings-1440-results.png        results after the query
scratchpad/web_shots/sp-today-listings-1440-results-full.png
scratchpad/web_shots/sp-today-zero-1440.png                    nothing matches (URL path)
scratchpad/web_shots/sp-today-zero-typed-1440.png              nothing matches (typed path, replay)
scratchpad/web_shots/sp-today-landing-1440-viewport.png        landing
scratchpad/web_shots/sp-today-landing-1440-full.png
scratchpad/web_shots/sp-today-car-1440-viewport.png            car page
scratchpad/web_shots/sp-today-local-1440.png                   local render of the live HTML (parity check)
```
Measurements behind section 2.1: `scratchpad/web_shots/sp-today-capture-log.json` (copy in
`docs/web_speed_search_2026_11/search_promotion_raw/pass3/out/`).

---

## 3. What promoting search would amplify (measured)

### 3.1 The showcased examples return nothing (C5, live, ZIP 92694 / 25 mi)

| Shown where | Query | What the live parser kept | Live cars |
|---|---|---|---|
| top-bar placeholder `_nav_search.html:12` (rendered by `_nav_marketing.html`, which `landing.html` includes, hidden there, and `register.html`) | Accord with sunroof under $30k | max price, packages "sunroof", trim "accord"; **no make or model** | **0** |
| landing placeholder `landing.html:28` | White BMW X5 with M Sport under $70k | white, BMW, max price, packages "m sport", trim "sport"; **model X5 lost** | **0** |
| landing sample `landing.html:56` | Something reliable for a first car, automatic, under $20k, low miles if possible. | body style "Car", max price, packages and trim "automatic" | **0** |
| listings hint, screen readers only `listings.html:60` | white BMW X5 under $70k | white, BMW, X5, max price | 20 |

The parse call per query is `GET /api/search/smart/parse?query=...&zip_code=92694&radius=25`, the cars come from
`GET /listings?q=...&zip_code=92694&radius=25` (the length of the server-rendered grid, capped at 200). Locally, the same parser drops the model whenever a model is named
without its make and a trim or package phrase follows ("Accord with sunroof", "RAV4 with sunroof", "X5 M Sport"); "Honda Accord with
sunroof" keeps it (C8, `parse_probe2.json`). Fixing the parser belongs to the search-quality work. For promotion the rule is simple:
**never show a query that has not been parsed and counted in the shopper's area** (P0-3).

`listings.js:400-413` already holds a rotating-examples feature that never runs: it looks for `.smart-search-hint__text`, which no
template or script produces (`grep -rn smart-search-hint__text frontend backend` finds only the lookup). Two of its four examples are
in the table above.

### 3.2 Every keystroke is a full search (C3, C11)

Typing "white bmw x5 under 70k" (22 characters), counting the requests the page starts:

| Where | Key delay | Parse GET | Search POST | Other |
|---|---|---|---|---|
| live JS, network blocked (C3) | 150 ms, desktop | 22 | 22 | 1 POST listings-geo, 1 GET area payload |
| live JS, network blocked (C3) | 90 ms, desktop | 22 | 22 | 1 POST listings-geo |
| live JS, CPU x4, phone (C3) | 150 ms | 22 | 22 | 2 long tasks, 109 ms in total (max 55 ms) |
| offline replay (C11) | 90, 150, 300 ms; 150 ms with CPU x4 | 22 | 22 | |
| sandbox fix V2 (C11) | 90, 150, 300 ms | 5 | 1 | |

- The parse request goes out on every key (`listings.js:374-377`); the search POST follows after a 50 ms debounce
  (`listings.js:13`, `:379`). Typing speed does not matter below about 500 ms per key.
- Both endpoints share one `smart:{ip}` bucket of 90 events per minute (`backend/routes/listings_api.py:532` and `:548`,
  `backend/config.py:55`). One typed query uses 44, so a third inside a minute exceeds 90 (arithmetic; I did not trigger a live 429).
- Each POST runs the full search and writes a `search_events` row (`listings_api.py:595`); for a signed-in user it also tries a
  `user_search_history` row (`:606`).
- Local cost of the pieces (read-only, 20-run medians): parse 3.1 to 6.0 ms for make, model and price queries; 74 to 192 ms when trim,
  package or nonsense words are present ("Accord with sunroof under $30k" 86.8 ms, "Zzzz" 114.5 ms, "cheap reliable commuter" 123.2
  ms, "Model or VIN", the sidebar's own placeholder, 191.5 ms); the first call loads the vocabulary in 1,826 ms; SQL per search 75 to 516
  ms across the 31 of 34 probe queries that reach SQL (C8).
- Of the 22 prefixes, "w", "wh", "whi" and "whit" do not parse. The API answers those `no_parse_match` with 0 results and logs
  `result_count = 0` (4 of 22; for "rav4 hybrid under 35k", 3 of 21).

### 3.3 The first keystroke starts the full-area download (C6)

`listings.js:339-345` calls `__DS_ensureListingsCarsLoaded()` on the first typed character. For 92694 / 25 mi the live
`GET /api/listings/cars?zip=92694&radius=25` returned **17,109,857 bytes decoded, 1,633,216 bytes on the wire (gzip)**, 15,457 cars,
in 1,118 ms. A search-first page should draw its first results from the search response and load the area payload later (on the first filter
interaction, or at idle). The speed reports own the payload itself.

### 3.4 Results are capped without notice (C5, C8)

- `POST /api/search/smart` calls `search_cars` with its default `SEARCH_CARS_DEFAULT_LIMIT = 500`
  (`backend/db/repositories/search_repo.py:56`; `hybrid_search.py:776` passes no limit).
- The server-rendered `GET /listings?q=` grid is capped at `LISTINGS_SEARCH_GRID_MAX = 200` (`backend/listings/routes.py:31`).
- Live "electric SUV under $45k" returned exactly 200 cars; the local count is 385. "Tacoma 4x4 under 40k miles" is 500 locally.
- The JavaScript has no notice about result caps (`grep` for capped, truncated and 500+ in `listings.js`, `main.js` and `listings/*.js` finds only an unrelated backoff comment, `zip_state.js:59`).

Consequence for promotion: a count shown next to an example must be below the cap and must match what the click shows. "On the lot"
rows therefore apply the Make and Model **filters** to the area payload (uncapped) instead of running a text search; "Camry" has 823
cars in the live area, which a text search would show as 500.

### 3.5 Today's logging cannot measure any of this (C9)

`search_events` lives in `users.db` (SQLite; SQLCipher in production) in 1.5.8 and in the staged tip
(`backend/db/search_analytics_db.py:20-53`; writers `backend/routes/listings_api.py:595` and `backend/listings/routes.py:243`).
`user_search_history` is in the inventory Postgres (`backend/db/inventory_pg.py:571-586`, writer
`backend/db/repositories/search_history_repo.py:99`).

- **Prefix rows.** In the local `backend/users.db` (developer traffic, not shoppers; opened read-only) one 3-second burst, rows 2 to
  23 ("h", "hel", "help", "help me f"...), wrote 22 rows.
- **Unstable session key.** Those 22 rows carry **7 different `session_key` values**: `analytics_session_key()`
  (`search_analytics_db.py:95-104`) mints a key inside the Flask session on the first API call, and an aborted in-flight POST never
  delivers its Set-Cookie. "Sessions that searched" over-counts.
- **No denominator and no filter events.** Sessions that never search write nothing; filter clicks write nothing (the grid filters
  client-side); `/listings` GETs log only when `q` or a package filter and a ZIP are present (`listings/routes.py:220-258`).
- **Prefix pollution for signed-in users.** `user_search_history` dedupes only identical filters within 10 minutes
  (`search_history_repo.py:22`, `:130-135`).
- **Size of the distortion.** The settled-search query (section 6) turns the 99 local raw rows into 76 settled searches and moves the
  zero-result share from 73.7% to 65.8%; the 22-row burst collapses only to 7, because of the 7 minted keys.

---

## 4. The options

### 4.0 Rules, base, and how the prototypes were checked

Owner rules honoured: no rounded white boxes or bordered cards as containers, no pill badges, no icon-title-blurb grids, no stat tiles,
no marquee, no boxed floating search card (a bare inline line), no decorative scanline animation, not the hero-marquee-stats-features
skeleton. Hairlines, spacing and type only. The line is the landing page's own bare line (`.lps-search*`, `12-viewers.css:233-377`)
with its focus underline (`--wood-trim`), re-used as is.

**Tokens.** `proto-common.css` is loaded after the site's 15 real stylesheets, uses 57 `var(--...)` references (`--navy`, `--text`,
`--text-muted`, `--text-faint`, `--border-light`, `--border-medium`, `--bg-page`, `--wood-trim`, `--font-ui`) and one literal color,
`#a68968`, the landing's own champagne bronze (`.lps-kicker`, `12-viewers.css:401-407`).

**Base shared by A, B and C.**
1. The line is the first thing: beside the 44 px menu button in the top row on a phone (17 px type, so iOS does not zoom), above the
   page on desktop.
2. "Near [ZIP] Within [25 mi]" under it. This is the one area control; the filter column's ZIP and radius fields are hidden at 961 px
   and up and stay in the DOM. The Dealerships picker stays visible when the page script shows it (proved by `opt1_results_picker`).
3. "Filters" as a text link on phones (it keeps `id="listings-filters-open"`, so `main.js` needs no change).
4. Real examples rotate in the empty line (3.2 s each, frozen on focus, off under reduced motion) and `/` focuses it from anywhere.
5. The ZIP banner and the start-prompt ZIP form give way to the line's Near field; the guest banner moves into the start block (the
   `banner-above` pages keep it where the template puts it, to show the collision with the menu button).

**Checks run on the prototypes (C10).**

| Check | Result |
|---|---|
| Computed-style audit of every new region (boxed, pill, rounded, shadow, background image, fixed or absolute, animation), 13 prototype pages at 390 and 1440 (26 runs; the 4 runs of today's pages add nothing) | 0 boxed, 0 pill, 0 rounded, 0 shadow, 0 background image. 2 `filled` and 2 `animated` hits are the demo's 2 px blinking caret (B); 2 `floating` hits are the car-page line placed in the row that is empty today |
| Filter panel, static count in today's live HTML and 7 prototype pages | identical in all: 4 selects, 1,496 checkboxes, 13 accordion sections, 17 buttons. Only the search text input leaves the column |
| Overflow and edge clipping at 320, 360, 390, 414, 768, 1024, 1280, 1440 px, 9 pages | 0 of 72 combinations overflow or clip |
| First screen, phone start state | search fields 0 to 1; boxed controls 7 to 0; taps under 44 px 16 to 0; fields under 16 px 4 to 0 |
| First screen, desktop start state | search fields 2 to 1; ZIP fields 4 to 1; boxed controls 12 to 3 (the 3 left are the column's Price, Mileage, Condition) |
| Behaviour (real keystrokes): rotating placeholder, `/`, typing demo, list keys | see each option below |

The controls under 44 px left on phone results pages (`small_targets.py`): in A, the site's own chips and "Clear all" (24 to 28 px high, unchanged
from today); in B, the six words of the sentence (31 to 32 px high, above the 24 px minimum of WCAG 2.2 target size, below the 44 px used elsewhere); in C,
one link 43.6 px wide.

### 4.1 Option A: Search line first

**What the shopper sees.** Phone: the line in the top row beside the menu button; under it "Near 92694 · Within 25 mi · Filters"; a
"Try" list of five real queries with counts; "On the lot within 25 mi of 92694", the eight most listed models with counts; "Show all
15,457 cars within 25 mi". Desktop: the same, with the line across the top. Before a search no car is shown (owner rule of 2026-09-28).
After a search: "20 cars", the chips and "Clear all" in one summary line, then the cards.

Live counts in the screenshots (ZIP 92694 / 25 mi, C5, C6):
- Try: RAV4 hybrid under $35k 53 · Camry hybrid under $28k 34 · used Tesla Model 3 under $30k 24 · Mercedes GLE under 30k miles 48 ·
  Ford F-150 under $45k 52.
- On the lot: Camry 823, Tacoma 756, RAV4 561, Tundra 390, Corolla 317, F-150 258, Corolla Cross 255, Sienna 249.
- The demo query "white bmw x5 under 70k": 20 cars. The cards are the live rows for that query.

**Two desktop layouts of the same line**
- **A, in the results column** (`opt1_*`): line at x=672 to 1273 (601 px, 22 px type) at the top of the results column.
- **A-wide, above both columns** (`opt1_*_wide`): the line is the first grid item of `.listings-layout` and spans both columns:
  x=320 to 1273 (953 px, 26 px type). Recommended.

| (C10) | Today | A, column | A-wide |
|---|---|---|---|
| line, 1440 | 320x49 px, 15 px, inside the filter column | 601 px wide at y=35 | 953 px wide at y=32 |
| line, 1024 | 320x49 px | **208 px** wide | **553 px** wide |
| "Filters" heading y, 1440 | 88 | 49 | 237 |
| accordion sections on the first screen, 1440 | 7 | 12 | 9 |
| first car after the search, 1440 | 241 | 250 | 266 |
| first car after the search, phone | 479 | 336 | 336 (the page has one column, A and A-wide are the same) |

A-wide puts the search first in reading order and gives the field a usable width at laptop sizes. It costs 188 px of height before the
filter column starts, yet the column still shows more sections on the first screen than today (9 against 7), because its own search and
ZIP rows moved up into the line.

**Screenshots.** Phone 390:
```
scratchpad/web_shots/sp-opt1-start-390.png                  start, ZIP known
scratchpad/web_shots/sp-opt1-start-390-full.png
scratchpad/web_shots/sp-opt1-first-visit-390.png            no ZIP yet ("use my location")
scratchpad/web_shots/sp-opt1-results-390.png                after the search
scratchpad/web_shots/sp-opt1-start-banner-above-390.png     guest banner left above (menu button covers its first words, as live)
scratchpad/web_shots/sp-opt1-results-banner-above-390.png
scratchpad/web_shots/sp-opt1-car-390.png                    the line on the car page
scratchpad/web_shots/sp-car-today-local-390.png             car page today
scratchpad/web_shots/sp-opt1-start-wide-390.png             A-wide on a phone: the same layout as A (first car y=336 in both)
scratchpad/web_shots/sp-opt1-results-wide-390.png
scratchpad/web_shots/sp-opt1-results-picker-390.png
```
Desktop 1440 and 1024 (and the 920 px band, where there is no menu button and the Filters link shows):
```
scratchpad/web_shots/sp-opt1-start-1440.png                 A, in the column
scratchpad/web_shots/sp-opt1-start-wide-1440.png            A-wide
scratchpad/web_shots/sp-opt1-start-wide-1440-full.png
scratchpad/web_shots/sp-opt1-start-wide-1024.png            A-wide at 1024
scratchpad/web_shots/sp-opt1-first-visit-1440.png
scratchpad/web_shots/sp-opt1-results-1440.png
scratchpad/web_shots/sp-opt1-results-wide-1440.png
scratchpad/web_shots/sp-opt1-results-picker-1440.png        Dealerships picker shown in the column
scratchpad/web_shots/sp-opt1-start-banner-above-1440.png
scratchpad/web_shots/sp-opt1-results-banner-above-1440.png
scratchpad/web_shots/sp-opt1-car-1440.png
scratchpad/web_shots/sp-car-today-local-1440.png
scratchpad/web_shots/sp-opt1-start-920.png
scratchpad/web_shots/sp-opt1-results-920.png
```
**Prototype pages.** `opt1_start.html`, `opt1_start_wide.html`, `opt1_first_visit.html`, `opt1_results.html`,
`opt1_results_wide.html`, `opt1_results_picker.html`, `opt1_start_banner_above.html`, `opt1_results_banner_above.html`, `opt1_car.html`.

**Behaviour (C10, `behaviors.json`).** The empty line's placeholder moved through the examples at 3.7, 6.9, 10.1, 13.3 and 16.5 s
("RAV4 hybrid under $35k", "Camry hybrid under $28k", "used Tesla Model 3 under $30k", "Mercedes GLE under 30k miles", "Ford F-150
under $45k"), stayed put 4 s after focus, and did not rotate in 7 s with reduced motion on. `/` moved focus to the line on the listings
page (desktop and phone widths) and on the car page, did not type a slash into it, and left focus alone inside the ZIP field.

**Files that change (v1.5.8 line numbers)**

| File | Change |
|---|---|
| `frontend/templates/_search_line.html` (new) | The line: `role="search"`, `#smart-search-input` (same id, so `listings.js` and the inline dealer-picker script at `listings.html:661,825` need no rebinding), Near and Within (`name="zip_code"`, `name="radius"` inside `#search-form`, so `main.js:848` binds them at init), the Filters link with `id="listings-filters-open"` and `#listings-filters-mobile-count`, the Try list. Markup: `search_promotion_raw/pass3/scripts/build_protos.py`, function `search_line()` |
| `frontend/templates/listings.html` | Include the partial as the first child of `.listings-layout` (before L52, A-wide) or at the top of `#dash-content` (before L362, A). L55-58 retitle to "Filters / Refine any search". L60-62 hint text moves into the partial. L64-75 remove `.filter-search-row` (input L67-72, `#smart-parse-chips` L73) from the column. L77-126 `#listings-geo-bar`: CSS hides ZIP and Radius at 961 px and up; the Dealerships picker (L101-124) stays. L362-379 delete the ZIP banner (null-safe: `main.js:769-820`). L381-386 the toolbar Filters button becomes the link in the line. L435-453 start prompt: body becomes the Try list and the lot list, **keep `id="listings-start-prompt"`** (`backend/tests/test_listings_scoped_grid.py:327` asserts it). L32-44 guest banner moves into the start block |
| `frontend/templates/_nav_sidebar_guest.html` L10-12, `_nav_sidebar.html` L105-109 | Skip `_nav_search_sidebar.html` when `nav_active == 'listings'` and on the car page |
| `frontend/templates/car.html` L159-161 | Include the line beside `.vdp-back` (the row that is empty today; prototype `opt1_car.html`; 0 px of new height at 390 and 1440, `scratchpad/web_shots/sp-car-measure.json`) |
| `frontend/templates/_nav_search.html` L12, `landing.html` L28 and L56 | Placeholder and sample come from the verified-example source (P0-3) |
| `frontend/static/css/12-viewers.css` | Keep `.lps-search*`, `.lps-mini*` (L233-377), `.lps-kicker` (L401), responsive L733-780. Add the new `.srch*` rules after L377 (prototype `search_promotion_raw/pass3/prototype/proto-common.css`, about 250 lines for the base, Try, lot, phone and car rules, and 12 for the wide layout). No new stylesheet: all 15 already load on every page (`_head_css.html`), so no extra render-blocking request. Coordinate with F5 of `docs/structural_eval_2026_11_04/frontend-css-templates.md`, which moves `lps-*` (12-viewers L142-812) into a landing-only sheet: the line rules must stay where /listings and the car page load them |
| `frontend/static/css/02-listings.css` | Delete `.smart-search-*`, `.filter-search-row`, `.smart-search-hint*` (L1-66, L566-580, L809-838, L910-996, L1043-1119) and `.listings-start-prompt__zip/__geo/__go` (L1437-1471) with the markup they style. `.smart-parse-*` (L68-100) stay in A. Add the grid rule next to L482-497: `grid-template-areas: "search search" "sidebar main"` from 961 px, `"search" "main"` below |
| `frontend/static/css/11-overrides.css` L365-381 | Delete `.smart-search-input--carmax` |
| `frontend/static/css/09-components.css` L422-444, L884-886 | Delete the `.listings-filters-mobile-btn` and `-count` rules (today's rounded, bordered Filters button). The sheet itself (L892-922) is unchanged and pinned by `backend/tests/test_visual_review_mobile_history_recalls.py` |
| `frontend/static/listings.js` | L13 and L370-379: the debounce of P0-1. Delete the dead rotation L400-413. Add `/` and the rotating placeholder (about 40 lines; prototype `proto.js`, functions `rotatePlaceholder` and `slash`) |
| `frontend/static/main.js` | L609-639 (`showListingsStartPrompt`): fill or toggle the Try and lot block instead of the ZIP text. Nothing else if the new link keeps its id (L896, L2359-2370) |
| `frontend/static/listings/chips.js` L28-29 | Stop emitting the ZIP and radius chips; the line shows the area |
| `backend/listings/routes.py` | Render the Try and lot lists from a cached per-area source (P0-3); mint the analytics key and write the `view` row on render (P0-2) |

Effort M, risk medium: the ZIP gate lives inside the 2,771-line `main.js` closure (F1 of the 2026-11-04 JS report). Gate: screenshot
diff of `/listings` at 390 and 1440 before and after, plus `backend/tests/js/listings_golden.test.js`.

### 4.2 Option B: Read it back

**Idea.** A, plus two pieces.

1. **Before a search, a query-to-filters demo.** A verified query types itself in large type, and each word lights its row on the
   right: Color white · Make BMW · Model X5 · Price under $70,000 · Near 92694 · 25 mi, then "20 cars match →". It cycles through three
   queries the live parser handled (white BMW X5 under $70k, RAV4 hybrid under $35k, used Tesla Model 3 under $30k), stops when the
   shopper focuses the line, shows its finished first frame under `prefers-reduced-motion`, and shows no cars. The landing page can
   run the same demo in place of its "Ask it like you'd say it" sample (`landing.html:51-64`), which returns 0 today.
2. **After a search, one sentence replaces both chip rows:** "20 cars: <u>white</u> <u>BMW X5</u> × under <u>$70,000 ▾</u> within
   <u>25 mi</u> of <u>92694</u> · Clear". Each underlined word edits its filter in place; the price is an inline select. The exact parsed
   price gets its own option, so the sentence and the column agree (today $70,000 becomes $75k). Column rows the query set carry the
   words "from search" in small champagne capitals, in place of today's cyan outline.

**Screenshots.**
```
scratchpad/web_shots/sp-opt2-start-390.png                    demo, frozen mid-sentence
scratchpad/web_shots/sp-opt2-start-390-full.png
scratchpad/web_shots/sp-opt2-start-1440.png
scratchpad/web_shots/sp-opt2-start-reduced-motion-390.png     finished frame
scratchpad/web_shots/sp-opt2-start-reduced-motion-1440.png
scratchpad/web_shots/sp-opt2-demo-frame-05-1440.png           real animation, 5 characters typed
scratchpad/web_shots/sp-opt2-demo-frame-12-1440.png           12
scratchpad/web_shots/sp-opt2-demo-frame-18-1440.png           18
scratchpad/web_shots/sp-opt2-demo-frame-23-1440.png           23 (price row lit, count visible)
scratchpad/web_shots/sp-opt2-demo-q2-1440.png                 second query, finished
scratchpad/web_shots/sp-opt2-demo-q3-1440.png                 third query, finished
scratchpad/web_shots/sp-opt2-results-390.png
scratchpad/web_shots/sp-opt2-results-390-full.png
scratchpad/web_shots/sp-opt2-results-1440.png
```
**Prototype pages.** `opt2_start.html` (open with `#animate` to watch the demo run), `opt2_results.html`.

**Behaviour (C10).** With real timing the three queries typed in turn. The rows lit as their words landed: at 5 characters "Color";
at 12 "Color, Make, Model"; at 23 "Color, Make, Model, Price", and the count appeared with the last word. The second query's rows read
"Make Toyota, Model RAV4, Fuel hybrid, Price under $35,000, Near 92694 · 25 mi, 53 cars match →"; the third "Condition pre-owned,
Make Tesla, Model Model 3, Price under $30,000, 24 cars match →" (the live parse of each). Focusing the line stopped the demo (the text
did not change in the next 3 s). With reduced motion the finished first frame showed at once, every row lit.

**Measured.** In the results state the Near field gives way to the sentence (0 ZIP fields; the area is the words "25 mi" and "92694").
First car after the search: phone y=377, desktop y=255 (today 479 and 241).

**Files that change (on top of A)**

| File | Change |
|---|---|
| `frontend/static/listings.js` | Replace `fillParseChips` (L95-175) with `renderReadback(filters)`, which builds the word buttons. `applyHighlights` (L74-93) stays and feeds the "from search" words |
| `frontend/static/main.js` | In `__DS_applySmartFilters` (L1819) and `setScalarSelect` (L1868-1874) insert an exact option when the parsed value has no bracket instead of snapping $70k to $75k |
| `frontend/templates/listings.html` | Remove the chip row `#listings-active-chips-wrap` (L424-427), its "Clear all" moves into the sentence; remove `#smart-parse-chips` (L73) |
| `frontend/static/search_demo.js` (new) | The typing demo (prototype `proto.js`, function `demo()`, about 40 lines) |
| `frontend/templates/landing.html` L51-64 | The demo replaces the dialogue sample |
| `frontend/static/css/12-viewers.css` | `.srch-demo*`, `.tok*`; the "from search" rule; delete the cyan `.smart-parse-active` rules (`02-listings.css:82-95`) and `.smart-parse-*` (L68-100) |
| `backend/routes/listings_api.py` | The examples source returns the parsed rows and the count per example, so the demo shows the parser's real output |

Effort M on top of A; the word editing is the larger half. Risk medium: the sentence must stay in sync with the column, and today two
code paths (chips and parse line) do that.

### 4.3 Option C: A search that remembers and recovers

**Idea.** A, plus three pieces.

1. **On focus, or `/` from any page, a suggestion list opens in place under the line.** It pushes the page down; it is not a floating
   box. Hairline-separated rows: **Recent** (this device for guests, `localStorage` `ds.recentSearches`, at most 10, wrapped in
   try/catch; signed-in users from `GET /api/profile/search-history?limit=20`, `listings_api.py:850`); **Searched near 92694, last 7
   days** (settled searches from `search_events` by area; only queries typed by at least 3 different sessions and verified to return
   at least 5 cars); **Or go straight to a filter** (Price · Mileage · Make · Model · Body style · Fuel · Drivetrain · Color, each
   opening that filter section). Keys: Up and Down choose, Enter runs, Esc closes.
2. **A live count** beside the typed query on desktop ("0 cars"), from the parse plus a count-only query. On a phone there is no room
   beside the 44 px arrow; the count moves to the zero-result headline.
3. **Zero-result recovery.** "Nothing matches all of 'red Tesla Model Y under $30k' within 25 mi of 92694. Drop one part:" with real
   counts (live): Any color 15 cars; Up to $36,000 1 car; Within 100 mi 1 car; then the first three of the 15 any-color cars. Today
   the same query ends on a blank page (2.3). A query the parser cannot act on ("help me find a") gets the list of what the search
   reads plus the verified examples.

Relaxation counts are cheap: each was one `search_cars` count of 67 to 120 ms locally across 12 relaxations (C8, `proto_data.json`).

**Screenshots.**
```
scratchpad/web_shots/sp-opt3-focus-390.png          suggestion list on focus
scratchpad/web_shots/sp-opt3-focus-390-full.png
scratchpad/web_shots/sp-opt3-focus-1440.png
scratchpad/web_shots/sp-opt3-zero-390.png           recovery
scratchpad/web_shots/sp-opt3-zero-390-full.png
scratchpad/web_shots/sp-opt3-zero-1440.png
scratchpad/web_shots/sp-today-zero-1440.png         today, same query (for comparison)
scratchpad/web_shots/sp-today-zero-390.png
```
**Prototype pages.** `opt3_focus.html`, `opt3_zero.html`. In the prototype the "searched near you" rows are verified examples with live
counts; the real list needs P0-2's settled logging first.

**Behaviour (C10).** With the line focused, Down moved the active row through "rav4 hybrid under 35k", "camry hybrid under 28k",
"Ford F-150 under $45k", "Mercedes GLE under 30k miles"; Up moved back; Esc hid the list.

**Measured.** In the zero state the first preview car is at phone y=427 and desktop y=413, after the three recovery rows.

**Files that change (on top of A)**

| File | Change |
|---|---|
| `frontend/static/search_suggest.js` (new) | Focus list, keys, `localStorage` recents (try/catch), the fetch of the "searched near" list (prototype `proto.js`, function `panelKeys`, about 20 lines for the keys) |
| `frontend/templates/_search_line.html` | Markup for the list container and the count span |
| `frontend/templates/listings.html` L463-473 | `#empty-state` becomes the recovery block |
| `frontend/static/listings.js` | Handle `empty_message` and `search_meta.mode` (`listings_api.py:607-619`); render the relaxation rows. `main.js:1431-1451` stops writing the hint into the filter column |
| `backend/routes/listings_api.py` | New `GET /api/search/relax?q=&zip=&radius=` (parse, then one count per dropped constraint: radius x2 and x4, color, price +20%, mileage +50%, condition, trim words; cached per filters and area). New `GET /api/search/popular?zip=&radius=` (top settled queries in that area over 7 days, at least 3 sessions each) |
| `backend/utils/hybrid_search.py` | A `relaxations(filters, geo)` helper next to `query_is_actionable` (L575) |
| `frontend/static/css/12-viewers.css` | `.srch-panel*`, `.srch-rows*`, `.srch-zero*` (prototype `proto-common.css`) |

Effort M on top of A. Risk low to medium: the two new endpoints are read-only and cacheable.

### 4.4 Side by side

| | A | B | C |
|---|---|---|---|
| Answers | where do I type, what can I type | what did it understand | what do I do when it finds nothing |
| Phone first screen | line, area, Try, lot | line, area, demo | line, area, list on focus |
| After a search | summary line with chips | one editable sentence | summary line, recovery when empty |
| New files | `_search_line.html` | `search_demo.js` | `search_suggest.js`, 2 endpoints |
| Needs the P0 items | P0-1, P0-3 | P0-1, P0-3 | P0-1, P0-2, P0-3 |
| Effort / risk | M / medium | M on A / medium | M on A / low to medium |

### 4.5 Guest banner

The live banner ("Save cars you like - create a free account. Want advanced features? Go Premium.") sits above the page and the fixed menu button
covers its first words on a phone (`scratchpad/web_shots/sp-today-listings-390-viewport.png`; `phone-spacing.md` PS-03). The prototypes move it into the
start block, where it ends the list and leaves with the first search; the `banner-above` pages keep it in place so the owner can
compare. First car after a phone search: 336 (moved) against 417 (kept above). Whether the banner moves is a product choice
(conversion); it is not needed for search promotion.

### 4.6 Where else the line goes

- Car page: the bare line in the empty "Back to results" row (A, `opt1_car`).
- Landing: it already has the line; only its placeholder and sample change.
- Other pages with the sidebar keep the "Find cars" mini search, with the same verified placeholder.

### 4.7 Opening the prototypes

The pages fetch their data over HTTP, so open them through a local server:
```
cd scratchpad/search_proto
python3 -m http.server 8777        # then open http://127.0.0.1:8777/index.html
```
`opt2_start.html#animate` runs the typing demo. The raw scripts and results are in
`docs/web_speed_search_2026_11/search_promotion_raw/pass3/` (`README.txt`).

---

## 5. Shared prerequisites (needed by every option)

**P0-1. Stop searching on every key** (`listings.js:13`, `:370-379`). Sandbox experiment (C11, `fix_experiment_typing.py`, zero live
requests): the repository's `listings.js` served with in-memory patches, "white bmw x5 under 70k" typed at four speeds. Requests the
page started (parse + search = limiter events):

| Variant | 90 ms/key | 150 ms/key | 300 ms/key | 500 ms/key |
|---|---|---|---|---|
| today (parse on every key, search 50 ms after the last key) | 22+22 = 44 | 44 | 44 | 44 |
| V1: parse 150 ms after the last key, search 450 ms | 1+1 = 2 | 21+1 = 22 | 22+1 = 23 | 22+22 = 44 |
| **V2: parse when the last character is a space, otherwise 400 ms after the last key; search 450 ms after the last key** | 5+1 = 6 | 6 | 6 | 22+22 = 44 |

V2 is the one to build: 6 events per typed query instead of 44, at every normal typing speed, so 15 queries a minute fit the limiter
instead of 2 (90 / 6 and 90 / 44, arithmetic on the measured counts). At 500 ms per key (2 characters a second) every variant falls back to today's behavior; a parse cache keyed on the
normalized query (and on `_inventory_db_cache_key()`) removes the repeats. Keep the instant client token preview. Send the search on
Enter at once. Effort S, risk low. Verify by rerunning `fix_experiment_typing.py` against the changed file.

**P0-2. Make sessions countable.**
- Call `analytics_session_key(session)` when `/listings` and `/` render, so the key exists before any request.
- Write a `source='view'` row once per session per day on render.
- Write `source='filter'` once per filter burst (about 2 s after the last change): `POST /api/search/event` with CSRF and the visible
  result count; a new route in `listings_api.py`.
- Write `source='rate_limited'` when `smart:{ip}` answers 429 (today it logs nothing).
- Put the origin in `filters_json` as `_origin` (`try`, `demo`, `recent`, `relax`, `typed`) and the layout in `_ui`.

No schema change: `source` is free text (`search_analytics_db.py:107-154`). Effort S to M.

**P0-3. Only verified examples.** One server function turns the area's inventory into candidate queries (top models, "{model} hybrid
under ${p75}", "used {model} under ${p50}", "{color} {model}"). Each candidate is parsed, must come back actionable with make or model
intact, and must count at least 5 and below 200 in the area (below the grid cap, 3.4). The result is cached per (ZIP3, radius) for 1
hour. The Try line, the placeholders, the demo, the landing and the lot list all use it. The lot list is a `GROUP BY make, model` over
the same area, not a text search. Measured cost: one count per candidate, 60 to 142 ms locally across the 10 candidate queries (C8). Example: "Ford Mustang
Mach-E under $40k" parses to model "Mustang Mach E" and returns 0 (local), so the verifier must reject it. Effort M.

**P0-4. Parser fixes** for "{model} with ...", "F-150 crew cab" (parses to model "E-150") and "near 92694" are owned by
`search-quality.md`. Until they land, P0-3 keeps broken phrasings out of sight.

---

## 6. Success metrics and how to measure them

**Definitions** (per session, using the stable key from P0-2). A "settled search" is a query the shopper kept: the same session does
not send a longer or shorter version of the same text within 2 s.

| Metric | Definition | Works today? | Needs |
|---|---|---|---|
| Search adoption | listings sessions with at least 1 settled search / listings sessions | numerator yes, denominator no | `view` rows (P0-2) |
| Filter-only sessions | sessions with a `filter` row and no settled search / listings sessions | no | `filter` rows (P0-2) |
| Neither | sessions with a view and no search or filter / listings sessions | no | `view` and `filter` rows |
| Zero-result rate | settled searches with `result_count = 0` / settled searches | yes (settled query below) | stable key makes it exact |
| Time to first search | median seconds from the `view` row to the first settled search in the same session | no | `view` rows |
| Example use (A, B) | settled searches with `_origin` in (`try`, `demo`) / settled searches | no | `_origin` |
| Recovery acceptance (C) | a zero-result settled search followed within 2 min by a settled search with `_origin = 'relax'` and `result_count > 0` | no | `_origin` |
| Signed-in history quality | query rows, filter-only rows and prefix rows in `user_search_history` | yes | nothing |
| Guardrail: limiter | count of `rate_limited` rows per day | no | `rate_limited` rows (P0-2) |

**Settled searches** (SQLite, `users.db` in 1.5.8 and 1.5.16; file `search_promotion_raw/pass3/scripts/settled_searches.sql`):

```sql
WITH s AS (
  SELECT id, session_key, user_id, source,
         lower(trim(query_text)) AS q, result_count, occurred_at,
         LEAD(occurred_at)              OVER w AS next_at,
         LEAD(lower(trim(query_text)))  OVER w AS next_q
  FROM search_events
  WHERE source IN ('smart_search', 'listings')
    AND query_text IS NOT NULL AND trim(query_text) <> ''
    AND occurred_at >= :since
  WINDOW w AS (PARTITION BY coalesce(session_key, 'u' || user_id, 'row' || id) ORDER BY id)
),
settled AS (
  SELECT * FROM s
  WHERE next_at IS NULL
     OR (julianday(next_at) - julianday(occurred_at)) * 86400.0 > 2.0
     OR NOT (substr(next_q, 1, length(q)) = q OR substr(q, 1, length(next_q)) = next_q)
)
SELECT (SELECT COUNT(*) FROM s) AS raw_query_rows, COUNT(*) AS settled_searches,
       SUM(result_count = 0) AS settled_zero,
       ROUND(100.0 * SUM(result_count = 0) / COUNT(*), 1) AS settled_zero_pct,
       (SELECT ROUND(100.0 * SUM(result_count = 0) / COUNT(*), 1) FROM s) AS raw_zero_pct,
       COUNT(DISTINCT session_key) AS sessions_with_settled_search
FROM settled;
```

On the local `backend/users.db` (developer traffic, opened `mode=ro`, C9): raw 99 rows, 76 settled searches, 50 of them with zero
results, zero share 65.8% (raw 73.7%), 75 session keys. Restricted to the 22-row burst: 22 raw, 7 settled, 7 session keys. The
Postgres version for after the users cutover (`settled_searches_pg.sql`, schema `migrations/V013__users.sql`) ran clean against the
local Postgres `search_events`, which is empty (0 rows).

**Adoption, filter-only, neither** (`adoption_sqlite.sql`, `adoption_pg.sql`). Both run today and return 0 sessions, because no `view` rows
exist yet:

```sql
WITH sess AS (
  SELECT session_key,
         MAX(source = 'view') AS viewed,
         MAX(source IN ('smart_search','listings') AND trim(coalesce(query_text,'')) <> '') AS searched,
         MAX(source = 'filter') AS filtered
  FROM search_events
  WHERE occurred_at >= :since AND session_key IS NOT NULL
  GROUP BY session_key)
SELECT SUM(viewed) AS listings_sessions,
       ROUND(100.0*SUM(viewed AND searched)/NULLIF(SUM(viewed),0),1) AS search_adoption_pct,
       ROUND(100.0*SUM(viewed AND filtered AND NOT searched)/NULLIF(SUM(viewed),0),1) AS filter_only_pct,
       ROUND(100.0*SUM(viewed AND NOT filtered AND NOT searched)/NULLIF(SUM(viewed),0),1) AS neither_pct
FROM sess;
```

**Signed-in history** (inventory Postgres, `user_history_pg.sql`):

```sql
WITH h AS (
  SELECT user_id, query_text, result_count, created_at::timestamptz AS at,
         lead(query_text) OVER (PARTITION BY user_id ORDER BY created_at, id) AS next_q,
         lead(created_at::timestamptz) OVER (PARTITION BY user_id ORDER BY created_at, id) AS next_at,
         (filters_json::jsonb ? 'q') AS has_q
  FROM user_search_history
  WHERE created_at::timestamptz >= now() - interval '90 days')
SELECT count(*) AS history_rows,
       count(*) FILTER (WHERE has_q) AS query_rows,
       count(*) FILTER (WHERE NOT has_q) AS filter_only_rows,
       count(*) FILTER (WHERE has_q AND next_q IS NOT NULL AND next_at - at < interval '3 seconds'
                        AND starts_with(lower(next_q), lower(query_text))) AS keystroke_prefix_rows,
       count(*) FILTER (WHERE has_q AND result_count = 0) AS zero_result_query_rows,
       count(DISTINCT user_id) AS users
FROM h;
```
Local result, read-only: 6 rows, 2 with a query, 4 filter-only, 0 prefix rows, 0 zero-result query rows, 2 users (test data). Two
caveats: the filter-only rows come only from full page loads that carry filter parameters (deep links, saved-search runs), never from
clicks in the column, and some are radius-only (`{"radius":"50"}`); and `usage_summary()` behind the site-admin hub counts raw rows
(`search_analytics_db.py:172-264`), so its "zero-result searches" includes every keystroke prefix.

**An outside denominator, not measured here.** The page loads the Cloudflare beacon (`static.cloudflareinsights.com/beacon.min.js`),
so Cloudflare Web Analytics may already count `/listings` visits. If so, it cross-checks the `view` rows. I could not read it.

**How to run the test.** Assign the layout by session: `crc32(analytics_session_key) % 2` picks the old page or the option, stored in the
Flask session and written into `_ui` on every event and every `view` row. Running both at the same time removes seasonality, which a
before-and-after comparison cannot. The cost: both layouts coexist in the CSS for the test (scope the new rules under
`body.search-first`).

**Sample size** (C12, normal approximation, two-sided, alpha 0.05, power 0.80; the baseline is unknown because no production data was
read): sessions per arm to detect +10 points: 199 from a 10% share, 294 from 20%, 356 from 30%, 388 from 40% and from 50%. Zero-result
share 30% to 20%: 294 settled searches per arm. A +5 point change from 30% needs 1,377 per arm; +15 points needs 163. Compare with
the weekly count of `view` rows once P0-2 ships.

**Decision rule (a starting point for the owner to set).**
1. Ship P0-1 and P0-2 alone, then record two weeks of baseline adoption, filter-only, neither, time to first search and zero-result rate.
2. Ship the option to half the sessions and record until each arm has the sample size above.
3. Call it a success when adoption rises by at least 10 points, "neither" falls, the settled zero-result rate does not rise, the
   `rate_limited` count stays near 0, and filter use inside search sessions does not drop by more than a third (the filters are meant
   to stay useful).

---

## 7. Where Redis is useful for this feature

Production has `REDIS_URL` support only as a readiness probe (`backend/config.py:96-98`, `backend/routes/health.py:34-48,109-117`;
`redis>=8.0.1` is in `requirements.txt:21`). One small `backend/utils/shared_cache.py` (get and set JSON with a TTL; Redis when
`REDIS_URL` is set, an in-process dict otherwise) covers every use below. `redis.md` owns the larger case.

| Use | Key and TTL | Needed with 1 worker? | Why Redis |
|---|---|---|---|
| Verified examples and the lot list per area (P0-3) | `srch:ex:{zip3}:{radius}`, 1 h | no, in-process works | shared across workers and survives a deploy, so the first visitor after a deploy does not pay the 5 to 10 counts |
| Parse cache (P0-1) | `srch:parse:{vocab_key}:{sha1(q)}`, 24 h | no | the 74 to 192 ms parses are paid once per fleet, not per process |
| Relaxation and live counts (C) | `srch:relax:{sha1(filters)}:{zip}:{radius}`, 10 min | no | repeated empty searches in one area share the answers |
| "Searched near you" (C) | `srch:pop:{zip3}`, 1 h | no | computed from `search_events`, which is SQLite today and not readable by other services |
| Rate limiter for `smart:{ip}` | INCR plus EXPIRE per window | no: the in-memory limiter is exact at 1 worker | needed as soon as workers exceed 1. At 1.5.8 the entrypoint falls back to a SQLite file when workers differ from 1 (`scripts/docker-entrypoint-web.sh:56-63`); at the staged tip that option is gone (N37.11, `ip_rate_limit.py`), so the ceiling becomes the limit times the worker count |

Not a Redis job here:
- **Recent searches.** `localStorage` for guests (a per-viewer convenience); the Postgres `user_search_history` for signed-in users.
- **`search_events` itself.** It is the analytics record and stays durable in the database.

---

## 8. Recommendation and order

1. **P0-1 (V2 debounce, parse cache), P0-2 (countable sessions) and P0-3 (verified examples)**, in one release. They are small, cut
   44 requests per typed query to 6, start the baseline, and stop the site from promoting empty examples. Fix the top-bar and landing
   placeholders and the landing sample with P0-3 at once: they cost nothing.
2. **Option A with the wide layout.** It is the change that gives a phone a search field (0 to 1), removes three of the four ZIP fields
   on desktop, and gives the field a usable width at laptop sizes.
3. **Option B's sentence and demo.** You asked for the typing demo; the sentence removes the two chip rows and the $70k and $75k
   disagreement.
4. **Option C's recovery** once two weeks of settled logging show how often searches come back empty; the suggestion list can follow,
   since `localStorage` recents are cheap.

Every CSS change goes through screenshots at 390 and 1440 px. `search_promotion_raw/pass3/scripts/shoot_protos.py` and
`audit_design.py` are a working start for the repo-side screenshot gate that F2 of the 2026-11-04 CSS report notes is missing.

Limits of this evaluation.
- The prototypes are static pages: no server, no real search. Their counts and cards are the live site's rows for these queries on
  2026-11-10; they will drift.
- The typed zero-result state is an offline replay with canned search answers (2.3); the URL path is live.
- iOS focus zoom is a platform rule; no iPhone was used.
- No production `search_events` or `user_search_history` data was read, so every baseline is unknown; the sample sizes assume a share.
- Local counts come from the scanner's database (the same queries counted 24, 58, 35, 22, 55 locally against 20, 53, 34, 24, 48 live);
  the report uses live counts wherever a number appears on a screen.

---

## 9. Commands, outputs and request accounting

All paths are under `.` unless noted. `SP3` is
`scratchpad/sp3`. Copies of every script
and every small output are in `docs/web_speed_search_2026_11/search_promotion_raw/pass3/` (`scripts/`, `out/`, `prototype/`). Files in
`search_promotion_raw/` outside `pass3/` belong to the superseded 2026-11-09 draft and are not cited here.

### 9.1 Commands

Run from the scratchpad folder (`cd $SP3/..`). `PY` is `.venv/bin/python`; every script is run as `$PY -I sp3/scripts/<name>.py`
(the scripts hold absolute paths, so the working folder only matters for the relative `sp3/` prefix).

| Tag | What | Command and result |
|---|---|---|
| C1 | version | `curl -s -A "$UA" https://sarraficars.com/api/health` gave `{"commit":"0fd91872bedb07a30ecdd7e19773db5fdc53b1a7","status":"ok","version":"1.5.8"}` |
| C2 | static parity | `$PY -I sp3/scripts/static_parity.py` (saved as run; uses `live.py`): 15 stylesheets plus `listings.js`, `main.js`, `sidebar.js`, `listings/chips.js` fetched with `?v=1792774194`, SHA-256 against `frontend/static`: **19/19 identical** (`out/static_parity.txt`; 19 requests) |
| C3 | live capture of today | `$PY -I sp3/scripts/capture_live.py` (53.2 s; 10 live requests: 6 documents, parse x2, zip-coords, area payload). Static files served from the repository, every POST and other API GET aborted in the browser. Output: the `web_shots/sp-today-*.png` files and `sp-today-capture-log.json` (154 static files served locally, 744 third-party image requests allowed, 80 POST attempts aborted) |
| C4 | live zero-result state | `$PY -I sp3/scripts/capture_zero.py` (run twice: 5 requests, then 4 with `ZERO_WIDTHS=1440` and the geo-hint probe); `sp-today-zero-{1440,390}.png` |
| C5 | live example queries | `$PY -I sp3/scripts/live_examples.py` (11 queries x parse + `/listings?q=` = 22 requests, `out/live_examples.json`), then `live_examples2.py` for "Ford F-150 under $45k" and "Honda Civic under $25k" (4 requests, `out/live_examples2.json`) |
| C6 | live grids and the area payload | `$PY -I sp3/scripts/live_grids.py` (5 requests: demo grid, Model Y grid, two relaxations, `GET /api/listings/cars?zip=92694&radius=25`: 1,633,216 B gzip on the wire, 17,109,857 B decoded, 15,457 cars, 1,117.7 ms; `out/live_grids_summary.json`) |
| C7 | pages the prototypes are built from | `live.get('/listings', save_to=...)` (1,548,917 B decoded, 189,497 B brotli) and `live.get('/car/2002740', save_to=...)` (61,602 B decoded, 3.05 s); 2 requests, run as inline snippets with `live.py` |
| C8 | local read-only probes | `cd <repo> && INVENTORY_DATABASE_URL=postgresql://localhost:5432/cars PGOPTIONS="-c default_transaction_read_only=on" SMART_SEARCH_SKIP_VECTOR_RERANK=1 USERS_DB_PATH=$SP3/users_probe_copy.db .venv/bin/python $SP3/scripts/{proto_data,parse_probe,parse_probe2,local_counts}.py <out.json>`. `PGOPTIONS` makes every Postgres transaction read-only; `USERS_DB_PATH` points at a scratch copy so the module-level bootstrap cannot touch `users.db` |
| C9 | logging evidence | `sqlite3` through Python with `file:backend/users.db?mode=ro` and `settled_searches.sql`, `adoption_sqlite.sql`; `PGOPTIONS="-c default_transaction_read_only=on" psql -h localhost -d cars -X -A -f user_history_pg.sql` (and the two PG files). Results `out/logging_*.txt` |
| C10 | prototypes | `make_proto_data_live.py`, `build_protos.py <scratch>`, `build_car_proto.py <scratch>`, then `run_all_local.sh` (runs `shoot_protos.py`, `shoot_car.py`, `audit_design.py`, `typing_offline.py`), then `behaviors.py`, `widths_check.py`, `filter_pos.py`, `count_filters.py`, `small_targets.py`. The shooter serves `search_proto/` from a local `http.server` and aborts every off-host request except third-party car photos |
| C11 | offline replays | `typing_offline.py` (saved live HTML, repository static files, canned answers; zero live requests), `fix_experiment_typing.py` (same, with `listings.js` patched in memory), `zero_typed_offline.py` |
| C12 | sample sizes | `$PY -I sp3/scripts/sample_size.py` (output `out/sample_size.txt`) |

### 9.2 Live request accounting

72 requests, all `GET`, none to a login, form or POST endpoint. By step: C1 1, C7 2, C2 19, C3 10, C5 26, C6 5, C4 9 = 72. Pacing: `scripts/live.py`
sleeps at least 1.1 s between requests (across processes) and refuses to pass 280; the browser gate in `capture_live.py` keeps at least
1.05 s and refuses to pass 30 per run. The smallest gap in the two browser logs is 1.05 s; the ledger's own timestamps have 1 s
resolution. The two clients never ran at the same time. Rate limits of the site were not triggered (no 429 seen).

### 9.3 Prototype build

- **Source:** the live `/listings` HTML (C7) with scripts and JSON islands stripped and all 1,496 filter checkboxes kept.
- **Styles:** the 15 real stylesheets from `/static/css`, through the `search_proto/static` symlink to `frontend/static`, then `proto-common.css`.
- **Result cards:** drawn by the site's own `ds_core.js`, `sc-helpers.js` and `listings/card.js` from the live rows (C6).
- **Network:** no request from the prototypes reached sarraficars.com (the shooter's `blocked` list holds only the Cloudflare beacon).

---

## 10. Appendix: queries checked

### 10.1 Live and local counts, 25 mi of 92694

| Query | Live cars (C5) | Local cars (C8) | Live parse kept |
|---|---|---|---|
| white BMW X5 under $70k | 20 | 24 | white, BMW, X5, max price |
| RAV4 hybrid under $35k | 53 | 58 | Toyota RAV4, hybrid, max price |
| Camry hybrid under $28k | 34 | 35 | Toyota Camry, hybrid, max price |
| used Tesla Model 3 under $30k | 24 | 22 | Tesla Model 3, pre-owned, max price |
| Mercedes GLE under 30k miles | 48 | 55 | Mercedes-benz GLE, max mileage |
| Ford F-150 under $45k | 52 | 88 | Ford F-150, max price |
| Honda Civic under $25k | 47 | 55 | Honda Civic, max price |
| electric SUV under $45k | 200 (grid cap) | 385 | body style SUV, electric, max price |
| Tesla Model Y under $30k | 15 | 18 | Tesla Model Y, max price |
| red Tesla Model Y under $30k | 0 | 0 | red, Tesla Model Y, max price |
| Ford Mustang Mach-E under $40k (not live-checked) | | 0 | model "Mustang Mach E": hyphen lost |

Zero-result recovery for "red Tesla Model Y under $30k" (live): any color 15; up to $36,000 (`red Tesla Model Y under $36k`) 1; within 100 mi 1.

### 10.2 Typed prefixes that do not parse (C8)

"white bmw x5 under 70k": w, wh, whi, whit (4 of 22). "rav4 hybrid under 35k": r, ra, rav (3 of 21). Each one reaches the server, which
answers `no_parse_match` with 0 results and writes a `result_count = 0` row.
