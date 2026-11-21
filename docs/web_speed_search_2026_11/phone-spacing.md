# Phone spacing and layout audit: sarraficars.com, 2026-11-09

Scope: the live site (release 1.5.8) on phones, 390x844 and 360x740, with 1440x900 for comparison.
Pages: home (`/`), listings empty state (`/listings`), listings after a search
(`/listings?zip_code=28202&radius=25`), search results (`/listings?q=toyota+rav4+under+35k&zip_code=28202&radius=25`,
the URL the home-page search form produces), a car page (`/car/1733731?zip_code=28202`, the first card with a
photo) and a dealership page (`/dealership/townandcountrytoyota-com`, that card's dealer).

A third pass (2026-11-10, "Third pass" under Re-verification) added five more pages a phone user reaches from the
first screen (`/login`, `/register`, `/find-dealers`, `/compare`, `/premium`), one zero-result search URL, and the
two states the first two passes never opened: the compare tray and the menu drawer's tap sizes. It added PS-19
to PS-24 (new defects), extended PS-03, PS-05, PS-06 and PS-07 with new numbers, and corrected one description in PS-06.

Every number below came from a script run in this workflow session: a first pass (capture, audit and fix tests,
2026-11-09 08:44 to 09:20 EST), a second pass (the same evening, see "Re-verification") that re-measured the
headline numbers with separate code, re-checked every citation against a `git archive` of `phase/N37a`, tested the
patch at 320 and 375 px, and added PS-16 to PS-18, and the third pass (2026-11-10 20:19 to 21:00 EST), which
re-ran the second pass's independent measurement from the same cache (3,902 values compared, no geometry
difference), made 9 live requests, and tested the new fixes offline. The commands are in "How it was measured".
CSS citations are `file:line` at `phase/N37a` (faea2b985). The live CSS is byte-identical to that branch (see
"Version identity"). Proposed fixes were checked as screenshots at 390, 360 and 1440 px. Nothing in the
repository was changed except this file and the `phone-spacing_raw/` folder next to it (copies of the scripts and
small results; see "Files"). No branch was checked out; only read-only git commands were used (`git show`,
`git archive`, `git log`, `git ls-tree`, `git grep`, `git rev-parse`, `git diff --stat`, `git status`), and no
database was touched.

## Summary

There are five P1 problems on phones.

1. **No search bar on the phone listings screen (PS-01).** The search input sits inside the filter sidebar.
   Below 960 px that sidebar becomes a hidden bottom sheet, so a phone user sees no search bar on `/listings` or
   on search results. A query typed on the home page shows up only as a filter chip. At 1440 px the input is at
   y=139.5.
2. **Too much chrome above the first car (PS-02).** On search results the first car starts at y=507 at both phone
   widths. That is 60% of an 844-px screen and 76% of the 664 px iPhone Safari shows. On a plain listings search
   it starts at y=418. Above it sit a 154-px results toolbar in three rows and 83–139 px of filter chips.
3. **The menu button sits on top of content (PS-03).** It is a fixed 44-px button. On `/listings` it covers the
   first words of the guest banner ("Save cars you like"). While scrolling it floats over card text, the car
   page's "At a glance" and the dealership map's zoom control. Each page also reserves a 56-px band for it in the
   wrong place.
4. **Home page header takes a fifth of the screen and stays pinned (PS-04).** It is 156 px tall in three rows and
   sticky: 18.5% of 390x844 and 21.1% of 360x740. The hero search line starts at y=450 (390) and y=494 (360).
5. **Inputs trigger iOS focus zoom (PS-05).** Every visible text field and select on the listings pages is set
   below 16 px (13–15 px). iOS Safari zooms the page when such a field gets focus.

Most of the rest is cramped tap targets, mismatched side margins and duplicated controls. On the listings search
at 390 px, 88 of 113 tap targets are under 44 px. The left text margin is 12 px on listings, 16 px on the car
page, 28 px on the dealership car grid and 20 px on home.

**Nothing overflows sideways at the three requested widths.** On all 18 page-and-width combinations the page is
exactly as wide as the screen, and no element sticks out past either edge. At 320 px (iPhone SE class) one thing
does: the ZIP row of the empty listings page is 4.1 px too wide and the "Set location" button is clipped
(PS-16). The second pass also found a footer-link indent on the home page (PS-17) and one risk that Chromium
cannot show: the menu drawer is sized in `100vh` (PS-18, not reproduced).

**Third pass, six more defects (PS-19 to PS-24), all measured and with a tested fix.**

- **Compare tray (PS-19).** Tick "Compare" on two cars and the tray that appears is unreadable and in the way. The
  car titles are white (`rgb(255,255,255)`) on a near-white tray (`rgb(250,248,245)`): contrast 1.06 : 1. The tray is
  120 px tall (168 px with four cars) against 72 px of reserved page padding, so it covers the end of the page. On a
  car page it sits on top of the fixed "View listing" bar and hides it. Its thumbnails never load because
  `compare.js:204` builds a `style` attribute without the closing quote.
- **Menu drawer (PS-20).** The drawer is the main navigation on every non-home phone page. Its nav links are
  35.5 px tall, the logo link 19 px, the close button 22x32 and the search fields 33–36 px (the ZIP field is 48 px wide).
- **Dealership page (PS-21).** The phone number (19 px), website (19 px) and Google Maps / Apple Maps / Waze links
  (24.4 px) are the main phone actions on a dealer page. The first patch left them.
- **Register page (PS-22).** The header is 200.5 px (23.8% of 390x844, 27.1% of 360x740) and pinned. The sign-in
  page next to it has a different header (a 44-px menu button).
- **Short pages (PS-23).** Sign-in and Compare scroll 47 px for nothing (`.page` is `100vh`, then the version footer).
- **Find dealers (PS-24).** The three location tabs are stacked at 30 px each, a map legend that should wait for
  results shows at once, and the 520-px map box holds a 320-px map. The page is 1,567 px tall and 1,006 px after the fix.

The same pass found the login fields at 15 px (iOS focus zoom, added to PS-05) and put a number on PS-03: at 390 px
the floating menu button sits over text, a control or a photo at 45 of 45 sampled scroll positions on the listings
search, 5 of 6 on the car page and 48 of 51 on the dealership page.

**Scorecard** (390x844 unless noted; every value is a measurement in this report; "tested" means the first patch plus the third-pass rules, simulated in the page).

| indicator | now | tested |
|---|---|---|
| horizontal overflow (12 pages at 360, 390 and 1440; the original six also at 320, 375, 412 and 430) | none, except the ZIP row at 320 (4.1 px) | none |
| search bar visible on the three listings screens without opening the Filters sheet | 0 of 3 | 3 of 3 (y=81) |
| first car's top, listings search / search results (share of the 844-px screen) | y=418 (49.5%) / 507 (60.1%) | 325 (38.5%) / 377 (44.7%) |
| home header | 156 px, pinned (18.5%) | 125 px, scrolls away |
| form fields under 16 px (iOS focus zoom), phone pages | listings 4 of 4, results 4 of 4, empty 6 of 6, dealership 4 of 4, car 2 of 2, login 4 of 4 | 0 |
| tap boxes under 44 px, listings search | 88 of 113 | 26 of 113 (24 dealer links with a 40-px hit area, 2 inside a sentence) |
| floating menu button over text, a photo or a control while scrolling (listings search / car / dealership) | 45 of 45 / 5 of 6 / 48 of 51 positions | 0 / 0 / 0 |
| distinct left text margins across the 12 pages at 390 | 6 (12, 16, 20, 22, 24, 28 px) | 4 (16, 20, 22, 24; home, register and compare not changed) |
| compare tray, title contrast | 1.06 : 1 | 11.77 : 1 |
| short pages that scroll for nothing | 2 (sign-in, Compare: 47 px) | 0 |

A tested patch is in "The tested patch". It is CSS plus two small template edits (move the search row, shorten
two toolbar labels), and it follows the owner rules: no cards, boxes, pills or tiles, only hairlines and spacing.
Measured effect at 390 px, before → after:

- Search results: first car 507 → 377. Listings search: 418 → 325, even with a new 48-px search line on top.
  The search line now sits at y=81 on every listings state, and the first car's price now fits on the 664-px
  iPhone first screen on both pages.
- Home: header 156 px pinned → 125 px that scrolls away; search line 450 → 305.
- Car page: price 713 → 619.
- Tap targets under 44 px on the listings search: 88 → 26. The 26 left are 24 dealer links, which now have a
  40-px-or-more invisible tap area (checked by hit-testing), and 2 links inside a sentence.
- Fields under 16 px: 0 on every phone page.
- Margins: 16/16 on listings and the dealership grid.
- At 1440 px the car page and home page are unchanged.
- What the patch costs, which the first pass did not state: each of the 24 cards grows 14 px (377 → 391; the
  Compare / photos / heart row becomes 44 px tall and the 11-px text becomes 12 px), so the listings-search page is
  267 px longer overall (10,161 → 10,428), the dealership page 314 px longer (11,573 → 11,887), and the fixed car
  bottom bar 7 px taller (62 → 69). Home and the car page get shorter (3,574 → 3,256 and 2,037 → 1,984).

## Version identity (live 1.5.8 vs phase/N37a)

- The live page footer reads `v1.5.8` (`.app-version-footer` landmark text in `out/listings-empty-390.json`).
- After the capture, all 48 static files fetched from the live site (16 CSS, the scripts, the font) were compared
  byte-for-byte with `git show <ref>:frontend/static/<path>`:
  - all 48 match `main` (tag `v1.5.8`);
  - all 16 CSS files, including `15-dealership.css`, match `phase/N37a` (297,329 bytes);
  - only `main.js` and `listings/card.js` differ from `phase/N37a`.

  An earlier check, run right after the smoke test, covered the first 17 files and gave the same result.
- `git diff --stat main phase/N37a -- frontend/` lists 6 files:
  - `car.html`: +10 lines. A "Details coming" note, and the depreciation forecast now renders only when there is
    a real price.
  - `listings/card.js`: +7. An optional "Details coming" line for pending cars.
  - `main.js`: 27 lines, all about keeping the current results page on refresh.
  - `car_packages.js`: 2 lines.
  - Two dev templates.

  None of these touch the spacing measured here. N37a line numbers for `car.html` are 5 lines higher after
  line 248, and `card.js:152` on live is `card.js:157` at N37a. Both are cited at N37a below.
- Second-pass re-check, 2026-11-10 01:41 UTC (2026-11-09 20:41 EST):
  - One GET of `/` still returns `v1.5.8` and the same stylesheet token (`?v=1792774194` on all 15 `<link>`s) as the
    first capture, so the live CSS has not changed since the capture.
  - The 48 cached static files were compared again with `git show main:` and `git show phase/N37a:`. 48 equal
    `main`. 46 equal `phase/N37a`; the two that differ are `main.js` and `listings/card.js`. All 16 repository
    stylesheets (297,329 bytes) and `vendor/leaflet/leaflet.css` equal `phase/N37a`.
- Third-pass re-check, 2026-11-10 20:20 EST (2026-11-11 01:20:38 UTC): one GET of `/` again returns 200, 15,303 bytes,
  `v1.5.8` and the same 15 stylesheet links `?v=1792774194`; `phase/N37a` is still `faea2b985` and `main` is `0fd91872b`; the two
  static files the new pages load equal `main` and `phase/N37a`. Details under "Third pass".

## How it was measured

Tools: `.venv/bin/python` (Playwright 1.60.0, bundled Chromium,
headless). Phones are emulated with `is_mobile=True, has_touch=True, device_scale_factor=2`, an iOS Safari user
agent at 390x844 and an Android Chrome user agent at 360x740. Desktop is 1440x900 at DPR 1. Playwright's own
"iPhone 13" profile gives 390x664 as the area Safari actually shows, so "664" below means the real iPhone first
screen.

### Rules followed against the live site

Every request went through
`scratchpad/phone_audit/scripts/netcache.py`:

- **GET only.** Any other method is aborted before it leaves the browser.
- **Rate.** Live request starts are at least 1.1 s apart.
- **Cap.** A persisted hard cap of 270 live requests.
- **Replay.** Every response is saved to disk and served from there on every later load. The 360 and 1440 passes,
  the CSS tracing and all fix screenshots therefore cost no live requests.
- **Blocked.** Car-page prefetches from `nav_perf.js` (`fetch()` of `/car/<id>`) and the Cloudflare analytics
  beacon were aborted.

Ledger, from
`scratchpad/phone_audit/http_cache/live_requests.jsonl`:

- **63 live requests in the first pass**, all GET, all returned 200, between 13:44:59 and 13:48:47 UTC.
  - 27 scripts, 17 stylesheets, 9 API fetches, 6 documents, 3 images, 1 font.
  - Minimum gap between starts: 1.100 s.
- **1 more in the second pass:** `curl` GET of `https://sarraficars.com/` at 2026-11-10T01:41:12 UTC, HTTP 200,
  15,303 bytes, to re-check the version and the stylesheet token. Total **64 live requests**, far under the 300 cap.
  Everything else in the second pass was an offline replay (`NetCache(offline=True)`; the run summaries print
  `'live': 0`). The ledger `live_requests.jsonl` and `live_count.json` (64) were updated by hand for that GET.
- **Never sent:**
  - 12 POSTs that page JavaScript tried during the live capture: `POST /api/session/listings-geo` and
    `POST /api/search/smart`.
  - 90 `nav_perf.js` car-page prefetches.
  - 19 Cloudflare beacon loads (1 in the smoke test, 18 in the full capture).
- **Not the live site:** 90 GETs went to third parties. The 80 distinct URLs among them are dealer photo hosts
  (images.dealer.com, randymariongmc.net, carscommerce.inc and others) and 6 OpenStreetMap map tiles. Three
  dealer photos failed in the browser with `net::ERR_BLOCKED_BY_ORB`; their retries account for the other 10
  requests.
- Nothing logged in, and no form was submitted. Search pages were opened by URL. The only clicks were the
  "Filters" button and the menu button.

### Commands

`SCR=scratchpad/phone_audit`

```sh
PY=.venv/bin/python
cd $SCR
# A. live capture: the only step that sends requests to sarraficars.com
$PY -I scripts/capture.py --vps 390 --pages home > out/smoke_home_390.log   # smoke test, 18 live GETs
$PY -I scripts/capture.py --vps 390,360,1440 > out/capture_all.log          # 45 more live GETs (63 total)
$PY -I scripts/table.py                                                     # "Before" table below
# B. live static vs git: python snippet that compares each cached /static/ body with
#    `git show phase/N37a:frontend/static/<path>` and `git show main:...`
#    (output: 48 files; CSS == N37a True; all == main True; != N37a: main.js, listings/card.js)
# C. which CSS rule sets each property (CDP CSS.getMatchedStylesForNode; offline replay only)
$PY -I scripts/trace.py trace/probes1.json trace/trace1.json
$PY -I scripts/winners.py trace/trace1.json > trace/trace1_winners.txt
#    every cited line was then printed with `git show phase/N37a:frontend/static/css/<file>`
# D. before/after (offline replay; the fix CSS is served from a fake same-origin URL, so the
#    page's own CSP still applies and nothing is bypassed)
$PY -I scripts/fixcheck.py before && $PY -I scripts/fixcheck.py after       # run 2: all pages and widths
$PY -I scripts/fixcheck.py before --vps 390 --pages listings-search,car     # run 3: hit-test baseline
$PY -I scripts/fixcheck.py after                                            # run 3: final patch
$PY -I scripts/compare.py > out/compare_run3.md
$PY -I scripts/footer_check.py                                              # PS-14 text overlap
$PY -I scripts/price_check.py                                               # PS-02 first car's price position
```

Second pass (all offline, zero live requests except the one GET in E). `V` is
`scratchpad/verify`:

```sh
# E. one live GET: version and stylesheet token (logged in the ledger as request 64)
curl -sS -A "<iPhone Safari UA>" -o $V/home_now.html -D $V/home_now.headers https://sarraficars.com/
#    grep -o "v1\.5\.[0-9]*" -> v1.5.8; grep -o 'static/css/[0-9a-z-]*\.css?v=[0-9]*' -> 15 links, all ?v=1792774194
# F. citations: extract phase/N37a and print the text at every cited line
cd . && git archive phase/N37a frontend | tar -x -C $V/../n37a_verify
$PY -I $V/cite_check.py                      # 98 full-form citations printed; 21 shorthand ones printed by hand with sed/awk
#    live static files vs git: 48 cached /static/ bodies compared with `git show main:` and `git show phase/N37a:`
# G. independent re-measurement (new JavaScript, not audit.js), before and after the patch, 3 widths x 6 pages
$PY -I $V/remeasure.py $V/remeasure_all.json --drawer     # output: remeasure_all.json, remeasure_all.log
# H. extra checks: car-page tap rule, 1440 empty state, 320 and 375 overflow, footer indent
$PY -I $V/extra_checks.py                    # output: extra_checks.json
$PY -I $V/extra_checks2.py                   # ZIP row min-width fix at 320, 360, 390: extra_checks2.json
$PY -I $V/extra_checks3.py                   # whole patch at 320 and 375, drawer geometry: extra_checks3.json
$PY -I $V/home320.py                         # home header and search line at 320, before and after the patch
$PY -I $V/zipthreshold.py                    # PS-16: ZIP row spill at 320, 360, 370, 375, 380, 385, 390
$PY -I $SCR/scripts/footer_check.py          # PS-14 numbers again
```

Each `audit.js` pass measures:

- page width against screen width, and any element that sticks out past an edge without a clipping parent;
- every tap target: links, buttons, inputs and selects, with a wrapping label used as the target. A target counts
  as "under 44" when its width or its height is under 44 px. Controls that are not visible (`display: none`,
  `visibility: hidden` or inside an `opacity: 0` ancestor) are not counted;
- every fixed or sticky element, and what sits under it (`elementsFromPoint`), at the top and at the bottom of
  the page;
- the left and right text margin;
- text that wraps or gets clipped;
- text under 12 px;
- font size of each form field;
- the positions of about 90 named page parts.

Raw results are in `$SCR/out/*.json` (capture) and `$SCR/out/fix-{before,after}-*.json` (fix runs).

## Before: measured state of the live site

Columns:

- **taps <44**: targets under 44 px out of all targets; in brackets, how many of those are on the first screen.
- **fields <16px**: text fields and selects with a font size under 16 px, out of all rendered fields. On the app
  pages this includes the two search fields in the slide-out menu, which are off screen until the menu opens.
- **margin L/R**: the most common left and right text margin.
- **first content**: the main thing on the page — the home headline, the start prompt, the first car card, or the
  car title.

| page | width | page h | page w | first content y | search input y | fixed/sticky on screen | taps <44 (first screen) | taps <24 | fields <16px | margin L/R | text <12px |
|---|---|---|---|---|---|---|---|---|---|---|---|
| home | 390 | 3574 | 390 | 264 | 450.3 | header 156 px, 18.5% | 5/19 (1) | 5 | 0/3 | 20/20 | 5 |
| home | 360 | 3648 | 360 | 264 | 494.4 | header 156 px, 21.1% | 5/19 (1) | 5 | 0/3 | 20/20 | 5 |
| home | 1440 | 2108 | 1440 | 186 | 393.7 | header 78 px, 8.7% | 13/19 (9) | 7 | 2/3 | 186/186 | 5 |
| listings empty | 390 | 969 | 390 | 481.7 | not visible | menu button 44 px | 14/15 (14) | 4 | 6/6 | 12/16 | 2 |
| listings empty | 360 | 1020 | 360 | 500.5 | not visible | menu button 44 px | 14/15 (12) | 4 | 6/6 | 12/16 | 2 |
| listings empty | 1440 | 1104 | 1440 | 254.7 | 139.5 in sidebar (first capture: 147.5) | — | 22/42 (21) | 5 | 12/12 | — | 6 |
| listings search | 390 | 10161 | 390 | 418 | not visible | menu button 44 px | 88/113 (14) | 51 | 4/4 | 12/16 | 73 |
| listings search | 360 | 10161 | 360 | 418 | not visible | menu button 44 px | 88/113 (12) | 51 | 4/4 | 12/16 | 73 |
| listings search | 1440 | 5735 | 1440 | 208 | 139.5 | — | 97/141 (25) | 52 | 10/10 | — | 77 |
| search results | 390 | 10370 | 390 | 507 | not visible | menu button 44 px | 99/124 (19) | 52 | 4/4 | 12/16 | 75 |
| search results | 360 | 10370 | 360 | 507 | not visible | menu button 44 px | 99/124 (18) | 52 | 4/4 | 12/16 | 75 |
| search results | 1440 | 5872 | 1440 | 265 | 139.5 | — | 108/152 (32) | 53 | 10/10 | — | 79 |
| car | 390 | 2037 | 390 | 615.1 | not visible | menu button + bottom bar 62 px, 7.3% | 12/22 (9) | 3 | 2/2 | 16/16 | 9 |
| car | 360 | 2046 | 360 | 608.6 | not visible | menu button + bottom bar 62 px, 8.4% | 12/22 (7) | 3 | 2/2 | 16/16 | 9 |
| car | 1440 | 1349 | 1440 | 174.6 | not visible | — | 23/29 (17) | 4 | 2/2 | — | 9 |
| dealership | 390 | 11573 | 390 | 469.3 | none on page | menu button 44 px | 95/121 (6) | 53 | 4/4 | 28/16 | 75 |
| dealership | 360 | 11706 | 360 | 469.3 | none on page | menu button 44 px | 95/121 (3) | 53 | 4/4 | 28/16 | 75 |
| dealership | 1440 | 4932 | 1440 | 350.3 | none on page | — | 104/143 (20) | 54 | 7/7 | — | 78 |

Notes on the table:

- **Desktop margins.** Margins at 1440 are left out ("—"): the left column there is the navigation sidebar, not a
  page margin.
- **listings empty, 1440.** The first capture flagged the sidebar search input as "not visible" and put it at
  y=147.5, although the screenshot shows it. The second pass explains this. At 139.5 the input is visible in the
  offline replay (`remeasure.py` and the first pass's own `fix-before-listings-empty-1440.json` both give 139.5), and
  none of its ancestors has `opacity` or `display` set. The listings page runs a 0.28-s entrance animation
  (`02-listings.css:1092`, keyframes `:1104-1114`: opacity 0 → 1 and `translateY(8px)` → 0) on the intro and search
  card. A measurement taken during it sees opacity 0 and a position 8 px low, which are exactly the two
  discrepancies (147.5 − 139.5 = 8). The sidebar's controls were also counted as hidden at that moment, so the first
  capture's other numbers for this one row were low. The table now shows the settled replay (`fix-before-listings-empty-1440.json`,
  and `remeasure.py` agrees): tap targets under 44 px 22 of 42 (was 21 of 22), fields under 16 px 12 of 12 (was 6 of
  6), text under 12 px 6 (was 2). A comparison of the first capture with the settled replay for all 18 page-and-width
  combinations found this to be the only row that differs. No phone number changes.
- **listings empty, first content.** This is the start prompt ("Start a search to see cars").

## Findings

P1 means it blocks the phone task or the owner's request, P2 is clearly broken spacing, and P3 is polish.
Screenshot names: `ps-<page>-<width>-fold.png` is the live first screen, `-full.png` is the whole page,
`ps-fix-*` is the same page with the proposed patch, and `ps-base-*` is the offline "before" from the fix runs.
`ps2-*` files come from the second pass (320 and 375 px, footer and ZIP-row checks).

| ID | Pri | Problem (390 px unless stated) | Main cause at `phase/N37a` | Effort |
|---|---|---|---|---|
| PS-01 | P1 | No search bar on the phone listings screens; the query appears only as a chip | `listings.html:52,65-75`; `09-components.css:901-913` | S |
| PS-02 | P1 | Toolbar (154 px) and chips (83-139 px) push the first car to y=418 / 507 | `11-overrides.css:78-87,169-176`; `09-components.css:397-412,618-624` | S |
| PS-03 | P1 | Fixed menu button covers text, a photo or a control at 45 of 45 scroll positions on the listings search; its 56-px reserve is in the wrong place | `07-dealer-admin.css:555-573,655-672`; `11-overrides.css:73-76,259-262` | S |
| PS-04 | P1 | Home header 156 px and pinned; hero search line at y=450 | `08-marketing.css:115-120`; `00-base.css:128-140`; `12-viewers.css:157,169,235,243` | S |
| PS-05 | P1 | Every listings field, the menu's search fields and the login fields are 13-15 px, so iOS zooms on focus | `09-components.css:127-131,454-457,996-999`; `11-overrides.css:365-388`; `07-dealer-admin.css:402-412`; `01-nav.css:589-603` | S |
| PS-06 | P2 | 88 of 113 tap targets under 44 px on a listings search | `02-listings.css:1327`; `09-components.css:781-786,1104-1111,646-677` and others | S |
| PS-07 | P2 | Left margin 12 / 16 / 20 / 22 / 24 / 28 px by page | `07-dealer-admin.css:692-695`; `10-pages.css:4-6`; `13-landing-auth.css:344-348` | S |
| PS-08 | P2 | Car title and price below the first screen | `03-car-page.css:79,1808-1813`; `05-car-tools.css:1034`; `11-overrides.css:178-182` | S |
| PS-09 | P2 | Empty listings page asks for the ZIP twice; toolbar with nothing to sort | `listings.html:362-379,435-453`; `02-listings.css:1377-1384,1423-1426` | S |
| PS-10 | P2 | Dealership Filters sheet stops 112 px above the bottom | `15-dealership.css:164` | S |
| PS-11 | P3 | Wrapped or cut text (home buttons, placeholder, dealer line, menu search) | `12-viewers.css:651-658,745-747`; `01-nav.css:379-382`; `07-dealer-admin.css:352-357` | S |
| PS-12 | P3 | Desktop section padding on home (628 px) | `12-viewers.css:154-157,423-425,475-477,514-516` | S |
| PS-13 | P3 | Text under 12 px (11 px card text, 10 px labels) | `02-listings.css:1221-1224,1268-1269,1320-1321`; `03-car-page.css:1134-1136` | S |
| PS-14 | P3 | Car bottom bar hides the version footer | `09-components.css:924-934,963-965` | S |
| PS-15 | P3 | Broken-photo fallback blocked by the CSP | `card.js:157` | S |
| PS-16 | P3 | ZIP row wider than its box below 380 px; clipped 4.1 px at 320 | `09-components.css:171-173` | S |
| PS-17 | P3 | Home footer links indented 22 px past the left margin | `12-viewers.css:700-705` | S |
| PS-18 | P2 (not reproduced) | Menu drawer sized in `100vh`; iOS toolbar may hide "Sign in" / "Get started" | `07-dealer-admin.css:263-283` | S |
| PS-19 | P2 | Compare tray: titles white on near-white (1.06 : 1), 120-px tray over 72 px of reserve, hides the car "View listing" bar, thumbnails never load | `10-pages.css:149,164,173`; `09-components.css:1131-1133,1183-1189`; `compare.js:204` | S |
| PS-20 | P2 | Menu drawer targets under 44 px: nav links 35.5, logo 19, close 22x32, search fields 33-36 | `07-dealer-admin.css:485-497,301-323,352-357,402-447` | S |
| PS-21 | P2 | Dealership phone, website and directions links are 19 to 24 px tall | `15-dealership.css:28-34,138-140` | S |
| PS-22 | P2 | Register header 200.5 px and pinned; sign-in has a different header | `_nav_marketing.html:9`; `13-landing-auth.css:379`; `00-base.css:128-140` | S |
| PS-23 | P3 | Short pages (sign-in, Compare) scroll 47 px for nothing | `00-base.css:123-126`; `07-dealer-admin.css:251`; `08-marketing.css:96-103` | S |
| PS-24 | P3 | Find dealers: tabs stacked at 30 px, legend shown before results, 520-px map box around a 320-px map | `11-overrides.css:222-230`; `10-pages.css:522-533,574-578,640-648`; `find_dealers.html:98` | S |

### PS-01 (P1): the search bar is not on the phone listings screen

**What was measured**

- `#smart-search-input` is not shown on the listings empty state, the listings search or search results at 390 and
  360 px.
- It renders only inside the Filters sheet. Once the sheet is opened it sits at y=213.5 (390) and y=178.3 (360).
- At 1440 px it is visible in the sidebar at y=139.5.
- On search results the query reaches a phone user only as the chip "toyota rav4 under 35k ×".

**Screenshots**

- Live, search results: `scratchpad/web_shots/ps-search-results-390-fold.png`
- Live, Filters sheet open: `scratchpad/web_shots/ps-listings-search-390-filters-open-fold.png`
- After the patch, search results: `scratchpad/web_shots/ps-fix-search-results-390-fold.png`
- After the patch, at 1440: `scratchpad/web_shots/ps-fix-listings-search-1440-fold.png`

**Cause**

- `frontend/templates/listings.html:52` opens `<aside class="listings-sidebar">`, and the search row
  (`listings.html:65-75`) is inside it.
- `frontend/static/css/09-components.css:901-913`, inside the 960-px-and-under block at `:883`, sets
  `body.listings-page .listings-sidebar { display: none; position: fixed; ... }`. Only
  `body.listings-filters-open` shows it (`:920-922`).
- The input's own size and box are set by four separate rules, all with a 15-px font and rounded corners:
  `02-listings.css:32-44`, `:570-578`, `:976-983` and `11-overrides.css:365-375`.

**Fix**

- Template: move the `<div class="filter-search-row">` from `listings.html:65-75` to be the first child of
  `<div id="dash-content">` (`listings.html:361`).
  - Scripts find the input by id, and it stays inside `form#search-form`, which wraps both columns.
  - The parsed-query line `#smart-parse-chips` moves with it.
- Style it as a bare line, not a box: magnifier, 17-px text, 48 px tall, one navy hairline (stylesheet block
  PS-5 in "The tested patch"). Fold the four duplicate input rules into that one.
- Rename the sheet heading "Search cars" (`listings.html:56`) to "Filters". All filters stay as they are.

**After the patch**

- At 390 and 360 the search line is at y=81 on all three listings states.
- On search results the query reads "toyota rav4 under 35k", with the parsed line "Toyota RAV4 · Under $35,000"
  under it. That is the query-to-parsed-filters display the owner liked.
- At 1440 the line sits on top of the results column (y=71), so the first card moves from 208 to 278 on the
  listings search and from 265 to 354 on search results. That is the cost of putting search above filters on
  desktop.

**Effort and risk:** S. Risk is low to medium: the desktop sidebar loses its search box (intended); re-run
`backend/tests/js/listings_golden.test.js`.

### PS-02 (P1): the results toolbar and filter chips push the first car down

**What was measured (same at 390 and 360)**

- The guest banner ends at y=81.
- The first control starts at y=149, after a blank band (see PS-03).
- The toolbar is 154 px tall in three rows, plus a 16-px margin:
  1. Filters and the result count.
  2. "Save this search" and "Saved searches".
  3. "Per page" and "Sort".
- The filter chips take 83 px on the listings search: one row of chips, then "Clear all" on its own row.
- On search results the chips take 139 px: three rows of chips plus the "Clear all" row.
- The first card is at 418 (listings search) and 507 (search results).
- The first car's price spans y 657–681 on the listings search and 746–770 on search results (measured with
  `$SCR/scripts/price_check.py`). On a 664-px iPhone screen the price is cut off on the first and fully below the
  fold on the second.

**Screenshots**

- Live, listings search: `scratchpad/web_shots/ps-listings-search-390-fold.png`
- Live, listings search at 360: `scratchpad/web_shots/ps-listings-search-360-fold.png`
- Live, search results: `scratchpad/web_shots/ps-search-results-390-fold.png`
- After the patch: `scratchpad/web_shots/ps-fix-listings-search-390-fold.png`

**Cause**

- Toolbar layout:
  - `11-overrides.css:78-80` (768-px block at `:17`): `.listings-toolbar { flex-direction: column }`.
  - `11-overrides.css:83-87`: `.listings-toolbar__start, .listings-toolbar__end { width: 100%; justify-content: space-between }`.
  - `09-components.css:397-404`: gap 12px 20px, margin-bottom 16px, padding-bottom 16px.
  - `09-components.css:408-412`: the start group wraps.
- Chips:
  - `11-overrides.css:169-171` (600-px block at `:150`): `.listings-active-chips-wrap { flex-direction: column }`.
  - `11-overrides.css:174-176`: `.listings-chips-clear { align-self: flex-start }`.
  - `09-components.css:618-624`: padding 10px 0, margin-bottom 16px.

**Fix (600 px and under)**

- Toolbar in two rows: "Filters · Sort" on one, "count · Save search · ♡ Saved" on the other.
  - Use `display: contents` on the two toolbar groups and `order` on the controls.
  - Hide "Per page" on phones (24 per page stays the default), or move it to the pagination row.
  - All rows 44 px tall.
- Template: shorten the two labels so row 2 fits at 360. `listings.html:392` "Save this search" → "Save search",
  and `listings.html:397` "Saved searches" → "Saved". No script rewrites these labels; `main.js` does not set them.
- Chips on one line that scrolls sideways, fading at the right edge (the pattern already used for the home-page
  nav, `11-overrides.css:41-55`; its `mask-image` is on line 54), with "Clear all" at the end of the line.

**After the patch**

| | before | after |
|---|---|---|
| toolbar | 154 px | 107 px |
| chips | 83 / 139 px | 45 px |
| first card, listings search (390 and 360) | 418 | 325 |
| first card, search results (390 and 360) | 507 | 377 |
| first car's price, listings search | 657–681 | 565–589 |
| first car's price, search results | 746–770 | 617–641 |

The "after" figures already include the new 48-px search line from PS-01. After the patch the first car's price
fits on the 664-px iPhone first screen on both pages.

**Effort and risk:** S, low. It changes phones only.

### PS-03 (P1): the fixed menu button covers content, and its reserved space is in the wrong place

**What was measured**

- `#app-sidebar-toggle` is fixed at (12, 12), 44x44, z-index 50, and is shown at 900 px and under.
- On `/listings` and search results, the elements under it at 390 and 360 are the banner's `strong`
  "Save cars you like" and the banner text itself.
- At the bottom of the page it covers:
  - "Pre-owned" and "SUV" on a card (listings search and search results, 390);
  - "At a glance" (car, 390);
  - "ENGINE" (car, 360);
  - "Sign in to write a review" (dealership, 390);
  - the map's "+" zoom button (dealership, 360).
- The 56-px space reserved for the button sits under the guest banner. The result is a 68-px blank band between
  the banner (ends at y=81) and the first control (y=149): 56 px of reserve plus 12 px of layout padding.
- On the car and dealership pages the same reserve pushes the first line below the button, leaving the rest of
  that strip empty. "Back to results" starts at y=56, and the dealership title at 75.6.
- The banner's close "✕" wraps onto its own line and is 22.7x18 px.
- **How often it covers something (third pass, `third_tests.py covers`).** The page was scrolled in steps of a quarter
  screen (844 px at 390) and five points inside the button were hit-tested with `elementsFromPoint`. A position counts
  when what lies under the button is text, an image or a control:

  | page at 390x844 | scroll positions | button over content | what was under it |
  |---|---|---|---|
  | listings search | 45 | 45 | 21 car photos, 4 prices, 3 titles, card links, distances, the saved-search button |
  | car | 6 | 5 | main photo, a thumbnail, the title, the deal label, a link |
  | dealership | 51 | 48 | 22 car photos, card links, prices, headings |

  With `.app-sidebar-toggle { position: absolute }` (the first patch) the same sampling finds 0 of 46, 0 of 6 and
  0 of 53 positions. These numbers are for the current fixed button; the "keep it fixed" alternative below was not
  sampled, and by construction it covers content wherever the user stops scrolling.

**Screenshots**

- Live, listings empty: `scratchpad/web_shots/ps-listings-empty-390-fold.png`
- Live, listings empty at 360: `scratchpad/web_shots/ps-listings-empty-360-fold.png`
- Live, car page: `scratchpad/web_shots/ps-car-390-fold.png`
- Live, dealership: `scratchpad/web_shots/ps-dealership-390-fold.png`
- After the patch, listings empty: `scratchpad/web_shots/ps-fix-listings-empty-390-fold.png`
- After the patch, car page: `scratchpad/web_shots/ps-fix-car-390-fold.png`

**Cause**

- The button:
  - `07-dealer-admin.css:555-573`: `.app-sidebar-toggle { position: fixed; top: 12px; left: 12px; z-index: 50; width: 44px; height: 44px }`.
  - `:641-643` shows it at 900 px and under.
- The reserved space:
  - `07-dealer-admin.css:655-667`: `padding-top: 56px` on `.car-detail`, `.dealer-research` and others.
  - `07-dealer-admin.css:669-672`: the same 56 px on `.listings-filter-shell`, which comes after the guest banner
    in the page.
  - `11-overrides.css:259-262` adds 12 px of layout padding.
- The banner:
  - `11-overrides.css:73-76`: `.guest-banner-content { flex-wrap: wrap }`.
  - `01-nav.css:341-347`: the close button has `padding: 2px 6px`.

**Fix (900 px and under)**

- Make the button scroll away with the first row instead of floating: `.app-sidebar-toggle { position: absolute }`.
  - Trade-off: the menu is then only reachable from the top of the page.
  - Alternative if the owner wants the menu reachable everywhere: leave the button `position: fixed` and apply only
    the banner and reserve changes below. The top of each page is then clean, but the button still floats over card
    text and the map zoom while scrolling (the bottom-of-page cases listed above). That alternative was not tested.
- Give the banner `padding-left: 68px; min-height: 68px` so it sits beside the button, and remove the reserve
  where the banner is present: `.guest-banner + .listings-filter-shell { padding-top: 0 }`.
- Car and dealership pages: change the 56-px reserve in `07:666` to 12 px, and put the first line beside the
  button with `margin-left: 48px; min-height: 44px` (the back link and the dealership label).
  - The pinned test `test_m1_sidebar_pages_clear_the_toggle` checks only that the selector text exists, so keep
    the selectors.
- Banner: stop wrapping (`flex-wrap: nowrap`) and make the close button 44x44.

**After the patch**

- Nothing sits under the button at the top or the bottom of any page.
- The banner is 69 px tall, and the first control is at y=81.
- The car back link sits next to the button (y=12), and the dealership title moves from 75.6 to 61.6.

**Effort and risk:** S. Risk is low to medium because of the "menu only at the top" trade-off.

### PS-04 (P1): the home-page header is 156 px tall and pinned; the hero pushes the search line below the middle

**What was measured**

- `nav.topbar--marketing` is 156 px tall and sticky. That is 18.5% of 390x844, 21.1% of 360x740 and 23.5% of the
  664-px iPhone screen, on every screen while scrolling.
- It has three rows: logo; "Sign in · Get started"; "Browse inventory · Find dealers".
- The headline is 132 px (three lines) at 390 and 176 px (four lines) at 360.
- The search line is at y=450.3 (390) and y=494.4 (360).

**Screenshots**

- Live, 390: `scratchpad/web_shots/ps-home-390-fold.png`
- Live, 360: `scratchpad/web_shots/ps-home-360-fold.png`
- After the patch, 390: `scratchpad/web_shots/ps-fix-home-390-fold.png`
- After the patch, 360: `scratchpad/web_shots/ps-fix-home-360-fold.png`
- After the patch, 1440 (unchanged): `scratchpad/web_shots/ps-fix-home-1440-fold.png`

**Cause**

- Three rows:
  - `08-marketing.css:115-120`, inside the 600-px block, sets `.topbar--marketing .nav-links { width: 100% }`.
  - That applies to both link groups. It defeats the 768-px layout in `11-overrides.css:17-56`, which keeps the
    logo and the account links on one row.
- Pinned: `00-base.css:128-140` sets `.topbar { position: sticky; top: 0 }` (`:136-137`).
- Hero spacing:
  - `12-viewers.css:157`: `.lps-stage { padding: 72px 0 64px }`.
  - `12-viewers.css:169`: `font-size: clamp(42px, 6.2vw, 72px)`, so the headline never goes below 42 px.
  - `12-viewers.css:235`: `.lps-search { margin: 54px auto 0 }`.
  - `12-viewers.css:243`: an 18-px gap inside the search line.

**Fix**

- In `08-marketing.css:117`, narrow the selector to `.topbar--marketing .nav-links--site`.
- At 600 px and under, set `.topbar--marketing { position: relative }`.
  - Use `relative`, not `static`: the trim line `.topbar::after` (`00-base.css:142-152`) is absolutely positioned
    and needs a positioned parent.
- At 560 px and under:
  - `.lps-stage` padding 36px / 44px;
  - headline `clamp(32px, 9.4vw, 42px)`;
  - search margin 30 px;
  - search-line gap 12 px.
- The same header is used by `register.html:16`.

**After the patch**

- The header is 125 px tall and scrolls away.
- The headline is 77 px (390) and 71 px (360), two lines.
- The search line is at y=305 (390) and y=299.1 (360).
- At 1440 every measured value is identical.

**Effort and risk:** S, low.

### PS-05 (P1): fields under 16 px trigger iOS focus zoom

**What was measured (fields under 16 px out of all visible fields)**

| page | fields under 16 px |
|---|---|
| listings empty | 6 of 6 |
| listings search | 4 of 4 |
| search results | 4 of 4 |
| dealership | 4 of 4 |
| car page | 2 of 2 (the menu's search fields) |
| login (third pass) | 4 of 4: the username and password fields are 15 px, and the menu's two search fields 13 px |
| register (third pass) | 2 of 5: the header's search and ZIP fields are 14 px; the form's own three fields are 16.5 px |
| find dealers (third pass) | 3 of 3: the menu's two fields (13 px) and the radius select (15 px) |

The Filters sheet adds the search (15 px) and ZIP, Price, Mileage and Condition (14 px each). iOS Safari zooms
the page whenever a field under 16 px gets focus. The home page already fixed this ("M8",
`12-viewers.css:751-783`: "triggered iOS focus zoom").

**Cause**

| field | size | rule |
|---|---|---|
| ZIP fields (banner and start prompt) | 14 px | `09-components.css:127-131` |
| Sort | 14 px | `09-components.css:454-457` |
| Per page | 14 px | `09-components.css:996-999` |
| Search | 15 px | `11-overrides.css:365-370` (also `02-listings.css:41`, `:573`, `:979`) |
| Sheet ZIP and selects | 14 px | `11-overrides.css:383-388` |
| Menu search | 13 px | `07-dealer-admin.css:402-412` |
| Login username and password (third pass) | 15 px | `01-nav.css:589-603` (`font-size: 15px` on line 598) |
| Register header search and ZIP (third pass) | 14 px | `01-nav.css:119-134` (line 129) and `:181-196` (line 190) |
| Find dealers radius select (third pass) | 15 px | `10-pages.css:461-471` (line 468; the select and its ZIP and city inputs share the rule) |

**Fix:** set those selectors to 16 px at 960 px and under. For the login form that is one line,
`.field-group input { font-size: 16px; }` at `01-nav.css:598` (tested in `ps3_fixes.css`: 15 px → 16 px, height
48.8 → 50 px). The register form already uses 16.5 px (`13-landing-auth.css:143-155`); the sign-in form still uses
the older bordered, rounded input (`01-nav.css:589-603`), so the two sibling forms also look different.

**After the patch:** no field under 16 px on any phone page (0/4, 0/4, 0/2, 0/3); with the third-pass rule the
login fields are 16 px.

**Effort and risk:** S, none.

### PS-06 (P2): tap targets under 44 px

**What was measured**

Listings search, 390: 88 of 113 targets under 44 px, 51 under 24 px.

| target | size | rule |
|---|---|---|
| dealer link, on every card | 16 px tall | `02-listings.css:1327-1328`, 12-px text, no padding |
| "Compare" label, on every card | 72x23 | `09-components.css:1104-1111`, padding 4px 0 |
| heart / save button, on every card | 32x32 | `09-components.css:781-786` |
| filter chips | 24 px | `09-components.css:646-656` |
| "Clear all" | 28 px | `09-components.css:675-677` |
| pagination | 34 px and 40 px | `09-components.css:1032-1033`, `:1064-1066`, `10-pages.css:261-263` |
| "Save this search" / "Saved searches" | 34 px | `09-components.css:468-480`, padding 0 |
| "Filters" | 40 px | `09-components.css:422-426` |
| per-page and sort selects | 39 px | `09-components.css:454-457`, `:996-999` |
| banner "✕" | 22.7x18 | `01-nav.css:341-347` |
| ZIP banner "✕" | 22.7x22 | `09-components.css:145-154` |
| "Use my location" | 32 px | `02-listings.css:1461-1465` |

The three per-card targets repeat on all 24 cards, which accounts for 72 of the 88.

Car page, 390: 12 of 22 under 44 px.

| target | size | rule |
|---|---|---|
| "Back to results" | 18 px tall | `11-overrides.css:278-291`, padding 0 |
| photo arrows | 40x40 | `11-overrides.css:241-243` shrinks the 48 px of `03-car-page.css:1592-1597` to 40 at 480 px and under |
| arrows in the extra control row | 40x40 | `03-car-page.css:1823-1825` |
| "Compare" | 35 px | `10-pages.css:180-184` |
| dealer link, "Share" and bottom-bar button | 37 px | `03-car-page.css:292-293`, `:311-312` |
| "Sign in to save" | 42 px | `05-car-tools.css:844-848` |

Dealership, 390: 95 of 121 under 44 px. This includes Leaflet's 30x30 zoom buttons. Outside the 24 cards, the
call and directions links are the ones that matter on a phone: the phone number (114x19), the website (290x19),
Google Maps, Apple Maps and Waze (24.4 px tall) and "Sign in to write a review" (17 px). They are PS-21.

Home, 390: 5 of 19 under 44 px: the logo link (148x21), "See all 268,800+ →" (131.8x18) and the three footer links
Listings, Sign in and Register (16 px tall each). The first patch fixes the footer links only (to 44 px); the logo and
"See all" stayed under 44. Third pass: an invisible 44-px tap area (`::after`, the same pattern as the dealer links)
raises the hit height of both from 16 to 40 px or more with no change in layout (header 156 px and page height
3,574 px, both as before). A `min-height: 44px` on the logo instead made the header 23 px taller (156 → 179 px) and
the page 49 px longer with "See all", so the invisible area is the right form.

Third-pass pages, 390 (taps under 44 of all, from `out/<page>-390.json` in the third-pass folder):

| page | under 44 | what |
|---|---|---|
| login | 3 of 6 | "Show password" 36x36 (`01-nav.css:617-634`), "Create one" 72.8x17, "Login" 358x43 |
| register | 5 of 16 | logo 148x21, header search field and ZIP label 34.5 px (PS-22 removes them from phones), "Show password" 36x36, "Sign in" 45.9x19 |
| find dealers | 8 of 10 | three location tabs 30 px tall, radius select and "Search dealers" 43 px, map zoom 30x30 (x2), "Leaflet" credit 14 px |
| compare (empty) | 2 of 3 | "Back to inventory" 148x18 (class `.vdp-back`, `compare.html:22`, so the first patch's tap area already covers it), "Browse inventory" 156x43 |
| premium | 1 of 2 | "free account" 91.9x19, a link inside a sentence |

The tested third-pass rules make the login toggle 44x44 and give "Create one" a 40-px or taller hit area; the
tab height on find dealers is fixed in PS-24.

**Screenshots**

- Live: `scratchpad/web_shots/ps-listings-search-390-full.png`
- After the patch: `scratchpad/web_shots/ps-fix-listings-search-390-full.png`

**Fix (960 px and under)**

- `min-height: 44px` on controls that already have a box, and 44x44 for the heart and the photo arrows.
- For text links, an invisible 44-px tap area added with `::after`, so nothing moves on screen: dealer link,
  "Back to results", the car's dealer link, "Use my location" and the ZIP "✕".
- Leaflet: `.leaflet-touch .leaflet-bar a` to 44 px.

**After the patch**

- Hit-testing ±12 and ±20 px above and below each link's centre: 24 of 24 dealer links now have a tap area of 40 px
  or more (0 of 24 before); "Back to results" and the car's dealer link 1 of 1 each (0 before).
- Measured boxes under 44 px:
  - listings search: 26 of 113, made up of the 24 dealer links (box still 16 px, tap area 40 px or more) and the 2
    links inside the banner sentence;
  - car page: 2 of 20, the car's dealer link (tap area now 40 px or more) and one link inside a sentence;
  - dealership: 33 of 120, made up of the 24 dealer links, the map marker and 8 other links. The first draft of this
    report called those 8 "review-section links"; the third pass listed them (`fix-after-dealership-390.json`): the
    phone number, the website, Google Maps, Apple Maps, Waze, "Sign in to write a review", and Leaflet's two
    attribution links ("Leaflet", "OpenStreetMap"). Six of them are fixed in PS-21; the attribution links are the
    map library's own credit line and are left alone.

**Cost (measured in the second pass, 390):** the first card grows from 377 to 391 px (the Compare / photos / heart
row is now 44 px tall, and PS-13 raises the 11-px card text to 12 px). Over 24 cards that makes the listings-search
page 267 px longer (10,161 → 10,428; 10,447 at 360), the search-results page 230 px longer (10,370 → 10,600) and the
dealership page 314 px longer (11,573 → 11,887). The fixed car-page bottom bar grows from 62 to 69 px because its
"View listing" button also gets the 44-px minimum; the 80-px clearance under the car page still covers it.

**Effort and risk:** S, low. The invisible tap areas overlap neighbours by up to 14 px, so test a tap on each card.

### PS-07 (P2): side margins differ by page

**What was measured**

| page | left margin | right margin |
|---|---|---|
| listings | 12 px | 16 px |
| car page | 16 px | 16 px |
| dealership heading | 16 px | — |
| dealership car grid | 28 px | 16 px |
| home and guest banner | 20 px | 20 px |
| register (third pass) | 22 px | 22 px |
| compare (third pass) | 24 px | 24 px |
| login, find dealers, premium (third pass) | 16 px | 16 px |

Across the 12 pages measured at 390 px the left text margin takes six values: 12, 16, 20, 22, 24 and 28 px. The
compare page's 24 px is `.compare-page-section { padding: 32px 24px 80px }` (`10-pages.css:4-6`), register's 22 px
is `.rgs-stage { padding: 40px 22px 56px }` (`13-landing-auth.css:344-348`, 560 px and under), and neither was
touched by the first patch. Using 16 px on both would match the listings (after the first patch), car, login and
find-dealers pages (this one-value change was not tested).

At 1440 the same rule indents the dealership filters 24 px from the heading (grid left edge 598, against 574
after the fix).

**Screenshots**

- Live, dealership: `scratchpad/web_shots/ps-dealership-390-fold.png`
- After the patch: `scratchpad/web_shots/ps-fix-dealership-390-fold.png`
- After the patch, 1440: `scratchpad/web_shots/ps-fix-dealership-1440-fold.png`

**Cause**

- `07-dealer-admin.css:692-695` sets `padding-left: clamp(12px, 2vw, 24px)` on
  `body.listings-page:has(.app-sidebar) .listings-layout`.
- That selector is more specific than the two rules meant to apply:
  - the 16 px of `11-overrides.css:259-262` (960 px and under);
  - the `padding: 0` of `15-dealership.css:159-164`, whose comment says it exists "so the filters sit inside the
    research page's own 1180px column".

**Fix:** wrap `07:692-695` in `@media (min-width: 961px)` and exclude `.dealership-research-page`.

**After the patch:** listings 16/16; the dealership grid starts at 16, level with its heading; at 1440 the
dealership filters line up with the heading at x=276. The listings desktop margin is unchanged.

**Effort and risk:** S, low.

### PS-08 (P2): the car page stack pushes title and price below the first screen

**What was measured (390)**

- "Back to results" at y=56.
- The photo block runs from 90 to 498. It includes a second control row "‹ 1 of 3 ›" at y=370. That row repeats
  the arrows already on the photo and the "1 / 3" counter on the photo.
- Thumbnails from 426 to 498, then a 48-px gap.
- The dealer line at 546, the title at 615 and the price at 713.
- The "Sign in to save · Compare" row is 120 px tall because the two buttons are stacked.
- At 360x740 the fixed bottom bar covers "$900 · 276,924 mi" on the first screen.
- On a 664-px iPhone screen the price (713) is below the fold.

**Screenshots**

- Live, 390: `scratchpad/web_shots/ps-car-390-fold.png`
- Live, 360: `scratchpad/web_shots/ps-car-360-fold.png`
- After the patch, 390: `scratchpad/web_shots/ps-fix-car-390-fold.png`
- After the patch, 360: `scratchpad/web_shots/ps-fix-car-360-fold.png`
- After the patch, 1440 (unchanged): `scratchpad/web_shots/ps-fix-car-1440-fold.png`

**Cause**

- The extra control row: `03-car-page.css:1808-1813` (`display: flex; margin-top: 12px`), template
  `car.html:225` at N37a. The arrows on the photo come from `car.html:209` and the counter from
  `03-car-page.css:341-344`.
- The 48-px gap: `03-car-page.css:79` (24-px gap between hero blocks) plus `05-car-tools.css:1034`
  (`.car-detail-info { padding: 24px 0 32px }`).
- The stacked buttons: `11-overrides.css:178-182` (`.car-detail-price-row { flex-direction: column }`).
- The 56-px reserve from PS-03.

**Fix**

- At 640 px and under, hide `.car-gallery-controls`. The arrows on the photo, swiping, the thumbnails and the
  counter all remain.
- At 640 px and under, set `.car-detail-info { padding-top: 0 }`.
- At 600 px and under, lay out the price row as `row wrap`.
- PS-03 puts the back link beside the menu button.

**After the patch**

| | before | after |
|---|---|---|
| title, 390 / 360 | 615.1 / 608.6 | 521.1 / 514.6 |
| price, 390 / 360 | 713.4 / 706.9 | 619.4 / 612.9 |
| Save · Compare row | 120 px | 75 px |
| page height, 390 | 2037 | 1984 |

On a 664-px iPhone screen the price is still under the bottom bar. Fixing that would need a shorter photo block,
which this patch does not propose.

**Effort and risk:** S, low.

### PS-09 (P2): the empty listings page asks for the ZIP twice and shows a toolbar with nothing to sort

**What was measured (390)**

- The ZIP banner is 145 px tall at y=149.
- The toolbar is 154 px at y=312, with "Save this search", "Per page" and "Sort" although there are no results.
- The start prompt begins at 481.7 and has its own ZIP field at y=683.
- Between the toolbar's bottom line (y=465.7) and the start prompt's top line (y=513.7) there are 48 px of empty
  space: two lines with nothing between them.
- At 360x740 the start prompt's ZIP field is at y=724.5–764.5 and its button at 772.5, so the main action is below
  the first screen.

**Screenshots**

- Live, 390: `scratchpad/web_shots/ps-listings-empty-390-fold.png`
- Live, 360: `scratchpad/web_shots/ps-listings-empty-360-fold.png`
- After the patch, 390: `scratchpad/web_shots/ps-fix-listings-empty-390-fold.png`
- After the patch, 360: `scratchpad/web_shots/ps-fix-listings-empty-360-fold.png`

**Cause**

- Two prompts in the template: `listings.html:362-379` (banner) and `:435-453` (start prompt). `main.js`
  `resolveListingsGeo` shows both.
- Spacing: `02-listings.css:1423-1426` (`.listings-start-prompt { padding: 32px 0 48px }`) stacked on
  `02-listings.css:1377-1384` (the start prompt's own block: 36 px of padding and a top line).

**Fix (CSS only)**

- While the start prompt and its ZIP field are showing, hide `#listings-zip-banner` with `:has()`. The site already
  uses `:has()` in `07-dealer-admin.css:240`.
- While the start prompt is showing, also hide the saved-search buttons and the per-page/sort group. "Filters" stays.
- At 600 px and under, set `.listings-start-prompt { padding-top: 0 }`.

**After the patch:** one ZIP prompt; the toolbar shrinks to 54 px with only "Filters"; the start prompt moves from
481.7 to 215 at both widths; page height drops from 969 to 891 (390) and from 1020 to 787 (360).

**Effort and risk:** S, low.

### PS-10 (P2): the dealership Filters sheet stops 112 px above the bottom of the screen

**What was measured:** on the dealership page the open sheet runs from top=12 to 732 at 390x844, leaving a 112-px
gap at the bottom with the blurred page showing under "Show results". At 360x740 the gap is 76.8 px. On
`/listings` the gap is 0.

**Screenshots**

- Live, 390: `scratchpad/web_shots/ps-dealership-390-filters-open-fold.png`
- After the patch: `scratchpad/web_shots/ps-fix-dealership-390-filters-open-fold.png`

**Cause**

- `15-dealership.css:164` sets `body.dealership-research-page .listings-sidebar { top: 12px; }`. That is the desktop
  sticky offset.
- It has the same specificity as `09-components.css:901-904` (`top: auto`) and loads later, so it wins.
- This is the "M7" bug again (`09-components.css:897-900`), only on this page.
- The pinned test `test_m7_filter_sheet_is_bottom_anchored` checks the text of `09-components.css`, so it cannot
  see this.

**Fix:** wrap `15-dealership.css:164` in `@media (min-width: 961px)`.

**After the patch:** the sheet top is at 124 (390) and 88.8 (360), with a 0-px gap.

**Effort and risk:** S, none.

### PS-11 (P3): text that wraps or is cut off

| problem | measured | cause | fix |
|---|---|---|---|
| Home closing buttons wrap to 2 lines | "Browse inventory" and "Create account" are 68 px tall each | `12-viewers.css:651-658` (`padding: 14px 30px`) and `:745-747` (`flex: 1` on halves 168 px wide) | at 560 px and under, `padding: 14px; white-space: nowrap`; after the patch they are one line |
| Home headline too tall | 3 lines (132 px) at 390, 4 lines (176 px) at 360 | `12-viewers.css:169` | PS-04; 77 / 71 px after |
| Home search placeholder cut off | the input is 207 px wide at 390 and shows "White BMW X5 with M S" | `12-viewers.css:240-245` (18-px gap), `:286-299` | the 12-px gap still cuts it ("…M Sp"); shorten the phone placeholder in `landing.html:28` (template) |
| Car dealer line in capitals wraps | 2 lines at 390, 3 at 360 | `01-nav.css:379-382` (13 px, +0.18em) and `03-car-page.css:90-92` (+0.14em) | at 600 px and under, `letter-spacing: 0.04em` (not in the tested patch) |
| Menu search row cramped | query field 131 px wide, ZIP field 48 px wide, both 33 px tall | `07-dealer-admin.css:352-357` (the row's grid, `minmax(0, 1fr) 48px 28px`), `:402-412` (fields) | font size fixed in PS-05; give the ZIP field its own row (not in the tested patch) |

Screenshots for this table:

- Home, live: `scratchpad/web_shots/ps-home-390-full.png`
- Home, after the patch: `scratchpad/web_shots/ps-fix-home-390-full.png`
- Menu open, live: `scratchpad/web_shots/ps-listings-empty-390-menu-open-fold.png`

### PS-12 (P3): home-page sections use desktop-sized spacing on phones

**What was measured:** the hero and the next three sections each carry 64–96 px of padding top and bottom. That is
628 px of padding in a 3,574-px page at 390.

**Cause**

| section | padding | rule |
|---|---|---|
| `.lps-stage` | 72 / 64 px | `12-viewers.css:154-157` |
| `.lps-dialogue` | 72 / 72 px | `12-viewers.css:423-425` |
| `.lps-data` | 84 / 84 px | `12-viewers.css:475-477` |
| `.lps-lot` | 84 / 96 px | `12-viewers.css:514-516` |

**Fix:** at 560 px and under, use 48 / 56 / 56–64 px.

**After the patch (together with PS-04):** page height 3,574 → 3,256 (390) and 3,648 → 3,280 (360).

**Effort and risk:** S, none.

### PS-13 (P3): text under 12 px on phones

**What was measured**

| page | text runs under 12 px | what |
|---|---|---|
| listings search | 73 | 71 card text runs at 11 px (trim, spec line, photo count) and two menu labels at 10 px |
| search results | 75 | 72 card text runs at 11 px, two menu labels at 10 px and one certified pre-owned label at 10 px |
| car page | 9 | six spec labels at 10 px, the payment label at 11 px, two menu labels at 10 px |
| home | 5 | two field labels at 10.5 px, three small headings at 11.5 px |
| dealership | 75 | 72 card text runs at 11 px, two menu labels at 10 px and the page label at 11.52 px |

**Cause**

- Cards: `02-listings.css:1221-1224`, `:1268-1269`, `:1320-1321`.
- Certified pre-owned label: `02-listings.css:1258-1260`.
- Car spec labels: `03-car-page.css:1134-1136`. Payment label: `05-car-tools.css:25-27`.
- Home: `12-viewers.css:160-161`, `:332-333`, `:401-402`.
- Menu labels: `07-dealer-admin.css:342-345`, `:472-474`.
- Dealership label: `15-dealership.css:8`.

**Fix:** a 12-px minimum at 600 px and under.

**After the patch:** 0 on listings search, car, home, dealership and listings empty. Search results keeps 1: the
certified pre-owned label is not in the patch.

**Effort and risk:** S, low (cards grow by about 1–2 px per line).

### PS-14 (P3): the fixed car-page bottom bar hides the version footer at the end of the page

**What was measured:** scrolled to the bottom, the footer text "v1.5.8" sits at y 810.7–825.7 while the bar starts
at 782 (390). At 360 it is 707.4–722.4 against 678. The text is hidden.

**Cause:** the bar is defined in `09-components.css:924-934`. Only `.car-detail` gets the 80-px clearance
(`09-components.css:963-965`), and the footer is outside it.

**Fix:** give `body.car-detail-page .app-version-footer` the same `padding-bottom` at 960 px and under.

**After the patch:** the footer text is at 748.7–763.7 against a bar starting at 775 (390), and 645.4–660.4 against
671 (360). No longer covered.

**Effort and risk:** S, none.

### PS-15 (P3): the fallback for broken car photos is blocked, so failed photos leave empty boxes

**What was measured**

- The browser console logged "Executing inline event handler violates the following Content Security Policy
  directive 'script-src 'self' 'nonce-…''":
  - once on the listings search;
  - twice on search results;
  - at both 390 and 1440.
- Three dealer photos failed with `net::ERR_BLOCKED_BY_ORB`: one cleveland-nissan.com PNG and two
  randymariongmc.net JPGs. The two randymariongmc.net photos are listing-card images on those same pages, so the
  console errors match the failed card photos.
- The home page's "On the lot" strip has a separate gap. It draws each photo as a CSS background
  (`landing.html:92`), which has no fallback at all. The failed Cleveland Nissan photo shows as an empty beige
  box: `scratchpad/web_shots/ps-home-390-full.png`

**Cause:** `frontend/static/listings/card.js:157` at N37a (`:152` on live) uses an inline handler,
`<img ... onerror="this.onerror=null;this.src='/static/placeholder.svg';">`. The site's security policy refuses
inline handlers, so the placeholder never loads. This is the same class of problem as F1 in
`docs/structural_eval_2026_11_04/frontend-css-templates.md`.

**Fix**

- Listing cards: attach the error handler from script instead. For example, one capture-phase `error` listener
  on `#results-grid` that swaps in the placeholder image. This is a JavaScript change, not CSS.
- Home strip: give `.lps-car-img` a placeholder image layered under the photo (a second background layer).

**Effort and risk:** S, low.

### PS-16 (P3): the ZIP row of the empty listings page is wider than its box below 380 px

Found in the second pass (offline replay, `extra_checks.py` and `extra_checks2.py`).

**What was measured**

- The row that holds the ZIP field and "Set location" (`.listings-zip-banner__controls`) is 298.1 px wide at 320 and
  at 360 (input 172 + gap 8 + button 118.1). At 390 it is 308 px and fits. Its box is the viewport width minus 82 px
  (12 px layout padding, 14 + 40 px banner padding, 16 px layout padding).
- How far the row spills past its box (`zipthreshold.py`, offline replay at seven widths):

  | viewport width | 320 | 360 | 370 | 375 | 380 | 385 | 390 |
  |---|---|---|---|---|---|---|---|
  | row width, px | 298.1 | 298.1 | 298.1 | 298.1 | 298.1 | 303 | 308 |
  | spill into the banner's right padding, px | 60.1 | 20.1 | 10.1 | 5.1 | 0.1 | 0 | 0 |
  | past the screen edge, px | 4.1 | 0 | 0 | 0 | 0 | 0 | 0 |

- 320 px: the row's right edge is at 324.1, which is 4.1 px past the screen. The page does not scroll sideways
  because `html` clips, so the right edge of the "Set location" button is cut off. The audit counts 2 overflowing
  elements.
- 360 px: the right edge is also at 324.1, against a box edge of 304. The button spills 20.1 px into the banner's
  right padding. Nothing is cut off, but the button no longer lines up with the text column above it.
- At 320 px the toolbar's Sort select is 102.5 px wide and shows "Best ma".

**Screenshots**

- 320, live replay: `scratchpad/web_shots/ps2-listings-empty-320-zipbanner.png`
- 320, with the fix: `scratchpad/web_shots/ps2-fix-listings-empty-320-zipbanner.png`
- 360, live: `scratchpad/web_shots/ps-listings-empty-360-fold.png`

**Cause**

- `09-components.css:171-173` (inside the 700-px block at `:162`) sets `.listings-zip-banner__controls { flex: 1 1 100% }`.
  As a flex item it keeps `min-width: auto`, so it cannot shrink below its content. The input's own width
  (`:127-129`, 7.5rem) is reset to `auto` at `:175-179`, so the input falls back to its intrinsic 172 px.

**Fix:** add `min-width: 0` to `.listings-zip-banner__controls` in that block. The input already has `min-width: 0`,
so it then shrinks to fit.

**After the patch**

- 320: the row is 238 px wide and ends at x=264, which is the box edge. The overflow count goes from 2 to 0. The
  input is 111.9 px and the button 118.1 px.
- 360: 278 px wide, ending at the 304 box edge. 390: unchanged (308 px, ending at 334).
- With the whole tested patch at 320 the Sort select is 167 px wide (it was 102.5), so "Best match" is not cut.
- PS-09's fix hides this banner while the start prompt shows its own ZIP field. In the replays that is the only
  state where the banner appears, so with PS-09 applied this rule is a guard. It is still safe on its own.

**Effort and risk:** S, none (one declaration; checked at 320, 360 and 390).

### PS-17 (P3): the home footer links are indented 22 px past the left margin

Found in the second pass while reading the crop of the bottom of the home page.

**What was measured (390 and 360, same values)**

- The © line starts at x=20. The first footer link, "Listings", starts at x=42. "Sign in" starts at 114.9 and
  "Register" at 182.2. Each link is 16 px tall.

**Screenshots**

- Live, bottom of the home page (crop): `scratchpad/phone_audit/crops/ps-home-390-full-01.png`
- Before, 390: `scratchpad/web_shots/ps2-home-390-footer-before.png`
- After, 390: `scratchpad/web_shots/ps2-home-390-footer-after.png`
- Before, 360: `scratchpad/web_shots/ps2-home-360-footer-before.png`
- After, 360: `scratchpad/web_shots/ps2-home-360-footer-after.png`
- Side by side, 390 (before left, after right): `scratchpad/verify/crops/footer-before-after-390.png`

**Cause**

- `12-viewers.css:700-705`: `.lps-foot-links a { margin-left: 22px }`. On desktop the link group sits at the right
  and the 22 px separates the links. On phones `.lps-foot-links` wraps (`:687-698`, `flex-wrap: wrap`), the link
  group drops under the © line, and its first link keeps the left margin.

**Fix (560 px and under):** `.lps-foot-links a { margin-left: 0; margin-right: 22px }`. The tested patch already
makes the links 44 px tall in the same block (`.lps-foot-links a { display: inline-flex; min-height: 44px }`).

**After the patch:** the links start at x=20, 92.9 and 160.2, each 44 px tall, level with the © line at x=20. Same at 360.

**Effort and risk:** S, none.

### PS-18 (P2, not reproduced here): the menu drawer is as tall as `100vh`

Found in the second pass by reading the CSS. This is a risk, not a measured defect, because Chromium cannot show it.

**What was measured**

- At 390x844 the drawer is 844 px tall. Its pinned "Sign in" and "Get started" links sit at y 751–786.5 and
  788.5–824, which is 20 px above the drawer's bottom edge.
- At 360x740 they are at 647–682.5 and 684.5–720. At 390x664 (Playwright's iPhone 13 and 14 profile, the area Safari
  shows with its toolbars) the drawer is 664 px tall and they are at 571–606.5 and 608.5–644.

**What was not measured:** Chromium's emulation has no collapsing browser toolbar, so `100vh` equals the visible
height there and nothing is hidden. In iOS Safari `100vh` is the taller height with the toolbars collapsed, so with
a toolbar showing, the bottom of a `100vh` fixed box sits under it. If that happens here, the pinned "Sign in" and
"Get started" (the drawer's only sign-in and sign-up links) are the part hidden, and the drawer's content is too
short to scroll them into view. Confirm on a real iPhone before changing anything.

**Screenshot of the drawer (Chromium, 390x844):** `scratchpad/web_shots/ps-listings-empty-390-menu-open-fold.png`

**Cause:** `07-dealer-admin.css:263-283`: `.app-sidebar { position: fixed; top: 0; min-height: 100vh;
max-height: 100vh; height: 100vh; overflow-y: auto }`. The site already uses `100dvh` once, at `02-listings.css:512`.

**Fix:** keep the three `100vh` lines and add a `100dvh` line after each (`height: 100dvh;` and so on), so browsers
without `dvh` keep today's behaviour.

**Effort and risk:** S, none.

### PS-19 (P2, third pass): the compare tray is unreadable, taller than its reserved space, and covers the car page's bottom bar

Found by ticking "Compare" on cards, a state the first two passes never opened. `states3.py compare`, offline replay,
two real clicks on the first two cards' "Compare" labels (four for the second run).

**What was measured**

- **Contrast.** The car titles in the tray are `rgb(255, 255, 255)` on the tray's `rgb(250, 248, 245)`: 1.06 : 1. The
  "×" is `rgba(255, 252, 248, 0.75)`: 1.04 : 1. The text is there and cannot be read. WCAG AA asks for 4.5 : 1. The same colours
  show at 1440 (tray 72 px tall, one row), so this is not phone-only.
- **Height against reserve.** At 390x844 and 360x740 the tray is 120 px tall with two cars (two rows) and 168 px with
  four (the slots wrap onto a second row, 88 px for the slots alone). The page keeps `padding-bottom: 72px`. Scrolled to the bottom
  the tray covers the version footer.
- **Car page.** At 390 the tray (`z-index: 85`) fills y 724–844 and the fixed "View listing" bar (`z-index: 50`) is y 782–844, so the
  tray lies over the whole bar and over the price row above it. At 360 the figures are 620–740 and 678–740.
- **Tap sizes.** The remove "×" is 13.3x16 px and "Clear all" is 17 px tall.
- **Thumbnails.** The two grey squares are never a photo. The tray's slot is built with
  `' style="background-image:url(\'' + safeImg + "')"`, which never closes the attribute, so the browser reads
  `style="background-image:url('https://…/UA.jpg') aria-hidden="` and drops the declaration. The computed `background-image`
  is `none` for both slots, and `aria-hidden` is lost.

**Screenshots**

- Base, two cars, listings: `scratchpad/web_shots/ps3-compare-base-listings-390-fold.png`
- Base, four cars: `scratchpad/web_shots/ps3-compare-base-n4-listings-390-fold.png`
- Base, car page: `scratchpad/web_shots/ps3-compare-base-car-390-fold.png`
- With the fix, four cars: `scratchpad/web_shots/ps3-compare-fix-n4-listings-390-fold.png`
- With the fix, car page: `scratchpad/web_shots/ps3-compare-fix-car-390-fold.png`

**Cause (phase/N37a)**

- Colours written for a dark tray: `10-pages.css:149` (`border-left: 1px solid rgba(255, 252, 248, 0.2)`), `:164`
  (`color: #fff`) and `:173` (`color: rgba(255, 252, 248, 0.75)`). The tray itself is light: `09-components.css:1133`
  (`background: rgba(250, 248, 245, 0.97)`).
- Reserve: `09-components.css:1183-1185` (72 px on the listings page), `:1187-1189` (140 px on the car page; the bar adds 62 px
  and the tray 120 px), `10-pages.css:268-272` (64 px under the pagination at 700 px and under).
- Stacking: tray `09-components.css:1126-1137` (`position: fixed; bottom: 0; z-index: 85`) against the bar
  `09-components.css:924-937` (`position: fixed; bottom: 0; z-index: 50`, inside the 960 px block).
- Thumbnail: `frontend/static/compare.js:204`, the same line on live and at `phase/N37a`.

**Fix (tested, `ps3_fixes.css` block PS-19)**

- Edit the three declarations at `10-pages.css:149,164,173` to `var(--border-light)`, `var(--navy)` and `var(--text-muted)`.
  Contrast becomes 11.77 : 1 for the title and 5.39 : 1 for the "×".
- At 600 px and under: reserve 132 px (`09-components.css:1184`); let the slots scroll sideways in one strip
  (`flex-wrap: nowrap; overflow-x: auto`, `flex: none` on a slot) so four cars no longer wrap to 168 px. The tray stays
  121 px.
- At 960 px and under, on the car page: lift the tray above the bar with one custom property for the bar's height,
  `body.car-detail-page { --car-cta-h: 62px }` and `.compare-tray { bottom: calc(var(--car-cta-h) + env(safe-area-inset-bottom)) }`,
  and make the `.car-detail` reserve at `:1188` `calc(var(--car-cta-h) + 140px + …)`. The bar is 62 px; the first patch
  makes it 69 px (PS-06), so set `--car-cta-h: 69px` if that patch is applied. Tested three ways at 390 and 360
  (`compare_both.py`): first patch alone, the tray (still `bottom: 0`) lies over the whole 69-px bar; third-pass rules alone,
  tray 661–782 over a 62-px bar 782–844, no overlap; both with `--car-cta-h: 69px`, tray 654–775 over bar 775–844, no overlap
  (360: 550–671 over 671–740).
- "×" and "Clear all": a 44-px tap area (`::after { inset: -14px -16px }`, `min-height: 44px`). The visible size does not change.
- `compare.js:204`: close the attribute, `... + safeImg + "')\""`. Add a unit test that the slot's `style` attribute parses.

**After the patch (final run)**

| | before | after |
|---|---|---|
| title / "×" contrast | 1.06 / 1.04 | 11.77 / 5.39 |
| tray height, 2 cars, 390 and 360 | 120 | 121 |
| tray height, 4 cars, 390 | 168 | 121 |
| page bottom padding, 390 | 72 | 132 |
| what the tray covers at the page bottom, 390 and 360 | the version footer | nothing |
| car page, 390: tray / bar | 724–844 / 782–844 (overlap) | 661–782 / 782–844 |
| car page, 360: tray / bar | 620–740 / 678–740 (overlap) | 557–678 / 678–740 |
| "Clear all" height | 17 | 44 |
| 1440: tray height, page height | 72, 5,807 | 72, 5,807 (colours only) |

**Effort and risk:** S. Low. The sideways strip is a new gesture on a phone with three or four cars.

### PS-20 (P2, third pass): the menu drawer's links and fields are under 44 px

The drawer is the only navigation on every non-home phone page. The first pass measured it
(`out/listings-empty-390.json`, `menu_metrics`) but did not list it.

**What was measured (drawer open, `/listings` at 390x844 and 360x740, the car page at 390; `third_tests.py drawer`)**

| target | before | after |
|---|---|---|
| logo link "Sarrafi Cars" | 108.5x19 | 108.5x44 |
| close "×" | 22x32 | 44x44 |
| Browse, Compare, Dealers, Sign in, Get started (each) | 235x35.5 | 235x44 |
| search field | 131x33.3 | 169x44 |
| ZIP field | 48x33.3 | 219x44 (own row) |
| search submit | 28x36 | 44x44 |
| drawer content against drawer height (390 / 360) | 844 of 844 / 740 of 740, no scroll | same, no scroll |

**Screenshots**

- Before: `scratchpad/web_shots/ps3-drawer-base-listings-empty-390-fold.png`
- After: `scratchpad/web_shots/ps3-drawer-fix-listings-empty-390-fold.png`
- 360, before and after: `scratchpad/web_shots/ps3-drawer-base-listings-empty-360-fold.png`, `scratchpad/web_shots/ps3-drawer-fix-listings-empty-360-fold.png`

**Cause (phase/N37a, `07-dealer-admin.css`)**

- Links: `:485-497`, `padding: 9px 10px 9px 8px; font-size: 14px; line-height: 1.25` = 17.5 + 18 = 35.5 px.
- Logo `:301-308` (no padding, 1.05rem); close `:314-323` (`font-size: 1.5rem; line-height: 1; padding: 4px`).
- Search row `:352-357` (`grid-template-columns: minmax(0, 1fr) 48px 28px`), field `:402-415`
  (`padding: 8px 4px 8px 24px; font-size: 13px`), submit `:434-447` (`min-height: 36px`).

**Fix (tested, 900 px and under)**

- `.app-sidebar__link { min-height: 44px }`; logo `inline-flex; min-height: 44px`; close `min-width/min-height: 44px`.
- Search row as two lines: field and a 44-px submit on the first, the ZIP field across the second
  (`grid-template-columns: minmax(0, 1fr) 44px`, ZIP `grid-column: 1 / -1; grid-row: 2`), inputs `min-height: 44px`.
  This is the "give the ZIP field its own row" change PS-11 left untested.

**Effort and risk:** S, low. The drawer still fits without scrolling at 844 and 740 (scroll height equals client height,
and the pinned "Sign in" and "Get started" end 20 px above the bottom edge at 390x844). On a 664-px iPhone screen check
that they still show (PS-18).

### PS-21 (P2, third pass): the call, website and directions links on the dealership page are 19 to 24 px tall

**What was measured (390 and 360; `third_tests.py dealer`)**

| link | box | hit height (largest band that still hits the link) |
|---|---|---|
| phone number | 114x19 | 16 px |
| website | 290x19 | 16 px |
| Google Maps / Apple Maps / Waze | 91.7 / 82.8 / 38.6 x 24.4 | 24 px |
| "Sign in to write a review" | 165x17 | 16 px |

The first patch left all of them (see the correction in PS-06). On a dealer page these are the actions a phone user came for.

**Cause:** `15-dealership.css:28-34`. `.dealer-research__contact { display: grid; gap: .5rem }`; its links
(`a { color: inherit }`) have no padding and sit inside "Website: …" and "Phone: …" rows 27 px apart.
`.dealer-research__nav a { padding: .2rem 0; font-size: .9rem }`. The sign-in link is `:138-140`.

**Fix (tested, 960 px and under)**

- Give each contact row 12.5 px of padding above and below (44-px pitch) and set `gap: 0`.
- Give every link above an invisible 44-px tap area (`position: relative` on the link and an `::after` 44 px tall, `top: 50%`).
- A first try with only the tap areas left the website link at 8 px, because the phone link's area lies over it (rows 27 px apart); hence the row padding.
- After the patch: all six links hit at 40 px or more at 390 and 360 and are unchanged at 1440. The page is 59 px
  taller at 390 (the contact rows are now 44 px apart instead of 27).

**Effort and risk:** S, low.

### PS-22 (P2, third pass): the register page's header is 200 px tall and pinned, and sign-in has a different header

**What was measured (`third_tests.py register`, `capture3.py`)**

- `nav.topbar.topbar--marketing` is 200.5 px tall and sticky: 23.8% of 390x844, 27.1% of 360x740. At 1440 it is 83.5 px.
  In landscape (844x390) it is 110.5 px, 28.3% of the screen.
- It has four rows: the logo; "Sign in" and "Get started"; a search line whose placeholder is cut mid-word ("Accord with sunroof unc"), with
  "Search" and a ZIP field; "Browse inventory" and "Find dealers". The page heading starts at y=268.5.
- The home page has the same header without the search row. `/login`, the next page in the same flow, has no top bar: a
  44-px menu button and the drawer.

**Screenshots:** before `scratchpad/web_shots/ps3-register-base-390-fold.png`; after (first patch plus the rule below)
`scratchpad/web_shots/ps3-register-both-390-fold.png`; page at 360 and 1440: `scratchpad/web_shots/ps3-register-360-fold.png`, `scratchpad/web_shots/ps3-register-1440-fold.png`.

**Cause:** `_nav_marketing.html:9` includes `_nav_search.html` in the header. Only the landing page hides it
(`13-landing-auth.css:379-382`, `.lps-landing .topbar .nav-search { display: none }`); the register page has no
`lps-landing` class. The header is pinned by `00-base.css:128-140` and the second row comes from `08-marketing.css:115-120` (both PS-04).

**Fix (tested)**

- At 600 px and under, `.topbar--marketing .nav-search { display: none }`.
- With the first patch's PS-04 rules (`.nav-links--account` shares the logo's row; `position: relative`):

| header at 390 and 360 | height | pinned |
|---|---|---|
| now | 200.5 | yes |
| search row hidden only | 156 | yes |
| first patch only | 169.5 | no |
| first patch and search row hidden | 125 | no (heading at y=193 instead of 268.5) |

At 1440 the header and page are unchanged (83.5 px, 1,025 px).

**Effort and risk:** S, low. A visitor who is signing up loses the header search on a phone; the menu and home page still have it.

### PS-23 (P3, third pass): short pages scroll 47 px for nothing

**What was measured (`third_tests.py short`):** `/login` and `/compare` are 891 px tall in an 844-px window (390) and 787 px in 740 (360).
Nothing is below the fold except the version stamp. After the fix both equal the window (844 and 740).

**Cause:** `.page { min-height: 100vh }` (`00-base.css:123-126`, and `07-dealer-admin.css:246-256`, line 251, for the menu pages) and
then `.app-version-footer` after it in the body (`08-marketing.css:95-103`: `padding: 14px 16px 18px` around one 12-px line, 47 px).

**Fix (tested, 960 px and under):** `.page, .page:has(.app-sidebar), .page.page--app-layout { min-height: calc(100vh - 47px) }`.
A body that is a flex column, with `.page { flex: 1 0 auto }`, would not hard-code the footer height (not tested).
Pages that already fill the screen are unchanged (home 3,574, car 2,037, premium 2,245). With the first patch the empty
listings page, which that patch shortens to 891 px, becomes 844.

**Effort and risk:** S, low. Re-run the first-screen screenshots, because the page box changes on every short page.

### PS-24 (P3, third pass): find dealers on a phone

**What was measured (390 and 360 give the same numbers; `third_tests.py fd`)**

- The three location tabs ("My location", "ZIP code", "City & state") are stacked at y 220.5, 260.5 and 300.5, 30 px tall each.
  They fit side by side in 244.5 px (73.4 + 55 + 72.1 + two 22-px gaps) of a 358-px column.
- The map legend ("In our database", "Google only") shows twice: at y=168.5 and again at y=536. The second is meant to
  appear after a search.
- The map box is 520 px tall (y 564) and the map in it 320 px, so a 200-px blank band sits under the map. Below it the
  empty-state block is 380 px tall with its message 154 px down.
- The page is 1,567 px tall; 1,006 px after the fix. At 1440 the second legend shows too; the fix makes that page 28 px shorter (1,040 → 1,012).

**Screenshots:** before `scratchpad/web_shots/ps3-fd-base-390-full.png` and `scratchpad/web_shots/ps3-find-dealers-390-fold.png`; after `scratchpad/web_shots/ps3-fd-fix-390-full.png`.

**Cause**

- Tabs: `11-overrides.css:222-226` (480 px and under: `flex-direction: column; gap: 10px`); each tab is `padding: 6px 0` (`10-pages.css:390-399`).
- Legend: `10-pages.css:522-524` sets `.find-dealers-legend--results { display: none }`, then `:526-533`
  sets `.find-dealers-legend { display: flex }`, which wins by order. The template's `hidden` attribute
  (`find_dealers.html:98`) loses to the same rule, and `find_dealers.js:342` only sets `legend.hidden = false`.
- Map box: `10-pages.css:640-648` (`height: 520px`) and `11-overrides.css:228-230` (`.find-dealers-map { height: min(50vh, 320px) }`);
  sidebar `10-pages.css:574-578`.

**Fix (tested)**

- `.find-dealers-legend[hidden] { display: none }` (all widths).
- At 480 px and under: tabs in a row (`flex-direction: row; flex-wrap: wrap; gap: 0 22px`) with `min-height: 44px`; the map box `height: min(50vh, 320px); min-height: 0`.
- At 960 px and under: `.find-dealers-sidebar { min-height: 0 }`.
- After: tabs 44 px in one row at y=220.5; map box 320 px at y=470; the page 1,006 px (was 1,567) at 390 and 360. At 1440 only the legend change shows.

**Effort and risk:** S, low.

## Checked and fine

- **Horizontal overflow:** none on any of the 18 page-and-width combinations.
  - The page is exactly as wide as the screen everywhere (`documentElement.scrollWidth == clientWidth`).
  - No element sticks out past an edge outside a scrolling or clipping parent (`overflowCount` 0 for all).
  - `html` has `overflow-x: clip`, so the element-level scan is the real test.
  - The patch keeps it at 0.
  - Second pass, narrower phones (offline replay, same element-level scan): at 375 px, 0 on all six pages. At 320 px,
    0 on five pages and 2 on the empty listings page (the ZIP row, PS-16). With the tested patch plus the PS-16
    and PS-17 rules, 0 on all 12 page-and-width combinations at 320 and 375.
  - Third pass: 412x915 and 430x932 (large phones), same six pages, before and with the first patch: 0 overflowing
    elements, `scrollWidth == innerWidth`, and a sideways scroll attempt (`scrollTo(300, 0)`) leaves `scrollX` at 0 on all
    24 runs. The first patch behaves the same there: home header 156 → 125 px, first card on the listings search 418 → 325
    (324 at 430), on search results 507 → 377 (376 at 430), tap targets under 44 on the listings search 88 → 26.
  - Third pass, the five new pages (`/login`, `/register`, `/find-dealers`, `/compare`, `/premium`) and the zero-result
    URL at 390, 360 and 1440: `overflowCount` 0 and `scrollWidth == innerWidth` on all 18 combinations.
  - Landscape 844x390 (iPhone 14): no overflow; the sticky home header is 62 px (15.9% of the 390-px height), the
    car page's menu button and bottom bar take 44 and 62 px (15.9% for the bar), and the register header is 110.5 px (28.3%, PS-22).
- **The `/listings` Filters sheet:**
  - anchored at the bottom with a 0-px gap at 390 and 360;
  - the "Show results" bar stays on screen (y 786–830 at 390, 682–726 at 360);
  - each filter row is 44 px tall (`11-overrides.css:416-421`).
- **Car tabs** wrap to two rows on purpose ("M4", `11-overrides.css:186-198`), and each tab is 44 px tall.
- **Desktop (1440):**
  - the patch leaves the home page and the car page unchanged in every measured value;
  - on listings and search results it adds only the search line at the top of the results column, which is
    intended;
  - on the dealership page it lines the filters up with the heading, which is intended.
- **Owner design rules, for the patch:** everything it adds is a hairline, text or spacing. The new search line is a
  bare inline line with a magnifier and one navy hairline (no box, no card, no shadow). The filter chips are already
  borderless text with a "×" (`09-components.css:646-656`: `border: none; background: transparent`), so making them
  44 px tall adds no pill. No stat tile, feature grid, badge, ribbon or animation is added.
  - Not changed, because they are controls rather than content containers: the rounded white "Filters" button, the
    Sort and Per-page selects and the ZIP inputs on the listings pages, and the bordered "Sign in to save" and
    "Compare" buttons on the car page. If the owner counts those as the boxes he dislikes, the same hairline
    treatment as the search line (`border: 0; border-bottom: 1px solid var(--navy)`) is the minimal change, but that
    is a design decision and it was not tested here.

## The tested patch

This is the exact stylesheet that produced every `ps-fix-*` screenshot. It is
`scratchpad/phone_audit/scripts/phone_fixes.css`,
loaded last on the page. The search row was moved, and the two toolbar labels shortened, by script in the page:
`MOVE_SEARCH_JS` and `LABELS_JS` in
`scratchpad/phone_audit/scripts/fixcheck.py`.

The block names in the stylesheet came from the testing order, so they do not match the finding numbers in this
report:

| stylesheet block | finding |
|---|---|
| PS-1 | PS-04 header |
| PS-2 and PS-3 | PS-03 menu button and banner |
| PS-4 | PS-07 margins |
| PS-5 | PS-01 search line |
| PS-6 and PS-7 | PS-02 toolbar and chips |
| PS-8 | PS-09 empty state |
| PS-9 | PS-05 field sizes |
| PS-10 and PS-10b | PS-06 tap targets |
| PS-11 and PS-11b | PS-08 car page |
| PS-12 | PS-04 hero, PS-11 and PS-12 home spacing |
| PS-13 | PS-10 dealership sheet |
| PS-14 | PS-14 footer |
| PS-15 | PS-13 small text |

The patch should not ship as another override sheet. `11-overrides.css` already exists to win fights between
other files (structural eval F4). Make each change at the rule cited in the matching finding, so the owning file
keeps owning its rules.

```css
@media (max-width: 600px) {
    .topbar--marketing .nav-links--account { width: auto; }
    .topbar--marketing { position: relative; }
}
@media (max-width: 900px) {
    .app-sidebar-toggle { position: absolute; }
    body.listings-page .guest-banner { padding-left: 68px; min-height: 68px; display: flex; align-items: center; }
    body.listings-page .guest-banner + .listings-filter-shell { padding-top: 0; }
    body:has(.app-sidebar) .car-detail { padding-top: 12px; }
    body:has(.app-sidebar) .car-detail > .vdp-back { margin-left: 48px; min-height: 44px; }
    body:has(.app-sidebar) .dealer-research { padding-top: 12px; }
    body:has(.app-sidebar) .dealer-research__eyebrow { margin-left: 48px; min-height: 44px; display: flex; align-items: center; }
}
@media (max-width: 768px) {
    .guest-banner-content { flex-wrap: nowrap; align-items: center; }
    .guest-banner-dismiss { flex: none; min-width: 44px; min-height: 44px; }
}
@media (max-width: 960px) {
    body.listings-page:has(.app-sidebar) .listings-layout,
    body.listings-page .page:has(.app-sidebar) .listings-layout { padding-left: 16px; }
}
body.dealership-research-page.listings-page:has(.app-sidebar) .listings-layout,
body.dealership-research-page.listings-page .page:has(.app-sidebar) .listings-layout { padding-left: 0; }
#dash-content > .filter-search-row { margin: 0 0 14px; }
#dash-content > .filter-search-row .smart-search-wrap { position: relative; }
body.listings-page #dash-content > .filter-search-row input.smart-search-input--carmax {
    width: 100%; min-height: 48px; height: auto; font-size: 17px; line-height: 1.3;
    padding: 12px 0 12px 30px; border: 0; border-bottom: 1px solid var(--navy);
    border-radius: 0; background: transparent no-repeat left 2px center / 18px 18px
    url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='%231c3552' stroke-width='1.8' stroke-linecap='round'%3E%3Ccircle cx='11' cy='11' r='8'/%3E%3Cpath d='M21 21l-4.35-4.35'/%3E%3C/svg%3E");
    box-shadow: none;
}
body.listings-page #dash-content > .filter-search-row input.smart-search-input--carmax:focus {
    box-shadow: none; border-bottom-width: 2px; outline: none;
}
@media (max-width: 600px) {
    .listings-toolbar { flex-direction: row; flex-wrap: wrap; align-items: center; gap: 8px 12px;
        padding-bottom: 10px; margin-bottom: 10px; }
    .listings-toolbar__start, .listings-toolbar__end { display: contents; }
    .listings-filters-mobile-btn { order: 1; min-height: 44px; }
    .listings-sort-select { order: 2; margin-left: auto; min-height: 44px; flex: 0 1 auto; max-width: 58%; }
    .listings-sort-label, .listings-per-page-label, .listings-per-page-select { display: none; }
    .results-count { order: 3; margin: 0; flex: 0 1 auto; }
    .listings-saved-search { order: 4; margin-left: auto; gap: 12px; }
    .listings-saved-search__save-btn, .listings-saved-search__list-btn { min-height: 44px; }
    .listings-active-chips-wrap { flex-direction: row; align-items: center; flex-wrap: nowrap;
        padding: 0; margin-bottom: 12px; }
    .listings-active-chips { flex: 1 1 auto; min-width: 0; flex-wrap: nowrap; overflow-x: auto;
        scrollbar-width: none; mask-image: linear-gradient(90deg, #000 calc(100% - 24px), transparent); }
    .listings-active-chips::-webkit-scrollbar { display: none; }
    .listings-filter-chip { flex: none; min-height: 44px; }
    .listings-chips-clear { align-self: center; flex: none; min-height: 44px; }
}
#dash-content:has(#listings-start-prompt:not([hidden]) #listings-start-prompt-zip:not([hidden])) #listings-zip-banner { display: none; }
#dash-content:has(> #listings-start-prompt:not([hidden])) :is(#listings-saved-search, .listings-toolbar__end) { display: none; }
#dash-content:has(> #listings-start-prompt:not([hidden])) #listings-toolbar { border-bottom: 0; }
@media (max-width: 600px) {
    .listings-start-prompt { padding-top: 0; padding-bottom: 24px; }
}
@media (max-width: 960px) {
    .listings-zip-banner__input, .listings-per-page-select, .listings-sort-select,
    body.listings-page .listings-filter-inner input.smart-search-input--carmax,
    body.listings-page .listings-filter-inner .pill-zip,
    body.listings-page .listings-filter-inner .pill-select,
    .sidebar-search__input { font-size: 16px; }
    .result-save-btn { width: 44px; height: 44px; }
    .result-compare-label { min-height: 44px; }
    .result-dealer-link, .vdp-back, .car-detail-dealer-link, .listings-start-prompt__geo,
    .listings-zip-banner__dismiss { position: relative; }
    .result-dealer-link::after, .vdp-back::after, .car-detail-dealer-link::after,
    .listings-start-prompt__geo::after, .listings-zip-banner__dismiss::after {
        content: ""; position: absolute; left: -4px; right: -4px; top: 50%; height: 44px;
        transform: translateY(-50%); min-width: 44px; }
    .listings-page-btn, .listings-page-num { min-height: 44px; min-width: 44px; }
    .car-gallery-arrow, .car-gallery-btn { width: 44px; height: 44px; }
    .compare-toggle-btn, .car-hero-link, .save-btn { min-height: 44px; }
    .listings-zip-banner__input, #listings-start-prompt-zip-submit, .listings-zip-banner__submit { min-height: 44px; }
    .leaflet-touch .leaflet-bar a { width: 44px; height: 44px; line-height: 44px; }
    body.dealership-research-page .listings-sidebar { top: auto; }
    body.car-detail-page .app-version-footer { padding-bottom: calc(80px + env(safe-area-inset-bottom, 0px)); }
}
@media (max-width: 640px) {
    .car-gallery-controls { display: none; }
    .car-detail-info { padding-top: 0; }
}
@media (max-width: 600px) {
    .car-detail-price-row { flex-direction: row; flex-wrap: wrap; align-items: center; }
    .result-trim, .result-meta--specs, .result-photo-count, .car-quick-spec-item dt,
    .lps-eyebrow, .lps-kicker, .lps-mini-label, .app-sidebar__group-label,
    .sidebar-search__heading, .car-finance-monthly-label, .dealer-research__eyebrow { font-size: 12px; }
}
@media (max-width: 560px) {
    .lps-stage { padding: 36px 0 44px; }
    .lps-headline { font-size: clamp(32px, 9.4vw, 42px); }
    .lps-search { margin-top: 30px; }
    .lps-search-main { gap: 12px; }
    .lps-dialogue { padding: 48px 0; }
    .lps-data { padding: 56px 0; }
    .lps-lot { padding: 56px 0 64px; }
    .lps-cta-row .lps-btn { padding-left: 14px; padding-right: 14px; white-space: nowrap; }
    .lps-foot-links a { display: inline-flex; align-items: center; min-height: 44px; }
}
```

The listing above is the tested file, grouped by breakpoint: the file's comments are removed and rules that
share a breakpoint are merged. The full file with comments is at the path above.

**Rules added in the second pass** (tested separately; they are not in `phone_fixes.css`; `extra_checks.py`,
`extra_checks2.py`, `extra_checks3.py`):

```css
@media (max-width: 700px) {                                   /* PS-16 */
    .listings-zip-banner__controls { min-width: 0; }
}
@media (max-width: 560px) {                                   /* PS-17 */
    .lps-foot-links a { margin-left: 0; margin-right: 22px; }
}
```

**Rules added in the third pass** (tested, and loaded after `phone_fixes.css`: PS-19 to PS-24, the login and register
rules of PS-05 and PS-06, and the two items the first pass called "not tested"). The file is
`scratchpad/ps_third/ps3_fixes.css`
(sha1 prefix `c38e5d7e6f43`; a copy is in `phone-spacing_raw/third_pass/`). The test runs used two earlier states of it:
`b45752b7676b` for the drawer, dealer, register, short-page, find-dealers and width runs (it differs from the final file only
in the car-page tray rule, which used a literal `62px` instead of `--car-cta-h`), and `cc8addb5cab4` for the last compare-tray
runs (the final file differs only in a comment). Like the first patch it is a simulation; apply each rule in the file that owns it.

```css
/* Third pass: candidate fixes for the findings added in the third pass (PS-19 to PS-24, plus the login and
   register rules of PS-05 and PS-06) and for the two items the first pass listed as "not tested". Served from a fake same-origin URL by NetCache,
   never sent to the live site. Owner rules: hairlines, text and spacing only, no boxes. */

/* PS-19 compare tray: the title and the remove button are white on a near-white tray */
.compare-tray__slot { border-left-color: var(--border-light); }
.compare-tray__slot-title { color: var(--navy); }
.compare-tray__slot-remove { color: var(--text-muted); }
@media (max-width: 960px) {
    .compare-tray__slot-remove { position: relative; }
    .compare-tray__slot-remove::after { content: ""; position: absolute; inset: -14px -16px; }
    .compare-tray__clear { min-height: 44px; }
    /* on the car page the tray must sit above the fixed bottom bar, not on top of it;
       --car-cta-h is the bar's height (62 px; 69 px once the 44-px button minimum is applied) */
    body.car-detail-page { --car-cta-h: 62px; }
    body.car-detail-page .compare-tray { bottom: calc(var(--car-cta-h) + env(safe-area-inset-bottom, 0px)); padding-bottom: 12px; }
    body.compare-tray-open.car-detail-page .car-detail { padding-bottom: calc(var(--car-cta-h) + 140px + env(safe-area-inset-bottom, 0px)); }
}
@media (max-width: 600px) {
    body.compare-tray-open.listings-page { padding-bottom: calc(132px + env(safe-area-inset-bottom, 0px)); }
    body.listings-page.compare-tray-open .listings-pagination { margin-bottom: 0; }
    /* four cars wrapped the slots onto a second row (tray 168 px); one strip keeps the tray at two rows */
    .compare-tray__slots { flex-wrap: nowrap; overflow-x: auto; width: 100%; scrollbar-width: none; }
    .compare-tray__slots::-webkit-scrollbar { display: none; }
    .compare-tray__slot { flex: none; }
}

/* PS-20 menu drawer: nav links 35.5 px, logo 19 px, close 22x32, search fields 36 px */
@media (max-width: 900px) {
    .app-sidebar__link { min-height: 44px; }
    .app-sidebar__logo { display: inline-flex; align-items: center; min-height: 44px; }
    .app-sidebar__close:not([hidden]) { min-width: 44px; min-height: 44px; align-items: center; justify-content: center; }
    .sidebar-search__row { grid-template-columns: minmax(0, 1fr) 44px; row-gap: 6px; }
    .sidebar-search__field--grow { grid-column: 1; grid-row: 1; }
    .sidebar-search__field--zip { grid-column: 1 / -1; grid-row: 2; }
    .sidebar-search__input--zip { text-align: left; padding-left: 24px; }
    .sidebar-search__submit { grid-column: 2; grid-row: 1; min-height: 44px; }
    .sidebar-search__input { min-height: 44px; font-size: 16px; }
}

/* PS-21 dealership page: phone number, website and directions links are 19 to 24 px tall */
@media (max-width: 960px) {
    .dealer-research__contact { gap: 0; }
    .dealer-research__contact > div { padding-block: 12.5px; }
    .dealer-research__contact a, .dealer-research__nav a, .dr-reviewform__signin a { position: relative; }
    .dealer-research__contact a::after, .dealer-research__nav a::after, .dr-reviewform__signin a::after {
        content: ""; position: absolute; left: -4px; right: -4px; top: 50%; height: 44px;
        transform: translateY(-50%); }
}

/* PS-22 register page: the marketing header carries a search row the landing page already hides */
@media (max-width: 600px) {
    .topbar--marketing .nav-search { display: none; }
}

/* PS-23 short pages scroll 47 px (page min-height 100vh + the version footer after it) */
@media (max-width: 960px) {
    .page, .page:has(.app-sidebar), .page.page--app-layout { min-height: calc(100vh - 47px); }
}

/* PS-24 find dealers on phones: tabs stacked, results legend shown before results (hidden attribute loses to display:flex), 520 px map shell and sidebar */
@media (max-width: 480px) {
    .find-dealers-location-modes { flex-direction: row; flex-wrap: wrap; align-items: center; gap: 0 22px; }
    .find-dealers-mode-tab span { display: inline-flex; align-items: center; min-height: 44px; box-sizing: border-box; }
    .find-dealers-map-shell { height: min(50vh, 320px); min-height: 0; }
}
@media (max-width: 960px) {
    .find-dealers-sidebar { min-height: 0; }
}
.find-dealers-legend[hidden] { display: none; }

/* Not tested in the first pass: home logo and "See all" tap size, car dealer line letter spacing */
@media (max-width: 600px) {
    .topbar--marketing .logo--link, .lps-lot-link { position: relative; }
    .topbar--marketing .logo--link::after, .lps-lot-link::after { content: ""; position: absolute; left: -4px; right: -4px;
        top: 50%; height: 44px; transform: translateY(-50%); }
    .car-detail-dealer-eyebrow { letter-spacing: 0.04em; }
}

/* PS-05 (login and register): login inputs are 15 px (iOS focus zoom), show-password button is 36 px */
@media (max-width: 960px) {
    .field-group input { font-size: 16px; }
    .password-toggle { width: 44px; height: 44px; }
    .auth-card a, .auth-section .auth-switch a { position: relative; }
    .auth-card a::after { content: ""; position: absolute; left: -4px; right: -4px; top: 50%; height: 44px; transform: translateY(-50%); }
}
```

The two checks that tie the third-pass rules to the first patch (final CSS, 390 and 360, `third_tests.py widths --css both`):

- **First patch plus the third-pass rules** against the first patch alone at 390: identical page height, first-card
  position and tap counts on the listings search (10,428; 325; 26 of 113), search results (10,600; 377; 27 of 120), car (1,984;
  2 of 20) and home (3,256; 3 of 19). Different, by design: the empty listings page 891 → 844 (PS-23) and the dealership page
  11,887 → 11,946 (PS-21, +59 px). At 360 the same six pages give home 3,280, empty listings 740, listings search 10,447,
  search results 10,650, car 1,977 and dealership 12,129, all with 0 overflow.
- **Third-pass rules alone at 1440:** the six pages have the same page height, first-card position and tap counts as without
  them (2,108; 1,104; 5,735; 5,872; 1,349; 4,932). Two rules are not phone-only and change 1440 on purpose: the compare tray's colours
  (PS-19) and the find-dealers legend `[hidden]` (PS-24, `/find-dealers` goes from 1,040 to 1,012 px).

PS-18 (`100dvh` on `.app-sidebar`) is not in either set because it could not be reproduced.

**Template changes**

- `listings.html:65-75`: move this block to be the first child of `#dash-content` (`listings.html:361`).
- `listings.html:56`: "Search cars" → "Filters".
- `listings.html:392`: "Save this search" → "Save search".
- `listings.html:397`: "Saved searches" → "Saved".
- `landing.html:28`: optionally, a shorter placeholder for phones.
- Third pass, JavaScript, one character: `frontend/static/compare.js:204` closes the `style` attribute
  (`' style="background-image:url(\'' + safeImg + "')\""`). No template change is needed for PS-19 to PS-24; the
  `[hidden]` rule in PS-24 is CSS (the template at `find_dealers.html:98` is already right).

**Tests that check stylesheet text**

`backend/tests/test_visual_review_mobile_history_recalls.py` checks stylesheet text, not rendering.

- Applying the patch at the cited rules keeps these passing: `test_m1` (the selector text stays in `07`), `test_m4`
  (no change to the tabs block in `11`), `test_m7` (no change in `09`) and `test_m8` (no change in `12`).
- The test file also has `test_m2_mobile_gallery_rule_follows_the_base_rule` (second pass; the first pass did not
  list it). It reads `03-car-page.css` and requires the first `aspect-ratio: 4 / 3;` (line 1650) to come after the
  base `height: 500px; max-height: 500px;` block (lines 1634-1635), and forbids `height: min(56vw, 340px);`. None
  of the car-page rules in this patch (arrow size, `.car-gallery-controls`, `.car-detail-info` padding) contain
  `aspect-ratio` or that height, so `test_m2` still passes. Do not add an `aspect-ratio: 4 / 3;` above line 1634.
- `test_m7` already shows the weakness: it passed while PS-10 regressed the same bug on the dealership page.
- A screenshot gate at 390 and 1440 px is what would have caught it. The scripts used here (`capture.py`,
  `audit.js`, `fixcheck.py`) are a starting point for that gate (structural eval F2/F3).

## Before → after, key numbers (390 px; 360 px only where it differs)

| page | metric | before | after |
|---|---|---|---|
| home | header height / position | 156 / sticky | 125 / not pinned |
| home | search line y (390 / 360) | 450.3 / 494.4 | 305 / 299.1 |
| home | page height (390 / 360) | 3574 / 3648 | 3256 / 3280 |
| listings empty | start prompt y (390 / 360) | 481.7 / 500.5 | 215 / 215 |
| listings empty | ZIP prompts shown | 2 | 1 |
| listings empty | fields under 16 px | 6/6 | 0/4 |
| listings search | search line y | not visible | 81 |
| listings search | first card y | 418 | 325 |
| listings search | toolbar / chips height | 154 / 83 | 107 / 45 |
| listings search | taps under 44 / under 24 | 88 / 51 of 113 | 26 / 24 of 113 |
| listings search | left / right margin | 12 / 16 | 16 / 16 |
| listings search | page height / first card height (cost of the 44-px rows) | 10161 / 377 | 10428 / 391 |
| search results | first card y | 507 | 377 |
| search results | chips height | 139 | 45 |
| search results | page height | 10370 | 10600 |
| car | back link / title / price y | 56 / 615.1 / 713.4 | 12 / 521.1 / 619.4 |
| car | taps under 44 | 12 of 22 | 2 of 20 |
| car | fixed bottom bar height | 62 | 69 |
| dealership | car grid left edge (hero 16) | 28 | 16 |
| dealership | page height | 11573 | 11887 |
| home | footer link left edge (copy line 20) | 42 | 20 (with PS-17) |
| listings empty, 320 px | ZIP row right edge (screen 320) | 324.1 | 264 (with PS-16) |
| dealership | Filters sheet bottom gap (390 / 360) | 112 / 76.8 | 0 / 0 |
| all phone pages | content under the menu button | banner text, card text, map zoom | none |
| all pages | horizontal overflow | 0 | 0 |
| menu button, 390 | scroll positions with text, a photo or a control under it: listings search / car / dealership | 45 of 45 / 5 of 6 / 48 of 51 | 0 of 46 / 0 of 6 / 0 of 53 |
| compare tray (third pass) | title contrast; tray height with 2 / 4 cars at 390 | 1.06 : 1; 120 / 168 px | 11.77 : 1; 121 / 121 px |
| compare tray on the car page, 390 | tray / bottom bar | 724–844 over 782–844 | 661–782 above 782–844 |
| menu drawer (third pass) | nav link / logo / close / ZIP field | 35.5 / 19 / 22x32 / 48x33.3 | 44 / 44 / 44x44 / 219x44 |
| dealership (third pass) | phone / website / directions hit height | 16 / 16 / 24 px | 40 px or more |
| register (third pass) | header height / pinned (390 and 360) | 200.5 / yes | 125 / no (with the first patch) |
| login (third pass) | field font / show-password / page height at 390 | 15 px / 36x36 / 891 | 16 px / 44x44 / 844 |
| compare, empty (third pass) | page height at 390 / 360 | 891 / 787 | 844 / 740 |
| listings empty, with the first patch (third pass) | page height at 390 | 891 | 844 |
| find dealers (third pass) | page height; tab height; legends shown; map box at 390 | 1,567; 30 px; 2; 520 px | 1,006; 44 px; 1; 320 px |
| home (third pass) | logo / "See all" hit height (box unchanged) | 16 / 16 px | 40 px or more |

The full table for all 18 combinations is in
`scratchpad/phone_audit/out/compare_run3.md`.

## Re-verification (second pass, 2026-11-09 20:35 to 21:05 EST)

The first pass wrote this report. Before it was handed on, a second pass checked it. Apart from one GET (see "Rules
followed"), everything ran offline against the first pass's disk cache. Commands E to H are listed under "Commands".

**1. Citations.** `git archive phase/N37a frontend` was extracted to scratch and the text at every `file:line` in
PS-01 to PS-15 was printed: 98 full-form citations (`cite_check.py`) and 21 shorthand ones (`:641-643`, `:920-922`,
`:666` and similar, printed with `sed`/`awk`). All resolve to the rule the text names, except:

- `11-overrides.css:53` (chip fade) is `padding-bottom: 2px;`; the `mask-image` is on line 54 and the rule spans
  `:41-55`. Corrected in PS-02.
- `07-dealer-admin.css:370-378` (menu ZIP field) is `.sidebar-search__field--zip { grid-column: auto }` and
  `.visually-hidden`. The 48-px ZIP column comes from the row's grid at `:352-357`. Corrected in PS-11.
- The list of pinned tests omitted `test_m2`. Added under "The tested patch".

**2. Independent re-measurement.** `remeasure.py` loads the six pages at 390, 360 and 1440 from the cache and
measures with new JavaScript (not `audit.js`), before and after the tested patch. Every headline number in this
report reproduced exactly:

| metric (390 unless noted) | this report | second-pass run |
|---|---|---|
| home: header height and position, search line y, headline height | 156 sticky, 450.3, 132 | 156 sticky, 450.3, 132.3 |
| listings empty: banner bottom, toolbar y and height, start prompt y | 81, 311.7 and 154, 481.7 | 81, 311.7 and 154, 481.7 |
| listings search: toolbar h, chips h, first card y, its price y | 154, 83, 418, 657–681 | 154, 83, 418, 657–681 |
| search results: chips h, first card y, its price y | 139, 507, 746–770 | 139, 507, 746–770 |
| car: back link, title, price y; bottom bar y and height | 56, 615.1, 713.4; 782 and 62 | 56, 615.1, 713.4; 782 and 62 |
| dealership: grid left edge; Filters sheet bottom gap at 390 / 360 | 28; 112 / 76.8 | 28; 112 / 76.8 |
| page height at 390: home, empty, search, results, car, dealership | 3574, 969, 10161, 10370, 2037, 11573 | identical |
| page height at 360: same pages | 3648, 1020, 10161, 10370, 2046, 11706 | identical |
| page height at 1440: same pages | 2108, 1104, 5735, 5872, 1349, 4932 | identical |
| taps under 44 / all: listings search, search results, dealership, home | 88/113, 99/124, 95/121, 5/19 | identical |
| taps under 24, listings search | 51 | 51 |
| fields under 16 px: empty, search, results, dealership, home | 6/6, 4/4, 4/4, 4/4, 0/3 | identical |
| horizontal scroll possible (`scrollTo(300, 0)` then `scrollX`) | none | 0 on all 18 combinations |
| after patch: header, search line y, headline (home) | 125, 305, 77 | 125, 305, 77 |
| after patch: first card y, listings search / search results | 325 / 377 | 325 / 377 |
| after patch: toolbar h, chips h (listings search) | 107, 45 | 107, 45 |
| after patch: car back link, title, price y | 12, 521.1, 619.4 | 12, 521.1, 619.4 |
| after patch: taps under 44, listings search / dealership | 26/113, 33/120 | 26/113, 33/120 |
| after patch: Filters sheet gap, dealership 390 / 360 | 0 / 0 | 0 / 0 |
| at 1440, home and car, before vs after | identical | identical (every measured value) |

`footer_check.py` (PS-14) was re-run too and gave the same footer and bar positions.

**3. One difference, explained.** The new script first reported 23 targets under 44 of 33 and 10 form fields on the
car page, against 12 of 22 and 2 here. The new script did not skip controls inside `opacity: 0` containers (hidden
chat and tool panels). With that rule added (`extra_checks.py`, A) it gives 22 targets, 12 under 44, and 2 fields,
the same as this report. The numbers in this report were right; the definition is now written under "How it was
measured".

**4. One row corrected.** The first capture's 1440 empty-state row ("not visible, y=147.5", 21 of 22 taps, 6 of 6
fields, 2 small-text runs) was a mid-animation reading (see the notes under the Before table). The settled values are
139.5, 22 of 42, 12 of 12 and 6. Comparing the first capture's metrics (height, taps, taps under 24, fields, small text,
margins) with the settled replay for all 18 page-and-width combinations found no other difference. No finding or
phone number depended on that row.

**5. Narrower phones.** The same cache was replayed at 375x667 and 320x568 (iPhone SE class). Overflow: none at
375; at 320 only the ZIP row (PS-16). The whole patch gives 0 overflow on all six pages at both widths, no wrapped
controls, and 0 fields under 16 px. At 320 the home header is three rows (156 px) both before and after the patch,
because the logo and the two account links do not fit on one row at that width. The patch still stops it being
pinned, and it moves the hero search line from y=494.4 (87% of the 568-px screen) to y=341.2 (60%; measured by
`home320.py`).

**6. Found and added:** PS-16 (ZIP row overflow), PS-17 (footer indent), PS-18 (menu drawer in `100vh`, not
reproduced). The cost of the patch (card and page height, bottom bar height) was measured and is stated in PS-06 and
in the Summary.

**7. Not re-run.** The CDP matched-styles trace (`trace.py`) was not repeated. The cited rules were read from
`phase/N37a` instead. The measured before/after change of each patch rule in item 2 is consistent with the cited rule
being the one that wins (for example PS-10: the sheet top moves from 12 to 124 px when the patch overrides only that
one property of `15-dealership.css:164`).

## Third pass (2026-11-10 20:19 to 21:00 EST)

The first two passes were complete, so this pass did not redo the capture. It re-checked that nothing had changed, reproduced the
second pass, covered what the first two passes had not opened, and tested the two items the first pass called "not tested".
`T=scratchpad/ps_third`.

**1. The live site had not changed.**

- One GET of `/` at 2026-11-11 01:20:38 UTC (2026-11-10 20:20 EST) with the iPhone Safari user agent: HTTP 200, 15,303 bytes
  (the same size as the second pass), footer `v1.5.8`, 15 stylesheet links all `?v=1792774194`, `server: cloudflare`,
  `cf-cache-status: DYNAMIC`. This is request 1 in this pass's ledger (`$T/http_cache/live_requests.jsonl`).
- `git rev-parse` gives `phase/N37a` = `faea2b9854bdcdf757f8cdd7db70ddb4d09e22a1` and `main` = `0fd91872bedb07a30ecdd7e19773db5fdc53b1a7`,
  as before. `git diff --stat main phase/N37a -- frontend/` lists the same 6 files.
- The two static files the new pages load (`password_toggle.js` 1,062 bytes, `find_dealers.js` 26,216 bytes) equal `main` and
  `phase/N37a` byte for byte. `git diff --quiet main phase/N37a` on `login.html`, `register.html`, `find_dealers.html`,
  `compare.html`, `premium.html`, `_nav_marketing.html`, `_nav_search.html`, `static/compare.js` and `static/css/` finds no difference.
  So every finding holds for live 1.5.8 and for the staged release.

**2. The second pass's numbers reproduce.**

- `remeasure.py --drawer` re-run from the same disk cache, output compared with `verify/remeasure_all.json`: 3,902 values, 4 differ
  (the live-request counter, before and after, and two harness counters: aborted POSTs 15 → 13, cache hits 1,228 → 1,227). Every
  geometry, tap-target and field-size value is identical.
- `cite_check.py` re-run on a fresh `git archive phase/N37a frontend` made by this pass (`diff -rq` against the second pass's
  extraction: no difference): 106 citation lines resolved, none missing or out of range. Each citation added in this pass was
  printed from the same tree with `awk` and read.

**3. What was covered that the first two passes had not opened (all offline replay except the ledger below).**

- **Five more pages and one zero-result URL** at 390, 360 and 1440: `/login`, `/register`, `/find-dealers`, `/compare`, `/premium`, and
  `/listings?q=zzqxv+notacar&zip_code=28202&radius=25` (`capture3.py`, 18 captures, `ps3-<page>-<width>-fold.png` and `-full.png`).
  `/login` was already in the first pass's cache (request 62 of the first pass, a `fetch()` made by a page script on
  2026-11-09 13:48 UTC, 10,649 bytes, same release), so it cost no request here.
  *Live requests this pass: 9* (1 curl, 8 from the capture: `password_toggle.js`, `/register`, `/find-dealers`, `find_dealers.js`,
  `/compare`, `/premium`, the 1.5 MB zero-result page and its `GET /api/search/smart/parse`), all GET, all 200, at least 1.1 s
  apart. The first two passes used 64, so 73 of the 300 allowed. Never sent: 5 POSTs the pages tried during the live capture (4 `POST /api/search/smart`,
  1 `POST /api/session/listings-geo`). The capture also made 32 GETs to third-party hosts (not the live site; they were not itemised).
- **The compare tray**, by real clicks on the cards (`states3.py compare`, two and four cars, 390, 360 and 1440, listings and car pages;
  `compare_both.py` for the tray with the first patch).
- **The menu drawer, the dealership links, the register header, short pages, find dealers, 412 and 430 px, landscape, and the menu
  button's coverage while scrolling** (`third_tests.py drawer | dealer | register | short | fd | widths | landscape | covers`).
- **Small probes**: `probe_login.py` (login fields), `probe_home.py` and `probe_home_hit.py` (home logo and "See all", car dealer line),
  `probe_short.py` (why short pages are 47 px too tall), `probe_noresults.py`, `probe_csp.py` (tray thumbnails).
- **Consolidated rerun:** `run_final.sh` re-ran every fix test with the final stylesheet (`out/final_run.log`).

**Commands, third pass** (`SCR` is the scratchpad folder, `T=$SCR/ps_third`; every line except the first `curl` and `capture3.py`
is an offline replay, `NetCache(offline=True)`, which aborts anything not in the disk cache):

```sh
PY=.venv/bin/python
curl -sS -A "<iPhone Safari UA>" -o $T/home_now3.html -D $T/home_now3.headers https://sarraficars.com/   # live request 1
cd . && git archive phase/N37a frontend | tar -x -C $T/n37a      # read-only; also: git archive main frontend
cp -R $SCR/phone_audit/http_cache/meta $SCR/phone_audit/http_cache/bodies $T/http_cache/                 # seed the replay cache; own ledger
$PY -I $T/capture3.py --vps 390,360,1440 --cap 120 > $T/capture3.log                                    # live requests 2 to 9
$PY -I $SCR/verify/remeasure.py $T/remeasure_third.json --drawer > $T/remeasure_third.log               # reproduction, 3,902 values
$PY -I $T/states3.py compare --vps 390,360,1440 --n 2 [--css ps3_fixes.css]                              # compare tray, two cars
$PY -I $T/states3.py compare --vps 390,360 --n 4 [--css ps3_fixes.css]                                   # four cars
$PY -I $T/compare_both.py ps3only|phoneonly|both --vps 390,360                                           # tray with the first patch
$PY -I $T/third_tests.py drawer   --vps 390,360      --css none|ps3 --tag base|fix
$PY -I $T/third_tests.py dealer   --vps 390,360,1440 --css none|ps3 --tag base|fix
$PY -I $T/third_tests.py register --vps 390,360,1440 --css none|ps3|phone|both --tag base|ps3|phone|both
$PY -I $T/third_tests.py short    --vps 390,360      --css none|ps3 --tag base|fix
$PY -I $T/third_tests.py fd       --vps 390,360,1440 --css none|ps3 --tag base|fix
$PY -I $T/third_tests.py widths   --vps 412,430 --css none --tag base ; ... --css phone --tag phone     # large phones
$PY -I $T/third_tests.py widths   --vps 390 --css phone --tag phone390 ; --vps 390,360 --css both --tag both390_360
$PY -I $T/third_tests.py widths   --vps 1440 --css none --tag base1440 ; --css ps3 --tag ps3_1440
$PY -I $T/third_tests.py landscape --vps 844l --css none --tag base
$PY -I $T/third_tests.py covers   --vps 390 --css none --tag base ; --css phone --tag phone             # menu button sampling
$PY -I $T/probe_login.py ; $PY -I $T/probe_home.py ; $PY -I $T/probe_home_hit.py                          # login fields, home logo and "See all"
$PY -I $T/probe_noresults.py ; $PY -I $T/probe_short.py ; $PY -I $T/probe_csp.py
$T/run_final.sh                                                                                          # every fix test again, final stylesheet
$PY -I $T/cite_check3.py > $T/cite_check3_out.txt                                                        # citation text, new tree
```

**4. Corrections to the earlier text.**

- PS-06 had called the 8 links left on the dealership page after the first patch "review-section links". They are the phone number, the website,
  Google Maps, Apple Maps, Waze, "Sign in to write a review" and Leaflet's two credit links. Corrected in PS-06; six are fixed in PS-21.
- PS-06 said the home logo and "See all" fix was "not tested". It was: an invisible 44-px tap area, 16 → 40 px or more, no layout change.
- PS-03 now has a measured count for how often the fixed menu button sits over content.
- PS-05 now includes the login fields (15 px) and the register header search (14 px); PS-07 the five new pages' gutters.

## Limitations

- **Chromium, not iOS Safari.** iOS focus zoom (PS-05) is a property of the CSS value and is not reproduced here.
  The 664-px first-screen height comes from Playwright's iPhone profile (`iPhone 13` and `iPhone 14`: viewport
  390x664). PS-18 (the `100vh` drawer) cannot be reproduced in Chromium at all and needs a real iPhone.
- **Replays, not fresh loads.** Every second-pass number, and the 320 and 375 px screenshots (`ps2-*`), come from
  replaying the first pass's cached responses (the server sends the same HTML at every width). They show layout, not
  server behaviour. The replay aborted 13 uncached requests in the full run; that matches the 3 photos that already
  failed in the live capture plus their 10 retries, so geometry is unaffected.
- **Smart search was not run live.** `POST /api/search/smart` was never sent (GET only), so the search-results grid
  is the server's own GET rendering of the same query. The layout is the same component either way.
  `POST /api/session/listings-geo` was not sent either; the ZIP came from the URL.
- **Harness noise.** The "net::ERR_FAILED" console lines in the captures are this tool's own aborted requests.
- **Patch is a simulation.** The `ps-fix-*` screenshots are the live page plus the stylesheet above, with the two
  template edits done by script in the page. They were not rendered from repository code.
- **Third pass, zero-result search not observed.** `/listings?q=zzqxv+notacar&...` shows the toolbar and chips and nothing under
  them in the replay, because the page asks `POST /api/search/smart` for its results and POSTs are never sent here. Whether the
  real page then shows "No vehicles matched your filters." (`listings.html:463-473`, `02-listings.css:1364-1384`) was not checked.
  Its markup is flat (hairlines, no card), and its phone padding (`05-car-tools.css:1105-1108`, `min-height: 36vh`) was not measured.
- **Third pass, one capture per page.** The five new pages and the zero-result URL were each fetched once (`/login` by the first pass on
  2026-11-09, the others on 2026-11-10); the 360 and 1440 captures replay that HTML. The compare-tray tests seed `localStorage` (`ds_compare_ids`) or click the cards, in an offline page; they show
  layout and colour, not server behaviour. The thumbnail defect (`compare.js:204`) does not depend on the browser engine.
- **Third pass, simulated fixes.** Like the first patch, `ps3_fixes.css` is the live page plus a stylesheet. The car-page tray
  offset assumes the bar is 62 px (69 px with the first patch; `--car-cta-h`), and the short-page rule hard-codes the 47-px version
  footer (a flex-column body would not).

## Files

**Durable copies.** The scratchpad paths below belong to this session and disappear with it. Copies of the scripts,
the fix stylesheet, the request ledger and the second-pass results (316 KB; no screenshots, no HTTP cache) are in
`docs/web_speed_search_2026_11/phone-spacing_raw/` (`first_pass/` and
`second_pass/`). The scripts in them still point at the scratchpad paths, so to re-run them the captured cache and
screenshots would have to be regenerated. `second_pass/home_now.headers` has the session cookie, the reporting
endpoint and the CSP nonce redacted. The screenshots exist only in the scratchpad `web_shots/` folder.

**Scripts** (all under `scratchpad/phone_audit/scripts/`):

- `scratchpad/phone_audit/scripts/netcache.py`
- `scratchpad/phone_audit/scripts/capture.py`
- `scratchpad/phone_audit/scripts/audit.js`
- `scratchpad/phone_audit/scripts/trace.py`
- `scratchpad/phone_audit/scripts/winners.py`
- `scratchpad/phone_audit/scripts/table.py`
- `scratchpad/phone_audit/scripts/fixcheck.py`
- `scratchpad/phone_audit/scripts/compare.py`
- `scratchpad/phone_audit/scripts/footer_check.py`
- `scratchpad/phone_audit/scripts/price_check.py`
- `scratchpad/phone_audit/scripts/phone_fixes.css`

**Request log**

- `scratchpad/phone_audit/http_cache/live_requests.jsonl`
- `scratchpad/phone_audit/http_cache/live_count.json` (64: 63 from the first pass, 1 from the second)

**Second-pass scripts and results** (all under `scratchpad/verify/`; the `n37a_verify/` folder next to it is the `git archive` of `phase/N37a`):

- `cite_check.py`: prints the text at every full-form citation
- `remeasure.py`, `remeasure_all.json`, `remeasure_all.log`: independent re-measurement, 3 widths x 6 pages, before and after the patch
- `extra_checks.py`, `extra_checks.json`: car-page tap rule, 1440 empty state, 320 and 375 overflow, footer indent
- `extra_checks2.py`, `extra_checks2.json`: PS-16 fix at 320, 360 and 390
- `extra_checks3.py`, `extra_checks3.json`: whole patch at 320 and 375, drawer geometry
- `home320.py`: home header and search line at 320, before and after
- `zipthreshold.py`: PS-16 ZIP row spill at seven widths (output is the table in PS-16)
- `home_now.html`, `home_now.headers`: the one live GET of `/` (version and stylesheet token)
- `crops/`: `dealer-bottom-0.png` and `dealer-bottom-1.png` (bottom of the dealership page at 390), `footer-before-after-390.png`

**Which CSS rule sets what**

- `scratchpad/phone_audit/trace/trace1.json`
- `scratchpad/phone_audit/trace/trace1_winners.txt`

**Live screenshots, first screen** (also `-full.png` for each page and width):

- `scratchpad/web_shots/ps-home-390-fold.png`
- `scratchpad/web_shots/ps-home-360-fold.png`
- `scratchpad/web_shots/ps-home-1440-fold.png`
- `scratchpad/web_shots/ps-listings-empty-390-fold.png`
- `scratchpad/web_shots/ps-listings-empty-360-fold.png`
- `scratchpad/web_shots/ps-listings-empty-1440-fold.png`
- `scratchpad/web_shots/ps-listings-search-390-fold.png`
- `scratchpad/web_shots/ps-listings-search-360-fold.png`
- `scratchpad/web_shots/ps-listings-search-1440-fold.png`
- `scratchpad/web_shots/ps-search-results-390-fold.png`
- `scratchpad/web_shots/ps-search-results-360-fold.png`
- `scratchpad/web_shots/ps-search-results-1440-fold.png`
- `scratchpad/web_shots/ps-car-390-fold.png`
- `scratchpad/web_shots/ps-car-360-fold.png`
- `scratchpad/web_shots/ps-car-1440-fold.png`
- `scratchpad/web_shots/ps-dealership-390-fold.png`
- `scratchpad/web_shots/ps-dealership-360-fold.png`
- `scratchpad/web_shots/ps-dealership-1440-fold.png`

**Live screenshots, phone states**

- `scratchpad/web_shots/ps-listings-search-390-filters-open-fold.png`
- `scratchpad/web_shots/ps-listings-search-390-filters-open-bottom.png`
- `scratchpad/web_shots/ps-listings-search-360-filters-open-fold.png`
- `scratchpad/web_shots/ps-dealership-390-filters-open-fold.png`
- `scratchpad/web_shots/ps-dealership-360-filters-open-fold.png`
- `scratchpad/web_shots/ps-listings-empty-390-menu-open-fold.png`
- `scratchpad/web_shots/ps-listings-empty-360-menu-open-fold.png`

**After the patch, first screen** (each also has a matching `-full.png`, for example
`scratchpad/web_shots/ps-fix-listings-search-390-full.png`):

- `scratchpad/web_shots/ps-fix-home-390-fold.png`
- `scratchpad/web_shots/ps-fix-home-360-fold.png`
- `scratchpad/web_shots/ps-fix-home-1440-fold.png`
- `scratchpad/web_shots/ps-fix-listings-empty-390-fold.png`
- `scratchpad/web_shots/ps-fix-listings-empty-360-fold.png`
- `scratchpad/web_shots/ps-fix-listings-empty-1440-fold.png`
- `scratchpad/web_shots/ps-fix-listings-search-390-fold.png`
- `scratchpad/web_shots/ps-fix-listings-search-360-fold.png`
- `scratchpad/web_shots/ps-fix-listings-search-1440-fold.png`
- `scratchpad/web_shots/ps-fix-search-results-390-fold.png`
- `scratchpad/web_shots/ps-fix-search-results-360-fold.png`
- `scratchpad/web_shots/ps-fix-search-results-1440-fold.png`
- `scratchpad/web_shots/ps-fix-car-390-fold.png`
- `scratchpad/web_shots/ps-fix-car-360-fold.png`
- `scratchpad/web_shots/ps-fix-car-1440-fold.png`
- `scratchpad/web_shots/ps-fix-dealership-390-fold.png`
- `scratchpad/web_shots/ps-fix-dealership-360-fold.png`
- `scratchpad/web_shots/ps-fix-dealership-1440-fold.png`

**After the patch, Filters sheet open**

- `scratchpad/web_shots/ps-fix-listings-search-390-filters-open-fold.png`
- `scratchpad/web_shots/ps-fix-listings-search-360-filters-open-fold.png`
- `scratchpad/web_shots/ps-fix-dealership-390-filters-open-fold.png`
- `scratchpad/web_shots/ps-fix-dealership-360-filters-open-fold.png`

**Screenshots added in the second pass** (all in `scratchpad/web_shots/`):

**Second pass, live replay at 320 and 375 px, first screen**

- `scratchpad/web_shots/ps2-car-320-fold.png`
- `scratchpad/web_shots/ps2-car-375-fold.png`
- `scratchpad/web_shots/ps2-dealership-320-fold.png`
- `scratchpad/web_shots/ps2-dealership-375-fold.png`
- `scratchpad/web_shots/ps2-home-320-fold.png`
- `scratchpad/web_shots/ps2-home-375-fold.png`
- `scratchpad/web_shots/ps2-listings-empty-320-fold.png`
- `scratchpad/web_shots/ps2-listings-empty-375-fold.png`
- `scratchpad/web_shots/ps2-listings-search-320-fold.png`
- `scratchpad/web_shots/ps2-listings-search-375-fold.png`
- `scratchpad/web_shots/ps2-search-results-320-fold.png`
- `scratchpad/web_shots/ps2-search-results-375-fold.png`

**Second pass, with the tested patch plus the PS-16 and PS-17 rules, 320 and 375 px**

- `scratchpad/web_shots/ps2-fix-car-320-fold.png`
- `scratchpad/web_shots/ps2-fix-car-375-fold.png`
- `scratchpad/web_shots/ps2-fix-dealership-320-fold.png`
- `scratchpad/web_shots/ps2-fix-dealership-375-fold.png`
- `scratchpad/web_shots/ps2-fix-home-320-fold.png`
- `scratchpad/web_shots/ps2-fix-home-375-fold.png`
- `scratchpad/web_shots/ps2-fix-listings-empty-320-fold.png`
- `scratchpad/web_shots/ps2-fix-listings-empty-375-fold.png`
- `scratchpad/web_shots/ps2-fix-listings-search-320-fold.png`
- `scratchpad/web_shots/ps2-fix-listings-search-375-fold.png`
- `scratchpad/web_shots/ps2-fix-search-results-320-fold.png`
- `scratchpad/web_shots/ps2-fix-search-results-375-fold.png`

**Second pass, ZIP row (PS-16), before and with the fix, at 320, 360 and 390**

- `scratchpad/web_shots/ps2-fix-listings-empty-320-zipbanner.png`
- `scratchpad/web_shots/ps2-fix-listings-empty-360-zipbanner.png`
- `scratchpad/web_shots/ps2-fix-listings-empty-390-zipbanner.png`
- `scratchpad/web_shots/ps2-listings-empty-320-zipbanner.png`
- `scratchpad/web_shots/ps2-listings-empty-360-zipbanner.png`
- `scratchpad/web_shots/ps2-listings-empty-390-zipbanner.png`

**Second pass, home footer (PS-17), before and with the fix**

- `scratchpad/web_shots/ps2-home-360-footer-after.png`
- `scratchpad/web_shots/ps2-home-360-footer-before.png`
- `scratchpad/web_shots/ps2-home-390-footer-after.png`
- `scratchpad/web_shots/ps2-home-390-footer-before.png`

**Third pass: scripts, results and screenshots**

- Working folder `scratchpad/ps_third/`; durable copy (scripts, small JSON results, logs, the request ledger and
  a redacted `home_now3.headers`; 700 KB; no screenshots, no HTTP cache, no HTML) in `docs/web_speed_search_2026_11/phone-spacing_raw/third_pass/`
  (`scripts/`, `results/`, `logs/`).
- Scripts: `capture3.py` (live capture of the new pages), `states3.py` (compare tray by clicks), `third_tests.py`
  (`drawer`, `dealer`, `register`, `short`, `fd`, `widths`, `landscape`, `covers`), `compare_both.py`, `run_final.sh`, the six `probe_*.py`
  files, `cite_check3.py` and `ps3_fixes.css`.
- Results: `results/t_<test>_<variant>.json`, `compare_state_<base|fix>_n<2|4>.json`, `comparecar_<variant>.json`, one
  `<page>-<width>.json` per capture (the audit of each new page), `remeasure_third.json` (the reproduction of the second pass),
  `live_requests.jsonl` and `live_count.json` (9 live requests).
- Screenshots, all in `scratchpad/web_shots/` and all prefixed `ps3-`:

  | what | files |
  |---|---|
  | five new pages and the zero-result URL, live, first screen and full page | `ps3-login-390-fold.png`, `ps3-register-390-fold.png`, `ps3-find-dealers-390-fold.png`, `ps3-compare-390-fold.png`, `ps3-premium-390-fold.png`, `ps3-no-results-390-fold.png`; the same with `360` and `1440`, and `-full.png` for each |
  | compare tray, before | `ps3-compare-base-listings-390-fold.png`, `ps3-compare-base-listings-390-bottom.png`, `ps3-compare-base-car-390-fold.png`, `ps3-compare-base-n4-listings-390-fold.png` (and `360`, `1440` where run) |
  | compare tray, after | `ps3-compare-fix-n2-listings-390-fold.png`, `ps3-compare-fix-n4-listings-390-fold.png`, `ps3-compare-fix-car-390-fold.png`, `ps3-comparecar-both-390-fold.png` (first patch plus third-pass rules) |
  | menu drawer, before and after | `ps3-drawer-base-listings-empty-390-fold.png`, `ps3-drawer-fix-listings-empty-390-fold.png` (also `360`, and `-car-`) |
  | dealership links, before and after (clip) | `ps3-dealer-links-base-390.png`, `ps3-dealer-links-fix-390.png` (also `360`, `1440`) |
  | register header | `ps3-register-base-390-fold.png`, `ps3-register-ps3-390-fold.png`, `ps3-register-phone-390-fold.png`, `ps3-register-both-390-fold.png` (also `360`, `1440`) |
  | find dealers, before and after | `ps3-fd-base-390-full.png`, `ps3-fd-fix-390-full.png` (also `360`, `1440`) |
  | login, before and after | `ps3-login-base-390-fold.png`, `ps3-login-fix-390-fold.png` |
  | landscape 844x390 | `ps3-landscape-base-<page>-844l-fold.png` for home, register, listings-search, car, dealership, find-dealers |

