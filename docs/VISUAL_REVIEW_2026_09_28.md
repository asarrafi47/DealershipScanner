# Visual review, 2026-09-28

Scope: the Sarrafi Cars web app at http://localhost:5001 (branch `feature/http-only-scans`), captured headlessly at 1440 px and 390 px, logged out and as a throwaway free user. Every finding below was verified against the screenshot, the live page, the template/JS/CSS, and (where a number is claimed) read-only SQL on the app's Postgres. Screenshots and the capture script live in `workspace/visual_review_2026_09_28/` (`capture.py`, `manifest.jsonl`, `creds.json`).

Owner's rule applied throughout: no "AI template" look (no gradients, hero cards, emoji, generic dashboard tiles); palettes navy+champagne or graphite+cobalt; data must be correct, sourced, and findable at a glance.

Severity rubric: **high** = wrong or missing data, or a task the shopper cannot complete; **medium** = ambiguity or friction, task still completable; **low** = polish.

## Summary

The app is in good shape where it counts least and weakest where it counts most. Pages render cleanly at 1440, the navy+champagne palette holds on most surfaces, the Specs tab already sources colors and options ("Per window sticker", "trim catalog"), and the database behind the pages is rich: 214,678 active listings, vPIC decodes for nearly every VIN, EPA links, dated price provenance, 142,470 rows with real days-on-lot data. The three biggest problems are all about numbers, not pixels. First, the default `/listings` page shows advertised monthly payments as sale prices ("$85" Transit, 23 consecutive "$122" Jettas and Taoses) and Best-match sorts them to the top, because the `payment_listed` guard is never fed by the listings serializer (IH-01, ES-2). Second, the car Overview leads with three fabricated or mislabeled figures: "Days on Market: 0" is days since our scanner first saw the row, the "Fresh inventory: firm pricing" verdict is derived from that, and "Predictive Local Turnaround: 34 Days" is a hard-coded segment+brand constant on a banned gradient bar that contradicts the app's own data (real CR-V median 50.5 days) (DC-3, IH-09, ES-3, TC-4, SA-04). Third, data the app already holds never reaches the page: the market band says "too few comparable listings" for a trim with 406 active comparables because `market_price_stats` was last computed 2026-07-18 and nothing schedules a rebuild (TC-1); price history is double-JSON-encoded so the widget renders "No pricing adjustments" for all 39,824 listings that have one (TC-5); the compare page prints "Packages & options —" for 99.98% of cars with a packages blob and flags a false transmission difference against vPIC (DC-7); a dealer's $722 markup over its own feed MSRP is hidden by a one-directional trust rule (DC-4, IH-06); and the trim ladder drops the car's own trim (DC-2, TC-7). Close behind: engine-only horsepower shown unqualified on ~36,700 hybrids (DC-1), and unknown makes/models silently stripped from the URL so a "Toyota Supra near 92694" search shows 4,569 cars of every make (ES-1).

## Ranked findings

Sorted high to low, then by breadth of impact. Findings that describe the same defect from different lenses are grouped on one line where practical and cross-referenced in the per-page sections.

| # | ID | Page | Sev | Problem | Proposal | Template | Effort |
|---|----|------|-----|---------|----------|----------|--------|
| 1 | IH-01 / ES-2 | listings | high | Monthly payments ($85, $122) rendered as sale prices on the first screen of the default Browse page; Best-match sorts them first. 357 active rows priced $1–999; $28,443 Taos became $122 in the 09-28 scan. `/api/listings/cars` returns `deal_score=null`, `market=null`, so the `payment_listed` branch never fires. | Add `payment_listed = is_payment_shaped_price(price, year=year)` to the listings serializer; branch `renderCardHtml` on it ("$122/mo advertised — price not listed"); exclude from price sort/Best match; gate the write so a sub-$1k value never overwrites a five-figure price without a provenance flag. | `frontend/static/main.js` L1370-1442; listings serializer feeding `/api/listings/cars`; `backend/utils/market_price.py` | S (display) / M (write gate) |
| 2 | DC-3 / IH-09 / ES-3 | car (Overview) | high | "Days on Market: 0 Days" is days since our first scan (`first_seen_at` = today; 18,873 of 214,678 active rows first seen today); "FRESH INVENTORY: FIRM PRICING" is derived from it; "Predictive Local Turnaround: 34 Days" is `SEGMENT_BASELINE_DAYS.suv 42 + BRAND_MODIFIER_DAYS.honda −8`, no listing or local input. Sits directly under a badge admitting "too few comparable listings". | Replace with one sourced line "First seen by Sarrafi Cars on Sep 28, 2026 (0 days ago)"; show the leverage badge only after ≥7 observed days; remove the turnaround card until computed from `listing_removed_at − first_seen_at` (see TC-4). | `frontend/templates/car.html` L519-545; `frontend/static/car_page.js` L2465-2517, 2634-2653; `frontend/static/car_page_helpers.js` L32-51, 279-291 | S (remove) / M (compute) |
| 3 | TC-4 / SA-04 | car (Overview) | high | The "Market Turnaround Velocity" gauge is a five-stop grey→amber→green `linear-gradient` (banned), the marker fails contrast (1.5:1) and colour/position disagree (amber = "average" placed in the "hot" zone); the number contradicts real data (Honda CR-V: 4,102 removed listings, median 50.5 days, p75 52, vs the constant 34). | Delete the gauge. Put a two-row table "Days on lot for this model" (median, p75, n, date range) computed read-time from `cars` for same make+model within the shopper's radius, plus "This listing: N days". If a chart is wanted, a 5-bucket single-navy histogram, no gradient. | `car.html` L534-551; `car_page.js` L2595-2655; `car_page_helpers.js`; `css/03-car-page.css` L2342-2432 (gradients at 2349, 2380) | M |
| 4 | TC-1 | car (Overview) | high | Hero says "No market read — too few comparable listings" for a 2027 CR-V Hybrid Sport-L with 407 active same-trim listings across 49 dealers (median $41,298; this car $41,297). `market_price_stats` has 584 rows, all `computed_at 2026-07-18`, 20 for MY2027, zero for any CR-V; nothing schedules `compute_market_stats.py`. `has_pricing_insights` is therefore false for every viewer, paid or not. | Schedule the rebuild (LaunchAgent or nightly script). Then render the Market price band block as one horizontal band (p25–p75 bar, median tick, this listing marked) with "n listings · d dealers · computed <date>", and a 3-row table beside it. Replace the hero badge with the band when n ≥ 5. | `car.html` L315-340, 555-582; `backend/intelligence/market_pricing.py`; `backend/scripts/compute_market_stats.py` | M (schedule is S) |
| 5 | TC-5 | car (Overview) | high | "Price Adjustment History" renders "No pricing adjustments recorded yet" for every car, including 1013935 with six dated points: `_price_history_json_for_vdp` returns a JSON string and `car.html:523` applies `\| tojson` again, so `JSON.parse` yields a string, `Array.isArray` fails, and the list renderer is dead code. 39,824 active listings carry ≥2 price points. | Fix the double-encoding first (return a list from the serializer or drop `\| tojson`). Then render as a 3-column table (Date · Price · Change, newest first, ≤8 rows) with a 240×48 single-navy step sparkline when ≥3 points. Dedupe same-day/source flips before collapsing oscillations, since 1013935's alternation looks like two scan sources disagreeing. | `backend/utils/car_serialize/serialize.py` L551-583; `car.html` L520-532; `car_page.js` L2460-2575 | S (bug) / M (table) |
| 6 | DC-7 | compare | high | "Packages & options — / —" although both rows hold 20 features and a warranty block: `_package_summary` reads only `packages_normalized`/`possible_packages`; 102,910 of 104,252 active blobs start with `{"features"`, only 22 carry the read keys. Transmission row flags "Automatic vs CVT" as a difference; vPIC says e-CVT vs CVT (both CVT family), 355 active 2027 CR-V Hybrids are bucketed Automatic from feed code DDU. | `_package_summary`: fall back to `pkg['features']` (first 8), add a Warranty row from `pkg['warranty']`; relabel "Features & options". Transmission: prefer vPIC `TransmissionStyle` in `serialize.py` when present; show "Automatic (feed: DDU)" muted; do not mark differing when both normalize to the CVT family. | `backend/utils/compare_specs.py` L70-97, 167-168; `serialize.py` L239-258; `frontend/templates/compare.html` | S |
| 7 | DC-1 | car (Specs) | high | "Horsepower 145 hp" for a strong hybrid is the gasoline engine alone (vPIC `EngineHP`), not system output (204 hp); EPA hp (190, non-hybrid) is correctly gated, then `serialize.py:324-327` fills from `vpic_horsepower()` with no electrification check. 36,711 of 39,761 HEV/PHEV decodes on active listings carry a numeric `EngineHP`. No source rendered. | When `ElectrificationLevel` contains HEV/PHEV or `FuelTypeSecondary='Electric'`, suppress the vPIC fallback or emit `horsepower_note='engine only (vPIC); hybrid system output not filed'`; render "145 hp · engine only, per NHTSA vPIC"; add a source tag (vPIC / trim page) to every hp value. | `car.html` ~L676; `serialize.py` L311-327; `backend/utils/vpic_specs.py:specs_from_decode` L109-111 | S |
| 8 | ES-1 | listings | high | An unrecognised or zero-result make/model is silently stripped from the URL (`history.replaceState` in `syncUrl`) and the shopper is shown every car near the ZIP: `make=Zzzz` → 16,488 cars; `make=Toyota&model=Supra&radius=10` drops both and shows 4,569 cars of every make; `make=Ferrari&zip=37402` drops a real make. Reachable from saved-search links. The designed `#empty-state` works for typed search, not URL-borne filters. | Keep the value as a chip ("Make: Supra — no listings") and show `#empty-state` naming the filter with a one-click "Remove"; if the URL must be normalised, put a `role=status` banner above the grid saying what was dropped. Never rewrite the URL without telling the user. | `frontend/templates/listings.html` L494-502; `frontend/static/main.js` L1882 (`syncUrl`), `listings_boot.js` | M |
| 9 | DC-2 / TC-7 | car (Specs, Trim lineup) | high | Ladder shows only TRAILSPORT and SPORT for a Sport-L; `resolve_trim_ladder(...)` returns `listing_trim='Sport'`, `matched=False`, steps from a 2026 CSV; Sport-L (692 active) and Sport Touring (439) are absent; no "This vehicle", no source line (confidence line needs `quality=='high'` or matched, caveat needs >3 steps). Prices exist for every rung (Sport $38,185 · TrailSport $41,205 · Sport-L $41,298 · Sport Touring $44,625). | Exact-match the listing trim before prefix-matching; refuse to render when the listing's trim is not in the steps; build CR-V Hybrid rungs from data. Render the ladder as a table: Trim · Market median (n) · Sticker median (`trim_msrp_bands`) · Δ vs this car, current row highlighted, OEM order kept; always print the source line. | `car.html` L948-1010; `backend/enrichment/trim_ladder/selection.py` L357; `backend/routes/cars_pages.py` L589; `frontend/static/car_trim_ladder.js` | M |
| 10 | IH-06 / DC-4 | car (price header) | high | New CR-V listed $722 above its feed MSRP ($40,575, corroborated by 18 listings across 11 dealers) shows the asking price alone: `msrp_trust` rule (b) rejects any MSRP below price for every source, so markups are hidden and only discounts shown. 29,416 of 142,451 active New listings have `0 < msrp < price` (p50 +$225, p90 +$2,349). API returns `msrp=None`. | Split rule (b) by condition: New non-CPO with MSRP in band and below price → return it as "Dealer-listed MSRP" with "+$722 over MSRP (dealer feed)"; keep the drop for Used/CPO. Add the "+$x over" branch beside the "−$x vs sticker" branch. Still require a plausibility band/corroboration before printing "over sticker". | `car.html` L256-300; `backend/utils/msrp_trust.py:resolve_display_msrp` L240-243 | S (rule) / M (corroboration) |
| 11 | M3 / TC-3 | compare (390) | high | Only one of two compared cars is visible: `.compare-table` min-width 640 px in a 340 px wrap, label column static, car 2 at x=464–665. At no scroll offset are two values of a row visible together; scrolled right the labels are gone. Eleven "differs" rows carry the blue tint with one value on screen. | Under 640 px: drop the table min-width, `th[scope=row]` sticky left (96 px, 13 px labels), car columns min-width 118 px so two fit, photo 80 px, right-edge fade + `scroll-snap-type: x mandatory`; "View listing" buttons in a sticky bottom row. Or render a stacked spec list with a sticky car-header strip. | `compare.html`; `css/10-pages.css` L144-185 (155, 183); `css/11-overrides.css` L178 | M |
| 12 | IH-02 | listings | medium | Cards never state condition; a 2027 Taos at 5 mi and a 2011 Prius at 172,567 mi differ only by odometer; the 2011 RAV4 at "0 mi" defeats even that. `carListingCondition()` already normalizes but is used only for filtering. | Prepend the normalized token to the meta line ("New · 5 mi · Gasoline · FWD" / "Pre-owned · …"); map the 8 raw spellings to New / Pre-owned / CPO; keep the CPO badge. | `main.js` L1444-1449, L166; `sc-helpers.js` L140 | S |
| 13 | DC-6 / IH-03 | listings, car, compare | medium | "0 mi" printed as fact for a 2011 used RAV4 (cars 1434577, `mileage=0`, condition Used). Across active rows mileage is NULL for 0 and 0 for 74,695; 752 Used rows have 0, so 0 is the feed's "not listed" sentinel. `carListingCondition` treats `mileage===0` as "new" when condition is blank. Same on the car hero (headline weight), At-a-glance and compare. | When condition ≠ New (or year < current−1) and mileage is 0/null, print "Mileage not listed" in the card, hero, tile and compare row; exclude from `mileage_asc`; drop the `mileage===0 → new` inference. | `main.js` L1445, L169-172; `car.html` L300-305, L491; `backend/utils/compare_specs.py:_fmt_mileage` | S |
| 14 | IH-08 | car (hero) | medium | Neither condition nor location is above the fold: eyebrow is only "HENDRICK HONDA"; Year tile duplicates the title; "Charlotte, NC" appears only in the Dealership tab and the dealer's own photo watermark. City/state/zip and the session ZIP are already in the route. | Eyebrow "Hendrick Honda · Charlotte, NC 28273 · N mi from <zip>" (haversine from dealer lat/lon to session zip); replace the Year tile with Condition. | `car.html` L224, L487-495; `backend/routes/cars_pages.py` L690; `backend/listings/dealer_map.py` L119-121 | S |
| 15 | IH-07 / M2 | car (390) | medium | First 390 px screen is a 500 px-tall gallery box letterboxing a 4:3 photo (~268 px drawn, ~115 px grey bands), pager and thumbnails; price first appears at y≈902, under the sticky "View listing" bar. The mobile rule at `03-car-page.css:1599` (`height: min(56vw,340px)`) is dead: the unconditional `height:500px` at L1754 comes later with equal specificity. | Move the L1754 block above the 700 px media block (or scope it `min-width:701px`); mobile rule `aspect-ratio: 4/3; height:auto; max-height:none`. Consider ordering eyebrow + h1 + price above the gallery on ≤640 px and showing the price in the sticky bar. | `car.html` L178-330; `css/03-car-page.css` L1599, L1754 | S (CSS) / M (reorder) |
| 16 | DC-5 / ES-5 | car (History) | medium | "Detailed history report available. Open the full report below." and a primary "View full CARFAX report" button for a VIN with `carfax_url NULL`, `history_highlights []`, 0 mi New; the button is carfax.com's paid VIN lookup. Copy at `car.html:1307` is unconditional; L1310-1311 substitute the generic lookup. | Branch on `car.carfax_url`: without one, "No history report on file for this VIN (new vehicle, 0 mi as listed)" and demote the link to a secondary "Look up this VIN on CARFAX (may require purchase)" beside the free NHTSA recall check. | `car.html` L1297-1314; `car_page.js` L430-563 | S |
| 17 | DC-8 | car (Dealership) | medium | Five filled stars + trophy-emoji "Top Rated Dealer" for a 4.8 rating: `Math.round` to whole stars, numeric value only in an `aria-label`, no fetch date (2026-07-18, 72 days old; newest fetch across all dealers 2026-07-29). `dealership.html` prints "4.7 (951 reviews)" for the same field, so the two pages disagree. | Render "4.8 · 13,610 Google reviews · as of 18 Jul 2026" with fractional star fill; reuse the dealership.html format; replace the emoji badge with a plain text tag or drop it; pass `google_rating_fetched_at` into `dealer_info`. | `car.html` L231-240, L1341-1350; `car_page.js` L2268-2300; `car_page_helpers.js` L232-242 | S |
| 18 | TC-6 | car (Specs) | medium | "Estimated Future Value" is `price × residual^(step/5)` with residual clamped 0.3–0.7 (41,297 × 0.5^(1/5) = 35,951 reproduces the footer), filled with a blue-to-white `createLinearGradient`. The DB holds observed prices for the same trim: 2026 n=279 $39,890 @ 10 mi; 2025 n=55 $35,461; 2024 n=64 $33,110. Model's Year-1 sits $3.9k below what one-year-old Sport-Ls list for. | Replace with an observed table "What older Sport-Ls list for today" (Model year · Listings · Median price · Median mileage · vs this car) from `cars` grouped by year; plot the medians as dots on a plain single-stroke line, no fill; keep the residual model only as a dashed labelled projection beyond the newest observed year; drop the chart when <2 older years have n≥10. | `car.html` ~L926-932; `car_page.js` L1990-2180 (gradient 2095-2106); `car_page_helpers.js` L19-20, 140-174 | M |
| 19 | DC-9 | car (Specs) | low | "Efficiency 43 City / 36 Hwy" and the $5,985 fuel-cost figure come from a 2026 EPA row matched at 0.67 confidence (`prev_year` method) with no provenance, while colors/options on the same page are sourced. The number happens to be right for 2027. | Add `fuel_economy_source` ("EPA, 2026 model year, matched on engine/drive") as a muted note under the Efficiency row and the TCO fuel figure; prefix "est." when confidence < 0.7; do the same for drivetrain/fuel from vPIC. | `car.html` L718-722, ~L862; `serialize.py` L292-295 | S |
| 20 | IH-04 | listings (390) | medium | ~250 px of controls (Filters, Save this search, Saved searches, Per page, Sort, chip card) sit above the first card; first price at y≈607, second at y≈977, so one price per first screen. Nothing collapses at phone widths. | On ≤640 px fold Save/Saved into the Filters sheet, hide Per-page (keep Sort), render chips as one horizontal-scroll row under the count; target first price above y≈420. | `listings.html` L430-477; `css/09-components.css` L971-1051 | M |
| 21 | M7 | listings (390) | medium | Mobile filter sheet inherits desktop sticky `top:16px`: fixed at 0,16 390×720, ends 108 px short of the bottom with content peeking through, and the sticky "Show results" bar floats mid-sheet covering the Make accordion header. | In the ≤960 px rule add `top:auto` (or `inset: auto 0 0 0`), `max-height: min(88vh,720px)`; move `.listings-filter-actions` to the sheet's real bottom with safe-area padding. | `css/02-listings.css` L775; `css/09-components.css` L985-1020 | S |
| 22 | IH-10 / ES-6 | dealership | medium | Header has name + "★ 4.7 (951 reviews)" but no city/state/phone/website until the Contact block at y≈4,400 of 4,743; all 24 cards repeat the "AutoSavvy San Antonio" row; the Reviews section says "No reviews yet" beneath a 951-review header because the Google source is never labelled. | Header meta line "San Antonio, TX 78249 · (726) 842-9851 · website" (+ "N mi from <zip>" when known); label the rating "4.7 · 951 Google reviews"; retitle the section "Sarrafi shopper reviews" with empty copy that cites the Google figure; pass a flag to `renderCardHtml` that suppresses the per-card dealer row on this page. | `dealership.html` L254-267, L659-672, L768-777; `backend/routes/dealership_page.py` L725; `main.js` L1413-1415, 1450-1456 | S |
| 23 | DC-10 | dealership | low | "Address: San Antonio, TX, 78249" labelled as a street address; `street_address` NULL for 217 of 621 active dealers. Lat/lon is a real point (not a ZIP centroid), so nav buttons are fine. | When `street_address` is null, label "Area: San Antonio, TX 78249 (street address not on file)"; keep the nav buttons and pin as-is. | `dealership.html` L773; `dealership_page.py` L718-725 | S |
| 24 | TC-8 | dealership | low | Inventory header is "3,958 TOTAL · 0 NEW · 3,958 PRE-OWNED"; nothing says what the lot sells. Year × count × median and top makes are one query away. | Add a 5-row "Lot at a glance" table (Model year · Cars · Median price) and a top-5 makes list; link makes to `?make=` (there is no year filter yet). | `dealership.html` L282-300 | S |
| 25 | ES-4 / SA-10 | nhtsa_recalls | medium | Opening without a VIN shows a red "Could not reach NHTSA right now (missing_vin). Try again shortly." NHTSA was never contacted (`backend/main.py` L1407-1412 returns before the fetch); the template's generic branch interpolates the raw code; status is colour-only, no role. | Add `{% elif lookup_error == 'missing_vin' %}` rendering a neutral note and a 17-char VIN input submitting `?vin=`; keep the outage box for transport errors with `role="alert"`, a leading "Error:" label and no raw codes. | `frontend/templates/nhtsa_recalls.html` L33-37; `backend/main.py` L1407-1412; `backend/routes/dealers_recalls.py` L80-98 | S |
| 26 | M1 | compare, nhtsa_recalls, login, dealership (390) | medium | Fixed 44×44 hamburger covers the first content line on every sidebar page missing from the mobile `padding-top:56px` allowlist: "HTSA recall lookup", "ack to inventory", eyebrow "COME". | Minimum: add `.compare-page-section, .nhtsa-recalls-section, .auth-section, .dealer-research` to the rule at `07-dealer-admin.css:667`. Better: a 56 px mobile top bar in the sidebar partials and `body:has(.app-sidebar) { padding-top:56px }` under the 900 px media query; delete the allowlist. | `_nav_sidebar_guest.html`, `_nav_sidebar.html`; `css/07-dealer-admin.css` L574, L660-675 | S |
| 27 | M4 | car (390) | medium | VDP tab strip overflows (scrollWidth 424 vs 364) with scrollbars hidden: reads "Overview Specs History Dealership Con"; after selecting Comments, "ew Specs History Dealership Comments". | In the ≤600 px block set `.car-vdp-tabs { flex-wrap: wrap }` and `.car-vdp-tab { flex:1 1 auto; min-height:44px; text-align:center }`; or keep one row with a right-edge mask fade and `scrollIntoView({inline:'center'})` on activation. | `car.html` L499-505; `css/03-car-page.css` L421-436; `css/11-overrides.css` L227 | S |
| 28 | M5 | dealership (390), shared tray | medium | Compare tray is 126 px tall with white chip text on a cream bar (10-pages.css overrides 09-components.css), blank thumbs (dealership page never supplies image meta), 13×16 remove and 55×17 clear controls; body reserves 72 px so the tray hides the version footer. | Reconcile the two style sets in one file (navy-soft chips with navy text); supply image meta on the dealership page; on ≤640 px chips as thumbnails only, "Compare N" as a full-width 44 px button with a 44×44 Clear; set body padding-bottom from `tray.offsetHeight`. | `listings.html` L522-530; `car.html` L1442; `css/09-components.css` L1223-1290; `css/10-pages.css` L244-285; `compare.js` L56-69, L186-222 | M |
| 29 | M6 | find_dealers (390) | medium | Location-mode pills stack into a 110×124 column of 32 px targets leaving ~250 px of the row empty; `11-overrides.css:276` forces `flex-direction: column` although the three pills (~303 px) fit the 358 px form. | Delete the override; `display:grid; grid-template-columns: repeat(3,1fr)`; spans `display:block; text-align:center; padding:12px 8px` so each option is ≥44 px tall. | `find_dealers.html` L43-56; `css/10-pages.css` L466-516; `css/11-overrides.css` L276-280 | S |
| 30 | M8 | home (390) | medium | Landing search controls are 20–31 px tall (search input 211×20, "Search →" 81×31, ZIP 72×22, radius 61×22, 13.5 px text triggers iOS focus zoom). | Under ≤640 px: min-height 44–48 px and padding on `.lps-search-input`, `.lps-search-go` and `.lps-mini input/select`; font-size 16 px on the mini controls; keep the hairline look and palette. | `landing.html` L20-45; `css/12-viewers.css` L240-349, L729 | S |
| 31 | SA-01 | login, profile, compare, find_dealers, register, nhtsa_recalls | medium | Two primary-button systems, both gradients: `.primary-button` is a 999 px glossy pill (`linear-gradient(180deg, #2a4d73, navy, navy-deep)`, inset highlight, drop shadow); `.rgs-submit` is a 10 px rectangle that is also a gradient with a `0 10px 24px` shadow. `.topbar` carries a gradient and a 3 px "wood-trim" gradient stripe. | One primary button: flat `var(--navy)`, `var(--main-radius)`, 1 px `var(--navy-deep)` border, no inset/shadow, hover `var(--navy-deep)`. Point `.primary-button`, `.rgs-submit`, `.compare-table__cta` and the find-dealers submit at it; delete the gradients from `.primary-button`, `.rgs-submit`, `.topbar` and the `--chrome-face`/`--wood-trim` consumers. | `css/01-nav.css` L538-547; `css/00-base.css` L107-135; `css/13-landing-auth.css` L251-263; `login.html:37`, `register.html:69`, `compare.html` | M |
| 32 | SA-07 | login / register | medium | Login uses `_nav_public.html` (dark sidebar, serif wordmark, boxed inputs, glossy pill, "Sign in with Google" text link); register uses `_nav_marketing.html` (dark top bar, letterspaced sans wordmark, underline inputs, flat rectangle, Google-logo button labelled "Google"). They link to each other. | Render login through the same `_nav_marketing.html` + `.rgs-stage` shell, reuse `.rgs-field` and the unified primary button; `_oauth_signin.html` emits the Google-logo button "Continue with Google" on both; one wordmark treatment. | `login.html`, `register.html`, `_oauth_signin.html` | M |
| 33 | SA-03 | compare | medium | "Differs" rows are marked by colour alone: `rgba(42,77,115,0.08)` composites to 1.13:1 against untinted rows; the intended navy label cue at `10-pages.css:220` loses specificity to L174; no aria/text cue. | Append `<span class="compare-diff-tag">differs</span>` in the row header (gated by the existing `{% if row.differs %}`), 3 px navy `border-left`, bold values; raise the L220 selector's specificity. | `compare.html` L51; `css/10-pages.css` L174, L215-220 | S |
| 34 | SA-05 | dashboard | medium | Activity block is a 2×2 grid of KPI tiles with a white-to-cream gradient (`.dash-hub-stat`, 16 px radius), one tile is "— ZIP not set" and the sentence beneath repeats it. Directly hits the owner's banned "stat tiles" pattern. | One inline stat line "18 cars viewed · 0 saved · 20 active picks" (numbers linked to their lists, 20 px semibold, no boxes); delete the ZIP tile, keep the sentence; remove the gradient token. | `dashboard.html` L32-57; `css/04-dashboard.css` L400-413; `css/11-overrides.css` L199 | S |
| 35 | TC-9 | dashboard | low | "Browsing trends" is six outlined tiles each holding one make and a count. | Plain two-column table with a proportional single-navy inline bar in the count cell, hairline rows, no boxes. | `dashboard.html` L60-71 | S |
| 36 | SA-06 | premium | medium | Six "icon + title + blurb + PREMIUM badge" tiles duplicate the Free vs Premium table directly below; plan card carries "MOST POPULAR". Icons are reused across tiles. | Delete the tile grid; add a one-line "What it does" column to the table; replace "MOST POPULAR" with a plain "Recommended" line under the Assistant price. | `premium.html` L64, L97-141, L147-200+ | M |
| 37 | ES-10 / SA-09 | home | medium | ~900 px of the landing page (data grid, "On the lot right now" strip of four cars, closing CTA) is `opacity:0` until an IntersectionObserver fires; blank in full-page capture, print (`emulate_media(print)` still [1,0,0,0]) and no-JS (all four sections including the above-fold quote stay hidden). | Add `document.documentElement.classList.add('js')` in the head and scope the hidden state to `.js .lps-reveal`; set rootMargin so sections within one viewport of the fold reveal on load; or drop the entrance animation. | `landing.html` L66-160; `css/12-viewers.css` L406-417, L776 | S |
| 38 | ES-7 | find_dealers | low | Zero-result search (99723, 10 mi) hides the pre-search panel and shows "Dealers (0)" over a ~600 px blank panel; map recentres on unlabeled Alaska with no marker or ring. A status line and radius select do exist in the form card above. | When `dealers.length === 0`, keep the results panel hidden and swap the empty block's text to "No dealers within 10 mi of 99723" with inline 25/50/100 mi buttons; keep the map centred with a radius ring. | `find_dealers.html` L109-118; `find_dealers.js` L325-370, L530-536 | S |
| 39 | ES-8 | compare | low | Invalid ids are silently dropped (`/compare?ids=1470314,999999999` → one column, no notice); removed listings are NOT dropped and render as live with a "View listing" that 404s; "—" means both "none on file" and "not checked". | Pass dropped ids to the template and render a `role=status` line; mark removed columns "no longer available"; use "None listed" vs "Not checked yet" instead of a bare dash. | `compare.html` L92-96; `backend/routes/cars_pages.py` L404-432; `backend/db/cars_repo.py` L155 | S |
| 40 | ES-9 | account_billing | low | "Free — Free" (name already "Free") and the operator note "Stripe billing is not enabled on this environment." shown to users; Included features is one sentence. | "Free plan · $0/mo" once; user statement with a `/premium` link; show the Stripe note only to admin/dev sessions; list the concrete free-tier features. | `account_billing.html` L28-40, L52-60 | S |
| 41 | SA-08 | account_profile | low | "Plans & subscription" is browser-default blue: no global `a` colour rule and no `.account-profile a` rule; same gap on the premium-branch "Billing" link. | Global `a { color: var(--navy); text-decoration-color: var(--border-medium); text-underline-offset: 3px }` in `00-base.css`; then check dev/admin pages. | `account_profile.html` L50, L55; `css/00-base.css` | S |
| 42 | IH-05 | listings | low | Card second line repeats the trim from the title ("Prius Two / TWO") or prints a lone dash; body-style line shows feed vocabulary ("Cars" 10,506, "Vans" 2,153, "Compact" 1,099, "CrewMax" 859, plus "LE", "HYBRID AWD", "5 seats"). | Omit `result-trim` when dash-like or already the title's suffix; normalize `body_style` once at serialization; use the freed line for condition + "listed N days". | `main.js` L1435-1451; `backend/utils/field_clean.py:coerce_body_style_stored` | S |
| 43 | M9 | listings (390) | low | Compare checkbox 13×13 in a 94×29 pill, heart 36×36, dealer links 16 px, banner dismiss buttons 23×22 / 23×18, "Clear all" 75×28. | 44×44 hit areas for Compare and Save (padding or `::before` inset), block-level dealer links with 12 px vertical padding, 44×44 dismiss buttons, chips ≥40 px. | `main.js` cards; `css/02-listings.css`; `css/09-components.css` L188, L872; `css/01-nav.css` L439 | S |

## Per-page findings

### Home (`/`)

Shots: `workspace/visual_review_2026_09_28/home__default__1440.png`, `home__default__390.png`

- ES-10 / SA-09 (medium): three `.lps-reveal` sections at `opacity:0` until scroll; blank ~900 px band in any non-scrolling render; no-JS hides all four.
- M8 (medium): search controls 20–31 px tall at 390 px; 13.5 px text will zoom on iOS.
- SA-01 (medium): `.topbar` gradient and wood-trim stripe on the marketing chrome.

### Listings (`/listings`, `/listings?make=Toyota&zip_code=92694&radius=25`)

Shots: `listings__default__1440.png`, `listings__default__390.png`, `listings_toyota_92694__default__1440.png`, `listings_toyota_92694__default__390.png`

- IH-01 / ES-2 (high): payments as prices, first screen of the default page, Best-match sorts them first.
- ES-1 (high): zero-result make/model silently stripped from the URL; whole regional inventory shown instead of the empty state.
- IH-02 (medium): no condition word on cards.
- DC-6 / IH-03 (medium): "0 mi" on a 2011 used RAV4; 0 is the feed's sentinel.
- IH-04 (medium): ~250 px of controls above the first card at 390 px.
- M7 (medium): mobile filter sheet pinned to `top:16px`, 108 px short, "Show results" floats mid-sheet.
- IH-05 (low): redundant trim line, raw body-style vocabulary.
- M9 (low): sub-44 px card and banner controls.

### Car page (`/car/1470314`, 2027 Honda CR-V Hybrid Sport-L, Hendrick Honda)

Shots: `car__overview__{1440,390}.png`, `car__tab_specs__{1440,390}.png`, `car__tab_history__{1440,390}.png`, `car__tab_dealership__{1440,390}.png`, `car__tab_comments__{1440,390}.png`, and the `car__logged_in_*` set including `car__logged_in_tab_options__{1440,390}.png`

Hero and price header:
- IH-06 / DC-4 (high): $722 markup over the corroborated feed MSRP hidden by the one-directional trust rule.
- TC-1 (high): "No market read — too few comparable listings" against 407 comparables; `market_price_stats` stale since 2026-07-18, never scheduled.
- IH-08 (medium): no condition or location above the fold; Year tile duplicates the title.
- IH-07 / M2 (medium): 500 px letterboxed gallery at 390 px pushes the price under the sticky CTA; mobile height rule is dead CSS.
- M4 (medium): tab strip clipped at 390 px with hidden scrollbar.

Overview tab:
- DC-3 / IH-09 / ES-3 (high): "Days on Market: 0", "Fresh inventory: firm pricing", "Predictive Local Turnaround: 34 Days" — scan age and a constant presented as market facts.
- TC-4 / SA-04 (high): gradient velocity gauge; number contradicts the app's own removed-listing data; marker fails contrast; colour and position disagree.
- TC-5 (high): price history double-encoded, widget dead for all 39,824 listings with history.

Specs tab:
- DC-1 (high): engine-only horsepower on a strong hybrid, no qualifier or source; ~36,700 hybrids affected.
- DC-2 / TC-7 (high): trim ladder omits the car's own trim and the two most common trims; no "This vehicle", no source line; no prices where the DB has them.
- TC-6 (medium): "Estimated Future Value" is a clamped formula with a gradient fill while observed same-trim prices exist for 2024–2026.
- DC-9 (low): efficiency and fuel-cost figures carry no model-year/confidence provenance.

History tab:
- DC-5 / ES-5 (medium): "Detailed history report available" and a primary CARFAX button with no report on file; the button is carfax.com's paid lookup.

Dealership tab:
- DC-8 (medium): 4.8 rounded to five filled stars, numeric only in aria-label, trophy emoji badge, no fetch date (72 days old); format disagrees with the dealership page.

Comments and Options tabs: captured, no findings filed.

### Compare (`/compare?ids=1470314,1470312`)

Shots: `compare__default__1440.png`, `compare__default__390.png`

- DC-7 (high): packages row blank for 99.98% of cars with data; false transmission difference against vPIC.
- M3 / TC-3 (high): only one car visible at 390 px; no scroll cue; labels scroll away with the second car.
- SA-03 (medium): differs rows marked by a 1.13:1 tint only; navy label cue is dead CSS.
- SA-01 (medium): glossy gradient "View listing" pills.
- M1 (medium): hamburger covers "Back to inventory" at 390 px.
- ES-8 (low): dropped ids silent; removed listings render as live; ambiguous dashes.

### Dealership (`/dealership/autosavvy-com`)

Shots: `dealership__default__1440.png`, `dealership__default__390.png`, `dealership__logged_in_hidden__{1440,390}.png` (hidden-state capture, no findings filed)

- IH-10 / ES-6 (medium): no location/phone in the header until y≈4,400; every card repeats the dealer row; unlabelled Google rating contradicts "No reviews yet".
- M5 (medium): compare tray unreadable chips, blank thumbs, tiny controls, covers the footer at 390 px.
- M1 (medium): hamburger over eyebrow and title at 390 px.
- DC-10 (low): city-only string labelled "Address:".
- TC-8 (low): "0 NEW" header says nothing about the lot.

### Find dealers (`/find-dealers`)

Shots: `find_dealers__default__1440.png`, `find_dealers__default__390.png`

- M6 (medium): mode pills forced into a column at 390 px, 32 px tall.
- SA-01 (medium): gradient "Search dealers" pill.
- ES-7 (low): zero-result state is a blank panel and an unanchored map (status line and radius select do exist).

### NHTSA recalls (`/nhtsa-recalls`)

Shots: `nhtsa_recalls__default__1440.png`, `nhtsa_recalls__default__390.png`

- ES-4 / SA-10 (medium): red "Could not reach NHTSA (missing_vin)" for an empty-input state; raw error code; colour-only status.
- M1 (medium): hamburger covers the "N" of the H1 at 390 px.

### Login and register (`/login`, `/register`)

Shots: `login__default__{1440,390}.png`, `register__default__{1440,390}.png`

- SA-07 (medium): two different chromes, input styles, wordmarks and OAuth buttons in one flow.
- SA-01 (medium): both primary buttons are gradients (the register one only looks flat).
- M1 (medium): hamburger over the "Welcome" eyebrow at 390 px.

### Dashboard (`/dashboard`, logged in)

Shots: `dashboard__logged_in__1440.png`, `dashboard__logged_in__390.png`

- SA-05 (medium): gradient KPI tile grid with an empty "—" tile; banned pattern.
- TC-9 (low): browsing trends as six boxed tiles.

### Premium (`/premium`)

Shots: `premium__default__1440.png`, `premium__default__390.png`

- SA-06 (medium): six feature tiles duplicate the comparison table; "MOST POPULAR" badge.

### Account profile (`/account/profile`, logged in)

Shots: `account_profile__logged_in__{1440,390}.png`

- SA-08 (low): default-blue "Plans & subscription" link.

### Account billing (`/account/billing`, logged in)

Shots: `account_billing__logged_in__{1440,390}.png`

- ES-9 (low): "Free — Free" and the operator Stripe note.

## Ship in 1.4.0

S-effort items at high or medium severity, plus the S-sized first step of three M items. Ordered by value.

1. TC-5 — fix the double `tojson` on `data-price-history` (one-line bug; table redesign can follow).
2. IH-01 / ES-2 — `payment_listed` on the listings serializer + card branch + exclude from price sort (the write-path gate is the M follow-up).
3. DC-1 — hybrid guard on the vPIC horsepower fallback + source tag.
4. DC-7 — packages fallback to `features`, warranty row, vPIC transmission precedence.
5. IH-09 / DC-3 / ES-3 — replace the Days-on-Market block with a sourced "First seen" line, gate the badge, remove the turnaround card and its gradients (TC-4's computed replacement is the M follow-up).
6. TC-1 — schedule `compute_market_stats.py` (the band chart is the M follow-up).
7. IH-06 / DC-4 — condition-aware rule (b) in `msrp_trust` and the "+$x over MSRP" branch.
8. IH-02 — condition token on cards.
9. DC-6 / IH-03 — "Mileage not listed" for Used/older cars at 0 mi, on cards, hero, tiles and compare.
10. IH-08 — location in the eyebrow, Condition tile replaces Year.
11. M2 — reorder the dead mobile gallery-height rule.
12. DC-5 / ES-5 — branch the History copy on `carfax_url`.
13. DC-8 — numeric rating, fetch date, drop the emoji badge.
14. ES-4 / SA-10 — `missing_vin` branch on the recall page.
15. M1 — extend the mobile padding-top allowlist (or the `body:has` rule).
16. M4 — wrap the VDP tabs at ≤600 px.
17. M6 — three-column mode picker on find-dealers.
18. M7 — `top:auto` on the mobile filter sheet.
19. M8 — 44 px landing search controls, 16 px fonts.
20. IH-10 / ES-6 — dealership header meta line, "Google reviews" label, "Sarrafi shopper reviews" retitle, suppress per-card dealer row.
21. SA-03 — "differs" tag and border on compare rows.
22. SA-05 — dashboard Activity as one stat line.
23. ES-10 / SA-09 — `.js .lps-reveal` scoping on the landing page.

## Next

M-effort and low-severity items.

High/medium, M effort:
- ES-1 — keep zero-result filters as chips with the empty state; stop rewriting the URL silently.
- DC-2 / TC-7 — trim ladder exact-match + refuse-to-render + table with market/sticker medians.
- TC-4 — "Days on lot for this model" table from `listing_removed_at − first_seen_at`.
- TC-1 — market band chart and hero badge once stats are fresh.
- TC-5 — price history table + step sparkline, with same-day/source dedupe.
- M3 / TC-3 — sticky-label, two-column compare table at 390 px.
- IH-06 — corroboration/plausibility band before printing "over sticker".
- IH-01 / ES-2 — write-path gate so sub-$1k values never overwrite a five-figure price unflagged.
- TC-6 — observed depreciation table + plain line; drop the gradient fill.
- IH-04 — collapse the listings toolbar at ≤640 px.
- IH-07 — order title/price above the gallery on mobile; price in the sticky bar.
- M5 — compare tray style reconciliation, image meta on the dealership page, measured body padding.
- SA-01 — one flat primary button; delete the gradients from `.primary-button`, `.rgs-submit`, `.topbar`.
- SA-07 — one auth shell for login and register.
- SA-06 — delete the premium tile grid; fold the copy into the table.

Low (polish):
- DC-9 — efficiency/fuel-cost provenance note.
- DC-10 — "Area:" label when no street address.
- TC-8 — "Lot at a glance" table on the dealership page.
- TC-9 — browsing trends as a hairline table with inline bars.
- ES-7 — find-dealers zero-result copy and radius buttons.
- ES-8 — compare dropped-id notice, removed-listing marker, "None listed" vs "Not checked yet".
- ES-9 — billing page copy.
- SA-08 — global anchor colour rule.
- IH-05 — trim-line dedupe and body-style normalization.
- M9 — 44 px card and banner controls.

## Pages and states that could not be captured

- Car page, logged out, Options tab: not rendered for guests (`car.html:503` `{% if show_options_tab %}`), so no logged-out `car__tab_options` shot at either width; the logged-in Options shots cover the panel.
- The VDP has no separate "market" or "window sticker" tab (tabs are Overview / Specs / Options / History / Dealership / Comments), so those were not captured as pages.
- No cookie/consent banner exists in the app, so no dismissed-banner state.
- The landing page's below-fold sections (ES-10 / SA-09) are blank in the full-page captures by construction; they were verified live with scrolling instead.
- The mobile filter sheet (M7), zero-result find-dealers state (ES-7), zero-result listings (ES-1) and open compare tray on the dealership page (M5) are not in the review shots; each was reproduced live with Playwright and the measurements are in the finding text. Scratch captures (`listings_filters_open.png`, `cmp_scrolled.png`, `home_nojs.png`) were left in the session scratchpad, not the workspace folder.

Capture side effects to clean up (none touched the repo or the inventory DB):
- Throwaway review user `vr0928_151241` (id 1125) in `backend/users.db`, with one hidden dealership and two search-history rows.
- `scripts/create_demo_free_user.py --help` does not honour `--help` and created/reset a `demo_free` account (id 1) in the repo-root `users.db` (the script's default `DB_PATH`, not the running app's `backend/users.db`). Both files are gitignored; the repo-root file (86 KB, mtime 15:08) may be safe to delete if it did not exist before.
