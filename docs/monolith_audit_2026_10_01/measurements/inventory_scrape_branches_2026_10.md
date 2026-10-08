# inventory_scrape branches: what discovery needs (P0B.6)

- Unit: P0B.6 (dead-code-22) of `docs/REMEDIATION_PLAN_2026_10.md`. Feeds D-DC5 and the Phase 17B entry gate.
- Dates: drafted 2026-10-07; every claim re-checked against the code and the artifacts on 2026-10-08 (section 7 lists what the review changed).
- Revision: code read at `dbdbf5cba` in a detached worktree. All line numbers are at that revision unless marked HEAD. At HEAD `afa063a22` every cited file is unchanged except two:
  - `discovery_capture.py` (P1B.4 moved recipe I/O to threads; lines after :84 move by +4, so the scrape call is :128-132, the dedupe :151-165 and the promote :167-185);
  - `recipes.py` (P1B.2-P1B.4): `EndpointRecipe.key` is at :190, `promote_from_ledger` at :889 and `_learn_dealer_com_page_size` at :1037.
- Method: read-only code study, checked against artifacts already on the MBP:
  - 1,830 NetworkObserver ledgers in `workspace/debug/netledger_*.json` (360 dealers, 2026-07-04 to 2026-09-26 UTC);
  - the 10 discovery capture reports in `workspace/dealer_logs/*/capture_*.json`;
  - the recipe files in `workspace/recipes/*.json`, read with plain `json.load` (never `load_recipes`, which can write);
  - the scanner logs: 333 text files in `workspace/*.log`, `workspace/scanlogs/` (without `imgbatch_listener/`) and `workspace/pipeline/`. Section 3 gives the counting rules.

  No browser was launched, no network request was made and no database was opened. Nothing was written except this file.
- Limits:
  - Most ledgers come from the browser-era scan. `dealer_run` called the same `scrape_inventory_path` until `ff76d4e58` (2026-09-26), and inventory_scrape.py and the scrapers are unchanged since then. Three things differ:
    - City and state: `dealer_run` passed `dealer_city`/`dealer_state`; discovery passes neither.
    - Path order: `dealer_run` ran the paths concurrently; discovery runs them one after another, each under a 240 s timeout.
    - Promotion: `dealer_run` handed every path's endpoints straight to `promote_from_ledger`, with no dedupe and no validation. Discovery dedupes across paths and validates.
  - The observer has ledgered non-JSON XHR bodies (HTML fragments) only since 2026-09-26 (`b20224d24`, `3c181a70e`). A July or August ledger without an endpoint for a branch therefore says nothing about HTML-fragment XHRs. The rows below say so where it matters.
  - Log timestamps are the host's local time, and that time zone changed over the summer. Ledger and recipe stamps are UTC.
  - Mini logs were not read: SSH to the mini is not approved.

## 1. What discovery keeps from `scrape_inventory_path`

`discovery_capture.capture_endpoints` (discovery_capture.py:124-130) is the only caller. `git grep` finds no other in `backend/`, scripts or tests. It unpacks the five-tuple as follows:

| Output | Use in discovery | Where |
|---|---|---|
| `records` (intercept bodies, plus the synthetic `*_inventory` rows the platform branches append) | Its length only is added to `out["records"]`. The count is logged in discovery.md (discovery_probe.py:359), printed (:468) and copied by `runner.run_discovery_capture` and the lifecycle step. No decision reads it: lifecycle routes on `skipped`, `error` and `recipes_after` (lifecycle.py:238-244). | discovery_capture.py:129; runner.py:82; lifecycle.py:236 |
| `html` (page content) | Discarded | :124 |
| `url_denied` | One log line | :131-132 |
| `card_locations` | Discarded (`_card_locs`) | :124 |
| `captured_endpoints` = `observer.ledger.endpoints` | Deduped across paths by `(method, url without query)`. `html_fragment_cards` endpoints are the exception: they also key on the section query (:153-157). `promote_from_ledger(validate=True)` then turns the deduped endpoints into recipes. A path that hits the 240 s timeout (:127) contributes nothing, because `asyncio.wait_for` cancels it before it returns. | :130, :147-161, :163-178 |

The ledger is the only output that turns into anything. A branch therefore matters to discovery only if it changes the ledger. These rules (network_observer.py) decide that:

- **Only this page's responses.** The observer sees only `page.on("response")` events of the page it is attached to (:568). It never sees HTTP done in Python (`requests`, `aiohttp`), and never sees a page in a different browser context.
- **Which responses qualify.** The status must be below 400 and not 204, 205 or 304 (:613-615). The response must be one of:
  - JSON by content-type;
  - an `xhr`/`fetch` response whose body sniffs as JSON (:619-633, :689-734);
  - an `xhr`/`fetch` response whose body is an HTML fragment with at least 5 VINs.

  A JSON body is ledgered only if `classify_payload` puts it in the capture tier, or if it wraps a VIN-bearing HTML fragment (:635-659). Document navigations (server-rendered SRP pages) are never ledgered.
- **Endpoint key.** The key is `(method, host, path)` (:387-389). The first response for a key fixes the URL and its query. A later response for the same key:
  - adds to `occurrences`;
  - raises `vehicle_rows` and the `field_coverage` fractions to their maximum;
  - fills `total_count`, the POST body or the auth headers only when the first response lacked them (:448-463).
- **Early-return snapshot.** The three platform branches return `list(observer.ledger.endpoints.values())` from inside the `try` (inventory_scrape.py:326, :356, :393). Python evaluates that list before the `finally` drains in-flight body reads (:538) and saves the ledger file (:543). A new key whose body read is still pending at that moment is in the saved ledger file, but not in what discovery promotes. The generic path returns after the `finally` (:550), so it is not affected. See finding 5.5.

An in-page `fetch()` issued through `page.evaluate` is an ordinary page request, so it does fire the response event. The dealer.com evidence in row 1b confirms this: those bodies can only have come from that path.

## 2. Classification

Ledger reach:
- **direct**: the branch's own requests become ledger endpoints.
- **indirect**: the branch makes the page fire requests that become endpoints.
- **none**: the branch's output is rows or state that discovery discards.

Verdict: **keep**, or **slim** (a slimming candidate, specified in section 4; nothing was changed here).

The six branches named by the plan map to these rows: dealer.com bulk fetch and nudge (:8) → 1a, 1b; autoWALL (:330-355) → 2a, 2b; ShopperExpress (:357-390) → 3a, 3b; PixelMotion pagination and cookie banner (:154, :272-276) → 4a-4d; card location (:102) → 5; location filter → 6.

| # | Branch | file:line | What it does | Ledger reach (evidence) | Verdict |
|---|---|---|---|---|---|
| 1a | dealer.com nudge | inventory_scrape.py:211-218 → dealer_com_bulk_fetch.py:198-228 | Runs when bulk fetch is enabled and no POST template and no JSON have been seen (:211). Clicks the first visible `.pagination-next`, `button:has-text("Next")`, `a:has-text("Next")` or `.pagination a` once, then waits up to 6 s for a template (:213-216). The page fires its own inventory XHR, which `handle_request` (:116-137) and the observer both see. | **Indirect, high.** The logs hold 610 nudge clicks on 189 dealer names. 362 of the 366 completed bulk fetches came after a nudge on the same log, dealer and path. Only 4 ran without one. The nudge runs only when no template exists, so in those 362 runs the template was captured only after the click. The selectors are generic: 248 nudges were not followed by a bulk fetch. 122 of those were on paths that are not dealer.com `*/index.htm` SRPs: `/used-vehicles/` 26, `/llm/inventory/` 21, `/new-vehicles/` 19, `/gs-vehicle/list` 11, and others. On a server-rendered first page, the nudge is the only "Next" click before the platform branches. The generic pagination loop (:426-516) clicks only when it already holds rows: `need_more` needs a total and `explore_next` needs rows. | **keep** (rename it: it is a generic "click Next once when nothing was captured" step, not a dealer.com one) |
| 1b | dealer.com bulk fetch | inventory_scrape.py:220-252 → dealer_com_bulk_fetch.py:96-185 | Re-posts the captured template to `.../ws-inv-data/getInventory` through in-page `fetch` (`page.evaluate`, :116). It sets `pageSize` to `INVENTORY_PAGE_SIZE` (default 500) and steps the `start` offsets. The template comes from the page's own `getInventory` or `getInventoryAndFacets` POST; `handle_request` rewrites the AndFacets URL to `getInventory` (:130-133). It makes up to 51 POSTs (:114), then replaces `records` and sets `bulk_complete`, which skips click pagination (:422-425). | **Direct, high, first POST only.** The page itself calls `getInventoryAndFacets`: 292 ledger endpoints, with page sizes 8-28 in the bodies that parse. All 292 `getInventory` ledger endpoints come from the bulk fetch. The 253 parseable bodies all carry `pageSize "500"` and an `inventoryParameters` holding only `start`. The other 39 are cut at 2,000 characters (an older ledger limit) but still show `"pageSize":"500"`. All 308 live dealer.com `getInventory` recipes (186 dealers) carry that body shape. Synth builds every dealer.com recipe from `camelbacktoyota-com`'s `getInventory` recipe (saved 2026-07-17, pageSize 500), swapping only the siteId (synth/platforms/dealer_com.py:18, :46-54). The replay's page-size learning (recipes.py:518-537) exists because of this body: "the captured preferences say pageSize 500; the server answers 48". POSTs 2..N hit the same key. They add only `occurrences` and can raise the max counters, but never change the stored URL or body. In the logs, 256 of the 366 bulk runs fetched more than one page (up to 17). | **keep the first POST; slim pages 2..N** |
| 2a | autoWALL HTTP | inventory_scrape.py:328-340 → autowall.py:305-382 | Runs when `provider` or the profile says autowall, or when `_is_autowall_html` matches. It opens a `requests` session: two homepage warm-ups, up to 49 SRP GETs, and one VDP GET per car that lacks spec fields (4 threads). The rows go into a synthetic `.../autowall_inventory` record, then the branch returns early (:356). | **None.** This is Python HTTP, which the observer cannot see, and the list pages are documents anyway. There are no `gs-vehicle` endpoints in any of the 1,830 ledgers, and bergetoyota-com's 3 path ledgers hold 0 endpoints. The branch runs once per path. In the logs, autoWALL rows came from 4 stores: three Long of Chattanooga stores, under 8 name spellings, and longofathens-com. All of those were browser-era scans, where the rows were the scan output; under discovery the same rows are discarded. The scan has its own HTTP route: the synth `autowall` template (synth/platforms/autowall.py:16-45; longofathens-com's live recipe). | **slim** |
| 2b | autoWALL Playwright fallback | inventory_scrape.py:341-348 → autowall.py:391-533 | Runs if the HTTP half returns nothing. It opens a **fresh browser context** (:418-422), falling back to the observed page only if that fails (:411, :423-424). It walks `/gs-vehicle/list?page=N` as document navigations, then crawls the VDPs over `requests` with the context's cookies. | **None.** It uses a different context, server-rendered documents and Python VDP HTTP. In the logs the fallback ran 12 times on 4 stores:<br>• Volvo Cars Chattanooga, twice: one run produced 269 rows (2026-06-16).<br>• Mercedes-Benz at Long of Chattanooga, once.<br>• Berge Toyota, 3 times: 0 cards on page 1, and 0 ledger endpoints.<br>• Central Houston Nissan, 6 times. This is a dealer.com site that the site profiler labelled `autowall` (confidence 0.57, synth_full.log 2026-08-04), which set `_is_autowall_early`. Steps 1a and 1b had already ledgered its dealer.com endpoints before the branch, which then spent its time on 427-byte `gs-vehicle` pages. | **slim** |
| 3a | ShopperExpress API | inventory_scrape.py:358-377 → shopperexpress.py:267-328 | `aiohttp` GET of `/wp-json/v1/vehicles`, then every VDP at concurrency 20 (20 s timeout each). The rows go into a synthetic `.../shopperexpress_inventory` record, then the branch returns early (:393). | **None.** Python HTTP, run once per path. In the logs (2026-08-04, 4 paths in parallel), Burns Honda took about 112 s for 476 VDPs, and kiaofcleveland-com about 145 s for 223 VDPs, with 0 rows extracted. kiaofcleveland-com's per-path timeline was 214 s from navigation to ledger save: 55 s of pre-branch waits, the 145 s crawl, then 12 s of page fallback. Discovery runs up to 4 paths one after another. Each path has the 240 s timeout, and a timeout drops that path's endpoints. For this dealer those endpoints were the only route into the ledger (row 3b). The scan has HTTP routes for this platform: the synth `wp_vehicles_index` template (4 live recipes: burnshonda, hondaofpasadena, kiaofchattanooga, hondaofcleveland) and the recovery strategy `shopperexpress_api` (inventory_recovery.py:85, :696-709). | **slim** |
| 3b | ShopperExpress page fallback | inventory_scrape.py:379-384 → shopperexpress.py:454-548 | Runs only when the API half returned no rows. It navigates **the observed page** to `/listings/` and `/used-listings/` (goto, :466) and reads JSON-LD with `page.evaluate`. It clicks `a.btn-next` up to 30 times, stopping when a page's JSON-LD is empty or adds no new VIN (:476-518). | **Direct for the two navigations, proven once; none observed for the Next clicks.** kiaofcleveland-com, 2026-08-04: the API listed 223 cars, but 0 VDPs parsed, so the fallback ran and read 48 JSON-LD rows. All 4 path ledgers then held `GET /wp-json/v1/vehicles/listings` and `/used-listings` (reason `score`, 50 rows, occurrences 1 each). Those are the dealer's only two live recipes (saved 2026-08-05 03:06 UTC), and they replay the whole lot. The HTTP scans wrote 220 rows on 2026-09-25 and 211 on 2026-09-28 (scan_runs.md), against API listing counts of 218 and 211. kiaofchattanooga-com is on the same platform; its API succeeded and no fallback ran, and its 4 ledgers hold 0 endpoints. These are the only `wp-json` endpoints in all 1,830 ledgers. An occurrence count of 1 means no Next click sent another request to those keys. Those ledgers predate HTML-fragment ledgering, though, so a fragment XHR fired by a click would not have been recorded either. False positive: on holmanhondacentennial-com the profiler said `roadster`, but `_is_shopperexpress_html` matched. The API returned 403, and the fallback navigated away from the SRP on all 6 paths. | **keep the two navigations; slim the JSON-LD row building; the Next clicks are unproven** (keep them only with a stop rule, spec step 6) |
| 4a | Cookie banner dismissal | inventory_scrape.py:154-156 (all sites), :281 (PixelMotion) → pixel_motion.py:214-227 | Clicks a generic "Allow all cookies" / "Accept All" / `[data-action="accept"]` button. | **Indirect.** It clears overlays that would block the Next clicks in 1a, 4c and the generic loop. It is not PixelMotion-specific. The function logs nothing, so there is no direct evidence either way. | **keep** (move it to nav; P17B.5 keeps it in pixel_motion.py until then) |
| 4b | PixelMotion detection | inventory_scrape.py:272-275 → pixel_motion.py:34-43 | `_is_pixel_motion_html(peek_html)`, or profile provider `pixel_motion`, selects the branch. | **Indirect** (it gates 4c) | **keep** |
| 4c | PixelMotion pagination clicks | inventory_scrape.py:292-310 | Clicks `.vlpm3Pages__next` up to 20 times on the observed page, 0.8 s apart. | **Direct, proven.** Page 1 is server-rendered, so nothing is ledgered without a click. mcpeeks-com, 2026-09-26: the ledgers written before 14:05 UTC (before the HTML-fragment observer change) hold 0 endpoints. From 14:05 UTC on, the new, used and cpo path ledgers each hold `VlpAjaxEndpoint.php?...inv_page=2...` (`html_fragment_cards`, 23-24 VINs). Occurrences are 10 on new, 12 on cpo and 1 on used. The certified path (1 car on one page) never did. `pipeline/mcpeeks_capture/probe4.out` shows the fragments arriving about 3.3 s apart after the lazy scroll, with no nudge line, so they come from this loop. They became mcpeeks-com's 3 live `html_cards` recipes (saved 2026-09-26 14:39 UTC). They are the only 6 HTML-fragment endpoints in all ledgers. `.vlpm3Pages__next` is not in `NEXT_SELECTORS` (constants.py:18-33), and the generic loop is never reached, because the branch returns at :326. On the used path, the only page-2 fragment was logged 12 ms after the loop ended. It reached discovery only because `await page.content()` (:325) yielded first (finding 5.5). | **keep** |
| 4d | PixelMotion SSR row parse | inventory_scrape.py:276-279, :293-299, :311-324 | Runs `parse_pixel_motion_inventory_html` on each page's HTML and appends a synthetic `.../pixel_motion_inventory` record. | **None.** The two mcpeeks-com captures before the fragment change (2026-09-26 13:24 and 13:54 UTC) report `records=4`, one synthetic record per path, with 0 endpoints and 0 recipes. | **slim** (the parser itself stays: inventory_recovery.py:466 and test_pixel_motion* use it, until P17B.4 decides `html_next_data`) |
| 5 | Card-location scrape | inventory_scrape.py:101-107, called at :209 and :475 → inventory_card_location.py:110-127 | `page.evaluate` DOM walk for VIN → lot-location text. No network. It runs once after hydration and once per generic pagination click. | **None.** Discovery discards the dict. The only consumer was `apply_card_locations_to_vehicles` in dealer_run. Its input stopped on 2026-09-26: `ff76d4e58` removed dealer_run's `scrape_inventory_path` call, which left `merged_card_locations` always empty. The call itself was deleted in `14dc85936` (2026-10-01). Today only test_inventory_card_location.py imports the module, and backend/attribution/__init__.py:33-34 already calls it "dead under HTTP-only". | **slim** (delete) |
| 6 | Location filter | inventory_scrape.py:175-198 → nav.py:490-687 (`try_apply_location_filter`), gated by `SCANNER_SISTER_STORE_FILTER` (default on, dealer/location.py:69-71, imported through the `dealer_location` alias) | Expands a "Location" facet and clicks the label closest to the dealer name. The match needs a difflib similarity of at least 0.45. Below 0.72 the label must also contain a distinctive non-make token from the name. It then clears `records`, `found_data`, the capture event, the POST template and `api_post_url`. Discovery passes no city or state (discovery_capture.py:125-126), so the match is by name only. The name is `_manifest_name(dealer_id) or dealer_id`, so it is often the slug. | **None observed.** The filter clears `records` but not the ledger. The filtered request has the same key as the unfiltered one fired earlier, so the first (unfiltered) URL and body stay in the ledger. The logs hold 47 click lines (44 distinct) on 7 stores, under 10 name spellings. One click was wrong: Manheim California clicked "Your Manheim Account" at similarity 0.58. The other six stores (freewayhonda, toyotaplace, toyotaoforange, donmcgilltoyota, burientoyota, cumberlandtoyota) are all on one Typesense cluster. Their live recipes carry no location filter: `filter_by` is None, or `condition:Used` for Cumberland. The one theoretical route into the ledger is through 1b, because the reset template feeds the bulk `getInventory` POST and `prepare_bulk_post_body` keeps every template facet. It was never seen: no `getInventory` or `getInventoryAndFacets` ledger body carries a facet beyond `start`, and no clicked store was on dealer.com. Store scoping now lives in synth/replay (`recipe_synth._carscommerce_store_filter`, DEP `lc=`) and in the rooftop gate. | **slim** (delete from discovery; keep `sister_store_filter_enabled`, which dealer_run_steps/enrich.py:118 and `filter_sister_store_vehicles` (dealer/location.py:360) read) |

The rest of the function, for completeness (not in the plan's branch list):
- **The capture itself, keep:**
  - navigation, hydration and JSON wait (:150-173);
  - idle wait and drain (:200-208);
  - scroll pulses (:254-259);
  - lazy scroll (:261-270, :518-525);
  - generic pagination (:395-516; its decisions read `records`, so `records` stays internal);
  - the `finally` drain and `save_ledger` (:534-549).
- **The final `page.content()` (:527-531), slim:** its comment justifies it by the HTML recovery strategies, which P17B.4 retires, and discovery discards the result.
- **The comment at :81 is wrong.** It says the PixelMotion profile hint jumps "straight to SSR branch, skip JSON-wait loops", but `_is_pixel_motion_early` is first read at :275, after the scroll pulses and the lazy scroll (finding 5.6).

## 3. Evidence summary

| Measure | Value | Source |
|---|---|---|
| Ledgers / dealers / endpoints | 1,830 / 360 / 1,410 (July 742 files, Aug 1,016, Sept 72; 900 files hold no endpoint) | workspace/debug |
| Endpoint reasons | legacy 1,209, score 195, html_fragment_cards 6 | same |
| dealer.com `getInventoryAndFacets` / `getInventory` endpoints | 292 / 292. All 253 parseable `getInventory` bodies have pageSize "500" and `inventoryParameters` = `start` only. The 39 truncated ones still show pageSize "500" | same |
| Live dealer.com `getInventory` recipes | 308 on 186 dealers, all pageSize "500" with `start` only | workspace/recipes |
| Nudge clicks / bulk completions / bulk after nudge / multi-page bulk | 610 (189 dealer names) / 366 / 362 / 256 (max 17 pages) | logs |
| Nudges with no bulk fetch after them | 248 (122 on non-`index.htm` paths) | logs |
| autoWALL, ShopperExpress, PixelMotion endpoints in ledgers | `gs-vehicle` 0; `wp-json/v1/vehicles` 8 (kiaofcleveland-com only); `VlpAjaxEndpoint` 6 (mcpeeks-com only) | workspace/debug |
| autoWALL rows / fallback runs | rows from 4 stores; fallback 12 runs on 4 stores, rows once | logs |
| Location-filter clicks | 47 lines (44 distinct) on 7 stores, 1 wrong element | logs |
| dealer.com sections across paths (finding 5.1) | 96 multi-path runs (93 dealers) whose `getInventory` body parses on 2 or more paths. All 96 carry a different `pageAlias`/`listing.config.id` per path. | workspace/debug |
| Discovery capture reports | 10 on 5 dealers (2026-09-26 UTC):<br>• claremontcdjr and group1fordofsouthaustin (CarsCommerce, captured on dealer.com paths): 1 endpoint each.<br>• honestcardeal (Supabase): 0, 0, then 1.<br>• mcpeeks (PixelMotion): records 4 with 0 endpoints twice; then 27 records with 1 endpoint (sections collapsed by the dedupe); then 3 endpoints and 3 recipes after the section-key fix.<br>• orangecountykia: timed out on 3 dealer.com paths, 745 s, 0 endpoints. | dealer_logs/*/capture_*.json |

Counting rules for the log rows:
- Lines are matched on the exact log formats: `Dealer.com template nudge [...]`, `Dealer.com bulk fetch complete [...] — N page(s)`, `autoWALL ...`, `ShopperExpress ...` and `Location filter [...]: clicking`.
- A bulk fetch counts as "after a nudge" when a nudge line for the same dealer name and path appears earlier in the same file.
- Duplicate lines (same timestamp and text in two files) were checked: removing them changes only the location-filter count.
- "Stores" merges name spellings of one store, for example "Freeway Honda" and "freewayhonda-com".
- The 10-07 draft had slightly different totals (604 nudges, 364 bulk completions, 255 multi-page); the numbers above are the reproducible ones.

## 4. Follow-up spec for Phase 17B (not done here)

Proposed unit **P17B.12 (from P0B.6): slim discovery's `scrape_inventory_path`**. Kind: deletion. Size: M.

- **Scheduling.**
  - Run it in its own wave after P17B.5. It edits pixel_motion.py (it moves `_dismiss_cookie_banner`), and the tests touch test_pixel_motion*, both of which P17B.5 owns.
  - Run it before P17B.11, which then lists it.
- **Earlier units move the line numbers.** P16.4 (shopperexpress.py, SSRF guard), P17A.1 (inventory_scrape.py:177 import), P17A.7 (nav.py) and P17A.8 (autowall.py:536) all edit these files first. Re-anchor every line number in this section by symbol.

**Files:**
- backend/scanner/phases/inventory_scrape.py
- backend/scanner/phases/nav.py
- backend/scanner/scrapers/dealer_com_bulk_fetch.py
- backend/scanner/scrapers/shopperexpress.py
- backend/scanner/scrapers/autowall.py
- backend/scanner/scrapers/pixel_motion.py (move `_dismiss_cookie_banner` only)
- backend/scanner/discovery_capture.py (the tuple unpack)
- backend/scripts/discovery_probe.py (:359 wording)
- backend/scanner/inventory_card_location.py (delete)
- backend/tests/test_inventory_card_location.py (delete)
- backend/tests/test_dealer_com_bulk_fetch.py
- backend/tests/test_inventory_scrape_discovery.py (new)
- backend/attribution/__init__.py (docstring)

**Change:**
1. **Card location.** Delete `_capture_card_locations` and its two calls. Delete `inventory_card_location.py` and its test, after a consumer grep (only the test imports it today). The return tuple keeps its 5-slot shape, with an empty dict, until discovery_capture's unpack changes in the same commit.
2. **Location filter.** Delete :175-198 and `nav.try_apply_location_filter` (:490-687; it has no other caller). Keep `sister_store_filter_enabled`.
3. **Bulk fetch.**
   - Add `max_pages: int | None` to `fetch_dealer_com_inventory_bulk`. Discovery passes 1, which keeps the first `getInventory` POST and with it the ledger endpoint and the template.
   - Keep `bulk_complete`, so click pagination is still skipped.
   - Optionally send the page's own `preferences.pageSize` instead of 500, since replay relearns it anyway (recipes.py:518-537). Make that choice explicitly: the synth reference (camelbacktoyota-com) keeps its 500 body either way.
4. **Nudge.** Keep the behaviour. Rename it (for example `nudge_first_page_xhr`) and move it to nav, because it is not dealer.com-specific.
5. **autoWALL.**
   - On detection, skip both fetches. Do not return early: fall through to the generic path, so a profiler false positive (centralhoustonnissan-com) still gets the full capture.
   - Write `"autowall: rows via the synth autowall template; discovery skipped the HTML walk"` to a new `notes` field in the capture report.
   - This is safe because no autoWALL fetch has ever reached the ledger.
   - After a consumer grep, delete `fetch_autowall_inventory_http`, `scrape_autowall_via_playwright` and the helpers that only they use (`_enrich_vehicles_from_vdp`, `_parse_vdp_snapshot`, `_AUTOWALL_DESKTOP_UA`, `_HTTP_HEADERS`). Keep `_is_autowall_html` and `parse_autowall_inventory_html`, which synth/platforms/autowall.py:34 uses.
6. **ShopperExpress.**
   - Drop `fetch_shopperexpress_inventory` from discovery. The function stays, because inventory_recovery.py:696-698 uses it; platform_registry.py:288 only names it in a note.
   - Replace `scrape_shopperexpress_from_page` in discovery with a navigation-only helper. On the observed page it goes to `/listings/` and `/used-listings/` and waits for the listings XHR (a `wait_for_event("response", pred)` with a short timeout, then a drain).
   - Drop the JSON-LD row building. Delete `scrape_shopperexpress_from_page` and `_scrape_shopperexpress_path` once nothing calls them.
   - Next clicks have no observed ledger effect. If they are kept, cap them at a few and stop as soon as a click lands no new response. The current stop rule depends on JSON-LD parsing, which goes.
   - Run the helper after the generic loop, or only when the profile's provider is `shopperexpress`. A bare HTML match must not navigate away from another platform's SRP (finding 5.2).
   - Whatever unit later retires shopperexpress.py's page half must keep this helper: it is the only route that has ever ledgered a ShopperExpress listings endpoint.
7. **PixelMotion.** Keep the detection, the cookie dismissal and the `.vlpm3Pages__next` click loop. Drop the `parse_pixel_motion_inventory_html` accumulation and the synthetic record. Move `_dismiss_cookie_banner` into nav and update P17B.5's KEEP note.
8. **Final `page.content()`.** Stop it (:527-531) and return `None` for html, unless P17B.4 rebuilds an HTML strategy that needs it.
9. **One return point.** Every branch falls out of the `try`, and the tuple, including the endpoint list, is built after the `finally` drain (finding 5.5).
10. **Optional: skip the pre-branch waits.** When the profile already names pixel_motion, autowall or shopperexpress, skip the 10 s scroll pulses and the lazy scroll before the branch. That makes the comment at :81 true. Do it only if the A/B shows the same ledger keys.
11. **Capture report.** `records` now counts only real intercepts. Say so in the discovery.md line (discovery_probe.py:359).

**Tests** (offline, with a fake Playwright page and context that record `goto`/`click`/`evaluate` calls and let a test emit response events):
- autoWALL detected: neither `fetch_autowall_inventory_http` (gone) nor any Playwright navigation to `/gs-vehicle/list` happens; the note is set; and the generic path still runs.
- ShopperExpress detected: no `aiohttp` call, and `goto` is called for `/listings/` and `/used-listings/` on the observed page. A faked `/wp-json/v1/vehicles/listings` response lands in the returned endpoints.
- dealer.com with a template captured: exactly one in-page POST is issued, the ledger holds a `getInventory` endpoint with that body, and no pagination click follows.
- PixelMotion: a faked page-2 fragment response after the `.vlpm3Pages__next` click lands in the ledger as `html_fragment_cards`.
- A new endpoint whose body read completes during the final drain is in the returned endpoint list.
- No location-filter call, and no card-location `evaluate`.

test_dealer_com_bulk_fetch, test_network_observer, test_pixel_motion and test_pixel_motion_stock_leak must still pass, and `import backend.scanner.discovery_capture` must work.

**Accept:**
- Live A/B before merge, run as one process on mains power. The harness must not call `capture_endpoints`: its `load_recipes` calls can rewrite the cache and push up to the DB, even with `promote=False`. Instead:
  - set `SCANNER_ALLOW_BROWSER=1`, `RECIPES_DB_DISABLED=1` and a scratch `RECIPES_CACHE_DIR`;
  - open a Playwright context and call `scrape_inventory_path` directly for each profile path;
  - compare the old code against the new code, in two worktrees;
  - use mcpeeks-com, kiaofcleveland-com, longofathens-com, centralhoustonnissan-com and one more dealer.com dealer.
- A/B results:
  - The set of ledger keys `(method, host, path)` per dealer is identical, or a superset on the ShopperExpress and autoWALL-false-positive side, and the dealer.com `getInventory` body is unchanged apart from the page size choice.
  - Seconds and request counts drop.
  - A sha256 manifest of `workspace/recipes` and `SELECT count(*), max(updated_at) FROM dealer_recipes` are unchanged.
- Each A/B dealer gets a dated "discovery slimming A/B" block in discovery.md.
- `git grep -n "try_apply_location_filter\|inventory_card_location\|scrape_inventory_card_locations\|fetch_autowall_inventory_http\|scrape_autowall_via_playwright"` returns nothing in `backend/`.

**Never-lose-capability check:**
- Every dropped branch produced rows that discovery discarded.
- The scan already has HTTP routes for those platforms: synth `autowall`, synth `wp_vehicles_index`, recovery `shopperexpress_api` and synth `html_cards`.
- Card location lost its last consumer on 2026-10-01. If per-car lot location is wanted again, it belongs in the HTTP `html_cards` parser. That is an owner question, outside this unit.

## 5. Incidental findings (outside the branch list; flagged for owners)

1. **discovery_capture drops dealer.com sections.**
   - The cross-path dedupe at discovery_capture.py:147-161 (HEAD :151-165) keys on `(method, url without query)`.
   - In the ledgers, all 96 multi-path dealer.com runs (93 dealers) whose `getInventory` body parses on at least two paths carry a different `pageAlias`/`listing.config.id` per path (new, used, certified) at the same URL. Discovery therefore promotes only the first path's section.
   - The browser-era `dealer_run` passed every path's endpoints to `promote_from_ledger`, whose recipe key separates dealer.com sections (recipes.py:109-117, `_recipe_section_discriminator` :54-86).
   - CarsCommerce is not affected. The recipe key collapses its bodies too, by design: a whole-lot replay strips section facets (`_CC_SECTION_FACETS`, :513).
   - It has not yet been seen in a real discovery capture: no dealer.com capture has succeeded since 2026-09-26.
   - Fix: dedupe on `EndpointRecipe.key()` semantics, or on `(method, url, post body)`. This belongs with P17B.11 or a recipe-store phase, not a deletion unit.
2. **Platform detectors fire on other platforms and then return early.**
   - `_is_shopperexpress_html` matched holmanhondacentennial-com (a Roadster/CarsCommerce site).
   - The site profiler labelled centralhoustonnissan-com (dealer.com) `autowall` at confidence 0.57.
   - The early returns skip the generic pagination. After slimming, the autoWALL detection falls through instead (spec step 5), and the ShopperExpress navigation runs only on profile agreement, or after the generic loop (spec step 6).
3. **The facet-scoped `getInventoryAndFacets` recipes** (pageSize 8-28; P0B.1's "facet-filtered fragment" shape) are the page's own request. Without the bulk POST they would be the only dealer.com capture, which is why 1b's first POST is kept.
4. **kiaofcleveland-com's two recipes replay the whole lot** (answering the 10-07 question): 220 and 211 rows against API counts of 218 and 211. They are labelled `provider_hint: dealer_dot_com` because the browser-era `dealer_run` promoted them with the manifest provider (default `dealer_dot_com`, dealer_run.py:214 at `ff76d4e58~1`), not with the profiler's `shopperexpress`. The label is cosmetic for replay (`pagination: none`).
5. **The platform branches return a snapshot of the endpoint list taken before the final drain** (section 1). On mcpeeks-com's used path (2026-09-26, probe4.out), the only page-2 fragment arrived 12 ms after the click loop ended. It made it into discovery's list only because `page.content()` yielded first. A slower response would have reached the ledger file and been missed by `promote_from_ledger`. Spec step 9 fixes this.
6. **Profile-known platforms still pay the JSON waits.**
   - Despite the comment at :81, each path spends about 55 s on hydration, the scroll pulses and the lazy scroll before the PixelMotion, autoWALL or ShopperExpress branch: mcpeeks-com 2026-09-26, 10:34:20 → 10:35:14 local; kiaofcleveland-com 2026-08-04, 20:02:45 → 20:03:40 local.
   - That is about 4 minutes of a 4-path capture.
   - Spec step 10 is the optional fix.
7. **The synth dealer.com reference body is a certified-used section.**
   - camelbacktoyota-com's `getInventory` recipe carries `pageAlias INVENTORY_LISTING_DEFAULT_AUTO_CERTIFIED_USED` and `listing.config.id "auto-certified-used,auto-used-mpp"`.
   - Synth swaps only the siteId (synth/platforms/dealer_com.py:54).
   - recipes.py (HEAD :1142-1146) already notes Camelback's "certified-only config stays open".
   - Not measured here. P0B.1 and P0B.2 replay dealer.com recipes against the site totals, and that is the check: a synthesized dealer.com recipe that returns only certified cars would show up as a large gap.

## 6. What this means for D-DC5

**None of these branches runs in a scan.** `scrape_inventory_path`'s only caller is discovery_capture. D-DC5 decides the scan-time browser paths (`--allow-browser`, the recovery page strategies, the chain/gap_fill Playwright fallbacks). It does not decide these branches, but this study informs it in three ways.

- **Every platform whose discovery branch is slimmed already scans over HTTP:** synth `autowall`, synth `wp_vehicles_index` / recovery `shopperexpress_api`, synth `html_cards`, synth dealer.com.
- **What slimming saves per capture:**
  - the ~200-line location filter in nav;
  - the 150-line card-location module;
  - a full-lot VDP crawl per path on autoWALL and ShopperExpress sites (112-145 s per path in the 2026-08-04 logs);
  - up to 50 extra in-page POSTs per dealer.com path.

  It changes no ledger key that has ever produced a recipe.
- **Constraints for Phase 17B:**
  - P17B.5 already keeps `_dismiss_cookie_banner`, `_is_pixel_motion_html`, `parse_pixel_motion_inventory_html` and `_PIXEL_PATHS`. It must not treat inventory_scrape's own `.vlpm3Pages__next` loop as part of `scrapers/pixel_motion.py`'s page scraper.
  - shopperexpress.py is not in P17B.5's file list. Whichever unit retires its page half must keep the navigation-only helper (spec step 6).

## 7. Review log (2026-10-08)

Every file:line in this document was re-read at `dbdbf5cba`, and every artifact number was recomputed from the files named in section 3. These are the changes from the 10-07 draft:
- **Line ranges.** nav.py location filter :490-688 → :490-687. recipes.py page-size learning :518-538 → :518-537. synth autowall :16-40 → :16-45. Added the HEAD line map for the two files changed since `dbdbf5cba`.
- **Ledger rules.** Added the status check and the capture-tier classification. Later responses for a key also fill a missing POST body and missing auth headers. Added the early-return snapshot (finding 5.5).
- **1a.** Nudges 604 → 610; bulk completions 364 → 366; unaccompanied bulk 2 → 4. "242 nudges on non-dealer.com paths" was not reproducible; it is replaced by 248 nudges with no bulk fetch, 122 of them on non-`index.htm` paths.
- **1b.** Multi-page bulk 255/364 → 256/366. "Every one of the 292" is now backed for the 39 truncated bodies too. POSTs 2..N can also raise the max counters.
- **2a.** "8 dealers produced rows" → 4 stores (8 name spellings), all in browser-era scans.
- **2b.** Central Houston Nissan was a site-profiler false positive (`provider=autowall`, 0.57), not an `_is_autowall_html` match. Berge Toyota's fallback found 0 cards, so it got no rows, not "rows it throws away".
- **3a.** Added the 214 s per-path timeline against the 240 s timeout.
- **3b.** The Next clicks have no observed ledger effect (occurrences 1), so the verdict now keeps only the navigations. kiaofcleveland's recipes are shown to replay the whole lot.
- **4c.** "Every path's ledger" → new, used and cpo, not certified. Occurrences are 10, 12 and 1, not "10-12". The nudge is ruled out as the source.
- **5 (card location).** The consumer's input died in `ff76d4e58` (2026-09-26), and the call was deleted in `14dc85936` (2026-10-01). The draft said the call was deleted in `ff76d4e58`.
- **6 (location filter).** Added the signature-token rule and the `api_post_url` reset. "10 dealers" → 7 stores (10 name spellings). Added the dealer/location.py:360 reader.
- **Section 4.**
  - "Shares scrapers/{autowall,shopperexpress,pixel_motion}.py with P17B.5" was wrong: P17B.5's files include only pixel_motion.py.
  - Added the earlier units that move these files.
  - Added autowall.py, discovery_capture.py and discovery_probe.py to the file list.
  - autoWALL now falls through instead of returning.
  - Added one return point (step 9) and the optional wait skip (step 10).
  - The A/B no longer calls `capture_endpoints`, whose `load_recipes` can write.
- **Section 5.**
  - Finding 1: CarsCommerce removed, and the 97/102 ratio replaced by 96/96 parseable runs.
  - Finding 2: corrected the detector involved.
  - Finding 4: answered.
  - Findings 5-7: added.
- **Section 6.** "Two full-lot VDP crawls per path" → one per path on those platforms. D-DC5's scope stated precisely.
