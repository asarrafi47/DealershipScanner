# Independent dealers: why so few are in our inventory

Owner question, 2026-11-14: "I noticed not many 3rd party dealerships are in our inventory, why
is that? We should include them as well." This file answers WHY from the code, the docs and the
committed fixtures. `PLAN.md` beside it says HOW to include them, and cards L26 to L34 of
`docs/scan_loop/TASKS.md` are the work. Written in a cloud session with no database, no Railway and
no dealer-site access: every number below is from code or from the committed fixtures; anything only
the production database can answer is marked **[DB]** and is measured by the report script of
section 4.

## 0. The short answer

1. **The scan list is closed.** The nightly fleet scans only dealers that already have active cars
   (plus a hand-kept revived list). A dealer the scanner has never scanned can never join by itself.
   The dealer registry that discovery fills is not read by the fleet at all.
2. **The dealers that were let in long ago were likely picked as brand stores.** The nationwide
   discovery that built the old 22k-row manifest dropped every dealer whose name had no car brand;
   how much of today's roster came from it is **[DB]**. The
   newer Google Places discovery returns at most 20 places per circle, about one circle per metro.
3. **Small and used-only lots would be scored as failures.** Under 20 cars on a first scan is
   `no_rows`; used cars only is `inaccurate` (and fails gate GF5) unless a person adds the dealer to
   an allow list. In the fixtures this hits 1 of the 5 failing independents; the others fail on data
   quality, a broken recipe or a group feed.
4. **The live site hides cars with any missing spec field** (19 fields), which hits thin independent
   feeds hardest. How many is **[DB]**.
5. **In the committed 2026-11-03 fixtures, 6 of 172 dealers (3.5%) are independents**, and only 1
   of those 6 scanned `ok` (17%), against 100 of 166 franchise stores (60%).

"3rd party dealers" read three ways (section 6): independent lots (in scope; this plan),
national used-car chains such as CarMax and Carvana (kept out on purpose by two rules; allowed only
from their own site and only if their terms allow, default no), and marketplaces such as Cars.com,
Autotrader and CarGurus (never scanned; we only ever read a dealer's own site).

## 1. How a dealer becomes scannable (the roster), and whom each path lets in

| # | Path | Where | Who gets in | Who stays out |
|---|---|---|---|---|
| 1 | Fleet roster: `SCAN_FLEET=1` takes `_load_scannable_dealers()` (active cars UNION stored recipes), drops CarMax, then `apply_scannable_roster_rule` | `backend/scripts/fleet_scan.py:219-252` (CarMax at `:237`), `backend/scanner/manifest.py:223-305` (the UNION SQL at `:251-277`) | dealers with active rows in `cars` | every dealer with no cars yet, whatever kind |
| 2 | Roster rule: keep dealers with active inventory, minus do-not-scan; a recipe-only dealer only if listed in `revived_dealers.txt` | `backend/scanner/manifest.py:361-394` (recipe-only exclusion `:383-388`); lists `deploy/railway/do_not_scan.txt` (4 ids), `deploy/railway/revived_dealers.txt` | dealers already holding cars | a dealer with a fresh recipe but no cars (the 2026-09-29 group-feed incident, `manifest.py:307-316`) |
| 3 | `DEALERS_FROM_ACTIVE_INVENTORY=1`: cars with `listing_active` only | `backend/scanner/manifest.py:178-221`, `:437-444` | dealers with live rows | anyone whose rows all went inactive, and every new dealer |
| 4 | `DEALERS_FROM_DB=1`: the `dealerships` registry with a website | `backend/scanner/manifest.py:74-136`, `:446-450` | every registry row with a site, any kind | nothing by kind; but no Railway job sets this flag (fleet_scan only reads `SCAN_FLEET`/`SCAN_DEALERS`, `fleet_scan.py:5-11`, `:231-252`) |
| 5 | Explicit list `SCAN_DEALERS` / `--dealers`: never filtered | `fleet_scan.py:231-233`, `manifest.py:418-423` | anyone a person names | nobody, but the loop's request path does not use it (row 6) |
| 6 | Loop requests (probe, validate, rescan, apply): ids go through the roster rule; "every id must be in the manifest", else `unknown_dealer` | `docs/scan_loop/BUNDLE.md:226` (section 6 rule 2), `docs/scan_loop/TASKS.md:180-182` (L11) | roster dealers | **every dealer not yet on the roster**: the loop cannot onboard a new dealer today |
| 7 | Fleet shards: the roster split by active row count | `fleet_scan.py:271-281` | no kind effect | none |
| 8 | Built-in skip list: CarMax by substring | `manifest.py:475-500` (`carmax.com`, `carmax-com`), `fleet_scan.py:237` | all others | CarMax (national retailer) |
| 9 | Scan hints `skip_reason`, `requires_browser`, `needs_http_proxy` | `backend/scanner/recipe_store.py:107-122`, `backend/scanner/delta_scan.py:154-166` | | dealers marked out of scope (OEM sites, RV, brokers, aggregators per the July audit **[DB]**) and dealers whose site needs a browser; a browser is never used (`docs/NETWORK_SCAN_PROCESS.md`, Goal) |
| 10 | Discovery tiers DMV, Google Places, OpenStreetMap, DuckDuckGo URL fill -> `dealerships` registry | `backend/discovery/pipeline.py:96-295` (Places `:142`, OSM `:163-172`, non-dealer filter `:192`, upsert `:293`) | the registry only | nothing reaches the scan roster from here (rows 1 to 6) |
| 11 | DMV tier: only North Carolina has a loader | `backend/discovery/dmv/registry.py:13-15` | NC licensees | every other state's licensed-dealer lists |
| 12 | Google Places tier: `searchNearby`, `includedTypes=["car_dealer"]`, `maxResultCount` 20, one 50 km circle or a centre plus 8 circles | `backend/discovery/google_places.py:62-63`, `:257-266`, `:143-161` | the 20 places Google ranks first per circle | the rest of a metro's dealers (often hundreds) |
| 13 | National sweep: top 20 US cities, radius 15 miles, 200-call budget | `backend/scripts/national_scan.py:34`, `:44-52`, `:53` | about 20 x 20 places at most | everything outside those circles |
| 14 | Old nationwide Overture discovery (removed June 2026): "filtered down to franchised OEM dealers" | `docs/discovery/NATIONWIDE_OVERTURE_DISCOVERY.md:3-10`, `:36` ("filters out independent lots"); `backend/discovery/overture_discovery.py:92-104` | franchise names only | every independent lot |
| 15 | `is_probable_non_dealer`: salvage, rental, RV, restaurant, auctions, Carvana, CarMax, OEM corporate sites | `backend/discovery/non_dealer_filter.py:23-62`, `:84-94`, `:166-206` | independents are kept (it is biased to keep, `:10-13`) | CarMax and Carvana by name and host (`:59-60`, `:89-90`); the July audit found a restaurant and "Carvana Headquarters" slipped through earlier versions (now `:47-49`, `:59`) |
| 16 | Website clean-up: marketplace and directory hosts are dropped as a dealer's site | `backend/discovery/normalize.py:87-135`, `:189` | dealers with their own domain | a lot whose only web page is a Cars.com, CarGurus, Autotrader or directory listing: it gets no URL, so it can never be scanned |
| 17 | Group-feed attribution: rooftop gate and VIN owner guard | `backend/scanner/pipeline/assess.py:139-147` | single-store sites | a store whose feed is a group's (it keeps only its own rows; an independent rarely shares a group feed) |
| 18 | Admin "onboard dealer" button: enqueues an `onboard` job in `dealer_jobs` | `backend/dealer/admin/onboard_api.py:23-40`; paused, answers 409, since the worker was deleted on 2026-10-08 (`backend/tests/test_admin_onboard_paused.py:1-7`) | nobody today | everyone: the one manual way to add a single new dealer from the site is off |

**Verdict on the roster.** The scan list grows only by a person naming dealers (the admin button of row 18 is paused). Kind plays no role
in rows 1 to 7 and 18; the bias came in through what was named in the past (rows 12 to 14) and through the
gates below.

## 2. Hypotheses

| # | Hypothesis | Verdict | Evidence |
|---|---|---|---|
| H1 | Discovery sources or Places queries favour franchise brands | **Supported (mechanism); size [DB]** | Places returns 20 per circle (`google_places.py:62-63`, `:265`), so a metro gets its 20 most prominent places; the national sweep spent about one call per metro (`national_scan.py:30-34`, `:53`). `car_dealer` itself includes used lots, so the query is not franchise-only. DMV, the source that lists every licensed lot, covers NC only (`dmv/registry.py:13-15`). |
| H2 | The manifest came from brand dealer locators | **Supported in effect** (franchise-name filter, not locators) | The old 22k manifest was built from Overture places "filtered down to franchised OEM dealers" (`NATIONWIDE_OVERTURE_DISCOVERY.md:9`, `:36`). No OEM dealer-locator scraper exists in the code (search for `locator` finds only brochure and page-navigation code). |
| H3 | The recipe-only rule keeps out dealers without a recipe; recipes exist mostly for franchise platforms | **Supported, partly** | The rule keeps out every dealer without active cars, recipe or not (`manifest.py:383-388`). Platform templates: 18 in `backend/scanner/synth/platforms/`; most are franchise website vendors (Dealer.com, DealerOn, Dealer eProcess, Team Velocity, Jazel, oneAudi). Two serve independents with evidence: `html_cards.py:17-24` (Quantum Auto Sales) and `overfuel.py` (2 independents in the fixtures). `autowall.py` is a generic HTML list template whose only known site is a franchise store (`autowall.py:18`); whether it serves independents is to verify. How many recipes per kind: **[DB]**. |
| H4 | Independents' sites use platforms with no template, or load stock through widgets, iframes or JavaScript only | **Plausible; only probes and the database can tell** | `requires_browser` is a recorded hint (`recipe_store.py:117`) that ends in `no_recipe` (`delta_scan.py:160-166`). Platforms common among used lots (DealerCenter, Frazer, Carsforsale.com-hosted sites, Dealer Car Search, AutoManager, V12) have **no** fingerprint or template in `backend/scanner` (a search for each name finds none; "carsforsale" appears once, as a VDP text marker in `backend/scanner/vdp/extract.py:228`). Their actual share: **to verify** by probe. |
| H5 | Quality gates, VIN-less pages or rooftop rules drop or hold their rows | **Mechanism present; not shown as the cause** | First scan with fewer than 20 rows is `no_rows` (`pipeline/constants.py:24`, `assess.py:138`, `:148-150`); gate GF1 fails under 20 (`loop/gates.py:18`, `:96`). Used-only is `inaccurate` unless listed (`assess.py:170-177`, `deploy/railway/one_condition_ok.txt`: 2 dealers) and fails GF5 at 20 cars or more unless in `deploy/railway/accepted_gaps.json` (2 dealers, `gates.py:26`). A lot without VINs on its pages cannot be stored: rows are keyed by VIN ("cars are one row per VIN", `manifest.py:314-315`). In the fixtures, 5 of 6 independents fail, but only one (`lbmboaz-com`, one condition) hits these rules, and it also fails the incomplete-rows check; the rest are the key-field floor (2), a broken recipe (1) and a group-feed rooftop cut (1) (section 3). For a never-scanned small or used-only lot these rules would fail it on its first scan; how many such lots exist is **[DB]**. |
| H6 | Reconcile floor or retirement removes small inventories | **Refuted for now** | Production scans start with reconcile off (`docs/scan_loop/TASKS.md` rule 8; `BUNDLE.md:227`). When on, a run under 60% of baseline never retires (`pipeline/constants.py:22`), which protects small lots too. |
| H7 | Production data is old because the 2026-11-03 merge is parked | **Supported for freshness, neutral for kind** | The 11-03 run was a Mac run on a scratch database (`backend/tests/fixtures/ops_publish/README.md`); its 172 fixture dealers include the same 6 independents, so merging it would not add independents. Freshness per kind: **[DB]** (report section "fresh"). |
| H8 | The live site's read path hides them | **Supported for incomplete cars; refuted for brand, registry or type** | Search filters on `listing_active` and the review flag only (`backend/db/repositories/search/where.py:139-145`); the registry clause applies only when a user filters by dealership (`where.py:361-376`). Nothing requires a brand, a registry match or a dealer type. But production by default hides any car with a missing spec-sheet field (`backend/utils/listing_completeness.py:49-70`, `:176-294`; mileage counts for used and certified cars only, `:134-135`; default off in production unless `LISTINGS_INCLUDE_INCOMPLETE_CARS` is set, which is **[DB/Railway]**, `backend/db/repositories/data_quality_repo.py:113-128`), and the grid cards follow the same flag (`backend/db/repositories/grid_cards_repo.py:213-225`). Thin feeds (trim 0% and colour 0% at `premiumautosinc-com`, fixture) are hit hardest. Count per kind: **[DB]**. |
| H9 | A dealer-type field exists | **Partly** | `dealerships.oem_brand` (migration `V020`, filled from the name by `google_places.py:54-60`), `google_primary_type` and `platform` exist. No `dealer_type` or franchise flag anywhere in `cars`, `dealer_recipes` or the bundle. `backend/discovery/dealer_kind.py` (this PR) is the first franchise/independent heuristic, report-only. |

## 3. Composition of the committed fixtures

Source: `backend/tests/fixtures/ops_publish/scan_2026_11_03_s0`, `_s3`, `_s6` (`triage.json.gz`), 172
dealers, the only committed dealer list with verdicts (`backend/dealers.json` holds 26 old franchise
entries and no verdicts). Every one of the 172 had a recipe; there is no `no_recipe` and no `error`
verdict, because the fixtures hold only dealers already on the roster.

### 3.1 The heuristic

`backend/discovery/dealer_kind.py` (`classify_dealer`), first match wins: marketplace host;
national-retailer name or host; a car brand as a word in the name or inside the host (with a list of
place names that only look like brands, such as Oxford or Stanford); sells new cars (10 or more new
rows and 20% or more of rows); a used-lot word ("auto sales", "autos", "motorcars", "pre-owned",
...); used only with no brand (and 5 or more makes when makes are known); else unknown. The fixtures
carry the host, the new and used counts and the platform, but no makes and no names.

### 3.2 Precision, checked by hand on 33 dealers

Checked from the dealer id, the platform and the triage text only (no site visits).

| Dealers | Heuristic | Hand verdict |
|---|---|---|
| 20 with a brand in the host, picked across platforms: `5starford-com`, `albanysubaru-com`, `audihuntsville-com`, `bmwofbloomfield-com`, `capomazda-com`, `cdjrofalbertville-com`, `cleveland-nissan-com`, `coastlinecdjr-com`, `cumberlandkia-com`, `dthondachicago-com`, `fjmercedes-com`, `hileyvwhuntsville-com`, `lambonb-com`, `mbzla-com`, `miniofcharleston-com`, `northcountrycjdr-cars`, `tuttleclickstustinjeep-com`, `villagevw-com`, `welbornchevroletofrome-com`, `lexusofbellevue-com` | franchise | 20 right |
| `billluke-com`, `paradiseautos-com`, `wisimonson-net`, `jasonlewisautomotive-com`, `integrityofchattanooga-com`, `ocauto-com`, `davekirk-com` (no brand in host, sell new cars) | franchise | 4 right (Bill Luke, Paradise, W.I. Simonson, Jason Lewis are known franchise groups); 3 plausible but not confirmable offline (they list 207, 262 and 59 new cars, which only a franchise can sell) |
| `quantumautosales-com`, `autoboutiqueohio-com`, `premiumautosinc-com`, `autosavvy-com` | independent | 4 right (used-only lots; Quantum is on the used-only allow list) |
| `overdrive-usa-com` | independent (used only) | right by lot type, wrong by owner: its feed is shared with `cumberlandchryslercenter-com` and `cumberlandkia-com` (triage `vin_owner_conflict_owners`), so it is a franchise group's used-car store |
| `lbmboaz-com` | independent (used only) | not confirmable offline; its verdict says "only one condition captured", which may be a franchise feed that lost its new cars |

Result: 31 of 33 right, 1 debatable, 1 unknown; no false positive found among the franchise calls.
Weak spots are written in the module docstring.

### 3.3 Counts, verdicts, reasons and platforms per kind

Pinned by `backend/tests/test_dealer_mix_report.py::test_fixture_composition_counts`.

| Kind | Dealers | ok | thin | inaccurate | no_rows | error | no_recipe | ok share |
|---|---|---|---|---|---|---|---|---|
| franchise | 166 | 100 | 11 | 54 | 1 | 0 | 0 | 60% |
| independent | 6 | 1 | 2 | 1 | 2 | 0 | 0 | 17% |
| national retailer | 0 | | | | | | | |
| marketplace | 0 | | | | | | | |
| unknown | 0 | | | | | | | |
| all | 172 | 101 | 13 | 55 | 3 | 0 | 0 | 59% |

Rules that fired: brand in host 159, sells new 7, used-lot word 4, used only 2.

Top failure reasons:
- **Independent (5 of 6 fail):** key fields under 90% (`premiumautosinc-com`: price 81%, trim 0%,
  colour 0%; `autoboutiqueohio-com`: price 80%), both on Overfuel; recipe replay gave 0 rows
  (`autosavvy-com`, Dealer.com recipe); 14 rows under the 20-row floor after the rooftop gate
  (`overdrive-usa-com`); one condition only (`lbmboaz-com`, used 239, new 0). The one `ok`
  (`quantumautosales-com`) passes only because a person listed it in `one_condition_ok.txt`.
- **Franchise (66 of 166 fail):** of the 54 `inaccurate` (a dealer can have several reasons), 29 have
  NHTSA discrepancies (mostly `vpic_fuel`, `vpic_model`), 25 incomplete fields (price, colour), 10
  one condition only and 6 VIN owner conflicts; all 11 `thin` have a key field under 90% (trim and
  price lead); the 1 `no_rows` (`mckennasubaru-com`) is a recipe that replayed 0 rows.

Platform mix: independents: Overfuel 2, Dealer.com 2, html cards 1, Dealer eProcess 1. Franchise:
Dealer.com 116, DealerOn Cosmos 16, Dealer eProcess 10, Team Velocity 6, Typesense 5, Jazel 3,
Motive 3, Overfuel 1, unknown 2, oneAudi 1, Chapman 1, WP vehicles 1, DealerOn 1.

**What the fixtures say.** Independents are rare (3.5%) and, once in, fail about twice as often by
share (5 of 6, 83%, against 66 of 166, 40%; only 6 dealers). The five failures have different causes:
two are data quality under the 90% key-field floor, which `PLAN.md` keeps for independents
(`premiumautosinc-com`, `autoboutiqueohio-com`); one is a broken Dealer.com recipe (`autosavvy-com`);
one is a group feed cut by the rooftop gate to 14 of 156 rows (`overdrive-usa-com`, a group's used
store rather than a small lot); one is incomplete rows plus a single condition (`lbmboaz-com`). Only
`lbmboaz-com` hits a rule built for franchise lots (both conditions), and it also fails the
incomplete-rows check. The six are too few to estimate the fleet share or to show that the gates are
the cause; the report script measures the first, and the onboarding batches (L33) the second.

## 4. The report script: what it tells us and how to run it

`backend/scripts/dealer_mix_report.py` (with `backend/discovery/dealer_kind.py`): read-only, standard
library plus psycopg, reads `INVENTORY_DATABASE_URL`, connects with
`default_transaction_read_only=on` and a 30 s statement timeout, sets the session READ ONLY, runs
six SELECTs, prints counts and dealer ids only (no VIN, name, address, phone or URL). A missing table
or column prints one `skip ...` line and the rest goes on.

It answers:
- registry rows against the scan roster: registry ids with active cars, scanned, never seen; active
  dealers not in the registry; recipe-only dealers (H3);
- per kind: dealers, cars (new and used), lots under 20 cars (H5), recipe present or absent, scan
  hints (`requires_browser`, `skip_reason`, `needs_http_proxy`, H4), last scan result from
  `scan_runs` (error, no rows, under 20 rows, rows, never scanned), platform, a freshness histogram
  of active cars by `scraped_at` age (H7), and active cars hidden as incomplete in production (H8);
- registry rows never seen, per kind and state, and how many have a marketplace page or no site as
  their website (H1, rows 10 to 16);
- the dealer ids of every non-franchise kind (first 80 each), to check the heuristic by hand.

Run it on the owner's Mac, in the repository folder that is linked to Railway (the same folder the
deploy scripts run from), as one line. Before this PR is merged use the branch; after the merge use
`origin/dev` in both places:

```
git fetch -q origin claude/independent-dealers-1 && P=$( { git show FETCH_HEAD:backend/discovery/dealer_kind.py; git show FETCH_HEAD:backend/scripts/dealer_mix_report.py; } | base64 | tr -d '\n') && timeout -s KILL 300 railway ssh -s web -- "echo $P | base64 -d | python3 -" | tee dealer_mix.txt
```

It sends the two files to `python3` inside the web container (the base64 pattern of
`docs/SCANNING_OPS_LOG.md:427`), changes nothing on Railway, and takes well under a minute. **Paste
back** everything from `DEALER MIX REPORT` to `END DEALER MIX REPORT`, plus any `skip` lines above
it. It holds dealer ids and counts only.

## 5. What only the database can tell

Each item is a line of the report: the registry size and how much of it was ever scanned (a July 2026
audit, not committed to this repository, reported 418 registry rows and about 198 roster dealers,
disjoint; **[DB]**, not re-checked here); the real
share of independents among active dealers and cars; how many independents carry `requires_browser`
or `skip_reason`; how many independents sit in the registry unscanned and in which states; how many
of their cars production hides as incomplete; and how fresh their cars are.

## 6. The three readings of "3rd party dealers"

| Bucket | Examples | Kept out by | Allowed to include? | Sensible? |
|---|---|---|---|---|
| Independent lots | "ABC Auto Sales", Quantum Auto Sales | the closed roster (section 1 rows 1 to 6), past franchise-only discovery (rows 12 to 14), small-lot and used-only gates (H5) | Yes, from the dealer's own site, when its robots rules and terms allow automated reading | Yes: the plan in `PLAN.md` |
| National used-car chains | CarMax, Carvana, Vroom, EchoPark, DriveTime | CarMax: the built-in skip (`manifest.py:475-476`, `fleet_scan.py:237`); CarMax and Carvana: the non-dealer filter (`non_dealer_filter.py:59-60`, `:89-90`); `normalize.py:102-103` keeps their sites as valid targets | Only from their own site and only if their terms and robots rules allow it; their terms commonly forbid automated access (**to verify** per company before any probe) | Not now: one company, national stock, delivery-based prices; one row per VIN would mix far-away cars into local search. Default: keep them out |
| Marketplaces | Cars.com, Autotrader, CarGurus, TrueCar, Facebook Marketplace | `normalize.py:94-101` drops them as dealer sites; never a scan target | **No.** We only ever read a dealer's own site; we never scrape a marketplace | No |
