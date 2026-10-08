# Validation paths, VIN floors and fetch policy on a live sample (P0B.1)

- Unit: P0B.1 of `docs/REMEDIATION_PLAN_2026_10.md` (owner-decisions-1). Inputs for D-OD1, D-OD2, D-OD3, D-OD7, D-OD8.
- Run: 2026-10-08, 12:21-13:42 UTC, from the MacBook Pro on the home IP (owner approval of 2026-10-07: 0B network measurements from the MBP; battery allowed for this run). Code at `dbdbf5cba` in a detached worktree; recipes and dealer logs from the main checkout's `workspace/`.
- **Home-IP caveat.** Every status below was seen from a residential IP. Railway egress was refused on 2026-09-29 by edges that answer a home IP (the first Railway runs staled 25+ dealers' recipes on 403s; `recipes.egress_tag` docstring). The block and challenge numbers can be worse from Railway; the parse, walk and stop-condition numbers do not depend on the IP.
- Harness, sample list and a redacted per-recipe CSV: `validation_paths_2026_10/` next to this file (`harness/`, `recipes_measured.csv`). The harness paths point at the run's scratch directory; edit `RUN` / `WT` to re-run.

## 1. Answers in one place

| Decision | What the sample says |
|---|---|
| D-OD1 (min-VIN rule) | Stored `vehicle_rows` overstates the junk: of the 15 sampled "sub-10" recipes, 8 replay 10-567 live VINs (dealer.com `getInventoryAndFacets` captures stored at their page size, a Team Velocity pair stored as 0, a cosmos model section, a CPO feed). The 7 that stay under 10 are widgets, VIN lists, a `?vin=` probe, a Gatsby index, a lease special and a location facet; together they add **2 VINs** to the scan union on 1 of 42 dealers under "admit > 0" and **0** under the recommended rule. Offline (stored counts, all 594 files): the recommended rule changes the union of **1 dealer** (mclarennb-com, the McLaren shape, +8 new); "admit > 0" changes 32 dealers / 36 recipes. **Risk found:** the first-hit threshold `min(10, max(3, ceil(0.5 x vehicle_rows)))` newly accepts 2 junk recipes in the sample (crownlexus ws-rec widget, 6 VINs; covertbuickgmc location facet, 4 VINs, all duplicates). Apply it only to recipes the union rule admits. |
| D-OD2 (which count decides) | Verdicts flip between pages=2 and pages=40 on **2 of 42 dealers**: hanselbmwofsantarosa-com ok -> `rejected:coverage` (a false reject: 163 kept VINs measured against a group-wide site total of 407, 244 sibling rows refused) and tustinhyundai-com `rejected:short_page` -> `uncertain:short_page`. The p2 and p40 VIN counts are equal for only 34 of 90 recipes. A full walk does not arm the coverage reject when it hits the 40-page cap: downeyhyundai-com (480 of 1,357 new) and hyundaiofcookeville-com (457 of 1,080 used) stay `ok` while truncated. |
| D-OD3 (one fetcher) | `validate_recipe` and the scan-path set check agree on 66 of 90 recipes, differ on 24, and never disagree about the 5-VIN floor. Every difference except 2 (the lot moved between walks) has a code cause: dealer.com page size never learned in `validate_recipe` (15 recipes on 7 dealers, 10 of them stop at exactly 48), stale stored `total_count` stops DEP/HTML walks early (6), and `_validate_cosmos` drops the recipe's query (`cpo=1`) and counts 334 instead of 124 (1). On the fetch side, replay pays **2.66 wire requests per page** (800 for 301 pages) on 11 of 42 dealers; synth's navigation headers plus a same-site Referer clear the same Cloudflare HTML edges with plain `requests` on **8 of 8 hosts**, where replay's headers get a 403 challenge on 8 of 8. Porting synth's HTML-edge handling into replay (option A) is supported. |
| D-OD7 (fetch policy) | The only block status seen was **403** (628 of 2,195 requests). Zero 405, 429, 503 or other 5xx. Challenges arrived as 403 bodies; no 200 challenge page was seen on any feed, VDP or probe in this sample. At 7 Cloudflare HTML hosts the `chrome` profile fails and `chrome124` clears (194 wasted `chrome` attempts in the full walks): starting rotation from the per-host winner saves one request per page. VDPs: chrome-only cleared **30 of 49**, rotation **44 of 49**, rotation with the profile's own UA **49 of 49**. The explicit Chrome UA lost 5 of 49 VDPs under `safari17_0` (all audisouthaustin-com, which only Safari with its own UA clears) and 1 of 22 non-`chrome` feed probes (a `chrome124` page on hyundaiofcookeville-com); it gained nothing anywhere. "The profile sets its own UA" is not worse; it is better. |
| D-OD8 (replay retry) | **No transient 429/5xx on any replay page** (798 non-cached replay page fetches, 1,856 wire requests). The only non-200, non-403 answers were a deterministic 204 (toyotaofhb-com `?vin=` recipe, 3 of 3) and 2 curl_cffi transport errors that the rotation absorbed. The D-OD8 condition ("only if P0B.1 sees transient 429/5xx") is not met on the home IP. |

## 2. Method

- **Writes made impossible, not just avoided.**
  - Recipes read with `json.load` from `workspace/recipes/*.json`; `load_recipes` (which writes both ways) was never called.
  - After import and before any measurement ran, these were replaced by functions that raise `WriteAttempt`, and each replacement was called once to prove it raises: `recipes.save_recipes`, `mark_stale`, `record_stale_status`, `clear_stale_status`, `load_recipes`, `promote_from_ledger`, `try_fetch_via_recipes`; `recipe_store.db_save_recipes`, `set_scan_hints`; `recipe_validation.gate_recipes`, `write_discovery_log`, `record_recipe_status`; `attribution.disown.disown_foreign_rooftop_vins` (and its `rooftop_disown` re-export); `dealer_place.learn_place`. No `WriteAttempt` fired during the run.
  - The DB session was read-only (`PGOPTIONS=-c default_transaction_read_only=on`, asserted with `SHOW` at start). The parse path only reads `dealerships` and `scan_hints` (registry place).
  - The process ran from a scratch directory, so the relative `RECIPES_DIR` and `DEALER_LOGS_ROOT` pointed at nothing real.
- **Proof.**

  | Check | Before (2026-10-08 08:31 UTC) | After (13:42 UTC) |
  |---|---|---|
  | sha256 manifest of `workspace/recipes` | 594 files, aggregate `bdb1709a2560c70bba60deab82d9341b69f100f56333f053598c8624161d7969` | identical, 0 files changed |
  | `SELECT max(updated_at), count(*) FROM dealer_recipes` (local) | `2026-09-29T21:24:36.072506+00:00`, 687 | identical |
  | md5 over every row's `dealer_id, updated_at, md5(recipes_json), md5(scan_hints)` | `4b54a62ac42443f17a587f4c870816c5` | identical |

- **Pacing.** Every wire request (plain `requests`, curl_cffi, urllib) went through one wrapper that holds at least **1.5 s between requests to the same host** and logs status, profile, size and challenge markers. That includes the rotation attempts inside `_replay_request`, which has no pacing of its own. `SCANNER_SYNTH_FETCH_DELAY` was 0, so synth did not pace twice. One dealer at a time.
- **Order.** Per dealer, the three measurements (`validate_recipe` per recipe, `validate_recipe_set` pages=2, pages=40) ran in a seeded random order, and so did the recipes inside `validate_recipe`.
- **Response cache.** The set checks went through `recipes._replay_request` (the scan's own function) with a per-dealer cache of 200 answers, so pages 1-2 of the p40 walk and the p2 walk share one fetch. Non-200 answers were never cached; they were fetched again by the next measurement. `validate_recipe`'s generic JSON path used the same cached function; its cosmos, Team Velocity, DEP and HTML paths use synth's fetchers and were always live.
- **Place.** Both paths got the store place the lifecycle re-validation uses (`attribution.place.store_place`: registry plus page-learned street from hints), read-only.
- `gate_recipes` was never called. `one_condition_ok.txt` was read from the main checkout.
- **Requests made:** 1,856 for the validation paths (42 dealers), 251 for the VDP probes, 88 for the feed page-1 probes; 2,195 in all.

## 3. Sample

42 dealers, 90 live recipes, none of P0B.2's eight. Fixed members: hondaofelcajon-com and toyotaofhb-com (named by the plan); the 9 HTML walks whose stored total differs from their rows (the 10th, duvalford-com, belongs to P0B.2); mcgrathcityhonda-com (stored total below rows); highcountrytoyota-com and robinsford-com (more HTML walks, 13 HTML-walk dealers in all); darcarshondatenafly-com and covertbuickgmc-com (small sides); 8 sub-10 dealers (bmwoffremont, howardorloffvolvocars, crownlexus, jordanford, gardenahonda, mikecalverttoyota, scottclarkstoyota, audibellevue). The rest were drawn with a seeded shuffle from single-shape dealers with at least 50 local active cars: 4 cosmos, 4 Team Velocity, 4 carscommerce, 4 dealer.com, 1 typesense, 1 algolia. mclarennb-com was left out (0 live cars since 2026-08-05), as the plan says.

| dealer | live recipes by shape | local active |
|---|---|---|
| hondaofelcajon-com | dep_srp_page x2 | 380 |
| toyotaofhb-com | page_query (Team Velocity) x2, other (none) x2 | 823 |
| darcarshondatenafly-com | other (none) x1 | 362 |
| covertbuickgmc-com | carscommerce x2 | 1197 |
| downeyhyundai-com | dep_srp_page x2 | 624 |
| hyundaiofcookeville-com | dep_srp_page x2 | 374 |
| mymetrohonda-com | dep_srp_page x1 | 957 |
| nissanofcookeville-com | dep_srp_page x1 | 277 |
| autoboutiqueohio-com | html_page_query / jazel x1 | 981 |
| 5starford-com | html_page_query / jazel x1 | 810 |
| encinitasford-com | html_page_query / jazel x1 | 214 |
| fivestarforddallas-com | html_page_query / jazel x1 | 922 |
| hemborgford-com | html_page_query / jazel x1 | 284 |
| mcgrathcityhonda-com | dep_srp_page x1 | 225 |
| highcountrytoyota-com | html_page_query / jazel x2 | 232 |
| robinsford-com | html_page_query / jazel x1 | 276 |
| bmwoffremont-com | dealer_com x6 | 363 |
| howardorloffvolvocars-com | dealer_com x6 | 324 |
| crownlexus-com | dealer_com x2, other (none) x1 | 967 |
| jordanford-net | other (none) x2, page_query (Team Velocity) x2 | 792 |
| gardenahonda-com | other (none) x8 | 630 |
| mikecalverttoyota-com | cosmos_pt x4 | 730 |
| scottclarkstoyota-com | page_query (Team Velocity) x2 | 0 |
| audibellevue-com | other (none) x2 | 30 |
| pugmirefordcartersville-com | cosmos_pt x2 | 522 |
| bentleygmc-com | cosmos_pt x2 | 992 |
| lenoircityford-com | cosmos_pt x2 | 245 |
| bentleyhyundai-com | cosmos_pt x2 | 416 |
| hanselbmwofsantarosa-com | page_query (Team Velocity) x2 | 164 |
| hyundaiofsanbruno-com | page_query (Team Velocity) x2 | 497 |
| tustinhyundai-com | page_query (Team Velocity) x2 | 315 |
| northhollywoodtoyota-com | page_query (Team Velocity) x2 | 574 |
| marinochryslerjeepdodge-net | carscommerce x1 | 458 |
| villaford-com | carscommerce x1 | 531 |
| kiaonatlantic-com | carscommerce x1 | 546 |
| tuttleclickstustinjeep-com | carscommerce x1 | 175 |
| hendrickbmwnorthlake-com | dealer_com x1 | 448 |
| bmwmainline-com | dealer_com x6 | 455 |
| audisouthaustin-com | dealer_com x1 | 401 |
| harbinfordscottsboro-com | dealer_com x1 | 334 |
| hondaofcartersville-com | other (typesense_page) x1 | 390 |
| doggetthondamedcenter-com | other (algolia_page) x1 | 945 |

## 4. Offline part (all 594 recipe files, stored counts)

### 4a. Live recipes under 10 and under 5 VINs

- **38 live recipes on 33 dealers** store fewer than 10 VINs; **12 recipes on 11 dealers** store fewer than 5. This matches the plan's 10-07 count.
- By shape: `getInventoryAndFacets` fragments 18, ws-rec / vehicles-recommendations widgets 9, other single-shot 2, paginated but unpinned 2, VIN in URL or body 2, condition-pinned paginated sides 3, Gatsby page-data 1, `?vin=` 1.
- The local `dealer_recipes.scan_hints` carry **no `recipe_status` for any dealer** (0 of 687 rows), so "the recorded gate verdict" in the recommended rule cannot be read locally today. P12B.5 adds gate provenance; until then the offline estimate treats a missing verdict as eligible.
- The last column shows the live count where the recipe was in the sample. Stored counts are a poor guide: the fragment captures store their page size (1-24) and replay the whole section (13-197 VINs for the sampled sub-10 ones; crownlexus-com's 24-row capture replays 884); scottclarkstoyota-com's two Team Velocity feeds store 0 and replay 239 and 567.

| dealer | stored VINs | pagination | shape | pins | local active | excluded | live p40 VINs (if sampled) | recipe |
|---|---|---|---|---|---|---|---|---|
| autonationhondacostamesa-com | 1 | dealer_com_start | fragment:getInventoryAndFacets | - | 379 |  | - | `www.autonationhondacostamesa.com/api/widget/ws-inv-data/getI` |
| autonationtoyotacerritos-com | 2 | dealer_com_start | fragment:getInventoryAndFacets | - | 940 |  | - | `www.autonationtoyotacerritos.com/api/widget/ws-inv-data/getI` |
| autonationtoyotatempe-com | 1 | dealer_com_start | fragment:getInventoryAndFacets | - | 791 |  | - | `www.autonationtoyotatempe.com/api/widget/ws-inv-data/getInve` |
| bmwofdenverdowntown-com | 9 | dealer_com_start | fragment:getInventoryAndFacets | - | 481 |  | - | `www.bmwofdenverdowntown.com/api/widget/ws-inv-data/getInvent` |
| bmwoffairfax-com | 3 | dealer_com_start | fragment:getInventoryAndFacets | - | 474 |  | - | `www.bmwoffairfax.com/api/widget/ws-inv-data/getInventoryAndF` |
| bmwoffremont-com | 1 | dealer_com_start | fragment:getInventoryAndFacets | - | 363 |  | 13 | `www.bmwoffremont.com/api/widget/ws-inv-data/getInventoryAndF` |
| bmwofmountainview-com | 5 | dealer_com_start | fragment:getInventoryAndFacets | - | 449 |  | - | `www.bmwofmountainview.com/api/widget/ws-inv-data/getInventor` |
| centralhoustonnissan-com | 6 | dealer_com_start | fragment:getInventoryAndFacets | - | 175 |  | - | `www.centralhoustonnissan.com/api/widget/ws-inv-data/getInven` |
| conicellitoyotaofspringfield-com | 3 | dealer_com_start | fragment:getInventoryAndFacets | - | 466 |  | - | `www.conicellitoyotaofspringfield.com/api/widget/ws-inv-data/` |
| dchhondaofmissionvalley-com | 9 | dealer_com_start | fragment:getInventoryAndFacets | - | 339 |  | - | `www.dchhondaofmissionvalley.com/api/widget/ws-inv-data/getIn` |
| howardorloffvolvocars-com | 8 | dealer_com_start | fragment:getInventoryAndFacets | - | 324 |  | 197 | `www.howardorloffvolvocars.com/api/widget/ws-inv-data/getInve` |
| howardorloffvolvocars-com | 8 | dealer_com_start | fragment:getInventoryAndFacets | - | 324 |  | 115 | `www.howardorloffvolvocars.com/api/widget/ws-inv-data/getInve` |
| howardorloffvolvocars-com | 8 | dealer_com_start | fragment:getInventoryAndFacets | - | 324 |  | 35 | `www.howardorloffvolvocars.com/api/widget/ws-inv-data/getInve` |
| hudsontoyota-com | 9 | dealer_com_start | fragment:getInventoryAndFacets | - | 469 |  | - | `www.hudsontoyota.com/api/widget/ws-inv-data/getInventoryAndF` |
| kianorthaustin-com | 3 | dealer_com_start | fragment:getInventoryAndFacets | - | 433 |  | - | `www.kianorthaustin.com/api/widget/ws-inv-data/getInventoryAn` |
| lexusofbellevue-com | 6 | dealer_com_start | fragment:getInventoryAndFacets | - | 525 |  | - | `www.lexusofbellevue.com/api/widget/ws-inv-data/getInventoryA` |
| machaikford-com | 2 | dealer_com_start | fragment:getInventoryAndFacets | - | 770 |  | - | `www.machaikford.com/api/widget/ws-inv-data/getInventoryAndFa` |
| southcountylexus-com | 2 | dealer_com_start | fragment:getInventoryAndFacets | - | 15 |  | - | `www.southcountylexus.com/api/widget/ws-inv-data/getInventory` |
| gardenahonda-com | 6 | none | gatsby:page-data | - | 630 |  | 0 | `www.gardenahonda.com/page-data/index/page-data.json` |
| covertbuickgmc-com | 5 | carscommerce_page | paginated, unpinned (other) | - | 1197 |  | 4 | `websites-search.api.carscommerce.inc/api/v1/listings/22701/s` |
| mikecalverttoyota-com | 9 | cosmos_pt | paginated, unpinned (other) | - | 730 |  | 12 | `www.mikecalverttoyota.com/api/vhcliaa/vehicle-pages/cosmos/s` |
| audibellevue-com | 4 | none | pagination:none (other single-shot) | - | 30 |  | 2 | `audibellevue.autonation.com/api/dealer_new_inventory?…` |
| darcarshondatenafly-com | 6 | none | pagination:none (other single-shot) | - | 362 |  | 10 | `g58lko3etj-dsn.algolia.net/1/indexes/production-inventory-gl` |
| toyotaofhb-com | 7 | none | query:?vin= | - | 823 |  | 0 | `www.toyotaofhb.com/api/Inventory/vehicle?…` |
| mclarennb-com | 8 | dep_srp_page | side:condition-pinned paginated (new) | new | 0 |  | - | `www.mclarennb.com/search/new-mclaren/?tp=new` |
| scottclarkstoyota-com | 0 | page_query | side:condition-pinned paginated (new) | new | 0 |  | 567 | `www.scottclarkstoyota.com/inventory-new.json` |
| scottclarkstoyota-com | 0 | page_query | side:condition-pinned paginated (used) | used | 0 |  | 239 | `www.scottclarkstoyota.com/inventory-used.json` |
| jordanford-net | 6 | none | vin-list:VIN in URL/body | - | 792 |  | 2 | `www.jordanford.net/api/KeyFeatures/GetListOfSpecialVins?inve` |
| toyotaofhb-com | 7 | none | vin-list:VIN in URL/body | - | 823 |  | 0 | `www.toyotaofhb.com/api/KeyFeatures/GetListOfSpecialVins?inve` |
| bmwofdenverdowntown-com | 6 | none | widget:vehicles-recommendations/ws-rec | - | 481 |  | - | `www.bmwofdenverdowntown.com/api/widget/ws-rec-vehicles/vehic` |
| bmwofmonrovia-net | 6 | none | widget:vehicles-recommendations/ws-rec | - | 481 |  | - | `www.bmwofmonrovia.net/api/widget/ws-rec-vehicles/vehicles-re` |
| crownlexus-com | 6 | none | widget:vehicles-recommendations/ws-rec | - | 967 |  | 6 | `www.crownlexus.com/api/widget/ws-rec-vehicles/vehicles-recom` |
| fortmillford-com | 6 | none | widget:vehicles-recommendations/ws-rec | - | 369 |  | - | `www.fortmillford.com/api/widget/ws-rec-vehicles/vehicles-rec` |
| hondaofserramonte-com | 9 | none | widget:vehicles-recommendations/ws-rec | - | 235 |  | - | `www.hondaofserramonte.com/api/widget/ws-rec-vehicles/vehicle` |
| lexusserramonte-com | 9 | none | widget:vehicles-recommendations/ws-rec | - | 342 |  | - | `www.lexusserramonte.com/api/widget/ws-rec-vehicles/vehicles-` |
| longbeachbmw-com | 7 | none | widget:vehicles-recommendations/ws-rec | - | 490 |  | - | `www.longbeachbmw.com/api/widget/ws-rec-vehicles/vehicles-rec` |
| momentumbmw-net | 6 | none | widget:vehicles-recommendations/ws-rec | - | 537 |  | - | `www.momentumbmw.net/api/widget/ws-rec-vehicles/vehicles-reco` |
| mountainstatestoyota-com | 6 | none | widget:vehicles-recommendations/ws-rec | - | 962 | P0B.2 | - | `www.mountainstatestoyota.com/api/widget/ws-rec-vehicles/vehi` |

### 4b. D-OD1 union impact from stored counts

| Option | Dealers whose union changes | Recipes added | Note |
|---|---|---|---|
| Today (each recipe at least 10, `SCANNER_RECIPE_MIN_VEHICLES`) | baseline | 0 | darcarshondatenafly-com has no recipe at 10 or more by stored count (6); live it now replays 10 |
| Admit anything above 0 | 32 | 36 (at most 205 stored VINs, before overlap) | the widgets, VIN lists and fragments |
| Recommended (condition-pinned paginated sides with an ok/uncertain verdict; `pagination=none` refused; union at 5) | 1 | 1 | mclarennb-com: used 28 + new side 8 = 36 (P12B.5's McLaren test shape) |

### 4c. HTML walks with a stored total

**53 HTML-walk recipes** (dep_srp_page / html_page_query / jazel_srp_page / html_cards) carry a stored `total_count`: **42** equal to `vehicle_rows`, **10** with total above rows, **1** with total below rows (mcgrathcityhonda-com, 144 rows vs 138). All 11 non-equal ones except duvalford-com (P0B.2) were measured live (section 6).

## 5. Agreement: `validate_recipe` vs the set check

`validate_recipe` is the count every save path stores as `vehicle_rows`. The pages=40 `_check_recipe` walk is the closest stand-in for the scan's replay: the same request function (`recipes._replay_request`) and the same 40-page cap. It still differs from the scan in its stop reader (`extract_site_total` vs `get_total_count`) and its store place (roster place vs `DealerCtx`), which P12B.1 and P12B.2 align.

| shape | dealers | recipes | count = p40 | count > p40 | count < p40 | disagree on >=5 | p2 = p40 VINs | p40 hit 40-page cap |
|---|---|---|---|---|---|---|---|---|
| carscommerce | 5 | 6 | 6 | 0 | 0 | 0 | 2 | 0 |
| cosmos_pt | 5 | 12 | 11 | 1 | 0 | 0 | 6 | 0 |
| dealer_com | 7 | 23 | 8 | 0 | 15 | 0 | 5 | 0 |
| dep_srp_page | 6 | 9 | 5 | 1 | 3 | 0 | 1 | 2 |
| html_page_query / jazel | 7 | 8 | 4 | 0 | 4 | 0 | 0 | 1 |
| other (algolia_page) | 1 | 1 | 1 | 0 | 0 | 0 | 1 | 0 |
| other (none) | 6 | 16 | 16 | 0 | 0 | 0 | 16 | 0 |
| other (typesense_page) | 1 | 1 | 1 | 0 | 0 | 0 | 1 | 0 |
| page_query (Team Velocity) | 7 | 14 | 14 | 0 | 0 | 0 | 2 | 0 |

| dealer | shape | recipe | stored rows | stored total | validate_recipe | p2 VINs | p40 VINs | site total | p40 statuses | error |
|---|---|---|---|---|---|---|---|---|---|---|
| 5starford-com | html_page_query / jazel | `www.5starford.com/inventory/all-vehicles/` | 96 | 1338 | 947 | 48 | 952 | None | 200 |  |
| audisouthaustin-com | dealer_com | `www.audisouthaustin.com/api/widget/ws-inv-data/getInventory` | 48 | None | 48 | 96 | 408 | 466 | 200 |  |
| bmwmainline-com | dealer_com | `www.bmwmainline.com/api/widget/ws-inv-data/getInventory` | 50 | 312 | 48 | 96 | 357 | 357 | 200 |  |
| bmwmainline-com | dealer_com | `www.bmwmainline.com/api/widget/ws-inv-data/getInventoryAndFacets` | 24 | 312 | 312 | 48 | 357 | 357 | 200 |  |
| bmwmainline-com | dealer_com | `www.bmwmainline.com/api/widget/ws-inv-data/getInventory` | 50 | 97 | 48 | 96 | 116 | 116 | 200 |  |
| bmwmainline-com | dealer_com | `www.bmwmainline.com/api/widget/ws-inv-data/getInventory` | 41 | 41 | 48 | 54 | 54 | 54 | 200 |  |
| bmwoffremont-com | dealer_com | `www.bmwoffremont.com/api/widget/ws-inv-data/getInventory` | 50 | 348 | 48 | 74 | 74 | 364 | 200 |  |
| bmwoffremont-com | dealer_com | `www.bmwoffremont.com/api/widget/ws-inv-data/getInventory` | 50 | 266 | 48 | 96 | 249 | 249 | 200 |  |
| crownlexus-com | dealer_com | `www.crownlexus.com/api/widget/ws-inv-data/getInventory` | 100 | 100 | 48 | 96 | 347 | 347 | 200 |  |
| encinitasford-com | html_page_query / jazel | `www.encinitasford.com/inventory/all-vehicles/` | 213 | 214 | 216 | 24 | 252 | None | 200 |  |
| fivestarforddallas-com | html_page_query / jazel | `www.fivestarforddallas.com/inventory/all-vehicles/` | 922 | 924 | 927 | 72 | 933 | None | 200 |  |
| harbinfordscottsboro-com | dealer_com | `www.harbinfordscottsboro.com/api/widget/ws-inv-data/getInventory` | 100 | 100 | 48 | 96 | 354 | 354 | 200 |  |
| hendrickbmwnorthlake-com | dealer_com | `www.hendrickbmwnorthlake.com/api/widget/ws-inv-data/getInventory` | 280 | 280 | 62 | 96 | 612 | 980 | 200 |  |
| howardorloffvolvocars-com | dealer_com | `www.howardorloffvolvocars.com/api/widget/ws-inv-data/getInventory` | 50 | 191 | 48 | 96 | 197 | 197 | 200 |  |
| howardorloffvolvocars-com | dealer_com | `www.howardorloffvolvocars.com/api/widget/ws-inv-data/getInventoryAndFa` | 8 | 191 | 192 | 16 | 197 | 197 | 200 |  |
| howardorloffvolvocars-com | dealer_com | `www.howardorloffvolvocars.com/api/widget/ws-inv-data/getInventory` | 50 | 103 | 48 | 96 | 115 | 115 | 200 |  |
| howardorloffvolvocars-com | dealer_com | `www.howardorloffvolvocars.com/api/widget/ws-inv-data/getInventoryAndFa` | 8 | 103 | 104 | 16 | 115 | 115 | 200 |  |
| howardorloffvolvocars-com | dealer_com | `www.howardorloffvolvocars.com/api/widget/ws-inv-data/getInventoryAndFa` | 8 | 22 | 24 | 16 | 35 | 35 | 200 |  |
| hyundaiofcookeville-com | dep_srp_page | `www.hyundaiofcookeville.com/search/new-hyundai/?tp=new` | 337 | 337 | 348 | 24 | 360 | 360 | 200 |  |
| mcgrathcityhonda-com | dep_srp_page | `www.mcgrathcityhonda.com/search/new-honda/?tp=new%2F&ct=48` | 144 | 138 | 144 | 96 | 256 | 256 | 200 |  |
| mikecalverttoyota-com | cosmos_pt | `www.mikecalverttoyota.com/api/vhcliaa/vehicle-pages/cosmos/srp/vehicle` | 12 | None | 334 | 124 | 124 | 124 | 200 |  |
| mymetrohonda-com | dep_srp_page | `www.mymetrohonda.com/search/new-honda/?tp=new&ct=48` | 748 | 753 | 768 | 96 | 779 | 779 | 200 |  |
| nissanofcookeville-com | dep_srp_page | `www.nissanofcookeville.com/search/used/?tp=used&ct=48` | 1229 | 1311 | 980 | 94 | 979 | 1050 | 200 |  |
| robinsford-com | html_page_query / jazel | `www.robinsford.com/inventory/all-vehicles/` | 266 | 266 | 276 | 24 | 293 | None | 200 |  |

Causes, each confirmed in code:

1. **dealer.com page size (15 recipes, 7 dealers).** `validate_recipe`'s generic loop (`recipe_validation.py:1013-1032`) never calls `_learn_dealer_com_page_size`, which `_check_recipe` (`:636-638`) and the scan (`recipes.py:1108-1109`) both call. Page 2 starts at the captured preference's offset, past the end, and the walk stops at exactly 48 (10 recipes) or short of the lot (24, 62, 104, 192, 312). This is the 2026-09-24 "stopped at 48" bug, still alive in the count every save path stores. Example: harbinfordscottsboro-com stores 100, `validate_recipe` says 48, the scan path walks 354 of 354.
2. **Stale stored total (6 recipes).** `validate_recipe` stops at `recipe.total_count` (`:1031`, `:1093`, `:1125`); the set check stops at the page's own count. mcgrathcityhonda-com: stored total 138, `validate_recipe` 144, live 256. mymetrohonda-com 768 vs 779, hyundaiofcookeville-com new 348 vs 360, encinitasford-com 216 vs 252, robinsford-com 276 vs 293, fivestarforddallas-com 927 vs 933.
3. **Cosmos query dropped (1 recipe).** `_validate_cosmos` (`:1134`) strips the query string, which on mikecalverttoyota-com carried `cpo=1`: `validate_recipe` counted 334 (all used), the replay sees 124 (CPO only). The saved `vehicle_rows` describes a request the scan never sends.
4. **Small timing differences** (5starford-com 947 vs 952, nissanofcookeville-com 980 vs 979): the lot moved between walks.

None of the 24 differences crosses the 5-VIN floor, so today they change `vehicle_rows`, not pass/fail.

## 6. HTML walks: VINs past the stored total, and the 40-page cap

| dealer | pagination | recipe | stored rows | stored total | validate_recipe | p40 VINs | site total | pages | cap hit | p40 - stored total |
|---|---|---|---|---|---|---|---|---|---|---|
| 5starford-com | jazel_srp_page | `www.5starford.com/inventory/all-vehicles/` | 96 | 1338 | 947 | 952 | None | 40 | yes | -386 |
| autoboutiqueohio-com | html_page_query | `www.autoboutiqueohio.com/inventory` | 1000 | 1001 | 885 | 885 | None | 37 |  | -116 |
| downeyhyundai-com | dep_srp_page | `www.downeyhyundai.com/search/used/?tp=used` | 195 | 195 | 183 | 183 | 183 | 16 |  | -12 |
| downeyhyundai-com | dep_srp_page | `www.downeyhyundai.com/search/new-hyundai/?tp=new` | 480 | 1384 | 480 | 480 | 1357 | 40 | yes | -904 |
| encinitasford-com | jazel_srp_page | `www.encinitasford.com/inventory/all-vehicles/` | 213 | 214 | 216 | 252 | None | 22 |  | 38 |
| fivestarforddallas-com | jazel_srp_page | `www.fivestarforddallas.com/inventory/all-vehicles/` | 922 | 924 | 927 | 933 | None | 28 |  | 9 |
| hemborgford-com | jazel_srp_page | `www.hemborgford.com/inventory/all-vehicles/` | 292 | 294 | 285 | 285 | None | 25 |  | -9 |
| highcountrytoyota-com | html_page_query | `www.highcountrytoyota.com/inventory/used` | 117 | 117 | 92 | 92 | None | 7 |  | -25 |
| highcountrytoyota-com | html_page_query | `www.highcountrytoyota.com/inventory/new` | 186 | 186 | 138 | 138 | None | 10 |  | -48 |
| hondaofelcajon-com | dep_srp_page | `www.hondaofelcajon.com/search/used/?tp=used&ct=48` | 56 | 56 | 65 | 65 | 65 | 2 |  | 9 |
| hondaofelcajon-com | dep_srp_page | `www.hondaofelcajon.com/search/new-honda/?tp=new&ct=48` | 343 | 343 | 357 | 357 | 357 | 8 |  | 14 |
| hyundaiofcookeville-com | dep_srp_page | `www.hyundaiofcookeville.com/search/pre-owned/?tp=pre_owned` | 451 | 1282 | 457 | 457 | 1080 | 40 | yes | -825 |
| hyundaiofcookeville-com | dep_srp_page | `www.hyundaiofcookeville.com/search/new-hyundai/?tp=new` | 337 | 337 | 348 | 360 | 360 | 30 |  | 23 |
| mcgrathcityhonda-com | dep_srp_page | `www.mcgrathcityhonda.com/search/new-honda/?tp=new%2F&ct=48` | 144 | 138 | 144 | 256 | 256 | 6 |  | 118 |
| mymetrohonda-com | dep_srp_page | `www.mymetrohonda.com/search/new-honda/?tp=new&ct=48` | 748 | 753 | 768 | 779 | 779 | 17 |  | 26 |
| nissanofcookeville-com | dep_srp_page | `www.nissanofcookeville.com/search/used/?tp=used&ct=48` | 1229 | 1311 | 980 | 979 | 1050 | 23 |  | -332 |
| robinsford-com | jazel_srp_page | `www.robinsford.com/inventory/all-vehicles/` | 266 | 266 | 276 | 293 | None | 26 |  | 27 |

- **Past the stored total (7 recipes, 7 dealers):** mcgrathcityhonda-com +118, encinitasford-com +38, robinsford-com +27, mymetrohonda-com +26, hyundaiofcookeville-com new +23, hondaofelcajon-com new +14 and used +9, fivestarforddallas-com +9. These are P12B.10's "VINs beyond the stored total" targets.
- **Truncated by the 40-page cap (3 recipes):** downeyhyundai-com new (12 per page, 480 of 1,357), hyundaiofcookeville-com pre-owned (12 per page, 457 of 1,080), 5starford-com (24 per page, 952; stored total 1,338). The scan's replay has the same cap (`recipes.py:507`), so the scan under-collects these three today (local active: 624, 374, 810). The two DEP recipes lack the `ct=48` page size that the other DEP recipes carry; with it, 40 pages would hold 1,920.
- **Lot below the stored total:** autoboutiqueohio-com (885 vs 1,001), nissanofcookeville-com (979 vs 1,311; site says 1,050), highcountrytoyota-com (92 vs 117 used, 138 vs 186 new), hemborgford-com (285 vs 294), downeyhyundai-com used (183 vs 195). The walks ended on an empty page, not on an error.
- 7 of the 13 HTML-walk dealers expose no site total (5 jazel, 1 overfuel, highcountrytoyota-com's HTML pages), so the gate says `uncertain:site_total_unknown` at any page count and never judges coverage for them.

## 7. Verdicts: pages=2 vs pages=40 (D-OD2)

| dealer | p2 status | p40 status | flip | p2 VINs | p40 VINs | site total | p2 cov | p40 cov | p40 reasons |
|---|---|---|---|---|---|---|---|---|---|
| hanselbmwofsantarosa-com | ok | rejected:coverage | FLIP | 107 | 163 | 407 | 0.263 | 0.4 | coverage: 163 of 407 with no further page |
| tustinhyundai-com | rejected:short_page | uncertain:short_page | FLIP | 135 | 240 | 394 | 0.343 | 0.609 | short_page |
| 5starford-com | uncertain:site_total_unknown | uncertain:site_total_unknown |  | 48 | 952 | None | None | None | site_total_unknown: platform exposes no count; coverage cannot be judged |
| audibellevue-com | ok | ok |  | 32 | 32 | 30 | 1.067 | 1.067 |  |
| audisouthaustin-com | ok | ok |  | 96 | 408 | 466 | 0.206 | 0.876 |  |
| autoboutiqueohio-com | uncertain:site_total_unknown | uncertain:site_total_unknown |  | 50 | 885 | None | None | None | site_total_unknown: platform exposes no count; coverage cannot be judged |
| bentleygmc-com | ok | ok |  | 384 | 958 | 515 | 0.746 | 1.86 |  |
| bentleyhyundai-com | ok | ok |  | 384 | 886 | 493 | 0.779 | 1.797 |  |
| bmwmainline-com | ok | ok |  | 192 | 473 | 357 | 0.538 | 1.325 |  |
| bmwoffremont-com | ok | ok |  | 170 | 323 | 364 | 0.467 | 0.887 |  |
| covertbuickgmc-com | ok | ok |  | 200 | 763 | 764 | 0.262 | 0.999 |  |
| crownlexus-com | ok | ok |  | 121 | 1159 | 887 | 0.136 | 1.307 |  |
| darcarshondatenafly-com | uncertain:one_condition_unverified | uncertain:one_condition_unverified |  | 10 | 10 | 10 | 1.0 | 1.0 | one_condition_unverified |
| doggetthondamedcenter-com | ok | ok |  | 1022 | 1022 | 1022 | 1.0 | 1.0 |  |
| downeyhyundai-com | ok | ok |  | 48 | 663 | 1540 | 0.031 | 0.431 |  |
| encinitasford-com | uncertain:site_total_unknown | uncertain:site_total_unknown |  | 24 | 252 | None | None | None | site_total_unknown: platform exposes no count; coverage cannot be judged |
| fivestarforddallas-com | uncertain:site_total_unknown | uncertain:site_total_unknown |  | 72 | 933 | None | None | None | site_total_unknown: platform exposes no count; coverage cannot be judged |
| gardenahonda-com | uncertain:site_total_unknown | uncertain:site_total_unknown |  | 649 | 649 | None | None | None | site_total_unknown: platform exposes no count; coverage cannot be judged |
| harbinfordscottsboro-com | ok | ok |  | 96 | 354 | 354 | 0.271 | 1.0 |  |
| hemborgford-com | uncertain:site_total_unknown | uncertain:site_total_unknown |  | 24 | 285 | None | None | None | site_total_unknown: platform exposes no count; coverage cannot be judged |
| hendrickbmwnorthlake-com | ok | ok |  | 96 | 612 | 980 | 0.098 | 0.624 |  |
| highcountrytoyota-com | uncertain:site_total_unknown | uncertain:site_total_unknown |  | 64 | 230 | None | None | None | site_total_unknown: platform exposes no count; coverage cannot be judged |
| hondaofcartersville-com | ok | ok |  | 392 | 392 | 780 | 0.503 | 0.503 |  |
| hondaofelcajon-com | ok | ok |  | 161 | 422 | 422 | 0.382 | 1.0 |  |
| howardorloffvolvocars-com | ok | ok |  | 192 | 312 | 197 | 0.975 | 1.584 |  |
| hyundaiofcookeville-com | ok | ok |  | 48 | 817 | 1440 | 0.033 | 0.567 |  |
| hyundaiofsanbruno-com | ok | ok |  | 153 | 522 | 522 | 0.293 | 1.0 |  |
| jordanford-net | ok | ok |  | 166 | 789 | 647 | 0.257 | 1.219 |  |
| kiaonatlantic-com | ok | ok |  | 200 | 535 | 536 | 0.373 | 0.998 |  |
| lenoircityford-com | ok | ok |  | 251 | 251 | 161 | 1.559 | 1.559 |  |
| marinochryslerjeepdodge-net | ok | ok |  | 200 | 445 | 446 | 0.448 | 0.998 |  |
| mcgrathcityhonda-com | ok | ok |  | 96 | 256 | 256 | 0.375 | 1.0 |  |
| mikecalverttoyota-com | ok | ok |  | 397 | 847 | 723 | 0.549 | 1.172 |  |
| mymetrohonda-com | rejected:one_condition | rejected:one_condition |  | 96 | 779 | 779 | 0.123 | 1.0 | one_condition: only new rows while the site sells both (recipes pinned to ['new'] with no recipe for the other side; the used side answers 48 VIN(s) at https:// |
| nissanofcookeville-com | rejected:one_condition | rejected:one_condition |  | 94 | 979 | 1050 | 0.09 | 0.932 | one_condition: only used rows while the site sells both (recipes pinned to ['used'] with no recipe for the other side; the new side answers 46 VIN(s) at https:/ |
| northhollywoodtoyota-com | ok | ok |  | 200 | 659 | 659 | 0.303 | 1.0 |  |
| pugmirefordcartersville-com | ok | ok |  | 326 | 556 | 422 | 0.773 | 1.318 |  |
| robinsford-com | uncertain:site_total_unknown | uncertain:site_total_unknown |  | 24 | 293 | None | None | None | site_total_unknown: platform exposes no count; coverage cannot be judged |
| scottclarkstoyota-com | ok | ok |  | 200 | 806 | 1135 | 0.176 | 0.71 |  |
| toyotaofhb-com | ok | ok |  | 200 | 905 | 764 | 0.262 | 1.185 |  |
| tuttleclickstustinjeep-com | ok | ok |  | 169 | 169 | 169 | 1.0 | 1.0 |  |
| villaford-com | ok | ok |  | 200 | 453 | 453 | 0.442 | 1.0 |  |

- **2 flips in 42.**
  - hanselbmwofsantarosa-com: the used Team Velocity feed is a group feed (244 sibling rows refused over 7 pages, 32 kept). The full walk exhausts, so the coverage rule fires: 163 kept against a site total of 407 that counts the siblings. A working recipe set would be rejected under option (A) as written. The same arithmetic holds hondaofcartersville-com at 0.503 (392 kept, 358 refused, site total 780), one sale away from the 0.50 floor.
  - tustinhyundai-com: page 2 of the used feed adds no kept VIN (65 sibling rows refused), which reads as a short page. At p2 the coverage is 0.343, so it is a reject; at p40 the new feed adds 105 VINs and the coverage passes 0.50, so it becomes `uncertain`. P12B.2's "terminate on raw VINs" would remove this short page.
- **Two live sets are rejected at both page counts** (no flip, but relevant to P12B.7's re-judge): mymetrohonda-com and nissanofcookeville-com are `rejected:one_condition` (new-only and used-only recipes; the other side answers 48 and 46 VINs at its own URL).
- **The coverage rule cannot see a capped walk.** `exhausted` stays false when the walk hits 40 pages, so downeyhyundai-com (0.43) and hyundaiofcookeville-com (0.57 overall; the used recipe alone is 0.42) pass as `ok` while truncated.
- **Set totals over 1.0.** When the set's recipes are not provably disjoint (cosmos page ids, dealer.com `listing.config.id` sections), the set total is the max, not the sum, and coverage reads 1.17-1.86 (bentleygmc-com, bentleyhyundai-com, howardorloffvolvocars-com, lenoircityford-com, pugmirefordcartersville-com, bmwmainline-com, crownlexus-com, jordanford-net, mikecalverttoyota-com, toyotaofhb-com). That hides a missing section from the coverage rule.
- Cost: the pages=40 walks read 776 pages for the 90 recipes (about 18 per dealer), against 158 for pages=2.

## 8. D-OD1 on the sample (live VIN sets)

Union per option from the pages=40 VIN sets. Only dealers with a sub-floor recipe live are listed; on the other 38 dealers every live recipe yields 10 or more or 0.

| dealer | p2 verdict | today union (recipes) | admit >0 (+new VINs) | recommended (+new VINs) | sub-floor recipes | zero-VIN recipes |
|---|---|---|---|---|---|---|
| audibellevue-com | ok | 30 (1) | 32 (+2) | 30 (+0) | 2 VINs none pins=- new=2 `audibellevue.autonation.com/api/dealer_new_invento` | 0 |
| covertbuickgmc-com | ok | 763 (1) | 763 (+0) | 763 (+0) | 4 VINs carscommerce_page pins=- new=0 `websites-search.api.carscommerce.inc/api/v1/listin` | 0 |
| crownlexus-com | ok | 1159 (2) | 1159 (+0) | 1159 (+0) | 6 VINs none pins=- new=0 `www.crownlexus.com/api/widget/ws-rec-vehicles/vehi` | 0 |
| jordanford-net | ok | 789 (2) | 789 (+0) | 789 (+0) | 2 VINs none pins=- new=0 `www.jordanford.net/api/KeyFeatures/GetKeyFeaturesB`; 2 VINs none pins=- new=0 `www.jordanford.net/api/KeyFeatures/GetListOfSpecia` | 0 |

First-hit threshold `min(10, max(3, ceil(0.5 x vehicle_rows)))` vs today's 10, per recipe:

| dealer | recipe | live VINs | stored rows | new threshold | today accepts | new rule accepts |
|---|---|---|---|---|---|---|
| covertbuickgmc-com | carscommerce listing 22701, Location facet `custom_text_6 = Covert Buick GMC` | 4 | 5 | 3 | no | yes |
| crownlexus-com | `ws-rec-vehicles/vehicles-recommendations` widget | 6 | 6 | 3 | no | yes |

- Of the 8 sub-floor-by-storage recipes that replay 10 or more, 7 are paginated section feeds and 1 is darcarshondatenafly-com's single-shot CPO feed (below). The `getInventoryAndFacets` ones duplicate a `getInventory` recipe of the same section in 3 of 4 dealers (0 unique VINs), but on crownlexus-com the fragment capture is the main source (884 VINs, 812 unique).
- The 7 that stay under 10 contribute 2 unique VINs in total (audibellevue-com's A5 lease special).
- darcarshondatenafly-com's only recipe is an Algolia CPO side (`cpo:"true"` inside the `filters` string, `pagination=none`). It replays 10 today and 6 at capture. `recipe_condition_filter` does not read Algolia filter strings, so the recommended rule treats it as an unpinned single-shot and refuses it whenever it dips under the floor; the dealer then has no recipe.
- **First-hit risk.** Under the proposed first-hit threshold, crownlexus-com's ws-rec widget (6 VINs, stored 6, threshold 3) and covertbuickgmc-com's location-facet recipe (4 VINs, stored 5, threshold 3) would pass. Both sit after the real feed in their files, so today's order hides them. If the main recipe goes stale or 403s, a first-hit replay would return 6 or 4 VINs as a success. Today the floor of 10 refuses both.

## 9. Fetch behaviour (D-OD3, D-OD7, D-OD8)

### 9a. Status counts

- Validation paths, 1,856 requests: 200 x1,304, 403 x547, 204 x3, transport error x2. Of the 403s: 299 on plain `requests` (248 carried challenge markers), 200 on curl_cffi `chrome` (all with challenge markers), 48 on urllib (the Akamai Team Velocity feeds). **0 x405, 0 x429, 0 x503, 0 other 5xx.**
- VDP and feed probes, 339 requests: 200 x253, 403 x81, 410 x5 (one sold car on bmwmainline-com).
- Replay non-200 pages, by URL across the three measurements: only toyotaofhb-com's `?vin=` recipe (204 every time). No page answered non-200 once and 200 later, so nothing transient was seen.

### 9b. What replay pays per page

On 11 of 42 dealers the replay needed TLS impersonation: 800 wire requests for 301 page fetches (2.66 per page).

- **Cloudflare HTML walks** (5starford, downeyhyundai, fivestarforddallas, hondaofelcajon, hyundaiofcookeville, mcgrathcityhonda, mymetrohonda, nissanofcookeville): plain `requests` with replay's JSON headers gets a 403 challenge, the `chrome` profile gets a 403 challenge, `chrome124` gets 200. That is 3.0 requests per page (2.0 on 5starford, where `chrome` mostly cleared). Synth's urllib fetch with the navigation headers and a same-site Referer got 200 on the first request on all of them (`count` phase: 461 x200).
- **Akamai Team Velocity feeds** (hyundaiofsanbruno, jordanford, scottclarkstoyota): plain clients get 403 with any header set (urllib and `requests` alike); every impersonation profile clears. 2.0 per page.

### 9c. Feed page 1: profile x UA, and headers alone

| dealer | pagination | url | chrome+UA | chrome own | chrome124+UA | chrome124 own | safari+UA | safari own | requests, replay headers | requests, synth nav headers + same-site Referer |
|---|---|---|---|---|---|---|---|---|---|---|
| 5starford-com | jazel_srp_page | `www.5starford.com/inventory/all-vehicles/` | 403+ch | 403+ch | 200 | 200 | 200 | 200 | 403+ch | 200 |
| downeyhyundai-com | dep_srp_page | `www.downeyhyundai.com/search/used/?tp=used&p=1` | 403+ch | 403+ch | 200 | 200 | 200 | 200 | 403+ch | 200 |
| fivestarforddallas-com | jazel_srp_page | `www.fivestarforddallas.com/inventory/all-vehicles/` | 403+ch | 403+ch | 200 | 200 | 200 | 200 | 403+ch | 200 |
| hondaofelcajon-com | dep_srp_page | `www.hondaofelcajon.com/search/used/?tp=used&ct=48&p=1` | 403+ch | 403+ch | 200 | 200 | 200 | 200 | 403+ch | 200 |
| hyundaiofcookeville-com | dep_srp_page | `www.hyundaiofcookeville.com/search/pre-owned/?tp=pre_owned&p` | 403+ch | 403+ch | 403+ch | 200 | 200 | 200 | 403+ch | 200 |
| hyundaiofsanbruno-com | page_query | `hyundaiofsanbruno.com/inventory-used.json?page=1` | 200 | 200 | 200 | 200 | 200 | 200 | 403 | 403 |
| jordanford-net | page_query | `www.jordanford.net/inventory-used.json?page=1` | 200 | 200 | 200 | 200 | 200 | 200 | 403 | 403 |
| mcgrathcityhonda-com | dep_srp_page | `www.mcgrathcityhonda.com/search/new-honda/?tp=new%2F&ct=48&p` | 403+ch | 403+ch | 200 | 200 | 200 | 200 | 403+ch | 200 |
| mymetrohonda-com | dep_srp_page | `www.mymetrohonda.com/search/new-honda/?tp=new&ct=48&p=1` | 403+ch | 403+ch | 200 | 200 | 200 | 200 | 403+ch | 200 |
| nissanofcookeville-com | dep_srp_page | `www.nissanofcookeville.com/search/used/?tp=used&ct=48&p=1` | 403+ch | 403+ch | 200 | 200 | 200 | 200 | 403+ch | 200 |
| scottclarkstoyota-com | page_query | `www.scottclarkstoyota.com/inventory-used.json?page=1` | 200 | 200 | 200 | 200 | 200 | 200 | 403 | 403 |

- Headers alone clear the Cloudflare HTML edges: plain `requests` with synth's `_browser_headers()` + same-site Referer + `Sec-Fetch-Site: same-origin` got 200 on 8 of 8 HTML hosts; replay's headers got 403 with a challenge on 8 of 8. This is what P12A.2 plans for HTML paginations.
- The Akamai JSON feeds need impersonation; headers do not help (3 of 3 refused either way).
- UA: on the 8 HTML hosts `chrome` fails with either UA (16 of 16), `chrome124` and `safari17_0` clear with either UA (31 of 32; the exception is one `chrome124`+Chrome-UA 403 on hyundaiofcookeville-com while its own UA cleared). No probe was cleared by the explicit UA and refused with the profile's own.

### 9d. VDPs: chrome-only vs rotation, explicit UA vs the profile's own

10 dealers x 5 VDPs (picked read-only from local active cars; 3 Dealer Inspire / carscommerce hosts). Each VDP got five probes in a seeded random order: today's `vdp.prefetch._fetch_html` (curl_cffi `chrome` + Chrome/126 UA, then plain `requests` unless the status is a block), `chrome124` + UA, `safari17_0` + Chrome UA, `safari17_0` with its own UA, `chrome` with its own UA.

| dealer | VDPs | live (not 404/410) | today's prefetch | chrome only (curl_cffi) | rotation c->c124->saf | chrome124+UA | safari+Chrome UA | safari own UA | chrome own UA | statuses |
|---|---|---|---|---|---|---|---|---|---|---|
| audisouthaustin-com | 5 | 5 | 0 | 0 | 0 | 0 | 0 | 5 | 0 | {"403": 20, "200": 5} |
| bentleygmc-com | 5 | 5 | 5 | 5 | 5 | 5 | 5 | 5 | 5 | {"200": 25} |
| bmwmainline-com | 5 | 4 | 4 | 4 | 4 | 4 | 4 | 4 | 4 | {"200": 20, "410": 5, "403": 1} |
| covertbuickgmc-com | 5 | 5 | 0 | 0 | 5 | 5 | 5 | 5 | 0 | {"200": 15, "403": 5, "403+ch": 5} |
| encinitasford-com | 5 | 5 | 5 | 5 | 5 | 5 | 5 | 5 | 5 | {"200": 25} |
| hondaofelcajon-com | 5 | 5 | 5 | 5 | 5 | 5 | 5 | 5 | 4 | {"200": 24, "403": 1} |
| kiaonatlantic-com | 5 | 5 | 0 | 0 | 5 | 5 | 5 | 5 | 1 | {"200": 16, "403": 4, "403+ch": 5} |
| lenoircityford-com | 5 | 5 | 5 | 5 | 5 | 5 | 5 | 5 | 5 | {"200": 25} |
| tustinhyundai-com | 5 | 5 | 5 | 5 | 5 | 5 | 5 | 5 | 5 | {"200": 25} |
| villaford-com | 5 | 5 | 1 | 1 | 5 | 5 | 5 | 5 | 0 | {"200": 16, "403": 5, "403+ch": 4} |
| **total** | 50 | 49 | 30 | 30 | 44 | 44 | 44 | 49 | 29 |  |

- **Rotation clear rate:** chrome only 30 of 49 live VDPs (61%); rotation chrome -> chrome124 -> safari17_0 with the explicit UA 44 of 49 (90%); the same rotation with the profile's own UA on safari 49 of 49 (100%).
- **UA effect:** `safari17_0` with the Chrome/126 UA failed all 5 audisouthaustin-com VDPs (dealer.com), which `safari17_0` with its own UA cleared, and which nothing else cleared. `chrome` with its own UA cleared 29, with the explicit UA 30 (one hondaofelcajon-com 403, not a pattern).
- **Dealer Inspire hosts** (covertbuickgmc, kiaonatlantic, villaford), serialized at 1.5 s: `chrome` cleared 1 of 15, `chrome124` and `safari17_0` 15 of 15. No 429.
- **Not measured: fleet-like concurrency on Dealer Inspire.** The plan asks for 2-3 DI hosts at fleet concurrency (6 workers x 150 ms, rotation vs slow-host serialization). This run's rule was at least 1 s between requests per host, so that burst was not sent. Rotation clears DI at the serialized rate; whether it also clears at 6-wide needs an owner-approved burst test (about 30 requests per host).

## 10. Inputs for the decisions

These lines are written to be pasted into Section 7 of the plan. The plan was not edited here, because Phase 1A was merging into the main checkout at the same time.

- **D-OD1.** P0B.1 (2026-10-08, home IP, 42 dealers / 90 recipes): 8 of 15 sampled sub-10-by-storage recipes replay 10-567 VINs (stored counts are page sizes); the 7 real sub-10 recipes are widgets, VIN lists, `?vin=`, Gatsby index, a lease special, a location facet, worth 2 union VINs on 1 dealer. The recommended union rule changes 1 dealer offline (mclarennb +8) and 0 in the sample. The first-hit threshold as written admits crownlexus's 6-VIN widget and covertbuickgmc's 4-VIN facet; limit it to recipes the union rule admits. `recipe_condition_filter` misses Algolia `filters` strings (darcarshondatenafly CPO side).
- **D-OD2.** P0B.1: p2 vs p40 verdicts flip on 2 of 42 dealers (hanselbmw ok -> coverage reject, a false reject against a group-wide site total; tustinhyundai short_page reject -> uncertain). p2 = p40 VIN counts on 34 of 90 recipes. A walk capped at 40 pages never arms the coverage reject (downeyhyundai 480/1,357, hyundaiofcookeville 457/1,080 stay ok). Before (A) ships: compute coverage against kept + sibling-refused VINs, and treat "40 pages, not exhausted" as truncated (D-SR4's `replay_truncated:`).
- **D-OD3.** P0B.1: `validate_recipe` vs the scan-path walk agree on 66 of 90 recipes; the 24 differences are the missing dealer.com page-size learn (15), the stale stored total stop (6), cosmos query stripping (1) and lot drift (2). On 8 of 8 Cloudflare HTML hosts, plain `requests` with synth's navigation headers + same-site Referer gets 200 where replay's headers get a 403 challenge; replay pays 2.66 requests per page on 11 of 42 dealers. Supports (A).
- **D-OD7.** P0B.1: block statuses seen: 403 only (628 of 2,195; 0 x405/429/503). Challenges came as 403 bodies. VDP clear rate: chrome only 30/49, rotation 44/49, rotation with profile UA 49/49. The explicit Chrome UA under `safari17_0` lost 5/49 VDPs (audisouthaustin) and gained none, so the profile sets its own UA. Starting from the per-host winner saves one request per page at 7 Cloudflare HTML hosts (`chrome` loses, `chrome124` wins). DI at fleet concurrency not measured (needs an owner-approved burst test).
- **D-OD8.** P0B.1: no transient 429/5xx on 798 replay page fetches; the condition for the retry is not met on the home IP. Re-check on Railway egress before deciding "none".

## 11. Dealer logs

Each of the 42 dealers has one dated block in `workspace/dealer_logs/<dealer>/discovery.md` ("2026-10-08 13:45 UTC validation-path measurement (P0B.1, no writes)") and one in `scan_runs.md` ("measurement P0B.1, no writes"). The blocks give per recipe: stored rows and total, the `validate_recipe` count, the p2 and p40 VINs and statuses, the site total, the wire statuses by client and profile, and the VDP and feed-probe results where taken. `_learning/` was not touched (P0B.2 owns `platform_playbook.md` in this wave).

## 12. Limits

- One pass per dealer, from one residential IP, on one morning. Lots moved during the run (a few VINs between walks).
- 200 answers were cached per dealer, so the p2 walk did not re-fetch pages 1-2 of the p40 walk. A transient failure that a repeat would have shown is only visible where a non-200 was re-fetched (none occurred).
- `validate_recipe` got the lifecycle's store place (registry plus hints), not the homepage-learned place `ensure_recipe` passes. Both parse paths saw the same place, so the agreement numbers compare walkers and fetchers, not places.
- The D-OD1 offline numbers use stored counts; section 8 shows how far those are from live counts.
