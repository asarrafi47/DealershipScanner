# Incomplete Listings: Patterns and Issues (October 2026)

Date: 2026-10-09. Scope: the local Postgres inventory (a mirror of prod as of 2026-09-28), read-only. Every finding below was checked by a second, independent pass; where that pass corrected a number, the corrected number is used. The exception is the material a completeness review added the same day (items 2.21-2.25, section 3.3 and the placeholder-photo note in 2.4). Those items are single-pass and marked that way. Retired rows (`listing_active=0`) are kept separate from active rows throughout. "Data says" is what the database shows. "Code says" is what the source suggests, with file:line.

Related earlier work: `docs/DATA_COMPLETENESS_FINDINGS_2026_09_28.md` and `workspace/data_quality/incomplete_2026_09_28/findings.json` (F01, F02, F07, F08, F12, F13, F17 overlap items below). Remediation phases refer to `docs/REMEDIATION_PLAN_2026_10.md`.

---

## 1. Summary

**Gray area: brand-new arrivals (owner note, 2026-10-09).** A car that appeared on the lot in the last few days often has no price, photos or full details yet at the dealer itself. That is not a scanner or data defect, so new arrivals must not be read as failures. Item 2.23 shows the effect: cars first seen 2026-09-28 are 16.15% incomplete against 2.4-5.9% for 09-20 to 09-26. Any fix or metric built on this list should separate a "new arrival" window (first_seen_at within N days) from real gaps: for example a grace period before a car counts as incomplete, or a "details coming" state instead of the incomplete pill. Only a fresh scan settles whether the 09-28 cohort filled in at the source.

**How many.** 214,675 cars are active. The `incomplete_listings` index flags 11,592 of them (5.40%). That count is roughly right, but about 2,700 cars sit on the wrong side of it:

- About 1,370 flagged cars (range 1,150-1,650) are complete under today's code. Most were flagged for engine, fuel type or body style before a VIN decode or a later write filled the gap.
- 1,256 used or certified cars stored at 0 miles are incomplete under the mileage rule added on 2026-09-28, and none carries the code. 965 of them are not in the index at all.
- 381 cars whose stored fuel type is an injection system ("Direct Injection") are incomplete under today's code. None is flagged.

Recomputed with today's code, about 11,200-11,600 active cars (5.2-5.4%) are incomplete. Most are missing exactly one field: 84% of indexed cars miss one, 12% miss two.

**What is missing (active cars):**

| Field | Flagged in index | Real under today's code |
|---|---|---|
| price (price and MSRP both empty) | 4,676 | 4,676 (plus 357 junk prices under $1,000 that count as present, 2.21) |
| trim | 3,545 | 3,545 (about 577 of them are a display false positive, F1) |
| exterior color | 2,469 | 2,469 |
| engine | 1,379 | about 438 |
| fuel type | 684 | about 630 (most of the 684 are stale; 381 new ones are unflagged) |
| body style | 678 | about 260 |
| transmission | 382 | about 310 |
| drivetrain | 307 | about 245 |
| cylinders | 268 | about 230 (111 are Metro Honda order-number rows) |
| mileage | 0 | 1,256 |
| model / make | 129 / 2 | 129 / 2 |

Not counted, by design or by omission (single pass, items 2.4, 2.21, 2.22):
- **Photos.** 5,394 active cars have no photo URL. About 3,000 more show only a placeholder ("photo unavailable", "coming soon"), and about 3,200 show only OEM stock art. Placeholders count as photos.
- **Interior color.** 6,712 active cars have none. The rule excludes it as "dealer-discretionary", but 5,092 of those blanks are at 296 dealers that publish interior color on at least 75% of their own rows.
- **Junk prices.** 357 active cars have a price between $10 and $999 (for example $122 on a new Taos, $799 on 152 cars of 58 models at one store). Any non-NULL price counts as present, and nothing filters out low junk values before display.

The cars first seen in the last scan (09-28) are incomplete at 16.2%, against 2.4-5.9% for the cars first seen from 09-20 to 09-26. They make up 26% of all incomplete cars (item 2.23).

**Top three takeaways.**

1. **The index is stale, and nothing rebuilds it.** The last full rebuild was 2026-08-26 19:14:53 UTC. The fleet runs `scanner.py --scan-only`, which returns before the step that rebuilds the index, and the nightly job that also rebuilt it is disabled. So about 1,370 complete cars still carry the flag (in production that hides them from the grid by default and shows a false "Incomplete listing" banner), the 2026-09-28 mileage rule never reached the index, and 8,738 index rows (43%) belong to retired cars. See X1-X3.
2. **Real gaps are price, trim and exterior color, and they cluster by dealer.** These three fields are about three quarters of all real missing-field codes (over 80% if the 0-mile mileage codes are left out). 31 dealers hold half of all incomplete cars and 125 hold 80%. The main causes are page-level recovery that never reaches the car (Dealer Inspire hosts throttle us, the 800-page cap cuts the tail, some feeds carry no detail URL, guessed URLs 404 or land on the wrong page), feeds that never carry the field, and new arrivals that the dealer has not priced yet. A few dealer-specific defects account for about 1,630 rows on their own: Gardena Honda (630), Metro Honda order-number rows (391), Old Orchard Nissan two-month-old rows (236), Toyota Carlsbad's capped tail (213), Hendrick Lexus Charlotte (162). Five more stores are at 25-43% incomplete and are not explained by any item above (357 cars, item 2.25).
3. **The recovery machinery is mostly idle or aimed at the wrong rows.** No scheduled off-scan recovery has run since 2026-08-05. Inline auto-heal targets fields whose feed coverage is low rather than cars that are incomplete: 23,807 VINs targeted, at most 530 rows filled, and 86% of price gaps never targeted. `recover_incomplete_listings.py` has never written a row. The VDP prefetch takes candidates in feed order, with no priority for cars that have gaps.

**Freshness caveat.** The local DB's newest active `scraped_at` is 2026-09-28T22:59Z. Prod ran its last fleet on 2026-09-29 and nothing has scanned since, so prod's per-car index syncs from that run may have fixed some rows locally reported as stale (notably the mileage codes). Prod's index and Railway's environment were not inspected. Several fixes landed in code after this data and only take effect on the next scan or rebuild: F02 coverage rule (988f21902), F12 mileage rule (20a342de5), F13 not-a-fuel rule (95849d008), the `pages_needed` prefetch hint (87a3ff0f9).

---

## 2. The list

### 2.0 Ranking

Ranked by active listings affected, weighted by user impact (price, photos and trim count more than color or drivetrain). Counts are the corrected ones.

| Rank | ID | Item | Active listings affected | User impact | Plan coverage |
|---|---|---|---|---|---|
| 1 | X1 | Index never rebuilt; complete cars stay flagged | about 1,370 | High: hidden in prod, false banner | Not covered |
| 2 | 2.1 | Price missing | 4,676 | High | Partly (P12A.5; hint shipped, needs Phase 6 scans) |
| 3 | 2.2 | Trim missing | 3,545 | High | Partly (P8B.6) |
| 4 | 2.4 | No photos (not counted as incomplete); placeholder-only photos counted as photos | 5,394 with none; about 3,000 placeholder-only | High | Not covered |
| 5 | 2.3 | Body style holds a category or trim string (counted complete) | about 23,900 | Medium | Not covered |
| 6 | 2.5 | Exterior color missing | 2,469 | Medium | Partly (P0B.1 measures; P12A.5) |
| 7 | 2.22 | Interior color blanks at dealers that publish it (excluded from the rule) | about 5,100 | Medium-low | Not covered |
| 8 | 2.6 | VDP prefetch budget, order, throttling | up to about 3,000 caused; 582 proven | Cause of 2.1, 2.4, 2.5 | Partly (P12A.5) |
| 9 | 2.23 | First-scan arrivals 3x as likely to be incomplete | 3,048 (26% of incomplete) | Pattern (cause of 2.1, 2.5) | Partly (Phase 6B resumes scans) |
| 10 | 2.9 | Gardena Honda: every car incomplete | 630 | High (one store) | Not covered |
| 11 | 2.21 | Junk prices under $1,000 counted complete | 357 (+4 junk prices over $1M) | High (wrong price shown) | Not covered |
| 12 | 2.8 | Guessed Dealer.com VDP URLs | about 1,360 incomplete; about 1,690 dead dealer links | Medium-high | Not covered |
| 13 | 2.7 | Used cars stored at 0 mi | 1,256 | Medium | Not covered |
| 14 | 2.10 | Metro Honda order-number "VIN" rows | 391 | Medium | Not covered |
| 15 | 2.25 | High-rate dealers not explained by other items | 357 at 5 dealers | Medium | Not covered |
| 16 | 2.11 | Stale active rows kept by reconcile guards | about 1,024 | Medium | Partly (Phase 5A/5B, P6B.6) |
| 17 | F1 | Mercedes trim in the model name | 577 | Medium (false flag) | Not covered |
| 18 | 2.12 | Engine residual | 438 | Medium | Partly (P8A.5, P8B.4, P10B.5) |
| 19 | 2.13 | Fuel type junk or blank | about 460 fixable | Medium | Partly (P8A.7, P8B.1) |
| 20 | 2.14 | Drivetrain and transmission (HD trucks, undecoded VINs) | about 440 | Low-medium | Partly (P8A.6, P8B.4) |
| 21 | 2.16 | Auto-heal aims at feed coverage, not gaps | 4,044 price gaps never targeted | Machinery | Not covered |
| 22 | 2.15 | No off-scan recovery since 2026-08-05 | all 11.6k | Machinery | Partly (P0B.5, P18A.5) |
| 23 | M1 | Trimless check hides real trim gaps | about 201 | Medium (missed flag) | Not covered |
| 24 | M2 | Non-VIN identifiers never flagged | about 585 | Low | Not covered |
| 25 | F4 | MY2026 BMW "used" at 0 mi | 473 | Low (ambiguous) | Not covered |
| 26 | F5 | Old, high-mileage used cars without price | 523 | Low (unverified) | Not covered |
| 27 | X3 | Retired cars in the index | 0 active (admin views) | Admin | Partly (P5A.4, P5B.3 touch the writers) |
| 28 | 2.17 | VIN decode coverage gaps | 3,497 undecoded; 704 partial | Cause of 2.12-2.14 | Partly (P8A.5, P8B.3, P8B.4) |
| 29 | F3 | Trailers, motorcycles, carts, boats judged as cars | 32 | Low | Not covered |
| 30 | 2.19 | Recovery writes without provenance | up to 273 | Low (audit) | Partly (P8A.3, P8B.9) |
| 31 | 2.18 | `recover_incomplete_listings.py` and `recovery_*` columns dead | 0 written | Low | Not covered |
| 32 | M3 | CTP condition outside the mileage rule | 76 | Low | Not covered |
| 33 | M5 | Wrong electrification on complete cars | 32 BEVs; 1,409 PHEVs | Medium (wrong, not missing) | Partly (P8A.7) |
| 34 | 2.24 | Smaller gaps: blank make/model with a clean decode; implausible odometers | 18; 26 | Low | Not covered |
| 35 | 2.20 | Pre-F02 capture coverage reported price 100% | reporting only | Low | Not covered |
| 36 | X4 | Per-car index sync cost | 0 | Scanner CPU | Covered (P11B.2) |
| 37 | X5 | Full-rebuild hazards (memory, races, boot) | 0 (latent) | Latent | Partly (P13A.5) |

---

### 2.1 Price missing (4,676 active)

**Affected.** 4,676 active cars have both `price` and `msrp` empty. The index matches exactly: every one is flagged, and no flagged car has a price. Used and certified cars are missing price at 4.5% (3,223 of 72,224), new cars at 1.0% (1,453 of 142,451).

**Pattern (data says).** Buckets, using each dealer's last scan and its newest write batch:

| Bucket | Rows | Dealers |
|---|---|---|
| Feed priced under 99% of rows, row written by the last run | 3,790 | 175 |
| -- VDP fetched for every candidate, still no price | 1,166 | 69 |
| -- VDP cap of 800 reached (all 800 fetched) | 1,199 | 10 |
| -- cap and 300 s wall clock hit | 224 | 6 |
| -- partly fetched (wall clock or slow host) | 425 | 39 |
| -- host exhausted by 403/429, 0 pages fetched | 715 | 44 |
| -- no detail URL at prefetch time, 0 candidates | 61 | 7 |
| Non-VIN identifier rows (391 at mymetrohonda-com, item 2.10) | 394 | 4 |
| Stale rows not in the last run (236 at oldorchardnissan-com, item 2.11) | 298 | 8 |
| Feed at least 99% priced, row still NULL | 172 | - |
| Last run wrote 0 rows / no run | 19 / 3 | 4 / - |

Other signals:
- New arrivals: 3,180 of the 3,223 unpriced used cars were first seen within 7 days of their last scrape. Cars that arrived in the last scan are unpriced at 15.2%, against 2.1% for carried-over cars. Many are "not priced yet".
- At capped dealers the VDP does carry price: toyotacarlsbad-com prices 942 of 1,155 rows from the VDP although its feed carries 0% price; bmwofmonrovia-net goes from 1,280 feed-unpriced to 36 NULL; crownlexus-com from 1,140 to 2.
- DealerOn cosmos stores leave recent, low-mileage used cars unpriced at 6.9%, against 0.5-2.2% on other platforms (covertfordaustin-com 217 of 1,220 used; fordofmurfreesboro-com 180 of 1,110). Platform triage, not "call for price", until checked.
- The description holds a dollar amount on only 79 of the 4,676, so text recovery is not viable. `price_provenance_json` is NULL on all of them.
- Top 10 dealers hold 2,084 (45%): mymetrohonda-com 392, oldorchardnissan-com 237, fordofmurfreesboro-com 225, covertfordaustin-com 222, toyotacarlsbad-com 213, markkia-com 208, autoboutiqueohio-com 207, ...

**Root cause (code says).**
- Parsers correctly emit 0 for masked prices (`backend/parsers/dealer_dot_com.py:91-145` floor 1000; `backend/parsers/overfuel.py:60-64`; `backend/parsers/carscommerce.py:31-34`, `:111-120`), and `coerce_price` stores NULL (`backend/scanner/upsert/rows.py:92-100`).
- The only page-level fill that runs in the fleet is the HTTP-first VDP prefetch, which is budget-bound and takes candidates in feed order (item 2.6). Price is never carried forward from the DB (`backend/scanner/vdp/prefetch.py:1-9`, `:30-46`), so a throttled run cannot fill it.
- The cap now widens itself from `scan_hints.timing.pages_needed` (`prefetch.py:148-166`, commit 87a3ff0f9). 80 dealers already hold a hint above 800 (toyotacarlsbad-com 1,155, covertfordaustin-com 2,053). The 09-28 runs predate it, and nothing has scanned since.
- The upsert never clears a price (`backend/scanner/upsert/sql.py:48`), so an unpriced row was never priced in any scan. The flip side: a price the dealer withdraws stays on our page.

**Examples.** 1461257 `2T36CRAV1TW41D467` toyotacarlsbad-com (new, in the 355-row cap tail, placeholder image); 964580 `5XYK6CAF7PG137744` markkia-com (certified; recipe price coverage 0.62; the VDP was fetched); 1161234 `5NPEG4JA0MH105291` autoboutiqueohio-com (overfuel, 207 of 207 still NULL after fetches); 1106801 `1FTFW3L82TKD56857` kingfordsales-com.

**Suggested fix.**
1. Rank prefetch candidates by completeness code, price first (item 2.6).
2. Resume scanning so the `pages_needed` hint takes effect (Toyota Carlsbad clears itself).
3. Add a "price hidden at source" state only where the VDP was fetched and had no price, and only after two scans in a row. Do not show "Call for price" at dealers whose VDPs carry prices.
4. Check the DealerOn cosmos VDP price parse on covertfordaustin-com and fordofmurfreesboro-com.
5. Retire the Metro Honda order-number rows (2.10) and fix Old Orchard's recipe (2.11).

**Plan coverage.** Partly. P12A.5 moves the VDP prefetch onto the shared fetch policy but leaves budget and order alone. Phase 6B resumes scanning, which activates the shipped cap hint. Gap-first ordering and a hidden-price state are not covered.

---

### 2.2 Trim missing (3,545 active)

**Affected.** 3,545 active cars, all with `trim` empty; a recompute of 400 of them keeps the flag on all 400. About 577 are a display false positive (F1). The rest are real under the current rule.

**Pattern (data says).**
- By make: Mercedes-Benz 858, BMW 700, Honda 605 (512 of them at gardenahonda-com), Lexus 315 ("Lexus" 158 + "LEXUS" 157).
- The feed row carries no trim: 3,147 titles are exactly "year make model", and the 108 with extra words add body, cab or engine text, not a trim.
- Mercedes is recent and concentrated. 774 of 858 Mercedes trim gaps were first seen on or after 09-24; the blank-trim rate rose from 2.6% to 9.4%. wisimonson-net (184), mbmanhattan-com (167) and mercedesdallas-com (163) hold 514. At mercedesdallas-com, older RWD C 300 rows carry trim "Sedan" while those first seen 09-26 onward are blank. That points to a capture change in the late-September HTTP scans.
- Lexus gaps are real: 282 of 315 are used or certified cars missing the grade (Premium, Luxury, F SPORT) that other Lexus rows carry.
- vPIC cannot fill most of them. The corroboration guard rejects 1,989 decoded trims as not named in the listing and 115 as multi-value lists. Of the ones it would pass, 256 equal the model name, 46 are lists such as "Macan, Macan T", and the 135 "clean" ones are mostly fragments such as "350" on an RX 350. Genuinely recoverable: about 0-19.
- If the structured backfill ran on trim-only gaps today, tier 1 would write a junk title-derived trim ("Sedan", "SUV") into 288 rows, mostly at mercedesdallas-com (163) and mercedesfortworth-com (58).

**Root cause (code says).**
- A blank trim is missing unless `catalog_model_is_trimless` finds every catalog trim equal to the model (`backend/utils/listing_completeness.py:199-214`, `backend/enrichment/knowledge_engine.py:23-57`). Aliasing the dealer model to the catalog family would exempt only about 3 rows, because the catalog lists several trims per family.
- BMW looks better only because `apply_bmw_model_trim_display` splits "330i" into model and trim (`backend/utils/car_serialize/bmw.py:181-227`, called at `serialize.py:94`). Mercedes has no equivalent.
- The vPIC trim guard (`backend/enrichment/spec_structured_backfill.py:66-86`) is deliberate (audit A.1 #9: 2,989 wrong trims came from vPIC). Its two-component rule (`:79-85`) still lets an enumeration through when one component is in the title.

**Examples.** 979646 `W1NKM4GB8TF497207` mercedesbenzgreenway-com "2026 Mercedes-Benz GLC" (real gap); 1101129 `2T2HZMAA4MC204718` crownlexus-com 2021 RX 350 (grade missing); 908398 `1HGCY1F28TA061052` gardenahonda-com Accord Sedan (vPIC "LX", blocked); 1042935 `WP1AA2A58SLB15128` infinitiofcharlotte-com (guard would accept "Macan, Macan T").

**Suggested fix.**
1. Mercedes-only designation split (F1).
2. Compare August and late-September payloads at the three Mercedes stores to find why trim stopped arriving.
3. Gardena Honda separately (2.9).
4. Fix the tier-1 junk trim and reject two-value lists before running the backfill on trim-only gaps.
5. Owner decision: may a single-valued vPIC trim fill on feeds known to carry no trim at all (Gardena)?

**Plan coverage.** Partly. P8B.6 ("honest tier-1 labels") covers the junk tier-1 trim. The Mercedes split and the late-September capture change are not covered.

---

### 2.3 Body style holds a category, size class or trim string (about 23,900 active, counted complete)

**Affected.** Users see a non-canonical body style on 28,508 active cars (13.3%), and 26,902 of those are not in the index. 23,905 hold a trim string or a size or category label. The other 4,603 hold real but un-normalized cab or van labels.

**Pattern (data says), after the serializer's own normalization:**
- Category or size class: 14,319 cars at 188 dealers. "Cars" alone is 10,506 rows across makes (Toyota 2,622, Honda 1,853, BMW 1,375, Ford 788, Mercedes 763), mostly new. This looks like a feed's vehicle-category field (Cars/Trucks/SUVs/Vans) mapped into body style, and it is sometimes wrong: a Subaru Crosstrek (SUV) shows "Cars". Others: "Midsize Cars" 1,262, "Compact" 1,099, "Compact Cars" 631.
- Copy of the trim: 4,773 cars at 69 dealers ("LE" 645, "SE" 431, "LIMITED CREWMAX 5.5" 268). Top stores: avondaletoyota-com 731, toyotacarlsbad-com 495, toyotaescondido-com 336, autonationtoyotacerritos-com 336, hughwhitehonda-com 295.
- Drive, seat or other trim text: 4,813 at 168 dealers ("HYBRID AWD" 573, "PLUG-IN HYBRID AWD" 458, "5 seats" 439, "TX 350 AWD" 233, "7 PASSENGER" 214).
- Cab and van labels left un-normalized: 4,603 ("Vans" 2,153, "CrewMax" 859, "4D SuperCrew" 437).
- EPA classes such as "Small Sport Utility Vehicle 4WD" (2,684 cars) already display correctly as SUV or Truck.

**Root cause (code says).** Completeness only checks that body style is non-empty (`backend/utils/listing_completeness.py:224-225`), and `coerce_body_style_stored` returns unknown strings unchanged (`backend/utils/field_clean.py:286-333`). The body filter expands through `body_styles_for_filter` (`field_clean.py:360-376`, used at `hybrid_search.py:193-203`), which never includes "Cars" or "LE", so these cars probably drop out of body-type filtering. That last point is from the code; it was not tested end to end.

**Examples.** 1194199 `5TDCSKFC6TS291996` autonationtoyotairvine-com Sienna "WOODLAND EDITION"; 1144703 `5TDAAAB66TS088238` lexusescondido-com TX 350 "TX 350 AWD"; 1259189 `5TFWA5DB7TX39B955` avondaletoyota-com Tundra "LIMITED CREWMAX 5.5"; 1022211 `4S4GUHD69T3823704` austinsubaru-com Crosstrek "Cars".

**Suggested fix.** Extend `coerce_body_style_stored` to a canonical vocabulary. When the value is not canonical, derive the body from vPIC BodyClass or the catalog and keep the raw text as provenance. Count a non-canonical body as missing. Fix the feed mapping where a category field lands in body style.

**Plan coverage.** Not covered.

---

### 2.4 No photos (5,394 active, deliberately not counted)

**Affected.** 5,394 active cars have no usable image URL: 3,707 new cars at 70 dealers and 1,687 used or certified cars at 83 dealers. None is in the index.

**Pattern (data says).** Concentrated: toyotaofriverside-com 549 of 1,354, claremonttoyota-com 429 of 1,030, oldorchardnissan-com 402 of 402 (the whole lot), righttoyota-com 390 of 1,198, righthonda-com 251 of 556, markkia-com 222 of 1,022, toyotacarlsbad-com 213 of 1,155, infinitiofcharlotte-com 183 of 2,518. At markkia-com and toyotacarlsbad-com these are exactly the rows the 09-28 VDP prefetch never reached (item 2.6), so the photos would have come from the detail page.

**Root cause (code says).** "images" is in `_NON_ACTIONABLE` (`backend/utils/listing_completeness.py:259-261`); the comment says photo-less cars are sorted to the bottom and that scanner photo loss is "tracked per-dealer". This review did not find that per-dealer signal in the pipeline's assess step.

**Suggested fix.** Keep photos out of the per-car pill if the owner prefers. Add a per-dealer photo-coverage alarm to the dealer_pipeline assess step that counts placeholder-only rows as photo-less (see the note below). Put photo-less rows into the gap-first prefetch ordering (item 2.6).

**Plan coverage.** Not covered.

Note: counted in this write-up with one SQL (Appendix B.9) that approximates `_has_http_image`; it reproduces the earlier pass's 5,394 exactly.

**Placeholder-only photos are counted as photos (single pass, Appendix B.14).** 5,394 is a floor. 9,856 more active cars have an http main image that 10 or more active cars share, and no gallery image used by fewer than 5 of them, so they show no photo of the car itself. Split by the main image URL:
- 3,032 placeholders at 87 dealers: "photo_unavailable_640.png", "unavailable_stockphoto.png" (545 cars at 47 Dealer.com stores), "coming soon" banners, Chapman "default.jpg". 684 of them are in the index for other codes.
- 3,168 OEM stock renders, nearly all on new cars. These are arguably acceptable.
- 3,656 other shared images: per-dealer banners and color-matched OEM art.

Top placeholder dealers: chapmanfordaz-com 276, mymetrohonda-com 235 (the order-number rows of 2.10), fordofmurfreesboro-com 223, koonstoyotatysonscorner-com 158, tedrussellford-net 139, bentleygmc-com 118, covertfordaustin-com 118. At koonstoyotatysonscorner-com, all 158 placeholder rows are exactly its 158 eight-character "VIN" rows (M2), and every 17-character row has a real photo. That points to pre-arrival allocations, not a capture failure.

Code says:
- `_has_http_image` accepts any http URL (`listing_completeness.py:69-90`).
- The parsers' `_is_placeholder_url` (`backend/parsers/base.py:381-395`) matches "coming-soon", "placeholder", "no-image" and "nophoto". It does not match "photo_unavailable", "unavailable_stockphoto", "coming_soon", "comingsoon" or "default.jpg".
- 501 stored main images do contain a token it matches, so at least one write path skips the filter (not traced).

Examples: 1078973 `1FTEW2KP3TFC42955` chapmanfordaz-com; 1058058 `1FTFW1E88PFA37186` fordofmurfreesboro-com; 1082782 `TU36C999` koonstoyotatysonscorner-com.

Fix: one shared placeholder test, used by the parsers, completeness and the per-dealer photo alarm. It should also treat an image shared by many cars at one dealer as a placeholder.

---

### 2.5 Exterior color missing (2,469 active)

**Affected.** 2,469 active cars; `exterior_color` is NULL on all of them (no value was rejected by a normalizer), and a recompute of 155 keeps all 155 flagged. A color word appears in the description of only 101.

**Pattern (data says), by the dealer's last VDP prefetch outcome (exclusive buckets):**

| Cause | Rows | Dealers |
|---|---|---|
| No detail URL at prefetch time, 0 candidates (gardenahonda-com 618) | 657 | 8 |
| 0 pages fetched: host exhausted by 403/429, or wall clock | 566 | 46 |
| Some pages fetched, rest skipped at the cap | 448 | 24 |
| Stale rows not in the last run (item 2.11) | 437 | 29 |
| Fetched with no cap, still no color | 349 | - |
| Other | 12 | - |

- Throttling is the largest single cause: rows at dealers whose last run hit host exhaustion or the wall clock total 1,311 (53%), 792 of them at 73 Dealer Inspire stores whose recipe uses the CarsCommerce API (380 of the 437 stale rows are at throttled dealers too). Rows explained by the 800 cap alone, with no throttling, are only 288 at 9 dealers (toyotacarlsbad-com, bmwofmonrovia-net, fordofmurfreesboro-com, ...).
- CarsCommerce used cars miss color at 3.13%, against DDC 0.39% and 1.17% for all other platforms: about 490-680 excess rows. 435 of them also lack interior color, which suggests an empty `styles` block in the feed.
- hendricklexuscharlotte-com: all 162 new Lexus rows lack color (all 175 used and certified rows have it). They were written on 09-26 by a run that `scan_runs.md` does not log, and that run fetched 0 detail pages (host exhausted).

**Root cause (code says).**
- CarsCommerce color is read only from `styles[0]` (`backend/scanner/carscommerce_harvest.py:67-71`, `:129`).
- Dealer Inspire hosts are pre-marked slow (`backend/scanner/vdp/prefetch.py:717-734`), get a 60-page budget per run (`:120-128`; the docstring says 120), and are marked exhausted after 3 refusals (`:60`, `:662-671`).
- Prefetch runs before detail URLs are stamped (`backend/scanner/phases/dealer_run.py:112` vs `:117`), so feeds without a link (dealermasters, Chapman, Motive) get 0 candidates.

**Examples.** 920478 infinitiofcharlotte-com (0 of 800 fetched on 09-28, 2 of 800 on 09-24; 141 color gaps); 1205382 `2T2GGCEZ9TC133459` hendricklexuscharlotte-com; 908350 `19XFL2H87TE036500` gardenahonda-com; 1461289 `4T36CRAV6TU39G648` toyotacarlsbad-com.

**Suggested fix.** Gap-first prefetch ordering, with the slow-host budget spent on rows missing price or color first (item 2.6). Read `styles[1..]` and extra fields from the CarsCommerce payload before declaring a gap. Stamp detail URLs before the prefetch, or make those parsers emit them. Verify or retire stale rows rather than recapturing them.

**Plan coverage.** Partly. P0B.1 measures Dealer Inspire hosts at fleet concurrency; P12A.5 moves the prefetch onto one fetch policy but keeps slow-host serialization unchanged. Budget and gap-first order are not covered.

---

### 2.6 The VDP prefetch never reaches many gap rows (up to about 3,000 caused; 582 proven)

**Affected.** 228 dealers' last prefetch stopped early: 166 throttled, 62 cut by the cap or wall clock. They hold 7,339 of the 11,592 incomplete cars, but they also hold 66% of active cars, and their incomplete rate (4.84% throttled, 5.74% cap/wall) is close to that of dealers whose prefetch ran fully (5.33%). Excess over that baseline, capped at pages left unfetched, bounds the effect at about 3,013 cars. Rows proven to be in the unfetched tail (gap rows with no description at dealers whose descriptions come only from the VDP): 582.

**Pattern (data says).**
- Throttled dealers fetched 8,200 of 69,507 candidate pages (11.8%).
- On 09-28, 44 dealers skipped 11,294 candidates at the cap: 21 fetched all 800, 18 were cut by the 300 s wall clock, and a few fetched far fewer for other reasons.
- Clear cases: toyotacarlsbad-com (800 fetched, 355 skipped; all 213 price-and-color gaps are new inserts with no description), infinitiofcharlotte-com (0 of 800; 310 incomplete), mymetrohonda-com (49 of 547 fetched on 09-28 against 800 on 09-24), autoboutiqueohio-com (176 of 207 price gaps have no description and no Carfax link).
- Counter-examples that show fetching is not always the fix: markkia-com's 208 price gaps were all fetched; 166 of fordofmurfreesboro-com's 226 gaps carry the VDP's description.

**Root cause (code says).**
- Candidates are built in feed order and sliced `candidates[:cap]` (`backend/scanner/vdp/prefetch.py:586-600`).
- `_vehicle_wants_http_prefetch` also admits rows that need only a description, a thin gallery or extras (`:293-315`). `_extras_missing` fires when `carfax_url` is empty, which is almost every new car, so new cars are permanent candidates and use up the budget.
- Price is never carried from the DB (`:1-9`, `:30-46`), so the docstring's claim that the slow-host budget converges across runs (`:120-129`) does not hold for price.
- When retries end in exhaustion, `statuses` is empty, so the 403s do not show in the run stats.

**Suggested fix.** Order candidates by completeness code: price, exterior color, trim and photos first; description, gallery and extras last. Do not count a missing Carfax link on new cars. Keep a per-VIN attempt ledger so successive runs cover the tail. Record `candidates_with_gap` and 403/429 counts in `summary_json`. For Dealer Inspire hosts, either an impersonated TLS client through `net/client` or a feed-side source.

**Plan coverage.** Partly: P12A.5 (fetch policy), P0B.1 (measurement), Phase 6B (scans resume, so the cap hint applies). Gap-first order and the attempt ledger are not covered.

---

### 2.7 Used cars stored at 0 miles (1,256 active)

**Affected.** 1,256 active used or certified cars have `mileage = 0` (none NULL), at 86 dealers. Today's code counts all of them as missing mileage; the index carries the code on none (see X2). Users do not see "0 mi": since 8851754cf the serializer, car page and grid card show "Mileage not listed" (`backend/utils/car_serialize/api_identity.py:108-112`, `frontend/static/listings/card.js:24-31`). The harm is the queue, the pill and the filter.

**Pattern (data says).**
- Clear capture failures: crownlexus-com 326 (303 of them MY2024 or older; the F12 commit cites a VDP showing 10,914 mi), gardenahonda-com 117 of 117 used, integrityofchattanooga-com 98 of 98. At crownlexus-com all 326 zero-mile rows have `is_cpo` NULL while every used row with a CPO flag has mileage, which suggests a second feed path that reads neither.
- Ambiguous: 473 MY2026-27 BMWs at four stores, filed under used by the dealers' own URLs (F4).
- 665 of the 1,256 are MY2025 or older and implausible at 0 mi (e.g. 159518 1993 DeVille at lbmboaz-com).

**Root cause (code says).** `_mileage_blank` treats 0 on a used or certified row as missing (`backend/utils/listing_completeness.py:126-141`, F12, commit 20a342de5, 2026-09-28 18:19 UTC). F12 also made the parsers return None and the upsert replace a stale 0 with NULL, but all 1,256 rows were scraped before it landed, and a rescan that finds no odometer still flags (NULL is missing too). The pipeline coverage gate does not measure mileage (`backend/scanner/pipeline/constants.py:25-27`).

**Examples.** 1279300 `7SAYGDEF5NF494385` crownlexus-com; 1142097 `19VDE1F7XEE014556` gardenahonda-com 2014 Acura ILX; 1173612 `5UX13EU01T9113960` bmwofaustin-com MY2026 "Pre-Owned"; 96912 `WBA43FJ03TCW78042` hendrickbmwnorthlake-com.

**Suggested fix.** Rebuild the index (X1/X2). Add mileage to the coverage gate for used rows. VDP odometer gap fill at crownlexus-com. Decide F4 before the rebuild. Add "ctp" to the used tokens (M3).

**Plan coverage.** Not covered.

---

### 2.8 Guessed Dealer.com VDP URLs (about 1,360 incomplete at affected hosts)

**Affected.** 20,111 active cars carry a synthesized `/{new|used|certified}-inventory/vin-<VIN>.htm` URL, and 89% of them are complete: on real Dealer.com hosts the guess mostly works (09-28 prefetch: 24,859 x 200 against 353 x 404). The harm is on hosts where the template is wrong, about 1,363 incomplete cars:

- **Dead URL (404/403):** gardenahonda-com 623 (392 "impersonation exhausted ... 404" lines in a 4 min 45 s auto-heal window, 0 patched), autocollectionofmurfreesboro-com 109 (its feed is `isapi_xml.php`, mislabelled dealer_dot_com; 0 of 176 prefetched), 9 Algolia stores 125 (0 of 2,365 fetched, all 404; mbofbrooklyn-com 598 x 404), lexusofsilverspring-com 35, northparklexus-com 9, dreyerreinboldbmwnorth-com 6, carvision-com 3, milehighhonda-com 1, deluz-chevrolet-com 1 (158 x 404).
- **Wrong page (200 on a landing page):** Dealer eProcess, 444 incomplete at 19 dealers. The prefetch fills the car from the landing page: lexusofchattanooga-com has 278 rows sharing "New Lexus for Sale in Chattanooga, TN...", parksidekia-com has 292 cars across 17 models and 40 colors carrying one "2026 Kia K4 in White Pearl" description, pacificvolkswagen 191, bmwofbloomfield-com 137.
- **User-facing:** the guess is served as the "view at dealer" link (`backend/utils/car_serialize/api_dealer.py:16-26`), so dead-template hosts show broken links on every active car: gardenahonda 630, lexusofsilverspring 508, carvision 395, deluz-chevrolet 158.

**Root cause (code says).**
- `suggest_dealer_style_vdp_url` invents the path (`backend/parsers/vdp_urls.py:17-70`); `resolve_vehicle_source_url` / `apply_vehicle_source_url` (`:172-200`) apply it to every row from `backend/scanner/upsert/rows.py:38` and `phases/dealer_run_steps/enrich.py:266`. Nothing marks it as a guess.
- The Dealer eProcess scraper stores the real link under `_source_url` (`backend/scanner/scrapers/dealer_eprocess.py:640-641`, `:786-787`), which the resolver never reads (`vdp_urls.py:182-191`). Its parser maps no trim from the inline data (`backend/parsers/dealer_eprocess.py:262-280`).
- `_apply_page_fields` fills price, colors, specs, description and gallery from any 200 page with no VIN check (`backend/scanner/vdp/prefetch.py:404-447`). `incomplete_recovery.prepare_vdp_urls` adds up to 12 more guesses offline (`backend/utils/incomplete_recovery.py:387-399`).

**Examples.** 884987 `WBA63HZ00VCY72653` bmwofbloomfield-com (trim blank, description is SRP meta text); `3KMDA3DB9VE003504` parksidekia-com (an EV3 carrying the K4 description); 875970 autocollectionofmurfreesboro-com (no color, VDP 404 then host exhausted); `https://www.gardenahonda.com/new-inventory/vin-19XFL2H80TE044342.htm` (404).

**Suggested fix.** Have the resolver read `_source_url`, and have each parser emit its native link. Flag guessed URLs and validate them per host (store the 200/404 rate in `scan_hints`); skip unvalidated guesses in recovery and do not serve them as the dealer link. Refuse to apply page fields unless the page contains the row's VIN. Correct the wrong `provider_hint` labels.

**Plan coverage.** Not covered (P17B.2's platform registry is adjacent).

---

### 2.9 Gardena Honda: every active car is incomplete (630 active)

**Affected.** gardenahonda-com: 630 of 630 active cars are incomplete under today's code (623 in the index). Missing: exterior color 618, trim 565, mileage 117 (every used row is at 0 mi), price 1. That is 5.4% of the fleet's incomplete cars from one store. The index also holds 1,076 retired rows for this dealer.

**Root cause.**
- Code says: the Dealer Masters feed carries no trim, mileage, color or stock number (`backend/parsers/dealermasters.py:19-22`), and `_map` (`:84-107`) emits no detail URL. So the VDP prefetch had 0 candidates on both 09-26 and 09-28, and the upsert's guessed Dealer.com URL 404s (2.8).
- Data says: `dealer_recipes` holds 8 recipes (the static query plus 7 page-data routes mislabelled dealer_dot_com), all stamped `field_coverage` trim 0 and color 0. The stamps are unreliable: they also say price 0.0 while 629 of 630 rows are priced.
- vPIC has a trim for 568 of the 576 blank-trim rows (e.g. 1142098 "Sport", 947423 "SE"), but the guard refuses it because the listing never names the trim (2.2). Color has no vPIC source.

**Examples.** 1142098 `19XFL2H80TE044342` 2026 Civic Hatchback; 1142253 `1HGCY2F56TA068453` Accord Hybrid; 947423 `1HGCY1F48TA074305` Accord Sedan; 1015265 `5FNYF8H01PB033539` used 2023 Passport at 0 mi.

**Suggested fix.** Run dealer-discovery to learn the Dealer Masters per-vehicle route (Gatsby page-data or the HTML VDP), emit it as the source URL, and map trim, color, odometer and stock from it. Until then, mark the dealer thin by platform so the 404 prefetch is not retried every run. Owner decision on the vPIC trim policy for feeds known to carry no trim.

**Plan coverage.** Not covered (dealer-discovery workflow).

---

### 2.10 Metro Honda order-number "VIN" rows (391 incomplete of 410 active)

**Affected.** mymetrohonda-com has 410 active rows whose `vin` is a Honda order or allocation number (`A07-41293-00089`, `S06-41046-00005`, `08-41048-00066`), equal to `stock_number`. All were first seen and last scraped at 2026-09-24T14:05:15Z. Under today's code 391 are incomplete: price 391; model, engine, transmission and cylinders 111 each (the "7-field" set, model blank); body style and fuel type 126 each. The index says 404, with stale body and fuel codes. The same dealer's 547 real-VIN rows are nearly all complete (20 incomplete). 38 hybrids among the order-number rows are stored as "Regular Gasoline".

**Why they are still listed (data says).** The 09-28 run (verdict ok, 547 rows) captures only 17-character VINs, so it could not see these rows. Reconcile refused: "547 rows < 60% of baseline 934" (`workspace/dealer_logs/mymetrohonda-com/scan_runs.md:26`), and the 934 includes these 410 rows. The per-bucket guard also refuses (new 353 < 0.6 x 744). Whether the units are still on the dealer site was not checked (no network); a 10-08 measurement counted 779 vehicles on the site against 957 active rows.

**Root cause (code says).**
- `backend/parsers/dealer_eprocess.py:171-172` takes `vehicleIdentificationNumber` or `mpn` and rejects only values shorter than 11 characters. The same length-only gate sits in `dealermasters.py:90`, `chapman.py:177`, `overfuel.py:107`, `team_velocity.py:194`, `motive_ridemotive.py:137`, `jazel.py:158`. No VIN-shape gate exists at upsert.
- Reconcile's baseline counts every active row scraped in 30 days (`backend/scanner/pipeline/run.py:56`, `:62-70`; share and floor at `reconcile.py:79-84`; per-bucket guard `:101-142`), and `inventory_reconcile.py:245` considers only 17-character VINs.

**Examples.** 994656 `A07-41293-00089` 2026 Civic Si (now missing price only); 994887 `08-41048-00066` 2025 Accord SE; 994776 `A07-41294-00014` (7-field set, model blank); 995023 `S06-41046-00005` 2025 Pilot.

**Suggested fix.** Gate on VIN shape at parse and upsert (`looks_like_real_vin`, `backend/parsers/vdp_urls.py:12-14`), exempting pre-1981 cars and non-road vehicles. Compute the reconcile baseline from valid-VIN rows only. Confirm on the site, then retire the 410 rows. This was logged as F17 on 2026-09-28 and is still open.

**Plan coverage.** Not covered. Phase 5A rewrites the retirement policy but not VIN-shape handling; P6A.3 names mymetrohonda only for the VIN guard.

---

### 2.11 Stale active rows kept alive by reconcile guards (about 1,024 incomplete)

**Affected.** 8,576 active rows at 23 dealers are older than their dealer's newest write batch. 1,171 are in the index (13.7%, against 5.1% for rows in the latest batch); about 1,024 are incomplete under today's code. 95% of the stale rows are 12 hours to 7 days old, which is the guard working as intended on partial runs. Four dealers (listed below) hold 930 of the 1,171 stale indexed rows (79%); outside the top three the stale and fresh rates are equal (4.91% vs 5.06%).

**The dealers (data says).**
- mymetrohonda-com 404: see 2.10.
- oldorchardnissan-com 236: 389 active rows last scraped 2026-08-05 (238 used, 235 with no price and no MSRP; all 402 active rows have no photo). The recipe is one filtered WordPress plugin call that returns 12 rows. The 09-26 reconcile retired 390 rows and the retirement was manually reverted (`workspace/dealer_logs/oldorchardnissan-com/scan_runs.md:24-27`). On 09-28 reconcile refused at the 20-row floor (`backend/scanner/pipeline/reconcile.py:79-81`); the 30-day baseline was 13.
- hendricklexuscharlotte-com 167: 162 new Lexus without color from a 09-26 write that `scan_runs.md` does not log; the 09-28 run returned 33 certified rows, verdict no_rows, so reconcile skipped (`reconcile.py:66-68`). These may be the real lot; the color gap is the issue (2.5).
- covertbuickgmc-com 123: 386 rows from 09-25/26 still active although run 1222's reconcile ran "ok".
- Also: mckennasubaru-com has 36 active rows last scraped 2026-07-21. Four active cars have a blank `dealer_id` and can never be reconciled: 992393, 1003650, 1009963, 949494 (three are in the index).

**Examples.** 945324 `19XFC2E55HE079850` oldorchardnissan-com (price, color); 945335 `WMZYT5C52K3G93558` oldorchardnissan-com 2019 MINI Countryman; 1205376 `2T2AGCEZ1TC132862` hendricklexuscharlotte-com.

**Suggested fix.** Fix Old Orchard's recipe through dealer-discovery (12 of about 400 cars captured). Keep a per-row last-seen ledger and send rows unseen for N accepted runs to VDP verification (404 retires, 200 refreshes). Two-strike retirement. Clean up blank `dealer_id` rows.

**Plan coverage.** Partly. Phase 5A (one retirement policy), P5B.4 (two-strike), P5B.5-P5B.6 (audit), P6B.6 (prod retirement audit), P12B.10 (rescan under-collected dealers). The last-seen ledger and VDP verification are not covered.

---

### 2.12 Engine residual (438 of 1,379 indexed)

**Affected.** A full recompute of all 1,379 active engine flags (re-run by the completeness review; the earlier "840 + 51 + 438" summed to 1,329, not 1,379):
- 885 now render an engine and have a cached vPIC decode.
- 51 now render "Electric", including all 29 Cadillac OPTIQ option-code rows ("X0D+ER3").
- 5 were filled by a later write.
- 438 are still missing.

Of the 438, 131 are non-VIN rows (111 at mymetrohonda-com) and 307 have a real VIN. About 7 of those are trailers or RVs, leaving about 300 real vehicles.

**Pattern (data says).**
- 2027 GLC 300 with dealer text "I-4 cyl": 124 rows at mercedesbenzgreenway-com (96) and mbofstevenscreek-com (28). Their vPIC rows are partial decodes (ErrorCode 5,14) fetched 07-18 to 08-05. The same VIN pattern decoded on 09-25 to 09-28 returns ErrorCode 0 with DisplacementL 2 for 508 VINs, so a re-decode would very likely fix them.
- 102 real VINs were never decoded (autonationhondafremont-com 2027 CR-V Hybrid 32; BMW 3MW.../3MF... M340 and M2).
- 269 of the 438 have a known cylinder count; 212 of those carry dealer engine text without liters (mostly "I-4 cyl", "I-6 cyl", "V-8 cyl", "T8").
- MY2027 catalog: 461 of 545 MY2027 `epa_master` rows are raw (blank trim, full EPA name in the model column), and a matching raw row blocks the prior-year fallback (`backend/catalog/resolver.py:150-162`): "GLC 300" links 227 of 231 in 2026 and 0 of 468 in 2027. The engine display reads the dictionary CSVs (`enrich_from_dictionary.py:171`), not the link, so this travels with the gap rather than causing it.

**Root cause (code says).** `build_engine_display` returns a dash whenever no displacement is found, even when the layout and cylinder count are known (`backend/utils/car_serialize/engine.py:659-668`); completeness then flags engine (`listing_completeness.py:218`). `decode_missing_vins` inserts only missing VINs with `ON CONFLICT DO NOTHING` (`backend/enrichment/vpic_facts.py:161-197`), and nothing checks ErrorCode, so a partial early decode is kept forever.

**Examples.** 934491 `W1NKM4GB9VU154744` mercedesbenzgreenway-com (ErrorCode 5,14); 1026998 `3MW69FT06T8G94604` bmwrockville-com M340 (no vPIC row); 1040928 `5J6RS5H56VL001314` autonationhondafremont-com CR-V Hybrid; 1157131 `YV4HD0RCXV1569439` howardorloffvolvocars-com XC60 "T8".

**Suggested fix.** Re-decode vPIC rows with ErrorCode not 0 for model years at least the current year minus 1 (704 active index rows; 124 GLC immediately). Decode the never-decoded VINs. Show a layout-only engine line ("I4") when cylinders are verified, and accept it as present (about 200 cars). Filter option-code engine text.

**Plan coverage.** Partly. P8A.5 adds a decode refresh by age (selection by ErrorCode is not stated), P8B.4 decodes on every scan path, P10B.5 relinks the 461 appended catalog rows. The layout-only display is not covered.

---

### 2.13 Fuel type junk or blank (about 460 fixable)

**Affected.** Under today's code about 630 active cars miss fuel type. The index shows 684, but most of those are stale and a different 381 are not flagged at all.

**Pattern (data says).**
- **381 injection-system values** ("Direct Injection" 361, "Port/Direct Injection" 20): righthonda-com 143, southbaybmw-com 132, irvinesubaru-com 43, hillcountryhonda-com 26, ourismanhonda-com 22, hanselbmwofsantarosa-com 15. Today's serializer blanks them, so all 381 will flag at the next rebuild (361 with fuel as their only gap). vPIC says Gasoline for 366; the other 15 are 2027 X5 partial decodes.
- **247 blank:** 79 have a usable vPIC fuel (72 Gasoline, 4 FFV, 3 mild hybrid), 39 of them used Fords at covertbuickgmc-com; 15 are trailers (vPIC "Not Applicable"); 153 have no decode.
- **403 stale flags** on "Regular Gasoline" / "Premium Gasoline", which now display as Gasoline. 38 of these are hybrids at mymetrohonda-com where the stored value is wrong, and the new display hides it.
- **"F"** is shown literally on 30 cars (hublerchevyauto-com 23, ocauto 7), several of them flex-fuel.

**Root cause (code says).** F13 (95849d008) rejects injection tokens at write time (`backend/utils/field_clean.py:223-228`), but the upsert keeps the old value whenever the new one is NULL (`backend/scanner/upsert/sql.py:62`), so the junk never clears. `vin_overrides` writes fuel only for electrified or diesel decodes, never plain Gasoline (`backend/enrichment/vpic_facts.py:76-85`), though the F13 comment expects a vPIC heal to fill these. The display reads only the dealer column plus a diesel or sticker override (`backend/utils/car_serialize/api_specs.py:39-42`, `engine.py:499-535`).

**Examples.** 219185 `5UX33EU00T9427446` southbaybmw-com "Direct Injection"; 1447429 `2HKRS4H78VH408704` righthonda-com; 999239 `1G1YB3D53V5102934` hublerchevyauto-com (blank, vPIC Gasoline); 1000408 `1FADP3K26GL202546` hublerchevyauto-com "F".

**Suggested fix.** One-time repair that sets the not-a-fuel values to NULL, then fill blank or non-fuel values from vPIC FuelTypePrimary plus ElectrificationLevel through the precedence helper, leaving mild hybrids alone. Map "F" and similar single letters. Fix the 38 mislabelled hybrids.

**Plan coverage.** Partly. P8A.7 heals PHEV fuel labels; P8B.1 stops the upsert reverting higher-ranked facts. A Gasoline fill and the junk repair are not covered.

---

### 2.14 Drivetrain and transmission (about 440 active)

**Affected.** Drivetrain: 307 flagged. 43 are stale and 16 are trailers, which leaves 248 by subtraction. The first pass's recompute gave 243, so a few trailers are probably among the stale rows; about 245 are live gaps. Transmission: 382 flagged and about 310 live. 111 of those are Metro Honda order-number rows (2.10), which leaves about 200 here. The "about 440" in the ranking is these two counts added (about 245 + about 200).

**Pattern (data says).**
- **HD pickups, chassis cabs and commercial vans** (Silverado/Sierra HD, Super Duty, Express/Savana cutaways, Low Cab Forward) are 5.4% of inventory but 133 of the current drivetrain gaps: 11.5 per 1,000 against 0.62 for other vehicles. Of the 133: 54 have vPIC "4x2", 46 were never decoded, 32 have a decode with a blank DriveType (incomplete vehicles). Six dealers hold 117: burnschevycadillac-com 45, selmanchevy-com 21, landersmclartychevrolet-com 21, hublerchevyauto-com 12, pugmirefordcartersville-com 9, jasonlewisautomotive-com 9.
- **80 HD transmission gaps** have a blank feed value, a blank vPIC TransmissionStyle and no catalog row; 62 are at howardbentleyauto-com (29, Sierra 2500 HD), chapmanfordaz-com (16, E-Series), romefordlinc-com (10), hendrickchryslerdodgejeepramofconcord-com (7, ProMaster).
- **Never decoded:** burnschevycadillac-com has only 22 of 464 active VINs in the vPIC cache.
- **Naming misses:** Mazda3 (EPA model "3") 16 and Kia EV3 (EPA "EV3 Long Range FWD") 10 have vPIC "4x2" and no catalog link.
- Earlier claims that these dealers capture 0% drivetrain are wrong: their capture is 78-98%.

**Root cause (code says).** vPIC "4x2" deliberately resolves to no drivetrain, to be broken by the dealer feed or the catalog (`backend/vehicle_facts/drivetrain.py:13-17`). The catalog tiebreak exists (`vpic_facts.py:333-344`, `api_specs.py:126-137`), but these rows have `epa_master_id` NULL, and EPA has no rows for vehicles over 8,500 lb GVWR.

**Examples.** 1053811 `1GB2ALE79TF340458` selmanchevy-com Silverado 2500 HD (4x2, GVWR 2H); 1124778 `1HA6GVC7XTN006631` landersmclartychevrolet-com Express Cutaway 4500 (blank DriveType); 1038515 `1GC4YNEY6PF180937` burnschevycadillac-com (never decoded); 1084438 `JM1BPABL4T1907219` mazdaoflodi-com Mazda3 (4x2).

**Suggested fix.** Decode burnschevycadillac-com's VINs. Add a model-family drivetrain and transmission table for HD and commercial vehicles that works when DriveType is blank, gated on GVWR and body class (Silverado/Sierra HD, Express/Savana, F-Super Duty, E-Series and Ram HD at 4x2 are RWD; ProMaster is FWD). Add catalog aliases for Mazda3 and EV3. Keep the owner rule that 4x2 is never mapped blindly.

**Plan coverage.** Partly. P8A.6 aligns read-time drivetrain (keeps 4x2 unmapped); P8B.4 decodes on every scan path. The HD family table is not covered.

---

### 2.15 No off-scan recovery since 2026-08-05 (machinery)

**Affected.** All 11,592 indexed cars; 11,529 of them miss a field the old `heal_from_recipes` could fill.

**Data says.**
- `~/Library/LaunchAgents/com.sarraficars.nightly-http-refresh.plist.disabled`; the live `nightly-data-quality` job only grades data.
- The last completed nightly ran 2026-08-03 (`workspace/scanlogs/nightly_20260803.log`): `heal_from_recipes` replayed 75,101 VINs at 209 dealers and patched 51 rows in 15,009 s (`:3659-3660`); `harvest_html_jsonld` had 22 of 22 candidates blocked (`:3670`); `harvest_carscommerce` patched 24 (`:1496`). A second run that began the same day never finished (its delta step took 25.2 h; about 278 rows patched across 191 dealers) and blocked the 08-04 and 08-05 starts.
- One-shot scripts ran through 08-08, including `heal_matcher_trims` on 08-08, which set 1,710 trims to NULL; 273 of the 295 active incomplete cars it touched now lack trim.
- What still runs in the fleet: the VDP prefetch, inline auto-heal (gap fill including the window sticker; `listing_gap_fill` stamps on 729 active cars from 09-24 to 09-28), the write-time structured backfill, and the pipeline's vPIC heal.

**Code says.** dealer_pipeline runs `scanner.py --scan-only` (`backend/scanner/pipeline/runner.py:115`). Scan-only turns off post-repair, window sticker, gap fill, dictionary enrich, enrich and vPIC (`backend/scanner/scan_efficiency.py:81-101`; `cli.py:452-454`), and the orchestrator returns before the post-scan tail (`orchestrator.py:462-465`). `fleet_scan.py:354-362` adds only market stats and grid cards. The recovery scripts are wired only into `deploy/nightly_http_refresh.sh:168-185`.

**Suggested fix.** Add an explicit recover phase to `fleet_scan` after the shards: a paced, host-grouped gap fill over the index's active VINs, gap-first; `heal_from_recipes` only for dealers with price or trim gaps and with a yield gate (about 330 rows in about 18 hours was not worth a fleet replay); then the index rebuild. Record per-phase yield in `fleet_summary.json`.

**Plan coverage.** Partly. P0B.5 measures each retired nightly step; P18A.5 retires the nightly and requires a capability table mapping every old step to a replacement; P8B.3 and P8B.4 cover the decode. No recover phase is planned.

---

### 2.16 Auto-heal aims at feed coverage, not at incomplete cars (machinery)

**Affected.** 4,044 of 4,676 price gaps (at 244 dealers) and 1,574 of 2,469 color gaps (at 162 dealers) sit at dealers whose feed coverage is above the 60% trigger, so auto-heal never targets them.

**Data says (09-28 fleet logs).** 64 dealer runs triggered. 23,807 VINs were targeted, but 6,118 were dropped by the 400-VIN cap, 1,339 belonged to runs that never finished and 685 were double counted across shard logs. The completed runs worked 16,350 VINs and patched 199 rows, plus at most 331 structured backfills: 1.2-3.2%. Triggers: transmission 30, body style 29, fuel 12, price 6, interior color 6, exterior color 5. The targets were mostly complete already: Longo Toyota was targeted on body style (1,242 VINs) with 3 cars flagged; Mountain States Toyota on transmission (962 VINs) with 2 flagged, because the write-time backfill and earlier scans had already filled them. Audi Bellevue's price trigger was false by construction: the feed check counts only `price`, completeness also accepts MSRP, and 29 of 30 had an MSRP. Auto-heal stats are not saved to `scan_runs.summary_json`.

**Code says.** Coverage comes from the in-memory feed rows (`backend/scanner/phases/dealer_run_steps/after_write.py:18-35`; `backend/scanner/post_scan/auto_heal.py:24-31`, `:69-77`). The cap is 400 (`gap_fill.py:43-48`, `:432`). The page-fetch set excludes trim, engine and model (`gap_fill.py:489-492`).

**Suggested fix.** Select the dealer's VINs whose index codes intersect what gap fill can fill, and trigger on a count, not a lot-wide percentage. Add a trim page parse (the prefetch already extracts trim). Persist the stats.

**Plan coverage.** Not covered (P8B.9 changes only the gap-fill write path).

---

### 2.17 VIN decode coverage (cause of 2.12-2.14)

**Data says.**
- 10,751 of 11,592 active index rows have a cached decode. 704 active index rows carry a partial decode (657 are MY2026 or later; the oldest was fetched 2026-07-05).
- 3,497 active real-VIN MY2026-27 cars have no decode at all (2,477 MY2026, 1,020 MY2027).
- A dry run of the real `heal_rows` still has 60 body-style writes pending (39 clear a flagged gap, e.g. 1032865 `SB1ADADE8TE004281` groovetoyota-com, 1033973 `1HTKJPVM2KH217437` covertfordaustin-com). An earlier claim that the heal was fully applied came from a harness bug.
- 16,923 Toyota/Lexus VINs with letters in serial positions 12-17 decode with ErrorCode "1,400" and are used anyway; they pass the ISO check digit and decode DriveType and Model correctly, so this is low risk.
- `nhtsa_vpic_cache.fetched_at` mixes two text formats; compare it only after a `::timestamptz` cast.

**Code says.** `lookup_vpic_from_cache` remembers an empty decode for the life of the process (`backend/enrichment/knowledge_engine.py:1284-1295`), cleared only by `heal_rows` (`vpic_facts.py:307`).

**Suggested fix.** ErrorCode-aware re-decode on a schedule; decode on every scan path; re-sync the index after the heal (X1).

**Plan coverage.** Partly: P8A.5 (refresh by age), P8B.3 (per-batch heal), P8B.4 (inline decode).

---

### 2.18 `recover_incomplete_listings.py` and the `recovery_*` columns are dead

**Data says.** `recovery_status`, `recovery_attempted_at`, `recovery_source`, `recovery_notes`, `missing_field_count` and `recoverability_score` are NULL on all 357,149 rows, active and retired, and in the legacy SQLite files too. `data_quality_score` is set on every active car (average 85.7 indexed, 94.3 not).

**Code says.** Nothing calls the script. Run by path it fails at import (`ROOT = parents[1]` is `backend/`, `recover_incomplete_listings.py:24-30`); `python -m backend.scripts.recover_incomplete_listings` works. It selects `SELECT * FROM cars` with no active filter and runs the full detail context per car (`:87-112`), about 24 ms x 357k, or 2.4 hours before choosing a candidate. Its own field list omits price and trim (`backend/utils/incomplete_recovery.py:28-39`). The Dealer.com inventory merge overwrites a stored value with any longer string (`:209-215`), with no vPIC guard in `update_car_row_partial`. The VDP step was already ported to HTTP on 09-26. Five of the six columns are redacted from public JSON (`car_serialize/_common.py:24-36`); `recovery_attempted_at` passes through as a dash. Already noted in `DATA_COMPLETENESS_FINDINGS_2026_09_28.md:192`, `:202`.

**Suggested fix.** Retire the script, or rebuild its selection on `incomplete_listings JOIN cars` (active), fill blanks only, write with provenance, and reuse `recovery_attempted_at` as a per-car attempt ledger so known-dead hosts stop being retried.

**Plan coverage.** Not covered.

---

### 2.19 Recovery writes without provenance (up to 273 active)

**Data says.** No `spec_source_json` entry names a recipe, harvest, JSON-LD or web-search source. From 07-16 to 08-05, `heal_from_recipes` patched 2,518 rows (transmission 1,706, price 575) and `harvest_carscommerce` 2,462 (colors 2,182, photos 842); none can be traced now. 273 active cars carry a `listing_gap_fill` stamp on a field that DuckDuckGo could fill. All 11 logged gap-fill runs report `ddg_patch_fields: 0`, so DuckDuckGo writes are a risk, not an observed problem.

**Code says.** Raw `UPDATE cars` at `heal_from_recipes.py:235`, `harvest_carscommerce.py:100`, `harvest_html_jsonld.py:227` skip `update_car_row_partial`'s coercion and guards (`cars_repo.py:702-736`) and write no provenance. Gap fill stamps DuckDuckGo values as `listing_gap_fill` with the VDP URL (`gap_fill.py:485`, `:557-572`, `:586-592`). `--dry-run` can still mark shared recipes stale or persist coverage (`recipes.py:1602`, `:1671-1677`).

**Suggested fix.** One apply helper with source labels (recipe_heal, carscommerce, html_jsonld, web_search). Make dry runs side-effect free.

**Plan coverage.** Partly. P8A.3 builds the apply helper (its source list already names recipe_heal and html_jsonld) and P8B.9 tags web_search, but no unit routes these three scripts through it. P1B.5 keeps `heal_from_recipes` persisting on purpose.

---

### 2.20 Pre-F02 capture coverage reported price 100% (reporting only)

**Data says.** 132 dealers' latest runs report capture price coverage of at least 99% while writing 1,877 active price-less rows (692 newly inserted). Example: toyotacarlsbad-com run 1552 reports capture 1.0 next to recipe coverage 0.0. `dealer_logs` copy the inflated figure ("coverage before upsert: price 100.0%"). Triage impact is nil: all 11 dealers with 10% or more price-less rows already had non-ok verdicts. Separately, `backend/scanner/pipeline/dealer_logs.py:70` truncates the http_first JSON at 300 characters, so 16 of 129 CarsCommerce lines do not parse.

**Code says.** Before 988f21902 (2026-09-28 18:01 UTC), `_has` counted a parser's default price 0 as captured (`backend/scanner/phases/dealer_run_steps/enrich.py:36-48`); only about 4 runs used the fix. `_has` still accepts any non-empty string while `coerce_price` would reject it.

**Suggested fix.** Mark pre-F02 price and MSRP coverage untrusted; parse numbers in `_has` the same way as `coerce_price`; stop truncating the log JSON.

**Plan coverage.** Not covered.

---

### 2.21 Junk prices counted complete (357 active under $1,000; single pass)

**Affected.** 357 active cars carry a price from $10 to $999. No active car has a price of 0 or below. All 357 count as priced, and only 34 are in the index (for other codes). Another 101 cars are priced $1,000-$2,999, and some of them are the same pattern. 4 rows carry junk prices over $1M.

**Pattern (data says).** The typical case is one number repeated across unrelated models at one dealer. This is the masked-price signature that `dealer_dot_com.py:74-84` describes for Dealer.com.

| Dealer | Rows | Price | Notes |
|---|---|---|---|
| harperacura-net | 168 | $699-799 | 152 at $799 across 58 models; includes a 2026 MDX and a 2022 Silverado at 155k mi |
| pacificvolkswagen-com | 33 | $122 | all new |
| ourismanhondaoftysonscorner-com | 22 | $999 | |
| mountainvalleymotors-net | 20 | $689 | |
| audinorthlake-com | 15 | $899 | |
| glakeshyundaiofdublin-com | 12 | $500 | |
| cdodge-com | 12 | $799 | |

- **Repeats under $5,000.** 14 dealer-price pairs repeat across 5 or more models: 315 cars at 14 dealers. They include bellhonda-com, with 38 new Hondas at $1,295, and lexusofjacksonville-com, with 12 used cars at $1,095.
- **By platform.** 232 of the 357 are at Dealer eProcess stores (harperacura, pacificvolkswagen, ourismanhonda Tysons, groovesubaru); the rest are Dealer.com-labelled.
- **Source.** Every row has `price_provenance_json`, so a scan wrote the value. The Dealer.com-labelled values were first written in late September, after the Dealer.com floor landed (2026-08-06), so they arrived through a path the floor does not cover. That path was not traced.
- **Junk prices over $1M:**
  - 1403564 `W1N2M3CB3VF044020` mblaguna-com GLC at $7,782,274 (MSRP $7,782,152).
  - 1126532 `5J6RS3H78TL021194` doggetthondamedcenter-com CR-V at $3,559,029.
  - 1151864 and 583752 at mbbeverlyhills-com, both $1,000,084 with MSRP $999,999.

  The display withholds these as outliers, so the user sees no price, but completeness still counts them as priced. The other 4 active rows over $500k are real exotics at lambonb-com.

**Root cause (code says).**
- Completeness reads the raw row, so any non-NULL price or MSRP counts as present (`backend/utils/listing_completeness.py:181-192`).
- `coerce_price` accepts any positive integer (`backend/scanner/upsert/rows.py:92-100`).
- Price floors are uneven across parsers:
  - The Dealer.com parser floors masked prices at $1,000 (`backend/parsers/dealer_dot_com.py:84`).
  - The VDP parse clamps prices to $500-$2.5M (`backend/scanner/utils/vdp_price_merge.py:27-32`).
  - The Dealer eProcess parser takes `offers.price` with no floor (`backend/parsers/dealer_eprocess.py:134-138`).
- The read-time guard withholds only high outliers, at more than 5x the cohort median (`backend/utils/price_plausibility.py:72`, `:139-171`, applied at `backend/utils/car_serialize/api_price.py:13-32`). Nothing guards the low side, so $122 is shown as the price. It probably also sorts first in a low-to-high price sort; that was not tested.
- The upsert keeps any positive price when a later scan sends NULL (`backend/scanner/upsert/sql.py:48`). A fee or placeholder picked up once therefore stays until the feed sends a different positive number.

**Examples.** 1093131 `5J8YE1H89TL052199` harperacura-net 2026 MDX $699; 1023811 `3VVED7B21VM009989` pacificvolkswagen-com 2027 Taos $122; 1092731 `1GCRDEED2NZ571271` harperacura-net 2022 Silverado $799; 1027321 bellhonda-com 2027 CR-V Hybrid $1,295.

**Suggested fix.**
1. Apply one floor at `coerce_price`, with a provenance note: under $1,000 is missing.
2. Add a low-side cohort test to `implausible_price`, for example under 15% of the cohort median, and catch one value repeated across many models at one dealer.
3. Have completeness judge the price the display shows, not the raw column.
4. One-time repair: set these prices to NULL so the VDP prefetch treats them as price gaps.

**Plan coverage.** Not covered. The plan mentions `price_plausibility` only for its cache.

---

### 2.22 Interior color is excluded, but most blanks are capture gaps (about 5,100 active; single pass)

**Affected.** 6,712 active cars have no interior color. Completeness drops the code by design (`_NON_ACTIONABLE`, `backend/utils/listing_completeness.py:247-262`). The comment's reason is that dealers often never publish interior color, citing High Country Toyota at 0%. That holds for only 2 dealers: gardenahonda-com (625 of 630 blank) and highcountrytoyota-com (231 of 232). The other blanks split as follows:
- 5,092 blanks (76%) are at 296 dealers that publish interior color on at least 75% of their own rows. 3,410 of those cars are not in the index at all.
- 764 blanks are at 9 dealers in between (25-95% blank).
- 152 dealers (55,715 cars) have no blank at all.

No junk values ("unknown", "n/a", "tbd") are stored.

**Pattern (data says).** The dealers are the same ones whose VDP prefetch fell short (2.6): toyotacarlsbad-com 219, infinitiofcharlotte-com 207, bmwofdallas-com 187, kengrodyfordsandiego-com 151, beaverchevrolet-com 146, hublerchevyauto-com 135, claremonttoyota-com 133. 1,313 of the 5,092 are new cars, and 945 arrived in the 09-28 scan (2.23). 999 of them also lack exterior color.

**Root cause (code says).** The exclusion is per field, not per dealer. The prefetch already treats a blank interior color as a reason to fetch (`backend/scanner/vdp/prefetch.py:313`) and fills it from the page (`:422-428`). These rows are the ones it never reached.

**Examples.** 1044470 `1FMCU9NA8SUA73891` infinitiofcharlotte-com; 969876 `SALEXERU7M2052054` bmwofdallas-com; 884528 `YV4M12RC7S1074307` bmwofdallas-com.

**Suggested fix.** Count interior color as missing when the dealer publishes it on most rows (for example 75% or more in the last accepted run), and keep it non-actionable elsewhere. Order it after price and exterior color in the gap-first prefetch (2.6).

**Plan coverage.** Not covered.

---

### 2.23 First-scan arrivals are three times as likely to be incomplete (3,048 active; single pass)

**Data says.** Incomplete rate by `first_seen_at`, active cars, index-based:

| First seen | Active | Incomplete | Rate |
|---|---|---|---|
| July | 14,499 | 233 | 1.61% |
| August | 20,231 | 779 | 3.85% |
| 09-01 to 09-19 | 1,379 | 145 | 10.51% (134 are gardenahonda-com) |
| 09-20 to 09-23 | 7,835 | 191 | 2.44% |
| 09-24 | 49,692 | 2,933 | 5.90% |
| 09-25 | 51,842 | 2,347 | 4.53% |
| 09-26 | 50,324 | 1,916 | 3.81% |
| 09-28 | 18,873 | 3,048 | 16.15% |

- **Share.** The 09-28 cohort is 8.8% of active cars but 26% of the incomplete ones. It spans 422 dealers, all of them scanned before; no dealer was new on 09-28.
- **By field.** Compared with the 09-24 to 09-26 arrivals, the 09-28 arrivals miss price at 8.7% vs 1.7% (1,646 rows), exterior color at 2.6% vs 1.1% (494), and trim at 2.5% vs 1.7% (474).
- **Top dealers.** toyotacarlsbad-com 213 of 479, howardbentleyauto-com 184 of 952, covertfordaustin-com 119 of 171, crownlexus-com 90 of 467, wisimonson-net 77 of 97.

**Reading (data plus code).** Two effects show up together, and this data cannot separate them:
- A dealer may not have priced or photographed a new arrival yet (2.1).
- A row gets one VDP chance per run. New inserts sit at the tail of the feed order or past the cap (2.6), and price is never carried from the DB (`prefetch.py:1-9`).

Nothing has scanned since 09-28, so these rows never had a second chance. The next scan is the test: if the cohort's rate falls toward 4-6%, most of it is timing.

**Suggested fix.** Record how many scans a gap has survived. Call a gap a capture failure only after two scans with a VDP fetch, and put first-sighting gaps first in the prefetch order (2.6). Report a per-run "new arrivals incomplete" rate in `summary_json`.

**Plan coverage.** Partly. Phase 6B resumes scanning, which gives these rows their second pass. The gap-age signal is not covered.

---

### 2.24 Smaller gaps (single pass)

- **Blank model or make with a clean VIN decode (18 active outside Metro Honda).** 129 active cars have a blank model, and 111 of them are Metro Honda order-number rows (2.10). Of the other 18:
  - groovetoyota-com 11: 2026-27 Crown and Crown Signia. The titles read "2027 Toyota  NIGHTSHADE" and "2026 Toyota  Limited". vPIC returns the model cleanly (ErrorCode 0) for 10 of the 11.
  - harperacura-net 2 Morgans with make and model blank. vPIC decodes MORGAN Super 3 and Plus Four.
  - bmwcarlsbad-com 2 7 Series. vPIC returns a list, "750i, Alpina B7".
  - lhmlakewoodford-com 3 "New 2026 Ford" rows with partial or failed decodes.

  The structured backfill lists make and model as vPIC targets (`backend/enrichment/spec_structured_backfill.py:41-54`), but the clean Crown decodes were not applied. Why was not traced. Examples: 1202155 `JTDAGACF9V3000896`, 1033089 `JTDACAAJ6T3057671` groovetoyota-com; 1092837 `SA9M3TAA1PP202041` harperacura-net. Fix: fill a blank make or model from a single-valued ErrorCode 0 decode. The listing cannot name a model it does not carry, so the trim-style corroboration guard should not apply here.
- **Odometers that are present but implausible.** 18 used cars from MY2023 or older read 1-9 mi (11 at exactly 1 mi; infinitiofcharlotte-com 7). 8 active cars read over 500,000 mi. Separately, 1,070 cars labelled "new" read over 1,000 mi (159 over 5,000), which probably means demo units. All of these count as complete. Low priority, but a "used at under 10 mi and over a year old" check belongs with the F12 rule.

**Plan coverage.** Not covered.

---

### 2.25 High-rate dealers not explained by other items (357 incomplete at 5 dealers; single pass)

**Data says.** 459 dealers have active cars:
- 44 have no incomplete car.
- 16 are at 25% or more. One of those is the blank `dealer_id` group of 4 cars (2.11).
- 5 are at 50% or more.
- Only gardenahonda-com is near 100%.

Half of all incomplete cars sit at 31 dealers, and 80% at 125 (B.12). Most of the 25%-plus dealers already have an item: Gardena (2.9), autocollectionofmurfreesboro-com and bmwofbloomfield-com (2.8; bmwofbloomfield's 240 trim gaps come from the Dealer eProcess parser mapping no trim), oldorchardnissan-com (2.11), hendricklexuscharlotte-com (2.5), mymetrohonda-com (2.10), and the three Mercedes stores (F1, 2.2, 2.12). Five are not covered:

| Dealer | Active | Incomplete | Main gap | What the data shows |
|---|---|---|---|---|
| kingfordsales-com | 304 | 131 (43%) | price 121 | 84 of 84 certified and 37 of 45 used are unpriced; every new car is priced. The 09-28 run fetched all 304 VDPs (all 200) and still found no price (`dealer_logs/kingfordsales-com/scan_runs.md:5-12`). Either the store hides pre-owned prices or the VDP price sits outside JSON-LD and meta tags. |
| koonstoyotaarlington-com | 302 | 83 (27%) | price 76 | 76 of 219 new cars are unpriced; all used and certified cars are priced. 14 rows are 8-character codes (M2). |
| raypearman-com | 213 | 61 (29%) | trim 49 | Lincoln Nautilus 26, Aviator 15, Navigator 7; transmission 14. Dealer.com. |
| centurybmwhuntsville-com | 180 | 46 (26%) | trim 45 | X3 13, X5 9, 5 Series 5, X7 5. The BMW model/trim split (`bmw.py:181-227`) does not cover these. Not traced. |
| audihuntsville-com | 140 | 36 (26%) | price 36 | 36 of 137 new cars are unpriced. OneAudi. |

**Suggested fix.** Check one VDP by hand at kingfordsales-com and koonstoyotaarlington-com. That decides between "price hidden at source" (the state proposed in 2.1) and a parse gap. Run the trim-source comparison from 2.2 on raypearman-com and centurybmwhuntsville-com. Add a fleet alarm for any dealer above 25% incomplete with no open item.

**Plan coverage.** Not covered.

---

## 3. False positives and missed gaps

### 3.1 Flagged but should not be

**F1. Mercedes models that carry the trim in the model name (577 active).** Mercedes rows whose model holds the designation ("C 300" 166, "GLC 300" 141, "GLA 250" 51, "CLA 220" 45, "E 450" 28, "GLB 250" 25) and whose trim is blank: 447 2WD, 130 AWD. Other stores store the same cars as model "C-Class", trim "C 300" (629 rows). The buyer already sees the designation. The 281 Mercedes rows with family-only models ("GLC", "CLA", "Sprinter 2500") are real gaps, and so are the Lexus grade gaps (282 of 315 are used cars missing Premium or Luxury). Code: no Mercedes counterpart to `backend/utils/car_serialize/bmw.py:181-227`. Fix: a Mercedes-only rule that moves a letters-plus-digits designation (not Sprinter) into trim, or counts it as the trim. Examples: 1178442 `W1KAF4GB3TR356756` mercedesdallas-com "C 300"; 1111456 `W1NKM4GB2VF650795` wisimonson-net "GLC 300"; 1111014 `W1KNJ4GB7VJ032260` wisimonson-net "CLA 220". Plan: not covered.

**F2. Spec flags on cars that are now complete (about 1,370 active).** See X1.

**F3. Non-road vehicles judged as cars (32 active).** 20 trailers (vPIC VehicleType TRAILER) flagged for engine, transmission, drivetrain, fuel and cylinders; 6 motorcycles flagged for drivetrain; 3 golf or electric carts; 3 boats or watercraft. About 12 would leave the index with class-aware rules; the rest also have real price, trim or color gaps. Used trailers at 0 mi will also pick up mileage under F12. Pre-1981 classics are not false positives: they are real gaps that a short VIN cannot fill. Code: one passenger-car field set for every row (`listing_completeness.py:218-233`); vPIC "Not Applicable" is collapsed to blank (`backend/utils/vpic_specs.py:69`). Examples: 1137686 Big Tex trailer; 1122270 `4YDF36821M2502064` billpenneytoyota-com Keystone Cougar; 1120486 Harley-Davidson Sportster; 159606 `J0A0000000Y106625` highcountrytoyota-com Yamaha golf cart. Fix: required fields by vPIC VehicleType, or keep non-cars off the car grid. Plan: not covered.

**F4. MY2026-27 BMWs listed as used at 0 mi (473 active; ambiguous).** Will flag at the next rebuild. At bmwofaustin-com 189 of 190 carry an MSRP and 188 are priced at or above it (average 1.004), which looks like new units filed under used, not loaners. bmwnorthscottsdale-com (2 of 160 with MSRP), hendrickbmwnorthlake-com (0 of 95; its MY2026 used cars that do report mileage have a median of 7 mi) and bmwoffortwashington-com (0 of 27) have no such signal. F12 proposed exempting current-year loaner programs; `_mileage_blank` does not. Owner decision: exempt, relabel the condition, or keep flagging. Examples: 95900 `5UX23EU0XT9208568` hendrickbmwnorthlake-com; 1173804 `WBX63GM07T5659689` bmwofaustin-com. Plan: not covered.

**F5. Old, high-mileage used cars without a price (523 active; unverified).** Unpriced rate rises with age (before 2010: 21.7%; 2025 or later: 2.2%) and mileage (175k+ mi: 20.5%; under 30k: 2.8%). 523 are pre-2016 or 125k+ mi at dealers that price over 80% of used stock (180 at dealers under 5%). These may be wholesale or as-is units the dealer does not price, but no page was fetched to confirm, and the 173 descriptions that mention as-is or wholesale are all priced. Fix: a non-actionable "source unpriced" code only after the car was seen unpriced in two scans with a VDP fetch. Examples: 1446507 `1FTNX21L54EA46387` pugmirefordcartersville-com 2004 F-250 at 336,738 mi; 1349229 `JN8AS5MV4AW122865` harehonda-com 2010 Rogue. Plan: not covered.

### 3.2 Not flagged but should be

**M1. The trimless check hides about 201 real trim gaps.** 274 blank-trim cars are exempted because `epa_master` lists the model name as the only trim; about 73 are truly single-trim (2026 Prelude, Mirai, 2023 XM, RS Q8, older Corvettes), and about 201 are not: Land Cruiser 150, Odyssey 13, WRX 8, 2024 XM 3, about 27 others. The 141 MY2027 Land Cruiser blanks at 27 dealers are priced $67.6k-77.8k, the "Land Cruiser" grade band, not the "1958" grade ($60.1k-67.2k); vPIC gives no trim for them. Code: `knowledge_engine.py:23-57` trusts EPA rows, and its docstring example ("Land Cruiser -> 1958") no longer matches the catalog. Examples: 126475 `JTEABFAJ8VK072385` righttoyota-com 2027 Land Cruiser; 947537 gardenahonda-com 2026 Odyssey; 1002034 toyotaofanaheim-com 2019 WRX. Fix: an explicit single-trim list or a factory trim ladder; recover the Land Cruiser grade from the MSRP band. Plan: not covered.

**M2. Non-VIN identifiers never flagged "vin" (about 585 road vehicles).** 410 Metro Honda order numbers (2.10), 172 Koons Toyota 8-character codes that are VIN positions 10-17 ("T134AV81", "S132EH84"; their `stock_number` holds a separate KRT/KTT value), and 3 malformed VINs (940615 "00000001"; 1137710 and 1331380 truncated to 16 characters). Do not flag the 1,172 Toyota/Lexus 17-character VINs that contain I, O or Q: the check digit validates on all 1,172 when I=9, O=6, Q=8, they are Toyota-issued, and they are less often incomplete (1.6%) than numeric VINs (3.9%). Also exempt the about 20 legitimate short serials (pre-1981 classics, boats, carts, equipment). Code: only a blank or "unknown*" VIN is flagged (`listing_completeness.py:237-241`). Plan: not covered.

**M3. CTP condition is outside the mileage rule (76 active at 0 mi).** "CTP" (GM courtesy transportation, 1,020 active at 20 dealers) is not one of the used tokens (`listing_completeness.py:123`), so a 0-mile CTP car is never flagged; "CarBravo" is covered. Single-pass observation. Plan: not covered.

**M4. Body style categories and trim strings (about 23,900).** See 2.3.

**M5. Wrong electrification on cars counted complete.** 32 cars that vPIC says are BEVs store and display "Gasoline" (Cadillac OPTIQ x30, e.g. 1090576 `3GYK3EM56VS107180`, which shows engine "Electric" next to fuel "Gasoline"; VW ID.4 1194528). 1,409 vPIC PHEVs are stored as "Hybrid". vPIC itself is wrong for 2027 BMW 550e (WBA13FK...): it says BEV, so an inline-six PHEV shows "Electric" with 0 cylinders (106855 `WBA13FK05VCX97005` bmwofontario-com; 229873 `WBA13FK09VCY15540` bmwofmonrovia-net). The "vPIC outranks the feed" rule needs a guard for known vPIC filing errors. Single-pass observations. Plan: P8A.7 heals the 1,409 PHEV labels; the BEV fuel fix and the vPIC 550e guard are not covered.

**M6. No photos (5,394), plus about 3,000 placeholder-only cars counted as having photos.** See 2.4.

**M7. Junk prices under $1,000 (357) and interior color at publishing dealers (about 5,100).** See 2.21 and 2.22.

### 3.3 Angles checked and found not to drive incompleteness (single pass)

- **Group feeds and attribution.**
  - Dealers whose feed scope is a group (`dealer_feed_scope.scope='group'`: 2 dealers, 916 active cars) are incomplete at 5.35%. That is the fleet's 5.40%. Rooftop-scoped dealers (25) are at 2.74%.
  - 3,317 active rows have a detail-URL host that differs from the dealer id; they are 7.57% incomplete. 231 of their 251 incomplete rows are covertfordaustin-com, whose host is covertford.com (2.1).
  - `car_attribution` covers 911 active cars. Confirmed rows are 2.2% incomplete; conflicting rows 4 of 36.
  - No 17-character VIN is active at two dealers.
  - For the attribution owners: mbbeverlyhills-com holds 160 active rows whose detail host is mbontario.com, and none is incomplete. This is a possible misfiling, not a completeness issue.
- **Title, year, condition.** 0 active cars are blank on any of them. 14 active cars are pre-1981.
- **Make spelling.** Every major make is stored in two or three casings ("Toyota"/"TOYOTA", "Mercedes-benz"/"Mercedes-Benz"/"MERCEDES-BENZ"). Per-make tallies must group on `upper(make)`; 2.2's Lexus 315 is 158 + 157 for this reason.
- **Dealer onboarding.** No dealer's first scan fell on 09-28, so the 09-28 cohort effect (2.23) is not new dealers.

---

## 4. Index and infrastructure issues

**X1. The index is never rebuilt, so about 1,370 complete cars stay flagged.**

- **Affected:** about 1,370 active cars (pooled recompute of 2,400 sampled index rows: 11.96%; range about 1,150-1,650).
- **Data says:**
  - Stale codes: engine (64-74% of flagged rows now clear), fuel type (63-72%) and body style (68-83%). Price, trim and exterior color codes are 100% accurate, and so are model and make.
  - 1,380 active index rows with a spec code had their vPIC decode cached after the index row was written; 796 of them are flagged for engine alone. Cast `fetched_at` to timestamptz; text comparison gives 1,218 and 744.
  - 6,866 index rows share the single stamp 2026-08-26T19:14:53+00:00 (236 active). A full rebuild deletes and rewrites every row with one timestamp, so none has run since.
  - Why the codes cleared: in different samples, between half and three quarters were a vPIC decode cached after the sync (the display reads the cache, so no write to `cars` happens). The rest were writes after the sync: the tier-1 backfill in the same post-write loop, model_specs filling body style and fuel, the catalog link, and `heal_rows`. Code drift since 09-28 explains about 1%.
- **Examples:** 1414040 `W1NKM4GB3VU170972` mercedesbenzgreenway-com (indexed "engine" 09-28 14:56Z, vPIC 17:20Z, now complete); 971761 `W1NKM4GB9VU170572` mbofstevenscreek-com (14:54:14 vs 17:25:37); 1010576 `5XYPDESA2VG053953` kianorthaustin-com; 1195335 `SB1ADADE6TE003906` bellroadtoyota-com (cleared by a `heal_rows` write).
- **User impact (code says):**
  - Production hides indexed cars from the grid by default (`backend/db/repositories/data_quality_repo.py:105-122`, `is_production_env`; `deploy/docker-compose.yml:57`, `k8s/configmap.yaml:29` set `LISTINGS_INCLUDE_INCOMPLETE_CARS=0`; `listings_repo.py:394-396` drops them).
  - The car page shows "Missing: Engine" from the index row alone (`backend/routes/car_detail/listing.py:17-24`, `frontend/templates/car.html:165-175`) above a spec sheet that shows an engine.
  - Railway's env was not checked.
- **Root cause (code says):**
  - The per-car sync is step 1 of `run_post_write_enrichment` (`backend/scanner/upsert/post_write.py:62-63`); the backfill (`:63-67`), model_specs and catalog link (`:95-97`) run after it.
  - The pipeline's vPIC step (`backend/scanner/pipeline/vpic.py:13-31`; `heal_rows` raw UPDATE at `backend/enrichment/vpic_facts.py:357`) never re-syncs.
  - The full rebuild exists only in `orchestrator.py:190-196` (skipped by `--scan-only` at `:462-465`, which dealer_pipeline always passes at `pipeline/runner.py:115`), `post_scan/job.py:210-217`, and `deploy/nightly_http_refresh.sh:185` (disabled).
  - The 09-09 full scan that would have rebuilt was killed before its tail.
  - `ensure_incomplete_index_built` is a no-op once the meta row exists (`backend/db/incomplete_listings_db.py:284-299`).
- **Fix:**
  - Run `fast_rebuild_incomplete_listings_index` once per dealer_pipeline or fleet run, after the vPIC heal and reconcile, and not per shard.
  - Move the post-write sync after the backfill and catalog link.
  - An invariant that alarms when an index row is older than its VIN's decode.
  - Note the empty-decode memo (2.17): a re-sync in the same process must follow the heal.
- **Plan coverage:** Not covered. P8B.3 adds per-batch `vpic_for_dealers` without an index sync. P0B.5 measures `rebuild_listings_index.py`. P18A.5 retires the nightly that held the only scheduled rebuild, so its capability table must name a replacement.

**X2. Completeness rule changes never reach the index (1,256 active).** The F12 mileage rule (2.7) landed at 18:19 UTC on 09-28, after every index write (latest 17:21:41Z). No index row anywhere carries "mileage": 965 affected cars are absent and 291 are indexed for other codes only. The same applies to the F13 fuel rule (381 cars, 2.13). `incomplete_listings_meta` holds only `index_bootstrap_v1 = 1` (`incomplete_listings_db.py:28`) with no rules version or rebuild time. The dealer_logs "incomplete after fill" lines were also computed under the old rule. Prod caveat: the 09-29 prod fleet ran code that contains F12, and its per-car syncs would have added the code for dealers it rescanned. Fix: store a rules version and `last_rebuilt_at` in the meta table, rebuild when the version differs, and show the rebuild age on the admin data-quality page. Plan: not covered.

**X3. 8,738 index rows (43%) belong to retired cars (0 active affected; admin views wrong).** 20,330 rows: 11,592 active, 8,738 retired, 0 orphans, 0 VIN mismatches, 0 duplicates. All retired rows were written before their car's retirement (09-09 to 09-28).
- **Effects:**
  - `get_incomplete_listings_count` is a bare `COUNT(*)` (`incomplete_listings_db.py:302-314`), feeding `platform_stats.py:176` and `data_quality_hub.py:114`: 20,330 shown instead of 11,592, 75% too high.
  - Per-code totals are inflated: trim 7,981 vs 3,545 active, exterior color 5,681 vs 2,469, make 168 vs 2. Among active cars, price is the top gap, not trim.
  - `get_cars_by_ids` has no active filter (`cars_repo.py:155-185`), so the admin incomplete list loads all 20,330 cars (about 41 MiB of row data) and the export includes retired cars.
  - `get_dealership_issue_stats` joins all cars (`data_quality_repo.py:148-171`). 6 of the 25 dealers on /admin/data-quality would drop out of an active-only top 25 (chapmanfordaz-com 373 shown / 19 active; burnshonda-com 276 / 12).
  - The public grid is unaffected.
- **Root cause:** the retirement writers (`pipeline/reconcile.py:94`, `:153`; `inventory_reconcile.py:326`; `dealer/portal_sync.py:202`; `attribution/disown.py:75`) are raw UPDATEs that never touch the index, and only the full rebuild (X1) removes retired rows.
- **Examples:** 382122 `19XFL2H80TE034345` gardenahonda-com (retired 09-09); 897532 `1C4RJXR66SW579533` chapmanfordaz-com (retired 09-23); 890845 `7FARS6H85TE145589` burnshonda-com (retired 09-25).
- **Fix:** delete index rows in a shared retire helper; filter admin queries to active cars.
- **Plan coverage:** partly. P5A.4 and P5B.3 touch every retirement writer but do not mention the index; add it there.

**X4. Per-car index sync cost (0 listings; scanner CPU).** An earlier measurement of 36 ms vs 1.9 ms per car was an ordering artifact. Measured cold, the full-context sync path costs about 21-22 ms wall and 6 ms CPU per car, and the rebuild's light path about 20-21 ms and 4-5 ms. The real cost is about 9 fresh Postgres connections per car (about 12-14 ms); with `inventory_pg.borrow_read_connection()` the sync drops to 6-9 ms. `post_write.py:64` also re-reads the car (2.1 ms). Passing `detail_ctx` (`incomplete_listings_db.py:168`) saves another 1-3 ms with identical results (0 of 240 cars differed). Plan: covered by P11B.2 (borrowed connection); the duplicate read and `detail_ctx` are small additions.

**X5. Full-rebuild hazards (latent).**
- `fast_rebuild` loads every active row with `SELECT *` into dicts (`incomplete_listings_db.py:208-209`): about 1.7-2.05 GB of Python objects plus about 0.5 GB of client result buffer.
- It computes for 7-26 minutes, then runs `DELETE FROM incomplete_listings` plus INSERT (`:247-256`). Per-car syncs from other shards in that window are overwritten with snapshot state.
- With the meta row missing, every web process (`schema_repo.py:287` via `main.py:97`), scanner (`cli.py:339`), worker loop and index read path (`:304`, `:319`, `:333`) would each run its own full rebuild, guarded only by a per-process lock.
- Fix: stream ids in chunks, upsert per chunk, delete only rows older than the rebuild start, take an advisory lock, never rebuild in the web process.
- Plan coverage: partly. P13A.5 keeps the rebuild out of the web bootstrap; streaming and the lock are not covered.

---

## 5. Quick wins

Ordered by listings affected per unit of effort. The first one needs owner approval (CPU and RAM on the MacBook; run it on the mini or on mains power).

1. **Rebuild the index once, then keep it rebuilt.** One `fast_rebuild_incomplete_listings_index` (about 7-26 min CPU, about 2 GB RAM) clears about 1,370 stale flags (in prod those cars return to the grid), adds about 1,256 mileage and 381 fuel codes, and drops 8,738 retired rows. The total stays near 11,500, but about 2,700 cars change sides. Then add the rebuild to `fleet_scan.py`'s post steps and to the end of dealer_pipeline, after the vPIC step (X1, X2, X3).
2. **Admin queries count only active cars** (one join each in `get_incomplete_listings_count`, `get_cars_by_ids` callers and `get_dealership_issue_stats`), and the retire helper deletes index rows (X3).
3. **Mercedes designation split** in the serializer, like the BMW one: 577 false flags (F1).
4. **Price floor:** treat a price under $1,000 as missing in `coerce_price` and in completeness, and add a low-side check to `implausible_price`. This stops $122 and $799 being shown as prices on 357 cars and turns them into prefetch targets (2.21).
5. **Fuel repair:** set the 381 injection-system values to NULL and fill from vPIC FuelTypePrimary, plus the 79 blank cars with a usable decode: about 460 (2.13).
6. **Metro Honda:** confirm on the site, retire the 410 order-number rows, and add a VIN-shape gate to `parsers/dealer_eprocess.py:171-172`: 391 incomplete plus that dealer's reconcile unblocked (2.10).
7. **Engine:** re-decode partial vPIC rows for MY2026 and later (124 GLC immediately; 704 index rows in all), and accept a layout-only engine line when cylinders are verified (about 200) (2.12).
8. **Gap-first prefetch order** and no Carfax requirement for new cars, so the budget reaches price, color and photo gaps (582 proven, up to about 3,000) (2.6). First-sighting rows (2.23) and dealer-relative interior color (2.22) belong in the same ordering.
9. **Read `_source_url`, and require the VIN on the page** before applying VDP fields: stops landing-page text being written onto cars at 19 Dealer eProcess stores (2.8).
10. **One placeholder-image test** (add "photo_unavailable", "unavailable_stockphoto", "coming_soon", "comingsoon" and "default.jpg" to `_is_placeholder_url`, and share it with completeness and the dealer photo alarm): about 3,000 placeholder-only cars become visible as photo-less (2.4).
11. **Exclude non-road vehicles by vPIC VehicleType** (32) and **add "ctp" to the used tokens** (76) (F3, M3).
12. **Resume scanning** (Phase 6B): the shipped `pages_needed` hint widens the cap and Toyota Carlsbad's 213 rows should clear without a code change (2.1, 2.6). The rescan also tests how much of the 09-28 cohort's 16% was timing (2.23).

---

## Appendix A. Claims corrected or withdrawn during verification

No finding was refuted outright. These parts of the original findings did not hold and are not used above:

- **Stale index flags.** Not mainly a vPIC effect in every sample. Roughly half to three quarters come from the vPIC cache; the rest come from writes after the sync. Code drift explains about 1%.
- **Zero-mile used cars "shown publicly as 0 mi".** False: the display says "Mileage not listed". The BMW-store group is not proven to be capture failures.
- **"Rows from an earlier weak run" (Hendrick Lexus Charlotte).** Backwards: the earlier run (339 rows) was the full one; the 33-row run came later.
- **"2.7x more likely to be incomplete" for stale rows.** Holds only for three dealers; elsewhere stale and fresh rows are equal.
- **"32,240 rows not written by the last run".** A timing heuristic error; the real stale set is 8,576.
- **CarsCommerce price and trim gaps blamed on the WAF.** Not supported: other platforms with working prefetch have equal price-gap rates. Only exterior color shows a CarsCommerce excess.
- **Mercedes/Lexus trim gaps caused by catalog naming.** Wrong mechanism: the catalog lists several trims for the family names too. Lexus is real gaps, not a false positive.
- **"The VIN decode can fill 1,899 trims".** Withdrawn: the guard refuses them on purpose, and only about 0-19 are genuine trims.
- **"vPIC heal fully applied".** A harness bug (`sqlite3.Row` row factory passed to `_vpic_flat_rows`). 60 body-style writes are still pending.
- **Toyota/Lexus I/O/Q VINs "malformed" or "pre-production placeholders".** Wrong: they pass the ISO check digit with I=9, O=6, Q=8 and are less often incomplete than numeric VINs.
- **Metro Honda rows as "pre-production false positives".** Reframed: they are stale order-number rows that reconcile cannot retire.
- **Guessed URLs "404 on every non-Dealer.com platform".** Wrong: they mostly work on Dealer.com and Dealer eProcess. Chapman and Motive had no fetch evidence. Old Orchard and Chapman were dropped from the count.
- **Auto-heal "0.8% yield".** Corrected to 1.2-3.2%. "No trim path at all" is corrected: the structured backfill can write trim; there is no page-parse path for it.
- **"No off-scan recovery since 08-05".** Narrowed: one-shot scripts ran until 08-08, and inline gap fill still runs during scans.
- **Prefetch "cap-bound" for 40 dealers.** Half were cut by the wall clock instead. markkia-com and most of fordofmurfreesboro-com were fetched.
- **Index sync "36 ms vs 1.9 ms".** An ordering artifact; the real cost is connection churn.
- **Rebuild memory "several GB".** About 2 GB. The race window is 7-26 min, not 6.8.
- **Pre-F02 coverage bug "hid price-thin dealers from triage".** It did not: all of them already had non-ok verdicts.
- **Fixes that are already shipped.** The pages_needed cap widening (87a3ff0f9), the scanner writing NULL for a missing odometer (F12), and rejection of injection tokens (F13) all exist. What is missing is a rebuild and a repair of the stored values.
- **Example ids with the wrong VIN in the originals,** corrected above: 1461257 is `2T36CRAV1TW41D467`; 994656 is `A07-41293-00089`; 945295 and 1205362 were mismatched; 890845 is `7FARS6H85TE145589`; 970323's VIN is valid.

Corrected by the completeness review (arithmetic in the verified draft):
- **Engine split (2.12).** "840 resolve from vPIC + 51 Electric + 438 missing" summed to 1,329 of 1,379. A full recompute of all 1,379 gives 885 + 51 + 5 + 438.
- **"A few dealer-specific defects account for about 1,900 rows".** The five named stores sum to 1,632.
- **Drivetrain (2.14).** 307 − 43 stale − 16 trailers is 248, not 243. It is stated now as about 245, and the "about 440" is explained as drivetrain plus non-Metro transmission.
- **"Four dealers hold 79% of the stale incomplete rows" (2.11).** The base is the 1,171 stale indexed rows (930 of them), not the about 1,024 still incomplete.

## Appendix B. Queries used

All SQL ran on local Postgres with `PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=60000'`.

B.1 Headline counts (re-run for this document, unchanged since 2026-09-28):
```sql
select (select count(*) from cars where listing_active=1),                         -- 214675
       (select count(*) from incomplete_listings),                                 -- 20330
       (select count(*) from incomplete_listings il join cars c
          on c.id=il.car_id and c.listing_active=1),                               -- 11592
       (select max(scraped_at) from cars where listing_active=1),                  -- 2026-09-28T22:59:34Z
       (select max(updated_at) from incomplete_listings);                          -- 2026-09-28T22:59:36Z
```

B.2 Last full rebuild and index age by day:
```sql
select updated_at, count(*) from incomplete_listings group by 1 order by 2 desc limit 3;
-- 2026-08-26T19:14:53+00:00 | 6866
select substr(il.updated_at,1,10) d, count(*), sum((c.listing_active=1)::int) active,
       sum((c.listing_active=0)::int) retired
from incomplete_listings il left join cars c on c.id=il.car_id group by 1 order by 1;
```

B.3 Index rows written before their VIN's decode (cast both sides):
```sql
select count(*) filter (where il.missing_fields_json::jsonb ?| array['engine','transmission','drivetrain','cylinders','fuel_type','body_style']
                          and v.fetched_at::timestamptz >= il.updated_at::timestamptz) late_vpic   -- 1380
from incomplete_listings il join cars c on c.id=il.car_id and c.listing_active=1
left join nhtsa_vpic_cache v on v.vin=c.vin;
```

B.4 Used cars at 0 mi vs the index:
```sql
with u as (select c.id, il.car_id is not null in_idx,
                  coalesce(il.missing_fields_json like '%"mileage"%', false) has_code
           from cars c left join incomplete_listings il on il.car_id=c.id
           where c.listing_active=1 and c.mileage=0
             and (c.is_cpo=1 or lower(coalesce(c.condition,'')) ~ '(used|certif|pre-owned|preowned|pre owned|cpo|carbravo)'))
select count(*), sum((not in_idx)::int), sum((in_idx and not has_code)::int), sum(has_code::int) from u;
-- 1256 | 965 | 291 | 0
```

B.5 Per-field codes, active vs retired:
```sql
select code, count(*) filter (where c.listing_active=1) active, count(*) filter (where c.listing_active=0) retired
from incomplete_listings il join cars c on c.id=il.car_id,
     jsonb_array_elements_text(il.missing_fields_json::jsonb) code group by 1 order by 2 desc;
```

B.6 Price cause buckets: the dealer's latest `scan_runs` row with `capture_coverage`, split by `recipe_coverage.price < 0.99` and by `vdp_prefetch.http_first` (fetched, skipped_cap, wall_clock_hit, host_exhausted, candidates). "Written by the last run" uses the dealer's newest `scraped_at` batch (within 10 min of its max), not `finished_at - duration`, which misses rows.

B.7 Guessed VDP URLs by platform:
```sql
select coalesce(r.provider_hint,'<none>'), count(*) inc,
       count(*) filter (where c.source_url ~ '/(new|used|certified)-inventory/vin-[A-Za-z0-9]{17}\.htm') guessed
from incomplete_listings il join cars c on c.id=il.car_id and c.listing_active=1
left join dealer_recipes r on r.dealer_id=c.dealer_id group by 1 order by 3 desc;
```

B.8 Non-VIN identifiers by dealer:
```sql
select c.dealer_id, count(*), count(il.car_id)
from cars c left join incomplete_listings il on il.car_id=c.id
where c.listing_active=1 and c.vin !~ '^[A-HJ-NPR-Z0-9]{17}$' group by 1 order by 2 desc;
```

B.9 Photo-less active cars (approximates `_has_http_image`):
```sql
with t as (select dealer_id, lower(coalesce(condition,'')) cond,
                  (coalesce(image_url,'') ~* '^https?://' or coalesce(gallery,'') ~* '"https?://') has_img
           from cars where listing_active=1)
select case when cond like 'new%' then 'new' else 'used/cpo/other' end,
       count(*) filter (where not has_img), count(distinct dealer_id) filter (where not has_img), count(*)
from t group by 1;   -- new 3707 / 70 dealers; used 1687 / 83 dealers
```

B.10 Stale active rows (relative to the dealer's newest write):
```sql
with a as (select c.id, c.scraped_at::timestamptz sa,
                  max(c.scraped_at::timestamptz) over (partition by c.dealer_id) mx
           from cars c where c.listing_active=1)
select (sa < mx - interval '6 hour') stale, count(*), count(il.car_id)
from a left join incomplete_listings il on il.car_id=a.id group by 1;   -- stale: 8576 / 1171
```

B.11 HD and commercial segment:
```sql
select case when c.model ~* '(2500|3500|4500|5500|6500|f-?250|f-?350|f-?450|f-?550|super ?duty|cutaway|chassis|e-series|express|savana|transit|sprinter|promaster|low cab)'
            then 'HD' else 'other' end seg,
       count(*), count(*) filter (where c.epa_master_id is null),
       count(*) filter (where il.missing_fields_json::jsonb ? 'drivetrain'),
       count(*) filter (where il.missing_fields_json::jsonb ? 'transmission')
from cars c left join incomplete_listings il on il.car_id=c.id where c.listing_active=1 group by 1;
```

B.12 Concentration and per-dealer rates:
```sql
with a as (select c.dealer_id, count(il.car_id) inc from cars c join incomplete_listings il on il.car_id=c.id
           where c.listing_active=1 group by 1),
     b as (select inc, sum(inc) over (order by inc desc rows unbounded preceding) cum, sum(inc) over () tot,
                  row_number() over (order by inc desc) rn from a)
select min(rn) filter (where cum>=tot*0.5), min(rn) filter (where cum>=tot*0.8), max(tot), count(*) from b;  -- 31 | 125 | 11592 | 415
-- per dealer: active n, indexed inc; 459 dealers, 44 with inc=0, 16 at >=25%, 5 at >=50%
```

B.13 Junk prices:
```sql
select dealer_id, count(*), min(price), max(price), sum((msrp>0)::int)
from cars where listing_active=1 and price>0 and price<1000 group by 1 order by 2 desc;   -- 357 rows; 34 indexed
with g as (select dealer_id, price, count(*) n, count(distinct lower(model)) models
           from cars where listing_active=1 and price>0 and price<5000 group by 1,2)
select count(*), sum(n), count(distinct dealer_id) from g where models>=5;                -- 14 | 315 | 14
```

B.14 Placeholder-only photos (main image shared by 10+ active cars, no gallery URL used by fewer than 5 of them):
```sql
with s as (select image_url from cars where listing_active=1 and image_url like 'http%' group by 1 having count(*)>=10),
c2 as (select c.id, c.dealer_id, c.condition, c.gallery, c.image_url from cars c join s using (image_url) where c.listing_active=1),
u  as (select c2.id, m[1] url from c2, regexp_matches(coalesce(c2.gallery,''), '"(https?://[^"]+)"', 'g') m),
f  as (select url, count(distinct id) k from u group by 1),
per as (select c2.id, c2.image_url, count(u.url) filter (where f.k<5) uniq
        from c2 left join u on u.id=c2.id left join f on f.url=u.url group by 1,2)
select case when image_url ~* '(unavailable|coming|placeholder|/missing/|default|no[-_]?image|noimage|no[-_]?photo|untitled|awaiting)' then 'placeholder'
            when image_url ~* '(autodata|stockphoto|stock-images|stock_images|media\.rti\.toyota|/ddc/vehicles/|chrome|evox|izmo|jellybean)' then 'oem_stock'
            else 'other_shared' end, count(*)
from per where uniq=0 group by 1;   -- placeholder 3032 | oem_stock 3168 | other_shared 3656
```

B.15 Interior color blanks by the dealer's own publish rate:
```sql
with d as (select dealer_id, count(*) n, sum((coalesce(trim(interior_color),'')='')::int) blank
           from cars where listing_active=1 group by 1)
select case when blank=0 then 'none' when blank*1.0/n>=0.95 then '>=95%' when blank*1.0/n>=0.25 then '25-95%' else '<25%' end,
       count(*), sum(n), sum(blank) from d group by 1;   -- <25%: 296 dealers, 5092 blanks; >=95%: 2 dealers, 856
```

B.16 Incomplete rate by first-seen cohort:
```sql
select substr(first_seen_at,1,10) d, count(*), count(il.car_id),
       sum((il.missing_fields_json like '%"price"%')::int)
from cars c left join incomplete_listings il on il.car_id=c.id
where c.listing_active=1 and c.first_seen_at >= '2026-09-24' group by 1 order by 1;   -- 09-28: 18873 | 3048 | 1646
```

B.17 Engine recompute: `critic2/eng.py` in the session scratchpad. It loads every active index row carrying "engine" (1,379), recomputes `listing_missing_field_codes` with `merge_verified_specs`, and buckets the cleared rows by whether `engine_display` says Electric and whether a vPIC row is cached.

**Recomputes with the real code.** Every "under today's code" figure came from `merge_verified_specs(include_extended_specs=False)` plus `listing_missing_field_codes`, the same path as `fast_rebuild`, run read-only with network fetches blocked. Samples:
- index rows: 500, 600, 700, 1,000 and 1,200 random rows;
- all 1,379 engine flags, all 410 Metro Honda rows, all 381 injection-fuel rows, all 274 trimless-suppressed rows, all 630 Gardena rows;
- 300 to 1,000 non-indexed cars.

Scripts are in the session scratchpad, `/private/tmp/claude-501/-Users-asarrafi-Projects-DealershipScanner/b17fc126-ed02-4bd1-b99f-43987a3a982a/scratchpad/` (`scripts/recompute.py`, `idxint/recompute.py`, `idxint/timing.py`, `skeptic_d1/`, `rm6/`, `frc/`, `vehicle_mix/`; the completeness review's SQL and `eng.py` are in `critic2/`). It is temporary and will not survive the session.

## Appendix C. Method and limits

- **Data and sources:** local Postgres only. No prod access, no Railway, and no fetches to dealer sites. Statements that need a page fetch, such as "the dealer does not publish a price" or "the order-number units are gone from the site", are marked unverified.
- **Verification:** each finding was produced by one analysis pass and re-checked by an independent pass with different samples.
- **Single-pass items:** these were not independently re-verified: CTP (M3), the electrification mislabels (M5), photo-less counts beyond the B.9 query, and everything the completeness review added (2.21-2.25, 3.3, the placeholder note in 2.4 and B.12-B.17). The placeholder and stock split in B.14 is by URL keyword, so the 3,656 "other shared" bucket mixes dealer banners with OEM art. The 1,379-car engine recompute in B.17 is a full population, not a sample.
- **Not examined:** the in-transit (`on_lot`) code, which is non-actionable by design; whether any page or API carries a price for the Kingford, Koons Arlington and Audi Huntsville gaps (that needs a fetch); and prod's index after the 09-29 fleet.
- **Writes:** none to any database or tracked file. This document is the only file written.
