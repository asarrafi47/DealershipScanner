# Search quality: how good is the search bar?

Date: 2026-11-09 (second pass, 20:45 to 21:30 EST), verified and extended on 2026-11-10 (third pass, 20:18 to 20:55 EST, section 13).
Live site: https://sarraficars.com, release 1.5.8
(`GET /api/health` returned `{"commit":"0fd91872bedb07a30ecdd7e19773db5fdc53b1a7","status":"ok","version":"1.5.8"}` on 2026-11-09
and again on 2026-11-10 at 20:20 EST).
Code read: `main` = tag `v1.5.8` (0fd91872b). Between `v1.5.8` and the staged tip `faea2b985` the search code itself (parser,
`hybrid_search.py`, routes, `listings.js`, templates) changes in one regex (`_CYLINDER_RE`, the `i4` rule) and in how the embedding
model is loaded
(`git diff --stat v1.5.8 faea2b985 -- backend/utils/query_parser.py backend/utils/hybrid_search.py backend/vector backend/routes/listings_api.py frontend/static/listings.js`).
Three data-quality changes in the same range do reach search results (colour lexicon word boundaries, a price floor at write time,
distance ranking by registry id; `git diff --stat v1.5.8 faea2b985 -- backend/utils backend/db/repositories`). Section 13.4 measures them.
Failure classes F1-F5 and F8-F12 below hold unchanged for 1.5.11-1.5.16; F6 is fixed for the i-series and F7 changes in part.

Provenance. An earlier pass of this assignment (08:43 to 09:17 EST on 2026-11-09) left raw data and scripts but no report. Its raw
files are untouched in `raw/search-quality/`. The second pass re-ran everything from scratch into `raw/search-quality/run2/` and reuses
only the 60-query definition (`raw/search-quality/queries.py`). The live replay agreed with the earlier pass on all 60 queries: identical
parse JSON, identical result counts and identical top-5 car ids (60/60 each; `stats_report.py`, section 3.4).
Live traffic of the second pass: **138 anonymous GET requests**, none to a login or form endpoint, no POST, at least 1.1 s apart; the
earlier pass used 157.
The third pass (2026-11-10, `raw/search-quality/run3/`) re-ran the live parse of all 60 queries and 6 extras, 34 of the 60 live `/listings?q=` GETs, the
local replay, the sandbox experiments and the cold-start measurement, and added probes (8 more queries and the 40 most listed models). Nothing changed: 66 of 66 live parses, 34 of 34
live result counts and 34 of 34 top-5 id lists equal the second pass (section 13.1). Live traffic of the third pass: **159 anonymous GET
requests** (all HTTP 200, none to a login or form endpoint, no POST, at least 1.5 s apart, a 170-request cap in `live3.py`). Each pass is
a separate run under the 300-request limit; the three together sent 454 requests.

---

## 1. Summary for the owner

### 1.1 The score

| | Points | Share |
|---|---|---|
| Parse correctness (did the bar understand the words?) | **73 / 120** | 61% |
| Result relevance (are the cars shown the cars asked for?) | **72 / 120** | 60% |
| **Overall (60 queries x 2 x 2)** | **145 / 240** | **60%** |

- 27 of 60 queries are fully right (2 and 2). 17 are wrong on both counts (0 and 0).
- 16 of 60 return no cars on the live site. 2 of those (the nonsense queries) are correct. The other 14 are wrong, and for 13 of the
  14 the area holds matching cars (between 8 and 8,629 of them); the 14th (`BMW X5 with M Sport under $70k AWD under 50k miles`) has 26
  near-matches once the wrong filter is dropped.
- Rubric and one line of reasoning for every query: sections 3.4 and 4.2. The scores are my judgement under the stated rubric; the earlier
  pass scored the same set 146/240 and the two passes differ on one query (D4, a deliberate override).
- Stable: the third pass (2026-11-10, section 13) re-ran the live parse of all 60 queries and the live results of 34 of them; parse JSON, result
  counts and top-5 ids are identical to 2026-11-09, on the same release 1.5.8. Replaying the staged tip's parser projects 149/240 (62%): only `bmw i4` changes.

### 1.2 How good is it?

It is a **good keyword-to-filter translator for one grammar** and a **poor search engine for everything else**.

- Works: `make model` with a misspelt make, price caps (`under $25,000`, `honda civic under 20k`), a year or `2020 or newer`, year ranges,
  `new / used / certified`, fuel (`hybrid`, `electric`, `plug-in hybrid suv`), body label plus cap, colour plus model, a pasted VIN,
  Mercedes line codes (`gle 350`, `c300`), and nonsense (an empty list and a message). Category scores of 75% to 100%: misspelling, body
  style, fuel, colour, feature, condition, nonsense.
- Does not work: natural language (20% of the points), the example queries the site itself shows or contains (0%), drivetrain phrases
  (38%), ranges and over/between phrasing (50%), and anything where an ordinary word of the query happens to be a trim, a package or a body
  label in the database.
- It is not a semantic search. The pgvector layer is not in use in production (`GET /api/ready` returned
  `{"checks":{"db":"ok","pgvector":"skipped","redis":"skipped"},"ready":true}`, and live GET searches returned 200 rows, more than the
  semantic path's top 100 allows), and even when it is on, free text without a parsed filter never reaches it (section 5.12). What shoppers get is regex plus fuzzy matching against the inventory vocabulary, then SQL.

### 1.3 The findings that matter most

1. **A named model can return nothing.** The three best-selling pickup lines, Silverado 1500 (643 cars in the 50-mile area), Sierra 1500
   (247) and Ram 1500 (118), are 1,008 of 13,298 cars (7.6%) and return no cars when typed by name: the vocabulary contains a body style
   called `1500` and the parser filters on it. Live: `chevrolet silverado 1500` returned 0 cars. Likewise `f-150 lariat` and
   `jeep wrangler rubicon` (junk model names built from retired listings) return 0 while 71 and 45 such cars exist (section 5.1).
2. **Ordinary words become hard filters.** 16 of the 60 queries carry a stray AND filter built from a word of the query (`truck`,
   `interior`, `black`, `good`, `tan`, `tow`, `car`, `automatic`, `sport`, `rear wheel drive`). `pickup truck` keeps 14% of the pickups;
   `family suv under 30k with good mpg` returns 0; `tow package` returns 4 cars of 2,002 (section 5.4).
3. **Numbers.** Of 54 phrasings of price, mileage and year, 23 parse right. `under 30,000 miles` sets a $30 price cap and returns 0
   (8,629 cars have 30,000 miles or fewer). `over 50k` becomes a cap of $50,000. `before 2015` becomes 2015 only. There is no minimum
   price or mileage anywhere in the filter schema (section 5.3).
4. **The site's own examples fail.** Of the 7 example queries the site shows or contains in code, 6 return 0 cars and the 7th returns
   cars with the model dropped (section 4.3; all seven re-run live in section 13.5). Do not promote the bar with them.
5. **Colours.** 32,707 active cars (15% of the fleet) have `gray` or `grey` in the paint name; 1,220 of them (4%) are in the `gray`
   family. Live: `gray honda civic` returned 62 Civics and none has gray in its name (35 are `Platinum White Pearl`);
   `red toyota camry` returned 43 Camrys of which 13 (30%) are `Predawn Gray Mica` (the substring `red` in `Predawn`) (section 5.7).
6. **Cost.** Every keystroke sends one GET and one POST. A 22-character typed query is 44 requests and 7.7 s of server CPU (measured
   locally); a third such query inside a minute meets HTTP 429 from the 90-per-minute limiter and the page shows no error (section 5.11).
   Each POST also inserts one analytics row into the encrypted users database before it answers: 44 ms per row, 1.08 s for a 22-character
   query, measured on a throwaway file (section 13.3). The admin "top queries" and "zero-result searches" counts are built from those prefix rows.
7. **The staged releases barely move the score.** The staged tip's parser, replayed on the 60 queries, changes one parse (`bmw i4`,
   0/0 to 2/2; projected score 149/240 = 62%, local replay). Its colour change (word-boundary matching) removes 76 false `tan` and 20 false
   `red` (`Predawn Gray Mica`) memberships in the live area, but it also takes `blue` from 71 cars (`Blueprint`), `gray` from 56 (37 `Moonstone
   Gray Metallic`) and `red` from 9 genuine reds (`Redline`, `Infrared`), and the lexicon still has no `gray`/`grey`; its price floor needs a
   repair run on the existing rows (section 13.4).

### 1.4 What to do, in order

Fix correctness before promoting the bar, because promotion multiplies exposure to these failures. In sandbox runs (section 7, nothing in
the repo changed) six small parser changes lift the mean F-score over the 57 testable queries from 0.515 to 0.718, cut "zero results although
cars exist" from 13 queries to 5, and lift correct number phrasings from 23/54 to 41/54. Order: (1) vocabulary from active rows with minimum
counts, (2) a needle and trim policy, (3) number grammar plus a minimum bound, (4) verify every shown example through the parser,
(5) colour lexicon and body-label clean-up, (6) zero-result recovery and an interpretation line, (7) one request per keystroke at most
and a visible 429. Redis does not fix any of the 12 classes; it can cache parse and POST results (section 8).

---

## 2. How the search bar works

### 2.1 Where it is on screen

| Where | Element | Measured (this pass) |
|---|---|---|
| `/listings`, desktop | `#smart-search-input`, `frontend/templates/listings.html:66-72`, placeholder "Make, model, keyword, or VIN" | visible, 320 x 49 px at (284,140) in the filter column (`pw_shots1440.json`); a second "Model or VIN" field sits in the navy sidebar |
| `/listings`, phone 390 px | same element | **not visible on load** (`search_input_visible_on_load: false`); it is inside the Filters sheet, 2 taps away, at (20,214) 350 x 49 px (`pw_shots390.json`) |
| `/` landing | `.lps-search-input`, `landing.html:28`, a bare line, placeholder "White BMW X5 with M Sport under $70k..." | 621 x 26 px at (380,394) on 1440; 207 x 44 px on 390 (`pw_shots1440.json`, `pw_shots390.json`) |
| top bar, other pages | `_nav_search.html:12`, placeholder "Accord with sunroof under $30k · M Sport · VIN..." | not exercised |
| hint text on `/listings` | `listings.html:60-62`, class `smart-search-hint--sr` (screen-reader only) | no visible example; the rotating examples in `listings.js:400-413` look for `.smart-search-hint__text`, which no template emits, so they never show |

### 2.2 What the page sends

`frontend/static/listings.js`, function `onSmartInput` (L329-380), on every `input` event with text:

1. immediately: `runTokenPreview` (L178-208) filters the already loaded area client-side by AND of the query words;
2. next animation frame: `GET /api/search/smart/parse?query=..&zip_code=..&radius=..` (L225-254), answered by
   `api_search_smart_parse` (`backend/routes/listings_api.py:529-542`): `{ok, filters, highlight}`, no database search;
3. 50 ms later (`SMART_SEARCH_FULL_DEBOUNCE_MS`, L13): `POST /api/search/smart` with JSON `{query, zip_code, radius}` and an
   `X-CSRF-Token` header (L256-327), answered by `api_search_smart` (`listings_api.py:545-620`):
   `{ok, api_version, filters, results[], highlight, search_meta{mode, sql_count, vector_candidate_count}, empty_message}`;
4. on the first character only: `__DS_ensureListingsCarsLoaded()` (L339-346) fetches the whole area, `GET /api/listings/cars?zip=28202&radius=50`
   (14,545,187 bytes decoded, TTFB 3.4 s in the second pass's fetch; the third pass saw the same 14,545,187 bytes, 1,583,932 bytes gzip on the wire, TTFB 5.3 s),
   so that the client-side matchers have something to filter.

Earlier requests are aborted in the browser (`AbortController`); the server does not cancel work it has started. The parsed filters
are drawn twice: as chips under the input (`fillParseChips`, L95-175, which prints raw database labels) and by
`__DS_applySmartFilters` (`frontend/static/main.js:1819-1893`), which unticks every filter checkbox and then ticks the matches. The
final grid is the POST's `results` rendered with `preserveOrder`. The same POST is called by the native iOS app
(`backend/mobile/contract.py:66-72`), and the parser is also used by the car chat (`backend/routes/ai_chat_bp.py:238,272`) and the nearby
dealers search (`backend/listings/nearby_dealers.py:1069`).

The landing form, the top-bar form and "Run again" links navigate to `GET /listings?q=..&zip_code=..&radius=..`, which runs the search
on the server and embeds up to 200 cars in the page (`backend/listings/routes.py:31,220-236`).

### 2.3 What the server does

```
q (<=200 chars) -> parse_natural_query                      backend/utils/query_parser.py:1177-1289
     vocabulary = SELECT DISTINCT make, model / body_style / TRIM(trim) FROM cars   (L116-171, cached for the process)
     regex: price cap, mileage cap, years, drivetrain, cylinders, fuel, engine litres, condition, features
     thefuzz WRatio >= 75 over 2,974 "make model" labels; trim hint over 7,276 trims; 11,857 OEM option names
 -> filters_dict_to_search_cars_kwargs                      hybrid_search.py:136-277
 -> query_is_actionable ? no -> mode no_parse_match, 0 cars, "No vehicles matched that search..."   (L575-590)
 -> search_cars(...)  SQL, area = ZIP + radius, nearest 500 matches (search_repo.py:56, search/ranking.py:28)
 -> _rank_smart_search_results                              hybrid_search.py:671-703
     order: no real photo, call-for-price, single photo | equipment-needle count | vector rank | data quality | price
 -> serialize_cars_for_listings_grid -> JSON
```

Equipment needles are substring tests (`INSTR(LOWER(col), needle)`) over five text columns (`packages`, `description`, `title`, `trim`,
`engine_description`), every needle ANDed (`backend/db/repositories/search/where.py`, `add_equipment`). Price and mileage caps keep
unpriced rows (`price IS NULL OR price <= ? OR price = 0`, `add_price_mileage_caps`).

### 2.4 The semantic layer

`backend/vector/pgvector_service.py:205-237` `query_cars` embeds the query with `all-MiniLM-L6-v2` (384 dimensions) and reads
`listing_semantic_embeddings`; it is enabled when `PGVECTOR_URL` or `DATABASE_URL` is set. Production reports `pgvector: skipped`, and
live `GET /listings?q=` searches of this pass returned 200 rows (27 of the 60 queries, for example A1, A2, B1 and C1), which the semantic path (top 100) cannot produce.

---

## 3. Method

### 3.1 The test set

60 queries in 13 categories (`raw/search-quality/queries.py`): make/model/trim (7, including a pasted VIN), misspellings (6),
price/mileage/year ranges (8), body styles (5), drivetrain (4), fuel (5), colours (4), features (5), condition (3), location words (3),
natural language (5), the site's own examples (3), nonsense (2). Each query carries the shopper intent in words and a predicate over
card fields and listing text, so "is this car what was asked for" is computed, not eyeballed.

### 3.2 What was run

| What | Command (from `docs/web_speed_search_2026_11/raw/search-quality/run2`) | Output |
|---|---|---|
| Live: parse GET and `/listings?q=` GET for each query, 1 health, 6 extras | `PYTHONPATH=. .venv/bin/python run_live2.py` | `results_live.json`, `run_live.log`, `live_requests.jsonl` |
| Live: 4 documents cached for the browser | `... python pw_shots2.py fetch` | 4 GETs: `/`, `/listings?zip_code=28202&radius=50`, `/api/listings/filter-options`, `/api/listings/cars?zip=28202&radius=50` |
| Live: confirmations | `... python confirm_live_trucks.py`; one `/listings?q=chevrolet silverado 1500` GET; one `/api/ready` GET | `confirm_live_trucks.json`, `confirm_live_silverado1500.json`, `ready_live.json` |
| Local replay of the POST path, the GET path, an uncapped filter set | `INVENTORY_DATABASE_URL='postgresql:///cars?host=/tmp' PGOPTIONS='-c default_transaction_read_only=on' LISTINGS_INCLUDE_INCOMPLETE_CARS=0 PYTHONPATH=. .venv/bin/python run_local2.py` | `results_local.json`, `run_local.log` |
| Judge live top 5 with listing text, area truth | `... enrich_live2.py`, `... live_area_truth2.py`, `... relaxed_truth2.py` | `results_live_enriched.json`, `live_area_truth.json`, `relaxed_truth.json` |
| Scores and tables | `.venv/bin/python score2.py` | `score_summary.json`, `score_rows.md`, `appendix_top5.md` |

Query parameters of the live replay: `zip_code=28202` (Charlotte NC), `radius=50`. All local commands use the read-only database
setting `default_transaction_read_only=on`; nothing was written to a database and no repository file was edited.

### 3.3 Why GET, and how good a proxy it is

The page's results come from `POST /api/search/smart`; the rules for this pass allow only anonymous GETs, so the live results are those of
`GET /listings?q=` (same parser, same SQL filters, different cap and ordering). The live **parse** requests are exactly the page's own
parse GETs. The POST path was replayed locally with the same code. Equivalence check on the local replay: the top-5 ids of the GET path
equal those of the POST path for 53 of 60 queries, result counts are consistent for all 60, and the 7 that differ are broad queries where
the cap differs (500 nearest for POST, 200 for GET): D1 `suv`, D2, E1, H2, H3, H5, J2. The local parse equals the live parse for 59 of 60
queries (D4: production also knows the body style `Convertible/Cabriolet`) and for 2 of 3 confirmation parses (`f-150` alone: the local
vocabulary has a junk pair `Ford E-150`, production returned `Ford F-150`). The local inventory is a different snapshot from production:
9,485 cars in the area locally against 13,298 live.

### 3.4 Scoring rules

- **Parse 2**: every stated constraint is represented and nothing wrong is added. **1**: the main intent is represented but a stated
  constraint is missed, or a wrong extra is added that does not change the result much. **0**: the main intent is lost, or a wrong or
  extra filter zeroes or inverts the result.
- **Relevance 2**: all of the top 5 satisfy the intent and at least half of the matching cars are reachable (or: nonsense answered with
  nothing and a message). **1**: the top 5 satisfy but under half of the matching cars are reachable, or the top 5 are mostly wrong while
  the intent cars sit inside the result set, or a stated human override (D4). **0**: nothing relevant is reachable (no results although cars
  exist, or none of the results is relevant).
- "Reachable" uses the **local uncapped filter set** (`search_cars(..., limit=None)`) against the intent predicate on the local area
  (column "filter recall" below); "area truth" is the number of cars in the live 50-mile area (13,298 cars, the payload of
  `/api/listings/cars`) that satisfy the predicate. Truth for text features is a lower bound, because only 8,078 of the 13,298 live ids
  exist in the local snapshot that supplies listing text.
- Comparison with the first pass (`stats_report.py` -> `stats_report.json`): parse JSON identical for 60/60 queries, result counts identical 60/60, top-5 ids identical
  60/60; live parse GET median 312 ms (first pass) against 322 ms (this pass); `/listings?q` TTFB median 1,465 against 1,411 ms.

---

## 4. Results

### 4.1 By category

| Category | Queries | Parse | Relevance | Share of points |
|---|---|---|---|---|
| make / model / trim | 7 | 8 / 14 | 8 / 14 | 57% |
| misspelling | 6 | 9 / 12 | 9 / 12 | 75% |
| price / mileage / year range | 8 | 8 / 16 | 8 / 16 | 50% |
| body style | 5 | 9 / 10 | 6 / 10 | 75% |
| drivetrain | 4 | 3 / 8 | 3 / 8 | 38% |
| fuel | 5 | 8 / 10 | 7 / 10 | 75% |
| colour | 4 | 6 / 8 | 6 / 8 | 75% |
| feature | 5 | 8 / 10 | 8 / 10 | 80% |
| condition | 3 | 5 / 6 | 6 / 6 | 92% |
| location words | 3 | 3 / 6 | 5 / 6 | 67% |
| natural language | 5 | 2 / 10 | 2 / 10 | 20% |
| the site's own examples | 3 | 0 / 6 | 0 / 6 | 0% |
| nonsense | 2 | 4 / 4 | 4 / 4 | 100% |

Parse: 31 queries at 2, 11 at 1, 18 at 0. Relevance: 30 at 2, 12 at 1, 18 at 0. Queries per failure class (a query can carry two):
F1 5, F2 4, F3 4, F4 16, F5 7, F6 1, F7 5, F8 3, F9 1, F11 2 (`score_summary.json`; F10 and F12 are not visible in a one-query score).

### 4.2 The 60 queries

Columns: **live parse** = the filters `GET /api/search/smart/parse` returned. **live results** = cars in the `/listings?q=` grid (cap 200)
and, in brackets, how many of the top 5 satisfy the intent. **area truth** = cars in the live area that satisfy the intent.
**filter recall** = share of the local area's matching cars that the parsed filters reach (uncapped). **ms** = live parse GET total /
live `/listings?q` TTFB / local POST path server time (median of 3). **P**, **R** = parse and relevance scores. **class** = failure
classes of section 5.

| id | query | live parse | live results (top 5 ok) | area truth | filter recall | ms parse / TTFB / POST | P | R | class | one-line reason |
|---|---|---|---|---|---|---|---|---|---|---|
| A1 | `toyota camry` | make="Toyota"; model="Camry" | 200 (5/5) | 347 | 100% | 163 / 1688 / 93 | 2 | 2 | - | make+model exact; top 5 Camry, 200 shown of 347 in the area (grid cap) |
| A2 | `honda cr-v` | make="Honda"; model="CR-V" | 200 (5/5) | 344 | 100% | 218 / 1379 / 93 | 2 | 2 | - | make+model exact; top 5 CR-V / CR-V Hybrid, 200 shown of 344 |
| A3 | `f-150 lariat` | make="Ford"; model="F-150 Lariat" | 0 (0/0) | 71 | 0% | 284 / 1241 / 118 | 0 | 0 | F1 | model='F-150 Lariat' is a junk vocabulary row (one retired listing); 0 cars while 71 F-150 Lariats are in the area |
| A4 | `jeep wrangler rubicon` | make="Jeep"; model="Wrangler Rubicon" | 0 (0/0) | 45 | 0% | 153 / 1221 / 144 | 0 | 0 | F1 | model='Wrangler Rubicon' is a junk vocabulary row; 0 cars while 45 Rubicons are in the area |
| A5 | `mercedes gle 350` | make="Mercedes-Benz"; model="GLE"; trim_contains="350" | 37 (5/5) | 38 | 98% | 149 / 985 / 72 | 2 | 2 | - | Mercedes line code -> GLE + trim 350; 37 cars (38 in area), top 5 all GLE 350 |
| A6 | `bmw i4` | cylinders=4; make="BMW" | 200 (0/5) | 19 | 0% | 255 / 1241 / 81 | 0 | 0 | F6 | 'i4' read as 4 cylinders, model lost; 200 BMWs, none an i4 (19 i4 in the area; staged 1.5.16 changes the regex) |
| A7 | `4T1G11AK6MU520618` | {} | 1 (1/1) | 1 | 100% | 394 / 1736 / 188 | 2 | 2 | - | 17-character VIN skips the parser (no filters) and returns exactly the pasted car |
| B1 | `toyta camry` | make="Toyota"; model="Camry" | 200 (5/5) | 347 | 100% | 1132 / 1294 / 180 | 2 | 2 | - | make typo absorbed because the model is spelled right; top 5 Camry |
| B2 | `hundai tucson` | make="Hyundai"; model="Tucson" | 51 (5/5) | 51 | 100% | 506 / 1735 / 161 | 2 | 2 | - | make typo absorbed; 51 cars = every Tucson in the area |
| B3 | `chevy silverdo` | make="Chevrolet"; model="Chevy Van" | 0 (0/0) | 963 | 0% | 544 / 1532 / 159 | 0 | 0 | F1,F2 | 'silverdo' fuzzy-matched the junk model 'Chevy Van'; 0 cars while 963 Silverados are in the area |
| B4 | `mercedez benz c300` | make="Mercedes-Benz"; model="C-Class"; trim_contains="300" | 28 (5/5) | 29 | 100% | 173 / 1121 / 88 | 2 | 2 | - | 'c300' -> C-Class + trim 300; 28 of 29 C 300 |
| B5 | `porshe cayenne` | make="Porsche"; model="Cayenne" | 11 (5/5) | 11 | 100% | 334 / 1514 / 184 | 2 | 2 | - | make typo absorbed; 11 cars = all Cayennes |
| B6 | `nissan rouge` | make="Nissan" | 200 (0/5) | 85 | 100% | 466 / 1532 / 180 | 1 | 1 | F2 | make kept, model typo dropped by the literal-token guard; 200 Nissans, top 5 none Rogue (49 Rogues inside the 200) |
| C1 | `under $25,000` | max_price=25000 | 200 (5/5) | 2079 | 100% | 245 / 2790 / 152 | 2 | 2 | F8 | price cap parsed; top 5 all <= $25k (200 shown of 2,079) |
| C2 | `honda civic under 20k` | make="Honda"; max_price=20000; model="Civic" | 18 (5/5) | 16 | 100% | 148 / 1117 / 33 | 2 | 2 | - | make+model+cap; 18 cars, top 5 Civics <= $20k |
| C3 | `between 20k and 30k` | max_price=20000 | 200 (0/5) | 2783 | 0% | 371 / 1434 / 196 | 0 | 0 | F3 | 'between 20k and 30k' -> max_price 20000 (the schema has no lower bound); 200 cars all <= $20k, none in the band |
| C4 | `over 50k` | max_price=50000; trim_contains="over" | 22 (0/5) | 4247 | 0% | 353 / 1532 / 260 | 0 | 0 | F3,F4 | 'over 50k' -> max_price 50000 + trim 'over'; 22 Jeep Overland trims <= $50k, the inverse of the intent |
| C5 | `under 30,000 miles` | max_mileage=30000; max_price=30 | 0 (0/0) | 8629 | 0% | 526 / 1589 / 208 | 0 | 0 | F3 | '30,000 miles' also sets max_price=30 (comma backtrack); 0 cars while 8,629 have <= 30k miles |
| C6 | `2020 or newer tacoma` | make="Toyota"; min_year=2020; model="Tacoma" | 200 (5/5) | 456 | 100% | 722 / 1357 / 182 | 2 | 2 | - | '2020 or newer' supported; top 5 Tacoma 2020+ |
| C7 | `2018-2021 4runner` | make="Toyota"; max_year=2021; min_year=2018; model="4Runner" | 22 (5/5) | 22 | 100% | 343 / 1519 / 156 | 2 | 2 | - | year range + model; 22 cars = all |
| C8 | `newer than 2019 ram 1500` | body_style=["1500"]; make="Ram"; max_year=2019; min_year=2019; model="1500" | 0 (0/0) | 140 | 0% | 337 / 1705 / 203 | 0 | 0 | F1,F3 | 'newer than 2019' -> year=2019 only, body_style='1500' junk label; 0 cars while 140 Ram 1500 2020+ exist |
| D1 | `suv` | body_style=["SUV", "Sport Utility Vehicle [SUV]/Multipurpose Vehicle [MPV]", "+1"] | 200 (5/5) | 6778 | 97% | 146 / 1650 / 168 | 2 | 2 | F8,F9 | SUV labels (raw DB strings shown as chips); top 5 SUVs, 200 shown of 6,778 |
| D2 | `pickup truck` | body_style=["Pickup", "Truck", "+6"]; packages_json_contains="truck" | 200 (5/5) | 3594 | 14% | 600 / 1630 / 167 | 1 | 1 | F4 | truck labels plus an AND text needle 'truck'; top 5 right but only 14% of local pickups reachable (364 of 2,665) |
| D3 | `minivan` | body_style=["Minivan", "Minivan - 2WD"] | 52 (5/5) | 288 | 3% | 125 / 1098 / 78 | 2 | 1 | F7 | Minivan labels; 52 of 283 minivans reachable, most are labelled 'Van'/'Vans' (local recall 3%) |
| D4 | `convertible` | body_style=["Convertible", "Convertible/Cabriolet"] | 129 (5/5) | 140 | 94% | 162 / 1120 / 83 | 2 | 1 | F7,F8 | Convertible label; 129 cars = all labelled, but 43 are Ford Broncos and the top 5 are all Broncos (override from 2) |
| D5 | `sedan under 25k` | body_style=["Sedan", "Sedan/Saloon"]; max_price=25000 | 200 (5/5) | 577 | 26% | 137 / 1386 / 68 | 2 | 1 | F7 | Sedan labels + cap; top 5 right, sedans labelled 'Cars' unreachable (local recall 26%) |
| E1 | `awd suv` | body_style=["SUV", "Sport Utility Vehicle [SUV]/Multipurpose Vehicle [MPV]", "+1"]; drivetrain=["AWD", "4WD"] | 200 (5/5) | 4811 | 93% | 155 / 1387 / 193 | 2 | 2 | - | AWD/4WD + SUV; top 5 AWD SUVs |
| E2 | `4x4 truck` | body_style=["Truck", "Small Pickup Trucks 2WD", "+4"]; drivetrain=["AWD", "4WD"]; packages_json_contains="truck" | 200 (5/5) | 2994 | 12% | 170 / 1606 / 151 | 1 | 1 | F4 | extra AND needle 'truck'; top 5 right, local recall 12% |
| E3 | `rear wheel drive` | drivetrain=["RWD"]; trim_contains="rear wheel drive" | 0 (0/0) | 1462 | 0% | 315 / 1095 / 146 | 0 | 0 | F4 | RWD + trim_contains='rear wheel drive'; 0 cars while 1,462 RWD cars are in the area |
| E4 | `mercedes 4matic` | body_style=["4MATIC"]; drivetrain=["AWD", "4WD"]; make="MERCEDES" | 0 (0/0) | 205 | 0% | 357 / 1264 / 169 | 0 | 0 | F1 | make 'MERCEDES' (junk variant) + body_style '4MATIC'; 0 cars while 205 Mercedes AWD are in the area |
| F1 | `hybrid` | fuel_type="hybrid" | 200 (5/5) | 2077 | 92% | 190 / 1570 / 158 | 2 | 2 | - | fuel hybrid; top 5 hybrids |
| F2 | `plug-in hybrid suv` | body_style=["SUV", "Sport Utility Vehicle [SUV]/Multipurpose Vehicle [MPV]", "+1"]; fuel_type="plug-in hybrid" | 148 (5/5) | 150 | 100% | 146 / 1050 / 36 | 2 | 2 | - | plug-in hybrid + SUV; 148 of 150 |
| F3 | `electric` | fuel_type="electric" | 200 (5/5) | 277 | 100% | 146 / 1186 / 47 | 2 | 2 | - | fuel electric; top 5 EVs |
| F4 | `diesel truck` | body_style=["Diesel", "Truck", "+5"]; fuel_type="diesel"; packages_json_contains="truck" | 200 (5/5) | 642 | 15% | 128 / 1387 / 92 | 1 | 1 | F4 | Diesel body label + AND needle 'truck'; top 5 right, local recall 15% |
| F5 | `mild hybrid` | fuel_type="hybrid" | 200 (0/5) | 19 | 18% | 1092 / 1548 / 299 | 1 | 0 | F7 | 'mild hybrid' -> full hybrid; mild hybrids are stored as Gasoline; top 5 full hybrids, no mild hybrid |
| G1 | `white rav4` | exterior_color=["white"]; make="Toyota"; model="RAV4" | 16 (5/5) | 16 | 100% | 167 / 1306 / 98 | 2 | 2 | - | paint family + model; 16 cars = all |
| G2 | `black interior` | interior_color=["black"]; packages_json_contains_all=["interior", "black"] | 200 (5/5) | 9327 | 2% | 148 / 9376 / 1304 | 1 | 1 | F4 | interior black + AND needles 'interior','black'; top 5 right but 156 of 6,896 local black-interior cars (2%) |
| G3 | `red mustang` | exterior_color=["red"]; make="Ford"; model="Mustang" | 12 (5/5) | 12 | 100% | 148 / 938 / 89 | 2 | 2 | F2 | paint family + model; 12 = all (2 of the top 5 are Mustang Mach-E, model-prefix expansion) |
| G4 | `blue suv with tan leather` | body_style=["SUV", "Sport Utility Vehicle [SUV]/Multipurpose Vehicle [MPV]", "+1"]; exterior_color=["blue"]; packages_json_contains_all=["leather", "tan"] | 137 (2/5) | 64 | 12% | 393 / 2464 / 518 | 1 | 1 | F4 | 'tan' became a text needle, not an interior colour; 2 of top 5 have tan/beige inside, #1 is a $2,484 QX80 |
| H1 | `third row suv` | body_style=["SUV", "Sport Utility Vehicle [SUV]/Multipurpose Vehicle [MPV]", "+1"]; packages_json_contains="third row" | 103 (5/5) | 2172 | 0% | 160 / 1443 / 207 | 2 | 1 | F7 | SUV + needle 'third row'; top 5 right but 103 of ~2,172 three-row SUVs (literal phrase only) |
| H2 | `sunroof` | packages_json_contains="sunroof" | 200 (5/5) | 2142 | 79% | 133 / 3519 / 959 | 2 | 2 | F11 | needle 'sunroof'; top 5 right; 3.5 s live TTFB |
| H3 | `apple carplay` | packages_json_contains="apple carplay" | 200 (5/5) | 3289 | 100% | 138 / 2744 / 872 | 2 | 2 | - | needle 'apple carplay'; top 5 right; 2.7 s live TTFB |
| H4 | `tow package` | packages_json_contains="tow"; trim_contains="tow" | 4 (4/4) | 2002 | 0% | 378 / 1327 / 157 | 0 | 1 | F4 | adds trim_contains='tow' (matches 'RTL Towing'); 4 cars vs 2,002 tow-equipped in the area |
| H5 | `heated seats and remote start` | packages_json_contains_all=["remote start", "heated"] | 200 (5/5) | 1368 | 100% | 940 / 3677 / 1154 | 2 | 2 | F11 | two AND needles; top 5 right; 3.7 s live TTFB |
| I1 | `new tacoma` | inventory_condition="new"; make="Toyota"; model="Tacoma" | 200 (5/5) | 336 | 100% | 158 / 1153 / 93 | 2 | 2 | - | condition new + model; top 5 new Tacomas |
| I2 | `used bmw` | inventory_condition="pre_owned"; make="BMW" | 200 (5/5) | 546 | 100% | 148 / 1646 / 93 | 2 | 2 | - | condition used + make; top 5 used BMWs |
| I3 | `certified pre-owned lexus` | inventory_condition="cpo"; make="Lexus"; packages_json_contains="lexus" | 86 (5/5) | 86 | 100% | 158 / 1355 / 72 | 1 | 2 | F4 | extra text needle 'lexus' (harmless here); 86 = every CPO Lexus |
| J1 | `honda near charlotte` | make="Honda" | 200 (5/5) | 1432 | 100% | 446 / 1709 / 235 | 1 | 2 | F5 | 'near charlotte' ignored (harmless: the ZIP field is Charlotte); top 5 Hondas |
| J2 | `trucks in matthews nc` | body_style=["Trucks"]; packages_json_contains="truck" | 200 (5/5) | 3594 | 14% | 386 / 2034 / 290 | 1 | 1 | F4,F5 | location ignored; 'Trucks' label + 'truck' needle; top 5 right, local recall 14% |
| J3 | `toyota dealer in concord` | make="Toyota" | 200 (5/5) | 2510 | 100% | 362 / 1568 / 245 | 1 | 2 | F5 | 'in concord' ignored; top 5 Toyotas |
| K1 | `family suv under 30k with good mpg` | body_style=["SUV", "Sport Utility Vehicle [SUV]/Multipurpose Vehicle [MPV]", "+1"]; fuel_type="hybrid"; max_price=30000; packages_json_contains="good" | 0 (0/0) | 571 | 0% | 864 / 1075 / 199 | 0 | 0 | F4,F5 | SUV/price/hybrid parsed, but 'good' became an AND needle; 0 cars while 571 efficient SUVs <= $30k exist |
| K2 | `cheap reliable commuter` | {} | 0 (0/0) | 131 | 0% | 485 / 1159 / 119 | 0 | 0 | F5 | nothing parsed (no vocabulary for cheap/reliable/commuter); empty + generic message, 131 plausible cars |
| K3 | `fast car for a teenager` | body_style=["Car", "CAR"]; trim_contains="car" | 0 (0/0) | 94 | 0% | 328 / 1350 / 193 | 0 | 0 | F4,F5 | 'car' -> body_style 'Car'/'CAR' + trim 'car'; 0 cars, 94 plausible cars |
| K4 | `Something reliable for a first car, automatic, under $20k, low miles if possible` | body_style=["Car", "CAR"]; max_price=20000; packages_json_contains="automatic"; trim_contains="automatic" | 0 (0/0) | 151 | 0% | 443 / 1069 / 158 | 0 | 0 | F4,F5 | 'automatic' -> trim + needle (transmission is never parsed), 'first car' -> body 'Car'; 0 cars, 151 plausible cars |
| K5 | `gas saver` | fuel_type="hybrid" | 200 (5/5) | 4084 | 46% | 171 / 1626 / 161 | 2 | 2 | - | designed synonym gas saver -> hybrid; top 5 hybrids |
| M1 | `Accord with heated seats under $30k` | max_price=30000; packages_json_contains="heated"; trim_contains="accord" | 0 (0/0) | 8 | 0% | 378 / 1157 / 175 | 0 | 0 | F2,F4 | 'Accord with ...' fuzzy-hits 'BMW i3 with Range Extender'; model lost, trim_contains='accord'; 0 cars, 95 Accords <= $30k exist |
| M2 | `BMW X5 with M Sport under $70k AWD under 50k miles` | drivetrain=["AWD", "4WD"]; make="BMW"; max_mileage=50000; max_price=70000; model="X5"; packages_json_contains="m sport"; trim_contains="sport" | 0 (0/0) | 0 | - | 332 / 1893 / 155 | 0 | 0 | F4 | make/model/price/miles/AWD right, but adds trim_contains='sport' + needle 'm sport'; 0 cars while 26 X5 <= $70k AWD <= 50k mi exist |
| M3 | `Silverado crew cab with tow package under 80k miles` | make="Chevrolet"; max_mileage=80000; model="Silverado"; packages_json_contains="silverado crew cab with tow"; trim_contains="tow" | 0 (0/0) | 15 | 0% | 437 / 1276 / 279 | 0 | 0 | F4 | trim 'tow' + needle 'silverado crew cab with tow'; 'crew cab' unmapped; 0 cars while 895 Silverados <= 80k mi exist |
| L1 | `asdf qwerty` | {} | 0 (0/0) | - | - | 1132 / 1333 / 117 | 2 | 2 | - | nothing parsed; empty + 'No vehicles matched' message |
| L2 | `banana` | {} | 0 (0/0) | - | - | 498 / 1710 / 117 | 2 | 2 | - | nothing parsed; empty + message |

The top 5 cars of every query, as judged, are in Appendix A.

### 4.3 More probes

**Supplementary live parses** (`run_live2.py`, extras X1-X6):

| id | query | live parse | result |
|---|---|---|---|
| X1 | `red toyota camry` | exterior_color red, Toyota, Camry | 43 cars (live grid): 14 `Supersonic Red`, 13 `Predawn Gray Mica` (30%, gray cars filed as red), 16 other reds |
| X2 | `gray honda civic` | exterior_color gray, Honda, Civic | 62 cars: 35 `Platinum White Pearl`, 27 silver names, 0 with gray in the name (30 gray-named Civics are in the area) |
| X3 | `honda crv` | make Honda only | model dropped (no hyphen) |
| X4 | `toyota rav 4` | make Toyota, **model "Toyota"** | local replay: 3 cars |
| X5 | `one owner` | make **Mullen**, model **One** | an EV maker; local replay 0 cars |
| X6 | `2020+ tacoma` | min_year 2020, **max_year 2020**, Toyota Tacoma | local replay 4 cars (2020 only) |

**The examples the site shows or contains** (`shown_examples_local.py`, local parse and POST path, 28202 / 50 mi):

| Where | Query | Cars | Parse (abridged) |
|---|---|---|---|
| landing placeholder, `landing.html:28` | White BMW X5 with M Sport under $70k | 0 | white, BMW, trim_contains "sport", needle "m sport"; **model X5 lost** |
| top-bar placeholder, `_nav_search.html:12` | Accord with sunroof under $30k | 0 | trim_contains "accord", needle "sunroof"; **model and make lost** |
| landing sample, `landing.html:56` | Something reliable for a first car, automatic, under $20k, low miles if possible | 0 | body_style Car/CAR, trim and needle "automatic" |
| `listings.js:402` hint 1 (never displayed) | Accord with heated seats under $30k | 0 | trim_contains "accord", needle "heated" |
| hint 2 | BMW X5 with M Sport under $70k AWD under 50k miles | 0 | adds trim_contains "sport" |
| hint 3 | Silverado crew cab with tow package under 80k miles | 0 | trim "tow", needle "silverado crew cab with tow" |
| hint 4 | White RAV4 with sunroof hybrid under $35k | 14 | **model RAV4 lost**: any white hybrid with a sunroof |
| sr-only hint on `/listings` | white BMW X5 under $70k | 17 | correct |
| (variant of hint 1) | Honda Accord with heated seats under $30k | 9 | correct: adding the make keeps the model |

**Sweeps over the local parser** (`alias_sweep.py`, `model_sweep2.py`; local snapshot):

- *67 spellings of 31 popular models* (`honda crv`, `f150`, `rav 4`, `mazda3`, `toyta`, `camery`...): 38 resolve to the right model (57%),
  11 keep only the make, 5 parse to nothing, **12 pick a wrong model** (`toyota rav 4`, `toyota camery`, `toyota corola`, `toyota tocoma` -> model
  "Toyota"; `mazda cx5` -> "MAZDA 3"; `chevy equinoxe` -> "Chevy Van"; `bmw 330i` -> "330i"), 1 wrong make (`dodge ram`).
- *54 phrasings of price, mileage and year*: 23 right, 21 wrong, 7 meanings the schema cannot hold that are turned into a wrong filter
  (`over 20k` -> cap $20,000; `between 15k and 20k` -> cap $15,000), 3 clean refusals. Examples: `under 50,000 miles` -> price $50 and
  mileage 50,000; `under 50k mi` -> **price** $50,000; `50k miles or less` -> nothing; `2020+`, `since 2020`, `after 2019`,
  `newer than 2019` -> that single year; `before 2015`, `older than 2015`, `2015 or older` -> **min and max 2015**.
- *The 40 most listed models in the area* (53.5% of the area's cars, 7,110 of 13,298), typed as `make model`: 30 resolve cleanly
  (71.1% of those cars), 6 return the model plus sibling models (13.5%: `Bronco` also returns 104 `Bronco Sport` among 294 results; `Corolla`
  36 sibling models such as `Corolla Cross`), **3 return nothing (14.2% = 1,008 cars: Silverado 1500, Sierra 1500, Ram 1500)**, 1 returns the
  wrong set (`Highlander`: 39 of 84 results are `Grand Highlander` models). Confirmed on production for `chevrolet silverado 1500` and
  `gmc sierra 1500` (live parse contains `body_style ["1500"]`) and, by C8, for Ram 1500; `chevrolet silverado 1500` returned 0 cars.

---

## 5. Failure classes and the code path behind each

| Class | Name | Queries | Responsible code |
|---|---|---|---|
| F1 | Vocabulary pollution: retired or junk strings are treated as real makes, models, body styles | A3 A4 B3 C8 E4, 1500 trucks, X4 | `query_parser.py:116-171` (`_load_inventory_keywords`), `:287-330`, `:1061-1158` |
| F2 | Model matching: ties, the literal-token guard and its fallback, no aliases, sub-model conflation | B3 B6 G3 M1, 67-sweep, X3 X4 | `query_parser.py:1083-1086`, `:1115-1130`, `:564-591`; `hybrid_search.py:78-131` |
| F3 | Numbers and ranges: grammar gaps, no lower bounds | C3 C4 C5 C8, 54-sweep, X6 | `query_parser.py:482-535`, `:1161-1174`; `search_repo.py:65-92`; `where.py` `add_price_mileage_caps` |
| F4 | Ordinary words become hard AND filters (equipment needles, trim hints, body labels) | 16 queries | `query_parser.py:726-763`, `:883-926`, `:929-954`; `where.py` `add_equipment`, `add_trim_needles` |
| F5 | Natural-language intent and location words are not understood | K1-K4, J1-J3, X5 | `query_parser.py:656-662` (`_FUEL_RE`), `:260-284`; `hybrid_search.py:575-590` |
| F6 | Greedy single-token rules (`i4` -> 4 cylinders, `mpg` -> hybrid) | A6, K1 | `query_parser.py:616-627`, `:656-662` (the `i4` rule is fixed in the staged tip, 13.4) |
| F7 | Data taxonomy gaps: body labels, fuel, features, colour families, implausible prices | D3 D4 D5 F5 H1 G*, X1 X2 | `utils/interior_color_buckets.py:127` (word boundaries added in the staged tip, 13.4); `utils/field_clean.py` (`fuel_types_for_filter`, `body_styles_for_filter`) |
| F8 | Result window and ordering: nearest 500, then cheapest-with-best-data | D1 D4 C1 and most broad queries | `search_repo.py:56`, `search/ranking.py:28`, `hybrid_search.py:671-703`, `listings/routes.py:31` |
| F9 | Zero results: no recovery, raw labels on screen | all 14 wrong zeros | `listings_api.py:612-614`, `main.js:1401-1470`, `listings.js:95-175` |
| F10 | Page state: stale selects, snapping, typing clears ticked filters | any 2nd query | `main.js:1819-1893`, `listings/filters.js:245-262`, `listings.js:329-380` |
| F11 | Cost per keystroke, limiter, cold start, analytics write per POST | all | `listings.js:13,225-327`, `listings_api.py:529-548,590-606`, `config.py:55`, `query_parser.py:98-107,906`, `search_analytics_db.py:107-154`, `users_sqlite.py:44-61` |
| F12 | The semantic layer is off, and the GET path would break if it were on | K2 | `hybrid_search.py:36-47,375-457,575-590,729-802`, `pgvector_service.py:205-237` |

### 5.1 F1 Vocabulary pollution

*Symptom.* `f-150 lariat` -> model "F-150 Lariat" (0 cars, 71 exist); `jeep wrangler rubicon` -> model "Wrangler Rubicon" (0 cars, 45 exist);
`newer than 2019 ram 1500`, `chevrolet silverado 1500`, `gmc sierra 1500` -> body_style ["1500"] (0 cars); `mercedes 4matic` -> make "MERCEDES"
plus body_style "4MATIC" (0 cars, 205 exist); `chevy silverdo` -> model "Chevy Van" (0 cars, 963 Silverados exist, F2 explains why).
Inventory-weighted effect: 1,008 of 13,298 area cars (7.6%) sit in the three 1500 truck lines that cannot be found by name (section 4.3).

*Cause.* The parser learns its vocabulary from the whole `cars` table, retired rows included, and keeps it for the life of the process:

- `query_parser.py:125` `SELECT DISTINCT make, model FROM cars ...`, `:155` `SELECT DISTINCT body_style FROM cars`, `:163`
  `SELECT DISTINCT TRIM(trim) ...`: no `listing_active` filter and no minimum row count (only the colour query at `:136-138` filters active rows).
- `:116` `@lru_cache(maxsize=4)` keyed by `_inventory_db_cache_key()` (`:98-107`), which is a file `stat` of a SQLite path and does not
  change when Postgres data changes (verified: the key was identical across calls in one process). So the vocabulary is built once per
  process and never refreshed: a model that first appears in tomorrow's scan is unknown until the next restart.
- `_match_body_style_filters` (`:287-330`) turns every distinct `body_style` string that appears as a word in the query into a filter,
  which is how the label `1500` (a trim-like value) filters `ram 1500`.
- `_parse_single_vehicle_identity` (`:1061-1158`) runs a fuzzy match over all labels (`:1083-1086`) and accepts a pair if its words
  appear in the query (`_model_appears_in_text`, `:564-591`), so `F-150 Lariat` passes because the query literally contains it (WRatio 95
  for `Ford F-150 Lariat`, `trace_parse.log`).

*Local numbers* (`db_facts.py`, local snapshot): 357,149 rows, 214,675 active; make/model pairs 2,974 against 2,380 among active rows;
body styles 694 against 493; trims 7,276 against 5,437. Examples: `(Ford, F-150 Lariat)` 1 row, 0 active; `(Chevrolet, Chevy Van)` 1 row,
0 active; `(Toyota, CAMRY LE/XLE/SE)` 1 row, 0 active; `(Ford, F150)` 2,666 rows, 3 active; `(Toyota, Toyota)` 1 row, 1 active;
`(Mullen, One)` 8 active (a real EV maker, which `one owner` matches). Production's own junk differs (its parse of C8 returned
body_style `1500`, its parse of E4 make `MERCEDES`; neither has an active row in the local snapshot).

*Sandbox* (section 7): an active-only vocabulary shrinks pairs 2,974 -> 2,380, body styles 694 -> 493, trims 7,276 -> 5,437; with minimum
counts (pair 3 rows, body style 100, trim 3) it is 1,303 / 59 / 2,474; `f-150 lariat` becomes Ford F-150 + trim "lariat" (F-score 0 -> 1.0),
`jeep wrangler rubicon` likewise, `newer than 2019 ram 1500` reaches 1.0.

*Fix.* Build the vocabulary from active rows with a minimum count, drop numeric-only and non-body-style `body_style` strings, give the
cache a TTL or a data-version key, and add plural body words to `_BODY_STYLE_CUES` (the sandbox regressed `trucks in matthews nc` because
the plural currently works only through the junk label `Trucks`). Effort S to M, risk low to medium.

### 5.2 F2 Model matching

- **Ties.** `process.extractOne(working, pair_labels, scorer=WRatio, score_cutoff=75)` (`:1083-1086`) scores any label that contains the
  query word the same and returns the first of the ties, in alphabetical make order. For `Accord with heated seats under $30k` the text
  handed to the matcher is `accord with` and `BMW i3 with Range Extender` ties at 86 with five Honda Accord labels (`trace_parse.log`);
  BMW is first. A later check rejects it (its other words are not in the query) and no second candidate is tried, so "accord" never becomes a model
  and falls through to `trim_contains` (M1: 0 cars, 95 Accords under $30k exist in the area). `camry under 20k under 60k miles` returns model
  "Camry Hybrid" and so skips the other Camrys (`alias_sweep.py`, numbers part).
- **The guard rejects the right candidate and a fallback picks a junk one.** `chevy silverdo`: the best candidate is `Chevrolet Silverado`
  (WRatio 97) but `_model_appears_in_text` (`:564-591`) and `_all_nonnum_dist_in_query` (`:1115-1128`) require the model's words to be
  literally in the query, so it is rejected; the fallback (`:1130`, `best_model = min(valid, key=len)`) takes the shortest model of the make
  that does contain a literal query word, and the make's slang "chevy" is in the junk model `Chevy Van`. The same path gives `toyota rav 4`
  (best `Toyota RAV4` 96, rejected because the query says "rav 4") -> model "Toyota", and `toyota camery`, `toyota corola`, `toyota tocoma`.
- **Misspelt models lose the model.** A misspelt make survives (the model carries it: `toyta camry`, `hundai tucson`, `porshe cayenne`); a
  misspelt model with a correct make keeps only the make: `nissan rouge`, `honda acord`, `ford mustnag`, `ford explorar`, `kia sportege`
  (B6: 200 Nissans, none of the top 5 a Rogue, 49 Rogues inside the 200).
- **No aliases.** `crv`, `rav 4`, `rav-4`, `mazda3`, `ram1500` parse to nothing; `mazda cx5` to "MAZDA 3"; `honda crv` to the make only (X3, live).
  Digit-only tokens are skipped as model candidates (`_extract_best_token`, `:67-95`).
- **Sub-model conflation.** `expand_inventory_models` (`hybrid_search.py:78-131`) accepts any model that equals, starts with or contains
  the typed word: `bronco` returns 190 Broncos and 104 Bronco Sport, `mustang` includes `Mustang Mach-E`, `highlander` includes
  `Grand Highlander` (39 of 84 results). In the live area: Bronco 175 against Bronco Sport 119, Mustang 88 against Mach-E 20,
  Highlander 89 against Grand Highlander 66.

*Fix.* Tie-break equal scores by inventory popularity, ignore stopwords for the fuzzy match, try the next candidate instead of a
shortest-name fallback, allow a model typo when exactly one model of the matched make is within one or two edits (and show "did you mean"),
add a small alias table (`crv`, `f150`, `rav 4`, `mazda3`), treat `Bronco Sport` as its own model. Sandbox V4 (popularity tie-break) alone
changed 2 parses and fixed none and cost 2 aliases; it only matters in combination (section 7). Effort M, risk medium.

### 5.3 F3 Numbers and ranges

- `_extract_max_price` (`:482-510`): every "N k" and "$N" is a cap and the smallest wins. There is no notion of over, between or a band:
  `between 20k and 30k` -> cap $20,000 (below the lower bound), `over 50k` -> cap $50,000 plus `trim_contains "over"`.
- Comma backtrack: the first regex ends with a look-ahead `(?!\s*miles?\b)`; for `under 30,000 miles` the engine gives back digits until the
  look-ahead passes, matches `30,` and reads it as 30, so the query sets `max_price = 30` as well as `max_mileage = 30000`. The page shows
  the chips "Under $30" and "Under 30,000 mi" (`sq2-listings-1440-q2.png`) and 0 vehicles.
- `_extract_mileage_cap` (`:513-535`) needs `under/below/less than/max` before the number and the word `miles`: `under 50k mi` becomes a
  **price** cap; `50k miles or less`, `fewer than 50k miles`, `no more than 50,000 miles` set nothing.
- `_extract_years` (`:1161-1174`) knows `N or newer` and ranges; any other single year becomes min = max = that year: `2020+`, `since 2020`,
  `after 2019`, `newer than 2019`, and the inverse phrases `before 2015`, `older than 2015`, `2015 or older`.
- The schema has no lower bound: `search_cars` (`search_repo.py:65-92`) takes `max_price` and `max_mileage` only; the Price and Mileage
  selects on the page are caps too. Caps keep unpriced rows (`where.py` `add_price_mileage_caps`).

*Fix.* Normalise thousands separators, read units (`mi`, `miles`, `k`) before deciding price or mileage, add `over/above/at least/between` with
`min_price`/`min_mileage` in `SearchQuery`, `where.py` and the UI, add the year phrases above. Sandbox V2 (grammar only, no minimum
bound): 23/54 -> 41/54 phrasings correct; `under 30,000 miles` and `newer than 2019 ram 1500` reach F-score 1.0. Effort S for the grammar, M
for the lower bounds. Risk low (pure functions, one test each).

### 5.4 F4 Ordinary words become hard AND filters

Three sources, all ANDed in SQL:

1. **Equipment needles.** `_extract_all_package_search_terms` (`:726-763`). The loop at `:744-751` tests every one of 11,857 OEM option names
   with a plain substring (`nlow in low`); some option names are ordinary words: `TRUCK`, `INTERIOR`, `Black`, `COLOR`, `Automatic`,
   `Lexus`. So `pickup truck` gets the needle `truck`, `black interior` gets `interior` and `black`, `Chevrolet Colorado` gets `color`
   (substring of "colorado"). `_WITH_EQUIPMENT_RE` (`:716-719`, used at `:757-761`) turns the first word after "with" into a needle:
   `with good mpg` -> `good`, `with tan leather` -> `tan`. `where.py` `add_equipment` requires every needle as a substring in one of five columns.
2. **Trim hints.** `_extract_trim_hint` (`:883-926`) makes `trim_contains` out of any query word that is, or sits inside, one of 7,276 trim
   strings (`:906` and the reverse pass at `:916`): `tow`, `car`, `automatic`, `sport` (from "M Sport"), `accord`, `over`,
   `rear wheel drive`. `_strip_vehicle_identity_noise` (`:929-954`) strips `awd|4wd|rwd...` tokens but not the phrases
   `rear wheel drive`, `all wheel drive`, so E3 `rear wheel drive` gets RWD **and** `trim_contains "rear wheel drive"` (0 cars, 1,462 RWD exist).
3. **Body labels.** `car` equals the label `Car`/`CAR`: K3 `fast car for a teenager` and K4 (`first car`) get `body_style ["Car","CAR"]`.

*Measured.* Needle `truck` keeps 14% / 12% / 15% / 14% of local pickups for `pickup truck`, `4x4 truck`, `diesel truck`, `trucks in matthews nc`;
`black interior` reaches 156 of 6,896 black-interior cars (2%); `tow package` returns 4 Honda Passport "RTL Towing" of 2,002 tow-equipped
cars; `family suv under 30k with good mpg` returns 0 (571 efficient SUVs under $30k exist); `BMW X5 with M Sport under $70k AWD under 50k miles`
returns 0 (26 X5 at or under $70k, AWD, under 50k miles exist). 16 of 60 queries are affected.

*Fix.* ANDed text needles only for curated features (`_FEATURE_MAP`), explicit "x package" phrases and long multi-word option names;
unknown words are ignored or used for ranking and reported back as ignored; trim hints only when a make or model has been resolved
and never for common words. Sandbox V5 (needle policy) lifts the mean F-score 0.515 -> 0.588 alone, V6 (trim stoplist) to 0.550; together
with the rest to 0.718. Effort S to M, risk low to medium (some recall that today accidentally depends on a stray needle changes).

### 5.5 F5 Natural-language intent and location words

There is no vocabulary for `cheap`, `reliable`, `commuter`, `fast`, `teenager`, `first car`, `low miles`, `good mpg` (apart from the
designed synonyms in `_FUEL_RE`, `:656-662`: `mpg`, `fuel efficient`, `gas saver` -> hybrid). Unparsed words either vanish
(K2 `cheap reliable commuter` -> `{}` -> `no_parse_match`, the generic message, 131 plausible cars exist) or become filters (F4).
Transmission is never parsed (`automatic` -> trim and needle). `one owner` -> make Mullen (X5). Location words are ignored without a
word to the shopper: `honda near charlotte`, `trucks in matthews nc`, `toyota dealer in concord` return the ZIP field's area, which is
right only because the field already says Charlotte. The page shows the Honda chip and nothing about "charlotte".

*Fix.* A small intent lexicon that maps to filters and sorts (`cheap` -> price ascending, `low miles` -> mileage ascending, `commuter`
and `reliable` -> a curated model set, `automatic` -> transmission), geocoding of a place word into the ZIP, and an "ignored words"
line so the shopper sees what was not understood. Effort M, risk low.

### 5.6 F6 Greedy single-token rules

`_CYLINDER_RE` (`:616-627`) reads `i4` as inline-4: `bmw i4` -> `cylinders 4`, model lost (200 BMWs, none an i4; 19 i4 in the area);
the same applies to `i3`, `i5`, `i7`. The staged 1.5.16 changes the alternative to `(?:inline|straight)`. `mpg` in `_FUEL_RE` makes
`good mpg` a hybrid filter (K1). Effort S.

### 5.7 F7 Data taxonomy gaps

- **Body labels.** `minivan` maps to `Minivan` and `Minivan - 2WD` (`_BODY_STYLE_CUES`, `:260-284`). In the live area 283 cars are minivan
  models (Odyssey, Sienna, Pacifica, Carnival, Grand Caravan): 52 carry the label `Minivan`, 73 `Van`, 157 `Vans`, 1 `7 PASSENGER`. `sedan` maps
  to `Sedan` while 991 area cars are labelled `Cars` (10,506 active rows locally, 4.9%); D3 reaches 52 of 283 and D5 26% of sedans
  locally. `convertible` returns 129 cars of which 43 are Ford Broncos; the top 5 are Broncos.
- **Fuel.** Mild hybrids are stored as `Gasoline` (`fuel_types_for_filter` lists `Gasoline/Mild Electric Hybrid` under Gasoline) and nothing parses
  `mild hybrid`, so F5 returns full hybrids (0 of the top 5 is a mild hybrid).
- **Features.** There is no structured field for third row or towing: `third row suv` returns 103 of about 2,172 three-row SUVs in the
  area (literal phrase only), `tow package` 4 of 2,002.
- **Colour families.** `infer_interior_color_buckets` (`utils/interior_color_buckets.py:127`) matches a 33-phrase lexicon with a plain
  substring test (`if phrase in s`). Consequences measured on the local snapshot (`color_lexicon_experiment.py`, `db_facts.py`): 76,795
  of 212,206 active cars with a paint name (36%) are in `other`; the lexicon has no `gray`/`grey`, so 32,707 gray-named cars (15%) are
  mostly `other` (only 1,220 in `gray`); `red` is a substring of `Predawn` (167 `Predawn Gray Mica` cars are red), of `Inspired` and of
  `preferred`; `tan` is a substring of `Tanzanite` (486 blue cars are tan) and `titanium`. Live: X1 (13 of 43 gray Camrys under "red"),
  X2 (no gray-named Civic among 62 results). The facets "Red" and "Gray" in the sidebar share the same data.
  Re-measured on the live area in pass 3 (the 13,298-car payload carries `exterior_color_families`, and the 1.5.8 function reproduces it
  for all 13,298 cars): 1,950 cars (14.7%) have `gray` or `grey` in the paint name and 71 of them (3.6%) are in the gray family; 1,844 are
  only in `other`. The `tan` family holds 79 cars and 76 have `tan` only inside another word (`Tanzanite` 31, `Manhattan` 24, `Titanium` 14,
  `Octane`/`Tango` Red 7); the `red` family holds 724 cars and 20 of them are `Predawn Gray Mica`. `Platinum White Pearl` is in `silver`
  and `gray` because `platinum` is a gray phrase of the lexicon. The staged tip fixes the substring half only (section 13.4).
- **Prices.** 44 cars in the live area are listed at exactly $2,484, all from one dealer (27 flagged `payment_listed`, 17 not); sorted
  cheapest-first they surface as top results (G4 #1: a 2020 INFINITI QX80 at $2,484). 57 cars cost under $3,000: 39 are flagged
  `payment_listed` and 18 are not (17 at $2,484 and 1 at $900; pass 3 recount).

*Sandbox* (`color_lexicon_experiment.py`): word-boundary matching plus `gray`/`grey` takes `other` from 76,795 to 47,826 cars, puts all
32,707 gray-named cars in `gray`, and removes 373 red and 1,074 tan false positives (a caveat: `Blueprint`, 1,457 cars, is only blue by
substring and would need its own entry). Effort S for the lexicon, M to re-bucket stored values. Body labels and mild hybrids need an
ingest-time normaliser (model-aware: an Odyssey is a minivan whatever the feed says). Effort M.

### 5.8 F8 Result window and ordering

`search_cars` returns the **nearest 500** matches (`search_repo.py:56`; `search/ranking.py:28` ranks ids by distance and the rows are hydrated
up to the limit); the page's GET path embeds the nearest 200 (`listings/routes.py:31,228`). The POST path then re-sorts those rows by
photo and price flags, needle count, vector rank, data-quality score and price (`hybrid_search.py:671-703`), not by distance, relevance
or deal. Effects: `suv` shows "1-24 of 500" although the page itself holds 6,617 SUV-labelled cars (`sq2-cap-suv-1440.png`,
`pw_cap2.json`); 12 of 60 queries return exactly 500 locally; the top 5 of 7 broad queries differ between the GET and POST paths;
the top 5 are the cheapest of the nearest 500 that have photos and prices, e.g. `third row suv` #1 is a 2014 Ford Expedition EL with
211,813 miles at $6,984, and `blue suv with tan leather` #1 is the $2,484 QX80. Effort M (rank by a relevance score, show the true total,
plausibility-check prices at query time), risk medium (changes what every shopper sees first).

### 5.9 F9 Zero results: no recovery, raw labels on screen

When filters were parsed but nothing matches, the page says "0 vehicles near 28202" next to the chips; the generic empty message exists only for
the not-parsed case (`listings_api.py:612-614`, `NO_PARSE_MATCH_MESSAGE`). There is no "drop one word" suggestion, no nearest matches,
no word about which words were ignored. The chips print database labels: "Body: SUV, Sport Utility Vehicle [SUV]/Multipurpose Vehicle [MPV], SUVs",
"Body: Car, CAR", "Equipment: good" (`fillParseChips`, `listings.js:95-175`; `sq2-listings-1440-q4.png`). Effort M; the sibling report
`search-promotion.md` sketches the recovery rows. Risk low.

### 5.10 F10 Page state

Measured on the real page in one session, typing queries one after another (`pw_shots1440.json`, `pw_clobber2.json`):

- After `under 30,000 miles` the Price select is `25000` (the parse said $30, snapped to the smallest bracket that covers it) and Mileage `30000`.
  The next three queries leave them as they were (`f-150 lariat`: chips still "Under $25k" and "Under 30,000 mi"; `family suv under 30k...`: Price
  becomes `35000` for a parsed cap of $30,000; the long query: `25000` for $20,000). `__DS_applySmartFilters` only sets a scalar when the
  new parse has one and never resets it, and `scalarSelectValue` (`filters.js:245-262`) snaps to the next bracket. The parsed line and the
  chip row disagree, and later filter-based renders use the looser cap.
- Typing replaces ticked filters. With Make = Toyota ticked (2,510 results), typing `suv` unticked Toyota and showed 500 cars of all makes
  (`main.js:1822-1825` unticks every checkbox first). Search and filters do not compose.
- The count moves between interpretations: in the 22-character typing test the grid showed 0, then "1-24 of 92" at 0.21 s, then "1-24 of 49" at
  0.44 s (token preview, parse render, POST).

*Fix.* Reset scalars that the new parse does not set, add the exact value as an option instead of snapping, merge the parse into the ticked
filters or say clearly that it replaces them. Effort M, risk medium (the ZIP gate and filters live in the 2,771-line `main.js` closure that
`search-promotion.md` names as the regression risk).

### 5.11 F11 Cost per keystroke, limiter, cold start

- **Requests.** Typing a 22-character query at 180 ms and at 60 ms per key sent 22 parse GETs and 22 POSTs on desktop (`pw_storm1440.json`). On the phone
  emulation (CPU x4) 22 parse GETs and 3 POSTs completed (the page aborts the rest), with 41 and 40 long tasks totalling 5.3 s and 5.5 s of
  main-thread time in a 5.5 s typing run (max 212 ms and 255 ms; `pw_storm390.json`). The phone numbers are Chromium with CPU throttling, not a device.
- **Server work.** Replaying every prefix through the server code (`prefix_cost2.py`, warm vocabulary, one process): the 22-character
  `toyota camry under 30k` -> parse endpoint 1.4 s + POST 6.3 s = **7.7 s** (worst single POST 2.2 s); the 55-character
  `white toyota rav4 hybrid under 35k with sunroof and awd` -> 4.2 s + 15.9 s = 20.1 s; the 80-character landing sample -> 9.4 s + 16.2 s = 25.6 s.
  The browser aborts requests, the server finishes them.
- **Limiter.** Both endpoints share `smart:{ip}`, 90 events per 60 s (`config.py:55`, `listings_api.py:532,548`; sliding window in
  `utils/ip_rate_limit.py`, verified by calling it: 90 allowed, the 91st refused). One 22-character query is 44 events, so two typed queries in
  a minute use 88 and the third is cut after 2 more events. Simulated server answers in the real page (`pw_limit2.py`): the 55-character
  query typed at 120 ms per key sent 110 requests, 90 were answered and 20 got HTTP 429 from the 46th keystroke (`...with sunroo`); the page
  shows no error (the POST `catch` only stops the spinner, `listings.js:290,317-326`) and the grid stays at the last answered prefix
  (`sq2-limit-429-1440.png`). Pasting a query sends one pair and avoids the problem.
- **Cold start** (3 fresh processes, `cold_start.jsonl`): vocabulary load 938-1,004 ms, OEM package-name scan 544-1,041 ms, so the first search
  after every restart waits about 1.5 to 2.0 s; Python RSS 247-250 MB with the parser loaded. Afterwards a parse is 5.7 ms for `toyota camry` and
  79-82 ms for a query with trim and equipment words, because `_extract_trim_hint` (`:906`) compiles up to 7,276 regexes per call.
- **Slow SQL.** Equipment needles scan five text columns with `INSTR`: local POST 1,304 ms for `black interior`, 1,154 ms for `heated seats and remote
  start`, 959 ms for `sunroof`; live `/listings?q` TTFB 9,376 ms for `black interior` (first pass: 5,030 ms). Needle queries have a live median TTFB of 1,606 ms against
  1,386 ms for the others (17 against 43 queries).
- **Analytics write per POST (pass 3).** Before it answers, `api_search_smart` calls `record_search_event` (`listings_api.py:590-606`), which opens
  `users.db` with `get_users_conn()`, inserts one `search_events` row and commits (`search_analytics_db.py:107-154`). Production must open that
  file through SQLCipher (`credential_db_encryption.py`, SEC-088, and `_connect_sqlcipher`, `users_sqlite.py:44-61`), a new connection per call.
  Measured on throwaway files, never the real `users.db` (`run3/analytics_cost.py`): encrypted open + select + close median 42.2 ms,
  `record_search_event` median 44.1 ms (p90 44.5 ms); plain SQLite 0.38 ms and 1.74 ms; a 22-character typed query writes 22 rows in 1,077 ms
  (30 ms plain). The page sends one POST per keystroke, so every prefix becomes a row: in the typed replay of the 55-character
  `white toyota rav4 hybrid under 35k with sunroof and awd`, 29 of the 46 distinct prefix states return no cars (4 `no_parse_match`, 25 filters
  that match nothing; `run2/post_payloads_prefix.json`).
  The admin dashboard (`search_analytics_db.py:usage_summary`) counts `total_searches`, `zero_result_searches` and `top_queries` from these rows, so those
  numbers measure keystrokes, not searches. The figure is a laptop measurement; the Railway CPU and whether the key derivation releases the GIL
  are not measured.

*Fix.* One request per keystroke at most (merge parse and search), debounce 250-300 ms, a visible message on 429, build the vocabulary on a
background thread at start, index trims by token instead of compiling regexes, record one analytics event per settled query (after the
debounce, or on submit) instead of per prefix. Effort S to M, risk low.

### 5.12 F12 The semantic layer is off, and the GET path would break if it were on

- Production: `/api/ready` reports `pgvector: skipped`; live GET results reach 200 rows (for example `toyota camry`), more than the semantic path's
  top 100, so no semantic candidate set is being applied. (Whether `DATABASE_URL` is set on the web service, which would also enable the layer,
  cannot be read from outside; the behaviour is what shows it is not in effect.)
- Even with the layer on, free text without a parsed filter never reaches it: `query_is_actionable` (`hybrid_search.py:575-590`) returns false and
  `hybrid_smart_search` answers `no_parse_match` before any vector call (the docstring at `:729-745` says vague queries use semantic-first recall;
  the code path is unreachable for them). The only use left is re-ranking inside the SQL result when the parse has no make and no needle
  (`_should_skip_vector_rerank`, `:654-668`).
- Latent bug: if `PGVECTOR_URL` or `DATABASE_URL` is ever set on the web service, the GET path (`hybrid_search_with_kwargs`, `:439-457`)
  replaces the SQL set by the vector top 100 and applies only the request's facet parameters, not the parsed filters. Demonstrated with a
  stand-in candidate list (`semantic_path_demo.py`): `toyota camry` through the GET path returns 40 cars of which 10 are Camrys (Chevrolet Trax x9, Trailblazer x3...);
  the POST path still returns 148 Camrys.
- `query_cars` runs `ensure_schema` (CREATE EXTENSION / TABLE / INDEX IF NOT EXISTS) on every call (`pgvector_service.py:213`) and loads torch and the
  MiniLM model on first use.

*Fix.* Decide whether the layer is part of the product. If yes: use it as a re-ranker and as the fallback for unparsed text, fix the GET
path to apply parsed filters, move `ensure_schema` out of the request. If no: remove it from the docstrings and the readiness probe.
Effort S to remove, M to wire.

---

## 6. Latency summary

| Measurement | Value | Source |
|---|---|---|
| Live `GET /api/search/smart/parse` (what each keystroke sends), 60 queries | median 322 ms, p90 600 ms, min 125 ms, max 1,132 ms | `results_live.json` |
| Live `GET /listings?q=..&zip_code=28202&radius=50`, 60 queries | TTFB median 1,411 ms, p90 2,034 ms, max 9,376 ms; HTML 1.55-1.83 MB | `results_live.json` |
| Live `/listings?zip_code=28202&radius=50` with no `q` (baseline, one request) | TTFB 653 ms, 1,548,612 bytes | `live_requests.jsonl` (`pw-cache:listings`) |
| Live first-keystroke area download `/api/listings/cars?zip=28202&radius=50` | 14,545,187 bytes, TTFB 3,398 ms | `live_requests.jsonl` |
| Local parse (median of 5, warm) | median 64 ms, p90 113 ms, max 168 ms | `results_local.json` |
| Local POST path (parse + SQL + rank, median of 3) | median 159 ms, p90 290 ms, max 1,304 ms | `results_local.json` |
| Local POST payload | median 72 KB (7.7 KB gzip), p90 569 KB (52 KB), max 632 KB (63 KB); 12 of 60 queries hit the 500-car cap | `results_local.json` |
| Run-to-run, 12 hours apart | parse median 312 ms then 322 ms; TTFB median 1,465 then 1,411 ms | section 3.4 |

The live POST cannot be timed under the GET-only rule; the local figures are for a laptop, and the production worker is a single gunicorn
worker shared with every other request, so the per-keystroke work of section 5.11 competes with page loads.

---

## 7. Fix experiments (sandbox)

Nothing in the repository was edited. `sandbox_patches.py` installs each variant on the imported parser in-process (monkeypatch);
`fix_experiments.py` parses the 60 queries and 6 extras, runs the changed ones through the uncapped local filter set, and re-runs the 67-alias
and 54-number sweeps. The F-score here is the harmonic mean of precision and recall of the filter set against the intent predicate (the
JSON keys say `f1`); it is not a failure class.

| Variant | Mean F-score (57 queries) | Zero results although cars exist | Aliases right (of 67) | Numbers right (of 54) |
|---|---|---|---|---|
| V0 current 1.5.8 | 0.515 | 13 | 38 | 23 |
| V1a vocabulary from active rows only | 0.526 | 11 | 39 | 23 |
| V1b active rows, pair >= 3 rows, body style >= 100, trim >= 3 | 0.559 | 9 | 40 | 23 |
| V2 number grammar | 0.547 | 12 | 38 | 41 |
| V3 drivetrain phrases (`rear wheel drive` -> `rwd`) | 0.532 | 12 | 38 | 23 |
| V4 equal fuzzy scores broken by inventory popularity | 0.515 | 13 | 36 | 23 |
| V5 equipment-needle policy | 0.588 | 12 | 38 | 23 |
| V6 trim-hint stoplist | 0.550 | 11 | 38 | 23 |
| V1b + V2 + V3 | 0.627 | 8 | 40 | 41 |
| **V1b + V2 + V3 + V4 + V5 + V6** | **0.718** | **5** | 37 | 41 |

Under the full set 31 parses change, 15 queries improve by 0.1 or more and 1 regresses (`trucks in matthews nc`, because plural body words
are only recognised through the junk label `Trucks`). The 15: A3, A4, B3, C3, C5, C8, D2, E2, E3, E4, F4, G2, H4, K1, M1; largest gains: A3, A4, C5,
C8, E3, G2 (0.04 -> 1.0), H4 (0.004 -> 0.997), D2/E2/F4 (0.22-0.27 -> 0.99), C3 (0.003 -> 0.86), E4 (0 -> 0.72). Still wrong after the set: A6 (`i4`),
C4 (`over 50k`, no minimum bound), B3 (the make only), D3 and D5 (labels), H1 (third row), the natural-language queries K1-K4 and M3. The alias
figure falls from 40 to 37 in the full set; the popularity tie-break alone lowers it by 2 (36), so it needs tuning, not copying.
Colour lexicon: section 5.7.

---

## 8. Redis for the search bar

Production has no Redis configured (`/api/ready`: `redis: skipped`). Redis is not a quality lever: none of the 12 classes is a caching problem and a
cache would serve the wrong answers faster. It is a cost lever for what the keystroke storm does (section 5.11):

1. **Parse cache** keyed by normalised query text plus a vocabulary version. A parse costs median 64 ms (p90 113 ms) of single-interpreter CPU
   and every typed prefix is parsed twice (parse endpoint and POST). Prefixes repeat across shoppers (`toyota`, `toyota c`, `toyota camry`).
   Ceiling on saved work: the parse share of section 5.11, 1.4 s to 9.4 s per typed query. The hit rate is **not measured** (no usage logs).
2. **POST result cache** keyed by the hash of the parsed filters, ZIP, radius and the hidden-dealer set, short TTL: saves median 159 ms and
   up to 1.3 s of SQL plus the 72-632 KB JSON build per hit.
3. **Shared limiter**: only when workers exceed 1 (today `GUNICORN_WORKERS=1`); `utils/ip_rate_limit.py` is per process or SQLite.
4. **Vocabulary snapshot**: the 1.5-2.0 s cold start. A background build at start removes it without Redis.
5. **Analytics event buffer** (pass 3): the `search_events` INSERT per POST costs 44 ms with an encrypted connection (section 13.3). A Redis list
   push would take it off the request path, but it would still record keystroke prefixes; recording one event per settled query removes
   the cost and the noise with no new service.

The platform-wide Redis plan is in `redis.md` in this directory.

---

## 9. Recommendations, in order

| # | Change | Fixes | Effort | Risk |
|---|---|---|---|---|
| 1 | Vocabulary from active rows with minimum counts, TTL refresh, numeric/body-label blocklist, plural body words | F1, part of F2 | S-M | low-medium |
| 2 | Needle and trim policy: AND only curated features and explicit packages; unknown words ignored and reported | F4 | S-M | low-medium |
| 3 | Number grammar with units and thousands separators; `min_price`, `min_mileage`, over/between; year phrases | F3 | S (grammar), M (bounds in schema and UI) | low |
| 4 | Gate every shown example through the parser; today 6 of 7 return nothing | F5 (promotion) | S | none |
| 5 | Colour lexicon (word boundary, `gray`/`grey`), body-label and mild-hybrid normalisation at ingest | F7 | S-M | low-medium |
| 6 | Interpretation line (understood and ignored words), zero-result recovery rows, "did you mean" | F9, F2, F5 | M | low |
| 7 | One request per keystroke, 250-300 ms debounce, visible 429, vocabulary on a background thread, token-indexed trims, one analytics event per settled query instead of per prefix | F11 | S-M | low |
| 8 | Reset scalars, exact price options, compose search with ticked filters | F10 | M | medium |
| 9 | Result ordering by relevance and distance, true totals, price plausibility | F8 | M | medium |
| 10 | Decide the semantic layer; fix the GET path before enabling it | F12 | S-M | low |
| 11 | Intent lexicon (cheap, low miles, commuter, automatic) and place words | F5 | M | low |

Use the 60-query set, the 67 aliases and the 54 number phrasings as the regression gate: they run in about four minutes locally
(`run_local2.py`, `alias_sweep.py`) and need no live traffic. The design rules of the owner hold for all of it: items 6 and 8 are about
words and underlined text on a bare line, not boxes or pills; layout options are in `search-promotion.md`.

---

## 10. Screenshots

All under `scratchpad/web_shots/`.
Real page logic, the deployed 1.5.8 static files (served from the identical tree), live HTML and area payload cached from this pass's four
GETs, the two search endpoints answered locally by the 1.5.8 server code on the local inventory (no POST ever left the machine). The
phone captures are Chromium with an iPhone user-agent string at 390 x 844 and device scale 2, not Safari.

| State | 1440 | 390 |
|---|---|---|
| landing | `sq2-landing-1440.png` | `sq2-landing-390.png` |
| listings, area loaded, no query | `sq2-listings-1440-initial.png` | `sq2-listings-390-initial.png` (no search box on screen) |
| `white rav4` (works: 8 vehicles, chips "Toyota RAV4", "Exterior: white") | `sq2-listings-1440-q1.png` | `sq2-listings-390-q1-sheet.png`, `sq2-listings-390-q1.png` |
| `under 30,000 miles` (0 vehicles; chips "Under $30", "Under 30,000 mi"; Price select $25k) | `sq2-listings-1440-q2.png` | `sq2-listings-390-q2-sheet.png`, `sq2-listings-390-q2.png` |
| `f-150 lariat` (0 vehicles; stale "Under $25k", "Under 30,000 mi" chips) | `sq2-listings-1440-q3.png` | `sq2-listings-390-q3-sheet.png`, `sq2-listings-390-q3.png` |
| `family suv under 30k with good mpg` (0 vehicles; raw labels; Price select $35k for $30,000) | `sq2-listings-1440-q4.png` | `sq2-listings-390-q4-sheet.png`, `sq2-listings-390-q4.png` |
| landing sample, 80 characters (0 vehicles) | `sq2-listings-1440-q5.png` | `sq2-listings-390-q5-sheet.png`, `sq2-listings-390-q5.png` |
| `suv`: "1-24 of 500" | `sq2-cap-suv-1440.png` | - |
| limiter trips at the 46th keystroke, no error | `sq2-limit-429-1440.png` | - |

Measurements behind them: `pw_shots1440.json`, `pw_shots390.json`, `pw_storm1440.json`, `pw_storm390.json`, `pw_limit2.json`, `pw_cap2.json`,
`pw_clobber2.json` (all in `raw/search-quality/run2/`). The first pass's screenshots (`search_landing_*.png`, `search_listings_*`) are in the same folder.

---

## 11. Every command

All Python is `.venv/bin/python`; scripts are in `docs/web_speed_search_2026_11/raw/search-quality/run2/`
(the shared definition is `../queries.py`). `LOCALENV` below means
`INVENTORY_DATABASE_URL='postgresql:///cars?host=/tmp' PGOPTIONS='-c default_transaction_read_only=on' LISTINGS_INCLUDE_INCOMPLETE_CARS=0 PYTHONPATH=.`.

```
# live (anonymous GET, paced >= 1.1 s, counted in live_requests.jsonl; cap 140 for this pass, 138 used)
PYTHONPATH=. python run_live2.py          # 1 health + 120 replay + 8 extras = 129
PYTHONPATH=... python pw_shots2.py fetch                                           # 4 documents cached for the browser = 133
PYTHONPATH=... python confirm_live_trucks.py                                       # 3 parse GETs = 136
#   + 1 GET /listings?q=chevrolet silverado 1500 (confirm_live_silverado1500.json) + 1 GET /api/ready (ready_live.json) = 138
# local
LOCALENV python run_local2.py ; python enrich_live2.py ; python live_area_truth2.py ; python relaxed_truth2.py ; python score2.py
python stats_report.py               # every derived statistic of sections 1, 3.3, 3.4, 4.3, 6 (no network, no database)
LOCALENV python cold_start.py        # three times, cold_start.jsonl
LOCALENV python alias_sweep.py ; python model_sweep2.py ; python shown_examples_local.py ; python db_facts.py
LOCALENV python fix_experiments.py ; python color_lexicon_experiment.py ; python semantic_path_demo.py ; python trace_parse.py
LOCALENV python prefix_cost2.py
# browser (no live traffic: documents cached, search endpoints answered locally)
LOCALENV python precompute_post2.py "white rav4" "under 30,000 miles" "f-150 lariat" "family suv under 30k with good mpg" "Something reliable for a first car, automatic, under \$20k, low miles if possible" "toyota camry under 30k" "suv"
LOCALENV python precompute_prefix_post.py
python pw_shots2.py shots1440 ; python pw_shots2.py shots390 ; python pw_shots2.py storm1440 ; python pw_shots2.py storm390
python pw_limit2.py ; python pw_cap2.py ; python pw_clobber2.py
# code facts
git diff --stat v1.5.8 faea2b985 -- backend/utils/query_parser.py backend/utils/hybrid_search.py backend/vector backend/routes/listings_api.py frontend/static/listings.js
git diff v1.5.8 faea2b985 -- backend/utils/query_parser.py
```

Files in `raw/search-quality/run2/`: `results_live.json`, `results_local.json`, `results_live_enriched.json`, `live_area_truth.json`,
`relaxed_truth.json`, `score_summary.json`, `score_rows.md`, `appendix_top5.md`, `alias_sweep.json`, `model_sweep.json`, `fix_experiments.json`,
`color_lexicon_experiment.json`, `semantic_path_demo.json`, `shown_examples_local.json`, `db_facts.json`, `prefix_cost.json`, `cold_start.jsonl`,
`trace_parse.log`, `stats_report.json`, `pw_*.json`, `live_requests.jsonl`, `ready_live.json`, `confirm_live_*.json`. The earlier pass's files are the rest of `raw/search-quality/`.

Pass 3 (2026-11-10), scripts and outputs in `raw/search-quality/run3/` (`LOCALENV` as above; `SP` = the scratchpad `sq_run3/` folder holding the
fetched area payload and the staged-tip source files read with `git show`, no checkout):

```
# live (anonymous GET, paced >= 1.5 s, counted in live_requests.jsonl; cap 170, 159 used)
PYTHONPATH=. python verify_live3.py      # health, ready, 68 parse GETs (60 queries + extras X1-X8), 36 /listings?q= GETs, 1 area payload = 107
PYTHONPATH=... python extra_live3.py                                              # ram 1500, ford f-150, chevy silverado: 3 parse + 3 /listings?q= = 113
PYTHONPATH=... python sweep_live3.py                                              # live parse of '<make> <model>' for the 40 most listed models = 153
PYTHONPATH=... python examples_live3.py                                           # the 3 shown examples not in the 60-query set: 3 parse + 3 /listings?q= = 159
python compare3.py                                                                # pass 3 vs pass 2 (live): compare3.json
# local
LOCALENV python run_local3.py ; python compare_local3.py                         # 66 queries through the POST path, GET path, uncapped filter set
LOCALENV python cold_start3.py                                                    # three times, cold_start3.jsonl
LOCALENV python fix_experiments.py                                                # 10 sandbox variants, fix_experiments3.log
LOCALENV python analytics_cost.py cipher ; LOCALENV python analytics_cost.py plain   # per-POST analytics write, throwaway files in SP
LOCALENV python staged_parser.py ; python staged_colors.py                        # staged-tip parser and colour function vs 1.5.8
LOCALENV python trace_parse3.py "Accord with heated seats under \$30k" "under 30,000 miles" "chevy silverdo"   # parser internals
# code facts (read-only)
git diff --stat v1.5.8 faea2b985 -- backend/utils backend/db/repositories backend/routes/listings_api.py backend/vector frontend/static
git diff v1.5.8 faea2b985 -- backend/utils/interior_color_buckets.py backend/utils/query_parser.py
git show faea2b985:backend/utils/price_floor.py
```

Pass 3 files: `results_live3.json`, `results_live3_extra.json`, `results_live3_examples.json`, `sweep_live3.json`, `sweep_live3_tally.json`, `compare3.json`, `results_local.json`,
`compare_local3.json`, `cold_start3.jsonl`, `fix_experiments.json`, `analytics_cost_cipher.json`, `analytics_cost_plain.json`, `staged_parser.json`,
`staged_colors.json`, `live_requests.jsonl`, `verify_live3.log`, `run_local3.log`, `fix_experiments3.log`, `sweep_live3.log`.

---

## 12. Limits of this evaluation

- One ZIP (28202, Charlotte, 50 miles) and one vantage point; other areas have other vocabularies and counts.
- The page's POST was never sent live (GET-only rule); section 3.3 gives the proxy check.
- The local inventory is a different snapshot from production (59 of 60 parses equal; counts differ). Live counts are the truth where stated; text-feature
  truths are lower bounds; sandbox and sweep numbers are local.
- Scores are judgement under the rubric; they are not weighted by real traffic because no usage log was available. The set over-represents unusual
  phrasing compared with typical shopper queries; the inventory-weighted model sweep (section 4.3) is the figure that does not depend on the set.
- The 429 behaviour is a simulation of the server's documented limiter in the real page; the client reaction is real. The phone numbers are emulation.
- **Test traffic is in the production analytics.** Every `GET /listings?q=..&zip_code=28202&radius=50` runs a search and calls
  `record_search_event(source="listings")` (`backend/listings/routes.py:237-253`), so the three passes wrote about 169 rows (64 + 63 + 42 page GETs,
  counted from each pass's request logs) with the test queries as `query_text` (for example `banana`, `asdf qwerty`) and `geo_zip` 28202,
  on 2026-11-09 08:43-09:01 and 20:45-21:10 EST and 2026-11-10 20:20-20:40 EST, from a Chrome 130 on macOS user-agent string. The parse GET
  (`/api/search/smart/parse`) writes nothing. The owner may want to exclude those windows from the dashboard's top queries; nothing was deleted.
- The pgvector state in production is inferred from outside (section 13.6); `PGVECTOR_URL` and `DATABASE_URL` cannot be read without Railway access.
- The 40-model live sweep (13.5) judges the parse JSON only; whether the result set is complete is the local sweep's question (section 4.3).

---

## 13. Pass 3: verification and additions (2026-11-10)

The report is the basis for what the owner does next, so it was re-checked instead of trusted. The findings stand: nothing in sections 1 to 12
changed except where this section says so. What is new: the analytics write per keystroke (13.3), what the staged releases change in search
results (13.4), a live sweep of the 40 most listed models and a live run of the site's own examples (13.5) and the pgvector configuration evidence (13.6).

### 13.1 What was re-run, and what matched

Scripts and outputs are in `raw/search-quality/run3/`; commands in section 11.

| Check | Result |
|---|---|
| Release (`GET /api/health`, `GET /api/ready`, 2026-11-10 20:20) | `{"commit":"0fd91872bedb07a30ecdd7e19773db5fdc53b1a7","status":"ok","version":"1.5.8"}` (TTFB 268 ms); `{"checks":{"db":"ok","pgvector":"skipped","redis":"skipped"},"ready":true}` (179 ms). Same release as the second pass |
| Live parse GET, 60 queries + extras X1-X6 | **66 of 66 identical** to the second pass (`compare3.json`) |
| Live `/listings?q=` GET for 34 queries (the 14 wrong zeros, 9 clean successes, 11 others) | result counts **34 of 34 identical**, top-5 car ids **34 of 34 identical** |
| Live latency, same endpoints | parse GET total median 246 ms, p90 370 ms, min 100 ms, max 491 ms (n = 68) against median 330 ms, p90 544 ms for the 66 ids the second pass also measured; `/listings?q` TTFB median 1,183 ms, p90 1,752 ms, max 4,871 ms (n = 34) against 1,317 / 1,735 / 2,744 ms for the same 34 ids. Both runs are one vantage point, one evening each, 1.1 s and 1.5 s apart; the difference is not a speed-up of the site |
| Live area payload `GET /api/listings/cars?zip=28202&radius=50` | 13,298 cars; 14,545,187 bytes decoded (the same count as the second pass), 1,583,932 bytes gzip on the wire; TTFB 5,324 ms, total 5,504 ms |
| Local replay (`run_local3.py`, `compare_local3.py`) | 66 of 66 identical in parse, POST count, POST top-5 ids, GET count, GET top-5 ids, uncapped filter-set count and mode; area 9,485 rows in both; parse median 60.9 ms (second pass 64.4), POST path median 154.2 ms (158.6); POST payload 72 kB median over the 60 queries, 12 queries at the 500-car cap, as in section 6 |
| Sandbox experiments (`fix_experiments.py`) | 10 of 10 variants identical in mean F-score, zero-results count, alias tally and number tally, and 66 of 66 parses identical in each; the table of section 7 is reproduced |
| Cold start (`cold_start3.py`, 3 fresh processes) | vocabulary load 952 / 898 / 1,186 ms, package-name scan 539 / 539 / 616 ms, first parse after the loads 21.3-21.7 ms, second parse 5.9-6.5 ms, RSS 247.7-249.5 MB (second pass: 938-1,004, 544-1,041, 5.7-5.8, 247-250) |
| Parser trace (`trace_parse3.py`) for `Accord with heated seats under $30k`, `under 30,000 miles`, `chevy silverdo` | the mechanisms of sections 5.1-5.3 reproduce: working text `accord with` with WRatio 86 shared by `BMW i3 with Range Extender` and four Honda Accord labels (BMW first), `max_price_candidates` 30.0 beside `mileage_cap` 30000, best candidate `Chevrolet Silverado` 97 ending as model `Chevy Van` |
| Live traffic of the pass | 159 anonymous GETs between 20:20:04 and 20:39:54 EST, all HTTP 200: 1 health, 1 ready, 114 parse (`/api/search/smart/parse`), 42 `/listings?q=`, 1 area payload; paced at least 1.5 s apart; no POST, no login or form endpoint (`run3/live_requests.jsonl`) |
| Score arithmetic, recomputed from the table of section 4.2 | parse 73, relevance 72, total 145 of 240; parse 31 / 11 / 18 and relevance 30 / 12 / 18 at 2 / 1 / 0; 27 queries fully right, 17 wrong twice; 16 live zeros (2 correct); failure-class counts F1 5, F2 4, F3 4, F4 16, F5 7, F6 1, F7 5, F8 3, F9 1, F11 2; category totals as section 4.1 |
| Code references | spot-checked in the checkout (HEAD = v1.5.8, no tracked file modified): `listings.js` 13, 95, 178, 225, 256, 329, 400; `listings_api.py` 529-548, 590-606, 898-899; `config.py` 55; `query_parser.py` 98-107, 116-171 and the function map of section 5; `hybrid_search.py` 36-47, 78-131, 375-470, 557-575, 671, 729; `interior_color_buckets.py` 100-135; `landing.html` 28, 56; `_nav_search.html` 12, 15; `listings.html` 67-70; `health.py` 63-80; `mobile/contract.py` 66-72; `ai_chat_bp.py` 238, 272; `nearby_dealers.py` 1069. All matched |
| Screenshots | `sq2-listings-1440-q2.png` (input `under 30,000 miles`, chips "Under $30" and "Under 30,000 mi", Price select $25k, "0 vehicles near 28202") and `sq2-listings-390-initial.png` (no search box on the first phone screen, only "Filters 2") match their captions in section 10 |

### 13.2 Live-area figures, measured again

All from the payload above (13,298 cars, `scratchpad/sq_run3/area_cars.json`), except where marked.

| Claim in the report | Pass 3 |
|---|---|
| Silverado 1500 643, Sierra 1500 247, Ram 1500 118 cars: 1,008 of 13,298 (7.6%) | 643, 247, 118 = 1,008; a second spelling of the make, `RAM` (29 `RAM 1500`), is also in the area |
| BMW i4 19; `F-150` with `Lariat` in the trim 71; `Wrangler` with `Rubicon` in the trim 45; Mercedes AWD 205; RWD 1,462; 30,000 miles or fewer 8,629 | 19; 71; 45; 205; 1,462; 8,629 |
| 44 cars at exactly $2,484 from one dealer, 27 payment-flagged | 44 at $2,484 from `Randy Marion Buick GMC` across 31 models, 27 payment-flagged; also 13 at $900 from `Town and Country Toyota` across 7 models; 57 cars under $3,000, of which 39 payment-flagged |
| Gray-named cars 15%, 4% of them in the gray family | 1,950 (14.7%), 71 (3.6%), 1,844 only in `other` |
| `red` and `tan` substring false positives | 20 `Predawn Gray Mica` in `red`; 76 of the 79 `tan` cars (`Tanzanite` 31, `Manhattan` 24, `Titanium` 14, `Octane` Red 4, `Tango` Red 3) |
| `red toyota camry`: 13 of 43 are `Predawn Gray Mica`; `gray honda civic`: no gray-named car | 43 Camrys in the red family, 13 of them `Predawn Gray Mica`; 33 Civics in the gray family, 17 `Platinum White Pearl`, 13 `Solar Silver Metallic`, none gray-named (30 gray-named Civics exist, none in `gray`) |
| Body labels: 991 `Cars`, 950 `Sedan`; 283 minivan models labelled 52 `Minivan`, 73 `Van`, 157 `Vans`, 1 `7 PASSENGER` | 991; 950; 284 minivan models labelled 52, 73, 158, 1 (one car more than the second pass) |
| Fuel: mild hybrids are stored as Gasoline | the area has five fuel values, Gasoline 10,282, Hybrid 1,892, Diesel 662, Electric 277, Plug-In Hybrid 185; none is a mild hybrid, consistent with section 5.7 |

Also seen: 16 makes appear under two spellings (`RAM` 71 and `Ram` 197, `CADILLAC` 152 and `Cadillac` 81, `LEXUS` 178 and `Lexus` 105, `Ford`/`FORD`...) and 26
make/model pairs differ only by case. Search is not affected, because the make and model matches are case-insensitive (`where.py:218,247`), and
that is why the live parse of `honda accord` returns the make as `honda` (a lowercase vocabulary pair).

### 13.3 New finding: every keystroke also writes an analytics row, through an encrypted connection

*Code path.* `api_search_smart` (`listings_api.py:545-620`) runs the search, then calls `record_search_event(source="smart_search", query_text=q, ...)`
(`:590-606`) before it returns. That function (`search_analytics_db.py:107-154`) opens `users.db` with `get_users_conn()`, inserts one row into
`search_events` and commits, with a new connection every call. In production `get_users_conn()` must use SQLCipher: `backend/main.py:89` runs
`assert_production_security_config`, whose `assert_credential_db_encryption_config` (`credential_db_encryption.py:35`) refuses to start the app without
`USERS_DB_ENCRYPTION_KEY` and `sqlcipher3` when `is_production_env()` is true (`FLASK_ENV=production`, or on Railway with the variable unset; SEC-088), and
`_connect_sqlcipher` (`users_sqlite.py:44-61`) sets the key and reads `sqlite_master` on each open. The page POSTs once per keystroke (section 2.2),
so a typed query writes one row per prefix. The page route `GET /listings?q=` writes one row per page (`listings/routes.py:237-253`).

*Measurement* (`run3/analytics_cost.py`, throwaway database files in the scratchpad, never the real `users.db`; local Mac, the real function):

| Connection | `get_users_conn()` open + `SELECT` + close | `record_search_event` | 22-character typed query (22 calls) |
|---|---|---|---|
| SQLCipher (production mode) | median 42.2 ms, p90 42.8 ms | **median 44.1 ms**, p90 44.5 ms, n = 25 | 22 rows, **1,076.8 ms** |
| plain SQLite (local dev) | 0.38 ms | 1.74 ms | 22 rows, 30.4 ms |

The zip-to-state lookup inside the call is 1.27 ms of that. So the 44 ms is the cost of opening an encrypted connection (key derivation), paid
per keystroke on the request thread before the response is sent. The second pass's server-time figures (section 5.11, 6) did not include it, because
the local replay stops at `serialize_cars_for_listings_grid`; the production POST is that much slower, on a CPU that may be slower than this Mac (not measured). Whether
the key derivation releases the GIL under the one gunicorn worker is also not measured.

*Consequences.* (1) About 44 ms more latency per POST and more CPU per typed query (22 characters: about 1.1 s). (2) `search_events` holds keystroke prefixes: in the typed replay of
`white toyota rav4 hybrid under 35k with sunroof and awd` 29 of 46 distinct prefix states return no cars. The admin dashboard
(`search_analytics_db.py:usage_summary`) computes `total_searches`, `zero_result_searches` and `top_queries` from those rows, so the owner's
search statistics count keystrokes. Production's copy of the table cannot be read from outside; once prefixes are collapsed it could be used to weigh the 60-query score by real
traffic, which section 12 could not do.
(3) The three passes of this evaluation added about 169 `source="listings"` rows (section 12).

*Fix.* Record one event per settled query (after a 400-600 ms pause, or on submit or Enter), not per prefix; keep one pooled or cached connection if the write stays on the request path; or hand
the event to a background thread. Effort S, risk low. This is the cheapest speed gain on the search path and needs no Redis.

### 13.4 What the staged releases (1.5.11-1.5.16, tip `faea2b985`) change for search

`git diff --stat v1.5.8 faea2b985 -- backend/utils backend/db/repositories backend/routes/listings_api.py backend/vector frontend/static` shows the changes below
that reach search results; the parser, `hybrid_search.py`, `listings.js` and the templates are otherwise identical (the diff of `query_parser.py` is one line).

| Staged change | Code | Measured effect |
|---|---|---|
| `i4`, `i3`, `i5`, `i7` are no longer read as cylinders | `query_parser.py:620`, `_CYLINDER_RE` (`i` removed from the inline alternative) | `staged_parser.py`: of 12 i-series probes plus the 60 queries (72 parses) 9 differ; among the 60 only A6. `bmw i4` -> `{"make":"BMW","model":"i4"}`; local replay 13 cars, all i4 (the 1.5.8 parse returns 207 BMWs and no i4). Side effects: `2.0 i4 engine` now also gets make BMW and model i4; `bmw m3 i6` loses the model M3. **Projected score 149 / 240 (62%)** (A6 0/0 -> 2/2), a local replay, not a live measurement |
| Colour lexicon matches whole words | `interior_color_buckets.py:127`, commit `92b3e14fe` (F09) | `staged_colors.py` on the 13,298 live cars: 231 change family. `tan` 79 -> 3 (all 76 removed are false), `red` 724 -> 695 (20 `Predawn Gray Mica` removed, but also 9 genuine reds: `Redline` 4, `Infrared` 3, `Redeye` 1, `Redwood` 1), `blue` 911 -> 840 (`Blueprint` 69 and `Bluestone` 2 lose it), `gray` 1,504 -> 1,448 (`Moonstone Gray Metallic` 37, `Quicksilver Metallic` 8 and others lose it), `silver` 1,374 -> 1,365; 159 cars gain `other`. Gray-named cars in the `gray` family fall from 71 to 32 of 1,950 because the lexicon still has no `gray` or `grey` phrase |
| Price floor and repeated-price withholding | new `utils/price_floor.py` (N6.7): floor $1,000, a price shared by 5 or more models at one dealer is withheld; called at write time by `scanner/upsert/rows.py:16`, `scanner/upsert/sql.py:17`, `utils/listing_completeness.py:21`; `scripts/repair_junk_prices.py` for stored rows | the live area's 44 cars at $2,484 (31 models, one dealer) and 13 at $900 (7 models) both fall under the rule, but only when those rows are rewritten or repaired; no search-time change |
| Distance ranking by dealership registry id | `db/repositories/search/ranking.py` | sister stores that share a URL host no longer share the first store's distance; not measured here |
| Build limit for `/api/listings/cars` (SEC-A-005) | `listings_api.py`, `RATE_LIMIT_LISTINGS_BUILD_PER_MIN` | cache-miss builds per IP are limited; the first-keystroke area download is one when the cache is cold. Not in 1.5.8 |
| Embedding model loader | new `vector/embedding_model.py`, `vector/pgvector_service.py` | none while the semantic layer is off |

Reading: deploying the staged releases fixes one of the 60 queries and the `i`-series class, removes colour false positives while creating colour false negatives, and adds the
price guard for new writes. F1-F5, F8-F12 and the rest of F7 need the changes of section 9. The colour fix should ship with lexicon entries for `gray`/`grey`, `blueprint`,
`bluestone`, `moonstone`, `quicksilver`, `redline`, `infrared`, `redeye`, `redwood` (effort S); otherwise the facets trade one error for another.

### 13.5 Live probes added in pass 3

| id | query | live parse | live result |
|---|---|---|---|
| X7 | `chevrolet silverado 1500` | `{"body_style":["1500"],"make":"Chevrolet","model":"Silverado 1500"}` | 0 cars (TTFB 1,184 ms) |
| X8 | `gmc sierra 1500` | `{"body_style":["1500"],"make":"GMC","model":"Sierra 1500"}` | 0 cars (1,406 ms) |
| Y1 | `ram 1500` | `{"body_style":["1500"],"make":"Ram","model":"1500"}` | 0 cars (872 ms) |
| Y2 | `ford f-150` | `{"make":"Ford","model":"F-150"}` | 200 cars, top 5 all F-150 (3,175 ms) |
| Y3 | `chevy silverado` | `{"make":"Chevrolet","model":"Silverado"}` | 200 cars, top 5 `Silverado 1500` (1,141 ms) |

All three best-selling pickup lines fail when the shopper types the model number that is part of their name (`silverado 1500`, `sierra 1500`, `ram 1500`), and
work without it (`chevy silverado`). The first item of section 1.3 is therefore confirmed on production for all three by the page's own parse request.

**The example queries the site shows, live.** Section 4.3 had the placeholders and hint 4 from the local replay only; `examples_live3.py` ran them against production
(parse GET and `/listings?q=` GET, ZIP 28202, 50 miles). Together with M1-M3 and K4 from the 60-query set, all seven examples are now live-verified:

| Where the site shows it | Query | Live parse | Live result |
|---|---|---|---|
| landing placeholder (`landing.html:28`) | `White BMW X5 with M Sport under $70k` | `{"exterior_color":["white"],"make":"BMW","max_price":70000,"packages_json_contains":"m sport","trim_contains":"sport"}`: the model X5 is lost | **0 cars** (TTFB 1,324 ms) |
| top-bar placeholder (`_nav_search.html:12`) | `Accord with sunroof under $30k` | `{"max_price":30000,"packages_json_contains":"sunroof","trim_contains":"accord"}`: make and model are lost | **0 cars** (1,060 ms) |
| landing sample sentence (`landing.html:56`) | K4 `Something reliable for a first car, automatic, under $20k, low miles if possible` | see section 4.2 | 0 cars |
| hint 1 / 2 / 3 (`listings.js:403-405`, never displayed) | M1, M2, M3 | see section 4.2 | 0 / 0 / 0 cars |
| hint 4 (`listings.js:406`, never displayed) | `White RAV4 with sunroof hybrid under $35k` | `{"exterior_color":["white"],"fuel_type":"hybrid","max_price":35000,"packages_json_contains":"sunroof"}`: RAV4 is lost | 15 cars, none a RAV4 (top 5: Audi A5 Sportback, Hyundai Elantra Hybrid, Kia Niro, Volvo S60, Volvo XC60; TTFB 3,123 ms) |

So 6 of the 7 examples return no cars and the 7th returns cars of other models, as section 1.3 says. The screen-reader-only hint `white BMW X5 under $70k` (`listings.html:60-62`) returned 17 cars
in the local replay (section 4.3) and was not run live. The hint strings in `listings.js` carry ` · ` separators; the queries above are the normalised form that section 4.3 also uses.

**Live sweep of the 40 most listed models** (`sweep_live3.py`; the page's parse GET for `<make> <model>`, judged from the JSON; the 40 models are 7,110 of the 13,298 area cars):

| Verdict | Models | Cars | Share |
|---|---|---|---|
| exact make + model, nothing else | 34 | 5,667 | 79.7% |
| usable: `Honda CR-V Hybrid` -> CR-V + `fuel_type` hybrid; `Honda Accord Hybrid` -> Accord + hybrid; `Chevrolet Colorado` + a harmless `color` needle (the title contains it, but five text columns are scanned) | 3 | 435 | 6.1% |
| **broken by a junk `body_style` of `1500`**: Silverado 1500, Sierra 1500, Ram 1500 (0 cars) | 3 | **1,008** | **14.2%** |

The local sweep of section 4.3 reached the same 14.2% for the same three models, so the local vocabulary and production agree on the models that matter most. (Sibling-model noise such as
`Bronco` returning `Bronco Sport` is a result-set property and is not visible in the parse; section 4.3 has it.)

### 13.6 The semantic layer in production: the evidence, tightened

- `pgvector_service._pg_url()` (`pgvector_service.py:46-48`) returns `PGVECTOR_URL` or, if that is empty, `DATABASE_URL`; `pgvector_configured()` is true when either is set. The readiness
  probe checks `PGVECTOR_URL` only (`health.py:63-80`), so `pgvector: skipped` does not by itself prove that `DATABASE_URL` is unset.
- The Railway README gives the web service `INVENTORY_DATABASE_URL=${{Postgres.DATABASE_URL}}` (`deploy/railway/README.md:17,167`), a different variable. With neither `PGVECTOR_URL`
  nor `DATABASE_URL` on the service, `_semantic_car_ids` returns `[]` at its first test (`hybrid_search.py:36-47`).
- Either way the observable result is the same: `_semantic_car_ids` returns `[]` when the layer is unconfigured and also on any exception (`:42-46`), and a non-empty
  list would cap the GET result at the vector top 100 (`:444-459`). 27 of the 60 live GET searches returned 200 rows (A1, A2, B1, C1 and others), so no semantic candidate set was applied.
  Production could not be inspected for the variables; the conclusion "the layer does not take part in search" is behavioural and holds in both cases.

### 13.7 Small additions

- All three search inputs set `spellcheck="false"` (`listings.html:70`, `landing.html:28`, `_nav_search.html:15`), so the browser's own spelling hint is off, while the parser
  absorbs only some typos (make typos yes, model typos no; section 5.2). There is no suggestion list, `datalist` or suggest endpoint in the templates, `listings.js` or the routes
  (`/api/search/smart/parse` and `/api/search/smart` are the only search routes, `listings_api.py:898-899`).
- The landing page's own sentence "Ask it like you'd say it ... Something reliable for a first car, automatic, under $20k, low miles if possible" (`landing.html:56`) is query K4,
  which returns 0 cars on production (second pass and again in pass 3: parse identical, 0 cars).
- On the phone the landing search line is 207 px wide and clips its placeholder to `White BMW X5 with M S` (`sq2-landing-390.png`), so the one example the line shows is cut
  mid-word, and that example (`White BMW X5 with M Sport under $70k`) returns 0 cars in the local replay of the POST path, with the model X5 lost (section 4.3).

---

## Appendix A. Top 5 results per query (live `/listings?q=`, as judged)

`[ok]` satisfies the intent (judged with the listing text where the car exists in the local snapshot), `[no]` does not.
Format: year make model, trim, price, mileage, body, drivetrain, fuel, exterior / interior colour, condition.

- **A1** `toyota camry`: live grid 200 cars, local POST 148 (sql_first_relevance_rank); live parse 163 ms, live `/listings?q` TTFB 1688 ms, local POST 93 ms
  - [ok] 2021 Toyota Camry, SE, $16629, 109996 mi, Sedan, FWD, Gasoline, Predawn Gray Mica / Black, Used
  - [ok] 2021 Toyota Camry, LE, $18751, 95928 mi, Sedan, FWD, Gasoline, Super White / Ash, Used
  - [ok] 2020 Toyota Camry, LE, $18850, 76159 mi, Sedan, FWD, Gasoline, Ruby Flare Pearl / Black, Used
  - [ok] 2021 Toyota Camry, SE, $19976, 93671 mi, Sedan, AWD, Gasoline, Predawn Gray Mica / Black, Used
  - [ok] 2023 Toyota Camry, LE, $22842, 50383 mi, Sedan, FWD, Gasoline, Celestial Silver Metallic / Black, Used
- **A2** `honda cr-v`: live grid 200 cars, local POST 200 (sql_first_relevance_rank); live parse 218 ms, live `/listings?q` TTFB 1379 ms, local POST 93 ms
  - [ok] 2014 Honda CR-V, EX-L, $15494, 97425 mi, SUV, FWD, Gasoline, White Diamond Pearl / Beige, Used
  - [ok] 2014 Honda CR-V, EX-L, $15549, 89849 mi, SUV, AWD, Gasoline, White Diamond Pearl / Gray, Used
  - [ok] 2020 Honda CR-V, EX-L, $22694, 87538 mi, SUV, FWD, Gasoline, Modern Steel Metallic / Black, Used
  - [ok] 2025 Honda CR-V, LX, $28136, 19752 mi, SUV, FWD, Gasoline, Urban Gray Pearl / Black, Used
  - [ok] 2024 Honda CR-V Hybrid, Sport, $29895, 35132 mi, SUV, FWD, Hybrid, Urban Gray Pearl / Black, Used
- **A3** `f-150 lariat`: live grid 0 cars, local POST 0 (sql_first); live parse 284 ms, live `/listings?q` TTFB 1241 ms, local POST 118 ms
  - (no results)
- **A4** `jeep wrangler rubicon`: live grid 0 cars, local POST 0 (sql_first); live parse 153 ms, live `/listings?q` TTFB 1221 ms, local POST 144 ms
  - (no results)
- **A5** `mercedes gle 350`: live grid 37 cars, local POST 56 (sql_first_relevance_rank); live parse 149 ms, live `/listings?q` TTFB 985 ms, local POST 72 ms
  - [ok] 2023 Mercedes-Benz GLE, GLE 350, $43000, 46194 mi, SUV, RWD, Gasoline, Obsidian Black Metallic / Black, Used
  - [ok] 2024 Mercedes-Benz GLE, GLE 350, $48434, 34892 mi, SUV, AWD, Gasoline, Obsidian Black Metallic / Bahia Brown/Black, Used
  - [ok] 2017 Mercedes-Benz GLE, GLE 350, $14065, 160384 mi, SUV, AWD, Gasoline, Iridium Silver Metallic / Saddle Brown/Black, Used
  - [ok] 2020 Mercedes-Benz GLE, GLE 350, $20291, 119143 mi, SUV, AWD, Gasoline, Polar White / Black, Used
  - [ok] 2022 Mercedes-Benz GLE, GLE 350, $41098, 71738 mi, SUV, AWD, Gasoline, MANUFAKTUR Diamond White Bright / Macchiato Beige/Black, Used
- **A6** `bmw i4`: live grid 200 cars, local POST 207 (sql_first_relevance_rank); live parse 255 ms, live `/listings?q` TTFB 1241 ms, local POST 81 ms
  - [no] 2020 BMW 3 Series, 330i xDrive, $18937, 98157 mi, Sedan, AWD, Gasoline, Alpine White / Black, Used
  - [no] 2019 BMW X3, sDrive30i, $17498, 95030 mi, SUV, AWD, Gasoline, White / Canberra Beige/Black, Used
  - [no] 2024 BMW 4 Series, 430i xDrive Gran Coupe, $38890, 28004 mi, Sedan, AWD, Gasoline, Mineral White Metallic / Tacora Red, Certified Pre-Owned
  - [no] 2014 BMW 4 Series, 428i xDrive, $15787, 59937 mi, Coupe, AWD, Gasoline, Glacier Silver Metallic / Black, Used
  - [no] 2018 BMW X3, xDrive30i, $19349, 76573 mi, SUV, AWD, Gasoline, Black Sapphire Metallic / Black, Used
- **A7** `4T1G11AK6MU520618`: live grid 1 cars, local POST 1 (vin_exact); live parse 394 ms, live `/listings?q` TTFB 1736 ms, local POST 188 ms
  - [ok] 2021 Toyota Camry, SE, $16629, 109996 mi, Sedan, FWD, Gasoline, Predawn Gray Mica / Black, Used
- **B1** `toyta camry`: live grid 200 cars, local POST 148 (sql_first_relevance_rank); live parse 1132 ms, live `/listings?q` TTFB 1294 ms, local POST 180 ms
  - [ok] 2021 Toyota Camry, SE, $16629, 109996 mi, Sedan, FWD, Gasoline, Predawn Gray Mica / Black, Used
  - [ok] 2021 Toyota Camry, LE, $18751, 95928 mi, Sedan, FWD, Gasoline, Super White / Ash, Used
  - [ok] 2020 Toyota Camry, LE, $18850, 76159 mi, Sedan, FWD, Gasoline, Ruby Flare Pearl / Black, Used
  - [ok] 2021 Toyota Camry, SE, $19976, 93671 mi, Sedan, AWD, Gasoline, Predawn Gray Mica / Black, Used
  - [ok] 2023 Toyota Camry, LE, $22842, 50383 mi, Sedan, FWD, Gasoline, Celestial Silver Metallic / Black, Used
- **B2** `hundai tucson`: live grid 51 cars, local POST 31 (sql_first_relevance_rank); live parse 506 ms, live `/listings?q` TTFB 1735 ms, local POST 161 ms
  - [ok] 2021 Hyundai Tucson, SEL, $16459, 80714 mi, SUV, FWD, Gasoline, Magnetic Force / Gray, Used
  - [ok] 2017 Hyundai Tucson, Value, $14992, 75887 mi, SUV, FWD, Gasoline, Gray / Black, Used
  - [ok] 2020 Hyundai Tucson, SE, $15032, 86652 mi, SUV, FWD, Gasoline, Red / Black, Certified
  - [ok] 2020 Hyundai Tucson, Value, $15380, 94160 mi, SUV, AWD, Gasoline, Stellar Silver / Gray, Used
  - [ok] 2019 Hyundai Tucson, Value, $17646, 47877 mi, SUV, FWD, Gasoline, Black Noir Pearl / Gray, Used
- **B3** `chevy silverdo`: live grid 0 cars, local POST 0 (sql_first); live parse 544 ms, live `/listings?q` TTFB 1532 ms, local POST 159 ms
  - (no results)
- **B4** `mercedez benz c300`: live grid 28 cars, local POST 33 (sql_first_relevance_rank); live parse 173 ms, live `/listings?q` TTFB 1121 ms, local POST 88 ms
  - [ok] 2020 Mercedes-Benz C-Class, C 300, $22320, 66085 mi, Sedan, RWD, Gasoline, Selenite Gray Metallic / AMG Black, Used
  - [ok] 2025 Mercedes-Benz C-Class, C 300, $40450, 15456 mi, Sedan, AWD, Gasoline, Black / Macchiato Beige, Used
  - [ok] 2020 Mercedes-Benz C-Class, C 300, $22091, 86698 mi, Cars, RWD, Gasoline, Selenite Grey Metallic / Silk Beige, Used
  - [ok] 2021 Mercedes-Benz C-Class, C 300, $26799, 29474 mi, Cars, AWD, Gasoline, Brilliant Blue Metallic / Silk Beige/Black, Used
  - [ok] 2022 Mercedes-Benz C-Class, C 300, $35879, 28842 mi, Cars, RWD, Hybrid, Polar White / Macchiato Beige, Used
- **B5** `porshe cayenne`: live grid 11 cars, local POST 11 (sql_first_relevance_rank); live parse 334 ms, live `/listings?q` TTFB 1514 ms, local POST 184 ms
  - [ok] 2020 Porsche Cayenne, Base, $39000, 46216 mi, SUV, AWD, Gasoline, White / Black/Bordeaux Red, Used
  - [ok] 2025 Porsche Cayenne, Base, $72057, 16772 mi, SUV, AWD, Gasoline, Select / Black/Mojave Beige, Used
  - [ok] 2020 Porsche Cayenne, Base, $32117, 82522 mi, SUV, AWD, Gasoline, Dolomite Silver Metallic / Black/Mojave Beige, Used
  - [ok] 2023 Porsche Cayenne, Platinum Edition, $53662, 45859 mi, SUV, AWD, Gasoline, White / Black/Mojave Beige, Used
  - [ok] 2021 Porsche Cayenne, GTS, $54494, 80661 mi, SUV, AWD, Gasoline, Moonlight Blue Metallic / Black, Used
- **B6** `nissan rouge`: live grid 200 cars, local POST 225 (sql_first_relevance_rank); live parse 466 ms, live `/listings?q` TTFB 1532 ms, local POST 180 ms
  - [no] 2016 Nissan Maxima, 3.5 S, $13995, 85614 mi, Sedan, FWD, Gasoline, Red / Charcoal, Used
  - [no] 2022 Nissan Altima, 2.5 SR, $15075, 86188 mi, Sedan, FWD, Gasoline, Scarlet Ember Tintcoat / Sport, Used
  - [no] 2016 Nissan Maxima, Platinum, $15995, 90079 mi, Sedan, FWD, Gasoline, Gun Metallic / Charcoal, Used
  - [no] 2024 Nissan Sentra, SV, $17995, 62409 mi, Sedan, FWD, Gasoline, Rosewood / Charcoal, Used
  - [no] 2021 Nissan Altima, 2.5 SR, $18505, 53261 mi, Sedan, AWD, Gasoline, Pearl White / Sport, Used
- **C1** `under $25,000`: live grid 200 cars, local POST 500 (sql_first_relevance_rank); live parse 245 ms, live `/listings?q` TTFB 2790 ms, local POST 152 ms
  - [ok] 2017 Ford Focus, SE, $9559, 48754 mi, Hatchback, FWD, Gasoline, White Gold Metallic / Black, Used
  - [ok] 2019 Kia Sorento, L, $11693, 106707 mi, SUV, FWD, Gasoline, Sparkling Silver / Satin Black, Used
  - [ok] 2014 Lincoln Navigator, Base, $11995, 164914 mi, SUV, RWD, Gasoline, Tuxedo Black Metallic / Charcoal Black, Used
  - [ok] 2016 Nissan Maxima, 3.5 S, $13995, 85614 mi, Sedan, FWD, Gasoline, Red / Charcoal, Used
  - [ok] 2019 GMC Terrain, SLE, $14010, 77201 mi, SUV, FWD, Gasoline, Ebony Twilight Metallic / Jet Black, Used
- **C2** `honda civic under 20k`: live grid 18 cars, local POST 8 (sql_first_relevance_rank); live parse 148 ms, live `/listings?q` TTFB 1117 ms, local POST 33 ms
  - [ok] 2022 Honda Civic Sedan, LX, $19656, 75990 mi, Sedan, FWD, Gasoline, Lunar Silver Metallic / Black, Used
  - [ok] 2021 Honda Civic Hatchback, EX, $19858, 77971 mi, Hatchback, FWD, Gasoline, Crystal Black Pearl / Black, Used
  - [ok] 2023 Honda Civic, Sport, $19895, 78581 mi, Hatchback, FWD, Gasoline, Platinum White Pearl / Black, Used
  - [ok] 2012 Honda Civic, LX, $10399, 128863 mi, Cars, FWD, Gasoline, Alabaster Silver Metallic / Gray, Used
  - [ok] 2012 Honda Civic Cpe, Si, $10698, 156056 mi, Coupe, FWD, Gasoline, Alabaster Silver Metallic / Black, Used
- **C3** `between 20k and 30k`: live grid 200 cars, local POST 500 (sql_first_relevance_rank); live parse 371 ms, live `/listings?q` TTFB 1434 ms, local POST 196 ms
  - [no] 2013 Volvo XC60, 3.2, $6000, 222979 mi, SUV, FWD, Gasoline, Black Stone / Off Black, Used
  - [no] 2017 Ford Focus, SE, $9559, 48754 mi, Hatchback, FWD, Gasoline, White Gold Metallic / Black, Used
  - [no] 2019 Kia Sorento, L, $11693, 106707 mi, SUV, FWD, Gasoline, Sparkling Silver / Satin Black, Used
  - [no] 2014 Lincoln Navigator, Base, $11995, 164914 mi, SUV, RWD, Gasoline, Tuxedo Black Metallic / Charcoal Black, Used
  - [no] 2016 Nissan Maxima, 3.5 S, $13995, 85614 mi, Sedan, FWD, Gasoline, Red / Charcoal, Used
- **C4** `over 50k`: live grid 22 cars, local POST 16 (sql_first_relevance_rank); live parse 353 ms, live `/listings?q` TTFB 1532 ms, local POST 260 ms
  - [no] 2021 Jeep Gladiator, Overland, $29666, 57699 mi, Truck, 4WD, Gasoline, Gray Clearcoat / Black, Used
  - [no] 2018 Jeep Grand Cherokee, Overland, $12898, 169266 mi, SUV, 4WD, Gasoline, Granite Crystal Metallic Clearcoat / Black, Used
  - [no] 2020 Jeep Gladiator, Overland, $22495, 113718 mi, Truck, 4WD, Gasoline, Black / Black, Used
  - [no] 2020 Jeep Grand Cherokee, Overland, $24495, 84232 mi, SUV, 4WD, Gasoline, White / Black, Used
  - [no] 2022 Jeep Grand Cherokee, Overland, $29000, 43040 mi, SUV, 4WD, Gasoline, Gray / Global Black, Used
- **C5** `under 30,000 miles`: live grid 0 cars, local POST 0 (sql_first); live parse 526 ms, live `/listings?q` TTFB 1589 ms, local POST 208 ms
  - (no results)
- **C6** `2020 or newer tacoma`: live grid 200 cars, local POST 167 (sql_first_relevance_rank); live parse 722 ms, live `/listings?q` TTFB 1357 ms, local POST 182 ms
  - [ok] 2022 Toyota Tacoma 2WD, SR5, $32075, 59615 mi, Truck, RWD, Gasoline, Barcelona Red Metallic / Black/Red, Used
  - [ok] 2023 Toyota Tacoma 2WD, TRD Sport, $32673, 43559 mi, Truck, RWD, Gasoline, Magnetic Gray Metallic / Cement, Used
  - [ok] 2023 Toyota Tacoma, TRD Off-Road, $36581, 32760 mi, Truck, 4WD, Gasoline, Ice / Cement, Used
  - [ok] 2020 Toyota Tacoma, TRD Sport, $38218, 28645 mi, Truck, 4WD, Gasoline, Cement / Black/Gun Metal, Used
  - [ok] 2022 Toyota Tacoma, SR, $21060, 110112 mi, 4D Access Cab, 4WD, Gasoline, White / Cement, Used
- **C7** `2018-2021 4runner`: live grid 22 cars, local POST 14 (sql_first_relevance_rank); live parse 343 ms, live `/listings?q` TTFB 1519 ms, local POST 156 ms
  - [ok] 2019 Toyota 4Runner, Limited, $28499, 98642 mi, SUV, RWD, Gasoline, Blizzard Pearl / Graphite, Used
  - [ok] 2021 Toyota 4Runner, SR5, $36161, 52979 mi, SUV, 4WD, Gasoline, Super White / Graphite, Used
  - [ok] 2018 Toyota 4Runner, SR5, $20574, 150158 mi, SUV, 4WD, Gasoline, Magnetic Gray Metallic / Graphite, Used
  - [ok] 2018 Toyota 4Runner, TRD Off Road, $24881, 171122 mi, SUV, 4WD, Gasoline, Midnight Black Metallic / Black Graphite, Used
  - [ok] 2018 Toyota 4Runner, SR5 Premium, $25994, 170276 mi, SUV, 4WD, Gasoline, Nautical Blue Metallic / Graphite, Used
- **C8** `newer than 2019 ram 1500`: live grid 0 cars, local POST 0 (sql_first); live parse 337 ms, live `/listings?q` TTFB 1705 ms, local POST 203 ms
  - (no results)
- **D1** `suv`: live grid 200 cars, local POST 500 (sql_first_relevance_rank); live parse 146 ms, live `/listings?q` TTFB 1650 ms, local POST 168 ms
  - [ok] 2016 Jeep Cherokee, Latitude, $10879, 110800 mi, SUV, FWD, Gasoline, Granite Crystal Metallic Clearcoat / Black, Used
  - [ok] 2019 Chevrolet Trax, LS, $11998, 85438 mi, SUV, FWD, Gasoline, SILVER ICE METALLIC / JET BLACK, CLOTH SEAT TRIM, Used
  - [ok] 2018 Volkswagen Tiguan, SE, $12369, 97494 mi, SUV, FWD, Gasoline, Pure White / Titan Black, Certified
  - [ok] 2018 Jeep Grand Cherokee, Overland, $12898, 169266 mi, SUV, 4WD, Gasoline, Granite Crystal Metallic Clearcoat / Black, Used
  - [ok] 2019 Ford Escape, SEL, $13398, 91752 mi, SUV, 4WD, Gasoline, Gray / Black, Certified
- **D2** `pickup truck`: live grid 200 cars, local POST 364 (sql_first_relevance_rank); live parse 600 ms, live `/listings?q` TTFB 1630 ms, local POST 167 ms
  - [ok] 2012 Ford F-150, XLT, $13879, 126438 mi, Truck, 4WD, Gasoline, Silver / Gray, Used
  - [ok] 2015 Ram 1500, Big Horn, $14898, 148913 mi, Truck, 4WD, Gasoline, Gray / Diesel Gray/Black, Used
  - [ok] 2015 Chevrolet Silverado 1500, LT, $16998, 147026 mi, Truck, 4WD, Gasoline, TUNGSTEN METALLIC / JET BLACK, CLOTH SEAT TRIM, Used
  - [ok] 2023 GMC Sierra 1500, Pro, $24879, 77262 mi, Truck, RWD, Gasoline, Summit White / Jet Black, Certified
  - [ok] 2020 Ram 1500 Classic, Tradesman, $25398, 62936 mi, Truck, 4WD, Gasoline, Patriot Blue Pearlcoat / Diesel Gray/Black, Used
- **D3** `minivan`: live grid 52 cars, local POST 4 (sql_first_relevance_rank); live parse 125 ms, live `/listings?q` TTFB 1098 ms, local POST 78 ms
  - [ok] 2024 Honda Odyssey, Elite, $37104, 27548 mi, Minivan, FWD, Gasoline, Lunar Silver Metallic / Mocha, Used
  - [ok] 2024 Honda Odyssey, Elite, $37995, 51822 mi, Minivan, FWD, Gasoline, Platinum White Pearl / Beige, Used
  - [ok] 2024 Toyota Sienna, XLE, $38526, 77428 mi, Minivan, FWD, Hybrid, Celestial Silver Metallic / Graphite, Used
  - [ok] 2025 Toyota Sienna, XLE, $39876, 52174 mi, Minivan, FWD, Hybrid, Magnetic Gray Metallic / Gray, Used
  - [ok] 2025 Toyota Sienna, XLE, $41337, 30035 mi, Minivan, FWD, Hybrid, Pearl / Gray, Used
- **D4** `convertible`: live grid 129 cars, local POST 105 (sql_first_relevance_rank); live parse 162 ms, live `/listings?q` TTFB 1120 ms, local POST 83 ms
  - [ok] 2022 Ford Bronco, Outer Banks, $31016, 91281 mi, Convertible, 4WD, Gasoline, Cactus Gray / Roast w/Black Onyx, Used
  - [ok] 2026 Ford Bronco, Base, $39995, 2031 mi, Convertible, 4WD, Gasoline, Avalanche / Gray, New
  - [ok] 2026 Ford Bronco, Base, $40495, 95 mi, Convertible, 4WD, Gasoline, Avalanche / Gray, New
  - [ok] 2026 Ford Bronco, Big Bend, $41495, 12 mi, Convertible, 4WD, Gasoline, Oxford White / Dk Gry W/Blk Onyx Clth Bu, New
  - [ok] 2026 Ford Bronco, Base, $42375, 12 mi, Convertible, 4WD, Gasoline, Oxford White / Gray, New
- **D5** `sedan under 25k`: live grid 200 cars, local POST 86 (sql_first_relevance_rank); live parse 137 ms, live `/listings?q` TTFB 1386 ms, local POST 68 ms
  - [ok] 2016 Nissan Maxima, 3.5 S, $13995, 85614 mi, Sedan, FWD, Gasoline, Red / Charcoal, Used
  - [ok] 2015 Honda Accord Sedan, Sport, $14017, 101165 mi, Sedan, FWD, Gasoline, Maroon / Black, Used
  - [ok] 2022 Nissan Altima, 2.5 SR, $15075, 86188 mi, Sedan, FWD, Gasoline, Scarlet Ember Tintcoat / Sport, Used
  - [ok] 2021 Kia Forte, GT-Line, $15706, 60489 mi, Sedan, FWD, Gasoline, Fire Orange / Black, Used
  - [ok] 2016 Nissan Maxima, Platinum, $15995, 90079 mi, Sedan, FWD, Gasoline, Gun Metallic / Charcoal, Used
- **E1** `awd suv`: live grid 200 cars, local POST 500 (sql_first_relevance_rank); live parse 155 ms, live `/listings?q` TTFB 1387 ms, local POST 193 ms
  - [ok] 2018 Jeep Grand Cherokee, Overland, $12898, 169266 mi, SUV, 4WD, Gasoline, Granite Crystal Metallic Clearcoat / Black, Used
  - [ok] 2019 Ford Escape, SEL, $13398, 91752 mi, SUV, 4WD, Gasoline, Gray / Black, Certified
  - [ok] 2011 Jeep Wrangler Unlimited, Rubicon, $13897, 100876 mi, SUV, 4WD, Gasoline, Black Clear Coat / Black, Used
  - [ok] 2018 Ford Edge, Sport, $14500, 100419 mi, SUV, AWD, Gasoline, Black / Ebony, Used
  - [ok] 2018 Jeep Grand Cherokee, Limited, $16000, 112097 mi, SUV, 4WD, Gasoline, White / Black, Used
- **E2** `4x4 truck`: live grid 200 cars, local POST 257 (sql_first_relevance_rank); live parse 170 ms, live `/listings?q` TTFB 1606 ms, local POST 151 ms
  - [ok] 2012 Ford F-150, XLT, $13879, 126438 mi, Truck, 4WD, Gasoline, Silver / Gray, Used
  - [ok] 2015 Ram 1500, Big Horn, $14898, 148913 mi, Truck, 4WD, Gasoline, Gray / Diesel Gray/Black, Used
  - [ok] 2015 Chevrolet Silverado 1500, LT, $16998, 147026 mi, Truck, 4WD, Gasoline, TUNGSTEN METALLIC / JET BLACK, CLOTH SEAT TRIM, Used
  - [ok] 2020 Ram 1500 Classic, Tradesman, $25398, 62936 mi, Truck, 4WD, Gasoline, Patriot Blue Pearlcoat / Diesel Gray/Black, Used
  - [ok] 2023 Ford F-150, XL, $25869, 133165 mi, Truck, 4WD, Gasoline, Antimatter Blue Metallic / Black, Certified
- **E3** `rear wheel drive`: live grid 0 cars, local POST 0 (sql_first); live parse 315 ms, live `/listings?q` TTFB 1095 ms, local POST 146 ms
  - (no results)
- **E4** `mercedes 4matic`: live grid 0 cars, local POST 0 (sql_first); live parse 357 ms, live `/listings?q` TTFB 1264 ms, local POST 169 ms
  - (no results)
- **F1** `hybrid`: live grid 200 cars, local POST 500 (sql_first_relevance_rank); live parse 190 ms, live `/listings?q` TTFB 1570 ms, local POST 158 ms
  - [ok] 2021 Kia Sorento Hybrid, EX, $20576, 81373 mi, SUV, FWD, Hybrid, Silky Silver / Black, Used
  - [ok] 2024 Ford Escape, ST-Line, $22840, 20687 mi, SUV, FWD, Hybrid, Star White Metallic Tri Coat / Ebony, Used
  - [ok] 2022 Audi A3, Premium, $23894, 20082 mi, Sedan, FWD, Hybrid, Manhattan Gray Metallic / Gray, Used
  - [ok] 2024 Honda Accord Hybrid, Sport, $24895, 61326 mi, Sedan, FWD, Hybrid, Radiant Red Metallic / Black, Used
  - [ok] 2023 Kia Niro, EX Touring, $25150, 19205 mi, SUV, FWD, Hybrid, Graphite Gray / Medium Gray, Used
- **F2** `plug-in hybrid suv`: live grid 148 cars, local POST 71 (sql_first_relevance_rank); live parse 146 ms, live `/listings?q` TTFB 1050 ms, local POST 36 ms
  - [ok] 2024 Jeep Wrangler, Sport S 4xe, $27600, 19689 mi, SUV, 4WD, Plug-In Hybrid, Bright White Clearcoat / Black, Used
  - [ok] 2026 Toyota RAV4 Plug-In Hybrid, SE, $44477, 3390 mi, SUV, AWD, Plug-In Hybrid, Ice / Black, Used
  - [ok] 2026 Toyota RAV4 Plug-in Hybrid, SE, $44741, 0 mi, SUV, AWD, Plug-In Hybrid, Storm Cloud / Black/Blue fabric, New
  - [ok] 2026 Toyota RAV4 Plug-in Hybrid, SE, $45116, 2 mi, SUV, AWD, Plug-In Hybrid, Midnight Black Metallic / Black/Blue fabric, New
  - [ok] 2026 Toyota RAV4 Plug-in Hybrid, SE, $45116, 2 mi, SUV, AWD, Plug-In Hybrid, Midnight Black Metallic / Black/Blue fabric, New
- **F3** `electric`: live grid 200 cars, local POST 194 (sql_first_relevance_rank); live parse 146 ms, live `/listings?q` TTFB 1186 ms, local POST 47 ms
  - [ok] 2023 Tesla Model Y, Long Range, $31174, 45473 mi, SUV, AWD, Electric, Blue / Black, Used
  - [ok] 2023 Ford F-150 Lightning, XLT, $43772, 20664 mi, Truck, AWD, Electric, Oxford White / Medium Dark Slate, Used
  - [ok] 2026 Ford Mustang Mach-E, Premium, $48225, 12 mi, SUV, AWD, Electric, Adriatic Blue Metallic / Black Onyx, New
  - [ok] 2019 Audi e-tron, Premium Plus, $20794, 62751 mi, SUV, AWD, Electric, Mythos Black Metallic / Okapi Brown, Used
  - [ok] 2023 Chevrolet Bolt EUV, Premier, $21493, 51453 mi, SUV, FWD, Electric, Summit White / Jet Black/Nightshift Blue, Used
- **F4** `diesel truck`: live grid 200 cars, local POST 82 (sql_first_relevance_rank); live parse 128 ms, live `/listings?q` TTFB 1387 ms, local POST 92 ms
  - [ok] 2021 Ram 1500, Limited, $29000, 143467 mi, Truck, 4WD, Diesel, Red / Black, Used
  - [ok] 2022 Chevrolet Silverado 1500, LT, $31998, 123590 mi, Truck, 4WD, Diesel, Dark Ash Metallic / Jet Black, Cloth Seat Trim, Used
  - [ok] 2021 GMC Sierra 1500, AT4, $33988, 103693 mi, Truck, 4WD, Diesel, Onyx Black / Jet Black With Kalahari Accents, Perforated Leather Front Seat Trim, Used
  - [ok] 2021 GMC Sierra 1500, 4WD Crew Cab Short Box Elevation, $37493, 65645 mi, Truck, 4WD, Diesel, Cayenne Red / Jet Black, Used
  - [ok] 2021 GMC Sierra 1500, Denali, $41189, 70306 mi, Truck, 4WD, Diesel, ONYX BLACK / JET BLACK, FORGE PERFORATED LEATHER SEAT TRIM, Used
- **F5** `mild hybrid`: live grid 200 cars, local POST 500 (sql_first_relevance_rank); live parse 1092 ms, live `/listings?q` TTFB 1548 ms, local POST 299 ms
  - [no] 2021 Kia Sorento Hybrid, EX, $20576, 81373 mi, SUV, FWD, Hybrid, Silky Silver / Black, Used
  - [no] 2024 Ford Escape, ST-Line, $22840, 20687 mi, SUV, FWD, Hybrid, Star White Metallic Tri Coat / Ebony, Used
  - [no] 2022 Audi A3, Premium, $23894, 20082 mi, Sedan, FWD, Hybrid, Manhattan Gray Metallic / Gray, Used
  - [no] 2024 Honda Accord Hybrid, Sport, $24895, 61326 mi, Sedan, FWD, Hybrid, Radiant Red Metallic / Black, Used
  - [no] 2023 Kia Niro, EX Touring, $25150, 19205 mi, SUV, FWD, Hybrid, Graphite Gray / Medium Gray, Used
- **G1** `white rav4`: live grid 16 cars, local POST 8 (sql_first_relevance_rank); live parse 167 ms, live `/listings?q` TTFB 1306 ms, local POST 98 ms
  - [ok] 2017 Toyota RAV4, LE, $16099, 106076 mi, SUV, FWD, Gasoline, Super White / Black, Used
  - [ok] 2019 Toyota RAV4, LE, $19299, 110771 mi, SUV, FWD, Gasoline, Super White / Black, Used
  - [ok] 2022 Toyota RAV4, LE, $21947, 91266 mi, SUV, FWD, Gasoline, White / Nutmeg, Used
  - [ok] 2025 Toyota RAV4, LE, $26896, 49900 mi, SUV, AWD, Gasoline, White / Black, Used
  - [ok] 2021 Toyota RAV4, XLE, $27381, 56606 mi, SUV, FWD, Gasoline, Super White / Black, Used
- **G2** `black interior`: live grid 200 cars, local POST 156 (sql_first_relevance_rank); live parse 148 ms, live `/listings?q` TTFB 9376 ms, local POST 1304 ms
  - [ok] 2019 Chevrolet Trax, LS, $11998, 85438 mi, SUV, FWD, Gasoline, SILVER ICE METALLIC / JET BLACK, CLOTH SEAT TRIM, Used
  - [ok] 2018 Ford Edge, Sport, $14500, 100419 mi, SUV, AWD, Gasoline, Black / Ebony, Used
  - [ok] 2010 Chevrolet Avalanche, LTZ, $15894, 144273 mi, Truck, 4WD, Gasoline, Black / Ebony, Used
  - [ok] 2023 Chrysler Pacifica, Touring L, $18700, 83190 mi, Vans, FWD, Gasoline, Black / Black/Alloy/Black, Used
  - [ok] 2019 Jeep Cherokee, Limited, $19894, 71980 mi, SUV, 4WD, Gasoline, Granite Crystal Metallic Clearcoat / Black, Used
- **G3** `red mustang`: live grid 12 cars, local POST 7 (sql_first_relevance_rank); live parse 148 ms, live `/listings?q` TTFB 938 ms, local POST 89 ms
  - [ok] 2015 Ford Mustang, EcoBoost, $16100, 78996 mi, Coupe, RWD, Gasoline, Ruby Red Metallic / Ebony, Used
  - [ok] 2026 Ford Mustang, EcoBoost Premium, $37220, 12 mi, Coupe, RWD, Gasoline, Race Red / Black Onyx, New
  - [ok] 2022 Ford Mustang Mach-E, Premium, $27639, 33636 mi, SUV, RWD, Electric, Red Metallic / Light Space Gray, Used
  - [ok] 2024 Ford Mustang, EcoBoost Premium, $28815, 46806 mi, Convertible, RWD, Gasoline, Race Red / Space Gray, Used
  - [ok] 2022 Ford Mustang Mach-E, GT, $32394, 37800 mi, SUV, AWD, Electric, Rapid Red Metallic Tinted Clearcoat / Black Onyx/Copper, Used
- **G4** `blue suv with tan leather`: live grid 137 cars, local POST 55 (sql_first_relevance_rank); live parse 393 ms, live `/listings?q` TTFB 2464 ms, local POST 518 ms
  - [no] 2020 INFINITI QX80, LUXE, $2484, 94734 mi, SUV, 4WD, Gasoline, Blue / Graphite, Used
  - [ok] 2012 Toyota RAV4, Limited, $7498, 200708 mi, SUV, 4WD, Gasoline, Blue / Beige, Used
  - [no] 2020 Chevrolet Equinox, LT, $14931, 95850 mi, SUV, FWD, Gasoline, Pacific Blue Metallic / Jet Black, Used
  - [no] 2022 Jeep Cherokee, Limited, $16995, 104183 mi, SUV, 4WD, Gasoline, Blue / Black, Used
  - [ok] 2019 Subaru Outback, 2.5i, $17495, 108817 mi, SUV, AWD, Gasoline, Blue / Titanium Gray, Used
- **H1** `third row suv`: live grid 103 cars, local POST 3 (sql_first_relevance_rank); live parse 160 ms, live `/listings?q` TTFB 1443 ms, local POST 207 ms
  - [ok] 2014 Ford Expedition EL, XLT, $6984, 211813 mi, SUV, 4WD, Gasoline, White / Camel, Used
  - [ok] 2015 Nissan Pathfinder, SV, $7776, 145712 mi, SUV, 4WD, Gasoline, Glacier White / Charcoal, Used
  - [ok] 2015 Nissan Pathfinder, SL, $11744, 108712 mi, SUV, 4WD, Gasoline, Arctic Blue / Almond, Used
  - [ok] 2019 GMC Acadia, SLT-1, $16132, 118186 mi, SUV, FWD, Gasoline, Red Quartz Tintcoat / Cocoa/Light Ash Gray, Used
  - [ok] 2016 Toyota Highlander, Limited, $17499, 175000 mi, SUV, AWD, Gasoline, Predawn Gray Mica / Black, Used
- **H2** `sunroof`: live grid 200 cars, local POST 500 (sql_first_relevance_rank); live parse 133 ms, live `/listings?q` TTFB 3519 ms, local POST 959 ms
  - [ok] 2016 Volvo XC60, T5 Premier, $14000, 56634 mi, SUV, AWD, Gasoline, Ice White / Off-Black, Used
  - [ok] 2015 Volkswagen Passat, 1.8T SE w/Sunroof & Nav, $7696, 147470 mi, Cars, FWD, Gasoline, Black Uni / Cornsilk Beige, Used
  - [ok] 2012 Toyota Sequoia, Limited, $7698, 263205 mi, SUV, RWD, Gasoline, Magnetic Gray Metallic / Graphite, Used
  - [ok] 2012 Volkswagen Eos, Executive, $8696, 90396 mi, Convertible, FWD, Gasoline, Horizon Blue Metallic / Titan Black, Used
  - [ok] 2016 Toyota Sienna, XLE 8-Passenger, $9427, 239859 mi, Vans, FWD, Gasoline, Silver Sky Metallic / Ash, Used
- **H3** `apple carplay`: live grid 200 cars, local POST 500 (sql_first_relevance_rank); live parse 138 ms, live `/listings?q` TTFB 2744 ms, local POST 872 ms
  - [ok] 2019 Chevrolet Trax, LS, $11998, 85438 mi, SUV, FWD, Gasoline, SILVER ICE METALLIC / JET BLACK, CLOTH SEAT TRIM, Used
  - [ok] 2018 Ford Edge, Sport, $14500, 100419 mi, SUV, AWD, Gasoline, Black / Ebony, Used
  - [ok] 2018 Jeep Grand Cherokee, Limited, $16000, 112097 mi, SUV, 4WD, Gasoline, White / Black, Used
  - [ok] 2019 BMW X3, sDrive30i, $17498, 95030 mi, SUV, AWD, Gasoline, White / Canberra Beige/Black, Used
  - [ok] 2020 Chevrolet Equinox, LS, $18000, 57280 mi, SUV, FWD, Gasoline, SILVER ICE METALLIC / MEDIUM ASH GRAY, PREMIUM CLOTH SEAT TRIM, Used
- **H4** `tow package`: live grid 4 cars, local POST 4 (sql_first_relevance_rank); live parse 378 ms, live `/listings?q` TTFB 1327 ms, local POST 157 ms
  - [ok] 2026 Honda Passport, RTL Towing, $48499, 10 mi, SUV, AWD, Gasoline, Sonic Gray Pearl / Gray, New
  - [ok] 2026 Honda Passport, RTL Towing, $48759, 0 mi, SUV, AWD, Gasoline, Platinum White Pearl / Gray, New
  - [ok] 2026 Honda Passport, RTL Towing, $48907, 0 mi, SUV, AWD, Gasoline, Platinum White Pearl / Black, New
  - [ok] 2026 Honda Passport, RTL Towing, $48989, 0 mi, SUV, AWD, Gasoline, Platinum White Pearl / Black, New
- **H5** `heated seats and remote start`: live grid 200 cars, local POST 500 (sql_first_relevance_rank); live parse 940 ms, live `/listings?q` TTFB 3677 ms, local POST 1154 ms
  - [ok] 2018 Ford Edge, Sport, $14500, 100419 mi, SUV, AWD, Gasoline, Black / Ebony, Used
  - [ok] 2018 Jeep Grand Cherokee, Limited, $16000, 112097 mi, SUV, 4WD, Gasoline, White / Black, Used
  - [ok] 2015 Chevrolet Silverado 1500, LT, $16998, 147026 mi, Truck, 4WD, Gasoline, TUNGSTEN METALLIC / JET BLACK, CLOTH SEAT TRIM, Used
  - [ok] 2023 Chrysler Pacifica, Touring L, $18700, 83190 mi, Vans, FWD, Gasoline, Black / Black/Alloy/Black, Used
  - [ok] 2025 Chevrolet Malibu, 1LT, $19000, 48170 mi, Cars, FWD, Gasoline, Lakeshore Blue Metallic / Jet Black, Premium cloth seat trim, Used
- **I1** `new tacoma`: live grid 200 cars, local POST 104 (sql_first_relevance_rank); live parse 158 ms, live `/listings?q` TTFB 1153 ms, local POST 93 ms
  - [ok] 2025 Toyota Tacoma, SR, $34074, 0 mi, XtraCab, RWD, Gasoline, Ice Cap / Black fabric, New
  - [ok] 2025 Toyota Tacoma, SR, $34074, 0 mi, XtraCab, RWD, Gasoline, Ice Cap / Black fabric, New
  - [ok] 2025 Toyota Tacoma, SR, $34074, 0 mi, XtraCab, RWD, Gasoline, Ice Cap / Black fabric, New
  - [ok] 2025 Toyota Tacoma, SR, $35385, 0 mi, Truck, RWD, Gasoline, Ice Cap / Black fabric, New
  - [ok] 2025 Toyota Tacoma, SR, $35385, 0 mi, Truck, RWD, Gasoline, Ice Cap / Black fabric, New
- **I2** `used bmw`: live grid 200 cars, local POST 317 (sql_first_relevance_rank); live parse 148 ms, live `/listings?q` TTFB 1646 ms, local POST 93 ms
  - [ok] 2020 BMW 3 Series, 330i xDrive, $18937, 98157 mi, Sedan, AWD, Gasoline, Alpine White / Black, Used
  - [ok] 2019 BMW X3, sDrive30i, $17498, 95030 mi, SUV, AWD, Gasoline, White / Canberra Beige/Black, Used
  - [ok] 2021 BMW X5, sDrive40i, $20867, 113358 mi, SUV, RWD, Gasoline, Black Sapphire Metallic / Cognac, Certified Pre-Owned
  - [ok] 2020 BMW X5, xDrive40i, $28963, 73362 mi, SUV, AWD, Gasoline, Black Sapphire Metallic / Black, Used
  - [ok] 2024 BMW 4 Series, 430i xDrive Gran Coupe, $38890, 28004 mi, Sedan, AWD, Gasoline, Mineral White Metallic / Tacora Red, Certified Pre-Owned
- **I3** `certified pre-owned lexus`: live grid 86 cars, local POST 90 (sql_first_relevance_rank); live parse 158 ms, live `/listings?q` TTFB 1355 ms, local POST 72 ms
  - [ok] 2025 LEXUS NX 350, 350 Premium, $49595, 11215 mi, SUV, 4WD, Gasoline, Caviar / Black, Certified
  - [ok] 2024 LEXUS TX 350, TX 350 Luxury, $50869, 54491 mi, SUV, FWD, Gasoline, Wind Chill Pearl / Black, Certified
  - [ok] 2021 LEXUS NX 300, 300 F SPORT, $30274, 67339 mi, SUV, FWD, Gasoline, Obsidian / F-SPO Black, Certified
  - [ok] 2023 LEXUS UX 250h, 250h F SPORT Handling, $35342, 19710 mi, SUV, FWD, Hybrid, Ultra White / Rioja Red - Black Meta, Certified
  - [ok] 2023 LEXUS IS 300, 300, $36542, 35760 mi, Sedan, RWD, Gasoline, Iridium / Black - Black Open POR, Certified
- **J1** `honda near charlotte`: live grid 200 cars, local POST 500 (sql_first_relevance_rank); live parse 446 ms, live `/listings?q` TTFB 1709 ms, local POST 235 ms
  - [ok] 2015 Honda Accord Sedan, Sport, $14017, 101165 mi, Sedan, FWD, Gasoline, Maroon / Black, Used
  - [ok] 2014 Honda CR-V, EX-L, $15494, 97425 mi, SUV, FWD, Gasoline, White Diamond Pearl / Beige, Used
  - [ok] 2014 Honda CR-V, EX-L, $15549, 89849 mi, SUV, AWD, Gasoline, White Diamond Pearl / Gray, Used
  - [ok] 2020 Honda Accord Sedan, Sport, $17924, 98369 mi, Sedan, FWD, Gasoline, Platinum White Pearl / Black, Used
  - [ok] 2016 Honda Accord Coupe, EX-L, $18994, 92834 mi, Coupe, FWD, Gasoline, Crystal Black Pearl / Black, Used
- **J2** `trucks in matthews nc`: live grid 200 cars, local POST 364 (sql_first_relevance_rank); live parse 386 ms, live `/listings?q` TTFB 2034 ms, local POST 290 ms
  - [ok] 2012 Ford F-150, XLT, $13879, 126438 mi, Truck, 4WD, Gasoline, Silver / Gray, Used
  - [ok] 2015 Ram 1500, Big Horn, $14898, 148913 mi, Truck, 4WD, Gasoline, Gray / Diesel Gray/Black, Used
  - [ok] 2015 Chevrolet Silverado 1500, LT, $16998, 147026 mi, Truck, 4WD, Gasoline, TUNGSTEN METALLIC / JET BLACK, CLOTH SEAT TRIM, Used
  - [ok] 2023 GMC Sierra 1500, Pro, $24879, 77262 mi, Truck, RWD, Gasoline, Summit White / Jet Black, Certified
  - [ok] 2020 Ram 1500 Classic, Tradesman, $25398, 62936 mi, Truck, 4WD, Gasoline, Patriot Blue Pearlcoat / Diesel Gray/Black, Used
- **J3** `toyota dealer in concord`: live grid 200 cars, local POST 500 (sql_first_relevance_rank); live parse 362 ms, live `/listings?q` TTFB 1568 ms, local POST 245 ms
  - [ok] 2018 Toyota C-HR, XLE, $15917, 82202 mi, SUV, FWD, Gasoline, Magnetic Gray Metallic / Black, Used
  - [ok] 2021 Toyota Camry, SE, $16629, 109996 mi, Sedan, FWD, Gasoline, Predawn Gray Mica / Black, Used
  - [ok] 2022 Toyota Corolla, SE, $17540, 94427 mi, Sedan, FWD, Gasoline, Blue / Black, Used
  - [ok] 2021 Toyota Camry, LE, $18751, 95928 mi, Sedan, FWD, Gasoline, Super White / Ash, Used
  - [ok] 2020 Toyota Camry, LE, $18850, 76159 mi, Sedan, FWD, Gasoline, Ruby Flare Pearl / Black, Used
- **K1** `family suv under 30k with good mpg`: live grid 0 cars, local POST 0 (sql_first); live parse 864 ms, live `/listings?q` TTFB 1075 ms, local POST 199 ms
  - (no results)
- **K2** `cheap reliable commuter`: live grid 0 cars, local POST 0 (no_parse_match); live parse 485 ms, live `/listings?q` TTFB 1159 ms, local POST 119 ms
  - (no results)
- **K3** `fast car for a teenager`: live grid 0 cars, local POST 0 (sql_first); live parse 328 ms, live `/listings?q` TTFB 1350 ms, local POST 193 ms
  - (no results)
- **K4** `Something reliable for a first car, automatic, under $20k, low miles if possible`: live grid 0 cars, local POST 0 (sql_first); live parse 443 ms, live `/listings?q` TTFB 1069 ms, local POST 158 ms
  - (no results)
- **K5** `gas saver`: live grid 200 cars, local POST 500 (sql_first_relevance_rank); live parse 171 ms, live `/listings?q` TTFB 1626 ms, local POST 161 ms
  - [ok] 2021 Kia Sorento Hybrid, EX, $20576, 81373 mi, SUV, FWD, Hybrid, Silky Silver / Black, Used
  - [ok] 2024 Ford Escape, ST-Line, $22840, 20687 mi, SUV, FWD, Hybrid, Star White Metallic Tri Coat / Ebony, Used
  - [ok] 2022 Audi A3, Premium, $23894, 20082 mi, Sedan, FWD, Hybrid, Manhattan Gray Metallic / Gray, Used
  - [ok] 2024 Honda Accord Hybrid, Sport, $24895, 61326 mi, Sedan, FWD, Hybrid, Radiant Red Metallic / Black, Used
  - [ok] 2023 Kia Niro, EX Touring, $25150, 19205 mi, SUV, FWD, Hybrid, Graphite Gray / Medium Gray, Used
- **M1** `Accord with heated seats under $30k`: live grid 0 cars, local POST 0 (sql_first); live parse 378 ms, live `/listings?q` TTFB 1157 ms, local POST 175 ms
  - (no results)
- **M2** `BMW X5 with M Sport under $70k AWD under 50k miles`: live grid 0 cars, local POST 0 (sql_first); live parse 332 ms, live `/listings?q` TTFB 1893 ms, local POST 155 ms
  - (no results)
- **M3** `Silverado crew cab with tow package under 80k miles`: live grid 0 cars, local POST 0 (sql_first); live parse 437 ms, live `/listings?q` TTFB 1276 ms, local POST 279 ms
  - (no results)
- **L1** `asdf qwerty`: live grid 0 cars, local POST 0 (no_parse_match); live parse 1132 ms, live `/listings?q` TTFB 1333 ms, local POST 117 ms
  - (no results)
- **L2** `banana`: live grid 0 cars, local POST 0 (no_parse_match); live parse 498 ms, live `/listings?q` TTFB 1710 ms, local POST 117 ms
  - (no results)
