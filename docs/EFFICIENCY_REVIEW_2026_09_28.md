# Efficiency review: backend and database (2026-09-28)

Scope: the Flask app served by `run.py` (Werkzeug dev server, debug on, threaded, one process,
PID 99054) at http://localhost:5001 against Postgres 17.10 (`postgresql:///cars?host=/tmp`),
214,678 active rows in `cars` (357,148 total). Read-only: no code, database or process was
changed. All SQL ran with `default_transaction_read_only=on`; `EXPLAIN (ANALYZE, BUFFERS)` on
SELECTs only. Timings are 5 sequential `curl` requests per endpoint, median reported; the first
request is called out where it differs.

## 1. Measured endpoints

| Endpoint | Median | First hit | Bytes (raw / gzip) | Served from |
|---|---|---|---|---|
| `/` (landing, anonymous) | 150 ms | 161 ms | 15,294 | Not cached: `public_listings_count()` runs `COUNT(*)` over the fleet on every request (153 ms of DB, Q6c below). Featured cars are cached 300 s. |
| `/listings` | 31 ms | 3,710 ms | 1,460,089 / 165,896 | Facet options: in-process cache, 60 s time-bucket token, stale-while-revalidate. First hit built the facets inline. `pack_car_rows` (61k rows) runs per request. |
| `/listings?make=Toyota` | 30 ms | 31 ms | 1,460,108 | Same document; the make filter is applied client-side in main.js. |
| `/listings?zip_code=92694&radius=25` | 45 ms | 177 ms | 1,455,982 | Same, plus `zip_to_coords` and a nearby round-robin over the grid for the 48 SSR cards. Two of five runs took 177 and 213 ms (background rebuild contention). |
| `/api/listings/cars` (curl default, no `Accept-Encoding`) | 174 ms | 2,830 ms | 291,849,433 | In-process cache of the gzipped body; the non-gzip path streams `gzip` decompression per request (291 MB written to the socket). |
| `/api/listings/cars` with `Accept-Encoding: gzip` | 5.5 ms | - | 32,298,381 | Cache hit, `ETag: W/"6-ca0fe54b64c7dc58-214678"`, `Cache-Control: public, max-age=0, s-maxage=60, stale-while-revalidate=30`, `Vary: Accept-Encoding`. No `X-Cache` header exists. |
| `/api/listings/cars` with matching `If-None-Match` | 0.7 ms | - | 0 (304) | ETag path. |
| `/api/listings/cars?zip_code=92694&radius=25` | 173 ms | - | 291,849,433 | Identical to the unfiltered payload: the route ignores query parameters (radius filtering is client-side). |
| `/api/listings/filter-options` | 3.0 ms | - | 869,571 / 82,652 | Facet cache (same object as `/listings`). `jsonify` per request; no ETag. |
| `/api/listings/geo-coords` | 2.7 ms | timed out at 60 s (HTTP 000), second call 24 ms | 164,532 / 39,474 | Cached per 60 s token; `Cache-Control: private, max-age=300`. The first call runs `ensure_dealership_registry_backfill()` (an `UPDATE cars ... LIKE` loop, once per process) plus three geo queries, inline on a GET. |
| `/car/1470314` | 52 ms | 1,039 ms | 62,791 | Not cached server-side (`Cache-Control: private, max-age=180`). Cold hit pays imports and lru caches. |
| `/car/1469872` (2026 Jeep Grand Cherokee, CDJR sticker-eligible) | 35 ms | 342 ms | 64,669 | Same path; sticker fetch is deferred to `car_packages.js`, so sticker eligibility adds nothing to render time. |
| `/car/1469351` (2007 Toyota Tacoma) | 37 ms | 155 ms | 55,520 | Same. |
| `/car/1470314?embed=1` | 45 ms | 83 ms | 57,766 | `embed` is the only query flag `car_detail` reads; there is no flag to skip AI/sticker sections. |
| `/api/cars/1470314` (JSON of the same context) | 48 ms | 1,233 ms | 17,672 | Same builder. |
| `/dealership/infinitiofcharlotte-com` (2,518 active rows) | 91 ms | 98 ms | 211,351 | Not cached: filters the 214,678-dict grid list and recomputes facets on every request (`_dealer_grid_cars`, `_dealer_facets`). |
| `/dealership/autosavvy-com` (3,958 active rows) | 98 ms | 106 ms | 158,704 | Same. |
| `/find-dealers` | 1.3 ms | - | 14,324 | Static template. |
| `POST /api/search/smart` `{"query":"used toyota under 30k near 92694","zip_code":"92694","radius":25}` (+ `X-CSRF-Token` from `GET /api/auth/csrf`, session cookie) | 229 ms | 2,748 ms | 317 | Not cached. Parser output `{'make': 'Toyota', 'max_price': 30000, 'trim_contains': 'used'}`; mode `sql_first`; **0 results** because the word "used" became a trim substring filter. Two of eight runs took 0.6 s and 3.5 s. |
| `POST /api/search/smart` `{"query":"used toyota under 30k"}` | 672 ms | - | 340 | Same, without the geo pass. |
| `/listings?q=used+toyota+under+30k` (text search on the page) | **36.8 s** | 40.9 s | **237,158,192** | Not cached. The HTML embeds every one of the 214,678 active cars (`"dealer_id"` counted 214,678 times). Process RSS jumped from 2.66 GB to 7.77 GB during the request. See Finding D1. |
| `/health` | 0.7 ms | - | 43 | No DB. |
| `/api/ready` | 2.8 ms | - | 105 | One DB ping; pgvector/redis skipped. |

Notes on caching: the only cache validators the app emits are the `ETag` on `/api/listings/cars`
and the HTML page's session cookie (`Vary: Cookie`). The `_gzip_large_json` after-request hook
gzips every HTML/JSON response over 2 KB per request when the client advertises gzip (about 30 ms of
CPU for the 1.46 MB `/listings` document: 59 ms with gzip vs 31 ms without).

## 2. Database findings

### 2.1 Environment

* `pg_stat_statements` is **not installed** (extensions: `plpgsql`, `vector`, `dblink`), so the
  queries below were reconstructed from the builders (`search_repo.search_cars`,
  `listings_repo._build_grid_cars_uncached`, `_build_filter_options_uncached`,
  `public_listings_count`, `cars_repo.get_car_by_id`, `dealership_page._dealer_grid_cars`,
  `search_history_repo`). `backend/db/inventory_pg.py` rewrites `IFNULL`->`COALESCE` and
  `INSTR`->`strpos`, so the Postgres text is as shown.
* Server settings: `shared_buffers=128MB`, `work_mem=4MB`, `effective_cache_size=4GB`,
  `random_page_cost=4`, `track_io_timing=off`, `max_connections=100`.
* Connections: `db_conn()` calls `psycopg.connect(..., autocommit=False)` for every call; there is
  no pool. A car page opens roughly seven connections; `pg_stat_activity` showed one
  `idle in transaction` session (implicit read transaction held until `close()`).

### 2.2 Table state (`pg_stat_user_tables`, `pg_class`)

| Table | live | dead | last autovacuum | size (heap / toast / idx) |
|---|---|---|---|---|
| cars | 356,218 | 51,424 (14%) | 2026-09-28 13:14 | 673 MB / 442 MB / 84 MB (1,221 MB total) |
| nhtsa_vpic_cache | 329,036 | 1 | 2026-09-28 13:23 | 34 MB / **847 MB** / 17 MB (raw vPIC bodies in TOAST) |
| cars_epa_link_backup_20260921 | 163,165 | 0 | 09-21 | 15 MB (backup table) |
| cars_backup_capacity_20260718 | - | - | - | 127 MB (backup table) |
| package_observations | 102,997 | 1,483 | 09-25 | 40 MB |
| package_values | 58,197 | 6,389 | never | 29 MB |
| incomplete_listings | 20,330 | 3,701 | 09-28 10:56 | - |
| dealerships | 630 | 113 | never | small |
| epa_master | 461 | 0 | never | 20 MB |

Bloat is modest; autovacuum is keeping up on `cars`. The two backup tables (142 MB) and the
vPIC TOAST (847 MB of raw JSON bodies) are storage, not latency.

`listing_active` has **no NULLs** (1: 214,678 rows, 0: 142,470 rows), `marked_for_review` is
100% NULL. `pg_stats` for `make` shows 82 distinct values including case duplicates
(`Lexus`/`LEXUS`, `Ram`/`RAM`, `Hyundai`/`HYUNDAI`, `Cadillac`/`CADILLAC`, `INFINITI`).

### 2.3 Indexes on the tables in scope

`cars`: `cars_pkey (id)`, `cars_vin_key (vin)`, `idx_cars_dealer_listing (dealer_id, listing_active)`,
and six partial indexes whose predicate is `COALESCE(listing_active, 1) = 1`:
`idx_cars_active_facet_combo (make, model, trim, fuel_type, cylinders, drivetrain, body_style)`,
`idx_cars_active_make (make)`, `idx_cars_active_price (price)`, `idx_cars_active_registry`,
`idx_cars_active_packages (make, model)`, `idx_cars_active_zip (zip_code)`.
`dealerships`: pkey, `created_at`, `google_place_id`, `zip_code`. `nhtsa_vpic_cache`: pkey on `vin`.
`saved_cars`: pkey, `(user_id, car_id)`. `user_hidden_dealers`: pkey, `(user_id, created_at)`,
`(user_id, dealer_id)`. `user_search_history`: pkey, `(user_id, created_at DESC, id DESC)`.

Usage (`pg_stat_user_indexes.idx_scan`): `cars_pkey` 3.58M, `cars_vin_key` 2.09M,
`nhtsa_vpic_cache_pkey` 4.05M, `idx_cars_dealer_listing` 215k, `idx_cars_active_facet_combo` 24k,
`idx_cars_active_make` 1,351, `idx_cars_active_packages` 66, `idx_cars_active_registry` 48,
`idx_cars_active_price` 12, `idx_cars_active_zip` **0** (the column was dropped from use).

### 2.4 EXPLAIN (ANALYZE, BUFFERS) of the hot queries

All plans share one defect: the planner cannot estimate `COALESCE(listing_active, 1) = 1`
(estimates `rows=1` to `rows=9`, actual 49k to 214k), so it picks a bitmap scan on the partial
index for every fleet-wide query, and at `work_mem=4MB` the bitmap goes lossy
(`Heap Blocks: exact=43465 lossy=33155`, `Rows Removed by Index Recheck: 44318`). Every fleet scan
also reads the whole 673 MB heap from the OS cache (`shared read=77481, hit=3`) because
`shared_buffers` is 128 MB.

| # | Query (as built) | Rows | Time | Plan | Notes |
|---|---|---|---|---|---|
| Q1 | `search_cars` make filter: `SELECT * FROM cars WHERE COALESCE(listing_active,1)=1 AND COALESCE(marked_for_review,0)=0 AND LOWER(TRIM(COALESCE(make,''))) IN ('toyota')` | 49,219 | **198 ms** | Bitmap Heap Scan on `idx_cars_active_facet_combo` (no Index Cond), Filter removes 165,459 rows | The `LOWER(TRIM(...))` wrapper defeats every make index. 49k rows x 2.5 KB = ~120 MB shipped to Python per call. |
| Q1b | Q1 + hidden-dealer clause `AND (dealer_id IS NULL OR dealer_id NOT IN ('autosavvy-com','billluke-com','covertfordaustin-com'))` | 48,878 | 167 ms | Same plan, clause evaluated in the Filter | **No measurable cost** (within run-to-run noise of Q1). |
| Q1c | Q1 with an index-friendly predicate `make IN ('Toyota','TOYOTA','toyota')` | 49,219 | **58 ms** | Bitmap Index Scan with `Index Cond: make = ANY(...)`, 35k buffers, no lossy blocks | 3.4x faster; what a functional index or normalized make would buy. |
| Q2 | `get_car_by_id`: `SELECT * FROM cars WHERE id = 1470314` | 1 | 0.11 ms | `cars_pkey` | Fine. |
| Q3 | Dealer rows: `SELECT * FROM cars WHERE dealer_id='infinitiofcharlotte-com' AND COALESCE(listing_active,1)=1` | 2,518 | 31 ms | BitmapAnd(`idx_cars_active_facet_combo` full 252k TIDs, `idx_cars_dealer_listing`) | 19 of 31 ms is the useless active-index bitmap. (The page does not run this; it scans the grid list in memory.) |
| Q4 | vPIC: `SELECT * FROM nhtsa_vpic_cache WHERE vin = (SELECT vin FROM cars WHERE id=1470314)` | 1 | 0.41 ms | pkey | Fine. |
| Q5 | Grid build (`LISTINGS_GRID_CAR_COLUMNS ... ORDER BY price ASC`) | 214,678 | **441 ms** | Bitmap Heap Scan + `Sort Method: external merge Disk: 163224kB` | With `SET work_mem='256MB'`: quicksort in memory, no lossy blocks, **300 ms**. Runs at least every 300 s. |
| Q6 | Facet cascade: `SELECT DISTINCT make, model, trim, fuel_type, cylinders, drivetrain, body_style, forced_induction, year, engine_description, engine_l FROM cars WHERE active AND make IS NOT NULL AND TRIM(make) != '' ORDER BY make, model, trim` | 61,383 | **758 ms** | Sort external merge 20 MB, Unique | Runs every 60 s (time-bucket token). |
| Q6b | Facet colors: `SELECT exterior_color, interior_color, interior_color_buckets FROM cars WHERE active` | 214,678 | 151 ms | Bitmap Heap Scan | Every 60 s. |
| Q6c | `public_listings_count`: `SELECT COUNT(*) FROM cars WHERE active AND not flagged` | 1 | **153 ms** | Bitmap Heap Scan, all 77k pages | Runs on every `/` request; also every 60 s inside the facet rebuild. |
| Q6d | Facet packages: `SELECT make, model, packages FROM cars WHERE active AND packages IS NOT NULL AND packages NOT IN (...)` | 104,252 | 156 ms | Index Scan `idx_cars_active_packages` | Then 104k `json.loads` in Python, every 60 s. |
| Q7 | `record_search_history` latest row: `SELECT id, filters_json, created_at FROM user_search_history WHERE user_id=1 ORDER BY created_at DESC, id DESC LIMIT 1` | 0 | 0.012 ms | `idx_user_search_history_user` | Table is empty (0 rows in `user_search_history`, `user_hidden_dealers`, `saved_cars`). Insert + `COUNT(*)` + conditional prune + commit is 3-4 round trips per signed-in search; sub-millisecond at this size. |
| Q8 | Toyota + `(price IS NULL OR price <= 30000 OR price = 0)` (smart search shape) | 8,551 | 81 ms | BitmapOr on `idx_cars_active_price` x3, Filter on make removes 40,540 | Price index carries it; the make test is still a filter. |
| Q9 | 100 semantic candidate ids: `... AND id IN (100 ids)` | 100 | 24 ms | BitmapAnd(`idx_cars_active_facet_combo` 252k TIDs = 10 ms, `cars_pkey`) | The bad estimate drags the active-index bitmap into a primary-key lookup. |
| Q10 | Equipment needle: `strpos(LOWER(COALESCE(packages,'')),'sunroof')>0 OR ... description ... title ... trim ... engine_description` | 39,128 | **738 ms** | Bitmap Heap Scan, 181k buffers (TOAST for description) | Package/feature search path (`packages_json_contains*`). |

### 2.5 Hidden-dealer exclusion and search-history cost

* `_exclude_dealer_ids_clause` adds `AND (dealer_id IS NULL OR dealer_id NOT IN (...))`. Q1 vs Q1b:
  198 ms vs 167 ms, i.e. no measurable cost; the predicate is evaluated on rows the scan already
  fetched. With up to 200 hidden dealers (`MAX_HIDDEN_DEALERS_PER_USER`) it stays a cheap array
  test. The Python-side `drop_hidden_dealer_cars` over the 48 bootstrap cards is negligible.
* `record_search_history` is one `SELECT ... LIMIT 1` (index, 0.012 ms), one `INSERT`, one
  `SELECT id ... ORDER BY id DESC LIMIT 1` (psycopg has no `lastrowid`), one `COUNT(*)`, and a
  `DELETE ... NOT IN (subquery LIMIT 100)` only when a user exceeds 100 rows, plus a commit. All on
  the indexed `(user_id, created_at DESC, id DESC)` path. Cost is dominated by the new
  `psycopg.connect` it opens, not the statements. Anonymous visitors skip it entirely.

### 2.6 Index and configuration recommendations

1. **Fix the estimate** (ship now, one statement): `CREATE STATISTICS cars_active_expr ON
   (COALESCE(listing_active, 1)), (COALESCE(marked_for_review, 0)) FROM cars; ANALYZE cars;`.
   PG 14+ collects stats on expressions, so the planner stops guessing `rows=1`. Longer term,
   since there are zero NULLs, `ALTER TABLE cars ALTER COLUMN listing_active SET NOT NULL, SET
   DEFAULT 1` and write `listing_active = 1` in the builders and index predicates.
2. **work_mem** for the app role: `ALTER ROLE <app_user> SET work_mem = '128MB'` (or `SET` per
   connection in `pg_connect`). Measured on Q5: 441 ms to 300 ms, no disk sort, no lossy bitmap
   recheck of 44k rows on every fleet scan.
3. **shared_buffers** 128 MB against a 673 MB heap + 442 MB TOAST: every fleet scan is 77k
   page reads from the OS cache. On the dev Mac raise to 1-2 GB; on Railway (Postgres 468 MB
   container) it explains why fleet scans are I/O-bound there.
4. **Case-insensitive make/model/trim**: either normalize make/model/trim casing at write time and
   change `add_multi_ci` to plain `IN`, or add
   `CREATE INDEX idx_cars_active_make_ci ON cars ((lower(trim(coalesce(make,''))))) WHERE
   COALESCE(listing_active,1) = 1` (and model). Measured gain per `search_cars` call: 198 ms to
   58 ms (Q1 vs Q1c). Normalizing also removes the duplicate facets (LEXUS/Lexus, RAM/Ram).
5. **Equipment search**: a `pg_trgm` GIN index on `packages` (and optionally `description`) turns
   Q10's 738 ms five-column `strpos` scan into an index lookup; or precompute a `tsvector`
   column. Medium effort; only matters if package search is used.
6. **Drop** `idx_cars_active_zip` (0 scans, column no longer used) and the two backup tables
   (142 MB) after confirming with the owner. Consider trimming `nhtsa_vpic_cache` raw bodies
   (847 MB TOAST) to the fields `flat_vpic_result_to_car_patch` reads.
7. Install `pg_stat_statements` (`shared_preload_libraries`) so the next review can measure
   instead of reconstruct.

## 3. Process and memory

### 3.1 Runtime

* `run.py` runs `app.run(debug=True, use_reloader=False)`: one Python 3.14 process, Werkzeug
  threaded (one thread per request), no worker recycling. `ps -M` showed 3-4 threads idle.
* Production (`scripts/docker-entrypoint-web.sh`) defaults to `GUNICORN_WORKERS=3 --threads 4
  --preload -c gunicorn.conf.py`; the Railway service overrides to `GUNICORN_WORKERS=1` after the
  2026-09-16 finding that three workers each held the fleet (13.7 GB -> 4.92 GB). The
  `gunicorn.conf.py` `post_fork` hook starts the prewarm per worker so the arbiter does not build
  a grid it never serves.

### 3.2 RSS samples of PID 99054 during this review

| Elapsed | RSS | What was happening |
|---|---|---|
| 2m28 | 3.59 GB | Just after boot; grid prewarm finished |
| 4m42 | 2.62 GB | Warm, idle (baseline) |
| 7m21 | 7.75 GB | Immediately after two `/listings?q=...` fleet-embedding requests |
| 8m18 | 5.15 GB | One minute later |
| 11m50 | 2.66 GB | Idle again |
| 12m26 | 7.77 GB | Immediately after one more `/listings?q=...` |
| 13m13-13m25 | 5.28 GB steady | High-water mark retained by the allocator |

Warm baseline is about 2.6 GB: `_grid_cars_cache_value` (214,678 dicts), `_grid_serialize_memo`
(same objects keyed by row digest), the 32 MB gzipped cars JSON, the facet options (0.87 MB JSON
plus 61k cascade rows), geo maps, and the EPA dictionary index. A single text search on
`/listings` adds about 5 GB transiently and leaves the process 2.6 GB heavier.

### 3.3 Startup and periodic work

Startup (`backend/main.py`): `init_users_db`, `init_admin_db`, `init_inventory_db`,
`init_dealer_portal_db`, `init_job_queue_schema` (schema checks/DDL), then a daemon thread:
EPA dictionary index (`_epa_paths_by_make_norm`), 3 s head start
(`LISTINGS_PREWARM_GRID_DELAY_S`), `_incomplete_car_ids_for_listings`, then the full grid build
(Q5 + `car_attribution_states` + serialization of 214k rows with a 2 ms sleep every 100 rows).
`rebuild_listings_index` is not run at startup; it is the nightly `rebuild_listings_index.py`
(`com.sarraficars.nightly-data-quality.plist`). Facet options, geo maps and the incomplete snapshot
are built inline on their first request (3.7 s and 60 s+ measured above).

Periodic, regardless of traffic (each is triggered by the next request that sees a stale token):

* Grid: rebuilt when the `pg_stat_user_tables` write fingerprint of `cars`,
  `incomplete_listings`, `market_price_stats` changes (checked at most every 60 s) and
  unconditionally every 300 s (`_GRID_MAX_CACHE_AGE_S`) for the two wall-clock fields. Each
  rebuild is Q5 (0.3-0.45 s DB, ~200 MB of tuples into Python) plus the memo pass; the
  `/api/listings/cars` gzip is re-encoded on the next hit (291 MB `jsonify` + gzip level 5).
* Facet options: rebuilt every **60 s** because `_listings_cache_token()` on Postgres is
  `int(time.time()) // 60` (a clock, not a data fingerprint): Q6 + Q6b + Q6c + Q6d + five
  `DISTINCT` queries, about 1.3 s of DB plus `json.loads` on 104k package blobs, every minute
  the site has any traffic. Geo maps (3 queries) and the incomplete-id snapshot follow the same
  60 s clock.
* Observed once in `pg_stat_activity`: **three concurrent** full-fleet grid SELECTs
  (`SELECT id, vin, title, year, make, ...`, `wait_event=Client`, 10-12 s into their
  transactions) while requests were in flight. The cold path of `listings_grid_serialized_cars()`
  (`_grid_cars_cache_value is None`) builds inline with no lock, so anything that clears the
  cache lets every concurrent request rebuild the fleet.

### 3.4 Per-request work that could be cached

* `/`: `public_listings_count()` `COUNT(*)` = 153 ms of a 150 ms page. Cache 300 s or use
  `len(listings_grid_serialized_cars())`.
* `/dealership/<id>`: linear scan of 214,678 grid dicts + `_dealer_facets` per request (90 ms).
  Cache per `(dealer_id, grid token)`.
* `/listings`: `pack_car_rows` re-encodes 61k cascade rows per request and `render_template`
  emits a 1.46 MB document that is then gzipped per request; the packed table and the facet
  block can be cached with the facet token. Small win (page is 31 ms warm).
* `/api/listings/filter-options` and `/api/listings/geo-coords`: `jsonify` of 0.87 MB / 0.16 MB
  per request, no ETag; cache the encoded bytes with the token like `/api/listings/cars` does.
* `db_conn()` opens a new `psycopg.connect` per call (no pool). A car page opens about seven
  (car row, incomplete fields, dealership, attribution, sticker MSRP, vision summary, registry
  coords), `/listings` three to five. Over a Unix socket this is 1-2 ms each; over Railway's
  TCP+TLS it is 20-50 ms each, i.e. 150-350 ms per car page in production that the local
  numbers hide.

## 4. Read-time joins on the car page

`car_detail` -> `get_car_by_id` (Q2) -> `_build_car_detail_view_context`
(`backend/routes/cars_pages.py:436`), which assembles per request:

| Piece | Source | Per-request cost | Materialized? |
|---|---|---|---|
| `prepare_car_detail_context` (`knowledge_engine_specs.py:1089`) | parses `cars.packages` JSON, sticker option sections, LLaVA interior, verified EPA specs from the in-memory dictionary | pure Python + in-memory EPA index | EPA dictionary built by `dictionary_rebuild.py`, loaded at prewarm |
| Window-sticker availability (`window_sticker_service`, `scanner.window_sticker`) | file/DB existence checks; fetch itself is async via `car_packages.js` `/packages/ensure` | small | On-demand (per the standing rule: leave the OEM fetch alone) |
| `serialize_car_for_api` | vPIC facts already merged into the row by the scanner | pure Python | vPIC values written at scan time (`nhtsa_vpic_cache`, Q4 is only hit by the scanner) |
| `get_missing_field_codes_for_car_id` | `incomplete_listings` by `car_id` | 1 query | Nightly `rebuild_listings_index.py` |
| `get_dealership_by_id` | `dealerships` pkey | 1 query | - |
| `car_attribution_states([id])`, `car_sticker_msrp_values([id])` | attribution / `car_image_text` sidecars | 2 queries | Written by the vision scan |
| `build_dealer_map_for_car` | registry coords, geopoints | 1 query | - |
| `market_price_for_car` + `detailed_deal_score` | **paid viewers only**; `deal_score_cache` holds `market_price_stats` bands in-process (300 s TTL) | 0-1 query | Nightly `compute_market_stats.py` |
| `resolve_trim_ladder` | ladder definitions JSON, `lru_cache`d | in-memory | `build_trim_ladders*.py` scripts (on disk) |
| `rarity_for_car` + `vision_summary_for_car` | `car_image_text` by `car_id` | 1 query + Python | Vision scan |
| `build_generated_spec_sheet` (only when no real sticker) + `build_unified_options_list` | pure Python over the above | ~ms | Not materialized (cheap) |

Measured: warm car page 35-52 ms for three different cars, 1.0 s on the first hit (imports and
`lru_cache` fills). There is no query flag to disable the AI/sticker sections (`car_detail` reads
only `embed`); the CDJR sticker-eligible car (35 ms) and a plain Toyota (37 ms) render in the same
time because the sticker fetch is deferred to the client. The car page is not a latency problem
locally; its avoidable cost is the seven un-pooled connections (see 3.4), which matters on Railway.

## 5. Findings that drive the ranking

* **D1. `/listings?q=...` embeds the whole fleet.** `listings_page` -> `hybrid_search_with_kwargs`.
  `parse_natural_query` produces structured filters, so the code asks pgvector for candidates;
  `pgvector_configured()` is false here (`PGVECTOR_URL` unset), `_semantic_car_ids` returns `[]`,
  and the fallback is `search_cars(**sql_kwargs)` where `sql_kwargs` came from the request's
  facet params, not from the parsed query. With no facets set that is an unfiltered
  `SELECT * FROM cars WHERE active` (214,678 rows x 2.5 KB = ~500 MB into Python), then
  `serialize_cars_for_listings_grid` and Jinja embed all of them: 36.8 s, 237 MB, +5 GB RSS,
  mode `sql_fallback_no_semantic_index`. Any anonymous GET can do this; on a 1-worker Railway
  service it is an OOM. The smart-search API already maps parsed filters to SQL
  (`filters_dict_to_search_cars_kwargs`) and does not have this problem.
* **D2.** Smart search parsed "used" as `trim_contains='used'` and returned 0 rows for
  "used toyota under 30k" (correctness, noted for the owner; not an efficiency issue).
* **D3.** Planner estimates on `COALESCE(listing_active,1)=1` are off by 4-5 orders of magnitude,
  which costs 10-20 ms on every indexed lookup (Q3, Q9) and forces lossy bitmaps on fleet scans.
* **D4.** Facet/geo/incomplete caches expire on a 60 s clock instead of the write fingerprint the
  grid already uses.
* **D5.** `ensure_dealership_registry_backfill` runs an `UPDATE cars` loop inline on the first
  `GET /api/listings/geo-coords` of each process (60 s+ observed).

## 6. Ranked recommendations

| # | What | Expected gain | Effort | When |
|---|---|---|---|---|
| 1 | **Never embed an unbounded search result in `/listings`.** In `hybrid_search_with_kwargs`, when the semantic index is unavailable, merge the parsed filters into `sql_kwargs` (as `hybrid_smart_search` does) and refuse the unfiltered fallback; cap `initial_grid_cars` (e.g. 200) and add `LIMIT` support to `search_cars`. | Measured 36.8 s / 237 MB / +5 GB RSS per request -> sub-second, <300 KB; removes a trivial remote OOM. | S | Ship now |
| 2 | **Postgres planner and memory config**: `CREATE STATISTICS` on the two `COALESCE` expressions + `ANALYZE`; `work_mem` 128 MB for the app role; `shared_buffers` 1-2 GB on the dev box (and revisit on Railway). | Grid build 441 -> 300 ms (measured); no lossy recheck of 44k rows per fleet scan; 10-20 ms off every indexed `cars` lookup (Q3/Q9). | S | Ship now (config only) |
| 3 | **Cache `public_listings_count()`** (300 s TTL or `len(grid)`). | `/` 150 ms -> ~5 ms (153 ms of DB per landing view, measured). | S | Ship now |
| 4 | **Index-friendly make/model/trim filter**: normalize case at write time and use plain `IN`, or add expression indexes on `lower(trim(coalesce(col,'')))`. | `search_cars` per call 198 -> 58 ms (Q1 vs Q1c); also dedupes LEXUS/Lexus facets. | S-M | Next release |
| 5 | **Connection pool** (`psycopg_pool.ConnectionPool` behind `db_conn()`, autocommit for reads). | Removes ~7 connects per car page and 3-5 per listings page; 150-350 ms per car page on Railway (estimated from TCP+TLS connect cost), 5-15 ms locally; ends `idle in transaction` sessions. | M | Next release |
| 6 | **Facet/geo/incomplete caches keyed by the write fingerprint** (reuse `_pg_grid_write_fingerprint`) instead of the 60 s clock; also cache the encoded JSON bytes + ETag for `/api/listings/filter-options` and `/api/listings/geo-coords`. | Stops ~1.3 s of DB + 104k `json.loads` every minute under any traffic; filter-options 3 ms -> <1 ms. | S | Next release |
| 7 | **Move `ensure_dealership_registry_backfill` off the GET path** (startup thread or nightly job). | First `/api/listings/geo-coords` per process: 60 s+ timeout -> 25 ms; no writes on GET. | S | Ship now |
| 8 | **Dealership page cache** per `(dealer_id, grid token)` for the slice + facets. | 90-100 ms -> ~5 ms per view. | S | Next release |
| 9 | **Lock the grid cold path** (`listings_grid_serialized_cars` when the cache is `None`) with the existing rebuild lock / an Event. | Prevents the observed 3 concurrent full-fleet SELECTs (~600 MB of tuples in flight) after any cache clear. | S | Next release |
| 10 | **Equipment search index**: `pg_trgm` GIN on `packages` (and `description`) or a `tsvector` column. | Q10 738 ms -> tens of ms for package/feature searches. | M | Next release |
| 11 | **Storage hygiene**: drop `idx_cars_active_zip` (0 scans), the two backup tables (142 MB), and trim `nhtsa_vpic_cache` raw bodies (847 MB TOAST); install `pg_stat_statements`. | Smaller backups/restores; measurable next time. | S | Next release |
| 12 | Fix the "used" -> `trim_contains` parse (D2). | Smart search returns results for the most common query shape. | S | Ship now (correctness) |

Not recommended: paginating `/api/listings/cars`. The 32 MB gzip is served from cache in 5 ms with
a working ETag/304 path, is fetched after first paint, and the 2026-09-16 memory note documents
why the full payload is deliberate.
