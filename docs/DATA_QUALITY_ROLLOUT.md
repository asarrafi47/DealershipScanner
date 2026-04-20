# Data quality and hybrid search rollout

## What changed

- **Placeholders**: Shared normalization in `backend/utils/field_clean.py` maps `N/A`, `unknown`, em dashes, etc. to SQLite `NULL`. Used in Python parsers, upserts, semantic listing document build, enrichment writes, and car chat prompts.
- **Hybrid search**: `POST /api/search/smart` uses `hybrid_smart_search()` — pgvector recall for free text, then SQLite filters on `candidate_ids`. Response may include `search_meta`.
- **pgvector**: Listing embeddings skip junk text; SQL still applies structured filters on the candidate set.
- **Vision**: Enrichment vision prompt targets observable facts only; results merge into `packages` JSON under `observed_*` / `possible_packages` / `confidence`, not full package guessing.
- **Car chat**: Evidence blocks are ordered listing → notes/description → packages JSON → trim/EPA inferred → cache/web only when keyword triggers fire. Shorter answers, fewer automatic web fetches.
- **Scanner (Node)**: `sqlOptionalStr()` writes `NULL` instead of `N/A` for optional columns on upsert.

## Inventory scanner: Python vs Node

- **`scanner.py` (Python + Playwright)** is the canonical path for production-style dealership runs from this repo: manifest (`dealers.json`), stealth browser, JSON intercept + HTML fallback, optional VDP gallery merge, and SQLite upsert.
- **`scanner.js` (Node)** is a separate/legacy pipeline for some deployments; keep behavior aligned on cleaning (e.g. optional columns as SQL `NULL`), but treat **Python `scanner.py` as primary** when both exist unless your ops docs say otherwise.

**New / relevant env vars (Python scanner)**

| Variable | Purpose |
| --- | --- |
| `SCANNER_INTERCEPT_URL_DENY` | Comma-separated URL substrings to **always** ignore for inventory JSON (e.g. `carnow.com,payments`). Applied before same-site / allow rules. |
| `SCANNER_INTERCEPT_URL_ALLOW` | Extra comma-separated substrings that **allow** a response URL even when it is not on the dealer host (e.g. a vendor CDN path you trust). |
| `SCANNER_INVENTORY_WAIT_MS` | After each inventory `goto`, max time (ms) to wait for a JSON `response` event that passes the same URL gate as above (default `18000`). Listing payloads are still validated in the response handler (`find_vehicle_list`). |

At the end of each dealer, the scanner emits a single **`dealer_run_summary`** INFO line (JSON, ~2KB cap) with intercept counts, dedupe/VDP stats, and elapsed seconds.

- **Thin listing JSON**: CDK / `dealer_on` inventory payloads now map more color / engine / condition / MPG keys into SQLite. If a feed is still sparse, raise **`SCANNER_VDP_EP_MAX`** (Python `scanner.py` / `scanner_vdp`) so VDP and analytics merge can fill gaps—defaults stay conservative to avoid opening thousands of detail pages per run.
- **Same-VIN merges (Python scanner)**: Multiple JSON intercepts for one VIN union galleries and **fill only empty** spec slots (colors, drivetrain, Carfax URL, price if missing, etc.) so a thin row plus a rich row combine safely.
- **Dual stacks**: `scanner.py` and `scanner.js` can drift (intercept rules, pagination). Prefer **one canonical** pipeline for production; when both exist, align critical behavior manually or share constants/docs until a single implementation owns the rules.
- **VDP budget / long tail**: With low `SCANNER_VDP_EP_MAX`, rotate which dealers or runs get a higher cap periodically so thin-gallery inventory eventually receives VDP merges across scheduled jobs.

## Rollout steps

1. **Backup** `inventory.db` (and Postgres if you host vectors there).
2. **Deploy code** (Flask + scanner + optional Crawl4AI workers).
3. **Run migration** (from repo root):

   ```bash
   PYTHONPATH=. python3 scripts/migrate_placeholder_nulls.py
   ```

   Optionally:

   ```bash
   PYTHONPATH=. python3 scripts/migrate_placeholder_nulls.py --refresh-scores
   ```

4. **Hybrid search debug** (optional): set `HYBRID_SEARCH_DEBUG=1` when running Flask to log parsed filters, vector candidate counts, and result id ordering from `hybrid_smart_search`.

5. **Re-index** pgvector from SQLite so embeddings match cleaned text, for example:

   ```bash
   PYTHONPATH=. python3 -c "from backend.vector.pgvector_service import reindex_all; print(reindex_all())"
   ```
6. **Smoke-test** hybrid search (filtered query + free-text query) and car detail chat with and without web-trigger keywords.

## Manual test checklist

- [ ] `POST /api/search/smart` with `{"query": "2022 BMW"}` returns results and `search_meta.mode` is `semantic_then_sql` or `sql_only` / `sql_fallback_no_semantic_index` as expected.
- [ ] Same endpoint with `make`+`year` parsed from text still respects SQL filters (no unrelated vector-only cars when filters are “hard”).
- [ ] New scrape from `scanner.js` leaves `trim` / `transmission` / URLs as NULL when the site omits them (not `N/A`).
- [ ] Enrichment vision run produces `packages` JSON containing `observed_features` / `confidence` without overwriting `exterior_color` when already present.
- [ ] Car chat: factual question (“What is the mileage?”) answered from listing only; reliability question only fetches web/cache when trigger words appear.
- [ ] Filter dropdowns in UI omit junk placeholder strings after migration.

## Files touched (reference)

- `backend/utils/field_clean.py` — normalization, listing embedding text, quality score.
- `backend/hybrid_inventory_search.py` — hybrid merge.
- `backend/main.py` — smart search route.
- `backend/db/inventory_db.py` — schema helpers, filter options, quality score refresh.
- `backend/database.py` — upsert cleaning (if present in your tree).
- `backend/vector/pgvector_service.py` — reindex + `query_cars`.
- `backend/parsers/dealer_dot_com.py`, `backend/parsers/dealer_on.py` — cleaned rows.
- `backend/enrichment_service.py` — vision prompt + merge + write normalization.
- `backend/ai_agent.py` — car page chat evidence stack and web gating.
- `scanner.js` — `sqlOptionalStr` on upsert path.
- `scripts/migrate_placeholder_nulls.py` — DB cleanup.
