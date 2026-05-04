# Listings semantic search (Postgres pgvector + SQLite)

## Where data lives

| Item | Location |
|------|----------|
| **Source of truth** | SQLite `inventory.db`, table `cars` (path: `INVENTORY_DB_PATH` or `./inventory.db`) |
| **Semantic index** | Postgres table `listing_semantic_embeddings` (`car_id`, `document_text`, `embedding vector(N)`, …) |
| **Embedding text** | `backend/vector/listings_semantic.py` → `build_semantic_listing_document()` |
| **Indexer (cars + dealers + BMW)** | `backend/vector/pgvector_service.py` → `reindex_all()` / `reindex_inventory_only()` |
| **Query API** | `backend/vector/pgvector_service.py` → `query_cars()` |

| Item | Location |
|------|----------|
| **Connection** | `PGVECTOR_URL` or `DATABASE_URL` (libpq URI) |
| **Extension** | `CREATE EXTENSION vector;` once per database |
| **Similarity** | Cosine distance via `<=>` on `vector` columns |
| **Embeddings** | `sentence-transformers` — default model `all-MiniLM-L6-v2` (`LISTING_EMBEDDING_MODEL`) |
| **Misc artifacts** | `./data/vectors/` (replaces legacy Chroma persist paths) |

## Rebuild the index

```bash
# Full rebuild: listings, dealers, BMW OEM tables (if BMW DB present)
python -m backend.vector reindex

# Or
python scripts/reindex_vectors.py

# Listings + dealers only (skip BMW OEM tables)
python scripts/reindex_vectors.py --inventory-only
```

After large inventory imports or schema changes affecting indexed fields, re-run one of the above.

## Query flow

1. **Smart search (JS)** — User types in “Smart search”; `POST /api/search/smart` runs `parse_natural_query()` for structured hints, then **`hybrid_smart_search()`**.
2. **GET `/search?q=...`** — Same hybrid pipeline with filters from the form (`flask_request_to_search_cars_kwargs`).
3. **Hybrid pipeline** (`backend/hybrid_inventory_search.py`):
   - If there is free text: **`query_cars(query, n_results=100)`** → ordered candidate `car_id` list (pgvector).
   - **`search_cars(..., candidate_ids=candidates)`** runs SQLite with the same **dropdown / scalar filters** (make, model, price cap, zip radius, etc.). Incomplete/junk rows are still filtered by existing `is_car_incomplete` logic inside `search_cars`.
   - Result order follows **semantic similarity** among rows that pass SQL filters.
4. If the vector index is empty or errors: **SQL-only** fallback with the same filters (sorted by data quality / price as before). `search_meta.mode` is `semantic_then_sql` or `sql_fallback_no_semantic_index`; `vector_backend` is always `pgvector`.

**Dropdown facets** (`get_filter_options()`) are always computed from SQL, not from the vector index.

## Files touched by this feature

- `backend/vector/listings_semantic.py` — prose-style documents for embeddings  
- `backend/vector/pgvector_service.py` — schema, **`query_cars`**, **`reindex_all`**, dealer/BMW/knowledge/master-spec tables  
- `backend/hybrid_inventory_search.py` — semantic-first + SQL intersection  
- `backend/db/inventory_db.py` — `search_cars(..., candidate_ids=...)`  
- `backend/main.py` — `/search` passes `q`, server-rendered `initial_grid_cars` when `q` is set  
- `frontend/templates/listings.html` — `INITIAL_GRID_CARS`, hidden `q` field  
- `scripts/reindex_vectors.py` — CLI wrapper  

## Optional next steps

- **Similar vehicles**: kNN in pgvector for the same `car_id` embedding.  
- **Autocomplete**: small `query_cars` with low `n_results` on keystroke.  
- **Stronger hybrid score**: combine vector distance with `data_quality_score` in Python before returning (currently order is similarity-dominant).
