# Sarrafi Collection

Flask app + inventory scanner for dealership listings, enrichment (EPA / specs), and hybrid (SQL + pgvector) search.

## Quick start on a new machine

### 1. Clone and Python environment

```bash
git clone <your-remote-url> <repo-directory>
cd <repo-directory>
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Set `PYTHONPATH` to the repo root when running modules (or run from root as below).

### 2. Environment variables

Create a **`.env`** file in the repo root (it is gitignored). Copy variable names from:

- `docs/SECURITY_MASTER_TODO.md` — **Environment variables (quick reference)** table  
- Production requires `FLASK_ENV=production`, `SECRET_KEY` / `FLASK_SECRET_KEY`, `ADMIN_PASSWORD`, etc.

Never commit `.env` or API keys.

### 3. SQLite databases (local, not in git)

Runtime databases (`inventory.db`, `users.db`, `dev_users.db`, `dealer_portal.db`, `incomplete_listings.db`, …) are created when you run the app or scanner. Paths can be overridden with env vars such as `INVENTORY_DB_PATH`, `USERS_DB_PATH` (see security doc).

### 4. EPA dictionary (`*_EPA.csv`) — **tracked / optional bulk**

Vehicle EPA reference CSVs used by enrichment live under **`backend/dictionary/`** (and may also appear as a historical **`DICTIONARY/`** folder at the repo root in older clones). Per-file CSVs are small; large trees are the **listing/options scrapes** (see below), which are **not** committed.

To **rebuild or extend** EPA CSVs from fueleconomy.gov data:

```bash
# Download EPA bulk file (example)
curl -L -o /tmp/vehicles.csv.zip https://www.fueleconomy.gov/feg/epadata/vehicles.csv.zip
unzip -o /tmp/vehicles.csv.zip -d /tmp/

python backend/scripts/import_epa_to_dictionary.py --epa-csv /tmp/vehicles.csv --min-year 2000
# Optional: --dictionary /absolute/path/to/backend/dictionary
```

### 5. Listing / options CSVs (`*Complete_Options.csv`) — **not in git**

Large Wikipedia / NHTSA **options** scrapes (`csv_out/`, `csv_out_cleaned/`, `csv_out_cleaned_main/`, `csv_out_rejected_main/`, … under the repo root or under `backend/dictionary/`) are **ignored**. Regenerate when needed:

```bash
python backend/scripts/car_data_scraper.py --batch-nhtsa --output-dir backend/dictionary/csv_out
# Optional cleaning pipeline — see backend/scripts/clean_vehicle_csvs.py and prune scripts
```

These are separate from EPA `*_EPA.csv` files (see `backend/scripts/import_epa_to_dictionary.py` header comment).

**Trim ladders (premium VDP):** `backend/dictionary/trim_ladders.json` (hand-curated) and `trim_ladders_generated.json` (built from `*_Complete_Options.csv`). Regenerate after dictionary updates:

```bash
python -m backend.scripts.build_trim_ladders
```

**EPA master index (VDP efficiency + trim lookups):** `backend/enrichment/knowledge_engine.py` reads `epa_master` in `inventory.db` for fast MPG lookups. Populate it from dictionary `*_EPA.csv` files after imports or dictionary updates:

```bash
python -m backend.scripts.build_epa_master           # insert missing rows
python -m backend.scripts.build_epa_master --rebuild # full reload
```

The knowledge engine falls back to dictionary CSVs when `epa_master` is empty, but the SQLite index is faster and should be rebuilt in any environment that serves vehicle detail pages.

**Trim ladder audit (regression check):**

```bash
python backend/scripts/audit_trim_ladders.py
pytest backend/tests/test_trim_ladder_audit.py -q
```

### 6. Semantic search (Postgres + pgvector)

Listing embeddings are stored in **Postgres**, not in git. Local legacy/cache dirs under `data/vectors/` and `backend/data/chroma/` are ignored.

After you have an `inventory.db` (and Postgres with `CREATE EXTENSION vector;`):

```bash
export PGVECTOR_URL="postgresql://..."   # or DATABASE_URL
python backend/scripts/reindex_vectors.py
# Listings + dealers only:
python backend/scripts/reindex_vectors.py --inventory-only
```

Details: `docs/LISTINGS_PGVECTOR_SEARCH.md`.

### 7. Incomplete-listings dev index (SQLite helper)

```bash
python backend/scripts/rebuild_listings_index.py --fast
```

### 8. Optional: Node (scanner JS / tooling)

If you use the Node-side scanner (`backend/scanner/`):

```bash
cd backend/scanner && npm ci
```

### 9. Run the app

```bash
PYTHONPATH=. flask --app backend.main run
# or: python -c "from backend.main import app; app.run()" with the same PYTHONPATH
```

### 10. Scanner CLI

```bash
python scanner.py --help
python discovery.py --help
```

## What stays out of git (summary)

| Artifact | Reason | Rebuild |
|----------|--------|--------|
| `.env` | Secrets | Create locally |
| `*.db` (app DBs) | Private / large | App + scanner |
| `csv_out*` under root or `backend/dictionary/` | Large listing/options scrapes | `car_data_scraper.py` + cleaning scripts |
| `data/vectors/`, `backend/data/chroma/` | Embeddings / local vector stores | `scripts/reindex_vectors.py` |
| `backend/data/oem/`, pipeline JSON scratch | BMW / OEM pipeline outputs | OEM intake scripts |
| `.fuse*`, `backend/fuse_artifacts/` | macOS/FUSE noise | N/A — delete locally |

## More documentation

- `ios/README.md` — **Sarrafi Cars iOS** (native SwiftUI app; separate from the website)  
- `ios/ARCHITECTURE.md` — iOS module boundaries (`App/`, `Features/`, `Networking/`, …)  
- `docs/IOS_APP.md` — TestFlight / App Store signing  
- `docs/DATA_QUALITY_ROLLOUT.md` — data quality and migrations  
- `docs/SCANNER_NODE.md` — Node scanner notes  
- `docs/SECURITY_MASTER_TODO.md` — security configuration and SEC items  
