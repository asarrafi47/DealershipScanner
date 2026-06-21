# Nationwide Dealership Discovery via Overture Maps

> **Removed June 2026.** The manifest (`dealers.json`) reached ~22k entries — enough coverage
> for the scanner. This doc preserves the approach for one-off re-runs if needed.

## What it did

`run_nationwide_discovery.py` queried the Overture Maps Places dataset on S3 (GeoParquet) using
DuckDB to pull every `car_dealer` place in the US, filtered down to franchised OEM dealers, and
merged scanner-ready rows into `dealers.json`.

## How to re-run

Install DuckDB with the `httpfs` and `spatial` extensions, then restore the script from git:

```bash
git show HEAD~1:run_nationwide_discovery.py > run_nationwide_discovery.py
python run_nationwide_discovery.py
```

### Key flags

| Flag | Purpose |
|---|---|
| `--no-merge-manifest` | Query only — print stats, don't write `dealers.json` |
| `--release 2026-04-15.0` | Pin an Overture release instead of using latest |
| `--manifest PATH` | Write to a different manifest file |
| `--limit N` | Cap rows for testing |
| `-v` | Debug logging |

## How it worked

1. `connect_overture_duckdb()` — spins up an in-process DuckDB with `httpfs` + `spatial`
2. `fetch_us_car_dealers_rows()` — resolves the latest release via the Overture STAC catalog,
   then streams `theme=places, type=car_dealer, country=US` from S3 GeoParquet
3. `is_franchised_dealer(name)` — filters out independent lots by checking for OEM name tokens
4. `normalize_manifest_url(website)` — normalises to HTTPS, extracts `dealer_id` from hostname
5. `merge_json_export_rows()` — deduplicates by URL and upserts into `dealers.json`

The full run against the US dataset took roughly 1–10 minutes depending on network and DuckDB
S3 cache state.

## Relevant modules (still in codebase)

- `backend/discovery/overture_discovery.py` — Overture DuckDB query logic
- `backend/discovery/manifest_merge.py` — JSON manifest upsert
- `backend/dev/dealers.py` — `normalize_manifest_url`
