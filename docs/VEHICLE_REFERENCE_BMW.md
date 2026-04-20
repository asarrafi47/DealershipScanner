# BMW vehicle reference database

This project adds a **normalized SQLite** store under `data/vehicle_reference/bmw_reference.db` and CSV export that matches the fixed inventory template (Year, Make, Model, Trim, Body Style, Engine, Transmission, Drivetrain, Fuel Type, MPG, Packages, PackageDetails, ExtColorOpts, IntColorOpts, PassengerSeating).

**Important:** The shipped seeds intentionally avoid fabricating factory specs. Most series appear only in the **coverage** table until you ingest brochure- or EPA-backed rows. Vehicle seed rows exist to validate joins, provenance, and CSV shape.

## Commands

Run from the repository root (same layout as other tools using `SCRAPING.paths`).

```bash
# Recreate database from schema (vehicle_reference/schema/schema.sql) + seeds/bmw/*.json
python -m vehicle_reference.cli rebuild

# Export every BMW vehicle row to CSV
python -m vehicle_reference.cli export-all
# equivalent:
python -m vehicle_reference.cli export --out data/vehicle_reference/bmw_export_all.csv

# Export one model (matches ref_vehicle.series_name exactly)
python -m vehicle_reference.cli export --out data/vehicle_reference/bmw_x2_only.csv --model "X2"

python -m vehicle_reference.cli export --out data/vehicle_reference/bmw_3_series.csv --model "3 Series"

# Optional year window
python -m vehicle_reference.cli export --out /tmp/bmw_recent.csv --year-from 2020

# Regenerate bundled template sample
python -m vehicle_reference.cli export-sample
```

The default database path is `data/vehicle_reference/bmw_reference.db` (override with `--db`).

## Schema overview

| Table | Role |
|-------|------|
| `ref_brand` | Extensibility (`bmw` today). |
| `ref_source` | Provenance for vehicles, packages, and colors. |
| `ref_vehicle` | One flattenable configuration row (U.S. default in `market`). |
| `ref_package` / `ref_vehicle_package` | Option packages and many-to-many link. |
| `ref_exterior_color` / `ref_vehicle_exterior` | Exterior colors. |
| `ref_interior_color` / `ref_vehicle_interior` | Interior colors. |
| `ref_coverage_line` | Product lines to ingest (no per-trim specs required). |

Internal columns (`ref_vehicle.source_id`, linked package/color sources) are **not** exported in the stock CSV; use `--include-uncertainty` on export if you want an extra `UncertaintyNotes` column for QA (not part of your original template).

## Ingestion model

1. Add curated JSON under `vehicle_reference/seeds/bmw/` (lexicographic order: `bmw_01_…`, `bmw_02_…`, etc.).
2. Each vehicle object references a `source` block (`label`, `url`, optional `source_group_key`, `notes`).
3. Leave unknown scalar specs as omitted or `null` in JSON; the loader stores SQL `NULL` and export writes empty CSV cells.
4. For other brands later: insert a row in `ref_brand`, add `vehicle_reference/seeds/<brand>/`, and extend the CLI to pass `brand_code` (small code change).

## Coverage gaps (honest)

- **Model years 2000–present, all trims:** Not populated from factory PDFs in this repository snapshot. The `ref_coverage_line` table lists intended BMW **lines** (main series, X/SAV, i, and selected M / M SUV lines) with `coverage_status` mostly `pending`.
- **U.S. year ranges on coverage rows:** `first_model_year_us` / `last_model_year_us` are left **null** and `year_range_uncertain` defaults to **true** so we do not imply precision we have not sourced.
- **M models:** Included in coverage for ingestion planning; no M vehicle rows are seeded by default because option packaging differs sharply by MY and must track primary sources.
- **Colors and packages in demo X2 row:** Explicitly labeled as **non-authoritative** placeholders to exercise `Packages`, `PackageDetails`, `ExtColorOpts`, and `IntColorOpts` in CSV output. Replace with ordering-guide data before production use.

## Uncertain model years

Anything not yet loaded from an official **ordering guide**, **press kit**, or **EPA** artifact for that exact configuration should carry text in `ref_vehicle.uncertainty_notes` (and remain empty in spec columns). When you add verified data, clear or narrow that note and populate the scalar fields.

## Sample CSV

After `rebuild`, `export-sample` writes:

`data/vehicle_reference/samples/bmw_reference_template_sample.csv`

This matches the template column order in `vehicle_reference.csv_export.flat_export.CSV_COLUMNS` (also re-exported as `vehicle_reference.export_csv.CSV_COLUMNS`).
