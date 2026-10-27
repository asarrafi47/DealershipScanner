# Vehicle dictionary layout

Reference data for trim ladders, EPA specs, and Wikipedia/NHTSA option prose.

## Layout

| Path | Contents |
|------|----------|
| `epa/{Make}/` | `*_EPA.csv` — FuelEconomy.gov specs (authoritative) |
| `options/raw/{Make}/` | Rich `*_Complete_Options.csv` (Wikipedia/NHTSA/brochure) |
| `options/stubs/{Make}/` | Header-only placeholders (no rows yet) |
| `curated/` | `trim_ladders.json`, `trim_ladders_*`, merged ladders |
| `derived/trim_spec_sheets/` | Per-ladder structured spec JSON |
| `index/manifest.json` | `(year, make, model)` → file paths + row counts (offline report only: no runtime reader) |
| `index/dictionary_catalog.db` | SQLite lookup index (same data as manifest); not consulted for Complete_Options and not baked into the images |
| `index/make_aliases.json` | Make name normalization |

The legacy flat `*_Complete_Options.csv` files at the dictionary root were removed (P9.3); nothing writes there.

### In-image catalog: not needed (P9.6)

`find_complete_options_csv` resolves from an in-memory index built from the CSV filenames, so the
images ship no `dictionary_catalog.db` and `manifest.json` has no runtime reader. Measured 2026-10-26
on 50 cars (25 raw-only, 25 root-duplicate YMMs): the index builds once per process in 0.78 s; options
lookup median 0.21 ms (p95 0.87 ms) against 0.70 ms (p95 1.52 ms) with a catalog; `resolve_trim_ladder`
median 40 ms (p95 279 ms) against 44 ms (p95 324 ms). Baking the catalog into the images buys nothing.

Brochure PDFs: `backend/data/brochures/` (staging; kept by default — pass `--delete-after` to remove after success).

Derived brochure outputs:

| Path | Contents |
|------|----------|
| `derived/brochure_facts/{Make}/` | Raw standard equipment per trim (audit) |
| `derived/trim_adds_by_year/` | Per `catalog_key` trim `adds_by_trim` for the trim ladder UI |

## Rebuild everything

```bash
python -m backend.scripts.dictionary_rebuild
# After the first full run (normalize already done):
python -m backend.scripts.dictionary_rebuild --skip-normalize
```

Steps only:

```bash
python -m backend.scripts.migrate_dictionary_layout      # flat → sharded (once)
python -m backend.scripts.normalize_complete_options_schema
python -m backend.scripts.build_dictionary_manifest
python -m backend.scripts.build_trim_ladders
python -m backend.scripts.build_trim_ladders_from_epa
python -m backend.scripts.merge_trim_ladders
python -m backend.scripts.build_trim_spec_sheets --write-adds
```

## Import new data

```bash
python backend/scripts/import_epa_to_dictionary.py --epa-csv /tmp/vehicles.csv
python -m backend.scripts.import_brochures_to_dictionary --local-only
python -m backend.scripts.build_dictionary_manifest
```
