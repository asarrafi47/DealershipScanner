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
| `index/manifest.json` | `(year, make, model)` → file paths + row counts |
| `index/dictionary_catalog.db` | SQLite lookup index (same data as manifest) |
| `index/make_aliases.json` | Make name normalization |

Legacy flat CSVs at the dictionary root remain supported until migrated.

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
