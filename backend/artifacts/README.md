# Local artifacts

This folder holds **non-source** material so the repository root stays small:

- **`scratch/`** — ad-hoc CSV/JSON exports (gitignored contents; keep anything you need to commit elsewhere, e.g. under `data/`).
- **`scanner_backups/`** — optional copies of `scanner.js` from before edits.
- **`legacy/`** — JSON or other files with no code references, kept for reference only.

Runtime tools (`scanner.js`, `scanner.py`, `vdp_framework.js`, `bmw_enhancer.py`, `dealers.json`) stay at the **repo root** because they resolve imports/paths relative to that directory.

Default dealer-group JSON output from scraping helpers is `data/dealer_group_results.json` (see `SCRAPING.paths.default_json_results_path`).
