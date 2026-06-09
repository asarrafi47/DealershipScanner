# Local debug artifacts

This directory holds **generated** operator diagnostics. Nothing here should be committed except this README.

| Artifact | How to regenerate |
|----------|-------------------|
| `fail_*.png` | Scanner zero-vehicle runs (`backend/scanner/cli.py`); Playwright screenshots on failure |
| `last_scrape_samples.json` | `node backend/scanner/scanner.js` (see `/dev/api/audit-last-scrape`) |
| `e2e_screenshots/`, `*_report.json` | `python backend/scripts/e2e_deep_browser.py` |
| HAR network traces | `workspace/debug/fail_*.har` when `SCANNER_FAILURE_HAR=1` (SEC-066; may contain cookies/auth headers) |

Set `SCANNER_FAILURE_HAR=1` explicitly when you need HAR captures; default is off.
