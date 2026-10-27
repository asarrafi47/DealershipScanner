# Local debug artifacts

This directory holds **generated** operator diagnostics. Nothing here should be committed except this README.

| Artifact | How to regenerate |
|----------|-------------------|
| `fail_*.png` | Scanner zero-vehicle runs (`backend/scanner/cli.py`); Playwright screenshots on failure |
| `last_scrape_samples.json` | `node backend/scanner/scanner.js` (see `/dev/api/audit-last-scrape`) |
| `e2e_screenshots/`, `*_report.json` | Historical output of `backend/scripts/e2e_deep_browser.py` (deleted 2026-10-01, monolith audit; recover from git history) |
