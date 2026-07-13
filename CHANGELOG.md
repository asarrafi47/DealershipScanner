# Changelog

## [Unreleased]

### Added
- **Config:** central `Config` class in `backend/config.py` as the single place for environment-driven settings (remaining `os.getenv` call sites to migrate — see [master-todo.md](master-todo.md) A1).
- **Health endpoints:** `/api/health` (liveness) and `/api/ready` (readiness) on the web app, alongside the existing `/health` used by `deploy/up.sh`.
- **Scanner framework:** `ScraperChain` in `backend/scanner/chain.py` — composable scraper-strategy chain for the Python scanner stack.
- **CI:** GitHub Actions workflow running `pytest backend/tests -m "not integration"` on pushes and PRs. `integration`/`regression` marker scaffolding is in place (pytest.ini + conftest auto-tagging hooks), but no tests are tagged yet, so the filter currently deselects nothing and PR runs execute the full suite.
- **Docs kit:** this `CHANGELOG.md`, [master-todo.md](master-todo.md) (phased task board), and [RESUME_HERE.md](RESUME_HERE.md) (session pickup doc).

### Changed
- **Web app structure:** `backend/main.py` split into Flask blueprints; all route URLs, endpoint names, and import paths preserved (existing tests are the contract).
- **Data layer:** `backend/db/repositories` split into per-domain repository modules behind a facade that keeps the original import surface intact.

## [0.2.0] — 2026-07-08

### Added
- **Delta scans:** scan only changed inventory; heal-pass parallelization; recipe capture fixes; fresh-pg schema (`e612399`).
- **Auto-heal:** quality-driven per-car re-acquisition after every dealer scan; floor only fields gap-fill can re-acquire (`0ba0fb3`, `5221259`).
- **VDP prefetch:** reuse known fields and cheap HTTP before browser visits (`e62aa9b`); capture 360-spin assets (Impel/SpinCar/WebRotate) during enrichment (`7f972e0`).
- **Listings/admin:** CPO + forced-induction filters; dealer hub and onboarding polish (`1250591`).

### Changed
- **Scanner:** collapsed flat/subpackage module duplication into alias shims (`6ca2bda`); UI responsive fixes, dashboard speedup, dev LLM lifecycle (`e6da3d7`).
- **Repo hygiene:** untracked `.venv` (`7f8b31c`); removed dead MFA modules (`9215d85`).

### Fixed
- **Recovery:** three stacked bugs left junk price-less inventory in place after failed scans — recovery now cleans them up (`c8eaece`).
- **SQLite schema:** `zip_code` column was missing from the cars migration list, breaking fresh-DB migrations (`0eb471e`).
- **Tests:** suite was non-hermetic and hiding real bugs; made hermetic and fixed the bugs it exposed (`d044234`).

### Security
- **X-Forwarded-For spoofing:** client IP now read from the right end of the header chain, not the left (`d482b3e`).
- **Sessions:** revalidate paid session claims; trusted client IP handling; suspended-login guard (`a4c8042`).
