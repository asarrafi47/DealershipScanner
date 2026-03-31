"""
Scraping toolkit for dealer-site analysis (dealer group inference, HTTP fetch, HTML parsing).

Run the CLI from the repo root:
  python -m SCRAPING.cli --test
  python scripts/dealer_group_copyright.py --test

Package layout:
  paths          — repo paths (DB, manifest, default outputs)
  constants      — user agent, regex, vendor lists, page weights
  models         — SiteResult, Evidence
  text_utils     — URL normalize, DNS, whitespace, vendor checks
  sources        — load dealer URLs from SQLite / JSON
  html_extract   — footer, internal links, HTML → text blobs
  inference      — window-based phrase extraction + scoring
  org_validation — is_plausible_org_name, normalize, canonicalize, status thresholds
  redirects      — redirect / cross-domain plausibility
  fetch_requests — requests.Session helpers
  crawler        — Playwright + requests multi-page crawlers
  cli            — argparse orchestration and JSON/CSV export
  interrupt      — cooperative SIGINT handling
  fixture_tests  — offline tests: python -m SCRAPING.fixture_tests
"""
from __future__ import annotations

from SCRAPING.models import Evidence, SiteResult
from SCRAPING.paths import (
    DEFAULT_DB,
    MANIFEST_DEFAULT,
    ROOT,
    default_json_results_path,
)

__all__ = [
    "ROOT",
    "DEFAULT_DB",
    "MANIFEST_DEFAULT",
    "default_json_results_path",
    "Evidence",
    "SiteResult",
]
