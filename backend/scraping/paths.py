"""Repository paths for scraping tools (DB, manifest, default outputs)."""
from __future__ import annotations

from pathlib import Path

# This file is backend/scraping/paths.py, so the repo root is three parents up.
# It used to say "SCRAPING/ lives at repo root" and take two, which was true
# before the package moved under backend/ -- since then every path here has
# resolved inside backend/, which is how backend/inventory.db came to exist and
# collect a stale copy of dealer_recipes alongside the real Postgres table.
ROOT = Path(__file__).resolve().parent.parent.parent

DEFAULT_DB = ROOT / "inventory.db"
MANIFEST_DEFAULT = ROOT / "dealers.json"


def default_json_results_path() -> Path:
    """Default path for hybrid / dealer-group JSON output (under ``data/``)."""
    return ROOT / "data" / "dealer_group_results.json"


def default_hybrid_run_path() -> Path:
    return ROOT / "data" / "hybrid_run_latest.json"


def default_known_group_aliases_path() -> Path:
    return ROOT / "data" / "known_group_aliases.json"


def dated_hybrid_run_path() -> Path:
    """UTC timestamped filename under data/hybrid_runs/ for diffing runs."""
    from datetime import datetime, timezone

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    d = ROOT / "data" / "hybrid_runs"
    d.mkdir(parents=True, exist_ok=True)
    return d / f"hybrid_run_{ts}.json"
