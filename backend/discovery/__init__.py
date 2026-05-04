"""
Tiered dealership discovery pipeline.

Tier order (authoritative → gap-fill):
  1. DMV / state dealer-license data (per-state; pluggable)
  2. OpenStreetMap via Overpass API
  3. DuckDuckGo Instant Answer (URL gap-fill only)

Search center: Census ZCTA gazetteer internal point (``backend/ZIPs/*.txt`` or
``DISCOVERY_ZCTA_GAZETTEER``) when available; otherwise pgeocode. CLI defaults to
keeping dealers whose ZIP matches the seed ZCTA after enrichment (see
``--allow-adjacent-zips``).

Public API::

    from backend.discovery import run_discovery

    rows = run_discovery(zip_code="28210", radius_miles=25, dmv_state="NC")
    # Pass ``project_root=<repo root>`` to auto-load ``backend/ZIPs/*.txt``;
    # ``within_seed_zip_only=True`` matches the CLI default (seed ZCTA only).
    # → list[DealerCandidate]

Each DealerCandidate can be persisted with::

    from backend.db.dealerships_db import upsert_discovery_row
    for r in rows:
        upsert_discovery_row(r.to_db_dict())

Imports are lazy so ``python -m backend.discovery.cli`` does not load HTTP clients until needed.
"""
from __future__ import annotations

from typing import Any

__all__ = ["DealerCandidate", "run_discovery"]


def __getattr__(name: str) -> Any:
    if name == "run_discovery":
        from backend.discovery.pipeline import run_discovery

        return run_discovery
    if name == "DealerCandidate":
        from backend.discovery.candidate import DealerCandidate

        return DealerCandidate
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
