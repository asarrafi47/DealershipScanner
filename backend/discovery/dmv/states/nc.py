"""
North Carolina — pilot DMV tier.

Machine-readable bulk lists must be obtained from official NC sources (open data,
public records, or published licensee files). This module reads a **local CSV**
you place after verifying redistribution rights.

Environment / paths (first hit wins):
  - ``DISCOVERY_DMV_NC_CSV`` — absolute or cwd-relative path to CSV
  - ``<project>/data/discovery/dmv/NC/dealers.csv``

See ``docs/discovery/DMV_SOURCES.md`` for attribution and license notes.
"""
from __future__ import annotations

import os
from pathlib import Path

from backend.discovery.dmv.states.generic_csv import load_dmv_records_from_csv

STATE_CODE = "NC"


def nc_csv_path(project_root: Path | None = None) -> Path | None:
    env = (os.environ.get("DISCOVERY_DMV_NC_CSV") or "").strip()
    if env:
        p = Path(env).expanduser()
        return p if p.is_file() else None
    root = project_root or Path(__file__).resolve().parents[4]
    candidate = root / "data" / "discovery" / "dmv" / "NC" / "dealers.csv"
    return candidate if candidate.is_file() else None


def load_nc_records(project_root: Path | None = None):
    p = nc_csv_path(project_root)
    if not p:
        return []
    return load_dmv_records_from_csv(p)
