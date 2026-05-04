"""
Pluggable DMV tier: ``STATE_LOADERS`` maps 2-letter code → loader callable.

Each loader returns ``list[DMVRecord]`` for that state's published/bulk file(s).
"""
from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from backend.discovery.dmv.schema import DMVRecord
from backend.discovery.dmv.states import nc as nc_mod

STATE_LOADERS: dict[str, Callable[[Path | None], list[DMVRecord]]] = {
    "NC": lambda root=None: nc_mod.load_nc_records(root),
}


def fetch_dmv_records(state: str, project_root: Path | None = None) -> list[DMVRecord]:
    code = (state or "").strip().upper()
    if len(code) != 2:
        return []
    loader = STATE_LOADERS.get(code)
    if not loader:
        return []
    return loader(project_root)
