"""State DMV / licensee bulk data tier (pluggable per state)."""
from __future__ import annotations

from backend.discovery.dmv.registry import STATE_LOADERS, fetch_dmv_records
from backend.discovery.dmv.schema import DMVRecord

__all__ = ["DMVRecord", "STATE_LOADERS", "fetch_dmv_records"]
