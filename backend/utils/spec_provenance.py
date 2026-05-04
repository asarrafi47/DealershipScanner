"""Merge per-field provenance for spec backfill (``spec_source_json`` on ``cars``)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def merge_spec_source_json(
    existing: str | None,
    patch: dict[str, Any],
) -> str:
    """
    *patch* maps field name → {"source": "...", "url": "...", ...}.
    Shallow-merge into existing JSON object; newer ``fetched_at`` wins per key.
    """
    base: dict[str, Any] = {}
    if existing and str(existing).strip():
        try:
            parsed = json.loads(existing)
            if isinstance(parsed, dict):
                base = dict(parsed)
        except (json.JSONDecodeError, TypeError):
            base = {"_parse_error": True}
    for k, v in patch.items():
        if not isinstance(v, dict):
            continue
        entry = dict(v)
        entry.setdefault("fetched_at", utc_now_iso())
        base[str(k)] = entry
    return json.dumps(base, ensure_ascii=False)
