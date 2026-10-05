"""Dealer roster: manifest + cars table. Moved verbatim from backend/scripts/dealer_pipeline.py (audit F11)."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from backend.scanner.pipeline.db import _rows, get_conn


# --------------------------------------------------------------------------
# dealers
# --------------------------------------------------------------------------

def load_manifest_dealers(path: str) -> list[dict[str, Any]]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    items = raw.get("dealers") if isinstance(raw, dict) else raw
    out = []
    for d in items or []:
        if isinstance(d, dict) and d.get("dealer_id") and d.get("url"):
            out.append(d)
    return out


def dealers_from_db(dealer_ids: list[str]) -> dict[str, dict[str, Any]]:
    """{dealer_id: {dealer_id, url, name}} from the cars table for ids the manifest lacks."""
    if not dealer_ids:
        return {}
    conn = get_conn()
    try:
        rows = _rows(
            conn,
            "SELECT dealer_id, MAX(dealer_url) AS url, MAX(dealer_name) AS name FROM cars "
            "WHERE dealer_id IN (" + ",".join("?" * len(dealer_ids)) + ") AND COALESCE(listing_active,1)=1 GROUP BY dealer_id",
            tuple(dealer_ids),
        )
    finally:
        conn.close()
    return {r["dealer_id"]: {"dealer_id": r["dealer_id"], "url": str(r["url"] or ""), "name": str(r["name"] or r["dealer_id"]), "provider": "unknown"}
            for r in rows if r.get("url")}


def dealer_from_manifest(dealer_id: str, manifest: list[dict[str, Any]]) -> dict[str, Any] | None:
    for d in manifest:
        if d.get("dealer_id") == dealer_id:
            return d
    return None
