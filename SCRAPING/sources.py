"""Load dealer root URLs from SQLite inventory or JSON manifest."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from SCRAPING.text_utils import normalize_root


def load_roots_from_db(db_path: Path) -> list[str]:
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT DISTINCT dealer_url FROM cars "
            "WHERE dealer_url IS NOT NULL AND TRIM(dealer_url) != ''"
        ).fetchall()
    finally:
        conn.close()
    seen: set[str] = set()
    out: list[str] = []
    for (u,) in rows:
        r = normalize_root(u)
        if r and r not in seen:
            seen.add(r)
            out.append(r)
    return sorted(out)


def load_roots_from_manifest(path: Path) -> list[str]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    seen: set[str] = set()
    out: list[str] = []
    for row in data:
        u = row.get("url") or ""
        r = normalize_root(u)
        if r and r not in seen:
            seen.add(r)
            out.append(r)
    return out


def load_manifest_records(path: Path) -> list[dict[str, str | None]]:
    """Full dealer rows for adjudication (id, name, url, optional brand)."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    out: list[dict[str, str | None]] = []
    for row in data:
        u = row.get("url") or ""
        r = normalize_root(u)
        if not r:
            continue
        out.append(
            {
                "url": r,
                "dealer_id": row.get("dealer_id") or "",
                "dealer_name": row.get("name") or "",
                "brand": row.get("brand"),
            }
        )
    return out
