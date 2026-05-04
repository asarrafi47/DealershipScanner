"""
Load DMV-style dealer rows from a CSV with flexible column names.

Expected columns (case-insensitive; pick first match per role):
  name / business_name / dba / dealer_name
  street / address / addr / street_address
  city
  state
  zip / postal / zip_code
  website / url / web (optional)
"""
from __future__ import annotations

import csv
import re
from pathlib import Path
from backend.discovery.dmv.schema import DMVRecord


_ALIASES = {
    "name": ("business_name", "dba", "dealer_name", "legal_name", "name"),
    "street_address": ("street_address", "address", "addr", "street", "location_address"),
    "city": ("city",),
    "state": ("state", "st"),
    "zip_code": ("zip_code", "zip", "postal", "postal_code"),
    "website": ("website", "url", "web", "site"),
}


def _norm_header(h: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (h or "").strip().lower()).strip("_")


def _map_headers(fieldnames: list[str]) -> dict[str, str]:
    """Map canonical field → actual CSV header."""
    nh = {_norm_header(f): f for f in fieldnames}
    out: dict[str, str] = {}
    for canon, options in _ALIASES.items():
        for opt in options:
            key = _norm_header(opt)
            if key in nh:
                out[canon] = nh[key]
                break
    return out


def load_dmv_records_from_csv(path: Path) -> list[DMVRecord]:
    path = path.expanduser().resolve()
    if not path.is_file():
        return []
    rows: list[DMVRecord] = []
    with path.open(newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return []
        hm = _map_headers(list(reader.fieldnames))
        if "name" not in hm or "city" not in hm or "state" not in hm:
            return []
        for raw in reader:
            def g(key: str) -> str:
                col = hm.get(key)
                if not col:
                    return ""
                v = raw.get(col)
                return str(v).strip() if v is not None else ""

            name = g("name")
            if not name:
                continue
            site = g("website") or None
            rows.append(
                DMVRecord(
                    business_name=name,
                    street_address=g("street_address"),
                    city=g("city"),
                    state=g("state"),
                    zip_code=g("zip_code"),
                    website=site if site else None,
                )
            )
    return rows


def csv_probe_columns(path: Path) -> dict[str, object]:
    """Help operators validate a file before wiring it as a state pilot."""
    path = path.expanduser().resolve()
    if not path.is_file():
        return {"error": "not_found", "path": str(path)}
    with path.open(newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        fn = list(reader.fieldnames or [])
    hm = _map_headers(fn)
    return {
        "path": str(path),
        "headers": fn,
        "mapped": hm,
        "ready": bool(hm.get("name") and hm.get("city") and hm.get("state")),
    }
