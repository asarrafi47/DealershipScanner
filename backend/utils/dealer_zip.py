"""Normalize US ZIP codes and backfill car rows from dealership registry."""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Any

from backend.db.dealer_geo import normalize_dealer_host
from backend.utils.field_clean import is_effectively_empty

_ZIP5_RE = re.compile(r"(\d{5})")


def normalize_us_zip(value: Any) -> str | None:
    """Return a 5-digit US ZIP from raw strings (``29714-8803``, ``NC 28212``, etc.)."""
    if value is None or is_effectively_empty(value):
        return None
    s = str(value).strip()
    if s.lower() in ("nan", "null", "none"):
        return None
    m = _ZIP5_RE.search(s)
    return m.group(1) if m else None


def coalesce_car_zip(*, car_zip: Any = None, dealer_zip: Any = None) -> str | None:
    """Prefer per-vehicle ZIP; fall back to dealership ZIP."""
    z = normalize_us_zip(car_zip)
    if z:
        return z
    return normalize_us_zip(dealer_zip)


def _row_first(row: Any) -> Any:
    if row is None:
        return None
    if isinstance(row, dict):
        return next(iter(row.values()), None)
    return row[0]


def _registry_id_from_dealer_id(dealer_id: Any) -> int | None:
    s = str(dealer_id or "").strip()
    if s.startswith("db-"):
        try:
            return int(s[3:])
        except ValueError:
            return None
    return None


@lru_cache(maxsize=1)
def _manifest_zip_index() -> tuple[dict[str, str], dict[str, str]]:
    """``dealer_id`` and normalized URL host → 5-digit ZIP from ``dealers.json``."""
    by_id: dict[str, str] = {}
    by_host: dict[str, str] = {}
    try:
        import json

        from backend.scanner.constants import MANIFEST_PATH

        if not MANIFEST_PATH.is_file():
            return by_id, by_host
        with open(MANIFEST_PATH, encoding="utf-8") as f:
            dealers = json.load(f)
        if not isinstance(dealers, list):
            return by_id, by_host
        for d in dealers:
            if not isinstance(d, dict):
                continue
            z = normalize_us_zip(d.get("zip_code") or d.get("zipCode"))
            if not z:
                continue
            did = str(d.get("dealer_id") or "").strip()
            if did:
                by_id[did] = z
            host = normalize_dealer_host(str(d.get("url") or ""))
            if host:
                by_host[host] = z
    except OSError:
        pass
    return by_id, by_host


def lookup_dealership_zip(
    cursor: Any,
    *,
    registry_id: Any = None,
    dealer_url: str | None = None,
    dealer_id: str | None = None,
) -> str | None:
    """Resolve dealership ZIP from registry id, dealer slug, or dealer URL host."""
    rid: int | None = None
    if registry_id is not None:
        try:
            rid = int(registry_id)
        except (TypeError, ValueError):
            rid = None
    if rid is None:
        rid = _registry_id_from_dealer_id(dealer_id)

    if rid:
        cursor.execute(
            "SELECT zip_code FROM dealerships WHERE id = ? AND zip_code IS NOT NULL",
            (rid,),
        )
        z = normalize_us_zip(_row_first(cursor.fetchone()))
        if z:
            return z

    du = (dealer_url or "").strip()
    host = normalize_dealer_host(du) if du else ""

    if du or host:
        base = du.rstrip("/")
        url_lower = du.lower()
        base_lower = base.lower()
        base_slash_lower = (base_lower + "/") if not base_lower.endswith("/") else base_lower

        cursor.execute(
            """
            SELECT zip_code FROM dealerships
            WHERE zip_code IS NOT NULL AND TRIM(zip_code) != ''
              AND (
                LOWER(TRIM(COALESCE(dealer_website_url, ''))) IN (?, ?, ?)
                OR LOWER(TRIM(COALESCE(website_url, ''))) IN (?, ?, ?)
                OR LOWER(COALESCE(dealer_website_url, '')) LIKE ?
                OR LOWER(COALESCE(website_url, '')) LIKE ?
              )
            LIMIT 1
            """,
            (
                url_lower,
                base_lower,
                base_slash_lower,
                url_lower,
                base_lower,
                base_slash_lower,
                f"%{host}%",
                f"%{host}%",
            ),
        )
        z = normalize_us_zip(_row_first(cursor.fetchone()))
        if z:
            return z

        cursor.execute(
            """
            SELECT zip_code FROM dealer_geopoints
            WHERE zip_code IS NOT NULL AND TRIM(zip_code) != ''
              AND (
                LOWER(TRIM(dealer_url)) IN (?, ?, ?)
                OR LOWER(dealer_url) LIKE ?
              )
            LIMIT 1
            """,
            (url_lower, base_lower, base_slash_lower, f"%{host}%"),
        )
        z = normalize_us_zip(_row_first(cursor.fetchone()))
        if z:
            return z

    by_id, by_host = _manifest_zip_index()
    did = str(dealer_id or "").strip()
    if did and did in by_id:
        return by_id[did]
    if host and host in by_host:
        return by_host[host]
    return None


def enrich_vehicle_zip_from_dealership(vehicle: dict[str, Any], cursor: Any) -> None:
    """Mutates *vehicle* in place when ``zip_code`` is missing."""
    z = coalesce_car_zip(car_zip=vehicle.get("zip_code"))
    if z:
        vehicle["zip_code"] = z
        return
    dealer_z = lookup_dealership_zip(
        cursor,
        registry_id=vehicle.get("dealership_registry_id"),
        dealer_url=str(vehicle.get("dealer_url") or ""),
        dealer_id=str(vehicle.get("dealer_id") or ""),
    )
    if dealer_z:
        vehicle["zip_code"] = dealer_z


def backfill_car_zip_for_registry(cursor: Any, registry_id: int) -> int:
    """Set ``cars.zip_code`` from ``dealerships.zip_code`` where still empty."""
    cursor.execute("SELECT zip_code FROM dealerships WHERE id = ?", (int(registry_id),))
    z = normalize_us_zip(_row_first(cursor.fetchone()))
    if not z:
        return 0
    cursor.execute(
        """
        UPDATE cars
        SET zip_code = ?
        WHERE dealership_registry_id = ?
          AND (zip_code IS NULL OR TRIM(zip_code) = '')
        """,
        (z, int(registry_id)),
    )
    return int(getattr(cursor, "rowcount", 0) or 0)
