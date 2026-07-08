"""
Factory catalog lookup: ``catalog_trims`` / ``catalog_options`` / ``catalog_packages``
(EPA-sourced factory trim catalog, ~June 2026 scan; see ``catalog_trims.source``).

Given a car's (year, make, model, trim), finds the best-matching ``catalog_trims`` row
and returns its factory-available packages / standalone options. Same shape as
``knowledge_engine.lookup_epa_by_trim``: exact match first, falling back to a
bidirectional trim substring match (closest string-length match wins) when no exact
row exists. Returns ``{}`` when nothing matches or the matched trim has no
options/packages recorded (most rows don't — this catalog is sparsely populated).
"""
from __future__ import annotations

from functools import lru_cache
from typing import Any

from backend.db.inventory_db import get_conn


def _conn():
    return get_conn()


def _catalog_trim_lookup_key(
    year: int | None,
    make: str | None,
    model: str | None,
    trim: str | None,
) -> tuple[int, str, str, str] | None:
    if not year or not make or not model or not trim:
        return None
    try:
        y = int(year)
    except (TypeError, ValueError):
        return None
    mk = (make or "").strip()
    md = (model or "").strip()
    tr = (trim or "").strip()
    if not y or not mk or not md or not tr:
        return None
    return y, mk, md, tr


@lru_cache(maxsize=8192)
def _find_catalog_vehicle_id_cached(key: tuple[int, str, str, str]) -> int | None:
    year, make, model, trim = key
    return _find_catalog_vehicle_id_uncached(year, make, model, trim)


def clear_catalog_lookup_cache() -> None:
    _find_catalog_vehicle_id_cached.cache_clear()


def _find_catalog_vehicle_id_uncached(
    year: int,
    make: str,
    model: str,
    trim: str,
) -> int | None:
    conn = None
    try:
        conn = _conn()
        cur = conn.cursor()
        # Exact (case-insensitive) match first.
        cur.execute(
            """
            SELECT id
            FROM catalog_trims
            WHERE year = ? AND lower(make) = lower(?) AND lower(model) = lower(?)
              AND lower(trim) = lower(?)
            LIMIT 1
            """,
            (year, make.strip(), model.strip(), trim.strip()),
        )
        row = cur.fetchone()
        if row:
            return int(row[0])

        # Bidirectional substring fallback (dealer trim strings vs catalog trim strings
        # often differ only by drivetrain/body suffix, e.g. "330i" vs "330i xDrive").
        cur.execute(
            """
            SELECT id, trim
            FROM catalog_trims
            WHERE year = ? AND lower(make) = lower(?) AND lower(model) = lower(?)
              AND (lower(?) LIKE '%' || lower(trim) || '%'
                   OR lower(trim) LIKE '%' || lower(?) || '%')
            """,
            (year, make.strip(), model.strip(), trim.strip(), trim.strip()),
        )
        rows = cur.fetchall()
        if not rows:
            return None
        trim_clean = trim.strip()
        best = min(rows, key=lambda r: abs(len(str(r[1] or "")) - len(trim_clean)))
        return int(best[0])
    except Exception:
        return None
    finally:
        if conn is not None:
            conn.close()


def _fetch_catalog_options_and_packages(vehicle_id: int) -> dict[str, Any]:
    conn = None
    try:
        conn = _conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT package_name, package_msrp, is_required
            FROM catalog_packages
            WHERE vehicle_id = ?
            ORDER BY sort_order, package_name
            """,
            (vehicle_id,),
        )
        packages = [
            {"name": r[0], "msrp": r[1], "is_required": bool(r[2])}
            for r in cur.fetchall()
            if r[0]
        ]

        cur.execute(
            """
            SELECT option_name, option_msrp, category
            FROM catalog_options
            WHERE vehicle_id = ?
            ORDER BY category, option_name
            """,
            (vehicle_id,),
        )
        options = [
            {"name": r[0], "msrp": r[1], "category": r[2]}
            for r in cur.fetchall()
            if r[0]
        ]
    except Exception:
        return {}
    finally:
        if conn is not None:
            conn.close()

    if not packages and not options:
        return {}
    return {"packages": packages, "options": options}


def lookup_catalog_options_and_packages(
    year: int | None,
    make: str | None,
    model: str | None,
    trim: str | None,
) -> dict[str, Any]:
    """
    Factory packages / standalone options for the best-matching ``catalog_trims`` row.

    Returns ``{"packages": [...], "options": [...]}`` or ``{}`` when there's no
    catalog match, or the matched trim has no options/packages on file.
    """
    key = _catalog_trim_lookup_key(year, make, model, trim)
    if key is None:
        return {}
    vehicle_id = _find_catalog_vehicle_id_cached(key)
    if not vehicle_id:
        return {}
    return _fetch_catalog_options_and_packages(vehicle_id)
