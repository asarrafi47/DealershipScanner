"""
The one reader of ``epa_extended_specs``.

Before 2026-10-01 each consumer opened its own connection, wrote its own SELECT
and kept its own lru_caches over the same rows (``knowledge_engine`` x3,
``generated_spec_sheet`` x3, ``tco_fuel_estimates`` x2). They now read rows
through here; each still applies its own gate to the row (the attributable-
quote gate, the fuel-tank quote, the plausibility bands), because those are
different questions about the same row.

Row lookups (all memoized for the process; the table is a reference table
rebuilt by scripts -- call :func:`clear_cache` after a reload):

* :func:`row_by_master_id` -- the resolved catalog link (exact FK).
* :func:`row_by_ymmt` -- exact (year, make, model, trim) when a trim is given,
  else / then any row for (year, make, model).
* :func:`hp_family_suspicious` -- every trim of the family scraped to one
  horsepower (scrape-bug fingerprint).
* :func:`fields_shared_across_trims` -- fields whose value repeats across two or
  more trims of the family (not attributable to one trim).

Rows come back as plain dicts (a fresh copy per call) with every column in
:data:`COLUMNS`. ``None`` means no row.
"""
from __future__ import annotations

from functools import lru_cache
from typing import Any

#: Every spec column any consumer reads.
SPEC_COLUMNS: tuple[str, ...] = (
    "horsepower",
    "torque_lb_ft",
    "torque_nm",
    "curb_weight_lb",
    "curb_weight_kg",
    "zero_to_60_sec",
    "fuel_tank_gal",
    "ev_range_miles",
    "battery_kwh",
    "tow_capacity_lb",
)
COLUMNS: tuple[str, ...] = (*SPEC_COLUMNS, "year", "make", "model", "trim", "specs_json")
_SELECT = ", ".join(COLUMNS)

_Row = tuple[tuple[str, Any], ...]


def _get_conn():
    # Looked up at call time so tests that patch ``backend.db.inventory_db.get_conn``
    # (and the knowledge-engine ``_conn`` seam) reach this reader.
    from backend.db import inventory_db

    return inventory_db.get_conn()


def _fetch(where: str, params: tuple[Any, ...]) -> _Row | None:
    """One row or None. Raises on DB error (callers decide what failure means)."""
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT {_SELECT} FROM epa_extended_specs {where} LIMIT 1", params)
        row = cur.fetchone()
    finally:
        try:
            conn.close()
        except Exception:
            pass
    if not row:
        return None
    return tuple(zip(COLUMNS, row))


def _fetch_quiet(where: str, params: tuple[Any, ...]) -> _Row | None:
    try:
        return _fetch(where, params)
    except Exception:
        return None


@lru_cache(maxsize=8192)
def _by_master_id(epa_master_id: int) -> _Row | None:
    return _fetch_quiet("WHERE epa_master_id=?", (epa_master_id,))


@lru_cache(maxsize=16384)
def _by_ymmt(year: int, make: str, model: str, trim: str) -> _Row | None:
    if trim:
        row = _fetch_quiet(
            "WHERE year=? AND lower(make)=lower(?) AND lower(model)=lower(?) AND lower(trim)=lower(?)",
            (year, make, model, trim),
        )
        if row is not None:
            return row
    return _fetch_quiet(
        "WHERE year=? AND lower(make)=lower(?) AND lower(model)=lower(?)",
        (year, make, model),
    )


def row_by_master_id(epa_master_id: Any) -> dict[str, Any] | None:
    try:
        mid = int(epa_master_id)
    except (TypeError, ValueError):
        return None
    if mid <= 0:
        return None
    row = _by_master_id(mid)
    return dict(row) if row is not None else None


def row_by_ymmt(year: Any, make: Any, model: Any, trim: Any = "") -> dict[str, Any] | None:
    try:
        y = int(year)
    except (TypeError, ValueError):
        return None
    mk = str(make or "").strip()
    md = str(model or "").strip()
    if not y or not mk or not md:
        return None
    row = _by_ymmt(y, mk, md, str(trim or "").strip())
    return dict(row) if row is not None else None


@lru_cache(maxsize=16384)
def hp_family_suspicious(year: int, make: str, model: str) -> bool:
    """True when EVERY trim (>= 4 rows) of a (year, make, model) family stored the
    same horsepower -- a scrape-bug fingerprint (2011 E-Class: E350, E550 and the
    518-hp E63 all stored as 375). False on any error."""
    try:
        conn = _get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT COUNT(*), COUNT(DISTINCT horsepower) FROM epa_extended_specs "
                "WHERE year=? AND lower(make)=lower(?) AND lower(model)=lower(?) AND horsepower IS NOT NULL",
                (year, make.strip(), model.strip()),
            )
            n, distinct = cur.fetchone()
        finally:
            conn.close()
        return int(n or 0) >= 4 and int(distinct or 0) == 1
    except Exception:
        return False


@lru_cache(maxsize=16384)
def fields_shared_across_trims(
    year: int, make: str, model: str, fields: tuple[str, ...]
) -> frozenset[str] | None:
    """Fields (of *fields*) whose stored value is the SAME on two or more trims.

    Returns None when the spread cannot be read (DB error) -- callers decide; an
    unknown spread is not permission to render. Empty frozenset when the family
    has no rows.
    """
    def _int(v: Any) -> int:
        try:
            return int(v or 0)
        except (TypeError, ValueError):
            return 0

    cols = ", ".join(
        f"count(DISTINCT trim) FILTER (WHERE {f} IS NOT NULL), count(DISTINCT {f})" for f in fields
    )
    try:
        conn = _get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT {cols} FROM epa_extended_specs "
                "WHERE year=? AND lower(make)=lower(?) AND lower(model)=lower(?)",
                (year, make, model),
            )
            row = cur.fetchone()
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception:
        return None
    if not row:
        return frozenset()
    shared: set[str] = set()
    for idx, field in enumerate(fields):
        if _int(row[idx * 2]) >= 2 and _int(row[idx * 2 + 1]) <= 1:
            shared.add(field)
    return frozenset(shared)


def clear_cache() -> None:
    """Drop every memoized read (tests, and after a catalog reload)."""
    _by_master_id.cache_clear()
    _by_ymmt.cache_clear()
    hp_family_suspicious.cache_clear()
    fields_shared_across_trims.cache_clear()
