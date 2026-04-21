"""Parameterized SQL for store admin over ``inventory.db`` (scoped by user role)."""

from __future__ import annotations

import sqlite3
from typing import Any

from backend.db.inventory_db import get_conn


def _scope_sql(profile: dict[str, Any]) -> tuple[str, tuple[Any, ...]]:
    """
    Return SQL predicate (no leading ``AND``) plus bind values for dealer scoping.

    ``admin``: empty predicate. ``dealer_staff`` without dealer/registry: ``0`` (no rows).
    """
    role = (profile.get("role") or "dealer_staff").strip().lower()
    if role == "admin":
        return "", ()

    did = (profile.get("dealer_id") or "").strip()
    reg = profile.get("dealership_registry_id")
    parts: list[str] = []
    vals: list[Any] = []
    if did:
        parts.append("dealer_id = ?")
        vals.append(did)
    if reg is not None:
        try:
            ri = int(reg)
            parts.append("dealership_registry_id = ?")
            vals.append(ri)
        except (TypeError, ValueError):
            pass
    if not parts:
        return "0", ()
    if len(parts) == 1:
        return parts[0], tuple(vals)
    return "(" + " OR ".join(parts) + ")", tuple(vals)


def dashboard_summary(profile: dict[str, Any], *, top_dealers: int = 12) -> dict[str, Any]:
    """Active/inactive counts + optional per-dealer rollup for admins."""
    conn = get_conn()
    cur = conn.cursor()
    scope, bind = _scope_sql(profile)
    aw = ["(COALESCE(listing_active,1)=1)"]
    iw = ["(COALESCE(listing_active,1)!=1)"]
    if scope:
        aw.append(f"({scope})")
        iw.append(f"({scope})")
    active_sql = "SELECT COUNT(*) FROM cars WHERE " + " AND ".join(aw)
    cur.execute(active_sql, bind)
    active_n = int(cur.fetchone()[0])
    inact_sql = "SELECT COUNT(*) FROM cars WHERE " + " AND ".join(iw)
    cur.execute(inact_sql, bind)
    inactive_n = int(cur.fetchone()[0])

    dealers: list[dict[str, Any]] = []
    role = (profile.get("role") or "").strip().lower()
    if role == "admin":
        lim = max(1, min(50, int(top_dealers)))
        cur.execute(
            f"""
            SELECT dealer_id,
                   MAX(dealer_name) AS dealer_name,
                   SUM(CASE WHEN COALESCE(listing_active,1)=1 THEN 1 ELSE 0 END) AS active_units,
                   MAX(scraped_at) AS last_scraped_at
            FROM cars
            WHERE dealer_id IS NOT NULL AND trim(dealer_id) != ''
            GROUP BY dealer_id
            ORDER BY active_units DESC
            LIMIT {lim}
            """
        )
        for r in cur.fetchall():
            dealers.append(
                {
                    "dealer_id": r[0],
                    "dealer_name": r[1],
                    "active_units": int(r[2] or 0),
                    "last_scraped_at": r[3],
                }
            )
    stale_n = 0
    try:
        from backend.dealer_admin.merchandising import stale_price_days

        d = int(stale_price_days())
        sw = [
            "(price IS NOT NULL AND price > 0)",
            "datetime(replace(COALESCE(last_price_change_at, first_seen_at, scraped_at), 'Z', '')) "
            "<= datetime('now', ?)",
        ]
        sb = [f"-{d} days"]
        if scope:
            sw.append(f"({scope})")
            sb.extend(bind)
        cur.execute("SELECT COUNT(*) FROM cars WHERE " + " AND ".join(sw), sb)
        stale_n = int(cur.fetchone()[0])
    except (sqlite3.Error, TypeError, ValueError):
        stale_n = 0

    conn.close()
    return {
        "active_units": active_n,
        "inactive_units": inactive_n,
        "stale_price_units": stale_n,
        "top_dealers": dealers,
    }


def list_inventory_rows(
    profile: dict[str, Any],
    *,
    page: int = 1,
    per_page: int = 40,
    active_only: bool = True,
    sort: str = "scraped_at",
    direction: str = "desc",
    q: str = "",
    stale_only: bool = False,
) -> tuple[list[dict[str, Any]], int]:
    """Paginated car rows as dicts (includes inactive when ``active_only`` is False)."""
    scope, bind = _scope_sql(profile)
    where_parts: list[str] = []
    params: list[Any] = []
    if scope:
        where_parts.append(f"({scope})")
        params.extend(bind)

    if active_only:
        where_parts.append("(COALESCE(listing_active,1)=1)")

    qv = (q or "").strip()
    if qv:
        where_parts.append(
            "("
            "vin LIKE ? ESCAPE '\\' OR stock_number LIKE ? ESCAPE '\\' "
            "OR make LIKE ? ESCAPE '\\' OR model LIKE ? ESCAPE '\\'"
            ")"
        )
        like = qv.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        pat = f"%{like}%"
        params.extend([pat, pat, pat, pat])

    if stale_only:
        from backend.dealer_admin.merchandising import stale_price_days

        d = int(stale_price_days())
        where_parts.append(
            "(price IS NOT NULL AND price > 0 AND "
            "datetime(replace(COALESCE(last_price_change_at, first_seen_at, scraped_at), 'Z', '')) "
            "<= datetime('now', ?))"
        )
        params.append(f"-{d} days")

    where_sql = " AND ".join(where_parts) if where_parts else "1"

    sort_map = {
        "price": "price",
        "mileage": "mileage",
        "year": "year",
        "scraped_at": "scraped_at",
        "vin": "vin",
        "data_quality_score": "data_quality_score",
    }
    col = sort_map.get((sort or "").strip().lower(), "scraped_at")
    dire = "DESC" if (direction or "").strip().lower() != "asc" else "ASC"

    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    count_sql = f"SELECT COUNT(*) FROM cars WHERE {where_sql}"
    cur.execute(count_sql, params)
    total = int(cur.fetchone()[0])

    pp = max(5, min(100, int(per_page)))
    pg = max(1, int(page))
    offset = (pg - 1) * pp

    cur.execute(
        f"""
        SELECT * FROM cars
        WHERE {where_sql}
        ORDER BY {col} {dire}, id DESC
        LIMIT ? OFFSET ?
        """,
        [*params, pp, offset],
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows, total


def car_visible_to_profile(profile: dict[str, Any], car_id: int) -> bool:
    """True if scoped user may read this ``cars.id``."""
    scope, bind = _scope_sql(profile)
    if not scope:
        return True
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT 1 FROM cars WHERE id = ? AND ({scope})", (int(car_id), *bind))
    ok = cur.fetchone() is not None
    conn.close()
    return ok


def export_inventory_rows(profile: dict[str, Any], *, limit: int = 5000) -> list[dict[str, Any]]:
    lim = max(1, min(20000, int(limit)))
    scope, bind = _scope_sql(profile)
    wh = ["(COALESCE(listing_active,1)=1)"]
    params: list[Any] = []
    if scope:
        wh.append(f"({scope})")
        params.extend(bind)
    where_sql = " AND ".join(wh)
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT id, vin, stock_number, year, make, model, trim, condition, mileage, price, msrp,
               dealer_id, dealer_name, scraped_at, first_seen_at, last_price_change_at,
               listing_active, data_quality_score, source_url, dealer_url
        FROM cars
        WHERE {where_sql}
        ORDER BY dealer_id, make, model, vin
        LIMIT ?
        """,
        (*params, lim),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows
