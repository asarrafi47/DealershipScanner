"""Phase 1 of ``search_cars``: rank the matching ids in the database.

Only ids (and, inside a ZIP radius, three narrow columns) cross the wire here;
:mod:`.hydrate` fetches full rows afterwards in this rank order.
"""
from __future__ import annotations

from typing import Any, Callable

from backend.db.repositories.search.where import SqlWhere

# Same order ``ordering._sort_cars_by_price`` applies at the end: priced rows
# cheapest first, "call for price" rows last; ``id`` makes the LIMIT deterministic.
_SEARCH_PRICE_ORDER_SQL = (
    "ORDER BY CASE WHEN price IS NULL OR price = 0 THEN 1 ELSE 0 END, price ASC, id ASC"
)


def resolve_origin(zip_code, radius_miles) -> tuple[bool, tuple[float, float] | None]:
    """``(radius_requested, origin)``; an unknown ZIP gives ``(True, None)`` = no results."""
    from backend.db.geo import zip_to_coords

    if zip_code and radius_miles:
        return True, zip_to_coords(zip_code)
    return False, None


def rank_ids_by_distance(
    conn: Any,
    w: SqlWhere,
    origin: tuple[float, float],
    radius_miles,
    *,
    open_conn: Callable[..., Any],
) -> tuple[list[int], dict[int, float]]:
    """Ids inside the radius, nearest first (then priced first, cheapest, id).

    The distance test needs every candidate's dealer, not its row -- scan three
    narrow columns. Returns the ranked ids and each id's rounded distance.
    """
    from backend.db.dealer_geo import load_dealer_geo_index, lookup_dealer_coords
    from backend.db.geo import haversine

    # Plain connection: the index reader unpacks rows positionally, which
    # the name-keyed rows of the ``row_factory`` search connection do not allow.
    with open_conn() as _gc:
        dealer_geo = load_dealer_geo_index(_gc)
    narrow = conn.execute(
        f"SELECT id, price, dealer_url FROM cars WHERE {w.sql}", w.params
    ).fetchall()
    dist_by_url: dict[str, float] = {}
    ranked: list[tuple[float, int, float, int]] = []
    for r in narrow:
        r = dict(r)  # Postgres compat rows are name-keyed, sqlite3.Row takes both
        du = str(r["dealer_url"] or "")
        dist = dist_by_url.get(du)
        if dist is None:
            dest = lookup_dealer_coords(du, dealer_geo)
            dist = haversine(origin[0], origin[1], dest[0], dest[1]) if dest else -1.0
            dist_by_url[du] = dist
        if dist < 0 or dist > float(radius_miles):
            continue
        try:
            price = float(r["price"] or 0)
        except (TypeError, ValueError):
            price = 0.0
        cid = int(r["id"])
        ranked.append((round(dist, 1), 1 if price <= 0 else 0, price, cid))
    ranked.sort()
    return [t[3] for t in ranked], {t[3]: t[0] for t in ranked}


def rank_ids_by_price(conn: Any, w: SqlWhere, lim: int | None, *, scan_cap: int) -> list[int]:
    """Matching ids cheapest first; capped at ``max(lim, scan_cap)`` when bounded."""
    scan_sql = f"SELECT id FROM cars WHERE {w.sql} {_SEARCH_PRICE_ORDER_SQL}"
    scan_params = list(w.params)
    if lim is not None:
        scan_sql += " LIMIT ?"
        scan_params.append(max(lim, scan_cap))
    return [int(dict(r)["id"]) for r in conn.execute(scan_sql, scan_params).fetchall()]
