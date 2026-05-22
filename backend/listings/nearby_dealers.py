"""
Premium listings: nearby dealerships with active inventory, capped or search-scoped.
"""
from __future__ import annotations

import os
from typing import Any

_DEFAULT_CAP = max(1, int(os.environ.get("NEARBY_DEALER_LIST_CAP", "10")))


def _active_listing_counts(registry_ids: list[int]) -> dict[int, int]:
    if not registry_ids:
        return {}
    from backend.db.inventory_db import get_conn

    conn = get_conn()
    cur = conn.cursor()
    placeholders = ",".join("?" * len(registry_ids))
    cur.execute(
        f"""
        SELECT dealership_registry_id, COUNT(*) AS n
        FROM cars
        WHERE (COALESCE(listing_active, 1) = 1)
          AND dealership_registry_id IN ({placeholders})
          AND CAST(dealership_registry_id AS INTEGER) > 0
        GROUP BY dealership_registry_id
        """,
        registry_ids,
    )
    out = {int(row[0]): int(row[1]) for row in cur.fetchall() if row[0]}
    conn.close()
    return out


def resolve_nearby_dealers_for_listings(
    *,
    zip_code: str,
    radius_miles: float,
    search_query: str | None = None,
    cap: int | None = None,
) -> dict[str, Any]:
    """
    Dealers within radius that have ≥1 active listing.

    - **Broad** (no search query): sort by listing count desc, distance asc; cap at ``cap`` (default 10).
    - **Search** (non-empty ``search_query``): all dealers in radius that have matching
      inventory for the hybrid search (same ZIP/radius + query), uncapped.
    """
    from backend.db.dealerships_db import search_dealerships_by_radius
    from backend.db.geo import zip_to_coords
    from backend.utils.hybrid_search import hybrid_search_with_kwargs

    limit = cap if cap is not None else _DEFAULT_CAP
    zip_code = (zip_code or "").strip()
    q = (search_query or "").strip()
    search_mode = len(q) >= 2

    coords = zip_to_coords(zip_code)
    if not coords or radius_miles <= 0:
        return {
            "ok": False,
            "dealers": [],
            "mode": "search" if search_mode else "broad",
            "capped": False,
            "shown": 0,
            "total_with_inventory": 0,
            "total_in_radius": 0,
        }

    lat, lon = coords
    in_radius = search_dealerships_by_radius(lat, lon, float(radius_miles))
    total_in_radius = len(in_radius)
    if not in_radius:
        return {
            "ok": True,
            "dealers": [],
            "mode": "search" if search_mode else "broad",
            "capped": False,
            "shown": 0,
            "total_with_inventory": 0,
            "total_in_radius": 0,
        }

    registry_ids = [int(r["id"]) for r in in_radius if r.get("id")]
    counts = _active_listing_counts(registry_ids)

    with_stock: list[dict[str, Any]] = []
    for row in in_radius:
        did = int(row["id"])
        n = counts.get(did, 0)
        if n <= 0:
            continue
        with_stock.append(
            {
                "id": did,
                "name": row.get("name") or "",
                "city": row.get("city") or "",
                "state": row.get("state") or "",
                "distance_miles": row.get("distance_miles"),
                "listing_count": n,
            }
        )

    total_with_inventory = len(with_stock)
    if not with_stock:
        return {
            "ok": True,
            "dealers": [],
            "mode": "search" if search_mode else "broad",
            "capped": False,
            "shown": 0,
            "total_with_inventory": 0,
            "total_in_radius": total_in_radius,
        }

    if search_mode:
        sql_kwargs = {
            "zip_code": zip_code,
            "radius_miles": float(radius_miles),
            "include_incomplete": False,
        }
        cars, _meta = hybrid_search_with_kwargs(q, sql_kwargs, vector_top_k=150)
        match_ids: set[int] = set()
        for c in cars:
            try:
                rid = int(c.get("dealership_registry_id") or 0)
            except (TypeError, ValueError):
                rid = 0
            if rid > 0:
                match_ids.add(rid)
        filtered = [d for d in with_stock if d["id"] in match_ids]
        filtered.sort(
            key=lambda d: (-int(d["listing_count"]), float(d.get("distance_miles") or 9999)),
        )
        return {
            "ok": True,
            "dealers": filtered,
            "mode": "search",
            "capped": False,
            "shown": len(filtered),
            "total_with_inventory": total_with_inventory,
            "total_in_radius": total_in_radius,
            "search_query": q,
        }

    with_stock.sort(
        key=lambda d: (-int(d["listing_count"]), float(d.get("distance_miles") or 9999)),
    )
    capped = total_with_inventory > limit
    shown = with_stock[:limit] if capped else with_stock
    return {
        "ok": True,
        "dealers": shown,
        "mode": "broad",
        "capped": capped,
        "shown": len(shown),
        "total_with_inventory": total_with_inventory,
        "total_in_radius": total_in_radius,
        "cap": limit,
    }
