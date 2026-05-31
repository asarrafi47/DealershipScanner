"""
Premium listings: nearby dealerships with active inventory, capped or search-scoped.
"""
from __future__ import annotations

import os
from typing import Any

_DEFAULT_CAP = max(1, int(os.environ.get("NEARBY_DEALER_LIST_CAP", "10")))


def _dealers_with_inventory_near(
    lat: float,
    lon: float,
    radius_miles: float,
) -> list[dict[str, Any]]:
    """
    Dealers that have active inventory within *radius_miles* of (lat, lon).

    Uses the same geo sources as ``search_cars`` (``dealer_geopoints``, then registry
    coords), not registry lat/lon alone — so scanned dealers appear near the user's ZIP
    even when the nationwide registry row is missing or misplaced.
    """
    from backend.db.dealer_geo import (
        load_dealer_geo_index,
        lookup_dealer_coords,
        normalize_dealer_host,
    )
    from backend.db.dealerships_db import get_dealership_by_id
    from backend.db.geo import haversine
    from backend.db.inventory_db import get_conn

    from backend.listings.dealer_registry_match import registry_id_by_dealer_host

    conn = get_conn()
    dealer_geo = load_dealer_geo_index(conn)
    host_to_registry = registry_id_by_dealer_host(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT dealership_registry_id, MAX(dealer_url) AS dealer_url, COUNT(*) AS n
        FROM cars
        WHERE (COALESCE(listing_active, 1) = 1)
          AND CAST(dealership_registry_id AS INTEGER) > 0
        GROUP BY dealership_registry_id
        """
    )
    linked_rows = cur.fetchall()
    cur.execute(
        """
        SELECT dealer_url, COUNT(*) AS n
        FROM cars
        WHERE (COALESCE(listing_active, 1) = 1)
          AND (dealership_registry_id IS NULL OR CAST(dealership_registry_id AS INTEGER) <= 0)
          AND TRIM(COALESCE(dealer_url, '')) != ''
        GROUP BY dealer_url
        """
    )
    unlinked_rows = cur.fetchall()
    conn.close()

    by_id: dict[int, dict[str, Any]] = {}

    def _add_stock(reg_id: int, dealer_url: str, listing_count: int) -> None:
        if reg_id <= 0 or listing_count <= 0:
            return
        dest = lookup_dealer_coords(str(dealer_url or ""), dealer_geo)
        if not dest:
            dealer_row = get_dealership_by_id(reg_id)
            if dealer_row and dealer_row.get("latitude") is not None:
                try:
                    dest = (
                        float(dealer_row["latitude"]),
                        float(dealer_row["longitude"]),
                    )
                except (TypeError, ValueError):
                    dest = None
        if not dest:
            return
        dist = haversine(lat, lon, dest[0], dest[1])
        if dist > radius_miles:
            return
        dist_round = round(dist, 2)
        prev = by_id.get(reg_id)
        if prev is not None:
            prev["listing_count"] = int(prev["listing_count"]) + listing_count
            if dist_round < float(prev["distance_miles"]):
                prev["distance_miles"] = dist_round
            return
        dealer_row = get_dealership_by_id(reg_id)
        if not dealer_row:
            return
        by_id[reg_id] = {
            "id": reg_id,
            "name": dealer_row.get("name") or "",
            "city": dealer_row.get("city") or "",
            "state": dealer_row.get("state") or "",
            "distance_miles": dist_round,
            "listing_count": listing_count,
        }

    for reg_raw, dealer_url, count_raw in linked_rows:
        try:
            reg_id = int(reg_raw)
        except (TypeError, ValueError):
            continue
        try:
            listing_count = int(count_raw)
        except (TypeError, ValueError):
            listing_count = 0
        _add_stock(reg_id, str(dealer_url or ""), listing_count)

    for dealer_url, count_raw in unlinked_rows:
        try:
            listing_count = int(count_raw)
        except (TypeError, ValueError):
            listing_count = 0
        host = normalize_dealer_host(str(dealer_url or ""))
        reg_id = host_to_registry.get(host or "")
        if not reg_id:
            continue
        _add_stock(reg_id, str(dealer_url or ""), listing_count)

    out = list(by_id.values())
    out.sort(key=lambda d: (float(d["distance_miles"]), -int(d["listing_count"])))
    return out


def _cars_matching_search_in_radius(
    zip_code: str,
    radius_miles: float,
    search_query: str,
) -> list[dict[str, Any]]:
    """Inventory rows for *search_query* within ZIP/radius (aligned with listings geo)."""
    from backend.db.inventory_db import search_cars
    from backend.utils.hybrid_search import (
        _has_structured_filters,
        filters_dict_to_search_cars_kwargs,
        hybrid_search_with_kwargs,
    )
    from backend.utils.query_parser import parse_natural_query

    sql_kwargs: dict[str, Any] = {
        "zip_code": zip_code,
        "radius_miles": float(radius_miles),
        "include_incomplete": False,
    }
    parsed = parse_natural_query(search_query)
    if _has_structured_filters(parsed):
        merged = {**sql_kwargs, **filters_dict_to_search_cars_kwargs(parsed)}
        return search_cars(**merged)
    try:
        cars, _meta = hybrid_search_with_kwargs(
            search_query, sql_kwargs, vector_top_k=150
        )
        return cars
    except Exception:
        return search_cars(**sql_kwargs)


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
    radius_f = float(radius_miles)
    total_in_radius = len(search_dealerships_by_radius(lat, lon, radius_f))
    with_stock = _dealers_with_inventory_near(lat, lon, radius_f)
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
        from backend.db.inventory_db import get_conn as inv_get_conn
        from backend.listings.dealer_registry_match import (
            collect_registry_ids_from_cars,
            registry_id_by_dealer_host,
        )

        cars = _cars_matching_search_in_radius(zip_code, radius_f, q)
        conn = inv_get_conn()
        try:
            host_to_registry = registry_id_by_dealer_host(conn)
        finally:
            conn.close()
        match_ids = collect_registry_ids_from_cars(cars, host_to_registry)
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
