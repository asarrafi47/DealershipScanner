"""
Premium listings: nearby dealerships with active inventory, capped or search-scoped.
"""
from __future__ import annotations

import os
from typing import Any

_DEFAULT_CAP = max(1, int(os.environ.get("NEARBY_DEALER_LIST_CAP", "10")))


def _active_listing_counts(registry_ids: list[int]) -> dict[int, int]:
    """Active inventory count per dealership registry id (missing ids → omitted; use ``.get(id, 0)``)."""
    ids: list[int] = []
    for raw in registry_ids:
        try:
            rid = int(raw)
        except (TypeError, ValueError):
            continue
        if rid > 0:
            ids.append(rid)
    if not ids:
        return {}

    from backend.db.inventory_db import get_conn

    placeholders = ",".join("?" * len(ids))
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT CAST(dealership_registry_id AS INTEGER) AS rid, COUNT(*) AS n
        FROM cars
        WHERE COALESCE(listing_active, 1) = 1
          AND CAST(dealership_registry_id AS INTEGER) IN ({placeholders})
        GROUP BY CAST(dealership_registry_id AS INTEGER)
        """,
        ids,
    )
    rows = cur.fetchall()
    conn.close()
    out: dict[int, int] = {}
    for rid_raw, count_raw in rows:
        try:
            out[int(rid_raw)] = int(count_raw)
        except (TypeError, ValueError):
            continue
    return out


def _active_listing_counts_by_dealer_id(dealer_ids: list[str]) -> dict[str, int]:
    """Active inventory count per scanner ``dealer_id`` slug."""
    dids = sorted({(d or "").strip().lower() for d in dealer_ids if (d or "").strip()})
    if not dids:
        return {}

    from backend.db.inventory_db import get_conn

    placeholders = ",".join("?" * len(dids))
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT lower(trim(dealer_id)) AS did, COUNT(*) AS n
        FROM cars
        WHERE COALESCE(listing_active, 1) = 1
          AND dealer_id IS NOT NULL
          AND trim(dealer_id) != ''
          AND lower(trim(dealer_id)) IN ({placeholders})
        GROUP BY lower(trim(dealer_id))
        """,
        dids,
    )
    rows = cur.fetchall()
    conn.close()
    out: dict[str, int] = {}
    for did_raw, count_raw in rows:
        did = (did_raw or "").strip().lower()
        if not did:
            continue
        try:
            out[did] = int(count_raw)
        except (TypeError, ValueError):
            continue
    return out


def attach_listing_counts(dealers: list[dict[str, Any]]) -> None:
    """Set ``listing_count`` from registry id and/or website dealer slug."""
    from backend.dev.dealers import slug_from_url

    registry_ids = [
        int(d["registry_id"])
        for d in dealers
        if d.get("registry_id") is not None
    ]
    dealer_ids: list[str] = []
    for d in dealers:
        url = (d.get("website_url") or "").strip()
        if url:
            slug = slug_from_url(url)
            if slug and slug != "dealer":
                dealer_ids.append(slug)

    by_registry = _active_listing_counts(registry_ids)
    by_dealer_id = _active_listing_counts_by_dealer_id(dealer_ids)

    for d in dealers:
        count = 0
        rid = d.get("registry_id")
        if rid is not None:
            try:
                count = max(count, by_registry.get(int(rid), 0))
            except (TypeError, ValueError):
                pass
        url = (d.get("website_url") or "").strip()
        if url:
            did = slug_from_url(url)
            if did:
                count = max(count, by_dealer_id.get(did, 0))
        d["listing_count"] = count


def _parse_registry_ids(registry_ids: list[int]) -> list[int]:
    ids: list[int] = []
    for raw in registry_ids:
        try:
            rid = int(raw)
        except (TypeError, ValueError):
            continue
        if rid > 0:
            ids.append(rid)
    return ids


def _last_inventory_scraped_at(registry_ids: list[int]) -> dict[int, str]:
    """Latest ``cars.scraped_at`` per registry id (any listing, active or not)."""
    ids = _parse_registry_ids(registry_ids)
    if not ids:
        return {}

    from backend.db.inventory_db import get_conn

    placeholders = ",".join("?" * len(ids))
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT CAST(dealership_registry_id AS INTEGER) AS rid, MAX(scraped_at) AS last_scraped_at
        FROM cars
        WHERE dealership_registry_id IS NOT NULL
          AND CAST(dealership_registry_id AS INTEGER) IN ({placeholders})
        GROUP BY CAST(dealership_registry_id AS INTEGER)
        """,
        ids,
    )
    rows = cur.fetchall()
    conn.close()
    out: dict[int, str] = {}
    for rid_raw, ts in rows:
        if not ts:
            continue
        try:
            out[int(rid_raw)] = str(ts)
        except (TypeError, ValueError):
            continue
    return out


def _last_inventory_scraped_at_by_dealer_id(dealer_ids: list[str]) -> dict[str, str]:
    """Latest ``cars.scraped_at`` per scanner ``dealer_id`` slug."""
    dids = sorted({(d or "").strip().lower() for d in dealer_ids if (d or "").strip()})
    if not dids:
        return {}

    from backend.db.inventory_db import get_conn

    placeholders = ",".join("?" * len(dids))
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT lower(trim(dealer_id)) AS did, MAX(scraped_at) AS last_scraped_at
        FROM cars
        WHERE dealer_id IS NOT NULL
          AND trim(dealer_id) != ''
          AND lower(trim(dealer_id)) IN ({placeholders})
        GROUP BY lower(trim(dealer_id))
        """,
        dids,
    )
    rows = cur.fetchall()
    conn.close()
    out: dict[str, str] = {}
    for did_raw, ts in rows:
        did = (did_raw or "").strip().lower()
        if not did or not ts:
            continue
        out[did] = str(ts)
    return out


def _last_catalog_scan_at(
    *,
    registry_ids: list[int],
    dealer_ids: list[str],
) -> tuple[dict[int, str], dict[str, str]]:
    """``dealer_scan_registry.last_scan_at`` keyed by registry id and dealer slug (Postgres only)."""
    reg_ids = _parse_registry_ids(registry_ids)
    dids = sorted({(d or "").strip().lower() for d in dealer_ids if (d or "").strip()})
    if not reg_ids and not dids:
        return {}, {}

    from backend.db.inventory_pg import is_inventory_postgres, pg_connect

    if not is_inventory_postgres():
        return {}, {}

    conn = pg_connect()
    try:
        cur = conn.cursor()
        clauses: list[str] = []
        params: list[Any] = []
        if reg_ids:
            clauses.append("registry_id = ANY(%s)")
            params.append(reg_ids)
        if dids:
            clauses.append("dealer_id = ANY(%s)")
            params.append(dids)
        cur.execute(
            f"""
            SELECT registry_id, dealer_id, last_scan_at
            FROM dealer_scan_registry
            WHERE last_scan_at IS NOT NULL AND ({' OR '.join(clauses)})
            """,
            tuple(params),
        )
        by_registry: dict[int, str] = {}
        by_dealer_id: dict[str, str] = {}
        for reg_raw, did_raw, ts in cur.fetchall():
            if not ts:
                continue
            ts_s = str(ts)
            if reg_raw is not None:
                try:
                    by_registry[int(reg_raw)] = ts_s
                except (TypeError, ValueError):
                    pass
            did = (did_raw or "").strip().lower()
            if did:
                by_dealer_id[did] = ts_s
        return by_registry, by_dealer_id
    finally:
        conn.close()


def _max_iso_timestamp(*values: str | None) -> str | None:
    best: str | None = None
    for raw in values:
        ts = (raw or "").strip()
        if not ts:
            continue
        if best is None or ts > best:
            best = ts
    return best


def attach_last_synced_at(dealers: list[dict[str, Any]]) -> None:
    """Set ``last_synced_at`` on each dealer row (inventory scrape vs catalog scan, latest wins)."""
    from backend.dev.dealers import slug_from_url

    registry_ids = [
        int(d["registry_id"])
        for d in dealers
        if d.get("registry_id") is not None
    ]
    dealer_ids: list[str] = []
    for d in dealers:
        url = (d.get("website_url") or "").strip()
        if url:
            slug = slug_from_url(url)
            if slug and slug != "dealer":
                dealer_ids.append(slug)

    inv_ts = _last_inventory_scraped_at(registry_ids)
    inv_by_did = _last_inventory_scraped_at_by_dealer_id(dealer_ids)
    cat_by_reg, cat_by_did = _last_catalog_scan_at(
        registry_ids=registry_ids,
        dealer_ids=dealer_ids,
    )

    for d in dealers:
        candidates: list[str | None] = []
        rid = d.get("registry_id")
        if rid is not None:
            try:
                rid_i = int(rid)
            except (TypeError, ValueError):
                rid_i = None
            if rid_i:
                candidates.append(inv_ts.get(rid_i))
                candidates.append(cat_by_reg.get(rid_i))
        url = (d.get("website_url") or "").strip()
        if url:
            did = slug_from_url(url)
            if did:
                candidates.append(cat_by_did.get(did))
                candidates.append(inv_by_did.get(did))
        d["last_synced_at"] = _max_iso_timestamp(*candidates)


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
