"""
Public dealer locator: merge registry dealerships with Google Places car dealers.
"""
from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse

from thefuzz import fuzz

from backend.db.geo import haversine

_MATCH_DISTANCE_MI = 0.35
_MATCH_NAME_SCORE = 85


def _host_key(url: str | None) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    try:
        host = (urlparse(raw).netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def resolve_search_center(
    *,
    lat: float | None = None,
    lon: float | None = None,
    zip_code: str | None = None,
    city: str | None = None,
    state: str | None = None,
) -> tuple[float, float] | None:
    """Resolve (lat, lon) from explicit coords, ZIP, or city+state."""
    if lat is not None and lon is not None:
        try:
            la, lo = float(lat), float(lon)
            if -90 <= la <= 90 and -180 <= lo <= 180:
                return (la, lo)
        except (TypeError, ValueError):
            pass

    z = (zip_code or "").strip()
    if z:
        from backend.db.geo import zip_to_coords

        coords = zip_to_coords(z)
        if coords:
            return coords

    c = (city or "").strip()
    st = (state or "").strip().upper()
    if c and len(st) == 2:
        from backend.db.dealerships_db import geocode_city_state

        coords = geocode_city_state(c, st)
        if coords:
            return coords
    return None


def _registry_row_to_dealer(row: dict[str, Any], listing_count: int) -> dict[str, Any]:
    rid = int(row.get("id") or 0)
    return {
        "key": f"db-{rid}",
        "registry_id": rid,
        "name": (row.get("name") or "").strip(),
        "city": (row.get("city") or "").strip(),
        "state": (row.get("state") or "").strip(),
        "street_address": (row.get("street_address") or "").strip(),
        "zip_code": (row.get("zip_code") or "").strip(),
        "latitude": float(row["latitude"]),
        "longitude": float(row["longitude"]),
        "distance_miles": row.get("distance_miles"),
        "in_database": True,
        "listing_count": listing_count,
        "website_url": (row.get("dealer_website_url") or row.get("website_url") or "").strip(),
        "source": "database",
    }


def _matches_registry(google_row: dict[str, Any], registry: dict[str, Any]) -> bool:
    g_lat = float(google_row["latitude"])
    g_lon = float(google_row["longitude"])
    r_lat = float(registry["latitude"])
    r_lon = float(registry["longitude"])
    dist = haversine(g_lat, g_lon, r_lat, r_lon)
    if dist <= 0.15:
        return True

    g_host = _host_key(google_row.get("website_url"))
    r_host = _host_key(registry.get("dealer_website_url") or registry.get("website_url"))
    if g_host and r_host and g_host == r_host:
        return True

    if dist <= _MATCH_DISTANCE_MI:
        score = fuzz.token_set_ratio(
            (google_row.get("name") or "").strip(),
            (registry.get("name") or "").strip(),
        )
        if score >= _MATCH_NAME_SCORE:
            return True
    return False


def _google_candidate_to_row(candidate: Any, center_lat: float, center_lon: float) -> dict[str, Any]:
    lat = float(candidate.latitude)
    lon = float(candidate.longitude)
    return {
        "name": (candidate.name or "").strip(),
        "city": (candidate.city or "").strip(),
        "state": (candidate.state or "").strip(),
        "street_address": (candidate.street_address or "").strip(),
        "zip_code": (candidate.zip_code or "").strip(),
        "latitude": lat,
        "longitude": lon,
        "distance_miles": round(haversine(center_lat, center_lon, lat, lon), 2),
        "website_url": (candidate.dealer_website_url or candidate.website_url or "").strip(),
    }


def find_nearby_dealers(
    *,
    lat: float | None = None,
    lon: float | None = None,
    zip_code: str | None = None,
    city: str | None = None,
    state: str | None = None,
    radius_miles: float = 25.0,
    include_google: bool = True,
) -> dict[str, Any]:
    """
    Dealers within radius from our registry plus optional Google Places results.

    Registry rows are always returned (with or without inventory).
    Google-only dealers are appended when not matched to a registry row.
    """
    from backend.db.dealerships_db import search_dealerships_by_radius
    from backend.listings.nearby_dealers import _active_listing_counts

    radius_miles = max(1.0, min(float(radius_miles), 50.0))
    center = resolve_search_center(
        lat=lat, lon=lon, zip_code=zip_code, city=city, state=state
    )
    if not center:
        return {
            "ok": False,
            "error": "location_not_found",
            "dealers": [],
            "center": None,
            "radius_miles": radius_miles,
            "google_available": bool((os.environ.get("GOOGLE_MAPS_API_KEY") or "").strip()),
        }

    center_lat, center_lon = center
    registry_rows = search_dealerships_by_radius(center_lat, center_lon, radius_miles)
    registry_ids = [int(r["id"]) for r in registry_rows if r.get("id")]
    counts = _active_listing_counts(registry_ids)

    dealers: list[dict[str, Any]] = []
    matched_registry: set[int] = set()

    for row in registry_rows:
        rid = int(row["id"])
        dealers.append(_registry_row_to_dealer(row, counts.get(rid, 0)))

    google_available = bool((os.environ.get("GOOGLE_MAPS_API_KEY") or "").strip())
    if include_google and google_available:
        from backend.discovery.google_places import fetch_google_places_dealerships

        google_candidates = fetch_google_places_dealerships(
            center_lat, center_lon, radius_miles
        )
        for idx, candidate in enumerate(google_candidates):
            g_row = _google_candidate_to_row(candidate, center_lat, center_lon)
            match_id: int | None = None
            for reg in registry_rows:
                rid = int(reg["id"])
                if rid in matched_registry:
                    continue
                if _matches_registry(g_row, reg):
                    match_id = rid
                    matched_registry.add(rid)
                    break

            if match_id is not None:
                for d in dealers:
                    if d.get("registry_id") == match_id:
                        d["source"] = "both"
                        if not d.get("website_url") and g_row.get("website_url"):
                            d["website_url"] = g_row["website_url"]
                        if not d.get("street_address") and g_row.get("street_address"):
                            d["street_address"] = g_row["street_address"]
                        if not d.get("zip_code") and g_row.get("zip_code"):
                            d["zip_code"] = g_row["zip_code"]
                        break
                continue

            dealers.append(
                {
                    "key": f"gp-{idx}",
                    "registry_id": None,
                    "name": g_row["name"],
                    "city": g_row["city"],
                    "state": g_row["state"],
                    "street_address": g_row["street_address"],
                    "zip_code": g_row["zip_code"],
                    "latitude": g_row["latitude"],
                    "longitude": g_row["longitude"],
                    "distance_miles": g_row["distance_miles"],
                    "in_database": False,
                    "listing_count": 0,
                    "website_url": g_row["website_url"],
                    "source": "google",
                }
            )

    dealers.sort(key=lambda d: (not d.get("in_database"), float(d.get("distance_miles") or 9999)))

    return {
        "ok": True,
        "center": {"lat": center_lat, "lon": center_lon},
        "radius_miles": radius_miles,
        "dealers": dealers,
        "total": len(dealers),
        "in_database_count": sum(1 for d in dealers if d.get("in_database")),
        "google_available": google_available,
        "google_included": include_google and google_available,
    }
