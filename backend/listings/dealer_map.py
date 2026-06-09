"""Dealership map payload for car detail pages (Leaflet + Google Maps link)."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote_plus


def _format_address_line(
    *,
    street: str | None = None,
    city: str | None = None,
    state: str | None = None,
    zip_code: str | None = None,
) -> str:
    parts: list[str] = []
    st = (street or "").strip()
    if st:
        parts.append(st)
    cs = ", ".join(x for x in [(city or "").strip(), (state or "").strip()] if x)
    z = (zip_code or "").strip()
    if cs and z:
        parts.append(f"{cs} {z}")
    elif cs:
        parts.append(cs)
    elif z:
        parts.append(z)
    return " · ".join(parts)


def _google_maps_url(*, lat: float | None, lon: float | None, query: str) -> str:
    if lat is not None and lon is not None:
        return f"https://www.google.com/maps/search/?api=1&query={lat},{lon}"
    q = (query or "").strip()
    if q:
        return f"https://www.google.com/maps/search/?api=1&query={quote_plus(q)}"
    return "https://www.google.com/maps"


def _coords_from_dealer_info(dealer_info: dict[str, Any]) -> tuple[float, float] | None:
    try:
        lat = dealer_info.get("latitude")
        lon = dealer_info.get("longitude")
        if lat is None or lon is None:
            return None
        la, lo = float(lat), float(lon)
        if -90 <= la <= 90 and -180 <= lo <= 180:
            return (la, lo)
    except (TypeError, ValueError):
        pass
    return None


def _coords_from_dealer_url(dealer_url: str) -> tuple[float, float] | None:
    du = (dealer_url or "").strip()
    if not du:
        return None
    try:
        from backend.db.dealer_geo import load_dealer_geo_index, lookup_dealer_coords
        from backend.db.inventory_db import get_conn

        conn = get_conn()
        try:
            geo = load_dealer_geo_index(conn)
            hit = lookup_dealer_coords(du, geo)
            return hit if hit else None
        finally:
            conn.close()
    except Exception:
        return None


def _coords_from_registry(registry_id: int) -> tuple[float, float] | None:
    try:
        from backend.db.dealerships_db import get_dealership_by_id

        row = get_dealership_by_id(registry_id)
        if not row:
            return None
        return _coords_from_dealer_info(row)
    except Exception:
        return None


def _coords_from_zip(zip_code: str) -> tuple[float, float] | None:
    z = (zip_code or "").strip()
    if not z:
        return None
    try:
        from backend.db.geo import zip_to_coords

        return zip_to_coords(z)
    except Exception:
        return None


def build_dealer_map_for_car(
    car_raw: dict[str, Any],
    dealer_info: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """
    JSON-friendly map context for a single listing.

    Coordinates resolve in order: registry row, dealer URL geopoints, listing ZIP centroid.
    """
    name = (car_raw.get("dealer_name") or "").strip()
    street = city = state = zip_code = None

    if dealer_info:
        name = (dealer_info.get("name") or name).strip()
        street = (dealer_info.get("street_address") or "").strip() or None
        city = (dealer_info.get("city") or "").strip() or None
        state = (dealer_info.get("state") or "").strip() or None
        zip_code = (dealer_info.get("zip_code") or "").strip() or None

    lat_lon = None
    if dealer_info:
        lat_lon = _coords_from_dealer_info(dealer_info)
    if not lat_lon:
        reg_id = car_raw.get("dealership_registry_id")
        try:
            rid = int(reg_id) if reg_id is not None else 0
        except (TypeError, ValueError):
            rid = 0
        if rid > 0:
            lat_lon = _coords_from_registry(rid)
    if not lat_lon:
        lat_lon = _coords_from_dealer_url(str(car_raw.get("dealer_url") or ""))
    if not lat_lon:
        lat_lon = _coords_from_zip(str(car_raw.get("zip_code") or ""))

    address_line = _format_address_line(
        street=street,
        city=city,
        state=state,
        zip_code=zip_code,
    )
    if not name and not address_line and not lat_lon:
        return None

    lat = lon = None
    has_pin = False
    if lat_lon:
        lat, lon = lat_lon
        has_pin = True

    maps_query = name
    if address_line:
        maps_query = f"{name}, {address_line}" if name else address_line

    return {
        "name": name or "Dealership",
        "street_address": street or "",
        "city": city or "",
        "state": state or "",
        "zip_code": zip_code or "",
        "address_line": address_line,
        "lat": lat,
        "lon": lon,
        "has_pin": has_pin,
        "google_maps_url": _google_maps_url(lat=lat, lon=lon, query=maps_query),
    }
