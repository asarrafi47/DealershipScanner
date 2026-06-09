"""Dealer URL → lat/lon lookup for listings radius filtering."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse


def normalize_dealer_host(dealer_url: str) -> str:
    """Lowercase netloc without ``www.`` — stable key for URL variants."""
    try:
        host = (urlparse((dealer_url or "").strip()).netloc or "").lower()
    except ValueError:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host


def build_dealer_geo_index(
    rows: list[tuple[str, float, float]],
) -> dict[str, tuple[float, float]]:
    """
    Build a lookup dict keyed by full ``dealer_url`` and by normalized host.

    Host keys use the ``host:`` prefix so they cannot collide with URL strings.
    """
    out: dict[str, tuple[float, float]] = {}
    for dealer_url, lat, lon in rows:
        if not dealer_url:
            continue
        try:
            coords = (float(lat), float(lon))
        except (TypeError, ValueError):
            continue
        url_key = str(dealer_url).strip()
        if not url_key:
            continue
        out[url_key] = coords
        host = normalize_dealer_host(url_key)
        if host:
            out[f"host:{host}"] = coords
    return out


def lookup_dealer_coords(
    dealer_url: str,
    dealer_geo: dict[str, tuple[float, float]],
) -> tuple[float, float] | None:
    """Resolve dealer coordinates from exact URL or normalized host."""
    du = (dealer_url or "").strip()
    if not du or not dealer_geo:
        return None
    hit = dealer_geo.get(du)
    if hit:
        return hit
    host = normalize_dealer_host(du)
    if host:
        return dealer_geo.get(f"host:{host}")
    return None


def dealer_coords_client_map(
    dealer_geo: dict[str, tuple[float, float]],
) -> dict[str, list[float]]:
    """
    JSON-friendly map for ``GET /api/listings/geo-coords``.

    Includes full URLs and ``host:`` keys so the browser can match listing rows
    even when ``dealer_url`` text differs from ``dealer_geopoints.dealer_url``.
    """
    out: dict[str, list[float]] = {}
    for key, (lat, lon) in dealer_geo.items():
        out[key] = [lat, lon]
    return out


def load_dealer_geo_index(conn: Any) -> dict[str, tuple[float, float]]:
    """Merge dealer_geopoints with dealerships registry (registry fills gaps)."""
    rows: list[tuple[str, float, float]] = []
    seen_urls: set[str] = set()

    def add_row(url: str, lat: float, lon: float) -> None:
        u = (url or "").strip()
        if not u:
            return
        key = u.lower()
        if key in seen_urls:
            return
        seen_urls.add(key)
        rows.append((u, float(lat), float(lon)))

    try:
        for url, lat, lon in conn.execute(
            "SELECT dealer_url, lat, lon FROM dealer_geopoints "
            "WHERE lat IS NOT NULL AND lon IS NOT NULL"
        ).fetchall():
            if url:
                add_row(str(url).strip(), float(lat), float(lon))
    except Exception:
        pass
    try:
        for url, lat, lon in conn.execute(
            "SELECT website_url, latitude, longitude FROM dealerships "
            "WHERE latitude IS NOT NULL AND longitude IS NOT NULL "
            "AND website_url IS NOT NULL AND TRIM(website_url) != ''"
        ).fetchall():
            if url:
                add_row(str(url).strip(), float(lat), float(lon))
    except Exception:
        pass
    try:
        for url, lat, lon in conn.execute(
            "SELECT dealer_website_url, latitude, longitude FROM dealerships "
            "WHERE latitude IS NOT NULL AND longitude IS NOT NULL "
            "AND dealer_website_url IS NOT NULL AND TRIM(dealer_website_url) != ''"
        ).fetchall():
            if url:
                add_row(str(url).strip(), float(lat), float(lon))
    except Exception:
        pass
    return build_dealer_geo_index(rows)


def load_registry_coords_map(conn: Any) -> dict[str, list[float]]:
    """Dealership registry id → [lat, lon] for listings radius (cars often lack zip_code)."""
    out: dict[str, list[float]] = {}
    try:
        for rid, lat, lon in conn.execute(
            "SELECT id, latitude, longitude FROM dealerships "
            "WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
        ).fetchall():
            if rid is None:
                continue
            try:
                out[str(int(rid))] = [float(lat), float(lon)]
            except (TypeError, ValueError):
                continue
    except Exception:
        pass
    return out
