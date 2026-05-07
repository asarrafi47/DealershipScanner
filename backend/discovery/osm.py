"""
OpenStreetMap dealership POIs via Overpass API.

Tags queried:
  - ``shop=car`` (documented primary tag for car dealerships)
  - ``amenity=car_dealer`` (sometimes used)

Respects Overpass usage: single bounded query, modest timeout, descriptive User-Agent.
"""
from __future__ import annotations

import logging
import math
import os
from typing import Any, Sequence

import requests

from backend.discovery.candidate import DealerCandidate
from backend.discovery.normalize import normalize_us_state_to_code

logger = logging.getLogger(__name__)

DEFAULT_OVERPASS_URL = "https://overpass-api.de/api/interpreter"
# Public mirrors (try after primary) when the main instance returns 502/504 or times out.
DEFAULT_OVERPASS_FALLBACKS: tuple[str, ...] = (
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.fr/api/interpreter",
)
USER_AGENT = (
    "SarrafiCollection/1.0 (+https://example.local; dealership discovery batch query)"
)


def circle_to_bbox(lat: float, lon: float, radius_miles: float) -> tuple[float, float, float, float]:
    """Return (south, west, north, east) covering the circle (approximate)."""
    dlat = radius_miles / 69.172
    cos_lat = math.cos(math.radians(lat))
    cos_lat = max(abs(cos_lat), 0.2)
    dlon = radius_miles / (69.172 * cos_lat)
    return (lat - dlat, lon - dlon, lat + dlat, lon + dlon)


def _tags_addr_line(tags: dict[str, Any]) -> str:
    parts = []
    hn = (tags.get("addr:housenumber") or "").strip()
    st = (tags.get("addr:street") or "").strip()
    if hn or st:
        parts.append(f"{hn} {st}".strip())
    elif tags.get("addr:full"):
        parts.append(str(tags["addr:full"]).strip())
    return " ".join(parts).strip()


def _element_lat_lon(el: dict[str, Any]) -> tuple[float | None, float | None]:
    if el.get("lat") is not None and el.get("lon") is not None:
        try:
            return (float(el["lat"]), float(el["lon"]))
        except (TypeError, ValueError):
            pass
    c = el.get("center") or {}
    if c.get("lat") is not None and c.get("lon") is not None:
        try:
            return (float(c["lat"]), float(c["lon"]))
        except (TypeError, ValueError):
            pass
    return (None, None)


def overpass_endpoints(primary: str | None = None) -> list[str]:
    """
    Ordered Overpass API URLs to try.

    Override with env ``DISCOVERY_OVERPASS_URLS`` (comma-separated list).
    Set ``DISCOVERY_OVERPASS_NO_FALLBACK=1`` to use only the primary endpoint.
    """
    env = (os.environ.get("DISCOVERY_OVERPASS_URLS") or "").strip()
    if env:
        return [x.strip() for x in env.split(",") if x.strip()]
    main = (primary or "").strip() or DEFAULT_OVERPASS_URL
    if (os.environ.get("DISCOVERY_OVERPASS_NO_FALLBACK") or "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        return [main]
    out: list[str] = [main]
    for u in DEFAULT_OVERPASS_FALLBACKS:
        if u not in out:
            out.append(u)
    return out


def _post_overpass(
    overpass_url: str,
    query: str,
    *,
    sess: requests.Session,
    timeout_s: float,
) -> dict[str, Any] | None:
    headers = {"User-Agent": USER_AGENT, "Content-Type": "text/plain; charset=utf-8"}
    try:
        r = sess.post(
            overpass_url,
            data=query.encode("utf-8"),
            headers=headers,
            timeout=timeout_s,
        )
        # Retry next mirror on gateway overload / unavailable
        if r.status_code in (502, 503, 504):
            logger.warning(
                "Overpass HTTP %s from %s — will try fallback if configured",
                r.status_code,
                overpass_url,
            )
            return None
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        logger.warning("Overpass request failed (%s): %s", overpass_url, e)
        return None
    except ValueError as e:
        logger.warning("Overpass JSON parse failed (%s): %s", overpass_url, e)
        return None


def _parse_osm_element(el: dict[str, Any]) -> DealerCandidate | None:
    tags = el.get("tags") or {}
    kind = el.get("type")
    osm_id = el.get("id")
    if osm_id is None or kind not in ("node", "way", "relation"):
        return None
    oid = f"{kind[0]}/{osm_id}"
    shop = (tags.get("shop") or "").lower()
    amenity = (tags.get("amenity") or "").lower()
    if shop != "car" and amenity != "car_dealer":
        return None
    name = (tags.get("name") or tags.get("brand") or "").strip()
    if not name:
        return None
    lat, lon = _element_lat_lon(el)
    if lat is None or lon is None:
        return None
    city = (tags.get("addr:city") or tags.get("addr:hamlet") or "").strip()
    state = normalize_us_state_to_code(tags.get("addr:state") or "")
    z = (tags.get("addr:postcode") or "").strip()
    street = _tags_addr_line(tags)
    url = (tags.get("website") or tags.get("contact:website") or "").strip()

    return DealerCandidate(
        name=name,
        city=city,
        state=state,
        street_address=street,
        zip_code=z,
        latitude=lat,
        longitude=lon,
        dealer_website_url=url,
        website_url=url,
        osm_id=oid,
        source_osm=True,
    )


def fetch_osm_dealerships(
    lat: float,
    lon: float,
    radius_miles: float,
    *,
    overpass_url: str | None = None,
    overpass_urls: Sequence[str] | None = None,
    timeout_s: float = 90.0,
    session: requests.Session | None = None,
) -> list[DealerCandidate]:
    """
    Query Overpass for car dealerships inside a bounding box around *lat*, *lon*.
    Results are filtered again by haversine distance (bbox is conservative).

    Tries ``overpass_urls`` when provided; otherwise builds a list from ``overpass_url``
    (see :func:`overpass_endpoints`) so 502/504 on one mirror can succeed on another.
    """
    from backend.db.geo import haversine

    south, west, north, east = circle_to_bbox(lat, lon, radius_miles)
    # Overpass bbox order: south west north east
    q = f"""
[out:json][timeout:{min(int(timeout_s), 180)}];
(
  node["shop"="car"]({south},{west},{north},{east});
  way["shop"="car"]({south},{west},{north},{east});
  relation["shop"="car"]({south},{west},{north},{east});
  node["amenity"="car_dealer"]({south},{west},{north},{east});
  way["amenity"="car_dealer"]({south},{west},{north},{east});
  relation["amenity"="car_dealer"]({south},{west},{north},{east});
);
out center meta;
"""
    sess = session or requests.Session()
    endpoints: list[str]
    if overpass_urls:
        endpoints = list(overpass_urls)
    else:
        endpoints = overpass_endpoints(overpass_url)

    body: dict[str, Any] | None = None
    for ep in endpoints:
        body = _post_overpass(ep, q, sess=sess, timeout_s=timeout_s)
        if body is not None:
            logger.info("Overpass OK: %s", ep)
            break
    else:
        logger.warning(
            "Overpass: all %d endpoint(s) failed — empty OSM tier (see logs above)",
            len(endpoints),
        )
        return []

    elements = body.get("elements") or []
    out: list[DealerCandidate] = []
    seen: set[str] = set()
    for el in elements:
        c = _parse_osm_element(el)
        if not c or not c.osm_id:
            continue
        if c.osm_id in seen:
            continue
        if c.latitude is None or c.longitude is None:
            continue
        if haversine(lat, lon, c.latitude, c.longitude) > radius_miles:
            continue
        seen.add(c.osm_id)
        out.append(c)
    return out
