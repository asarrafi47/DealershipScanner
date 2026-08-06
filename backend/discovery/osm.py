"""
OpenStreetMap dealership POIs via Overpass API.

Tags queried:
  - ``shop=car`` (documented primary tag for car dealerships)
  - ``amenity=car_dealer`` (sometimes used)

Respects Overpass usage: bounded queries, modest timeout, descriptive User-Agent,
and slot-aware pacing (``/api/status``) with backoff instead of hammering on error.

Large areas should go through :func:`fetch_osm_dealerships_tiled`, which splits the
circle into smaller bounding boxes. A single 40+ mile bbox over a dense metro
reliably times out (HTTP 504) on the public instances.
"""
from __future__ import annotations

import logging
import math
import os
import random
import re
import time
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
_DEFAULT_USER_AGENT = (
    "DealershipScanner/1.0 (car-dealership POI discovery; batched bbox queries; "
    "contact via DISCOVERY_OVERPASS_USER_AGENT)"
)


def overpass_user_agent() -> str:
    """
    Descriptive User-Agent, overridable with ``DISCOVERY_OVERPASS_USER_AGENT``.

    Overpass etiquette asks for a real contact. Operators running this at volume
    should set the env var to something that includes a reachable address.
    """
    return (os.environ.get("DISCOVERY_OVERPASS_USER_AGENT") or "").strip() or _DEFAULT_USER_AGENT


# Kept as a module attribute for callers that imported it before the env override existed.
USER_AGENT = _DEFAULT_USER_AGENT

# Status codes worth retrying: gateway overload, rate limit, and the 406 the public
# instance returns when a query is refused under load.
_RETRYABLE_STATUS = frozenset({429, 406, 502, 503, 504})
_SLOT_RE = re.compile(r"Slot available after:.*?in (\d+) seconds", re.IGNORECASE)
_SLOTS_AVAILABLE_RE = re.compile(r"(\d+)\s+slots? available now", re.IGNORECASE)


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


def _status_url_for(overpass_url: str) -> str:
    return overpass_url.rstrip("/").removesuffix("/interpreter") + "/status"


def wait_for_overpass_slot(
    overpass_url: str,
    *,
    sess: requests.Session,
    max_wait_s: float = 90.0,
) -> None:
    """
    Block until the endpoint reports a free execution slot (bounded by *max_wait_s*).

    ``/api/status`` is the documented way to pace against an Overpass instance
    instead of discovering the limit by getting rejected. Any failure to read the
    status page is non-fatal — we simply proceed and let the retry path handle it.
    """
    try:
        r = sess.get(
            _status_url_for(overpass_url),
            headers={"User-Agent": overpass_user_agent()},
            timeout=20,
        )
        if r.status_code != 200:
            return
        text = r.text
    except requests.RequestException:
        return

    m = _SLOTS_AVAILABLE_RE.search(text)
    if m and int(m.group(1)) > 0:
        return
    waits = [int(x) for x in _SLOT_RE.findall(text)]
    if not waits:
        return
    delay = min(max(min(waits), 0) + 1, max_wait_s)
    if delay > 0:
        logger.info("Overpass %s: no free slot, waiting %.0fs", overpass_url, delay)
        time.sleep(delay)


def _post_overpass(
    overpass_url: str,
    query: str,
    *,
    sess: requests.Session,
    timeout_s: float,
    attempts: int = 3,
    respect_slots: bool = True,
) -> dict[str, Any] | None:
    """
    POST *query*, retrying the SAME endpoint with backoff on retryable statuses.

    Returns None when every attempt failed, so the caller can move to the next mirror.
    """
    headers = {"User-Agent": overpass_user_agent(), "Content-Type": "text/plain; charset=utf-8"}
    for attempt in range(1, max(1, attempts) + 1):
        if respect_slots:
            wait_for_overpass_slot(overpass_url, sess=sess)
        try:
            r = sess.post(
                overpass_url,
                data=query.encode("utf-8"),
                headers=headers,
                timeout=timeout_s,
            )
            if r.status_code in _RETRYABLE_STATUS:
                logger.warning(
                    "Overpass HTTP %s from %s (attempt %d/%d)",
                    r.status_code,
                    overpass_url,
                    attempt,
                    attempts,
                )
                if attempt < attempts:
                    time.sleep(min(60.0, (2 ** attempt) * 5.0) + random.uniform(0, 3))
                    continue
                return None
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            logger.warning(
                "Overpass request failed (%s, attempt %d/%d): %s", overpass_url, attempt, attempts, e
            )
            if attempt < attempts:
                time.sleep(min(60.0, (2 ** attempt) * 5.0) + random.uniform(0, 3))
                continue
            return None
        except ValueError as e:
            logger.warning("Overpass JSON parse failed (%s): %s", overpass_url, e)
            return None
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
    attempts: int = 3,
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
        body = _post_overpass(ep, q, sess=sess, timeout_s=timeout_s, attempts=attempts)
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


def tile_circle(
    lat: float,
    lon: float,
    radius_miles: float,
    tile_miles: float,
) -> list[tuple[float, float, float]]:
    """
    Cover the circle with a grid of smaller (lat, lon, radius) sub-queries.

    Each returned radius is the half-diagonal of its tile, so the union of the
    sub-circles fully covers the tile grid, which in turn covers the circle.
    Tiles whose centre is farther than ``radius + tile`` from the origin are dropped.
    """
    if tile_miles <= 0 or radius_miles <= tile_miles:
        return [(lat, lon, radius_miles)]

    step_lat = tile_miles / 69.172
    cos_lat = max(abs(math.cos(math.radians(lat))), 0.2)
    step_lon = tile_miles / (69.172 * cos_lat)
    n = int(math.ceil(radius_miles / tile_miles))
    sub_radius = tile_miles * math.sqrt(2.0) / 2.0

    out: list[tuple[float, float, float]] = []
    for i in range(-n, n + 1):
        for j in range(-n, n + 1):
            clat = lat + i * step_lat
            clon = lon + j * step_lon
            # Approximate distance from the metro centre to this tile centre.
            dy = (clat - lat) * 69.172
            dx = (clon - lon) * 69.172 * cos_lat
            if math.hypot(dx, dy) > radius_miles + tile_miles:
                continue
            out.append((clat, clon, sub_radius))
    return out


def fetch_osm_dealerships_tiled(
    lat: float,
    lon: float,
    radius_miles: float,
    *,
    tile_miles: float = 12.0,
    overpass_url: str | None = None,
    overpass_urls: Sequence[str] | None = None,
    timeout_s: float = 120.0,
    session: requests.Session | None = None,
    sleep_between_s: float = 2.0,
    attempts: int = 3,
) -> tuple[list[DealerCandidate], dict[str, int]]:
    """
    Same result set as :func:`fetch_osm_dealerships` but split into ``tile_miles``
    sub-queries so no single Overpass request covers a large dense metro.

    Returns ``(candidates, stats)`` where ``stats`` counts tiles attempted and tiles
    that came back empty-or-failed — the caller needs that to know whether a low
    count means "few dealers" or "the source did not answer".
    """
    from backend.db.geo import haversine

    sess = session or requests.Session()
    tiles = tile_circle(lat, lon, radius_miles, tile_miles)
    merged: dict[str, DealerCandidate] = {}
    stats = {"tiles": len(tiles), "tiles_empty": 0}

    for idx, (tlat, tlon, trad) in enumerate(tiles):
        found = fetch_osm_dealerships(
            tlat,
            tlon,
            trad,
            overpass_url=overpass_url,
            overpass_urls=overpass_urls,
            timeout_s=timeout_s,
            session=sess,
            attempts=attempts,
        )
        if not found:
            stats["tiles_empty"] += 1
        for c in found:
            if not c.osm_id or c.osm_id in merged:
                continue
            if c.latitude is None or c.longitude is None:
                continue
            # Clip back to the requested circle (tiles overshoot at the edges).
            if haversine(lat, lon, c.latitude, c.longitude) > radius_miles:
                continue
            merged[c.osm_id] = c
        if idx < len(tiles) - 1 and sleep_between_s > 0:
            time.sleep(sleep_between_s)

    return list(merged.values()), stats
