"""
Google Places API (New) — primary dealership discovery tier.

Uses `searchNearby` with `includedTypes=["car_dealer"]`.
The API caps each circle at 50 km / 20 results, so large radii are covered
by tiling multiple overlapping circles around the seed point.

Returns DealerCandidate objects with name, lat/lon, address, zip, website.
"""
from __future__ import annotations

import logging
import math
import os
import re
from typing import Any
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

import requests

from backend.discovery.candidate import DealerCandidate
from backend.discovery.google_place_rating import rating_from_place_dict
from backend.discovery.normalize import normalize_url, normalize_us_state_to_code, normalize_zip

logger = logging.getLogger(__name__)

NEARBY_URL = "https://places.googleapis.com/v1/places:searchNearby"
FIELD_MASK = (
    "places.id,places.displayName,places.websiteUri,places.formattedAddress,"
    "places.addressComponents,places.location,places.rating,places.userRatingCount"
)

# searchNearby hard cap per Google docs
_MAX_RADIUS_M = 50_000.0
_MAX_RESULTS = 20

# ── Billing guardrail ────────────────────────────────────────────────────────
# Each searchNearby is a BILLABLE Places API call. This process-wide budget is a
# hard fail-safe so a runaway/nationwide sweep can't silently rack up charges:
# once GOOGLE_PLACES_MAX_CALLS is reached, further calls are skipped (return no
# results) and a loud warning is logged. Default 0 = unlimited (preserves
# single-city behavior); nationwide entry points (national_scan.py) set a
# conservative cap. This is a code-side backstop — also set a per-day quota on
# the Places API in the Google Cloud console for a Google-enforced hard limit.
_call_count = 0
_budget_warned = False


def _call_budget() -> int:
    try:
        return max(0, int(os.environ.get("GOOGLE_PLACES_MAX_CALLS", "0")))
    except ValueError:
        return 0


def reset_places_call_count() -> None:
    global _call_count, _budget_warned
    _call_count = 0
    _budget_warned = False


def places_call_count() -> int:
    return _call_count


def _budget_exhausted() -> bool:
    global _budget_warned
    budget = _call_budget()
    if budget and _call_count >= budget:
        if not _budget_warned:
            logger.warning(
                "Google Places call budget reached (GOOGLE_PLACES_MAX_CALLS=%d, made=%d) — "
                "skipping further searchNearby calls to avoid billing. Raise the env var to continue.",
                budget, _call_count,
            )
            _budget_warned = True
        return True
    return False

# UTM / tracking params to strip from dealer website URLs
_STRIP_PARAMS = frozenset({
    "utm_source", "utm_medium", "utm_campaign", "utm_content",
    "utm_term", "utm_id", "gclid", "fbclid", "msclkid",
})


def _strip_tracking(url: str) -> str:
    """Remove UTM / ad-tracking query params from a URL."""
    try:
        p = urlparse(url)
        qs = {k: v for k, v in parse_qs(p.query, keep_blank_values=True).items()
              if k.lower() not in _STRIP_PARAMS}
        clean_query = urlencode({k: v[0] for k, v in qs.items()})
        return urlunparse(p._replace(query=clean_query))
    except Exception:
        return url


def _offset_point(lat: float, lon: float, bearing_deg: float, distance_miles: float) -> tuple[float, float]:
    """Return a point *distance_miles* away in *bearing_deg* direction."""
    R = 3958.8  # earth radius in miles
    d = distance_miles / R
    b = math.radians(bearing_deg)
    lat1 = math.radians(lat)
    lon1 = math.radians(lon)
    lat2 = math.asin(math.sin(lat1) * math.cos(d) + math.cos(lat1) * math.sin(d) * math.cos(b))
    lon2 = lon1 + math.atan2(
        math.sin(b) * math.sin(d) * math.cos(lat1),
        math.cos(d) - math.sin(lat1) * math.sin(lat2),
    )
    return math.degrees(lat2), math.degrees(lon2)


def _tile_circles(lat: float, lon: float, radius_miles: float) -> list[tuple[float, float, float]]:
    """
    Return (lat, lon, radius_m) circles that collectively cover *radius_miles*.

    Strategy: center circle + ring of 8 offset circles at 60% of radius.
    Each circle uses the 50 km API cap. Overlapping is intentional — dedup by place ID.
    """
    cap_miles = _MAX_RADIUS_M / 1609.34  # ~31 miles

    # If radius fits in one circle, just use it
    if radius_miles <= cap_miles:
        return [(lat, lon, min(radius_miles * 1609.34, _MAX_RADIUS_M))]

    circles: list[tuple[float, float, float]] = [(lat, lon, _MAX_RADIUS_M)]
    offset_dist = radius_miles * 0.60
    for bearing in range(0, 360, 45):
        olat, olon = _offset_point(lat, lon, bearing, offset_dist)
        circles.append((olat, olon, _MAX_RADIUS_M))
    return circles


def _parse_address_components(components: list[dict]) -> tuple[str, str, str]:
    """Return (city, state_code, zip) from Google addressComponents list."""
    city = state = zip_code = ""
    for comp in components:
        types = comp.get("types") or []
        if "locality" in types:
            city = comp.get("longText") or comp.get("shortText") or ""
        elif "administrative_area_level_1" in types:
            state = comp.get("shortText") or ""
        elif "postal_code" in types:
            zip_code = comp.get("longText") or comp.get("shortText") or ""
    return city.strip(), state.strip(), zip_code.strip()


def _street_from_formatted(formatted: str, city: str, state: str, zip_code: str) -> str:
    """
    Best-effort street: strip city / state / zip / country suffix from formattedAddress.
    e.g. "123 Main St, Charlotte, NC 28217, USA" -> "123 Main St"
    """
    addr = formatted.strip()
    for suffix in (f", {city}, {state} {zip_code}, USA", f", {city}, {state}, USA",
                   f", {state} {zip_code}, USA", ", USA"):
        if suffix and addr.endswith(suffix):
            addr = addr[: -len(suffix)]
            break
    return addr.strip()


def _parse_place(place: dict[str, Any]) -> DealerCandidate | None:
    name = (place.get("displayName") or {}).get("text") or ""
    name = name.strip()
    if not name:
        return None

    loc = place.get("location") or {}
    lat = loc.get("latitude")
    lon = loc.get("longitude")
    if lat is None or lon is None:
        return None

    components = place.get("addressComponents") or []
    city, state_raw, zip_raw = _parse_address_components(components)
    state = normalize_us_state_to_code(state_raw) or state_raw
    zip_code = normalize_zip(zip_raw) or zip_raw
    formatted = (place.get("formattedAddress") or "").strip()
    street = _street_from_formatted(formatted, city, state_raw, zip_raw)

    raw_url = (place.get("websiteUri") or "").strip()
    clean_url = ""
    if raw_url:
        stripped = _strip_tracking(raw_url)
        nu = normalize_url(stripped)
        if nu:
            clean_url = nu

    rating_fields = rating_from_place_dict(place)

    return DealerCandidate(
        name=name,
        city=city,
        state=state,
        street_address=street,
        zip_code=zip_code,
        latitude=float(lat),
        longitude=float(lon),
        dealer_website_url=clean_url,
        website_url=clean_url,
        source_web=True,
        google_place_id=rating_fields.place_id if rating_fields else None,
        google_rating=rating_fields.rating if rating_fields else None,
        google_review_count=rating_fields.review_count if rating_fields else None,
    )


def _query_nearby(
    lat: float,
    lon: float,
    radius_m: float,
    *,
    api_key: str,
    session: requests.Session,
    timeout_s: float = 20.0,
) -> list[dict]:
    headers = {
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": FIELD_MASK,
        "Content-Type": "application/json",
    }
    body = {
        "includedTypes": ["car_dealer"],
        "locationRestriction": {
            "circle": {
                "center": {"latitude": lat, "longitude": lon},
                "radius": float(min(radius_m, _MAX_RADIUS_M)),
            }
        },
        "maxResultCount": _MAX_RESULTS,
    }
    global _call_count
    if _budget_exhausted():
        return []
    _call_count += 1
    try:
        r = session.post(NEARBY_URL, headers=headers, json=body, timeout=timeout_s)
        r.raise_for_status()
        return r.json().get("places") or []
    except (requests.RequestException, ValueError) as e:
        logger.warning("Google Places searchNearby failed: %s", e)
        return []


def fetch_google_places_dealerships(
    lat: float,
    lon: float,
    radius_miles: float,
    *,
    api_key: str | None = None,
    session: requests.Session | None = None,
    timeout_s: float = 20.0,
) -> list[DealerCandidate]:
    """
    Discover car dealerships within *radius_miles* using Google Places searchNearby.

    Tiles multiple 50 km circles to cover large radii. Deduplicates by place ID.
    Falls back to env GOOGLE_MAPS_API_KEY when *api_key* not passed.
    """
    from backend.db.geo import haversine

    key = api_key or (os.environ.get("GOOGLE_MAPS_API_KEY") or "").strip()
    if not key:
        logger.warning("Google Places discovery skipped — no GOOGLE_MAPS_API_KEY")
        return []

    sess = session or requests.Session()
    circles = _tile_circles(lat, lon, radius_miles)
    logger.info("Google Places discovery: %d circle(s) for %.0f mi radius", len(circles), radius_miles)

    seen_ids: set[str] = set()
    raw_places: list[dict] = []

    for clat, clon, r_m in circles:
        places = _query_nearby(clat, clon, r_m, api_key=key, session=sess, timeout_s=timeout_s)
        for p in places:
            pid = p.get("id") or ""
            if pid and pid in seen_ids:
                continue
            if pid:
                seen_ids.add(pid)
            raw_places.append(p)

    out: list[DealerCandidate] = []
    for p in raw_places:
        c = _parse_place(p)
        if not c or c.latitude is None or c.longitude is None:
            continue
        if haversine(lat, lon, c.latitude, c.longitude) > radius_miles:
            continue
        out.append(c)

    logger.info("Google Places discovery: %d unique dealers in radius (from %d raw)", len(out), len(raw_places))
    return out
