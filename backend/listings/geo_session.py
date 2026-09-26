"""Remember last listings ZIP + radius (session) for dashboard recommendations."""

from __future__ import annotations

import math

LISTINGS_GEO_ZIP_SESSION_KEY = "listings_geo_zip"
LISTINGS_GEO_RADIUS_SESSION_KEY = "listings_geo_radius_mi"
LISTINGS_GEO_MAX_RADIUS_MI = 500.0


def _valid_radius(rm: float) -> bool:
    """True for a finite, sane, positive radius (rejects NaN/inf and absurd values)."""
    return math.isfinite(rm) and 0 < rm <= LISTINGS_GEO_MAX_RADIUS_MI


def _scalar(request, key: str) -> str:
    vals = [v.strip() for v in request.args.getlist(key) if v.strip()]
    return vals[-1] if vals else ""


def apply_listings_geo_to_session(session_obj: object, zip_code: str, radius_mi: float) -> bool:
    """Store ZIP + radius when the ZIP geocodes. Returns True if the session was updated."""
    z = (zip_code or "").strip()
    try:
        rm = float(radius_mi)
    except (TypeError, ValueError):
        return False
    if not z or not _valid_radius(rm):
        return False
    from backend.db.geo import zip_to_coords

    if zip_to_coords(z) is None:
        return False
    sess = session_obj
    sess[LISTINGS_GEO_ZIP_SESSION_KEY] = z
    sess[LISTINGS_GEO_RADIUS_SESSION_KEY] = rm
    return True


def persist_listings_geo_from_request(request, session_obj: object) -> None:
    """If query has a geocodable ZIP and a positive radius, store both on the session."""
    zip_code = _scalar(request, "zip_code")
    radius_s = _scalar(request, "radius")
    if not zip_code or not radius_s:
        return
    try:
        radius_mi = float(radius_s)
    except (TypeError, ValueError):
        return
    apply_listings_geo_to_session(session_obj, zip_code, radius_mi)


def listings_geo_kwargs_from_session(session_obj: object) -> dict:
    """Kwargs fragment for ``search_cars`` when session holds a saved listings area."""
    zip_code = session_obj.get(LISTINGS_GEO_ZIP_SESSION_KEY)
    raw_radius = session_obj.get(LISTINGS_GEO_RADIUS_SESSION_KEY)
    if not zip_code or raw_radius is None:
        return {}
    try:
        radius_mi = float(raw_radius)
    except (TypeError, ValueError):
        return {}
    if not _valid_radius(radius_mi):
        return {}
    z = str(zip_code).strip()
    if not z:
        return {}
    return {"zip_code": z, "radius_miles": radius_mi}
