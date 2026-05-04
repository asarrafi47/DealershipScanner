import math
from typing import Any

import numpy as np
import pgeocode

_nomi = pgeocode.Nominatim("us")


def zip_to_coords(zip_code):
    """Return (lat, lon) for a US zip code, or None if not found."""
    result = _nomi.query_postal_code(str(zip_code).strip())
    if result is None or math.isnan(result.latitude):
        return None
    return (result.latitude, result.longitude)


def us_postal_meta_for_zip(zip_code: str) -> dict[str, Any] | None:
    """
    Canonical USPS ZIP row from pgeocode (state_code, place_name, centroid).

    Prefer this over ``nearest_us_postal_meta`` when the dealer already has a tagged ZIP:
    nearest-neighbor across state lines can snap to the wrong jurisdiction.
    """
    raw = str(zip_code).strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) < 5:
        return None
    z = digits[:5].zfill(5)
    r = _nomi.query_postal_code(z)
    if r is None:
        return None
    try:
        lat = float(r["latitude"])
        if math.isnan(lat):
            return None
    except (TypeError, KeyError, ValueError):
        return None
    pc = _format_zip_cell(r["postal_code"])
    if not pc:
        pc = z
    st = str(r["state_code"] or "").strip().upper()
    if len(st) != 2:
        return None
    place = str(r["place_name"] or "").strip()
    return {"postal_code": pc, "state_code": st, "place_name": place}


def haversine(lat1, lon1, lat2, lon2):
    """Return distance in miles between two lat/lon points."""
    R = 3958.8
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def nearest_us_postal_meta(lat: float, lon: float) -> dict[str, Any] | None:
    """
    Nearest USPS ZIP row from pgeocode's GeoNames-derived table (lat/lon centroids).

    Returns keys: ``postal_code`` (5-digit string), ``state_code`` (2 letters),
    ``place_name`` (city label from dataset; approximate).
    """
    df = _nomi._data
    if df is None or df.empty:
        return None
    df = df.dropna(subset=["latitude", "longitude"])
    if df.empty:
        return None

    lat_i = math.radians(lat)
    lon_i = math.radians(lon)
    lat2 = np.radians(df["latitude"].values.astype(float))
    lon2 = np.radians(df["longitude"].values.astype(float))
    dlat = lat2 - lat_i
    dlon = lon2 - lon_i
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat_i) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    dist_mi = 3958.8 * 2.0 * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))
    idx = int(np.argmin(dist_mi))
    row = df.iloc[idx]

    raw_pc = row.get("postal_code")
    pc = _format_zip_cell(raw_pc)
    st = str(row.get("state_code") or "").strip().upper()
    if len(st) != 2:
        st = ""
    place = str(row.get("place_name") or "").strip()
    if not pc:
        return None
    return {"postal_code": pc, "state_code": st, "place_name": place}


def _format_zip_cell(raw) -> str:
    if raw is None:
        return ""
    try:
        if isinstance(raw, float) and math.isnan(raw):
            return ""
    except TypeError:
        pass
    s = str(raw).strip()
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 5:
        return digits[:5].zfill(5)
    if len(digits) > 0:
        return digits.zfill(5)[:5]
    return ""
