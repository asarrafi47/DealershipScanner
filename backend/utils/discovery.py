"""
Map-based dealership discovery via OpenStreetMap Overpass + optional Dealer.com vetting.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests


def project_root() -> Path:
    """DealershipScanner repo root (parent of /backend)."""
    return Path(__file__).resolve().parent.parent.parent


def write_discovery_manifest(dealers: list[dict[str, Any]]) -> Path:
    """
    Write dealers.discovery.json for scanner.js (Dealer.com entries with dealer_id only).
    """
    manifest: list[dict[str, Any]] = []
    for d in dealers:
        if not d.get("dealer_com") or not d.get("dealer_id"):
            continue
        manifest.append(
            {
                "name": d.get("name") or "Dealer",
                "url": d.get("website") or "",
                "provider": "dealer_dot_com",
                "dealer_id": d.get("dealer_id"),
            }
        )
    path = project_root() / "dealers.discovery.json"
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return path

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
USER_AGENT = "DealershipScanner/1.0 (local inventory discovery; contact via project maintainer)"


def _normalize_base_url(website: str | None) -> str | None:
    if not website or not str(website).strip():
        return None
    w = str(website).strip()
    if not w.startswith(("http://", "https://")):
        w = "https://" + w
    try:
        p = urlparse(w)
        if not p.netloc:
            return None
        return f"{p.scheme}://{p.netloc}".rstrip("/")
    except Exception:
        return None


def geocode_zip(zip_code: str) -> tuple[float, float] | None:
    """Convert US-style zip to lat/lon using Nominatim (geopy)."""
    from geopy.geocoders import Nominatim

    z = (zip_code or "").strip()
    if not z or len(z) < 5:
        return None
    geo = Nominatim(user_agent=USER_AGENT, timeout=12)
    loc = geo.geocode(f"{z}, USA")
    if loc is None:
        return None
    return float(loc.latitude), float(loc.longitude)


def _overpass_query(lat: float, lon: float, radius_m: int) -> str:
    # shop=car and amenity=car_dealer; nodes + ways with center
    return f"""[out:json][timeout:60];
(
  node["shop"="car"](around:{radius_m},{lat},{lon});
  node["amenity"="car_dealer"](around:{radius_m},{lat},{lon});
  way["shop"="car"](around:{radius_m},{lat},{lon});
  way["amenity"="car_dealer"](around:{radius_m},{lat},{lon});
);
out center;
"""


def _element_coords(el: dict[str, Any]) -> tuple[float, float] | None:
    if el.get("type") == "node":
        lat, lon = el.get("lat"), el.get("lon")
        if lat is not None and lon is not None:
            return float(lat), float(lon)
    if el.get("type") in ("way", "relation"):
        c = el.get("center")
        if c and c.get("lat") is not None and c.get("lon") is not None:
            return float(c["lat"]), float(c["lon"])
    return None


def _element_website(tags: dict[str, Any]) -> str | None:
    if not tags:
        return None
    for key in ("website", "contact:website", "url"):
        v = tags.get(key)
        if v and str(v).strip():
            return str(v).strip()
    return None


def _extract_dealer_id_from_json(data: Any) -> str | None:
    if data is None:
        return None
    if isinstance(data, dict):
        for k in ("dealerId", "dealer_id", "clientId", "dealerKey"):
            v = data.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        for v in data.values():
            found = _extract_dealer_id_from_json(v)
            if found:
                return found
    elif isinstance(data, list):
        for item in data:
            found = _extract_dealer_id_from_json(item)
            if found:
                return found
    return None


def _extract_dealer_id_from_text(text: str) -> str | None:
    m = re.search(r'"dealerId"\s*:\s*"([^"]+)"', text, re.I)
    if m:
        return m.group(1).strip()
    m = re.search(r'"dealer_id"\s*:\s*"([^"]+)"', text, re.I)
    if m:
        return m.group(1).strip()
    return None


def vet_dealer_com_website(base_url: str) -> dict[str, Any]:
    """
    Probe Dealer.com inventory API. Returns ok, status_code, dealer_id when parseable.
    """
    base = _normalize_base_url(base_url)
    out: dict[str, Any] = {"ok": False, "base_url": base, "status": None, "dealer_id": None, "error": None}
    if not base:
        out["error"] = "invalid_url"
        return out

    probe = f"{base}/api/widget/ws-inv-data/getInventory?pageNumber=1&pageSize=5"
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    try:
        r = requests.get(probe, timeout=15, headers=headers, allow_redirects=True)
        out["status"] = r.status_code
        if r.status_code == 403:
            out["error"] = "forbidden"
            return out
        if r.status_code not in (200, 404):
            out["error"] = f"http_{r.status_code}"
            return out
        out["ok"] = True
        if r.status_code == 200 and r.text:
            try:
                data = r.json()
            except (json.JSONDecodeError, ValueError):
                data = None
            dealer_id = _extract_dealer_id_from_json(data) if data is not None else None
            if not dealer_id:
                dealer_id = _extract_dealer_id_from_text(r.text)
            out["dealer_id"] = dealer_id
    except requests.RequestException as e:
        out["error"] = str(e)
    return out


def get_dealers_from_map(
    zip_code: str | None = None,
    radius_miles: float = 25.0,
    lat: float | None = None,
    lon: float | None = None,
    check_dealer_com: bool = True,
) -> dict[str, Any]:
    """
    Geofence OSM for car dealerships; optionally vet Dealer.com and resolve dealer_id.

    Provide either (zip_code) or (lat, lon). Zip is ignored if lat/lon are set.
    """
    radius_m = max(500, int(radius_miles * 1609.344))

    if lat is not None and lon is not None:
        coords = (float(lat), float(lon))
    elif zip_code:
        coords = geocode_zip(zip_code)
        if coords is None:
            return {"ok": False, "error": "geocode_failed", "dealers": []}
    else:
        return {"ok": False, "error": "need_zip_or_coords", "dealers": []}

    lat_c, lon_c = coords
    q = _overpass_query(lat_c, lon_c, radius_m)
    try:
        r = requests.post(OVERPASS_URL, data={"data": q}, timeout=90, headers={"User-Agent": USER_AGENT})
        r.raise_for_status()
        payload = r.json()
    except (requests.RequestException, json.JSONDecodeError) as e:
        return {"ok": False, "error": f"overpass: {e}", "dealers": []}

    elements = payload.get("elements") or []
    seen_urls: set[str] = set()
    raw: list[dict[str, Any]] = []

    for el in elements:
        tags = el.get("tags") or {}
        name = (tags.get("name") or tags.get("brand") or "Unknown dealer").strip()
        website = _element_website(tags)
        pos = _element_coords(el)
        if not website:
            continue
        base = _normalize_base_url(website)
        if not base or base in seen_urls:
            continue
        seen_urls.add(base)
        row: dict[str, Any] = {
            "name": name,
            "website": base,
            "lat": pos[0] if pos else None,
            "lon": pos[1] if pos else None,
            "osm_id": el.get("id"),
            "osm_type": el.get("type"),
        }
        if check_dealer_com:
            vet = vet_dealer_com_website(base)
            row["dealer_com"] = vet.get("ok", False)
            row["http_status"] = vet.get("status")
            row["dealer_id"] = vet.get("dealer_id")
            row["vet_error"] = vet.get("error")
        raw.append(row)

    raw.sort(key=lambda x: x.get("name") or "")
    return {
        "ok": True,
        "center": {"lat": lat_c, "lon": lon_c},
        "radius_miles": radius_miles,
        "dealers": raw,
    }
