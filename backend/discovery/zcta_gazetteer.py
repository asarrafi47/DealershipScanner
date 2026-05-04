"""
Load Census-style ZCTA gazetteer pipe-delimited tables for internal-point centroids.

Expected columns (header row):
  GEOID|GEOIDFQ|ALAND|AWATER|ALAND_SQMI|AWATER_SQMI|INTPTLAT|INTPTLONG

``GEOID`` is the 5-digit ZCTA code; ``INTPTLAT`` / ``INTPTLONG`` are the centroid.
"""
from __future__ import annotations

import csv
import math
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from backend.discovery.normalize import normalize_zip

__all__ = [
    "ZctaGazetteerRow",
    "characteristic_radius_miles",
    "default_gazetteer_file",
    "iter_zcta_zip_codes",
    "lookup_zcta_row",
    "resolve_zip_center",
    "suggested_search_radius_miles",
]


@dataclass(frozen=True)
class ZctaGazetteerRow:
    geoid: str
    lat: float
    lon: float
    aland_sqmi: float | None


def default_gazetteer_file(project_root: Path) -> Path | None:
    """
    Prefer ``DISCOVERY_ZCTA_GAZETTEER`` when set; otherwise the ``backend/ZIPs/*.txt``
    file that looks most like a ZCTA gazetteer (name hints + largest file).
    """
    env = (os.environ.get("DISCOVERY_ZCTA_GAZETTEER") or "").strip()
    if env:
        p = Path(env).expanduser()
        return p if p.is_file() else None

    zdir = project_root / "backend" / "ZIPs"
    if not zdir.is_dir():
        return None
    files = sorted(zdir.glob("*.txt"))
    if not files:
        return None

    def score(p: Path) -> tuple[int, float, str]:
        name = p.name.lower()
        s = 0
        if "zcta" in name:
            s += 4
        if "gaz" in name:
            s += 2
        try:
            sz = float(p.stat().st_size)
        except OSError:
            sz = 0.0
        return (s, sz, p.name.lower())

    return max(files, key=score)


def _parse_float(cell: str) -> float | None:
    t = (cell or "").strip()
    if not t or t == ".":
        return None
    try:
        return float(t)
    except ValueError:
        return None


@lru_cache(maxsize=4)
def _load_gazetteer_rows(path_str: str) -> dict[str, ZctaGazetteerRow]:
    path = Path(path_str)
    out: dict[str, ZctaGazetteerRow] = {}
    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f, delimiter="|")
        for i, row in enumerate(reader):
            if i == 0 and row and (row[0] or "").strip().upper() == "GEOID":
                continue
            if len(row) < 8:
                continue
            geoid_raw = (row[0] or "").strip()
            z = normalize_zip(geoid_raw)
            if not z:
                continue
            lat = _parse_float(row[6])
            lon = _parse_float(row[7])
            if lat is None or lon is None or math.isnan(lat) or math.isnan(lon):
                continue
            aland = _parse_float(row[4])
            out[z] = ZctaGazetteerRow(geoid=z, lat=lat, lon=lon, aland_sqmi=aland)
    return out


def lookup_zcta_row(path: Path, zip_code: str) -> ZctaGazetteerRow | None:
    z = normalize_zip(zip_code)
    if not z:
        return None
    rows = _load_gazetteer_rows(str(path.resolve()))
    return rows.get(z)


def iter_zcta_zip_codes(path: Path) -> list[str]:
    """Return sorted 5-digit ZCTA codes present in *path*."""
    rows = _load_gazetteer_rows(str(path.resolve()))
    return sorted(rows.keys())


def characteristic_radius_miles(row: ZctaGazetteerRow | None) -> float | None:
    """
    Radius in miles of a circle with the same land area as the ZCTA (``ALAND_SQMI``).

    Useful as an approximate search radius from the internal point; irregular shapes
    may extend farther from the centroid than this.
    """
    if row is None or row.aland_sqmi is None or row.aland_sqmi <= 0:
        return None
    return math.sqrt(float(row.aland_sqmi) / math.pi)


def suggested_search_radius_miles(
    row: ZctaGazetteerRow | None,
    *,
    fallback: float = 15.0,
    lo: float = 5.0,
    hi: float = 45.0,
) -> float:
    """
    Miles to search around the ZCTA internal point: area-derived circle radius,
    slightly inflated and clamped (irregular shapes may need ``--radius`` override).
    """
    r = characteristic_radius_miles(row)
    if r is None:
        return fallback
    inflated = r * 1.12
    return max(lo, min(hi, inflated))


def resolve_zip_center(
    zip_code: str,
    *,
    project_root: Path | None,
    gazetteer_path: Path | None,
) -> tuple[float, float, str]:
    """
    Return ``(lat, lon, source)`` where source is ``zcta_gazetteer`` or ``pgeocode``.

    Prefers the gazetteer when a path resolves and the ZIP exists in the file;
    otherwise falls back to ``zip_to_coords``.
    """
    from backend.db.geo import zip_to_coords

    z = normalize_zip(zip_code)
    if not z:
        raise ValueError("zip_code must be a 5-digit US ZIP")

    path = gazetteer_path
    if path is None and project_root is not None:
        path = default_gazetteer_file(project_root)

    if path is not None and path.is_file():
        row = lookup_zcta_row(path, z)
        if row is not None:
            return (row.lat, row.lon, "zcta_gazetteer")

    coords = zip_to_coords(z)
    if not coords:
        raise ValueError(f"unknown or unsupported ZIP: {zip_code!r}")
    return (coords[0], coords[1], "pgeocode")
