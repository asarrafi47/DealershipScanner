"""
Regional market averages: make / model / trim + model year + mileage band.

When ``zip_code`` and ``radius_miles`` are set, only listings within that radius
(from the ZIP centroid) contribute to the average. Falls back to wider cohorts when
sample count is too low.
"""
from __future__ import annotations

import os
from collections import defaultdict
from pathlib import Path
from typing import Any

from backend.db.inventory_db import db_conn, get_conn

_MIN_LISTINGS_FOR_AVG = max(2, int(os.environ.get("MARKET_PRICE_MIN_SAMPLES", "3")))
_YEAR_WINDOW = max(0, int(os.environ.get("MARKET_PRICE_YEAR_WINDOW", "1")))
_DEFAULT_RADIUS_MI = float(os.environ.get("MARKET_PRICE_DEFAULT_RADIUS_MI", "50"))

# Cache: (db_mtime, zip, radius) -> CohortIndex snapshot
_cohort_cache: dict[tuple[float, str, float], "CohortIndex"] = {}


def _inventory_db_mtime() -> float:
    try:
        from backend.db.inventory_db import DB_PATH

        p = Path(DB_PATH)
        if p.is_file():
            return p.stat().st_mtime
    except Exception:
        pass
    return 0.0


def _trim_key(make: str | None, model: str | None, trim: str | None) -> tuple[str, str, str]:
    return (
        str(make or "").strip().lower(),
        str(model or "").strip().lower(),
        str(trim or "").strip().lower(),
    )


def mileage_band(mileage: Any) -> str:
    """Bucket mileage for cohort matching (used + low-mile new)."""
    try:
        m = int(float(mileage))
    except (TypeError, ValueError):
        return "unknown"
    if m < 0:
        return "unknown"
    if m <= 25000:
        return "0-25k"
    if m <= 50000:
        return "25-50k"
    if m <= 75000:
        return "50-75k"
    if m <= 100000:
        return "75-100k"
    return "100k+"


def _parse_year(year: Any) -> int | None:
    try:
        y = int(year)
        return y if 1900 <= y <= 2100 else None
    except (TypeError, ValueError):
        return None


def cohort_key(
    make: str | None,
    model: str | None,
    trim: str | None,
    year: int | None,
    mb: str,
) -> str:
    mk, md, tr = _trim_key(make, model, trim)
    ys = str(year) if year is not None else "*"
    return f"{mk}|{md}|{tr}|{ys}|{mb}"


def _load_dealer_geo() -> dict[str, tuple[float, float]]:
    out: dict[str, tuple[float, float]] = {}
    conn = get_conn()
    cur = conn.cursor()
    try:
        for url, lat, lon in cur.execute(
            "SELECT dealer_url, lat, lon FROM dealer_geopoints "
            "WHERE lat IS NOT NULL AND lon IS NOT NULL"
        ).fetchall():
            if url:
                out[str(url).strip()] = (float(lat), float(lon))
    except Exception:
        pass
    if not out:
        try:
            for url, lat, lon in cur.execute(
                "SELECT website_url, latitude, longitude FROM dealerships "
                "WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
            ).fetchall():
                if url:
                    out[str(url).strip()] = (float(lat), float(lon))
        except Exception:
            pass
    conn.close()
    return out


def _car_coords(
    car: dict[str, Any],
    dealer_geo: dict[str, tuple[float, float]],
) -> tuple[float, float] | None:
    from backend.db.geo import zip_to_coords

    zc = str(car.get("zip_code") or "").strip()
    if zc:
        coords = zip_to_coords(zc)
        if coords:
            return (float(coords[0]), float(coords[1]))
    du = str(car.get("dealer_url") or "").strip()
    if du and du in dealer_geo:
        return dealer_geo[du]
    return None


def _filter_cars_by_geo(
    cars: list[dict[str, Any]],
    *,
    zip_code: str | None,
    radius_miles: float | None,
) -> tuple[list[dict[str, Any]], str]:
    """
    Return (cars in region, human label for UI).

    When geo is omitted, uses all active inventory (``geo_label`` = nationwide).
    """
    z = (zip_code or "").strip()
    try:
        radius = float(radius_miles) if radius_miles is not None else 0.0
    except (TypeError, ValueError):
        radius = 0.0

    if not z or radius <= 0:
        return cars, "nationwide inventory"

    from backend.db.geo import haversine, zip_to_coords

    origin = zip_to_coords(z)
    if origin is None:
        return cars, "nationwide inventory"

    dealer_geo = _load_dealer_geo()
    kept: list[dict[str, Any]] = []
    for car in cars:
        dest = _car_coords(car, dealer_geo)
        if not dest:
            continue
        dist = haversine(origin[0], origin[1], dest[0], dest[1])
        if dist <= radius:
            kept.append(car)

    label = f"within {radius:g} mi of {z}"
    return kept, label


class CohortIndex:
    """Price lists grouped by (make, model, trim, year, mileage_band)."""

    __slots__ = ("groups", "geo_label", "region_count")

    def __init__(
        self,
        groups: dict[tuple[str, str, str, int | None, str], list[float]],
        *,
        geo_label: str,
        region_count: int,
    ) -> None:
        self.groups = groups
        self.geo_label = geo_label
        self.region_count = region_count

    def _prices(
        self,
        mk: str,
        md: str,
        tr: str,
        *,
        year: int | None,
        year_window: int,
        mileage_band: str | None,
        any_mileage: bool,
    ) -> list[float]:
        prices: list[float] = []
        years: list[int | None]
        if year is not None:
            years = list(range(year - year_window, year + year_window + 1))
        else:
            years = [None]

        mbs: list[str]
        if any_mileage or not mileage_band:
            mbs = sorted({k[4] for k in self.groups if k[0] == mk and k[1] == md and k[2] == tr})
        else:
            mbs = [mileage_band]

        for y in years:
            for mb in mbs:
                if y is None:
                    for key, vals in self.groups.items():
                        if key[0] == mk and key[1] == md and key[2] == tr and key[4] == mb:
                            prices.extend(vals)
                else:
                    prices.extend(self.groups.get((mk, md, tr, y, mb), []))
        return prices

    def lookup(
        self,
        make: str | None,
        model: str | None,
        trim: str | None,
        year: Any,
        mileage: Any,
    ) -> tuple[dict[str, Any] | None, str]:
        """Return (stats dict, cohort description) using fallback tiers."""
        mk, md, tr = _trim_key(make, model, trim)
        if not mk or not md:
            return None, ""

        y = _parse_year(year)
        mb = mileage_band(mileage)

        attempts: list[tuple[str, int, str | None, bool]] = [
            ("same year & mileage band", 0, mb if mb != "unknown" else None, False),
            (f"year ±{_YEAR_WINDOW} & same mileage band", _YEAR_WINDOW, mb if mb != "unknown" else None, False),
            ("same year, any mileage", 0, None, True),
            (f"year ±{_YEAR_WINDOW}, any mileage", _YEAR_WINDOW, None, True),
        ]
        if mb != "unknown":
            attempts.append((f"any year, {mb} mileage band", 0, mb, False))
        attempts.append(("same trim (any year / mileage)", 0, None, True))

        for label, ywin, mb_sel, any_mi in attempts:
            if y is None and ywin == 0 and "same year" in label:
                continue
            prices = self._prices(
                mk,
                md,
                tr,
                year=y,
                year_window=ywin,
                mileage_band=mb_sel,
                any_mileage=any_mi,
            )
            if len(prices) < _MIN_LISTINGS_FOR_AVG:
                continue
            avg = sum(prices) / len(prices)
            stats = {
                "avg_price": round(avg, 2),
                "min_price": round(min(prices), 2),
                "max_price": round(max(prices), 2),
                "count": len(prices),
            }
            desc = _cohort_description(make, model, trim, year, mileage, label, self.geo_label)
            return stats, desc

        return None, ""

    def exact_cohort_map(self) -> dict[str, dict[str, Any]]:
        """Exact (year + mileage band) cohorts for client-side grid enrichment."""
        out: dict[str, dict[str, Any]] = {}
        for (mk, md, tr, y, mb), prices in self.groups.items():
            if y is None or len(prices) < _MIN_LISTINGS_FOR_AVG:
                continue
            avg = sum(prices) / len(prices)
            key = cohort_key(mk, md, tr, y, mb)
            out[key] = {
                "avg_price": round(avg, 2),
                "avg_price_display": f"${avg:,.0f}",
                "sample_count": len(prices),
            }
        return out


def _load_active_listing_rows() -> list[dict[str, Any]]:
    import sqlite3

    with db_conn(row_factory=sqlite3.Row) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT make, model, trim, year, mileage, price, zip_code, dealer_url
            FROM cars
            WHERE (COALESCE(listing_active, 1) = 1)
              AND price IS NOT NULL
              AND CAST(price AS REAL) > 0
              AND TRIM(IFNULL(make, '')) != ''
              AND TRIM(IFNULL(model, '')) != ''
            """
        )
        return [dict(r) for r in cur.fetchall()]


def get_cohort_index(
    *,
    zip_code: str | None = None,
    radius_miles: float | None = None,
) -> CohortIndex:
    mtime = _inventory_db_mtime()
    z = (zip_code or "").strip()
    try:
        r = float(radius_miles) if radius_miles is not None else 0.0
    except (TypeError, ValueError):
        r = 0.0
    cache_key = (mtime, z, round(r, 2))
    cached = _cohort_cache.get(cache_key)
    if cached is not None:
        return cached

    all_rows = _load_active_listing_rows()
    regional, geo_label = _filter_cars_by_geo(all_rows, zip_code=z, radius_miles=r if z else None)

    groups: dict[tuple[str, str, str, int | None, str], list[float]] = defaultdict(list)
    for row in regional:
        mk, md, tr = _trim_key(row.get("make"), row.get("model"), row.get("trim"))
        if not mk or not md:
            continue
        y = _parse_year(row.get("year"))
        mb = mileage_band(row.get("mileage"))
        try:
            price = float(row.get("price") or 0)
        except (TypeError, ValueError):
            continue
        if price <= 0:
            continue
        groups[(mk, md, tr, y, mb)].append(price)

    idx = CohortIndex(dict(groups), geo_label=geo_label, region_count=len(regional))
    _cohort_cache[cache_key] = idx
    return idx


def _cohort_description(
    make: str | None,
    model: str | None,
    trim: str | None,
    year: Any,
    mileage: Any,
    tier_label: str,
    geo_label: str,
) -> str:
    parts = [str(make or "").strip(), str(model or "").strip()]
    tr = str(trim or "").strip()
    if tr:
        parts.append(tr)
    y = _parse_year(year)
    if y:
        parts.append(str(y))
    mb = mileage_band(mileage)
    if mb != "unknown":
        parts.append(f"{mb} mi")
    base = " ".join(p for p in parts if p) or "This vehicle"
    return f"{base} ({tier_label}; {geo_label})"


def _vs_market_from_delta(delta_pct: float) -> str:
    if delta_pct <= -3:
        return "below_market"
    if delta_pct >= 3:
        return "above_market"
    return "near_market"


def market_price_for_car(
    car: dict[str, Any],
    *,
    zip_code: str | None = None,
    radius_miles: float | None = None,
) -> dict[str, Any] | None:
    """Compare listing price to regional cohort average with year/mileage fallbacks."""
    try:
        price = float(car.get("price") or 0)
    except (TypeError, ValueError):
        return None
    if price <= 0:
        return None

    z = (zip_code or "").strip() or None
    radius = radius_miles
    if z and (radius is None or float(radius) <= 0):
        radius = _DEFAULT_RADIUS_MI

    idx = get_cohort_index(zip_code=z, radius_miles=radius)
    stats, cohort_label = idx.lookup(
        car.get("make"),
        car.get("model"),
        car.get("trim"),
        car.get("year"),
        car.get("mileage"),
    )
    if not stats:
        return None

    avg = float(stats["avg_price"])
    if avg <= 0:
        return None

    delta = price - avg
    delta_pct = round((delta / avg) * 100.0, 1)
    vs = _vs_market_from_delta(delta_pct)
    labels = {
        "below_market": "Below regional market average for this cohort",
        "near_market": "Near regional market average for this cohort",
        "above_market": "Above regional market average for this cohort",
    }

    return {
        "avg_price": avg,
        "avg_price_display": f"${avg:,.0f}",
        "listing_price": price,
        "listing_price_display": f"${price:,.0f}",
        "sample_count": int(stats["count"]),
        "min_price": stats.get("min_price"),
        "max_price": stats.get("max_price"),
        "delta_dollars": round(delta, 2),
        "delta_pct": delta_pct,
        "vs_market": vs,
        "vs_market_label": labels.get(vs, ""),
        "cohort_label": cohort_label,
        "geo_label": idx.geo_label,
        "mileage_band": mileage_band(car.get("mileage")),
    }


def trim_price_stats_for_client(
    *,
    zip_code: str | None = None,
    radius_miles: float | None = None,
) -> dict[str, Any]:
    """
    Payload for listings grid: exact cohort keys plus geo metadata.

    Client should use the same fallback key order as ``cohort_key_attempts``.
    """
    z = (zip_code or "").strip() or None
    radius = radius_miles
    if z and (radius is None or float(radius) <= 0):
        radius = _DEFAULT_RADIUS_MI

    idx = get_cohort_index(zip_code=z, radius_miles=radius)
    return {
        "geo_label": idx.geo_label,
        "region_listing_count": idx.region_count,
        "cohorts": idx.exact_cohort_map(),
        "year_window": _YEAR_WINDOW,
        "min_samples": _MIN_LISTINGS_FOR_AVG,
    }


def cohort_key_attempts(car: dict[str, Any]) -> list[str]:
    """
    Cohort keys to try on the client (exact cohort map first, then widen).

    Keep in sync with ``frontend/static/main.js`` ``marketCohortKeyAttempts``.
    """
    mk, md, tr = _trim_key(car.get("make"), car.get("model"), car.get("trim"))
    y = _parse_year(car.get("year"))
    mb = mileage_band(car.get("mileage"))
    keys: list[str] = []

    def add(years: list[int | None], mbs: list[str]) -> None:
        for yy in years:
            for band in mbs:
                keys.append(cohort_key(mk, md, tr, yy, band))

    if y is not None and mb != "unknown":
        add([y], [mb])
        for dy in range(1, _YEAR_WINDOW + 1):
            add([y - dy, y + dy], [mb])
    if y is not None:
        for band in ("0-25k", "25-50k", "50-75k", "75-100k", "100k+", "unknown"):
            if band != mb:
                keys.append(cohort_key(mk, md, tr, y, band))
        for dy in range(1, _YEAR_WINDOW + 1):
            for band in ("0-25k", "25-50k", "50-75k", "75-100k", "100k+", "unknown"):
                keys.append(cohort_key(mk, md, tr, y - dy, band))
                keys.append(cohort_key(mk, md, tr, y + dy, band))
    if mb != "unknown":
        for yy in range(2010, 2031):
            keys.append(cohort_key(mk, md, tr, yy, mb))
    return keys


def attach_market_to_listing_cars(
    cars: list[dict[str, Any]],
    *,
    zip_code: str | None = None,
    radius_miles: float | None = None,
) -> None:
    """Mutates listing grid dicts in place with a ``market`` key when stats exist."""
    z = (zip_code or "").strip() or None
    radius = radius_miles
    if z and (radius is None or float(radius) <= 0):
        radius = _DEFAULT_RADIUS_MI
    idx = get_cohort_index(zip_code=z, radius_miles=radius)

    for c in cars:
        intel = market_price_for_car(c, zip_code=z, radius_miles=radius)
        if not intel:
            c["market"] = None
            continue
        c["market"] = {
            "avg_price_display": intel["avg_price_display"],
            "delta_pct": intel["delta_pct"],
            "vs_market": intel["vs_market"],
            "cohort_label": intel.get("cohort_label"),
            "geo_label": intel.get("geo_label"),
            "sample_count": intel.get("sample_count"),
        }


# Backward-compatible alias for tests / callers
def get_trim_price_stats() -> dict[tuple[str, str, str], dict[str, Any]]:
    """Legacy trim-only stats (nationwide, no year/mileage). Prefer ``get_cohort_index``."""
    idx = get_cohort_index()
    legacy: dict[tuple[str, str, str], dict[str, Any]] = {}
    roll: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    for (mk, md, tr, _y, _mb), prices in idx.groups.items():
        roll[(mk, md, tr)].extend(prices)
    for key, prices in roll.items():
        if len(prices) < _MIN_LISTINGS_FOR_AVG:
            continue
        avg = sum(prices) / len(prices)
        legacy[key] = {
            "avg_price": round(avg, 2),
            "min_price": round(min(prices), 2),
            "max_price": round(max(prices), 2),
            "count": len(prices),
        }
    return legacy
