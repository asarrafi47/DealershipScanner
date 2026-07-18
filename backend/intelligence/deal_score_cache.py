"""In-process cache of ``market_price_stats`` for fast per-listing deal scoring.

The stats table is tiny (~584 bands), so the whole table is loaded into a dict
keyed by config once per process (TTL-refreshed) instead of doing a per-car
indexed lookup. This keeps the listings hot path — which serializes many cars
per request — to zero extra DB round-trips: serialization scores each car
offline against the pre-fetched band via
:func:`backend.intelligence.market_pricing.deal_score`.

Public surface
--------------
* :func:`public_deal_score` — coarse, free consumer badge block (label + delta +
  pct + band median), or ``None`` when the car has no price / no qualifying band.
* :func:`detailed_deal_score` — full band breakdown (adds p25/p75, sample and
  dealer counts, mileage band) for entitled ``FEATURE_MARKET_INTEL`` viewers.
* :func:`refresh_cache` — force a reload (e.g. after a nightly recompute).
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from backend.intelligence.market_pricing import (
    LABEL_INSUFFICIENT,
    STATS_TABLE,
    config_key,
    deal_score,
    mileage_band_label,
    mileage_band_low,
    normalize_condition,
)

logger = logging.getLogger(__name__)

# The stats table only changes on the nightly recompute; a short TTL keeps a
# long-lived web process from pinning a stale snapshot without hammering the DB.
_CACHE_TTL_SEC = 300.0

_lock = threading.Lock()
_bands: dict[tuple[int, str, str, str, str, int], dict[str, Any]] | None = None
_loaded_at = 0.0

# Fields the free consumer badge is allowed to show. The detailed band
# breakdown (p25/p75, sample/dealer counts) is gated behind FEATURE_MARKET_INTEL.
_COARSE_KEYS = ("label", "delta", "pct_from_median", "band_median")

# A price this far outside the band is almost always a listing artifact (a lease
# payment, a deposit, or a data-entry typo) rather than a genuine deal — surfacing
# a "-100% below market" badge for a $122 truck would be worse than showing nothing.
# We suppress the deal score for those rather than mutate the underlying computation.
_MIN_PLAUSIBLE_PCT = -50.0
_MAX_PLAUSIBLE_PCT = 100.0

_LOAD_SQL = f"""
SELECT year, make, model, trim, condition,
       mileage_band_low, mileage_band_high,
       median_price, p25_price, p75_price,
       sample_count, dealer_count
FROM {STATS_TABLE}
"""


def _load_bands() -> dict[tuple[int, str, str, str, str, int], dict[str, Any]]:
    """Read every band row into a config-keyed dict (band dicts match ``lookup_band``)."""
    from backend.db.inventory_pg import is_inventory_postgres, pg_connect

    if not is_inventory_postgres():
        return {}

    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(_LOAD_SQL)
            rows = cur.fetchall()
    finally:
        conn.close()

    out: dict[tuple[int, str, str, str, str, int], dict[str, Any]] = {}
    for row in rows:
        (
            year,
            make,
            model,
            trim,
            condition,
            band_low,
            band_high,
            median,
            p25,
            p75,
            n,
            dealers,
        ) = row
        key = (
            int(year),
            str(make),
            str(model),
            str(trim),
            str(condition),
            int(band_low),
        )
        out[key] = {
            "band_median": float(median),
            "band_p25": float(p25),
            "band_p75": float(p75),
            "sample_count": int(n),
            "dealer_count": int(dealers),
            "mileage_band_low": int(band_low),
            "mileage_band_high": (int(band_high) if band_high is not None else None),
            "mileage_band_label": mileage_band_label(int(band_low)),
            "condition": condition,
        }
    return out


def _bands_map(force: bool = False) -> dict[tuple[int, str, str, str, str, int], dict[str, Any]]:
    global _bands, _loaded_at
    now = time.monotonic()
    with _lock:
        stale = _bands is None or (now - _loaded_at) > _CACHE_TTL_SEC
        if force or stale:
            try:
                _bands = _load_bands()
                _loaded_at = now
            except Exception:
                logger.warning("deal-score band cache load failed", exc_info=True)
                if _bands is None:
                    _bands = {}
        return _bands


def band_for_car(car: dict[str, Any]) -> dict[str, Any] | None:
    """Pre-fetched band dict matching this car's config, or ``None``.

    Uses the same bucketing primitives as the compute + lookup paths so the
    in-memory key never drifts from the persisted one.
    """
    key = config_key(car.get("year"), car.get("make"), car.get("model"), car.get("trim"))
    if key is None:
        return None
    year, make, model, trim = key
    full_key = (
        year,
        make,
        model,
        trim,
        normalize_condition(car.get("condition")),
        mileage_band_low(car.get("mileage")),
    )
    return _bands_map().get(full_key)


def _score(car: dict[str, Any]) -> dict[str, Any] | None:
    """Score a car offline against the cached band; ``None`` when unqualified."""
    try:
        ds = deal_score(car, band=band_for_car(car))
    except Exception:
        logger.debug("deal_score failed for car id=%r", car.get("id"), exc_info=True)
        return None
    if ds.get("label") in (None, LABEL_INSUFFICIENT) or ds.get("delta") is None:
        return None
    pct = ds.get("pct_from_median")
    if pct is not None and (pct < _MIN_PLAUSIBLE_PCT or pct > _MAX_PLAUSIBLE_PCT):
        return None
    return ds


def public_deal_score(car: dict[str, Any]) -> dict[str, Any] | None:
    """Coarse, free deal-score block (label/delta/pct/band_median) or ``None``."""
    ds = _score(car)
    if ds is None:
        return None
    return {k: ds.get(k) for k in _COARSE_KEYS}


def detailed_deal_score(car: dict[str, Any]) -> dict[str, Any] | None:
    """Full band breakdown for entitled viewers, or ``None`` when unqualified."""
    return _score(car)


def refresh_cache() -> None:
    """Force the next lookup to reload the stats table (post-recompute hook)."""
    _bands_map(force=True)
