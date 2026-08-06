"""
Read-time plausibility guard for ``cars.price``.

Measured 2026-08-05 over 122,663 active listings: 33 exceed $300k and 212 sit
under $500. Most of the >$300k group is genuinely priced — a McLaren 765LT at
$699,900 and a Lamborghini Revuelto at $679,900 are real asking prices, and
so are the eye-watering (but internally consistent, repeat-scraped) Mercedes-
Benz of Ontario markups on G-Class / Maybach S-Class / Porsche 911 Turbo —
those track within ~2-3x of their own model's active-listing median, which is
exactly what dealer ADM (additional dealer markup) on allocation-constrained
trims looks like. A flat dollar ceiling would suppress all of them.

The two confirmed-broken rows look nothing like that:
  - 2026 Ford Bronco Base, id 160558, $449,150 — its own (year, make, model,
    trim) cohort of 43 *other* active Broncos has a median of $46,039. 9.8x.
  - 2023 Nissan GT-R Nismo, id 914716, $429,175 — its cohort is too thin (2
    other active GT-Rs) to judge statistically; flagged by inspection only,
    not by this guard. Documented as a known gap below.

So the guard is COHORT-RELATIVE, not a fixed ceiling: a price is suppressed
only when it is far outside the spread of *other active listings of the same
vehicle*, and only when there are enough of those peers to make the spread
mean something. No comparison data -> no suppression, same as everywhere else
suppression is used in this codebase (battery_kwh, curb_weight_lb, tow_capacity_lb
in ``backend/utils/car_serialize/serialize.py``): showing nothing beats
showing an invented number, but showing nothing ALSO beats hiding a real one
for lack of evidence. A rare exotic with no peers (McLaren 765LT: cohort size
1) is therefore left alone, exactly like a mainstream car with no peers would
be.

Known gap: the GT-R above is not caught by this guard, and no fixed-ceiling
tweak was added to catch it, because a threshold tight enough to flag its
~2.7x cohort ratio would also flag the legitimate ~2-3x Mercedes-Benz of
Ontario / Porsche markups above. Suppressing real prices to catch one more
fake one is the wrong trade for this codebase (see the trim-overlay
provenance disaster in project memory). The GT-R is instead a candidate for
manual review (``recovery_status`` / ``marked_for_review``), not silent
suppression.

Cache design mirrors ``backend.intelligence.deal_score_cache``: the whole
per-(make, model[, trim, year]) median table is loaded once per process
(TTL-refreshed) so the listings-grid hot path pays zero extra DB round-trips
per car.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

_CACHE_TTL_SEC = 300.0

_lock = threading.Lock()
# (make, model, trim, year) -> (median_price, sample_count) — lowercased key parts.
_trim_year_medians: dict[tuple[str, str, str, int], tuple[float, int]] | None = None
# (make, model) -> (median_price, sample_count) — fallback when the narrow cohort is too thin.
_model_medians: dict[tuple[str, str], tuple[float, int]] | None = None
_loaded_at = 0.0

# Peers required, EXCLUDING the car being judged, before a cohort is trusted to
# say anything about spread. Below this, "outlier" and "rare car" look the same.
_MIN_PEERS = 5

# A price has to clear this multiple of its cohort's median to be treated as
# implausible. Calibrated against the >$300k audit: the confirmed-legitimate
# Mercedes-Benz of Ontario / Porsche dealer-markup rows sit at 2.1x-3.2x their
# model's active-listing median; the confirmed-broken Bronco sits at 9.8x.
# 5x sits in the gap and does not require a marque allow-list.
_IMPLAUSIBLE_MULTIPLE = 5.0

_LOAD_TRIM_YEAR_SQL = """
    SELECT lower(make), lower(model), lower(COALESCE(trim, '')), year,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY price), count(*)
    FROM cars
    WHERE listing_active = 1 AND price > 0 AND year IS NOT NULL
    GROUP BY 1, 2, 3, 4
"""

_LOAD_MODEL_SQL = """
    SELECT lower(make), lower(model),
           percentile_cont(0.5) WITHIN GROUP (ORDER BY price), count(*)
    FROM cars
    WHERE listing_active = 1 AND price > 0
    GROUP BY 1, 2
"""


def _load() -> tuple[
    dict[tuple[str, str, str, int], tuple[float, int]],
    dict[tuple[str, str], tuple[float, int]],
]:
    from backend.db.inventory_pg import is_inventory_postgres, pg_connect

    if not is_inventory_postgres():
        # SQLite (tests / dev) has no percentile_cont; the guard simply abstains.
        return {}, {}

    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(_LOAD_TRIM_YEAR_SQL)
            trim_year = {
                (make, model, trim, int(year)): (float(median), int(n))
                for make, model, trim, year, median, n in cur.fetchall()
            }
            cur.execute(_LOAD_MODEL_SQL)
            by_model = {
                (make, model): (float(median), int(n))
                for make, model, median, n in cur.fetchall()
            }
    finally:
        conn.close()
    return trim_year, by_model


def _ensure_loaded(force: bool = False) -> None:
    global _trim_year_medians, _model_medians, _loaded_at
    now = time.monotonic()
    with _lock:
        stale = _trim_year_medians is None or (now - _loaded_at) > _CACHE_TTL_SEC
        if force or stale:
            try:
                _trim_year_medians, _model_medians = _load()
                _loaded_at = now
            except Exception:
                logger.warning("price-plausibility cache load failed", exc_info=True)
                if _trim_year_medians is None:
                    _trim_year_medians, _model_medians = {}, {}


def refresh_cache() -> None:
    """Force a reload (tests; after a bulk price change)."""
    _ensure_loaded(force=True)


def implausible_price(car: dict[str, Any], price: float) -> bool:
    """
    True when *price* is a statistical outlier against active listings of the
    SAME (year, make, model, trim), falling back to (make, model) when that
    cohort is too thin. False whenever there isn't enough peer data to judge —
    never invents a "correct" price, only says whether this one is trustworthy.
    """
    if price is None or price <= 0:
        return False
    make = str(car.get("make") or "").strip().lower()
    model = str(car.get("model") or "").strip().lower()
    if not make or not model:
        return False

    _ensure_loaded()
    assert _trim_year_medians is not None and _model_medians is not None

    median: float | None = None
    year = car.get("year")
    if isinstance(year, int):
        trim = str(car.get("trim") or "").strip().lower()
        hit = _trim_year_medians.get((make, model, trim, year))
        if hit and hit[1] - 1 >= _MIN_PEERS:
            median = hit[0]

    if median is None:
        hit = _model_medians.get((make, model))
        if hit and hit[1] - 1 >= _MIN_PEERS:
            median = hit[0]

    if median is None or median <= 0:
        return False
    return price > median * _IMPLAUSIBLE_MULTIPLE
