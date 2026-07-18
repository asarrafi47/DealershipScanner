"""Composite dealer scoring.

Blends three independent, per-dealer signals into a single 0-100 buyer-facing
``composite`` score (higher = better dealer to buy from):

1. **REPUTATION** - ``google_rating`` + ``google_review_count`` from the
   ``dealerships`` table. A raw star rating is noisy for dealers with only a
   handful of reviews, so we Bayesian-shrink it toward a neutral prior weighted
   by review volume (a 5.0 from 3 reviews scores below a 4.6 from 900).

2. **PRICING AGGRESSIVENESS** - how a dealer's listings price against the live
   market, read *through* :mod:`backend.intelligence.market_pricing` (never by
   re-querying ``cars``). For each active priced listing we take
   :func:`market_pricing.deal_score`'s signed ``pct_from_median`` (negative =
   cheaper than the cohort median) and reduce the dealer to the **median** of
   those percentages. A negative dealer median = systematically underpriced =
   buyer-friendly = a *higher* pricing score.

3. **INVENTORY** - selection size/mix straight from ``cars`` (count of active
   priced listings, new/used split, distinct models). A saturating log curve so
   a 1,500-car megastore isn't 300x a healthy 50-car lot.

The three sub-scores are each normalized to 0-100 and combined with documented
weights. Missing components degrade gracefully: a dealer with no reviews (or no
scoreable listings because every cohort is thin) simply drops that term and the
remaining weights renormalize.

Design: the scoring math (:func:`reputation_score`, :func:`pricing_score`,
:func:`inventory_score`, :func:`aggregate_pricing`, :func:`composite_score`) is
pure and unit-testable - it takes plain values / dicts and touches no database.
The DB-backed loaders and the :func:`dealer_score` / :func:`rank_dealers`
entrypoints at the bottom build on that core. Nothing here mutates anything;
it is a read-only analytics module that *imports* the pricing/signals modules
rather than editing them.
"""

from __future__ import annotations

import math
import statistics
from typing import Any, Iterable, Mapping, Optional

from backend.dev.dealers import slug_from_url
from backend.intelligence import market_pricing

Car = Mapping[str, Any]

# --- Tunables -----------------------------------------------------------------

#: Composite weights. Must be positive; they are renormalized over whichever
#: components are actually available for a given dealer.
DEFAULT_WEIGHTS: dict[str, float] = {
    "reputation": 0.40,
    "pricing": 0.40,
    "inventory": 0.20,
}

#: Reputation shrinkage prior: pull thin-review ratings toward ``REP_PRIOR_MEAN``
#: stars with the strength of ``REP_PRIOR_WEIGHT`` pseudo-reviews.
REP_PRIOR_MEAN = 4.0
REP_PRIOR_WEIGHT = 20.0
REP_MAX_STARS = 5.0

#: Pricing: a dealer median of ``-PRICING_PIVOT_PCT`` maps to a perfect 100 and
#: ``+PRICING_PIVOT_PCT`` to 0; at-market (0%) is the neutral 50.
PRICING_PIVOT_PCT = 15.0

#: Inventory: listing count at which the selection score saturates to ~100.
INVENTORY_SATURATION = 500

#: Below-market label from the pricing module (kept as a name, not re-derived).
_BELOW = market_pricing.LABEL_BELOW
_INSUFFICIENT = market_pricing.LABEL_INSUFFICIENT


# --- Sub-score primitives (pure) ----------------------------------------------


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def reputation_score(
    google_rating: Any,
    google_review_count: Any,
    *,
    prior_mean: float = REP_PRIOR_MEAN,
    prior_weight: float = REP_PRIOR_WEIGHT,
) -> Optional[float]:
    """0-100 reputation score from a star rating, shrunk by review volume.

    Returns ``None`` when there is no usable rating (the dealer then simply
    lacks a reputation term rather than being penalized). The star rating is
    Bayesian-shrunk toward ``prior_mean`` with weight ``prior_weight`` reviews::

        shrunk = (prior_weight * prior_mean + n * rating) / (prior_weight + n)

    so a 5.0 backed by 3 reviews lands well below a 4.6 backed by 900, and a
    dealer with 0 reviews but a rating scores exactly the prior.
    """
    try:
        rating = float(google_rating)
    except (TypeError, ValueError):
        return None
    if rating <= 0:
        return None
    try:
        n = max(0, int(google_review_count))
    except (TypeError, ValueError):
        n = 0
    shrunk = (prior_weight * prior_mean + n * rating) / (prior_weight + n)
    return round(_clamp(shrunk / REP_MAX_STARS * 100.0), 2)


def pricing_score(
    median_pct_from_market: Optional[float],
    *,
    pivot_pct: float = PRICING_PIVOT_PCT,
) -> Optional[float]:
    """0-100 buyer-friendliness from a dealer's median ``pct_from_median``.

    ``median_pct_from_market`` is the median (across the dealer's scoreable
    listings) of each listing's signed percent vs its cohort median. Negative =
    cheaper than market = buyer-friendly = higher score. 0% -> 50, ``-pivot`` ->
    100, ``+pivot`` -> 0 (clamped). ``None`` in -> ``None`` out (no scoreable
    inventory).
    """
    if median_pct_from_market is None:
        return None
    score = 50.0 - float(median_pct_from_market) * (50.0 / pivot_pct)
    return round(_clamp(score), 2)


def inventory_score(
    listing_count: int,
    *,
    saturation: int = INVENTORY_SATURATION,
) -> float:
    """0-100 selection score, saturating-log in the listing count.

    ``log1p(count) / log1p(saturation)`` so selection rewards depth with
    diminishing returns and never runs away. 0 listings -> 0.
    """
    n = max(0, int(listing_count))
    if n <= 0:
        return 0.0
    return round(_clamp(100.0 * math.log1p(n) / math.log1p(saturation)), 2)


def aggregate_pricing(deal_scores: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Reduce a dealer's per-listing :func:`market_pricing.deal_score` dicts.

    Skips ``insufficient_data`` listings (thin cohorts) so they don't distort
    the dealer. Returns::

        median_pct_from_market  median signed % vs cohort (None if nothing scoreable)
        below_market_share      fraction of scoreable listings labeled below_market
        mean_delta              mean $ delta vs cohort median (negative = cheaper)
        scored_listings         count of listings with a real cohort
        priced_listings         count of listings fed in
    """
    scores = list(deal_scores)
    usable = [
        d
        for d in scores
        if d.get("label") != _INSUFFICIENT and d.get("pct_from_median") is not None
    ]
    if not usable:
        return {
            "median_pct_from_market": None,
            "below_market_share": None,
            "mean_delta": None,
            "scored_listings": 0,
            "priced_listings": len(scores),
        }
    pcts = [float(d["pct_from_median"]) for d in usable]
    deltas = [float(d["delta"]) for d in usable if d.get("delta") is not None]
    below = sum(1 for d in usable if d.get("label") == _BELOW)
    return {
        "median_pct_from_market": round(statistics.median(pcts), 2),
        "below_market_share": round(below / len(usable), 4),
        "mean_delta": round(statistics.mean(deltas), 2) if deltas else None,
        "scored_listings": len(usable),
        "priced_listings": len(scores),
    }


def inventory_mix(listings: Iterable[Car]) -> dict[str, Any]:
    """Size + mix summary of a dealer's active listings."""
    rows = list(listings)
    total = len(rows)
    new = sum(1 for c in rows if market_pricing.normalize_condition(c.get("condition")) == "new")
    priced = sum(1 for c in rows if _as_price(c.get("price")) > 0)
    models = {
        (str(c.get("make") or "").strip().lower(), str(c.get("model") or "").strip().lower())
        for c in rows
        if c.get("make") and c.get("model")
    }
    return {
        "size": total,
        "new_count": new,
        "used_count": total - new,
        "new_share": round(new / total, 4) if total else 0.0,
        "priced_count": priced,
        "distinct_models": len(models),
    }


def _as_price(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def composite_score(
    reputation: Optional[float],
    pricing: Optional[float],
    inventory: Optional[float],
    *,
    weights: Mapping[str, float] = DEFAULT_WEIGHTS,
) -> Optional[float]:
    """Weighted blend of the three sub-scores over whichever are present.

    ``None`` sub-scores are dropped and the remaining weights renormalize, so a
    dealer with no reviews is scored on pricing + inventory alone rather than
    being zeroed. Returns ``None`` only when every component is missing.
    """
    parts = [
        ("reputation", reputation),
        ("pricing", pricing),
        ("inventory", inventory),
    ]
    num = 0.0
    denom = 0.0
    for name, value in parts:
        if value is None:
            continue
        w = float(weights.get(name, 0.0))
        if w <= 0:
            continue
        num += w * float(value)
        denom += w
    if denom <= 0:
        return None
    return round(num / denom, 2)


# --- Per-dealer assembly (pure over loaded data) ------------------------------


def score_listings(listings: Iterable[Car], band_index: Mapping[Any, Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Score each listing against the preloaded band index (offline, no DB).

    Reuses the pricing module's exact bucketing primitives (``config_key``,
    ``normalize_condition``, ``mileage_band_low``) so bands never drift, then
    delegates the actual scoring to :func:`market_pricing.deal_score` with the
    matched band passed in.
    """
    out: list[dict[str, Any]] = []
    for car in listings:
        band = _match_band(car, band_index)
        out.append(market_pricing.deal_score(car, band=band))
    return out


def build_dealer_score(
    dealer_id: str,
    listings: Iterable[Car],
    reputation: Optional[Mapping[str, Any]],
    band_index: Mapping[Any, Mapping[str, Any]],
    *,
    weights: Mapping[str, float] = DEFAULT_WEIGHTS,
) -> dict[str, Any]:
    """Assemble the full score dict for one dealer from already-loaded data.

    ``reputation`` is the matched ``dealerships`` row (or ``None``);
    ``band_index`` is the in-memory ``market_price_stats`` map from
    :func:`load_band_index`.
    """
    rows = list(listings)
    rep = reputation or {}
    pricing = aggregate_pricing(score_listings(rows, band_index))
    mix = inventory_mix(rows)

    rep_score = reputation_score(rep.get("google_rating"), rep.get("google_review_count"))
    price_score = pricing_score(pricing["median_pct_from_market"])
    inv_score = inventory_score(mix["size"])
    composite = composite_score(rep_score, price_score, inv_score, weights=weights)

    return {
        "dealer_id": dealer_id,
        "dealer_name": rep.get("name") or _first(rows, "dealer_name") or dealer_id,
        "oem_brand": rep.get("oem_brand"),
        "composite": composite,
        "reputation": {
            "score": rep_score,
            "google_rating": rep.get("google_rating"),
            "google_review_count": rep.get("google_review_count"),
            "matched": bool(reputation),
        },
        "pricing_aggressiveness": {
            "score": price_score,
            **pricing,
        },
        "inventory": {
            "score": inv_score,
            **mix,
        },
    }


def _first(rows: list[Car], key: str) -> Any:
    for c in rows:
        v = c.get(key)
        if v:
            return v
    return None


# --- Band matching (reuses pricing primitives) --------------------------------


def _band_key(car: Car) -> Optional[tuple[Any, ...]]:
    key = market_pricing.config_key(
        car.get("year"), car.get("make"), car.get("model"), car.get("trim")
    )
    if key is None:
        return None
    year, make, model, trim = key
    return (
        make,
        model,
        trim,
        year,
        market_pricing.normalize_condition(car.get("condition")),
        market_pricing.mileage_band_low(car.get("mileage")),
    )


def _match_band(car: Car, band_index: Mapping[Any, Mapping[str, Any]]) -> Optional[dict[str, Any]]:
    k = _band_key(car)
    if k is None:
        return None
    band = band_index.get(k)
    return dict(band) if band is not None else None


# --- DB loaders ---------------------------------------------------------------

_LISTING_FIELDS = (
    "vin, dealer_id, dealer_name, year, make, model, trim, price, mileage, condition"
)


def load_band_index(conn) -> dict[tuple[Any, ...], dict[str, Any]]:
    """Load the whole ``market_price_stats`` table into an in-memory band map.

    Keyed identically to :func:`market_pricing.lookup_band`'s WHERE clause
    ``(make, model, trim, year, condition, mileage_band_low)`` so a single pass
    over the dealer's listings scores offline - no per-car SQL round-trip.
    """
    market_pricing.ensure_schema(conn)
    sql = f"""
        SELECT make, model, trim, year, condition,
               mileage_band_low, mileage_band_high,
               median_price, p25_price, p75_price,
               sample_count, dealer_count
        FROM {market_pricing.STATS_TABLE}
    """
    index: dict[tuple[Any, ...], dict[str, Any]] = {}
    with conn.cursor() as cur:
        cur.execute(sql)
        for (
            make,
            model,
            trim,
            year,
            condition,
            band_low,
            band_high,
            median,
            p25,
            p75,
            n,
            dealers,
        ) in cur.fetchall():
            key = (make, model, trim, int(year), condition, int(band_low))
            index[key] = {
                "band_median": float(median),
                "band_p25": float(p25),
                "band_p75": float(p75),
                "sample_count": int(n),
                "dealer_count": int(dealers),
                "mileage_band_low": int(band_low),
                "mileage_band_high": (int(band_high) if band_high is not None else None),
                "mileage_band_label": market_pricing.mileage_band_label(int(band_low)),
                "condition": condition,
            }
    return index


def load_dealer_listings(conn) -> dict[str, list[dict[str, Any]]]:
    """Load active priced listings grouped by ``dealer_id``."""
    sql = (
        f"SELECT {_LISTING_FIELDS} FROM cars "
        "WHERE COALESCE(listing_active, 1) = 1 "
        "AND dealer_id IS NOT NULL "
        "AND price IS NOT NULL AND price > 0"
    )
    groups: dict[str, list[dict[str, Any]]] = {}
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        for row in cur.fetchall():
            car = dict(zip(cols, row))
            did = str(car.get("dealer_id"))
            groups.setdefault(did, []).append(car)
    return groups


def load_reputation_map(conn) -> dict[str, dict[str, Any]]:
    """Map ``dealer_id`` slug -> reputation row from ``dealerships``.

    The ``cars.dealer_id`` slug (``billluke-com``) is derived from a dealer's
    site host exactly like ``scanner.js``/``dealers.json`` via
    :func:`slug_from_url`, so we slug each dealership's ``website_url`` (falling
    back to ``dealer_website_url``) to reconnect the two tables even when the
    sparse ``dealership_registry_id`` FK is unset.
    """
    sql = """
        SELECT name, website_url, dealer_website_url,
               google_rating, google_review_count, oem_brand
        FROM dealerships
        WHERE COALESCE(is_active, 1) = 1
    """
    out: dict[str, dict[str, Any]] = {}
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        for row in cur.fetchall():
            d = dict(zip(cols, row))
            url = (d.get("website_url") or d.get("dealer_website_url") or "").strip()
            if not url:
                continue
            slug = slug_from_url(url)
            if not slug or slug == "dealer":
                continue
            prev = out.get(slug)
            # Prefer the row that actually carries a rating on slug collisions.
            if prev is None or (prev.get("google_rating") is None and d.get("google_rating") is not None):
                out[slug] = d
    return out


# --- Public entrypoints -------------------------------------------------------


def _score_all(conn, *, weights: Mapping[str, float] = DEFAULT_WEIGHTS) -> list[dict[str, Any]]:
    band_index = load_band_index(conn)
    listings_by_dealer = load_dealer_listings(conn)
    reputation = load_reputation_map(conn)
    results = []
    for did, listings in listings_by_dealer.items():
        results.append(
            build_dealer_score(did, listings, reputation.get(did), band_index, weights=weights)
        )
    return results


def dealer_score(
    dealer_id: str,
    *,
    conn=None,
    weights: Mapping[str, float] = DEFAULT_WEIGHTS,
) -> Optional[dict[str, Any]]:
    """Compute the composite score for a single ``dealer_id``.

    Opens its own read-only connection when ``conn`` is not supplied (a raw
    psycopg connection, as the pricing module's ``with conn.cursor()`` requires).
    Returns ``None`` when the dealer has no active priced listings.
    """
    if conn is not None:
        return _dealer_score_conn(dealer_id, conn, weights=weights)
    from backend.db.inventory_pg import pg_connect

    c = pg_connect()
    try:
        return _dealer_score_conn(dealer_id, c, weights=weights)
    finally:
        c.close()


def _dealer_score_conn(dealer_id: str, conn, *, weights: Mapping[str, float]) -> Optional[dict[str, Any]]:
    band_index = load_band_index(conn)
    listings_by_dealer = load_dealer_listings(conn)
    listings = listings_by_dealer.get(str(dealer_id))
    if not listings:
        return None
    reputation = load_reputation_map(conn).get(str(dealer_id))
    return build_dealer_score(str(dealer_id), listings, reputation, band_index, weights=weights)


def rank_dealers(
    *,
    conn=None,
    limit: Optional[int] = None,
    ascending: bool = False,
    min_inventory: int = 1,
    weights: Mapping[str, float] = DEFAULT_WEIGHTS,
) -> list[dict[str, Any]]:
    """Rank all dealers by ``composite`` (best first unless ``ascending``).

    Dealers with fewer than ``min_inventory`` active priced listings, or with no
    computable composite, are excluded. Opens its own connection if none given.
    """
    if conn is not None:
        rows = _score_all(conn, weights=weights)
    else:
        from backend.db.inventory_pg import pg_connect

        c = pg_connect()
        try:
            rows = _score_all(c, weights=weights)
        finally:
            c.close()
    ranked = [
        r
        for r in rows
        if r["composite"] is not None and r["inventory"]["size"] >= min_inventory
    ]
    ranked.sort(key=lambda r: r["composite"], reverse=not ascending)
    if limit:
        return ranked[: int(limit)]
    return ranked
