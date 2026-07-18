"""Market pricing intelligence.

Computes a per-CONFIG (year + make + model + trim) market price band from the
live ``cars`` inventory table, controlling for **condition** (new vs used) and
**mileage** (10k-mile bands). The bands are persisted to ``market_price_stats``
by :func:`rebuild_stats_table` (driven by ``backend/scripts/compute_market_stats.py``,
nightly-friendly) so per-listing scoring is a single indexed lookup.

Public surface
--------------
* :func:`rebuild_stats_table` - (re)compute + persist all qualifying bands.
* :func:`deal_score` - score one car against its band.
* :func:`lookup_band` - fetch the raw band row for a config.
* :func:`normalize_condition`, :func:`mileage_band_low`, :func:`config_key` -
  the bucketing primitives (shared by compute + score paths so they never drift).

A config is only persisted / scored when it has a real sample behind it:
``>= MIN_LISTINGS`` active priced listings across ``>= MIN_DEALERS`` distinct
dealers. Thinner configs score as ``insufficient_data``.
"""

from __future__ import annotations

from typing import Any

# --- Tunables -------------------------------------------------------------

#: 10k-mile bands; a mileage this high or above collapses into one top band so
#: a 250k-mile car doesn't spawn a singleton cohort.
MILEAGE_BAND_WIDTH = 10_000
MILEAGE_BAND_CAP = 200_000

#: Minimum evidence for a config to be trusted (guards against thin cohorts).
MIN_LISTINGS = 5
MIN_DEALERS = 2

#: +/- this fraction of the band median is considered "at market".
AT_MARKET_PCT = 5.0

STATS_TABLE = "market_price_stats"

LABEL_BELOW = "below_market"
LABEL_AT = "at_market"
LABEL_ABOVE = "above_market"
LABEL_INSUFFICIENT = "insufficient_data"


# --- Bucketing primitives -------------------------------------------------


def normalize_condition(condition: Any) -> str:
    """Collapse raw listing condition into ``new`` or ``used``.

    Only brand-new cars are "new"; Used / Certified / Certified Pre-Owned all
    price as the used market, so they share a cohort.
    """
    text = str(condition or "").strip().lower()
    if text == "new":
        return "new"
    return "used"


def mileage_band_low(mileage: Any) -> int:
    """Lower bound (in miles) of the 10k band this mileage falls in.

    Missing / negative mileage is treated as 0 (new-car / unknown floor band).
    Anything at or above :data:`MILEAGE_BAND_CAP` collapses to the top band.
    """
    try:
        m = int(float(mileage))
    except (TypeError, ValueError):
        m = 0
    if m < 0:
        m = 0
    if m >= MILEAGE_BAND_CAP:
        return MILEAGE_BAND_CAP
    return (m // MILEAGE_BAND_WIDTH) * MILEAGE_BAND_WIDTH


def mileage_band_label(band_low: int) -> str:
    """Human-readable band label, e.g. ``30k-40k`` or ``200k+``."""
    if band_low >= MILEAGE_BAND_CAP:
        return f"{MILEAGE_BAND_CAP // 1000}k+"
    high = band_low + MILEAGE_BAND_WIDTH
    return f"{band_low // 1000}k-{high // 1000}k"


def _norm_text(value: Any) -> str:
    return str(value or "").strip().lower()


def config_key(year: Any, make: Any, model: Any, trim: Any) -> tuple[int, str, str, str] | None:
    """Normalized (year, make, model, trim) key, or ``None`` if not keyable.

    ``trim`` normalizes to ``""`` when absent - a valid (if coarse) config.
    Missing year/make/model make the config unusable and return ``None``.
    """
    try:
        y = int(float(year))
    except (TypeError, ValueError):
        return None
    make_n = _norm_text(make)
    model_n = _norm_text(model)
    if not make_n or not model_n:
        return None
    return (y, make_n, model_n, _norm_text(trim))


# --- Schema + compute -----------------------------------------------------

_CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {STATS_TABLE} (
    year               integer NOT NULL,
    make               text    NOT NULL,
    model              text    NOT NULL,
    trim               text    NOT NULL,
    condition          text    NOT NULL,
    mileage_band_low   integer NOT NULL,
    mileage_band_high  integer,
    median_price       double precision NOT NULL,
    p25_price          double precision NOT NULL,
    p75_price          double precision NOT NULL,
    sample_count       integer NOT NULL,
    dealer_count       integer NOT NULL,
    computed_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (make, model, trim, year, condition, mileage_band_low)
);
"""

# Single-statement recompute: bucket every active priced row, aggregate to
# bands, keep only bands with real evidence. Cheap enough to run nightly.
_REBUILD_SQL = f"""
WITH base AS (
    SELECT
        year,
        lower(make)  AS make,
        lower(model) AS model,
        lower(coalesce(nullif(trim, ''), '')) AS trim,
        CASE WHEN lower(coalesce(condition, '')) = 'new' THEN 'new' ELSE 'used' END AS condition,
        LEAST(
            (GREATEST(coalesce(mileage, 0), 0) / {MILEAGE_BAND_WIDTH}) * {MILEAGE_BAND_WIDTH},
            {MILEAGE_BAND_CAP}
        ) AS band_low,
        price,
        dealer_id
    FROM cars
    WHERE coalesce(listing_active, 1) = 1
      AND price IS NOT NULL
      AND price > 0
      AND year IS NOT NULL
      AND make IS NOT NULL
      AND model IS NOT NULL
)
INSERT INTO {STATS_TABLE} (
    year, make, model, trim, condition,
    mileage_band_low, mileage_band_high,
    median_price, p25_price, p75_price,
    sample_count, dealer_count, computed_at
)
SELECT
    year, make, model, trim, condition,
    band_low,
    CASE WHEN band_low >= {MILEAGE_BAND_CAP} THEN NULL ELSE band_low + {MILEAGE_BAND_WIDTH} END,
    percentile_cont(0.5)  WITHIN GROUP (ORDER BY price),
    percentile_cont(0.25) WITHIN GROUP (ORDER BY price),
    percentile_cont(0.75) WITHIN GROUP (ORDER BY price),
    count(*),
    count(DISTINCT dealer_id),
    now()
FROM base
GROUP BY year, make, model, trim, condition, band_low
HAVING count(*) >= %(min_listings)s
   AND count(DISTINCT dealer_id) >= %(min_dealers)s
"""


def ensure_schema(conn) -> None:
    """Create ``market_price_stats`` if it does not exist yet."""
    with conn.cursor() as cur:
        cur.execute(_CREATE_SQL)


def rebuild_stats_table(
    conn,
    *,
    min_listings: int = MIN_LISTINGS,
    min_dealers: int = MIN_DEALERS,
) -> int:
    """Recompute every qualifying band and replace the stats table contents.

    Truncate-then-insert inside the caller's transaction, so a failure leaves
    the previous night's bands intact. Returns the number of bands written.
    The caller owns ``commit()``.
    """
    ensure_schema(conn)
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {STATS_TABLE}")
        cur.execute(
            _REBUILD_SQL,
            {"min_listings": min_listings, "min_dealers": min_dealers},
        )
        cur.execute(f"SELECT count(*) FROM {STATS_TABLE}")
        row = cur.fetchone()
    return int(row[0]) if row else 0


# --- Lookup + scoring -----------------------------------------------------

_LOOKUP_SQL = f"""
SELECT median_price, p25_price, p75_price, sample_count, dealer_count,
       mileage_band_low, mileage_band_high, condition
FROM {STATS_TABLE}
WHERE make = %(make)s
  AND model = %(model)s
  AND trim = %(trim)s
  AND year = %(year)s
  AND condition = %(condition)s
  AND mileage_band_low = %(band_low)s
"""


def lookup_band(conn, car: dict[str, Any]) -> dict[str, Any] | None:
    """Fetch the persisted band row matching this car, or ``None``.

    ``car`` uses the same keys as a serialized listing: ``year``, ``make``,
    ``model``, ``trim``, ``condition``, ``mileage``.
    """
    key = config_key(car.get("year"), car.get("make"), car.get("model"), car.get("trim"))
    if key is None:
        return None
    year, make, model, trim = key
    params = {
        "year": year,
        "make": make,
        "model": model,
        "trim": trim,
        "condition": normalize_condition(car.get("condition")),
        "band_low": mileage_band_low(car.get("mileage")),
    }
    with conn.cursor() as cur:
        cur.execute(_LOOKUP_SQL, params)
        row = cur.fetchone()
    if not row:
        return None
    median, p25, p75, n, dealers, band_low, band_high, condition = row
    return {
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


def _label_for(pct_from_median: float, at_market_pct: float) -> str:
    if pct_from_median < -at_market_pct:
        return LABEL_BELOW
    if pct_from_median > at_market_pct:
        return LABEL_ABOVE
    return LABEL_AT


def deal_score(
    car: dict[str, Any],
    *,
    conn=None,
    band: dict[str, Any] | None = None,
    at_market_pct: float = AT_MARKET_PCT,
) -> dict[str, Any]:
    """Score a car against its market band.

    Returns a dict with::

        {
          "label": below_market | at_market | above_market | insufficient_data,
          "band_median": float | None,
          "delta": float | None,          # price - band_median (negative = cheaper)
          "pct_from_median": float | None, # delta / band_median * 100
          "sample_count": int,
          "dealer_count": int,
          ...band metadata when available
        }

    Supply a live ``conn`` (a psycopg connection) for the lookup, or pass a
    pre-fetched ``band`` (e.g. from :func:`lookup_band`) to score offline.
    Any config with no qualifying band, or a car with no usable price, scores
    ``insufficient_data``.
    """
    if band is None and conn is not None:
        band = lookup_band(conn, car)

    try:
        price = float(car.get("price"))
    except (TypeError, ValueError):
        price = 0.0

    if band is None or price <= 0:
        return {
            "label": LABEL_INSUFFICIENT,
            "band_median": (band.get("band_median") if band else None),
            "delta": None,
            "pct_from_median": None,
            "sample_count": (band.get("sample_count", 0) if band else 0),
            "dealer_count": (band.get("dealer_count", 0) if band else 0),
        }

    median = band["band_median"]
    delta = price - median
    pct = (delta / median * 100.0) if median else 0.0
    return {
        "label": _label_for(pct, at_market_pct),
        "band_median": median,
        "band_p25": band.get("band_p25"),
        "band_p75": band.get("band_p75"),
        "delta": round(delta, 2),
        "pct_from_median": round(pct, 2),
        "sample_count": band.get("sample_count", 0),
        "dealer_count": band.get("dealer_count", 0),
        "mileage_band_label": band.get("mileage_band_label"),
        "condition": band.get("condition"),
    }
