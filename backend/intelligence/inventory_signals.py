"""Inventory aging + price-drop signals.

Turns two already-populated columns on the ``cars`` table into buyer/seller
signals:

* ``first_seen_at``    -> DAYS-ON-LOT and an aging bucket
  (fresh / normal / stale / very_stale). Long-sitting inventory = more
  negotiable (buyer leverage); fresh, fast-turning inventory = hot demand.
* ``price_provenance_json`` (a JSON list of ``{"date", "price"}`` snapshots,
  with ``last_price_change_at`` as a coarse fallback) -> PRICE DROPS, their
  magnitude, and recency.

The *core* functions (:func:`days_on_lot`, :func:`aging_bucket`,
:func:`recent_price_drop`) are pure: they take a ``car`` mapping and an optional
``now`` and touch no database, so they are trivially unit-testable. The
DB-backed loaders and aggregate helpers at the bottom build on them.

Nothing here mutates the database or the serialization layer -- it is a
read-only analytics module.
"""

from __future__ import annotations

import json
import statistics
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional

# --- aging bucket thresholds (days on lot) -------------------------------------
FRESH_MAX_DAYS = 14        # fresh:      days_on_lot < 14
STALE_MIN_DAYS = 60        # stale:      60 < days_on_lot <= 90
VERY_STALE_MIN_DAYS = 90   # very_stale: days_on_lot > 90
# normal is everything between FRESH_MAX_DAYS and STALE_MIN_DAYS (inclusive).

# A price change is "recent" if it landed within this many days of ``now``.
DEFAULT_RECENT_WINDOW_DAYS = 30

Car = Mapping[str, Any]


# --- timestamp / value parsing -------------------------------------------------
def _now(now: Optional[datetime] = None) -> datetime:
    if now is not None:
        return now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)


def _parse_ts(value: Any) -> Optional[datetime]:
    """Parse an ISO-8601 timestamp (tolerating a trailing ``Z``) to aware UTC."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    s = str(value).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        # last resort: date-only prefix
        try:
            dt = datetime.fromisoformat(s[:10])
        except ValueError:
            return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f


# --- days on lot / aging -------------------------------------------------------
def days_on_lot(car: Car, now: Optional[datetime] = None) -> Optional[int]:
    """Whole days between ``first_seen_at`` and ``now``.

    Returns ``None`` when ``first_seen_at`` is missing/unparseable. Negative
    spans (clock skew / future timestamps) clamp to 0.
    """
    first_seen = _parse_ts(car.get("first_seen_at"))
    if first_seen is None:
        return None
    delta = _now(now) - first_seen
    return max(0, delta.days)


def aging_bucket(car: Car, now: Optional[datetime] = None) -> Optional[str]:
    """Classify a car by days on lot.

    ``fresh`` (<14d), ``normal`` (14-60d), ``stale`` (60-90d),
    ``very_stale`` (>90d). ``None`` when days-on-lot is unknown.
    """
    d = days_on_lot(car, now=now)
    if d is None:
        return None
    if d < FRESH_MAX_DAYS:
        return "fresh"
    if d <= STALE_MIN_DAYS:
        return "normal"
    if d <= VERY_STALE_MIN_DAYS:
        return "stale"
    return "very_stale"


def is_stale(car: Car, now: Optional[datetime] = None) -> bool:
    """True when the car is stale *or* very_stale (i.e. > STALE_MIN_DAYS)."""
    return aging_bucket(car, now=now) in ("stale", "very_stale")


# --- price history / drops -----------------------------------------------------
def price_history(car: Car) -> list[tuple[datetime, float]]:
    """Return ``[(when, price), ...]`` sorted ascending by date.

    Reads ``price_provenance_json`` (a JSON string or already-decoded list of
    ``{"date", "price"}``). Entries without a valid date or a positive price are
    dropped. Empty when there is no usable provenance.
    """
    raw = car.get("price_provenance_json")
    if not raw:
        return []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (ValueError, TypeError):
            return []
    if not isinstance(raw, list):
        return []
    out: list[tuple[datetime, float]] = []
    for entry in raw:
        if not isinstance(entry, Mapping):
            continue
        when = _parse_ts(entry.get("date"))
        price = _as_float(entry.get("price"))
        if when is None or price is None or price <= 0:
            continue
        out.append((when, price))
    out.sort(key=lambda t: t[0])
    return out


def _empty_drop() -> dict[str, Any]:
    return {
        "dropped": False,
        "when": None,
        "days_ago": None,
        "is_recent": False,
        "from_price": None,
        "to_price": None,
        "drop_amount": None,
        "drop_pct": None,
        "num_drops": 0,
        "peak_price": None,
        "current_price": None,
        "total_drop_from_peak": None,
        "total_drop_pct_from_peak": None,
    }


def recent_price_drop(
    car: Car,
    now: Optional[datetime] = None,
    recent_within_days: int = DEFAULT_RECENT_WINDOW_DAYS,
) -> dict[str, Any]:
    """Detect the most recent *downward* price move and summarize markdowns.

    The returned dict always has the same keys::

        dropped                 True if any downward move exists in history
        when                    ISO date of the most recent drop (or None)
        days_ago                whole days since that drop (or None)
        is_recent               drop landed within ``recent_within_days``
        from_price / to_price   prices bracketing the most recent drop
        drop_amount             from_price - to_price (>0)
        drop_pct                percent off from_price
        num_drops               count of downward moves across all history
        peak_price              highest price ever seen
        current_price           latest price seen
        total_drop_from_peak    peak_price - current_price (>=0)
        total_drop_pct_from_peak percent off peak

    Falls back to ``last_price_change_at`` only for the ``when``/``days_ago`` of
    an otherwise-detected drop when provenance lacks dates. With <2 usable price
    points there is nothing to compare, so ``dropped`` is False.
    """
    result = _empty_drop()
    history = price_history(car)
    if len(history) >= 2:
        prices = [p for _, p in history]
        result["peak_price"] = max(prices)
        result["current_price"] = prices[-1]
        result["total_drop_from_peak"] = round(result["peak_price"] - prices[-1], 2)
        if result["peak_price"] > 0:
            result["total_drop_pct_from_peak"] = round(
                100.0 * result["total_drop_from_peak"] / result["peak_price"], 2
            )

        last_drop: Optional[tuple[datetime, float, float]] = None
        num_drops = 0
        for (_, prev_price), (when, price) in zip(history, history[1:]):
            if price < prev_price:
                num_drops += 1
                last_drop = (when, prev_price, price)
        result["num_drops"] = num_drops
        if last_drop is not None:
            when, from_price, to_price = last_drop
            days_ago = max(0, (_now(now) - when).days)
            result.update(
                dropped=True,
                when=when.date().isoformat(),
                days_ago=days_ago,
                is_recent=days_ago <= recent_within_days,
                from_price=round(from_price, 2),
                to_price=round(to_price, 2),
                drop_amount=round(from_price - to_price, 2),
                drop_pct=round(100.0 * (from_price - to_price) / from_price, 2)
                if from_price > 0
                else None,
            )
    return result


# --- aggregate / dealer-level helpers ------------------------------------------
def aging_distribution(
    cars: Iterable[Car], now: Optional[datetime] = None
) -> dict[str, Any]:
    """Bucket counts + summary stats of days-on-lot over ``cars``."""
    buckets = {"fresh": 0, "normal": 0, "stale": 0, "very_stale": 0, "unknown": 0}
    ages: list[int] = []
    total = 0
    for car in cars:
        total += 1
        b = aging_bucket(car, now=now)
        buckets[b if b else "unknown"] += 1
        d = days_on_lot(car, now=now)
        if d is not None:
            ages.append(d)
    stats: dict[str, Any] = {
        "total": total,
        "buckets": buckets,
        "stale_share": round((buckets["stale"] + buckets["very_stale"]) / total, 4)
        if total
        else 0.0,
    }
    if ages:
        stats["days_on_lot"] = {
            "min": min(ages),
            "median": round(statistics.median(ages), 1),
            "mean": round(statistics.mean(ages), 1),
            "max": max(ages),
        }
    return stats


def _dealer_key(car: Car) -> str:
    return (
        car.get("dealer_name")
        or car.get("dealer_id")
        or (
            str(car.get("dealership_registry_id"))
            if car.get("dealership_registry_id") is not None
            else None
        )
        or "unknown"
    )


def stale_share_by_dealer(
    cars: Iterable[Car],
    now: Optional[datetime] = None,
    min_inventory: int = 10,
) -> list[dict[str, Any]]:
    """Per-dealer share of stale (>60d) inventory, high-share first.

    Dealers with fewer than ``min_inventory`` aged listings are excluded so a
    single old car does not top the chart. High stale share = a lot sitting on
    negotiable metal.
    """
    agg: dict[str, dict[str, Any]] = {}
    for car in cars:
        b = aging_bucket(car, now=now)
        if b is None:
            continue
        d = agg.setdefault(_dealer_key(car), {"dealer": _dealer_key(car), "total": 0, "stale": 0, "_ages": []})
        d["total"] += 1
        if b in ("stale", "very_stale"):
            d["stale"] += 1
        age = days_on_lot(car, now=now)
        if age is not None:
            d["_ages"].append(age)
    rows = []
    for d in agg.values():
        if d["total"] < min_inventory:
            continue
        ages = d.pop("_ages")
        d["stale_share"] = round(d["stale"] / d["total"], 4)
        d["median_days_on_lot"] = round(statistics.median(ages), 1) if ages else None
        rows.append(d)
    rows.sort(key=lambda r: (r["stale_share"], r["median_days_on_lot"] or 0), reverse=True)
    return rows


def price_drop_summary(
    cars: Iterable[Car],
    now: Optional[datetime] = None,
    recent_within_days: int = DEFAULT_RECENT_WINDOW_DAYS,
) -> dict[str, Any]:
    """Portfolio-level view of markdown activity across ``cars``."""
    total = 0
    with_drop = 0
    recent = 0
    drop_pcts: list[float] = []
    for car in cars:
        total += 1
        info = recent_price_drop(car, now=now, recent_within_days=recent_within_days)
        if info["dropped"]:
            with_drop += 1
            if info["drop_pct"] is not None:
                drop_pcts.append(info["drop_pct"])
            if info["is_recent"]:
                recent += 1
    out: dict[str, Any] = {
        "total": total,
        "cars_with_drop": with_drop,
        "drop_share": round(with_drop / total, 4) if total else 0.0,
        "recent_drops": recent,
    }
    if drop_pcts:
        out["avg_drop_pct"] = round(statistics.mean(drop_pcts), 2)
        out["median_drop_pct"] = round(statistics.median(drop_pcts), 2)
    return out


def top_price_drops(
    cars: Iterable[Car],
    now: Optional[datetime] = None,
    limit: int = 10,
    recent_only: bool = False,
    recent_within_days: int = DEFAULT_RECENT_WINDOW_DAYS,
) -> list[dict[str, Any]]:
    """Biggest markdowns (by percent off), largest first."""
    rows = []
    for car in cars:
        info = recent_price_drop(car, now=now, recent_within_days=recent_within_days)
        if not info["dropped"] or info["drop_pct"] is None:
            continue
        if recent_only and not info["is_recent"]:
            continue
        rows.append(
            {
                "vin": car.get("vin"),
                "year": car.get("year"),
                "make": car.get("make"),
                "model": car.get("model"),
                "dealer": _dealer_key(car),
                "days_on_lot": days_on_lot(car, now=now),
                "aging_bucket": aging_bucket(car, now=now),
                **{k: info[k] for k in ("from_price", "to_price", "drop_amount", "drop_pct", "when", "days_ago", "is_recent", "num_drops")},
            }
        )
    rows.sort(key=lambda r: r["drop_pct"], reverse=True)
    return rows[:limit]


def oldest_active(
    cars: Iterable[Car], now: Optional[datetime] = None, limit: int = 10
) -> list[dict[str, Any]]:
    """Longest-sitting listings, oldest first (buyer-leverage candidates)."""
    rows = []
    for car in cars:
        d = days_on_lot(car, now=now)
        if d is None:
            continue
        rows.append(
            {
                "vin": car.get("vin"),
                "year": car.get("year"),
                "make": car.get("make"),
                "model": car.get("model"),
                "dealer": _dealer_key(car),
                "price": _as_float(car.get("price")),
                "days_on_lot": d,
                "aging_bucket": aging_bucket(car, now=now),
            }
        )
    rows.sort(key=lambda r: r["days_on_lot"], reverse=True)
    return rows[:limit]


# --- turn-time (from removed listings) -----------------------------------------
def turn_time_days(car: Car) -> Optional[int]:
    """Days a (now-removed) listing spent on the lot before disappearing.

    Uses ``first_seen_at`` -> ``listing_removed_at``. Returns ``None`` when either
    is missing or the span is nonsensical (negative). A removed listing is our
    best proxy for a *sold* car; short spans = hot, fast-turning models.
    """
    first_seen = _parse_ts(car.get("first_seen_at"))
    removed = _parse_ts(car.get("listing_removed_at"))
    if first_seen is None or removed is None:
        return None
    days = (removed - first_seen).days
    return days if days >= 0 else None


def fastest_turning_models(
    cars: Iterable[Car],
    min_count: int = 5,
    limit: int = 10,
    slowest: bool = False,
) -> list[dict[str, Any]]:
    """Rank ``make model`` by median turn time among removed listings.

    Only groups with at least ``min_count`` removed listings are ranked, so the
    ordering reflects a repeatable pattern rather than one lucky sale. Ascending
    (fastest first) unless ``slowest`` is True. Fastest turn = strongest demand.
    """
    groups: dict[tuple[str, str], list[int]] = {}
    for car in cars:
        t = turn_time_days(car)
        if t is None:
            continue
        make = (car.get("make") or "").strip()
        model = (car.get("model") or "").strip()
        if not make or not model:
            continue
        groups.setdefault((make, model), []).append(t)
    rows = []
    for (make, model), times in groups.items():
        if len(times) < min_count:
            continue
        rows.append(
            {
                "make": make,
                "model": model,
                "count": len(times),
                "median_turn_days": round(statistics.median(times), 1),
                "mean_turn_days": round(statistics.mean(times), 1),
                "min_turn_days": min(times),
                "max_turn_days": max(times),
            }
        )
    rows.sort(key=lambda r: (r["median_turn_days"], r["mean_turn_days"]), reverse=slowest)
    return rows[:limit]


# --- DB loaders ----------------------------------------------------------------
_ACTIVE_FIELDS = (
    "vin, year, make, model, trim, price, msrp, dealer_name, dealer_id, "
    "dealership_registry_id, first_seen_at, last_price_change_at, "
    "price_provenance_json"
)
_REMOVED_FIELDS = (
    "vin, year, make, model, dealer_name, dealer_id, "
    "first_seen_at, listing_removed_at"
)


def _fetch_dicts(sql: str) -> list[dict[str, Any]]:
    from backend.db.inventory_db import db_conn

    with db_conn(row_factory=dict) as conn:
        cur = conn.cursor(row_factory=dict)
        cur.execute(sql)
        return [dict(r) for r in cur.fetchall()]


def load_active_cars(limit: Optional[int] = None) -> list[dict[str, Any]]:
    """Load active listings with the columns the signal functions need."""
    sql = (
        f"SELECT {_ACTIVE_FIELDS} FROM cars "
        "WHERE COALESCE(listing_active, 1) = 1 AND first_seen_at IS NOT NULL"
    )
    if limit:
        sql += f" LIMIT {int(limit)}"
    return _fetch_dicts(sql)


def load_removed_cars(limit: Optional[int] = None) -> list[dict[str, Any]]:
    """Load removed (sold-proxy) listings for turn-time analysis."""
    sql = (
        f"SELECT {_REMOVED_FIELDS} FROM cars "
        "WHERE COALESCE(listing_active, 1) = 0 "
        "AND first_seen_at IS NOT NULL AND listing_removed_at IS NOT NULL"
    )
    if limit:
        sql += f" LIMIT {int(limit)}"
    return _fetch_dicts(sql)
