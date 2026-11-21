"""
Low/high-side price plausibility for ``cars.price`` at write time (N6.7, IL-06, D-N14).

Owner decision D-N14 (2026-10-18): a price under $1,000 is not a price. The clean
room held 234 active cars at $10-$999 (24 dealers; harperacura 89 at $699-799,
ourismanhondaoftysonscorner 36 at $999) and one Accord at $6,001,323. A value
repeated across 5 or more models at one dealer is a placeholder or a monthly payment
and is withheld as well (7 dealer/price pairs, 184 cars; the wider 10-15 rule found
9 pairs / 205 cars).

Same policy as the read-time guard in ``price_plausibility.py``: an implausible
figure is withheld (NULL), never replaced with an invented one, and the reason is
returned so the caller can log it.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable

PRICE_FLOOR = 1_000
PRICE_CAP = 1_000_000
# Distinct models at one dealer sharing one price before the value is withheld.
REPEAT_MIN_MODELS = 5


def classify_price(value: Any) -> tuple[int | None, str | None]:
    """``(price, note)``: price is a positive int inside the floor and cap, else None.

    ``note`` is None for a clean value or a plain missing/unparseable/<= 0 one; it
    names the rule (``below_floor`` / ``above_cap``) when a parsed value was refused.
    """
    try:
        price = int(round(float(value))) if value is not None and str(value).strip() != "" else None
    except (TypeError, ValueError, OverflowError):
        price = None
    if price is None or price <= 0:
        return None, None
    if price < PRICE_FLOOR:
        return None, f"price_below_floor:{price}<{PRICE_FLOOR}"
    if price > PRICE_CAP:
        return None, f"price_above_cap:{price}>{PRICE_CAP}"
    return price, None


def repeated_value_pairs(
    rows: Iterable[tuple[Any, Any, Any]], *, min_models: int = REPEAT_MIN_MODELS
) -> dict[tuple[Any, int], int]:
    """Dealer/price pairs whose price repeats across >= ``min_models`` distinct models.

    ``rows`` are ``(dealer_id, model, price)``. Returns ``{(dealer_id, price): cars}``.
    Prices at or under zero and rows without a dealer or model are ignored.
    """
    models: dict[tuple[Any, int], set[str]] = defaultdict(set)
    cars: dict[tuple[Any, int], int] = defaultdict(int)
    for dealer_id, model, price in rows:
        try:
            p = int(round(float(price)))
        except (TypeError, ValueError):
            continue
        m = str(model or "").strip().lower()
        if p <= 0 or not dealer_id or not m:
            continue
        models[(dealer_id, p)].add(m)
        cars[(dealer_id, p)] += 1
    return {k: cars[k] for k, ms in models.items() if len(ms) >= min_models}
