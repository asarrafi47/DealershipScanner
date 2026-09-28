"""When a listing's mileage is "not listed" rather than a real odometer figure.

DC-6 / IH-03 (visual review 2026-09-28): "0 mi" printed as fact on a 2011
used RAV4 (car 1434577, ``mileage=0``, condition Used). Across active rows
74,695 carry 0 and 752 of those are Used: for a pre-owned car, or a "new"
car older than last model year, 0 is the feed's "not listed" sentinel. A NULL
is not listed on any car. A new, current car at 0 mi is a real figure.
"""

from __future__ import annotations

from datetime import date
from typing import Any


def _mileage_value(raw: Any) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    s = str(raw).strip().replace(",", "")
    if not s or s in ("—", "-"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def mileage_not_listed(
    mileage: Any,
    *,
    condition: Any = None,
    is_cpo: Any = None,
    year: Any = None,
    today: date | None = None,
) -> bool:
    """True when the page should say "Mileage not listed" instead of a number."""
    mi = _mileage_value(mileage)
    if mi is None:
        return True
    if mi > 0:
        return False
    cond = str(condition or "").strip().lower()
    is_new = cond == "new" and is_cpo not in (1, True, "1")
    if not is_new:
        return True
    try:
        y = int(year)
    except (TypeError, ValueError):
        return False
    return y < (today or date.today()).year - 1
