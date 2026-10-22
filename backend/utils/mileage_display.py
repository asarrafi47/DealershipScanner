"""When a listing's mileage is "not listed" rather than a real odometer figure,
and when the page prints no mileage line at all.

DC-6 / IH-03 (visual review 2026-09-28): "0 mi" printed as fact on a 2011
used RAV4 (car 1434577, ``mileage=0``, condition Used). Across active rows
74,695 carry 0 and 752 of those are Used: for a pre-owned car, or a "new"
car older than last model year, 0 is the feed's "not listed" sentinel. A NULL
is not listed on any car. A new, current car at 0 mi is a real figure.

D-N12 (owner decision 2026-10-18, IL-01): a New, non-CPO car with no odometer
value is complete, and the page shows no mileage line for it; used and CPO
cars keep "Mileage not listed". "New, non-CPO" is "not
:func:`is_used_or_certified`", the predicate ``listing_completeness`` uses for
the ``mileage`` code, so the page and the index agree on which cars those are.
One exception is display-only: a "new" car older than last model year keeps
"Mileage not listed" (:func:`mileage_line_hidden`).
"""

from __future__ import annotations

from datetime import date
from typing import Any

# Condition tokens that mark a car sold as used or certified (substring match on
# the lowered condition text; "carbravo" is GM's CPO program name).
_USED_OR_CERTIFIED_TOKENS = ("used", "certif", "pre-owned", "preowned", "pre owned", "cpo", "carbravo")


def is_used_or_certified(condition: Any, is_cpo: Any = None) -> bool:
    """True when ``is_cpo`` is set or the condition carries a used / certified token.

    Anything else (New, blank, or another value such as GM's ``ctp`` courtesy
    transport or ``demo``) is not used / certified. ``listing_completeness``
    passes the raw ``condition`` column; the display surfaces pass the condition
    the page shows (the serializer may derive one for a blank column).
    """
    if is_cpo in (1, True, "1"):
        return True
    cond = str(condition or "").strip().lower()
    return any(tok in cond for tok in _USED_OR_CERTIFIED_TOKENS)


def odometer_absent(mileage: Any) -> bool:
    """No odometer value at all: NULL or an empty / whitespace string (0 is a value)."""
    return mileage is None or (isinstance(mileage, str) and not mileage.strip())


def _older_than_last_model_year(year: Any, today: date | None) -> bool:
    try:
        y = int(year)
    except (TypeError, ValueError):
        return False
    return y < (today or date.today()).year - 1


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
    """True when the page should say "Mileage not listed" instead of a number.

    Check :func:`mileage_line_hidden` first: when it is True the page prints no
    mileage line, whatever this returns.
    """
    mi = _mileage_value(mileage)
    if mi is None:
        return True
    if mi > 0:
        return False
    cond = str(condition or "").strip().lower()
    is_new = cond == "new" and is_cpo not in (1, True, "1")
    if not is_new:
        return True
    return _older_than_last_model_year(year, today)


def mileage_line_hidden(
    mileage: Any,
    *,
    condition: Any = None,
    is_cpo: Any = None,
    year: Any = None,
    today: date | None = None,
) -> bool:
    """True when the page prints no mileage line at all (D-N12).

    That is a car with no odometer value (:func:`odometer_absent`) that is not
    used / certified (:func:`is_used_or_certified`), unless its model year is
    older than last year: an old "new" car keeps "Mileage not listed", as it
    does at 0, because the New label is the doubtful part. A 0 is a value and
    never hides the line (:func:`mileage_not_listed` decides "0 mi" or "Mileage
    not listed"), and neither does a present but unparseable value, which
    ``listing_completeness`` still counts as missing.
    """
    if not odometer_absent(mileage):
        return False
    if is_used_or_certified(condition, is_cpo):
        return False
    return not _older_than_last_model_year(year, today)
