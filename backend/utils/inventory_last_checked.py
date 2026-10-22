""""Inventory last checked <date>" on the dealer page and the car page (N3.11).

Owner decision D-N19 (2026-10-18): while prod inventory is stale, say so. One
plain line of text per page, never a card or box. The date is the newest
``cars.scraped_at`` across the dealer's rows; the car page shows its dealer's
date, not its own row's. Data under 48 hours old needs no caveat, so the line
is hidden then, and when no date is on file. Both pages put the result in the
template context as ``inventory_last_checked`` (``None`` hides the line).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from backend.utils.first_seen import _parse

logger = logging.getLogger(__name__)

# The line shows once the dealer's newest scrape is at least this old.
FRESH_WINDOW = timedelta(hours=48)


def last_checked_label(scraped_at: Any, *, now: datetime | None = None) -> str | None:
    """The date to print ("Sep 28, 2026", UTC), or ``None`` when the line is hidden.

    Hidden when *scraped_at* is missing or unparseable, or under ``FRESH_WINDOW``
    old against the real current UTC time (*now* is for tests). A date in the
    future is under the window too. The date shape is the first-seen line's.
    """
    dt = _parse(scraped_at)
    if dt is None:
        return None
    dt = dt.astimezone(timezone.utc)
    ref = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if ref - dt < FRESH_WINDOW:
        return None
    return f"{dt:%b} {dt.day}, {dt.year}"


def dealer_last_checked_label(dealer_id: Any, *, now: datetime | None = None) -> str | None:
    """:func:`last_checked_label` for one dealer's newest scrape (one indexed read).

    The line is a caveat, not page content: a failed read hides it and is
    logged, and never takes the page down with it.
    """
    did = str(dealer_id or "").strip()
    if not did:
        return None
    try:
        from backend.db.inventory_db import dealer_last_scraped_at

        value = dealer_last_scraped_at(did)
    except Exception:
        logger.warning("inventory last-checked read failed for dealer %r", did, exc_info=True)
        return None
    return last_checked_label(value, now=now)
