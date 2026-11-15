"""Which ``cars`` rows a user posted, and when the scanner must leave them alone (card S20).

``cars`` has no "posted by a user" column: a car a seller posted through ``/inventory``
is copied into ``cars`` by ``backend/dealer/portal_sync.py`` with ``dealer_id``
``dealer-portal-<user id>``, and that prefix is the marker (``DESIGN.md`` section 23.6).

This module is the one place the scanner, the reconcile and attribution scripts and the
repositories read that marker. It imports nothing but the standard library, because the
scanner image (``requirements-scanner.txt``) has no Flask and ``backend.dealer`` imports
Flask blueprints. ``portal_sync.py`` keeps its own copy of the prefix for the web side;
``backend/tests/test_scanner_keeps_user_cars.py`` pins the two to the same text.

The rule (``DESIGN.md`` sections 23.4 step 9 and 23.6): no scanned dealer ever takes,
retires, deletes or moves a user's car. A user row that the seller retired (sold,
deleted, expired, hidden) stops blocking its VIN only after ``USER_ROW_GRACE_DAYS``.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

DEALER_PORTAL_DEALER_ID_PREFIX = "dealer-portal-"
USER_ROW_GRACE_DAYS = 7


def is_dealer_portal_dealer_id(dealer_id: Any) -> bool:
    """True for a ``cars.dealer_id`` of a user-posted car (``dealer-portal-<user id>``)."""
    return isinstance(dealer_id, str) and dealer_id.strip().startswith(DEALER_PORTAL_DEALER_ID_PREFIX)


def user_row_cutoff(now: datetime | None = None) -> datetime:
    """A user row retired before this moment no longer blocks its VIN."""
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return now - timedelta(days=USER_ROW_GRACE_DAYS)


def user_row_cutoff_iso(now: datetime | None = None) -> str:
    """The cutoff as text, compared against ``cars.listing_removed_at`` (a TEXT column).

    ``YYYY-MM-DDTHH:MM:SS`` with no zone suffix: every writer of that column stores UTC
    in ISO form (``...+00:00`` from the portal and reconcile, ``...Z`` elsewhere), and
    both sort after this prefix-only text, so the comparison is a plain string order on
    SQLite and Postgres alike.
    """
    return user_row_cutoff(now).replace(tzinfo=None, microsecond=0).isoformat()


def _parse(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00").replace(" ", "T", 1))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def user_row_blocks_vin(owner: Any, listing_active: Any, listing_removed_at: Any,
                        now: datetime | None = None) -> bool:
    """True when ``owner`` is a user and its row still protects the VIN.

    Protected: a live row (``listing_active`` 1 or NULL), or a retired row whose
    ``listing_removed_at`` is missing, unreadable or at most ``USER_ROW_GRACE_DAYS`` old.
    Unknown always protects: a user's car is never lost to a guess.
    """
    if not is_dealer_portal_dealer_id(owner):
        return False
    try:
        active = listing_active is None or int(listing_active) == 1
    except (TypeError, ValueError):
        active = True
    if active:
        return True
    removed = _parse(listing_removed_at)
    return removed is None or removed >= user_row_cutoff(now)


__all__ = [
    "DEALER_PORTAL_DEALER_ID_PREFIX",
    "USER_ROW_GRACE_DAYS",
    "is_dealer_portal_dealer_id",
    "user_row_blocks_vin",
    "user_row_cutoff",
    "user_row_cutoff_iso",
]
