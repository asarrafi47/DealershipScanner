"""Which ``cars.dealer_id`` values belong to a person's own listing (S20).

A car posted through the dealer portal is stored in ``cars`` under ``dealer-portal-<user id>``.
The scanner must never roster, claim, delete or re-attribute such a row, so the scanner, the
upsert guard and the repair scripts all ask this one helper.

Standard library only, on purpose: the scanner image has no Flask, and importing
``backend.dealer`` (whose package ``__init__`` loads a blueprint) from the scanner would fail
there. ``backend.dealer.portal_sync`` re-exports these names, so existing imports still work.
"""
from __future__ import annotations

from typing import Any

# Namespace prefix for cars.dealer_id on portal-sourced rows, so they can
# never collide with a real scanned rooftop's dealer_id and are easy to spot.
DEALER_PORTAL_DEALER_ID_PREFIX = "dealer-portal-"

# SQL LIKE pattern for the same prefix (no wildcard characters in the prefix itself).
DEALER_PORTAL_LIKE = DEALER_PORTAL_DEALER_ID_PREFIX + "%"


def dealer_portal_dealer_id(user_id: int) -> str:
    return f"{DEALER_PORTAL_DEALER_ID_PREFIX}{int(user_id)}"


def is_dealer_portal_dealer_id(dealer_id: Any) -> bool:
    return isinstance(dealer_id, str) and dealer_id.startswith(DEALER_PORTAL_DEALER_ID_PREFIX)
