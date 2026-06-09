"""
Dealer Google rating cache — read-only on web requests; writes via cron/scanner only.
"""
from __future__ import annotations

from typing import Any


def dealer_info_with_cached_google_rating(dealer_info: dict[str, Any] | None) -> dict[str, Any] | None:
    """Return dealer_info unchanged (ratings come from the dealerships registry cache)."""
    return dealer_info
