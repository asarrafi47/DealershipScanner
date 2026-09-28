"""Dealer Google rating as the car page prints it (DC-8, visual review 2026-09-28).

The Dealership tab rounded 4.8 to five filled stars, kept the number only in an
``aria-label``, added a trophy-emoji "Top Rated Dealer" badge and gave no fetch
date (the newest fetch across all dealers was 2026-07-29). The page now prints
"4.8 · 13,610 Google reviews · as of 18 Jul 2026" with the stars filled to the
fraction, and no badge.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


def _as_of(raw: Any) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        dt = raw
    else:
        s = str(raw).strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            return None
    return f"{dt.day} {dt:%b %Y}"


def dealer_rating_display(dealer: dict[str, Any] | None) -> dict[str, Any] | None:
    """``{"rating", "fill_pct", "reviews", "as_of"}`` from a dealerships row, or None."""
    if not dealer:
        return None
    try:
        rating = float(dealer.get("google_rating"))
    except (TypeError, ValueError):
        return None
    if not (0 <= rating <= 5):
        return None
    reviews = None
    try:
        n = int(dealer.get("google_review_count"))
        if n >= 0:
            reviews = f"{n:,} Google review" + ("" if n == 1 else "s")
    except (TypeError, ValueError):
        pass
    return {
        "rating": f"{rating:.1f}",
        "fill_pct": round(rating / 5 * 100, 1),
        "reviews": reviews or "Google rating",
        "as_of": _as_of(dealer.get("google_rating_fetched_at")),
    }
