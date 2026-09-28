""""First seen by Sarrafi Cars on <date> (N days)" for the car page.

IH-09 / DC-3 / ES-3 (visual review 2026-09-28): the Overview printed "Days on
Market: 0" when the number is days since OUR scanner first saw the row
(``first_seen_at``; 18,873 of 214,678 active rows were first seen that day), a
"Fresh inventory: firm pricing" verdict derived from it, and a "Predictive
Local Turnaround" that was a hard-coded segment + brand constant. What we know
is when we first saw the listing, so that is what the page says, with its
date. The leverage badge needs at least a week of observation behind it.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

BADGE_MIN_DAYS = 7


def _parse(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        s = str(value).strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s.replace(" ", "T", 1) if "T" not in s else s)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _badge(days: int) -> dict[str, str] | None:
    if days < BADGE_MIN_DAYS:
        return None
    if days > 60:
        return {"cls": "high-leverage", "label": "Aged inventory: over 60 days"}
    if days >= 31:
        return {"cls": "mid-leverage", "label": "Over 30 days on our radar"}
    return {"cls": "recent-listing", "label": "Under a month on our radar"}


def first_seen_fields(row: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
    """``first_seen_iso`` / ``first_seen_date`` / ``first_seen_days`` / ``first_seen_badge``.

    Reads ``first_seen_at``, else ``created_at`` (the row's insert time, which is
    also when we first saw it). Never ``scraped_at``: that is the LAST scan.
    All four are None when neither is on file, so the page can say so.
    """
    dt = _parse(row.get("first_seen_at")) or _parse(row.get("created_at"))
    if dt is None:
        return {"first_seen_iso": None, "first_seen_date": None, "first_seen_days": None, "first_seen_badge": None}
    dt = dt.astimezone(timezone.utc)
    ref = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    days = max(0, (ref.date() - dt.date()).days)
    return {
        "first_seen_iso": dt.isoformat(),
        "first_seen_date": f"{dt:%b} {dt.day}, {dt.year}",
        "first_seen_days": days,
        "first_seen_badge": _badge(days),
    }
