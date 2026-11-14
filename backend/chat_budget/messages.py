"""Plain sentences for a refused chat message (CHAT_LIMITS.md section 7).

No Flask, no database. Times are written in the reset time zone ("3:00 PM Eastern") with how
long that is away ("in about 25 minutes"). A denial never says which hidden counter (phone,
device, address) was hit and never mentions other accounts.
"""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

_ZONE_NAMES = {
    "America/New_York": "Eastern",
    "America/Detroit": "Eastern",
    "America/Chicago": "Central",
    "America/Denver": "Mountain",
    "America/Phoenix": "Mountain",
    "America/Los_Angeles": "Pacific",
    "America/Anchorage": "Alaska",
    "Pacific/Honolulu": "Hawaii",
    "UTC": "UTC",
}

# error codes in the JSON the chat panel receives
ERR_USER = "user_chat_limit_reached"
ERR_LISTING = "listing_chat_limit_reached"
ERR_BUDGET = "chat_budget_reached"
ERR_PAUSED = "chat_paused"
ERR_UNAVAILABLE = "chat_unavailable"


def zone_label(tz_name: str) -> str:
    if tz_name in _ZONE_NAMES:
        return _ZONE_NAMES[tz_name]
    try:
        return datetime.now(ZoneInfo(tz_name)).strftime("%Z") or tz_name
    except Exception:
        return tz_name


def clock_text(when: datetime, tz_name: str) -> str:
    """'3:00 PM Eastern' (no leading zero on the hour)."""
    try:
        local = when.astimezone(ZoneInfo(tz_name))
    except Exception:
        local = when.astimezone(timezone.utc)
        tz_name = "UTC"
    hour = local.hour % 12 or 12
    suffix = "AM" if local.hour < 12 else "PM"
    return f"{hour}:{local.minute:02d} {suffix} {zone_label(tz_name)}"


def away_text(when: datetime, now: datetime) -> str:
    """'25 minutes' or '6 hours': how far away ``when`` is (rounded up under an hour)."""
    seconds = max(0, int((when - now).total_seconds()))
    if seconds < 3600:
        minutes = max(1, -(-seconds // 60))
        return f"{minutes} minute" + ("" if minutes == 1 else "s")
    hours = max(1, round(seconds / 3600))
    return f"{hours} hour" + ("" if hours == 1 else "s")


def questions(n: int) -> str:
    """'1 question' / '5 questions'."""
    return f"{n} question" + ("" if n == 1 else "s")


def hour_limit_text(limit: int, resets_at: datetime, now: datetime, tz_name: str) -> str:
    return (
        f"You have used your {questions(limit)} for this hour. You can ask again at "
        f"{clock_text(resets_at, tz_name)}, in about {away_text(resets_at, now)}."
    )


def day_limit_text(limit: int, resets_at: datetime, now: datetime, tz_name: str) -> str:
    return (
        f"You have used your {questions(limit)} for today. They come back at "
        f"{clock_text(resets_at, tz_name)}, in about {away_text(resets_at, now)}."
    )


def listing_limit_text(limit: int, resets_at: datetime, tz_name: str) -> str:
    return (
        f"You have asked {questions(limit)} about this car today. You can ask about other "
        f"cars, or come back tomorrow at {clock_text(resets_at, tz_name)}."
    )


def budget_text(resets_at: datetime, tz_name: str) -> str:
    return (
        "The assistant has reached its budget for today and will be back at "
        f"{clock_text(resets_at, tz_name)}. Searching and browsing still work."
    )


PAUSED_TEXT = "The assistant is paused for now. Searching and browsing still work."
UNAVAILABLE_TEXT = "Chat is not available right now. Searching and browsing still work."
