"""C2: the plain sentences (CHAT_LIMITS.md section 7), exactly."""

from __future__ import annotations

from datetime import datetime, timezone

from backend.chat_budget import messages as m

UTC = timezone.utc
NY = "America/New_York"
# 2:35 PM Eastern on Nov 13 2026 (EST, UTC-5)
NOW = datetime(2026, 11, 13, 19, 35, tzinfo=UTC)
TOP = datetime(2026, 11, 13, 20, 0, tzinfo=UTC)  # 3:00 PM Eastern
MIDNIGHT = datetime(2026, 11, 14, 5, 0, tzinfo=UTC)  # 12:00 AM Eastern


def test_hour_limit_sentence():
    assert m.hour_limit_text(10, TOP, NOW, NY) == (
        "You have used your 10 questions for this hour. You can ask again at 3:00 PM Eastern, "
        "in about 25 minutes."
    )


def test_day_limit_sentence():
    assert m.day_limit_text(40, MIDNIGHT, NOW, NY) == (
        "You have used your 40 questions for today. They come back at 12:00 AM Eastern, "
        "in about 9 hours."
    )


def test_car_limit_sentence():
    assert m.listing_limit_text(10, MIDNIGHT, NY) == (
        "You have asked 10 questions about this car today. You can ask about other cars, "
        "or come back tomorrow at 12:00 AM Eastern."
    )


def test_budget_sentence():
    assert m.budget_text(MIDNIGHT, NY) == (
        "The assistant has reached its budget for today and will be back at 12:00 AM Eastern. "
        "Searching and browsing still work."
    )


def test_paused_sentence():
    assert m.PAUSED_TEXT == "The assistant is paused for now. Searching and browsing still work."


def test_away_text_rounding():
    assert m.away_text(NOW.replace(minute=36), NOW) == "1 minute"
    assert m.away_text(NOW.replace(minute=35, second=30), NOW) == "1 minute"
    assert m.away_text(TOP, NOW) == "25 minutes"
    assert m.away_text(MIDNIGHT, NOW) == "9 hours"


def test_error_codes_match_the_design():
    assert (m.ERR_USER, m.ERR_LISTING, m.ERR_BUDGET, m.ERR_PAUSED) == (
        "user_chat_limit_reached",
        "listing_chat_limit_reached",
        "chat_budget_reached",
        "chat_paused",
    )


def test_noon_and_pm_formatting():
    noon = datetime(2026, 11, 13, 17, 0, tzinfo=UTC)
    assert m.clock_text(noon, NY) == "12:00 PM Eastern"
    assert m.clock_text(datetime(2026, 7, 4, 13, 0, tzinfo=UTC), NY) == "9:00 AM Eastern"
