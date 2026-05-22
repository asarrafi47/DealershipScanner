"""Car-page chat: when Playwright / live web research may run for a request."""

from __future__ import annotations

import os

from backend.utils.runtime_env import is_production_env


def web_research_playwright_allowed(session_user_id: int | None) -> bool:
    """
    Whether Playwright (Brave search → follow link) may run for car chat.

    Does not affect pgvector model-knowledge cache reads (cheap, no browser).

    Environment
    -----------
    CAR_CHAT_WEB_RESEARCH
        unset / empty / ``auto`` — development: allow Playwright. Production: allow
        only if the user has ``session['user_id']`` **or** ``CAR_CHAT_WEB_RESEARCH_PUBLIC=1``.
    ``0`` / ``false`` / ``no`` / ``off`` — never run Playwright.
    ``1`` / ``true`` / ``yes`` / ``on`` — always allow Playwright (still filtered by
        ``WEB_RESEARCH_ALLOWED_HOSTS`` and built-in href blocklist when configured).

    CAR_CHAT_WEB_RESEARCH_PUBLIC
        When ``1`` and policy mode is ``auto`` in production, anonymous users may
        trigger Playwright (same as legacy behavior; higher abuse risk).
    """
    raw = (os.environ.get("CAR_CHAT_WEB_RESEARCH") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    if raw in ("1", "true", "yes", "on"):
        return True

    if not is_production_env():
        return True

    pub = (os.environ.get("CAR_CHAT_WEB_RESEARCH_PUBLIC") or "").strip().lower()
    if pub in ("1", "true", "yes", "on"):
        return True

    return session_user_id is not None


def car_chat_listing_daily_limit() -> int:
    """Max chat messages allowed per listing per 24h (0 = disabled)."""
    try:
        return max(0, int(os.environ.get("CAR_CHAT_MAX_PER_LISTING_DAILY", "10")))
    except (TypeError, ValueError):
        return 10


def car_chat_rate_limits() -> tuple[int, int, int]:
    """
    Return (per_ip_and_car_per_min, per_ip_all_cars_per_min, global_per_min).

    ``global_per_min`` of 0 disables the deployment-wide sliding window.
    """
    try:
        per_pair = max(1, int(os.environ.get("RATE_LIMIT_CAR_CHAT_PER_MIN", "24")))
    except (TypeError, ValueError):
        per_pair = 24
    try:
        per_ip = max(1, int(os.environ.get("RATE_LIMIT_CAR_CHAT_PER_IP_PER_MIN", "48")))
    except (TypeError, ValueError):
        per_ip = 48
    try:
        global_rpm = max(0, int(os.environ.get("RATE_LIMIT_CAR_CHAT_GLOBAL_PER_MIN", "0")))
    except (TypeError, ValueError):
        global_rpm = 0
    return per_pair, per_ip, global_rpm
