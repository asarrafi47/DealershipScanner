"""Budget alert lines and the retention fallback inside the web process (card C6, CHAT_LIMITS.md section 9).

The gate calls :func:`note_day_total` after every accepted message with the site-wide day row it
just wrote. The first time a day passes each alert level (``chat.alert_pcts``, default 50 and 80)
one ``chat_budget`` log line is written; the first refusal by the site budget writes one more.
Lines carry the local date, a percentage and totals only: no message text, name, phone number,
hash or address, and every number is printed with thousands separators so no line holds a digit
run of 7 or more. Memory per process (the site runs ONE gunicorn worker); a restart may repeat a
line once, which is harmless.

:func:`maybe_purge` deletes rows older than 35 days at most once an hour per process, as the
fallback for the nightly job (``backend/scripts/chat_budget_nightly.py``).
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
from typing import Callable

from backend import settings_store as ss
from backend.db.repositories import chat_usage_repo as repo

_log = logging.getLogger("backend.chat_budget")

PURGE_EVERY_S = 3600.0

_lock = threading.Lock()
_seen: set[tuple[str, int]] = set()
_last_purge: float | None = None
_clock: Callable[[], float] = time.monotonic


def reset() -> None:
    """Tests: forget which lines were written and when the last purge ran."""
    global _last_purge
    with _lock:
        _seen.clear()
        _last_purge = None


def set_clock(fn: Callable[[], float] | None) -> None:
    """Tests: a fake monotonic clock for the purge interval (None restores the real one)."""
    global _clock
    _clock = fn or time.monotonic
    reset()


def budget_pct(tokens: int, cost_micro: int, *, tokens_limit: int, cents_limit: int) -> int:
    """Percent of the day's budget used: the larger of the token share and the dollar share (floored)."""
    shares = [0]
    if tokens_limit > 0:
        shares.append(int(tokens) * 100 // tokens_limit)
    if cents_limit > 0:
        shares.append(int(cost_micro) * 100 // (cents_limit * 10_000))
    return max(shares)


def dollars(cost_micro: int) -> str:
    return f"${int(cost_micro) / 1_000_000:,.2f}"


def _day_label(now: datetime, tz: str) -> str:
    return repo.day_start(now, tz).astimezone(repo._zone(tz)).date().isoformat()


def _first(day: str, level: int) -> bool:
    with _lock:
        if (day, level) in _seen:
            return False
        for old in [m for m in _seen if m[0] != day]:  # keep only today's marks
            _seen.discard(old)
        _seen.add((day, level))
        return True


def note_day_total(global_day: tuple[int, int, int] | None, *, now: datetime, tz: str) -> None:
    """Write one line the first time today passes each alert level. Never raises."""
    if not global_day:
        return
    try:
        messages, tokens, cost = global_day
        pct = budget_pct(tokens, cost, tokens_limit=ss.get_int("chat.budget_tokens_per_day"),
                         cents_limit=ss.get_int("chat.budget_usd_per_day_cents"))
        day = _day_label(now, tz)
        for level in sorted(ss.get("chat.alert_pcts") or ()):
            if pct >= int(level) and _first(day, int(level)):
                _log.warning(
                    "chat_budget: day %s passed %d%% of the budget (now %d%%; messages %s, tokens %s, %s)",
                    day, int(level), pct, f"{int(messages):,}", f"{int(tokens):,}", dollars(cost),
                )
    except Exception as exc:  # an alert line must never break a chat message
        _log.error("chat_budget: alert check failed (%s)", type(exc).__name__)


def note_budget_reached(*, now: datetime, tz: str) -> None:
    """Write one line the first time today the site budget refuses a message. Never raises."""
    try:
        day = _day_label(now, tz)
        if _first(day, 100):
            _log.warning("chat_budget: day %s reached the budget; chat is stopped until midnight", day)
    except Exception as exc:
        _log.error("chat_budget: alert check failed (%s)", type(exc).__name__)


def maybe_purge(*, now: datetime) -> None:
    """Delete rows older than the retention at most once an hour per process. Never raises."""
    global _last_purge
    t = _clock()
    with _lock:
        if _last_purge is not None and t - _last_purge < PURGE_EVERY_S:
            return
        _last_purge = t
    try:
        n = repo.delete_older_than(now=now)
        if n:
            _log.info("chat_budget: removed %s rows older than %d days", f"{n:,}", repo.RETENTION_DAYS)
    except Exception as exc:
        _log.error("chat_budget: retention purge failed (%s)", type(exc).__name__)
