"""Editable settings: row in ``app_settings`` > environment variable > code default (card C1).

Read by the chat gate (and later by other features). A value is read at most every 30
seconds per worker, so a change made in the database takes effect within a minute. A bad
value (not a number, negative, absurdly large, unknown time zone) is ignored with one log
line (key and reason only, never the value) and the next level is used, so a typo can never
lock anyone out. If the database cannot be read, or the table does not exist yet, the
environment and the defaults are used.

No Flask import: plain values only.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable
from zoneinfo import ZoneInfo

_log = logging.getLogger(__name__)

CACHE_SECONDS = 30.0
_TRUE = ("1", "true", "yes", "on")
_FALSE = ("0", "false", "no", "off")


@dataclass(frozen=True)
class Spec:
    key: str
    env: str
    default: Any
    kind: str = "int"  # int | bool | tz | pcts
    max_value: int = 1_000_000  # an int above this is absurd and ignored


SPECS: dict[str, Spec] = {
    s.key: s
    for s in (
        Spec("chat.enabled", "CHAT_ENABLED", True, "bool"),
        Spec("chat.user_per_hour", "CHAT_USER_PER_HOUR", 10),
        Spec("chat.user_per_day", "CAR_CHAT_MAX_PER_USER_DAILY", 40),
        Spec("chat.listing_per_day", "CAR_CHAT_MAX_PER_LISTING_DAILY", 10),
        Spec("chat.phone_per_day", "CHAT_PHONE_PER_DAY", 40),
        Spec("chat.device_per_day", "CHAT_DEVICE_PER_DAY", 60),
        Spec("chat.ip_per_day", "CHAT_IP_PER_DAY", 100),
        Spec("chat.new_account_factor_pct", "CHAT_NEW_ACCOUNT_FACTOR_PCT", 50, max_value=100),
        Spec("chat.budget_usd_per_day_cents", "CHAT_BUDGET_CENTS_PER_DAY", 500, max_value=100_000_000),
        Spec("chat.budget_tokens_per_day", "CHAT_BUDGET_TOKENS_PER_DAY", 2_000_000, max_value=10_000_000_000),
        # 0 = work it out from the price settings, or reserve no cost when prices are unset
        Spec("chat.est_cost_micro_per_message", "CHAT_EST_COST_MICRO", 0, max_value=100_000_000),
        Spec("chat.est_tokens_in_per_message", "CHAT_EST_TOKENS_IN", 2500, max_value=1_000_000),
        Spec("chat.est_tokens_out_per_message", "CHAT_EST_TOKENS_OUT", 500, max_value=1_000_000),
        Spec("chat.price_in_micro_per_mtok", "CHAT_PRICE_IN_MICRO_PER_MTOK", 0, max_value=1_000_000_000),
        Spec("chat.price_out_micro_per_mtok", "CHAT_PRICE_OUT_MICRO_PER_MTOK", 0, max_value=1_000_000_000),
        Spec("chat.web_research_weight", "CHAT_WEB_RESEARCH_WEIGHT", 3, max_value=100),
        Spec("chat.reset_tz", "CHAT_RESET_TZ", "America/New_York", "tz"),
        Spec("chat.alert_pcts", "CHAT_ALERT_PCTS", (50, 80), "pcts"),
    )
}

_lock = threading.Lock()
_cache: dict[str, Any] = {"at": None, "rows": {}}
_clock: Callable[[], float] = time.monotonic
_loader: Callable[[], dict[str, str]] | None = None  # tests can replace the database read
_logged: set[tuple[str, str]] = set()


def reset_cache() -> None:
    with _lock:
        _cache["at"] = None
        _cache["rows"] = {}
        _logged.clear()


def set_clock(fn: Callable[[], float] | None) -> None:
    """Tests: a fake monotonic clock (None restores the real one)."""
    global _clock
    _clock = fn or time.monotonic
    reset_cache()


def set_loader(fn: Callable[[], dict[str, str]] | None) -> None:
    """Tests: replace the database read (None restores the real one)."""
    global _loader
    _loader = fn
    reset_cache()


def _note(key: str, why: str) -> None:
    mark = (key, why)
    if mark in _logged:
        return
    _logged.add(mark)
    _log.warning("settings_store: ignoring a bad value for %s (%s); using the next level", key, why)


def parse(spec: Spec, raw: Any) -> Any:
    """The validated value, or None when ``raw`` is not usable (caller falls to next level)."""
    text = str(raw).strip()
    if spec.kind == "bool":
        low = text.lower()
        if low in _TRUE:
            return True
        if low in _FALSE:
            return False
        return None
    if spec.kind == "tz":
        try:
            ZoneInfo(text)
        except Exception:
            return None
        return text
    if spec.kind == "pcts":
        out: list[int] = []
        for piece in text.split(","):
            piece = piece.strip()
            if not piece:
                continue
            if not piece.isdigit() or not (1 <= int(piece) <= 1000):
                return None
            out.append(int(piece))
        if not out or len(out) > 10:
            return None
        return tuple(sorted(set(out)))
    # int
    if not text.isdigit():  # rejects "", "-1", "1.5", "abc"
        return None
    n = int(text)
    if n > spec.max_value:
        return None
    return n


def _load_rows() -> dict[str, str]:
    """All app_settings rows, cached for CACHE_SECONDS. Errors give an empty mapping."""
    now = _clock()
    with _lock:
        at = _cache["at"]
        if at is not None and now - at < CACHE_SECONDS:
            return _cache["rows"]
    rows: dict[str, str] = {}
    try:
        if _loader is not None:
            rows = dict(_loader())
        else:
            from backend.db.connect import inventory_dsn

            if inventory_dsn(require=False):
                from backend.db.repositories.chat_usage_repo import load_app_settings

                rows = load_app_settings()
    except Exception as exc:  # missing table, database down: environment and defaults apply
        _note("app_settings", type(exc).__name__)
        rows = {}
    with _lock:
        _cache["at"] = now
        _cache["rows"] = rows
    return rows


def resolve(key: str) -> tuple[Any, str]:
    """(value, source) where source is ``row``, ``env`` or ``default``."""
    spec = SPECS[key]
    row = _load_rows().get(key)
    if row is not None:
        val = parse(spec, row)
        if val is not None:
            return val, "row"
        _note(key, "settings row")
    env = os.environ.get(spec.env)
    if env is not None and env.strip() != "":
        val = parse(spec, env)
        if val is not None:
            return val, "env"
        _note(key, "environment")
    return spec.default, "default"


def get(key: str) -> Any:
    return resolve(key)[0]


def get_int(key: str) -> int:
    return int(get(key))


def get_bool(key: str) -> bool:
    return bool(get(key))


def get_str(key: str) -> str:
    return str(get(key))


def all_settings() -> dict[str, dict[str, Any]]:
    """Every setting with its current value and source (for the admin page, card C5)."""
    out: dict[str, dict[str, Any]] = {}
    for key in SPECS:
        value, source = resolve(key)
        out[key] = {"value": value, "source": source, "env": SPECS[key].env}
    return out
