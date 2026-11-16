"""The Flask side of the chat gate (card C3): what every chat route calls, in one place.

``gate.py`` stays free of Flask (plain values in). This module reads the request and the
session and turns them into those plain values:

* :func:`burst_allowed` is the ONE per-minute burst guard used by car chat, compare chat,
  sidebar chat and narration. It is in memory today (``backend.utils.ip_rate_limit``); card R3
  swaps its backend here, in one place. The daily money buckets never live here: they are in
  Postgres, through :func:`open_guard`.
* :func:`open_guard` builds ``chat_budget.guard(...)`` for the signed-in account (or none),
  the device cookie hash, the real client address hash and its /24 (IPv4) or /48 (IPv6)
  network hash. The phone hash joins in card C4.
* :func:`denial_response` is the 429 (or 503) JSON a refusal answers with: the existing
  ``{"ok": false, "error": ...}`` shape plus ``message`` and ``resets_at``.
* :func:`sticker_guard` wraps the window-sticker model calls (``window_sticker_service``) when
  they run for a shopper's request, even inside the packages worker thread (the request's
  subjects are carried in a context variable copied into that thread).
"""

from __future__ import annotations

import contextlib
import contextvars
import ipaddress
import logging
import os
from typing import Any, Callable, Iterator

from backend.chat_budget.gate import ChatDenied, Denial, Guard, guard

_log = logging.getLogger(__name__)

# kind names passed to guard(...) by each route (one shared set of buckets for all of them)
KIND_CAR = "car"
KIND_COMPARE = "compare"
KIND_SIDEBAR = "sidebar"
KIND_NARRATE = "narrate"
KIND_STICKER = "sticker"
STICKER_WEIGHT = 3

# burst scopes (per-minute, in memory)
BURST_CAR = "car"
BURST_COMPARE = "compare"
BURST_SIDEBAR = "sidebar"
BURST_NARRATE = "narrate"


# ---------------------------------------------------------------- per-minute burst guard


def _narrate_global_rpm() -> int:
    """Global cap on narration calls per minute (``RATE_LIMIT_NARRATE_GLOBAL_PER_MIN``, default 300; 0 = off)."""
    raw = os.environ.get("RATE_LIMIT_NARRATE_GLOBAL_PER_MIN")
    if raw is None or not str(raw).strip():
        return 300
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 300


def _allow(key: str, max_events: int) -> bool:
    """The single backend call of the burst guard (in memory today; card R3 moves it to Redis)."""
    from backend.utils.ip_rate_limit import allow_request

    return allow_request(key, max_events=max_events, window_seconds=60.0)


def burst_allowed(scope: str, ip: str, pair: str | None = None) -> bool:
    """Per-minute burst guard shared by the four chat routes. True when the request may go on.

    The bucket names and numbers are the ones each route used before card C3, so nothing a
    shopper sees changes: global (off by default), per address, and per address and car (car
    chat) or per address on compare chat. Narration keeps its own global cap (default 300).
    """
    from backend.utils.car_chat_policy import car_chat_rate_limits

    rpm_pair, rpm_ip, rpm_global = car_chat_rate_limits()
    if scope == BURST_NARRATE:
        rpm = _narrate_global_rpm()
        if rpm > 0 and not _allow("narrate:global", rpm):
            return False
        return _allow(f"narrate:ip:{ip}", max(rpm_ip, 30))
    prefix = "aichat" if scope == BURST_SIDEBAR else "chat"
    if rpm_global > 0 and not _allow(f"{prefix}:global", rpm_global):
        return False
    if not _allow(f"{prefix}:ip:{ip}", rpm_ip):
        return False
    if scope == BURST_CAR and pair is not None:
        return _allow(f"chat:{ip}:{pair}", rpm_pair)
    if scope == BURST_COMPARE:
        return _allow(f"chat:compare:ip:{ip}", rpm_pair)
    return True


# ---------------------------------------------------------------- subjects and the guard


def network_of(ip: str | None) -> str | None:
    """The /24 (IPv4) or /48 (IPv6) network an address belongs to, as text; None for junk."""
    try:
        addr = ipaddress.ip_address((ip or "").strip())
    except ValueError:
        return None
    if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped is not None:
        addr = addr.ipv4_mapped
    bits = 24 if isinstance(addr, ipaddress.IPv4Address) else 48
    return str(ipaddress.ip_network((addr, bits), strict=False))


def request_ip() -> str:
    """The real client address counted for chat (Cloudflare-aware, IPv6 by /64)."""
    from flask import request

    from backend.utils.client_ip import phone_client_ip

    return phone_client_ip(request)


def _subjects(*, new_device: bool = True) -> dict[str, Any]:
    """Plain values for guard(...) from the current request (no message text, no phone number).

    ``new_device=False`` reads the device cookie without making one (the packages route is not a
    chat route, so it never sets the cookie; a missing cookie simply means no device counter).
    """
    from flask import request

    from backend.auth.phone import numbers
    from backend.billing import access as paid_access
    from backend.utils import device_id

    ctx = paid_access.current_access()
    uid = ctx.user_id if ctx.logged_in and ctx.user_id else None
    ip = request_ip()
    net = network_of(ip)
    try:
        did = device_id.ensure_device_cookie(request) if new_device else device_id.read_device_id(request)
        dev = device_id.device_hash(did) if did else None
        ip_hash = numbers.hash_ip(ip)
        net_hash = numbers.hash_value("ipnet", net) if net else None
    except Exception:  # the pepper is missing in production: the gate itself refuses (fails closed)
        dev = ip_hash = net_hash = None
    return {
        "user_id": uid,
        "phone_hash": None,  # card C4
        "device_hash": dev,
        "ip_hash": ip_hash,
        "net_hash": net_hash,
        "is_admin": bool(ctx.is_admin),
    }


def open_guard(kind: str, *, listing_id: int | str | None = None, weight: int = 1) -> Guard:
    """``chat_budget.guard(...)`` for this request; use it as ``with open_guard(...) as g:``."""
    s = _subjects()
    return guard(
        kind,
        s["user_id"],
        s["phone_hash"],
        s["device_hash"],
        s["ip_hash"],
        listing_id,
        weight,
        net_hash=s["net_hash"],
        is_admin=s["is_admin"],
    )


def message_weight(message: str, allow_web_research: bool) -> int:
    """How many messages one question counts as: the web-research weight (setting, default 3) when
    the answer may go to the web (the same test the assistant uses before it researches), else 1."""
    if not allow_web_research:
        return 1
    from backend.chat_budget.gate import web_research_weight
    from backend.intelligence.ai.agent import _is_price_question, _needs_web_research

    if _needs_web_research(message) and not _is_price_question(message):
        return web_research_weight()
    return 1


def give_back_if_unanswered(chat_guard: Any, out: dict) -> None:
    """The assistant caught its own model error and answered with an error and no text: the shopper
    got no answer, so the message is given back (CHAT_LIMITS.md rule 9; the gate's hourly cap on
    give-backs still applies). Tokens really spent stay counted."""
    if out.get("error") and not str(out.get("reply") or "").strip():
        chat_guard.fail()


def denial_response(denial: Denial):
    """``(json, status)``: the old ``{"ok": false, "error": ...}`` plus ``message`` and ``resets_at``."""
    from flask import jsonify

    return jsonify({"ok": False, **denial.to_json()}), denial.status


# ---------------------------------------------------------------- window sticker calls

# Set while a shopper's request (or the packages worker it started) may reach a sticker model
# call: a function that opens the guard for that request. Unset in the scanner and scripts, where
# the sticker calls are batch work outside the chat budget.
_sticker_guard_factory: contextvars.ContextVar[Callable[[], Guard] | None] = contextvars.ContextVar(
    "chat_budget_sticker_guard", default=None
)
# The first refusal met in this context (read back by the packages route for its payload).
_sticker_denial: contextvars.ContextVar[list | None] = contextvars.ContextVar("chat_budget_sticker_denial", default=None)


def bind_sticker_guard() -> contextvars.Context:
    """Capture this request's subjects for the sticker calls; returns the context to run the work in.

    The packages route runs its pipeline through ``ctx.run(...)`` (also inside its worker thread),
    so a model call deep in ``window_sticker_service`` is counted against the right shopper.
    """
    s = _subjects(new_device=False)

    def factory() -> Guard:
        return guard(
            KIND_STICKER,
            s["user_id"],
            s["phone_hash"],
            s["device_hash"],
            s["ip_hash"],
            None,
            STICKER_WEIGHT,
            net_hash=s["net_hash"],
            is_admin=s["is_admin"],
        )

    ctx = contextvars.copy_context()
    box: list = []
    ctx.run(_sticker_guard_factory.set, factory)
    ctx.run(_sticker_denial.set, box)
    return ctx


def sticker_denial(ctx: contextvars.Context) -> Denial | None:
    """The refusal a sticker call met while running in ``ctx`` (None when none was refused)."""
    box = ctx.get(_sticker_denial)
    return box[0] if box else None


@contextlib.contextmanager
def sticker_guard() -> Iterator[bool]:
    """Wrap one sticker model call. Yields True when the call may run, False when it was refused.

    Outside a shopper's request (scanner, scripts) it yields True and counts nothing.
    """
    factory = _sticker_guard_factory.get()
    if factory is None:
        yield True
        return
    try:
        g = factory().__enter__()
    except ChatDenied as denied:
        box = _sticker_denial.get()
        if box is not None and not box:
            box.append(denied.denial)
        yield False
        return
    try:
        yield True
    except BaseException as exc:
        g.__exit__(type(exc), exc, exc.__traceback__)
        raise
    g.__exit__(None, None, None)
