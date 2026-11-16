"""Burst speed bumps on phone start and check (card R2, docs/auth_sms/TASKS.md "Redis cards").

Short Redis windows per phone hash, per device hash and per /24 (IPv4) or /48 (IPv6) group hash,
plus a resend cool-down per challenge. They sit in FRONT of the Postgres counts of card S6 and
can only make things stricter:

* a Redis "no" gives the same neutral reply as a cap (the caller picks it; it never depends on
  whether the number has an account), writes no send row, costs 0 cents and is not counted
  against the Postgres per-phone, device or address limits;
* Redis unset, skipped by its breaker or failing: every function here says "go on", so Postgres
  alone decides, exactly as before;
* Postgres stays the only authority for the daily caps, pools, locks, attempt reservation and
  spend sums, so a flush or an eviction here can never reset or raise any of them.

Keys: the window keys go through ``ip_rate_limit`` (``sc:rl:<sha256>``); the cool-down key is
``sc:ph:cool:<sha256 of the challenge id>``. Only hashes are ever stored, and every key expires.
"""

from __future__ import annotations

import hashlib

from backend.utils import ip_rate_limit, redis_client

WINDOW_S = 60.0
# (subject, limit per minute). Below the point where Postgres would refuse anyway, these only
# take a flood off the database (the advisory lock, the counts and the refusal rows).
START_LIMITS = {"phone": 3, "device": 6, "group": 12}
CHECK_LIMITS = {"phone": 10, "device": 10, "group": 30}


def configured() -> bool:
    """True when ``REDIS_URL`` is set (the bumps may run). Unset: nothing here touches Redis."""
    return redis_client.configured()


def _allowed(step: str, limits: dict[str, int], subjects: dict[str, str | None]) -> bool:
    if not configured():
        return True
    for subject, limit in limits.items():
        value = subjects.get(subject)
        if not value:
            continue
        verdict = ip_rate_limit.redis_window_allows(
            "ph:%s:%s:%s" % (step, subject, value), max_events=limit, window_seconds=WINDOW_S
        )
        if verdict is False:
            return False
        # None: Redis did not answer; Postgres alone decides.
    return True


def start_allowed(*, phone_hash: str, device_hash: str | None, group_hash: str | None) -> bool:
    """Start and resend: may this send go on to the Postgres reservation?"""
    return _allowed("start", START_LIMITS, {"phone": phone_hash, "device": device_hash, "group": group_hash})


def check_allowed(*, phone_hash: str, device_hash: str | None, group_hash: str | None) -> bool:
    """Code check: may this attempt go on to the Postgres attempt reservation?"""
    return _allowed("check", CHECK_LIMITS, {"phone": phone_hash, "device": device_hash, "group": group_hash})


def _cool_key(public_id: str) -> str:
    return redis_client.key("ph", "cool", hashlib.sha256(public_id.encode("utf-8")).hexdigest()[:40])


def cooldown_active(public_id: str) -> bool:
    """True while the resend cool-down of this challenge runs. Redis unset or failing: False."""
    client = redis_client.get_client()
    if client is None:
        return False
    try:
        active = bool(client.exists(_cool_key(public_id)))
    except Exception as exc:
        redis_client.record_failure(exc)
        return False
    redis_client.record_success()
    return active


def start_cooldown(public_id: str, seconds: int) -> None:
    """Begin the resend cool-down after a text really went out. Never raises."""
    if seconds <= 0:
        return
    client = redis_client.get_client()
    if client is None:
        return
    try:
        client.set(_cool_key(public_id), "1", ex=int(seconds))
    except Exception as exc:
        redis_client.record_failure(exc)
        return
    redis_client.record_success()
