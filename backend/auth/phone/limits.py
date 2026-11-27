"""Send limits, pools and caps for phone codes (card S6, DESIGN.md sections 5 and 6).

The numbers come from ``settings_store`` (row, then environment, then default; the send and
spend caps under env-only ceilings). The decision runs inside ``auth_phone_repo.reserve_send``,
which holds the site-wide advisory lock, so the counts it reads are exact and every reserve in
the site is serialised. Postgres is the only authority here: nothing in this module reads Redis
or process memory, so a restart or a cache flush can never reset or raise a cap.

``decide`` returns None (send) or the outcome to record:
- ``blocked_limit``: a per-number, per-address, per-device or per-group limit (the person's own);
- ``blocked_cap``: the site send cap, the spend cap, the prepaid guard or any pool share (the
  same busy sentence for everyone, so a refusal never tells known numbers from unknown ones).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from backend import settings_store

POOL_KNOWN = "known"
POOL_NEW = "new"
POOL_RESEND = "resend"
POOL_SELLER = "seller"
SHOPPER_POOLS = (POOL_KNOWN, POOL_NEW, POOL_RESEND)

# Rows that are (or may become) a real text: they use the day's sends and the person's allowance.
REAL_SEND_OUTCOMES = ("pending", "sent")
# Rows that count as a real text only when they carry a cost (review R1): a ``provider_error`` after
# which a text may have gone out (read timeout, vendor 5xx) keeps its estimated cost, while a
# definite vendor refusal is recorded at 0 and uses none of the day's sends.
COSTED_SEND_OUTCOMES = ("provider_error",)
# Rows a number is charged for: real texts and vendor refusals of a real attempt. Turnstile,
# limit and cap refusals are left out, so a stranger cannot use up someone else's number.
PHONE_COUNTED_OUTCOMES = ("pending", "sent", "provider_error", "blocked_voip")
# Rows a source (address, device, address group) is charged for: everything it asked for except
# the outcomes that cost nothing and are not the person's fault (card S6 (e)).
SOURCE_EXCLUDED_OUTCOMES = ("unavailable", "blocked_cap")

BLOCKED_LIMIT = "blocked_limit"
BLOCKED_CAP = "blocked_cap"
# A refusal by the new-number pool (whole day or per address group). The person sees the same busy
# sentence as at a cap, but the row is recorded as ``blocked_limit`` so it counts against the
# source's address, device and group limits: otherwise an unknown number's refusal would be free
# while a known number gets a code, a free way to learn which numbers have accounts.
POOL_BUSY = "pool_busy"


def outcome_for(verdict: str | None) -> str | None:
    """The send-row outcome of a :func:`decide` verdict."""
    return BLOCKED_LIMIT if verdict == POOL_BUSY else verdict

# Phone-wide wrong-code lock (card S6 (c)): only after this many wrong codes in 24 hours across all
# challenges and browsers, so a stranger in another browser cannot lock the real holder out.
PHONE_WIDE_MAX_WRONG = 15
PHONE_WIDE_LOCK_MINUTES = 60

# Breaker (card S6 (e)): after this many ``unavailable`` answers in a row, skip the vendor this long.
BREAKER_RUN = 5
# What ``auth_phone_repo.recent_vendor_outcomes`` reports for a ``provider_error`` row with a cost
# (a start that may have sent, review R1). A read label only, never stored.
MAYBE_SENT = "maybe_sent"
# Vendor answers that count as failures for the breaker.
BREAKER_FAILURES = ("unavailable", "pending", MAYBE_SENT)
BREAKER_SKIP = timedelta(minutes=5)


@dataclass(frozen=True)
class Limits:
    daily_send_cap: int = 30
    daily_spend_cap_cents: int = 300
    phone_per_15min: int = 2
    phone_per_day: int = 3
    ip_per_hour: int = 3
    ip_per_day: int = 5
    device_per_day: int = 3
    prefix_per_day: int = 6
    no_cookie_prefix_per_day: int = 2
    new_pool_prefix_per_day: int = 2
    new_number_share_pct: int = 50
    resend_share_pct: int = 20
    dealer_share_pct: int = 10
    prepaid_cents: int = 2000
    prepaid_stop_pct: int = 90
    topup_at: datetime | None = None
    reset_tz: str = "America/New_York"

    @property
    def reserved_sends(self) -> int:
        """The dealer and seller pool (3 of 30): shoppers can never use it."""
        return self.daily_send_cap * self.dealer_share_pct // 100

    @property
    def shopper_sends(self) -> int:
        return self.daily_send_cap - self.reserved_sends

    @property
    def new_pool_sends(self) -> int:
        return self.daily_send_cap * self.new_number_share_pct // 100

    @property
    def resend_pool_sends(self) -> int:
        return self.daily_send_cap * self.resend_share_pct // 100


def load_limits() -> Limits:
    """Today's numbers from the settings store (cached 30 seconds per process)."""
    g = settings_store.get
    return Limits(
        daily_send_cap=int(g("sms.daily_send_cap")),
        daily_spend_cap_cents=int(g("sms.daily_spend_cap_cents")),
        phone_per_15min=int(g("sms.phone_per_15min")),
        phone_per_day=int(g("sms.phone_per_day")),
        ip_per_hour=int(g("sms.ip_per_hour")),
        ip_per_day=int(g("sms.ip_per_day")),
        device_per_day=int(g("sms.device_per_day")),
        prefix_per_day=int(g("sms.prefix_per_day")),
        no_cookie_prefix_per_day=int(g("sms.no_cookie_prefix_per_day")),
        new_pool_prefix_per_day=int(g("sms.new_pool_prefix_per_day")),
        new_number_share_pct=int(g("sms.new_number_share_pct")),
        resend_share_pct=int(g("sms.resend_share_pct")),
        dealer_share_pct=int(g("sms.dealer_share_pct")),
        prepaid_cents=int(g("sms.prepaid_cents")),
        prepaid_stop_pct=int(g("sms.prepaid_stop_pct")),
        topup_at=g("sms.topup_at"),
        reset_tz=str(g("chat.reset_tz")),
    )


def day_start(now: datetime, tz_name: str) -> datetime:
    """Midnight of ``now``'s day in ``tz_name`` (``CHAT_RESET_TZ``), as an aware UTC time."""
    tz = ZoneInfo(tz_name)
    local = now.astimezone(tz)
    midnight = datetime.combine(local.date(), time(0, 0), tzinfo=tz)
    return midnight.astimezone(timezone.utc)


class Window(Protocol):
    def count(self, *, since: datetime, until: datetime | None = None, outcomes: Any = None,
              exclude_outcomes: Any = None, costed_outcomes: Any = None, **filters: Any) -> int:
        """Rows matching the filters. With ``outcomes``, a row also matches when its outcome is in
        ``costed_outcomes`` and its ``est_cost_cents`` is above 0."""
        ...

    def cost_cents(self, *, since: datetime, until: datetime | None = None, **filters: Any) -> Decimal: ...

    def seller_challenges(self, *, user_id: int, since: datetime) -> int: ...


@dataclass(frozen=True)
class SendAsk:
    """What one send would be, in hashes only."""

    phone_hash: str
    ip_hash: str
    ip_prefix_hash: str
    device_hash: str | None  # None: no valid device cookie
    pool: str
    cost_cents: Decimal  # text plus lookup estimate of this send
    user_id: int | None = None  # the account asking (required for the reserved seller pool)


def decide(window: Window, ask: SendAsk, limits: Limits, now: datetime) -> str | None:
    """None to send, or ``blocked_limit``, ``pool_busy`` or ``blocked_cap`` (record it with
    :func:`outcome_for`). Pure: counts through ``window`` only."""
    day = day_start(now, limits.reset_tz)
    last_24h = now - timedelta(hours=24)

    # --- the person's own limits (rolling windows) ---------------------------------------------
    def phone(since: datetime) -> int:
        return window.count(since=since, outcomes=PHONE_COUNTED_OUTCOMES, phone_hash=ask.phone_hash)

    def source(since: datetime, **f: Any) -> int:
        return window.count(since=since, exclude_outcomes=SOURCE_EXCLUDED_OUTCOMES, **f)

    if phone(now - timedelta(minutes=15)) >= limits.phone_per_15min:
        return BLOCKED_LIMIT
    if phone(last_24h) >= limits.phone_per_day:
        return BLOCKED_LIMIT
    if source(now - timedelta(hours=1), ip_hash=ask.ip_hash) >= limits.ip_per_hour:
        return BLOCKED_LIMIT
    if source(last_24h, ip_hash=ask.ip_hash) >= limits.ip_per_day:
        return BLOCKED_LIMIT
    if ask.device_hash is not None and source(last_24h, device_hash=ask.device_hash) >= limits.device_per_day:
        return BLOCKED_LIMIT
    if source(last_24h, ip_prefix_hash=ask.ip_prefix_hash) >= limits.prefix_per_day:
        return BLOCKED_LIMIT
    if ask.device_hash is None and (
        source(last_24h, ip_prefix_hash=ask.ip_prefix_hash, has_device_cookie=False) >= limits.no_cookie_prefix_per_day
    ):
        return BLOCKED_LIMIT

    # --- pools and site caps (calendar day in the reset time zone) ------------------------------
    def real(**f: Any) -> int:
        return window.count(since=day, outcomes=REAL_SEND_OUTCOMES, costed_outcomes=COSTED_SEND_OUTCOMES, **f)

    if ask.pool == POOL_SELLER:
        if real(pool=POOL_SELLER) >= limits.reserved_sends:
            return BLOCKED_CAP
        # one send of the reserved pool per phone and per device (resends count inside the pool)
        if real(pool=POOL_SELLER, phone_hash=ask.phone_hash) >= 1:
            return BLOCKED_CAP
        if ask.device_hash is not None and real(pool=POOL_SELLER, device_hash=ask.device_hash) >= 1:
            return BLOCKED_CAP
        # and one per account
        if ask.user_id is None or window.seller_challenges(user_id=ask.user_id, since=day) >= 1:
            return BLOCKED_CAP
    else:
        shopper = sum(real(pool=p) for p in SHOPPER_POOLS)
        if shopper >= limits.shopper_sends:
            return BLOCKED_CAP
        if ask.pool == POOL_NEW:
            if real(pool=POOL_NEW) >= limits.new_pool_sends:
                return POOL_BUSY
            if real(pool=POOL_NEW, ip_prefix_hash=ask.ip_prefix_hash) >= limits.new_pool_prefix_per_day:
                return POOL_BUSY
        if ask.pool == POOL_RESEND and real(pool=POOL_RESEND) >= limits.resend_pool_sends:
            return BLOCKED_CAP

    if real() >= limits.daily_send_cap:
        return BLOCKED_CAP
    if window.cost_cents(since=day) + ask.cost_cents > limits.daily_spend_cap_cents:
        return BLOCKED_CAP
    since_topup = limits.topup_at or datetime(2000, 1, 1, tzinfo=timezone.utc)
    stop_at = Decimal(limits.prepaid_cents) * limits.prepaid_stop_pct / 100
    if window.cost_cents(since=since_topup) + ask.cost_cents > stop_at:
        return BLOCKED_CAP
    return None
