"""The phone verification service (card S6, DESIGN.md sections 5, 6, 7 and 10).

``AuthPhoneService(provider, turnstile, repo, clock, config)`` with ``start``, ``check``,
``resend``, ``cancel`` and ``sign_in``. No Flask: the routes (card S8) pass plain values and
show the returned sentence. Every answer is one of our own sentences; vendor text never leaves
the provider adapter.

Order of checks for a send (DESIGN.md section 6): consent, Turnstile, number and country, then
reserve first (one transaction under the site-wide advisory lock: counts, pools, caps and the
``pending`` row with its estimated cost), then the line-type lookup, then the vendor call, then
the row moves to its result. Postgres decides every cap, pool, lock and attempt: in-process
memory and Redis never do, so a restart or a flush cannot reset a limit. With ``REDIS_URL`` set,
short Redis speed bumps (card R2, ``burst.py``) run just before the Postgres counts: a bump
answers like a cap, writes no row and costs nothing; unset or down, Postgres alone decides.

Logs: logger ``auth_phone``, ``key=value`` lines, never a number, a code or vendor text; a
6-character hash prefix only on ``blocked_*`` and ``check_locked`` events.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import secrets
import time as _time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Protocol

import phonenumbers

from backend.auth.phone import burst
from backend.auth.phone import config as phone_config
from backend.auth.phone import limits as lim
from backend.auth.phone import numbers
from backend.auth.phone import provider as p

_log = logging.getLogger("auth_phone")

# ---------------------------------------------------------------- sentences (DESIGN.md sections 6 and 15)

SENT_TEMPLATE = "We sent a 6-digit code to the number {masked}."
COULD_NOT_SEND = "We could not send a code to that number. Check it and try again, or try later."
BUSY = "Sign-in by text is busy right now. Please try again after midnight Eastern time."
SELLER_BUSY = (
    "Verification by text is busy today. We saved your request; try again tomorrow, or we will review it by hand."
)
UNAVAILABLE = "Sign-in by text is not available right now. Please try again later."
OPTED_OUT = "Texts to this number are blocked. Reply START to the number, or use another number."
CONSENT_REQUIRED = "Tick the box to continue."
NOT_A_PERSON = "We could not check that you are a person. Please try again."
RELOAD = "Please reload the page and try again."
RESEND_TOO_SOON = "Please wait a minute before asking for another code."
CODE_FAILED = "That code did not work. Check it and try again, or ask for a new code."
CODE_LOCKED = "Too many wrong codes. Please wait 15 minutes and try again."
CODE_RETRY = "We could not check the code right now. Please try again."
START_AGAIN = "That code has expired or was already used. Please ask for a new code."
CANCELLED = "Enter a different number."
TRY_AGAIN = "Please try again in a minute."

# ---------------------------------------------------------------- reason codes returned to callers

R_SENT = "sent"
R_DISABLED = "disabled"
R_CONSENT = "consent_required"
R_TURNSTILE = "turnstile_failed"
R_INVALID = "invalid_number"
R_COUNTRY = "country_not_allowed"
R_NOT_ALLOWED = "not_on_allow_list"
R_NO_DEVICE = "no_device_cookie"
R_LIMIT = "limit"
R_BUSY = "busy"
R_LINE = "line_refused"
R_UNAVAILABLE = "unavailable"
R_OPTED_OUT = "opted_out"
R_PROVIDER = "provider_error"
R_TOO_SOON = "too_soon"
R_LOCKED = "locked"
R_NO_CHALLENGE = "no_challenge"
R_APPROVED = "approved"
R_WRONG = "wrong_code"
R_EXPIRED = "expired"
R_TOO_MANY = "too_many"
R_RETRY = "retry"
R_CANCELLED = "cancelled"

LOOKUP_CACHE_TTL_S = 30 * 24 * 3600
RESERVING_STALE_S = 60


@dataclass(frozen=True)
class ServiceConfig:
    """The numbers the service reads; defaults come from ``backend.auth.phone.config``."""

    enabled: bool = True
    code_ttl_s: int = 300
    resend_wait_s: int = 60
    max_wrong_tries: int = 5
    lock_minutes: int = 15
    est_cost_cents: int = 6
    est_lookup_cents: int = 1
    dormant_days: int = 180
    allowed_numbers: frozenset[str] = frozenset()
    consent_text_version: str = phone_config.CONSENT_TEXT_VERSION
    locale: str = "en"

    @classmethod
    def from_env(cls) -> ServiceConfig:
        allowed = frozenset(
            e for e in (numbers.try_normalize(raw) for raw in phone_config.allowed_numbers()) if e
        )
        return cls(
            enabled=phone_config.enabled(),
            code_ttl_s=phone_config.code_ttl_s(),
            resend_wait_s=phone_config.resend_wait_s(),
            max_wrong_tries=phone_config.max_wrong_tries(),
            lock_minutes=phone_config.lock_minutes(),
            est_cost_cents=phone_config.est_cost_cents(),
            est_lookup_cents=phone_config.est_lookup_cents(),
            dormant_days=dormant_days(),
            allowed_numbers=allowed,
        )


def dormant_days() -> int:
    """``AUTH_PHONE_DORMANT_DAYS`` (default 180)."""
    return phone_config._int("AUTH_PHONE_DORMANT_DAYS", 180, minimum=1)


# ---------------------------------------------------------------- results


@dataclass(frozen=True)
class StartOutcome:
    """What a send request returns. Never says whether the number has an account."""

    ok: bool
    reason: str
    sentence: str
    public_id: str | None = None
    session_nonce: str | None = None
    retry_after_s: int | None = None


@dataclass(frozen=True)
class CheckOutcome:
    approved: bool
    reason: str
    sentence: str
    phone_e164: str | None = field(default=None, repr=False)  # only when approved, for sign_in
    purpose: str | None = None
    consent_at: datetime | None = None


@dataclass(frozen=True)
class SignInOutcome:
    ok: bool
    reason: str  # signed_in | signed_up | dormant_fresh | retry
    user_id: int | None = None
    identity_id: int | None = None


class Accounts(Protocol):
    """Where accounts are created (card S7's ``save_phone_user`` plugs in here).

    ``adopt_or_create`` must be idempotent by email: a retry with the same email returns the
    same account id instead of making a second one.
    """

    def adopt_or_create(self, *, email: str, username: str) -> int: ...


class Turnstile(Protocol):
    def verify(self, token: str | None, ip: str | None) -> Any: ...


# ---------------------------------------------------------------- logging


def _event(event: str, **fields: Any) -> None:
    parts = ["event=%s" % event]
    for k, v in fields.items():
        if v is None:
            continue
        parts.append("%s=%s" % (k, v))
    _log.info(" ".join(parts))


def _h6(value: str | None) -> str | None:
    return (value or "")[:6] or None


# ---------------------------------------------------------------- the service


class AuthPhoneService:
    def __init__(
        self,
        provider: p.PhoneVerifyProvider,
        turnstile: Turnstile,
        repo: Any = None,
        clock: Callable[[], datetime] | None = None,
        config: ServiceConfig | None = None,
        *,
        lookup_cache: p.LookupCache | None = None,
        limits_loader: Callable[[], lim.Limits] = lim.load_limits,
        sends_paused: Callable[[], bool] | None = None,
    ) -> None:
        if repo is None:
            from backend.db.repositories import auth_phone_repo as repo  # noqa: PLW0127
        self.provider = provider
        self.turnstile = turnstile
        self.repo = repo
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.config = config or ServiceConfig.from_env()
        if lookup_cache is None:
            lookup_cache = repo.PgLookupCache(clock=self.clock)
        self.lookup_cache = lookup_cache
        self.limits_loader = limits_loader
        self._sends_paused = sends_paused or _settings_paused
        self._alerted: set[tuple[str, str]] = set()

    # ------------------------------------------------------------ helpers

    def _now(self) -> datetime:
        now = self.clock()
        return now if now.tzinfo else now.replace(tzinfo=timezone.utc)

    def _parse(self, raw: str | None) -> tuple[str | None, str | None, str | None]:
        """(e164, country, refusal reason). A number from a country not on the list still gets its
        E.164 form, so its refusal can be counted by hash."""
        try:
            return numbers.normalize(raw), None, None
        except numbers.PhoneNumberRefused as exc:
            if exc.reason != numbers.COUNTRY_NOT_ALLOWED:
                return None, None, R_INVALID
        try:
            parsed = phonenumbers.parse((raw or "").strip(), "US")
            region = phonenumbers.region_code_for_number(parsed) or "ZZ"
            e164 = numbers.normalize(raw, allowed_countries=frozenset({region}))
        except Exception:
            return None, None, R_INVALID
        return e164, region, R_COUNTRY

    def _wrong_keys(self, phone_hash: str, device_hash: str | None, session_nonce: str) -> tuple[str, str]:
        """(browser key, phone-wide key) of the wrong-code counters. The browser key is the number
        with the device (or, without a device cookie, the browser session nonce)."""
        who = ("d:" + device_hash) if device_hash else ("n:" + session_nonce)
        browser = "b:" + hashlib.sha256((phone_hash + "|" + who).encode("utf-8")).hexdigest()
        return browser, phone_hash

    def _locked(self, phone_hash: str, device_hash: str | None, session_nonce: str, now: datetime) -> bool:
        browser, phone_wide = self._wrong_keys(phone_hash, device_hash, session_nonce)
        return (
            self.repo.wrong_try_state(browser, now=now).locked
            or self.repo.wrong_try_state(phone_wide, now=now).locked
        )

    def _breaker_open(self, now: datetime) -> bool:
        recent = self.repo.recent_vendor_outcomes(limit=lim.BREAKER_RUN)
        if len(recent) < lim.BREAKER_RUN:
            return False
        if any(outcome != "unavailable" for outcome, _ in recent):
            return False
        newest = max(at for _, at in recent)
        return newest > now - lim.BREAKER_SKIP

    def _is_known(self, e164: str) -> bool:
        ident = self.repo.find_identity_by_phone(e164)
        return ident is not None and ident.state == "active" and ident.user_id is not None

    def _sent_outcome(self, masked_number: str, public_id: str, nonce: str) -> StartOutcome:
        return StartOutcome(True, R_SENT, SENT_TEMPLATE.format(masked=masked_number), public_id, nonce)

    def _busy(self, purpose: str) -> StartOutcome:
        return StartOutcome(False, R_BUSY, SELLER_BUSY if purpose == "seller_verify" else BUSY)

    def _reserve(self, row: dict[str, Any], ask: lim.SendAsk, limits: lim.Limits, now: datetime) -> tuple[int, str | None]:
        """Reserve the send under the site-wide lock; (send id, verdict of ``limits.decide``)."""
        verdicts: list[str | None] = []

        def _decide(window: Any) -> str | None:
            verdict = lim.decide(window, ask, limits, now)
            verdicts.append(verdict)
            return lim.outcome_for(verdict)

        reservation = self.repo.reserve_send(self.repo.SendRow(**row), now=now, decide=_decide)
        return reservation.send_id, (verdicts[-1] if verdicts else None)

    def _record_refusal(self, row_kwargs: dict[str, Any], outcome: str, now: datetime) -> None:
        self.repo.record_send(self.repo.SendRow(**row_kwargs, outcome=outcome), now=now)

    def _alerts(self, limits: lim.Limits, now: datetime) -> None:
        """``cap_50pct`` once a day at the alert level, prepaid 50 and 75 percent once per top-up."""
        day = lim.day_start(now, limits.reset_tz)
        sent = self.repo.count_sends(since=day, outcomes=lim.REAL_SEND_OUTCOMES)
        threshold = phone_config._int("SMS_ALERT_SEND_THRESHOLD", limits.daily_send_cap // 2, minimum=0)
        mark = ("cap_50pct", day.isoformat())
        if threshold and sent >= threshold and mark not in self._alerted:
            self._alerted.add(mark)
            _event("cap_50pct", sends=sent, cap=limits.daily_send_cap)
        since = limits.topup_at or datetime(2000, 1, 1, tzinfo=timezone.utc)
        spent = self.repo.sum_cost_cents(since=since)
        for pct in (50, 75):
            mark = ("prepaid_%d" % pct, since.isoformat())
            if limits.prepaid_cents and spent * 100 >= Decimal(limits.prepaid_cents) * pct and mark not in self._alerted:
                self._alerted.add(mark)
                _event("prepaid_%dpct" % pct, spent_cents=spent, prepaid_cents=limits.prepaid_cents)

    # ------------------------------------------------------------ start

    def start(
        self,
        raw_phone: str | None,
        *,
        consent: bool,
        turnstile_token: str | None,
        ip: str | None,
        device_hash: str | None,
        session_nonce: str | None = None,
        purpose: str = "login",
        user_id: int | None = None,
    ) -> StartOutcome:
        """Send a code (or refuse). Every refusal after the number is known is a send row."""
        started = _time.monotonic()
        if not self.config.enabled:
            return StartOutcome(False, R_DISABLED, UNAVAILABLE)
        if consent is not True:
            return StartOutcome(False, R_CONSENT, CONSENT_REQUIRED)
        nonce = session_nonce or secrets.token_urlsafe(18)
        now = self._now()
        e164, foreign_country, refusal = self._parse(raw_phone)
        ts = self.turnstile.verify(turnstile_token, ip)
        if e164 is None:
            # Nothing to count by: no row, the neutral sentence (or the Turnstile one).
            if not getattr(ts, "ok", False):
                return StartOutcome(False, R_TURNSTILE, NOT_A_PERSON)
            return StartOutcome(False, R_INVALID, COULD_NOT_SEND)
        phone_hash = numbers.hash_phone(e164)
        row = dict(
            phone_hash=phone_hash,
            ip_hash=numbers.hash_ip(ip),
            ip_prefix_hash=numbers.hash_value("ip_prefix", _ip_prefix(ip)),
            country=foreign_country or numbers.country_of(e164) or "ZZ",
            purpose=purpose,
            pool=lim.POOL_SELLER if purpose == "seller_verify" else lim.POOL_NEW,
            has_device_cookie=device_hash is not None,
            device_hash=device_hash,
        )
        if not getattr(ts, "ok", False):
            self._record_refusal(row, "blocked_turnstile", now)
            _event("blocked_turnstile", purpose=purpose, h=_h6(phone_hash))
            return StartOutcome(False, R_TURNSTILE, NOT_A_PERSON)
        if refusal == R_COUNTRY:
            self._record_refusal(row, "blocked_country", now)
            _event("blocked_country", purpose=purpose, h=_h6(phone_hash))
            return StartOutcome(False, R_COUNTRY, COULD_NOT_SEND)
        if self.config.allowed_numbers and e164 not in self.config.allowed_numbers:
            _event("blocked_allow_list", purpose=purpose, h=_h6(phone_hash))
            return StartOutcome(False, R_NOT_ALLOWED, COULD_NOT_SEND)
        if self._sends_paused():
            # Site-level stops are recorded as blocked_cap (cost 0, never against the person), so
            # they never look like a vendor answer to the breaker.
            self._record_refusal(row, "blocked_cap", now)
            _event("sends_paused", purpose=purpose)
            return StartOutcome(False, R_UNAVAILABLE, UNAVAILABLE)
        if not burst.start_allowed(phone_hash=phone_hash, device_hash=device_hash, group_hash=row["ip_prefix_hash"]):
            # Card R2: the same reply as a cap for every caller, no send row, nothing counted.
            _event("blocked_burst", purpose=purpose, step="start", h=_h6(phone_hash))
            return self._busy(purpose)
        if self._locked(phone_hash, device_hash, nonce, now):
            self._record_refusal(row, "blocked_limit", now)
            _event("blocked_limit", purpose=purpose, why="wrong_code_lock", h=_h6(phone_hash))
            return StartOutcome(False, R_LIMIT, COULD_NOT_SEND)
        if self._breaker_open(now):
            self._record_refusal(row, "blocked_cap", now)
            _event("provider_breaker_open", purpose=purpose)
            return StartOutcome(False, R_UNAVAILABLE, UNAVAILABLE)

        known = self._is_known(e164)
        if purpose != "seller_verify":
            row["pool"] = lim.POOL_KNOWN if known else lim.POOL_NEW
        cached = None if known else self.lookup_cache.get(phone_hash)
        needs_lookup = not known and cached is None
        lookup_cents = self.config.est_lookup_cents if needs_lookup else 0
        text_cents = self.config.est_cost_cents
        row["est_cost_cents"] = text_cents
        row["lookup_cost_cents"] = lookup_cents
        limits = self.limits_loader()
        ask = lim.SendAsk(
            phone_hash=phone_hash,
            ip_hash=row["ip_hash"],
            ip_prefix_hash=row["ip_prefix_hash"],
            device_hash=device_hash,
            pool=row["pool"],
            cost_cents=Decimal(text_cents + lookup_cents),
            user_id=user_id,
        )
        send_id, verdict = self._reserve(row, ask, limits, now)
        if verdict == lim.BLOCKED_LIMIT:
            _event("blocked_limit", purpose=purpose, h=_h6(phone_hash))
            if device_hash is None:
                return StartOutcome(False, R_NO_DEVICE, RELOAD)
            return StartOutcome(False, R_LIMIT, COULD_NOT_SEND)
        if verdict in (lim.BLOCKED_CAP, lim.POOL_BUSY):
            _event(lim.outcome_for(verdict), purpose=purpose, why=verdict, h=_h6(phone_hash))
            _event("cap_hit", purpose=purpose)
            return self._busy(purpose)

        # Line type: skipped for known numbers; refused lines are cached too (30 days).
        if not known:
            info = cached
            if info is None:
                info = self.provider.lookup(e164)
                if info.reason != p.LINE_LOOKUP_FAILED:
                    self.lookup_cache.put(phone_hash, info, LOOKUP_CACHE_TTL_S)
            if info.reason == p.LINE_LOOKUP_FAILED:
                # blocked_cap, not unavailable: a Lookup outage must not trip the Verify breaker,
                # so known numbers (which skip Lookup) keep getting codes. Costs nothing to the person.
                self.repo.set_send_outcome(send_id, "blocked_cap", est_cost_cents=0)
                _event("provider_error", purpose=purpose, step="lookup")
                return StartOutcome(False, R_UNAVAILABLE, UNAVAILABLE)
            if not info.ok:
                self.repo.set_send_outcome(send_id, "blocked_voip", est_cost_cents=0)
                _event("blocked_voip", purpose=purpose, reason=info.reason, h=_h6(phone_hash))
                return StartOutcome(False, R_LINE, COULD_NOT_SEND)

        result = self.provider.start(e164, locale=self.config.locale)
        if not result.ok:
            return self._start_failed(send_id, result, purpose)
        self.repo.set_send_outcome(send_id, "sent", est_cost_cents=text_cents * max(1, int(result.attempts or 1)))
        challenge = self.repo.create_challenge(
            phone_e164=e164,
            phone_hash=phone_hash,
            purpose=purpose,
            session_nonce=nonce,
            provider=self.provider.name,
            provider_ref=result.provider_ref,
            user_id=user_id,
            now=now,
            ttl_s=self.config.code_ttl_s,
        )
        burst.start_cooldown(challenge.public_id, self.config.resend_wait_s)
        _event(
            "start", provider=self.provider.name, purpose=purpose,
            latency_ms=int((_time.monotonic() - started) * 1000),
        )
        self._alerts(limits, now)
        return self._sent_outcome(numbers.mask(e164), challenge.public_id, nonce)

    def _start_failed(self, send_id: int, result: p.StartResult, purpose: str) -> StartOutcome:
        if result.reason == p.START_UNAVAILABLE:
            self.repo.set_send_outcome(send_id, "unavailable", est_cost_cents=0)
            _event("provider_unavailable", purpose=purpose)
            return StartOutcome(False, R_UNAVAILABLE, UNAVAILABLE)
        self.repo.set_send_outcome(send_id, "provider_error", est_cost_cents=0)
        _event("provider_error", purpose=purpose, reason=result.reason)
        if result.reason == p.START_OPTED_OUT:
            return StartOutcome(False, R_OPTED_OUT, OPTED_OUT)
        if result.reason in (p.START_INVALID_NUMBER, p.START_BLOCKED):
            return StartOutcome(False, R_PROVIDER, COULD_NOT_SEND)
        return StartOutcome(False, R_PROVIDER, UNAVAILABLE, retry_after_s=result.retry_after_s)

    # ------------------------------------------------------------ resend

    def resend(
        self,
        public_id: str,
        session_nonce: str,
        *,
        turnstile_token: str | None,
        ip: str | None,
        device_hash: str | None,
    ) -> StartOutcome:
        """Another text for an open challenge: 60 seconds after the last one, all limits apply."""
        if not self.config.enabled:
            return StartOutcome(False, R_DISABLED, UNAVAILABLE)
        now = self._now()
        ch = self.repo.fetch_challenge(public_id, session_nonce)
        if ch is None or ch.status != "open" or not ch.phone_e164:
            return StartOutcome(False, R_NO_CHALLENGE, START_AGAIN)
        if ch.last_send_at > now - timedelta(seconds=self.config.resend_wait_s) or burst.cooldown_active(public_id):
            # Card R2: the Redis cool-down is a second copy of the 60-second gate (it only adds
            # refusals), with the same reply; the atomic Postgres gate below still decides.
            return StartOutcome(False, R_TOO_SOON, RESEND_TOO_SOON)
        ts = self.turnstile.verify(turnstile_token, ip)
        known = self._is_known(ch.phone_e164)
        if ch.purpose == "seller_verify":
            pool = lim.POOL_SELLER
        else:
            pool = lim.POOL_RESEND if known else lim.POOL_NEW  # a new number's resends stay in its pool
        row = dict(
            phone_hash=ch.phone_hash,
            ip_hash=numbers.hash_ip(ip),
            ip_prefix_hash=numbers.hash_value("ip_prefix", _ip_prefix(ip)),
            country=numbers.country_of(ch.phone_e164) or "ZZ",
            purpose=ch.purpose,
            pool=pool,
            has_device_cookie=device_hash is not None,
            device_hash=device_hash,
        )
        if not getattr(ts, "ok", False):
            self._record_refusal(row, "blocked_turnstile", now)
            _event("blocked_turnstile", purpose=ch.purpose, h=_h6(ch.phone_hash))
            return StartOutcome(False, R_TURNSTILE, NOT_A_PERSON)
        if self._sends_paused():
            self._record_refusal(row, "blocked_cap", now)
            return StartOutcome(False, R_UNAVAILABLE, UNAVAILABLE)
        if not burst.start_allowed(phone_hash=ch.phone_hash, device_hash=device_hash, group_hash=row["ip_prefix_hash"]):
            _event("blocked_burst", purpose=ch.purpose, step="resend", h=_h6(ch.phone_hash))
            return self._busy(ch.purpose)
        if self._locked(ch.phone_hash, device_hash, session_nonce, now):
            self._record_refusal(row, "blocked_limit", now)
            _event("blocked_limit", purpose=ch.purpose, why="wrong_code_lock", h=_h6(ch.phone_hash))
            return StartOutcome(False, R_LIMIT, COULD_NOT_SEND)
        if self._breaker_open(now):
            self._record_refusal(row, "blocked_cap", now)
            return StartOutcome(False, R_UNAVAILABLE, UNAVAILABLE)
        # The 60-second gate is taken atomically before anything is spent, so two racing resends
        # cannot both text.
        # The code life restarts only after the text really went out (restart_code_life below), so a
        # refused resend never stretches the old code past its 5 minutes.
        if not self.repo.take_resend_gate(public_id, session_nonce, now=now, min_wait_s=self.config.resend_wait_s):
            return StartOutcome(False, R_TOO_SOON, RESEND_TOO_SOON)
        text_cents = self.config.est_cost_cents
        row["est_cost_cents"] = text_cents
        limits = self.limits_loader()
        ask = lim.SendAsk(
            phone_hash=ch.phone_hash, ip_hash=row["ip_hash"], ip_prefix_hash=row["ip_prefix_hash"],
            device_hash=device_hash, pool=pool, cost_cents=Decimal(text_cents), user_id=ch.user_id,
        )
        send_id, verdict = self._reserve(row, ask, limits, now)
        if verdict == lim.BLOCKED_LIMIT:
            _event("blocked_limit", purpose=ch.purpose, h=_h6(ch.phone_hash))
            return StartOutcome(False, R_LIMIT, COULD_NOT_SEND)
        if verdict in (lim.BLOCKED_CAP, lim.POOL_BUSY):
            _event(lim.outcome_for(verdict), purpose=ch.purpose, why=verdict, h=_h6(ch.phone_hash))
            _event("cap_hit", purpose=ch.purpose)
            return self._busy(ch.purpose)
        result = self.provider.start(ch.phone_e164, locale=self.config.locale)
        if not result.ok:
            return self._start_failed(send_id, result, ch.purpose)
        self.repo.set_send_outcome(send_id, "sent", est_cost_cents=text_cents * max(1, int(result.attempts or 1)))
        self.repo.restart_code_life(
            public_id, session_nonce, now=now, ttl_s=self.config.code_ttl_s, provider_ref=result.provider_ref
        )
        burst.start_cooldown(public_id, self.config.resend_wait_s)
        _event("start", provider=self.provider.name, purpose=ch.purpose, resend=1)
        self._alerts(limits, now)
        return self._sent_outcome(numbers.mask(ch.phone_e164), public_id, session_nonce)

    # ------------------------------------------------------------ check

    def check(
        self,
        public_id: str,
        session_nonce: str,
        code: str | None,
        *,
        device_hash: str | None,
        ip: str | None = None,
    ) -> CheckOutcome:
        """Check a code: the attempt is reserved before the vendor call; approved closes the
        challenge exactly once. A code typed in another browser fails (the nonce does not match).
        ``ip`` only feeds the Redis speed bump of card R2 (its /24 or /48 group)."""
        if not self.config.enabled:
            return CheckOutcome(False, R_DISABLED, UNAVAILABLE)
        now = self._now()
        ch = self.repo.fetch_challenge(public_id, session_nonce)
        if ch is None or ch.status != "open" or not ch.phone_e164:
            _event("check_expired", reason="no_challenge")
            return CheckOutcome(False, R_NO_CHALLENGE, START_AGAIN)
        if ch.expires_at <= now:
            self.repo.finish_challenge(public_id, "expired", now=now, session_nonce=session_nonce)
            _event("check_expired", purpose=ch.purpose)
            return CheckOutcome(False, R_EXPIRED, START_AGAIN)
        browser_key, phone_key = self._wrong_keys(ch.phone_hash, device_hash, session_nonce)
        if self._locked(ch.phone_hash, device_hash, session_nonce, now):
            _event("check_locked", purpose=ch.purpose, h=_h6(ch.phone_hash))
            return CheckOutcome(False, R_LOCKED, CODE_LOCKED)
        clean = "".join(ch_ for ch_ in (code or "") if ch_.isascii() and ch_.isdigit())
        if len(clean) != 6:
            return CheckOutcome(False, R_WRONG, CODE_FAILED)  # not a code: no attempt, no vendor call
        # Card R2: only a well-formed code about to reserve a real attempt reaches the bump, so empty
        # or short codes cannot fill anyone's window for free.
        if burst.configured() and not burst.check_allowed(
            phone_hash=ch.phone_hash,
            device_hash=device_hash,
            group_hash=numbers.hash_value("ip_prefix", _ip_prefix(ip)) if ip else None,
        ):
            # No attempt reserved, nothing counted, the same words for every number.
            _event("blocked_burst", purpose=ch.purpose, step="check", h=_h6(ch.phone_hash))
            return CheckOutcome(False, R_BUSY, TRY_AGAIN)
        attempt = self.repo.reserve_attempt(public_id, session_nonce, now=now, max_attempts=self.config.max_wrong_tries)
        if attempt is None:
            self.repo.finish_challenge(public_id, "failed", now=now, session_nonce=session_nonce)
            _event("check_locked", purpose=ch.purpose, why="attempts", h=_h6(ch.phone_hash))
            return CheckOutcome(False, R_TOO_MANY, CODE_LOCKED)
        result = self.provider.check(ch.phone_e164, code=clean, provider_ref=ch.provider_ref)
        if result.approved:
            closed = self.repo.close_approved(public_id, session_nonce, now=now)
            if closed is None:
                return CheckOutcome(False, R_NO_CHALLENGE, START_AGAIN)
            self.repo.clear_wrong_tries(browser_key)
            _event("check_ok", provider=self.provider.name, purpose=ch.purpose)
            return CheckOutcome(
                True, R_APPROVED, "", phone_e164=closed.phone_e164, purpose=closed.purpose, consent_at=ch.created_at
            )
        if result.reason in (p.CHECK_NETWORK_ERROR, p.CHECK_THROTTLED):
            self.repo.refund_attempt(public_id, session_nonce)
            _event("provider_error", purpose=ch.purpose, step="check", reason=result.reason)
            return CheckOutcome(False, R_RETRY, CODE_RETRY)
        if result.reason == p.CHECK_WRONG_CODE:
            state = self.repo.add_wrong_try(
                browser_key, now=now, max_tries=self.config.max_wrong_tries, lock_minutes=self.config.lock_minutes
            )
            self.repo.add_wrong_try(
                phone_key, now=now, max_tries=lim.PHONE_WIDE_MAX_WRONG, lock_minutes=lim.PHONE_WIDE_LOCK_MINUTES
            )
            if state.locked or attempt >= self.config.max_wrong_tries:
                if attempt >= self.config.max_wrong_tries:
                    self.repo.finish_challenge(public_id, "failed", now=now, session_nonce=session_nonce)
                _event("check_locked", purpose=ch.purpose, h=_h6(ch.phone_hash))
                return CheckOutcome(False, R_LOCKED, CODE_LOCKED)
            _event("check_wrong", purpose=ch.purpose)
            return CheckOutcome(False, R_WRONG, CODE_FAILED)
        # expired, too many or unknown at the vendor: the challenge ends.
        end = "expired" if result.reason == p.CHECK_EXPIRED else "failed"
        self.repo.finish_challenge(public_id, end, now=now, session_nonce=session_nonce)
        _event("check_expired", purpose=ch.purpose, reason=result.reason)
        return CheckOutcome(False, R_EXPIRED if end == "expired" else R_TOO_MANY, START_AGAIN)

    # ------------------------------------------------------------ cancel

    def cancel(self, public_id: str, session_nonce: str) -> StartOutcome:
        """"Use a different number": the open challenge ends and its number is cleared."""
        self.repo.finish_challenge(public_id, "cancelled", now=self._now(), session_nonce=session_nonce)
        return StartOutcome(False, R_CANCELLED, CANCELLED)

    # ------------------------------------------------------------ sign in (account creation order)

    def sign_in(self, approved: CheckOutcome, accounts: Accounts) -> SignInOutcome:
        """Find or create the account for an approved code (card S6 (g) and (h)).

        1) INSERT the identity ``reserving`` (a unique violation means another tab won: use it);
        2) ``adopt_or_create`` with a deterministic placeholder email and username derived from the
           identity, so a retry adopts the same account; 3) mark the identity ``active``.
        A ``reserving`` row older than 60 seconds is finished by repeating 2 and 3. A number not
        used for ``AUTH_PHONE_DORMANT_DAYS`` is detached from its old account and gets a fresh one.
        """
        if not approved.approved or not approved.phone_e164:
            raise ValueError("sign_in needs an approved check")
        e164 = approved.phone_e164
        now = self._now()
        ident = self.repo.find_identity_by_phone(e164)
        if ident is not None and ident.state == "active" and ident.user_id is not None:
            last = ident.last_login_at or ident.verified_at
            if last is not None and last < now - timedelta(days=self.config.dormant_days):
                self.repo.detach_dormant(ident.id, now=now)
                _event("dormant_detached")
                ident = self.repo.find_identity_by_phone(e164)
            else:
                self.repo.touch_login(ident.id, now=now)
                _event("login", purpose=approved.purpose)
                return SignInOutcome(True, "signed_in", int(ident.user_id), ident.id)
        fresh_from_dormant = ident is not None and ident.dormant_user_id is not None
        if ident is None:
            try:
                ident = self.repo.insert_identity(
                    phone_e164=e164,
                    phone_hash=numbers.hash_phone(e164),
                    country=numbers.country_of(e164) or "US",
                    now=now,
                    consent_at=approved.consent_at or now,
                    consent_text_version=self.config.consent_text_version,
                    line_type=_cached_line_type(self.lookup_cache, numbers.hash_phone(e164)),
                    state="reserving",
                )
            except self.repo.IdentityTaken:
                ident = self.repo.find_identity_by_phone(e164)
                if ident is None:
                    return SignInOutcome(False, "retry")
                if ident.state == "active" and ident.user_id is not None:
                    self.repo.touch_login(ident.id, now=now)
                    return SignInOutcome(True, "signed_in", int(ident.user_id), ident.id)
                return SignInOutcome(False, "retry")  # the other tab is creating the account
        elif ident.state == "reserving" and not fresh_from_dormant:
            started = ident.detached_at or ident.created_at
            if started is not None and started > now - timedelta(seconds=RESERVING_STALE_S):
                return SignInOutcome(False, "retry")
        email, username = placeholder_for(ident)
        user_id = int(accounts.adopt_or_create(email=email, username=username))
        if not self.repo.activate_identity(ident.id, user_id, now=now):
            again = self.repo.find_identity_by_phone(e164)
            if again is not None and again.state == "active" and again.user_id is not None:
                return SignInOutcome(True, "signed_in", int(again.user_id), again.id)
            return SignInOutcome(False, "retry")
        _event("signup", purpose=approved.purpose)
        return SignInOutcome(True, "dormant_fresh" if fresh_from_dormant else "signed_up", user_id, ident.id)


# ---------------------------------------------------------------- module helpers


def placeholder_for(ident: Any) -> tuple[str, str]:
    """``p-<identity id>-<hmac8>@phone.invalid`` and a username from the identity id.

    Deterministic for one identity and one detach generation, so a retry adopts the same account,
    and different after a dormant detach, so the next holder never adopts the old account.
    """
    generation = ident.detached_at.isoformat() if getattr(ident, "detached_at", None) else ""
    tag = hmac.new(
        numbers._subkey("placeholder"), ("%d|%s" % (ident.id, generation)).encode("utf-8"), hashlib.sha256
    ).hexdigest()[:8]
    return "p-%d-%s@phone.invalid" % (ident.id, tag), "user-%d-%s" % (ident.id, tag[:4])


def _cached_line_type(cache: Any, phone_hash: str) -> str | None:
    try:
        info = cache.get(phone_hash)
    except Exception:
        return None
    return None if info is None else info.line_type


def _ip_prefix(ip: str | None) -> str:
    """The /24 (IPv4) or /48 (IPv6) group of an address, as text."""
    import ipaddress

    try:
        addr = ipaddress.ip_address((ip or "").strip())
    except ValueError:
        return "unknown"
    if addr.version == 6 and addr.ipv4_mapped is not None:
        addr = addr.ipv4_mapped
    plen = 24 if addr.version == 4 else 48
    return str(ipaddress.ip_network("%s/%d" % (addr, plen), strict=False))


def _settings_paused() -> bool:
    from backend import settings_store

    return bool(settings_store.get_bool("auth_phone.sends_paused"))
