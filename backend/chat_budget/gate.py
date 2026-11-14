"""The chat budget gate (card C2, CHAT_LIMITS.md sections 2, 3 and 6).

Usage, the same way on every chat route::

    try:
        with guard("car", user_id, phone_hash, device_hash, ip_hash, listing_id, weight=1):
            answer = model_call()          # complete() reports its tokens to the guard
    except ChatDenied as denied:
        return jsonify(denied.denial.to_json()), denied.denial.status

On entry the guard reserves an estimate against the account, phone, device, address,
account-and-car and whole-site rows in one transaction (all or nothing). The strictest
limit that would be passed wins. On exit it replaces the estimate with the real tokens and
cost of every model call made inside. The message is given back only when NO answer text was
produced (a call counts as answered when its text is non-empty, whatever its stop reason: a
reply cut short by the token limit that returned text stays counted), only up to
``chat.giveback_per_hour`` (default 3) per account per hour, and tokens are never given back
(the tokens really spent always count). For a multi-call route (web research, weight 3) it is
given back only if no call produced text. A streamed reply is wrapped whole: the guard stays
open until the last chunk is sent, the route calls :meth:`Guard.note_text` when the first text
goes out, and a ``GeneratorExit`` (the browser left) after that never gives a message back.
Admins skip the per-subject limits and the new-account tiers, never the site budget. If the
database cannot be reached chat fails closed. No Flask here: callers pass plain values
(hashes are made by ``backend.utils.device_id``).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from backend import settings_store as ss
from backend.chat_budget import messages as msg
from backend.db.repositories import chat_usage_repo as repo
from backend.db.repositories.chat_usage_repo import Charge, Violation

_log = logging.getLogger(__name__)
_PEPPER_LOGGED = False

# Rank when several limits are hit with the same reset time (lower wins).
_KIND_RANK = {"budget": 0, "user": 1, "listing": 2, "phone": 3, "device": 4, "ip": 5}


@dataclass(frozen=True)
class Denial:
    kind: str  # budget | paused | unavailable | user_hour | user_day | listing | phone | device | ip
    error: str  # JSON error code
    message: str  # the plain sentence
    status: int = 429
    resets_at: datetime | None = None

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {"error": self.error, "message": self.message}
        out["resets_at"] = (
            self.resets_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if self.resets_at else None
        )
        return out


class ChatDenied(Exception):
    def __init__(self, denial: Denial):
        super().__init__(denial.kind)
        self.denial = denial


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _scaled(limit: int, new_account: bool, pct: int) -> int:
    if limit > 0 and new_account:
        return max(1, limit * pct // 100)
    return limit


def _tokens_of(usage: Any) -> tuple[int, int]:
    """(input, output) tokens from an SDK usage object or mapping; cache tokens count as input."""

    def pick(name: str) -> int:
        v = usage.get(name) if isinstance(usage, dict) else getattr(usage, name, 0)
        try:
            return max(0, int(v or 0))
        except (TypeError, ValueError):
            return 0

    if usage is None:
        return 0, 0
    tin = pick("input_tokens") + pick("cache_creation_input_tokens") + pick("cache_read_input_tokens")
    return tin, pick("output_tokens")


def cost_micro(tokens_in: int, tokens_out: int) -> int:
    """Millionths of a dollar from the price settings (0 while the prices are unset)."""
    p_in = ss.get_int("chat.price_in_micro_per_mtok")
    p_out = ss.get_int("chat.price_out_micro_per_mtok")
    return -(-(tokens_in * p_in + tokens_out * p_out) // 1_000_000)


def _denial_for(v: Violation, *, now: datetime, tz: str, user_day_limit: int, hit_limit: int) -> Denial:
    reset = repo.next_hour(now, tz) if v.period == "hour" else repo.next_day(now, tz)
    if v.kind == "global":
        return Denial("budget", msg.ERR_BUDGET, msg.budget_text(reset, tz), 429, reset)
    if v.kind == "listing":
        return Denial("listing", msg.ERR_LISTING, msg.listing_limit_text(hit_limit, reset, tz), 429, reset)
    if v.kind == "user" and v.period == "hour":
        return Denial("user_hour", msg.ERR_USER, msg.hour_limit_text(hit_limit, reset, now, tz), 429, reset)
    # account day, and the hidden phone, device and address counters: the same day sentence
    shown = hit_limit if v.kind == "user" else (user_day_limit or hit_limit)
    kind = "user_day" if v.kind == "user" else v.kind
    return Denial(kind, msg.ERR_USER, msg.day_limit_text(shown, reset, now, tz), 429, reset)


def _strictest(violations: list[Violation], now: datetime, tz: str) -> Violation:
    def reset_of(v: Violation) -> datetime:
        return repo.next_hour(now, tz) if v.period == "hour" else repo.next_day(now, tz)

    def key(v: Violation):
        rank = "budget" if v.kind == "global" else v.kind
        return (-reset_of(v).timestamp(), _KIND_RANK.get(rank, 9), v.period != "day")

    return sorted(violations, key=key)[0]


class Guard:
    def __init__(
        self,
        kind: str,
        user_id: int | str | None,
        phone_hash: str | None,
        device_hash: str | None,
        ip_hash: str | None,
        listing_id: int | str | None,
        weight: int = 1,
        *,
        is_admin: bool = False,
        new_account: bool = False,
        account_age_s: float | None = None,
        now: datetime | None = None,
    ):
        self.kind = kind
        self.user_id = user_id
        self.phone_hash = phone_hash
        self.device_hash = device_hash
        self.ip_hash = ip_hash
        self.listing_id = listing_id
        self.weight = max(1, int(weight or 1))
        self.is_admin = is_admin
        self.new_account = new_account
        self.account_age_s = account_age_s
        self._now = now
        self._text_seen = False
        self._failed = False
        self._no_model = False
        self._calls = 0
        self._tin = 0
        self._tout = 0
        self._res: repo.Reservation | None = None
        self._token = None
        self.denial: Denial | None = None

    # -- model usage ---------------------------------------------------------
    def fail(self) -> None:
        """The request ended without an answer for the shopper: give the message back."""
        self._failed = True

    def no_model_call(self) -> None:
        """The route answered without any model (a canned reply): the estimate is replaced by 0.

        Without this call, a block that reported no model usage keeps the estimate charged, so
        spend made outside complete() (another thread, a stream) can never go uncounted.
        """
        self._no_model = True

    def note_text(self) -> None:
        """A streaming route calls this when the first piece of answer text goes out: from then on the
        message is never given back, even if the browser disconnects (``GeneratorExit``)."""
        self._text_seen = True

    def _on_result(self, result: Any) -> None:
        tin, tout = _tokens_of(getattr(result, "usage", None))
        self._tin += tin
        self._tout += tout
        self._calls += 1
        text = getattr(result, "text", None)
        if isinstance(text, str) and text.strip():
            self._text_seen = True  # answered, even when the reply was cut at max_tokens

    @property
    def tokens(self) -> tuple[int, int]:
        return self._tin, self._tout

    # -- context manager -----------------------------------------------------
    def tier(self) -> str:
        """``new`` (under 24 hours old or no account), ``mid`` (1 to 7 days, or age unknown) or ``established``
        (over 7 days). Admins are ``admin``: only the site budget applies to them."""
        if self.is_admin:
            return "admin"
        if self.user_id in (None, "") or self.new_account:
            return "new"
        age = self.account_age_s
        if age is None:
            return "mid"
        if age < 24 * 3600:
            return "new"
        return "established" if age >= 7 * 24 * 3600 else "mid"

    def _tier_charges(self) -> list[Charge]:
        """Sub-budgets of the day's budget: new accounts share a slice, newer-than-a-week accounts cannot
        reach the reserve kept for established ones."""
        tier = self.tier()
        if tier in ("admin", "established"):
            return []
        cents = ss.get_int("chat.budget_usd_per_day_cents")
        tokens = ss.get_int("chat.budget_tokens_per_day")
        out: list[Charge] = []
        reserve = min(100, ss.get_int("chat.established_reserve_pct"))
        if reserve > 0:
            open_pct = 100 - reserve
            out.append(Charge("global", "open", tokens_limit=max(1, tokens * open_pct // 100) if open_pct else 1,
                              cost_limit_micro=max(1, cents * 10_000 * open_pct // 100) if open_pct else 1))
        if tier == "new":
            pct = ss.get_int("chat.new_account_budget_pct")
            out.append(Charge("global", "new", tokens_limit=max(1, tokens * pct // 100),
                              cost_limit_micro=max(1, cents * 10_000 * pct // 100)))
        return out

    def _charges(self) -> list[Charge]:
        tz = ss.get_str("chat.reset_tz")
        pct = ss.get_int("chat.new_account_factor_pct")
        free = self.is_admin  # admins skip per-subject limits (and the new-account tiers), never the site budget
        charges: list[Charge] = []
        if self.user_id not in (None, ""):
            uid = str(self.user_id)
            charges.append(
                Charge(
                    "user",
                    uid,
                    hour_limit=0 if free else _scaled(ss.get_int("chat.user_per_hour"), self.new_account, pct),
                    day_limit=0 if free else _scaled(ss.get_int("chat.user_per_day"), self.new_account, pct),
                )
            )
            if self.listing_id not in (None, ""):
                charges.append(
                    Charge(
                        "listing",
                        f"{uid}:{self.listing_id}",
                        day_limit=0 if free else _scaled(ss.get_int("chat.listing_per_day"), self.new_account, pct),
                    )
                )
        for kind, value, setting in (
            ("phone", self.phone_hash, "chat.phone_per_day"),
            ("device", self.device_hash, "chat.device_per_day"),
            ("ip", self.ip_hash, "chat.ip_per_day"),
        ):
            if value:
                charges.append(Charge(kind, str(value), day_limit=0 if free else ss.get_int(setting)))
        charges.append(
            Charge(
                "global",
                repo.GLOBAL_KEY,
                tokens_limit=ss.get_int("chat.budget_tokens_per_day"),
                cost_limit_micro=ss.get_int("chat.budget_usd_per_day_cents") * 10_000,
            )
        )
        charges.extend(self._tier_charges())
        return charges

    def __enter__(self) -> "Guard":
        now = self._now or _utcnow()
        tz = ss.get_str("chat.reset_tz")
        if not ss.get_bool("chat.enabled") or ss.get_int("chat.budget_usd_per_day_cents") == 0 \
                or ss.get_int("chat.budget_tokens_per_day") == 0:
            self.denial = Denial("paused", msg.ERR_PAUSED, msg.PAUSED_TEXT, 429, None)
            raise ChatDenied(self.denial)
        if not _pepper_ready():
            self.denial = Denial("unavailable", msg.ERR_UNAVAILABLE, msg.UNAVAILABLE_TEXT, 503, None)
            raise ChatDenied(self.denial)
        est_in = ss.get_int("chat.est_tokens_in_per_message") * self.weight
        est_out = ss.get_int("chat.est_tokens_out_per_message") * self.weight
        est_cost = ss.get_int("chat.est_cost_micro_per_message") * self.weight or cost_micro(est_in, est_out)
        charges = self._charges()
        try:
            result = repo.reserve(
                charges,
                now=now,
                tz_name=tz,
                weight=self.weight,
                est_tokens_in=est_in,
                est_tokens_out=est_out,
                est_cost_micro=est_cost,
            )
        except Exception as exc:  # database unreachable: chat fails closed, the rest of the site is untouched
            _log.error("chat_budget: reserve failed (%s); chat closed", type(exc).__name__)
            self.denial = Denial("unavailable", msg.ERR_UNAVAILABLE, msg.UNAVAILABLE_TEXT, 503, None)
            raise ChatDenied(self.denial) from None
        if not result.ok:
            worst = _strictest(result.violations, now, tz)
            self.denial = _denial_for(
                worst, now=now, tz=tz, user_day_limit=_user_day_shown(self), hit_limit=worst.limit
            )
            try:
                repo.add_denied(worst.kind, worst.key, worst.period, now=now, tz_name=tz)
            except Exception as exc:
                _log.error("chat_budget: could not record a denial (%s)", type(exc).__name__)
            raise ChatDenied(self.denial)
        self._res = result.reservation
        from backend.llm import client as llm_client

        self._token = llm_client.set_usage_sink(self._on_result)
        return self

    def _claim_give_back(self) -> bool:
        """One of the account's give-backs for this hour. No account, a spent cap or a database error
        means no give-back: the message stays counted (the safe side)."""
        if self.user_id in (None, ""):
            return False
        cap = ss.get_int("chat.giveback_per_hour")
        try:
            return repo.claim_give_back(
                str(self.user_id), now=self._now or _utcnow(), cap=cap, tz_name=ss.get_str("chat.reset_tz")
            )
        except Exception as exc:
            _log.error("chat_budget: could not take a give-back slot (%s)", type(exc).__name__)
            return False

    def __exit__(self, exc_type, exc, tb) -> bool:
        if self._token is not None:
            from backend.llm import client as llm_client

            llm_client.reset_usage_sink(self._token)
            self._token = None
        if self._res is None:
            return False
        if exc_type is None and not self._failed and self._calls == 0 and not self._no_model:
            # No usage was reported and the route did not say "no model needed": keep the
            # estimate charged (the model may have been called outside complete()).
            self._res = None
            return False
        # Give the message back only when no answer text was shown (see the module docstring).
        no_answer = (exc_type is not None or self._failed or self._calls > 0) and not self._text_seen
        back = no_answer and self._claim_give_back()
        try:
            repo.true_up(
                self._res,
                tokens_in=self._tin,
                tokens_out=self._tout,
                cost_micro=cost_micro(self._tin, self._tout),
                messages_back=back,
            )
        except Exception as exc2:  # the estimate stays counted: safe for the budget
            _log.error("chat_budget: true-up failed (%s)", type(exc2).__name__)
        self._res = None
        return False  # never swallow the caller's exception


def _pepper_ready() -> bool:
    """False (logged once) when the hash pepper is missing in production: chat then fails closed."""
    global _PEPPER_LOGGED
    from backend.utils import device_id

    try:
        device_id.pepper()
    except device_id.PepperMissing:
        if not _PEPPER_LOGGED:
            _PEPPER_LOGGED = True
            _log.error("chat_budget: PHONE_HASH_PEPPER is not set; chat is closed until it is set")
        return False
    return True


def _user_day_shown(g: Guard) -> int:
    n = ss.get_int("chat.user_per_day")
    return _scaled(n, g.new_account, ss.get_int("chat.new_account_factor_pct")) if n else 0


def web_research_weight() -> int:
    """How many messages a web-research answer counts as (setting, default 3)."""
    return max(1, ss.get_int("chat.web_research_weight"))


def guard(
    kind: str,
    user_id: int | str | None,
    phone_hash: str | None,
    device_hash: str | None,
    ip_hash: str | None,
    listing_id: int | str | None,
    weight: int = 1,
    **kwargs: Any,
) -> Guard:
    """Build the guard for one chat message (see the module docstring)."""
    return Guard(kind, user_id, phone_hash, device_hash, ip_hash, listing_id, weight, **kwargs)
