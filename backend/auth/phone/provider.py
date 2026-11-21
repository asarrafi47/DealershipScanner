"""The interface every text-code vendor adapter implements (DESIGN.md section 3).

This module has no vendor and no Flask imports. Results carry short reason codes of our own,
never vendor text, so nothing a vendor says can reach a page, a log line or an exception.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

# StartResult.reason
START_SENT = "sent"
START_THROTTLED = "throttled"  # the vendor asked us to slow down (HTTP 429, too many sends)
START_INVALID_NUMBER = "invalid_number"
START_BLOCKED = "blocked"  # the vendor's fraud protection refused this send
START_NETWORK_ERROR = "network_error"
START_PROVIDER_ERROR = "provider_error"
START_UNAVAILABLE = "unavailable"  # provider down, balance empty, account suspended, bad login or timeout
START_OPTED_OUT = "opted_out"  # the recipient blocked texts from us (STOP)
START_REASONS = (
    START_SENT, START_THROTTLED, START_INVALID_NUMBER, START_BLOCKED, START_NETWORK_ERROR, START_PROVIDER_ERROR,
    START_UNAVAILABLE, START_OPTED_OUT,
)

# CheckResult.reason
CHECK_APPROVED = "approved"
CHECK_WRONG_CODE = "wrong_code"
CHECK_EXPIRED = "expired"
CHECK_TOO_MANY = "too_many"  # the vendor's own attempt limit was reached
CHECK_THROTTLED = "throttled"
CHECK_NETWORK_ERROR = "network_error"  # says nothing about the code: do not count it as a wrong try
CHECK_UNKNOWN = "unknown"
CHECK_REASONS = (
    CHECK_APPROVED, CHECK_WRONG_CODE, CHECK_EXPIRED, CHECK_TOO_MANY, CHECK_THROTTLED, CHECK_NETWORK_ERROR,
    CHECK_UNKNOWN,
)

# LineInfo.reason
LINE_OK = "ok"
LINE_VOIP = "voip"
LINE_LANDLINE = "landline"
LINE_NOT_MOBILE = "not_mobile"  # toll-free, premium, pager and other lines that cannot take a login text
LINE_INVALID_NUMBER = "invalid_number"
LINE_UNKNOWN = "no_line_type"  # the vendor reported no line type: refused (a VoIP number may report none)
LINE_LOOKUP_FAILED = "lookup_failed"  # the vendor could not be asked: the caller fails closed
LINE_REASONS = (LINE_OK, LINE_VOIP, LINE_LANDLINE, LINE_NOT_MOBILE, LINE_INVALID_NUMBER, LINE_UNKNOWN, LINE_LOOKUP_FAILED)

# Lookup line types (Twilio's spelling) and what we do with them. Anything not listed in
# ALLOWED_LINE_TYPES is refused, and so is a lookup that gives no type at all (it fails closed).
VOIP_LINE_TYPES = frozenset({"fixedVoip", "nonFixedVoip"})
ALLOWED_LINE_TYPES = frozenset({"mobile", "personal"})


@dataclass(frozen=True)
class StartResult:
    ok: bool
    provider_ref: str | None  # the vendor's verification id
    retry_after_s: int | None
    reason: str  # one of START_REASONS
    attempts: int = 1  # vendor create calls made: 2 means a second text may have gone out


@dataclass(frozen=True)
class CheckResult:
    approved: bool
    reason: str  # one of CHECK_REASONS


@dataclass(frozen=True)
class LineInfo:
    ok: bool  # True: the number may be texted (and signed up)
    line_type: str | None  # mobile | landline | fixedVoip | nonFixedVoip | ...
    country: str | None
    reason: str  # one of LINE_REASONS


def classify_line_type(line_type: str | None) -> tuple[bool, str]:
    """(allowed, reason) for a lookup line type. VoIP, landline and non-mobile lines are refused."""
    if line_type is None or line_type in ("", "unknown"):
        return False, LINE_UNKNOWN
    if line_type in ALLOWED_LINE_TYPES:
        return True, LINE_OK
    if line_type in VOIP_LINE_TYPES:
        return False, LINE_VOIP
    if line_type == "landline":
        return False, LINE_LANDLINE
    return False, LINE_NOT_MOBILE


@runtime_checkable
class PhoneVerifyProvider(Protocol):
    name: str

    def lookup(self, e164: str) -> LineInfo: ...

    def start(self, e164: str, *, locale: str) -> StartResult: ...

    def check(self, e164: str, *, code: str, provider_ref: str | None) -> CheckResult: ...


@runtime_checkable
class LookupCache(Protocol):
    """Where line-type answers are kept, keyed by phone hash, so a number is paid for once.

    Production needs a durable cache (the table S5 adds, wired in S6); process memory is lost on
    every restart. The cache holds a classification only: no number, no code.
    """

    def get(self, phone_hash: str) -> LineInfo | None: ...

    def put(self, phone_hash: str, info: LineInfo, ttl_s: int) -> None: ...
