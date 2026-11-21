"""A pretend text-code vendor for tests and local development. It never sends anything.

Approves the code ``000000``, keeps an outbox, and can be told to fail, throttle, report a VoIP
line or stall. It refuses to load in production. No vendor and no Flask imports.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass

from backend.auth.phone import provider as p
from backend.utils.runtime_env import is_production_env

FAKE_CODE = "000000"


class FakeProviderForbidden(RuntimeError):
    """The fake provider was asked for in production."""


@dataclass(frozen=True)
class OutboxMessage:
    """One pretend text. Holds the number so a test can see where it would have gone."""

    to: str
    locale: str
    provider_ref: str


class FakePhoneProvider:
    name = "fake"

    def __init__(
        self,
        *,
        code: str = FAKE_CODE,
        sleeper: Callable[[float], None] = time.sleep,
        stall_s: float = 0.0,
    ) -> None:
        if is_production_env():
            raise FakeProviderForbidden("the fake phone provider cannot load in production")
        self._code = code
        self._sleep = sleeper
        self.stall_s = stall_s
        self.outbox: list[OutboxMessage] = []
        self.voip_numbers: set[str] = set()
        self.landline_numbers: set[str] = set()
        self.lookup_calls: list[str] = []
        self.start_calls = 0
        self.check_calls = 0
        self._start_failures: list[str] = []
        self._check_failures: list[str] = []
        self._lookup_fail = False
        self._expired: set[str] = set()
        self._refs: dict[str, str] = {}  # provider_ref -> e164
        self._open: dict[str, str] = {}  # e164 -> latest provider_ref
        self._approved: set[str] = set()

    # --- test controls -------------------------------------------------------------------
    def fail_next_start(self, reason: str = p.START_PROVIDER_ERROR, *, times: int = 1) -> None:
        self._start_failures.extend([reason] * times)

    def throttle_next_start(self, retry_after_s: int = 30) -> None:
        self._start_failures.append("throttled:%d" % retry_after_s)

    def fail_next_check(self, reason: str = p.CHECK_NETWORK_ERROR, *, times: int = 1) -> None:
        self._check_failures.extend([reason] * times)

    def fail_next_lookup(self) -> None:
        self._lookup_fail = True

    def mark_voip(self, e164: str) -> None:
        self.voip_numbers.add(e164)

    def mark_landline(self, e164: str) -> None:
        self.landline_numbers.add(e164)

    def expire(self, provider_ref: str) -> None:
        self._expired.add(provider_ref)

    def clear(self) -> None:
        self.__init__(code=self._code, sleeper=self._sleep, stall_s=self.stall_s)

    # --- interface -----------------------------------------------------------------------
    def lookup(self, e164: str) -> p.LineInfo:
        self.lookup_calls.append(e164)
        self._maybe_stall()
        if self._lookup_fail:
            self._lookup_fail = False
            return p.LineInfo(False, None, None, p.LINE_LOOKUP_FAILED)
        if e164 in self.voip_numbers:
            return p.LineInfo(False, "nonFixedVoip", "US", p.LINE_VOIP)
        if e164 in self.landline_numbers:
            return p.LineInfo(False, "landline", "US", p.LINE_LANDLINE)
        return p.LineInfo(True, "mobile", "US", p.LINE_OK)

    def start(self, e164: str, *, locale: str) -> p.StartResult:
        self.start_calls += 1
        self._maybe_stall()
        if self._start_failures:
            raw = self._start_failures.pop(0)
            if raw.startswith("throttled:"):
                return p.StartResult(False, None, int(raw.split(":", 1)[1]), p.START_THROTTLED)
            return p.StartResult(False, None, None, raw)
        ref = "FAKE%04d" % (len(self._refs) + 1)
        self._refs[ref] = e164
        self._open[e164] = ref
        self.outbox.append(OutboxMessage(to=e164, locale=locale, provider_ref=ref))
        return p.StartResult(True, ref, None, p.START_SENT)

    def check(self, e164: str, *, code: str, provider_ref: str | None) -> p.CheckResult:
        self.check_calls += 1
        self._maybe_stall()
        if self._check_failures:
            reason = self._check_failures.pop(0)
            return p.CheckResult(False, reason)
        ref = provider_ref or self._open.get(e164)
        if ref is None or self._refs.get(ref) != e164 or ref in self._approved:
            return p.CheckResult(False, p.CHECK_EXPIRED)  # nothing open for this number
        if ref in self._expired:
            return p.CheckResult(False, p.CHECK_EXPIRED)
        if code == self._code:
            self._approved.add(ref)  # a code works once
            return p.CheckResult(True, p.CHECK_APPROVED)
        return p.CheckResult(False, p.CHECK_WRONG_CODE)

    def _maybe_stall(self) -> None:
        if self.stall_s:
            self._sleep(self.stall_s)
