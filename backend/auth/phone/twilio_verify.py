"""Twilio Verify adapter and Lookup line-type check (DESIGN.md section 3). The only file that knows Twilio.

* ``start``  = Verify v2 create verification, channel ``sms``.
* ``check``  = Verify v2 verification check. Twilio holds the code; we never see or store it.
* ``lookup`` = Lookup v2 with ``line_type_intelligence``. VoIP (fixed and non-fixed), landline and
  other lines that cannot take a login text are refused. Answers are kept in a ``LookupCache``
  keyed by phone hash for 30 days.

Vendor error text never leaves this file: every failure becomes one of our short reason codes, and
the only things logged are an event name, the operation, the reason and an integer status. The
Twilio HTTP client logs request URLs (which hold the number) and bodies at INFO level, so it is
given a disabled logger.
"""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Callable
from typing import Any

import requests

from backend.auth.phone import config
from backend.auth.phone import provider as p
from backend.auth.phone.numbers import hash_phone

_log = logging.getLogger("auth_phone")
_silent_http_log = logging.getLogger("auth_phone.twilio_http_silenced")
_silent_http_log.disabled = True
_silent_http_log.propagate = False


class _DropEverything(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003 - logging API name
        return False


_DROP_ALL = _DropEverything()


def silence_vendor_http_logs() -> None:
    """Drop every record of the HTTP libraries under the Twilio client. ``urllib3.connectionpool`` logs
    the request path at DEBUG (a Lookup path holds the number) and at WARNING on a retry, so it is
    silenced as well as Twilio's own client logger. Idempotent."""
    for name in ("urllib3.connectionpool", "twilio.http_client"):
        lg = logging.getLogger(name)
        if _DROP_ALL not in lg.filters:
            lg.addFilter(_DROP_ALL)

TIMEOUT_S = 5
LOOKUP_CACHE_TTL_S = 30 * 24 * 3600

# Twilio's own error numbers (integers, no text), mapped to our reasons.
_START_THROTTLED_CODES = frozenset({60203})  # max send attempts reached
_START_INVALID_CODES = frozenset({60200, 60205, 21211, 21614})
_START_BLOCKED_CODES = frozenset({60410})  # delivery attempt blocked (fraud protection)
_CHECK_TOO_MANY_CODES = frozenset({60202})  # max check attempts reached
_CHECK_WRONG_CODES = frozenset({60200})  # a code that is not even well formed
_START_UNAVAILABLE_CODES = frozenset({20003, 20005})  # bad login, account not active (suspended)
_START_OPTED_OUT_CODES = frozenset({21610})  # recipient replied STOP
_UNAVAILABLE_HTTP = frozenset({401, 402, 403})  # login refused, balance empty, forbidden

# Verify's ``status`` of a check, one row per value Twilio documents.
_CHECK_STATUS_REASONS = {
    "pending": p.CHECK_WRONG_CODE,  # the code did not match and the verification is still open
    "canceled": p.CHECK_UNKNOWN,
    "max_attempts_reached": p.CHECK_TOO_MANY,
    "deleted": p.CHECK_EXPIRED,
    "failed": p.CHECK_EXPIRED,
    "expired": p.CHECK_EXPIRED,
}

_NETWORK_ERRORS: tuple[type[BaseException], ...] = (requests.RequestException, OSError)


def _is_connect_failure(exc: BaseException) -> bool:
    """True only when the request provably never reached the vendor (TCP connect failed or timed out)."""
    if isinstance(exc, requests.ConnectTimeout):
        return True
    if isinstance(exc, requests.ConnectionError) and not isinstance(exc, requests.ReadTimeout):
        first = exc.args[0] if exc.args else None
        return type(first).__name__ in ("NewConnectionError", "ConnectTimeoutError")
    return False


def _vendor_error(exc: BaseException) -> tuple[int | None, int | None] | None:
    """(http status, vendor error number) when ``exc`` is a Twilio API error, else None."""
    from twilio.base.exceptions import TwilioRestException

    if not isinstance(exc, TwilioRestException):
        return None
    status = exc.status if isinstance(exc.status, int) else None
    code = exc.code if isinstance(exc.code, int) else None
    return status, code


def _log_failure(op: str, reason: str, exc: BaseException) -> None:
    """One line: event, operation, reason and the integer numbers. Never the exception text."""
    vendor = _vendor_error(exc)
    status, code = vendor if vendor else (None, None)
    _log.warning(
        "event=provider_error provider=twilio_verify op=%s reason=%s http_status=%s vendor_code=%s",
        op, reason, status, code,
    )


class TwilioVerifyProvider:
    name = "twilio_verify"

    def __init__(
        self,
        client: Any,
        service_sid: str,
        *,
        cache: p.LookupCache | None = None,
        hash_fn: Callable[[str], str] = hash_phone,
    ) -> None:
        self._client = client
        self._service_sid = service_sid
        self._cache = cache
        self._hash = hash_fn

    # --- start ---------------------------------------------------------------------------
    def start(self, e164: str, *, locale: str) -> p.StartResult:
        result, never_reached_vendor = self._start_once(e164, locale)
        if never_reached_vendor:
            # One retry, only when the request cannot have reached Twilio (connect failed). A read
            # timeout or reset after sending may already have produced a paid text: never retry that.
            # The decision travels in the return value: this object is shared by request threads.
            second, _ = self._start_once(e164, locale)
            return dataclasses.replace(second, attempts=2)
        return result

    def _start_once(self, e164: str, locale: str) -> tuple[p.StartResult, bool]:
        """(result, True when the request provably never reached the vendor). No instance state."""
        kwargs: dict[str, Any] = {"to": e164, "channel": "sms"}
        if locale:
            kwargs["locale"] = locale
        try:
            verification = self._client.verify.v2.services(self._service_sid).verifications.create(**kwargs)
        except Exception as exc:  # noqa: BLE001 - every failure becomes a reason code
            reason = self._start_reason(exc)
            _log_failure("start", reason, exc)
            return p.StartResult(False, None, None, reason), _is_connect_failure(exc)
        ref = getattr(verification, "sid", None)
        return p.StartResult(True, ref if isinstance(ref, str) and ref else None, None, p.START_SENT), False

    @staticmethod
    def _start_reason(exc: BaseException) -> str:
        vendor = _vendor_error(exc)
        if vendor is not None:
            status, code = vendor
            if status == 429 or code in _START_THROTTLED_CODES:
                return p.START_THROTTLED
            if code in _START_OPTED_OUT_CODES:
                return p.START_OPTED_OUT
            if code in _START_BLOCKED_CODES:
                return p.START_BLOCKED
            if code in _START_INVALID_CODES:
                return p.START_INVALID_NUMBER
            if status in _UNAVAILABLE_HTTP or (status is not None and status >= 500) or code in _START_UNAVAILABLE_CODES:
                return p.START_UNAVAILABLE
            return p.START_PROVIDER_ERROR
        if isinstance(exc, requests.Timeout):
            return p.START_UNAVAILABLE
        if isinstance(exc, _NETWORK_ERRORS):
            return p.START_NETWORK_ERROR
        return p.START_PROVIDER_ERROR

    # --- check ---------------------------------------------------------------------------
    def check(self, e164: str, *, code: str, provider_ref: str | None) -> p.CheckResult:
        kwargs: dict[str, Any] = {"code": code}
        if provider_ref:
            kwargs["verification_sid"] = provider_ref
        else:
            kwargs["to"] = e164
        try:
            check = self._client.verify.v2.services(self._service_sid).verification_checks.create(**kwargs)
        except Exception as exc:  # noqa: BLE001 - every failure becomes a reason code
            reason = self._check_reason(exc)
            _log_failure("check", reason, exc)
            return p.CheckResult(False, reason)
        status = getattr(check, "status", None)
        if status == "approved":
            return p.CheckResult(True, p.CHECK_APPROVED)
        return p.CheckResult(False, _CHECK_STATUS_REASONS.get(status, p.CHECK_UNKNOWN)
                             if isinstance(status, str) else p.CHECK_UNKNOWN)

    @staticmethod
    def _check_reason(exc: BaseException) -> str:
        vendor = _vendor_error(exc)
        if vendor is not None:
            status, code = vendor
            if code in _CHECK_TOO_MANY_CODES:
                return p.CHECK_TOO_MANY
            if status == 429:
                return p.CHECK_THROTTLED
            if status == 404:
                return p.CHECK_EXPIRED  # no open verification: expired, already used or never sent
            if code in _CHECK_WRONG_CODES:
                return p.CHECK_WRONG_CODE
            return p.CHECK_UNKNOWN
        if isinstance(exc, _NETWORK_ERRORS):
            return p.CHECK_NETWORK_ERROR
        return p.CHECK_UNKNOWN

    # --- lookup --------------------------------------------------------------------------
    def lookup(self, e164: str) -> p.LineInfo:
        key = None
        if self._cache is not None:
            key = self._hash(e164)
            cached = self._cache.get(key)
            if cached is not None:
                return cached
        try:
            data = self._client.lookups.v2.phone_numbers(e164).fetch(fields="line_type_intelligence")
        except Exception as exc:  # noqa: BLE001 - every failure becomes a reason code
            vendor = _vendor_error(exc)
            reason = p.LINE_INVALID_NUMBER if vendor and vendor[0] == 404 else p.LINE_LOOKUP_FAILED
            _log_failure("lookup", reason, exc)
            return p.LineInfo(False, None, None, reason)
        info = self._line_info(data)
        if self._cache is not None and key is not None and info.reason != p.LINE_UNKNOWN:
            self._cache.put(key, info, LOOKUP_CACHE_TTL_S)
        return info

    @staticmethod
    def _line_info(data: Any) -> p.LineInfo:
        country = getattr(data, "country_code", None)
        country = country if isinstance(country, str) and country else None
        if getattr(data, "valid", True) is False:
            return p.LineInfo(False, None, country, p.LINE_INVALID_NUMBER)
        intel = getattr(data, "line_type_intelligence", None)
        line_type = intel.get("type") if isinstance(intel, dict) else None
        line_type = line_type if isinstance(line_type, str) and line_type else None
        allowed, reason = p.classify_line_type(line_type)
        return p.LineInfo(allowed, line_type, country, reason)


def _build_client(creds: config.TwilioCredentials) -> Any:
    """The real Twilio client: API-key login, 5-second timeout, its request logging switched off."""
    from twilio.http.http_client import TwilioHttpClient
    from twilio.rest import Client

    silence_vendor_http_logs()

    http = TwilioHttpClient(timeout=TIMEOUT_S, logger=_silent_http_log)
    if creds.style == "api_key":
        return Client(creds.api_key_sid, creds.secret, creds.account_sid, http_client=http)
    return Client(creds.account_sid, creds.secret, http_client=http)


_default_cache: p.LookupCache | None = None


def set_lookup_cache(cache: p.LookupCache | None) -> None:
    """Install the durable line-type cache (card S6 wires the database one). None means no cache."""
    global _default_cache
    _default_cache = cache


def build_twilio_verify(*, client: Any = None, cache: p.LookupCache | None = None) -> TwilioVerifyProvider:
    """Build the adapter from the environment, or around a given (fake) client."""
    service_sid = config.twilio_verify_service_sid()
    if client is None:
        creds = config.twilio_credentials()
        if creds is None or not service_sid:
            raise RuntimeError("twilio_not_configured")
        client = _build_client(creds)
    return TwilioVerifyProvider(client, service_sid, cache=cache if cache is not None else _default_cache)
