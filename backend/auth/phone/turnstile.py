"""Cloudflare Turnstile check, run on the server for every send request (DESIGN.md section 6).

``verify_turnstile(token, ip)`` posts the browser's token to Cloudflare's siteverify with
``TURNSTILE_SECRET_KEY``. It **fails closed**: a missing token, a missing secret, a slow or
unreachable Cloudflare, an odd answer and a replayed token all mean no text. The HTTP layer is
injectable, so tests use a fake one and never touch the network. Nothing here logs the token, the
secret or the address; Cloudflare's error text is never kept, only our reason code.
"""

from __future__ import annotations

import hashlib
import logging
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

import requests

from backend.auth.phone import config
from backend.utils.runtime_env import is_production_env

_log = logging.getLogger("auth_phone")

SITEVERIFY_URL = "https://challenges.cloudflare.com/turnstile/v0/siteverify"
TIMEOUT_S = 3
MAX_TOKEN_LEN = 2048  # Cloudflare's documented maximum
REPLAY_WINDOW_S = 300  # a token is only valid for 5 minutes anyway

# TurnstileResult.reason
OK = "ok"
MISSING_TOKEN = "missing_token"
NOT_CONFIGURED = "not_configured"
REJECTED = "rejected"
REPLAYED = "replayed"
TIMEOUT = "timeout"
UNREACHABLE = "unreachable"
BAD_RESPONSE = "bad_response"
REASONS = (OK, MISSING_TOKEN, NOT_CONFIGURED, REJECTED, REPLAYED, TIMEOUT, UNREACHABLE, BAD_RESPONSE)

# (status code, parsed JSON body) from one POST; raises requests exceptions on a network failure.
HttpPost = Callable[[str, dict[str, str], float], tuple[int, Any]]


@dataclass(frozen=True)
class TurnstileResult:
    ok: bool
    reason: str  # one of REASONS


class TurnstileVerifier(Protocol):
    def verify(self, token: str | None, ip: str | None) -> TurnstileResult: ...


def _requests_post(url: str, data: dict[str, str], timeout: float) -> tuple[int, Any]:
    response = requests.post(url, data=data, timeout=timeout)
    try:
        body = response.json()
    except ValueError:
        body = None
    return response.status_code, body


class _SeenTokens:
    """Tokens already presented, kept briefly: a token is single use. Process-local; Cloudflare's
    own duplicate check is the authority, this refuses a replay without paying for the call."""

    def __init__(self, clock: Callable[[], float], max_entries: int = 5000) -> None:
        self._clock = clock
        self._max = max_entries
        self._seen: OrderedDict[str, float] = OrderedDict()
        self._lock = threading.Lock()

    @staticmethod
    def _key(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    def _expire(self, now: float) -> None:
        while self._seen and (len(self._seen) >= self._max or next(iter(self._seen.values())) <= now):
            self._seen.popitem(last=False)

    def is_used(self, token: str) -> bool:
        """True when this token already got a definitive answer from Cloudflare."""
        key = self._key(token)
        now = self._clock()
        with self._lock:
            self._expire(now)
            return key in self._seen

    def mark_used(self, token: str) -> None:
        """Remember a token after Cloudflare answered definitively (accepted or rejected it). Never called
        for a timeout or an unreadable answer: a good token must survive a slow Cloudflare."""
        key = self._key(token)
        now = self._clock()
        with self._lock:
            self._expire(now)
            self._seen[key] = now + REPLAY_WINDOW_S


class CloudflareTurnstile:
    def __init__(
        self,
        secret: str | None = None,
        *,
        http_post: HttpPost = _requests_post,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._secret = secret
        self._post = http_post
        self._seen = _SeenTokens(clock)

    def verify(self, token: str | None, ip: str | None) -> TurnstileResult:
        secret = self._secret if self._secret is not None else config.turnstile_secret_key()
        result = self._verify(secret, token, ip)
        if not result.ok:
            _log.info("event=blocked_turnstile reason=%s", result.reason)
        return result

    def _verify(self, secret: str, token: str | None, ip: str | None) -> TurnstileResult:
        text = token.strip() if isinstance(token, str) else ""
        if not text or len(text) > MAX_TOKEN_LEN:
            return TurnstileResult(False, MISSING_TOKEN)
        if not secret:
            return TurnstileResult(False, NOT_CONFIGURED)
        if self._seen.is_used(text):
            return TurnstileResult(False, REPLAYED)
        data = {"secret": secret, "response": text}
        if ip:
            data["remoteip"] = ip
        try:
            status, body = self._post(SITEVERIFY_URL, data, TIMEOUT_S)
        except requests.Timeout:
            return TurnstileResult(False, TIMEOUT)
        except Exception:  # noqa: BLE001 - any failure to ask means no text
            return TurnstileResult(False, UNREACHABLE)
        if status != 200 or not isinstance(body, dict) or not isinstance(body.get("success"), bool):
            return TurnstileResult(False, BAD_RESPONSE)
        self._seen.mark_used(text)  # a definitive answer: single use from here on
        if body["success"]:
            return TurnstileResult(True, OK)
        codes = body.get("error-codes")
        if isinstance(codes, list) and "timeout-or-duplicate" in codes:
            return TurnstileResult(False, REPLAYED)
        return TurnstileResult(False, REJECTED)


class FakeTurnstileVerifier:
    """For tests and local development. Accepts the tokens it is given, each once. Never in production."""

    def __init__(self, valid_tokens: set[str] | frozenset[str] = frozenset({"pass"})) -> None:
        if is_production_env():
            raise RuntimeError("the fake Turnstile verifier cannot load in production")
        self._valid = set(valid_tokens)
        self._used: set[str] = set()
        self.calls: list[tuple[str | None, str | None]] = []

    def verify(self, token: str | None, ip: str | None) -> TurnstileResult:
        self.calls.append((token, ip))
        if not token:
            return TurnstileResult(False, MISSING_TOKEN)
        if token in self._used:
            return TurnstileResult(False, REPLAYED)
        if token not in self._valid:
            return TurnstileResult(False, REJECTED)
        self._used.add(token)
        return TurnstileResult(True, OK)


_default: TurnstileVerifier | None = None
_default_lock = threading.Lock()


def set_verifier(verifier: TurnstileVerifier | None) -> None:
    """Install a verifier (tests: the fake). None puts the Cloudflare one back."""
    global _default
    with _default_lock:
        _default = verifier


def get_verifier() -> TurnstileVerifier:
    global _default
    with _default_lock:
        if _default is None:
            _default = CloudflareTurnstile()
        return _default


def verify_turnstile(token: str | None, ip: str | None) -> TurnstileResult:
    """Check one Turnstile token with the process-wide verifier. Fails closed."""
    return get_verifier().verify(token, ip)
