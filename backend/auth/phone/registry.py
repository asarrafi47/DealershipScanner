"""Chooses the provider for this process (``AUTH_PHONE_PROVIDER``). Never builds the fake in production.

No vendor library is imported until the vendor's adapter is asked for.
"""

from __future__ import annotations

import threading

from backend.auth.phone import config
from backend.auth.phone.provider import PhoneVerifyProvider
from backend.utils.runtime_env import is_production_env

_lock = threading.Lock()
_cached: PhoneVerifyProvider | None = None


class ProviderUnavailable(RuntimeError):
    """No usable provider. ``str(exc)`` is a short reason code, never a value."""


def get_provider() -> PhoneVerifyProvider:
    """The process-wide provider. Raises :class:`ProviderUnavailable` instead of falling back."""
    global _cached
    with _lock:
        if _cached is None:
            _cached = _build(config.provider_name())
        return _cached


def _build(name: str) -> PhoneVerifyProvider:
    if name == config.PROVIDER_FAKE:
        if is_production_env():
            raise ProviderUnavailable("fake_provider_forbidden_in_production")
        from backend.auth.phone.fake_provider import FakePhoneProvider

        return FakePhoneProvider()
    if name == config.PROVIDER_TWILIO:
        # The Twilio Verify adapter arrives with card S3.
        raise ProviderUnavailable("twilio_adapter_not_available")
    raise ProviderUnavailable("unknown_provider")


def reset() -> None:
    """Forget the cached provider (tests; a changed environment)."""
    global _cached
    with _lock:
        _cached = None
