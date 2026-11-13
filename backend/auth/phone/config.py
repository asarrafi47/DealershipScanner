"""Settings for phone sign-in, read from the environment on every call (DESIGN.md section 20).

Names only live in this file, never values. A malformed number falls back to its default.
Nothing here logs a value.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from backend.utils.runtime_env import is_production_env

_TRUE = ("1", "true", "yes", "on")
PEPPER_MIN_LENGTH = 32
PROVIDER_FAKE = "fake"
PROVIDER_TWILIO = "twilio_verify"
KNOWN_PROVIDERS = (PROVIDER_FAKE, PROVIDER_TWILIO)


def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def _int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = _env(name)
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value >= minimum else default


def enabled() -> bool:
    """The master switch. Off (unset) means every phone route answers 404 and nothing is sent."""
    return _env("AUTH_PHONE_LOGIN_ENABLED").lower() in _TRUE


def provider_name() -> str:
    """``AUTH_PHONE_PROVIDER``. Unset means the fake outside production and Twilio Verify in it."""
    raw = _env("AUTH_PHONE_PROVIDER").lower()
    if raw:
        return raw
    return PROVIDER_TWILIO if is_production_env() else PROVIDER_FAKE


def allowed_countries() -> frozenset[str]:
    """``SMS_ALLOWED_COUNTRIES``: comma-separated ISO region codes, default ``US``."""
    raw = _env("SMS_ALLOWED_COUNTRIES")
    parts = {p.strip().upper() for p in raw.split(",") if p.strip()}
    return frozenset(parts) if parts else frozenset({"US"})


def allowed_numbers() -> frozenset[str]:
    """``AUTH_PHONE_ALLOWED_NUMBERS``: optional list that limits sign-in during the first test days.

    Entries are as typed; callers normalise them. Empty means no restriction.
    """
    raw = _env("AUTH_PHONE_ALLOWED_NUMBERS")
    return frozenset(p.strip() for p in raw.split(",") if p.strip())


def daily_send_cap() -> int:
    return _int("SMS_DAILY_SEND_CAP", 300, minimum=0)


def daily_spend_cap_cents() -> int:
    return _int("SMS_DAILY_SPEND_CAP_CENTS", 1500, minimum=0)


def alert_send_threshold() -> int:
    return _int("SMS_ALERT_SEND_THRESHOLD", 150, minimum=0)


def est_cost_cents() -> int:
    return _int("SMS_EST_COST_CENTS", 6, minimum=0)


def code_ttl_s() -> int:
    return _int("AUTH_PHONE_CODE_TTL_S", 300, minimum=1)


def max_wrong_tries() -> int:
    return _int("AUTH_PHONE_MAX_WRONG_TRIES", 5, minimum=1)


def lock_minutes() -> int:
    return _int("AUTH_PHONE_LOCK_MINUTES", 15, minimum=1)


def resend_wait_s() -> int:
    return _int("AUTH_PHONE_RESEND_WAIT_S", 60, minimum=0)


def pepper() -> str:
    """``PHONE_HASH_PEPPER`` (empty when unset)."""
    return _env("PHONE_HASH_PEPPER")


def turnstile_site_key() -> str:
    return _env("TURNSTILE_SITE_KEY")


def turnstile_secret_key() -> str:
    return _env("TURNSTILE_SECRET_KEY")


def twilio_verify_service_sid() -> str:
    return _env("TWILIO_VERIFY_SERVICE_SID")


@dataclass(frozen=True)
class TwilioCredentials:
    """Which Twilio login the adapter uses. ``repr`` never shows a secret."""

    account_sid: str = field(repr=False)
    api_key_sid: str = field(default="", repr=False)
    secret: str = field(default="", repr=False)  # the API key secret, or the auth token
    style: str = "api_key"  # "api_key" or "auth_token"


def twilio_credentials() -> TwilioCredentials | None:
    """The API key (``TWILIO_API_KEY_SID`` + ``TWILIO_API_KEY_SECRET`` + ``TWILIO_ACCOUNT_SID``).

    The auth token (``TWILIO_AUTH_TOKEN`` + ``TWILIO_ACCOUNT_SID``) is accepted as a fallback.
    The API key wins when both are set. None when neither style is complete.
    """
    account = _env("TWILIO_ACCOUNT_SID")
    key_sid = _env("TWILIO_API_KEY_SID")
    key_secret = _env("TWILIO_API_KEY_SECRET")
    token = _env("TWILIO_AUTH_TOKEN")
    if account and key_sid and key_secret:
        return TwilioCredentials(account_sid=account, api_key_sid=key_sid, secret=key_secret)
    if account and token:
        return TwilioCredentials(account_sid=account, secret=token, style="auth_token")
    return None


def twilio_webhook_secrets() -> tuple[str, ...]:
    """Secrets that may sign a Twilio callback: the auth token, then the API key secret."""
    out = [_env("TWILIO_AUTH_TOKEN"), _env("TWILIO_API_KEY_SECRET")]
    return tuple(s for s in out if s)


def twilio_ready() -> bool:
    return twilio_credentials() is not None and bool(twilio_verify_service_sid())


def missing_for_production() -> list[str]:
    """Names (never values) of what a production start with the flag on is missing or has wrong."""
    if not enabled():
        return []
    problems: list[str] = []
    provider = provider_name()
    if provider not in KNOWN_PROVIDERS:
        problems.append("AUTH_PHONE_PROVIDER (unknown provider)")
    if provider == PROVIDER_FAKE:
        problems.append("AUTH_PHONE_PROVIDER (the fake provider is not allowed in production)")
    if len(pepper()) < PEPPER_MIN_LENGTH:
        problems.append("PHONE_HASH_PEPPER (missing or shorter than %d characters)" % PEPPER_MIN_LENGTH)
    if not turnstile_secret_key():
        problems.append("TURNSTILE_SECRET_KEY")
    if not turnstile_site_key():
        problems.append("TURNSTILE_SITE_KEY")
    if provider == PROVIDER_TWILIO:
        if twilio_credentials() is None:
            problems.append(
                "TWILIO_ACCOUNT_SID with TWILIO_API_KEY_SID and TWILIO_API_KEY_SECRET "
                "(or TWILIO_AUTH_TOKEN)"
            )
        if not twilio_verify_service_sid():
            problems.append("TWILIO_VERIFY_SERVICE_SID")
    return problems


def assert_phone_login_config_for_production() -> None:
    """Refuse to start in production with the flag on and a missing or unsafe value.

    Does nothing when the flag is off or the process is not production.
    """
    if not is_production_env():
        return
    problems = missing_for_production()
    if problems:
        raise RuntimeError(
            "AUTH_PHONE_LOGIN_ENABLED is on but phone sign-in is not safely configured: "
            + "; ".join(problems)
            + ". Set these on the service or turn the flag off."
        )
