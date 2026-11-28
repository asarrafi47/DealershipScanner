"""Settings for phone sign-in, read from the environment on every call (DESIGN.md section 20).

Names only live in this file, never values. A malformed number falls back to its default.
Nothing here logs a value.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from dataclasses import dataclass, field

from backend.utils.runtime_env import is_production_env

_log = logging.getLogger("auth_phone")
_warned: set[str] = set()

_TRUE = ("1", "true", "yes", "on")
PEPPER_MIN_LENGTH = 32
# Version of the consent text the phone page shows. The legal pages (card S14) are approved by the owner
# by setting LEGAL_PAGES_APPROVED_VERSION to this same value; until then the flag may not be on in production.
CONSENT_TEXT_VERSION = "1"
PROVIDER_FAKE = "fake"
PROVIDER_TWILIO = "twilio_verify"
KNOWN_PROVIDERS = (PROVIDER_FAKE, PROVIDER_TWILIO)


def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def _warn_once(name: str) -> None:
    """One warning per variable name when its value is unusable. Never the value."""
    if name in _warned:
        return
    _warned.add(name)
    _log.warning("event=config_malformed var=%s using_default=1", name)


def reset_warnings() -> None:
    """Forget which malformed variables were already reported (tests)."""
    _warned.clear()


def _int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = _env(name)
    if not raw:
        return default
    # ASCII digits only: int() would also take "٣" (Arabic-Indic), "1_0" and "+5".
    if not (raw.isascii() and raw.isdigit()):
        _warn_once(name)
        return default
    value = int(raw)
    if value < minimum:
        _warn_once(name)
        return default
    return value


# Set by the production boot check (review R5) when the flag is on but a soft check failed: phone
# sign-in is then off for this process while the rest of the site starts normally. Per process.
_boot_disabled: str | None = None


def flag_set() -> bool:
    """``AUTH_PHONE_LOGIN_ENABLED`` as written, before the boot check. Routes use :func:`enabled`."""
    return _env("AUTH_PHONE_LOGIN_ENABLED").lower() in _TRUE


def enabled() -> bool:
    """The master switch. Off (unset) means every phone route answers 404 and nothing is sent.

    Also False while :func:`boot_disabled_reason` is set (the production boot check found a problem)."""
    return flag_set() and _boot_disabled is None


def boot_disabled_reason() -> str | None:
    """Why the production boot check turned phone sign-in off for this process (names only), or None."""
    return _boot_disabled


def disable_for_boot(reason: str) -> None:
    """Turn phone sign-in off for this process and say so in ONE critical log line (never a value)."""
    global _boot_disabled
    _boot_disabled = reason
    _log.critical(
        "event=phone_login_boot_disabled AUTH_PHONE_LOGIN_ENABLED is on but phone sign-in is not safely "
        "configured, so it is OFF in this process (admin, dealer and password sign-in are unaffected): %s",
        reason,
    )


def reset_boot_state() -> None:
    """Forget a boot-time disable (tests, and the start of every boot check)."""
    global _boot_disabled
    _boot_disabled = None


def provider_name() -> str:
    """``AUTH_PHONE_PROVIDER``. Unset means Twilio Verify, everywhere: the fake provider is opt-in
    (``AUTH_PHONE_PROVIDER=fake``, refused in production) and is never chosen because a variable is missing."""
    return _env("AUTH_PHONE_PROVIDER").lower() or PROVIDER_TWILIO


def allowed_countries() -> frozenset[str]:
    """``SMS_ALLOWED_COUNTRIES``: comma-separated ISO region codes, default ``US``."""
    raw = _env("SMS_ALLOWED_COUNTRIES")
    if not raw:
        return frozenset({"US"})
    parts = {p.strip().upper() for p in raw.split(",") if p.strip()}
    if not parts or any(not (len(p) == 2 and p.isascii() and p.isalpha()) for p in parts):
        _warn_once("SMS_ALLOWED_COUNTRIES")
        return frozenset({"US"})
    return frozenset(parts)


def allowed_numbers() -> frozenset[str]:
    """``AUTH_PHONE_ALLOWED_NUMBERS``: optional list that limits sign-in during the first test days.

    Entries are as typed; callers normalise them. Empty means no restriction.
    """
    raw = _env("AUTH_PHONE_ALLOWED_NUMBERS")
    return frozenset(p.strip() for p in raw.split(",") if p.strip())


def allow_list_set() -> bool:
    """True when ``AUTH_PHONE_ALLOWED_NUMBERS`` holds anything at all. A set list is a restriction even
    when none of its entries parse: a typo must close sign-in, never open it to every number."""
    return bool(allowed_numbers())


def allow_list_unparsable_count() -> int:
    """How many allow-list entries are not a number the site would send to (never which ones)."""
    from backend.auth.phone import numbers  # numbers imports this module

    return sum(1 for raw in allowed_numbers() if numbers.try_normalize(raw) is None)


# First-month launch values (owner decision 2026-11-14): 30 texts and 300 cents a day. They are
# the code defaults on purpose, so an unset variable on release day can never mean a bigger day.
# 300 sends and 1500 cents are only later targets the owner may choose after a month of real numbers.
DEFAULT_DAILY_SEND_CAP = 30
DEFAULT_DAILY_SPEND_CAP_CENTS = 300


def daily_send_cap() -> int:
    return _int("SMS_DAILY_SEND_CAP", DEFAULT_DAILY_SEND_CAP, minimum=0)


def daily_spend_cap_cents() -> int:
    return _int("SMS_DAILY_SPEND_CAP_CENTS", DEFAULT_DAILY_SPEND_CAP_CENTS, minimum=0)


def alert_send_threshold() -> int:
    """``SMS_ALERT_SEND_THRESHOLD``; unset means half of the day's send cap (15 at the launch cap of 30)."""
    return _int("SMS_ALERT_SEND_THRESHOLD", daily_send_cap() // 2, minimum=0)


def est_cost_cents() -> int:
    return _int("SMS_EST_COST_CENTS", 6, minimum=0)


def est_lookup_cents() -> int:
    """``SMS_EST_LOOKUP_CENTS``: the line-type lookup cost counted at reserve time (default 1)."""
    return _int("SMS_EST_LOOKUP_CENTS", 1, minimum=0)


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


def dev_pepper_allowed() -> bool:
    """``ALLOW_DEV_PHONE_PEPPER``: opt-in to the fixed development pepper. Never true in production,
    and never implied by an unset ``PHONE_HASH_PEPPER``."""
    return _env("ALLOW_DEV_PHONE_PEPPER").lower() in _TRUE and not is_production_env()


def legal_pages_approved_version() -> str:
    """``LEGAL_PAGES_APPROVED_VERSION``: the consent text version the owner approved (empty when unset)."""
    return _env("LEGAL_PAGES_APPROVED_VERSION")


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


def twilio_webhook_token() -> str:
    """The account auth token, the only secret Twilio signs callbacks with (an API key secret never
    matches). Never required at boot: callbacks are not used at launch."""
    return _env("TWILIO_AUTH_TOKEN")


def webhook_enabled() -> bool:
    """``TWILIO_WEBHOOK_ENABLED``: callbacks are NOT used at launch, so this is off unless set."""
    return _env("TWILIO_WEBHOOK_ENABLED").lower() in _TRUE


def webhook_active() -> bool:
    """The webhook route is live only when ``TWILIO_WEBHOOK_ENABLED`` and ``TWILIO_AUTH_TOKEN`` are both set;
    otherwise it answers 404 and does nothing."""
    return webhook_enabled() and bool(twilio_webhook_token())


def twilio_ready() -> bool:
    return twilio_credentials() is not None and bool(twilio_verify_service_sid())


def _proxy_chain_spoofable() -> bool:
    """True when X-Forwarded-For is trusted for 2+ hops with no trusted ranges (backend/utils/client_ip.py):
    the per-address phone limits would then key on an address the client wrote itself."""
    from backend.utils import client_ip as cip  # imported here: keeps this module import-light

    return cip.trust_proxy_headers() and cip.trusted_proxy_hops() >= 2 and cip.trusted_proxy_cidrs() is None


def support_contact() -> str:
    """``SUPPORT_CONTACT``: where a person can get help (an address or a link), shown beside the consent text
    and the person-check message. CTIA asks for a HELP contact, so production refuses the flag without it."""
    return _env("SUPPORT_CONTACT")[:200]



def hard_problems_for_production() -> list[str]:
    """What stops a production start outright (review R5): the fake provider, a missing or short pepper.

    Names only, never values. Empty when the flag is off."""
    if not flag_set():
        return []
    problems: list[str] = []
    if provider_name() == PROVIDER_FAKE:
        problems.append("AUTH_PHONE_PROVIDER (the fake provider is not allowed in production)")
    if len(pepper()) < PEPPER_MIN_LENGTH:
        problems.append("PHONE_HASH_PEPPER (missing or shorter than %d characters)" % PEPPER_MIN_LENGTH)
    return problems


def _legal_problems() -> list[str]:
    """``legal.missing_legal_pages()``, called defensively: a missing module or an error is a problem too."""
    try:
        from backend.auth.phone import legal
    except Exception as exc:  # noqa: BLE001 - the boot check must never crash on it
        return ["legal pages (the legal module could not be loaded: %s)" % type(exc).__name__]
    try:
        missing = list(legal.missing_legal_pages() or [])
    except Exception as exc:  # noqa: BLE001
        return ["legal pages (the check failed: %s)" % type(exc).__name__]
    return ["legal pages missing (%s)" % ", ".join(str(m) for m in missing)] if missing else []


def soft_problems_for_production() -> list[str]:
    """Every other production problem (review R5): it turns phone sign-in off, the site still starts.

    Names only, never values. Empty when the flag is off. A check that raises is reported as a problem."""
    if not flag_set():
        return []
    problems: list[str] = []

    def _try(name: str, check: Callable[[], list[str]]) -> None:
        try:
            problems.extend(check())
        except Exception as exc:  # noqa: BLE001 - a failing check disables phone sign-in, never the site
            problems.append("%s (the check failed: %s)" % (name, type(exc).__name__))

    provider = provider_name()
    if provider not in KNOWN_PROVIDERS:
        problems.append("AUTH_PHONE_PROVIDER (unknown provider)")
    _try("AUTH_PHONE_ALLOWED_NUMBERS", lambda: [
        "AUTH_PHONE_ALLOWED_NUMBERS (an entry is not a valid number in SMS_ALLOWED_COUNTRIES)"
    ] if allow_list_unparsable_count() else [])
    if legal_pages_approved_version() != CONSENT_TEXT_VERSION:
        problems.append("LEGAL_PAGES_APPROVED_VERSION (must equal the consent text version)")
    _try("TRUSTED_PROXY_CIDRS", lambda: [
        "TRUSTED_PROXY_CIDRS (set it to cloudflare: with TRUSTED_PROXY_HOPS of 2 or more and no trusted "
        "ranges, a request sent straight to the origin picks its own address and escapes every per-address limit)"
    ] if _proxy_chain_spoofable() else [])
    if not turnstile_secret_key():
        problems.append("TURNSTILE_SECRET_KEY")
    if not turnstile_site_key():
        problems.append("TURNSTILE_SITE_KEY")
    if not support_contact():
        problems.append("SUPPORT_CONTACT (the HELP contact shown beside the consent text)")
    if provider == PROVIDER_TWILIO:
        if twilio_credentials() is None:
            problems.append(
                "TWILIO_ACCOUNT_SID with TWILIO_API_KEY_SID and TWILIO_API_KEY_SECRET "
                "(or TWILIO_AUTH_TOKEN)"
            )
        if not twilio_verify_service_sid():
            problems.append("TWILIO_VERIFY_SERVICE_SID")
    problems.extend(_legal_problems())
    return problems


def missing_for_production() -> list[str]:
    """Names (never values) of everything a production start with the flag on is missing or has wrong:
    the hard problems first, then the soft ones."""
    return hard_problems_for_production() + soft_problems_for_production()


def assert_phone_login_config_for_production() -> None:
    """The production boot check for phone sign-in (review R5).

    Does nothing at all when the process is not production or the flag is off. Otherwise a hard
    problem (fake provider, missing or short pepper) raises RuntimeError and the start fails; any
    other problem logs one critical line and turns phone sign-in off for this process
    (:func:`boot_disabled_reason`), so admin, dealer and password sign-in keep working.
    """
    if not is_production_env() or not flag_set():
        return
    reset_boot_state()
    hard = hard_problems_for_production()
    if hard:
        raise RuntimeError(
            "AUTH_PHONE_LOGIN_ENABLED is on but phone sign-in is not safely configured: "
            + "; ".join(hard)
            + ". Set these on the service or turn the flag off."
        )
    try:
        soft = soft_problems_for_production()
    except Exception as exc:  # noqa: BLE001 - belt and braces: never crash the site on a soft check
        soft = ["phone configuration (the check failed: %s)" % type(exc).__name__]
    if soft:
        disable_for_boot("; ".join(soft))
