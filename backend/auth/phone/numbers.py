"""Phone numbers: one canonical form (E.164), a masked form for screens, hashes for limits.

No function here logs or puts its input into an exception message; a refusal carries only a
short reason code.
"""

from __future__ import annotations

import hashlib
import hmac
import re

import phonenumbers
from phonenumbers import PhoneNumberType

from backend.auth.phone import config
from backend.utils.client_ip import canonical_ip  # noqa: F401  (the one /64 form; re-exported)
from backend.utils.runtime_env import is_production_env

# Reason codes of a refusal (never the input).
EMPTY = "empty"
INVALID = "invalid"
PREMIUM = "premium_rate"
COUNTRY_NOT_ALLOWED = "country_not_allowed"

_MAX_INPUT_LEN = 40
_REFUSED_TYPES = (PhoneNumberType.PREMIUM_RATE, PhoneNumberType.SHARED_COST)
# The development pepper: used only when ``ALLOW_DEV_PHONE_PEPPER`` opts in, never in production, never
# because ``PHONE_HASH_PEPPER`` happens to be unset. Longer than the minimum length (32 characters).
DEV_PEPPER = "dev-only-pepper-not-a-secret-000000"
PEPPER_MIN_LENGTH = config.PEPPER_MIN_LENGTH


class PhoneNumberRefused(ValueError):
    """The number cannot be used. ``str(exc)`` and ``.reason`` are a short code only."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class PepperMissing(RuntimeError):
    """No usable ``PHONE_HASH_PEPPER`` (unset, or shorter than the minimum, and no dev opt-in).
    The message names the variable only, never a value."""


def normalize(
    raw: str | None,
    *,
    allowed_countries: frozenset[str] | set[str] | None = None,
    default_region: str = "US",
) -> str:
    """Return the E.164 form of ``raw`` or raise :class:`PhoneNumberRefused`.

    Refused: empty, letters (so vanity numbers and extensions), unparsable or invalid numbers,
    premium-rate and shared-cost numbers, and any country not in ``allowed_countries`` (default
    ``SMS_ALLOWED_COUNTRIES``, then ``US``). The country comes from the library, not the prefix:
    +1 also covers Canada and the Caribbean.
    """
    text = raw.strip() if isinstance(raw, str) else ""
    if not text:
        raise PhoneNumberRefused(EMPTY)
    if len(text) > _MAX_INPUT_LEN or any(ch.isalpha() for ch in text):
        raise PhoneNumberRefused(INVALID)
    try:
        number = phonenumbers.parse(text, default_region)
    except phonenumbers.NumberParseException:
        raise PhoneNumberRefused(INVALID) from None
    if number.extension or not phonenumbers.is_valid_number(number):
        raise PhoneNumberRefused(INVALID)
    if phonenumbers.number_type(number) in _REFUSED_TYPES:
        raise PhoneNumberRefused(PREMIUM)
    region = phonenumbers.region_code_for_number(number)
    allowed = frozenset(allowed_countries) if allowed_countries is not None else config.allowed_countries()
    if not region or region not in allowed:
        raise PhoneNumberRefused(COUNTRY_NOT_ALLOWED)
    return phonenumbers.format_number(number, phonenumbers.PhoneNumberFormat.E164)


def try_normalize(raw: str | None, **kwargs) -> str | None:
    """:func:`normalize`, or None on any refusal."""
    try:
        return normalize(raw, **kwargs)
    except PhoneNumberRefused:
        return None


def country_of(e164: str) -> str | None:
    """ISO region of a normalised number (``US``, ``CA``...), or None."""
    try:
        return phonenumbers.region_code_for_number(phonenumbers.parse(e164, None)) or None
    except phonenumbers.NumberParseException:
        return None


def mask(value: str | None) -> str:
    """``ending 23``: the last two digits only. Two asterisks when there are fewer than two digits."""
    digits = re.sub(r"[^0-9]", "", value or "")  # ASCII digits only (\d would also match Arabic-Indic ones)
    return "ending " + (digits[-2:] if len(digits) >= 2 else "**")


def pepper_bytes() -> bytes:
    """THE pepper reader (the device cookie and the chat gate use it too).

    ``PHONE_HASH_PEPPER`` of at least 32 characters. Unset or too short raises :class:`PepperMissing`,
    except outside production with ``ALLOW_DEV_PHONE_PEPPER`` on, which gives the fixed development pepper.
    """
    value = config.pepper()
    if value:
        if len(value) < PEPPER_MIN_LENGTH:
            raise PepperMissing("PHONE_HASH_PEPPER is shorter than %d characters" % PEPPER_MIN_LENGTH)
        return value.encode("utf-8")
    if not is_production_env() and config.dev_pepper_allowed():
        return DEV_PEPPER.encode("utf-8")
    raise PepperMissing("PHONE_HASH_PEPPER is not set")


def _subkey(domain: str) -> bytes:
    """Per-domain key: HMAC(pepper, domain). A hash of one kind never matches another kind."""
    return hmac.new(pepper_bytes(), domain.encode("utf-8"), hashlib.sha256).digest()


def _hmac(domain: str, value: str) -> str:
    return hmac.new(_subkey(domain), (domain + ":" + value).encode("utf-8"), hashlib.sha256).hexdigest()


def hash_value(domain: str, value: str) -> str:
    """Keyed hash of ``value`` in the namespace ``domain`` (``kind:value`` format, hex SHA-256 HMAC)."""
    return _hmac(domain, value)


def hash_phone(e164: str) -> str:
    """HMAC-SHA256 of a normalised number with ``PHONE_HASH_PEPPER`` (hex, 64 characters)."""
    return _hmac("phone", e164)


def hash_ip(ip: str | None) -> str:
    """HMAC-SHA256 of the counted form of an address (IPv6 by /64), same pepper, own domain."""
    return _hmac("ip", canonical_ip(ip))


def hash_device(device_id: str) -> str:
    """HMAC-SHA256 of a device cookie id, own domain, same pepper."""
    return _hmac("device", device_id)


def short_hash(value: str) -> str:
    """A hash cut to 8 characters, the only form allowed in a log line."""
    return (value or "")[:8]
