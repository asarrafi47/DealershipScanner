from __future__ import annotations

import re
from typing import Final

import pyotp

_B32_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Z2-7]+=*$")


def new_base32_secret() -> str:
    return pyotp.random_base32()


def normalize_totp_code(code: str) -> str:
    return "".join(ch for ch in (code or "") if ch.isdigit())


def is_valid_base32_secret(secret: str) -> bool:
    s = (secret or "").strip().upper().replace(" ", "")
    if len(s) < 16:
        return False
    return bool(_B32_RE.fullmatch(s))


def otpauth_uri(*, secret: str, account_name: str, issuer: str) -> str:
    s = (secret or "").strip().upper().replace(" ", "")
    return pyotp.totp.TOTP(s).provisioning_uri(name=account_name, issuer_name=issuer)


def verify_totp(*, secret: str, code: str) -> bool:
    s = (secret or "").strip().upper().replace(" ", "")
    if not is_valid_base32_secret(s):
        return False
    c = normalize_totp_code(code)
    if len(c) != 6:
        return False
    return bool(pyotp.TOTP(s).verify(c, valid_window=1))

