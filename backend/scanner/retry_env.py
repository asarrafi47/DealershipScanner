"""Allowlist for smart-retry subprocess environment overrides."""
from __future__ import annotations

from typing import Any

_EXPLICIT_ALLOWED = frozenset({"PUPPETEER_EXECUTABLE_PATH"})

# Always reject — even if a key would match another allow rule.
_ALWAYS_DENIED = frozenset(
    {
        "INVENTORY_DATABASE_URL",
        "PYTHONPATH",
        "LD_PRELOAD",
        "LD_LIBRARY_PATH",
        "DYLD_INSERT_LIBRARIES",
        "PATH",
        "HOME",
        "SHELL",
        "SECRET_KEY",
        "USERS_DB_ENCRYPTION_KEY",
        "DEV_USERS_DB_ENCRYPTION_KEY",
        "ANTHROPIC_API_KEY",
        "GOOGLE_OAUTH_CLIENT_SECRET",
        "STRIPE_SECRET_KEY",
    }
)


def is_retry_env_key_allowed(key: str) -> bool:
    """Return True when ``key`` may be copied into a scanner worker subprocess env."""
    k = (key or "").strip()
    if not k:
        return False
    if k in _ALWAYS_DENIED:
        return False
    if k in _EXPLICIT_ALLOWED:
        return True
    return k.startswith("SCANNER_")


def filter_retry_env(env: dict[str, Any] | None) -> dict[str, str]:
    """Return only allowlisted keys from ``env`` (values coerced to str)."""
    out: dict[str, str] = {}
    for key, val in (env or {}).items():
        k = str(key).strip()
        if is_retry_env_key_allowed(k):
            out[k] = str(val)
    return out
