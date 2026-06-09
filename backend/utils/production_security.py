"""Production deployment guards and privileged-access policy helpers."""

from __future__ import annotations

import logging
import os

from backend.utils.credential_db_encryption import assert_credential_db_encryption_config
from backend.utils.runtime_env import is_production_env

_log = logging.getLogger(__name__)


def app_admin_dev_pass_through_allowed() -> bool:
    """
    When False, app ``role=admin`` users must use ``/dev/login`` (dev_users.db), not app session alone.
    Production defaults to False unless ``ALLOW_APP_ADMIN_DEV_PASS_THROUGH=1``.
    """
    if not is_production_env():
        return True
    v = (os.environ.get("ALLOW_APP_ADMIN_DEV_PASS_THROUGH") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def assert_production_security_config() -> None:
    """Fail fast on dangerous production configuration (SEC-081, SEC-088)."""
    if not is_production_env():
        return

    assert_credential_db_encryption_config()

    dev_console = (os.environ.get("DEV_CONSOLE") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    dev_secret = bool((os.environ.get("DEV_CONSOLE_SECRET") or "").strip())
    if dev_console and not dev_secret:
        raise RuntimeError(
            "DEV_CONSOLE=1 requires DEV_CONSOLE_SECRET in production (SEC-081). "
            "Set a long random secret or disable DEV_CONSOLE."
        )

    if (os.environ.get("CAR_CHAT_WEB_RESEARCH_PUBLIC") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        _log.warning(
            "CAR_CHAT_WEB_RESEARCH_PUBLIC=1 in production allows Playwright web research for "
            "anonymous users (cost/abuse risk)."
        )

    if (os.environ.get("BILLING_STRIPE_ENABLED") or "").strip().lower() not in (
        "1",
        "true",
        "yes",
        "on",
    ):
        _log.warning(
            "BILLING_STRIPE_ENABLED is off in production; paid features use login gates only."
        )

    if not (os.environ.get("RATE_LIMIT_SQLITE_PATH") or "").strip():
        _log.warning(
            "RATE_LIMIT_SQLITE_PATH unset in production; per-IP rate limits are per worker process."
        )

    cors = (os.environ.get("SOCKETIO_CORS_ORIGINS") or "").strip()
    if cors == "*":
        _log.warning("SOCKETIO_CORS_ORIGINS=* in production allows any Socket.IO browser origin.")

    if not (os.environ.get("DEV_IP_ALLOWLIST") or "").strip():
        _log.warning(
            "/dev is reachable without DEV_IP_ALLOWLIST; set comma-separated IPs/CIDRs for defense in depth."
        )

    if (os.environ.get("ALLOW_DEV_PUBLIC_REGISTER") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        _log.warning(
            "ALLOW_DEV_PUBLIC_REGISTER=1 allows self-serve /dev/register operator accounts."
        )

    if (os.environ.get("TRUST_PROXY_HEADERS") or "").strip().lower() in ("1", "true", "yes", "on"):
        _log.warning(
            "TRUST_PROXY_HEADERS=1: rate limits trust X-Forwarded-For — use only behind a trusted reverse proxy."
        )
