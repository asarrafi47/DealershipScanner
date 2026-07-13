"""Central config loaded from environment (.env in dev).

Single env read point (zumai pattern): ``os.getenv`` for app-level settings
should live here; other modules import :class:`Config` and read attributes or
call accessors instead of scattering ``os.environ[...]`` lookups.

Two access styles, chosen deliberately:

* **Eager class attributes** — evaluated once, when this module is first
  imported. Used for values that are fixed for the life of the process and
  that no test overrides across ``importlib.reload(backend.main)`` (reloading
  ``backend.main`` does *not* reload this module, so eager attributes would go
  stale for those).
* **Lazy static methods** — read ``os.getenv`` at call time. Used for values
  tests override with ``monkeypatch.setenv`` (``SECRET_KEY``,
  ``BILLING_STRIPE_ENABLED``, ``CSP_ENFORCE``, ...) either per request or
  before a ``reload(backend.main)``.

Production fail-fast checks (SEC-001 missing ``SECRET_KEY``, SEC-002 admin
password) intentionally stay at their current call sites (``backend.main``,
``init_admin_db``); this module never substitutes dev defaults for them.
"""

import os

from backend.utils.project_env import load_project_dotenv

# Must run before any Config attribute is evaluated: class attributes read the
# environment exactly once, at import time.
load_project_dotenv()

_TRUTHY = ("1", "true", "yes", "on")


def _bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).lower() in _TRUTHY


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


class Config:
    # Auth / registration
    MIN_PASSWORD_LENGTH = max(8, _int("MIN_PASSWORD_LENGTH", 8))

    # Request body limits. Cap JSON POST bodies (smart search, chat) while
    # allowing dealer multipart uploads (8 MiB+).
    CHAT_MAX_MESSAGE_CHARS = _int("CHAT_MAX_MESSAGE_CHARS", 4000)
    CHAT_MAX_BODY_BYTES = _int("CHAT_MAX_BODY_BYTES", 65536)
    _DEFAULT_MAX_CONTENT = max(9 * 1024 * 1024, CHAT_MAX_BODY_BYTES * 2)
    MAX_REQUEST_BODY_BYTES = _int("MAX_REQUEST_BODY_BYTES", _DEFAULT_MAX_CONTENT)

    # Per-IP rate limits (events per minute).
    RATE_LIMIT_SMART_SEARCH_PER_MIN = _int("RATE_LIMIT_SMART_SEARCH_PER_MIN", 90)
    RATE_LIMIT_LOGIN_PER_MIN = _int("RATE_LIMIT_LOGIN_PER_MIN", 30)
    RATE_LIMIT_REGISTER_PER_MIN = _int("RATE_LIMIT_REGISTER_PER_MIN", 10)
    RATE_LIMIT_DEALER_LOCATOR_PER_MIN = _int("RATE_LIMIT_DEALER_LOCATOR_PER_MIN", 30)
    RATE_LIMIT_NHTSA_RECALLS_PER_MIN = _int("RATE_LIMIT_NHTSA_RECALLS_PER_MIN", 30)

    # ------------------------------------------------------------------
    # Lazy accessors: env read at call time (test-overridden / per-request).
    # ------------------------------------------------------------------

    @staticmethod
    def secret_key_raw() -> str:
        """Flask session secret (``SECRET_KEY`` or legacy ``FLASK_SECRET_KEY``).

        Returns "" when unset — the SEC-001 production fail-fast in
        ``backend.main`` depends on the empty value; no dev default here.
        """
        return (os.getenv("SECRET_KEY") or os.getenv("FLASK_SECRET_KEY") or "").strip()

    @staticmethod
    def billing_stripe_enabled() -> bool:
        return (os.getenv("BILLING_STRIPE_ENABLED") or "").strip().lower() in _TRUTHY

    @staticmethod
    def csp_enforce_raw() -> str:
        """Tri-state ``CSP_ENFORCE`` (off / on / unset -> production default)."""
        return (os.getenv("CSP_ENFORCE") or "").strip().lower()

    @staticmethod
    def csp_report_only_enabled() -> bool:
        return (os.getenv("CSP_REPORT_ONLY") or "").strip().lower() in _TRUTHY

    @staticmethod
    def socketio_cors_origins_raw() -> str:
        return (os.getenv("SOCKETIO_CORS_ORIGINS") or "").strip()

    @staticmethod
    def public_base_url() -> str:
        return (os.getenv("PUBLIC_BASE_URL") or "").strip()

    @staticmethod
    def mfa_qr_base_url() -> str:
        return (os.getenv("MFA_QR_BASE_URL") or "").strip()

    @staticmethod
    def redis_url() -> str:
        """Optional Redis (readiness probe); "" means not configured."""
        return (os.getenv("REDIS_URL") or "").strip()
