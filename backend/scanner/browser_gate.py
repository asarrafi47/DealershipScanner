"""The one switch that decides whether this process may open a headless browser.

Policy (2026-09-26, docs/HTTP_ONLY_SCANS_PLAN.md): scans are HTTP-only, always.
A browser may run only inside discovery (``discovery_probe --browser-capture``)
to learn how an unknown site presents its inventory so a recipe can be written.

``SCANNER_ALLOW_BROWSER=1`` is the only way to allow a launch. ``SCANNER_HTTP_ONLY``
is kept for the old callers and defaults to on; setting it to 0 does NOT allow a
browser by itself (that was the hole: an "HTTP-only" fleet run still launched
Chromium per process, opened a page per dealer, and auto-heal launched sync
Playwright per VIN batch).

Every launch / context / page-creating call site asks ``require_browser(caller)``
so a regression fails loudly with the caller's name instead of silently opening
Chromium.
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger("scanner")

_TRUTHY = ("1", "true", "yes", "on")


class BrowserForbidden(RuntimeError):
    """Raised when scan-path code tries to use a browser under the HTTP-only policy."""


def browser_allowed() -> bool:
    return (os.environ.get("SCANNER_ALLOW_BROWSER") or "").strip().lower() in _TRUTHY


def http_only() -> bool:
    """True unless a browser is explicitly allowed. ``SCANNER_HTTP_ONLY=0`` alone
    changes nothing (logged once so the operator sees why)."""
    if browser_allowed():
        return False
    raw = (os.environ.get("SCANNER_HTTP_ONLY") or "").strip().lower()
    if raw and raw not in _TRUTHY and not getattr(http_only, "_warned", False):
        logger.warning("SCANNER_HTTP_ONLY=%s ignored: scans are HTTP-only; set SCANNER_ALLOW_BROWSER=1 for discovery captures", raw)
        http_only._warned = True  # type: ignore[attr-defined]
    return True


def require_browser(caller: str) -> None:
    """Assert the policy at a launch site; ``caller`` names it in the error."""
    if not browser_allowed():
        raise BrowserForbidden(
            f"{caller}: headless browser use is forbidden in scans (HTTP-only policy); "
            "run this through discovery_probe --browser-capture (SCANNER_ALLOW_BROWSER=1)"
        )


def describe() -> str:
    return "browser: allowed (discovery)" if browser_allowed() else "browser: forbidden (HTTP-only scan)"


__all__ = ["BrowserForbidden", "browser_allowed", "http_only", "require_browser", "describe"]
