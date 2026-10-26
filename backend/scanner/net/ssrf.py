"""
Scanner SSRF guard (P16.4): one place that decides whether the scanner may open
a URL.

A recipe, a dealer registry row or a redirect target is data from outside the
process. Without this guard a recipe URL of ``http://100.64.0.1/`` (a tailnet
node) or ``http://169.254.169.254/`` reaches the host's own network. The rule is
the shared one in :mod:`backend.utils.outbound_url` (globally reachable only,
DNS-resolved, TTL-cached); this module adds the scanner policy:

* ``SCANNER_ALLOW_PRIVATE_HOSTS=1`` switches the guard off (a lab against a
  private fixture host); read live each call.
* ``SCANNER_HTTP_PROXY`` set means the proxy resolves and connects, so a private
  address here says nothing about where the request lands; the guard is exempt.
  ``HTTP_PROXY`` / ``HTTPS_PROXY`` do not exempt it: those are ambient, and a
  malicious name must not buy its way past the guard through them.
* a non-HTTP(S) scheme is refused, except that a URL with no host is left to the
  caller (a relative URL is joined onto a checked base first).
"""
from __future__ import annotations

import asyncio
import os
import urllib.error
from urllib.parse import urlparse

from backend.utils import outbound_url

ALLOW_PRIVATE_ENV = "SCANNER_ALLOW_PRIVATE_HOSTS"


class BlockedDestination(urllib.error.URLError, ConnectionError):
    """The scanner refused to open a URL whose host is not globally reachable.

    Both a ``URLError`` and a ``ConnectionError`` so the existing handlers of
    either family treat it as one more failed fetch."""

    def __init__(self, url: str, reason: str = "host not globally reachable"):
        super().__init__(f"scanner SSRF guard refused {url!r}: {reason}")
        self.url = url
        self.reason_text = reason


def guard_enabled() -> bool:
    if (os.environ.get(ALLOW_PRIVATE_ENV) or "").strip() == "1":
        return False
    if (os.environ.get("SCANNER_HTTP_PROXY") or "").strip():
        return False
    return True


def url_blocked(url: str) -> str | None:
    """A reason string when *url* must not be opened, else None."""
    if not guard_enabled():
        return None
    raw = str(url or "").strip()
    try:
        parsed = urlparse(raw)
        host = parsed.hostname or ""
    except ValueError:
        return "unparseable URL"
    if parsed.scheme.lower() not in ("http", "https"):
        return f"scheme {parsed.scheme or '(none)'!r} not allowed"
    if not host:
        return "no host"
    if outbound_url.destination_host_blocked_cached(host):
        return "host not globally reachable"
    return None


def check_url(url: str) -> None:
    """Raise :class:`BlockedDestination` when *url* must not be opened."""
    reason = url_blocked(url)
    if reason:
        raise BlockedDestination(str(url), reason)


async def acheck_url(url: str) -> None:
    """:func:`check_url` for async callers; a DNS lookup runs off the event loop."""
    if not guard_enabled():
        return
    await asyncio.to_thread(check_url, url)
