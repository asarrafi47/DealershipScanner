"""
Shared HTTP-fetch plumbing for the browser-free harvesters, with OPTIONAL,
env-var-driven proxy support.

Anti-bot-walled dealers (Cloudflare-challenged sites like hondaofelcajon.com,
pacificvolkswagen.com) reject datacenter IPs. Routing the plain-HTTP harvesters
through a residential / rotating proxy lets those requests through. This module
centralizes that so no individual script has to reimplement proxy wiring.

Configuration
-------------
Set ``SCANNER_HTTP_PROXY`` to a full proxy URL to route ALL harvester fetches
through it::

    SCANNER_HTTP_PROXY=http://user:pass@residential.proxy.example:8000

If ``SCANNER_HTTP_PROXY`` is unset/empty we fall back to the standard
``HTTPS_PROXY`` / ``HTTP_PROXY`` environment variables. If NONE of those are
set, this module is a strict no-op: ``urllib`` uses its default global opener
and ``requests`` gets ``proxies=None`` — i.e. byte-for-byte the previous
direct-connection behavior (which already honors the standard proxy env vars on
its own).

Public API
----------
* :func:`proxy_url` — the effective proxy URL (or ``None``).
* :func:`proxied_opener` — a ``urllib`` opener honoring the proxy (or the
  default opener when unset).
* :func:`open_url` — open a urllib ``Request``/URL through that opener.
* :func:`requests_proxies` — a ``proxies=`` dict for the ``requests`` library
  (or ``None`` to keep requests' default behavior).
"""
from __future__ import annotations

import os
import urllib.request
from typing import Any

# Preferred scanner-specific var, then the de-facto standard ones.
_PROXY_ENV_VARS = ("SCANNER_HTTP_PROXY", "HTTPS_PROXY", "HTTP_PROXY")


def proxy_url() -> str | None:
    """Return the effective proxy URL, or ``None`` when no proxy is configured.

    Reads (in order) ``SCANNER_HTTP_PROXY``, ``HTTPS_PROXY``, ``HTTP_PROXY``.
    Empty/whitespace values are treated as unset. Read live each call so tests
    (and runs) that set the env var after import still take effect.
    """
    for name in _PROXY_ENV_VARS:
        val = (os.environ.get(name) or "").strip()
        if val:
            return val
    return None


def proxied_opener() -> urllib.request.OpenerDirector:
    """A ``urllib`` opener that routes through the configured proxy.

    When no proxy is configured, returns an opener with the default handler set
    — equivalent to the module-global opener ``urllib.request.urlopen`` uses,
    so the no-proxy path is unchanged.
    """
    url = proxy_url()
    if not url:
        return urllib.request.build_opener()
    handler = urllib.request.ProxyHandler({"http": url, "https": url})
    return urllib.request.build_opener(handler)


def open_url(req_or_url: Any, timeout: float | None = None):
    """Open a urllib ``Request`` (or URL string) honoring the configured proxy.

    Drop-in for ``urllib.request.urlopen(req, timeout=...)``. Raises the same
    exceptions on connection/proxy failure, so existing try/except handlers keep
    working unchanged.
    """
    opener = proxied_opener()
    if timeout is None:
        return opener.open(req_or_url)
    return opener.open(req_or_url, timeout=timeout)


def requests_proxies() -> dict[str, str] | None:
    """A ``proxies=`` mapping for the ``requests`` library, or ``None``.

    ``None`` preserves requests' default behavior (which already honors the
    standard ``HTTP_PROXY`` / ``HTTPS_PROXY`` env vars on its own). When
    ``SCANNER_HTTP_PROXY`` is set we return an explicit mapping so the
    scanner-specific var wins for both schemes.
    """
    val = (os.environ.get("SCANNER_HTTP_PROXY") or "").strip()
    if not val:
        return None
    return {"http": val, "https": val}
