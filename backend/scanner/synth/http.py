"""
Plain-HTTP fetch layer for recipe synthesis: browser-navigation headers, global
fetch pacing, Cloudflare challenge detection, and the urllib -> curl_cffi
(TLS-impersonated) escalation used by every synth template and validator.

Moved verbatim from ``backend/scanner/recipe_synth.py`` (audit F-6). Since audit
F-18 the curl_cffi rotation, proxy mapping and challenge detection come from the
shared layer :mod:`backend.scanner.net.client`; this module keeps its own options
(pacing per attempt, challenge + thin-body rejection) and names. The pacing
clock (``_last_fetch_at``) lives here: ``_pace`` is one global clock for the
whole process, whichever module calls it.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any
from urllib.parse import urlparse

from backend.scanner.http_fetch import open_url
from backend.scanner.net import client as net_client

# Challenge detection lives in the shared HTTP layer (audit F-18); the names stay
# importable from here (and from the recipe_synth facade) for their callers.
from backend.scanner.net.client import (
    _CHALLENGE_BEACON_MARKERS,
    _CHALLENGE_MARKERS,
    _MAX_CHALLENGE_SHELL_BYTES,
    _MIN_REAL_HTML_BYTES,
    looks_like_challenge,
)

logger = logging.getLogger("scanner")


_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Full browser-navigation header set. Plain GETs with only a UA get 403/429'd by
# several of these dealer platforms; the Sec-Fetch-* + Accept-Language set gets
# the same HTML a real first-visit browser navigation would.
def _browser_headers() -> dict[str, str]:
    return {
        "User-Agent": _BROWSER_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "identity",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Upgrade-Insecure-Requests": "1",
    }


# ── Global fetch pacing (rate-limit / Cloudflare defense) ─────────────────────
#
# Our egress IP is rate-limited from heavy scraping and bursts trigger a
# Cloudflare "Just a moment" challenge; paced requests succeed. When
# ``SCANNER_SYNTH_FETCH_DELAY`` (seconds) is set, every HTTP fetch in this module
# (homepage, SRP probes, replay walks) waits so that consecutive requests are at
# least that far apart. Default 0 => no pacing, so unit tests and callers that
# stub the network are unaffected. The synthesis sweep sets it to ~4s.
try:
    _MIN_FETCH_INTERVAL = float(os.environ.get("SCANNER_SYNTH_FETCH_DELAY", "0") or 0)
except ValueError:
    _MIN_FETCH_INTERVAL = 0.0
_pace_lock = threading.Lock()
_last_fetch_at = 0.0


def _pace() -> None:
    """Block until at least ``_MIN_FETCH_INTERVAL`` has elapsed since the last
    fetch, so a sequential sweep never bursts. No-op when the delay is 0."""
    global _last_fetch_at
    if _MIN_FETCH_INTERVAL <= 0:
        return
    with _pace_lock:
        wait = _last_fetch_at + _MIN_FETCH_INTERVAL - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last_fetch_at = time.monotonic()


# ── HTTP fetch ────────────────────────────────────────────────────────────────


# HTTP statuses worth a short retry — transient rate limits / gateway blips, not
# a hard "this needs a browser" signal.
_RETRYABLE_STATUS = net_client.RETRYABLE_STATUSES


def _fetch_impersonated(
    url: str,
    *,
    timeout: float = 25.0,
    headers: dict[str, str] | None = None,
    min_bytes: int = _MIN_REAL_HTML_BYTES,
) -> str | None:
    """
    Fetch with a real browser's TLS fingerprint, or None if that is not possible.

    *headers* are sent on top of the impersonated profile's own (a same-site
    ``Referer`` is what some Cloudflare rules key on, see :func:`_dep_fetch_page`);
    *min_bytes* lets an HTML page-walk accept a short past-the-last-result page.

    Returns None rather than raising when ``curl_cffi`` is absent, so this stays a pure
    enhancement: without it the caller falls through to the original urllib path and
    behaves exactly as before.

    The same quality bar as the urllib path applies -- a challenge interstitial is a
    couple of hundred KB of markup and would otherwise read as a successful fetch.
    """
    cffi_requests = net_client.curl_cffi_requests()
    if cffi_requests is None:
        return None

    from backend.scanner.chain import ImpersonatingFetcher

    proxies = net_client.scanner_proxies()

    def _accept(resp: Any) -> bool:
        if resp.status_code != 200:
            return False
        html = resp.text or ""
        if len(html) < min_bytes:
            return False
        if looks_like_challenge(html):
            return False
        return True

    def _failed(profile: str, exc: Exception) -> None:
        logger.debug("recipe_synth impersonate %s failed %s: %s", profile, url[:70], str(exc)[:90])

    got = net_client.rotate_impersonation(
        cffi_requests, url,
        profiles=ImpersonatingFetcher.PROFILES, accept=_accept,
        before_attempt=_pace, on_error=_failed,
        timeout=timeout, proxies=proxies, allow_redirects=True, headers=headers or None,
    )
    if got.response is None:
        return None
    return got.response.text or ""


def fetch_dealer_html(url: str, *, timeout: float = 25.0, retries: int = 2) -> str | None:
    """Plain-HTTP GET of *url*, returning decoded HTML or ``None``.

    Uses the proxy-aware :func:`open_url` and a browser-like navigation header
    set. Retries briefly on transient rate-limit / gateway statuses (429/5xx).
    Cheapest path first: plain urllib, then -- only where that fails, returns a thin
    body, or trips a challenge marker -- a retry with a real browser's TLS fingerprint
    (:func:`_fetch_impersonated`). The edge in front of most dealer platforms rejects on
    handshake, not headers, so no amount of header dressing clears it and the failure was
    being reported as "needs a browser". That is why 130 rooftops held a recipe row with
    zero recipes: synthesis never saw their HTML. Escalating rather than leading with
    impersonation keeps the common path unchanged and one request cheaper.

    Returns ``None`` (meaning "not synthesizable over HTTP — needs a browser") only once
    both have failed.
    """
    import time
    import urllib.error
    import urllib.request

    if not url or not url.lower().startswith("http"):
        return None

    html: str | None = None
    for attempt in range(retries + 1):
        _pace()
        req = urllib.request.Request(url, headers=_browser_headers())
        try:
            resp = open_url(req, timeout=timeout)
            raw = resp.read()
            enc = resp.headers.get_content_charset() or "utf-8"
            html = raw.decode(enc, "replace")
            break
        except urllib.error.HTTPError as e:
            if e.code in _RETRYABLE_STATUS and attempt < retries:
                time.sleep(2.0 * (attempt + 1))
                continue
            logger.debug("recipe_synth fetch failed %s: HTTP %s", url[:80], e.code)
            return _fetch_impersonated(url, timeout=timeout)
        except Exception as e:  # URLError, socket timeout, decode, ...
            logger.debug("recipe_synth fetch failed %s: %s", url[:80], str(e)[:120])
            return None
    if html is None:
        return None
    if len(html) < _MIN_REAL_HTML_BYTES:
        logger.debug("recipe_synth fetch %s: thin body (%d bytes) — challenge/shell", url[:80], len(html))
        return _fetch_impersonated(url, timeout=timeout)
    if looks_like_challenge(html):
        logger.debug("recipe_synth fetch %s: challenge marker present — retry impersonated", url[:80])
        return _fetch_impersonated(url, timeout=timeout)
    return html


def _dep_fetch_page(url: str) -> tuple[str | None, str]:
    """Proxy-aware GET of an SRP page: ``(html or None, final_url_after_redirects)``.

    Unlike :func:`fetch_dealer_html` this does NOT reject "thin" bodies — an SRP
    page past the last result is a valid (short) page, and the caller stops when
    the parser extracts no more VINs.

    Always sends a same-site ``Referer`` (the page's own origin). Some Cloudflare
    rule sets in front of Dealer eProcess sites (Honda of El Cajon, 2026-09-23)
    serve a managed challenge to any SRP request WITHOUT a same-site Referer and
    the real page WITH one -- cookies and warm-up do not matter. When the plain
    client is rejected on the handshake (403/405/429) the fetch escalates to TLS
    impersonation with the same headers, mirroring :func:`fetch_dealer_html` and
    ``recipes._replay_request`` so capture, validation and replay clear one edge.
    """
    import urllib.error
    import urllib.request

    parts = urlparse(url)
    referer = f"{parts.scheme}://{parts.netloc}/"
    headers = {**_browser_headers(), "Referer": referer, "Sec-Fetch-Site": "same-origin"}
    _pace()
    try:
        resp = open_url(urllib.request.Request(url, headers=headers), timeout=25.0)
        html = resp.read().decode(resp.headers.get_content_charset() or "utf-8", "replace")
        final = resp.geturl() or url
    except urllib.error.HTTPError as e:
        if e.code not in _DEP_ESCALATE_STATUSES:
            logger.debug("dep fetch failed %s: HTTP %s", url[:80], e.code)
            return None, url
        logger.debug("dep fetch %s: HTTP %s — retry impersonated", url[:80], e.code)
        return _fetch_impersonated(url, headers=headers, min_bytes=0), url
    except Exception as e:
        logger.debug("dep fetch failed %s: %s", url[:80], str(e)[:120])
        return None, url
    if html and looks_like_challenge(html):
        logger.debug("dep fetch %s: challenge shell — retry impersonated", url[:80])
        return _fetch_impersonated(url, headers=headers, min_bytes=0), url
    return html, final


# Statuses a WAF returns when it dislikes the TLS handshake rather than the request
# (same set as recipes._TLS_FINGERPRINT_STATUSES).
_DEP_ESCALATE_STATUSES = frozenset(net_client.TLS_FINGERPRINT_STATUSES)


def _dep_fetch_html(url: str) -> str | None:
    """:func:`_dep_fetch_page` without the final URL (HTML page-walk helper)."""
    return _dep_fetch_page(url)[0]


def _cosmos_get_json(url: str) -> Any | None:
    """Proxy-aware GET returning parsed JSON (or ``None``).

    Falls back to a TLS-impersonated fetch when the plain urllib GET is
    rejected. The same edge-fingerprint rejection documented on
    :func:`fetch_dealer_html` (a bare-urllib 403 is not proof the endpoint is
    gone) also guards Team Velocity JSON feeds on at least one dealer:
    scottclarkhonda.com/inventory-used.json 403s plain urllib (Akamai Bot
    Manager, ak_bmsc cookie) but returns 200 under curl_cffi impersonation.
    Without this fallback, both synthesis and validate_recipe silently see
    zero VINs for any dealer whose feed sits behind such an edge, and the
    platform gets misreported as needing a browser when it does not.
    """
    import urllib.request

    _pace()
    req = urllib.request.Request(url, headers={**_browser_headers(), "Accept": "application/json"})
    try:
        resp = open_url(req, timeout=25.0)
        return json.loads(resp.read().decode("utf-8", "replace"))
    except Exception as e:
        logger.debug("json GET failed %s: %s", url[:80], str(e)[:120])
        text = _fetch_impersonated(url, timeout=25.0)
        if not text:
            return None
        try:
            return json.loads(text)
        except (ValueError, TypeError) as e2:
            logger.debug("json GET impersonated parse failed %s: %s", url[:80], str(e2)[:120])
            return None
