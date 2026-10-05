"""
The one HTTP layer the scanner's fetchers share (audit F-18).

Before this module, five call sites each carried their own copy of the same
plumbing: ``chain.ImpersonatingFetcher`` / ``RequestsFetcher``,
``synth.http._fetch_impersonated`` / ``fetch_dealer_html``,
``recipes._replay_impersonated`` / ``_replay_request``, ``vdp.prefetch._fetch_html``
and ``vdp.vdp_recipes._fetch_json``. This module holds the primitives they share:

* :func:`import_curl_cffi` / :func:`curl_cffi_requests` / :func:`curl_cffi_available`
  -- the optional TLS-impersonating client, imported lazily so the scanner imports
  without it;
* :func:`import_requests` -- the plain client, also lazy;
* :func:`scanner_proxies` -- the ``proxies=`` mapping built from
  :func:`backend.scanner.http_fetch.proxy_url`;
* :func:`send` -- one request through either client;
* :func:`rotate_impersonation` -- the browser-signature rotation loop, with an
  acceptance predicate, a per-attempt hook (pacing) and an error hook (logging);
* :func:`looks_like_challenge` and the status sets -- challenge detection and
  status classification.

It is a refactor, not a policy: every option is a parameter, and each call site
passes the options that reproduce what it did before (replay sends no proxy,
VDP prefetch does no challenge detection, timeouts differ per site). Those
differences are pinned by ``backend/tests/test_scanner_http_characterization.py``.
The proxy plumbing for urllib (``open_url``) stays in
:mod:`backend.scanner.http_fetch`.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from types import ModuleType
from typing import Any

from backend.scanner import http_fetch

# Browser signatures to rotate, ordered by observed hit rate. Edges run different
# rule sets: over a 30-dealer sample "chrome" alone cleared about half, while
# chrome/chrome124/safari17_0 together cleared 29 of 30.
IMPERSONATE_PROFILES = ("chrome", "chrome124", "safari17_0")

# ── Status classification ────────────────────────────────────────────────────

# Statuses a WAF returns when it dislikes the TLS handshake rather than the request.
# Cloudflare and Akamai use 403; DataDome and some Akamai configs use 405/429.
TLS_FINGERPRINT_STATUSES = (403, 405, 429)

# What VDP prefetch treats as a fingerprint block that plain requests will not
# clear (it differs from TLS_FINGERPRINT_STATUSES: 503 in, 405 out).
VDP_FINGERPRINT_BLOCK_STATUSES = (403, 429, 503)

# HTTP statuses worth a short retry -- transient rate limits / gateway blips, not
# a hard "this needs a browser" signal.
RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})


def is_fingerprint_block(status: int, statuses: Iterable[int] = TLS_FINGERPRINT_STATUSES) -> bool:
    """True when *status* is one the caller treats as a TLS-fingerprint rejection."""
    return status in statuses


# ── Challenge detection ──────────────────────────────────────────────────────

# Markers that mean "this is a JS challenge / anti-bot shell, not real HTML".
_CHALLENGE_MARKERS = (
    "just a moment",
    "checking your browser",
    "cf-challenge",
    "__cf_chl",
    "attention required",
    "enable javascript and cookies",
    "cf-browser-verification",
    "px-captcha",
)
# Markers that Cloudflare also injects into REAL pages as a passive bot-management
# beacon (``s.src='/cdn-cgi/challenge-platform/scripts/...'``). On their own they
# prove nothing: Honda of El Cajon serves a 459KB genuine Dealer eProcess homepage
# carrying that beacon, and treating it as a challenge made the whole dealer read as
# ``homepage_unreachable_200``. A beacon only counts as a challenge when the body is
# thin (a real interstitial is a few KB) or a strong marker is present as well.
_CHALLENGE_BEACON_MARKERS = ("/cdn-cgi/challenge-platform",)
_MIN_REAL_HTML_BYTES = 2000
_MAX_CHALLENGE_SHELL_BYTES = 20_000


def looks_like_challenge(html: str) -> bool:
    """True when *html* is an anti-bot interstitial rather than a real page.

    Strong markers ("just a moment", ``__cf_chl``, ...) decide on their own. A
    passive beacon (``/cdn-cgi/challenge-platform``) decides only for a thin body,
    because Cloudflare injects the same script tag into pages it served normally.
    """
    low = html.lower()
    if any(m in low for m in _CHALLENGE_MARKERS):
        return True
    if len(html) < _MAX_CHALLENGE_SHELL_BYTES and any(m in low for m in _CHALLENGE_BEACON_MARKERS):
        return True
    return False


# ── Clients ──────────────────────────────────────────────────────────────────


def import_curl_cffi() -> ModuleType:
    """``curl_cffi.requests``, imported now. Raises ImportError when absent."""
    from curl_cffi import requests as cffi_requests

    return cffi_requests


def curl_cffi_requests() -> ModuleType | None:
    """``curl_cffi.requests``, or None when curl_cffi is not installed."""
    try:
        return import_curl_cffi()
    except ImportError:
        return None


def curl_cffi_available() -> bool:
    """True when the ``curl_cffi`` package imports."""
    try:
        import curl_cffi  # noqa: F401

        return True
    except ImportError:
        return False


def import_requests() -> ModuleType:
    """The ``requests`` library, imported now."""
    import requests

    return requests


# ── Proxies ──────────────────────────────────────────────────────────────────


def scanner_proxies() -> dict[str, str] | None:
    """``{"http": p, "https": p}`` for the effective proxy (``SCANNER_HTTP_PROXY``,
    then ``HTTPS_PROXY`` / ``HTTP_PROXY``), or None when none is configured."""
    proxy = http_fetch.proxy_url()
    return {"http": proxy, "https": proxy} if proxy else None


# ── Sending ──────────────────────────────────────────────────────────────────


def send(lib: Any, method: str, url: str, *, via_get: bool, **kwargs: Any) -> Any:
    """One request through *lib* (``curl_cffi.requests`` or ``requests``).

    *via_get* calls ``lib.get(url, ...)``; otherwise ``lib.request(method, url, ...)``.
    *kwargs* go through untouched, so a call site sends exactly the keyword set it
    names (no defaults are added here).
    """
    if via_get:
        return lib.get(url, **kwargs)
    return lib.request(method, url, **kwargs)


@dataclass
class Rotation:
    """Outcome of :func:`rotate_impersonation`."""

    response: Any | None  # the accepted response, or None
    profile: str | None  # the signature that produced it
    last_status: int | None  # status of the last attempt; None when it raised


def rotate_impersonation(
    cffi: Any,
    url: str,
    *,
    accept: Callable[[Any], bool],
    method: str = "GET",
    via_get: bool = True,
    profiles: Iterable[str] = IMPERSONATE_PROFILES,
    before_attempt: Callable[[], None] | None = None,
    on_error: Callable[[str, Exception], None] | None = None,
    **kwargs: Any,
) -> Rotation:
    """Try each browser signature in *profiles* until *accept(response)* is true.

    *before_attempt* runs before every attempt (fetch pacing). *on_error* gets
    ``(profile, exc)`` for an attempt that raised; the rotation then moves on.
    ``impersonate=<profile>`` is added to *kwargs* per attempt.
    """
    last_status: int | None = None
    for profile in profiles:
        if before_attempt is not None:
            before_attempt()
        try:
            resp = send(cffi, method, url, via_get=via_get, impersonate=profile, **kwargs)
        except Exception as exc:  # curl_cffi raises its own error hierarchy
            last_status = None
            if on_error is not None:
                on_error(profile, exc)
            continue
        if accept(resp):
            return Rotation(resp, profile, resp.status_code)
        last_status = resp.status_code
    return Rotation(None, None, last_status)
