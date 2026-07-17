"""
HTTP-first recipe synthesis: build a replayable inventory-API recipe for a
dealer WITHOUT launching a browser, for platforms we already understand.

The scanner normally learns each dealer's inventory API by watching Playwright
network traffic and promoting it to a recipe (see ``backend.scanner.recipes``).
The key insight this module delivers is that the browser is needed once per
*platform*, not per *dealer*: once we know a platform's endpoint shape, any
dealer on that platform can have its recipe SYNTHESIZED over plain HTTP by

  1. fetching the dealer's page (:func:`fetch_dealer_html`),
  2. fingerprinting the platform from HTML markers (:func:`fingerprint_platform`),
  3. extracting the per-dealer params (domain / account id / api key), and
  4. filling a platform template borrowed from a real captured recipe
     (:func:`synthesize_recipe`).

This module NEVER launches a browser; all fetches go through the proxy-aware
``backend.scanner.http_fetch.open_url``. A synthesized recipe is only a
*candidate* — callers MUST validate it by replaying it over HTTP
(:func:`validate_recipe`) and counting real VINs before trusting or saving it.
A synthesized recipe that yields no VINs means the platform template no longer
fits this dealer and the browser is still required.

Adding a platform later is one entry in :data:`PLATFORM_TEMPLATES`.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

from backend.scanner.http_fetch import open_url
from backend.scanner.recipes import (
    PAGINATION_CARSCOMMERCE,
    PAGINATION_DEALER_COM,
    EndpointRecipe,
    _mutate_for_page,
    _replay_request,
    _unique_vins,
    load_recipes,
)

logger = logging.getLogger("scanner")

RECIPES_DIR = Path("workspace") / "recipes"

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
    "/cdn-cgi/challenge-platform",
)
_MIN_REAL_HTML_BYTES = 2000


# ── HTTP fetch ────────────────────────────────────────────────────────────────


# HTTP statuses worth a short retry — transient rate limits / gateway blips, not
# a hard "this needs a browser" signal.
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


def fetch_dealer_html(url: str, *, timeout: float = 25.0, retries: int = 2) -> str | None:
    """Plain-HTTP GET of *url*, returning decoded HTML or ``None``.

    Uses the proxy-aware :func:`open_url` and a browser-like navigation header
    set. Retries briefly on transient rate-limit / gateway statuses (429/5xx).
    Returns ``None`` (meaning "not synthesizable over HTTP — needs a browser")
    when the fetch ultimately fails, the response is suspiciously tiny (< ~2KB),
    or the body carries a Cloudflare/JS-challenge marker.
    """
    import time
    import urllib.error
    import urllib.request

    if not url or not url.lower().startswith("http"):
        return None
    html: str | None = None
    for attempt in range(retries + 1):
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
            return None
        except Exception as e:  # URLError, socket timeout, decode, ...
            logger.debug("recipe_synth fetch failed %s: %s", url[:80], str(e)[:120])
            return None
    if html is None:
        return None
    if len(html) < _MIN_REAL_HTML_BYTES:
        logger.debug("recipe_synth fetch %s: thin body (%d bytes) — challenge/shell", url[:80], len(html))
        return None
    low = html.lower()
    if any(m in low for m in _CHALLENGE_MARKERS):
        logger.debug("recipe_synth fetch %s: challenge marker present — needs browser", url[:80])
        return None
    return html


def _origin(dealer_url: str) -> str:
    """``https://host`` for *dealer_url* (scheme defaulted to https)."""
    p = urlparse(dealer_url if "://" in dealer_url else "https://" + dealer_url)
    scheme = p.scheme or "https"
    host = p.netloc or p.path
    return f"{scheme}://{host}".rstrip("/")


# ── Reference templates (borrowed from real captured recipes) ─────────────────


def _load_reference_recipe(dealer_id: str, url_contains: str) -> EndpointRecipe | None:
    """First non-stale recipe of *dealer_id* whose URL contains *url_contains*."""
    for r in load_recipes(dealer_id):
        if url_contains in r.url and not r.stale:
            return r
    # fall back to stale (template body is still valid even if that dealer's
    # capture went stale — we only borrow the POST shape / shared key)
    for r in load_recipes(dealer_id):
        if url_contains in r.url:
            return r
    return None


# ── Platform: Dealer.com (ws-inv-data) ────────────────────────────────────────

# Reference recipe supplying the Dealer.com POST body template.
_DEALER_COM_REF_ID = "camelbacktoyota-com"
_DEALER_COM_REF_SITEID = "camelbacktoyotavtg"
_DEALER_COM_INVENTORY_PATH = "/api/widget/ws-inv-data/getInventory"

_SITEID_RE = re.compile(r'["\']siteId["\']\s*:\s*["\']([\w-]+)["\']')


def _detect_dealer_com(html: str, dealer_url: str) -> bool:
    low = html.lower()
    if "carscommerce" in low or "teamvelocityportal" in low:
        return False  # disambiguate from other platforms that also embed a siteId
    markers = ("data-widget-name", "ddc", "ws-inv-data", _DEALER_COM_INVENTORY_PATH)
    if sum(1 for m in markers if m in low) < 2:
        return False
    return bool(_extract_dealer_com_siteid(html))


def _extract_dealer_com_siteid(html: str) -> str | None:
    m = _SITEID_RE.search(html)
    if m:
        return m.group(1)
    return None


def _synth_dealer_com(dealer_id: str, dealer_url: str, html: str) -> EndpointRecipe | None:
    site_id = _extract_dealer_com_siteid(html)
    if not site_id:
        return None
    ref = _load_reference_recipe(_DEALER_COM_REF_ID, "getInventory")
    if not ref or not ref.post_template:
        logger.warning("recipe_synth: Dealer.com reference template unavailable (%s)", _DEALER_COM_REF_ID)
        return None
    # The reference dealer's siteId appears verbatim in siteId + pageId; swap it
    # to the target dealer's siteId. The ws-inv-data endpoint is lenient about
    # the pageId version suffix (verified against live dealers), so this single
    # substitution is enough to make the body dealer-correct.
    post_template = ref.post_template.replace(_DEALER_COM_REF_SITEID, site_id)
    url = _origin(dealer_url) + _DEALER_COM_INVENTORY_PATH
    return EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="POST",
        content_type="application/json; charset=utf-8",
        post_template=post_template,
        auth_headers={},
        pagination=PAGINATION_DEALER_COM,
        provider_hint="dealer_dot_com",
    )


# ── Platform: CarsCommerce (websites-search.api) ──────────────────────────────

_CARSCOMMERCE_REF_ID = "courtesychev-com"
_CARSCOMMERCE_HOST = "websites-search.api.carscommerce.inc"

_CCID_RES = (
    re.compile(r'["\']ccid["\']\s*:\s*["\'](\d{3,})["\']'),
    re.compile(r'/api/v1/listings\\?/(\d{3,})'),
    re.compile(r'var\s+account\s*=\s*["\'](\d{3,})["\']'),
)
_APIKEY_RE = re.compile(r'["\']apiKey["\']\s*:\s*["\']([A-Za-z0-9]{16,})["\']')


def _detect_carscommerce(html: str, dealer_url: str) -> bool:
    low = html.lower()
    if _CARSCOMMERCE_HOST not in low and "carscommerce" not in low:
        return False
    return bool(_extract_ccid(html))


def _extract_ccid(html: str) -> str | None:
    for rx in _CCID_RES:
        m = rx.search(html)
        if m:
            return m.group(1)
    return None


def _extract_carscommerce_key(html: str) -> str | None:
    """apiKey embedded in the dealer HTML (CarsCommerce ships it client-side)."""
    m = _APIKEY_RE.search(html)
    return m.group(1) if m else None


def _synth_carscommerce(dealer_id: str, dealer_url: str, html: str) -> EndpointRecipe | None:
    ccid = _extract_ccid(html)
    if not ccid:
        return None
    ref = _load_reference_recipe(_CARSCOMMERCE_REF_ID, "/listings/")
    if not ref or not ref.post_template:
        logger.warning("recipe_synth: CarsCommerce reference template unavailable (%s)", _CARSCOMMERCE_REF_ID)
        return None
    # Prefer the key the dealer's own page ships; the CarsCommerce search key is
    # shared across all their dealers, so the reference recipe's key is a safe
    # fallback if the page doesn't expose it.
    api_key = _extract_carscommerce_key(html) or (ref.auth_headers or {}).get("x-api-key")
    if not api_key:
        return None
    url = f"https://{_CARSCOMMERCE_HOST}/api/v1/listings/{ccid}/search"
    return EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="POST",
        content_type="application/json; charset=utf-8",
        post_template=ref.post_template,
        auth_headers={"x-api-key": api_key},
        pagination=PAGINATION_CARSCOMMERCE,
        provider_hint="dealer_dot_com",  # CarsCommerce payloads route through the generic dealer_dot_com mapper
    )


# ── Platform: Team Velocity (stretch — endpoint not derivable from HTML) ───────


def _detect_team_velocity(html: str, dealer_url: str) -> bool:
    low = html.lower()
    return "teamvelocityportal" in low or "inventoryapibaseurl" in low


# TODO(team_velocity): the HTML exposes inventoryApiBaseUrl
# ('https://websites.api.teamvelocityportal.com/') and accountId, but the
# endpoint path + POST body shape can't be derived from the HTML alone — it
# needs one browser capture to record the request shape. Until then Team
# Velocity is recognized (so we report it precisely) but not synthesizable, and
# those dealers still need the browser. Register with ``synth=None``.


# ── Platform registry ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class PlatformTemplate:
    name: str
    detect: Callable[[str, str], bool]
    # ``None`` synth => platform is recognized but not synthesizable yet
    # (browser still required); still useful to report *why*.
    synth: Callable[[str, str, str], EndpointRecipe | None] | None


# Ordered most-specific-first; :func:`fingerprint_platform` returns the first hit.
PLATFORM_TEMPLATES: list[PlatformTemplate] = [
    PlatformTemplate("carscommerce", _detect_carscommerce, _synth_carscommerce),
    PlatformTemplate("dealer_dot_com", _detect_dealer_com, _synth_dealer_com),
    PlatformTemplate("team_velocity", _detect_team_velocity, None),
]

_TEMPLATES_BY_NAME = {t.name: t for t in PLATFORM_TEMPLATES}


def fingerprint_platform(html: str, dealer_url: str) -> str | None:
    """Return the platform name detected in *html*, or ``None`` if unrecognized."""
    if not html:
        return None
    for tmpl in PLATFORM_TEMPLATES:
        try:
            if tmpl.detect(html, dealer_url):
                return tmpl.name
        except Exception:  # a bad regex on weird HTML must not abort the sweep
            continue
    return None


def is_synthesizable(platform: str | None) -> bool:
    """True if we have a template that can build a recipe for *platform*."""
    t = _TEMPLATES_BY_NAME.get(platform or "")
    return bool(t and t.synth)


def synthesize_recipe(
    dealer_id: str, dealer_url: str, html: str, platform: str | None = None
) -> EndpointRecipe | None:
    """Build a candidate :class:`EndpointRecipe` for *dealer_id* from *html*.

    *platform* may be supplied (from a prior :func:`fingerprint_platform`) or
    left ``None`` to fingerprint here. Returns ``None`` when the platform is
    unrecognized, has no template, or the per-dealer params can't be extracted.
    The returned recipe is a *candidate*: validate it (:func:`validate_recipe`)
    before saving.
    """
    platform = platform or fingerprint_platform(html, dealer_url)
    tmpl = _TEMPLATES_BY_NAME.get(platform or "")
    if not tmpl or not tmpl.synth:
        return None
    try:
        return tmpl.synth(dealer_id, dealer_url, html)
    except Exception as e:
        logger.debug("recipe_synth synth failed [%s/%s]: %s", dealer_id, platform, str(e)[:150])
        return None


# ── Validation (replay over HTTP, count real VINs) ────────────────────────────

_VALIDATE_MAX_PAGES = 40


def validate_recipe(
    recipe: EndpointRecipe,
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    *,
    max_pages: int = _VALIDATE_MAX_PAGES,
) -> int:
    """Replay *recipe* over plain HTTP and return the unique VIN count.

    Walks the recipe's pagination shape (single-shot for ``PAGINATION_NONE``),
    parses each page with the provider parser, and counts distinct VINs. No
    browser, no DB writes, no recipe-file mutation — a pure yield probe.
    """
    from backend.parsers import parse

    template: Any = None
    if recipe.post_template:
        try:
            template = json.loads(recipe.post_template)
        except ValueError:
            template = None
    if recipe.method != "GET" and template is None:
        return 0

    from backend.scanner.recipes import PAGINATION_NONE

    pages = 1 if recipe.pagination == PAGINATION_NONE else max_pages
    vins: set[str] = set()
    for page_i in range(pages):
        body = _mutate_for_page(recipe, template, page_i) if template is not None else None
        status, parsed = _replay_request(recipe, body, base_url)
        if status != 200 or parsed is None:
            break
        page_vehicles = list(parse(
            recipe.provider_hint or "", parsed,
            base_url=base_url, dealer_id=dealer_id,
            dealer_name=dealer_name, dealer_url=base_url,
        ))
        new = _unique_vins(page_vehicles) - vins
        if not new:
            break
        vins |= new
        if recipe.total_count and len(vins) >= recipe.total_count:
            break
    return len(vins)
