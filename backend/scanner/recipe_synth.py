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
from urllib.parse import urlparse, urlunparse

from backend.scanner.http_fetch import open_url
from backend.scanner.recipes import (
    PAGINATION_CARSCOMMERCE,
    PAGINATION_DEALER_COM,
    PAGINATION_DEP_SRP,
    PAGINATION_NONE,
    PAGINATION_PAGE_QUERY,
    PAGINATION_TYPESENSE,
    EndpointRecipe,
    _mutate_for_page,
    _replay_request,
    _unique_vins,
    _url_for_page,
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
    # Full-lot body: (1) drop the reference recipe's Used/CPO type restriction so
    # new inventory is included (mirrors carscommerce_harvest include_new — the
    # delta replay honors facetFilters and would silently exclude every new car);
    # (2) raise perPage to 100 (the harvester's _PER_PAGE; API max is 200) so the
    # bounded page walk reaches large accounts — at perPage 20 the 40-page cap
    # tops out at 800, short of dealers like Bill Luke (~1,979).
    try:
        body = json.loads(ref.post_template)
    except ValueError:
        body = None
    if isinstance(body, dict):
        body.pop("facetFilters", None)
        body["perPage"] = 100
        post_template = json.dumps(body)
    else:
        post_template = ref.post_template
    url = f"https://{_CARSCOMMERCE_HOST}/api/v1/listings/{ccid}/search"
    return EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="POST",
        content_type="application/json; charset=utf-8",
        post_template=post_template,
        auth_headers={"x-api-key": api_key},
        pagination=PAGINATION_CARSCOMMERCE,
        provider_hint="dealer_dot_com",  # CarsCommerce payloads route through the generic dealer_dot_com mapper
    )


# ── Platform: DealerOn cosmos (ws/vhcliaa SRP) ────────────────────────────────

# DealerOn cosmos SRP endpoint:
#   https://{domain}/api/vhcliaa/vehicle-pages/cosmos/srp/vehicles/{account}/{pagecfg}
# parameterized by two ids, both recoverable over plain HTTP:
#   account  — the dealer id, in the homepage HTML (site-provider="dealeron",
#              data-website-id="do-{account}", "dealerId":"{account}").
#   pagecfg  — the SRP page config id, NOT on the homepage but embedded in the
#              used-inventory SRP page as an itemlist page config
#              ({"dealerId":...,"pageId":{pagecfg},"pageType":"itemlist"...}).
#              The cosmos endpoint accepts the SRP page's pageId as the pagecfg
#              (verified live), so no browser capture is needed.
# The endpoint paginates session-free via ?pg=N&pn=96 (same as heal's
# _cosmos_pages); validate_recipe walks it that way for cosmos URLs.
_COSMOS_PATH = "/api/vhcliaa/vehicle-pages/cosmos/srp/vehicles"
_COSMOS_PAGE_SIZE = 96
# SRP pages that carry a Used-scoped itemlist page config.
_COSMOS_SRP_PATHS = ("/used-inventory/", "/searchused.aspx", "/used-vehicles/", "/inventory/used")

_COSMOS_ACCOUNT_RES = (
    re.compile(r'data-website-id="do-(\d+)"'),
    re.compile(r'"dealerId"\s*:\s*"?(\d+)"?'),
    re.compile(r'/static/dealer-(\d+)/'),
)
# {"dealerId":"25003","pageId":2483381,"pageType":"itemlist"...}
_COSMOS_ITEMLIST_RE = re.compile(
    r'"dealerId"\s*:\s*"?(\d+)"?\s*,\s*"pageId"\s*:\s*(\d+)\s*,\s*"pageType"\s*:\s*"itemlist"'
)


def _detect_dealer_on_cosmos(html: str, dealer_url: str) -> bool:
    low = html.lower()
    markers = ('site-provider="dealeron"', 'data-website-id="do-', "vhcliaa", "cosmos/srp/vehicles")
    if not any(m in low for m in markers):
        return False
    return bool(_extract_cosmos_account(html))


def _extract_cosmos_account(html: str) -> str | None:
    for rx in _COSMOS_ACCOUNT_RES:
        m = rx.search(html)
        if m:
            return m.group(1)
    return None


def _extract_cosmos_pagecfg(html: str) -> tuple[str, str] | None:
    """(account, pagecfg) from a Used SRP itemlist page config, or ``None``."""
    m = _COSMOS_ITEMLIST_RE.search(html)
    if m:
        return m.group(1), m.group(2)
    return None


def _synth_dealer_on_cosmos(dealer_id: str, dealer_url: str, html: str) -> EndpointRecipe | None:
    account = _extract_cosmos_account(html)
    # The pagecfg lives on the Used SRP page, not the homepage — fetch one.
    origin = _origin(dealer_url)
    pair: tuple[str, str] | None = _extract_cosmos_pagecfg(html)
    if pair is None:
        for path in _COSMOS_SRP_PATHS:
            srp = fetch_dealer_html(origin + path)
            if not srp:
                continue
            pair = _extract_cosmos_pagecfg(srp)
            if pair:
                break
    if pair is None:
        return None
    srp_account, pagecfg = pair
    account = account or srp_account
    if not account or not pagecfg:
        return None
    url = f"{origin}{_COSMOS_PATH}/{account}/{pagecfg}"
    return EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="GET",
        content_type="application/json",
        post_template=None,
        auth_headers={},
        # Stored single-shot (like browser-captured cosmos recipes); the cosmos
        # ?pg=N&pn=96 walk is handled by validate_recipe and heal's _cosmos_pages.
        pagination=PAGINATION_NONE,
        provider_hint="dealer_on_cosmos",
    )


# ── Platform: Typesense (multi_search) ────────────────────────────────────────

# Typesense dealers (Toyota of Orange, Toyota Place, Freeway Honda) serve SRP
# inventory from a hosted Typesense collection. All three share one host + one
# search-only api key; only the collection is per-dealer. Every param the recipe
# needs is embedded client-side in the page JS:
#   __tsHost   = "hjnrb3s21408ezpfp.a1.typesense.net"
#   __tsApiKey = "<shared search-only key>"
#   currentIndex = "vehicles-<DEALER>"     (per-dealer collection)
_TYPESENSE_REF_ID = "toyotaoforange-com"
_TYPESENSE_REF_COLLECTION = "vehicles-TOY04247"

_TS_HOST_RES = (
    re.compile(r'__tsHost\s*=\s*["\']([^"\']+)["\']'),
    re.compile(r'([a-z0-9]+\.a1\.typesense\.net)'),
)
_TS_KEY_RES = (
    re.compile(r'__tsApiKey\s*=\s*["\']([A-Za-z0-9]{16,})["\']'),
    re.compile(r'x-typesense-api-key=([A-Za-z0-9]{16,})'),
)
_TS_COLLECTION_RES = (
    re.compile(r'currentIndex\s*=\s*["\'](vehicles-[A-Za-z0-9]{3,})["\']'),
    re.compile(r'["\'](vehicles-[A-Z0-9]{5,10})["\']'),
)


def _extract_ts_host(html: str) -> str | None:
    for rx in _TS_HOST_RES:
        m = rx.search(html)
        if m:
            return m.group(1)
    return None


def _extract_ts_key(html: str) -> str | None:
    for rx in _TS_KEY_RES:
        m = rx.search(html)
        if m:
            return m.group(1)
    return None


def _extract_ts_collection(html: str) -> str | None:
    for rx in _TS_COLLECTION_RES:
        m = rx.search(html)
        if m:
            return m.group(1)
    return None


def _detect_typesense(html: str, dealer_url: str) -> bool:
    if "typesense.net" not in html.lower():
        return False
    return bool(_extract_ts_collection(html))


def _ts_ref_key() -> str | None:
    """Shared search-only key parsed from the reference recipe URL."""
    ref = _load_reference_recipe(_TYPESENSE_REF_ID, "multi_search")
    if not ref:
        return None
    m = re.search(r'x-typesense-api-key=([A-Za-z0-9]+)', ref.url)
    return m.group(1) if m else None


def _synth_typesense(dealer_id: str, dealer_url: str, html: str) -> EndpointRecipe | None:
    collection = _extract_ts_collection(html)
    if not collection:
        return None
    ref = _load_reference_recipe(_TYPESENSE_REF_ID, "multi_search")
    if not ref or not ref.post_template:
        logger.warning("recipe_synth: Typesense reference template unavailable (%s)", _TYPESENSE_REF_ID)
        return None
    host = _extract_ts_host(html) or urlparse(ref.url).hostname
    # Key is shared across all dealers on this host; prefer the page's, fall back
    # to the reference recipe's.
    key = _extract_ts_key(html) or _ts_ref_key()
    if not host or not key:
        return None
    # Full-lot body: point every search at this dealer's collection AND drop the
    # reference recipe's `filter_by: "condition:Used"` so the recipe replays the
    # whole collection (new + used) — otherwise new inventory is excluded (same
    # defect fixed for CarsCommerce). Verified: dropping the filter takes Toyota
    # of Orange from 98 used to ~971 total.
    try:
        body = json.loads(ref.post_template)
    except ValueError:
        body = None
    if isinstance(body, dict) and isinstance(body.get("searches"), list):
        for s in body["searches"]:
            if isinstance(s, dict):
                s["collection"] = collection
                s.pop("filter_by", None)
                # Raise per_page to the Typesense max (250) so the bounded 40-page
                # walk reaches large collections instead of capping at 40*24=960.
                s["per_page"] = 250
        post_template = json.dumps(body)
    else:
        # Fallback: at least swap the collection so the recipe targets this dealer.
        post_template = ref.post_template.replace(_TYPESENSE_REF_COLLECTION, collection)
    url = f"https://{host}/multi_search?x-typesense-api-key={key}"
    return EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="POST",
        content_type="application/json; charset=utf-8",
        post_template=post_template,
        auth_headers={},
        pagination=PAGINATION_TYPESENSE,
        provider_hint="typesense",
    )


# ── Platform: sister.tv (Elasticsearch) — detected, not synthesizable ──────────

# TODO(sister_tv): recognized but NOT synthesizable from current dealer HTML.
# The es-data-v2.sister.tv/vehicles/inventory/_search GET replays fine (a
# library-scoped q=library_id:<id> returns vehicles), but the library_id is not
# in the live HTML of the two known dealers (audiofcostamesa/audifletcherjones,
# fjmercedes) — both have MIGRATED to CarsCommerce (ccid 5379783 / 2924) and are
# already browser-free via the CarsCommerce template. No current dealer embeds a
# sister.tv library_id to extract/validate against, so per the guardrail
# ("only register a template that REPLAYS and yields VINs") sister.tv stays
# synth=None. If a dealer resurfaces exposing library_id, add extraction here.
def _detect_sister_tv(html: str, dealer_url: str) -> bool:
    low = html.lower()
    return "es-data-v2.sister.tv" in low or "sister.tv/vehicles" in low


# ── Platform: Team Velocity (same-origin JSON inventory feed) ──────────────────

# Team Velocity (Right Honda, Right Toyota, Mark Kia) hydrates its SRP from a Vue
# SPA whose XHR endpoint (/api/Inventory/getinventorymultiselectionfilters) only
# returns filter FACETS — NOT the vehicle list. BUT the platform also publishes
# plain same-origin paginated JSON feeds (linked from inventorysitemap.xml), one
# per condition:
#   https://{domain}/inventory-used.json   (used; CPO is a subset of used)
#   https://{domain}/inventory-new.json    (new — a separate feed, disjoint VINs)
# shape: {totalVehicles, totalPages, nextPage, pageSize, vehicles:[...]}, walked
# via ?page=N. There is NO combined feed, so we emit one recipe per feed to cover
# the FULL lot (used + new). Parameterized by the dealer DOMAIN alone; the generic
# dealer_dot_com parser maps its vehicle objects; validate_recipe walks ?page=N.
_TEAM_VELOCITY_FEED = "/inventory-used.json"
# Used already contains CPO (verified: cpo VINs ⊆ used), so used + new = full lot.
_TEAM_VELOCITY_FEEDS = ("/inventory-used.json", "/inventory-new.json")


def _detect_team_velocity(html: str, dealer_url: str) -> bool:
    low = html.lower()
    return "teamvelocityportal" in low or "inventoryapibaseurl" in low


def _tv_feed_recipe(dealer_id: str, origin: str, feed_path: str) -> EndpointRecipe | None:
    """Build one Team Velocity feed recipe, probing page 1 for existence + total."""
    url = origin + feed_path
    first = _cosmos_get_json(url)
    if not isinstance(first, dict) or not first.get("vehicles"):
        return None
    try:
        total = int(first.get("totalVehicles") or 0) or None
    except (TypeError, ValueError):
        total = None
    return EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="GET",
        content_type="application/json",
        post_template=None,
        auth_headers={},
        # GET ?page=N feed — replay + delta walk every page (see PAGINATION_PAGE_QUERY).
        pagination=PAGINATION_PAGE_QUERY,
        total_count=total,
        provider_hint="dealer_dot_com",
    )


def _synth_team_velocity(dealer_id: str, dealer_url: str, html: str) -> list[EndpointRecipe]:
    """Emit a recipe per condition feed (used + new) — no combined feed exists."""
    origin = _origin(dealer_url)
    recipes = [r for fp in _TEAM_VELOCITY_FEEDS
               if (r := _tv_feed_recipe(dealer_id, origin, fp)) is not None]
    return recipes


# ── Platform: Dealer eProcess (server-rendered SRP + JSON-LD) ─────────────────

# Dealer eProcess ("Phoenix") dealers have NO JSON inventory API. Each SRP is a
# server-rendered HTML page carrying one JSON-LD @type:"Vehicle" block per card
# (12/page), paginated over plain HTTP with ?p=N. The recipe is therefore an HTML
# page-walk (PAGINATION_DEP_SRP) whose parser (dealer_eprocess) extracts VINs +
# prices from the embedded JSON-LD. The endpoint is parameterized by the dealer
# DOMAIN alone — no per-dealer account id / api key is needed.
#
# The friendly /used-inventory/ and /new-inventory/ paths usually filter by
# condition, so the FULL lot is the union of both feeds (mirrors Team Velocity's
# used+new). Some dealers configure BOTH paths to show the entire lot; we detect
# that (near-identical page-1 VINs) and emit a single recipe to avoid double work.
_DEP_SRP_PATHS = ("/used-inventory/", "/new-inventory/")
_DEP_COUNT_RE = re.compile(r'data-vehicle_count="(\d+)"')


def _detect_dealer_eprocess(html: str, dealer_url: str) -> bool:
    return "dealereprocess" in html.lower()


def _dep_fetch_html(url: str) -> str | None:
    """Proxy-aware GET returning the raw SRP HTML text (or ``None`` on failure).

    Unlike :func:`fetch_dealer_html` this does NOT reject "thin" bodies — an SRP
    page past the last result is a valid (short) page, and the caller stops when
    the parser extracts no more VINs.
    """
    import urllib.request

    try:
        resp = open_url(urllib.request.Request(url, headers=_browser_headers()), timeout=25.0)
        return resp.read().decode(resp.headers.get_content_charset() or "utf-8", "replace")
    except Exception as e:
        logger.debug("dep fetch failed %s: %s", url[:80], str(e)[:120])
        return None


def _dep_page_vins(html: str, dealer_id: str, dealer_url: str) -> set[str]:
    from backend.parsers.dealer_eprocess import parse as _dep_parse

    return _unique_vins(_dep_parse(html, base_url=dealer_url, dealer_id=dealer_id, dealer_url=dealer_url))


def _dep_srp_recipe(
    dealer_id: str, origin: str, path: str
) -> tuple[EndpointRecipe, set[str]] | None:
    """Build one DEP SRP recipe, probing page 1 for real vehicles + total count.

    Returns ``(recipe, page1_vins)`` or ``None`` when page 1 yields no vehicles.
    """
    url = origin + path
    html = _dep_fetch_html(url)
    if not html or "dealereprocess" not in html.lower():
        return None
    vins = _dep_page_vins(html, dealer_id, origin)
    if not vins:
        return None
    m = _DEP_COUNT_RE.search(html)
    total = int(m.group(1)) if m else None
    recipe = EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="GET",
        content_type="text/html",
        post_template=None,
        auth_headers={},
        pagination=PAGINATION_DEP_SRP,
        total_count=total,
        provider_hint="dealer_eprocess",
    )
    return recipe, vins


def _synth_dealer_eprocess(dealer_id: str, dealer_url: str, html: str) -> list[EndpointRecipe]:
    """Emit DEP SRP recipes — used + new, deduped when both show the whole lot."""
    origin = _origin(dealer_url)
    built: list[tuple[EndpointRecipe, set[str]]] = [
        r for p in _DEP_SRP_PATHS if (r := _dep_srp_recipe(dealer_id, origin, p)) is not None
    ]
    if not built:
        return []
    if len(built) == 2:
        (r_used, v_used), (r_new, v_new) = built
        # If the two friendly paths surface the same lot (page-1 VINs overlap
        # heavily and totals match), one recipe already covers everything.
        overlap = len(v_used & v_new)
        smaller = min(len(v_used), len(v_new)) or 1
        if overlap / smaller >= 0.5 and (r_used.total_count == r_new.total_count):
            return [r_used]
    return [r for r, _ in built]


# ── Platform registry ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class PlatformTemplate:
    name: str
    detect: Callable[[str, str], bool]
    # ``None`` synth => platform is recognized but not synthesizable yet
    # (browser still required); still useful to report *why*. A synth may return
    # a single recipe, ``None``, or a list (a platform that needs several
    # endpoints for full coverage, e.g. Team Velocity's used + new feeds).
    synth: Callable[[str, str, str], "EndpointRecipe | list[EndpointRecipe] | None"] | None


# Ordered most-specific-first; :func:`fingerprint_platform` returns the first hit.
PLATFORM_TEMPLATES: list[PlatformTemplate] = [
    PlatformTemplate("carscommerce", _detect_carscommerce, _synth_carscommerce),
    PlatformTemplate("typesense", _detect_typesense, _synth_typesense),
    PlatformTemplate("dealer_on_cosmos", _detect_dealer_on_cosmos, _synth_dealer_on_cosmos),
    PlatformTemplate("dealer_dot_com", _detect_dealer_com, _synth_dealer_com),
    PlatformTemplate("sister_tv", _detect_sister_tv, None),
    PlatformTemplate("team_velocity", _detect_team_velocity, _synth_team_velocity),
    PlatformTemplate("dealer_eprocess", _detect_dealer_eprocess, _synth_dealer_eprocess),
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


def synthesize_recipes(
    dealer_id: str, dealer_url: str, html: str, platform: str | None = None
) -> list[EndpointRecipe]:
    """Build ALL candidate recipes for *dealer_id* from *html*.

    Most platforms yield one recipe; some (Team Velocity) yield several to cover
    the full lot. Returns ``[]`` when the platform is unrecognized, has no
    template, or the per-dealer params can't be extracted. Each recipe is a
    *candidate*: validate it (:func:`validate_recipe`) before saving.
    """
    platform = platform or fingerprint_platform(html, dealer_url)
    tmpl = _TEMPLATES_BY_NAME.get(platform or "")
    if not tmpl or not tmpl.synth:
        return []
    try:
        result = tmpl.synth(dealer_id, dealer_url, html)
    except Exception as e:
        logger.debug("recipe_synth synth failed [%s/%s]: %s", dealer_id, platform, str(e)[:150])
        return []
    if result is None:
        return []
    return list(result) if isinstance(result, list) else [result]


def synthesize_recipe(
    dealer_id: str, dealer_url: str, html: str, platform: str | None = None
) -> EndpointRecipe | None:
    """Build the primary candidate recipe (first of :func:`synthesize_recipes`).

    Kept for single-recipe callers; returns ``None`` when nothing is synthesized.
    """
    recipes = synthesize_recipes(dealer_id, dealer_url, html, platform)
    return recipes[0] if recipes else None


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

    # DealerOn cosmos GETs paginate session-free via ?pg=N&pn=96 (not a POST-body
    # shape), so they need their own walk — same mechanism as heal's _cosmos_pages.
    if _COSMOS_PATH.split("/api")[-1] in recipe.url or "cosmos/srp/vehicles" in recipe.url:
        return _validate_cosmos(recipe, base_url, dealer_id, dealer_name, max_pages)
    # Team Velocity same-origin JSON feed paginates via ?page=N (nextPage/totalPages).
    if (
        recipe.pagination == PAGINATION_PAGE_QUERY
        or _TEAM_VELOCITY_FEED in recipe.url
        or recipe.url.endswith(("-used.json", "-cpo.json", "-new.json"))
    ):
        return _validate_json_feed(recipe, base_url, dealer_id, dealer_name, max_pages)
    # Dealer eProcess SRP: HTML page-walk (?p=N) with JSON-LD vehicles.
    if recipe.pagination == PAGINATION_DEP_SRP:
        return _validate_dep(recipe, base_url, dealer_id, dealer_name, max_pages)

    template: Any = None
    if recipe.post_template:
        try:
            template = json.loads(recipe.post_template)
        except ValueError:
            template = None
    if recipe.method != "GET" and template is None:
        return 0

    pages = 1 if recipe.pagination == PAGINATION_NONE else max_pages
    vins: set[str] = set()
    for page_i in range(pages):
        body = _mutate_for_page(recipe, template, page_i) if template is not None else None
        status, parsed = _replay_request(recipe, body, base_url, _url_for_page(recipe, page_i))
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


def _cosmos_get_json(url: str) -> Any | None:
    """Proxy-aware GET returning parsed JSON (or ``None``)."""
    import urllib.request

    req = urllib.request.Request(url, headers={**_browser_headers(), "Accept": "application/json"})
    try:
        resp = open_url(req, timeout=25.0)
        return json.loads(resp.read().decode("utf-8", "replace"))
    except Exception as e:
        logger.debug("json GET failed %s: %s", url[:80], str(e)[:120])
        return None


def _validate_json_feed(
    recipe: EndpointRecipe, base_url: str, dealer_id: str, dealer_name: str, max_pages: int
) -> int:
    """Walk a same-origin ``?page=N`` JSON inventory feed and count unique VINs.

    Team Velocity's ``/inventory-used.json`` feed carries ``totalPages`` /
    ``nextPage``; we page until those run out (or a page adds no new VINs).
    """
    from backend.parsers import parse

    clean = urlunparse(urlparse(recipe.url)._replace(query="", fragment=""))
    vins: set[str] = set()
    for pg in range(1, max_pages + 1):
        body = _cosmos_get_json(f"{clean}?page={pg}")
        if not isinstance(body, dict) or not body.get("vehicles"):
            break
        page_vehicles = list(parse(
            recipe.provider_hint or "dealer_dot_com", body,
            base_url=base_url, dealer_id=dealer_id,
            dealer_name=dealer_name, dealer_url=base_url,
        ))
        new = _unique_vins(page_vehicles) - vins
        if not new:
            break
        vins |= new
        try:
            total_pages = int(body.get("totalPages") or 0)
        except (TypeError, ValueError):
            total_pages = 0
        if not body.get("nextPage") or (total_pages and pg >= total_pages):
            break
    return len(vins)


def _validate_dep(
    recipe: EndpointRecipe, base_url: str, dealer_id: str, dealer_name: str, max_pages: int
) -> int:
    """Walk a Dealer eProcess SRP via ``?p=N`` and count unique JSON-LD VINs."""
    from backend.parsers import parse

    clean = urlunparse(urlparse(recipe.url)._replace(query="", fragment=""))
    vins: set[str] = set()
    for pg in range(1, max_pages + 1):
        html = _dep_fetch_html(f"{clean}?p={pg}")
        if not html:
            break
        page_vehicles = list(parse(
            recipe.provider_hint or "dealer_eprocess", html,
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


def _validate_cosmos(
    recipe: EndpointRecipe, base_url: str, dealer_id: str, dealer_name: str, max_pages: int
) -> int:
    """Walk a cosmos SRP endpoint via ``?pg=N&pn=96`` and count unique VINs."""
    from backend.parsers import parse

    clean = urlunparse(urlparse(recipe.url)._replace(query="", fragment=""))
    vins: set[str] = set()
    for pg in range(1, max_pages + 1):
        body = _cosmos_get_json(f"{clean}?pg={pg}&pn={_COSMOS_PAGE_SIZE}")
        if not isinstance(body, dict) or not body.get("DisplayCards"):
            break
        page_vehicles = list(parse(
            recipe.provider_hint or "dealer_on_cosmos", body,
            base_url=base_url, dealer_id=dealer_id,
            dealer_name=dealer_name, dealer_url=base_url,
        ))
        new = _unique_vins(page_vehicles) - vins
        if not new:
            break
        vins |= new
        total = int(((body.get("Paging") or {}).get("PaginationDataModel") or {}).get("TotalCount") or 0)
        if total and len(vins) >= total:
            break
    return len(vins)
