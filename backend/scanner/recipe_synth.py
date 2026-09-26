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

from datetime import datetime, timezone

import json
import logging
import os
import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from backend.scanner.http_fetch import open_url
from backend.scanner.recipes import (
    recipe_is_store_scoped,
    PAGINATION_ALGOLIA,
    PAGINATION_CARSCOMMERCE,
    PAGINATION_DEALER_COM,
    PAGINATION_DEP_SRP,
    PAGINATION_GRAPHQL_OFFSET,
    PAGINATION_HTML_PAGE,
    PAGINATION_JAZEL_SRP,
    PAGINATION_NONE,
    PAGINATION_COSMOS_PT,
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


# ── HTTP fetch ────────────────────────────────────────────────────────────────


# HTTP statuses worth a short retry — transient rate limits / gateway blips, not
# a hard "this needs a browser" signal.
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


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
    try:
        from curl_cffi import requests as cffi_requests
    except ImportError:
        return None

    from backend.scanner.chain import ImpersonatingFetcher
    from backend.scanner.http_fetch import proxy_url

    proxy = proxy_url()
    proxies = {"http": proxy, "https": proxy} if proxy else None

    for profile in ImpersonatingFetcher.PROFILES:
        _pace()
        try:
            resp = cffi_requests.get(
                url, impersonate=profile, timeout=timeout, proxies=proxies,
                allow_redirects=True, headers=headers or None,
            )
        except Exception as exc:
            logger.debug("recipe_synth impersonate %s failed %s: %s", profile, url[:70], str(exc)[:90])
            continue
        if resp.status_code != 200:
            continue
        html = resp.text or ""
        if len(html) < min_bytes:
            continue
        if looks_like_challenge(html):
            continue
        return html
    return None


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
    recipe = EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="POST",
        content_type="application/json; charset=utf-8",
        post_template=post_template,
        auth_headers={"x-api-key": api_key},
        pagination=PAGINATION_CARSCOMMERCE,
        provider_hint="dealer_dot_com",  # CarsCommerce payloads route through the generic dealer_dot_com mapper
    )
    try:
        extras = _carscommerce_store_filter(recipe, dealer_id, dealer_url, html)
    except Exception as exc:  # noqa: BLE001 - the unfiltered recipe still works for single-store accounts
        logger.debug("carscommerce store filter probe failed [%s]: %s", dealer_id, exc)
        extras = []
    return [recipe, *extras] if extras else recipe


# Group accounts (Hendrick: one ccid, 10,569 cars, 15 rooftops) tag each store's own
# cars in a custom_text facet — custom_text_11 = "Buford, GA" on Mall of Georgia
# Mazda (309 of 10,569); custom_text_4 / custom_text_2 hold every rooftop's city.
# The browser SRP sends that as facetFilters; over HTTP we learn it from the facet
# values on page 1 and the store's own place, then filter server-side instead of
# walking 106 pages and refusing 97% of the rows.
# custom_text_N semantics differ per ACCOUNT: on 2172862 (Mall of Georgia Mazda)
# custom_text_11 is the single store tag "Buford, GA"; on 5363312 (Hendrick's
# 11,651-car group) custom_text_2 is "City, ST" per rooftop, custom_text_3 the city,
# custom_text_11 an unrelated 1/2/3 code and custom_text_4 the certification
# program. So no facet name can be trusted by itself: every candidate value that
# spells this store's place is REPLAYED, and it is chosen only when every returned
# listing's own rooftop stamp (``dealer.location``, the field the attribution gate
# reads) is this store. Measured 2026-09-24: an unverified custom_text_2="Cary"
# kept 2 of Hendrick Buick GMC Cary's 390 cars.
_CC_CUSTOM_FACETS = tuple(f"custom_text_{i}" for i in range(1, 21))
# The Dealer Inspire page embeds the account's field map, e.g.
#   "Location":"custom_text_4","buford_location":"custom_text_11"   (Mall of Georgia Mazda)
#   "Location":"custom_text_2"                                      (Rick Hendrick Chevy Naples)
#   "Location":"custom_text_25","meta_location":"custom_text_11"    (Hendrick Buick GMC Cary)
# so the store facet is read from the site itself, not guessed; custom_text_25 is
# outside any blind 1..20 census.
_CC_LOCATION_MAP_RE = re.compile(r'"([A-Za-z_.]*[Ll]ocation[A-Za-z_]*)"\s*:\s*"(custom_text_\d+)"')


def _cc_location_facets(html: str) -> list[tuple[str, str]]:
    """[(field_name, facet)] the site maps to a location, exact "Location" first."""
    seen: dict[str, str] = {}
    for m in _CC_LOCATION_MAP_RE.finditer(html or ""):
        seen.setdefault(m.group(2), m.group(1))
    ranked = sorted(seen.items(), key=lambda kv: (kv[1] != "Location", "meta" in kv[1].lower(), kv[0]))
    return [(name, facet) for facet, name in ranked]
_CC_PLACE_ONLY_RE = re.compile(r"[A-Za-z .'\-]{2,40},\s*[A-Za-z]{2}(?:\s+\d{5})?")  # "Buford, GA" is a place, not a store name
_CC_SINGLE_STORE_MAX = 1000  # an account larger than this is a group even when its stamps say nothing
_CC_FACET_CHUNK = 7  # the API 400s on unknown facet names; small chunks keep one bad name from blanking the census


def _cc_facet_census(recipe: EndpointRecipe, body: dict, origin: str, facets: tuple[str, ...] = _CC_CUSTOM_FACETS) -> tuple[int, dict[str, list[tuple[str, int]]]]:
    total = 0
    values: dict[str, list[tuple[str, int]]] = {}
    for i in range(0, len(facets), _CC_FACET_CHUNK):
        probe = dict(body)
        probe.pop("facetFilters", None)
        probe.update({"page": 1, "perPage": 1, "facets": list(facets[i:i + _CC_FACET_CHUNK])})
        status, parsed = _replay_request(recipe, probe, origin)
        if status != 200 or not isinstance(parsed, dict):
            logger.info("carscommerce facet census chunk %d: status %s", i // _CC_FACET_CHUNK, status)
            continue
        data = parsed.get("data") or {}
        total = total or int(data.get("total_vehicle_count") or 0)
        for f in data.get("facets") if isinstance(data.get("facets"), list) else []:
            if not isinstance(f, dict):
                continue
            name = f.get("name") or f.get("field") or f.get("key")
            vals = f.get("values") if isinstance(f.get("values"), list) else None
            if name and vals:
                values[str(name)] = [(str(v.get("key")), int(v.get("doc_count") or 0)) for v in vals if isinstance(v, dict) and v.get("key") is not None]
    return total, values


def _cc_rooftop_sig(rt: dict) -> tuple[str, str]:
    """What identifies this rooftop stamp: ("street", <street key + city>) when
    it carries a street, ("name", <store name>) when it names a store, else
    ("place", <city state>). The phone line is left out on purpose: Stevenson
    Hendrick Honda's two feeds spell the same store's phone "396-1116" and
    "395-1116" (2026-09-24)."""
    from backend.parsers import _nrm, _street_key

    if rt.get("address") and re.match(r"\d", str(rt["address"]).strip()):
        return ("street", _street_key(rt["address"]) + "|" + _nrm(rt.get("city")) + _nrm(rt.get("state")))
    if rt.get("address"):
        # rooftop_of files a name-only label ("loaner", "none") under address too
        return ("tag", _nrm(rt["address"]))
    name = str(rt.get("alt_name") or rt.get("name") or "").strip()
    if name and not _CC_PLACE_ONLY_RE.fullmatch(name) and "<" not in name:
        return ("name", _nrm(name))
    return ("place", _nrm(rt.get("city")) + _nrm(rt.get("state")))


def _cc_rooftop_place_of(rt: dict) -> str:
    if rt.get("city") and rt.get("state"):
        return f"{rt['city']}, {rt['state']}".lower()
    return str(rt.get("name") or "").lower()


def _cc_rooftop_place(listing: dict) -> str:
    """"city, st" from the listing's own rooftop stamp (what the gate matches on)."""
    from backend.parsers.carscommerce import rooftop_of

    rt = rooftop_of(listing) or {}
    if rt.get("city") and rt.get("state"):
        return f"{rt['city']}, {rt['state']}".lower()
    return str(rt.get("name") or "").lower()


def _cc_verify_store_filter(recipe: EndpointRecipe, body: dict, origin: str, facet: str, key: str, label: str,
                            *, identity: bool = False, site_name: str = "") -> dict[str, Any]:
    """Replay page 1 under the candidate filter and describe what came back.

    ``ok`` means: rows came back, every one is stamped in this store's city and
    they all carry ONE rooftop identity. Same city is not enough: Mall of Georgia
    Mazda's account tags Mazda, MINI and a Hendrick store all "Buford, GA" under
    its Location facet (738 cars) while ``buford_location`` tags the Mazda store
    alone (309); Hendrick's "Cary, NC" Location value covers three Cary stores
    (713) while ``source_id`` 178465 — the page's own ``oem_code`` — is the Buick
    GMC store's feed (439). 2026-09-24."""
    from backend.parsers.carscommerce import rooftop_of

    probe = dict(body)
    probe.pop("facets", None)
    probe.update({"page": 1, "perPage": 60, "facetFilters": {facet: [key]}})
    status, parsed = _replay_request(recipe, probe, origin)
    if status != 200 or not isinstance(parsed, dict):
        return {"ok": False, "rows": 0, "places": [f"status {status}"], "rooftops": [], "names": []}
    listings = [x for x in ((parsed.get("data") or {}).get("listings") or []) if isinstance(x, dict)]
    stamps = [rooftop_of(x) or {} for x in listings]
    places = sorted({_cc_rooftop_place(x) for x in listings})
    rooftops = sorted({str(rt.get("key") or "") for rt in stamps})
    sigs = sorted({_cc_rooftop_sig(rt) for rt in stamps if rt})
    names = sorted({str(rt.get("alt_name") or rt.get("name") or "") for rt in stamps
                    if (rt.get("alt_name") or rt.get("name")) and rt.get("city")
                    and not _CC_PLACE_ONLY_RE.fullmatch(str(rt.get("alt_name") or rt.get("name")).strip())
                    and "<" not in str(rt.get("alt_name") or rt.get("name"))})
    # An identity feed (the page's own OEM code / store name) whose single
    # rooftop carries NO locale is still this store: Tutton CDJR's feed 27250
    # stamps only the store name, Group 1 Toyota North Austin's 42409 only a
    # name with no city (2026-09-26). A Location-facet candidate never gets
    # that benefit — a bare tag could be any store.
    placeless = bool(listings) and all(not (rt.get("city") and rt.get("state")) for rt in stamps)
    # rows the feed does not stamp at all (Group 1 Toyota North Austin 42409, Lenoir
    # City CDJR 45544): nothing contradicts the identity, and nothing to group by
    stampless = bool(listings) and not any(stamps)
    # For an identity-backed filter the stamps only VETO when one of them says
    # another place or another store: lot tags ("ALL", "SPC", "TOW/JORGE R/…")
    # and the store's own name are not contradictions.
    from backend.parsers import _looks_like_address as _addr_like
    from backend.parsers import _looks_like_store_name as _store_like
    from backend.parsers import _nrm as _pnrm

    def _contradicts(rt: dict) -> bool:
        if rt.get("city") and rt.get("state") and _cc_rooftop_place_of(rt) != label.lower():
            return True
        nm = str(rt.get("alt_name") or rt.get("name") or "").strip()
        # rooftop_of files a name-only label under "address" too; a label that
        # reads as a store name (and not as a street) is a store name
        junk = "/" in nm or sum(ch.isdigit() for ch in nm) > len(nm) * 0.3  # "TOW/JORGE R/367676": a lot note, not a store
        if nm and not junk and _store_like(nm) and not _CC_PLACE_ONLY_RE.fullmatch(nm) and "<" not in nm and not _addr_like(nm):
            own = _pnrm(site_name)
            return not (own and (_pnrm(nm) == own or own.startswith(_pnrm(nm)) or _pnrm(nm).startswith(own)))
        return False

    no_contradiction = bool(listings) and not any(_contradicts(rt) for rt in stamps if rt)
    consistent = identity and no_contradiction
    ok = bool(listings) and ((len(sigs) == 1 and (places == [label.lower()] or (identity and placeless))) or (identity and stampless) or consistent)
    return {"ok": ok, "rows": len(listings), "places": places, "rooftops": rooftops, "sigs": sigs, "names": names, "no_contradiction": no_contradiction,
            "total": int((parsed.get("data") or {}).get("total_vehicle_count") or 0)}


def _carscommerce_store_filter(recipe: EndpointRecipe, dealer_id: str, dealer_url: str, html: str) -> list[EndpointRecipe]:
    """Scope *recipe* to this store in place; returns EXTRA recipes to replay
    alongside it (a verified store-name Location facet that covers cars the
    identity feeds do not: Group 1 Toyota North Austin's 42409 holds 259 new
    cars, its used cars sit in group pools reachable only through
    custom_text_13="Group 1 Toyota North Austin", 2026-09-26)."""
    from backend.parsers.rooftop_aliases import record_rooftop_alias, roster_name_aliases
    from backend.scanner.dealer_place import learn_place, name_from_html, oem_code_from_html, place_label

    body = json.loads(recipe.post_template or "{}")
    origin = _origin(dealer_url)
    mapped = _cc_location_facets(html)
    facets = tuple(dict.fromkeys(["source_id"] + [f for _n, f in mapped] + list(_CC_CUSTOM_FACETS)))
    total, values = _cc_facet_census(recipe, body, origin, facets)
    if not values:
        return []
    oem_code = oem_code_from_html(html)
    site_name = name_from_html(html)
    if mapped or oem_code:
        logger.info("carscommerce [%s]: site says dealername=%r oem_code=%r; location facets %s", dealer_id, site_name, oem_code,
                    ", ".join(f"{n}={f}" for n, f in mapped) or "none")
    loc_values = [values.get(f) or [] for _n, f in mapped if (values.get(f) or [])]
    mapped_values_present = bool(loc_values)
    if mapped and loc_values and all(len(v) == 1 for v in loc_values):
        # Napleton Honda of Morton Grove (393 cars, feeds 207385 + MP21386): the
        # site's Location facets each hold ONE value, so the whole account is this
        # store; scoping to the OEM-code feed alone kept 49 of 393 (2026-09-26).
        logger.info("carscommerce [%s]: single-store account (%d cars): location facets %s hold one value each; no store filter",
                    dealer_id, total, ", ".join(f"{f}={v[0][0]!r}" for (_n, f), v in zip([m for m in mapped if values.get(m[1])], loc_values)))
        return []
    if mapped:
        logger.info("carscommerce [%s]: location facet values: %s", dealer_id,
                    "; ".join(f"{f}: " + ", ".join(f"{k}={n}" for k, n in (values.get(f) or [])[:6]) for _n, f in mapped))
    place = learn_place(dealer_id, dealer_url, html)
    label = place_label(place)
    if not label:
        logger.info("carscommerce [%s]: group account (%d cars) but no place evidence for this store; no store filter", dealer_id, total)
        return []
    city = label.split(",")[0].strip().lower()
    # Identity-backed feed ids: a source_id that IS the page's OEM dealer code
    # (178465 = Hendrick Buick GMC Cary's GM BAC) or spells the store's own name
    # ("MallofGeorgiaMazda" = "Mall of Georgia Mazda"). A store files under
    # several (dealer code + marketplace feeds: Mall of Georgia Mazda = 23978 with
    # 115 cars + MallofGeorgiaMazda with 194), so EVERY verified one is taken.
    def _nrm(s: str) -> str:
        return re.sub(r"[^a-z0-9]", "", str(s or "").lower())

    id_stem = _nrm(re.sub(r"-(com|net|org)$", "", dealer_id))
    identity = {x for x in (oem_code.lower() if oem_code else "", _nrm(site_name), id_stem) if x}

    def _is_identity(k: str) -> bool:
        nk = _nrm(k)
        if re.search(r"staging|test|sandbox|demo", k, re.I):
            return False  # "60503-staging" on Lexus of Greenwood Village: a staging feed, 62 of 484 cars
        if k in identity or nk in identity:
            return True
        # a named feed id is the store name without its town: "TuttonChryslerDodgeJeepRam"
        # (341 cars) for "Tutton Chrysler Dodge Jeep RAM of Jasper" (2026-09-26)
        site = _nrm(site_name)
        return len(nk) >= 10 and not nk.isdigit() and (site.startswith(nk) or (len(id_stem) >= 8 and nk.startswith(id_stem)))

    id_feeds: list[tuple[int, str]] = []
    name_facets: list[tuple[int, str, str]] = []
    mapped_rank = {f: i for i, (_n, f) in enumerate(mapped)}
    candidates: list[tuple[int, int, str, str]] = []
    for facet, vals in values.items():
        for key, n in vals:
            k = key.strip().lower()
            if n <= 0 or (total and n >= total):
                continue
            if facet == "source_id":
                if _is_identity(key):
                    id_feeds.append((n, key))
            elif k == label.lower() or k == city:
                candidates.append((1 + mapped_rank.get(facet, len(mapped_rank)), -n, facet, key))
            elif facet in mapped_rank and site_name and (_nrm(k) == _nrm(site_name) or (len(id_stem) >= 8 and _nrm(k) == id_stem)):
                # the site's own Location facet spelled as the store name
                # ("Group 1 Toyota North Austin" under custom_text_13, 2026-09-26)
                name_facets.append((n, facet, key))
    candidates.sort()
    tried: list[str] = []
    keys: list[str] = []
    verdicts: list[dict[str, Any]] = []
    for n, key in sorted(id_feeds, reverse=True):
        v = _cc_verify_store_filter(recipe, body, origin, "source_id", key, label, identity=True)
        tried.append(f"source_id={key!r} ({n} cars, identity): " + ("one rooftop, store-only" if v["ok"] else f"{len(v['rooftops'])} rooftops, places {v['places'][:4]}, stamps {[r[:50] for r in v['rooftops'][:3]]}"))
        if v["ok"]:
            keys.append(key)
            verdicts.append(v)
    if keys:
        # The OEM-code feed is often the SMALL one: Stevenson Hendrick Honda's
        # 208763 holds 45 cars while 9048741 — same "6720 Market St" rooftop —
        # holds 402 (2026-09-24). Every other feed id that returns exactly the
        # identity feeds' rooftop is that store's too. Only a rooftop that names
        # a store or a street qualifies; a bare "City, ST" stamp would merge a
        # same-town sibling.
        # An identity feed that stamps only "City, ST" (Stevenson Hendrick Mazda's
        # 24009) cannot vouch for a sibling feed by itself; the store's own street
        # (registry, or the page's JSON-LD streetAddress) can: the other feed's
        # single rooftop must sit at that street.
        from backend.parsers import _nrm as _pnrm
        from backend.parsers import _street_key

        own_sigs = {sg for v in verdicts for sg in v.get("sigs") or []}
        own_street = (_street_key(place.get("dealer_address")) + "|" + _pnrm(place.get("dealer_city")) + _pnrm(place.get("dealer_state"))) if place.get("dealer_address") else ""
        acceptable = {sg for sg in own_sigs if sg[0] != "place"}
        if own_street:
            acceptable.add(("street", own_street))
        others_consistent = True
        others_seen = 0
        if not acceptable:
            tried.append("no street or store name known for this store; sibling feed ids not merged")
        for n, key in sorted(((n, k) for k, n in values.get("source_id") or [] if k not in keys and n > 0 and not (total and n >= total)), reverse=True):
            v = _cc_verify_store_filter(recipe, body, origin, "source_id", key, label, site_name=site_name)
            others_seen += 1
            # a feed stamped with a STREET is only "this store" when it is our street
            # (Stevenson Hendrick Honda's siblings sit at other Wilmington streets)
            others_consistent = others_consistent and bool(v.get("no_contradiction")) and all(
                sg[0] != "street" or sg in acceptable for sg in (v.get("sigs") or []))
            same = bool(acceptable) and v["ok"] and bool(v.get("sigs")) and set(v["sigs"]) <= acceptable
            tried.append(f"source_id={key!r} ({n} cars): " + ("same rooftop, merged" if same else f"{len(v['rooftops'])} rooftops, places {v['places'][:3]}, not this store"))
            if same:
                keys.append(key)
                verdicts.append(v)
        bare_place_only = bool(own_sigs) and all(sg[0] == "place" for sg in own_sigs)  # "Naples, FL": a same-town sibling would look identical
        if others_seen and others_consistent and total <= _CC_SINGLE_STORE_MAX and not mapped_values_present and not bare_place_only:
            # Napleton Honda of Morton Grove: OEM feed 207385 (49) + marketplace
            # feed MP21386 (344, no stamps at all), Location facets empty, 393
            # cars in the account. No feed contradicts the store and the account
            # is store-sized: it IS the store. Scoping would keep 49 of 393.
            logger.info("carscommerce [%s]: single-store account by feed consistency (%d cars, %d other feed(s) with no contradicting stamp); no store filter",
                        dealer_id, total, others_seen)
            return []
    if not keys and place.get("dealer_address"):
        # No feed id carries the store's identity (Greenway CDJR of Rome: page
        # oem_code 45584, feeds 50377 / 45549 / a sibling's slug). A feed whose
        # rows all sit at the store's own STREET is the store's: the street is
        # the strongest locale evidence the gate itself accepts, and a same-town
        # sibling cannot share it. City alone is never enough here.
        from backend.parsers import _nrm as _pnrm
        from backend.parsers import _street_key

        own_street = _street_key(place.get("dealer_address")) + "|" + _pnrm(place.get("dealer_city")) + _pnrm(place.get("dealer_state"))
        for n, key in sorted(((n, k) for k, n in values.get("source_id") or [] if n > 0 and not (total and n >= total)), reverse=True)[:8]:
            v = _cc_verify_store_filter(recipe, body, origin, "source_id", key, label)
            at_street = v["ok"] and v.get("sigs") == [("street", own_street)]
            tried.append(f"source_id={key!r} ({n} cars, street check): " + ("at this store's street" if at_street else f"stamps {[r[:40] for r in v['rooftops'][:2]]}"))
            if at_street:
                keys.append(key)
                verdicts.append(v)
    # the facet the site itself calls "Location" outranks size: meta_location on
    # Group 1 Toyota North Austin tags 1,000 of the group's 1,787 cars with the
    # store's name while custom_text_13 (Location) is the store's own set
    name_facets.sort(key=lambda c: (mapped_rank.get(c[1], len(mapped_rank)), -c[0]))
    if not keys:
        # By elimination (Benson's Ingram Park Nissan, 2026-09-26): no feed carries
        # the page's OEM code, but every feed except ONE is stamped with another
        # store's name ("Ingram Park Chrysler Jeep Dodge", "Ingram Park Mazda",
        # "IPAC Pre-Owned Outlet") and that one carries no stamp at all. On a
        # store-sized account the unstamped feed is this store's.
        feeds = [(n, k) for k, n in values.get("source_id") or [] if n > 0 and not (total and n >= total)]
        if 2 <= len(feeds) <= 8 and total <= _CC_SINGLE_STORE_MAX * 2:
            clean: list[tuple[int, str, dict[str, Any]]] = []
            named_other = 0
            for n, key in feeds:
                v = _cc_verify_store_filter(recipe, body, origin, "source_id", key, label, identity=True, site_name=site_name)
                if v.get("no_contradiction") and v["rows"]:
                    clean.append((n, key, v))
                else:
                    named_other += 1
            if len(clean) == 1 and named_other == len(feeds) - 1:
                n, key, v = clean[0]
                keys.append(key)
                verdicts.append(v)
                tried.append(f"source_id={key!r} ({n} cars): the only feed not stamped with another store — this store's by elimination")
    chosen: tuple[str, list[str]] | None = ("source_id", keys) if keys else None
    if not chosen:
        for n, facet, key in name_facets:
            v = _cc_verify_store_filter(recipe, body, origin, facet, key, label, identity=True, site_name=site_name)
            tried.append(f"{facet}={key!r} ({n} cars, store-name facet): " + ("accepted" if v["ok"] else f"{len(v['rooftops'])} rooftops, places {v['places'][:4]}, stamps {[r[:40] for r in v['rooftops'][:3]]}"))
            if v["ok"]:
                chosen, verdicts = (facet, [key]), [v]
                break
    if not chosen:
        for _rank, negn, facet, key in candidates[:8]:
            v = _cc_verify_store_filter(recipe, body, origin, facet, key, label)
            tried.append(f"{facet}={key!r} ({-negn} cars): " + ("one rooftop, store-only" if v["ok"] else f"{len(v['rooftops'])} rooftops, places {v['places'][:4]}"))
            if v["ok"]:
                chosen, verdicts = (facet, [key]), [v]
                break
    if not chosen:
        census = ", ".join(f"{k}={n}" for k, n in sorted(values.get("source_id") or [], key=lambda kv: -kv[1])[:12])
        logger.info("carscommerce [%s]: group account (%d cars); no verified store filter for %r (tried: %s); source_id census: %s; gate filters per row",
                    dealer_id, total, label, "; ".join(tried) or "no facet value spells this place", census or "none")
        return []
    if chosen[0] == "source_id":
        # the feed's own spelling of this store, for the attribution gate
        for v in verdicts:
            for nm in v.get("names") or []:
                if nm and nm not in roster_name_aliases(dealer_id):
                    record_rooftop_alias(dealer_id, nm, evidence=f"source_id {chosen[1]} is this store's own feed id (page oem_code {oem_code!r} / name {site_name!r}); {v['rows']} rows one rooftop in {label}",
                                         observed=datetime.now(timezone.utc).date().isoformat())
                    logger.info("carscommerce [%s]: feed names this store %r (recorded as roster alias)", dealer_id, nm)
    extras: list[EndpointRecipe] = []
    if chosen[0] == "source_id" and name_facets:
        # the facet may cover a DIFFERENT subset than the feed ids (the used pool
        # vs the new feed), so its size says nothing; VIN dedupe absorbs overlap
        for n, facet, key in name_facets:
            v = _cc_verify_store_filter(recipe, body, origin, facet, key, label, identity=True, site_name=site_name)
            tried.append(f"{facet}={key!r} ({n} cars, store-name facet, extra): " + ("accepted" if v["ok"] else f"stamps {[r[:40] for r in v['rooftops'][:3]]}"))
            if v["ok"]:
                extra_body = dict(body)
                extra_body["facetFilters"] = {facet: [key]}
                extras.append(EndpointRecipe(
                    dealer_id=recipe.dealer_id, url=recipe.url, method=recipe.method, content_type=recipe.content_type,
                    post_template=json.dumps(extra_body), auth_headers=dict(recipe.auth_headers), pagination=recipe.pagination,
                    provider_hint=recipe.provider_hint, vehicle_rows=v["rows"], total_count=int(v.get("total") or 0),
                ))
                break
    body["facetFilters"] = {chosen[0]: chosen[1]}
    recipe.post_template = json.dumps(body)
    logger.info("carscommerce [%s]: group account (%d cars); store filter %s=%r verified on %d rows, %s cars%s (tried: %s)",
                dealer_id, total, chosen[0], chosen[1], sum(v["rows"] for v in verdicts), sum(int(v.get("total") or 0) for v in verdicts),
                f"; extra recipe {json.loads(extras[0].post_template)['facetFilters']} ({extras[0].total_count} cars)" if extras else "", "; ".join(tried))
    return extras


# ── Platform: DealerOn cosmos (ws/vhcliaa SRP) ────────────────────────────────

# DealerOn cosmos SRP endpoint:
#   https://{domain}/api/vhcliaa/vehicle-pages/cosmos/srp/vehicles/{account}/{pagecfg}
# parameterized by two ids, both recoverable over plain HTTP:
#   account  — the dealer id, in the homepage HTML (site-provider="dealeron",
#              data-website-id="do-{account}", "dealerId":"{account}").
#   pagecfg  — the SRP page config id, NOT on the homepage but embedded in the
#              used-inventory SRP page as its own page config
#              ({"dealerId":...,"pageId":{pagecfg},"pageType":"itemlist"|"custom"...}).
#              Most DealerOn dealers tag this "itemlist"; some (Toyota Direct,
#              Weatherford BMW of Berkeley — both fingerprinted via the shared
#              banrsaa.dealeron.com script host) tag the same used-SRP page
#              config "custom" instead. Either pageType's pageId replays fine
#              as the cosmos pagecfg (verified live for both), so no browser
#              capture is needed — just accept both pageType spellings.
# The endpoint paginates session-free via ?pt=N&pn=96 (pt = page number, pn =
# page size, 96 is the server max). NOT ?pg=: that parameter is ignored and
# returns page 1 again, which is why the August heal walked page 1 of every
# cosmos store 24 times (verified live 2026-09-23 on Cherokee County Toyota).
_COSMOS_PATH = "/api/vhcliaa/vehicle-pages/cosmos/srp/vehicles"
_COSMOS_PAGE_SIZE = 96
# SRP pages that carry a Used-scoped itemlist page config, and the New-scoped ones.
# Each SRP has its own pageId (Cherokee County Toyota: used 769890 = 106 cars,
# new 769883 = 282 cars); one recipe per section covers the lot.
_COSMOS_SRP_PATHS = ("/used-inventory/", "/searchused.aspx", "/used-vehicles/", "/inventory/used")
_COSMOS_SRP_PATHS_NEW = ("/new-inventory/", "/searchnew.aspx", "/new-vehicles/", "/inventory/new")

_COSMOS_ACCOUNT_RES = (
    re.compile(r'data-website-id="do-(\d+)"'),
    re.compile(r'"dealerId"\s*:\s*"?(\d+)"?'),
    re.compile(r'/static/dealer-(\d+)/'),
)
# {"dealerId":"25003","pageId":2483381,"pageType":"itemlist"...} — or, on some
# dealers (Toyota Direct, Weatherford BMW), the same shape tagged "custom".
_COSMOS_ITEMLIST_RE = re.compile(
    r'"dealerId"\s*:\s*"?(\d+)"?\s*,\s*"pageId"\s*:\s*(\d+)\s*,\s*"pageType"\s*:\s*"(?:itemlist|custom)"'
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


def _cosmos_pagecfg_from_paths(origin: str, paths: tuple[str, ...]) -> tuple[str, str] | None:
    for path in paths:
        srp = fetch_dealer_html(origin + path)
        if not srp:
            continue
        pair = _extract_cosmos_pagecfg(srp)
        if pair:
            return pair
    return None


def _synth_dealer_on_cosmos(dealer_id: str, dealer_url: str, html: str) -> list[EndpointRecipe]:
    """One recipe per SRP section (used, new): each has its own pageId."""
    account = _extract_cosmos_account(html)
    origin = _origin(dealer_url)
    pairs: list[tuple[str, str]] = []
    home_pair = _extract_cosmos_pagecfg(html)
    if home_pair:
        pairs.append(home_pair)
    for paths in (_COSMOS_SRP_PATHS, _COSMOS_SRP_PATHS_NEW):
        pair = _cosmos_pagecfg_from_paths(origin, paths)
        if pair and pair not in pairs:
            pairs.append(pair)
    out: list[EndpointRecipe] = []
    seen: set[str] = set()
    for srp_account, pagecfg in pairs:
        acct = account or srp_account
        if not acct or not pagecfg or pagecfg == "0" or pagecfg in seen:
            continue
        seen.add(pagecfg)
        out.append(EndpointRecipe(
            dealer_id=dealer_id,
            url=f"{origin}{_COSMOS_PATH}/{acct}/{pagecfg}",
            method="GET",
            content_type="application/json",
            post_template=None,
            auth_headers={},
            pagination=PAGINATION_COSMOS_PT,
            provider_hint="dealer_on_cosmos",
        ))
    return out


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
    # dealer_alchemist nodes:[{ host: 'hjnrb3s21408ezpfp.a1.typesense.net' }]
    re.compile(r'host["\']?\s*:\s*["\']([a-z0-9.-]+\.typesense\.net)["\']'),
    re.compile(r'([a-z0-9]+\.a1\.typesense\.net)'),
)
_TS_KEY_RES = (
    re.compile(r'__tsApiKey\s*=\s*["\']([A-Za-z0-9]{16,})["\']'),
    re.compile(r'x-typesense-api-key=([A-Za-z0-9]{16,})'),
    # dealer_alchemist (dv-framework / TypesenseInstantSearchAdapter) config:
    #   server: { apiKey: "…", nodes: [{ host: '…' }] }
    re.compile(r'apiKey["\']?\s*:\s*["\']([A-Za-z0-9]{16,})["\']'),
)
_TS_COLLECTION_RES = (
    re.compile(r'currentIndex\s*=\s*["\'](vehicles-[A-Za-z0-9]{3,})["\']'),
    # dealer_alchemist per-dealer collection:  indexName = "vehicles-TOY42087"
    re.compile(r'indexName["\']?\s*[:=]\s*["\'](vehicles-[A-Za-z0-9]{3,})["\']'),
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


# SRP paths that carry the TypesenseInstantSearchAdapter config when the homepage
# doesn't (dealer_alchemist inlines it on the inventory SRP, not the homepage).
_TS_SRP_PATHS = ("/new-vehicles/", "/inventory/", "/new-inventory/", "/used-vehicles/")

# query_by covering the getauto/Typesense document flavor — used to build a fresh
# multi_search body when no captured reference recipe is available (e.g.
# dealer_alchemist rooftops, which are self-describing from their adapter config).
_TS_DEFAULT_QUERY_BY = (
    "vin,stockNumber,lastEight,year,make,model,trim,exteriorColor,body,features,"
    "engine,transmission,drivetrain,fuel,genericColor,dealertag"
)


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


def _ts_default_body(collection: str) -> str:
    """A fresh full-collection multi_search body (no captured reference needed)."""
    return json.dumps({
        "searches": [{
            "collection": collection,
            "q": "*",
            "query_by": _TS_DEFAULT_QUERY_BY,
            "page": 1,
            "per_page": 250,
        }]
    })


def _synth_typesense(dealer_id: str, dealer_url: str, html: str) -> EndpointRecipe | None:
    # The per-dealer collection + adapter config live on the homepage for some
    # rooftops (currentIndex) and only on the inventory SRP for others
    # (dealer_alchemist inlines the TypesenseInstantSearchAdapter config there),
    # so fall back to fetching an SRP page when the homepage lacks the collection.
    collection = _extract_ts_collection(html)
    if not collection:
        origin = _origin(dealer_url)
        for path in _TS_SRP_PATHS:
            srp = fetch_dealer_html(origin + path)
            if srp and _extract_ts_collection(srp):
                html = srp  # re-extract host/key/collection from the SRP config
                collection = _extract_ts_collection(html)
                break
    if not collection:
        return None
    ref = _load_reference_recipe(_TYPESENSE_REF_ID, "multi_search")
    host = _extract_ts_host(html) or (urlparse(ref.url).hostname if ref else None)
    # Key is shared across all dealers on a host; prefer the page's, fall back to
    # the reference recipe's.
    key = _extract_ts_key(html) or _ts_ref_key()
    if not host or not key:
        return None
    # Full-lot body: point every search at this dealer's collection AND drop any
    # `filter_by: "condition:Used"` so the recipe replays the whole collection
    # (new + used) — otherwise new inventory is excluded (same defect fixed for
    # CarsCommerce). Verified: dropping the filter takes Toyota of Orange from 98
    # used to ~971 total. When no captured reference recipe exists, build a fresh
    # body — the adapter config is self-describing.
    body = None
    if ref and ref.post_template:
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
        post_template = _ts_default_body(collection)
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
# the FULL lot (used + new). Parameterized by the dealer DOMAIN alone; the dedicated
# team_velocity parser maps its vehicle objects (and owns VDP image/carfax
# completion); validate_recipe walks ?page=N.
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
        provider_hint="team_velocity",
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
# The SRP's "results per page" <select>: its numeric option values are the page
# sizes the server honours via ?ct=N (``ct=all`` is NOT honoured — it falls back
# to 12). Walking at the largest size cuts a 343-car new feed from 29 pages to 8.
# Anchor on the <select> TAG: the class name also appears in the page's inline
# CSS, thousands of bytes before the control. Option values are SRP URLs
# (``/search/used/?ct=48&tp=used``), so the size is read from their ``ct=``.
_DEP_PAGE_SIZE_SELECT_RE = re.compile(
    r'<select[^>]*results_per_page_controls__select[^>]*>(.*?)</select>', re.I | re.S
)
_DEP_OPTION_VALUE_RE = re.compile(r'<option[^>]*value="([^"]*)"', re.I)
_DEP_CT_RE = re.compile(r'(?:^|[?&;]|&amp;)ct=(\d+)')
_DEP_PAGE_SIZE_PARAM = "ct"
_DEP_PAGE_SIZE_CAP = 48


def _detect_dealer_eprocess(html: str, dealer_url: str) -> bool:
    return "dealereprocess" in html.lower()


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
_DEP_ESCALATE_STATUSES = frozenset({403, 405, 429})


def _dep_fetch_html(url: str) -> str | None:
    """:func:`_dep_fetch_page` without the final URL (HTML page-walk helper)."""
    return _dep_fetch_page(url)[0]


def _dep_page_size(html: str) -> int | None:
    """Largest numeric page size the SRP's results-per-page control offers, or None."""
    m = _DEP_PAGE_SIZE_SELECT_RE.search(html)
    if not m:
        return None
    sizes: list[int] = []
    for value in _DEP_OPTION_VALUE_RE.findall(m.group(1)):
        value = value.strip()
        ct = _DEP_CT_RE.search(value)
        if value.isdigit():
            sizes.append(int(value))
        elif ct:
            sizes.append(int(ct.group(1)))
    sizes = [n for n in sizes if 0 < n <= _DEP_PAGE_SIZE_CAP]
    return max(sizes) if sizes else None


def _with_query_param(url: str, key: str, value: str) -> str:
    parts = urlparse(url)
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != key]
    query.append((key, value))
    return urlunparse(parts._replace(query=urlencode(query), fragment=""))


def _dep_page_vins(html: str, dealer_id: str, dealer_url: str) -> set[str]:
    from backend.parsers.dealer_eprocess import parse as _dep_parse

    return _unique_vins(_dep_parse(html, base_url=dealer_url, dealer_id=dealer_id, dealer_url=dealer_url))


_DEP_LC_LINK_RE = re.compile(r'href=["\'](?:https?://[^/"\']+)?(/search/[^"\'#]*?[?&;](?:amp;)?lc=(\d+)[^"\'#]*)["\']', re.I)


def _dep_store_lc(html: str, dealer_id: str) -> str | None:
    """The site's own store id for DEP's ``lc=`` facet, or None.

    Group sites share one DEP inventory: lexusofknoxville.com's
    ``/search/pre-owned/?tp=pre_owned`` walked 1,641 cars (Chevrolet, GMC, Ford,
    "available in Franklin, TN") and filed them under Lexus of Knoxville
    (2026-09-26). The SRP's own model-facet links carry ``lc=<store id>`` in a
    slug that names the store (``/search/new-lexus-nx-450h+-lexus-of-knoxville/
    ?cy=37922&lc=15578&md=12629``); with it the same feed returns the store's
    lot only. Single-store sites either carry no ``lc`` (Fremont, Groove) or one
    value that is their own (Capital Toyota 8243), so a lone value is trusted.
    """
    found: dict[str, int] = {}
    slug_hits: dict[str, int] = {}
    name_slug = re.sub(r"[^a-z0-9]+", "-", (dealer_id or "").lower().replace("-com", "").replace("-net", "")).strip("-")
    for m in _DEP_LC_LINK_RE.finditer(html or ""):
        path, lc = m.group(1), m.group(2)
        found[lc] = found.get(lc, 0) + 1
        slug = path.split("?")[0].lower()
        if name_slug and name_slug.replace("-", "") in slug.replace("-", ""):
            slug_hits[lc] = slug_hits.get(lc, 0) + 1
    if slug_hits:
        return max(slug_hits, key=slug_hits.get)
    if len(found) == 1:
        return next(iter(found))
    return None


def _dep_srp_recipe(
    dealer_id: str, origin: str, path: str
) -> tuple[EndpointRecipe, set[str]] | None:
    """Build one DEP SRP recipe, probing page 1 for real vehicles + total count.

    Returns ``(recipe, page1_vins)`` or ``None`` when page 1 yields no vehicles.
    """
    html, final_url = _dep_fetch_page(origin + path)
    if not html or "dealereprocess" not in html.lower():
        return None
    vins = _dep_page_vins(html, dealer_id, origin)
    if not vins:
        return None
    m = _DEP_COUNT_RE.search(html)
    total = int(m.group(1)) if m else None
    # The friendly path often 302s to the real SRP (``/search/used/?tp=used``);
    # record THAT so the ``?p=N`` walk lands on the filtered feed rather than on a
    # redirect that may drop the page query. Keep the final URL's own params.
    url = final_url if _origin(final_url) == origin else origin + path
    url = urlunparse(urlparse(url)._replace(fragment=""))
    lc = _dep_store_lc(html, dealer_id)
    if lc and f"lc={lc}" not in url:
        scoped = _with_query_param(url, "lc", lc)
        scoped_html, _ = _dep_fetch_page(scoped)
        scoped_vins = _dep_page_vins(scoped_html, dealer_id, origin) if scoped_html else set()
        if scoped_vins:
            m2 = _DEP_COUNT_RE.search(scoped_html or "")
            scoped_total = int(m2.group(1)) if m2 else None
            logger.info("DEP store scope [%s]: lc=%s narrows %s from %s to %s car(s)", dealer_id, lc, path, total, scoped_total)
            url, html, vins, total = scoped, scoped_html, scoped_vins, scoped_total
    size = _dep_page_size(html)
    if size and (total is None or total > len(vins)):
        bigger = _with_query_param(url, _DEP_PAGE_SIZE_PARAM, str(size))
        big_html, _ = _dep_fetch_page(bigger)
        big_vins = _dep_page_vins(big_html, dealer_id, origin) if big_html else set()
        # Only keep the larger page size when the server honoured it.
        if len(big_vins) > len(vins):
            url, vins = bigger, big_vins
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


_DEP_NAV_SRP_RE = re.compile(r'href=["\'](?:https?://[^/"\']+)?(/search/(?:new|used|pre-owned|certified)[^"\'#?]*/?(?:\?[^"\'#]*)?)["\']', re.I)


def _dep_nav_srp_paths(html: str) -> list[str]:
    """SRP paths the site's own nav links to: one DEP variant serves
    /search/new-toyota/?mk=63&tp=new and /search/used-toyota/?tp=used instead
    of /new-inventory/ + /used-inventory/ (Capital Toyota, Groove Toyota,
    Lindsay Lexus of Alexandria: captured new-only recipes, 2026-09-26)."""
    out: list[str] = []
    for m in _DEP_NAV_SRP_RE.finditer(html or ""):
        p = m.group(1)
        base = p.split("?")[0].rstrip("/") + "/"
        if base not in {o.split("?")[0].rstrip("/") + "/" for o in out}:
            out.append(p if "?" in p else base)
    # keep one new-ish and one used-ish path at most, first seen wins
    keep: list[str] = []
    for want in ("new", "used", "pre-owned", "certified"):
        for p in out:
            if f"/search/{want}" in p.lower() and p not in keep:
                keep.append(p)
                break
    return keep


def _synth_dealer_eprocess(dealer_id: str, dealer_url: str, html: str) -> list[EndpointRecipe]:
    """Emit DEP SRP recipes — used + new, deduped when both show the whole lot."""
    origin = _origin(dealer_url)
    paths = list(_DEP_SRP_PATHS) + [p for p in _dep_nav_srp_paths(html) if p not in _DEP_SRP_PATHS]
    built: list[tuple[EndpointRecipe, set[str]]] = [
        r for p in paths if (r := _dep_srp_recipe(dealer_id, origin, p)) is not None
    ]
    if len(built) > 2:
        # the two largest distinct sets cover the lot; drop nav duplicates
        built.sort(key=lambda rv: -len(rv[1]))
        kept: list[tuple[EndpointRecipe, set[str]]] = []
        for r, v in built:
            if not any(len(v & kv) / (min(len(v), len(kv)) or 1) >= 0.5 for _kr, kv in kept):
                kept.append((r, v))
        built = kept[:2]
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


# ── Platform: Motive (ridemotive Algolia hosted search) ───────────────────────

# Motive dealers serve inventory from ONE shared Algolia index across the whole
# network, reachable over plain HTTP (the query hits the Algolia CDN, not the
# dealer origin, so it bypasses the dealer's Cloudflare). Only the per-dealer
# numeric dealer.id varies; the Algolia app id / search key / index prefix are the
# same network-wide (read from the dealer HTML env object, with known-good
# constants as a fallback). Body filter MUST use the dealer_ids array attribute as
# a quoted string (scalar dealer_id returns 0 for dealer-group members).
_MOTIVE_APP_ID = "G58LKO3ETJ"
_MOTIVE_API_KEY = "cc3dce06acb2d9fc715bc10c9a624d80"
_MOTIVE_INDEX_PREFIX = "production-inventory-"
_MOTIVE_SORT = "price_desc"
_MOTIVE_HITS_PER_PAGE = 1000

_MOTIVE_APPID_RE = re.compile(r'ALGOLIA_APP_ID\\?["\']?\s*:\s*\\?["\']([A-Za-z0-9]{6,})')
_MOTIVE_APIKEY_RE = re.compile(r'ALGOLIA_API_KEY\\?["\']?\s*:\s*\\?["\']([A-Za-z0-9]{16,})')
_MOTIVE_INDEX_RE = re.compile(r'ALGOLIA_INVENTORY_INDEX\\?["\']?\s*:\s*\\?["\']([A-Za-z0-9_.-]+?)\\?["\']')
_MOTIVE_DEALER_RES = (
    re.compile(r'\\?["\']dealer\\?["\']\s*:\s*\{[^{}]*?\\?["\']id\\?["\']\s*:\s*(\d+)'),
    re.compile(r'\\?["\']dealer_id\\?["\']\s*:\s*\\?["\']?(\d+)'),
)


def _detect_motive(html: str, dealer_url: str) -> bool:
    low = html.lower()
    if "ridemotive" not in low:
        return False
    return bool(_extract_motive_dealer_id(html))


def _extract_motive_dealer_id(html: str) -> str | None:
    for rx in _MOTIVE_DEALER_RES:
        m = rx.search(html)
        if m:
            return m.group(1)
    return None


def _synth_motive(dealer_id: str, dealer_url: str, html: str) -> EndpointRecipe | None:
    motive_dealer = _extract_motive_dealer_id(html)
    if not motive_dealer:
        return None
    m = _MOTIVE_APPID_RE.search(html)
    app_id = m.group(1) if m else _MOTIVE_APP_ID
    m = _MOTIVE_APIKEY_RE.search(html)
    api_key = m.group(1) if m else _MOTIVE_API_KEY
    m = _MOTIVE_INDEX_RE.search(html)
    prefix = m.group(1) if m else _MOTIVE_INDEX_PREFIX
    index = f"{prefix}global_{_MOTIVE_SORT}"
    url = f"https://{app_id}-dsn.algolia.net/1/indexes/{index}/query"
    body = {
        "filters": f'is_active:true AND dealer_ids:"{motive_dealer}"',
        "hitsPerPage": _MOTIVE_HITS_PER_PAGE,
        "page": 0,
    }
    return EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="POST",
        content_type="application/json",
        post_template=json.dumps(body),
        auth_headers={
            "X-Algolia-Application-Id": app_id,
            "X-Algolia-API-Key": api_key,
        },
        pagination=PAGINATION_ALGOLIA,
        provider_hint="motive_ridemotive",
    )


# ── Platform: Overfuel (Next.js SSR, __NEXT_DATA__ inventory) ──────────────────

# Overfuel SSR SRP embeds full per-vehicle records in <script id="__NEXT_DATA__">
# at props.pageProps.inventory.results (25/page, ?page=N). Parameterized by the
# dealer DOMAIN alone. The recipe is an HTML page-walk whose parser (overfuel)
# extracts __NEXT_DATA__ and walks inventory.results.
_OVERFUEL_SRP_PATH = "/inventory"


def _detect_overfuel(html: str, dealer_url: str) -> bool:
    return "overfuel" in html.lower()


def _synth_overfuel(dealer_id: str, dealer_url: str, html: str) -> EndpointRecipe | None:
    origin = _origin(dealer_url)
    url = origin + _OVERFUEL_SRP_PATH
    page1 = _dep_fetch_html(url)
    if not page1:
        return None
    from backend.parsers.overfuel import parse as _of_parse
    from backend.scanner.scrapers.next_data_inventory import parse_next_data_json_from_html

    vins = _unique_vins(_of_parse(page1, base_url=origin, dealer_id=dealer_id, dealer_url=origin))
    if not vins:
        return None
    total: int | None = None
    nd = parse_next_data_json_from_html(page1)
    try:
        meta = nd["props"]["pageProps"]["inventory"]["meta"]
        total = int(meta.get("total")) or None
    except (KeyError, TypeError, ValueError):
        total = None
    return EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="GET",
        content_type="text/html",
        post_template=None,
        auth_headers={},
        pagination=PAGINATION_HTML_PAGE,
        total_count=total,
        provider_hint="overfuel",
    )


# ── Platform: nabthat (Angular SSR SRP + schema.org Vehicle JSON-LD) ───────────

# nabthat SSR SRP returns text/html with 16 schema.org @type:Vehicle JSON-LD
# nodes per page, ?page=N honored server-side. Parameterized by the dealer DOMAIN
# alone. Same JSON-LD shape as Dealer eProcess, so it reuses the dealer_eprocess
# parser; the /inventory.json feed the platform advertises is a broken (502)
# Lambda route, so the SRP page-walk is the real path. Full lot = used + new.
_NABTHAT_SRP_PATHS = ("/inventory/used", "/inventory/new")


def _detect_nabthat(html: str, dealer_url: str) -> bool:
    return "nabthat.com" in html.lower()


def _nabthat_page_vins(html: str, dealer_id: str, origin: str) -> set[str]:
    from backend.parsers.dealer_eprocess import parse as _dep_parse

    return _unique_vins(_dep_parse(html, base_url=origin, dealer_id=dealer_id, dealer_url=origin))


def _nabthat_recipe(dealer_id: str, origin: str, path: str) -> tuple[EndpointRecipe, set[str]] | None:
    url = origin + path
    html = _dep_fetch_html(url)
    if not html:
        return None
    vins = _nabthat_page_vins(html, dealer_id, origin)
    if not vins:
        return None
    recipe = EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="GET",
        content_type="text/html",
        post_template=None,
        auth_headers={},
        pagination=PAGINATION_HTML_PAGE,
        provider_hint="dealer_eprocess",
    )
    return recipe, vins


def _synth_nabthat(dealer_id: str, dealer_url: str, html: str) -> list[EndpointRecipe]:
    origin = _origin(dealer_url)
    built: list[tuple[EndpointRecipe, set[str]]] = [
        r for p in _NABTHAT_SRP_PATHS if (r := _nabthat_recipe(dealer_id, origin, p)) is not None
    ]
    if not built:
        return []
    if len(built) == 2:
        (r_used, v_used), (r_new, v_new) = built
        overlap = len(v_used & v_new)
        smaller = min(len(v_used), len(v_new)) or 1
        if overlap / smaller >= 0.5:
            return [r_used]
    return [r for r, _ in built]


# ── Platform: Chapman (apiv2.chapmanapps.com flat JSON arrays) ─────────────────

# Chapman Auto Group runs an in-house Nuxt SSR SPA whose inventory is served by a
# clean REST API at apiv2.chapmanapps.com. GET /inventory/{arkona}/new and
# /inventory/{arkona}/used each return the dealer's full inventory as a single
# flat JSON array (no pagination). The only per-dealer input is the lowercase
# 'arkona' store code, extracted from the homepage HTML. Full lot = new + used.
_CHAPMAN_API_HOST = "apiv2.chapmanapps.com"
_CHAPMAN_ARKONA_RES = (
    re.compile(r'assets\.chapmanchoice\.com/img/dealers/([a-z0-9]+)\.webp', re.IGNORECASE),
    re.compile(r'\\?["\']arkona\\?["\']\s*:\s*\\?["\']([A-Za-z0-9]{2,8})\\?["\']'),
)
_CHAPMAN_CONDITIONS = ("new", "used")


def _detect_chapman(html: str, dealer_url: str) -> bool:
    low = html.lower()
    if "chapmanapps.com" not in low and "chapmanchoice.com" not in low:
        return False
    return bool(_extract_chapman_arkona(html))


def _extract_chapman_arkona(html: str) -> str | None:
    for rx in _CHAPMAN_ARKONA_RES:
        m = rx.search(html)
        if m:
            return m.group(1).lower()
    return None


def _chapman_recipe(dealer_id: str, arkona: str, condition: str) -> EndpointRecipe | None:
    url = f"https://{_CHAPMAN_API_HOST}/inventory/{arkona}/{condition}"
    body = _cosmos_get_json(url)
    if not isinstance(body, list) or not body:
        return None
    return EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="GET",
        content_type="application/json",
        post_template=None,
        auth_headers={},
        pagination=PAGINATION_NONE,
        total_count=len(body),
        provider_hint="chapman",
    )


def _synth_chapman(dealer_id: str, dealer_url: str, html: str) -> list[EndpointRecipe]:
    arkona = _extract_chapman_arkona(html)
    if not arkona:
        return []
    return [r for c in _CHAPMAN_CONDITIONS
            if (r := _chapman_recipe(dealer_id, arkona, c)) is not None]


# ── Platform: Jazel (SSR Angular SRP, jzlSetVehicleInfoContext) ────────────────

# Jazel serves a fully server-rendered SRP whose per-card vehicle JSON is embedded
# as window.jzlSetVehicleInfoContext('VIN', {...}) inline calls. Parameterized by
# the dealer DOMAIN alone; paginated by a path segment (.../srp-page-N/). The
# recipe is an HTML page-walk whose parser (jazel) regexes the calls. Note: Jazel
# is a dealer-GROUP platform (one SRP can mix rooftops), like mazdariverside.
_JAZEL_SRP_PATH = "/inventory/all-vehicles/"


def _detect_jazel(html: str, dealer_url: str) -> bool:
    low = html.lower()
    return (
        "jzlsetvehicleinfocontext" in low
        or "window.jzla5p" in low
        or "jazelc.com" in low
        or "jazel-cdn" in low
    )


def _synth_jazel(dealer_id: str, dealer_url: str, html: str) -> EndpointRecipe | None:
    origin = _origin(dealer_url)
    url = origin + _JAZEL_SRP_PATH
    page1 = _dep_fetch_html(url)
    if not page1:
        return None
    from backend.parsers.jazel import parse as _jazel_parse

    vins = _unique_vins(_jazel_parse(page1, base_url=origin, dealer_id=dealer_id, dealer_url=origin))
    if not vins:
        return None
    total: int | None = None
    m = re.search(r'([\d,]{1,7})\s+vehicles', page1, re.IGNORECASE)
    if m:
        try:
            total = int(m.group(1).replace(",", "")) or None
        except ValueError:
            total = None
    return EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="GET",
        content_type="text/html",
        post_template=None,
        auth_headers={},
        pagination=PAGINATION_JAZEL_SRP,
        total_count=total,
        provider_hint="jazel",
    )


# ── Platform: Dealer Masters (Gatsby SSG, allInventoryJson static-query file) ──
#
# Dealer Masters serves the FULL lot (new + used combined) from ONE Gatsby
# static-query data file at /page-data/sq/d/<queryhash>.json ->
# data.allInventoryJson.nodes. The hash is derived from the GraphQL query text
# (stable across rebuilds), but we resolve it dynamically so a query change
# self-heals on re-synth: read staticQueryHashes from the index/inventory
# page-data routes, then pick the sq/d file whose allInventoryJson yields the
# most VINs. Single GET, no pagination — the whole lot is in one file.
_DEALERMASTERS_INDEX_ROUTES = ("index", "used-inventory", "new-inventory")


# ── Platform: WordPress dealer sites with /wp-json/v1/vehicles ─────────────────
# Burns Honda, Honda of Cleveland, Honda of Pasadena, Kia of Chattanooga (2026-09-24):
# WordPress (WP Rocket, wpforms, an ADF lead plugin) exposing a REST index of every
# car as {title, link, search}. The index is the whole lot in one GET; identity
# comes from the title, everything else from the detail page's JSON-LD.
_WP_VEHICLES_PATH = "/wp-json/v1/vehicles"


def _detect_wp_vehicles_index(html: str, dealer_url: str) -> bool:
    low = html.lower()
    if "/wp-json/" not in low:
        return False
    return any(m in low for m in ("adf_lead_nonce", "asc_datalayer", "favorites_data", "wpforms_settings"))


def _fetch_wp_vehicles(origin: str) -> list[dict] | None:
    try:
        from curl_cffi import requests as cr

        r = cr.get(origin + _WP_VEHICLES_PATH, impersonate="chrome", timeout=30,
                   headers={"Accept": "application/json", "Referer": origin + "/"})
        if r.status_code != 200:
            return None
        js = r.json()
    except Exception:  # noqa: BLE001
        return None
    v = js.get("vehicles") if isinstance(js, dict) else None
    return v if isinstance(v, list) else None


def _synth_wp_vehicles_index(dealer_id: str, dealer_url: str, html: str) -> EndpointRecipe | None:
    origin = _origin(dealer_url)
    items = _fetch_wp_vehicles(origin)
    if not items or len(items) < 5:
        return None
    return EndpointRecipe(
        dealer_id=dealer_id,
        url=origin + _WP_VEHICLES_PATH,
        method="GET",
        content_type="application/json",
        post_template=None,
        auth_headers={},
        pagination=PAGINATION_NONE,
        provider_hint="wp_vehicles_index",
        vehicle_rows=len(items),
        total_count=len(items),
    )


# ── Platform: autoWALL (gratis solutions; server-rendered /gs-vehicle/list) ─────

# autoWALL sites (Long Chevrolet Buick GMC of Athens, 2026-09-24) answer plain
# HTTP: GET /gs-vehicle/list?filter=All&page=N returns an HTML SRP with one
# ``.vehicle-inventory-container[data-vin]`` card per car, 25 per page, and a
# "<N> Vehicles for Sale" title (309). The homepage does not carry the card
# markup (every discovery path 404s with the "Powered By autoWALL" title), so
# detection reads the nav links / badge and the SRP is fetched to confirm.
_AUTOWALL_LIST_PATH = "/gs-vehicle/list?filter=All"
_AUTOWALL_TOTAL_RE = re.compile(r"<title>\s*(\d{1,5})\s+Vehicles for Sale", re.I)


def _detect_autowall(html: str, dealer_url: str) -> bool:
    low = (html or "").lower()
    return "/gs-vehicle/list" in low or "powered_by_autowall" in low or "powered by autowall" in low


def _synth_autowall(dealer_id: str, dealer_url: str, html: str) -> EndpointRecipe | None:
    from backend.scanner.scrapers.autowall import _is_autowall_html, parse_autowall_inventory_html

    origin = _origin(dealer_url)
    url = origin + _AUTOWALL_LIST_PATH
    page = _dep_fetch_html(url)
    if not page or not _is_autowall_html(page):
        logger.info("autowall [%s]: %s did not return the card markup", dealer_id, url)
        return None
    rows = parse_autowall_inventory_html(page, base_url=origin, dealer_id=dealer_id, dealer_name=dealer_id, dealer_url=origin)
    if not rows:
        return None
    m = _AUTOWALL_TOTAL_RE.search(page)
    total = int(m.group(1)) if m else None
    logger.info("autowall [%s]: %d cards on page 1, title total %s", dealer_id, len(rows), total)
    return EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="GET",
        content_type="text/html",
        post_template=None,
        auth_headers={},
        pagination=PAGINATION_HTML_PAGE,
        provider_hint="autowall",
        vehicle_rows=len(rows),
        total_count=total,
    )


# ── Platform: OneAudi (omnigraph.audi.com GraphQL) ─────────────────────────────

# Audi's "falcon" renderer (audihuntsville.com, 2026-09-24). The SRP is SSR with the
# first 48 cars only and NO url pagination; the page embeds the Apollo cache of
# the query the app made, which carries every input we need: the dealer code
# ({"id":"dealer","items":["07B04"]}), the stat-import criterion, the market
# identifier (brand A / country us / language en) and the paging shape. The
# router at omnigraph.audi.com requires apollographql-client-name/-version
# headers (any values), disables introspection and validates enums against JSON
# variables — StockCarsType NEW / USED as variable values, never literals. Field
# names were read from the cached StockCar objects.
_ONEAUDI_GRAPHQL = "https://omnigraph.audi.com/graphql"
_ONEAUDI_SRP_PATHS = ("/all-inventory/", "/used-inventory/", "/new-inventory/", "/en/inventory/")
_ONEAUDI_DEALER_RE = re.compile(r'"id":"dealer","items":\["([A-Za-z0-9]{3,12})"\]')
_ONEAUDI_STATIMPORT_RE = re.compile(r'"id":"stat-import","items":\["([A-Za-z0-9_]{3,30})"\]')
_ONEAUDI_MARKET_RE = re.compile(r'"marketIdentifier":\{"brand":"([A-Za-z]{1,3})","country":"([a-z]{2})","language":"([a-z]{2})"\}')
_ONEAUDI_PAGE_SIZE = 48
_ONEAUDI_QUERY = (
    "query StockCarsScan($sp: StockCarSearchParameterInput!, $si: StockIdentifierInput!) { "
    "stockCarSearch(searchParameter: $sp, stockIdentifier: $si) { resultNumber results { cars { stockCar { "
    "vin titleText subtitleText cartypeText weblink commissionNumber gearText driveText "
    "modelInfo { genericModel { text code } modelyear } preUse { code text } "
    "carPrices { type price { value } } mileage { unitText value { number } } "
    "colorInfo { exteriorColor { colorInfo { text } baseColorInfo { text } } interiorColor { colorInfo { text } baseColorInfo { text } } } "
    "engineInfo { fuel { text } } images { url } dealer { id name city } dynamicAttributes { id value } "
    "} } } } }"
)


def _detect_oneaudi(html: str, dealer_url: str) -> bool:
    low = (html or "").lower()
    return "oneaudi-falcon" in low or "one.audi/" in low or "omnigraph.audi.com" in low


def _oneaudi_decoded(html: str) -> str:
    from urllib.parse import unquote

    # the cache is JSON inside JSON inside a url-encoded blob: quotes arrive as
    # %5C%22 / \\" / \\\\" — fold every backslash run before a quote
    return re.sub(r'\\+"', '"', unquote(html or ""))


def _oneaudi_inputs(html: str) -> dict[str, str] | None:
    dec = _oneaudi_decoded(html)
    m = _ONEAUDI_DEALER_RE.search(dec)
    if not m:
        return None
    out = {"dealer": m.group(1), "stat_import": "", "brand": "A", "country": "us", "language": "en"}
    si = _ONEAUDI_STATIMPORT_RE.search(dec)
    if si:
        out["stat_import"] = si.group(1)
    mk = _ONEAUDI_MARKET_RE.search(dec)
    if mk:
        out["brand"], out["country"], out["language"] = mk.group(1), mk.group(2), mk.group(3)
    return out


def _oneaudi_body(inputs: dict[str, str], stock_type: str) -> dict[str, Any]:
    criteria = [{"id": "dealer", "items": [inputs["dealer"]]}, {"id": "sold-order", "items": ["no"]}]
    if inputs.get("stat_import"):
        criteria.append({"id": "stat-import", "items": [inputs["stat_import"]]})
    return {
        "query": _ONEAUDI_QUERY,
        "variables": {
            "sp": {"criteria": criteria, "paging": {"limit": _ONEAUDI_PAGE_SIZE, "offset": 0},
                   "sort": {"direction": "ASC", "id": "DATE_PREDATEEND"}},
            "si": {"marketIdentifier": {"brand": inputs["brand"], "country": inputs["country"], "language": inputs["language"]},
                   "stockCarsType": stock_type},
        },
    }


def _synth_oneaudi(dealer_id: str, dealer_url: str, html: str) -> list[EndpointRecipe]:
    from backend.parsers.oneaudi import parse as _parse_oneaudi
    from backend.parsers.oneaudi import total_count as _oneaudi_total

    origin = _origin(dealer_url)
    inputs = _oneaudi_inputs(html)
    if not inputs:
        for path in _ONEAUDI_SRP_PATHS:
            page = _fetch_impersonated(origin + path, timeout=40.0) or _dep_fetch_html(origin + path)
            inputs = _oneaudi_inputs(page or "")
            if inputs:
                break
    if not inputs:
        logger.info("oneaudi [%s]: no stockCarSearch cache (dealer code) on the SRP pages", dealer_id)
        return []
    logger.info("oneaudi [%s]: dealer code %s, market %s/%s/%s, stat-import %r", dealer_id, inputs["dealer"], inputs["brand"], inputs["country"], inputs["language"], inputs.get("stat_import"))
    out: list[EndpointRecipe] = []
    for stock_type in ("NEW", "USED"):
        recipe = EndpointRecipe(
            dealer_id=dealer_id,
            url=_ONEAUDI_GRAPHQL,
            method="POST",
            content_type="application/json",
            post_template=json.dumps(_oneaudi_body(inputs, stock_type)),
            auth_headers={"apollographql-client-name": "dealershipscanner-stockcars", "apollographql-client-version": "1.0.0",
                          "Accept": "application/json"},
            pagination=PAGINATION_GRAPHQL_OFFSET,
            provider_hint="oneaudi",
        )
        status, parsed = _replay_request(recipe, json.loads(recipe.post_template), origin)
        rows = _parse_oneaudi(parsed, base_url=origin, dealer_id=dealer_id, dealer_name=dealer_id, dealer_url=origin) if parsed else []
        total = _oneaudi_total(parsed) if parsed else None
        errors = (parsed or {}).get("errors") if isinstance(parsed, dict) else None
        logger.info("oneaudi [%s]: %s page 1 status %s rows %d total %s%s", dealer_id, stock_type, status, len(rows), total,
                    f" errors {json.dumps(errors)[:300]}" if errors else "")
        if status == 200 and rows:
            recipe.vehicle_rows = len(rows)
            recipe.total_count = total
            out.append(recipe)
    return out


# ── Platform: server-rendered card pages (data-vin) ────────────────────────────

# Sites that render the whole lot into an HTML page with ``data-vin`` cards
# (Quantum Auto Sales, a "Responsive Automotive" Next.js site: 208 cars in
# /inventory/, 2026-09-26). The last template tried: it needs no fingerprint,
# only an SRP path that holds enough cards. The card parser
# (backend/parsers/html_cards.py) takes identity from JSON-LD or the detail
# slug; the HTTP-first detail pass fills the rest.
_HTML_CARDS_PATHS = ("/inventory/", "/inventory", "/cars-for-sale/", "/used-vehicles/", "/vehicles/", "/all-inventory/", "/used-cars/", "/inventory/used/", "/inventory/new/")
_HTML_CARDS_MIN = 10


def _detect_html_cards(html: str, dealer_url: str) -> bool:
    from backend.parsers.html_cards import detect

    if detect(html, _HTML_CARDS_MIN):
        return True
    low = (html or "").lower()
    return "data-vin=" in low and any(p in low for p in ("/inventory", "cars-for-sale", "/vehicles"))


def _synth_html_cards(dealer_id: str, dealer_url: str, html: str) -> list[EndpointRecipe]:
    from backend.parsers.html_cards import detect, parse

    origin = _origin(dealer_url)
    best: list[tuple[int, str, str]] = []
    for path in _HTML_CARDS_PATHS:
        page = _fetch_impersonated(origin + path, timeout=40.0) or _dep_fetch_html(origin + path)
        if not page or not detect(page, _HTML_CARDS_MIN):
            continue
        rows = parse(page, base_url=origin, dealer_id=dealer_id, dealer_name=dealer_id, dealer_url=origin)
        vins = _unique_vins(rows)
        if vins:
            best.append((len(vins), path, page))
    if not best:
        return []
    best.sort(reverse=True)
    out: list[EndpointRecipe] = []
    seen_vins: set[str] = set()
    for n, path, page in best[:2]:
        rows = parse(page, base_url=origin, dealer_id=dealer_id, dealer_name=dealer_id, dealer_url=origin)
        vins = _unique_vins(rows)
        if len(vins - seen_vins) < max(3, n // 5):
            continue  # same lot under another path
        seen_vins |= vins
        logger.info("html_cards [%s]: %s holds %d card(s)", dealer_id, path, n)
        out.append(EndpointRecipe(
            dealer_id=dealer_id, url=origin + path, method="GET", content_type="text/html", post_template=None, auth_headers={},
            pagination=PAGINATION_HTML_PAGE, provider_hint="html_cards", vehicle_rows=n, total_count=None,
        ))
    return out


def _detect_dealermasters(html: str, dealer_url: str) -> bool:
    return "dealermasters.com" in (html or "").lower()


def _dealermasters_static_query_hashes(origin: str) -> list[str]:
    """Union of Gatsby static-query hashes advertised by the inventory routes."""
    hashes: list[str] = []
    seen: set[str] = set()
    for route in _DEALERMASTERS_INDEX_ROUTES:
        data = _cosmos_get_json(f"{origin}/page-data/{route}/page-data.json")
        if not isinstance(data, dict):
            continue
        for h in data.get("staticQueryHashes") or []:
            h = str(h)
            if h and h not in seen:
                seen.add(h)
                hashes.append(h)
    return hashes


def _synth_dealermasters(dealer_id: str, dealer_url: str, html: str) -> EndpointRecipe | None:
    origin = _origin(dealer_url)
    from backend.parsers.dealermasters import parse as _dm_parse

    best_url: str | None = None
    best_vins: set[str] = set()
    for h in _dealermasters_static_query_hashes(origin):
        url = f"{origin}/page-data/sq/d/{h}.json"
        body = _cosmos_get_json(url)
        if body is None:
            continue
        vins = _unique_vins(_dm_parse(body, base_url=origin, dealer_id=dealer_id, dealer_url=origin))
        if len(vins) > len(best_vins):
            best_url, best_vins = url, vins
    if not best_url or not best_vins:
        return None
    return EndpointRecipe(
        dealer_id=dealer_id,
        url=best_url,
        method="GET",
        content_type="application/json",
        post_template=None,
        auth_headers={},
        pagination=PAGINATION_NONE,
        total_count=len(best_vins),
        provider_hint="dealermasters",
    )


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
    # typesense also fingerprints dealer_alchemist (dv-framework theme): same
    # shared Typesense host/key, same multi_search shape + parser, only the
    # per-dealer collection differs — extracted by the extended _TS_* regexes.
    PlatformTemplate("typesense", _detect_typesense, _synth_typesense),
    PlatformTemplate("dealer_on_cosmos", _detect_dealer_on_cosmos, _synth_dealer_on_cosmos),
    PlatformTemplate("dealer_dot_com", _detect_dealer_com, _synth_dealer_com),
    PlatformTemplate("sister_tv", _detect_sister_tv, None),
    PlatformTemplate("team_velocity", _detect_team_velocity, _synth_team_velocity),
    PlatformTemplate("dealer_eprocess", _detect_dealer_eprocess, _synth_dealer_eprocess),
    PlatformTemplate("motive_ridemotive", _detect_motive, _synth_motive),
    PlatformTemplate("overfuel", _detect_overfuel, _synth_overfuel),
    PlatformTemplate("nabthat", _detect_nabthat, _synth_nabthat),
    PlatformTemplate("chapman", _detect_chapman, _synth_chapman),
    PlatformTemplate("jazel", _detect_jazel, _synth_jazel),
    PlatformTemplate("dealermasters", _detect_dealermasters, _synth_dealermasters),
    PlatformTemplate("wp_vehicles_index", _detect_wp_vehicles_index, _synth_wp_vehicles_index),
    PlatformTemplate("autowall", _detect_autowall, _synth_autowall),
    PlatformTemplate("oneaudi", _detect_oneaudi, _synth_oneaudi),
    PlatformTemplate("html_cards", _detect_html_cards, _synth_html_cards),
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


# ── Universal browser-free fallback: generic schema.org Vehicle JSON-LD ────────
#
# The template path above is preferred: a replayable API/SRP recipe covers the
# WHOLE lot and paginates cleanly. But it only fires for platforms we have a
# template for. For the untemplated long tail — bespoke ``custom_standalone`` /
# luxury rooftops where a per-platform template has no ROI — there is still an
# elegant browser-free path IF the site server-renders schema.org Vehicle
# JSON-LD: the generic harvester (:func:`harvest_vehicles_from_html`) lists the
# lot straight from the HTML. This fallback detects that and confirms it yields
# real VINs, so such dealers are reported browser-free (strategy html_harvest)
# instead of "needs a browser". It is strictly additive — only consulted when no
# API template fingerprints/synthesizes.

# Inventory / SRP paths probed for embedded Vehicle JSON-LD when the page we were
# handed (usually the homepage) carries none. Ordered most-common-first. Both
# slashed and unslashed forms appear in the wild (dealer_eprocess/nabthat use a
# trailing slash; some CMSes 404 the other form rather than redirecting).
_HTML_HARVEST_SRP_PATHS = (
    "/used-inventory/",
    "/inventory",
    "/inventory/used",
    "/used-vehicles/",
    "/new-inventory/",
    "/vehicles/",
    "/all-inventory/",
    "/pre-owned/",
)


def harvest_html_vehicles(html: str) -> list[dict[str, Any]]:
    """Thin wrapper over :func:`html_jsonld_harvest.harvest_vehicles_from_html`
    (imported lazily to avoid a heavy import at module load)."""
    from backend.scanner.html_jsonld_harvest import harvest_vehicles_from_html

    return harvest_vehicles_from_html(html or "")


def detect_html_harvest(
    dealer_url: str,
    html: str | None,
    *,
    min_vins: int = 1,
    probe_srp: bool = True,
    max_srp_paths: int = 6,
) -> tuple[int, str | None]:
    """Universal browser-free fallback for untemplated-but-reachable dealers.

    Returns ``(vin_count, source_url)`` when *html* — or a server-rendered
    inventory / SRP page reachable over plain HTTP — embeds schema.org Vehicle
    JSON-LD carrying real VINs, else ``(0, None)``.

    First checks the HTML we already have (the caller's homepage/inventory
    fetch). If that carries fewer than *min_vins* distinct VINs and *probe_srp*
    is set, it fetches a short, ordered list of common inventory paths
    SEQUENTIALLY (paced by :func:`_pace`, with browser-navigation headers) and
    stops at the first page that clears the bar. Never launches a browser.
    """
    best_vins: set[str] = {v["vin"] for v in harvest_html_vehicles(html or "") if v.get("vin")}
    best_url: str | None = dealer_url if best_vins else None
    if len(best_vins) >= min_vins or not probe_srp:
        return len(best_vins), best_url

    origin = _origin(dealer_url)
    for path in _HTML_HARVEST_SRP_PATHS[:max_srp_paths]:
        page = fetch_dealer_html(origin + path)
        if not page:
            continue
        page_vins = {v["vin"] for v in harvest_html_vehicles(page) if v.get("vin")}
        if len(page_vins) > len(best_vins):
            best_vins, best_url = page_vins, origin + path
        if len(best_vins) >= min_vins:
            break
    return (len(best_vins), best_url) if best_vins else (0, None)


# ── Validation (replay over HTTP, count real VINs) ────────────────────────────

_VALIDATE_MAX_PAGES = 40


def validate_recipe(
    recipe: EndpointRecipe,
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    *,
    max_pages: int = _VALIDATE_MAX_PAGES,
    place: dict[str, str] | None = None,
) -> int:
    """Replay *recipe* over plain HTTP and return the unique VIN count.

    *place* (dealer_city / dealer_state / dealer_zip / dealer_address) is passed to
    the rooftop attribution gate: without it a group feed that names its rooftops
    by city refuses every row and the recipe validates to zero.

    Walks the recipe's pagination shape (single-shot for ``PAGINATION_NONE``),
    parses each page with the provider parser, and counts distinct VINs. No
    browser, no DB writes, no recipe-file mutation — a pure yield probe.
    """
    from backend.parsers import parse_kept

    # DealerOn cosmos GETs paginate session-free via ?pt=N&pn=96 (not a POST-body
    # shape), so they need their own walk — same mechanism as heal's _cosmos_pages.
    if _COSMOS_PATH.split("/api")[-1] in recipe.url or "cosmos/srp/vehicles" in recipe.url:
        return _validate_cosmos(recipe, base_url, dealer_id, dealer_name, max_pages, place)
    # Team Velocity same-origin JSON feed paginates via ?page=N (nextPage/totalPages).
    if (
        recipe.pagination == PAGINATION_PAGE_QUERY
        or _TEAM_VELOCITY_FEED in recipe.url
        or recipe.url.endswith(("-used.json", "-cpo.json", "-new.json"))
    ):
        return _validate_json_feed(recipe, base_url, dealer_id, dealer_name, max_pages, place)
    # Dealer eProcess SRP: HTML page-walk (?p=N) with JSON-LD vehicles.
    if recipe.pagination == PAGINATION_DEP_SRP:
        return _validate_dep(recipe, base_url, dealer_id, dealer_name, max_pages, place)
    # Server-rendered HTML page-walks reached with browser-navigation headers:
    #   PAGINATION_HTML_PAGE  — GET ?page=N (Overfuel __NEXT_DATA__, nabthat JSON-LD)
    #   PAGINATION_JAZEL_SRP  — GET path .../srp-page-N/ (Jazel inline JS objects)
    if recipe.pagination in (PAGINATION_HTML_PAGE, PAGINATION_JAZEL_SRP):
        return _validate_html_walk(recipe, base_url, dealer_id, dealer_name, max_pages, place)

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
        page_vehicles = list(parse_kept(
            recipe.provider_hint or "", parsed,
            base_url=base_url, dealer_id=dealer_id,
            dealer_name=dealer_name, dealer_url=base_url,
            trust_feed_scope=recipe_is_store_scoped(recipe, dealer_name),
            **(place or {}),
        ))
        new = _unique_vins(page_vehicles) - vins
        if not new:
            break
        vins |= new
        if recipe.total_count and len(vins) >= recipe.total_count:
            break
    return len(vins)


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


def _validate_json_feed(recipe: EndpointRecipe, base_url: str, dealer_id: str, dealer_name: str, max_pages: int, place: dict[str, str] | None = None) -> int:
    """Walk a same-origin ``?page=N`` JSON inventory feed and count unique VINs.

    Team Velocity's ``/inventory-used.json`` feed carries ``totalPages`` /
    ``nextPage``; we page until those run out (or a page adds no new VINs).
    """
    from backend.parsers import parse_kept

    clean = urlunparse(urlparse(recipe.url)._replace(query="", fragment=""))
    vins: set[str] = set()
    for pg in range(1, max_pages + 1):
        body = _cosmos_get_json(f"{clean}?page={pg}")
        if not isinstance(body, dict) or not body.get("vehicles"):
            break
        page_vehicles = list(parse_kept(
            recipe.provider_hint or "dealer_dot_com", body,
            base_url=base_url, dealer_id=dealer_id,
            dealer_name=dealer_name, dealer_url=base_url,
            trust_feed_scope=recipe_is_store_scoped(recipe, dealer_name),
            **(place or {}),
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


def _validate_dep(recipe: EndpointRecipe, base_url: str, dealer_id: str, dealer_name: str, max_pages: int, place: dict[str, str] | None = None) -> int:
    """Walk a Dealer eProcess SRP via ``?p=N`` and count unique JSON-LD VINs."""
    from backend.parsers import parse_kept

    vins: set[str] = set()
    for pg in range(max_pages):
        # _url_for_page keeps the recipe's own query (``tp=used``, ``ct=48``) and
        # sets ``p=N``; stripping the query used to drop the condition filter and
        # the page size the synthesizer had just chosen.
        html = _dep_fetch_html(_url_for_page(recipe, pg))
        if not html:
            break
        page_vehicles = list(parse_kept(
            recipe.provider_hint or "dealer_eprocess", html,
            base_url=base_url, dealer_id=dealer_id,
            dealer_name=dealer_name, dealer_url=base_url,
            trust_feed_scope=recipe_is_store_scoped(recipe, dealer_name),
            **(place or {}),
        ))
        new = _unique_vins(page_vehicles) - vins
        if not new:
            break
        vins |= new
        if recipe.total_count and len(vins) >= recipe.total_count:
            break
    return len(vins)


def _validate_html_walk(recipe: EndpointRecipe, base_url: str, dealer_id: str, dealer_name: str, max_pages: int, place: dict[str, str] | None = None) -> int:
    """Walk a server-rendered HTML page-walk recipe and count unique VINs.

    Uses the proxy-aware, browser-navigation-header fetch (:func:`_dep_fetch_html`)
    so paced requests get real HTML rather than a Cloudflare challenge, and the
    per-page URL from :func:`_url_for_page` (``?page=N`` for ``PAGINATION_HTML_PAGE``,
    ``.../srp-page-N/`` for ``PAGINATION_JAZEL_SRP``). Each page's HTML is handed
    to the provider parser (which accepts the raw HTML string).
    """
    from backend.parsers import parse_kept

    vins: set[str] = set()
    for page_i in range(max_pages):
        html = _dep_fetch_html(_url_for_page(recipe, page_i))
        if not html:
            break
        page_vehicles = list(parse_kept(
            recipe.provider_hint or "", html,
            base_url=base_url, dealer_id=dealer_id,
            dealer_name=dealer_name, dealer_url=base_url,
            trust_feed_scope=recipe_is_store_scoped(recipe, dealer_name),
            **(place or {}),
        ))
        new = _unique_vins(page_vehicles) - vins
        if not new:
            break
        vins |= new
        if recipe.total_count and len(vins) >= recipe.total_count:
            break
    return len(vins)


def _validate_cosmos(recipe: EndpointRecipe, base_url: str, dealer_id: str, dealer_name: str, max_pages: int, place: dict[str, str] | None = None) -> int:
    """Walk a cosmos SRP endpoint via ``?pt=N&pn=96`` and count unique VINs."""
    from backend.parsers import parse_kept

    clean = urlunparse(urlparse(recipe.url)._replace(query="", fragment=""))
    vins: set[str] = set()
    for pg in range(1, max_pages + 1):
        body = _cosmos_get_json(f"{clean}?pt={pg}&pn={_COSMOS_PAGE_SIZE}")
        if not isinstance(body, dict) or not body.get("DisplayCards"):
            break
        page_vehicles = list(parse_kept(
            recipe.provider_hint or "dealer_on_cosmos", body,
            base_url=base_url, dealer_id=dealer_id,
            dealer_name=dealer_name, dealer_url=base_url,
            trust_feed_scope=recipe_is_store_scoped(recipe, dealer_name),
            **(place or {}),
        ))
        new = _unique_vins(page_vehicles) - vins
        if not new:
            break
        vins |= new
        total = int(((body.get("Paging") or {}).get("PaginationDataModel") or {}).get("TotalCount") or 0)
        if total and len(vins) >= total:
            break
    return len(vins)
