"""
Dealership URL gap-fill via web search.

Strategy (tried in order per dealer):
1. DuckDuckGo HTML search  — real organic results, free, no key required
2. DuckDuckGo Instant Answer JSON — fast but rarely fires for dealerships (legacy fallback)
3. Google Places Find-Place  — highest accuracy; used only when GOOGLE_MAPS_API_KEY is set

All three filter aggregator/social domains before returning.
"""
from __future__ import annotations

import logging
import os
import re
import time
from typing import Any
from urllib.parse import unquote

import requests

from backend.discovery.normalize import is_aggregator_url, normalize_url

logger = logging.getLogger(__name__)

DDG_HTML_URL = "https://html.duckduckgo.com/html/"
DDG_INSTANT_URL = "https://api.duckduckgo.com/"
GOOGLE_PLACES_URL = "https://places.googleapis.com/v1/places:searchText"

# Realistic browser UA — DDG HTML blocks known bot agents
_HTML_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
_API_USER_AGENT = "SarrafiCollection/1.0 (+https://example.local; dealer website lookup)"

MAX_TIMEOUT_S = 20.0

# DuckDuckGo redirect URLs encode the destination in uddg= query param
_UDDG_RE = re.compile(r"[?&]uddg=([^&\"']+)")

# Set True when DDG returns 403 (hard IP ban) — skip HTML tier for rest of process lifetime
_ddg_html_banned: bool = False

# OEM manufacturer domains — prefer actual dealer domain over manufacturer subpage
_OEM_MANUFACTURER_HOSTS = frozenset({
    "toyota.com", "honda.com", "ford.com", "chevrolet.com", "gmc.com",
    "buick.com", "cadillac.com", "chrysler.com", "dodge.com", "jeep.com",
    "ram.com", "lincoln.com", "bmw.com", "mercedes-benz.com", "audi.com",
    "volkswagen.com", "vw.com", "hyundai.com", "kia.com", "subaru.com",
    "nissanusa.com", "mazda.com", "mitsubishicars.com", "acura.com",
    "lexus.com", "infiniti.com", "genesis.com", "porsche.com", "volvo.com",
    "landrover.com", "jaguarusa.com", "miniusa.com", "fiat.com",
    "alfaromeousa.com", "maserati.com",
})


def _host_of(url: str) -> str:
    from urllib.parse import urlparse
    try:
        return urlparse(url).netloc.lower().removeprefix("www.")
    except Exception:
        return ""


def _is_oem_manufacturer(url: str) -> bool:
    return _host_of(url) in _OEM_MANUFACTURER_HOSTS


def _pick_best(candidates: list[str]) -> str | None:
    """Return first non-aggregator URL, preferring dealer-owned domains over OEM manufacturer pages."""
    seen: set[str] = set()
    non_oem: list[str] = []
    oem_fallback: list[str] = []

    for raw in candidates:
        nu = normalize_url(raw)
        if not nu or nu in seen:
            continue
        seen.add(nu)
        if is_aggregator_url(nu):
            continue
        if _is_oem_manufacturer(nu):
            oem_fallback.append(nu)
        else:
            non_oem.append(nu)

    return (non_oem or oem_fallback or [None])[0]


# ── Strategy 1: DDG HTML search ────────────────────────────────────────────

def _ddg_html_find_dealer_url(
    name: str,
    city: str,
    state: str,
    *,
    timeout_s: float = MAX_TIMEOUT_S,
    session: requests.Session,
) -> str | None:
    """Parse actual DDG search results (HTML endpoint) for a dealer URL."""
    global _ddg_html_banned
    if _ddg_html_banned:
        return None

    q = f"{name} {city} {state} car dealership".strip()
    headers = {
        "User-Agent": _HTML_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }
    for attempt in range(2):
        if attempt > 0:
            time.sleep(3.0)
        try:
            r = session.get(
                DDG_HTML_URL,
                params={"q": q},
                headers=headers,
                timeout=min(timeout_s, MAX_TIMEOUT_S),
            )
        except requests.RequestException as e:
            logger.debug("DDG HTML search failed: %s", e)
            return None

        if r.status_code == 403:
            _ddg_html_banned = True
            logger.warning("DDG HTML returned 403 — disabling HTML tier for this run")
            return None

        if r.status_code == 202:
            logger.debug("DDG HTML rate-limited (202) for %r, attempt %d", name, attempt + 1)
            continue

        try:
            r.raise_for_status()
        except requests.RequestException as e:
            logger.debug("DDG HTML HTTP error: %s", e)
            return None

        raw_urls = [unquote(m.group(1)) for m in _UDDG_RE.finditer(r.text)]
        result = _pick_best(raw_urls)
        if result:
            return result

    return None


# ── Strategy 2: DDG Instant Answer (legacy fallback) ───────────────────────

def _collect_urls_from_topic(topic: dict[str, Any]) -> list[str]:
    out: list[str] = []
    if not isinstance(topic, dict):
        return out
    first = topic.get("FirstURL")
    if first:
        out.append(str(first))
    topics = topic.get("Topics")
    if isinstance(topics, list):
        for t in topics:
            out.extend(_collect_urls_from_topic(t))
    return out


def _ddg_instant_find_dealer_url(
    name: str,
    city: str,
    state: str,
    *,
    timeout_s: float = MAX_TIMEOUT_S,
    session: requests.Session,
) -> str | None:
    q = f"{name} {city} {state} official website".strip()
    params = {"q": q, "format": "json", "no_html": "1", "skip_disambig": "1"}
    headers = {"User-Agent": _API_USER_AGENT, "Accept": "application/json"}
    try:
        r = session.get(DDG_INSTANT_URL, params=params, headers=headers,
                        timeout=min(timeout_s, MAX_TIMEOUT_S))
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError) as e:
        logger.debug("DDG instant answer failed: %s", e)
        return None

    candidates: list[str] = []
    infobox = data.get("Infobox") or {}
    if isinstance(infobox, dict):
        for key in ("website", "Website", "url"):
            v = infobox.get(key)
            if isinstance(v, str) and v.startswith("http"):
                candidates.append(v)
    abstract_url = data.get("AbstractURL")
    if isinstance(abstract_url, str) and abstract_url.startswith("http"):
        candidates.append(abstract_url)
    for rt in data.get("RelatedTopics") or []:
        if isinstance(rt, dict):
            candidates.extend(_collect_urls_from_topic(rt))
    return _pick_best(candidates)


# ── Strategy 3: Google Places Find-Place ──────────────────────────────────

def _google_places_find_dealer_url(
    name: str,
    city: str,
    state: str,
    *,
    api_key: str,
    timeout_s: float = MAX_TIMEOUT_S,
    session: requests.Session,
) -> str | None:
    query = f"{name} {city} {state}"
    headers = {
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": "places.displayName,places.websiteUri",
        "Content-Type": "application/json",
    }
    try:
        r = session.post(
            GOOGLE_PLACES_URL,
            headers=headers,
            json={"textQuery": query},
            timeout=min(timeout_s, MAX_TIMEOUT_S),
        )
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError) as e:
        logger.debug("Google Places API failed: %s", e)
        return None

    for place in data.get("places") or []:
        website = (place.get("websiteUri") or "").strip()
        if website:
            nu = normalize_url(website)
            if nu and not is_aggregator_url(nu):
                return nu
    return None


# ── Public entry point ────────────────────────────────────────────────────

def ddg_find_dealer_url(
    name: str,
    city: str,
    state: str,
    *,
    timeout_s: float = MAX_TIMEOUT_S,
    session: requests.Session | None = None,
) -> str | None:
    """
    Return best dealer website URL using multi-strategy search.

    Order: DDG HTML search → DDG Instant Answer → Google Places (if key set).
    """
    name = (name or "").strip()
    city = (city or "").strip()
    state = (state or "").strip().upper()
    if not name:
        return None

    sess = session or requests.Session()

    # Strategy 1: DDG HTML — actual web results, works well for dealerships
    url = _ddg_html_find_dealer_url(name, city, state, timeout_s=timeout_s, session=sess)
    if url:
        logger.debug("DDG HTML found URL for %r: %s", name, url)
        return url

    # Strategy 2: DDG Instant Answer — legacy, rarely fires for dealerships
    url = _ddg_instant_find_dealer_url(name, city, state, timeout_s=timeout_s, session=sess)
    if url:
        logger.debug("DDG Instant Answer found URL for %r: %s", name, url)
        return url

    # Strategy 3: Google Places — highest accuracy, optional (requires API key)
    api_key = (os.environ.get("GOOGLE_MAPS_API_KEY") or "").strip()
    if api_key:
        url = _google_places_find_dealer_url(
            name, city, state, api_key=api_key, timeout_s=timeout_s, session=sess
        )
        if url:
            logger.debug("Google Places found URL for %r: %s", name, url)
            return url

    return None
