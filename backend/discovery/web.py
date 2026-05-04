"""
DuckDuckGo Instant Answer JSON API — URL gap-fill only (not primary geometry).

Uses ``https://api.duckduckgo.com`` with ``format=json``; defensive parsing.
Does not scrape HTML search results.
"""
from __future__ import annotations

import logging
from typing import Any
import requests

from backend.discovery.normalize import is_aggregator_url, normalize_url

logger = logging.getLogger(__name__)

DDG_INSTANT_URL = "https://api.duckduckgo.com/"
USER_AGENT = (
    "DealershipScanner/1.0 (+https://github.com/ DealershipScanner; "
    "dealer website lookup)"
)
MAX_DD_TIMEOUT_S = 15.0


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


def ddg_find_dealer_url(
    name: str,
    city: str,
    state: str,
    *,
    timeout_s: float = MAX_DD_TIMEOUT_S,
    session: requests.Session | None = None,
) -> str | None:
    """
    Return a plausible official website URL from DDG instant answers, or None.

    Query is constrained to the dealer name + city + state (geometry already known).
    """
    name = (name or "").strip()
    city = (city or "").strip()
    state = (state or "").strip().upper()
    if not name:
        return None
    q = f"{name} {city} {state} official website".strip()
    params = {"q": q, "format": "json", "no_html": "1", "skip_disambig": "1"}
    sess = session or requests.Session()
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    try:
        r = sess.get(
            DDG_INSTANT_URL,
            params=params,
            headers=headers,
            timeout=min(timeout_s, MAX_DD_TIMEOUT_S),
        )
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

    seen: set[str] = set()
    for raw in candidates:
        nu = normalize_url(raw)
        if not nu or nu in seen:
            continue
        seen.add(nu)
        if is_aggregator_url(nu):
            continue
        # Prefer HTTPS canonical
        return nu
    return None
