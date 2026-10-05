"""
Motive (ridemotive Algolia hosted search) platform template.
"""
from __future__ import annotations

import json
import re

from backend.scanner.recipes import EndpointRecipe, PAGINATION_ALGOLIA


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
