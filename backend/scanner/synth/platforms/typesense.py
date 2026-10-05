"""
Typesense (multi_search) platform template.
"""
from __future__ import annotations

import json
import re
from urllib.parse import urlparse

from backend.scanner.recipes import EndpointRecipe, PAGINATION_TYPESENSE
from backend.scanner.synth.common import _load_reference_recipe, _origin
from backend.scanner.synth.http import fetch_dealer_html


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
