"""
Dealer Masters (Gatsby SSG, allInventoryJson static-query file) platform template.
"""
from __future__ import annotations

from backend.scanner.recipes import _unique_vins, EndpointRecipe, PAGINATION_NONE
from backend.scanner.synth.common import _origin
from backend.scanner.synth.http import _cosmos_get_json


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
