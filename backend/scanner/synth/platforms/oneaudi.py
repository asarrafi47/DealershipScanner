"""
OneAudi (omnigraph.audi.com GraphQL) platform template.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any

from backend.scanner.recipes import _replay_request, EndpointRecipe, PAGINATION_GRAPHQL_OFFSET
from backend.scanner.synth.common import _origin
from backend.scanner.synth.http import _dep_fetch_html, _fetch_impersonated

logger = logging.getLogger("scanner")


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
