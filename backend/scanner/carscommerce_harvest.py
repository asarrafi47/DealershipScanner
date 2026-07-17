"""
CarsCommerce group harvester — browser-free bulk field acquisition.

Many dealers share ONE CarsCommerce search host
(``websites-search.api.carscommerce.inc``) behind ONE ``x-api-key``. Each
dealer's captured recipe targets ``listings/<ccid>/search``; a single account
(``ccid``) serves an entire dealer group's inventory. Replaying it with a large
page size returns the whole lot with fields the dealer's own HTML often hides —
both colors, mechanical specs, body, mileage, trim, images.

This module replays those recipes at ``perPage=100`` (the response nests
listings under ``data.listings`` at that size — a shape the generic parser
misses), walks pagination, and returns normalized rows keyed by VIN.

Price is intentionally NOT harvested here: some accounts mask used-car prices
in the search feed (placeholder values like 85 / 122), and we already get price
reliably from the primary VDP scan. Callers fill only empty columns anyway, but
excluding price removes any chance of a masked value reaching a NULL cell.
"""
from __future__ import annotations

import json
import logging
import time
import urllib.request
from typing import Any

logger = logging.getLogger("scanner")

CARSCOMMERCE_HOST = "websites-search.api.carscommerce.inc"
_PER_PAGE = 100
_MAX_PAGES = 80  # 8,000 vehicles/account ceiling — a whole dealer group
_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)

# Fields this harvester maps (never price — see module docstring).
HARVEST_FIELDS = (
    "exterior_color",
    "interior_color",
    "mileage",
    "trim",
    "engine_description",
    "drivetrain",
    "fuel_type",
    "body_style",
    "image_url",
)


def is_carscommerce_recipe(url: str) -> bool:
    return CARSCOMMERCE_HOST in (url or "")


def _style(listing: dict) -> dict:
    s = listing.get("styles")
    if isinstance(s, list) and s and isinstance(s[0], dict):
        return s[0]
    return s if isinstance(s, dict) else {}


def _s(val: Any) -> str | None:
    if val is None:
        return None
    t = str(val).strip()
    return t or None


def _map_listing(listing: dict) -> dict[str, Any] | None:
    vin = _s(listing.get("vin"))
    if not vin or len(vin) != 17:
        return None
    style = _style(listing)
    mech = listing.get("mechanical") or {}
    body = listing.get("body_details") or {}
    media = listing.get("media") or {}
    images = media.get("images") if isinstance(media.get("images"), list) else []

    mileage = listing.get("mileage")
    try:
        mileage = int(mileage) if mileage not in (None, "") else None
        if mileage is not None and mileage < 0:
            mileage = None
    except (TypeError, ValueError):
        mileage = None

    return {
        "vin": vin.upper(),
        "exterior_color": _s(style.get("exterior_color") or style.get("exterior_color_generic")),
        "interior_color": _s(style.get("interior_color") or style.get("interior_color_generic")),
        "mileage": mileage,
        "trim": _s(listing.get("trim")),
        "engine_description": _s(mech.get("engine")),
        "drivetrain": _s(mech.get("drivetrain")),
        "fuel_type": _s(mech.get("fuel_type")),
        "body_style": _s(body.get("type") or body.get("generic_type")),
        "image_url": _s(images[0]) if images else None,
        "_features": listing.get("features") if isinstance(listing.get("features"), list) else None,
    }


def _post_json(url: str, body: dict, api_key: str) -> dict | None:
    req = urllib.request.Request(
        url,
        method="POST",
        data=json.dumps(body).encode(),
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "User-Agent": _UA,
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception as exc:
        logger.debug("CarsCommerce POST failed: %s", exc)
        return None


def _listings_and_total(payload: dict) -> tuple[list, int]:
    """Handle both response shapes: data=[...] (small) and data.listings=[...] (perPage>=100)."""
    data = payload.get("data")
    if isinstance(data, dict):
        listings = data.get("listings") or []
        total = int(data.get("total_vehicle_count") or 0)
    elif isinstance(data, list):
        listings = [x for x in data if isinstance(x, dict)]
        total = 0
    else:
        listings = []
        total = 0
    if not total:
        total = int(((payload.get("meta") or {}).get("pagination") or {}).get("total") or 0)
    return listings, total


def harvest_recipe(
    url: str,
    api_key: str,
    post_template: dict,
    *,
    include_new: bool = True,
    delay_s: float = 0.3,
) -> dict[str, dict[str, Any]]:
    """Walk one CarsCommerce account; return VIN -> mapped field dict (merged across pages)."""
    body = json.loads(json.dumps(post_template))  # deep copy
    body["perPage"] = _PER_PAGE
    body["page"] = 1
    if include_new:
        # Drop the Used/CPO facet so New inventory is included too.
        body.pop("facetFilters", None)

    by_vin: dict[str, dict[str, Any]] = {}
    first = _post_json(url, body, api_key)
    if not first:
        return by_vin
    listings, total = _listings_and_total(first)
    total_pages = min(_MAX_PAGES, max(1, -(-total // _PER_PAGE)) if total else 1)

    def _absorb(rows: list) -> None:
        for listing in rows:
            if not isinstance(listing, dict):
                continue
            m = _map_listing(listing)
            if m:
                by_vin[m["vin"]] = m

    _absorb(listings)
    for page in range(2, total_pages + 1):
        if delay_s:
            time.sleep(delay_s)
        body["page"] = page
        payload = _post_json(url, body, api_key)
        if not payload:
            break
        rows, _ = _listings_and_total(payload)
        if not rows:
            break
        _absorb(rows)
    logger.info(
        "CarsCommerce harvest: %s account -> %d vehicles (total=%d, %d page(s))",
        url.split("/listings/")[-1].split("/")[0], len(by_vin), total, total_pages,
    )
    return by_vin


def harvest_dealer(dealer_id: str, *, include_new: bool = True) -> dict[str, dict[str, Any]]:
    """Harvest every non-stale CarsCommerce recipe for a dealer_id."""
    from backend.scanner.recipes import load_recipes

    merged: dict[str, dict[str, Any]] = {}
    for recipe in load_recipes(dealer_id):
        if recipe.stale or not is_carscommerce_recipe(recipe.url):
            continue
        if not recipe.post_template:
            continue
        try:
            template = json.loads(recipe.post_template)
        except ValueError:
            continue
        api_key = (recipe.auth_headers or {}).get("x-api-key") or ""
        if not api_key:
            continue
        merged.update(harvest_recipe(recipe.url, api_key, template, include_new=include_new))
    return merged
