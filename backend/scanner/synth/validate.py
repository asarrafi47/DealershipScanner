"""
Recipe validation by HTTP replay: :func:`validate_recipe` and its per-shape
walkers count real VINs a synthesized recipe yields. Moved verbatim from
``recipe_synth.py`` (audit F-6); not yet merged with
``backend.scanner.recipe_validation`` (follow-up).
"""
from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlparse, urlunparse

from backend.scanner.recipes import (
    _mutate_for_page,
    _replay_request,
    _unique_vins,
    _url_for_page,
    EndpointRecipe,
    PAGINATION_DEP_SRP,
    PAGINATION_HTML_PAGE,
    PAGINATION_JAZEL_SRP,
    PAGINATION_NONE,
    PAGINATION_PAGE_QUERY,
    recipe_is_store_scoped,
)
from backend.scanner.synth.http import _cosmos_get_json, _dep_fetch_html
from backend.scanner.synth.platforms.dealeron_cosmos import _COSMOS_PAGE_SIZE, _COSMOS_PATH
from backend.scanner.synth.platforms.team_velocity import _TEAM_VELOCITY_FEED


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
