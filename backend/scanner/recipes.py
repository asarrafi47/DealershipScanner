"""
Endpoint replay recipes: persist the inventory API endpoints a scan discovers so
future scans can fetch inventory over plain HTTP before launching Playwright.

A recipe is one captured endpoint (URL, method, POST template, auth headers,
pagination shape) proven to return vehicle rows during a browser scan. Recipes
are promoted from the ``NetworkObserver`` ledger at the end of each dealer run
and stored per dealer under ``workspace/recipes/<dealer_id>.json``.

Replay contract (``recipe_fetch`` phase, see ``try_fetch_via_recipes``):
  - attempt each stored recipe over HTTP with the recorded headers;
  - accept only when the parsed result yields >= ``min_vehicles`` unique VINs;
  - on 401/403 mark the recipe stale (auth rotated) — the browser scan that
    follows re-captures fresh auth headers and re-promotes.

Treat recipe files as sensitive-ish (they can embed public search API keys);
they stay under workspace/ which is not committed.
"""
from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import re
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger("scanner")

RECIPES_DIR = Path("workspace") / "recipes"

# Pagination shapes we know how to walk during replay.
PAGINATION_CARSCOMMERCE = "carscommerce_page"   # POST body {"page": N, "perPage": M}
PAGINATION_TYPESENSE = "typesense_page"          # POST body searches[].page / per_page
PAGINATION_DEALER_COM = "dealer_com_start"       # POST body inventoryParameters.start
PAGINATION_NONE = "none"                         # single-shot GET/POST


@dataclass
class EndpointRecipe:
    dealer_id: str
    url: str
    method: str
    content_type: str
    post_template: str | None
    auth_headers: dict[str, str] = field(default_factory=dict)
    pagination: str = PAGINATION_NONE
    vehicle_rows: int = 0
    total_count: int | None = None
    provider_hint: str = ""
    saved_at: float = 0.0
    last_ok_at: float = 0.0
    stale: bool = False
    stale_reason: str = ""

    def key(self) -> tuple[str, str]:
        p = urlparse(self.url)
        return (self.method, f"{(p.hostname or '').lower()}{p.path}")


def infer_pagination(url: str, post_template: str | None) -> str:
    host = (urlparse(url).hostname or "").lower()
    body = post_template or ""
    if "carscommerce" in host and '"page"' in body:
        return PAGINATION_CARSCOMMERCE
    if "typesense" in host:
        return PAGINATION_TYPESENSE
    if "ws-inv-data" in url and "inventoryParameters" in body:
        return PAGINATION_DEALER_COM
    return PAGINATION_NONE


def _recipe_path(dealer_id: str) -> Path:
    slug = re.sub(r"[^a-z0-9_-]+", "-", (dealer_id or "unknown").lower()).strip("-") or "unknown"
    return RECIPES_DIR / f"{slug}.json"


def load_recipes(dealer_id: str) -> list[EndpointRecipe]:
    path = _recipe_path(dealer_id)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    out: list[EndpointRecipe] = []
    for row in raw if isinstance(raw, list) else []:
        try:
            out.append(EndpointRecipe(**{k: v for k, v in row.items()
                                         if k in EndpointRecipe.__dataclass_fields__}))
        except TypeError:
            continue
    return out


def save_recipes(dealer_id: str, recipes: list[EndpointRecipe]) -> None:
    RECIPES_DIR.mkdir(parents=True, exist_ok=True)
    path = _recipe_path(dealer_id)
    path.write_text(json.dumps([asdict(r) for r in recipes], indent=1), encoding="utf-8")


def mark_stale(dealer_id: str, recipe: EndpointRecipe, reason: str) -> None:
    recipes = load_recipes(dealer_id)
    for r in recipes:
        if r.key() == recipe.key():
            r.stale = True
            r.stale_reason = reason[:200]
    save_recipes(dealer_id, recipes)


def promote_from_ledger(
    dealer_id: str,
    provider: str,
    ledger_endpoints: list[Any],
    *,
    min_vehicle_rows: int = 3,
    max_recipes: int = 4,
) -> int:
    """
    Merge qualifying ``CapturedEndpoint``s into the dealer's recipe file.
    Fresh captures replace stale/older entries for the same (method, host+path).
    Returns the number of recipes written.
    """
    now = time.time()
    candidates: list[EndpointRecipe] = []
    for ep in ledger_endpoints or []:
        rows = int(getattr(ep, "vehicle_rows", 0) or 0)
        total = getattr(ep, "total_count", None)
        if rows < min_vehicle_rows and not total:
            continue
        url = str(getattr(ep, "url", "") or "")
        if not url.startswith("http"):
            continue
        post = getattr(ep, "post_data_sample", None)
        candidates.append(
            EndpointRecipe(
                dealer_id=dealer_id,
                url=url,
                method=str(getattr(ep, "method", "GET") or "GET").upper(),
                content_type=str(getattr(ep, "content_type", "") or ""),
                post_template=post,
                auth_headers=dict(getattr(ep, "auth_headers", None) or {}),
                pagination=infer_pagination(url, post),
                vehicle_rows=rows,
                total_count=int(total) if total else None,
                provider_hint=provider or "",
                saved_at=now,
            )
        )
    if not candidates:
        return 0

    merged: dict[tuple[str, str], EndpointRecipe] = {r.key(): r for r in load_recipes(dealer_id)}
    for c in candidates:
        cur = merged.get(c.key())
        # A fresh capture always wins: newer auth headers, un-stales the entry.
        if cur is None or not cur.last_ok_at or cur.stale or c.vehicle_rows >= cur.vehicle_rows:
            c.last_ok_at = cur.last_ok_at if cur else 0.0
            merged[c.key()] = c
    ranked = sorted(merged.values(), key=lambda r: (r.stale, -(r.total_count or 0), -r.vehicle_rows))
    keep = ranked[:max_recipes]
    save_recipes(dealer_id, keep)
    logger.info(
        "Recipes [%s]: %d endpoint(s) promoted (%d candidate(s) this scan)",
        dealer_id, len(keep), len(candidates),
    )
    return len(keep)


# ── Replay ───────────────────────────────────────────────────────────────────


def recipe_fetch_enabled() -> bool:
    raw = (os.environ.get("SCANNER_RECIPE_FETCH") or "").strip().lower()
    return raw not in ("0", "false", "no", "off")


def recipe_min_vehicles() -> int:
    try:
        return max(1, int(os.environ.get("SCANNER_RECIPE_MIN_VEHICLES") or 10))
    except ValueError:
        return 10


_MAX_REPLAY_PAGES = 40
_REPLAY_TIMEOUT_S = 20.0


def _mutate_for_page(recipe: EndpointRecipe, template: Any, page_index: int) -> Any:
    """Return the request body for 0-based *page_index* per the recipe's pagination shape."""
    body = copy.deepcopy(template)
    if recipe.pagination == PAGINATION_CARSCOMMERCE and isinstance(body, dict):
        body["page"] = page_index + 1
    elif recipe.pagination == PAGINATION_TYPESENSE and isinstance(body, dict):
        for s in body.get("searches") or []:
            if isinstance(s, dict):
                s["page"] = page_index + 1
    elif recipe.pagination == PAGINATION_DEALER_COM and isinstance(body, dict):
        prefs = body.get("preferences") or {}
        try:
            page_size = int(str(prefs.get("pageSize") or 20))
        except (TypeError, ValueError):
            page_size = 20
        params = body.setdefault("inventoryParameters", {})
        if isinstance(params, dict):
            params["start"] = [str(page_index * page_size)]
    return body


def _replay_request(recipe: EndpointRecipe, body: Any, base_url: str) -> tuple[int, Any | None]:
    """One synchronous HTTP call. Returns (status, parsed_json_or_None)."""
    import requests

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Origin": base_url.rstrip("/"),
        "Referer": base_url.rstrip("/") + "/",
        **recipe.auth_headers,
    }
    try:
        if recipe.method == "GET":
            resp = requests.get(recipe.url, headers=headers, timeout=_REPLAY_TIMEOUT_S)
        else:
            headers["Content-Type"] = "application/json"
            resp = requests.request(
                recipe.method, recipe.url, headers=headers,
                data=json.dumps(body) if body is not None else None,
                timeout=_REPLAY_TIMEOUT_S,
            )
    except requests.RequestException as e:
        logger.debug("Recipe replay request failed (%s): %s", recipe.url[:80], str(e)[:150])
        return 0, None
    if resp.status_code != 200:
        return resp.status_code, None
    try:
        parsed = resp.json()
    except ValueError:
        return resp.status_code, None
    return resp.status_code, parsed if isinstance(parsed, (dict, list)) else None


def _unique_vins(vehicles: list[dict]) -> set[str]:
    return {v.get("vin", "").strip().upper() for v in vehicles if (v.get("vin") or "").strip()}


def last_known_vin_count(dealer_id: str) -> int:
    """Distinct VINs currently in the DB for *dealer_id* (0 on any failure)."""
    try:
        from backend.db.inventory_pg import is_inventory_postgres, pg_connect

        if not is_inventory_postgres():
            return 0
        conn = pg_connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT count(DISTINCT vin) FROM cars WHERE dealer_id = %s", (dealer_id,))
            return int(cur.fetchone()[0])
        finally:
            conn.close()
    except Exception:
        return 0


async def try_fetch_via_recipes(
    dealer_id: str,
    provider: str,
    base_url: str,
    dealer_name: str,
) -> tuple[list[tuple[str, Any]], int] | None:
    """
    Replay this dealer's stored recipes over plain HTTP (pre-Playwright).

    Returns ``(records, unique_vin_count)`` — intercept-shaped ``[(url, body), ...]``
    plus the VIN yield — when a recipe returns at least ``recipe_min_vehicles()``
    unique parseable VINs; ``None`` otherwise. The caller decides whether the yield
    is complete enough to skip the browser scrape (a recipe captured on one listing
    config can be type-filtered and cover only part of the lot).
    401/403 responses mark the recipe stale so the browser pass re-captures auth.
    """
    if not recipe_fetch_enabled():
        return None
    recipes = [r for r in load_recipes(dealer_id) if not r.stale]
    if not recipes:
        return None
    from backend.parsers import parse

    min_vehicles = recipe_min_vehicles()
    for recipe in recipes:
        template: Any = None
        if recipe.post_template:
            try:
                template = json.loads(recipe.post_template)
            except ValueError:
                template = None
        if recipe.method != "GET" and template is None:
            continue  # truncated/unparseable POST sample — can't replay safely
        records: list[tuple[str, Any]] = []
        vins: set[str] = set()
        pages = 1 if recipe.pagination == PAGINATION_NONE else _MAX_REPLAY_PAGES
        auth_dead = False
        for page_i in range(pages):
            body = _mutate_for_page(recipe, template, page_i) if template is not None else None
            status, parsed = await asyncio.to_thread(_replay_request, recipe, body, base_url)
            if status in (401, 403):
                if not vins:
                    # Failed before collecting anything — the recipe's auth is dead.
                    mark_stale(dealer_id, recipe, f"http_{status}")
                    logger.info(
                        "Recipe stale [%s] %s — HTTP %d (auth rotated?); browser scan will re-capture",
                        dealer_name, recipe.url[:80], status,
                    )
                    auth_dead = True
                else:
                    # Mid-pagination — likely a rate limit / page boundary, not dead
                    # auth. Keep the VINs already collected instead of discarding them.
                    logger.info(
                        "Recipe [%s] %s — HTTP %d after %d page(s); keeping %d VIN(s)",
                        dealer_name, recipe.url[:80], status, page_i, len(vins),
                    )
                break
            if parsed is None:
                break
            page_vehicles = list(parse(
                recipe.provider_hint or provider, parsed,
                base_url=base_url, dealer_id=dealer_id,
                dealer_name=dealer_name, dealer_url=base_url,
            ))
            new = _unique_vins(page_vehicles) - vins
            records.append((recipe.url, parsed))
            if not new:
                break
            vins |= new
            if recipe.total_count and len(vins) >= recipe.total_count:
                break
        if auth_dead:
            continue
        if len(vins) >= min_vehicles:
            recipe.last_ok_at = time.time()
            all_r = load_recipes(dealer_id)
            for r in all_r:
                if r.key() == recipe.key():
                    r.last_ok_at = recipe.last_ok_at
            save_recipes(dealer_id, all_r)
            logger.info(
                "Recipe fetch [%s]: %d unique VIN(s) from %d page(s) via %s",
                dealer_name, len(vins), len(records), recipe.url[:80],
            )
            return records, len(vins)
    return None
