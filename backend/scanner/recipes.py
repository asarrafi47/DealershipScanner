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
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

logger = logging.getLogger("scanner")

RECIPES_DIR = Path("workspace") / "recipes"

# Pagination shapes we know how to walk during replay.
PAGINATION_CARSCOMMERCE = "carscommerce_page"   # POST body {"page": N, "perPage": M}
PAGINATION_TYPESENSE = "typesense_page"          # POST body searches[].page / per_page
PAGINATION_DEALER_COM = "dealer_com_start"       # POST body inventoryParameters.start
PAGINATION_PAGE_QUERY = "page_query"             # GET URL ?page=N (Team Velocity JSON feed)
PAGINATION_DEP_SRP = "dep_srp_page"              # GET URL ?p=N (Dealer eProcess server-rendered SRP w/ JSON-LD)
PAGINATION_ALGOLIA = "algolia_page"              # POST body {"page": N (0-based), "hitsPerPage": M} (Motive/ridemotive Algolia)
PAGINATION_HTML_PAGE = "html_page_query"         # GET URL ?page=N returning HTML (Overfuel __NEXT_DATA__, nabthat JSON-LD SRP)
PAGINATION_JAZEL_SRP = "jazel_srp_page"          # GET path-walk .../srp-page-N/ returning HTML (Jazel SSR SRP)
PAGINATION_COSMOS_PT = "cosmos_pt"               # GET URL ?pt=N&pn=96 (DealerOn cosmos SRP JSON; Paging.PaginationDataModel)
PAGINATION_GRAPHQL_OFFSET = "graphql_offset"    # POST GraphQL body variables.sp.paging.{offset,limit} (OneAudi omnigraph stockCarSearch)
PAGINATION_NONE = "none"                         # single-shot GET/POST


def _recipe_section_discriminator(post_template: Any) -> str:
    """A stable tag distinguishing inventory *sections* served by the SAME
    endpoint URL.

    Dealer.com calls one ``ws-inv-data`` URL with different POST bodies for
    new / used / certified inventory (``pageAlias`` /
    ``preferences["listing.config.id"]``). Without this tag in the recipe key,
    those bodies collapse to a single recipe and every section but one is
    silently dropped at promotion — the HTTP replay then misses whole slices of
    the lot (e.g. all NEW cars), which shows up downstream as a permanently
    partial delta feed. Returns "" for platforms that do not split inventory by
    request body (CarsCommerce, Typesense, GET feeds), so their keys are
    unchanged and there is no regression.

    Only section-stable fields are used (never the per-page ``start``/``page``
    params), so every page of one section shares a key.
    """
    if not post_template:
        return ""
    body = post_template
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except (TypeError, ValueError):
            return ""
    if not isinstance(body, dict):
        return ""
    alias = str(body.get("pageAlias") or "").strip()
    if not alias:
        prefs = body.get("preferences")
        if isinstance(prefs, dict):
            alias = str(prefs.get("listing.config.id") or "").strip()
    return f"#{alias}" if alias else ""


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
    # price / trim / exterior_color fractions: from the network ledger at capture,
    # refreshed from parsed rows on every successful replay.
    field_coverage: dict[str, float] = field(default_factory=dict)

    def key(self) -> tuple[str, str]:
        p = urlparse(self.url)
        base = f"{(p.hostname or '').lower()}{p.path}"
        if self.provider_hint == "html_cards" and self.pagination == PAGINATION_HTML_PAGE and p.query:
            # one XHR path serves every condition, section in the query (PixelMotion)
            return (self.method, f"{base}?{html_page_section_key(self.url)}")
        # Dealer.com serves new/used/certified from ONE URL via different bodies;
        # discriminate them so all sections persist as distinct recipes.
        return (self.method, f"{base}{_recipe_section_discriminator(self.post_template)}")


def infer_pagination(url: str, post_template: str | None) -> str:
    host = (urlparse(url).hostname or "").lower()
    body = post_template or ""
    if "carscommerce" in host and '"page"' in body:
        return PAGINATION_CARSCOMMERCE
    if "typesense" in host:
        return PAGINATION_TYPESENSE
    if "ws-inv-data" in url and "inventoryParameters" in body:
        return PAGINATION_DEALER_COM
    if "/cosmos/srp/vehicles/" in url:
        return PAGINATION_COSMOS_PT
    return PAGINATION_NONE


def _recipe_slug(dealer_id: str) -> str:
    return re.sub(r"[^a-z0-9_-]+", "-", (dealer_id or "unknown").lower()).strip("-") or "unknown"


def _recipe_path(dealer_id: str) -> Path:
    return RECIPES_DIR / f"{_recipe_slug(dealer_id)}.json"


# Dealer identity is a pure function of the site URL (dev.dealers.slug_from_url),
# so a hostname change (aaronfordofescondido.com -> .org) mints a NEW dealer id
# and strands the recipes saved under the old one. ``_aliases.json`` bridges
# those rekeys: a flat ``{old_slug: current_dealer_id}`` map kept next to the
# recipe files. READS consult it (file loads here; DB loads in
# ``recipe_store``); WRITES never do — they always target the current slug, so
# a rekeyed dealer's next save migrates its content forward naturally. The
# underscore prefix keeps the file out of ``*.json`` dealer listings (see
# ``import_recipes_to_db`` / ``heal_from_recipes``).
ALIASES_FILENAME = "_aliases.json"


def _load_recipe_aliases() -> dict[str, str]:
    """``{old_slug: current_dealer_id}`` from ``_aliases.json``.

    Failure-tolerant by design: a missing, unreadable, or corrupt alias file —
    or one that is not a flat string->string object — simply means no aliasing.
    """
    try:
        raw = json.loads((RECIPES_DIR / ALIASES_FILENAME).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(raw, dict):
        return {}
    return {
        _recipe_slug(k): v.strip()
        for k, v in raw.items()
        if isinstance(k, str) and isinstance(v, str) and k.strip() and v.strip()
    }


def resolve_alias_slug(dealer_id: str) -> str | None:
    """The retired slug that may still hold *dealer_id*'s recipes, or ``None``.

    Reverse lookup over the alias map: returns the old slug whose entry points
    at *dealer_id*'s current slug. Self-referential entries are ignored.
    """
    cur = _recipe_slug(dealer_id)
    for old_slug, new_id in _load_recipe_aliases().items():
        if old_slug != cur and _recipe_slug(new_id) == cur:
            return old_slug
    return None


def _recipe_read_path(dealer_id: str) -> Path:
    """Path to READ recipes from: the current slug's file when it exists,
    else an alias-mapped retired slug's file. Writes must keep using
    ``_recipe_path`` (always the current slug) so rekeyed content migrates
    forward on the next save instead of resurrecting the old identity."""
    path = _recipe_path(dealer_id)
    if path.exists():
        return path
    old_slug = resolve_alias_slug(dealer_id)
    if old_slug:
        aliased = RECIPES_DIR / f"{old_slug}.json"
        if aliased.exists():
            return aliased
    return path


def _rows_to_recipes(raw: Any) -> list[EndpointRecipe]:
    out: list[EndpointRecipe] = []
    for row in raw if isinstance(raw, list) else []:
        try:
            rec = EndpointRecipe(**{k: v for k, v in row.items()
                                    if k in EndpointRecipe.__dataclass_fields__})
        except TypeError:
            continue
        if rec.pagination == PAGINATION_NONE:
            # Files written before a pagination shape was known stay single-shot
            # forever otherwise (cosmos recipes replayed page 1 of 2+ for months).
            rec.pagination = infer_pagination(rec.url, rec.post_template)
        out.append(rec)
    return out


def load_recipes(dealer_id: str) -> list[EndpointRecipe]:
    """
    Local file first, reconciled against the shared ``dealer_recipes`` table
    (see ``backend.scanner.recipe_store``): a newer DB copy (captured on
    another machine) wins and re-materializes the file; a newer file (written
    by a scan that predates the DB store) is lazily pushed up. Either side
    being unavailable degrades to the other.
    """
    # Read may resolve through _aliases.json (URL-rekeyed dealer); any write
    # below targets the CURRENT slug so the content migrates forward.
    read_path = _recipe_read_path(dealer_id)
    file_rows: list[dict] = []
    try:
        raw = json.loads(read_path.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            file_rows = raw
    except (OSError, ValueError):
        pass

    db_rows: list[dict] = []
    db_saved = -1.0
    try:
        from backend.scanner.recipe_store import db_load_recipes

        found = db_load_recipes(_recipe_slug(dealer_id))
        if found is not None:
            db_rows, db_saved = found
    except Exception:  # noqa: BLE001 — DB is optional here
        pass

    def _max_saved(rows: list[dict]) -> float:
        vals = [float(r.get("saved_at") or 0) for r in rows if isinstance(r, dict)]
        return max(vals) if vals else 0.0

    file_saved = _max_saved(file_rows) if file_rows else -1.0
    if db_rows and db_saved > file_saved:
        # Another machine captured fresher recipes — adopt and cache locally
        # (under the current slug, even when the read came from an alias).
        try:
            RECIPES_DIR.mkdir(parents=True, exist_ok=True)
            _recipe_path(dealer_id).write_text(json.dumps(db_rows, indent=1), encoding="utf-8")
        except OSError:
            pass
        return _rows_to_recipes(db_rows)
    if file_rows and file_saved > db_saved:
        try:
            from backend.scanner.recipe_store import db_save_recipes

            db_save_recipes(_recipe_slug(dealer_id), file_rows)
        except Exception:  # noqa: BLE001
            pass
    return _rows_to_recipes(file_rows or db_rows)


def save_recipes(dealer_id: str, recipes: list[EndpointRecipe]) -> None:
    RECIPES_DIR.mkdir(parents=True, exist_ok=True)
    path = _recipe_path(dealer_id)
    rows = [asdict(r) for r in recipes]
    path.write_text(json.dumps(rows, indent=1), encoding="utf-8")
    try:
        from backend.scanner.recipe_store import db_save_recipes

        db_save_recipes(_recipe_slug(dealer_id), rows)
    except Exception:  # noqa: BLE001 — file write is the contract; DB is best-effort
        pass


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
    max_recipes: int = 8,
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
        ctype = str(getattr(ep, "content_type", "") or "")
        hint = provider or ""
        pagination = infer_pagination(url, post)
        if str(getattr(ep, "reason", "") or "") == "html_fragment_cards":
            # NetworkObserver ledgered an HTML fragment (PixelMotion / server-rendered
            # card pagers) rather than JSON: replay hands the text to the html_cards
            # parser and walks ?page=N until a page adds no VIN (mcpeeks-com, 2026-09-26).
            hint = "html_cards"
            if pagination == PAGINATION_NONE and not post:
                pagination = PAGINATION_HTML_PAGE
        candidates.append(
            EndpointRecipe(
                dealer_id=dealer_id,
                url=url,
                method=str(getattr(ep, "method", "GET") or "GET").upper(),
                content_type=ctype,
                post_template=post,
                auth_headers=dict(getattr(ep, "auth_headers", None) or {}),
                pagination=pagination,
                vehicle_rows=rows,
                total_count=int(total) if total else None,
                provider_hint=hint,
                saved_at=now,
                field_coverage={
                    k: float(v) for k, v in (getattr(ep, "field_coverage", None) or {}).items()
                },
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
            if cur is not None and not c.field_coverage and cur.field_coverage:
                c.field_coverage = dict(cur.field_coverage)
            merged[c.key()] = c
    # Rich feeds (price + trim/colour) replay first; VIN-only endpoints stay on
    # file for delta VIN-set work but can never outrank a real inventory feed.
    ranked = sorted(
        merged.values(),
        key=lambda r: (
            r.stale,
            0 if recipe_coverage_is_rich(r.field_coverage) else 1,
            -(r.total_count or 0),
            -r.vehicle_rows,
        ),
    )
    keep = ranked[:max_recipes]
    save_recipes(dealer_id, keep)
    thin = sum(1 for r in keep if r.field_coverage and not recipe_coverage_is_rich(r.field_coverage))
    logger.info(
        "Recipes [%s]: %d endpoint(s) promoted (%d candidate(s) this scan, %d thin: VINs without price/trim/colour)",
        dealer_id, len(keep), len(candidates), thin,
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


# facetFilters keys that select an SRP section (condition / make / model / body),
# never a rooftop; a whole-lot replay must not carry them
_CC_SECTION_FACETS = frozenset({"type_slug", "type", "make", "model_slug", "model", "body_type", "trim_slug", "year",
                                "fuel_type", "transmission", "exterior_color_generic", "features", "certified",
                                "special_use_category", "condition", "in_transit", "is_special", "drivetrain", "price_range"})


def _learn_dealer_com_page_size(template: Any, parsed: Any, dealer_name: str = "") -> int:
    """Copy the page size the dealer.com server actually used into the walk's
    template. The captured preferences say pageSize 500; the server answers 48
    and ignores the preference, so page 2 started at 500 — past the end — and 14
    dealers of the 2026-09-24 fleet run stopped at exactly 48 rows (Village VW:
    157 cars, Mtn View Ford: 436). Returns the size learned, or 0."""
    if not isinstance(template, dict) or not isinstance(parsed, dict):
        return 0
    info = parsed.get("pageInfo")
    try:
        real = int((info or {}).get("pageSize") or 0) if isinstance(info, dict) else 0
    except (TypeError, ValueError):
        real = 0
    if real <= 0:
        return 0
    prefs = template.setdefault("preferences", {})
    if isinstance(prefs, dict) and str(prefs.get("pageSize") or "") != str(real):
        logger.info("Recipe [%s] dealer.com pageSize %s -> %d (the server's)", dealer_name, prefs.get("pageSize"), real)
        prefs["pageSize"] = str(real)
    return real


def recipe_is_section_scoped(recipe: EndpointRecipe) -> bool:
    """True for a captured CarsCommerce recipe whose facetFilters pin one SRP
    section (used-only / new-only / one make): it can never yield the lot."""
    if recipe.pagination != PAGINATION_CARSCOMMERCE or not recipe.post_template:
        return False
    try:
        body = json.loads(recipe.post_template)
    except ValueError:
        return False
    ff = body.get("facetFilters") if isinstance(body, dict) else None
    return isinstance(ff, dict) and any(k in _CC_SECTION_FACETS for k in ff)


def recipe_is_store_scoped(recipe: EndpointRecipe, dealer_name: str = "") -> bool:
    """True when the request itself selects this store, so every row it returns
    is this store's by construction: CarsCommerce ``facetFilters.source_id``
    (the store's own feed ids), or a single facet value that IS the store's name
    (the site's Location facet, "Group 1 Toyota North Austin"). Both are set and
    replay-verified by ``recipe_synth._carscommerce_store_filter``; the API
    rejects unknown body keys, so the scope cannot be marked any other way."""
    if recipe.pagination != PAGINATION_CARSCOMMERCE or not recipe.post_template:
        return False
    try:
        body = json.loads(recipe.post_template)
    except ValueError:
        return False
    ff = body.get("facetFilters") if isinstance(body, dict) else None
    if not isinstance(ff, dict):
        return False
    ids = ff.get("source_id")
    if isinstance(ids, list) and ids:
        return True
    nrm = lambda s: re.sub(r"[^a-z0-9]", "", str(s or "").lower())  # noqa: E731
    own = {nrm(dealer_name), nrm(re.sub(r"-(com|net|org)$", "", recipe.dealer_id or ""))} - {""}
    for facet, vals in ff.items():
        if facet.startswith("custom_text_") and isinstance(vals, list) and len(vals) == 1 and nrm(vals[0]) in own:
            return True
    return False


def _mutate_for_page(recipe: EndpointRecipe, template: Any, page_index: int) -> Any:
    """Return the request body for 0-based *page_index* per the recipe's pagination shape."""
    body = copy.deepcopy(template)
    if recipe.pagination == PAGINATION_CARSCOMMERCE and isinstance(body, dict):
        body["page"] = page_index + 1
        # The browser often captures a widget's query, not the lot: Culver City
        # Toyota's recipe carried facetFilters {"custom_text_1": ["true"]} (a
        # "featured" flag) and replayed 30 of 529 cars. Boolean facet flags narrow
        # to a carousel; drop them. Section filters (type_slug etc.) live under
        # "filters" and are kept. perPage 100 is accepted and cuts the walk 5x.
        ff = body.get("facetFilters")
        if isinstance(ff, dict):
            # A browser capture is one SRP SECTION's query: 16 of the 44 no_rows
            # verdicts of the 2026-09-24 fleet run were carscommerce recipes with
            # facetFilters type_slug=["Certified Used"] / ["New"] + make + model_slug
            # (Lindsay Honda 109 used of 577, Germain Toyota 65 new of 401). Those
            # keys describe inventory sections, never the store; drop them. Store
            # identity keys (source_id, custom_text_N) are kept.
            kept = {
                k: v for k, v in ff.items()
                if k not in _CC_SECTION_FACETS
                and not (isinstance(v, list) and v and all(str(x).lower() in ("true", "false", "1", "0") for x in v))
            }
            if kept:
                body["facetFilters"] = kept
            else:
                body.pop("facetFilters", None)
        flt = body.get("filters")
        if isinstance(flt, dict) and any(k in _CC_SECTION_FACETS for k in flt):
            # same section keys under "filters" (a used-SRP capture writes
            # filters.type_slug=["used"]); "status" stays
            body["filters"] = {k: v for k, v in flt.items() if k not in _CC_SECTION_FACETS}
        try:
            if int(body.get("perPage") or 0) < 100:
                body["perPage"] = 100
        except (TypeError, ValueError):
            body["perPage"] = 100
    elif recipe.pagination == PAGINATION_TYPESENSE and isinstance(body, dict):
        for s in body.get("searches") or []:
            if isinstance(s, dict):
                s["page"] = page_index + 1
    elif recipe.pagination == PAGINATION_DEALER_COM and isinstance(body, dict):
        prefs = body.get("preferences") or {}
        # Do NOT strip preferences["listing.config.id"]: without it dealer.com
        # returns the lot but omits the VIN from every item (Lexus of Austin: 48
        # parsed as-is, 0 stripped; three no_rows verdicts on the 2026-09-26 mini
        # check). Camelback Toyota's certified-only config stays open (its widget
        # carries no VIN either way; workspace/pipeline/needs_work.txt).
        try:
            page_size = int(str(prefs.get("pageSize") or 20))
        except (TypeError, ValueError):
            page_size = 20
        params = body.setdefault("inventoryParameters", {})
        if isinstance(params, dict):
            params["start"] = [str(page_index * page_size)]
    elif recipe.pagination == PAGINATION_ALGOLIA and isinstance(body, dict):
        # Algolia pages are 0-based (page 0 is the first page of results).
        body["page"] = page_index
    elif recipe.pagination == PAGINATION_GRAPHQL_OFFSET and isinstance(body, dict):
        # OneAudi: {"query": ..., "variables": {"sp": {"paging": {"limit": 48, "offset": N}}}}
        sp = (body.get("variables") or {}).get("sp")
        if isinstance(sp, dict):
            paging = sp.setdefault("paging", {})
            if isinstance(paging, dict):
                try:
                    limit = int(paging.get("limit") or 48)
                except (TypeError, ValueError):
                    limit = 48
                paging["limit"] = limit
                paging["offset"] = page_index * limit
    return body


_HTML_PAGE_PARAMS = ("page", "inv_page", "paged", "pg", "p")


def _html_page_param(query: str) -> str:
    """Name of the page parameter an HTML pager URL already carries, else ``page``."""
    keys = [k for k, _ in parse_qsl(query or "", keep_blank_values=True)]
    for cand in _HTML_PAGE_PARAMS:
        if cand in keys:
            return cand
    return "page"


def html_page_section_key(url: str) -> str:
    """Query string of an HTML-page recipe minus its page parameter: the section
    (PixelMotion ``condition[]=new`` vs ``used``) that one endpoint path serves."""
    parts = urlparse(url)
    page_param = _html_page_param(parts.query)
    return urlencode(sorted((k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != page_param and k != "newUrl"))


def _url_for_page(recipe: EndpointRecipe, page_index: int) -> str:
    """Per-page request URL for 0-based *page_index*.

    ``PAGINATION_PAGE_QUERY`` / ``PAGINATION_HTML_PAGE`` set ``?page=N`` and
    ``PAGINATION_DEP_SRP`` sets ``?p=N`` (all 1-based); ``PAGINATION_JAZEL_SRP``
    walks a path segment ``.../srp-page-N/`` (page 1 is the bare SRP URL); every
    other pagination shape paginates via the request body, so the URL is returned
    unchanged.
    """
    if recipe.pagination == PAGINATION_JAZEL_SRP:
        # Page 1 is the bare all-vehicles SRP; page N>=2 is .../srp-page-N/.
        if page_index == 0:
            return recipe.url
        base = recipe.url.rstrip("/")
        return f"{base}/srp-page-{page_index + 1}/"
    if recipe.pagination not in (PAGINATION_PAGE_QUERY, PAGINATION_DEP_SRP, PAGINATION_HTML_PAGE, PAGINATION_COSMOS_PT):
        return recipe.url
    page_param = {PAGINATION_DEP_SRP: "p", PAGINATION_COSMOS_PT: "pt"}.get(recipe.pagination, "page")
    parts = urlparse(recipe.url)
    if recipe.pagination == PAGINATION_HTML_PAGE:
        # The captured URL names its own page parameter when the pager is an
        # XHR (PixelMotion: inv_page=2, newUrl=/inventory/new/?inv_page=2);
        # appending ?page=N to that URL returns page 1 forever (mcpeeks-com).
        page_param = _html_page_param(parts.query)
    drop = {page_param, "pn"} if recipe.pagination == PAGINATION_COSMOS_PT else {page_param}
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k not in drop]
    if recipe.pagination == PAGINATION_HTML_PAGE:
        query = [(k, re.sub(r"(?<=[?&]){}=\d+".format(re.escape(page_param)), f"{page_param}={page_index + 1}", v)) for k, v in query]
    query.append((page_param, str(page_index + 1)))
    if recipe.pagination == PAGINATION_COSMOS_PT:
        # pn = page size, 96 is the server maximum (verified 2026-09-23 on Cherokee
        # County Toyota: 282 cars = 3 pages of 96 instead of 24 of 12).
        query.append(("pn", "96"))
    return urlunparse(parts._replace(query=urlencode(query)))


# Statuses a WAF returns when it dislikes the TLS handshake rather than the request.
# Cloudflare and Akamai use 403; DataDome and some Akamai configs use 405/429.
_TLS_FINGERPRINT_STATUSES = (403, 405, 429)


def _replay_impersonated(
    recipe: EndpointRecipe, req_url: str, headers: dict[str, str], payload: str | None
) -> tuple[int, Any | None]:
    """
    Replay one request with a real browser's TLS fingerprint.

    Returns ``(0, None)`` when curl_cffi is not installed, so this stays a pure
    enhancement: without it the caller keeps the status the plain client already got.

    Profiles are rotated because edges run different rule sets -- across a 30-dealer
    sample "chrome" alone cleared about half while chrome/chrome124/safari17_0 together
    cleared 29 of 30.
    """
    try:
        from curl_cffi import requests as cffi_requests
    except ImportError:
        return 0, None

    for profile in ("chrome", "chrome124", "safari17_0"):
        try:
            if recipe.method == "GET":
                resp = cffi_requests.get(
                    req_url, headers=headers, impersonate=profile, timeout=_REPLAY_TIMEOUT_S
                )
            else:
                resp = cffi_requests.request(
                    recipe.method, req_url, headers=headers, data=payload,
                    impersonate=profile, timeout=_REPLAY_TIMEOUT_S,
                )
        except Exception as exc:  # curl_cffi raises its own error hierarchy
            logger.debug("impersonated replay %s failed: %s", profile, str(exc)[:120])
            continue
        if resp.status_code != 200:
            continue
        logger.info("recipe replay cleared via TLS impersonation (%s): %s", profile, req_url[:70])
        if recipe.pagination in (PAGINATION_DEP_SRP, PAGINATION_HTML_PAGE, PAGINATION_JAZEL_SRP) or recipe.provider_hint == "html_cards":
            return resp.status_code, resp.text or None
        try:
            parsed = resp.json()
        except Exception:
            return resp.status_code, None
        return resp.status_code, parsed if isinstance(parsed, (dict, list)) else None
    return 0, None


def _replay_request(
    recipe: EndpointRecipe, body: Any, base_url: str, url: str | None = None
) -> tuple[int, Any | None]:
    """One synchronous HTTP call. Returns (status, parsed_json_or_None).

    *url* overrides ``recipe.url`` for this call (used to walk ``?page=N``
    pagination); defaults to ``recipe.url``.
    """
    import requests

    req_url = url or recipe.url
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
    if recipe.method != "GET":
        headers["Content-Type"] = "application/json"
    payload = json.dumps(body) if body is not None else None

    try:
        if recipe.method == "GET":
            resp = requests.get(req_url, headers=headers, timeout=_REPLAY_TIMEOUT_S)
        else:
            resp = requests.request(
                recipe.method, req_url, headers=headers,
                data=payload, timeout=_REPLAY_TIMEOUT_S,
            )
    except requests.RequestException as e:
        logger.debug("Recipe replay request failed (%s): %s", recipe.url[:80], str(e)[:150])
        return _replay_impersonated(recipe, req_url, headers, payload)

    if resp.status_code in _TLS_FINGERPRINT_STATUSES:
        # The edge rejected the handshake, not the request. Retry with a real browser's
        # TLS signature before treating this as a dead recipe.
        #
        # This is the seam that made previously-saved recipes useless: impersonation had
        # been added to the synthesis and validation helpers, so a recipe would be built,
        # validated against live inventory and stored -- and then 403 the first time a
        # real scan replayed it here, which also flips it stale. Recipe capture and recipe
        # replay have to clear the same edge.
        status, parsed = _replay_impersonated(recipe, req_url, headers, payload)
        if status == 200:
            return status, parsed
        return resp.status_code, None

    if resp.status_code != 200:
        return resp.status_code, None
    # Some platforms serve inventory as server-rendered HTML (no JSON API): Dealer
    # eProcess / nabthat embed per-vehicle JSON-LD, Overfuel embeds __NEXT_DATA__,
    # and Jazel embeds per-card jzlSetVehicleInfoContext() calls. Hand the raw HTML
    # text to the provider parser instead of attempting resp.json().
    if recipe.pagination in (PAGINATION_DEP_SRP, PAGINATION_HTML_PAGE, PAGINATION_JAZEL_SRP) or recipe.provider_hint == "html_cards":
        return resp.status_code, resp.text or None
    try:
        parsed = resp.json()
    except ValueError:
        return resp.status_code, None
    return resp.status_code, parsed if isinstance(parsed, (dict, list)) else None


def _unique_vins(vehicles: list[dict]) -> set[str]:
    return {v.get("vin", "").strip().upper() for v in vehicles if (v.get("vin") or "").strip()}


# Fields a recipe replay must actually carry before it is allowed to REPLACE the
# browser scrape. VIN count alone is the wrong yardstick: on 2026-08-04 a fleet
# capture-only run accepted endpoints like Team Velocity's GetKeyFeaturesByVins
# and Gatsby page-data blobs because they listed VINs, then the 70%-of-known-VINs
# rule suppressed the SRP scrape, and 67k rows landed with no trim, price or
# colour. Every one of those fields lives on the VDP for those platforms.
_COVERAGE_FIELDS = ("price", "trim", "exterior_color")


def _field_present(v: dict, field: str) -> bool:
    val = v.get(field)
    if val is None:
        return False
    if field == "price":
        try:
            return float(val) > 0
        except (TypeError, ValueError):
            return False
    return bool(str(val).strip())


def recipe_field_coverage(vehicles: list[dict]) -> dict[str, float]:
    """Fraction of parsed vehicles carrying each of ``_COVERAGE_FIELDS``.

    ``{"n": <vehicle count>, "price": 0.0-1.0, "trim": ..., "exterior_color": ...}``.
    An empty list yields ``n=0`` and zero coverage everywhere.
    """
    n = len(vehicles)
    out: dict[str, float] = {"n": float(n)}
    for f in _COVERAGE_FIELDS:
        out[f] = (sum(1 for v in vehicles if _field_present(v, f)) / n) if n else 0.0
    return out


def recipe_min_price_coverage() -> float:
    try:
        return min(1.0, max(0.0, float(os.environ.get("SCANNER_RECIPE_MIN_PRICE_COVERAGE") or 0.5)))
    except ValueError:
        return 0.5


def recipe_min_field_coverage() -> float:
    try:
        return min(1.0, max(0.0, float(os.environ.get("SCANNER_RECIPE_MIN_FIELD_COVERAGE") or 0.5)))
    except ValueError:
        return 0.5


def recipe_coverage_is_rich(coverage: dict[str, float] | None) -> bool:
    """True when a recipe's rows look like an inventory feed, not a VIN list."""
    cov = coverage or {}
    price = float(cov.get("price", 0.0))
    listing = max(float(cov.get("trim", 0.0)), float(cov.get("exterior_color", 0.0)))
    return price >= recipe_min_price_coverage() and listing >= recipe_min_field_coverage()


def recipe_yield_replaces_browser(
    recipe_vins: int, known_vins: int, coverage: dict[str, float] | None
) -> tuple[bool, str]:
    """Decide whether a recipe replay may stand in for the browser SRP scrape.

    Both bars must clear: VIN yield >= 70% of the last known lot (the historical
    rule), AND the parsed rows carry a price on at least
    ``SCANNER_RECIPE_MIN_PRICE_COVERAGE`` of vehicles and a trim OR exterior colour
    on at least ``SCANNER_RECIPE_MIN_FIELD_COVERAGE``. Returns ``(ok, reason)``;
    the reason is logged so a "partial" verdict says which bar failed.
    """
    if known_vins <= 0:
        return False, "no_known_lot"
    if recipe_vins < int(0.7 * known_vins):
        return False, f"vins {recipe_vins}/{known_vins} < 70%"
    cov = coverage or {}
    price = float(cov.get("price", 0.0))
    listing = max(float(cov.get("trim", 0.0)), float(cov.get("exterior_color", 0.0)))
    if price < recipe_min_price_coverage():
        return False, f"price coverage {price:.0%} < {recipe_min_price_coverage():.0%}"
    if listing < recipe_min_field_coverage():
        return False, f"trim/colour coverage {listing:.0%} < {recipe_min_field_coverage():.0%}"
    return True, "ok"


def last_known_vin_count(dealer_id: str) -> int:
    """Distinct VINs currently LISTED for *dealer_id* (0 on any failure).

    Active rows only: counting retired rows too made the 70% browser-skip gate
    unreachable right after a reconcile (Hiley VW 2026-09-22: 338 feed VINs vs
    "624 known" of which 286 had just been unlisted), so a rich feed re-ran the
    browser and every store's gate drifted with its sales history."""
    try:
        from backend.db.inventory_pg import is_inventory_postgres, pg_connect

        if not is_inventory_postgres():
            return 0
        conn = pg_connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT count(DISTINCT vin) FROM cars WHERE dealer_id = %s "
                "AND listing_removed_at IS NULL AND COALESCE(listing_active, 1) = 1",
                (dealer_id,),
            )
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
    *,
    union: bool = False,
    coverage_out: dict[str, float] | None = None,
) -> tuple[list[tuple[str, Any]], int] | None:
    """
    Replay this dealer's stored recipes over plain HTTP (pre-Playwright).

    When ``coverage_out`` is a dict it is filled (in place) with
    :func:`recipe_field_coverage` over the parsed vehicles behind the returned
    records, so the caller can judge whether the replay is complete enough to
    REPLACE the browser scrape, not merely whether it listed enough VINs.

    Returns ``(records, unique_vin_count)`` — intercept-shaped ``[(url, body), ...]``
    plus the VIN yield — when a recipe returns at least ``recipe_min_vehicles()``
    unique parseable VINs; ``None`` otherwise. The caller decides whether the yield
    is complete enough to skip the browser scrape (a recipe captured on one listing
    config can be type-filtered and cover only part of the lot).
    401/403 responses mark the recipe stale so the browser pass re-captures auth.

    ``union=True`` replays EVERY non-stale recipe and returns the combined
    yield: a dealer's recipes are often per-listing-config (new / used / CPO
    pages), so the first passing recipe alone can cover only part of the lot.
    Delta scans want the union; the pre-browser scan fast path keeps the
    cheaper first-hit behavior.
    """
    # This store's postal address, for the rooftop gate.
    #
    # Without it, a group feed refuses EVERY row. That is not hypothetical: synthesising a
    # recipe for terrylabontechevy.com pulled 3,979 VINs -- HendrickCars.com's national
    # inventory -- and the gate logged "group feed names 48 rooftops (Buford GA, Cary NC,
    # Duluth GA, Durham NC, Greensboro NC, Merriam KS) and none is this store" on every
    # page. Terry Labonte Chevrolet IS the Greensboro NC store; the feed identifies its
    # rooftops by address block, not by name, so with no address to compare the gate could
    # not recognise the very dealer being scanned and threw the lot away.
    #
    # backend.parsers.parse says this explicitly -- "omit them on such a feed and every
    # page refuses every row" -- and delta_scan already passes them. This path did not, so
    # every dealer on a Hendrick / Sonic / Fletcher Jones / CarsCommerce group feed yielded
    # zero cars through recipe replay while looking like a healthy multi-thousand-VIN
    # recipe. Best-effort: a dealer missing from the registry falls back to the name and
    # host tiers, exactly as before.
    if not recipe_fetch_enabled():
        return None
    from backend.scanner.rooftop_disown import roster_place

    # Registry lookup hits the DB — keep it off the event loop, and don't pay
    # for it at all when recipe fetch is disabled.
    try:
        _place = (await asyncio.to_thread(roster_place, base_url)) or {}
        from backend.scanner.dealer_place import place_from_hints, place_kwargs

        if not _place.get("dealer_city"):
            _place = place_kwargs(await asyncio.to_thread(place_from_hints, dealer_id))
        elif not _place.get("dealer_address"):
            # roster has the town but no street: the page's JSON-LD street saved
            # by learn_place lets the gate keep street-block stamps at our address
            hinted = place_kwargs(await asyncio.to_thread(place_from_hints, dealer_id))
            if hinted.get("dealer_address"):
                _place = dict(_place, dealer_address=hinted["dealer_address"], dealer_address_source=hinted.get("dealer_address_source") or "site_jsonld")
    except Exception:  # noqa: BLE001 - attribution help must never break a scan
        _place = {}
    # load_recipes may consult Postgres (recipe_store sync) — keep that
    # blocking I/O off the event loop this coroutine runs on.
    loaded = await asyncio.to_thread(load_recipes, dealer_id)
    recipes = [r for r in loaded if not r.stale]
    if not recipes:
        return None
    from backend.parsers import parse

    min_vehicles = recipe_min_vehicles()
    union_records: list[tuple[str, Any]] = []
    union_vins: set[str] = set()
    union_vehicles: list[dict] = []
    for recipe in recipes:
        template: Any = None
        if recipe.post_template:
            try:
                template = json.loads(recipe.post_template)
            except ValueError:
                template = None
        if recipe.method != "GET" and template is None:
            # Truncated/unparseable POST sample — can't replay safely. Mark it
            # stale so the next browser scan re-captures a full body instead of
            # this recipe silently never working.
            await asyncio.to_thread(mark_stale, dealer_id, recipe, "unreplayable_post_template")
            logger.info(
                "Recipe stale [%s] %s — POST template unparseable (truncated capture?); "
                "browser scan will re-capture",
                dealer_name, recipe.url[:80],
            )
            continue
        records: list[tuple[str, Any]] = []
        vins: set[str] = set()
        vehicles: list[dict] = []
        pages = 1 if recipe.pagination == PAGINATION_NONE else _MAX_REPLAY_PAGES
        auth_dead = False
        for page_i in range(pages):
            body = _mutate_for_page(recipe, template, page_i) if template is not None else None
            page_url = _url_for_page(recipe, page_i)
            status, parsed = await asyncio.to_thread(_replay_request, recipe, body, base_url, page_url)
            if status in (401, 403):
                if not vins:
                    # Failed before collecting anything — the recipe's auth is dead.
                    await asyncio.to_thread(mark_stale, dealer_id, recipe, f"http_{status}")
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
            if isinstance(parsed, dict) and recipe_is_store_scoped(recipe, dealer_name):
                # dealer_run re-parses these payloads later (recovery / final
                # row set) without knowing the recipe; the marker travels with
                # the payload so no later parse() re-gates the store's own feed
                # (Tutton CDJR: 346 -> 5 rows through that second gate, 2026-09-26)
                parsed["_feed_scoped"] = True
            # rejected_out is REQUIRED, not optional decoration. parse() ends with:
            #
            #     if rejected_out is None:
            #         return kept + rejected
            #
            # so a caller that omits it gets back the very rows the rooftop gate just
            # refused. This path omitted it, and the effect was invisible because the
            # refusals still logged: replaying terrylabontechevy.com printed FORTY
            # "refusing all 100 row(s)" warnings and returned 3,931 VINs -- Hendrick's
            # national inventory, filed under one Greensboro store. That is precisely the
            # bug that gave bmwofmurrieta-com 1,921 cars across 37 makes.
            #
            # Passing a list makes parse() return only `kept`, which is what every other
            # caller already does.
            _refused: list[dict] = []
            page_vehicles = list(parse(
                recipe.provider_hint or provider, parsed,
                base_url=base_url, dealer_id=dealer_id,
                dealer_name=dealer_name, dealer_url=base_url,
                rejected_out=_refused,
                trust_feed_scope=recipe_is_store_scoped(recipe, dealer_name),
                **_place,
            ))
            new = _unique_vins(page_vehicles) - vins
            records.append((recipe.url, parsed))
            if page_i == 0 and recipe.pagination == PAGINATION_DEALER_COM:
                _learn_dealer_com_page_size(template, parsed, dealer_name)
            if page_i == 0:
                # The payload's own total wins over the file's: a stale total_count
                # (95 saved in August, lot now 106) stopped the walk after page 1.
                try:
                    from backend.parsers.base import get_total_count as _gtc

                    _t = _gtc(parsed)
                    if _t and _t > 0:
                        recipe.total_count = int(_t)
                except Exception:  # noqa: BLE001
                    pass
            vehicles.extend(v for v in page_vehicles if (v.get("vin") or "").strip().upper() in new)
            if not new:
                break
            vins |= new
            if recipe.total_count and len(vins) >= recipe.total_count:
                break
        if auth_dead:
            continue
        if len(vins) >= min_vehicles:
            recipe.last_ok_at = time.time()
            recipe.field_coverage = {
                k: v for k, v in recipe_field_coverage(vehicles).items() if k != "n"
            }
            all_r = load_recipes(dealer_id)
            for r in all_r:
                if r.key() == recipe.key():
                    r.last_ok_at = recipe.last_ok_at
                    r.field_coverage = dict(recipe.field_coverage)
            save_recipes(dealer_id, all_r)
            logger.info(
                "Recipe fetch [%s]: %d unique VIN(s) from %d page(s) via %s",
                dealer_name, len(vins), len(records), recipe.url[:80],
            )
            if not union:
                if coverage_out is not None:
                    coverage_out.update(recipe_field_coverage(vehicles))
                return records, len(vins)
            union_records.extend(records)
            union_vehicles.extend(v for v in vehicles if (v.get("vin") or "").strip().upper() not in union_vins)
            union_vins |= vins
    if union and len(union_vins) >= min_vehicles:
        if coverage_out is not None:
            coverage_out.update(recipe_field_coverage(union_vehicles))
        logger.info(
            "Recipe fetch [%s]: %d unique VIN(s) combined across %d recipe replay(s)",
            dealer_name, len(union_vins), len(union_records),
        )
        return union_records, len(union_vins)
    return None
