"""Site-total readers: what the platform itself says the lot holds.

Moved verbatim out of recipe_validation.py (P5B.2) so the replay walk in recipes.py
and the set gate read one set of extractors. recipe_validation re-exports every
name; ``PAGINATION_*`` are imported lazily because recipes.py imports this module.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:  # pragma: no cover
    from backend.scanner.recipes import EndpointRecipe

# The one minimum-VIN rule (P12B.5, D-OD1). Save paths keep any candidate that answers
# with more than 0 store VINs and require the kept set to reach RECIPE_MIN_VINS between
# them; the scan's union succeeds at the same number. "Above 0" alone would admit widget
# and fragment recipes (38 on 33 dealers), so a sub-floor recipe takes part in a scan only
# as a condition-pinned side of a paginated shape the gate judged ok/uncertain.
RECIPE_MIN_VINS = 5
# promote_from_ledger's capture prefilter: a ledgered endpoint with fewer parsed rows and
# no site total is not worth a replay. It is a candidate filter, not a floor on the lot.
PROMOTE_PREFILTER_ROWS = 3
# First-hit replay never demands fewer than this many VINs of a recipe that once held more.
FIRST_HIT_MIN_VINS = 3
# gate_verdict values under which a sub-floor side may join the scan union.
GATE_ADMITTING = ("ok", "uncertain")


def request_signature(method: Any, url: Any, post_template: Any) -> tuple[str, str, str]:
    """``(method, host+path+query, body digest)``: identical signatures are the same
    request. One definition for the identity report and the save-time duplicate refusal."""
    import hashlib
    from urllib.parse import urlparse

    p = urlparse(str(url or ""))
    where = f"{(p.hostname or '').lower()}{p.path}" + (f"?{p.query}" if p.query else "")
    body = str(post_template or "")
    digest = hashlib.sha1(body.encode("utf-8")).hexdigest()[:12] if body else ""
    return (str(method or "GET").upper(), where, digest)


def first_hit_floor(global_floor: int, vehicle_rows: int) -> int:
    """VINs a first-hit replay must return: ``min(global floor, max(3, ceil(0.5 x
    vehicle_rows)))``. A recipe saved with 6 rows replays 6 and is kept; a 1-VIN replay
    of a 40-row recipe is refused. A recipe with no saved row count keeps the global floor."""
    rows = int(vehicle_rows or 0)
    if rows <= 0:
        return global_floor
    return min(global_floor, max(FIRST_HIT_MIN_VINS, -(-rows // 2)))


def union_floor(global_floor: int) -> int:
    """VINs the scan union must reach: :data:`RECIPE_MIN_VINS`, or the operator's lower
    ``SCANNER_RECIPE_MIN_VEHICLES`` override."""
    return min(global_floor, RECIPE_MIN_VINS)


def sub_floor_admitted(recipe: "EndpointRecipe") -> bool:
    """May a replay under the global floor still join the union? Only a condition-pinned
    side of a paginated inventory shape whose recorded gate verdict is ok or uncertain.
    ``pagination=none`` (a widget or one-page fragment) and a recipe the gate never
    judged are refused."""
    from backend.scanner import recipes as _r

    if recipe.pagination == _r.PAGINATION_NONE:
        return False
    if (getattr(recipe, "gate_verdict", "") or "") not in GATE_ADMITTING:
        return False
    from backend.scanner.recipe_validation import recipe_condition_filter

    return bool(recipe_condition_filter(recipe))


_AUTOWALL_TOTAL_RE = re.compile(r"<title>\s*(\d{1,5})\s+Vehicles for Sale", re.I)
_DEP_COUNT_RE = re.compile(r'data-vehicle_count="(\d+)"')


def platform_of(recipe: "EndpointRecipe") -> str:
    from backend.scanner import recipes as _r

    PAGINATION_CARSCOMMERCE, PAGINATION_TYPESENSE = _r.PAGINATION_CARSCOMMERCE, _r.PAGINATION_TYPESENSE
    PAGINATION_DEALER_COM, PAGINATION_COSMOS_PT = _r.PAGINATION_DEALER_COM, _r.PAGINATION_COSMOS_PT
    PAGINATION_ALGOLIA, PAGINATION_GRAPHQL_OFFSET = _r.PAGINATION_ALGOLIA, _r.PAGINATION_GRAPHQL_OFFSET
    PAGINATION_PAGE_QUERY, PAGINATION_DEP_SRP = _r.PAGINATION_PAGE_QUERY, _r.PAGINATION_DEP_SRP
    PAGINATION_HTML_PAGE = _r.PAGINATION_HTML_PAGE
    url = (recipe.url or "").lower()
    pg = recipe.pagination
    if pg == PAGINATION_CARSCOMMERCE or "carscommerce" in url:
        return "carscommerce"
    if pg == PAGINATION_TYPESENSE or "typesense" in url:
        return "typesense"
    if pg == PAGINATION_DEALER_COM or "ws-inv-data" in url:
        return "dealer_dot_com"
    if pg == PAGINATION_COSMOS_PT or "cosmos/srp/vehicles" in url:
        return "dealer_on_cosmos"
    if pg == PAGINATION_ALGOLIA or "algolia" in url:
        return "algolia"
    if pg == PAGINATION_GRAPHQL_OFFSET:
        return "oneaudi"
    if pg == PAGINATION_PAGE_QUERY or url.endswith((".json",)) and "inventory" in url:
        return "team_velocity"
    if pg == PAGINATION_DEP_SRP:
        return "dealer_eprocess"
    if pg == PAGINATION_HTML_PAGE and ("gs-vehicle" in url or recipe.provider_hint == "autowall"):
        return "autowall"
    if "wp-json" in url:
        return "wp_vehicles_index"
    return recipe.provider_hint or "unknown"


def _int_or_none(v: Any) -> int | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return int(v)
    if isinstance(v, str) and v.strip().isdigit():
        return int(v.strip())
    return None


def _total_carscommerce(parsed: Any) -> int | None:
    if not isinstance(parsed, dict):
        return None
    data = parsed.get("data")
    if isinstance(data, dict):
        n = _int_or_none(data.get("total_vehicle_count"))
        if n is not None:
            return n
    meta = parsed.get("meta")
    if isinstance(meta, dict) and isinstance(meta.get("pagination"), dict):
        return _int_or_none(meta["pagination"].get("total"))
    return None


def _total_typesense(parsed: Any) -> int | None:
    if not isinstance(parsed, dict) or not isinstance(parsed.get("results"), list):
        return None
    founds = [_int_or_none(r.get("found")) for r in parsed["results"] if isinstance(r, dict)]
    founds = [f for f in founds if f is not None]
    return max(founds) if founds else None


def _total_dealer_com(parsed: Any) -> int | None:
    if not isinstance(parsed, dict):
        return None
    info = parsed.get("pageInfo")
    if isinstance(info, dict):
        return _int_or_none(info.get("totalCount"))
    return None


def _total_cosmos(parsed: Any) -> int | None:
    if not isinstance(parsed, dict):
        return None
    pdm = (parsed.get("Paging") or {}).get("PaginationDataModel") if isinstance(parsed.get("Paging"), dict) else None
    return _int_or_none(pdm.get("TotalCount")) if isinstance(pdm, dict) else None


def _total_algolia(parsed: Any) -> int | None:
    if not isinstance(parsed, dict):
        return None
    n = _int_or_none(parsed.get("nbHits"))
    if n is not None:
        return n
    results = parsed.get("results")
    if isinstance(results, list):
        best = [_int_or_none(b.get("nbHits")) for b in results if isinstance(b, dict) and b.get("hits")]
        best = [b for b in best if b is not None]
        return max(best) if best else None
    return None


def _total_oneaudi(parsed: Any) -> int | None:
    if not isinstance(parsed, dict):
        return None
    scs = (parsed.get("data") or {}).get("stockCarSearch") if isinstance(parsed.get("data"), dict) else None
    return _int_or_none(scs.get("resultNumber")) if isinstance(scs, dict) else None


def _total_team_velocity(parsed: Any) -> int | None:
    if not isinstance(parsed, dict):
        return None
    for k in ("totalCount", "total", "totalRecords", "totalVehicles"):
        n = _int_or_none(parsed.get(k))
        if n is not None:
            return n
    pages = _int_or_none(parsed.get("totalPages"))
    if pages == 1 and isinstance(parsed.get("vehicles"), list):
        return len(parsed["vehicles"])
    return None


def _total_html_regex(rx: re.Pattern) -> Callable[[Any], int | None]:
    def _f(parsed: Any) -> int | None:
        if not isinstance(parsed, str):
            return None
        m = rx.search(parsed)
        return int(m.group(1)) if m else None
    return _f


def _total_list_length(parsed: Any) -> int | None:
    """Single-shot feeds (wp-json vehicles index, Supabase ``?limit=1000``): the
    whole list IS the site's count."""
    if isinstance(parsed, list):
        return len(parsed)
    if isinstance(parsed, dict):
        for k in ("vehicles", "items", "data", "results"):
            if isinstance(parsed.get(k), list):
                return len(parsed[k])
    return None


def _total_generic(parsed: Any) -> int | None:
    from backend.parsers.base import get_total_count

    if isinstance(parsed, list):
        return len(parsed)
    try:
        return get_total_count(parsed)
    except Exception:  # noqa: BLE001
        return None


# One extractor per platform; anything unlisted falls back to the generic
# reader (``get_total_count``) and then to ``None`` — never a guess.
SITE_TOTAL_EXTRACTORS: dict[str, Callable[[Any], int | None]] = {
    "carscommerce": _total_carscommerce,
    "typesense": _total_typesense,
    "dealer_dot_com": _total_dealer_com,
    "dealer_on_cosmos": _total_cosmos,
    "algolia": _total_algolia,
    "motive_ridemotive": _total_algolia,
    "oneaudi": _total_oneaudi,
    "team_velocity": _total_team_velocity,
    "dealer_eprocess": _total_html_regex(_DEP_COUNT_RE),
    "autowall": _total_html_regex(_AUTOWALL_TOTAL_RE),
    "wp_vehicles_index": _total_list_length,
    "generic_json": _total_list_length,
    "html_cards": lambda parsed: None,
    "jazel": lambda parsed: None,
    "overfuel": lambda parsed: None,
    "nabthat": lambda parsed: None,
}


def extract_site_total(recipe: "EndpointRecipe", parsed: Any) -> int | None:
    from backend.scanner.recipes import PAGINATION_NONE

    fn = SITE_TOTAL_EXTRACTORS.get(platform_of(recipe))
    try:
        n = fn(parsed) if fn else _total_generic(parsed)
    except Exception:  # noqa: BLE001
        n = None
    if n is None and fn is not None and fn is not _total_generic and recipe.pagination == PAGINATION_NONE:
        n = _total_list_length(parsed)
    return n if (n is None or n >= 0) else None


# Readers that count a payload's list instead of reading a count field. Right for a
# single-shot feed (the whole list IS the lot), wrong for a paged one: page 1's length
# is the page size, so it must never stop a paginated walk.
_LIST_LENGTH_READERS = (_total_list_length, _total_generic)


def replay_site_total(recipe: "EndpointRecipe", parsed: Any) -> int | None:
    """The count a paginated walk may trust from page 1: ``max`` of the platform's
    count reader and ``get_total_count``, never a list length."""
    from backend.parsers.base import get_total_count

    fn = SITE_TOTAL_EXTRACTORS.get(platform_of(recipe))
    found: list[int] = []
    if fn is not None and fn not in _LIST_LENGTH_READERS:
        try:
            n = fn(parsed)
        except Exception:  # noqa: BLE001
            n = None
        if n is not None and n > 0:
            found.append(n)
    try:
        g = get_total_count(parsed)
    except Exception:  # noqa: BLE001
        g = None
    if g is not None and g > 0:
        found.append(int(g))
    return max(found) if found else None
