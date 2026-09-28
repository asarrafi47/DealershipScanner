"""Recipe-set validation against the site's own count (HTTP_ONLY_SCANS_PLAN Phase 3).

``validate_recipe`` (recipe_synth) answers one question — how many VINs does
this recipe yield — and every save path accepted anything with >= 5. That is
how one-condition, section-scoped and short-page recipes reached the fleet and
were only caught by the assess step a scan later (the 09-24 / 09-26 censuses:
16 section-scoped carscommerce captures, 14 dealer.com recipes stopped at 48
rows, nine "only one condition" verdicts, none of them a used-only lot).

:func:`validate_recipe_set` judges the SET of recipes a dealer is about to be
given, at synthesis / capture time, by replaying page 1 and page 2 of each
recipe through the same request function the scan uses and comparing what came
back with what the site itself says:

* ``site_total``  — the platform's own count (carscommerce ``total_vehicle_count``,
  dealer.com ``pageInfo.totalCount``, typesense ``found``, cosmos ``TotalCount``,
  autoWALL ``<title>``, DEP ``data-vehicle_count``, wp-json / flat list lengths;
  :data:`SITE_TOTAL_EXTRACTORS`, ``None`` when the platform exposes nothing);
* ``per_condition`` — VINs by new / used / certified as the parser read them;
* flags — ``one_condition``, ``section_scoped``, ``short_page``, ``auth_needed``,
  ``zero_rows``; and a verdict ``ok`` | ``reject`` | ``uncertain`` with reasons.

Pure with respect to storage: no recipe file, no car row, no DB write. The
side effects (discovery.md entry, ``scan_hints.recipe_status``) live in
:func:`gate_recipes`, which the save paths call.
"""
from __future__ import annotations

import copy
import json
import os
import logging
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qsl, urlparse

from backend.scanner.recipes import (
    PAGINATION_ALGOLIA,
    PAGINATION_CARSCOMMERCE,
    PAGINATION_COSMOS_PT,
    PAGINATION_DEALER_COM,
    PAGINATION_DEP_SRP,
    PAGINATION_GRAPHQL_OFFSET,
    PAGINATION_HTML_PAGE,
    PAGINATION_JAZEL_SRP,
    PAGINATION_NONE,
    PAGINATION_PAGE_QUERY,
    PAGINATION_TYPESENSE,
    EndpointRecipe,
    _CC_SECTION_FACETS,
    _learn_dealer_com_page_size,
    _mutate_for_page,
    _replay_request,
    _unique_vins,
    _url_for_page,
    recipe_is_section_scoped,
    recipe_is_store_scoped,
)

logger = logging.getLogger("scanner")

ROOT = Path(__file__).resolve().parents[2]
LOG_ROOT = Path(os.environ.get("DEALER_LOGS_ROOT") or (ROOT / "workspace" / "dealer_logs"))
ONE_CONDITION_OK_PATH = ROOT / "workspace" / "pipeline" / "one_condition_ok.txt"

# fetch(recipe, body, base_url, url) -> (status, parsed_json_or_html_or_None):
# the signature of recipes._replay_request, which both the synth's validate walk
# and the scan's replay use, so validation clears the same edge the scan will.
Fetch = Callable[[EndpointRecipe, Any, str, str | None], tuple[int, Any]]

PAGES_REPLAYED = 2
# Below this share of the site's count, a recipe set that has already run out of
# pages would come back ``no_rows`` from dealer_pipeline.assess (ROW_FLOOR 0.50).
COVERAGE_FLOOR = 0.50
# Page 1 must fall this far short of the site's count before an empty page 2
# counts as a short page (rooftop-gate refusals shave a few rows off a page).
SHORT_PAGE_TOLERANCE = 0.95
# Having seen at least this share of the site's count, a single condition is a
# statement about the lot, not about the two pages we happened to read.
WHOLE_LOT_SEEN = 0.90

_AUTOWALL_TOTAL_RE = re.compile(r"<title>\s*(\d{1,5})\s+Vehicles for Sale", re.I)
_DEP_COUNT_RE = re.compile(r'data-vehicle_count="(\d+)"')
_TS_CONDITION_RE = re.compile(r"condition\s*:=?\s*\[?\s*`?\"?'?([A-Za-z -]+)", re.I)


# ── condition vocabulary ───────────────────────────────────────────────────────

def normalize_condition(raw: Any) -> str:
    """``new`` | ``used`` | ``certified`` | ``unknown`` from any platform's label."""
    s = str(raw or "").strip().lower()
    if not s:
        return "unknown"
    if "certif" in s or s == "cpo":
        return "certified"
    if s.startswith("new") or s == "n":
        return "new"
    if "used" in s or "pre" in s or s in ("u", "preowned", "pre_owned"):
        return "used"
    return "unknown"


def _side(cond: str) -> str:
    """new vs. pre-owned: certified is a subset of used for the one-condition check."""
    return "new" if cond == "new" else "used"


# ── platform routing + site-total extractors ───────────────────────────────────

def platform_of(recipe: EndpointRecipe) -> str:
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


def extract_site_total(recipe: EndpointRecipe, parsed: Any) -> int | None:
    fn = SITE_TOTAL_EXTRACTORS.get(platform_of(recipe))
    try:
        n = fn(parsed) if fn else _total_generic(parsed)
    except Exception:  # noqa: BLE001
        n = None
    if n is None and fn is not None and fn is not _total_generic and recipe.pagination == PAGINATION_NONE:
        n = _total_list_length(parsed)
    return n if (n is None or n >= 0) else None


# ── what the recipe itself is pinned to ────────────────────────────────────────

def recipe_condition_filter(recipe: EndpointRecipe) -> set[str]:
    """Conditions the request itself is filtered to (empty when unfiltered).

    A recipe that carries a condition filter is proof the site has sections;
    with no sibling recipe for the other side, the set is one-condition by
    construction (the tp=used-only DEP captures, typesense ``condition:Used``,
    PixelMotion ``condition[]=new``, carscommerce ``type_slug``)."""
    out: set[str] = set()
    body: Any = None
    if recipe.post_template:
        try:
            body = json.loads(recipe.post_template)
        except ValueError:
            body = None
    if isinstance(body, dict):
        for holder in ("facetFilters", "filters"):
            h = body.get(holder)
            if isinstance(h, dict):
                for k in ("type_slug", "type", "condition"):
                    vals = h.get(k)
                    if isinstance(vals, list):
                        out |= {normalize_condition(v) for v in vals}
                    elif isinstance(vals, str):
                        out.add(normalize_condition(vals))
        for s in body.get("searches") or []:
            if isinstance(s, dict) and isinstance(s.get("filter_by"), str):
                for m in _TS_CONDITION_RE.finditer(s["filter_by"]):
                    out |= {normalize_condition(p) for p in re.split(r"[,|]", m.group(1))}
    parts = urlparse(recipe.url or "")
    q = parse_qsl(parts.query, keep_blank_values=True)
    for k, v in q:
        kl = k.lower()
        if kl in ("tp", "condition", "condition[]", "type", "inventory_type", "vehicletype"):
            out.add(normalize_condition(v))
    path = parts.path.lower()
    if recipe.pagination in (PAGINATION_DEP_SRP, PAGINATION_HTML_PAGE, PAGINATION_JAZEL_SRP):
        seg = re.search(r"/(new|used|pre-owned|preowned|certified)(?:[-/]|$)", path)
        if seg:
            out.add(normalize_condition(seg.group(1)))
    # Team Velocity feeds are one file per condition: /inventory-used.json,
    # /inventory-new.json, /inventory-cpo.json (recipe_synth._TEAM_VELOCITY_FEEDS).
    stem = re.search(r"-(new|used|cpo|certified)\.json$", path)
    if stem:
        out.add(normalize_condition(stem.group(1)))
    out.discard("unknown")
    return out


def one_condition_ok_ids(path: Path | None = None) -> set[str]:
    """Dealers whose lot legitimately carries a single condition (the pipeline's
    workspace/pipeline/one_condition_ok.txt: one id per line, ``#`` comments)."""
    try:
        text = (path or ONE_CONDITION_OK_PATH).read_text(encoding="utf-8")
    except OSError:
        return set()
    return {ln.split("#", 1)[0].strip() for ln in text.splitlines() if ln.split("#", 1)[0].strip()}


# ── the site's own condition census (carscommerce only, today) ─────────────────

def _cc_condition_census(recipe: EndpointRecipe, template: Any, fetch: Fetch, base_url: str) -> dict[str, int] | None:
    """``{condition: doc_count}`` from a one-row ``facets: [type_slug]`` probe over
    the whole account (facetFilters and section filters removed)."""
    if not isinstance(template, dict):
        return None
    probe = copy.deepcopy(template)
    # The whole account, not the recipe's own filter: claremontcdjr-com's
    # store filter picked the OEM-code feed (78 new of 671) and a census inside
    # that feed would have agreed with it. A group account answers for the
    # group, which for a franchise store still says "both".
    probe.pop("facetFilters", None)
    flt = probe.get("filters")
    if isinstance(flt, dict):
        probe["filters"] = {k: v for k, v in flt.items() if k not in _CC_SECTION_FACETS}
    probe.update({"page": 1, "perPage": 1, "facets": ["type_slug"]})
    try:
        status, parsed = fetch(recipe, probe, base_url, recipe.url)
    except Exception as exc:  # noqa: BLE001
        logger.debug("condition census failed [%s]: %s", recipe.dealer_id, str(exc)[:120])
        return None
    if status != 200 or not isinstance(parsed, dict):
        return None
    data = parsed.get("data")
    facets = data.get("facets") if isinstance(data, dict) else None
    if not isinstance(facets, list):
        return None
    out: dict[str, int] = {}
    for f in facets:
        if not isinstance(f, dict) or (f.get("name") or f.get("field") or f.get("key")) != "type_slug":
            continue
        for v in f.get("values") or []:
            if isinstance(v, dict) and v.get("key") is not None:
                c = normalize_condition(v.get("key"))
                out[c] = out.get(c, 0) + int(v.get("doc_count") or 0)
    return out or None


SITE_CONDITION_CENSUS: dict[str, Callable[[EndpointRecipe, Any, Fetch, str], dict[str, int] | None]] = {
    "carscommerce": _cc_condition_census,
}


# ── report ─────────────────────────────────────────────────────────────────────

@dataclass
class RecipeCheck:
    url: str
    method: str
    pagination: str
    platform: str
    pages: list[dict[str, Any]] = field(default_factory=list)
    vins: int = 0
    per_condition: dict[str, int] = field(default_factory=dict)
    gate_rejected: int = 0
    site_total: int | None = None
    pinned_conditions: list[str] = field(default_factory=list)
    section_scoped: bool = False
    store_scoped: bool = False
    auth_needed: bool = False
    # 401/403 after page 1 had already answered: the scan keeps those VINs
    # (try_fetch_via_recipes), so this is a warning, not a dead recipe.
    auth_mid_walk: bool = False
    short_page: bool = False
    exhausted: bool = False
    error: str = ""


@dataclass
class RecipeValidationReport:
    dealer_id: str
    verdict: str = "uncertain"
    reasons: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    vins_total: int = 0
    per_condition: dict[str, int] = field(default_factory=dict)
    site_total: int | None = None
    site_conditions: dict[str, int] | None = None
    coverage: float | None = None
    flags: dict[str, bool] = field(default_factory=dict)
    recipes: list[RecipeCheck] = field(default_factory=list)
    pages_replayed: int = PAGES_REPLAYED
    stamp: str = ""

    @property
    def reason(self) -> str:
        """The leading reason in full (``one_condition: only new rows while ...``)."""
        return self.reasons[0] if self.reasons else ("ok" if self.verdict == "ok" else self.verdict)

    @property
    def reason_code(self) -> str:
        """The leading reason's code alone (``one_condition``, ``site_total_unknown``)."""
        return self.reason.split(":", 1)[0].strip()

    @property
    def status(self) -> str:
        """``ok`` | ``rejected:<code>`` | ``uncertain:<code>`` for scan_hints.recipe_status
        (the full reasons travel in ``recipe_validation.reasons`` beside it)."""
        if self.verdict == "ok":
            return "ok"
        return f"{'rejected' if self.verdict == 'reject' else 'uncertain'}:{self.reason_code}"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return {
            "verdict": self.verdict, "reasons": list(self.reasons), "vins_total": self.vins_total,
            "per_condition": dict(self.per_condition), "site_total": self.site_total,
            "coverage": self.coverage, "flags": {k: v for k, v in self.flags.items() if v},
        }

    def to_markdown(self, context: str = "") -> str:
        head = f"## {self.stamp} recipe validation" + (f" ({context})" if context else "") + f" — {self.verdict.upper()}"
        L = [head]
        if self.reasons:
            L.append(f"- reasons: {'; '.join(self.reasons)}")
        cov = f"{self.coverage:.0%}" if self.coverage is not None else "-"
        L.append(f"- VINs over {self.pages_replayed} page(s) per recipe: {self.vins_total}; site total: "
                 f"{self.site_total if self.site_total is not None else 'not exposed'}; coverage: {cov}")
        L.append(f"- per condition: {json.dumps(self.per_condition)}"
                 + (f"; site census: {json.dumps(self.site_conditions)}" if self.site_conditions else ""))
        on = [k for k, v in self.flags.items() if v]
        L.append(f"- flags: {', '.join(on) if on else 'none'}")
        for r in self.recipes:
            statuses = "/".join(str(p.get("status")) for p in r.pages) or "-"
            pv = "+".join(str(p.get("new_vins", 0)) for p in r.pages) or "0"
            extra = []
            if r.pinned_conditions:
                extra.append(f"pinned={','.join(r.pinned_conditions)}")
            if r.section_scoped:
                extra.append("section_scoped")
            if r.store_scoped:
                extra.append("store_scoped")
            if r.short_page:
                extra.append("short_page")
            if r.auth_needed:
                extra.append("auth_needed")
            if r.auth_mid_walk:
                extra.append("auth_mid_walk")
            if r.gate_rejected:
                extra.append(f"gate_rejected={r.gate_rejected}")
            if r.error:
                extra.append(f"error={r.error}")
            L.append(f"  - {r.method} {r.url[:110]} [{r.platform}] status {statuses}, VINs {pv} = {r.vins}, "
                     f"site total {r.site_total if r.site_total is not None else '-'}, conditions {json.dumps(r.per_condition)}"
                     + (f" ({'; '.join(extra)})" if extra else ""))
        for n in self.notes:
            L.append(f"- note: {n}")
        return "\n".join(L)


# ── the check ──────────────────────────────────────────────────────────────────

def _parse_rows(recipe: EndpointRecipe, parsed: Any, *, base_url: str, dealer_id: str, dealer_name: str,
                place: dict[str, str] | None) -> tuple[list[dict], int]:
    """This store's rows for one page and how many the rooftop gate refused."""
    from backend.parsers import _cached_roster_place_items, parse

    kw = dict(_cached_roster_place_items(base_url))
    kw.update({k: v for k, v in (place or {}).items()
               if k in ("dealer_address", "dealer_city", "dealer_state", "dealer_zip") and v})
    rejected: list[dict] = []
    rows = parse(
        recipe.provider_hint or "", parsed,
        base_url=base_url, dealer_id=dealer_id, dealer_name=dealer_name, dealer_url=base_url,
        rejected_out=rejected, trust_feed_scope=recipe_is_store_scoped(recipe, dealer_name), **kw,
    )
    return list(rows or []), len(rejected)


def _check_recipe(recipe: EndpointRecipe, *, fetch: Fetch, base_url: str, dealer_id: str, dealer_name: str,
                  place: dict[str, str] | None, pages: int) -> tuple[RecipeCheck, set[str], dict[str, set[str]], Any, Any]:
    chk = RecipeCheck(url=recipe.url, method=recipe.method, pagination=recipe.pagination, platform=platform_of(recipe),
                      pinned_conditions=sorted(recipe_condition_filter(recipe)),
                      section_scoped=recipe_is_section_scoped(recipe), store_scoped=recipe_is_store_scoped(recipe, dealer_name))
    template: Any = None
    if recipe.post_template:
        try:
            template = json.loads(recipe.post_template)
        except ValueError:
            chk.error = "post_template is not JSON"
            return chk, set(), {}, None, None
    if recipe.method != "GET" and template is None:
        chk.error = "POST recipe without a body"
        return chk, set(), {}, None, None
    vins: set[str] = set()
    by_cond: dict[str, set[str]] = {}
    n_pages = 1 if recipe.pagination == PAGINATION_NONE else max(1, pages)
    page1_parsed: Any = None
    page1_vins = 0
    for i in range(n_pages):
        body = _mutate_for_page(recipe, template, i) if template is not None else None
        url = _url_for_page(recipe, i)
        rec: dict[str, Any] = {"page": i + 1}
        try:
            status, parsed = fetch(recipe, body, base_url, url)
        except Exception as exc:  # noqa: BLE001
            status, parsed = 0, None
            rec["error"] = str(exc)[:120]
        rec["status"] = status
        if status in (401, 403):
            if vins:
                chk.auth_mid_walk = True
            else:
                chk.auth_needed = True
        if status != 200 or parsed is None:
            rec["new_vins"] = 0
            chk.pages.append(rec)
            if i > 0:
                chk.exhausted = True
            break
        if i == 0:
            page1_parsed = parsed
            chk.site_total = extract_site_total(recipe, parsed)
            if recipe.pagination == PAGINATION_DEALER_COM and isinstance(template, dict):
                # the server's real page size, or page 2 starts past the end (recipes.py)
                _learn_dealer_com_page_size(template, parsed, dealer_name)
        rows, refused = _parse_rows(recipe, parsed, base_url=base_url, dealer_id=dealer_id, dealer_name=dealer_name, place=place)
        chk.gate_rejected += refused
        page_vins = _unique_vins(rows)
        new = page_vins - vins
        rec["rows"] = len(rows)
        rec["new_vins"] = len(new)
        chk.pages.append(rec)
        for r in rows:
            v = str(r.get("vin") or "").strip().upper()
            if v and v in new:
                by_cond.setdefault(normalize_condition(r.get("condition")), set()).add(v)
        vins |= new
        if i == 0:
            page1_vins = len(new)
        if not new:
            chk.exhausted = True
            break
        if chk.site_total is not None and len(vins) >= chk.site_total:
            chk.exhausted = True
            break
    if recipe.pagination == PAGINATION_NONE:
        chk.exhausted = True  # single-shot: there is no further page to read
    chk.vins = len(vins)
    chk.per_condition = {c: len(s) for c, s in sorted(by_cond.items())}
    # A short page is a 200 that adds nothing while the site says more exists;
    # a 401/403/5xx on page 2 is an auth or edge problem, reported as such.
    if (n_pages > 1 and len(chk.pages) >= 2 and chk.pages[1].get("status") == 200 and chk.pages[1].get("new_vins", 0) == 0
            and chk.site_total and page1_vins < chk.site_total * SHORT_PAGE_TOLERANCE):
        chk.short_page = True
    return chk, vins, by_cond, template, page1_parsed


def validate_recipe_set(
    dealer_id: str,
    recipes: list[EndpointRecipe],
    fetch: Fetch | None = None,
    *,
    base_url: str = "",
    dealer_name: str = "",
    place: dict[str, str] | None = None,
    pages: int = PAGES_REPLAYED,
    one_condition_ok: set[str] | None = None,
) -> RecipeValidationReport:
    """Judge *recipes* as the set the scan will replay for *dealer_id*.

    *fetch* defaults to ``recipes._replay_request`` (the scan's own request
    function). *place* is the store's postal place for the rooftop gate
    (``dealer_city`` / ``dealer_state`` / ``dealer_zip`` / ``dealer_address``).
    """
    fetch = fetch or _replay_request
    if not base_url:
        first = next((r.url for r in recipes if r.url), "")
        p = urlparse(first)
        base_url = f"{p.scheme}://{p.netloc}" if p.netloc else ""
    dealer_name = dealer_name or dealer_id
    ok_ids = one_condition_ok if one_condition_ok is not None else one_condition_ok_ids()
    rep = RecipeValidationReport(dealer_id=dealer_id, pages_replayed=pages,
                                 stamp=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    flags = {"one_condition": False, "section_scoped": False, "short_page": False, "auth_needed": False, "zero_rows": False}
    if not recipes:
        flags["zero_rows"] = True
        rep.flags = flags
        rep.verdict, rep.reasons = "reject", ["zero_rows: no recipe to validate"]
        return rep

    all_vins: set[str] = set()
    by_cond: dict[str, set[str]] = {}
    checked: list[tuple[EndpointRecipe, RecipeCheck, Any]] = []
    for r in recipes:
        chk, vins, conds, template, _page1 = _check_recipe(
            r, fetch=fetch, base_url=base_url, dealer_id=dealer_id, dealer_name=dealer_name, place=place, pages=pages)
        rep.recipes.append(chk)
        checked.append((r, chk, template))
        all_vins |= vins
        for c, s in conds.items():
            by_cond.setdefault(c, set()).update(s)
    rep.vins_total = len(all_vins)
    rep.per_condition = {c: len(s) for c, s in sorted(by_cond.items())}
    sides_seen = {_side(c) for c, n in rep.per_condition.items() if n and c != "unknown"}
    census: dict[str, int] | None = None
    if rep.vins_total and len(sides_seen) == 1:
        # one extra request, only when the pages read show a single side
        for r, chk, template in checked:
            fn = SITE_CONDITION_CENSUS.get(chk.platform)
            if fn is not None and chk.vins:
                census = fn(r, template, fetch, base_url)
                break
    rep.site_conditions = census

    # site total for the set: per-condition recipes add up; overlapping ones don't
    totals = [(c.site_total, tuple(c.pinned_conditions)) for c in rep.recipes if c.site_total is not None]
    if totals:
        pins = [p for _, p in totals]
        disjoint = len(totals) > 1 and all(pins) and len({s for p in pins for s in p}) == sum(len(p) for p in pins)
        rep.site_total = sum(t for t, _ in totals) if disjoint else max(t for t, _ in totals)
        rep.coverage = round(rep.vins_total / rep.site_total, 3) if rep.site_total else None
    exhausted = all(c.exhausted or c.error or c.auth_needed for c in rep.recipes)

    flags["auth_needed"] = any(c.auth_needed for c in rep.recipes)
    flags["zero_rows"] = rep.vins_total == 0
    section = [c for c in rep.recipes if c.section_scoped]
    flags["section_scoped"] = bool(section)
    flags["short_page"] = any(c.short_page for c in rep.recipes)

    reasons: list[str] = []
    notes: list[str] = []
    if rep.vins_total and len(sides_seen) == 1:
        only = next(iter(sides_seen))
        site_has_both: bool | None = None
        how = ""
        if census:
            site_sides = {_side(c) for c, n in census.items() if n and c != "unknown"}
            site_has_both = len(site_sides) == 2
            how = f"site census {json.dumps(census)}"
        else:
            pinned = {s for c in rep.recipes for s in c.pinned_conditions}
            pinned_sides = {_side(p) for p in pinned}
            if pinned and len(pinned_sides) == 1:
                # a condition filter is proof the site has sections; nothing covers the other one
                site_has_both = True
                how = f"recipes pinned to {sorted(pinned)} with no recipe for the other side"
            elif pinned and len(pinned_sides) == 2:
                # a recipe for the other side exists and returned nothing
                site_has_both = True
                other = ({"new", "used"} - {only}).pop()
                how = f"the {other} recipe returned no rows"
        if site_has_both:
            if dealer_id in ok_ids:
                notes.append(f"one condition by design ({only} only; listed in one_condition_ok.txt); {how}")
            else:
                flags["one_condition"] = True
                reasons.append(f"one_condition: only {only} rows while the site sells both ({how})")
        elif site_has_both is None:
            if rep.coverage is not None and rep.coverage >= WHOLE_LOT_SEEN:
                notes.append(f"one_condition_unverified: whole lot seen and every row is {only}; the site exposes no condition census")
            else:
                notes.append(f"single condition ({only}) on the pages read; site exposes no condition census — assess decides after the scan")

    if flags["zero_rows"]:
        head: list[str] = []
        if flags["auth_needed"]:
            codes = sorted({str(p.get("status")) for c in rep.recipes for p in c.pages if p.get("status") in (401, 403)})
            head.append(f"auth_needed: HTTP {'/'.join(codes)}; the recipe's auth is dead or the edge wants a browser")
        head.append("zero_rows: no VIN from any recipe")
        reasons = head + reasons
    elif flags["auth_needed"]:
        notes.append("auth_needed on " + ", ".join(c.url[:60] for c in rep.recipes if c.auth_needed))
    if section:
        if len(section) == len(rep.recipes):
            reasons.append("section_scoped: every recipe pins one SRP section (type/make/model facets); it cannot yield the lot")
        else:
            notes.append(f"section_scoped: {len(section)} of {len(rep.recipes)} recipe(s) pin one SRP section")
    if flags["short_page"]:
        short = [c for c in rep.recipes if c.short_page]
        detail = "; ".join(f"{c.url[:60]} page 1 {c.pages[0].get('new_vins', 0)} of {c.site_total}, page 2 empty" for c in short)
        if rep.coverage is not None and rep.coverage < COVERAGE_FLOOR:
            reasons.append(f"short_page: {detail}")
        else:
            notes.append(f"short_page: {detail}")
    if exhausted and rep.coverage is not None and rep.coverage < COVERAGE_FLOOR and not flags["zero_rows"] and not any(x.startswith("short_page") for x in reasons):
        reasons.append(f"coverage: {rep.vins_total} of {rep.site_total} with no further page")
    errors = [c for c in rep.recipes if c.error]
    for c in errors:
        notes.append(f"{c.url[:60]}: {c.error}")

    uncertain: list[str] = []
    if rep.site_total is None:
        uncertain.append("site_total_unknown: platform exposes no count; coverage cannot be judged")
    if flags["short_page"] and not any(x.startswith("short_page") for x in reasons):
        uncertain.append("short_page")
    if section and len(section) < len(rep.recipes):
        uncertain.append("section_scoped_partial")
    if any(n.startswith("one_condition_unverified") for n in notes):
        uncertain.append("one_condition_unverified")
    if flags["auth_needed"] and not flags["zero_rows"]:
        uncertain.append("auth_needed_partial")
    mid = [c for c in rep.recipes if c.auth_mid_walk]
    if mid and not flags["zero_rows"]:
        notes.append("auth dropped mid-walk on " + ", ".join(c.url[:60] for c in mid) + " (VINs before it kept, as the scan does)")
        uncertain.append("auth_mid_walk")

    rep.flags = flags
    rep.notes = notes
    if reasons:
        rep.verdict, rep.reasons = "reject", reasons
    elif uncertain:
        rep.verdict, rep.reasons = "uncertain", uncertain
    else:
        rep.verdict, rep.reasons = "ok", []
    return rep


# ── side effects the save paths share ──────────────────────────────────────────

def write_discovery_log(dealer_id: str, report: RecipeValidationReport, context: str = "", log_root: Path | None = None) -> Path | None:
    """Append the report to workspace/dealer_logs/<dealer_id>/discovery.md; a
    reject also gets its line in _learning/errors_index.md (the process doc)."""
    root = log_root or LOG_ROOT
    try:
        d = root / dealer_id
        d.mkdir(parents=True, exist_ok=True)
        p = d / "discovery.md"
        head = "" if p.exists() else f"# {dealer_id} — discovery\n\nProcess: docs/NETWORK_SCAN_PROCESS.md\n\n"
        with p.open("a", encoding="utf-8") as fh:
            fh.write(head + report.to_markdown(context).rstrip() + "\n\n")
        if report.verdict == "reject":
            idx = root / "_learning" / "errors_index.md"
            idx.parent.mkdir(parents=True, exist_ok=True)
            with idx.open("a", encoding="utf-8") as fh:
                fh.write(f"- {report.stamp} recipe_rejected:{report.reason_code} -> {dealer_id} "
                         f"({context or 'recipe validation'}; workspace/dealer_logs/{dealer_id}/discovery.md)\n")
        return p
    except OSError as exc:
        logger.debug("recipe validation log skipped [%s]: %s", dealer_id, exc)
        return None


def record_recipe_status(dealer_id: str, report: RecipeValidationReport, context: str = "") -> bool:
    """``scan_hints.recipe_status`` = ok | rejected:<reason> | uncertain:<reason>,
    plus the compact report, so the DB shows why a dealer has (or lacks) recipes."""
    try:
        from backend.scanner.recipe_store import set_scan_hints

        return bool(set_scan_hints(dealer_id, {
            "recipe_status": report.status,
            "recipe_validation": {**report.summary(), "stamp": report.stamp, "context": context},
        }))
    except Exception as exc:  # noqa: BLE001
        logger.debug("recipe_status hint skipped [%s]: %s", dealer_id, exc)
        return False


def gate_recipes(
    dealer_id: str,
    recipes: list[EndpointRecipe],
    *,
    base_url: str = "",
    dealer_name: str = "",
    place: dict[str, str] | None = None,
    fetch: Fetch | None = None,
    context: str = "synth",
    log_root: Path | None = None,
) -> tuple[list[EndpointRecipe], RecipeValidationReport]:
    """Validate the set and apply the verdict: ``reject`` returns ``[]`` (nothing
    to save) and logs the report; ``uncertain`` and ``ok`` return the recipes.
    Every outcome lands in discovery.md and ``scan_hints.recipe_status``."""
    report = validate_recipe_set(dealer_id, recipes, fetch, base_url=base_url, dealer_name=dealer_name, place=place)
    write_discovery_log(dealer_id, report, context, log_root)
    record_recipe_status(dealer_id, report, context)
    if report.verdict == "reject":
        logger.warning("recipe validation [%s] REJECT (%s): %s", dealer_id, context, "; ".join(report.reasons)[:300])
        return [], report
    if report.verdict == "uncertain":
        logger.info("recipe validation [%s] uncertain (%s): %s", dealer_id, context, "; ".join(report.reasons)[:300])
    else:
        logger.info("recipe validation [%s] ok (%s): %d VIN(s), site total %s", dealer_id, context, report.vins_total, report.site_total)
    return list(recipes), report


__all__ = [
    "RecipeCheck",
    "RecipeValidationReport",
    "SITE_TOTAL_EXTRACTORS",
    "extract_site_total",
    "gate_recipes",
    "normalize_condition",
    "one_condition_ok_ids",
    "platform_of",
    "recipe_condition_filter",
    "record_recipe_status",
    "validate_recipe_set",
    "write_discovery_log",
]
