"""Recipe-set validation against the site's own count (HTTP_ONLY_SCANS_PLAN Phase 3).

``validate_recipe`` (bottom of this module; re-exported by recipe_synth) answers one question — how many VINs does
this recipe yield — and every save path accepted anything with >= 5 (now :data:`RECIPE_MIN_VINS`). That is
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

``one_condition`` rejects only when the other side is known to exist: the site's
own census (carscommerce facets) or, for a set pinned to one side by URL, the
other side's own URL answering :data:`OTHER_SIDE_MIN_VINS` VINs. A pinned set
whose other side answers nothing (or cannot be derived, or whose sibling recipe
replayed with zero rows) is a used-only / new-only lot as far as the site says:
it saves as ``uncertain:one_condition_unverified`` and the post-scan assess
decides. Rejecting those looped forever: no recipe -> lifecycle re-synth
(rejected again) -> a browser capture a day -> rejected again (2026-09-28 review).

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
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qsl, urljoin, urlencode, urlparse, urlunparse

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
# Single-recipe yield probe (moved from synth/validate.py, audit F-6 follow-up).
from backend.scanner.net import ssrf
from backend.scanner.recipe_totals import (  # noqa: F401  (moved; re-exported for its callers and tests)
    RECIPE_MIN_VINS,
    request_signature,
    SITE_TOTAL_EXTRACTORS,
    _int_or_none,
    _total_carscommerce,
    _total_cosmos,
    _total_dealer_com,
    _total_generic,
    _total_list_length,
    extract_site_total,
    platform_of,
)
from backend.scanner.synth.http import _cosmos_get_json, _dep_fetch_html  # noqa: F401  (re-exported via the synth/validate shim)
from backend.scanner.synth.platforms.dealeron_cosmos import _COSMOS_PAGE_SIZE, _COSMOS_PATH  # noqa: F401  (re-exported via the synth/validate shim)
from backend.scanner.synth.platforms.team_velocity import _TEAM_VELOCITY_FEED  # noqa: F401  (re-exported via the synth/validate shim)

logger = logging.getLogger("scanner")

ROOT = Path(__file__).resolve().parents[2]
LOG_ROOT = Path(os.environ.get("DEALER_LOGS_ROOT") or (ROOT / "workspace" / "dealer_logs"))
ONE_CONDITION_OK_PATH = ROOT / "workspace" / "pipeline" / "one_condition_ok.txt"
# The tracked list ships in the image; the workspace file is a local, gitignored addition.
ONE_CONDITION_OK_TRACKED_PATH = ROOT / "backend" / "config" / "one_condition_ok.txt"

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
# A pinned-condition set (Team Velocity /inventory-used.json, DEP tp=used, an
# HTML /used-inventory/ walk) is one-condition only when the OTHER side is known
# to exist. The validator asks the other side's own URL: this many VINs there
# means the site sells both and the set misses a side. Below it the synth would
# have dropped that recipe anyway (ensure_recipe keeps a set of at least RECIPE_MIN_VINS), so
# the lot is used-only (or new-only) as far as the site says, and the set saves
# as ``uncertain:one_condition_unverified`` for the post-scan assess to judge.
# D-OD1 keeps the probe at the same number as the save floor, by decision rather than by echo.
OTHER_SIDE_MIN_VINS = RECIPE_MIN_VINS

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


_CONDITION_QUERY_KEYS = ("tp", "condition", "condition[]", "type", "inventory_type", "vehicletype")
_PATH_SEGMENT_RE = re.compile(r"/(new|used|pre-owned|preowned|certified)(?=[-/]|$)")


def _spell_like(sample: str, word: str) -> str:
    """``Used`` for a site that writes ``New``, ``used`` for one that writes ``new``."""
    return word.capitalize() if sample[:1].isupper() else word


def other_side_recipe(recipe: EndpointRecipe, only: str) -> EndpointRecipe | None:
    """The same recipe aimed at the other condition, derived from the URL alone.

    ``only`` is the side the set covers (``new`` / ``used``). Team Velocity feed
    stems (``/inventory-used.json`` -> ``/inventory-new.json``), condition query
    parameters (DEP ``tp=used``, PixelMotion ``condition[]=new``) and the path
    segments of the HTML walks (``/used-inventory/``, ``/inventory/new``) are
    swapped; certified / cpo count as the used side. ``None`` when the recipe
    is pinned in its body (typesense ``filter_by``, carscommerce facets) or the
    URL carries no condition: the caller then has nothing to probe.
    """
    other = "new" if only == "used" else "used"
    parts = urlparse(recipe.url or "")
    path, query, changed = parts.path, parts.query, False
    stem = re.search(r"-(new|used|cpo|certified)\.json$", path, re.I)
    if stem and _side(normalize_condition(stem.group(1))) == only:
        path = path[:stem.start(1)] + other + path[stem.end(1):]
        changed = True
    pairs = parse_qsl(query, keep_blank_values=True)
    out_pairs: list[tuple[str, str]] = []
    for k, v in pairs:
        if k.lower() in _CONDITION_QUERY_KEYS and _side(normalize_condition(v)) == only and normalize_condition(v) != "unknown":
            out_pairs.append((k, _spell_like(v, other)))
            changed = True
        else:
            out_pairs.append((k, v))
    if changed:
        query = urlencode(out_pairs, safe="[]/,:")
    if not changed and recipe.pagination in (PAGINATION_DEP_SRP, PAGINATION_HTML_PAGE, PAGINATION_JAZEL_SRP):
        seg = _PATH_SEGMENT_RE.search(path.lower())
        if seg and _side(normalize_condition(seg.group(1))) == only:
            path = path[:seg.start(1)] + other + path[seg.end(1):]
            changed = True
    if not changed:
        return None
    url = parts._replace(path=path, query=query).geturl()
    return replace(recipe, url=url, vehicle_rows=0, total_count=None)


def _read_id_list(path: Path) -> set[str]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return set()
    return {ln.split("#", 1)[0].strip() for ln in text.splitlines() if ln.split("#", 1)[0].strip()}


def one_condition_ok_ids(path: Path | None = None) -> set[str]:
    """Dealers whose lot legitimately carries a single condition: one id per line,
    ``#`` comments. The one loader (recipe validation, assess and the retirement
    policy): without *path* it is the union of the tracked backend/config/one_condition_ok.txt
    and the local workspace/pipeline/one_condition_ok.txt; with *path*, that file alone."""
    if path is not None:
        return _read_id_list(path)
    return _read_id_list(ONE_CONDITION_OK_TRACKED_PATH) | _read_id_list(ONE_CONDITION_OK_PATH)


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
    note: str = ""


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
    # The other side's own URL, asked when the pages read show one condition and
    # no census exists: {url, status, vins, other_vins, error}.
    other_side_probe: dict[str, Any] | None = None

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
        if self.other_side_probe:
            L.append(f"- other side probed: {json.dumps(self.other_side_probe)}")
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
                place: dict[str, str] | None) -> tuple[list[dict], set[str]]:
    """This store's rows for one page and the VINs the rooftop gate refused.

    Parsed through ``attribution.parse_page`` with a ``DealerCtx`` over *place*, the
    call the scan makes (recipes.py). *place* is already complete: the caller-side
    lookup is :func:`_store_place_for`, once per validation call."""
    from backend.attribution import DealerCtx, gate_place, parse_page

    ctx = DealerCtx(dealer_id, dealer_name, base_url, gate_place(place))
    refused: list[dict] = []
    rows = parse_page(recipe.provider_hint or "", parsed, ctx, refused,
                      trust_feed_scope=recipe_is_store_scoped(recipe, dealer_name))
    return list(rows or []), _unique_vins(refused)


def _store_place_for(base_url: str, dealer_id: str, place: dict[str, str] | None) -> dict[str, str]:
    """The place the scan would gate this store with: ``attribution.store_place``
    (registry plus the hinted street), merged key by key UNDER the caller's *place*
    (the caller wins). Read once per validation call, never per page. Any lookup
    failure (no DB) falls back silently to the caller's place, or ``{}``."""
    from backend.attribution import gate_place

    caller = gate_place(place)
    try:
        import backend.attribution as attribution

        found = gate_place(attribution.store_place(base_url, dealer_id))
    except Exception as exc:  # noqa: BLE001 - attribution help must never break a validation
        logger.debug("store place lookup skipped [%s]: %s", dealer_id, exc)
        found = {}
    return {**found, **{k: v for k, v in caller.items() if v}}


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
            if recipe.method != "GET":
                chk.error = "post_template is not JSON"
                return chk, set(), {}, None, None
            # D-OD6: a GET replays without a body, as the scan does (recipes.py)
            chk.note = "post_template is not JSON; replayed without a body"
    if recipe.method != "GET" and template is None:
        chk.error = "POST recipe without a body"
        return chk, set(), {}, None, None
    vins: set[str] = set()
    by_cond: dict[str, set[str]] = {}
    n_pages = 1 if recipe.pagination == PAGINATION_NONE else max(1, pages)
    page1_parsed: Any = None
    raw_vins: set[str] = set()
    page1_raw = 0
    for i in range(n_pages):
        body = _mutate_for_page(recipe, template, i) if template is not None else None
        url = _url_for_page(recipe, i)
        rec: dict[str, Any] = {"page": i + 1}
        try:
            # A recipe is data from outside: its endpoint host must be globally
            # reachable before any fetch (P16.4). Relative URLs resolve on base_url.
            ssrf.check_url(urljoin(base_url, url))
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
        chk.gate_rejected += len(refused)
        page_vins = _unique_vins(rows)
        new = page_vins - vins
        # Termination reads RAW rows (kept plus refused), as the scan's walk does: a page
        # of nothing but a group feed's sibling rooftops holds no kept VIN yet is not the end.
        new_raw = (page_vins | refused) - raw_vins
        rec["rows"] = len(rows)
        rec["new_vins"] = len(new)
        rec["raw_new_vins"] = len(new_raw)
        chk.pages.append(rec)
        for r in rows:
            v = str(r.get("vin") or "").strip().upper()
            if v and v in new:
                by_cond.setdefault(normalize_condition(r.get("condition")), set()).add(v)
        vins |= new
        raw_vins |= new_raw
        if i == 0:
            page1_raw = len(new_raw)
        if not new_raw:
            chk.exhausted = True
            break
        if chk.site_total is not None and len(raw_vins) >= chk.site_total:
            chk.exhausted = True
            break
    if recipe.pagination == PAGINATION_NONE:
        chk.exhausted = True  # single-shot: there is no further page to read
    chk.vins = len(vins)
    chk.per_condition = {c: len(s) for c, s in sorted(by_cond.items())}
    # A short page is a 200 that adds nothing (not even a refused sibling row) while the
    # site says more exists; a 401/403/5xx on page 2 is an auth or edge problem, reported as such.
    if (n_pages > 1 and len(chk.pages) >= 2 and chk.pages[1].get("status") == 200 and chk.pages[1].get("raw_new_vins", 0) == 0
            and chk.site_total and page1_raw < chk.site_total * SHORT_PAGE_TOLERANCE):
        chk.short_page = True
    return chk, vins, by_cond, template, page1_parsed


@dataclass
class RecipeChecks:
    """What :func:`check_all` read: one walk per recipe plus the context
    :func:`judge` needs (its census and other-side probes fetch again)."""

    dealer_id: str
    base_url: str
    dealer_name: str
    place: dict[str, str]
    pages: int
    fetch: Fetch
    walks: list[tuple[EndpointRecipe, RecipeCheck, set[str], dict[str, set[str]], Any]] = field(default_factory=list)

    def keep(self, recipes: list[EndpointRecipe]) -> "RecipeChecks":
        """The same read restricted to *recipes* (by identity): a save path that
        filters candidates by its floors judges only the ones it keeps, from the
        walk it already paid for."""
        ids = {id(r) for r in recipes}
        return RecipeChecks(self.dealer_id, self.base_url, self.dealer_name, self.place, self.pages, self.fetch,
                            [w for w in self.walks if id(w[0]) in ids])


def check_all(
    dealer_id: str,
    recipes: list[EndpointRecipe],
    fetch: Fetch | None = None,
    *,
    base_url: str = "",
    dealer_name: str = "",
    place: dict[str, str] | None = None,
    pages: int = PAGES_REPLAYED,
) -> RecipeChecks:
    """Walk every recipe once (*pages* each) and keep what was read. No verdict:
    :func:`judge` decides from these, so a save path can walk once and judge the
    cached checks against whatever floors apply.

    *fetch* defaults to ``recipes._replay_request`` (the scan's own request
    function). *place* is the store's postal place for the rooftop gate; the scan's
    own store place (:func:`_store_place_for`) is read once here and the caller's
    keys win."""
    fetch = fetch or _replay_request
    if not base_url:
        first = next((r.url for r in recipes if r.url), "")
        p = urlparse(first)
        base_url = f"{p.scheme}://{p.netloc}" if p.netloc else ""
    dealer_name = dealer_name or dealer_id
    place = _store_place_for(base_url, dealer_id, place) if recipes else dict(place or {})  # once per call, not per page
    out = RecipeChecks(dealer_id, base_url, dealer_name, place, pages, fetch)
    for r in recipes:
        out.walks.append((r, *_check_recipe(
            r, fetch=fetch, base_url=base_url, dealer_id=dealer_id, dealer_name=dealer_name, place=place, pages=pages)))
    return out


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
    checks: RecipeChecks | None = None,
) -> RecipeValidationReport:
    """Judge *recipes* as the set the scan will replay for *dealer_id*:
    :func:`check_all` then :func:`judge`. Pass *checks* (a prior ``check_all``) to
    judge a walk already read instead of walking again."""
    if checks is None:
        checks = check_all(dealer_id, recipes, fetch, base_url=base_url, dealer_name=dealer_name, place=place, pages=pages)
    return judge(checks, one_condition_ok=one_condition_ok)


def judge(checks: RecipeChecks, *, one_condition_ok: set[str] | None = None) -> RecipeValidationReport:
    """The verdict over walks :func:`check_all` read."""
    dealer_id, fetch, base_url, dealer_name, place, pages = (
        checks.dealer_id, checks.fetch, checks.base_url, checks.dealer_name, checks.place, checks.pages)
    ok_ids = one_condition_ok if one_condition_ok is not None else one_condition_ok_ids()
    rep = RecipeValidationReport(dealer_id=dealer_id, pages_replayed=pages,
                                 stamp=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    flags = {"one_condition": False, "section_scoped": False, "short_page": False, "auth_needed": False, "zero_rows": False}
    if not checks.walks:
        flags["zero_rows"] = True
        rep.flags = flags
        rep.verdict, rep.reasons = "reject", ["zero_rows: no recipe to validate"]
        return rep

    all_vins: set[str] = set()
    by_cond: dict[str, set[str]] = {}
    checked: list[tuple[EndpointRecipe, RecipeCheck, Any]] = []
    for r, chk, vins, conds, template, _page1 in checks.walks:
        rep.recipes.append(chk)
        checked.append((r, chk, template))
        all_vins |= vins
        for c, s_ in conds.items():
            by_cond.setdefault(c, set()).update(s_)
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
            other = "new" if only == "used" else "used"
            if pinned and len(pinned_sides) == 2:
                # The set already carries the other side's own recipe and it
                # answered with no rows: the site's own feed saying 0 is
                # evidence of a one-sided lot, not of a missing side (a
                # used-only Team Velocity store has an empty /inventory-new.json).
                other_chk = next((c for c in rep.recipes if any(_side(p) == other for p in c.pinned_conditions)), None)
                st = other_chk.pages[0].get("status") if other_chk and other_chk.pages else None
                notes.append(f"one_condition_unverified: the {other} recipe ({(other_chk.url if other_chk else '?')[:60]}) "
                             f"answered {st} with no rows; every row is {only} — assess decides after the scan")
            elif pinned and len(pinned_sides) == 1:
                # A condition filter is proof the site has sections, not that
                # the other one holds cars: per-condition synths (Team Velocity,
                # DEP tp=, HTML /used-inventory/) emit one recipe per side and
                # ensure_recipe drops an empty side before the gate. Ask the
                # other side itself before calling the set one-condition.
                probe_src = next((r for r, c, _t in checked if c.pinned_conditions and c.vins), None)
                probe = other_side_recipe(probe_src, only) if probe_src is not None else None
                if probe is None:
                    notes.append(f"one_condition_unverified: recipes pinned to {sorted(pinned)}; no URL for the {other} side "
                                 f"can be derived, so its existence is unknown — assess decides after the scan")
                else:
                    p_chk, p_vins, p_conds, _t, _p1 = _check_recipe(
                        probe, fetch=fetch, base_url=base_url, dealer_id=dealer_id, dealer_name=dealer_name, place=place, pages=1)
                    rep.other_side_probe = {"url": probe.url, "status": p_chk.pages[0].get("status") if p_chk.pages else None,
                                            "vins": len(p_vins - all_vins), "error": p_chk.error or None}
                    # Rows that carry a condition count only on the other side: a
                    # ?tp=new that redirects to the used SRP must not read as
                    # "new cars exist". Only condition-less rows fall back to
                    # VINs the pages read did not show.
                    known = {c: s for c, s in p_conds.items() if c != "unknown"}
                    other_vins = {v for c, s in known.items() if _side(c) == other for v in s} if known else (p_vins - all_vins)
                    n_other = len(other_vins)
                    rep.other_side_probe["other_vins"] = n_other
                    if n_other >= OTHER_SIDE_MIN_VINS:
                        site_has_both = True
                        how = (f"recipes pinned to {sorted(pinned)} with no recipe for the other side; "
                               f"the {other} side answers {n_other} VIN(s) at {probe.url[:80]}")
                    else:
                        st = rep.other_side_probe["status"]
                        notes.append(f"one_condition_unverified: recipes pinned to {sorted(pinned)}; the {other} side at "
                                     f"{probe.url[:60]} answered {st} with {n_other} VIN(s) (under the {OTHER_SIDE_MIN_VINS} the synth keeps), "
                                     f"so the lot reads {only}-only — assess decides after the scan")
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
    for c in rep.recipes:
        if c.note:
            notes.append(f"{c.url[:60]}: {c.note}")

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

_URL_QUERY = re.compile(r"(https?://[^\s?#)\]>\"'`]+)\?[^\s)\]>\"'`]*")


def strip_url_queries(text: str) -> str:
    """``text`` with every ``?query`` removed from the URLs in it: a recipe URL's query can
    carry an API key, and a log is not the place for one."""
    return _URL_QUERY.sub(r"\1", text)


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
            fh.write(head + strip_url_queries(report.to_markdown(context)).rstrip() + "\n\n")
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

        from backend.scanner.recipes import blocked_status_value, egress_tag, record_blocked

        status = report.status
        flags = report.flags or {}
        validation = {**report.summary(), "stamp": report.stamp, "context": context}
        if egress_tag() and report.verdict == "reject" and flags.get("auth_needed") and flags.get("zero_rows"):
            # Every recipe answered 401/403 from a tagged (datacenter) host: that
            # is this host's IP, not a dead recipe. Record the block under this
            # host's tag (P7.6) and leave recipe_status to the home verdict.
            return record_blocked(dealer_id, blocked_status_value("auth_needed"), {"recipe_validation": validation})
        return bool(set_scan_hints(dealer_id, {"recipe_status": status, "recipe_validation": validation}))
    except Exception as exc:  # noqa: BLE001
        logger.debug("recipe_status hint skipped [%s]: %s", dealer_id, exc)
        return False


def walk_candidates(
    dealer_id: str,
    candidates: list[EndpointRecipe],
    *,
    base_url: str,
    dealer_name: str = "",
    place: dict[str, str] | None = None,
    fetch: Fetch | None = None,
    pages: int | None = None,
) -> RecipeChecks:
    """The one full walk a save path pays for (P12B.4): every candidate read to
    :data:`_VALIDATE_MAX_PAGES` through the scan's replay request behind synth's
    fetch clock. The caller counts ``chk.vins`` per walk (that is ``vehicle_rows``),
    filters with its floors, then hands ``checks.keep(kept)`` to :func:`gate_recipes`
    so the verdict is judged from these reads instead of a second 2-page walk."""
    return check_all(dealer_id, candidates, fetch or _paced_replay, base_url=base_url, dealer_name=dealer_name,
                     place=place, pages=pages or _VALIDATE_MAX_PAGES)


def shared_with_other_dealers(dealer_id: str, recipes: list[EndpointRecipe]) -> dict[str, str]:
    """``{other dealer_id: url}`` for each other dealer whose live stored recipe is the
    same request (method, host+path+query, body) as one of *recipes*. Empty when the
    recipe store cannot be read."""
    from backend.scanner import recipe_store

    out: dict[str, str] = {}
    for r in recipes:
        sig = request_signature(r.method, r.url, r.post_template)
        for other, rows in recipe_store.db_other_dealers_with_url(dealer_id, r.url):
            if other in out:
                continue
            for row in rows:
                if not row.get("stale") and request_signature(row.get("method"), row.get("url"), row.get("post_template")) == sig:
                    out[other] = r.url
                    break
    return out


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
    record: bool = True,
    pages: int | None = None,
    checks: RecipeChecks | None = None,
    allow_shared: bool = False,
) -> tuple[list[EndpointRecipe], RecipeValidationReport]:
    """Validate the set and apply the verdict: ``reject`` returns ``[]`` (nothing
    to save) and logs the report; ``uncertain`` and ``ok`` return the recipes.

    With ``record=True`` (the default) every outcome lands in discovery.md (a
    reject also in ``_learning/errors_index.md``) and ``scan_hints.recipe_status``.
    ``record=False`` is for a caller that will not save the set (a dry run, or a
    dealer that keeps its healthy recipe): the same verdict comes back and
    nothing is written, so a status that describes no saved set never reaches
    the store.

    *pages* is how many pages each recipe is read for (default
    :data:`PAGES_REPLAYED`); *checks* is a walk already read by :func:`check_all`,
    judged as it is instead of fetching again.

    A set holding a request (method, URL, body) that another dealer's stored recipes
    already hold is refused as ``duplicate_endpoint`` (CF-3: a byte copy of another store's
    recipe served 1,284 rows from the wrong site). *allow_shared* skips that check for a
    group feed a person has confirmed is shared."""
    kw: dict[str, Any] = {}
    if pages is not None:
        kw["pages"] = pages
    if checks is not None:
        kw["checks"] = checks
    report = validate_recipe_set(dealer_id, recipes, fetch, base_url=base_url, dealer_name=dealer_name, place=place, **kw)
    shared = {} if allow_shared or report.verdict == "reject" else shared_with_other_dealers(dealer_id, recipes)
    if shared:
        report.verdict = "reject"
        report.flags["duplicate_endpoint"] = True
        report.reasons.insert(0, "duplicate_endpoint: same request stored for " + ", ".join(
            f"{d} ({u[:60]})" for d, u in sorted(shared.items())[:5]))
    if report.verdict in ("ok", "uncertain"):
        # Gate provenance: the scan admits a sub-floor pinned side only under these.
        stamp = time.time()
        for r in recipes:
            r.gate_verdict, r.gate_at = report.verdict, stamp
    if record:
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
    "RecipeChecks",
    "RecipeValidationReport",
    "SITE_TOTAL_EXTRACTORS",
    "extract_site_total",
    "check_all",
    "gate_recipes",
    "judge",
    "normalize_condition",
    "one_condition_ok_ids",
    "other_side_recipe",
    "platform_of",
    "recipe_condition_filter",
    "record_recipe_status",
    "shared_with_other_dealers",
    "validate_recipe_set",
    "walk_candidates",
    "write_discovery_log",
]


# ── Validation (replay over HTTP, count real VINs) ────────────────────────────

_VALIDATE_MAX_PAGES = 40


def _paced_replay(recipe: EndpointRecipe, body: Any, base_url: str, url: str | None = None) -> tuple[int, Any | None]:
    """The scan's own ``_replay_request`` behind synth's fetch clock, so a sweep
    that sets ``SCANNER_SYNTH_FETCH_DELAY`` (4 s in national_scan.py) spaces its
    validation requests exactly as the old synth walkers did. No delay set: no wait."""
    from backend.scanner.synth.http import _pace

    _pace()
    return _replay_request(recipe, body, base_url, url)


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

    A thin count over the one replay walker (:func:`_check_recipe`): the scan's
    own request function (paced), the scan's pagination shapes, and the same
    rooftop gate over the store's place (*place* wins key by key). A GET whose
    ``post_template`` is not JSON replays without a body; a POST without a
    parseable body counts zero. No browser, no DB writes, no recipe-file mutation.
    """
    place = _store_place_for(base_url, dealer_id, place)
    chk = _check_recipe(recipe, fetch=_paced_replay, base_url=base_url, dealer_id=dealer_id,
                        dealer_name=dealer_name, place=place, pages=max_pages)[0]
    return chk.vins


# One release of one-line delegates: recipe_synth's re-exports, the synth/validate
# shim and .claude/workflows/dealer-discovery.js still name these walkers.
def _validate_json_feed(recipe: EndpointRecipe, base_url: str, dealer_id: str, dealer_name: str, max_pages: int, place: dict[str, str] | None = None) -> int:
    return validate_recipe(recipe, base_url, dealer_id, dealer_name, max_pages=max_pages, place=place)


def _validate_dep(recipe: EndpointRecipe, base_url: str, dealer_id: str, dealer_name: str, max_pages: int, place: dict[str, str] | None = None) -> int:
    return validate_recipe(recipe, base_url, dealer_id, dealer_name, max_pages=max_pages, place=place)


def _validate_html_walk(recipe: EndpointRecipe, base_url: str, dealer_id: str, dealer_name: str, max_pages: int, place: dict[str, str] | None = None) -> int:
    return validate_recipe(recipe, base_url, dealer_id, dealer_name, max_pages=max_pages, place=place)


def _validate_cosmos(recipe: EndpointRecipe, base_url: str, dealer_id: str, dealer_name: str, max_pages: int, place: dict[str, str] | None = None) -> int:
    return validate_recipe(recipe, base_url, dealer_id, dealer_name, max_pages=max_pages, place=place)
