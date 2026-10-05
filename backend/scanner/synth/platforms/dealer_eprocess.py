"""
Dealer eProcess (server-rendered SRP + JSON-LD) platform template. Its page
fetch (``_dep_fetch_page`` / ``_dep_fetch_html``) is shared by every HTML
page-walk template and lives in :mod:`backend.scanner.synth.http`.
"""
from __future__ import annotations

import logging
import re
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from backend.scanner.recipes import _unique_vins, EndpointRecipe, PAGINATION_DEP_SRP
from backend.scanner.synth.common import _origin
from backend.scanner.synth.http import _dep_fetch_page

logger = logging.getLogger("scanner")


# ── Platform: Dealer eProcess (server-rendered SRP + JSON-LD) ─────────────────

# Dealer eProcess ("Phoenix") dealers have NO JSON inventory API. Each SRP is a
# server-rendered HTML page carrying one JSON-LD @type:"Vehicle" block per card
# (12/page), paginated over plain HTTP with ?p=N. The recipe is therefore an HTML
# page-walk (PAGINATION_DEP_SRP) whose parser (dealer_eprocess) extracts VINs +
# prices from the embedded JSON-LD. The endpoint is parameterized by the dealer
# DOMAIN alone — no per-dealer account id / api key is needed.
#
# The friendly /used-inventory/ and /new-inventory/ paths usually filter by
# condition, so the FULL lot is the union of both feeds (mirrors Team Velocity's
# used+new). Some dealers configure BOTH paths to show the entire lot; we detect
# that (near-identical page-1 VINs) and emit a single recipe to avoid double work.
_DEP_SRP_PATHS = ("/used-inventory/", "/new-inventory/")
_DEP_COUNT_RE = re.compile(r'data-vehicle_count="(\d+)"')
# The SRP's "results per page" <select>: its numeric option values are the page
# sizes the server honours via ?ct=N (``ct=all`` is NOT honoured — it falls back
# to 12). Walking at the largest size cuts a 343-car new feed from 29 pages to 8.
# Anchor on the <select> TAG: the class name also appears in the page's inline
# CSS, thousands of bytes before the control. Option values are SRP URLs
# (``/search/used/?ct=48&tp=used``), so the size is read from their ``ct=``.
_DEP_PAGE_SIZE_SELECT_RE = re.compile(
    r'<select[^>]*results_per_page_controls__select[^>]*>(.*?)</select>', re.I | re.S
)
_DEP_OPTION_VALUE_RE = re.compile(r'<option[^>]*value="([^"]*)"', re.I)
_DEP_CT_RE = re.compile(r'(?:^|[?&;]|&amp;)ct=(\d+)')
_DEP_PAGE_SIZE_PARAM = "ct"
_DEP_PAGE_SIZE_CAP = 48


def _detect_dealer_eprocess(html: str, dealer_url: str) -> bool:
    return "dealereprocess" in html.lower()


def _dep_page_size(html: str) -> int | None:
    """Largest numeric page size the SRP's results-per-page control offers, or None."""
    m = _DEP_PAGE_SIZE_SELECT_RE.search(html)
    if not m:
        return None
    sizes: list[int] = []
    for value in _DEP_OPTION_VALUE_RE.findall(m.group(1)):
        value = value.strip()
        ct = _DEP_CT_RE.search(value)
        if value.isdigit():
            sizes.append(int(value))
        elif ct:
            sizes.append(int(ct.group(1)))
    sizes = [n for n in sizes if 0 < n <= _DEP_PAGE_SIZE_CAP]
    return max(sizes) if sizes else None


def _with_query_param(url: str, key: str, value: str) -> str:
    parts = urlparse(url)
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != key]
    query.append((key, value))
    return urlunparse(parts._replace(query=urlencode(query), fragment=""))


def _dep_page_vins(html: str, dealer_id: str, dealer_url: str) -> set[str]:
    from backend.parsers.dealer_eprocess import parse as _dep_parse

    return _unique_vins(_dep_parse(html, base_url=dealer_url, dealer_id=dealer_id, dealer_url=dealer_url))


_DEP_LC_LINK_RE = re.compile(r'href=["\'](?:https?://[^/"\']+)?(/search/[^"\'#]*?[?&;](?:amp;)?lc=(\d+)[^"\'#]*)["\']', re.I)


def _dep_store_lc(html: str, dealer_id: str) -> str | None:
    """The site's own store id for DEP's ``lc=`` facet, or None.

    Group sites share one DEP inventory: lexusofknoxville.com's
    ``/search/pre-owned/?tp=pre_owned`` walked 1,641 cars (Chevrolet, GMC, Ford,
    "available in Franklin, TN") and filed them under Lexus of Knoxville
    (2026-09-26). The SRP's own model-facet links carry ``lc=<store id>`` in a
    slug that names the store (``/search/new-lexus-nx-450h+-lexus-of-knoxville/
    ?cy=37922&lc=15578&md=12629``); with it the same feed returns the store's
    lot only. Single-store sites either carry no ``lc`` (Fremont, Groove) or one
    value that is their own (Capital Toyota 8243), so a lone value is trusted.
    """
    found: dict[str, int] = {}
    slug_hits: dict[str, int] = {}
    name_slug = re.sub(r"[^a-z0-9]+", "-", (dealer_id or "").lower().replace("-com", "").replace("-net", "")).strip("-")
    for m in _DEP_LC_LINK_RE.finditer(html or ""):
        path, lc = m.group(1), m.group(2)
        found[lc] = found.get(lc, 0) + 1
        slug = path.split("?")[0].lower()
        if name_slug and name_slug.replace("-", "") in slug.replace("-", ""):
            slug_hits[lc] = slug_hits.get(lc, 0) + 1
    if slug_hits:
        return max(slug_hits, key=slug_hits.get)
    if len(found) == 1:
        return next(iter(found))
    return None


def _dep_srp_recipe(
    dealer_id: str, origin: str, path: str
) -> tuple[EndpointRecipe, set[str]] | None:
    """Build one DEP SRP recipe, probing page 1 for real vehicles + total count.

    Returns ``(recipe, page1_vins)`` or ``None`` when page 1 yields no vehicles.
    """
    html, final_url = _dep_fetch_page(origin + path)
    if not html or "dealereprocess" not in html.lower():
        return None
    vins = _dep_page_vins(html, dealer_id, origin)
    if not vins:
        return None
    m = _DEP_COUNT_RE.search(html)
    total = int(m.group(1)) if m else None
    # The friendly path often 302s to the real SRP (``/search/used/?tp=used``);
    # record THAT so the ``?p=N`` walk lands on the filtered feed rather than on a
    # redirect that may drop the page query. Keep the final URL's own params.
    url = final_url if _origin(final_url) == origin else origin + path
    url = urlunparse(urlparse(url)._replace(fragment=""))
    lc = _dep_store_lc(html, dealer_id)
    if lc and f"lc={lc}" not in url:
        scoped = _with_query_param(url, "lc", lc)
        scoped_html, _ = _dep_fetch_page(scoped)
        scoped_vins = _dep_page_vins(scoped_html, dealer_id, origin) if scoped_html else set()
        if scoped_vins:
            m2 = _DEP_COUNT_RE.search(scoped_html or "")
            scoped_total = int(m2.group(1)) if m2 else None
            logger.info("DEP store scope [%s]: lc=%s narrows %s from %s to %s car(s)", dealer_id, lc, path, total, scoped_total)
            url, html, vins, total = scoped, scoped_html, scoped_vins, scoped_total
    size = _dep_page_size(html)
    if size and (total is None or total > len(vins)):
        bigger = _with_query_param(url, _DEP_PAGE_SIZE_PARAM, str(size))
        big_html, _ = _dep_fetch_page(bigger)
        big_vins = _dep_page_vins(big_html, dealer_id, origin) if big_html else set()
        # Only keep the larger page size when the server honoured it.
        if len(big_vins) > len(vins):
            url, vins = bigger, big_vins
    recipe = EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="GET",
        content_type="text/html",
        post_template=None,
        auth_headers={},
        pagination=PAGINATION_DEP_SRP,
        total_count=total,
        provider_hint="dealer_eprocess",
    )
    return recipe, vins


_DEP_NAV_SRP_RE = re.compile(r'href=["\'](?:https?://[^/"\']+)?(/search/(?:new|used|pre-owned|certified)[^"\'#?]*/?(?:\?[^"\'#]*)?)["\']', re.I)


def _dep_nav_srp_paths(html: str) -> list[str]:
    """SRP paths the site's own nav links to: one DEP variant serves
    /search/new-toyota/?mk=63&tp=new and /search/used-toyota/?tp=used instead
    of /new-inventory/ + /used-inventory/ (Capital Toyota, Groove Toyota,
    Lindsay Lexus of Alexandria: captured new-only recipes, 2026-09-26)."""
    out: list[str] = []
    for m in _DEP_NAV_SRP_RE.finditer(html or ""):
        p = m.group(1)
        base = p.split("?")[0].rstrip("/") + "/"
        if base not in {o.split("?")[0].rstrip("/") + "/" for o in out}:
            out.append(p if "?" in p else base)
    # keep one new-ish and one used-ish path at most, first seen wins
    keep: list[str] = []
    for want in ("new", "used", "pre-owned", "certified"):
        for p in out:
            if f"/search/{want}" in p.lower() and p not in keep:
                keep.append(p)
                break
    return keep


def _dep_condition_bucket(url: str) -> str:
    q = dict(parse_qsl(urlparse(url).query, keep_blank_values=True))
    tp = (q.get("tp") or "").lower()
    path = urlparse(url).path.lower()
    if tp == "certified" or "certified" in path:
        return "certified"
    if tp in ("used", "pre_owned", "preowned") or "used" in path or "pre-owned" in path:
        return "used"
    return "new"


def _dep_pick_per_condition(built: list[tuple[EndpointRecipe, set[str]]]) -> list[tuple[EndpointRecipe, set[str]]]:
    """Largest recipe (by total, then page-1 VINs) per condition bucket; certified
    only when no used/pre-owned feed exists (it is a subset of pre-owned)."""
    best: dict[str, tuple[EndpointRecipe, set[str]]] = {}
    for r, v in built:
        b = _dep_condition_bucket(r.url)
        cur = best.get(b)
        if cur is None or ((r.total_count or 0), len(v)) > ((cur[0].total_count or 0), len(cur[1])):
            best[b] = (r, v)
    out = [best[b] for b in ("new", "used") if b in best]
    if "certified" in best and "used" not in best:
        out.append(best["certified"])
    return out


def _synth_dealer_eprocess(dealer_id: str, dealer_url: str, html: str) -> list[EndpointRecipe]:
    """Emit DEP SRP recipes — used + new, deduped when both show the whole lot."""
    origin = _origin(dealer_url)
    paths = list(_DEP_SRP_PATHS) + [p for p in _dep_nav_srp_paths(html) if p not in _DEP_SRP_PATHS]
    built: list[tuple[EndpointRecipe, set[str]]] = [
        r for p in paths if (r := _dep_srp_recipe(dealer_id, origin, p)) is not None
    ]
    if len(built) > 2:
        # One recipe per condition bucket (tp=new / tp=used|pre_owned /
        # certified), the largest total in each. Ranking by page-1 VIN count
        # alone kept new + a model-page nav recipe (RX 350, 53 cars) and dropped
        # the 100-car pre-owned feed on lexusofchattanooga-com (2026-09-26).
        built = _dep_pick_per_condition(built)
    if not built:
        return []
    if len(built) == 2:
        (r_used, v_used), (r_new, v_new) = built
        # If the two friendly paths surface the same lot (page-1 VINs overlap
        # heavily and totals match), one recipe already covers everything.
        overlap = len(v_used & v_new)
        smaller = min(len(v_used), len(v_new)) or 1
        if overlap / smaller >= 0.5 and (r_used.total_count == r_new.total_count):
            return [r_used]
    return [r for r, _ in built]
