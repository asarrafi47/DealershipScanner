"""Server-rendered page of /listings cards (crawlers, no-JS, first paint).

Web speed 2026-11 (PR 4). The listings grid is filled client-side from the area body of
``GET /api/listings/cars`` and paginated in the browser, so every /listings URL used to
serve the same document: ``?page=2`` was byte-identical to page 1 and a crawler or a
browser without JavaScript saw no car at all. When the URL carries a search and an area
is known, the page now carries the requested page of cards plus plain prev/next links.

Cost model (the TTFB must not grow with the inventory):

* The cards come from the SAME cached area body the API serves (``_cars_scope_cache`` in
  ``backend/routes/listings_api.py``, keyed by ZIP, snapped radius and hidden dealers),
  in the same order (the card store's ``(sort_bucket, sort_price, id)``, which is what
  the client's default "Best match" sort reproduces). Nothing here builds an area: when
  the body is not cached the page renders as before and one background build is started,
  which the client's own ``/api/listings/cars`` request then joins.
* Each cached body carries a compact :class:`ScopeIndex` (built with the body, ~50 bytes
  per car): ids, distances and dictionary-coded facet columns. A request filters that
  index (no JSON parsing) and reads only the page's cards from ``listings_grid_cards``
  by primary key.
* Filters the index cannot answer exactly like the client (colours, packages, dealer
  subset, inventory condition, CPO only, a non-default sort) skip the server page: the
  client renders it as before.
"""

from __future__ import annotations

import json
import math
import re
from array import array
from typing import Any, Iterable
from urllib.parse import urlencode

# Card keys mirrored by the client's facet filter (listings/filters.js
# carMatchesFacetFilters), compared case-insensitively after trimming (SC.normFilterStr).
_CI_COLUMNS = (
    ("make", "make"),
    ("model", "model"),
    ("trim", "trim"),
    ("fuel_type", "fuel_type"),
    ("transmission", "transmission"),
    ("drivetrain", "drivetrain"),
    ("forced_induction", "forced_induction"),
    ("body_style", "body_style"),
)

# URL params whose client semantics the index does not reproduce: a server page for
# them could disagree with what the client then shows, so the client renders alone.
_UNSUPPORTED_PARAMS = (
    "exterior_color", "interior_color", "package", "dealer_registry_id",
    "dealership_registry_id", "inventory_condition", "cpo_only",
    "engine_l_min", "engine_l_max", "engine_displacement_l_min", "engine_displacement_l_max",
)

# listings.html select options; any other value leaves the select on "Any".
_MAX_PRICE_OPTIONS = frozenset({"25000", "35000", "50000", "75000", "100000", "150000"})
_MAX_MILEAGE_OPTIONS = frozenset(
    str(m) for m in (5000, 10000, 15000, 20000, 25000, 30000, 40000, 50000, 75000, 100000)
)
PER_PAGE_CHOICES = (12, 24, 48)
DEFAULT_PER_PAGE = 24


def _norm(v: Any) -> str:
    """SC.normFilterStr: null/"" -> "", else trimmed lower-case text."""
    if v is None or v == "":
        return ""
    return _js_string(v).strip().lower()


def _js_string(v: Any) -> str:
    """``String(v)`` as JavaScript prints JSON scalars (4.0 -> "4", null -> "null")."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float) and math.isfinite(v) and v.is_integer():
        return str(int(v))
    return str(v)


def _num(v: Any) -> float:
    if isinstance(v, bool) or v is None:
        return math.nan
    if isinstance(v, (int, float)):
        return float(v)
    return math.nan


class ScopeIndex:
    """Facet columns of one cached area body, in body order."""

    __slots__ = ("ids", "dists", "prices", "miles", "cols", "vocabs", "nbytes")

    def __init__(self) -> None:
        self.ids = array("q")
        self.dists = array("f")
        self.prices = array("d")
        self.miles = array("d")
        self.cols: dict[str, array] = {}
        self.vocabs: dict[str, list[str]] = {}
        self.nbytes = 0

    def __len__(self) -> int:
        return len(self.ids)


def build_scope_index(cards_json: Iterable[str]) -> ScopeIndex:
    """Index the served cards (JSON strings, body order)."""
    idx = ScopeIndex()
    names = [col for _, col in _CI_COLUMNS] + ["cylinders"]
    codes: dict[str, dict[str, int]] = {n: {} for n in names}
    for n in names:
        idx.cols[n] = array("I")
        idx.vocabs[n] = []
    for raw in cards_json:
        try:
            c = json.loads(raw)
        except (TypeError, ValueError):
            continue
        if not isinstance(c, dict):
            continue
        try:
            cid = int(c.get("id"))
        except (TypeError, ValueError):
            continue
        idx.ids.append(cid)
        d = _num(c.get("distance_miles"))
        idx.dists.append(d if math.isfinite(d) else -1.0)
        idx.prices.append(_num(c.get("price")))
        idx.miles.append(_num(c.get("mileage")))
        for key, col in _CI_COLUMNS:
            _code(idx, codes, col, _norm(c.get(key)))
        _code(idx, codes, "cylinders", _js_string(c.get("cylinders")))
    idx.nbytes = (
        idx.ids.itemsize * len(idx.ids) * 4
        + sum(a.itemsize * len(a) for a in idx.cols.values())
        + sum(sum(len(s) + 50 for s in v) for v in idx.vocabs.values())
    )
    return idx


def _code(idx: ScopeIndex, codes: dict[str, dict[str, int]], col: str, value: str) -> None:
    table = codes[col]
    code = table.get(value)
    if code is None:
        code = len(idx.vocabs[col])
        idx.vocabs[col].append(value)
        table[value] = code
    idx.cols[col].append(code)


# ---------------------------------------------------------------------------
# What the client will filter on (its checked boxes / selects), from the URL
# ---------------------------------------------------------------------------


def effective_filters(active: dict, options: dict, args) -> dict | None:
    """The facet selections the client will apply on its first render, or None when the
    URL holds something the index cannot reproduce.

    The client filters on CHECKED boxes, and the template checks a box only for a URL
    value that is exactly one of its options, so a value with no option is no filter
    (``?make=toyota`` checks nothing). Same for the price/mileage selects."""
    for key in _UNSUPPORTED_PARAMS:
        if any(str(v).strip() for v in args.getlist(key)):
            return None
    sort = (args.get("sort") or "").strip()
    if sort and sort != "relevance":
        return None

    def picked(param: str, values: Iterable[Any]) -> list[str]:
        want = set(active.get(param) or [])
        return [str(v) for v in values if str(v) in want]

    out: dict[str, list[str]] = {}
    countries = picked("country", options.get("countries") or [])
    makes = picked("make", options.get("makes") or [])
    if countries:
        ctm = options.get("country_to_makes") or {}
        from_countries = [m for c in countries for m in (ctm.get(c) or [])]
        cnorm = {_norm(m) for m in from_countries}
        makes = [m for m in makes if _norm(m) in cnorm] if makes else from_countries
    out["make"] = makes
    model_want = set(active.get("model") or [])
    out["model"] = sorted({str(m) for _, m in (options.get("model_rows") or []) if m in model_want})
    trim_want = set(active.get("trim") or [])
    out["trim"] = sorted(
        {str(t or "") for _, _, t in (options.get("trim_rows") or []) if (t or "") in trim_want}
    )
    out["fuel_type"] = picked("fuel_type", options.get("fuel_types") or [])
    out["cylinders"] = picked("cylinders", options.get("cylinders") or [])
    out["transmission"] = picked("transmission", options.get("transmissions") or [])
    out["drivetrain"] = picked("drivetrain", options.get("drivetrains") or [])
    out["forced_induction"] = picked("forced_induction", options.get("forced_inductions") or [])
    out["body_style"] = picked("body_style", options.get("body_styles") or [])
    mp = str(active.get("max_price") or "")
    mm = str(active.get("max_mileage") or "")
    out["max_price"] = [mp] if mp in _MAX_PRICE_OPTIONS else []
    out["max_mileage"] = [mm] if mm in _MAX_MILEAGE_OPTIONS else []
    return out


def matching_positions(idx: ScopeIndex, eff: dict) -> list[int]:
    """Body positions of the cars the client's facet filter keeps."""
    checks: list[tuple[array, frozenset[int]]] = []
    for _, col in _CI_COLUMNS:
        wanted = eff.get(col) or []
        if not wanted:
            continue
        wn = {_norm(w) for w in wanted}
        allowed = frozenset(i for i, v in enumerate(idx.vocabs[col]) if v in wn)
        checks.append((idx.cols[col], allowed))
    cyls = eff.get("cylinders") or []
    if cyls:
        cw = {str(c) for c in cyls}
        checks.append((idx.cols["cylinders"], frozenset(i for i, v in enumerate(idx.vocabs["cylinders"]) if v in cw)))
    max_price = float(eff["max_price"][0]) if eff.get("max_price") else None
    max_miles = float(eff["max_mileage"][0]) if eff.get("max_mileage") else None

    n = len(idx)
    if not checks and max_price is None and max_miles is None:
        return list(range(n))
    out: list[int] = []
    prices, miles = idx.prices, idx.miles
    for i in range(n):
        ok = True
        for col, allowed in checks:
            if col[i] not in allowed:
                ok = False
                break
        if not ok:
            continue
        # JS: ``c.price > maxPrice`` is false for null/NaN, so those cars stay.
        if max_price is not None and prices[i] > max_price:
            continue
        if max_miles is not None and miles[i] > max_miles:
            continue
        out.append(i)
    return out


def page_number(args) -> int:
    """listings/zip_state.js pageFromSearch."""
    try:
        p = int(str(args.get("page") or "").strip())
    except ValueError:
        return 1
    return p if p > 0 else 1


def per_page(args) -> int:
    try:
        pp = int(str(args.get("per_page") or "").strip())
    except ValueError:
        return DEFAULT_PER_PAGE
    return pp if pp in PER_PAGE_CHOICES else DEFAULT_PER_PAGE


def page_href(path: str, args, page: int) -> str:
    """This URL with ``page`` replaced (page 1 drops the parameter)."""
    pairs = [(k, v) for k, v in args.items(multi=True) if k != "page"]
    if page > 1:
        pairs.append(("page", str(page)))
    qs = urlencode(pairs)
    return f"{path}?{qs}" if qs else path


def load_cards(ids: list[int]) -> dict[int, dict]:
    """Stored cards by id (primary-key lookup in ``listings_grid_cards``)."""
    if not ids:
        return {}
    from backend.db.repositories.grid_cards_repo import db_conn, ensure_grid_cards_table

    if not ensure_grid_cards_table():
        return {}
    marks = ",".join("?" for _ in ids)
    out: dict[int, dict] = {}
    with db_conn() as conn:
        rows = conn.execute(
            f"SELECT car_id, card_json FROM listings_grid_cards WHERE car_id IN ({marks})",
            tuple(ids),
        ).fetchall()
    for row in rows:
        try:
            cid, raw = int(row[0]), row[1]
            card = json.loads(raw)
        except (TypeError, ValueError, IndexError, KeyError):
            continue
        if isinstance(card, dict):
            out[cid] = card
    return out


def build_page(idx: ScopeIndex, eff: dict, args, path: str, zip_for_url: str = "") -> dict:
    """The requested page of cards (clamped like updatePaginationUI) plus pager links."""
    pos = matching_positions(idx, eff)
    total = len(pos)
    pp = per_page(args)
    total_pages = max(1, math.ceil(total / pp)) if total else 1
    page = min(page_number(args), total_pages)
    window = pos[(page - 1) * pp: page * pp]
    ids = [int(idx.ids[i]) for i in window]
    stored = load_cards(ids)
    cards = []
    for i in window:
        cid = int(idx.ids[i])
        card = stored.get(cid)
        if card is None:
            continue
        d = float(idx.dists[i])
        if d >= 0:
            card["distance_miles"] = round(d, 1)
        cards.append(card_view(card, zip_for_url))
    return {
        "cards": cards,
        "total": total,
        "page": page,
        "per_page": pp,
        "total_pages": total_pages,
        "start": (page - 1) * pp + 1 if total else 0,
        "end": min(page * pp, total),
        "prev_href": page_href(path, args, page - 1) if page > 1 else None,
        "next_href": page_href(path, args, page + 1) if page < total_pages else None,
    }


def active_chips(active: dict, options: dict, zip_code: str, radius: Any) -> list[dict]:
    """The chips main.js syncActiveFilterChips draws first, for the facets a server page
    supports (so the grid does not move down when the client fills the chip row)."""
    chips: list[dict] = []

    def add_checked(param: str, values: Iterable[Any], label=lambda v: str(v)) -> None:
        want = set(active.get(param) or [])
        seen: set[str] = set()
        for v in values:
            s = str(v)
            if s in want and s.strip().lower() not in seen:
                seen.add(s.strip().lower())
                chips.append({"param": param, "value": s, "label": label(v)})

    add_checked("make", options.get("makes") or [])
    add_checked("model", [m for _, m in (options.get("model_rows") or [])])
    add_checked("trim", [(t or "") for _, _, t in (options.get("trim_rows") or [])], lambda t: t or "—")
    add_checked("fuel_type", options.get("fuel_types") or [])
    add_checked("cylinders", options.get("cylinders") or [],
                lambda c: "Electric" if c == 0 else f"{c}-cylinder")
    add_checked("transmission", options.get("transmissions") or [])
    add_checked("drivetrain", options.get("drivetrains") or [])
    add_checked("forced_induction", options.get("forced_inductions") or [])
    add_checked("body_style", options.get("body_styles") or [])
    add_checked("country", options.get("countries") or [])
    z = (zip_code or "").strip()
    if re.fullmatch(r"\d{5}", z):
        chips.append({"param": "zip_code", "value": z, "label": z})
        if radius not in (None, ""):
            chips.append({"param": "radius", "value": str(radius), "label": f"{radius} mi radius"})
    mp = str(active.get("max_price") or "")
    if mp in _MAX_PRICE_OPTIONS:
        n = int(mp)
        chips.append({"param": "max_price", "value": mp,
                      "label": f"Under ${round(n / 1000)}k" if n >= 1000 else f"Under ${n}"})
    mm = str(active.get("max_mileage") or "")
    if mm in _MAX_MILEAGE_OPTIONS:
        chips.append({"param": "max_mileage", "value": mm, "label": f"Under {int(mm):,} mi"})
    return chips


# ---------------------------------------------------------------------------
# Card view model (mirrors listings/card.js renderCardHtml for the fields shown)
# ---------------------------------------------------------------------------

_DEALER_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_PLACEHOLDER = "/static/placeholder.svg"


def _condition_token(c: dict) -> str:
    """card.js cardConditionToken."""
    if c.get("is_cpo") in (True, 1, "1"):
        return "CPO"
    raw = str(c.get("condition") or "").strip().lower()
    if not raw or raw in ("—", "-"):
        return "Condition not listed"
    if "certified" in raw or re.search(r"\bcpo\b", raw):
        return "CPO"
    if raw == "new":
        return "New"
    return "Pre-owned"


def _usd(n: Any) -> str:
    """DS.formatUsd(n) default form."""
    if n is None or n == "":
        return "Call for Price"
    try:
        v = float(n)
    except (TypeError, ValueError):
        return "Call for Price"
    if v == 0 or not math.isfinite(v):
        return "Call for Price"
    return "$" + f"{round(v):,}"


def _dash_like(v: Any) -> bool:
    s = str(v or "").strip()
    return not s or s in ("—", "-", "--")


def _image_src(c: dict) -> str:
    gallery = c.get("gallery") if isinstance(c.get("gallery"), list) else []
    raw = str((gallery[0] if gallery and gallery[0] else None) or c.get("image_url") or "").strip()
    if raw.startswith(("http://", "https://")) or (raw.startswith("/") and not raw.startswith("//")):
        return raw
    return _PLACEHOLDER


def card_view(c: dict, zip_for_url: str) -> dict:
    """Plain strings for the server-rendered card (``_listings_ssr_card.html``)."""
    try:
        cid = int(c.get("id") or 0)
    except (TypeError, ValueError):
        cid = 0
    meta = [_condition_token(c)]
    if c.get("mileage_line_hidden") is not True:
        miles = _num(c.get("mileage"))
        if c.get("mileage_not_listed") is True or not math.isfinite(miles):
            meta.append("Mileage not listed")
        else:
            meta.append(f"{int(miles):,} mi")
    for key in ("fuel_type", "drivetrain"):
        if not _dash_like(c.get(key)):
            meta.append(str(c.get(key)))
    gallery = c.get("gallery") if isinstance(c.get("gallery"), list) else []
    try:
        photos = int(c.get("photo_count") or 0)
    except (TypeError, ValueError):
        photos = 0
    if photos <= 0:
        photos = len(gallery)
    dist = _num(c.get("distance_miles"))
    dist_text = ""
    if math.isfinite(dist) and dist >= 0:
        dist_text = f"{dist:.1f} mi away" if dist < 10 else f"{round(dist)} mi away"
    zip_clean = (zip_for_url or "").strip()
    zip_ok = bool(re.fullmatch(r"\d{5}", zip_clean))
    dealer_key = str(c.get("dealer_id") or "").strip()
    dealer_name = str(c.get("dealer_name") or "").strip()
    return {
        "id": cid,
        "href": f"/car/{cid}?zip_code={zip_clean}" if zip_ok else f"/car/{cid}",
        "image": _image_src(c),
        "title": str(c.get("title") or ""),
        "trim": str(c.get("trim") or ""),
        "price": _usd(c.get("price")),
        "payment_listed": c.get("payment_listed") is True,
        "meta": meta,
        "body_style": "" if _dash_like(c.get("body_style")) else str(c.get("body_style")),
        "distance": dist_text,
        "dealer_name": dealer_name,
        "dealer_href": f"/dealership/{dealer_key}" if dealer_name and _DEALER_KEY_RE.match(dealer_key) else "",
        "photos": f"{photos} photos" if photos > 1 else "",
        "incomplete": bool(c.get("public_incomplete")),
    }
