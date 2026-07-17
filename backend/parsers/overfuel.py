"""
Parser for the Overfuel platform (Next.js SSR SPA, ``generator=Overfuel``).

Overfuel embeds full per-vehicle records server-side in
``<script id="__NEXT_DATA__">`` at ``props.pageProps.inventory.results`` (25 per
page, paginated via ``?page=N``). This parser accepts EITHER the raw SRP HTML
string (it extracts ``__NEXT_DATA__`` itself, so it plugs into the html-page
recipe replay and the ``html_next_data`` recovery strategy) OR an
already-parsed ``__NEXT_DATA__`` dict.

Fields are Overfuel-native lowercase (vin, year, make, model, trim, price,
originalprice, specialprice, msrp, mileage, condition, certified,
exteriorcolor, interiorcolor, body, fuel, drivetrainstandard, stocknumber, url,
photos[] + featuredphoto, mpgcity, mpghwy, evrange, doors, seatingcapacity,
dealer{name,city,state}) — unmatched by any existing parser.
"""
import logging

from backend.parsers.base import norm_int, norm_str
from backend.scanner.scrapers.next_data_inventory import parse_next_data_json_from_html
from backend.utils.field_clean import clean_car_row_dict, normalize_optional_str

logger = logging.getLogger(__name__)


def _opt_str(v):
    if v is None:
        return None
    return normalize_optional_str(norm_str(v))


def _first(obj: dict, *keys) -> str | None:
    for k in keys:
        got = _opt_str(obj.get(k))
        if got:
            return got
    return None


def _results_from_next_data(nd) -> list:
    """Collect vehicle result dicts from a parsed __NEXT_DATA__ payload."""
    if not isinstance(nd, dict):
        return []
    props = nd.get("props")
    page_props = props.get("pageProps") if isinstance(props, dict) else None
    if not isinstance(page_props, dict):
        return []
    out: list = []
    seen_ids: set[int] = set()
    for key in ("inventory", "featured"):
        block = page_props.get(key)
        if isinstance(block, dict) and isinstance(block.get("results"), list):
            for r in block["results"]:
                if isinstance(r, dict) and id(r) not in seen_ids:
                    seen_ids.add(id(r))
                    out.append(r)
    return out


def _pick_price(doc: dict) -> float:
    for key in ("specialprice", "price", "originalprice", "msrp"):
        n = norm_int(doc.get(key))
        if n > 0:
            return float(n)
    return 0.0


def _gallery(doc: dict) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    featured = _opt_str(doc.get("featuredphoto"))
    if featured:
        seen.add(featured)
        out.append(featured)
    photos = doc.get("photos")
    if isinstance(photos, list):
        for p in photos:
            url = _opt_str(p.get("url") if isinstance(p, dict) else p)
            if url and url not in seen:
                seen.add(url)
                out.append(url)
    return out


def _condition(doc: dict) -> str:
    if doc.get("certified") is True:
        return "Certified"
    return _first(doc, "condition") or ""


def _detail_url(doc: dict, dealer_url: str, base_url: str) -> str | None:
    raw = _first(doc, "url", "vdpurl")
    if not raw:
        return None
    if raw.lower().startswith("http"):
        return raw
    root = (dealer_url or base_url or "").strip()
    if root and not root.lower().startswith("http"):
        root = "https://" + root
    if not root:
        return None
    return root.rstrip("/") + "/" + raw.lstrip("/")


def _map(doc: dict, base_url: str, dealer_id: str, dealer_name: str, dealer_url: str) -> dict | None:
    vin = norm_str(doc.get("vin") or doc.get("VIN") or "")
    if not vin or len(vin) < 11:
        return None
    gallery = _gallery(doc)
    hero = gallery[0] if gallery else ""
    row = {
        "vin": vin,
        "year": norm_int(doc.get("year")),
        "make": norm_str(doc.get("make")),
        "model": norm_str(doc.get("model")),
        "trim": norm_str(doc.get("trim")),
        "price": _pick_price(doc),
        "mileage": norm_int(doc.get("mileage")),
        "condition": _condition(doc),
        "exterior_color": _first(doc, "exteriorcolor", "exteriorcolorstandard") or "",
        "interior_color": _first(doc, "interiorcolor", "interiorcolorstandard") or "",
        "body_style": _first(doc, "body") or "",
        "drivetrain": _first(doc, "drivetrainstandard", "drivetrain") or "",
        "fuel_type": _first(doc, "fuel") or "",
        "engine_description": _first(doc, "engine") or "",
        "transmission": _first(doc, "transmission") or "",
        "stock_number": _first(doc, "stocknumber", "stock_number") or "",
        "image_url": hero,
        "gallery": gallery,
        "dealer_id": dealer_id,
        "dealer_name": dealer_name or dealer_id,
        "dealer_url": dealer_url or base_url,
        "mpg_city": norm_int(doc.get("mpgcity")) or None,
        "mpg_highway": norm_int(doc.get("mpghwy")) or None,
    }
    du = _detail_url(doc, dealer_url, base_url)
    if du:
        row["source_url"] = du
        row["_detail_url"] = du
    return row


def parse(raw_data, *, base_url: str = "", dealer_id: str = "", dealer_name: str = "", dealer_url: str = "") -> list[dict]:
    """Map an Overfuel SRP (HTML string or parsed __NEXT_DATA__ dict) to vehicle rows.

    Returns ``[]`` for anything with no Overfuel inventory results, so it is safe
    in the shared auto-detect fallback.
    """
    if isinstance(raw_data, str):
        nd = parse_next_data_json_from_html(raw_data)
    else:
        nd = raw_data
    out: list[dict] = []
    seen: set[str] = set()
    for doc in _results_from_next_data(nd):
        mapped = _map(doc, base_url, dealer_id, dealer_name, dealer_url)
        if mapped and mapped["vin"].upper() not in seen:
            seen.add(mapped["vin"].upper())
            out.append(clean_car_row_dict(mapped))
    return out
