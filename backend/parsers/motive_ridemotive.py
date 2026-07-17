"""
Parser for the Motive (ridemotive / Roadster-Motive express-checkout) platform.

Motive dealers hydrate their SRP from a single shared Algolia index over plain
HTTP. The Algolia ``/1/indexes/{index}/query`` response is ``{"hits": [ {flat
vehicle record}, ... ], "nbHits": N, "nbPages": P, ...}`` — each hit is one
vehicle as a flat document with Motive-specific field names, so it needs a
dedicated mapper (the Typesense mapper's field names don't line up and Motive's
images are OPAQUE asset keys that must be composed into CDN URLs).

Image keys (``images`` / ``webp_images``) are bare asset ids like
``"qni1ckmfy37..."`` and must be composed as
``https://images.app.ridemotive.com/<key>``.
"""
import logging
import re

from backend.parsers.base import extract_mileage, extract_price, norm_int, norm_str
from backend.utils.field_clean import clean_car_row_dict, normalize_optional_str

logger = logging.getLogger(__name__)

_IMAGE_CDN = "https://images.app.ridemotive.com/"


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


def _iter_hits(raw_data):
    """Yield each Algolia hit dict from a single- or multi-query response."""
    if not isinstance(raw_data, dict):
        return
    hits = raw_data.get("hits")
    if isinstance(hits, list):
        for h in hits:
            if isinstance(h, dict):
                yield h
        return
    # Multi-query shape {"results": [{"hits": [...]}]}
    results = raw_data.get("results")
    if isinstance(results, list):
        for res in results:
            if isinstance(res, dict) and isinstance(res.get("hits"), list):
                for h in res["hits"]:
                    if isinstance(h, dict):
                        yield h


def _compose_image(key) -> str | None:
    if not isinstance(key, str):
        return None
    s = key.strip()
    if not s:
        return None
    if s.startswith("//"):
        return "https:" + s
    if s.lower().startswith("http"):
        return s
    return _IMAGE_CDN + s.lstrip("/")


def _gallery(doc: dict) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for field in ("images", "webp_images", "image_keys", "photos"):
        vals = doc.get(field)
        if not isinstance(vals, list):
            continue
        for v in vals:
            url = _compose_image(v.get("key") if isinstance(v, dict) else v)
            if url and url not in seen:
                seen.add(url)
                out.append(url)
        if out:
            break
    return out


def _pick_price(doc: dict) -> float:
    for key in ("price", "retail_price", "rebate_price", "msrp"):
        n = norm_int(doc.get(key))
        if n > 0:
            return float(n)
    return extract_price(doc)


def _condition(doc: dict) -> str:
    cond = _first(doc, "car_condition", "condition")
    return cond or ""


def _detail_url(doc: dict, dealer_url: str, base_url: str) -> str | None:
    raw = _first(doc, "vdp_url", "url", "vehicle_url", "slug")
    root = (dealer_url or base_url or "").strip()
    if not raw:
        return None
    if raw.lower().startswith("http"):
        return raw
    if root and not root.lower().startswith("http"):
        root = "https://" + root
    if not root:
        return None
    if not raw.startswith("/"):
        raw = "/inventory/" + raw
    return root.rstrip("/") + raw


_ENGINE_L_RE = re.compile(r"(\d(?:\.\d+)?)\s*[lL]\b")


def _engine_liters(engine: str | None) -> float | None:
    if not engine:
        return None
    m = _ENGINE_L_RE.search(engine)
    if not m:
        return None
    try:
        f = float(m.group(1))
        return round(f, 2) if 0 < f < 20 else None
    except (TypeError, ValueError):
        return None


def _map(doc: dict, base_url: str, dealer_id: str, dealer_name: str, dealer_url: str) -> dict | None:
    vin = norm_str(doc.get("vin") or doc.get("VIN") or "")
    if not vin or len(vin) < 11:
        return None
    gallery = _gallery(doc)
    hero = gallery[0] if gallery else ""
    engine = _first(doc, "engine", "engine_description")
    row = {
        "vin": vin,
        "year": norm_int(doc.get("make_year") or doc.get("year")),
        "make": norm_str(doc.get("make")),
        "model": norm_str(doc.get("model")),
        "trim": norm_str(doc.get("car_trim") or doc.get("trim")),
        "price": _pick_price(doc),
        "mileage": extract_mileage(doc) or norm_int(doc.get("odometer")),
        "condition": _condition(doc),
        "exterior_color": _first(doc, "exterior_color", "generic_exterior_color") or "",
        "interior_color": _first(doc, "interior_color", "generic_interior_color") or "",
        "body_style": _first(doc, "body", "body_style", "car_body") or "",
        "drivetrain": norm_str(doc.get("drivetrain")),
        "fuel_type": _first(doc, "fuel_type", "fuel") or "",
        "engine_description": engine or "",
        "transmission": norm_str(doc.get("transmission")),
        "stock_number": _first(doc, "stock_number", "stockNumber", "stock") or "",
        "image_url": hero,
        "gallery": gallery,
        "dealer_id": dealer_id,
        "dealer_name": dealer_name or dealer_id,
        "dealer_url": dealer_url or base_url,
        "mpg_city": norm_int(doc.get("city_mpg") or doc.get("mpg_city")) or None,
        "mpg_highway": norm_int(doc.get("highway_mpg") or doc.get("mpg_highway")) or None,
        "engine_l": _engine_liters(engine),
    }
    du = _detail_url(doc, dealer_url, base_url)
    if du:
        row["source_url"] = du
        row["_detail_url"] = du
    return row


def parse(raw_data, *, base_url: str = "", dealer_id: str = "", dealer_name: str = "", dealer_url: str = "") -> list[dict]:
    """Map a Motive/ridemotive Algolia query response to standard vehicle rows.

    Detects the Algolia hits shape and returns ``[]`` for anything else so it is
    safe in the shared auto-detect fallback.
    """
    out: list[dict] = []
    seen: set[str] = set()
    for doc in _iter_hits(raw_data):
        mapped = _map(doc, base_url, dealer_id, dealer_name, dealer_url)
        if mapped and mapped["vin"].upper() not in seen:
            seen.add(mapped["vin"].upper())
            out.append(clean_car_row_dict(mapped))
    return out
