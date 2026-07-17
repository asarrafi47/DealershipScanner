"""
Parser for the sister.tv (Sincro / DealerFire Elasticsearch) search platform.

Some Fletcher Jones-style dealer groups (e.g. Audi Fletcher Jones Costa Mesa,
Fletcher Jones Mercedes) serve their SRP/VDP inventory from a hosted
Elasticsearch index at ``es-data-v2.sister.tv/vehicles/inventory/_search``. The
recipe capture replays fine over plain HTTP (GET, no auth), but the payload is
the raw Elasticsearch shape ``{"hits": {"total": N, "hits": [{"_source": {...}}]}}``
rather than any OEM platform shape, so it needs a dedicated ``_source`` mapper.

Each ``_source`` is one vehicle. Field names are the sister.tv flavor:
vin, year (str), make, model, trim, price_web / price_msrp (ints), miles (str),
ext_color, int_color, body, drivetrain, fuel_type, engine, transmission,
stock_no, new_used ("Used"/"New"), is_certified ("0"/"1"), model_code,
display_pics / detail_pics / third_party_pics (lists of image URLs), and
current_vdp_url_detail (list of {link, project_id, viewCount}) which carries the
dealer's real absolute VDP url. The record's own ``dealer_name`` / ``dealer_url``
are typically blank, so the dealer domain is recovered from the VDP link.
"""
import logging
from urllib.parse import urljoin, urlsplit

from backend.parsers.base import (
    extract_mileage,
    extract_price,
    norm_int,
    norm_str,
)
from backend.utils.field_clean import clean_car_row_dict, normalize_optional_str

logger = logging.getLogger(__name__)

# _source keys that mark a payload as a sister.tv vehicle (guards auto-detect
# against unrelated Elasticsearch responses).
_MARKER_KEYS = ("library_id", "current_vdp_url_detail", "price_web", "stock_no")


def _opt_str(v) -> str | None:
    if v is None:
        return None
    return normalize_optional_str(norm_str(v))


def _first(obj: dict, *keys) -> str | None:
    for k in keys:
        got = _opt_str(obj.get(k))
        if got:
            return got
    return None


def _iter_sources(raw_data):
    """Yield each ``_source`` dict from an Elasticsearch _search response."""
    if not isinstance(raw_data, dict):
        return
    hits = raw_data.get("hits")
    if not (isinstance(hits, dict) and isinstance(hits.get("hits"), list)):
        return
    for hit in hits["hits"]:
        if isinstance(hit, dict) and isinstance(hit.get("_source"), dict):
            yield hit["_source"]


def _looks_like_sister_tv(src: dict) -> bool:
    return any(k in src for k in _MARKER_KEYS)


def _pick_price(src: dict) -> float:
    # price_web is the platform's rendered web price; fall back to msrp, then the
    # shared extractor for any dollar-formatted string fields.
    for key in ("price_web", "price_msrp"):
        n = norm_int(src.get(key))
        if n > 0:
            return float(n)
    return extract_price(src)


def _gallery(src: dict) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for field in ("display_pics", "detail_pics", "third_party_pics", "stock_pics"):
        urls = src.get(field)
        if not isinstance(urls, list):
            continue
        for u in urls:
            if not isinstance(u, str):
                continue
            s = u.strip()
            if s.startswith("//"):
                s = "https:" + s
            if not s.lower().startswith("https://") or s in seen:
                continue
            seen.add(s)
            out.append(s)
    return out


def _best_vdp_link(src: dict) -> str | None:
    """Pick the best VDP link from current_vdp_url_detail (highest viewCount)."""
    detail = src.get("current_vdp_url_detail")
    if not isinstance(detail, list):
        # occasionally a bare string/url in an alternate field
        return _first(src, "current_vdp_url", "vdp_url", "url")
    best: str | None = None
    best_views = -1
    for item in detail:
        if not isinstance(item, dict):
            continue
        link = _opt_str(item.get("link"))
        if not link or not link.lower().startswith("https://"):
            continue
        views = norm_int(item.get("viewCount"))
        if views > best_views:
            best_views = views
            best = link
    if best:
        return best
    # no https link — accept the first usable link of any scheme
    for item in detail:
        if isinstance(item, dict):
            link = _opt_str(item.get("link"))
            if link:
                return link
    return None


def _dealer_url_from_link(link: str | None) -> str | None:
    if not link:
        return None
    parts = urlsplit(link if link.lower().startswith("http") else "https://" + link.lstrip("/"))
    if parts.netloc:
        return f"https://{parts.netloc}"
    return None


def _detail_url(src: dict, dealer_url: str, base_url: str) -> str | None:
    raw = _best_vdp_link(src)
    if not raw:
        return None
    if raw.lower().startswith("http"):
        return raw
    if raw.startswith("//"):
        return "https:" + raw
    # site-relative — resolve against the dealer's real domain, never sister.tv.
    root = (dealer_url or base_url or "").strip()
    if root and not root.lower().startswith("http"):
        root = "https://" + root
    if not root:
        return None
    try:
        return urljoin(root.rstrip("/") + "/", raw.lstrip("/"))
    except Exception:
        return None


def _condition(src: dict) -> str:
    if str(src.get("is_certified") or "").strip() in ("1", "true", "True"):
        return "Certified"
    nu = _first(src, "new_used")
    return nu or ""


def _map_source(src: dict, base_url: str, dealer_id: str, dealer_name: str, dealer_url: str) -> dict | None:
    vin = norm_str(src.get("vin") or src.get("VIN") or "")
    if not vin:
        return None

    vdp = _detail_url(src, dealer_url, base_url)
    # Records carry blank dealer_url; recover the real domain from the VDP link.
    resolved_dealer_url = (
        dealer_url
        or _opt_str(src.get("dealer_url"))
        or _dealer_url_from_link(vdp)
        or base_url
    )
    resolved_dealer_name = dealer_name or _first(src, "dealer_name") or dealer_id

    gallery = _gallery(src)
    hero = gallery[0] if gallery else ""

    row = {
        "vin": vin,
        "year": norm_int(src.get("year") or src.get("modelYear")),
        "make": norm_str(src.get("make")),
        "model": norm_str(src.get("model")),
        "trim": norm_str(src.get("trim")),
        "price": _pick_price(src),
        "mileage": extract_mileage(src) or norm_int(src.get("miles")),
        "condition": _condition(src),
        "exterior_color": _first(src, "ext_color", "exterior_color") or "",
        "interior_color": _first(src, "int_color", "interior_color") or "",
        "body_style": _first(src, "body", "body_style") or "",
        "drivetrain": norm_str(src.get("drivetrain")),
        "fuel_type": _first(src, "fuel_type", "fuel") or "",
        "engine_description": _first(src, "engine") or "",
        "transmission": norm_str(src.get("transmission")),
        "stock_number": _first(src, "stock_no", "stock_number", "stock") or "",
        "image_url": hero,
        "gallery": gallery,
        "dealer_id": dealer_id,
        "dealer_name": resolved_dealer_name,
        "dealer_url": resolved_dealer_url,
        "mpg_city": norm_int(src.get("city_mpg")) or None,
        "mpg_highway": norm_int(src.get("hwy_mpg")) or None,
    }

    if vdp:
        row["source_url"] = vdp
        row["_detail_url"] = vdp
    return row


def parse(raw_data, *, base_url: str = "", dealer_id: str = "", dealer_name: str = "", dealer_url: str = "") -> list[dict]:
    """Map a sister.tv Elasticsearch _search response to standard vehicle rows.

    Detects the sister.tv shape (``hits.hits[]._source`` carrying sister.tv marker
    fields) and returns [] for anything else so it is safe in the shared
    auto-detect fallback.
    """
    out: list[dict] = []
    for src in _iter_sources(raw_data):
        if not _looks_like_sister_tv(src):
            continue
        mapped = _map_source(src, base_url, dealer_id, dealer_name, dealer_url)
        if mapped:
            out.append(clean_car_row_dict(mapped))
    return out
