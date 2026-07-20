"""
Parser for the Typesense search platform (multi_search responses).

Several dealer groups (e.g. Toyota of Orange, Toyota Place, Freeway Honda) serve
their SRP inventory straight from a hosted Typesense collection. The recipe
capture replays fine over plain HTTP, but the payload shape is Typesense's own
``{"results": [{"hits": [{"document": {...}}]}]}`` rather than any of the OEM
platform shapes, so it needs a dedicated document mapper.

Each ``document`` is one vehicle. Field names are the getauto/Typesense flavor:
vin, yr/year, make, model, trim, exteriorColor/genericColor,
interiorColor/genericInteriorColor, body/compoundBody, drivetrain, fuel, engine,
transmission/transmissionType, stockNumber, mileage, condition, finalPriceInt /
internetPrice / msrp (dollar-formatted strings), imageUrls (list of URLs),
vdpUrl (site-relative), mpgCity/mpgHway.
"""
import logging
import re
from urllib.parse import urljoin

from backend.parsers.base import (
    extract_mileage,
    extract_price,
    norm_int,
    norm_str,
)
from backend.utils.field_clean import clean_car_row_dict, normalize_optional_str

logger = logging.getLogger(__name__)


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


def _iter_documents(raw_data):
    """Yield each Typesense hit ``document`` dict from a multi_search response."""
    if not (isinstance(raw_data, dict) and isinstance(raw_data.get("results"), list)):
        return
    for res in raw_data["results"]:
        if not isinstance(res, dict):
            continue
        hits = res.get("hits")
        if not isinstance(hits, list):
            continue
        for hit in hits:
            if isinstance(hit, dict) and isinstance(hit.get("document"), dict):
                yield hit["document"]


def _pick_price(doc: dict) -> float:
    # Prefer the pre-computed integer the platform sorts/renders on; fall back to
    # the dollar-formatted string fields via the shared price extractor.
    for key in ("finalPriceInt",):
        n = norm_int(doc.get(key))
        if n > 0:
            return float(n)
    return extract_price(doc)


def _pick_body(doc: dict) -> str | None:
    got = _first(doc, "body")
    if got:
        return got
    cb = doc.get("compoundBody")
    if isinstance(cb, list):
        for it in cb:
            got = _opt_str(it)
            if got:
                return got
    return None


def _gallery(doc: dict) -> list[str]:
    urls = doc.get("imageUrls")
    if not isinstance(urls, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
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


def _detail_url(doc: dict, dealer_url: str, base_url: str) -> str | None:
    raw = _first(doc, "vdpUrl", "vehicleUrl", "url", "href")
    if not raw:
        return None
    if raw.startswith("//"):
        return "https:" + raw
    if raw.lower().startswith("http"):
        return raw
    # vdpUrl is site-relative ("/vehicle/..."). Resolve against the dealer's real
    # domain (carried in the document) — never the Typesense host the recipe hit.
    root = (_dealer_url_from_doc(doc) or dealer_url or base_url or "").strip()
    if root and not root.lower().startswith("http"):
        root = "https://" + root
    if not root:
        return None
    try:
        return urljoin(root.rstrip("/") + "/", raw.lstrip("/"))
    except Exception:
        return None


_ENGINE_L_RE = re.compile(r"(\d(?:\.\d+)?)\s*[lL]\b")
_CYL_RE = re.compile(r"\b[VvIiWw](\d{1,2})\b|\b(\d{1,2})[- ]?cyl", re.IGNORECASE)


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


def _cylinders(engine: str | None) -> int | None:
    if not engine:
        return None
    m = _CYL_RE.search(engine)
    if not m:
        return None
    raw = m.group(1) or m.group(2)
    n = norm_int(raw)
    return n if 0 < n <= 16 else None


def _carfax_url(doc: dict) -> str | None:
    """Browser-free Carfax report URL from the Typesense ``carfax`` object.

    getauto/Typesense docs carry ``carfax: {url, snapshotKey, iconUrl}`` where
    ``url`` is the real ``carfax.com/vehiclehistory/...`` report link — no VDP
    visit needed. New cars simply omit it.
    """
    cf = doc.get("carfax")
    if isinstance(cf, dict):
        u = _opt_str(cf.get("url"))
        if u and u.lower().startswith("http") and "carfax" in u.lower():
            return u
    return None


def _packages_blob(doc: dict) -> dict | None:
    """Factory features / packages / options blob → cars.packages (JSON).

    The Typesense doc carries three flat lists straight from the feed:
    ``features`` (equipment), ``packages`` (factory package names) and
    ``options`` (added options). Mirrors the CarsCommerce packages convention.
    """
    blob: dict = {}

    def _clean_list(val) -> list[str]:
        if not isinstance(val, list):
            return []
        out: list[str] = []
        seen: set[str] = set()
        for x in val:
            s = _opt_str(x) if not isinstance(x, str) else norm_str(x)
            s = s.strip() if isinstance(s, str) else ""
            if s and s.lower() not in seen:
                seen.add(s.lower())
                out.append(s)
        return out[:150]

    feats = _clean_list(doc.get("features"))
    if feats:
        blob["features"] = feats
    pkgs = _clean_list(doc.get("packages"))
    if pkgs:
        blob["factory_packages"] = pkgs
    opts = _clean_list(doc.get("options"))
    if opts:
        blob["options"] = opts
    return blob or None


def _history_highlights(doc: dict) -> list[str]:
    """Vehicle-history badge phrases (e.g. 'CARFAX 1-Owner') from the feed."""
    vh = doc.get("vehicleHistory")
    out: list[str] = []
    seen: set[str] = set()
    if isinstance(vh, list):
        for x in vh:
            s = norm_str(x) if isinstance(x, str) else ""
            s = s.strip()
            if s and s.lower() not in seen:
                seen.add(s.lower())
                out.append(s)
    return out


def _dealer_name_from_doc(doc: dict) -> str | None:
    got = _first(doc, "dealerName")
    if got:
        return got
    dealer = doc.get("dealer")
    if isinstance(dealer, dict):
        return _opt_str(dealer.get("name"))
    return None


def _dealer_url_from_doc(doc: dict) -> str | None:
    dealer = doc.get("dealer")
    if isinstance(dealer, dict):
        u = _opt_str(dealer.get("url"))
        if u:
            if not u.lower().startswith("http"):
                u = "https://" + u.lstrip("/")
            return u
    return None


def _map_document(doc: dict, base_url: str, dealer_id: str, dealer_name: str, dealer_url: str) -> dict | None:
    vin = norm_str(doc.get("vin") or doc.get("VIN") or "")
    if not vin:
        return None

    dealer_name = dealer_name or _dealer_name_from_doc(doc) or dealer_id
    dealer_url = dealer_url or _dealer_url_from_doc(doc) or base_url

    gallery = _gallery(doc)
    hero = gallery[0] if gallery else ""

    cond = _first(doc, "condition")
    if not cond and doc.get("certified") is True:
        cond = "Certified"

    engine = _first(doc, "engine")

    row = {
        "vin": vin,
        "year": norm_int(doc.get("yr") or doc.get("year")),
        "make": norm_str(doc.get("make")),
        "model": norm_str(doc.get("model")),
        "trim": norm_str(doc.get("trim")),
        "price": _pick_price(doc),
        "mileage": extract_mileage(doc),
        "condition": cond or "",
        "exterior_color": _first(doc, "exteriorColor", "genericColor") or "",
        "interior_color": _first(doc, "interiorColor", "genericInteriorColor") or "",
        "body_style": _pick_body(doc) or "",
        "drivetrain": norm_str(doc.get("drivetrain")),
        "fuel_type": _first(doc, "fuel", "fuelType") or "",
        "engine_description": engine or "",
        "transmission": _first(doc, "transmission", "transmissionType") or "",
        "stock_number": _first(doc, "stockNumber", "stock_number", "stockNo") or "",
        "image_url": hero,
        "gallery": gallery,
        "dealer_id": dealer_id,
        "dealer_name": dealer_name,
        "dealer_url": dealer_url,
        "title": norm_str(doc.get("vehicleTitle") or doc.get("title") or ""),
        "mpg_city": norm_int(doc.get("mpgCity")) or None,
        "mpg_highway": norm_int(doc.get("mpgHway") or doc.get("mpgHighway")) or None,
        "engine_l": _engine_liters(engine),
        "cylinders": _cylinders(engine),
    }

    carfax = _carfax_url(doc)
    if carfax:
        row["carfax_url"] = carfax
    packages = _packages_blob(doc)
    if packages:
        row["packages"] = packages
    highlights = _history_highlights(doc)
    if highlights:
        row["history_highlights"] = highlights

    du = _detail_url(doc, dealer_url, base_url)
    if du:
        row["source_url"] = du
        row["_detail_url"] = du
    return row


def _row_belongs_to_dealer(doc: dict, dealer_name: str, dealer_url: str) -> bool:
    """
    Group-account guard: dealer groups share ONE Typesense collection, so a
    store's recipe replays the whole group's inventory. When the document
    declares its own dealer identity and it clearly differs from the target
    store, the row belongs to a sibling store — drop it. Documents without
    identity are always kept (single-store feeds unaffected).
    """
    import re as _re
    from urllib.parse import urlparse as _urlparse

    def _host(u: str) -> str:
        try:
            h = (_urlparse((u or "").strip()).netloc or "").lower()
            return h[4:] if h.startswith("www.") else h
        except ValueError:
            return ""

    def _nrm(s: str) -> str:
        return _re.sub(r"[^a-z0-9]+", "", (s or "").lower())

    doc_url = _dealer_url_from_doc(doc) or ""
    target_host = _host(dealer_url)
    if doc_url and target_host:
        doc_host = _host(doc_url)
        if doc_host and doc_host != target_host:
            return False
    doc_name = _dealer_name_from_doc(doc) or ""
    if doc_name and dealer_name:
        dn, tn = _nrm(doc_name), _nrm(dealer_name)
        if dn and tn and dn != tn and dn not in tn and tn not in dn:
            # Only trust a name mismatch when no URL evidence exists either way
            if not doc_url:
                return False
    return True


def parse(raw_data, *, base_url: str = "", dealer_id: str = "", dealer_name: str = "", dealer_url: str = "") -> list[dict]:
    """Map a Typesense multi_search response to standard vehicle rows.

    Detects the Typesense shape (``results[].hits[].document``) and returns [] for
    anything else so it is safe in the shared auto-detect fallback.
    """
    out: list[dict] = []
    dropped = 0
    for doc in _iter_documents(raw_data):
        if not _row_belongs_to_dealer(doc, dealer_name, dealer_url):
            dropped += 1
            continue
        mapped = _map_document(doc, base_url, dealer_id, dealer_name, dealer_url)
        if mapped:
            out.append(clean_car_row_dict(mapped))
    if dropped:
        import logging

        logging.getLogger("scanner").info(
            "typesense parse [%s]: dropped %d sibling-store row(s) from group feed",
            dealer_id or dealer_name or "?", dropped,
        )
    return out
