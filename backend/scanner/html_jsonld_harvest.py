"""
Browser-free harvester for the dealer-group HTML platform (the McKenna Cars
pattern): dealers whose VDPs embed each vehicle's data as schema.org JSON-LD in
server-rendered HTML, with NO inventory JSON API to replay as a recipe.

Given a VDP's HTML, :func:`harvest_fields_from_html` returns a normalized field
dict (vin, price, colors, images, trim, mileage, engine, drivetrain, fuel_type,
body_style). It REUSES the structured parsers in
``backend.scanner.utils.vdp_spec_parse`` for price / colors / mechanical specs
and adds JSON-LD extraction for the fields those helpers don't cover — VIN,
mileage, trim, and the image gallery.

Given an SRP / inventory-listing page, :func:`harvest_vehicles_from_html`
returns ONE normalized dict per distinct VIN found in the page's schema.org
Vehicle JSON-LD (year / make / model / vin / price / trim / mileage / colors /
images). This is the browser-free universal fallback for the untemplated long
tail (bespoke ``custom_standalone`` / luxury rooftops with no replayable API):
if the dealer server-renders vehicle JSON-LD we can list its inventory over
plain HTTP with no per-platform template.

JSON-LD shapes handled:
  * a bare object, a top-level list, or a ``{"@graph": [...]}`` wrapper
    (dealer.com VDPs nest the priced Vehicle one level down inside @graph);
  * vehicle nodes typed ``Car`` / ``Vehicle`` / ``Automobile`` / ``Product``
    (or any node carrying a VIN);
  * ``image`` as a string, a list, or an ImageObject (``url`` / ``contentUrl``),
    collected recursively so galleries in either shape are captured.
"""
from __future__ import annotations

import json
import re
from typing import Any

from backend.scanner.utils.vdp_spec_parse import (
    parse_color_from_listing_html,
    parse_html_for_vehicle_specs,
    parse_price_from_listing_html,
)

_LD_BLOCK = re.compile(
    r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.S | re.I,
)
_VEHICLE_TYPES = {"car", "vehicle", "automobile", "product"}


def _iter_jsonld_nodes(html: str):
    """Yield every JSON-LD object in the page, expanding ``@graph`` wrappers."""
    if not html:
        return
    for m in _LD_BLOCK.finditer(html[:1_500_000]):
        try:
            data = json.loads(m.group(1))
        except (ValueError, TypeError):
            continue
        items = data if isinstance(data, list) else [data]
        for it in items:
            if isinstance(it, dict) and isinstance(it.get("@graph"), list):
                for g in it["@graph"]:
                    if isinstance(g, dict):
                        yield g
            elif isinstance(it, dict):
                yield it


def _is_vehicle_node(node: dict) -> bool:
    types = node.get("@type")
    tlist = types if isinstance(types, list) else ([types] if types else [])
    tset = {str(t).lower() for t in tlist if t}
    if tset & _VEHICLE_TYPES:
        return True
    return bool(node.get("vehicleIdentificationNumber") or node.get("vin"))


def _clean_str(val: Any, limit: int = 300) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return s[:limit] if s else None


def _images_from_value(val: Any, out: list[str]) -> None:
    """Collect http(s) URLs from a JSON-LD ``image`` value (string / list /
    ImageObject), recursively — dedupe is done by the caller."""
    if isinstance(val, str):
        s = val.strip()
        if s.startswith("http"):
            out.append(s)
    elif isinstance(val, dict):
        for key in ("url", "contentUrl", "@id"):
            u = val.get(key)
            if isinstance(u, str) and u.strip().startswith("http"):
                out.append(u.strip())
    elif isinstance(val, list):
        for it in val:
            _images_from_value(it, out)


def _mileage_from_value(val: Any) -> int | None:
    """schema.org mileageFromOdometer: number, numeric string, or a
    QuantitativeValue dict ``{"value": ...}``."""
    if isinstance(val, dict):
        val = val.get("value")
    if val in (None, ""):
        return None
    try:
        n = int(float(str(val).replace(",", "").strip()))
    except (TypeError, ValueError):
        return None
    return n if 0 <= n < 2_000_000 else None


def _trim_from_node(node: dict) -> str | None:
    for key in ("vehicleConfiguration", "trim"):
        t = _clean_str(node.get(key), 120)
        if t:
            return t
    return None


def _engine_from_node(node: dict) -> str | None:
    eng = node.get("vehicleEngine")
    if isinstance(eng, dict):
        return _clean_str(eng.get("name") or eng.get("description"), 300)
    return _clean_str(eng, 300)


def harvest_fields_from_html(html: str) -> dict[str, Any]:
    """Normalized fields from a VDP's HTML. Keys are only present when a value
    was found. ``images`` is a de-duplicated list (may be empty).

    Never fetches the network. Values are best-effort; the caller decides which
    empty DB columns to fill.
    """
    out: dict[str, Any] = {}
    if not html:
        out["images"] = []
        return out

    images: list[str] = []
    seen: set[str] = set()

    def add_images(candidates: list[str]) -> None:
        for u in candidates:
            if u not in seen:
                seen.add(u)
                images.append(u)

    for node in _iter_jsonld_nodes(html):
        if not _is_vehicle_node(node):
            continue
        if "vin" not in out:
            vin = _clean_str(node.get("vehicleIdentificationNumber") or node.get("vin"), 20)
            if vin and len(vin) == 17:
                out["vin"] = vin.upper()
        if "mileage" not in out:
            mi = _mileage_from_value(node.get("mileageFromOdometer"))
            if mi is not None:
                out["mileage"] = mi
        if "trim" not in out:
            tr = _trim_from_node(node)
            if tr:
                out["trim"] = tr
        if "engine_description" not in out:
            eng = _engine_from_node(node)
            if eng:
                out["engine_description"] = eng
        # schema.org: `color` is the exterior color, `vehicleInteriorColor` the
        # interior. parse_color_from_listing_html misses the latter key, so read
        # both straight off the node here.
        if "exterior_color" not in out:
            ec = _clean_str(node.get("color"), 120)
            if ec:
                out["exterior_color"] = ec
        if "interior_color" not in out:
            ic = _clean_str(node.get("vehicleInteriorColor") or node.get("interiorColor"), 120)
            if ic:
                out["interior_color"] = ic
        node_imgs: list[str] = []
        _images_from_value(node.get("image"), node_imgs)
        # offers can carry their own image(s) on some platforms
        offers = node.get("offers")
        for offer in offers if isinstance(offers, list) else [offers]:
            if isinstance(offer, dict):
                _images_from_value(offer.get("image"), node_imgs)
        add_images(node_imgs)

    # Reuse the structured parsers for price / colors / mechanical specs.
    price = parse_price_from_listing_html(html)
    if price:
        out["price"] = float(price)

    if "exterior_color" not in out or "interior_color" not in out:
        colors = parse_color_from_listing_html(html)
        if "exterior_color" not in out and colors.get("exterior_color"):
            out["exterior_color"] = colors["exterior_color"][:120]
        if "interior_color" not in out and colors.get("interior_color"):
            out["interior_color"] = colors["interior_color"][:120]

    specs = parse_html_for_vehicle_specs(html)
    for key in ("drivetrain", "fuel_type", "body_style"):
        v = specs.get(key)
        if v and key not in out:
            out[key] = v
    # engine_description may come from DOM spec tables when JSON-LD lacked it
    if "engine_description" not in out and specs.get("engine_description"):
        out["engine_description"] = specs["engine_description"]

    out["images"] = images
    return out


# ── Multi-vehicle SRP / inventory-listing harvest ──────────────────────────────


def _vin_from_node(node: dict) -> str | None:
    vin = _clean_str(node.get("vehicleIdentificationNumber") or node.get("vin"), 20)
    if vin and len(vin) == 17:
        return vin.upper()
    return None


def _year_from_node(node: dict) -> int | None:
    for key in ("modelDate", "vehicleModelDate", "productionDate", "releaseDate"):
        raw = node.get(key)
        if raw in (None, ""):
            continue
        m = re.search(r"(19|20)\d{2}", str(raw))
        if m:
            return int(m.group(0))
    return None


def _make_from_node(node: dict) -> str | None:
    for key in ("brand", "manufacturer", "make"):
        val = node.get(key)
        if isinstance(val, dict):
            val = val.get("name")
        s = _clean_str(val, 60)
        if s:
            return s
    return None


def _model_from_node(node: dict) -> str | None:
    val = node.get("model")
    if isinstance(val, dict):
        val = val.get("name")
    return _clean_str(val, 80)


def _price_from_node(node: dict) -> float | None:
    """First positive numeric price in the node's ``offers`` (schema.org Offer /
    AggregateOffer, possibly nested in ``priceSpecification``)."""
    offers = node.get("offers")
    for offer in offers if isinstance(offers, list) else [offers]:
        if not isinstance(offer, dict):
            continue
        for cand in (
            offer.get("price"),
            offer.get("lowPrice"),
            (offer.get("priceSpecification") or {}).get("price")
            if isinstance(offer.get("priceSpecification"), dict)
            else None,
        ):
            if cand in (None, ""):
                continue
            try:
                p = float(str(cand).replace(",", "").replace("$", "").strip())
            except (TypeError, ValueError):
                continue
            if p > 0:
                return p
    return None


def _vehicle_fields_from_node(node: dict) -> dict[str, Any]:
    """Normalized fields for ONE schema.org Vehicle JSON-LD node. Keys are only
    present when a value was found; ``images`` is always present (may be empty)."""
    out: dict[str, Any] = {}
    vin = _vin_from_node(node)
    if vin:
        out["vin"] = vin
    for key, val in (
        ("year", _year_from_node(node)),
        ("make", _make_from_node(node)),
        ("model", _model_from_node(node)),
        ("trim", _trim_from_node(node)),
        ("price", _price_from_node(node)),
        ("engine_description", _engine_from_node(node)),
        ("mileage", _mileage_from_value(node.get("mileageFromOdometer"))),
    ):
        if val is not None and val != "":
            out[key] = val
    ec = _clean_str(node.get("color"), 120)
    if ec:
        out["exterior_color"] = ec
    ic = _clean_str(node.get("vehicleInteriorColor") or node.get("interiorColor"), 120)
    if ic:
        out["interior_color"] = ic
    imgs: list[str] = []
    _images_from_value(node.get("image"), imgs)
    offers = node.get("offers")
    for offer in offers if isinstance(offers, list) else [offers]:
        if isinstance(offer, dict):
            _images_from_value(offer.get("image"), imgs)
    seen: set[str] = set()
    out["images"] = [u for u in imgs if not (u in seen or seen.add(u))]
    return out


def harvest_vehicles_from_html(html: str) -> list[dict[str, Any]]:
    """Every VIN-bearing schema.org Vehicle JSON-LD node in *html*, as a list of
    normalized field dicts (one per distinct VIN, in document order).

    Unlike :func:`harvest_fields_from_html` — which collapses a VDP page to a
    single vehicle — this keeps each vehicle separate, so a server-rendered SRP /
    inventory page that embeds many Vehicle nodes yields the whole visible lot.
    Nodes without a valid 17-char VIN are skipped (a VIN is what makes a listing
    real and dedupe-able). Never fetches the network.
    """
    if not html:
        return []
    out: list[dict[str, Any]] = []
    seen_vins: set[str] = set()
    for node in _iter_jsonld_nodes(html):
        if not _is_vehicle_node(node):
            continue
        vin = _vin_from_node(node)
        if not vin or vin in seen_vins:
            continue
        seen_vins.add(vin)
        out.append(_vehicle_fields_from_node(node))
    return out
