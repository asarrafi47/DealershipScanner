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
