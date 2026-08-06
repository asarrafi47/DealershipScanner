"""
Parser for the Dealer Masters platform (Gatsby SSG, ``media.dealermasters.com``).

Dealer Masters serves inventory from Gatsby static-query data files
(``/page-data/sq/d/<queryhash>.json``). The full lot (new + used combined) lives
in one file at ``data.allInventoryJson.nodes`` — the query hash is derived from
the GraphQL query text, so it is stable across site rebuilds (recipe_synth
re-resolves it if it ever changes). This parser accepts that full page-data dict
(what recipe replay hands it), the inner ``{"allInventoryJson": ...}`` object, or
a bare list of nodes, and returns ``[]`` for anything else so it is safe in the
shared auto-detect fallback.

Per-vehicle fields are PascalCase and split across sub-objects:
  VIN                         (top-level, also VehicleInfo.VIN)
  VehicleInfo.{Year,Make,Model,BodyType,IsNew,VehicleStatus,InStockDate}
  Pricing.{List,Special,ExtraPrice1,Cost,...}   (Cost is always 0/hidden)
  ListOfPhotos[].{Order,PhotoUrl}

The feed carries no trim / mileage / color / stock number — those are VDP /
gap-fill heals downstream. Price, however, is fully present here (the generic
JSON walker missed it because it never descended into the nested ``Pricing``
object — the exact gap this parser closes).
"""
import logging

from backend.parsers.base import norm_int, norm_str
from backend.utils.field_clean import clean_car_row_dict, normalize_optional_str

logger = logging.getLogger(__name__)


def _opt_str(v):
    if v is None:
        return None
    return normalize_optional_str(norm_str(v))


def _nodes(raw_data) -> list:
    """Collect vehicle nodes from any accepted Dealer Masters shape."""
    obj = raw_data
    if isinstance(obj, dict):
        # Gatsby page-data wraps the query result under "data".
        if isinstance(obj.get("data"), dict):
            obj = obj["data"]
        inv = obj.get("allInventoryJson") if isinstance(obj, dict) else None
        if isinstance(inv, dict) and isinstance(inv.get("nodes"), list):
            return inv["nodes"]
        # Already the inventory object, or a {"nodes": [...]} container.
        if isinstance(obj, dict) and isinstance(obj.get("nodes"), list):
            return obj["nodes"]
        return []
    if isinstance(obj, list):
        return obj
    return []


def _pick_price(pricing: dict) -> float:
    """Advertised price: Special (sale/internet) when set, else List, else ExtraPrice1."""
    for key in ("Special", "List", "ExtraPrice1"):
        n = norm_int(pricing.get(key))
        if n > 0:
            return float(n)
    return 0.0


def _gallery(node: dict) -> list[str]:
    photos = node.get("ListOfPhotos")
    if not isinstance(photos, list):
        return []
    ordered = sorted(
        (p for p in photos if isinstance(p, dict) and p.get("PhotoUrl")),
        key=lambda p: norm_int(p.get("Order")) or 0,
    )
    out: list[str] = []
    seen: set[str] = set()
    for p in ordered:
        url = _opt_str(p.get("PhotoUrl"))
        if url and url not in seen:
            seen.add(url)
            out.append(url)
    return out


def _map(node: dict, dealer_id: str, dealer_name: str, dealer_url: str) -> dict | None:
    if not isinstance(node, dict):
        return None
    info = node.get("VehicleInfo") if isinstance(node.get("VehicleInfo"), dict) else {}
    pricing = node.get("Pricing") if isinstance(node.get("Pricing"), dict) else {}
    vin = norm_str(node.get("VIN") or info.get("VIN") or "")
    if not vin or len(vin) < 11:
        return None
    gallery = _gallery(node)
    row = {
        "vin": vin,
        "year": norm_int(info.get("Year")),
        "make": norm_str(info.get("Make")),
        "model": norm_str(info.get("Model")),
        "price": _pick_price(pricing),
        "condition": "New" if info.get("IsNew") else "Used",
        "body_style": _opt_str(info.get("BodyType")) or "",
        "image_url": gallery[0] if gallery else "",
        "gallery": gallery,
        "dealer_id": dealer_id,
        "dealer_name": dealer_name or dealer_id,
        "dealer_url": dealer_url,
    }
    return row


def parse(raw_data, *, base_url: str = "", dealer_id: str = "", dealer_name: str = "", dealer_url: str = "") -> list[dict]:
    """Map a Dealer Masters ``allInventoryJson`` payload to vehicle rows.

    Returns ``[]`` for anything without Dealer Masters inventory nodes, so it is
    safe in the shared auto-detect fallback.
    """
    out: list[dict] = []
    seen: set[str] = set()
    for node in _nodes(raw_data):
        mapped = _map(node, dealer_id, dealer_name, dealer_url or base_url)
        if mapped and mapped["vin"].upper() not in seen:
            seen.add(mapped["vin"].upper())
            out.append(clean_car_row_dict(mapped))
    return out
