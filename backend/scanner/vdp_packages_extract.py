"""
Structured VDP packages/options extraction from Playwright PAGE_EXTRACT_JS capture.

Parses dealer DOM sections (Included Packages & Options, Standard Features) into
``packages_normalized`` rows for the options tab. Skips flat spec/feature floods.
"""
from __future__ import annotations

import json
import re
from typing import Any

_PRICE_RE = re.compile(r"\$\s*([\d,]+(?:\.\d{2})?)")

_PACKAGE_SECTION_HINTS = (
    "included_packages",
    "included_options",
    "package",
    "option",
    "accessori",
    "equipment_group",
    "factory_installed",
)

_SPEC_SECTION_HINTS = (
    "detailed_spec",
    "specification",
    "convenience",
    "powertrain",
    "suspension",
    "entertainment",
    "safety",
    "dimension",
)


def _parse_price_label(text: str) -> tuple[int | None, str | None]:
    m = _PRICE_RE.search(text or "")
    if not m:
        return None, None
    raw = m.group(1).replace(",", "")
    try:
        cents = int(float(raw) * 100) if "." in raw else int(raw) * 100
        if cents <= 0:
            return None, None
        label = f"${int(cents / 100):,}" if cents % 100 == 0 else f"${cents / 100:,.2f}"
        return cents // 100 if cents % 100 == 0 else int(float(raw)), label
    except (TypeError, ValueError):
        return None, None


def _clean_feature(text: str) -> str:
    return " ".join((text or "").split())[:200]


def _section_is_package_like(section: str) -> bool:
    s = (section or "").lower()
    if any(h in s for h in _SPEC_SECTION_HINTS):
        return False
    if "standard_features" in s:
        return True
    return any(h in s for h in _PACKAGE_SECTION_HINTS)


def _should_keep_structured_item(item: dict[str, Any]) -> bool:
    section = str(item.get("section") or "").lower()
    kind = str(item.get("kind") or "").lower()
    if kind in ("spec_category", "feature"):
        return False
    if not _section_is_package_like(section):
        return False
    price = item.get("price")
    feats = item.get("features") or []
    if price is not None and int(price) > 0:
        return True
    if "standard_features" in section:
        return len(feats) >= 1
    if "package" in section or "option" in section or "accessori" in section:
        return len(feats) >= 1 or price is not None
    return False


def _normalize_structured_item(raw: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    name = _clean_feature(str(raw.get("name") or raw.get("title") or ""))
    if not name or len(name) < 2:
        return None
    price = raw.get("price")
    price_label = str(raw.get("price_label") or raw.get("priceLabel") or "").strip() or None
    if price is None and not price_label:
        price, price_label = _parse_price_label(name)
    if price is None and price_label:
        price, _ = _parse_price_label(price_label)
    feats: list[str] = []
    for f in raw.get("features") or raw.get("items") or []:
        if isinstance(f, str):
            cf = _clean_feature(f)
            if cf and cf.lower() != name.lower():
                feats.append(cf)
    section = str(raw.get("section") or raw.get("kind") or "option").strip().lower()
    kind = "package" if "package" in section or price is not None else "option"
    if "spec" in section or section == "detailed_specifications":
        kind = "spec_category"
    out: dict[str, Any] = {
        "name": name[:160],
        "name_verbatim": name[:160],
        "features": feats[:24],
        "source": "vdp_dom",
        "kind": kind,
        "section": section[:80],
        "confidence": 0.85 if feats or price is not None else 0.7,
    }
    if price is not None:
        out["price"] = int(price)
    if price_label:
        out["price_label"] = price_label[:24]
    if not _should_keep_structured_item(out):
        return None
    return out


def structured_items_from_bundle(bundle: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Flatten domPackagesStructured from a VDP capture bundle (no domFeatures flood)."""
    if not isinstance(bundle, dict):
        return []
    items: list[dict[str, Any]] = []
    structured = bundle.get("domPackagesStructured") or []
    if isinstance(structured, list):
        for raw in structured[:80]:
            norm = _normalize_structured_item(raw) if isinstance(raw, dict) else None
            if norm:
                items.append(norm)
    return items[:30]


def _load_packages_dict(vehicle: dict[str, Any]) -> dict[str, Any]:
    raw = vehicle.get("packages")
    if not raw:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return {}
    return {}


def _package_dedupe_key(entry: dict[str, Any]) -> str:
    name = str(entry.get("name") or "").strip().lower()
    price = entry.get("price")
    return f"{name}|{price or ''}"


def merge_vdp_packages_into_vehicle(vehicle: dict[str, Any], bundle: dict[str, Any] | None) -> bool:
    """
    Merge structured VDP packages into ``vehicle['packages']``.

    Returns True when new structured rows were added.
    """
    new_items = structured_items_from_bundle(bundle)
    if not new_items:
        return False

    pkg = _load_packages_dict(vehicle)
    existing_norm = pkg.get("packages_normalized")
    if not isinstance(existing_norm, list):
        existing_norm = []
    seen = {_package_dedupe_key(e) for e in existing_norm if isinstance(e, dict)}

    added = 0
    for item in new_items:
        key = _package_dedupe_key(item)
        if key in seen:
            continue
        seen.add(key)
        existing_norm.append(item)
        added += 1

    if added == 0:
        return False

    pkg["packages_normalized"] = existing_norm[:40]
    sections = bundle.get("domPackagesSections") if isinstance(bundle, dict) else None
    if isinstance(sections, list) and sections:
        pkg["vdp_packages_sections"] = sections[:12]
    pkg["vdp_packages_source"] = "vdp_dom"
    vehicle["packages"] = json.dumps(pkg, ensure_ascii=False, separators=(",", ":"))
    return True


def dealer_notes_from_bundle(bundle: dict[str, Any] | None) -> str:
    """Prefer explicit Dealer Notes section over generic description."""
    if not isinstance(bundle, dict):
        return ""
    notes = str(bundle.get("domDealerNotes") or "").strip()
    if len(notes) >= 40:
        return notes[:4000]
    return ""
