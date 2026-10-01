"""JSON serialization of the ``cars`` JSON/TEXT columns written by the upsert.

Each function maps one scanner value to the exact string (or None) the INSERT
binds; None means "absent" so the ON CONFLICT keep-if-nonempty clause preserves
the stored value.
"""
from __future__ import annotations

import json
from typing import Any

from backend.utils.in_transit import availability_spec_source_patch
from backend.utils.spec_provenance import merge_spec_source_json


def gallery_json(gallery: Any) -> str:
    """Gallery: stored as JSON string; always use json.dumps(list)."""
    if isinstance(gallery, list):
        return json.dumps(gallery)
    if gallery is not None and isinstance(gallery, str):
        try:
            json.loads(gallery)
            return gallery
        except (TypeError, ValueError):
            return "[]"
    return "[]"


def spin_frames_json(spin: Any) -> str | None:
    """360 spin frames: stored as JSON string like gallery (None when absent
    so the ON CONFLICT keep-if-nonempty clause preserves prior captures)."""
    if isinstance(spin, list):
        _spin_urls = [str(u).strip() for u in spin if u and str(u).strip().startswith("http")]
        return json.dumps(_spin_urls) if _spin_urls else None
    if isinstance(spin, str) and spin.strip():
        try:
            _parsed_spin = json.loads(spin)
            return spin if isinstance(_parsed_spin, list) and _parsed_spin else None
        except (TypeError, ValueError):
            return None
    return None


def history_highlights_json(highlights: Any) -> str:
    return json.dumps(highlights) if isinstance(highlights, list) else (highlights if isinstance(highlights, str) else "[]")


def packages_json(pkg_raw: Any) -> str | None:
    if isinstance(pkg_raw, dict):
        return json.dumps(pkg_raw, ensure_ascii=False)
    if isinstance(pkg_raw, str) and pkg_raw.strip() not in ("", "{}", "[]", "null"):
        return pkg_raw.strip()
    return None


def _as_json_text(spec_src: Any) -> str | None:
    return spec_src if isinstance(spec_src, str) else (json.dumps(spec_src) if isinstance(spec_src, dict) else None)


def spec_source_json(v: dict, prior_spec_src: str | None) -> Any:
    """Incoming provenance + availability / lot-location patches, merged into the
    stored provenance (``prior_spec_src``).

    The ON CONFLICT clause keeps column values via COALESCE but would replace
    spec_source_json wholesale, orphaning the provenance of every surviving
    enriched value; merging here keeps it.
    """
    spec_src = v.get("spec_source_json")
    avail_patch = availability_spec_source_patch(v)
    if avail_patch:
        spec_src = merge_spec_source_json(_as_json_text(spec_src), avail_patch)
    lot_loc = str(v.get("_lot_location") or v.get("_inventory_location") or "").strip()
    if lot_loc:
        spec_src = merge_spec_source_json(
            _as_json_text(spec_src),
            {
                "inventory_lot_location": {
                    "source": str(v.get("_lot_location_source") or "inventory")[:40],
                    "value": lot_loc[:200],
                },
            },
        )
    elif isinstance(spec_src, dict):
        spec_src = json.dumps(spec_src, ensure_ascii=False)
    elif spec_src is not None and not isinstance(spec_src, str):
        spec_src = str(spec_src)
    if prior_spec_src and spec_src and str(spec_src).strip():
        try:
            _new_prov = json.loads(spec_src)
        except (json.JSONDecodeError, TypeError):
            _new_prov = None
        if isinstance(_new_prov, dict):
            spec_src = merge_spec_source_json(prior_spec_src, _new_prov)
    return spec_src


PRICE_HISTORY_MAX_ENTRIES = 24


def price_history_json(
    existing_raw: Any, prev_price: Any, new_price: Any, scraped_at: str
) -> str | None:
    """Append a ``{date, price}`` snapshot when price changes (or seed on first sight).

    Read-modify-write against the row's own previous JSON; safe for the common case of
    one scan process per VIN. A rare concurrent-write race could drop a snapshot, which
    is acceptable since this is a nice-to-have history, not authoritative price data.
    """
    try:
        history = json.loads(existing_raw) if existing_raw else []
        if not isinstance(history, list):
            history = []
    except (TypeError, ValueError):
        history = []
    try:
        np = float(new_price) if new_price is not None else None
    except (TypeError, ValueError):
        np = None
    if np is None:
        return json.dumps(history) if history else None
    try:
        pp = float(prev_price) if prev_price is not None else None
    except (TypeError, ValueError):
        pp = None
    if not history or (pp is not None and np != pp):
        history.append({"date": scraped_at, "price": np})
    if len(history) > PRICE_HISTORY_MAX_ENTRIES:
        history = history[-PRICE_HISTORY_MAX_ENTRIES:]
    return json.dumps(history) if history else None
