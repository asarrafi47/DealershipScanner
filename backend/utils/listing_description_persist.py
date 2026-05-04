"""
Merge listing-description parse results into ``cars.packages`` JSON and optional
``interior_color`` + ``spec_source_json`` updates.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

from backend.utils.field_clean import is_effectively_empty
from backend.utils.interior_color_buckets import interior_color_buckets_json
from backend.utils.listing_description_extract import (
    LISTING_DESCRIPTION_PARSER_VERSION,
    extract_listing_description,
    normalize_listing_description,
)
from backend.utils.spec_provenance import merge_spec_source_json


def listing_description_source_fingerprint(normalized_text: str) -> str:
    return hashlib.sha256((normalized_text or "").encode("utf-8")).hexdigest()[:24]


def listing_description_parse_is_current(
    packages_val: Any,
    *,
    source_fingerprint: str,
    force: bool = False,
) -> bool:
    """
    True when ``cars.packages`` already embeds this normalized-description fingerprint
    for the current parser version (safe to skip re-parse).
    """
    if force:
        return False
    if packages_val is None or is_effectively_empty(packages_val):
        return False
    try:
        d = json.loads(packages_val) if isinstance(packages_val, str) else packages_val
    except (json.JSONDecodeError, TypeError):
        return False
    if not isinstance(d, dict):
        return False
    if str(d.get("listing_description_parser_version") or "") != LISTING_DESCRIPTION_PARSER_VERSION:
        return False
    meta = d.get("dealer_description_parsed")
    if not isinstance(meta, dict):
        return False
    return str(meta.get("source_text_sha256") or "") == source_fingerprint


def packages_column_is_sparse(packages_val: Any) -> bool:
    """True when packages JSON is missing or has no meaningful feature lists."""
    if packages_val is None or is_effectively_empty(packages_val):
        return True
    s = str(packages_val).strip()
    if s in ("{}", "[]", "null"):
        return True
    try:
        d = json.loads(s) if isinstance(packages_val, str) else packages_val
    except (json.JSONDecodeError, TypeError):
        return True
    if not isinstance(d, dict):
        return True
    for k in ("observed_features", "possible_packages", "observed_badges"):
        v = d.get(k)
        if isinstance(v, list) and len(v) > 0:
            return False
    if isinstance(d.get("packages_normalized"), list) and len(d["packages_normalized"]) > 0:
        return False
    return True


def merge_description_parse_into_packages(
    existing: str | None,
    parsed: dict[str, Any],
    *,
    source_fingerprint: str,
) -> str:
    """
    Shallow-merge into existing packages dict: replaces only description-derived keys.
    """
    base: dict[str, Any] = {}
    if existing and str(existing).strip() and not is_effectively_empty(existing):
        try:
            prev = json.loads(existing)
            if isinstance(prev, dict):
                base = dict(prev)
        except (json.JSONDecodeError, TypeError):
            base = {"legacy_packages_text": str(existing).strip()[:2000]}

    base["listing_description_parser_version"] = LISTING_DESCRIPTION_PARSER_VERSION
    base["dealer_description_parsed"] = {
        "parser_version": parsed.get("parser_version"),
        "parsed_at": parsed.get("parsed_at"),
        "source_text_sha256": source_fingerprint,
        "interior_color_hint": parsed.get("interior_color_hint"),
        "exterior_color_hint": parsed.get("exterior_color_hint"),
        "confidence": parsed.get("confidence"),
    }
    norm_pkgs: list[dict[str, Any]] = []
    for p in (parsed.get("packages") or [])[:14]:
        if not isinstance(p, dict):
            continue
        feats = [str(x)[:160] for x in (p.get("features") or [])[:10] if str(x).strip()]
        ev = [str(x)[:120] for x in (p.get("evidence_spans") or [])[:8] if str(x).strip()]
        norm_pkgs.append(
            {
                "name": p.get("name"),
                "name_verbatim": p.get("name_verbatim"),
                "canonical_name": p.get("canonical_name"),
                "catalog_matched": bool(p.get("catalog_matched")),
                "features": feats,
                "evidence_spans": ev,
                "confidence": p.get("confidence"),
            }
        )
    base["packages_normalized"] = norm_pkgs
    base["standalone_features_from_description"] = [
        str(x)[:200] for x in (parsed.get("standalone_features") or [])[:20] if str(x).strip()
    ]

    if not isinstance(base.get("confidence"), dict):
        base["confidence"] = {}
    desc_conf = parsed.get("confidence")
    if isinstance(desc_conf, dict):
        base["confidence"]["listing_description"] = desc_conf

    payload = json.dumps(base, ensure_ascii=False)
    if len(payload) > 7900:
        base["packages_normalized"] = norm_pkgs[:8]
        base["standalone_features_from_description"] = base["standalone_features_from_description"][:10]
        payload = json.dumps(base, ensure_ascii=False)
    return payload[:8000]


def build_row_updates_from_parse(
    car: dict[str, Any],
    parsed: dict[str, Any],
    *,
    merged_packages_json: str,
    source_fingerprint: str,
) -> dict[str, Any]:
    """
    Fields suitable for ``update_car_row_partial`` (packages + optional interior + provenance).
    """
    out: dict[str, Any] = {"packages": merged_packages_json}

    interior = parsed.get("interior_color_hint")
    hint_val = None
    conf = 0.0
    if isinstance(interior, dict):
        hint_val = interior.get("value")
        try:
            conf = float(interior.get("confidence") or 0)
        except (TypeError, ValueError):
            conf = 0.0
    if (
        hint_val
        and isinstance(hint_val, str)
        and hint_val.strip()
        and conf >= 0.5
        and is_effectively_empty(car.get("interior_color"))
    ):
        ic_val = hint_val.strip()[:120]
        out["interior_color"] = ic_val
        out["interior_color_buckets"] = interior_color_buckets_json(ic_val, car.get("make"))
        prov = {
            "interior_color": {
                "source": "listing_description",
                "parser_version": str(parsed.get("parser_version") or ""),
                "confidence": conf,
                "source_text_sha256": source_fingerprint,
            }
        }
        out["spec_source_json"] = merge_spec_source_json(car.get("spec_source_json"), prov)
    return out


def process_listing_description_for_row(
    row: dict[str, Any],
    *,
    skip_if_unchanged: bool = True,
    force: bool = False,
) -> dict[str, Any]:
    """
    Parse ``row['description']`` into structured packages / provenance updates.

    Returns ``{"applied": bool, "reason": str, "updates": dict|None}``.
    """
    desc = row.get("description") or ""
    norm = normalize_listing_description(desc)
    if len(norm) < 20:
        return {"applied": False, "reason": "description_too_short", "updates": None}

    fp = listing_description_source_fingerprint(norm)
    if (
        skip_if_unchanged
        and not force
        and listing_description_parse_is_current(row.get("packages"), source_fingerprint=fp, force=False)
    ):
        return {"applied": False, "reason": "unchanged", "updates": None}

    ctx = {
        "make": row.get("make"),
        "model": row.get("model"),
        "year": row.get("year"),
        "trim": row.get("trim"),
        "vin": row.get("vin"),
    }
    parsed = extract_listing_description(desc, ctx)
    merged = merge_description_parse_into_packages(row.get("packages"), parsed, source_fingerprint=fp)
    updates = build_row_updates_from_parse(row, parsed, merged_packages_json=merged, source_fingerprint=fp)
    return {"applied": True, "reason": "ok", "updates": updates}
