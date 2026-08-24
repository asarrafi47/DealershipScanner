"""Trim-to-trim differences derived from our own window-sticker corpus.

Every VIN-confirmed sticker the vision campaign reads carries that car's
factory equipment list. Group those by (year, make, model, trim), and the
difference between one trim's equipment and the trims below it IS "what this
trim adds" — provable line by line against photographed documents, which is
the provenance the brochure pipeline mostly lacks (the 2026-08 trim-adds
audit: only 13 of 3,185 brochure overlays carry provenance).

Contamination control: a single car's sticker mixes standard equipment with
that car's OPTIONS. For trims with >= 2 stickers we keep only items seen on
ALL of them (options rarely repeat identically); for single-sticker trims we
keep the list but drop anything that car's own sticker priced as an option.

Presentation-only: these adds fill a rung's expandable panel and never
justify the rung's existence — rung names still pass the provenance gate in
``evidence.py`` untouched.
"""
from __future__ import annotations

import json
import re
import time
from collections import defaultdict
from functools import lru_cache
from typing import Any

from backend.enrichment.trim_ladder._common import _norm_make, _norm_token

_CACHE_TTL_S = 600.0
_MAX_ADDS_SHOWN = 8
_MIN_EQUIPMENT_ITEMS = 3


def _norm_equip(item: Any) -> str:
    """Diff key for one equipment line: case/punct-insensitive, depluralized."""
    words = re.sub(r"[^a-z0-9]+", " ", str(item or "").lower()).split()
    return " ".join(w[:-1] if len(w) >= 5 and w.endswith("s") else w for w in words)


def _trim_key(trim: Any) -> str:
    return _norm_token(str(trim or ""))


def _load_sticker_trim_groups() -> dict[tuple[int, str, str], dict[str, dict[str, Any]]]:
    """(year, make_norm, model_norm) → trim_key → aggregated sticker evidence."""
    from backend.db.inventory_db import db_conn

    with db_conn() as conn:
        rows = conn.execute(
            """
            SELECT c.year, c.make, c.model, c.trim, c.id,
                   t.summary->>'equipment', t.summary->>'priced_options',
                   t.summary->>'base_msrp'
            FROM car_image_text t JOIN cars c ON c.id = t.car_id
            WHERE t.version = 100
              AND (t.summary->>'vin_confirmed') IN ('true', 'True')
              AND COALESCE(t.summary->>'sticker_image_urls', '[]') NOT IN ('[]', 'null', '')
              AND TRIM(COALESCE(c.trim, '')) <> ''
              AND jsonb_typeof(t.summary->'equipment') = 'array'
            """
        ).fetchall()

    grouped: dict[tuple[int, str, str], dict[str, dict[str, Any]]] = defaultdict(dict)
    for year, make, model, trim, car_id, equip_j, priced_j, base_j in rows:
        try:
            year_i = int(year)
        except (TypeError, ValueError):
            continue
        try:
            equipment = equip_j if isinstance(equip_j, list) else json.loads(equip_j or "[]")
        except (TypeError, ValueError):
            continue
        if not isinstance(equipment, list) or len(equipment) < _MIN_EQUIPMENT_ITEMS:
            continue
        try:
            priced = priced_j if isinstance(priced_j, list) else json.loads(priced_j or "[]")
        except (TypeError, ValueError):
            priced = []
        optional_keys = {
            _norm_equip(p.get("name")) for p in priced if isinstance(p, dict)
        }
        items = {}
        for e in equipment:
            k = _norm_equip(e)
            if k and k not in optional_keys:
                items.setdefault(k, str(e).strip())

        cfg = (year_i, _norm_make(make or ""), _norm_token(model or ""))
        tk = _trim_key(trim)
        if not tk:
            continue
        slot = grouped[cfg].setdefault(
            tk,
            {"display_trim": str(trim).strip(), "car_ids": [], "base_msrps": [],
             "per_sticker_items": []},
        )
        slot["car_ids"].append(int(car_id))
        slot["per_sticker_items"].append(items)
        try:
            b = float(base_j)
            if b > 0:
                slot["base_msrps"].append(b)
        except (TypeError, ValueError):
            pass

    # Collapse each trim's stickers: intersection for n>=2, sole list for n=1.
    for cfg, trims in grouped.items():
        for tk, slot in trims.items():
            per = slot.pop("per_sticker_items")
            if len(per) >= 2:
                common = set(per[0])
                for it in per[1:]:
                    common &= set(it)
                slot["items"] = {k: per[0][k] for k in common}
            else:
                slot["items"] = per[0]
            slot["sticker_count"] = len(per)
            b = sorted(slot.pop("base_msrps"))
            slot["median_base_msrp"] = b[len(b) // 2] if b else None
    return dict(grouped)


@lru_cache(maxsize=2)
def _groups_cached(bucket: int) -> dict:
    return _load_sticker_trim_groups()


def _groups() -> dict[tuple[int, str, str], dict[str, dict[str, Any]]]:
    return _groups_cached(int(time.time() // _CACHE_TTL_S))


def attach_sticker_adds(
    steps: list[dict[str, Any]],
    *,
    make: str,
    model: str,
    year: Any,
) -> int:
    """Fill ``sticker_adds`` on ladder steps from the sticker corpus.

    ``steps`` is the resolved ladder, TOP trim first. For each rung with
    sticker evidence, adds = its sticker equipment minus the union of every
    LOWER rung's sticker equipment. Rungs whose panel already carries
    brochure-cited ``adds`` are left alone — a photographed page beats a diff.
    Returns the number of steps annotated.
    """
    try:
        year_i = int(year)
    except (TypeError, ValueError):
        return 0
    cfg = (year_i, _norm_make(make or ""), _norm_token(model or ""))
    trims = _groups().get(cfg)
    if not trims:
        return 0

    # Union of equipment on all rungs BELOW step i (steps are top-first).
    n = len(steps)
    matched = [trims.get(_trim_key(s.get("trim") or s.get("name"))) for s in steps]
    below_union: list[set] = [set() for _ in range(n)]
    acc: set = set()
    for i in range(n - 1, -1, -1):
        below_union[i] = set(acc)
        if matched[i]:
            acc |= set(matched[i]["items"])

    annotated = 0
    for i, (step, ev) in enumerate(zip(steps, matched)):
        if not ev or step.get("adds"):
            continue
        if not below_union[i]:
            # Base rung, or no sticker evidence below to diff against — a raw
            # equipment dump is not "what this trim adds".
            continue
        add_keys = [k for k in ev["items"] if k not in below_union[i]]
        if not add_keys:
            continue
        step["sticker_adds"] = [ev["items"][k] for k in add_keys[:_MAX_ADDS_SHOWN]]
        step["sticker_adds_note"] = (
            f"Compared across {ev['sticker_count']} window sticker"
            f"{'s' if ev['sticker_count'] != 1 else ''} we photographed for this trim"
        )
        step["sticker_adds_car_ids"] = ev["car_ids"][:6]
        annotated += 1
    return annotated
