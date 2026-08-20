"""Sticker-printed colors as the page's authoritative colors.

Policy (2026-08-18, explicit): the window sticker is the single source of
truth — a Bronco whose Monroney prints "Avalanche Gray" shows Avalanche Gray
even when its photos read as white, and a Grand Wagoneer whose build sheet
prints "Midnight Sky (PCQ)" / "Global Black (SLX7)" shows those. ONLY a color
the document PRINTS carries this authority: ``car_image_text`` summaries tag
each color pair with ``color_source`` — ``"monroney"`` means transcribed off
the sticker document itself, ``"photo"`` means observed on the physical car in
gallery images. Photo observations have proven unreliable (dark leather under
warm showroom light misreads black interiors as brown) and NEVER supersede the
feed; they stay in the summary for a future adoption pass to weigh.

Read-time overlay following the ``msrp_overlay`` pattern: the caller batches
the ``car_image_text`` read (``cars_repo.car_sticker_msrp_values``) and this
module only shapes the payload. The fields are ``sticker_exterior_color`` /
``sticker_interior_color`` — distinct keys, never folded into
``exterior_color`` / ``interior_color``: the display supersedes the feed value
with provenance shown ("per window sticker"), and the ``cars`` columns are
never overwritten.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

# Matches knowledge_engine_specs' cap on the OEM-sticker color fields: a color
# NAME is short, and anything longer is an agent's caveat that belongs in notes.
_MAX_COLOR_LEN = 120


def _color(value: Any) -> str | None:
    s = str(value or "").strip()
    return s[:_MAX_COLOR_LEN] if s else None


def color_overlay_public_fields(sticker_facts: Mapping[str, Any] | None) -> dict[str, Any]:
    """Fields to merge into a serialized car, or ``{}`` when nothing qualifies.

    *sticker_facts* is this car's entry from
    ``cars_repo.car_sticker_msrp_values`` — the caller batches that read; this
    never looks anything up. Only ``color_source == "monroney"`` (printed on
    the sticker document) grants authority; a ``"photo"``-sourced color yields
    ``{}`` no matter how confident the reading looked.
    """
    if not isinstance(sticker_facts, Mapping):
        return {}
    if str(sticker_facts.get("color_source") or "").strip().lower() != "monroney":
        return {}

    fields: dict[str, Any] = {}
    exterior = _color(sticker_facts.get("exterior_color_seen"))
    interior = _color(sticker_facts.get("interior_color_seen"))
    if exterior:
        fields["sticker_exterior_color"] = exterior
    if interior:
        fields["sticker_interior_color"] = interior
    if not fields:
        return {}
    fields["sticker_color_source"] = "window_sticker_photo"
    fields["sticker_color_note"] = (
        "Per the window sticker photographed in this listing's gallery."
    )
    return fields
