"""
Merge LLaVA interior inference into inventory rows with safe overwrite rules.

Environment:

- ``INTERIOR_VISION_CONFIDENCE`` — minimum confidence (0-1) to apply vision merges,
  default ``0.55``.
- ``INTERIOR_VISION_OVERWRITE`` — when ``1``, allow overwriting non-placeholder
  ``interior_color`` with the vision guess text (use with care).

Provenance: ``spec_source_json`` gains ``interior_cabin_vision`` on every qualifying pass.
The ``interior_color`` provenance key is updated only when this module also sets the
``interior_color`` column (placeholder fill or overwrite), so listing-description
provenance is not clobbered when the dealer string is kept.

``packages["llava_interior_cabin"]`` stores a stable slice for UI badges.

Recommended model pull: ``ollama pull llava:13b`` (see ``ollama_llava.OLLAMA_VISION_MODEL``).
"""
from __future__ import annotations

import json
import os
from copy import deepcopy
from typing import Any

from backend.utils.field_clean import is_effectively_empty
from backend.utils.interior_color_buckets import infer_interior_color_buckets, merge_bucket_lists
from backend.utils.spec_provenance import merge_spec_source_json

INTERIOR_VISION_CONFIDENCE = float(os.environ.get("INTERIOR_VISION_CONFIDENCE", "0.55"))
INTERIOR_VISION_OVERWRITE = os.environ.get("INTERIOR_VISION_OVERWRITE", "").strip() in (
    "1",
    "true",
    "True",
    "yes",
    "YES",
)


def _parse_json_dict(val: Any) -> dict[str, Any]:
    if val is None:
        return {}
    if isinstance(val, dict):
        return dict(val)
    try:
        d = json.loads(str(val))
        return d if isinstance(d, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def build_updates_from_llava_result(
    *,
    row: dict[str, Any],
    llava: dict[str, Any],
) -> dict[str, Any]:
    """
    Given an existing DB row dict and LLaVA output from ``ollama_llava``, return kwargs for
    ``update_car_row_partial`` (only keys that should change).
    """
    conf = float(llava.get("confidence") or 0.0)
    if conf < INTERIOR_VISION_CONFIDENCE:
        return {}

    guess = str(llava.get("interior_guess_text") or "").strip()
    vision_buckets = [
        str(x).strip().lower()
        for x in (llava.get("interior_buckets") or [])
        if str(x).strip()
    ]

    if not guess and not vision_buckets:
        return {}

    interior_existing = row.get("interior_color")
    interior_empty = interior_existing is None or is_effectively_empty(str(interior_existing))

    new_interior: str | None = None
    if guess:
        if interior_empty:
            new_interior = guess[:120]
        elif INTERIOR_VISION_OVERWRITE:
            new_interior = guess[:120]

    base_text = (new_interior if new_interior is not None else str(interior_existing or "")).strip()
    lex_buckets = infer_interior_color_buckets(base_text if base_text else None, row.get("make"))
    merged_buckets = merge_bucket_lists(lex_buckets, vision_buckets)

    cabin_dict: dict[str, Any] = {
        "source": "llava_vision",
        "confidence": conf,
        "evidence": str(llava.get("evidence") or ""),
        "model": str(llava.get("model") or ""),
        "interior_buckets": vision_buckets,
        "interior_guess_text": guess,
    }
    prov_patch: dict[str, Any] = {"interior_cabin_vision": cabin_dict}
    if new_interior is not None:
        prov_patch["interior_color"] = {
            "source": "llava_vision",
            "confidence": conf,
            "evidence": str(llava.get("evidence") or ""),
            "model": str(llava.get("model") or ""),
            "buckets": vision_buckets,
        }

    spec_out = merge_spec_source_json(row.get("spec_source_json"), prov_patch)

    pkgs = deepcopy(_parse_json_dict(row.get("packages")))
    pkgs["llava_interior_cabin"] = {
        "interior_buckets": vision_buckets,
        "interior_guess_text": guess,
        "confidence": conf,
        "evidence": str(llava.get("evidence") or ""),
        "model": str(llava.get("model") or ""),
    }
    packages_out = json.dumps(pkgs, separators=(",", ":"))

    out: dict[str, Any] = {
        "spec_source_json": spec_out,
        "packages": packages_out,
        "interior_color_buckets": json.dumps(merged_buckets, separators=(",", ":")),
    }
    if new_interior is not None:
        out["interior_color"] = new_interior
    return out
