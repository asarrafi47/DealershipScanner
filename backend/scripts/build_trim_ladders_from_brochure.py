#!/usr/bin/env python3
"""Per-year trim ladders from brochure_text / trim_candidates (marketing trim names)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.brochure_trim_candidates import (  # noqa: E402
    filter_spurious_brochure_trims,
    load_brochure_text_json,
)
from backend.enrichment.dictionary_paths import (  # noqa: E402
    BROCHURE_TEXT_DIR,
    BROCHURE_TRIM_CANDIDATES_DIR,
    trim_ladders_brochure_path,
)
from backend.enrichment.trim_ladder import _is_valid_trim_name  # noqa: E402
from backend.enrichment.trim_ladder_knowledge import normalize_ladder_steps  # noqa: E402

_OUTPUT = trim_ladders_brochure_path()
_MIN_STEPS = 2


def _trims_for_catalog_key(ck: str) -> tuple[list[str], str, str, int]:
    cand = BROCHURE_TRIM_CANDIDATES_DIR / f"{ck.replace('|', '__')}.json"
    if cand.is_file():
        try:
            data = json.loads(cand.read_text(encoding="utf-8"))
            trims = filter_spurious_brochure_trims(
                list(data.get("trims_available") or []),
                make=str(data.get("make") or ""),
                model=str(data.get("model") or ""),
            )
            if len(trims) >= _MIN_STEPS:
                return (
                    trims,
                    str(data.get("make") or ""),
                    str(data.get("model") or ""),
                    int(data.get("year") or 0),
                )
        except (OSError, json.JSONDecodeError):
            pass
    full = load_brochure_text_json(ck)
    if not full:
        return [], "", "", 0
    from backend.enrichment.brochure_trim_candidates import parse_trim_names_from_text

    make = str(full.get("make") or "")
    model = str(full.get("model") or "")
    year = int(full.get("year") or 0)
    text = str(full.get("combined_trim_pages_text") or "")
    trims = parse_trim_names_from_text(text, make=make, model=model)
    trims = filter_spurious_brochure_trims(trims, make=make, model=model)
    return trims, make, model, year


def main() -> int:
    ladders: list[dict] = []
    seen: set[str] = set()

    for path in sorted(BROCHURE_TEXT_DIR.glob("*.json")):
        if path.name.startswith("_"):
            continue
        try:
            meta = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        ck = str(meta.get("catalog_key") or "")
        if not ck or ck in seen:
            continue
        trims, make, model, year = _trims_for_catalog_key(ck)
        if len(trims) < _MIN_STEPS or not make or not model or not year:
            continue
        steps = [
            {
                "name": t,
                "aliases": [],
                "adds": [],
            }
            for t in trims
            if _is_valid_trim_name(t, make=make, model=model)
        ]
        steps = normalize_ladder_steps(steps, make, model=model)
        if len(steps) < _MIN_STEPS:
            continue
        lid = f"brochure_{ck.replace('|', '__')}"
        seen.add(ck)
        ladders.append(
            {
                "id": lid,
                "catalog_key": ck,
                "make": make,
                "models": [model],
                "year_min": year,
                "year_max": year,
                "label": f"{make} {model} ({year}) brochure trims",
                "source": "brochure",
                "steps": steps,
            }
        )

    payload = {
        "version": 1,
        "generated_from": "derived/brochure_text + trim_candidates",
        "ladder_count": len(ladders),
        "ladders": ladders,
    }
    _OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    _OUTPUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {len(ladders)} brochure ladders to {_OUTPUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
