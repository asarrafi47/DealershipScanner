#!/usr/bin/env python3
"""Merge curated + EPA + generated trim ladders into one JSON file."""

from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.dictionary_paths import (  # noqa: E402
    CURATED_DIR,
    trim_ladders_brochure_path,
    trim_ladders_curated_path,
    trim_ladders_epa_path,
    trim_ladders_generated_path,
    trim_ladders_merged_path,
)


def _read_ladders(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    ladders = data.get("ladders")
    return ladders if isinstance(ladders, list) else []


def merge_ladders() -> Path:
    CURATED_DIR.mkdir(parents=True, exist_ok=True)
    out_path = trim_ladders_merged_path()
    if out_path.parent != CURATED_DIR and CURATED_DIR.is_dir():
        out_path = CURATED_DIR / "trim_ladders_merged.json"

    merged: list[dict] = []
    seen_ids: set[str] = set()

    for path, rank in (
        (trim_ladders_curated_path(), 0),
        (trim_ladders_brochure_path(), 1),
        (trim_ladders_epa_path(), 2),
        (trim_ladders_generated_path(), 3),
    ):
        for ladder in _read_ladders(path):
            if not isinstance(ladder, dict):
                continue
            lid = str(ladder.get("id") or "")
            if not lid or lid in seen_ids:
                continue
            seen_ids.add(lid)
            item = dict(ladder)
            if not item.get("source"):
                if rank == 0:
                    item["source"] = "curated"
                elif rank == 1:
                    item["source"] = "brochure"
                elif rank == 2:
                    item["source"] = "epa"
                else:
                    item["source"] = "wikipedia"
            merged.append(item)

    payload = {
        "version": 1,
        "ladder_count": len(merged),
        "sources": {
            "curated": trim_ladders_curated_path().name,
            "brochure": trim_ladders_brochure_path().name,
            "epa": trim_ladders_epa_path().name,
            "generated": trim_ladders_generated_path().name,
        },
        "ladders": merged,
    }
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out_path


def main() -> int:
    out = merge_ladders()
    data = json.loads(out.read_text(encoding="utf-8"))
    print(f"Wrote {out} ({data.get('ladder_count', 0)} ladders)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
