#!/usr/bin/env python3
"""Build draft trim_candidates/*.json from derived/brochure_text/."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.brochure_trim_candidates import (  # noqa: E402
    build_trim_candidate,
    persist_trim_candidate,
)
from backend.enrichment.dictionary_paths import BROCHURE_TEXT_DIR  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--limit", type=int, default=0)
    args = p.parse_args()

    paths = sorted(p for p in BROCHURE_TEXT_DIR.glob("*.json") if not p.name.startswith("_"))
    if args.limit > 0:
        paths = paths[: args.limit]

    written = skipped = low = 0
    for path in paths:
        out = path.parent.parent / "trim_candidates" / path.name
        if out.is_file() and not args.overwrite:
            skipped += 1
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            skipped += 1
            continue
        year = int(data.get("year") or 0)
        make = str(data.get("make") or "")
        model = str(data.get("model") or "")
        if not year or not make or not model:
            skipped += 1
            continue
        payload = build_trim_candidate(data, make=make, model=model, year=year)
        if payload.get("quality") == "low":
            low += 1
        persist_trim_candidate(payload)
        written += 1

    print(f"candidates: wrote={written} skipped={skipped} low_quality={low}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
