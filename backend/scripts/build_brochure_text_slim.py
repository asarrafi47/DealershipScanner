#!/usr/bin/env python3
"""Write slim brochure text JSON (trim pages only) under derived/brochure_text_slim/."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.dictionary_derived import slim_payload_from_full  # noqa: E402
from backend.enrichment.dictionary_paths import BROCHURE_TEXT_DIR, BROCHURE_TEXT_SLIM_DIR  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--overwrite", action="store_true")
    args = p.parse_args()

    BROCHURE_TEXT_SLIM_DIR.mkdir(parents=True, exist_ok=True)
    written = skipped = 0
    for src in sorted(BROCHURE_TEXT_DIR.glob("*.json")):
        if src.name.startswith("_"):
            continue
        dest = BROCHURE_TEXT_SLIM_DIR / src.name
        if dest.is_file() and not args.overwrite:
            skipped += 1
            continue
        try:
            data = json.loads(src.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            skipped += 1
            continue
        dest.write_text(json.dumps(slim_payload_from_full(data), indent=2), encoding="utf-8")
        written += 1

    print(f"slim: wrote={written} skipped={skipped} dir={BROCHURE_TEXT_SLIM_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
