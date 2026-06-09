#!/usr/bin/env python3
"""
Promote strong brochure extractions into trim_adds_by_year (GC quality bar).

Targets validation flags ``candidate_ready_no_overlay`` and YMMs with brochure
text but no curated overlay.

Usage:
    python -m backend.scripts.promote_trim_candidates --dry-run
    python -m backend.scripts.promote_trim_candidates --year-min 2020
    python -m backend.scripts.promote_trim_candidates --overwrite
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.brochure_promote import (  # noqa: E402
    existing_overlay_blocks_promotion,
    extract_promotable_overlay,
    promote_from_brochure_text,
)
from backend.enrichment.brochure_trim_candidates import load_brochure_text_json  # noqa: E402
from backend.enrichment.dictionary_paths import (  # noqa: E402
    TRIM_OVERLAY_VALIDATION_PATH,
)


def _catalog_keys_from_validation(*, year_min: int, only_flagged: bool) -> list[str]:
    if not TRIM_OVERLAY_VALIDATION_PATH.is_file():
        return []
    keys: list[str] = []
    for line in TRIM_OVERLAY_VALIDATION_PATH.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if int(row.get("year") or 0) < year_min:
            continue
        issues = row.get("issues") or []
        if only_flagged and "candidate_ready_no_overlay" not in issues:
            continue
        if str(row.get("overlay_status") or "") == "curated":
            continue
        ck = str(row.get("catalog_key") or "")
        if ck:
            keys.append(ck)
    return keys


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--overwrite", action="store_true", help="Replace promoted_brochure_auto overlays")
    p.add_argument("--year-min", type=int, default=2015)
    p.add_argument("--only-flagged", action="store_true", default=True)
    p.add_argument("--all-brochure", action="store_true", help="Try every catalog_key with brochure text")
    p.add_argument("--limit", type=int, default=0)
    args = p.parse_args()

    if args.all_brochure:
        from backend.enrichment.dictionary_paths import BROCHURE_TEXT_DIR

        keys = []
        for path in sorted(BROCHURE_TEXT_DIR.glob("*.json")):
            if path.name.startswith("_"):
                continue
            try:
                meta = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            y = int(meta.get("year") or 0)
            if y < args.year_min:
                continue
            ck = str(meta.get("catalog_key") or "")
            if ck:
                keys.append(ck)
    else:
        keys = _catalog_keys_from_validation(
            year_min=args.year_min,
            only_flagged=args.only_flagged,
        )

    if args.limit > 0:
        keys = keys[: args.limit]

    promoted = skipped = failed = 0
    samples: list[str] = []

    for ck in keys:
        if existing_overlay_blocks_promotion(ck) and not args.overwrite:
            skipped += 1
            continue
        data = load_brochure_text_json(ck)
        if not data:
            skipped += 1
            continue
        try:
            payload = extract_promotable_overlay(data)
            if not payload:
                skipped += 1
                continue
            if args.dry_run:
                promoted += 1
                if len(samples) < 12:
                    q = payload.get("promotion_quality") or {}
                    samples.append(
                        f"{ck} trims={q.get('trim_count')} "
                        f"strong_upper={q.get('upper_trims_with_adds')}"
                    )
                continue
            out = promote_from_brochure_text(ck, overwrite=args.overwrite)
            if out:
                promoted += 1
                if len(samples) < 12:
                    samples.append(ck)
            else:
                skipped += 1
        except Exception:
            failed += 1

    mode = "would_promote" if args.dry_run else "promoted"
    print(f"{mode}={promoted} skipped={skipped} failed={failed} (from {len(keys)} keys)")
    for s in samples:
        print(f"  {s}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
