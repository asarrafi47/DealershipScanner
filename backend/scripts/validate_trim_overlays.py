#!/usr/bin/env python3
"""Compare trim overlays, brochure candidates, and EPA trims; write validation report."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.dictionary_derived import epa_trim_names, get_overlay_status  # noqa: E402
from backend.enrichment.dictionary_paths import (  # noqa: E402
    BROCHURE_TRIM_CANDIDATES_DIR,
    TRIM_OVERLAY_VALIDATION_PATH,
)
from backend.enrichment.dictionary_catalog import build_manifest_entries  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--year-min", type=int, default=2015)
    args = p.parse_args()

    rows: list[dict] = []
    for entry in build_manifest_entries():
        year = entry.get("year")
        if year is None or int(year) < args.year_min:
            continue
        ck = str(entry.get("catalog_key") or "")
        overlay_path, overlay_status = get_overlay_status(ck)
        epa = epa_trim_names(entry.get("epa_path"))
        cand_trims: list[str] = []
        cand_path = BROCHURE_TRIM_CANDIDATES_DIR / f"{ck.replace('|', '__')}.json"
        if cand_path.is_file():
            try:
                c = json.loads(cand_path.read_text(encoding="utf-8"))
                cand_trims = list(c.get("trims_available") or [])
            except (OSError, json.JSONDecodeError):
                pass
        issues: list[str] = []
        if overlay_status == "curated" and cand_trims:
            ov_path = overlay_path
            if ov_path:
                from backend.enrichment.dictionary_paths import DICTIONARY_ROOT

                ov = json.loads((DICTIONARY_ROOT / ov_path).read_text(encoding="utf-8"))
                ov_trims = set((ov.get("adds_by_trim") or {}).keys())
                missing = [t for t in ov_trims if t not in cand_trims and t not in epa]
                if missing:
                    issues.append(f"overlay_trims_not_in_brochure_or_epa:{','.join(missing[:5])}")
        if overlay_status == "missing" and len(cand_trims) >= 2:
            issues.append("candidate_ready_no_overlay")
        if not epa and not cand_trims:
            issues.append("no_trim_sources")
        rows.append(
            {
                "catalog_key": ck,
                "year": year,
                "make": entry.get("make"),
                "model": entry.get("model"),
                "overlay_status": overlay_status,
                "epa_trim_count": len(epa),
                "brochure_trim_count": len(cand_trims),
                "issues": issues,
            }
        )

    TRIM_OVERLAY_VALIDATION_PATH.parent.mkdir(parents=True, exist_ok=True)
    with TRIM_OVERLAY_VALIDATION_PATH.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")
    flagged = sum(1 for r in rows if r.get("issues"))
    print(f"validation: rows={len(rows)} flagged={flagged} -> {TRIM_OVERLAY_VALIDATION_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
