#!/usr/bin/env python3
"""Build ``index/manifest.json`` and ``index/dictionary_catalog.db`` from dictionary CSVs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.dictionary_catalog import (  # noqa: E402
    build_manifest_entries,
    invalidate_catalog_cache,
    rebuild_catalog,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build dictionary manifest + SQLite catalog.")
    parser.add_argument("--stats", action="store_true", help="Print summary counts only.")
    args = parser.parse_args()

    entries = build_manifest_entries()
    if args.stats:
        stubs = sum(1 for e in entries if e.get("options_status") == "stub")
        rich = sum(1 for e in entries if e.get("options_status") == "rich")
        with_epa = sum(1 for e in entries if e.get("epa_path"))
        print(f"entries={len(entries)} epa={with_epa} options_rich={rich} options_stub={stubs}")
        return 0

    manifest, db = rebuild_catalog(entries, enrich_derived=True)
    invalidate_catalog_cache()
    print(f"Wrote {manifest} ({len(entries)} entries)")
    print(f"Wrote {db}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
