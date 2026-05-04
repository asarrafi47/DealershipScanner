#!/usr/bin/env python3
"""
Merge a JSON array (same shape as ``discover_dealerships.py --json``) into ``dealers.json``.

Examples::

  python scripts/discovery_merge_to_manifest.py discovered.json
  python scripts/discover_dealerships.py ... --json | python scripts/discovery_merge_to_manifest.py

Then run the scanner::

  python scanner.py
"""
from __future__ import annotations

import argparse
import json
import sys
import warnings
from pathlib import Path

warnings.filterwarnings(
    "ignore",
    message=r".*doesn't match a supported version.*",
)

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Merge discovery JSON export into dealers.json for scanner.py"
    )
    p.add_argument(
        "json_path",
        nargs="?",
        default=None,
        help="Path to JSON array file; omit to read stdin",
    )
    args = p.parse_args(argv)

    from backend.discovery.manifest_merge import merge_json_export_rows

    manifest_path = (ROOT / "dealers.json").resolve()

    if args.json_path:
        raw = Path(args.json_path).expanduser().read_text(encoding="utf-8")
        data = json.loads(raw)
    else:
        data = json.load(sys.stdin)

    if not isinstance(data, list):
        print("JSON must be an array of objects with name + url", file=sys.stderr)
        return 2

    stats = merge_json_export_rows(data, manifest_path=manifest_path)
    print(
        "dealers.json [%s] merge: inserted=%d updated=%d skipped=%d"
        % (manifest_path, stats["inserted"], stats["updated"], stats["skipped"]),
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
