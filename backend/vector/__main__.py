"""CLI: python -m backend.vector reindex"""

from __future__ import annotations

import argparse
import json
import sys


def main() -> int:
    p = argparse.ArgumentParser(description="Chroma vector index for DealershipScanner")
    p.add_argument("command", nargs="?", default="reindex", choices=("reindex",))
    args = p.parse_args()
    if args.command == "reindex":
        from backend.vector.chroma_service import reindex_all

        counts = reindex_all()
        print(json.dumps({"ok": True, "counts": counts}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
